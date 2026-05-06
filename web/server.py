#!/usr/bin/env python3
"""demo-csfle-cspe-cloud — single-port web UI on http://localhost:8893

Routes
  /                            — setup wizard (AWS creds + env discovery + cloud-side setup)
  /produce                     — producer page (form + buttons for both topics)
  /csfle/with-kek              — CSFLE consumer page, AWS creds in subprocess env
  /csfle/no-kek                — CSFLE consumer page, AWS creds stripped
  /cspe/with-kek               — CSPE  consumer page, AWS creds in subprocess env
  /cspe/no-kek                 — CSPE  consumer page, AWS creds stripped
  POST /aws-creds              — persist AWS creds to config/aws-session.env
  POST /bootstrap-step?n=0..3  — run scripts/0N_*.sh, stream stdout via SSE
  POST /produce-stream?topic=  — produce N records to the topic, stream subprocess stdout
  GET  /sse/<topic>/<role>     — spawn a consumer subprocess + stream its stdout
  GET  /env                    — return current .env values as JSON

Stack: Python stdlib only. Encryption / Kafka work happens in the CP 8.2.0
console producer/consumer Java CLI tools (subprocessed).
"""
from __future__ import annotations

import html
import json
import os
import select
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PORT       = 8893
REPO_DIR   = Path(__file__).resolve().parent.parent
ENV_FILE   = REPO_DIR / ".env"
AWS_FILE   = REPO_DIR / "config" / "aws-session.env"
CP_HOME    = Path.home() / "confluent-8.2.0"
JAVA_HOME  = "/opt/homebrew/opt/openjdk@21"
PRODUCER   = CP_HOME / "bin" / "kafka-json-schema-console-producer"
CONSUMER   = CP_HOME / "bin" / "kafka-json-schema-console-consumer"
EXECUTOR   = "io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor"

CONSUMER_PAGES = {
    # path-key             → (topic-key, role,           title,                                      accent)
    ("csfle", "with-kek"):  ("csfle",   "authorized",   "CSFLE Consumer — KEK access",              "#3fb950"),
    ("csfle", "no-kek"):    ("csfle",   "unauthorized", "CSFLE Consumer — NO KEK access",           "#e63946"),
    ("cspe",  "with-kek"):  ("cspe",    "authorized",   "CSPE Consumer — KEK access",               "#3fb950"),
    ("cspe",  "no-kek"):    ("cspe",    "unauthorized", "CSPE Consumer — NO KEK access",            "#e63946"),
}

CONSUMER_GROUPS = {
    ("csfle", "authorized"):   "demo-csfle-with-kek-web",
    ("csfle", "unauthorized"): "demo-csfle-no-kek-web",
    ("cspe",  "authorized"):   "demo-cspe-with-kek-web",
    ("cspe",  "unauthorized"): "demo-cspe-no-kek-web",
}

BOOTSTRAP_SCRIPTS = [
    # 00_discover_env.sh is intentionally omitted — the wizard's card 3 does
    # discovery interactively (env picker → cluster picker) and persists the
    # IDs + minted API keys via /cc-pick. The script remains in scripts/ for
    # CLI/Makefile users who set ENV_ID first.
    ("01_setup_keks.sh",     "Create 2 KEKs in AWS KMS + register in SR DEK Registry"),
    ("02_register_schemas.sh","Register CSFLE + CSPE schemas with their rule sets"),
    ("03_create_topics.sh",  "Create both Kafka topics in Confluent Cloud"),
]

# ── env-file helpers ─────────────────────────────────────────────────────────

def _read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


def _write_aws_session(key_id: str, secret: str, token: str, region: str) -> None:
    """Persist AWS creds to config/aws-session.env (gitignored)."""
    AWS_FILE.parent.mkdir(parents=True, exist_ok=True)
    body = (
        f"AWS_ACCESS_KEY_ID={key_id}\n"
        f"AWS_SECRET_ACCESS_KEY={secret}\n"
        + (f"AWS_SESSION_TOKEN={token}\n" if token else "")
        + f"AWS_REGION={region or 'us-west-2'}\n"
    )
    AWS_FILE.write_text(body)
    os.chmod(AWS_FILE, 0o600)


def _aws_creds() -> dict[str, str]:
    """Return AWS creds for an AUTHORIZED subprocess, in priority order:
    config/aws-session.env → ~/.aws/credentials → process env."""
    f = _read_env_file(AWS_FILE)
    if f.get("AWS_ACCESS_KEY_ID"):
        return {k: v for k, v in f.items() if k.startswith("AWS_")}
    creds_path = Path.home() / ".aws" / "credentials"
    if creds_path.exists():
        # Minimal parse — handle [default] only for now; users with profiles
        # can copy creds into config/aws-session.env via the wizard.
        section = None
        out: dict[str, str] = {}
        for line in creds_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1]
            elif section in ("default", os.environ.get("AWS_PROFILE", "default")) and "=" in line:
                k, _, v = line.partition("=")
                m = {
                    "aws_access_key_id":     "AWS_ACCESS_KEY_ID",
                    "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
                    "aws_session_token":     "AWS_SESSION_TOKEN",
                }.get(k.strip())
                if m:
                    out[m] = v.strip()
        if "AWS_REGION" in os.environ:
            out["AWS_REGION"] = os.environ["AWS_REGION"]
        if out:
            return out
    return {k: os.environ[k] for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                                       "AWS_SESSION_TOKEN", "AWS_REGION") if k in os.environ}


# ── subprocess command builders ──────────────────────────────────────────────

# CP 8.2.0 deprecated --consumer-property / --producer-property / --property.
# The replacements are:
#   --command-property    Kafka client config (security.protocol, sasl.*)
#   --formatter-property  consumer-side serdes config (schema.registry.url,
#                         rule.executors, etc. — formatter is the deserializer)
#   --reader-property     producer-side serdes config (input reader before send)
# All three deprecated flags still work, but they emit warnings on every line
# and the page UI surfaces them as red error rows. Use the new names.

def _principal_keys(env: dict[str, str], topic_key: str, role: str) -> dict[str, str]:
    """Resolve which Kafka + SR API keys a given (topic, role) pair should use.

    Each (topic, role) maps to a unique service account so each consumer page
    has only the KEK access it needs (CSFLE consumers can't see the CSPE KEK
    and vice versa). Six prefixes total:
      CSFLE_PRODUCER, CSPE_PRODUCER,
      CSFLE_CONSUMER_KEK, CSFLE_CONSUMER_NOKEK,
      CSPE_CONSUMER_KEK,  CSPE_CONSUMER_NOKEK

    Falls back to the OrgAdmin keys (KAFKA_API_KEY/SR_API_KEY) if the per-SA
    keys haven't been minted yet — keeps the Makefile CLI flow working before
    the wizard's RBAC step has run, and keeps backward compat with pre-RBAC
    .env files."""
    role_suffix = {
        "producer":          "PRODUCER",
        "consumer-with-kek": "CONSUMER_KEK",
        "consumer-no-kek":   "CONSUMER_NOKEK",
    }.get(role)
    prefix = f"{topic_key.upper()}_{role_suffix}" if role_suffix else None
    if prefix and env.get(f"{prefix}_KAFKA_API_KEY") and env.get(f"{prefix}_SR_API_KEY"):
        return {
            "kafka_key":    env[f"{prefix}_KAFKA_API_KEY"],
            "kafka_secret": env[f"{prefix}_KAFKA_API_SECRET"],
            "sr_key":       env[f"{prefix}_SR_API_KEY"],
            "sr_secret":    env[f"{prefix}_SR_API_SECRET"],
            "sa_id":        env.get(f"{prefix}_SA_ID", ""),
            "principal":    f"{topic_key}-{role}",
        }
    # Fallback: use OrgAdmin-derived keys
    return {
        "kafka_key":    env.get("KAFKA_API_KEY", ""),
        "kafka_secret": env.get("KAFKA_API_SECRET", ""),
        "sr_key":       env.get("SR_API_KEY", ""),
        "sr_secret":    env.get("SR_API_SECRET", ""),
        "sa_id":        "",
        "principal":    "orgadmin (RBAC step not yet run — using fallback OrgAdmin keys)",
    }


def _common_sr_props(env: dict[str, str], keys: dict[str, str], *, side: str) -> list[str]:
    """SR auth + URL flags. `side` selects --reader-property (producer) vs
    --formatter-property (consumer) — they configure the same underlying
    JSON-schema serializer / deserializer respectively."""
    flag = "--reader-property" if side == "producer" else "--formatter-property"
    return [
        flag, f"schema.registry.url={env['SR_URL']}",
        flag, "basic.auth.credentials.source=USER_INFO",
        flag, f"basic.auth.user.info={keys['sr_key']}:{keys['sr_secret']}",
    ]


def _common_kafka_props(env: dict[str, str], keys: dict[str, str]) -> list[str]:
    """Kafka client (producer or consumer) wire-protocol config — same flag
    name on both sides since CP 8.2.0."""
    sasl_jaas = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{keys["kafka_key"]}" password="{keys["kafka_secret"]}";'
    )
    return [
        "--bootstrap-server", env["BOOTSTRAP_SERVERS"],
        "--command-property", "security.protocol=SASL_SSL",
        "--command-property", "sasl.mechanism=PLAIN",
        "--command-property", f"sasl.jaas.config={sasl_jaas}",
    ]


def _build_consumer_cmd(topic_key: str, role: str, from_beginning: bool) -> list[str]:
    env = _read_env_file(ENV_FILE)
    topic = env[f"{topic_key.upper()}_TOPIC"]
    # Per-(topic,role) principal: each consumer page uses its own SA so e.g.
    # the CSFLE-with-KEK consumer can't see the CSPE KEK and vice versa.
    keys = _principal_keys(env, topic_key,
                           "consumer-with-kek" if role == "authorized" else "consumer-no-kek")
    cmd = [str(CONSUMER), "--topic", topic,
           *_common_kafka_props(env, keys),
           *_common_sr_props(env, keys, side="consumer")]
    if from_beginning:
        cmd += ["--from-beginning"]
    cmd += ["--group", f"{CONSUMER_GROUPS[(topic_key, role)]}-{int(time.time())}" if from_beginning
                       else CONSUMER_GROUPS[(topic_key, role)]]
    cmd += [
        "--formatter-property", "rule.executors=_default_",
        "--formatter-property", f"rule.executors._default_.class={EXECUTOR}",
        # Cloud CSFLE/CSPE: no client-side license JWT needed. Stream Governance
        # Advanced on the env covers the executor authorization. (On-prem CP
        # would also need confluent.value.encryption.license=<JWT> here.)
    ]
    return cmd


def _consumer_env(role: str) -> dict[str, str]:
    """Build subprocess env. Always start from the parent env so PATH/JAVA_HOME
    propagate, then either inject AWS creds (authorized) or block them
    (unauthorized — including IMDS to prevent EC2 instance role fallback)."""
    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        env.pop(k, None)
    if role == "authorized":
        env.update(_aws_creds())
    else:
        env["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        env["AWS_CONFIG_FILE"]             = "/dev/null"
        env["AWS_EC2_METADATA_DISABLED"]   = "true"
    return env


def _build_producer_cmd(topic_key: str) -> list[str]:
    env = _read_env_file(ENV_FILE)
    topic = env[f"{topic_key.upper()}_TOPIC"]
    # Per-topic producer principal: csfle-producer SA writes to mortgage-csfle
    # only (DevWrite on Topic:csfle + Subject:csfle-value + Kek:csfle-kek);
    # cspe-producer SA writes to mortgage-cspe only. Schema-id fetch uses the
    # OrgAdmin SR key (the producer SA might not have broad-enough subject
    # read for the latest-version lookup; safer to use the admin key for that
    # one query, then hand the resolved id to the subprocess which uses
    # producer SA's keys).
    keys = _principal_keys(env, topic_key, "producer")
    schema_id = _fetch_latest_schema_id(env, f"{topic}-value")
    return [
        str(PRODUCER), "--topic", topic,
        *_common_kafka_props(env, keys),
        *_common_sr_props(env, keys, side="producer"),
        "--reader-property", f"value.schema.id={schema_id}",
        "--reader-property", "auto.register.schemas=false",
        "--reader-property", "use.latest.version=true",
        "--reader-property", "rule.executors=_default_",
        "--reader-property", f"rule.executors._default_.class={EXECUTOR}",
        # Cloud: no license property — Stream Governance Advanced on the env
        # is what authorizes the encryption executor.
    ]


def _fetch_latest_schema_id(env: dict[str, str], subject: str) -> str:
    """GET /subjects/{subject}/versions/latest, return id as string."""
    import base64
    import urllib.request
    auth = "Basic " + base64.b64encode(
        f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()
    ).decode()
    req = urllib.request.Request(
        f"{env['SR_URL']}/subjects/{subject}/versions/latest",
        headers={"Authorization": auth},
    )
    try:
        return str(json.loads(urllib.request.urlopen(req, timeout=10).read())["id"])
    except Exception as e:
        raise RuntimeError(f"could not fetch schema id for {subject}: {e}") from e


def _fetch_schema_full(env: dict[str, str], subject: str) -> dict:
    """Return the raw SR `/subjects/<s>/versions/latest` response, with the
    embedded `schema` string parsed back into a dict for pretty-printing.
    Adds {"error": "..."} when the fetch fails (caller renders an error
    placeholder instead of the schema body)."""
    if not env.get("SR_URL") or not env.get("SR_API_KEY"):
        return {"error": "SR not configured"}
    import base64
    import urllib.error
    import urllib.request
    auth = "Basic " + base64.b64encode(
        f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()
    ).decode()
    req = urllib.request.Request(
        f"{env['SR_URL']}/subjects/{subject}/versions/latest",
        headers={"Authorization": auth},
    )
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=5).read())
    except urllib.error.HTTPError as e:
        return {"error": "not registered yet — run setup card 4" if e.code == 404
                          else f"HTTP {e.code}"}
    except Exception as e:
        return {"error": str(e)[:100]}
    # SR returns the schema as a JSON-encoded *string* — decode it for display
    if isinstance(d.get("schema"), str):
        try:
            d["schema_parsed"] = json.loads(d["schema"])
        except json.JSONDecodeError:
            d["schema_parsed"] = d["schema"]
    return d


def _fetch_schema_info(env: dict[str, str], subject: str) -> dict:
    """Best-effort schema details for the wizard's status card. Returns
    {id, version, rule_type, scope, kek, algo} on success, {"error": "..."}
    on any failure (SR not configured, schema not registered yet, network
    error, auth error). The wizard renders both shapes — never raises so
    page render isn't blocked by a transient SR issue."""
    if not env.get("SR_URL") or not env.get("SR_API_KEY"):
        return {"error": "SR not configured"}
    import base64
    import urllib.error
    import urllib.request
    auth = "Basic " + base64.b64encode(
        f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()
    ).decode()
    req = urllib.request.Request(
        f"{env['SR_URL']}/subjects/{subject}/versions/latest",
        headers={"Authorization": auth},
    )
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=5).read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"error": "not registered yet — run setup card 4"}
        return {"error": f"HTTP {e.code}"}
    except Exception as e:
        return {"error": str(e)[:100]}
    rs = d.get("ruleSet") or {}
    # CSFLE uses domainRules, CSPE uses encodingRules — show whichever's present
    rules = rs.get("domainRules") or rs.get("encodingRules") or []
    out = {"id": d.get("id", ""), "version": d.get("version", "")}
    if rules:
        r = rules[0]
        params = r.get("params") or {}
        tags = r.get("tags") or []
        out.update({
            "rule_type": r.get("type", ""),
            "scope":     ",".join(tags) if tags else "payload",
            "kek":       params.get("encrypt.kek.name", ""),
            "algo":      params.get("encrypt.algorithm", ""),
        })
    return out


# ── env-file upsert (used by /cc-pick to persist discovered values) ─────────

def _upsert_env(updates: dict[str, str]) -> None:
    """Replace keys in ENV_FILE if present, append otherwise. Preserves
    non-targeted lines (comments, ordering)."""
    if not ENV_FILE.exists():
        ENV_FILE.write_text((REPO_DIR / ".env.example").read_text())
    lines = ENV_FILE.read_text().splitlines()
    seen: set[str] = set()
    for i, line in enumerate(lines):
        if "=" in line and not line.lstrip().startswith("#"):
            k = line.split("=", 1)[0].strip()
            if k in updates:
                lines[i] = f"{k}={updates[k]}"
                seen.add(k)
    for k, v in updates.items():
        if k not in seen:
            lines.append(f"{k}={v}")
    ENV_FILE.write_text("\n".join(lines) + ("\n" if lines and not lines[-1].endswith("\n") else ""))


# ── Confluent Cloud helpers (used by /cc-* endpoints) ───────────────────────

def _run_confluent(args: list[str], *, env_extra: dict[str, str] | None = None,
                   timeout: int = 60) -> tuple[int, str, str]:
    """Run `confluent <args>`, return (rc, stdout, stderr)."""
    e = os.environ.copy()
    if env_extra:
        e.update(env_extra)
    try:
        p = subprocess.run(
            ["confluent", *args],
            capture_output=True, text=True, timeout=timeout, env=e,
        )
        return p.returncode, p.stdout, p.stderr
    except FileNotFoundError:
        return 127, "", "confluent CLI not found on PATH"
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout}s"


def _cc_status() -> dict:
    """Return {logged_in: bool, user: str|None}. Probes via `context list` —
    the current context is the marked one, and its `credential` field carries
    the username (e.g. "username-foo@bar.com"). Verified by an env-list call
    so a stale context that no longer authenticates reports as logged out."""
    rc, out, _ = _run_confluent(["context", "list", "-o", "json"], timeout=10)
    if rc != 0 or not out.strip():
        return {"logged_in": False, "user": None}
    try:
        ctxs = json.loads(out)
    except json.JSONDecodeError:
        return {"logged_in": False, "user": None}
    current = next((c for c in ctxs if c.get("is_current")), None)
    if not current:
        return {"logged_in": False, "user": None}
    cred = current.get("credential") or ""
    user = cred.removeprefix("username-") if cred.startswith("username-") else (current.get("name") or "")
    rc2, _, _ = _run_confluent(["environment", "list", "-o", "json"], timeout=10)
    return {"logged_in": rc2 == 0, "user": user}


def _cc_login(email: str, password: str) -> tuple[bool, str]:
    """Run `confluent login --save` with creds via env vars. Returns (ok, msg)."""
    if not email or not password:
        return False, "email + password required"
    rc, out, err = _run_confluent(
        ["login", "--save", "--no-browser"],
        env_extra={
            "CONFLUENT_CLOUD_EMAIL":    email,
            "CONFLUENT_CLOUD_PASSWORD": password,
        },
        timeout=30,
    )
    if rc == 0:
        return True, (out.strip() or f"signed in as {email}")
    return False, (err.strip() or out.strip() or f"login failed (rc={rc})")


def _cc_envs() -> list[dict]:
    rc, out, err = _run_confluent(["environment", "list", "-o", "json"], timeout=15)
    if rc != 0:
        raise RuntimeError(err.strip() or f"environment list failed (rc={rc})")
    return json.loads(out)


def _cc_clusters(env_id: str) -> list[dict]:
    rc, out, err = _run_confluent(
        ["kafka", "cluster", "list", "--environment", env_id, "-o", "json"],
        timeout=15,
    )
    if rc != 0:
        raise RuntimeError(err.strip() or f"cluster list failed (rc={rc})")
    return json.loads(out)


def _cc_describe_cluster(env_id: str, cluster_id: str) -> dict:
    rc, out, err = _run_confluent(
        ["kafka", "cluster", "describe", cluster_id, "--environment", env_id, "-o", "json"],
        timeout=15,
    )
    if rc != 0:
        raise RuntimeError(err.strip() or f"cluster describe failed (rc={rc})")
    return json.loads(out)


def _cc_describe_sr(env_id: str) -> dict:
    """Describe the Schema Registry for env_id. Returns {} if SR not enabled."""
    rc, out, err = _run_confluent(
        ["schema-registry", "cluster", "describe", "--environment", env_id, "-o", "json"],
        timeout=15,
    )
    if rc != 0:
        # SR not enabled — caller decides what to do
        return {}
    d = json.loads(out)
    return {
        # Newer CLI uses 'cluster' / 'endpoint_url'; older used 'id' / 'endpoint'.
        "id":  d.get("cluster")     or d.get("id")       or "",
        "url": d.get("endpoint_url") or d.get("endpoint") or "",
    }


def _cc_create_api_key(resource_id: str, env_id: str, description: str) -> dict:
    rc, out, err = _run_confluent(
        ["api-key", "create", "--resource", resource_id,
         "--environment", env_id, "--description", description, "-o", "json"],
        timeout=30,
    )
    if rc != 0:
        raise RuntimeError(err.strip() or f"api-key create failed (rc={rc})")
    return json.loads(out)


def _cc_delete_api_key_quiet(api_key: str) -> None:
    """Best-effort delete of an orphaned API key (e.g. when /cc-pick switches
    to a new cluster). Logs failures to stderr but never raises — the user can
    always clean up manually via Cloud UI or `confluent api-key delete`."""
    if not api_key:
        return
    rc, _out, err = _run_confluent(["api-key", "delete", api_key, "--force"], timeout=20)
    if rc != 0:
        sys.stderr.write(f"  ⚠ failed to delete orphaned API key {api_key}: "
                         f"{(err or '').strip()[:200]}\n")


# ── Service account + RBAC role binding helpers ──────────────────────────────

def _cc_create_service_account(name: str, description: str) -> dict:
    """Create a CC service account, return the parsed JSON. Idempotent in the
    sense that hitting an existing name returns the existing SA — but the CLI
    actually errors with 'already exists', so the caller handles that path
    via _cc_find_service_account_by_name."""
    rc, out, err = _run_confluent(
        ["iam", "service-account", "create", name, "--description", description, "-o", "json"],
        timeout=20,
    )
    if rc == 0:
        return json.loads(out)
    # CLI returns non-zero on duplicate name. Phrasing varies:
    #   "already exists" (older CLI), "already in use" (current CLI 4.59+)
    msg = ((err or "") + (out or "")).lower()
    if "already exists" in msg or "already in use" in msg:
        existing = _cc_find_service_account_by_name(name)
        if existing:
            return existing
        raise RuntimeError(f"sa '{name}' exists but couldn't be looked up by name "
                           f"(possibly outside this CLI's pagination window — "
                           f"delete manually or use a different env)")
    raise RuntimeError(err.strip() or out.strip() or f"sa create failed (rc={rc})")


def _cc_find_service_account_by_name(name: str) -> dict | None:
    rc, out, _err = _run_confluent(
        ["iam", "service-account", "list", "-o", "json"], timeout=30,
    )
    if rc != 0:
        return None
    try:
        for sa in json.loads(out):
            if sa.get("name") == name:
                return sa
    except json.JSONDecodeError:
        pass
    return None


def _cc_delete_service_account_quiet(sa_id: str) -> None:
    """Delete an SA. Cascades to its API keys + role bindings on the CC side."""
    if not sa_id:
        return
    rc, _out, err = _run_confluent(
        ["iam", "service-account", "delete", sa_id, "--force"], timeout=20,
    )
    if rc != 0:
        sys.stderr.write(f"  ⚠ failed to delete SA {sa_id}: "
                         f"{(err or '').strip()[:200]}\n")


def _cc_role_binding_create(sa_id: str, role: str, resource: str, env_id: str,
                            *, kafka_cluster: str | None = None,
                            sr_cluster: str | None = None,
                            prefixed: bool = False) -> tuple[bool, str]:
    """Create a single role binding. Returns (ok, msg). Idempotent on the CC
    side: rebinding an identical (principal, role, resource, scope) tuple is
    rejected with 'already exists' which we treat as success.

    `prefixed=True` makes the resource name a prefix match instead of a
    literal — needed for consumer groups since the wizard appends a timestamp
    to the group name on 'Start from beginning' (so the actual group is
    `demo-csfle-with-kek-web-1778026936`, not `demo-csfle-with-kek-web`)."""
    args = ["iam", "rbac", "role-binding", "create",
            "--principal", f"User:{sa_id}",
            "--role", role,
            "--resource", resource,
            "--environment", env_id]
    if prefixed:
        args.append("--prefix")
    if kafka_cluster:
        # Kafka-scoped bindings (Topic:*, Group:*, Cluster:*) need BOTH the
        # cluster's cloud-cluster ID and its kafka-cluster ID. They're the
        # same value (the lkc-XXX) but the CLI rejects the binding with
        # "cluster type needs org, env, and cloud-cluster" if --cloud-cluster
        # is missing.
        args += ["--cloud-cluster", kafka_cluster, "--kafka-cluster", kafka_cluster]
    if sr_cluster:
        args += ["--schema-registry-cluster", sr_cluster]
    rc, out, err = _run_confluent(args, timeout=20)
    if rc == 0:
        return True, f"bound {role} on {resource}"
    msg = (err or out or "").strip()
    if "already exists" in msg.lower():
        return True, f"binding already exists: {role} on {resource}"
    return False, msg[:200]


def _cc_ensure_cloud_api_key() -> tuple[str, str]:
    """Return (key, secret) for a cloud-scoped API key — needed for SRCM v3
    calls (which Kafka/SR-scoped keys can't authenticate). Reuses one from
    .env if present; otherwise mints a fresh one and persists. The minted key
    is OrgAdmin-scoped (inherits the logged-in user's roles)."""
    env = _read_env_file(ENV_FILE)
    if env.get("CLOUD_API_KEY") and env.get("CLOUD_API_SECRET"):
        return env["CLOUD_API_KEY"], env["CLOUD_API_SECRET"]
    rc, out, err = _run_confluent(
        ["api-key", "create", "--resource", "cloud",
         "--description", "demo-csfle-cspe-cloud (SG upgrade + admin)", "-o", "json"],
        timeout=30,
    )
    if rc != 0:
        raise RuntimeError(err.strip() or f"cloud api-key create failed (rc={rc})")
    k = json.loads(out)
    _upsert_env({"CLOUD_API_KEY": k["api_key"], "CLOUD_API_SECRET": k["api_secret"]})
    return k["api_key"], k["api_secret"]


def _srcm_request(method: str, sr_id: str, env_id: str,
                  cloud_key: str, cloud_secret: str,
                  body: dict | None = None) -> dict:
    """Issue a method (GET/PATCH) against /srcm/v3/clusters/<sr_id>. PATCH body
    follows the Confluent envelope shape: {"spec": {"package": "ADVANCED"}}."""
    import base64
    import urllib.request
    auth = "Basic " + base64.b64encode(f"{cloud_key}:{cloud_secret}".encode()).decode()
    url = f"https://api.confluent.cloud/srcm/v3/clusters/{sr_id}?environment={env_id}"
    headers = {"Authorization": auth, "Content-Type": "application/json"}
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def _cc_sg_package(sr_id: str, env_id: str,
                   cloud_key: str, cloud_secret: str) -> str:
    """Return the SR cluster's current package — typically 'ESSENTIALS' or
    'ADVANCED'. Empty string if the field is missing."""
    spec = _srcm_request("GET", sr_id, env_id, cloud_key, cloud_secret).get("spec") or {}
    return spec.get("package") or ""


def _cc_upgrade_sg(sr_id: str, env_id: str,
                   cloud_key: str, cloud_secret: str,
                   target: str = "ADVANCED") -> str:
    """PATCH the SR cluster to set spec.package to `target` and return the
    resulting package value. Idempotent: if already on the target, the API
    accepts the no-op PATCH and returns the same value."""
    out = _srcm_request("PATCH", sr_id, env_id, cloud_key, cloud_secret,
                        body={"spec": {"package": target}})
    return (out.get("spec") or {}).get("package") or ""


# ── HTML pages ───────────────────────────────────────────────────────────────

CSS = """*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0d1117;color:#c9d1d9;padding:1.6rem;min-height:100vh}
header{padding-left:1rem;margin-bottom:1.4rem}
header.accent{border-left:4px solid var(--accent,#58a6ff)}
h1{font-size:1.35rem;color:#f0f6fc}
.sub{font-size:.85rem;color:#8b949e;margin-top:.3rem}
nav{margin-bottom:1.4rem;padding-bottom:.8rem;border-bottom:1px solid #30363d;display:flex;gap:.6rem;flex-wrap:wrap;font-size:.82rem}
nav a{color:#58a6ff;text-decoration:none;padding:.25rem .55rem;border-radius:4px;border:1px solid #30363d}
nav a:hover{background:#161b22}
nav a.current{background:#1f6feb;color:#fff;border-color:#1f6feb}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:1.1rem;margin-bottom:1rem}
.card-title{font-size:.85rem;font-weight:700;color:#f0f6fc;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #30363d;padding-bottom:.55rem;margin-bottom:.85rem}
.card-sub{font-size:.78rem;color:#8b949e;margin-bottom:.7rem}
.row{display:flex;gap:.55rem;align-items:center;font-size:.83rem;padding:.3rem 0;border-bottom:1px solid #1c2128}
.row .lbl{color:#8b949e;min-width:170px}
.row .val{color:#c9d1d9;font-family:'SFMono-Regular',Consolas,monospace;font-size:.78rem;word-break:break-all}
.row .val.empty{color:#484f58;font-style:italic}
.btn{background:#1f6feb;color:#f0f6fc;border:none;border-radius:6px;padding:.45rem 1rem;font-size:.83rem;font-weight:600;cursor:pointer;transition:opacity .15s}
.btn:hover{opacity:.85}
.btn:disabled{opacity:.4;cursor:default}
.btn-secondary{background:#21262d;border:1px solid #30363d}
.btn-row{display:flex;gap:.6rem;margin-top:.7rem;flex-wrap:wrap}
input[type=text],input[type=password]{background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:.4rem .55rem;border-radius:4px;font-family:'SFMono-Regular',Consolas,monospace;font-size:.78rem;width:100%}
label{display:block;font-size:.78rem;color:#8b949e;margin:.55rem 0 .25rem}
.log{margin-top:.6rem;background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:.6rem;font-family:'SFMono-Regular',Consolas,monospace;font-size:.74rem;max-height:380px;overflow-y:auto;white-space:pre-wrap;word-break:break-word}
.log-ok{color:#3fb950}
.log-err{color:#e63946}
.log-dim{color:#484f58}
.records{display:grid;gap:.6rem}
.rec{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:.7rem;font-family:'SFMono-Regular',Consolas,monospace;font-size:.78rem;line-height:1.45;white-space:pre-wrap;word-break:break-word}
.rec.opaque{color:#8b949e;border-color:#4a1528}
.rec .pii-decoded{background:#1b4332;color:#3fb950;padding:.05rem .25rem;border-radius:3px}
.rec .pii-cipher{background:#4a1528;color:#ff7b72;padding:.05rem .25rem;border-radius:3px}
.banner{padding:.7rem 1rem;border-radius:6px;font-size:.82rem;margin-bottom:1rem}
.banner.warn{background:#3a2400;border:1px solid #d29922;color:#f0c674}
.banner.ok{background:#0e3019;border:1px solid #3fb950;color:#3fb950}
.banner.err{background:#3a0e15;border:1px solid #e63946;color:#ff7b72}
.kbd{font-family:'SFMono-Regular',Consolas,monospace;background:#0d1117;border:1px solid #30363d;border-radius:3px;padding:.05rem .35rem;font-size:.74rem}
"""

NAV_LINKS = [
    ("home",         "/",                   "Setup"),
    ("csfle-prod",   "/produce/csfle",      "CSFLE Producer"),
    ("csfle-auth",   "/csfle/with-kek",     "CSFLE w/ KEK"),
    ("csfle-noauth", "/csfle/no-kek",       "CSFLE no KEK"),
    ("cspe-prod",    "/produce/cspe",       "CSPE Producer"),
    ("cspe-auth",    "/cspe/with-kek",      "CSPE w/ KEK"),
    ("cspe-noauth",  "/cspe/no-kek",        "CSPE no KEK"),
]


def _nav(current: str) -> str:
    parts = ["<nav>"]
    for key, href, label in NAV_LINKS:
        cls = "current" if key == current else ""
        parts.append(f'  <a href="{href}" class="{cls}">{label}</a>')
    parts.append("</nav>")
    return "\n".join(parts)


def _page_shell(title: str, body: str, accent: str = "#58a6ff", nav_key: str = "home") -> str:
    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><title>{html.escape(title)}</title>
<style>:root{{--accent:{accent}}}{CSS}</style>
</head><body>
{_nav(nav_key)}
{body}
</body></html>"""


def _render_schema_rows(env: dict[str, str]) -> str:
    """Build the 2 schema-detail rows for the 'Current configuration' card.
    One row per topic — shows id, version, rule type, scope, KEK, algorithm.
    Empty string when SR isn't reachable yet (keeps the card lean before
    setup runs)."""
    rows = []
    for label, topic_key in (("CSFLE", "csfle"), ("CSPE", "cspe")):
        topic = env.get(f"{topic_key.upper()}_TOPIC")
        if not topic:
            continue
        subject = f"{topic}-value"
        info = _fetch_schema_info(env, subject)
        if "error" in info:
            val = f'<span class="val empty">({html.escape(info["error"])})</span>'
        else:
            parts = [f'id {info["id"]}', f'v{info["version"]}']
            if info.get("rule_type"):
                parts.append(f'{info["rule_type"]} [{info.get("scope","")}]')
            if info.get("kek"):
                parts.append(f'KEK={info["kek"]}')
            if info.get("algo"):
                parts.append(info["algo"])
            val = f'<span class="val">{html.escape(" · ".join(parts))}</span>'
        lbl = html.escape(f"{label} schema ({subject})")
        rows.append(f'<div class="row"><span class="lbl">{lbl}</span>{val}</div>')
    return "\n  ".join(rows)


def _render_schema_definition(env: dict[str, str], subject: str) -> str:
    """For the producer pages: pull the registered schema (body + ruleSet)
    from SR and render it as two pretty-printed JSON blocks. Replaces the old
    'Source data' card — the actual registered schema is more useful than a
    pointer to the local sample file. Renders an error placeholder when the
    fetch fails (so the page always loads)."""
    sd = _fetch_schema_full(env, subject)
    if "error" in sd:
        return (f'<div class="card-sub" style="color:#e63946">'
                f'Schema unavailable for {html.escape(subject)}: '
                f'{html.escape(sd["error"])}</div>')
    schema_body = json.dumps(sd.get("schema_parsed", {}), indent=2, ensure_ascii=False)
    rs = sd.get("ruleSet") or {}
    rules = rs.get("domainRules") or rs.get("encodingRules") or []
    rules_kind = ("domainRules" if rs.get("domainRules") else
                  "encodingRules" if rs.get("encodingRules") else "(none)")
    rules_str = json.dumps(rules, indent=2, ensure_ascii=False) if rules else "(no rules)"
    return f"""
  <div class="row"><span class="lbl">Subject</span><span class="val">{html.escape(subject)}</span></div>
  <div class="row"><span class="lbl">Schema id · version</span><span class="val">{sd.get("id","?")} · v{sd.get("version","?")}</span></div>
  <div class="card-sub" style="margin-top:.85rem">JSON Schema body</div>
  <pre class="rec">{html.escape(schema_body)}</pre>
  <div class="card-sub" style="margin-top:.85rem">Encryption rules ({html.escape(rules_kind)})</div>
  <pre class="rec">{html.escape(rules_str)}</pre>"""


def build_wizard_page() -> str:
    env = _read_env_file(ENV_FILE)
    aws = _read_env_file(AWS_FILE)
    def row(lbl: str, val: str) -> str:
        # html.escape() on every value — prevents env_name / cluster_name /
        # topic names (which can contain user-controlled data) from breaking
        # the page or injecting JS.
        cls = "val" if val else "val empty"
        return (f'<div class="row"><span class="lbl">{html.escape(lbl)}</span>'
                f'<span class="{cls}">{html.escape(val) if val else "(unset)"}</span></div>')
    body = f"""
<header class="accent"><h1>demo-csfle-cspe-cloud</h1>
  <div class="sub">Side-by-side CSFLE (field-level) vs CSPE (payload), all on Confluent Cloud, KEKs in your AWS KMS.</div>
</header>

<div class="card">
  <div class="card-title">1. AWS credentials</div>
  <div class="card-sub">For KMS and KEK creation / access.</div>
  {row("Current key", aws.get("AWS_ACCESS_KEY_ID", ""))}
  {row("Region",      aws.get("AWS_REGION", env.get("AWS_REGION", "us-west-2")))}
  <form onsubmit="event.preventDefault();saveAws()">
    <label>Paste all <span class="kbd">export AWS_*</span> lines from your terminal (or your AWS SSO console &quot;Command line or programmatic access&quot; block)</label>
    <textarea id="aws-paste" rows="5" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:.5rem;border-radius:4px;font-family:'SFMono-Regular',Consolas,monospace;font-size:.78rem;resize:vertical"
      placeholder='export AWS_ACCESS_KEY_ID="ASIA…"&#10;export AWS_SECRET_ACCESS_KEY="…"&#10;export AWS_SESSION_TOKEN="IQoJ…"'></textarea>
    <label>AWS_REGION</label>
    <input type="text" id="aws-region" value="{html.escape(aws.get("AWS_REGION", env.get("AWS_REGION", "us-west-2")))}" style="max-width:14rem">
    <div class="btn-row">
      <button type="submit" class="btn">Save</button>
      <span id="aws-status" class="card-sub" style="margin:0"></span>
    </div>
  </form>
</div>

<div class="card">
  <div class="card-title">2. Confluent Cloud sign-in</div>
  <div class="card-sub">Run as an OrgAdmin so the wizard can mint Kafka + SR API keys against the env you pick. Runs <span class="kbd">confluent login --save</span> in the background.</div>
  <div id="cc-status-line" class="row"><span class="lbl">Status</span><span class="val empty">checking…</span></div>
  <form onsubmit="event.preventDefault();ccLogin()">
    <label>Email</label>    <input type="text"     id="cc-email">
    <label>Password</label> <input type="password" id="cc-pass">
    <div class="btn-row">
      <button type="submit" class="btn">Sign in</button>
      <span id="cc-msg" class="card-sub" style="margin:0"></span>
    </div>
  </form>
</div>

<div class="card">
  <div class="card-title">3. Pick environment, cluster &amp; topic names</div>
  <div class="card-sub">The wizard reuses an existing env + Kafka cluster (no auto-provisioning). SR is per-env, looked up automatically. Mints fresh Kafka + SR API keys when you submit. SR must already be enabled — Cloud UI → Stream Governance → Enable.</div>
  <label>Environment</label>
  <select id="env-pick" onchange="loadClusters()" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:.4rem;border-radius:4px;font-size:.83rem">
    <option value="">— load —</option>
  </select>
  <label>Kafka cluster</label>
  <select id="cluster-pick" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:.4rem;border-radius:4px;font-size:.83rem">
    <option value="">(pick an env first)</option>
  </select>
  <label>CSFLE topic name</label> <input type="text" id="csfle-topic" value="{html.escape(env.get('CSFLE_TOPIC') or 'mortgage-csfle')}">
  <label>CSPE topic name</label>  <input type="text" id="cspe-topic"  value="{html.escape(env.get('CSPE_TOPIC')  or 'mortgage-cspe')}">
  <div class="btn-row">
    <button class="btn" onclick="savePick()">Save &amp; mint API keys</button>
    <span id="pick-msg" class="card-sub" style="margin:0"></span>
  </div>
</div>

<div class="card">
  <div class="card-title">4. Setup (Stream Governance · KEKs · RBAC · schemas · topics)</div>
  <div class="card-sub">Each step is idempotent. Step 1 verifies (or upgrades) Stream Governance to <span class="kbd">ADVANCED</span> (paid tier — encryption rules require it). Step 2 creates the 2 KEKs in AWS KMS + registers them in SR's DEK Registry. <strong>Step 3 mints 3 service accounts (producer, consumer-with-kek, consumer-no-kek) + 6 API keys + role bindings — the no-kek SA gets NO Kek binding so its DEK Registry lookups return 403.</strong> Steps 4-5 register the schemas with their rule sets and create the two Kafka topics. KEKs must precede RBAC because role bindings reference KEKs by name.</div>
  <div class="btn-row">
    <button class="btn btn-secondary" onclick="runSg()">1 · Stream Governance</button>
    <button class="btn btn-secondary" onclick="runStep(0)">2 · KEKs</button>
    <button class="btn btn-secondary" onclick="runRbac()">3 · RBAC</button>
    <button class="btn btn-secondary" onclick="runStep(1)">4 · schemas</button>
    <button class="btn btn-secondary" onclick="runStep(2)">5 · topics</button>
    <button class="btn" onclick="runAll()">Run all</button>
  </div>
  <div id="setup-log" class="log" style="display:none"></div>
</div>

<div class="card">
  <div class="card-title">Current configuration</div>
  {row("ENV_ID",            env.get("ENV_ID",""))}
  {row("CLUSTER_ID",        env.get("CLUSTER_ID",""))}
  {row("BOOTSTRAP_SERVERS", env.get("BOOTSTRAP_SERVERS",""))}
  {row("SR_URL",            env.get("SR_URL",""))}
  {row("CSFLE_TOPIC",       env.get("CSFLE_TOPIC",""))}
  {row("CSFLE_KEK_NAME",    env.get("CSFLE_KEK_NAME",""))}
  {row("CSFLE_KMS_ARN",     env.get("CSFLE_KMS_ARN",""))}
  {row("CSPE_TOPIC",        env.get("CSPE_TOPIC",""))}
  {row("CSPE_KEK_NAME",     env.get("CSPE_KEK_NAME",""))}
  {row("CSPE_KMS_ARN",      env.get("CSPE_KMS_ARN",""))}
  {_render_schema_rows(env)}
</div>

<script>
function saveAws() {{
  const status = document.getElementById('aws-status');
  // Parse "export KEY=value" or bare "KEY=value" lines (handles single, double, or no quotes)
  const raw = document.getElementById('aws-paste').value;
  const creds = {{}};
  for (const line of raw.split('\\n')) {{
    const m = line.trim().match(/^(?:export\\s+)?(AWS_\\w+)\\s*=\\s*['"]?([^'"\\n]*?)['"]?\\s*$/);
    if (m) creds[m[1]] = m[2];
  }}
  if (!creds['AWS_ACCESS_KEY_ID'] || !creds['AWS_SECRET_ACCESS_KEY']) {{
    status.style.color = '#e63946';
    status.textContent = "couldn't find AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in pasted text";
    return;
  }}
  const body = JSON.stringify({{
    key_id: creds['AWS_ACCESS_KEY_ID'],
    secret: creds['AWS_SECRET_ACCESS_KEY'],
    token:  creds['AWS_SESSION_TOKEN'] || '',
    region: document.getElementById('aws-region').value,
  }});
  fetch('/aws-creds', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body}})
    .then(r => r.json())
    .then(j => {{
      status.textContent = j.ok ? 'saved ✓ (reload to refresh)' : 'error: ' + j.error;
      status.style.color = j.ok ? '#3fb950' : '#e63946';
    }});
}}

function ccStatus() {{
  fetch('/cc-status').then(r=>r.json()).then(j => {{
    const line = document.getElementById('cc-status-line');
    if (j.logged_in) {{
      line.innerHTML = '<span class="lbl">Status</span><span class="val">signed in: ' + (j.user||'(context active)') + '</span>';
      loadEnvs();
    }} else {{
      line.innerHTML = '<span class="lbl">Status</span><span class="val empty">not signed in</span>';
    }}
  }});
}}
function ccLogin() {{
  const body = JSON.stringify({{
    email:    document.getElementById('cc-email').value,
    password: document.getElementById('cc-pass').value,
  }});
  document.getElementById('cc-msg').textContent = 'signing in…';
  fetch('/cc-login', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body}})
    .then(r => r.json())
    .then(j => {{
      const s = document.getElementById('cc-msg');
      s.textContent = j.msg;
      s.style.color = j.ok ? '#3fb950' : '#e63946';
      if (j.ok) {{ ccStatus(); loadEnvs(); }}
    }});
}}

function loadEnvs() {{
  fetch('/cc-envs').then(r=>r.json()).then(j => {{
    const sel = document.getElementById('env-pick');
    sel.innerHTML = '<option value="">— pick —</option>';
    if (!j.ok) {{ sel.innerHTML = '<option>error: ' + j.error + '</option>'; return; }}
    for (const e of j.envs) {{
      const o = document.createElement('option');
      o.value = e.id; o.dataset.name = e.name; o.textContent = e.id + ' · ' + e.name;
      sel.appendChild(o);
    }}
  }});
}}
function loadClusters() {{
  const env = document.getElementById('env-pick').value;
  const sel = document.getElementById('cluster-pick');
  sel.innerHTML = '<option value="">loading…</option>';
  if (!env) {{ sel.innerHTML = '<option value="">(pick an env first)</option>'; return; }}
  fetch('/cc-clusters?env=' + encodeURIComponent(env)).then(r=>r.json()).then(j => {{
    sel.innerHTML = '<option value="">— pick —</option>';
    if (!j.ok) {{ sel.innerHTML = '<option>error: ' + j.error + '</option>'; return; }}
    for (const c of j.clusters) {{
      const o = document.createElement('option');
      o.value = c.id; o.dataset.name = c.name;
      o.textContent = c.id + ' · ' + c.name + ' (' + (c.region||'?') + '/' + (c.cloud||'?') + ')';
      sel.appendChild(o);
    }}
  }});
}}
function savePick() {{
  const envSel  = document.getElementById('env-pick');
  const clusSel = document.getElementById('cluster-pick');
  const env_id  = envSel.value;
  const cluster_id = clusSel.value;
  if (!env_id || !cluster_id) {{
    document.getElementById('pick-msg').textContent = 'pick an env + cluster first'; return;
  }}
  const body = JSON.stringify({{
    env_id, cluster_id,
    env_name:     envSel.options[envSel.selectedIndex].dataset.name || '',
    cluster_name: clusSel.options[clusSel.selectedIndex].dataset.name || '',
    csfle_topic: document.getElementById('csfle-topic').value,
    cspe_topic:  document.getElementById('cspe-topic').value,
  }});
  document.getElementById('pick-msg').textContent = 'saving + minting keys…';
  fetch('/cc-pick', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body}})
    .then(r => r.json())
    .then(j => {{
      const s = document.getElementById('pick-msg');
      s.textContent = j.ok ? 'saved ✓ — reload to see config' : 'error: ' + j.msg;
      s.style.color = j.ok ? '#3fb950' : '#e63946';
    }});
}}

function _runSse(url, label) {{
  const log = document.getElementById('setup-log');
  log.style.display = 'block';
  log.textContent += '\\n══ ' + label + ' ══\\n';
  return new Promise(resolve => {{
    const es = new EventSource(url);
    let doneSeen = false;    // suppress the spurious "stream error" that fires
                             // when the server closes the connection AFTER the
                             // done event (EventSource can't tell EOF from drop)
    es.onmessage = e => {{
      try {{ const d = JSON.parse(e.data); if (d.line) {{
        log.textContent += d.line + '\\n'; log.scrollTop = log.scrollHeight;
      }} }} catch(_) {{}}
    }};
    es.addEventListener('done', e => {{
      const d = JSON.parse(e.data);
      log.textContent += d.ok ? '✓ done\\n' : '✗ exit ' + d.rc + '\\n';
      doneSeen = true;
      es.close();
      resolve(d.ok);
    }});
    es.onerror = () => {{
      if (doneSeen) return;    // clean close after done — no real error
      log.textContent += '✗ stream error\\n';
      es.close();
      resolve(false);
    }};
  }});
}}
function runSg()    {{ return _runSse('/bootstrap-sg',           'step 1 · Stream Governance'); }}
function runRbac()  {{ return _runSse('/bootstrap-rbac',         'step 3 · RBAC'); }}
function runStep(n) {{
  // n=0 → KEKs (label 'step 2'); n=1 → schemas (label 'step 4'); n=2 → topics (label 'step 5')
  const labels = ['2 · KEKs', '4 · schemas', '5 · topics'];
  return _runSse('/bootstrap-step?n=' + n, 'step ' + labels[n]);
}}

async function runAll() {{
  let ok = await runSg();    if (!ok) {{ _stop(); return; }}    // step 1
  ok = await runStep(0);     if (!ok) {{ _stop(); return; }}    // step 2 KEKs
  ok = await runRbac();      if (!ok) {{ _stop(); return; }}    // step 3 RBAC (must follow KEKs)
  ok = await runStep(1);     if (!ok) {{ _stop(); return; }}    // step 4 schemas
  ok = await runStep(2);     if (!ok) {{ _stop(); return; }}    // step 5 topics
}}
function _stop() {{ document.getElementById('setup-log').textContent += '— STOPPING —\\n'; }}

ccStatus();
</script>"""
    return _page_shell("demo-csfle-cspe-cloud · setup", body, nav_key="home")


def build_producer_page(topic_key: str) -> str:
    env = _read_env_file(ENV_FILE)
    sample = (REPO_DIR / "data" / "mortgage-records.json").read_text().splitlines()
    sample_count = len(sample)
    topic   = env.get(f"{topic_key.upper()}_TOPIC") or f"mortgage-{topic_key}"
    subject = f"{topic}-value"
    if topic_key == "csfle":
        title       = "CSFLE Producer"
        scheme      = "field-level encryption"
        explainer   = (f'<span class="kbd">ssn</span> field encrypted client-side via AES256_GCM; '
                       'other fields plaintext. Encryption fires on every produce because the SR '
                       "schema for this topic carries a <code>ruleSet.domainRules</code> entry of "
                       "type <code>ENCRYPT</code> scoped to the <code>PII</code> tag.")
        accent      = "#58a6ff"
        consumers   = ('<a href="/csfle/with-kek">→ CSFLE w/ KEK consumer</a>'
                       ' &middot; <a href="/csfle/no-kek">→ CSFLE no-KEK consumer</a>')
        nav_key     = "csfle-prod"
    else:
        title       = "CSPE Producer"
        scheme      = "payload encryption"
        explainer   = ("Entire serialized JSON value encrypted client-side; record structure is "
                       "not visible to anyone without the KEK. Encryption fires on every produce "
                       "because the SR schema for this topic carries a <code>ruleSet.encodingRules</code>"
                       " entry of type <code>ENCRYPT_PAYLOAD</code>.")
        accent      = "#58a6ff"
        consumers   = ('<a href="/cspe/with-kek">→ CSPE w/ KEK consumer</a>'
                       ' &middot; <a href="/cspe/no-kek">→ CSPE no-KEK consumer</a>')
        nav_key     = "cspe-prod"
    safe_topic = html.escape(topic)
    body = f"""
<header class="accent"><h1>{html.escape(title)}</h1>
  <div class="sub">topic <span class="kbd">{safe_topic}</span> · {scheme}</div>
</header>

<div class="card">
  <div class="card-title">Schema definition (live from Schema Registry)</div>
  {_render_schema_definition(env, subject)}
</div>

<div class="card">
  <div class="card-title">Produce</div>
  <div class="card-sub">{explainer}</div>
  <label>How many records to send (max {sample_count} from the sample file)</label>
  <input type="number" id="count" value="{sample_count}" min="1" max="{sample_count}" style="max-width:8rem">
  <div class="btn-row">
    <button class="btn" onclick="produce()">→ Produce to {safe_topic}</button>
    <span class="card-sub" style="margin:0">Then check: {consumers}</span>
  </div>
  <div id="prod-log" class="log" style="display:none"></div>
</div>

<script>
const MAX_RECORDS = {sample_count};
function produce() {{
  // Clamp to the sample-file size so the UI label matches what the backend
  // actually sends (backend silently caps at file length).
  let n = parseInt(document.getElementById('count').value, 10) || 1;
  if (n < 1)            n = 1;
  if (n > MAX_RECORDS)  n = MAX_RECORDS;
  document.getElementById('count').value = n;
  const log = document.getElementById('prod-log');
  log.style.display = 'block';
  log.textContent = '→ producing ' + n + ' records to {safe_topic} ...\\n';
  let doneSeen = false;
  const es = new EventSource('/produce-stream?topic={topic_key}&count=' + n);
  es.onmessage = e => {{
    const d = JSON.parse(e.data);
    if (d.line) {{ log.textContent += d.line + '\\n'; log.scrollTop = log.scrollHeight; }}
  }};
  es.addEventListener('done', e => {{
    const d = JSON.parse(e.data);
    log.textContent += d.ok ? '✓ done\\n' : '✗ exit ' + d.rc + '\\n';
    doneSeen = true; es.close();
  }});
  es.onerror = () => {{ if (!doneSeen) log.textContent += '✗ stream error\\n'; es.close(); }};
}}
</script>"""
    return _page_shell(f"demo-csfle-cspe-cloud · {html.escape(title)}", body, accent=accent, nav_key=nav_key)


def _render_consumer_config(env: dict[str, str], topic_key: str, role: str) -> str:
    """Effective consumer config rendered as label/value rows. Mirrors what
    `_build_consumer_cmd` actually passes to the subprocess + the AWS env
    state the subprocess will see. Secrets (Kafka secret, SR secret, AWS
    secret + session token) are shown only as a length-truncated prefix +
    `…` so the page can be screen-shared safely."""
    if not env.get("KAFKA_API_KEY") or not env.get("SR_URL"):
        return ('<div class="card-sub" style="color:#d29922">'
                '(consumer not yet configured — finish setup first)</div>')
    keys = _principal_keys(env, topic_key,
                           "consumer-with-kek" if role == "authorized" else "consumer-no-kek")
    principal = keys.get("principal", "?")
    sa_id = keys.get("sa_id", "")
    group_base = CONSUMER_GROUPS[(topic_key, role)]
    sasl_jaas = (f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                 f'username="{keys["kafka_key"]}" password="<redacted>";')
    sr_auth   = f'{keys["sr_key"]}:<redacted>'

    # The actual config lines the subprocess receives (via --command-property
    # for Kafka, --formatter-property for serdes).
    props = [
        ("bootstrap.servers",              env.get("BOOTSTRAP_SERVERS", "")),
        ("group.id",                       f"{group_base} (suffixed with timestamp on 'Start from beginning')"),
        ("auto.offset.reset",              "earliest"),
        ("security.protocol",              "SASL_SSL"),
        ("sasl.mechanism",                 "PLAIN"),
        ("sasl.jaas.config",               sasl_jaas),
        ("schema.registry.url",            env.get("SR_URL", "")),
        ("basic.auth.credentials.source",  "USER_INFO"),
        ("basic.auth.user.info",           sr_auth),
        ("rule.executors",                 "_default_"),
        ("rule.executors._default_.class", EXECUTOR),
    ]

    # AWS env — what the subprocess sees. _consumer_env() decides this based
    # on role: authorized injects creds; unauthorized strips them and points
    # the SDK at /dev/null + disables IMDS.
    if role == "authorized":
        aws = _aws_creds()
        aws_state = [
            ("AWS_ACCESS_KEY_ID",     aws.get("AWS_ACCESS_KEY_ID", "(none — KMS unwrap will fail)")),
            ("AWS_SECRET_ACCESS_KEY", "<redacted>" if aws.get("AWS_SECRET_ACCESS_KEY") else "(none)"),
            ("AWS_SESSION_TOKEN",     "<redacted>" if aws.get("AWS_SESSION_TOKEN") else "(none — long-lived key)"),
            ("AWS_REGION",            aws.get("AWS_REGION", "")),
        ]
    else:
        aws_state = [
            ("AWS_ACCESS_KEY_ID",           "(stripped — demo's no-KEK mode)"),
            ("AWS_SECRET_ACCESS_KEY",       "(stripped)"),
            ("AWS_SESSION_TOKEN",           "(stripped)"),
            ("AWS_SHARED_CREDENTIALS_FILE", "/dev/null"),
            ("AWS_CONFIG_FILE",             "/dev/null"),
            ("AWS_EC2_METADATA_DISABLED",   "true"),
        ]

    # Top: surface the Confluent principal + KEK RBAC state so the page makes
    # the security boundary visible. After the SA-split refactor, each page
    # has its own per-(topic,role) SA — and the "with-KEK" consumers see only
    # the KEK for THEIR topic (csfle-with-kek doesn't see cspe-kek and vice
    # versa).
    if not sa_id:
        sa_id = "(OrgAdmin fallback)"
    own_kek = env.get(f"{topic_key.upper()}_KEK_NAME", "?")
    other_kek_name  = "CSPE_KEK_NAME" if topic_key == "csfle" else "CSFLE_KEK_NAME"
    other_kek = env.get(other_kek_name, "?")
    if "orgadmin" in principal:
        kek_state = (f'<span class="val empty">'
                     f'(OrgAdmin fallback — RBAC step not yet run; KEK access via OrgAdmin role, '
                     f'AWS-stripped env still enforces the no-KEK boundary at the AWS layer)</span>')
    elif role == "authorized":
        kek_state = (f'<span class="val">DeveloperRead on Kek:{html.escape(own_kek)} '
                     f'<span class="kbd">only</span> · NO binding on Kek:{html.escape(other_kek)} '
                     f'(other topic\'s KEK)</span>')
    else:
        kek_state = ('<span class="val" style="color:#ff7b72">'
                     'NONE — DEK Registry returns 403 for any KEK lookup '
                     '(the Confluent-side enforcement of the no-KEK boundary)</span>')

    out = [
        '<div class="card-sub">Confluent identity</div>',
        f'<div class="row"><span class="lbl">Service account</span><span class="val">{html.escape(sa_id)}</span></div>',
        f'<div class="row"><span class="lbl">Effective principal</span><span class="val">{html.escape(principal)}</span></div>',
        f'<div class="row"><span class="lbl">KEK RBAC bindings</span>{kek_state}</div>',
        '<div class="card-sub" style="margin-top:.85rem">Subprocess properties</div>',
    ]
    for k, v in props:
        out.append(f'<div class="row"><span class="lbl">{html.escape(k)}</span>'
                   f'<span class="val">{html.escape(str(v))}</span></div>')
    out.append('<div class="card-sub" style="margin-top:.85rem">AWS env (subprocess)</div>')
    for k, v in aws_state:
        out.append(f'<div class="row"><span class="lbl">{html.escape(k)}</span>'
                   f'<span class="val">{html.escape(str(v))}</span></div>')
    return "\n  ".join(out)


def build_consumer_page(topic_key: str, role: str, title: str, accent: str) -> str:
    env   = _read_env_file(ENV_FILE)
    topic = env.get(f"{topic_key.upper()}_TOPIC", "")
    nav_key = f"{topic_key}-{'auth' if role == 'authorized' else 'noauth'}"
    opaque_class = "true" if (topic_key == "cspe" and role == "unauthorized") else "false"
    slug = "with-kek" if role == "authorized" else "no-kek"
    # CSPE no-KEK can't deserialize the encrypted payload as JSON, so the
    # records list will stay empty even though the consumer DOES read the
    # bytes from the topic. Surface that explicitly so the user doesn't
    # think the page is broken.
    cspe_nokek_note = (
        '<div class="banner warn">This page intentionally shows nothing — '
        'CSPE encrypts the <em>entire payload</em>, so the JSON deserializer '
        'has no schema-shaped bytes to parse without KEK access. The encrypted '
        "bytes ARE on the topic — verify with <span class=\"kbd\">kafka-console-consumer</span> "
        '(plain, not json-schema) or by switching to the <a href="/cspe/with-kek" '
        'style="color:#79c0ff">with-KEK page</a>. This is the demo\'s CSPE point: '
        'no-KEK consumers see <strong>nothing</strong>, vs CSFLE no-KEK where they '
        'see record structure with PII redacted.</div>'
        if topic_key == "cspe" and role == "unauthorized" else ""
    )
    body = f"""
<header class="accent"><h1>{html.escape(title)}</h1>
  <div class="sub">topic <span class="kbd">{html.escape(topic)}</span></div>
</header>
{cspe_nokek_note}

<div class="card">
  <div class="card-title">Consumer configuration</div>
  {_render_consumer_config(env, topic_key, role)}
</div>

<div class="card">
  <div class="card-title">Live consumer (Server-Sent Events)</div>
  <div class="card-sub">JSON records arrive in green; subprocess errors (auth, schema, KMS) appear in red. Reload the page to spawn a fresh consumer with a new group id (re-reads from beginning).</div>
  <div class="btn-row">
    <button class="btn"               onclick="start(true)">Start (from beginning)</button>
    <button class="btn btn-secondary" onclick="start(false)">Resume (live tail)</button>
    <button class="btn btn-secondary" onclick="stop()">Stop</button>
    <span id="consumer-status" class="card-sub" style="margin:0"></span>
  </div>
  <div id="records" class="records" style="margin-top:.7rem"></div>
</div>

<script>
let es = null;
function stop() {{ if (es) {{ es.close(); es = null; document.getElementById('consumer-status').textContent = 'stopped'; }} }}
function start(fromBeginning) {{
  stop();
  document.getElementById('records').innerHTML = '';
  document.getElementById('consumer-status').textContent = 'connecting…';
  const url = '/sse/{topic_key}/{slug}' + (fromBeginning ? '?from_beginning=1' : '');
  es = new EventSource(url);
  es.onopen = () => {{ document.getElementById('consumer-status').textContent = 'streaming…'; }};
  es.onmessage = e => {{
    let d; try {{ d = JSON.parse(e.data); }} catch(_) {{ return; }}
    if (!d.raw) return;    // server only emits raw record events
    const div = document.createElement('div');
    div.className = 'rec' + ({opaque_class} ? ' opaque' : '');
    div.textContent = d.raw;
    document.getElementById('records').appendChild(div);
    window.scrollTo(0, document.body.scrollHeight);
  }};
  es.onerror = () => {{
    // Server closed the stream OR a real network error. Either way, the
    // EventSource will auto-reconnect (which would re-spawn the consumer
    // subprocess); explicitly close to prevent that.
    if (es) {{ es.close(); es = null; }}
    document.getElementById('consumer-status').textContent = 'stream closed';
  }};
}}
window.addEventListener('beforeunload', stop);
</script>"""
    return _page_shell(title, body, accent=accent, nav_key=nav_key)


# ── HTTP handler ─────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    # Per-handler-instance lock guarding wfile writes. _sse_data and the bare
    # keepalive writes can be invoked from multiple threads (e.g. _stream_producer
    # has a drain_stdout background thread that emits SSE while the main thread
    # also emits per-record SSE) and wfile.write/flush are not thread-safe —
    # without this, frames can interleave mid-byte and corrupt the stream.
    def __init__(self, *a, **kw):
        self._sse_lock = threading.Lock()
        super().__init__(*a, **kw)

    def log_message(self, fmt: str, *args) -> None:
        # Quieter than default — only show errors
        if args and isinstance(args[0], str) and not args[0].startswith("2"):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send_html(self, body: str, status: int = 200) -> None:
        b = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        # Defensive: the demo's HTML/JS evolves frequently. A cached old page
        # surfaces as ghost bugs that look like server faults (e.g. a "✗ stream
        # error" line coming from a since-fixed JS handler). Force a fresh fetch.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(b)

    def _send_json(self, obj, status: int = 200) -> None:
        b = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _start_sse(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def _sse_data(self, payload: dict, event: str | None = None) -> bool:
        with self._sse_lock:
            try:
                if event:
                    self.wfile.write(f"event: {event}\n".encode())
                self.wfile.write(f"data: {json.dumps(payload)}\n\n".encode())
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError):
                return False

    def _sse_keepalive(self) -> bool:
        """SSE comment line — keeps the browser's EventSource from idle-closing
        the connection during long stretches without records. Returns False if
        the client has disconnected."""
        with self._sse_lock:
            try:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError):
                return False

    # ── routing ────────────────────────────────────────────────────────────

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "":
            self._send_html(build_wizard_page()); return
        if path == "/produce/csfle":
            self._send_html(build_producer_page("csfle")); return
        if path == "/produce/cspe":
            self._send_html(build_producer_page("cspe")); return
        if path == "/produce":
            # Backward-compat: old single producer URL → redirect to CSFLE producer
            self.send_response(302); self.send_header("Location", "/produce/csfle")
            self.end_headers(); return
        if path == "/env":
            self._send_json(_read_env_file(ENV_FILE)); return
        if path == "/cc-status":
            self._send_json(_cc_status()); return
        if path == "/cc-envs":
            try:
                self._send_json({"ok": True, "envs": _cc_envs()})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
            return
        if path == "/cc-clusters":
            env_id = parse_qs(parsed.query).get("env", [""])[0]
            if not env_id:
                self._send_json({"ok": False, "error": "missing env"}, status=400); return
            try:
                self._send_json({"ok": True, "clusters": _cc_clusters(env_id)})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
            return

        # SSE-streaming routes: GET because EventSource (the browser's SSE
        # primitive) only does GET. Side-effects ARE triggered (subprocess
        # spawn, KMS create-key, schema POST) — semantically un-RESTful but
        # idempotent in practice and the SSE protocol leaves no alternative.
        if path == "/bootstrap-step":
            n = int(parse_qs(parsed.query).get("n", ["0"])[0])
            if not (0 <= n < len(BOOTSTRAP_SCRIPTS)):
                self._send_json({"ok": False, "error": "bad step"}, status=400); return
            self._stream_bootstrap(n); return

        if path == "/bootstrap-sg":
            self._stream_sg_upgrade(); return

        if path == "/bootstrap-rbac":
            self._stream_rbac_setup(); return

        if path == "/produce-stream":
            qs = parse_qs(parsed.query)
            topic_key = qs.get("topic", ["csfle"])[0]
            count = int(qs.get("count", ["1"])[0])
            self._stream_producer(topic_key, count); return

        # Consumer pages
        for (topic_key, slug), (_, role, title, accent) in CONSUMER_PAGES.items():
            if path == f"/{topic_key}/{slug}":
                self._send_html(build_consumer_page(topic_key, role, title, accent)); return

        # Consumer SSE stream
        if path.startswith("/sse/"):
            parts = path.removeprefix("/sse/").split("/")
            if len(parts) == 2 and parts[0] in ("csfle", "cspe") and parts[1] in ("with-kek", "no-kek"):
                topic_key = parts[0]
                role = "authorized" if parts[1] == "with-kek" else "unauthorized"
                from_beginning = "from_beginning" in parse_qs(parsed.query)
                self._stream_consumer(topic_key, role, from_beginning)
                return
            self.send_error(404); return

        self.send_error(404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/aws-creds":
            length = int(self.headers.get("Content-Length", "0"))
            try:
                body = json.loads(self.rfile.read(length))
                _write_aws_session(
                    body.get("key_id", "").strip(),
                    body.get("secret", "").strip(),
                    body.get("token", "").strip(),
                    body.get("region", "").strip(),
                )
                _upsert_env({"AWS_REGION": body.get("region", "us-west-2").strip() or "us-west-2"})
                self._send_json({"ok": True}); return
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400); return

        if path == "/cc-login":
            length = int(self.headers.get("Content-Length", "0"))
            try:
                body = json.loads(self.rfile.read(length))
                ok, msg = _cc_login(body.get("email", "").strip(), body.get("password", ""))
                self._send_json({"ok": ok, "msg": msg})
            except Exception as e:
                self._send_json({"ok": False, "msg": str(e)}, status=500)
            return

        if path == "/cc-pick":
            # Body: {env_id, env_name, cluster_id, cluster_name, csfle_topic, cspe_topic}
            # Steps: describe cluster → describe SR → mint Kafka & SR API keys →
            #        write everything to .env. Returns the resulting state.
            length = int(self.headers.get("Content-Length", "0"))
            try:
                body = json.loads(self.rfile.read(length))
                env_id   = body["env_id"]
                env_name = body.get("env_name", "")
                cluster_id   = body["cluster_id"]
                cluster_name = body.get("cluster_name", "")
                csfle_topic  = body.get("csfle_topic", "mortgage-csfle").strip() or "mortgage-csfle"
                cspe_topic   = body.get("cspe_topic",  "mortgage-cspe" ).strip() or "mortgage-cspe"

                cluster = _cc_describe_cluster(env_id, cluster_id)
                bootstrap = (cluster.get("endpoint") or "").replace("SASL_SSL://", "")

                sr = _cc_describe_sr(env_id)
                if not sr.get("id"):
                    self._send_json({
                        "ok": False,
                        "msg": f"Schema Registry not enabled for env {env_id} — enable it in the "
                               "Cloud UI first (Stream Governance → Enable). The CLI no longer "
                               "supports SR enable.",
                    }, status=400)
                    return

                # Mint fresh API keys ONLY when needed: either we don't have
                # one yet, or the user switched to a different cluster/SR
                # (the existing key wouldn't authenticate against the new one).
                # When switching, also DELETE the orphaned old key to avoid
                # accumulating dead keys against CC's per-resource quota
                # (5 default per cluster). Failures to delete are logged but
                # not fatal — user can clean up via Cloud UI.
                current = _read_env_file(ENV_FILE)
                updates: dict[str, str] = {
                    "ENV_ID":            env_id,
                    "ENV_NAME":          env_name,
                    "CLUSTER_ID":        cluster_id,
                    "CLUSTER_NAME":      cluster_name,
                    "BOOTSTRAP_SERVERS": bootstrap,
                    "SR_ID":             sr["id"],
                    "SR_URL":            sr["url"],
                    "CSFLE_TOPIC":       csfle_topic,
                    "CSPE_TOPIC":        cspe_topic,
                }
                cluster_changed = current.get("CLUSTER_ID") and current.get("CLUSTER_ID") != cluster_id
                sr_changed      = current.get("SR_ID")      and current.get("SR_ID")      != sr["id"]
                if not current.get("KAFKA_API_KEY") or cluster_changed:
                    if cluster_changed and current.get("KAFKA_API_KEY"):
                        _cc_delete_api_key_quiet(current["KAFKA_API_KEY"])
                    k = _cc_create_api_key(cluster_id, env_id, "demo-csfle-cspe-cloud")
                    updates["KAFKA_API_KEY"]    = k["api_key"]
                    updates["KAFKA_API_SECRET"] = k["api_secret"]
                if not current.get("SR_API_KEY") or sr_changed:
                    if sr_changed and current.get("SR_API_KEY"):
                        _cc_delete_api_key_quiet(current["SR_API_KEY"])
                    s = _cc_create_api_key(sr["id"], env_id, "demo-csfle-cspe-cloud-sr")
                    updates["SR_API_KEY"]    = s["api_key"]
                    updates["SR_API_SECRET"] = s["api_secret"]
                _upsert_env(updates)
                self._send_json({"ok": True, "saved": list(updates.keys())})
            except KeyError as e:
                self._send_json({"ok": False, "msg": f"missing field {e}"}, status=400)
            except Exception as e:
                self._send_json({"ok": False, "msg": str(e)}, status=500)
            return

        self.send_error(404)

    # ── streaming workers ─────────────────────────────────────────────────

    def _stream_subprocess(self, cmd: list[str], env: dict[str, str], *,
                           stdin: bytes | None = None) -> int:
        """Spawn cmd, stream stdout via SSE, return exit code. SSE event 'done'
        carries final exit code."""
        self._start_sse()
        proc = None
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                bufsize=1,
                text=True,
            )
            if stdin is not None:
                # Write input then close stdin so the producer terminates cleanly
                threading.Thread(
                    target=lambda: (proc.stdin.write(stdin.decode("utf-8", "replace")),
                                    proc.stdin.close()),
                    daemon=True,
                ).start()
            while True:
                ready, _, _ = select.select([proc.stdout], [], [], 2.0)
                if ready:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    if not self._sse_data({"line": line.rstrip()}):
                        proc.kill()
                        return -1
                else:
                    if not self._sse_keepalive():
                        proc.kill()
                        return -1
                if proc.poll() is not None:
                    # drain any final lines
                    rest = proc.stdout.read()
                    if rest:
                        for line in rest.splitlines():
                            self._sse_data({"line": line})
                    break
            rc = proc.wait()
            self._sse_data({"ok": rc == 0, "rc": rc}, event="done")
            return rc
        except Exception as e:
            self._sse_data({"line": f"ERROR: {e}"})
            self._sse_data({"ok": False, "rc": -1}, event="done")
            return -1
        finally:
            if proc and proc.poll() is None:
                proc.kill()

    def _stream_bootstrap(self, n: int) -> None:
        script_name, _ = BOOTSTRAP_SCRIPTS[n]
        cmd = ["bash", str(REPO_DIR / "scripts" / script_name)]
        env = os.environ.copy()
        env["JAVA_HOME"] = JAVA_HOME
        # Inject AWS creds (needed for step 1 — KMS create-key)
        env.update(_aws_creds())
        self._stream_subprocess(cmd, env)

    # Per-role service account naming. Suffixed with the env_id last segment
    # so multiple demos in the same org don't collide on the SA name.
    @staticmethod
    def _sa_name(role: str, env_id: str) -> str:
        suffix = (env_id or "default").split("-")[-1]
        return f"demo-csfle-cspe-cloud-{role}-{suffix}"

    def _stream_rbac_setup(self) -> None:
        """Mint 3 service accounts (producer, consumer-with-kek, consumer-no-kek),
        mint Kafka + SR API keys for each, and create role bindings such that:

          - producer SA  → DeveloperWrite on Topic + Subject + Kek (CSFLE & CSPE)
          - consumer-with-kek SA → DeveloperRead on Topic + Subject + Group + Kek
          - consumer-no-kek SA   → DeveloperRead on Topic + Subject + Group ONLY
                                   (NO Kek binding — DEK Registry returns 403,
                                   onFailure=NONE swallows it, page shows the
                                   record-as-ciphertext contrast)

        Persists 12 keys to .env: PRODUCER_{KAFKA,SR}_API_{KEY,SECRET},
        CONSUMER_KEK_{KAFKA,SR}_API_{KEY,SECRET}, CONSUMER_NOKEK_{KAFKA,SR}_API_{KEY,SECRET}.
        Skips any SA whose keys are already present in .env (idempotent on re-run)."""
        self._start_sse()
        try:
            env = _read_env_file(ENV_FILE)
            for k in ("ENV_ID", "CLUSTER_ID", "SR_ID",
                      "CSFLE_TOPIC", "CSPE_TOPIC",
                      "CSFLE_KEK_NAME", "CSPE_KEK_NAME"):
                if not env.get(k):
                    self._sse_data({"line": f"✗ {k} missing — finish setup steps 1-2 first"})
                    self._sse_data({"ok": False, "rc": -1}, event="done")
                    return
            env_id   = env["ENV_ID"]
            cluster  = env["CLUSTER_ID"]
            sr       = env["SR_ID"]
            csfle_t  = env["CSFLE_TOPIC"]
            cspe_t   = env["CSPE_TOPIC"]
            csfle_k  = env["CSFLE_KEK_NAME"]
            cspe_k   = env["CSPE_KEK_NAME"]

            # Per-role binding spec — ONE SA per page so each principal has
            # only the KEK it actually needs (least privilege). The CSFLE
            # consumer SA gets DevRead on Kek:csfle-kek but NOT cspe-kek,
            # and vice versa.
            #   Each entry: (role, resource, scope, prefixed)
            #   scope: "kafka" → needs --kafka-cluster + --cloud-cluster;
            #          "sr" → needs --schema-registry-cluster
            #   prefixed: True for Group bindings — the wizard appends a
            #             timestamp on 'Start from beginning' (so the actual
            #             group is `<base>-1778026936`, not just `<base>`).
            specs = {
                "csfle-producer": [
                    ("DeveloperWrite", f"Topic:{csfle_t}",          "kafka", False),
                    ("DeveloperWrite", f"Subject:{csfle_t}-value",  "sr",    False),
                    ("DeveloperWrite", f"Kek:{csfle_k}",            "sr",    False),
                ],
                "cspe-producer": [
                    ("DeveloperWrite", f"Topic:{cspe_t}",           "kafka", False),
                    ("DeveloperWrite", f"Subject:{cspe_t}-value",   "sr",    False),
                    ("DeveloperWrite", f"Kek:{cspe_k}",             "sr",    False),
                ],
                "csfle-consumer-with-kek": [
                    ("DeveloperRead",  f"Topic:{csfle_t}",          "kafka", False),
                    ("DeveloperRead",  "Group:demo-csfle-with-kek", "kafka", True),
                    ("DeveloperRead",  f"Subject:{csfle_t}-value",  "sr",    False),
                    ("DeveloperRead",  f"Kek:{csfle_k}",            "sr",    False),  # NO cspe-kek
                ],
                "csfle-consumer-no-kek": [
                    ("DeveloperRead",  f"Topic:{csfle_t}",          "kafka", False),
                    ("DeveloperRead",  "Group:demo-csfle-no-kek",   "kafka", True),
                    ("DeveloperRead",  f"Subject:{csfle_t}-value",  "sr",    False),
                    # NO Kek bindings.
                ],
                "cspe-consumer-with-kek": [
                    ("DeveloperRead",  f"Topic:{cspe_t}",           "kafka", False),
                    ("DeveloperRead",  "Group:demo-cspe-with-kek",  "kafka", True),
                    ("DeveloperRead",  f"Subject:{cspe_t}-value",   "sr",    False),
                    ("DeveloperRead",  f"Kek:{cspe_k}",             "sr",    False),  # NO csfle-kek
                ],
                "cspe-consumer-no-kek": [
                    ("DeveloperRead",  f"Topic:{cspe_t}",           "kafka", False),
                    ("DeveloperRead",  "Group:demo-cspe-no-kek",    "kafka", True),
                    ("DeveloperRead",  f"Subject:{cspe_t}-value",   "sr",    False),
                    # NO Kek bindings.
                ],
            }
            # Maps role → .env-key prefix. 6 SAs × 5 vars each = 30 entries.
            env_prefix = {
                "csfle-producer":           "CSFLE_PRODUCER",
                "cspe-producer":            "CSPE_PRODUCER",
                "csfle-consumer-with-kek":  "CSFLE_CONSUMER_KEK",
                "csfle-consumer-no-kek":    "CSFLE_CONSUMER_NOKEK",
                "cspe-consumer-with-kek":   "CSPE_CONSUMER_KEK",
                "cspe-consumer-no-kek":     "CSPE_CONSUMER_NOKEK",
            }

            updates: dict[str, str] = {}
            for role, bindings in specs.items():
                sa_name = self._sa_name(role, env_id)
                self._sse_data({"line": f"\n══ {role} ══"})
                self._sse_data({"line": f"→ ensuring service account {sa_name}"})
                sa = _cc_create_service_account(
                    sa_name, f"demo-csfle-cspe-cloud — {role} principal")
                sa_id = sa["id"]
                self._sse_data({"line": f"  ✓ {sa_id}"})
                updates[f"{env_prefix[role]}_SA_ID"] = sa_id

                # Mint Kafka + SR API keys for this SA. Skip if .env already
                # has them (idempotent on re-run — re-running shouldn't burn
                # the per-cluster API key quota).
                kk = f"{env_prefix[role]}_KAFKA_API_KEY"
                ks = f"{env_prefix[role]}_KAFKA_API_SECRET"
                sk = f"{env_prefix[role]}_SR_API_KEY"
                ss = f"{env_prefix[role]}_SR_API_SECRET"
                if not env.get(kk):
                    self._sse_data({"line": f"→ minting Kafka API key for {sa_id}"})
                    rc, out, err = _run_confluent(
                        ["api-key", "create",
                         "--resource", cluster, "--environment", env_id,
                         "--service-account", sa_id,
                         "--description", f"demo-csfle-cspe-cloud {role} (kafka)",
                         "-o", "json"], timeout=30,
                    )
                    if rc != 0:
                        raise RuntimeError(f"kafka api-key create for {sa_id}: {(err or out).strip()}")
                    k = json.loads(out)
                    updates[kk] = k["api_key"]; updates[ks] = k["api_secret"]
                    self._sse_data({"line": f"  ✓ {k['api_key']}"})
                else:
                    self._sse_data({"line": f"  ✓ Kafka key already in .env ({env[kk]})"})
                if not env.get(sk):
                    self._sse_data({"line": f"→ minting SR API key for {sa_id}"})
                    rc, out, err = _run_confluent(
                        ["api-key", "create",
                         "--resource", sr, "--environment", env_id,
                         "--service-account", sa_id,
                         "--description", f"demo-csfle-cspe-cloud {role} (sr)",
                         "-o", "json"], timeout=30,
                    )
                    if rc != 0:
                        raise RuntimeError(f"sr api-key create for {sa_id}: {(err or out).strip()}")
                    k = json.loads(out)
                    updates[sk] = k["api_key"]; updates[ss] = k["api_secret"]
                    self._sse_data({"line": f"  ✓ {k['api_key']}"})
                else:
                    self._sse_data({"line": f"  ✓ SR key already in .env ({env[sk]})"})

                # Apply role bindings. Each binding is independent — log per result.
                for r_role, resource, scope, prefixed in bindings:
                    kw: dict = {"prefixed": prefixed}
                    if scope == "kafka":
                        kw["kafka_cluster"] = cluster
                    elif scope == "sr":
                        kw["sr_cluster"] = sr
                    ok, msg = _cc_role_binding_create(sa_id, r_role, resource, env_id, **kw)
                    marker = "✓" if ok else "✗"
                    pat = " (prefix)" if prefixed else ""
                    self._sse_data({"line": f"  {marker} {r_role:<15} {resource:<35} ({scope}){pat}"})
                    if not ok:
                        self._sse_data({"line": f"    {msg}"})

            # Persist all minted creds + SA ids.
            _upsert_env(updates)
            self._sse_data({"line": f"\n✓ wrote {len(updates)} entries to .env"})
            self._sse_data({"ok": True, "rc": 0}, event="done")
        except Exception as e:
            self._sse_data({"line": f"ERROR: {e}"})
            self._sse_data({"ok": False, "rc": -1}, event="done")

    def _stream_sg_upgrade(self) -> None:
        """Verify Stream Governance is on ADVANCED for the env in .env;
        upgrade via SRCM v3 PATCH if it isn't. Streams progress via SSE."""
        self._start_sse()
        try:
            env = _read_env_file(ENV_FILE)
            sr_id  = env.get("SR_ID", "")
            env_id = env.get("ENV_ID", "")
            if not (sr_id and env_id):
                self._sse_data({"line": "✗ SR_ID / ENV_ID missing — finish card 3 (env+cluster pick) first"})
                self._sse_data({"ok": False, "rc": -1}, event="done")
                return
            self._sse_data({"line": f"→ ensuring cloud API key (needed for SRCM v3 calls)"})
            ck, cs = _cc_ensure_cloud_api_key()
            self._sse_data({"line": f"  ✓ cloud key {ck}"})
            self._sse_data({"line": f"→ describing Stream Governance for {sr_id} (env={env_id})"})
            current = _cc_sg_package(sr_id, env_id, ck, cs)
            self._sse_data({"line": f"  current package: {current or '(unknown)'}"})
            if current == "ADVANCED":
                self._sse_data({"line": "✓ already on ADVANCED — no action needed"})
                self._sse_data({"ok": True, "rc": 0}, event="done")
                return
            # Upgrade. Note this has billing implications — log explicitly so
            # the user sees the click translated into a paid action.
            self._sse_data({"line": f"→ PATCH spec.package = ADVANCED  (BILLING: enables paid tier)"})
            new_pkg = _cc_upgrade_sg(sr_id, env_id, ck, cs, target="ADVANCED")
            if new_pkg == "ADVANCED":
                self._sse_data({"line": f"✓ upgraded to ADVANCED"})
                self._sse_data({"ok": True, "rc": 0}, event="done")
            else:
                self._sse_data({"line": f"✗ upgrade returned package={new_pkg!r} (expected ADVANCED)"})
                self._sse_data({"ok": False, "rc": 1}, event="done")
        except Exception as e:
            self._sse_data({"line": f"ERROR: {e}"})
            self._sse_data({"ok": False, "rc": -1}, event="done")

    def _stream_producer(self, topic_key: str, count: int) -> None:
        """Spawn the producer subprocess and feed it records ONE AT A TIME so
        the SSE log shows progress per message (the console-producer doesn't
        print per-record acks itself, so we emit our own line before each
        write). Subprocess stdout/stderr is drained in a background thread
        and forwarded to the same SSE stream — any executor errors (KMS
        denied, license missing, schema not found) surface inline."""
        try:
            cmd = _build_producer_cmd(topic_key)
        except Exception as e:
            self._start_sse()
            self._sse_data({"line": f"ERROR: {e}"})
            self._sse_data({"ok": False, "rc": -1}, event="done")
            return
        env = os.environ.copy()
        env["JAVA_HOME"] = JAVA_HOME
        env.update(_aws_creds())    # wrap/unwrap DEKs via AWS KMS

        data_path = REPO_DIR / "data" / "mortgage-records.json"
        records = [ln for ln in data_path.read_text().splitlines() if ln.strip()][:count]
        topic = _read_env_file(ENV_FILE).get(f"{topic_key.upper()}_TOPIC", "?")

        self._start_sse()
        self._sse_data({"line": f"▶ producing {len(records)} records to {topic}"})

        proc = None
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                bufsize=1,
                text=True,
            )

            # Background thread: forward subprocess stdout/stderr to the SSE
            # stream. Without this, executor errors (KMS denied, license check
            # failed, schema not found) would be silently buffered until the
            # subprocess exits.
            def drain_stdout() -> None:
                try:
                    for line in proc.stdout:
                        self._sse_data({"line": "  [producer] " + line.rstrip()})
                except (BrokenPipeError, ConnectionResetError, ValueError):
                    pass

            t = threading.Thread(target=drain_stdout, daemon=True)
            t.start()

            # Feed records one at a time; emit an SSE line per send showing
            # the FULL record JSON (no truncation — a typical mortgage record
            # is ~300 chars and the log box wraps cleanly). A small sleep
            # (50 ms) ensures the browser sees individual events instead of
            # a single batch when the producer is fast.
            for i, rec in enumerate(records, 1):
                self._sse_data({"line": f"→ #{i:>3}/{len(records)}  {rec}"})
                try:
                    proc.stdin.write(rec + "\n")
                    proc.stdin.flush()
                except (BrokenPipeError, OSError) as e:
                    self._sse_data({"line": f"✗ producer stdin closed mid-stream: {e}"})
                    break
                time.sleep(0.05)

            try:
                proc.stdin.close()
            except Exception:
                pass

            rc = proc.wait(timeout=60)
            t.join(timeout=2)
            self._sse_data({"line": f"▶ producer exited rc={rc}"})
            self._sse_data({"ok": rc == 0, "rc": rc}, event="done")
        except Exception as e:
            self._sse_data({"line": f"ERROR: {e}"})
            self._sse_data({"ok": False, "rc": -1}, event="done")
        finally:
            if proc and proc.poll() is None:
                proc.kill()

    def _stream_consumer(self, topic_key: str, role: str, from_beginning: bool) -> None:
        try:
            cmd = _build_consumer_cmd(topic_key, role, from_beginning)
        except KeyError as e:
            self._start_sse()
            self._sse_data({"line": f"ERROR: missing config key {e} — run the setup wizard first"})
            self._sse_data({"ok": False, "rc": -1}, event="done")
            return
        env = _consumer_env(role)
        self._start_sse()
        proc = None
        try:
            # Authorized: stderr → stdout so config errors (KMS denied due to a
            # real auth issue, schema 404, etc.) surface inline in red.
            # Unauthorized: KMS-denied warnings are EXPECTED on every record
            # (that's the whole point of the no-KEK page). Drop stderr to
            # DEVNULL — the page should show only the records-as-ciphertext
            # contrast, not a stream of "decrypt failed" noise.
            stderr_target = subprocess.STDOUT if role == "authorized" else subprocess.DEVNULL
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=stderr_target,
                text=True,
                bufsize=1,
                env=env,
            )
            while True:
                ready, _, _ = select.select([proc.stdout], [], [], 2.0)
                if ready:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    line = line.rstrip()
                    if not line:
                        continue
                    # JSON records from our schema are objects → start with `{`.
                    # Log lines start with `[YYYY-MM-DD …]` (INFO startup noise,
                    # WARN swallowed-decrypt for the no-kek path, etc.). Drop
                    # everything that isn't a record so both consumer pages
                    # are visually clean — the only thing that varies between
                    # them is whether the SSN field is plaintext or ciphertext.
                    # Real subprocess errors (auth failure, schema 404, license)
                    # still go to the server's terminal log via /tmp/wiz.log
                    # and the subprocess exit code; the page just doesn't show
                    # the noisy [YYYY-MM-DD] INFO/WARN lines.
                    if not line.lstrip().startswith("{"):
                        continue
                    if not self._sse_data({"raw": line}):
                        break
                else:
                    if not self._sse_keepalive():
                        break
        except Exception as e:
            # Errors from this loop itself (not the subprocess) — show on both
            # roles since they signal a wizard-side problem, not encryption.
            self._sse_data({"err": f"stream error: {e}"})
        finally:
            if proc and proc.poll() is None:
                proc.kill()


class _Server(ThreadingHTTPServer):
    """ThreadingHTTPServer subclass that suppresses the noisy traceback the
    stdlib prints when a browser closes a keep-alive connection mid-request.
    Those show as `ConnectionResetError [Errno 54] Connection reset by peer`
    in handle_one_request → rfile.readline; they're not crashes (the server
    keeps running, the next request works fine), just one closed socket."""
    def handle_error(self, request, client_address) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            return
        super().handle_error(request, client_address)


def main() -> None:
    print(f"demo-csfle-cspe-cloud → http://localhost:{PORT}", flush=True)
    srv = _Server(("127.0.0.1", PORT), Handler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    main()
