"""Schema-driven sample-record generator.

Replaces the static data/mortgage-records.json so adding a field to the schema
never silently desynchronizes from the data. Records are derived from the live
SR schema for `<topic>-value`: every property in the schema is populated, so
the JsonSchemaSerializer (which normalizes missing-string-properties to `null`
and rejects them) is always satisfied.

Two entry points:
  - import:  generate(env, topic_key, count) -> Iterator[str]   (used by web/server.py)
  - CLI:     python3 web/datagen.py <topic_key> <count>          (used by Makefile)

Field values are produced by name heuristics first (ssn, credit_card_number,
applicant_name, ...) and fall back to JSON Schema type defaults for unknown
fields. Adding a new field with a recognized name is automatic; adding one
with an unknown name yields a type-correct placeholder rather than a failure.
"""

from __future__ import annotations

import base64
import json
import os
import random
import sys
import urllib.request
import uuid
from pathlib import Path
from typing import Callable, Iterator

REPO_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_DIR / ".env"


# ── env reading ─────────────────────────────────────────────────────────────

def _read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


# ── SR fetch ────────────────────────────────────────────────────────────────

def _fetch_schema(env: dict[str, str], subject: str) -> dict:
    """GET /subjects/<subject>/versions/latest, return parsed `schema` dict."""
    auth = "Basic " + base64.b64encode(
        f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()
    ).decode()
    req = urllib.request.Request(
        f"{env['SR_URL']}/subjects/{subject}/versions/latest",
        headers={"Authorization": auth},
    )
    payload = json.loads(urllib.request.urlopen(req, timeout=10).read())
    # SR returns the schema as a JSON-encoded string — decode it
    return json.loads(payload["schema"])


# ── value generators (by field-name heuristic) ──────────────────────────────

_FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace",
                "Henry", "Iris", "Jack", "Kate", "Liam", "Maya", "Noah",
                "Olivia", "Paul", "Quinn", "Riya", "Sam", "Tara"]
_LAST_NAMES  = ["Johnson", "Martinez", "Smith", "Chen", "Patel", "Garcia",
                "Brown", "Davis", "Miller", "Wilson", "Khan", "Nguyen",
                "Singh", "Anderson", "Thomas", "Lee", "Walker", "Hall"]
_STREETS     = ["Maple Ave", "Oak Blvd", "Pine St", "Cedar Ln", "Elm Dr",
                "Birch Way", "Willow Ct", "Aspen Pl", "Sycamore Rd"]
_CITIES      = [("Austin", "TX", "78701"), ("Denver", "CO", "80203"),
                ("Seattle", "WA", "98101"), ("Boston", "MA", "02108"),
                ("Portland", "OR", "97205"), ("Miami", "FL", "33130"),
                ("Chicago", "IL", "60601"), ("Phoenix", "AZ", "85003")]


def _fake_name() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _fake_ssn() -> str:
    return f"{random.randint(100, 899):03d}-{random.randint(10, 99):02d}-{random.randint(1000, 9999):04d}"


def _fake_cc() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(16))


def _fake_cvv() -> str:
    return f"{random.randint(100, 999):03d}"


def _fake_address() -> str:
    num    = random.randint(100, 9999)
    street = random.choice(_STREETS)
    city, state, zipc = random.choice(_CITIES)
    return f"{num} {street}, {city} {state} {zipc}"


def _fake_email() -> str:
    return f"{random.choice(_FIRST_NAMES).lower()}.{random.choice(_LAST_NAMES).lower()}@example.com"


def _fake_phone() -> str:
    return f"+1-{random.randint(200, 999):03d}-{random.randint(200, 999):03d}-{random.randint(1000, 9999):04d}"


_BY_NAME: dict[str, Callable[[], object]] = {
    "loan_id":            lambda: str(uuid.uuid4()),
    "applicant_name":     _fake_name,
    "ssn":                _fake_ssn,
    "credit_card_number": _fake_cc,
    "card_cvv":           _fake_cvv,
    "annual_income_usd":  lambda: round(random.uniform(45000, 280000), 2),
    "loan_amount_usd":    lambda: round(random.uniform(150000, 850000), 2),
    "credit_score":       lambda: random.randint(620, 820),
    "property_address":   _fake_address,
    "email":              _fake_email,
    "phone":              _fake_phone,
}


def _by_type(prop: dict) -> object:
    """Type-based fallback for fields not in the name heuristics."""
    if "enum" in prop:
        return random.choice(prop["enum"])
    t = prop.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), t[0])
    if t == "string":
        return f"sample-{uuid.uuid4().hex[:8]}"
    if t == "integer":
        return random.randint(0, 1000)
    if t == "number":
        return round(random.uniform(0, 1000), 2)
    if t == "boolean":
        return random.choice([True, False])
    if t == "array":
        # Empty array satisfies most schemas (`items` constraint not enforced
        # if absent). If the schema requires elements, the user should add a
        # name-heuristic for that field.
        return []
    if t == "object":
        return {}
    return None


def generate_record(schema: dict) -> dict:
    """Walk the schema's `properties` and produce one record. All declared
    properties are populated (not just `required`) because the serializer
    treats missing string properties as null and rejects them."""
    out: dict[str, object] = {}
    for name, prop in schema.get("properties", {}).items():
        gen = _BY_NAME.get(name)
        out[name] = gen() if gen else _by_type(prop)
    return out


# ── public API ──────────────────────────────────────────────────────────────

def generate(env: dict[str, str], topic_key: str, count: int) -> Iterator[str]:
    """Yield `count` JSON-encoded records matching the schema for the topic."""
    topic = env[f"{topic_key.upper()}_TOPIC"]
    schema = _fetch_schema(env, f"{topic}-value")
    for _ in range(count):
        yield json.dumps(generate_record(schema))


def main() -> None:
    if len(sys.argv) != 3:
        sys.stderr.write("usage: datagen.py <csfle|cspe> <count>\n")
        sys.exit(2)
    topic_key, count_s = sys.argv[1], sys.argv[2]
    if topic_key not in ("csfle", "cspe", "csfle2"):
        sys.stderr.write(f"topic_key must be 'csfle', 'cspe', or 'csfle2', got {topic_key!r}\n")
        sys.exit(2)
    env = {**_read_env_file(ENV_FILE), **os.environ}  # os.environ wins (Makefile path)
    for line in generate(env, topic_key, int(count_s)):
        print(line, flush=True)


if __name__ == "__main__":
    main()
