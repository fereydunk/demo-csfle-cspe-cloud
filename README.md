# demo-csfle-cspe-cloud

Side-by-side demo of Confluent Cloud's two **client-side** encryption modes — both running entirely on **Confluent Cloud**, both with the customer holding the KEK in **AWS KMS** (KEK never shared with Confluent), both using the native **Schema Registry rule executor framework** so all encryption policy lives in SR, versioned per schema.

Two topics. Six service accounts. One demo. Encryption is enforced both at the **Confluent layer** (per-KEK RBAC) and at the **AWS layer** (KMS Decrypt), so the no-KEK consumer pages demonstrate the security boundary at both layers simultaneously.

| | CSFLE — Client-Side **Field-Level** Encryption | CSPE — Client-Side **Payload** Encryption |
|---|---|---|
| Rule lives in | `ruleSet.domainRules` | `ruleSet.encodingRules` |
| Rule type | `ENCRYPT` | `ENCRYPT_PAYLOAD` |
| Scope | Fields tagged `["PII"]` (`ssn` here) | The whole serialized payload |
| Unauthorized consumer sees | Record structure + non-PII fields plaintext; tagged fields are base64 ciphertext | Entire payload as opaque base64 wrapped in `{"__raw__": "<base64>"}` (matches CC console UI) |
| Topic in this demo | `mortgage-csfle` (renameable) | `mortgage-cspe` (renameable) |
| KEK in AWS KMS | `alias/mortgage-csfle-kek` | `alias/mortgage-cspe-kek` |
| Producer SA | `csfle-producer` | `cspe-producer` |
| Consumer SAs (with-KEK / no-KEK) | `csfle-consumer-with-kek` / `csfle-consumer-no-kek` | `cspe-consumer-with-kek` / `cspe-consumer-no-kek` |

The wizard creates the two KEKs in AWS KMS for you (no need to pre-create them), registers them in SR's DEK Registry, and mints all 6 service accounts with least-privilege role bindings — the CSFLE consumer can't see the CSPE KEK and vice versa.

## Prerequisites

- macOS with Homebrew + `openjdk@21` (`/opt/homebrew/opt/openjdk@21`)
- Confluent Platform 8.2.0 installed at `~/confluent-8.2.0` (the demo subprocesses its `kafka-json-schema-console-{producer,consumer}` and `kafka-console-consumer` CLI tools)
- `confluent` CLI installed (the wizard runs `confluent login --save` for you on card 2)
- `aws` CLI installed (used to create KMS keys; credentials provided via the wizard)
- Python 3 with `confluent-kafka` (`pip3 install confluent-kafka`) — needed by the CSPE no-KEK consumer page that bypasses the JSON-schema deserializer to render the encrypted blob; the other 3 pages don't need it
- A Confluent Cloud env with at least 1 Kafka cluster + Schema Registry enabled. Stream Governance can be on **Essentials** — the wizard auto-upgrades to **Advanced** in card 4 step 1 (CSFLE/CSPE rules require the Advanced tier).
- Confluent Cloud OrgAdmin role (so the wizard can mint Cloud + Kafka + SR API keys, PATCH the SR cluster's package, and create service accounts with role bindings)

No license JWT needed — Stream Governance Advanced authorizes the encryption executor on Cloud.

## Quick start

```bash
cd ~/demo-csfle-cspe-cloud
bash startup.sh        # opens http://localhost:8893 automatically
```

Walk the 4 cards top to bottom:

1. **Card 1 — AWS credentials**: paste your `export AWS_*` block (parser handles `export KEY=val`, `KEY="val"`, single/double/no quotes). Stored in `config/aws-session.env` (gitignored, mode 600).
2. **Card 2 — Confluent Cloud sign-in**: shows your existing context if logged in. Otherwise paste email + password — wizard runs `confluent login --save`.
3. **Card 3 — Pick env, cluster & topic names**: cascading dropdowns. Click "Save & mint API keys" — wizard describes the cluster + SR, mints fresh OrgAdmin Kafka + SR keys, deletes orphaned old keys when you switch clusters.
4. **Card 4 — Setup** (5 buttons + "Run all"). Build infrastructure first, grant access last:
   - **1 · Stream Governance** — verifies SG is `ADVANCED` (or upgrades via SRCM v3 PATCH; mints a Cloud-scoped API key on demand). **Billing implications: ADVANCED is a paid tier.**
   - **2 · KEKs** — `aws kms create-key` × 2 + `POST /dek-registry/v1/keks` × 2
   - **3 · schemas** — `POST /subjects/{topic}-value/versions` × 2 with their respective rule sets
   - **4 · topics** — `confluent kafka topic create` × 2
   - **5 · RBAC (last)** — mints **6 service accounts** (one per (topic, role) pair), 12 API keys (Kafka + SR per SA), and role bindings against the now-existing resources. Producer SAs get `DeveloperWrite` on Topic + Subject + their KEK. Consumer-with-KEK SAs get `DeveloperRead` on Topic + Subject + Group + **their own KEK only** (CSFLE consumer can't see CSPE KEK and vice versa). Consumer-no-KEK SAs get `DeveloperRead` on Topic + Subject + Group only — **no Kek binding** → SR returns 403 on DEK lookup → records fail to decrypt at the Confluent layer.

After bootstrap, 6 nav routes:

- **CSFLE Producer** (`/produce/csfle`): pick how many records (1 to 20 from the sample file), click Produce. Each record streamed inline.
- **CSPE Producer** (`/produce/cspe`): same flow.
- **CSFLE w/ KEK** (`/csfle/with-kek`): consumer with AWS creds in env. Click "Start (from beginning)".
- **CSFLE no KEK** (`/csfle/no-kek`): consumer with AWS creds STRIPPED + SA without Kek RBAC. Records arrive with `ssn` as base64 ciphertext.
- **CSPE w/ KEK** (`/cspe/with-kek`): consumer with AWS creds. Records arrive as plaintext JSON.
- **CSPE no KEK** (`/cspe/no-kek`): special path — uses the plain confluent-kafka Python client (no JSON deserialization) and emits each record as `{"__raw__": "<base64>", "__schema_id__": NNN}` matching how Confluent Cloud's console UI displays un-decryptable CSPE payloads.

## What you should observe

| Page | `ssn` field | Full record |
|---|---|---|
| `/csfle/with-kek` | `123-45-6789` (plaintext) | full JSON visible |
| `/csfle/no-kek`   | `<base64 ciphertext>` | rest of record visible (loan_id, name, etc.) |
| `/cspe/with-kek`  | `123-45-6789` (plaintext) | full JSON visible |
| `/cspe/no-kek`    | (not separately visible) | `{"__raw__": "<opaque base64>"}` — entire payload encrypted |

That contrast is the demo's point: **CSFLE gives unauthorized consumers partial visibility** (record structure + non-PII fields) — useful when downstream tooling needs to route/filter on non-PII columns; **CSPE gives them nothing** — useful when even the existence of fields is sensitive.

## Documentation

- **[ARCHITECTURE.md](./ARCHITECTURE.md)** — component view, data flow per page, trust boundaries, RBAC matrix, wire format details
- **[TROUBLESHOOTING.md](./TROUBLESHOOTING.md)** — common gotchas: expired AWS SSO, stale DEKs, port-in-use, browser cache, deprecation warnings, missing service accounts

## CLI usage (without the web UI)

```bash
make discover                 # 00_discover_env.sh — picks env/cluster from CC, mints API keys, writes .env
make keks                     # 01_setup_keks.sh — needs AWS creds in env or config/aws-session.env
make schemas                  # 02_register_schemas.sh
make topics                   # 03_create_topics.sh
make setup                    # all of the above

# Note: the CLI path doesn't auto-upgrade Stream Governance, doesn't create
# the 6 RBAC service accounts, and doesn't enforce per-KEK RBAC at the
# Confluent layer. Use the wizard if you want the full RBAC story.

make produce-csfle            # produce sample records to mortgage-csfle
make produce-cspe             # produce sample records to mortgage-cspe

make consume-csfle-auth       # AWS creds present → ssn decrypted
make consume-csfle-unauth     # AWS creds STRIPPED → ssn ciphertext
make consume-cspe-auth        # AWS creds present → full payload visible
make consume-cspe-unauth      # AWS creds STRIPPED → opaque payload

make web                      # launch the wizard at :8893
make kill                     # stop the wizard
```

## Schema versioning

Rules live in the schema, so any rule edit is a SR `POST /subjects/.../versions` that bumps the version automatically. To rotate a KEK or change the algorithm:

1. Edit `scripts/02_register_schemas.sh` (e.g. swap the algorithm or KEK alias)
2. Re-run `make schemas` — SR returns a new schema id
3. New produces use the new rule; old records still decrypt with their original DEK (DEK is stored per-record, scoped to its KEK at write time)

## File layout

```
.env                     # filled by wizard, gitignored
config/aws-session.env   # AWS creds, gitignored, mode 600
config/*.properties      # 6 templates (envsubst'd at runtime by Makefile)
schemas/mortgage_application.json
data/mortgage-records.json
scripts/00..03_*.sh
web/server.py            # single-port wizard + 2 producer pages + 4 consumer pages
Makefile
startup.sh
ARCHITECTURE.md
TROUBLESHOOTING.md
```

## Teardown

```bash
make clean-rbac          # delete the 6 service accounts (cascades to per-SA API keys + role bindings)
make clean-keys          # schedule both KMS keys for 7-day deletion + remove KMS aliases
# Manually delete topics / SR subjects / OrgAdmin API keys via Confluent Cloud UI or `confluent` CLI
# After clean-rbac, clear the 30 *_SA_ID/*_API_KEY/*_API_SECRET lines from .env
```

## Known assumption

The producer/consumer config uses ONE executor entry (`FieldEncryptionExecutor`) on the assumption that the rule executor framework dispatches by rule type internally — this works for both `ENCRYPT` (CSFLE) and `ENCRYPT_PAYLOAD` (CSPE) rules. If first-run shows "no executor for `ENCRYPT_PAYLOAD`", add a second executor entry for `PayloadEncryptionExecutor` (search for `TODO verify` in `config/producer-csfle.properties`).
