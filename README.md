# demo-csfle-cspe-cloud

Side-by-side demo of Confluent Cloud's two **client-side** encryption modes — both running entirely on **Confluent Cloud**, both with the customer holding the KEK in **AWS KMS** (KEK never shared with Confluent), both using the native **Schema Registry rule executor framework** so all encryption policy lives in SR, versioned per schema.

Two topics, six service accounts in the core demo — plus an optional **CSFLE2** multi-rule add-on (third topic, two more KEKs, five more SAs) that demonstrates two encryption rules on a single schema (PII + PCI under different KEKs). Encryption is enforced both at the **Confluent layer** (per-KEK RBAC) and at the **AWS layer** (KMS Decrypt), so the no-KEK consumer pages demonstrate the security boundary at both layers simultaneously.

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

After bootstrap, 6 nav routes (plus 5 more if the CSFLE2 add-on ran — see below):

- **CSFLE Producer** (`/produce/csfle`): pick how many records (1 to 100, generated fresh from the live schema by `web/datagen.py`), click Produce. Each record streamed inline.
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

## CSFLE2 — multi-rule encryption (PII + PCI on the same schema)

Optional add-on demonstrating **multiple encryption rules per schema** ([docs](https://staging-docs-independent.confluent.io/docs-cloud/PR/6763/current/security/encrypt/csfle/manage-multiple-rules.html) — Limited Availability feature, may need account-team enablement). Same field set as CSFLE/CSPE; new tags + a second KEK.

| | CSFLE2 — Multi-Rule Field-Level Encryption |
|---|---|
| Rules | 2 × `ENCRYPT` in `ruleSet.domainRules` |
| Tag → KEK mapping | `["PII"]` → `mortgage-csfle2-pii-kek` (encrypts `ssn`) · `["PCI"]` → `mortgage-csfle2-pci-kek` (encrypts `credit_card_number`, `card_cvv`) |
| Topic | `mortgage-csfle2` (renameable) |
| Schema | `schemas/mortgage_application_csfle2.json` (same fields as the others; only the tag annotations differ) |
| Producer SA | `csfle2-producer` (DevWrite on Topic + Subject + both KEKs) |
| Consumer SAs (4) | `csfle2-consumer-pii` / `csfle2-consumer-pci` / `csfle2-consumer-both` / `csfle2-consumer-none` |
| SR config | `validateRules=false` PUT on the SR-wide `/config` endpoint (permissive — single-rule subjects keep working unchanged) |
| Billing | Each rule billed separately — CSFLE2 = 2 rules = 2× encryption cost |

**Setup**: wizard card 4 step **6 · CSFLE2 setup** runs `scripts/04_setup_csfle2.sh` then mints the 5 service accounts. Existing CSFLE/CSPE setup is not touched.

5 new nav routes:

- **CSFLE2 Producer** (`/produce/csfle2`): produces records with both PII and PCI tagged fields encrypted under their respective KEKs.
- **CSFLE2 PII-only** (`/csfle2/pii`): consumer with `Kek:pii` only.
- **CSFLE2 PCI-only** (`/csfle2/pci`): consumer with `Kek:pci` only.
- **CSFLE2 both** (`/csfle2/both`): consumer with both KEKs.
- **CSFLE2 none** (`/csfle2/none`): consumer with no KEK access.

| Page | `ssn` (PII) | `credit_card_number` + `card_cvv` (PCI) |
|---|---|---|
| `/csfle2/pii`  | plaintext | base64 ciphertext |
| `/csfle2/pci`  | base64 ciphertext | plaintext |
| `/csfle2/both` | plaintext | plaintext |
| `/csfle2/none` | base64 ciphertext | base64 ciphertext |

Per-field decryption is independent — each tagged field carries its own DEK reference. With `onFailure: ERROR,NONE` on the read path, an SR DEK Registry 403 (the SA has no role binding for that KEK) leaves the ciphertext in place rather than hard-failing the record. Net effect: each consumer page sees exactly what its principal is entitled to.

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

make produce-csfle            # produce records to mortgage-csfle      (override count: COUNT=N)
make produce-cspe             # produce records to mortgage-cspe       (override count: COUNT=N)

make consume-csfle-auth       # AWS creds present → ssn decrypted
make consume-csfle-unauth     # AWS creds STRIPPED → ssn ciphertext
make consume-cspe-auth        # AWS creds present → full payload visible
make consume-cspe-unauth      # AWS creds STRIPPED → opaque payload

# CSFLE2 multi-rule add-on (separate from `make setup` since it needs
# CSFLE2_TOPIC set in .env first — done via the wizard's card 3)
make setup-csfle2             # 04_setup_csfle2.sh — KEKs + multi-rule schema + topic
make produce-csfle2           # produce records to CSFLE2 topic        (override count: COUNT=N)
make consume-csfle2-pii       # PII KEK only  → ssn plaintext, cc/cvv ciphertext
make consume-csfle2-pci       # PCI KEK only  → cc/cvv plaintext, ssn ciphertext
make consume-csfle2-both      # both KEKs     → all decrypted
make consume-csfle2-none      # no KEKs       → all ciphertext
make clean-csfle2-rbac        # delete the 5 CSFLE2 SAs
make clean-csfle2-keys        # schedule the 2 CSFLE2 KMS keys for deletion

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
config/*.properties      # 11 templates (6 CSFLE/CSPE + 5 CSFLE2; envsubst'd at runtime by Makefile)
schemas/mortgage_application.json          # CSFLE + CSPE (ssn=PII)
schemas/mortgage_application_csfle2.json   # CSFLE2 (same fields, ssn=PII + cc/cvv=PCI)
scripts/00..03_*.sh                        # CSFLE + CSPE setup
scripts/04_setup_csfle2.sh                 # CSFLE2 setup (KEKs + validateRules + schema + topic)
web/server.py            # single-port wizard + 3 producer pages + 8 consumer pages
web/datagen.py           # schema-driven sample-record generator (no static data file)
Makefile
startup.sh
ARCHITECTURE.md
TROUBLESHOOTING.md
```

## Teardown

```bash
make clean-rbac          # delete the 6 CSFLE/CSPE service accounts (cascades to per-SA API keys + role bindings)
make clean-keys          # schedule the 2 CSFLE/CSPE KMS keys for 7-day deletion + remove KMS aliases
make clean-csfle2-rbac   # delete the 5 CSFLE2 SAs (only relevant if you ran the CSFLE2 add-on)
make clean-csfle2-keys   # schedule the 2 CSFLE2 KMS keys for deletion (PII + PCI)
# Manually delete topics / SR subjects / OrgAdmin API keys via Confluent Cloud UI or `confluent` CLI
# After clean-rbac, clear the 30 *_SA_ID/*_API_KEY/*_API_SECRET lines from .env
```

## Encryption executor

The producer/consumer config uses **one** `FieldEncryptionExecutor` entry. The rule executor framework dispatches by rule type internally, so the same single entry handles `ENCRYPT` (CSFLE field-level), `ENCRYPT_PAYLOAD` (CSPE payload), and multi-rule `ENCRYPT` (CSFLE2 PII + PCI). Verified end-to-end across all three flows.
