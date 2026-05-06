# demo-csfle-cspe-cloud

Side-by-side demo of Confluent's two client-side encryption modes — both running entirely on Confluent Cloud, both with the customer holding the KEK in **AWS KMS** (KEK never shared with Confluent), both using the native **Schema Registry rule executor framework** so all encryption policy lives in SR (versioned).

| | CSFLE — Client-Side Field-Level Encryption | CSPE — Client-Side Payload Encryption |
|---|---|---|
| Rule lives in | `ruleSet.domainRules` | `ruleSet.encodingRules` |
| Rule type | `ENCRYPT` | `ENCRYPT_PAYLOAD` |
| Scope | Fields tagged `["PII"]` (`ssn` here) | Serialized payload |
| Unauthorized consumer sees | Record structure + non-PII fields plaintext; tagged fields are base64 ciphertext | Entire payload is opaque base64 ciphertext |
| Topic in this demo | `mortgage-csfle` (renameable in card 3) | `mortgage-cspe` (renameable in card 3) |
| KEK in AWS KMS | `alias/mortgage-csfle-kek` | `alias/mortgage-cspe-kek` |

The wizard creates the two KEKs in AWS KMS for you (no need to pre-create them) and registers them in SR's DEK Registry under their aliases.

## Prerequisites

- macOS with Homebrew + `openjdk@21` (`/opt/homebrew/opt/openjdk@21`)
- Confluent Platform 8.2.0 installed at `~/confluent-8.2.0` (the demo uses its `kafka-json-schema-console-{producer,consumer}` CLI tools)
- `confluent` CLI installed (the wizard runs `confluent login --save` for you on card 2)
- `aws` CLI installed (used to create KMS keys; credentials provided via the wizard's card 1)
- A Confluent Cloud env with at least 1 Kafka cluster + Schema Registry enabled. Stream Governance can be on **Essentials** — the wizard auto-upgrades to **Advanced** in card 4 step 1 (CSFLE/CSPE rules require the Advanced tier).
- Confluent Cloud OrgAdmin role (so the wizard can mint Kafka + SR + Cloud-scoped API keys, and PATCH the SR cluster's package)

No license JWT needed — Stream Governance Advanced authorizes the encryption executor on Cloud.

## Quick start

```bash
cd ~/demo-csfle-cspe-cloud
bash startup.sh        # http://localhost:8893 opens automatically
```

Then in the browser, walk the 4 cards top to bottom:

1. **Card 1 — AWS credentials**: paste your `export AWS_*` block (the wizard parses key/secret/optional session token from any of: `export KEY=val`, `KEY="val"`, single/double/no quotes). Stored in `config/aws-session.env` (gitignored, mode 600).
2. **Card 2 — Confluent Cloud sign-in**: shows your existing context if you're already logged in. Otherwise enter your CC email + password — the wizard runs `confluent login --save` in the background.
3. **Card 3 — Pick env, cluster & topic names**: cascading dropdowns (envs first, clusters populate when env selected). Two text inputs default to `mortgage-csfle` / `mortgage-cspe`. Clicking "Save & mint API keys" describes the cluster + SR, mints fresh Kafka + SR API keys (and deletes orphaned old ones if you switch clusters), and writes everything to `.env`.
4. **Card 4 — Setup** (5 buttons + "Run all"):
   - **1 · Stream Governance** — verifies SG is on `ADVANCED` (or upgrades via SRCM v3 PATCH; mints a Cloud-scoped API key on demand). **Billing implications: ADVANCED is a paid tier.**
   - **2 · KEKs** — `aws kms create-key` × 2 + `POST /dek-registry/v1/keks` × 2
   - **3 · RBAC** — mints 3 service accounts (`producer`, `consumer-with-kek`, `consumer-no-kek`), 6 API keys (Kafka + SR per SA), and role bindings: producer gets `DeveloperWrite` on Topic + Subject + Kek; consumer-with-kek gets `DeveloperRead` on those + Group; **consumer-no-kek gets `DeveloperRead` on Topic + Subject + Group ONLY — no Kek binding**, so its DEK Registry lookups return 403 (the Confluent-side enforcement of the no-KEK boundary). Must follow KEKs because bindings reference KEK names.
   - **4 · schemas** — `POST /subjects/{topic}-value/versions` for both topics with their respective rule sets
   - **5 · topics** — `confluent kafka topic create` × 2

After card 4 succeeds, use the nav to access:

- **CSFLE Producer** (`/produce/csfle`): pick how many records (1 to 20 from the sample file), click Produce. Each record is logged inline as it's sent.
- **CSPE Producer** (`/produce/cspe`): same flow.
- **CSFLE w/ KEK** (`/csfle/with-kek`): consumer with AWS creds in env. Click "Start (from beginning)".
- **CSFLE no KEK** (`/csfle/no-kek`): consumer with AWS creds STRIPPED.
- **CSPE w/ KEK** / **CSPE no KEK**: same pair for the CSPE topic.

## What you should observe

| Page | Expected output for `ssn` field | Expected output for full record |
|---|---|---|
| `/csfle/with-kek` | `123-45-6789` (plaintext) | full JSON visible |
| `/csfle/no-kek`   | `<base64 ciphertext>` | rest of record visible (loan_id, name, etc.) |
| `/cspe/with-kek`  | `123-45-6789` (plaintext) | full JSON visible |
| `/cspe/no-kek`    | (not separately visible) | entire value is opaque base64 — the whole serialized payload is encrypted |

That contrast is the demo's point: CSFLE gives you partial visibility (record structure + non-PII fields) for unauthorized consumers; CSPE gives them nothing. Subprocess errors (KMS denied, schema missing, license check) appear inline in red on each consumer page.

## CLI usage (without the web UI)

```bash
make discover                 # 00_discover_env.sh — picks env/cluster from CC, mints API keys, writes .env
make keks                     # 01_setup_keks.sh — needs AWS creds in env or config/aws-session.env
make schemas                  # 02_register_schemas.sh
make topics                   # 03_create_topics.sh
make setup                    # all of the above

# Note: the CLI path doesn't auto-upgrade Stream Governance. If your env is on
# Essentials, do the upgrade through the wizard's card 4 step 1 OR manually
# via the Cloud UI before running `make schemas`.

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
```

## Teardown

```bash
make clean-rbac          # delete the 3 service accounts (cascades to per-SA API keys + role bindings)
make clean-keys          # schedule both KMS keys for 7-day deletion + remove KMS aliases
# Manually delete topics / SR subjects / OrgAdmin API keys via Confluent Cloud UI or `confluent` CLI
# After clean-rbac, clear the 12 PRODUCER_*/CONSUMER_KEK_*/CONSUMER_NOKEK_* lines from .env
```

## Known assumption

The producer/consumer config uses ONE executor entry (`FieldEncryptionExecutor`) on the assumption that the rule executor framework dispatches by rule type internally — this works for both `ENCRYPT` (CSFLE) and `ENCRYPT_PAYLOAD` (CSPE) rules. If first-run shows "no executor for `ENCRYPT_PAYLOAD`", add a second executor entry for `PayloadEncryptionExecutor` (search for `TODO verify` in `config/producer-csfle.properties`).
