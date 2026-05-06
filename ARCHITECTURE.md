# Architecture

The demo has three independently-administered domains: **AWS** (KMS keys + IAM), **Confluent Cloud** (Kafka topics, Schema Registry, RBAC), and **the wizard** (a thin local orchestration layer that ties them together). The customer holds the KEK in their AWS account and never shares it with Confluent — encryption/decryption happens client-side inside the producer/consumer subprocesses.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Local laptop                                                       │
│                                                                     │
│   ┌────────────────────────────────────────────────────┐            │
│   │  Wizard — web/server.py on http://localhost:8893   │            │
│   │   · 4 setup cards (AWS · CC login · pick · setup)  │            │
│   │   · 2 producer pages, 4 consumer pages             │            │
│   └──────────────────────┬─────────────────────────────┘            │
│                          │ subprocess.Popen()                       │
│                          ▼                                          │
│   ┌────────────────────────────────────────────────────┐            │
│   │  CP 8.2.0 CLI tools (~/confluent-8.2.0/bin/)       │            │
│   │   · kafka-json-schema-console-{producer,consumer}  │            │
│   │   · kafka-console-consumer (CSPE no-KEK only)      │            │
│   │  Java client + FieldEncryptionExecutor             │            │
│   └─────┬──────────────────────────┬───────────────────┘            │
│         │                          │                                │
└─────────┼──────────────────────────┼────────────────────────────────┘
          │ Kafka SASL_SSL           │ HTTPS (SR REST)
          │ (per-SA API key)         │ (per-SA API key)
          ▼                          ▼                kms:Decrypt /
   ┌──────────────────┐   ┌────────────────────┐    GenerateDataKey
   │ Confluent Cloud  │   │ Confluent Cloud SR │    over HTTPS
   │ Kafka cluster    │   │  · subjects        │   ┌─────────────┐
   │  (lkc-XXX)       │   │  · DEK Registry    │   │   AWS KMS   │
   │  · mortgage-csfle│   │     (KEK metadata, │◀──│ alias/      │
   │  · mortgage-cspe │   │      wrapped DEKs) │   │   *-kek     │
   └──────────────────┘   │  · RBAC bindings   │   └─────────────┘
                          └────────────────────┘    ▲
                                                    │
                                                    │ kms:Decrypt
                                                    │ (consumer's
                                                    │  AWS creds)
                                                    │
                                          (consumer subprocess)
```

## Component responsibilities

### Wizard (`web/server.py`)

A ~1500-line single-file Python stdlib HTTP server (no framework). Three concerns:

1. **Bootstrap orchestration** — calls `confluent` CLI + AWS CLI + SR REST API to provision env / KEKs / schemas / topics / RBAC. Streams progress via Server-Sent Events to the browser.
2. **Subprocess management** — spawns the CP Java CLI tools per producer/consumer page click and forwards their stdout to the page via SSE. Per-(topic, role) AWS env injection / stripping happens here.
3. **Read-only inspection UI** — fetches schema definitions, role bindings, KEK metadata from SR/CC and renders them in the consumer pages so the security boundary is visible.

The wizard NEVER decrypts records itself — it just spawns Java consumers and forwards their output.

### CP CLI tools (subprocessed)

The actual Kafka I/O. Two binaries:

- `kafka-json-schema-console-producer` / `kafka-json-schema-console-consumer` — used by 5 of the 6 client pages. Speaks Confluent's wire format, integrates with SR for schema fetch + rule execution, runs `FieldEncryptionExecutor` for the `ENCRYPT` and `ENCRYPT_PAYLOAD` rules.
- `kafka-console-consumer` (plain, not json-schema) — used **only** by `/cspe/no-kek`, where the JSON-schema deserializer can't reconstruct the encrypted payload and we need raw bytes. The wizard's Python wrapper strips the 5-byte wire-format header (1 magic byte + 4-byte schema id) and base64-encodes the rest.

### Confluent Cloud — Kafka cluster

Vanilla Kafka. Knows nothing about CSFLE/CSPE. Stores whatever bytes the client serializes (encrypted or plaintext, all the same to the broker). Authorizes per-Topic + per-Group via the SA's API key.

### Confluent Cloud — Schema Registry

Holds three things:

1. **Schemas** — the JSON Schema body + its `ruleSet`. CSFLE topic's schema has `domainRules.ENCRYPT`; CSPE's has `encodingRules.ENCRYPT_PAYLOAD`.
2. **DEK Registry** (`/dek-registry/v1/keks/<name>` and `.../deks/<subject>/...`) — KEK metadata pointing at the AWS KMS ARN, plus the wrapped DEKs created on first produce.
3. **RBAC bindings** — `DeveloperRead`/`DeveloperWrite`/`ResourceOwner` on `Subject:*` and `Kek:*` resources. Per-KEK gating happens here (the no-KEK consumer SAs simply have no `Kek:*` binding).

### AWS KMS

Holds the symmetric AES-256 KEK. Two operations the demo cares about:

- `kms:GenerateDataKey` (producer-side) — creates a fresh DEK (datakey) wrapped with the KEK; the producer caches the unwrapped DEK in memory and stores the wrapped form in SR's DEK Registry.
- `kms:Decrypt` (consumer-side) — unwraps a DEK previously stored in SR, so the consumer can decrypt records.

Authorization: the AWS principal whose creds are in the consumer subprocess's env. The demo uses a SINGLE AWS identity (whatever creds you paste in card 1) for both producer + with-KEK consumers.

## Data flow

### Producer → Kafka

```
Wizard /produce/csfle button
    │
    ▼
spawn kafka-json-schema-console-producer with PRODUCER SA's keys
    │
    ▼  (per record)
1. read JSON record from stdin
2. fetch schema by id (uses producer SA's SR key)              [SR REST]
3. see ruleSet.domainRules.ENCRYPT scoped to "PII" tag
4. for each PII-tagged field:
   a. look up wrapped DEK for (KEK, subject) in SR DEK Registry  [SR REST]
   b. if absent: GenerateDataKey via AWS KMS, store wrapped DEK in SR
   c. encrypt the field value with the unwrapped DEK (AES-GCM)
5. serialize the (now partially-encrypted) record to JSON
6. prepend 5-byte wire-format header (magic + schema-id)
7. publish to Kafka                                           [SASL_SSL]
```

CSPE producer is identical except step 3-5 collapse: the entire serialized JSON gets encrypted as one blob via the `encodingRule`.

### Consumer ← Kafka (with-KEK path)

```
Wizard /csfle/with-kek "Start" button
    │
    ▼
spawn kafka-json-schema-console-consumer with consumer-with-kek SA's keys
+ AWS creds in env
    │
    ▼  (per record from Kafka)
1. read 5-byte header → schema id
2. fetch schema by id from SR                                   [SR REST]
3. see ruleSet.domainRules.ENCRYPT
4. for each PII-tagged field:
   a. look up wrapped DEK for (KEK, subject) in SR DEK Registry [SR REST]
        → 200 OK because consumer-with-kek SA has DeveloperRead on Kek:*
   b. unwrap DEK via AWS KMS Decrypt                            [AWS API]
        → success because subprocess has AWS creds with kms:Decrypt
   c. decrypt the field value
5. emit deserialized JSON to stdout (full plaintext)
```

### Consumer ← Kafka (no-KEK path)

For CSFLE no-KEK:

```
Wizard /csfle/no-kek "Start" button
    │
    ▼
spawn kafka-json-schema-console-consumer with consumer-no-kek SA's keys
+ AWS creds STRIPPED + AWS_SHARED_CREDENTIALS_FILE=/dev/null +
  AWS_EC2_METADATA_DISABLED=true
    │
    ▼  (per record)
1-3. (same — schema fetch via Subject:Read)
4. for each PII-tagged field:
   a. look up wrapped DEK in SR DEK Registry                    [SR REST]
        → 403 because consumer-no-kek SA has NO Kek binding
        → rule's onFailure=NONE on READ swallows the error
   b. (skipped — DEK never reached step b)
   c. (skipped — field stays as base64 ciphertext from the wire)
5. emit deserialized JSON; tagged fields contain the encrypted base64 string
```

For CSPE no-KEK:

```
Wizard /cspe/no-kek "Start" button
    │
    ▼
Python: confluent_kafka.Consumer with cspe-consumer-no-kek SA's keys
    │
    ▼  (per record)
1. raw bytes from Kafka
2. strip first 5 bytes (magic + schema id) — extract schema id for display
3. base64-encode the rest (= the encrypted payload bytes)
4. emit {"__raw__": "<base64>", "__schema_id__": NNN} via SSE
```

## Trust boundaries

```
┌─────────────────────── customer trust zone ──────────────────────────┐
│                                                                      │
│   AWS account ─── KMS KEK (private key material — never leaves AWS)  │
│   wizard process                                                     │
│   producer/consumer subprocesses                                     │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                              ▲
                              │  (1) wrapped DEK (encrypted bytes only)
                              │  (2) SR rule definition (KEK alias only)
                              │
                              ▼
┌─────────────────── Confluent Cloud trust zone ───────────────────────┐
│                                                                      │
│   Kafka cluster ─── stores encrypted records (cannot decrypt them)   │
│   Schema Registry ── stores schema + ruleSet + WRAPPED DEK only      │
│   RBAC ── enforces per-KEK access at the SR layer                    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

**Confluent never sees**:
- The unwrapped DEK (created/used only inside the customer's producer/consumer process)
- The KEK private key material (lives only in AWS KMS HSMs)
- The plaintext field values (encryption happens client-side before serialize)

**Confluent does see**:
- The wrapped DEK (encrypted bytes — useless without KEK)
- The KEK ARN reference in the schema rule (a string identifier)
- The encrypted record bytes on Kafka (ciphertext)
- The plaintext non-PII fields (CSFLE only — by design; that's the partial-visibility tradeoff)

## RBAC matrix (after card 4 step 5 runs)

The wizard mints **6 service accounts** with least-privilege bindings — each consumer page's SA has access to ONLY its own KEK. Cross-topic KEK access is impossible by design. (The CSFLE2 add-on adds 5 more SAs — see the CSFLE2 section below.)

| SA | Topic | Subject | Group | KEK |
|---|---|---|---|---|
| `csfle-producer` | DevWrite Topic:csfle | DevWrite Subject:csfle-value | — | DevWrite Kek:csfle-kek |
| `cspe-producer` | DevWrite Topic:cspe | DevWrite Subject:cspe-value | — | DevWrite Kek:cspe-kek |
| `csfle-consumer-with-kek` | DevRead Topic:csfle | DevRead Subject:csfle-value | DevRead Group:demo-csfle-with-kek* | DevRead Kek:csfle-kek (only) |
| `csfle-consumer-no-kek` | DevRead Topic:csfle | DevRead Subject:csfle-value | DevRead Group:demo-csfle-no-kek* | — |
| `cspe-consumer-with-kek` | DevRead Topic:cspe | DevRead Subject:cspe-value | DevRead Group:demo-cspe-with-kek* | DevRead Kek:cspe-kek (only) |
| `cspe-consumer-no-kek` | DevRead Topic:cspe | DevRead Subject:cspe-value | DevRead Group:demo-cspe-no-kek* | — |

`Group:` bindings are PREFIXED (`--prefix` flag) because the wizard appends a timestamp to the consumer group name on "Start (from beginning)" — e.g. `demo-csfle-with-kek-web-1778029231`.

Note: the no-KEK SAs still have `DeveloperRead` on `Subject:*-value` so the consumer can fetch the schema. They DON'T have any binding on `Kek:*`, so the DEK Registry lookup (which is gated by Kek RBAC, not Subject RBAC) returns 403 — that's the Confluent-layer enforcement of the no-KEK boundary.

## Wire format

Confluent's standard schema-aware wire format on every Kafka record:

```
+──────+─────────────────+────────────────────────────────────────────+
│ 0x00 │  schema id (big │  payload bytes                             │
│      │  endian, 4 B)   │  (CSFLE-encrypted fields embedded; CSPE-   │
│      │                 │   encrypted: entire JSON ciphertext)       │
+──────+─────────────────+────────────────────────────────────────────+
   1B          4B               variable
```

The CSPE no-KEK consumer page strips the first 5 bytes and base64-encodes the rest, producing the `__raw__` envelope you see (matching what Confluent Cloud's console UI shows for un-decryptable records).

## Why two enforcement layers

The demo deliberately enforces the no-KEK boundary at BOTH layers:

| Layer | Enforcement mechanism | What happens if you bypass the OTHER layer |
|---|---|---|
| **Confluent SR (RBAC)** | no-KEK SA has no `Kek:*` binding → DEK Registry returns 403 | If AWS IAM ever drifted (e.g., the no-KEK SA's underlying AWS account got `kms:Decrypt`), the SR layer still denies — no DEK to unwrap |
| **AWS IAM (KMS)** | no-KEK consumer subprocess has AWS_* env stripped → KMS denies | If Confluent RBAC ever drifted (admin accidentally granted DevRead on Kek:*), the AWS layer still denies — DEK retrieved but unwrap fails |

Defense in depth — neither layer alone is sufficient; together they make the no-KEK guarantee robust to misconfiguration on either side.

## CSFLE2 — multi-rule per schema (PII + PCI)

The CSFLE2 add-on registers a single subject (`${CSFLE2_TOPIC}-value`) with TWO `domainRules`, each `ENCRYPT`, each scoped to a different tag and KEK:

| Rule | Tag | KEK | Encrypts |
|---|---|---|---|
| `encryptPII` | `PII` | `mortgage-csfle2-pii-kek` | `ssn` |
| `encryptPCI` | `PCI` | `mortgage-csfle2-pci-kek` | `credit_card_number`, `card_cvv` |

The schema body (field set + `required` list) is identical to `mortgage_application.json`; only the tag annotations differ. PII and PCI tags are disjoint — no field carries both — so each tagged field is encrypted exactly once and consumers don't need overlapping access.

**Wire format**: each tagged field gets its own DEK reference in the encrypted ciphertext. A consumer with one KEK (say PII) can decrypt `ssn` independently of whether it has PCI access — the rule executor processes each field's encryption metadata independently and falls back to `onFailure: ERROR,NONE` (read mode = NONE) for fields whose KEK is denied.

**Per-page RBAC** (after wizard card 4 step 6 runs):

| SA | DevWrite (producer) / DevRead (consumer) on | Decrypts |
|---|---|---|
| `csfle2-producer`      | Topic + Subject + `Kek:pii` + `Kek:pci` | (writes) |
| `csfle2-consumer-pii`  | Topic + Subject + Group + `Kek:pii` only | `ssn` only — PCI fields stay ciphertext |
| `csfle2-consumer-pci`  | Topic + Subject + Group + `Kek:pci` only | `credit_card_number` + `card_cvv` only — `ssn` stays ciphertext |
| `csfle2-consumer-both` | Topic + Subject + Group + both KEKs | all tagged fields |
| `csfle2-consumer-none` | Topic + Subject + Group only | none — DEK Registry returns 403 for both KEKs |

**Enablement requirement**: multi-rule per schema is unlocked by `PUT /config` (SR-wide) with `{"validateRules": false}` — exactly what the [docs](https://staging-docs-independent.confluent.io/docs-cloud/PR/6763/current/security/encrypt/csfle/manage-multiple-rules.html) prescribe. The setting is permissive: it allows multi-rule schemas without altering single-rule behavior, so existing CSFLE/CSPE subjects keep working unchanged. The feature is in Limited Availability — if your CC org isn't enabled, `scripts/04_setup_csfle2.sh` fails at this PUT with a 403; contact your Confluent account team to enable.

**Cost**: each rule is billed separately. CSFLE2 = 2 rules = 2× encryption cost vs. a single-rule CSFLE topic.
