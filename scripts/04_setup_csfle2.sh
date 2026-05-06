#!/usr/bin/env bash
# CSFLE2 setup — multi-rule encryption demo (PII + PCI on the same schema).
#
# Self-contained, idempotent. Does the equivalent of 01+02+03 for CSFLE2:
#   1. Create 2 KMS keys + aliases (mortgage-csfle2-pii-kek, mortgage-csfle2-pci-kek)
#   2. Register both KEKs in SR's DEK Registry
#   3. PUT validateRules=false on the SR-wide /config endpoint (per docs — this
#      unlocks multi-rule per schema; permissive, doesn't change single-rule
#      behavior so existing CSFLE/CSPE subjects keep working unchanged)
#   4. Register schemas/mortgage_application_csfle2.json under ${CSFLE2_TOPIC} and
#      ${CSFLE2_TOPIC}-value with TWO domainRules (PII → pii-kek, PCI → pci-kek)
#   5. Create the ${CSFLE2_TOPIC} topic in Confluent Cloud
#
# Existing CSFLE/CSPE resources are not touched.
#
# Note: multi-rule per schema is a Limited Availability feature — if your CC
# org isn't enabled, the validateRules=false PUT (or the multi-rule schema
# register) may 403. Contact your Confluent account team to enable.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh first" >&2; exit 1; }
source "$ENV_FILE"
[[ -f "${REPO_DIR}/config/aws-session.env" ]] && set -a && source "${REPO_DIR}/config/aws-session.env" && set +a

[[ -n "${SR_URL:-}"        ]] || { echo "ERROR: SR_URL not set"        >&2; exit 1; }
[[ -n "${SR_API_KEY:-}"    ]] || { echo "ERROR: SR_API_KEY not set"    >&2; exit 1; }
[[ -n "${SR_API_SECRET:-}" ]] || { echo "ERROR: SR_API_SECRET not set" >&2; exit 1; }
[[ -n "${AWS_REGION:-}"    ]] || { echo "ERROR: AWS_REGION not set"    >&2; exit 1; }
[[ -n "${ENV_ID:-}"        ]] || { echo "ERROR: ENV_ID not set"        >&2; exit 1; }
[[ -n "${CLUSTER_ID:-}"    ]] || { echo "ERROR: CLUSTER_ID not set"    >&2; exit 1; }
[[ -n "${CSFLE2_TOPIC:-}"  ]] || { echo "ERROR: CSFLE2_TOPIC not set in .env (set via wizard card 3)" >&2; exit 1; }
command -v aws >/dev/null     || { echo "ERROR: aws CLI not installed"  >&2; exit 1; }

# Default the two KEK aliases if .env doesn't pin them
: "${CSFLE2_PII_KEK_NAME:=mortgage-csfle2-pii-kek}"
: "${CSFLE2_PCI_KEK_NAME:=mortgage-csfle2-pci-kek}"

upsert() {
  local k="$1" v="$2"
  if grep -qE "^${k}=" "$ENV_FILE"; then
    sed -i '' "s|^${k}=.*|${k}=${v}|" "$ENV_FILE"
  else
    echo "${k}=${v}" >> "$ENV_FILE"
  fi
}

ensure_kms_key() {
  # ensure_kms_key ALIAS DESCRIPTION → echoes the resolved ARN
  local alias="alias/$1"
  local description="$2"
  local existing
  existing=$(aws kms describe-key --key-id "$alias" --region "$AWS_REGION" --output json 2>/dev/null \
              | python3 -c "import sys,json; print(json.load(sys.stdin)['KeyMetadata']['Arn'])" 2>/dev/null || true)
  if [[ -n "$existing" ]]; then
    echo "$existing"
    return 0
  fi
  local key_id
  key_id=$(aws kms create-key \
            --description "$description" \
            --key-usage ENCRYPT_DECRYPT \
            --customer-master-key-spec SYMMETRIC_DEFAULT \
            --tags TagKey=Project,TagValue=demo-csfle-cspe-cloud \
            --region "$AWS_REGION" --output json \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['KeyMetadata']['KeyId'])")
  aws kms create-alias --alias-name "$alias" --target-key-id "$key_id" --region "$AWS_REGION" >/dev/null
  aws kms describe-key --key-id "$alias" --region "$AWS_REGION" --output json \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['KeyMetadata']['Arn'])"
}

register_kek_in_sr() {
  local kek_name="$1" kms_arn="$2" doc="$3"
  local body
  body=$(printf '{"name":"%s","kmsType":"aws-kms","kmsKeyId":"%s","shared":false,"doc":"%s"}' \
                "$kek_name" "$kms_arn" "$doc")
  local status
  status=$(curl -s -o /tmp/kek_response.json -w "%{http_code}" \
            -u "${SR_API_KEY}:${SR_API_SECRET}" \
            -H "Content-Type: application/json" \
            -X POST "${SR_URL}/dek-registry/v1/keks" \
            -d "$body")
  if [[ "$status" == "200" || "$status" == "409" ]]; then
    echo "  ✓ SR DEK registry: ${kek_name} (HTTP ${status})"
  else
    echo "  ✗ SR DEK registry: HTTP ${status}"
    cat /tmp/kek_response.json
    exit 1
  fi
}

# ── Step 1+2: KEKs ──────────────────────────────────────────────────────────
echo "── CSFLE2 PII KEK ─────────────────────────────────────────────────────"
echo "→ AWS KMS: alias/${CSFLE2_PII_KEK_NAME} in ${AWS_REGION} ..."
pii_arn=$(ensure_kms_key "$CSFLE2_PII_KEK_NAME" "demo-csfle-cspe-cloud — CSFLE2 PII KEK")
upsert CSFLE2_PII_KMS_ARN  "$pii_arn"
upsert CSFLE2_PII_KEK_NAME "$CSFLE2_PII_KEK_NAME"
echo "  ✓ ${pii_arn}"
echo "→ SR DEK Registry: ${CSFLE2_PII_KEK_NAME} ..."
register_kek_in_sr "$CSFLE2_PII_KEK_NAME" "$pii_arn" "CSFLE2 PII KEK for ${CSFLE2_TOPIC}"

echo ""
echo "── CSFLE2 PCI KEK ─────────────────────────────────────────────────────"
echo "→ AWS KMS: alias/${CSFLE2_PCI_KEK_NAME} in ${AWS_REGION} ..."
pci_arn=$(ensure_kms_key "$CSFLE2_PCI_KEK_NAME" "demo-csfle-cspe-cloud — CSFLE2 PCI KEK")
upsert CSFLE2_PCI_KMS_ARN  "$pci_arn"
upsert CSFLE2_PCI_KEK_NAME "$CSFLE2_PCI_KEK_NAME"
echo "  ✓ ${pci_arn}"
echo "→ SR DEK Registry: ${CSFLE2_PCI_KEK_NAME} ..."
register_kek_in_sr "$CSFLE2_PCI_KEK_NAME" "$pci_arn" "CSFLE2 PCI KEK for ${CSFLE2_TOPIC}"

# ── Step 3: enable multi-rule per schema (global SR config) ─────────────────
# Per docs (https://staging-docs-independent.confluent.io/docs-cloud/PR/6763/
# current/security/encrypt/csfle/manage-multiple-rules.html), the setting is
# applied at the SR-wide /config endpoint (not subject-scoped). Single-rule
# schemas (existing CSFLE/CSPE) keep working unchanged — the flag is permissive,
# it allows multi-rule but doesn't require or alter single-rule behavior.
echo ""
echo "── SR config: validateRules=false (global — unlocks multi-rule per schema) ──"
status=$(curl -s -o /tmp/cfg_response.json -w "%{http_code}" \
          -u "${SR_API_KEY}:${SR_API_SECRET}" \
          -H "Content-Type: application/json" \
          -X PUT "${SR_URL}/config" \
          -d '{"validateRules": false}')
if [[ "$status" == "200" ]]; then
  echo "  ✓ validateRules=false (HTTP 200) — multi-rule per schema unlocked SR-wide"
else
  echo "  ✗ HTTP ${status} — multi-rule per schema may not be enabled on this CC org"
  echo "    (Limited Availability — contact your Confluent account team)"
  cat /tmp/cfg_response.json
  exit 1
fi

# ── Step 4: register multi-rule schema ──────────────────────────────────────
SCHEMA_FILE="${REPO_DIR}/schemas/mortgage_application_csfle2.json"
[[ -f "$SCHEMA_FILE" ]] || { echo "ERROR: $SCHEMA_FILE not found" >&2; exit 1; }
SCHEMA_STR=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$SCHEMA_FILE")

build_csfle2_payload() {
  cat <<EOF
{
  "schemaType": "JSON",
  "schema": ${SCHEMA_STR},
  "metadata": {
    "properties": {
      "version":     "1.0.0",
      "owner":       "demo-csfle-cspe-cloud",
      "description": "MortgageApplication — CSFLE2: ssn (PII) + cc/cvv (PCI), two KEKs",
      "encryption":  "csfle2-multirule",
      "pii_fields":  "ssn",
      "pci_fields":  "credit_card_number,card_cvv"
    }
  },
  "ruleSet": {
    "domainRules": [
      {
        "name": "encryptPII",
        "kind": "TRANSFORM",
        "type": "ENCRYPT",
        "mode": "WRITEREAD",
        "tags": ["PII"],
        "params": {
          "encrypt.kek.name":   "${CSFLE2_PII_KEK_NAME}",
          "encrypt.kms.key.id": "${pii_arn}",
          "encrypt.kms.type":   "aws-kms",
          "encrypt.algorithm":  "AES256_GCM"
        },
        "onFailure": "ERROR,NONE"
      },
      {
        "name": "encryptPCI",
        "kind": "TRANSFORM",
        "type": "ENCRYPT",
        "mode": "WRITEREAD",
        "tags": ["PCI"],
        "params": {
          "encrypt.kek.name":   "${CSFLE2_PCI_KEK_NAME}",
          "encrypt.kms.key.id": "${pci_arn}",
          "encrypt.kms.type":   "aws-kms",
          "encrypt.algorithm":  "AES256_GCM"
        },
        "onFailure": "ERROR,NONE"
      }
    ]
  }
}
EOF
}

register_schema() {
  local subject="$1" payload="$2"
  local status
  status=$(curl -s -o /tmp/schema_response.json -w "%{http_code}" \
            -u "${SR_API_KEY}:${SR_API_SECRET}" \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            -X POST "${SR_URL}/subjects/${subject}/versions" \
            -d "$payload")
  if [[ "$status" == "200" ]]; then
    local sid
    sid=$(python3 -c "import json; print(json.load(open('/tmp/schema_response.json'))['id'])")
    echo "  ✓ ${subject} → schema id ${sid}"
  else
    echo "  ✗ ${subject} → HTTP ${status}"
    cat /tmp/schema_response.json
    exit 1
  fi
}

echo ""
echo "── Register CSFLE2 schema (PII + PCI rules) ───────────────────────────"
csfle2_payload=$(build_csfle2_payload)
register_schema "${CSFLE2_TOPIC}"        "$csfle2_payload"
register_schema "${CSFLE2_TOPIC}-value"  "$csfle2_payload"

# ── Step 5: create topic ────────────────────────────────────────────────────
echo ""
echo "── Create topic ${CSFLE2_TOPIC} ───────────────────────────────────────"
if confluent kafka topic create "$CSFLE2_TOPIC" \
     --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
     --partitions 3 --if-not-exists 2>&1 | tee /tmp/topic_create.log; then
  echo "  ✓ ready"
else
  echo "  ✗ topic create failed"
  exit 1
fi

echo ""
echo "CSFLE2 infrastructure ready. Next: run RBAC bootstrap (wizard card 4 step 6)"
echo "to mint the 5 service accounts (csfle2-producer + 4 consumers)."
