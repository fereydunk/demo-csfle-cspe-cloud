#!/usr/bin/env bash
# Register the MortgageApplication JSON Schema under TWO subjects each, with
# different rule sets:
#
#   ${CSFLE_TOPIC}-value  → ruleSet.domainRules → ENCRYPT (field-level, scoped by PII tag)
#   ${CSPE_TOPIC}-value   → ruleSet.encodingRules → ENCRYPT_PAYLOAD (payload)
#
# Also registers under the canonical (un-suffixed) subject name so both the
# user-visible and TopicNameStrategy serializer subjects resolve.
#
# Idempotent: SR returns the same schema ID on re-POST of identical content.
# Re-running with a changed schema body (e.g. add a tag) creates a NEW version
# under the same subject — that's how schema-versioned rule evolution works.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh first" >&2; exit 1; }
source "$ENV_FILE"

[[ -n "${SR_URL:-}"        ]] || { echo "ERROR: SR_URL not set"        >&2; exit 1; }
[[ -n "${SR_API_KEY:-}"    ]] || { echo "ERROR: SR_API_KEY not set"    >&2; exit 1; }
[[ -n "${CSFLE_KEK_NAME:-}" && -n "${CSFLE_KMS_ARN:-}" ]] || { echo "ERROR: CSFLE KEK not set — run 01_setup_keks.sh" >&2; exit 1; }
[[ -n "${CSPE_KEK_NAME:-}"  && -n "${CSPE_KMS_ARN:-}"  ]] || { echo "ERROR: CSPE KEK not set — run 01_setup_keks.sh"  >&2; exit 1; }

SCHEMA_FILE="${REPO_DIR}/schemas/mortgage_application.json"
SCHEMA_STR=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$SCHEMA_FILE")

build_csfle_payload() {
  cat <<EOF
{
  "schemaType": "JSON",
  "schema": ${SCHEMA_STR},
  "metadata": {
    "properties": {
      "version":     "1.0.0",
      "owner":       "demo-csfle-cspe-cloud",
      "description": "MortgageApplication — CSFLE: ssn encrypted via AES256_GCM (field-level)",
      "pii_fields":  "ssn",
      "encryption":  "csfle"
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
          "encrypt.kek.name":   "${CSFLE_KEK_NAME}",
          "encrypt.kms.key.id": "${CSFLE_KMS_ARN}",
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

build_cspe_payload() {
  cat <<EOF
{
  "schemaType": "JSON",
  "schema": ${SCHEMA_STR},
  "metadata": {
    "properties": {
      "version":     "1.0.0",
      "owner":       "demo-csfle-cspe-cloud",
      "description": "MortgageApplication — CSPE: payload encrypted (no field tags needed)",
      "encryption":  "cspe"
    }
  },
  "ruleSet": {
    "encodingRules": [
      {
        "name": "encryptPayload",
        "kind": "TRANSFORM",
        "type": "ENCRYPT_PAYLOAD",
        "mode": "WRITEREAD",
        "params": {
          "encrypt.kek.name":   "${CSPE_KEK_NAME}",
          "encrypt.kms.key.id": "${CSPE_KMS_ARN}",
          "encrypt.kms.type":   "aws-kms"
        },
        "onFailure": "ERROR,NONE"
      }
    ]
  }
}
EOF
}

register() {
  # register SUBJECT PAYLOAD
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

echo "── CSFLE schema (field-level, PII-tagged) ─────────────────────────────"
csfle_payload=$(build_csfle_payload)
register "${CSFLE_TOPIC}"        "$csfle_payload"
register "${CSFLE_TOPIC}-value"  "$csfle_payload"

echo ""
echo "── CSPE schema (payload, no tags needed) ──────────────────────────────"
cspe_payload=$(build_cspe_payload)
register "${CSPE_TOPIC}"         "$cspe_payload"
register "${CSPE_TOPIC}-value"   "$cspe_payload"

echo ""
echo "Schemas registered. Next: bash scripts/03_create_topics.sh"
