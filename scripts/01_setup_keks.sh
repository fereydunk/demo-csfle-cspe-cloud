#!/usr/bin/env bash
# Set up the two KEKs end-to-end:
#   1. Create CSFLE KEK in AWS KMS (alias: ${CSFLE_KEK_NAME})
#   2. Create CSPE  KEK in AWS KMS (alias: ${CSPE_KEK_NAME})
#   3. Register both in SR's DEK Registry under their aliases
#
# Idempotent: if a KMS alias already resolves to a key, reuses that ARN; if SR
# already has a KEK with that name (HTTP 409), treats it as success.
#
# Prereqs: AWS credentials in env (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY +
# optional AWS_SESSION_TOKEN, plus AWS_REGION). The web wizard writes these to
# config/aws-session.env and the Makefile/startup.sh source it before running.
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
command -v aws >/dev/null     || { echo "ERROR: aws CLI not installed"  >&2; exit 1; }

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
  # Check if alias already resolves to a key
  local existing
  existing=$(aws kms describe-key --key-id "$alias" --region "$AWS_REGION" --output json 2>/dev/null \
              | python3 -c "import sys,json; print(json.load(sys.stdin)['KeyMetadata']['Arn'])" 2>/dev/null || true)
  if [[ -n "$existing" ]]; then
    echo "$existing"
    return 0
  fi
  # Create a new symmetric ENCRYPT_DECRYPT key
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
  # register_kek_in_sr KEK_NAME KMS_ARN DESCRIPTION
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

echo "── CSFLE KEK ──────────────────────────────────────────────────────────"
echo "→ AWS KMS: alias/${CSFLE_KEK_NAME} in ${AWS_REGION} ..."
csfle_arn=$(ensure_kms_key "$CSFLE_KEK_NAME" "demo-csfle-cspe-cloud — CSFLE field-level KEK")
upsert CSFLE_KMS_ARN "$csfle_arn"
echo "  ✓ ${csfle_arn}"
echo "→ SR DEK Registry: ${CSFLE_KEK_NAME} ..."
register_kek_in_sr "$CSFLE_KEK_NAME" "$csfle_arn" "CSFLE field-level KEK for ${CSFLE_TOPIC}"

echo ""
echo "── CSPE KEK ───────────────────────────────────────────────────────────"
echo "→ AWS KMS: alias/${CSPE_KEK_NAME} in ${AWS_REGION} ..."
cspe_arn=$(ensure_kms_key "$CSPE_KEK_NAME" "demo-csfle-cspe-cloud — CSPE payload KEK")
upsert CSPE_KMS_ARN "$cspe_arn"
echo "  ✓ ${cspe_arn}"
echo "→ SR DEK Registry: ${CSPE_KEK_NAME} ..."
register_kek_in_sr "$CSPE_KEK_NAME" "$cspe_arn" "CSPE payload KEK for ${CSPE_TOPIC}"

echo ""
echo "Both KEKs ready. Next: bash scripts/02_register_schemas.sh"
