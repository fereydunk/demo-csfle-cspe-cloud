#!/usr/bin/env bash
# Discover the active Confluent Cloud env / cluster / SR via the `confluent`
# CLI session and write IDs to .env. Reuses any already-set values; only
# overwrites empty ones. Mints fresh API keys if KAFKA_API_KEY / SR_API_KEY
# are missing.
#
# Re-runnable: idempotent — rerunning with a fully-populated .env is a no-op.
#
# Prereq: `confluent login --save` must have been run.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || cp "${REPO_DIR}/.env.example" "$ENV_FILE"

source "$ENV_FILE"

upsert() {
  # upsert KEY VALUE — replace if present, append if absent
  local k="$1" v="$2"
  if grep -qE "^${k}=" "$ENV_FILE"; then
    # Use a different delimiter (|) since values can contain /
    sed -i '' "s|^${k}=.*|${k}=${v}|" "$ENV_FILE"
  else
    echo "${k}=${v}" >> "$ENV_FILE"
  fi
}

# ── Environment ──────────────────────────────────────────────────────────────
if [[ -z "${ENV_ID:-}" ]]; then
  envs_json=$(confluent environment list -o json)
  count=$(echo "$envs_json" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
  if [[ "$count" -eq 0 ]]; then
    echo "ERROR: no environments found — create one in CC first." >&2; exit 1
  elif [[ "$count" -eq 1 ]]; then
    ENV_ID=$(echo "$envs_json" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
    ENV_NAME=$(echo "$envs_json" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])")
  else
    echo "Multiple environments — pick one by setting ENV_ID in .env then re-run:" >&2
    echo "$envs_json" | python3 -c "import sys,json; [print(f'  {e[\"id\"]}  {e[\"name\"]}') for e in json.load(sys.stdin)]" >&2
    exit 1
  fi
  upsert ENV_ID   "$ENV_ID"
  upsert ENV_NAME "$ENV_NAME"
  echo "✓ environment ${ENV_ID} (${ENV_NAME})"
else
  echo "✓ environment ${ENV_ID} (already set)"
fi

# ── Kafka cluster ────────────────────────────────────────────────────────────
if [[ -z "${CLUSTER_ID:-}" ]]; then
  clusters_json=$(confluent kafka cluster list --environment "$ENV_ID" -o json)
  count=$(echo "$clusters_json" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
  if [[ "$count" -eq 0 ]]; then
    echo "ERROR: no Kafka clusters in env ${ENV_ID}" >&2; exit 1
  elif [[ "$count" -eq 1 ]]; then
    CLUSTER_ID=$(echo "$clusters_json"   | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
    CLUSTER_NAME=$(echo "$clusters_json" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['name'])")
  else
    echo "Multiple clusters in env ${ENV_ID} — pick one by setting CLUSTER_ID in .env then re-run:" >&2
    echo "$clusters_json" | python3 -c "import sys,json; [print(f'  {c[\"id\"]}  {c[\"name\"]} ({c.get(\"region\",\"?\")}/{c.get(\"cloud\",\"?\")})') for c in json.load(sys.stdin)]" >&2
    exit 1
  fi
  upsert CLUSTER_ID   "$CLUSTER_ID"
  upsert CLUSTER_NAME "$CLUSTER_NAME"
  # Describe to get the bootstrap endpoint
  desc=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json)
  BOOTSTRAP_SERVERS=$(echo "$desc" | python3 -c "import sys,json; ep=json.load(sys.stdin).get('endpoint',''); print(ep.replace('SASL_SSL://',''))")
  upsert BOOTSTRAP_SERVERS "$BOOTSTRAP_SERVERS"
  echo "✓ cluster ${CLUSTER_ID} (${CLUSTER_NAME}) → ${BOOTSTRAP_SERVERS}"
else
  echo "✓ cluster ${CLUSTER_ID} (already set)"
fi

# ── Schema Registry ──────────────────────────────────────────────────────────
if [[ -z "${SR_ID:-}" ]]; then
  sr_json=$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json 2>&1)
  if echo "$sr_json" | grep -q "not found\|not enabled"; then
    echo "ERROR: Schema Registry not enabled for env ${ENV_ID} — enable via Cloud UI" >&2
    echo "       (Stream Governance → Enable). The CLI no longer supports SR enable." >&2
    exit 1
  fi
  # Recent CLI returns the cluster id under 'cluster'; older versions used 'id'.
  SR_ID=$(echo "$sr_json"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('cluster') or d.get('id',''))")
  SR_URL=$(echo "$sr_json" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('endpoint_url') or d.get('endpoint',''))")
  upsert SR_ID  "$SR_ID"
  upsert SR_URL "$SR_URL"
  echo "✓ schema registry ${SR_ID} → ${SR_URL}"
else
  echo "✓ schema registry ${SR_ID} (already set)"
fi

# ── API keys ─────────────────────────────────────────────────────────────────
if [[ -z "${KAFKA_API_KEY:-}" ]]; then
  echo "→ minting Kafka API key for ${CLUSTER_ID} ..."
  out=$(confluent api-key create --resource "$CLUSTER_ID" --environment "$ENV_ID" --description "demo-csfle-cspe-cloud" -o json)
  KAFKA_API_KEY=$(echo "$out"    | python3 -c "import sys,json; print(json.load(sys.stdin)['api_key'])")
  KAFKA_API_SECRET=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin)['api_secret'])")
  upsert KAFKA_API_KEY    "$KAFKA_API_KEY"
  upsert KAFKA_API_SECRET "$KAFKA_API_SECRET"
  echo "  ✓ ${KAFKA_API_KEY}"
fi

if [[ -z "${SR_API_KEY:-}" ]]; then
  echo "→ minting SR API key for ${SR_ID} ..."
  out=$(confluent api-key create --resource "$SR_ID" --environment "$ENV_ID" --description "demo-csfle-cspe-cloud-sr" -o json)
  SR_API_KEY=$(echo "$out"    | python3 -c "import sys,json; print(json.load(sys.stdin)['api_key'])")
  SR_API_SECRET=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin)['api_secret'])")
  upsert SR_API_KEY    "$SR_API_KEY"
  upsert SR_API_SECRET "$SR_API_SECRET"
  echo "  ✓ ${SR_API_KEY}"
fi

echo ""
echo "Discovery complete. Next: bash scripts/01_setup_keks.sh"
