#!/usr/bin/env bash
# Create the two topics in Confluent Cloud (idempotent — `--if-not-exists`).
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh first" >&2; exit 1; }
source "$ENV_FILE"

[[ -n "${ENV_ID:-}"     ]] || { echo "ERROR: ENV_ID not set"     >&2; exit 1; }
[[ -n "${CLUSTER_ID:-}" ]] || { echo "ERROR: CLUSTER_ID not set" >&2; exit 1; }

create_topic() {
  local topic="$1"
  echo "→ ${topic} ..."
  if confluent kafka topic create "$topic" \
       --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
       --partitions 3 --if-not-exists 2>&1 | tee /tmp/topic_create.log; then
    echo "  ✓ ready"
  else
    # `--if-not-exists` returns 0 even when the topic exists, so a non-zero
    # exit is a real failure (auth, quota, etc.) — surface and abort.
    echo "  ✗ topic create failed"
    exit 1
  fi
}

create_topic "$CSFLE_TOPIC"
create_topic "$CSPE_TOPIC"

echo ""
echo "Both topics ready. Setup complete!"
echo ""
echo "  Produce:    make produce-csfle  /  make produce-cspe"
echo "  Consume:    make consume-csfle-auth  /  consume-csfle-unauth"
echo "              make consume-cspe-auth   /  consume-cspe-unauth"
echo "  Web UI:     bash startup.sh   →   http://localhost:8893"
