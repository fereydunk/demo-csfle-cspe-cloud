#!/usr/bin/env bash
# Preflight + launch the demo web UI on http://localhost:8893
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

# Source .env + AWS session if present (so subprocesses inherit the values)
[[ -f "${REPO_DIR}/.env" ]]                       && set -a && source "${REPO_DIR}/.env"                       && set +a
[[ -f "${REPO_DIR}/config/aws-session.env" ]]     && set -a && source "${REPO_DIR}/config/aws-session.env"     && set +a

# Preflight
command -v confluent >/dev/null || { echo "ERROR: confluent CLI not installed"  >&2; exit 1; }
command -v aws       >/dev/null || { echo "ERROR: aws CLI not installed (needed to create KMS keys)" >&2; exit 1; }
[[ -d ~/confluent-8.2.0 ]]      || { echo "ERROR: CP 8.2.0 not at ~/confluent-8.2.0 — install Confluent Platform" >&2; exit 1; }
/opt/homebrew/opt/openjdk@21/bin/java -version >/dev/null 2>&1 \
                                || { echo "ERROR: Java 21 not at /opt/homebrew/opt/openjdk@21" >&2; exit 1; }
confluent kafka cluster list >/dev/null 2>&1 \
                                || { echo "ERROR: confluent CLI not logged in — run 'confluent login --save'" >&2; exit 1; }
echo "✓ preflight ok"

# AWS creds: report what we have, but don't fail if missing — the wizard will
# accept paste-in credentials on the AWS card.
if [[ -n "${AWS_ACCESS_KEY_ID:-}" ]]; then
  echo "✓ AWS creds in env (${AWS_ACCESS_KEY_ID})"
elif [[ -f "${REPO_DIR}/config/aws-session.env" ]]; then
  echo "✓ AWS creds in config/aws-session.env"
elif [[ -f ~/.aws/credentials ]]; then
  echo "✓ AWS creds available (~/.aws/credentials)"
else
  echo "  AWS creds not set — paste them in the wizard's AWS card."
fi

# Kill anything holding port 8893 — covers both old wizards (matched by name)
# and any other process squatting on the port (matched by lsof). Belt-and-
# suspenders because pkill's name-pattern misses processes started with a
# different argv (e.g. `python3 -B web/server.py` from a different cwd).
PORT=8893
pkill -f "demo-csfle-cspe-cloud/web/server.py" 2>/dev/null && echo "  Stopped old web server (by name)." || true
HOLDERS=$(lsof -ti:${PORT} 2>/dev/null || true)
if [[ -n "${HOLDERS}" ]]; then
  echo "  Port ${PORT} still held by PID(s): ${HOLDERS} — sending SIGKILL"
  echo "${HOLDERS}" | xargs kill -9 2>/dev/null || true
fi
# Brief wait for the kernel to actually release the socket
for i in 1 2 3 4 5; do
  lsof -ti:${PORT} >/dev/null 2>&1 || break
  sleep 1
done
if lsof -ti:${PORT} >/dev/null 2>&1; then
  echo "ERROR: port ${PORT} still in use after 5s" >&2
  lsof -i:${PORT} >&2
  exit 1
fi

(sleep 2 && open "http://localhost:${PORT}") &
echo ""
echo "Starting web UI on http://localhost:${PORT} ..."
exec python3 "${REPO_DIR}/web/server.py"
