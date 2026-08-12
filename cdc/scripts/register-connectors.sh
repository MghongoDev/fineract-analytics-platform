#!/usr/bin/env bash
# =====================================================================
# Register (or reconcile) the Debezium connectors against Kafka Connect.
#
# Idempotent by design: uses PUT /connectors/<name>/config, so running it
# on every `make up` converges the running connector to what is in git
# instead of erroring with 409 Conflict. Config drift between the file
# and the cluster is therefore impossible to accumulate.
# =====================================================================
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_DIR="${CONNECTOR_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../debezium" && pwd)}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"

log()  { printf '[cdc] %s\n' "$*"; }
fail() { printf '[cdc][ERROR] %s\n' "$*" >&2; exit 1; }

wait_for_connect() {
  log "waiting for Kafka Connect at ${CONNECT_URL} (timeout ${TIMEOUT_SECONDS}s)"
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  until curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || fail "Kafka Connect not ready after ${TIMEOUT_SECONDS}s"
    sleep 3
  done
  log "Kafka Connect is up: $(curl -fsS "${CONNECT_URL}/" | tr -d '\n')"
}

register() {
  local file="$1"
  local name config
  name=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['name'])" "$file")
  config=$(python3 -c "
import json,sys
cfg = json.load(open(sys.argv[1]))['config']
# strip '//' documentation keys - Kafka Connect rejects unknown properties
print(json.dumps({k: v for k, v in cfg.items() if not k.startswith('//')}))
" "$file")

  log "registering connector '${name}' from $(basename "$file")"
  local status
  status=$(curl -s -o /tmp/connect-response.json -w '%{http_code}' \
    -X PUT -H 'Content-Type: application/json' \
    --data "${config}" \
    "${CONNECT_URL}/connectors/${name}/config")

  if [[ "${status}" != "200" && "${status}" != "201" ]]; then
    cat /tmp/connect-response.json >&2
    fail "connector '${name}' registration failed with HTTP ${status}"
  fi
  log "connector '${name}' accepted (HTTP ${status})"
}

check_status() {
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    local bad
    bad=$(curl -fsS "${CONNECT_URL}/connectors?expand=status" | python3 -c "
import json,sys
data = json.load(sys.stdin)
bad = []
for name, payload in data.items():
    status = payload.get('status', {})
    states = [status.get('connector', {}).get('state')] + \
             [t.get('state') for t in status.get('tasks', [])]
    if any(s != 'RUNNING' for s in states if s):
        bad.append(f\"{name}={states}\")
print(';'.join(bad))
")
    if [[ -z "${bad}" ]]; then
      log "all connectors RUNNING"
      curl -fsS "${CONNECT_URL}/connectors?expand=status" | python3 -m json.tool | head -40
      return 0
    fi
    log "waiting for connectors to reach RUNNING: ${bad}"
    sleep 5
  done
  curl -fsS "${CONNECT_URL}/connectors?expand=status" | python3 -m json.tool >&2 || true
  fail "connectors did not reach RUNNING state"
}

main() {
  wait_for_connect
  shopt -s nullglob
  local files=("${CONNECTOR_DIR}"/*-connector.json)
  (( ${#files[@]} )) || fail "no *-connector.json files found in ${CONNECTOR_DIR}"
  for file in "${files[@]}"; do
    register "${file}"
  done
  check_status
}

main "$@"
