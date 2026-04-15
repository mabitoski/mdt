#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${MMA_SERVER_ENV_FILE:-$SCRIPT_DIR/mma-server.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

RUN_ROOT="${MMA_SERVER_RUN_DIR:-$SCRIPT_DIR/runs}"
mkdir -p "$RUN_ROOT"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
payload_file="$RUN_ROOT/${stamp}-payload.json"
log_file="$RUN_ROOT/${stamp}-report.log"

{
  printf '[%s] starting mma-server-report\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "$SCRIPT_DIR/mma-server-report.sh" --output "$payload_file" "$@"
  printf '[%s] payload archived at %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$payload_file"
} 2>&1 | tee "$log_file"
