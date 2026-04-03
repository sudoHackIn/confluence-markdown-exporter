#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SPACE_KEY="${1:-${SPACE_KEY:-}}"
if [[ -z "${SPACE_KEY}" ]]; then
  echo "Usage: $0 <SPACE_KEY>"
  exit 1
fi

STATE_DB_PATH="${STATE_DB_PATH:-$REPO_ROOT/.local/export-state.db}"
ARTIFACTS_PATH="${ARTIFACTS_PATH:-$REPO_ROOT/state}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-10800}"

mkdir -p "$(dirname "$STATE_DB_PATH")" "$ARTIFACTS_PATH"

echo "[sync-resume] space=$SPACE_KEY timeout_seconds=$TIMEOUT_SECONDS"
uv run cf-export sync \
  --mode resume \
  --space-keys "$SPACE_KEY" \
  --timeout-seconds "$TIMEOUT_SECONDS" \
  --state-db-path "$STATE_DB_PATH" \
  --artifacts-path "$ARTIFACTS_PATH"
