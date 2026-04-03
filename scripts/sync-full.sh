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

mkdir -p "$(dirname "$STATE_DB_PATH")" "$ARTIFACTS_PATH"

echo "[sync-full] space=$SPACE_KEY"
uv run cf-export sync \
  --mode full \
  --space-keys "$SPACE_KEY" \
  --state-db-path "$STATE_DB_PATH" \
  --artifacts-path "$ARTIFACTS_PATH"
