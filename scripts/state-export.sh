#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

STATE_DB_PATH="${STATE_DB_PATH:-$REPO_ROOT/.local/export-state.db}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-$REPO_ROOT/state/state-snapshot.json}"

mkdir -p "$(dirname "$STATE_DB_PATH")" "$(dirname "$SNAPSHOT_PATH")"

echo "[state-export] db=$STATE_DB_PATH -> snapshot=$SNAPSHOT_PATH"
uv run cf-export state-export \
  --db-path "$STATE_DB_PATH" \
  --snapshot-path "$SNAPSHOT_PATH"
