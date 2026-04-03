#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SNAPSHOT_PATH="${SNAPSHOT_PATH:-$REPO_ROOT/state/state-snapshot.json}"
STATE_DB_PATH="${STATE_DB_PATH:-$REPO_ROOT/.local/export-state.db}"

mkdir -p "$(dirname "$STATE_DB_PATH")"

echo "[state-import] snapshot=$SNAPSHOT_PATH -> db=$STATE_DB_PATH"
uv run cf-export state-import \
  --snapshot-path "$SNAPSHOT_PATH" \
  --db-path "$STATE_DB_PATH"
