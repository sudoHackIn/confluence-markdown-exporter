#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "[bootstrap] Running uv sync in $REPO_ROOT"
uv sync
echo "[bootstrap] Done"
