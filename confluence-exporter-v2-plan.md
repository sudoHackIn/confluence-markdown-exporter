# Confluence Exporter V2 Plan (Incremental + Parallel + Resume)

## Context

Current Stage 1 Confluence export is functional but slow and fragile for large corpora (e.g., 8k+ pages), especially with VPN/proxy/SSL constraints and long-running sessions.

Main operational pain points:

- long full runs with weak visibility
- partial stops (e.g., at 1700/8000 pages) with manual restarts
- expensive repeated traversal
- occasional pathological pages/macros producing huge outputs

This document proposes a V2 export strategy designed for daily incremental sync and safe resume.

## Goals

- fast daily re-run: process only changed pages
- reliable resume: continue from checkpoint after failure
- controlled parallelism without triggering API collapse
- per-page fault isolation (bad page does not stop whole run)
- deterministic run artifacts and metrics

## Non-Goals

- replacing Confluence APIs with scraping
- introducing hard real-time sync
- solving all document quality/macro conversion issues in V2

## Proposed Runtime Model

Pipeline stages:

1. `discover`: identify candidate page IDs (incremental scope)
2. `fetch`: load page payloads via bounded concurrency
3. `convert`: markdown conversion and guards
4. `write`: atomic write and state update
5. `finalize`: run summary, checkpoints, cleanup

Design rules:

- each page is processed independently
- errors are recorded per page and do not abort full run by default
- global rate limiter controls request pressure
- checkpoints are persisted after each stage transition

## State Store (SQLite)

Use SQLite instead of only JSON lockfiles for robust resume and idempotency.

Suggested schema:

- `runs(run_id, started_at, finished_at, status, from_ts, to_ts, processed, updated, failed)`
- `pages_state(page_id, space_key, version, last_modified, content_hash, status, last_success_at, last_error)`
- `queue_checkpoint(run_id, page_id, stage, attempt, updated_at)`
- `attachments_state(attachment_id, page_id, version, content_hash, status, last_success_at)` (optional in phase 1)

Status model for page lifecycle:

- `DISCOVERED -> FETCHED -> CONVERTED -> WRITTEN`
- terminal error states tracked with retry count

## Incremental Daily Sync

Primary strategy:

1. read last successful `to_ts`
2. discover changed pages since `from_ts`
3. process only changed pages
4. update `to_ts` on successful finalize

Fallback strategy:

- if timestamp-based query is unavailable/unreliable in environment, compare `version/lastModified` during discover and enqueue only changed items.

## Concurrency & Throttling

Start conservatively:

- fetch workers: `8`
- convert workers: `min(cpu, 6)`
- attachments workers: `3`
- global RPS: `5`

Adaptive behavior:

- on `429/503`: reduce concurrency by 20-30% and apply backoff
- on sustained success windows: cautiously increase toward configured caps

## Reliability Policy

- retry budget per page: `3-5`
- exponential backoff with jitter
- run-level timeout guard
- atomic write (`tmp` + rename) to avoid half-written files
- per-run manifest and failure list

## Proposed Changes In Current Project

## Phase 0 (Immediate, low risk)

Existing patches already prepared under:

- `tools/confluence-stage1/patches/confluence-markdown-exporter-3.2.0-heartbeat.patch`
- `tools/confluence-stage1/patches/confluence-markdown-exporter-3.2.0-table-guard.patch`

Keep applying these for operational stability until V2 runner is ready.

## Phase 1 (New orchestrator in repo, keep upstream exporter)

Add a local orchestrator script/service that:

- maintains SQLite state
- schedules page jobs
- invokes exporter per page/batch
- writes run manifests

Suggested files to add:

- `tools/confluence-stage1/scripts/export-confluence-v2.sh`
- `tools/confluence-stage1/scripts/run_incremental_sync.py`
- `tools/confluence-stage1/meta/state/export-state.db` (runtime artifact, ignored in git)
- `tools/confluence-stage1/config/v2.example.yaml`

`v2.example.yaml` keys:

- `mode: incremental | full | resume`
- `from_ts: auto | ISO timestamp`
- `max_fetch_workers`
- `max_convert_workers`
- `max_attachment_workers`
- `global_rps`
- `max_retries`
- `timeout_seconds`

## Phase 2 (Parallel fetch/convert)

Implement worker pools in `run_incremental_sync.py`:

- queue of page IDs from discover
- fetch workers with shared limiter
- convert/write workers with checkpoint updates
- failed-pages journal (`meta/import-logs/<run_id>.failed.tsv`)

## Phase 3 (Daily automation)

Run daily incremental sync (CI cron or scheduler) with:

- `mode=incremental`
- `from_ts=auto`
- report generation
- alert if `failed/pages > threshold`

## Patch Sketches (for current project integration)

Below are sketches (not production-ready full patches) for directionally integrating V2.

### 1) New V2 entrypoint in stage1 script

```diff
diff --git a/tools/confluence-stage1/scripts/export-confluence.sh b/tools/confluence-stage1/scripts/export-confluence.sh
@@
-#!/usr/bin/env bash
+#!/usr/bin/env bash
 set -euo pipefail
 
+if [[ "${CME_V2_ENABLED:-false}" == "true" ]]; then
+  exec "$(dirname "$0")/export-confluence-v2.sh" "$@"
+fi
+
 # existing v1 flow...
```

### 2) Add V2 script wrapper

```diff
diff --git a/tools/confluence-stage1/scripts/export-confluence-v2.sh b/tools/confluence-stage1/scripts/export-confluence-v2.sh
new file mode 100755
@@
+#!/usr/bin/env bash
+set -euo pipefail
+
+ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
+CONFIG_PATH="${V2_CONFIG_PATH:-$ROOT_DIR/tools/confluence-stage1/config/v2.yaml}"
+
+python3 "$ROOT_DIR/tools/confluence-stage1/scripts/run_incremental_sync.py" \
+  --config "$CONFIG_PATH"
```

### 3) Add state bootstrap in Python runner

```diff
diff --git a/tools/confluence-stage1/scripts/run_incremental_sync.py b/tools/confluence-stage1/scripts/run_incremental_sync.py
new file mode 100644
@@
+def ensure_schema(conn):
+    conn.executescript(SCHEMA_SQL)
+
+def next_run(conn, mode, from_ts):
+    # insert row into runs(status='running')
+    ...
```

## Operational Defaults (recommended)

- `skip_unchanged = true`
- heartbeat patch enabled
- table guard patch enabled
- `max_fetch_workers = 8`
- `global_rps = 5`
- `max_retries = 4`

Tune upward only after observing:

- stable latency
- low `429/503`
- no VPN/proxy instability

## Validation Checklist

After V2 rollout:

1. first full run succeeds and records state in SQLite
2. next day run processes only changed pages
3. interrupted run resumes without reprocessing completed pages
4. failed pages are listed in run report
5. output remains deterministic across repeated runs

## Migration Plan

1. keep V1 as default, ship V2 behind `CME_V2_ENABLED=true`
2. run V1 and V2 in shadow mode on same small scope
3. compare output parity and performance metrics
4. make V2 default after acceptance window

## Open Decisions

- whether to keep direct dependency on upstream `confluence-markdown-exporter` or gradually embed a dedicated in-repo exporter module
- whether attachments should be processed inline with page jobs or in dedicated post-pass
- final strategy for deleted-page cleanup under partial runs

