"""V2 sync orchestrator: incremental, parallel, checkpointed exports backed by SQLite."""

from __future__ import annotations

import json
import logging
import queue
import secrets
import sqlite3
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

from tqdm import tqdm

from confluence_markdown_exporter.confluence import Page
from confluence_markdown_exporter.confluence import confluence
from confluence_markdown_exporter.utils.app_data_store import get_settings

logger = logging.getLogger(__name__)
system_random = secrets.SystemRandom()

PAGE_STAGE_DISCOVERED = "DISCOVERED"
PAGE_STAGE_FETCHED = "FETCHED"
PAGE_STAGE_CONVERTED = "CONVERTED"
PAGE_STAGE_WRITTEN = "WRITTEN"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    from_ts TEXT,
    to_ts TEXT,
    processed INTEGER NOT NULL DEFAULT 0,
    updated INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pages_state (
    page_id TEXT PRIMARY KEY,
    space_key TEXT,
    version INTEGER,
    last_modified TEXT,
    content_hash TEXT,
    status TEXT,
    last_success_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS queue_checkpoint (
    run_id TEXT NOT NULL,
    page_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, page_id)
);

CREATE INDEX IF NOT EXISTS idx_runs_status_started
    ON runs(status, started_at);
CREATE INDEX IF NOT EXISTS idx_queue_run_stage
    ON queue_checkpoint(run_id, stage);
"""


@dataclass(frozen=True)
class PageCandidate:
    """Metadata discovered for a Confluence page candidate."""

    page_id: str
    space_key: str
    version: int
    last_modified: str


@dataclass(frozen=True)
class SyncResult:
    """Run summary returned by V2 orchestrator."""

    run_id: str
    mode: str
    discovered: int
    enqueued: int
    processed: int
    updated: int
    failed: int
    from_ts: str | None
    to_ts: str


@dataclass(frozen=True)
class FetchedPage:
    """Page payload produced by fetch stage."""

    candidate: PageCandidate
    page: Page


@dataclass(frozen=True)
class ConvertedPage:
    """Markdown payload produced by convert stage."""

    candidate: PageCandidate
    page: Page
    markdown: str


@dataclass(frozen=True)
class PageResult:
    """Per-page processing result emitted by worker stages."""

    page_id: str
    success: bool
    stage: str
    attempt: int
    error: str | None = None


class PipelineStats:
    """Thread-safe counters for V2 staged pipeline progress."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fetched = 0
        self._converted = 0
        self._written = 0
        self._failed = 0

    def inc_fetched(self) -> None:
        with self._lock:
            self._fetched += 1

    def inc_converted(self) -> None:
        with self._lock:
            self._converted += 1

    def inc_written(self) -> None:
        with self._lock:
            self._written += 1

    def inc_failed(self) -> None:
        with self._lock:
            self._failed += 1

    def snapshot(self) -> tuple[int, int, int, int]:
        with self._lock:
            return (self._fetched, self._converted, self._written, self._failed)


class RateLimiter:
    """Simple process-local rate limiter based on minimum request spacing."""

    def __init__(self, requests_per_second: float) -> None:
        safe_rps = requests_per_second if requests_per_second > 0 else 1.0
        self._interval = 1.0 / safe_rps
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait_for_slot(self) -> None:
        """Block until the next request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed:
                    self._next_allowed = now + self._interval
                    return
                wait_seconds = self._next_allowed - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)


class V2StateStore:
    """SQLite-backed state store for V2 sync runs."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self.ensure_schema()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()

    def ensure_schema(self) -> None:
        """Create schema if it does not exist."""
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def start_run(self, mode: str, from_ts: str | None, to_ts: str) -> str:
        """Insert a running row and return generated run_id."""
        run_id = (
            f"run-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
            f"-{uuid.uuid4().hex[:8]}"
        )
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO runs(run_id, started_at, status, from_ts, to_ts)
                VALUES (?, ?, 'running', ?, ?)
                """,
                (run_id, _utc_now_iso(), from_ts, to_ts),
            )
            self._conn.commit()
        logger.info("Started V2 run %s (mode=%s)", run_id, mode)
        return run_id

    def finalize_run(
        self,
        run_id: str,
        *,
        status: str,
        processed: int,
        updated: int,
        failed: int,
    ) -> None:
        """Finalize a run row."""
        with self._lock:
            self._conn.execute(
                """
                UPDATE runs
                SET finished_at = ?, status = ?, processed = ?, updated = ?, failed = ?
                WHERE run_id = ?
                """,
                (_utc_now_iso(), status, processed, updated, failed, run_id),
            )
            self._conn.commit()

    def get_last_success_to_ts(self) -> str | None:
        """Return ``to_ts`` from the latest successful run if present."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT to_ts
                FROM runs
                WHERE status = 'success'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return None
        return str(row["to_ts"]) if row["to_ts"] else None

    def get_latest_incomplete_run_id(self) -> str | None:
        """Return latest run_id in running/failed state."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT run_id
                FROM runs
                WHERE status IN ('running', 'failed')
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return None
        return str(row["run_id"])

    def get_resume_page_ids(self, run_id: str) -> list[str]:
        """Return page ids that did not reach WRITTEN stage for a run."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT page_id
                FROM queue_checkpoint
                WHERE run_id = ?
                  AND stage != ?
                ORDER BY updated_at ASC
                """,
                (run_id, PAGE_STAGE_WRITTEN),
            ).fetchall()
        return [str(r["page_id"]) for r in rows]

    def get_pages_state(self, page_ids: list[str]) -> dict[str, sqlite3.Row]:
        """Return pages_state rows keyed by page_id for the provided IDs."""
        if not page_ids:
            return {}
        placeholders = ",".join(["?"] * len(page_ids))
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM pages_state WHERE page_id IN ({placeholders})",  # noqa: S608
                page_ids,
            ).fetchall()
        return {str(r["page_id"]): r for r in rows}

    def mark_discovered(self, run_id: str, candidate: PageCandidate) -> None:
        """Persist discovered candidate metadata and queue checkpoint."""
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO pages_state(page_id, space_key, version, last_modified, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(page_id) DO UPDATE SET
                  space_key = excluded.space_key,
                  version = excluded.version,
                  last_modified = excluded.last_modified,
                  status = excluded.status,
                  last_error = NULL
                """,
                (
                    candidate.page_id,
                    candidate.space_key,
                    candidate.version,
                    candidate.last_modified,
                    PAGE_STAGE_DISCOVERED,
                ),
            )
            self._conn.execute(
                """
                INSERT INTO queue_checkpoint(run_id, page_id, stage, attempt, updated_at)
                VALUES (?, ?, ?, 0, ?)
                ON CONFLICT(run_id, page_id) DO UPDATE SET
                  stage = excluded.stage,
                  attempt = 0,
                  updated_at = excluded.updated_at
                """,
                (run_id, candidate.page_id, PAGE_STAGE_DISCOVERED, now),
            )
            self._conn.commit()

    def mark_stage(self, run_id: str, page_id: str, stage: str, attempt: int) -> None:
        """Update page lifecycle stage both in queue and page state."""
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_checkpoint
                SET stage = ?, attempt = ?, updated_at = ?
                WHERE run_id = ? AND page_id = ?
                """,
                (stage, attempt, now, run_id, page_id),
            )
            self._conn.execute(
                """
                UPDATE pages_state
                SET status = ?, last_error = NULL
                WHERE page_id = ?
                """,
                (stage, page_id),
            )
            self._conn.commit()

    def mark_success(self, page_id: str) -> None:
        """Mark page as written successfully."""
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE pages_state
                SET status = ?, last_success_at = ?, last_error = NULL
                WHERE page_id = ?
                """,
                (PAGE_STAGE_WRITTEN, now, page_id),
            )
            self._conn.commit()

    def mark_failure(self, run_id: str, page_id: str, attempt: int, error: str) -> None:
        """Record terminal failure for page in queue/page state."""
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_checkpoint
                SET stage = 'FAILED', attempt = ?, updated_at = ?
                WHERE run_id = ? AND page_id = ?
                """,
                (attempt, now, run_id, page_id),
            )
            self._conn.execute(
                """
                UPDATE pages_state
                SET status = 'FAILED', last_error = ?
                WHERE page_id = ?
                """,
                (error[:4000], page_id),
            )
            self._conn.commit()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_timestamp_input(value: str | None) -> str | None:
    if not value or value == "auto":
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"Invalid ISO timestamp for from_ts: {value}"
        raise ValueError(msg) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _build_discover_cql(space_keys: list[str], from_ts: str | None) -> str:
    clauses = ["type=page"]
    if space_keys:
        escaped = ",".join(f'"{space_key}"' for space_key in space_keys)
        clauses.append(f"space in ({escaped})")
    if from_ts:
        dt = datetime.fromisoformat(from_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        clauses.append(f'lastmodified >= "{dt.strftime("%Y-%m-%d %H:%M")}"')
    return " AND ".join(clauses)


def _discover_pages(space_keys: list[str], from_ts: str | None) -> list[PageCandidate]:
    """Discover page candidates via CQL search with pagination."""
    cql = _build_discover_cql(space_keys=space_keys, from_ts=from_ts)
    logger.info(
        "Discover started: cql=%s",
        cql,
    )
    params: dict[str, Any] = {
        "cql": cql,
        "expand": "version,space",
        "limit": 250,
    }

    request_count = 0
    results: list[dict[str, Any]] = []
    response = confluence.get("rest/api/content/search", params=params)
    request_count += 1
    if response:
        results.extend(response.get("results", []))
        next_path = response.get("_links", {}).get("next")
        logger.info(
            "Discover progress: requests=%d pages=%d",
            request_count,
            len(results),
        )
    else:
        next_path = None

    while next_path:
        response = confluence.get(next_path)
        request_count += 1
        if not response:
            break
        results.extend(response.get("results", []))
        next_path = response.get("_links", {}).get("next")
        logger.info(
            "Discover progress: requests=%d pages=%d",
            request_count,
            len(results),
        )

    candidates: list[PageCandidate] = []
    for item in results:
        page_id = str(item.get("id", "")).strip()
        if not page_id:
            continue
        space_key = str(item.get("space", {}).get("key", ""))
        version_info = item.get("version", {})
        version = int(version_info.get("number", 0) or 0)
        last_modified = str(version_info.get("when", ""))
        candidates.append(
            PageCandidate(
                page_id=page_id,
                space_key=space_key,
                version=version,
                last_modified=last_modified,
            )
        )
    logger.info(
        "Discover completed: requests=%d pages=%d",
        request_count,
        len(candidates),
    )
    return candidates


def _filter_changed_candidates(
    mode: str,
    candidates: list[PageCandidate],
    store: V2StateStore,
) -> list[PageCandidate]:
    if mode == "full":
        return candidates

    current_state = store.get_pages_state([candidate.page_id for candidate in candidates])
    changed: list[PageCandidate] = []
    for candidate in candidates:
        row = current_state.get(candidate.page_id)
        if row is None:
            changed.append(candidate)
            continue
        if int(row["version"] or 0) != candidate.version:
            changed.append(candidate)
            continue
        if str(row["last_modified"] or "") != candidate.last_modified:
            changed.append(candidate)
            continue
        if str(row["status"] or "") != PAGE_STAGE_WRITTEN:
            changed.append(candidate)
    return changed


def _compute_backoff_seconds(attempt: int) -> float:
    base = min(60.0, 2 ** (attempt - 1))
    jitter = system_random.uniform(0.0, 0.3) * base
    return base + jitter


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
        ) as fd:
            tmp_path = Path(fd.name)
            fd.write(content)
        tmp_path.replace(path)
    except BaseException:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise


def _mark_page_failed(
    *,
    run_id: str,
    candidate: PageCandidate,
    stage: str,
    attempt: int,
    error: str,
    store: V2StateStore,
    results_queue: queue.Queue[PageResult],
    stats: PipelineStats,
) -> None:
    store.mark_failure(run_id, candidate.page_id, attempt, f"{stage}: {error}")
    stats.inc_failed()
    logger.warning(
        "Page %s failed at %s after %d attempts: %s",
        candidate.page_id,
        stage,
        attempt,
        error,
    )
    results_queue.put(
        item=PageResult(
            page_id=candidate.page_id,
            success=False,
            stage=stage,
            attempt=attempt,
            error=error,
        )
    )


def _fetch_worker(
    *,
    input_queue: queue.Queue[PageCandidate | None],
    output_queue: queue.Queue[FetchedPage],
    results_queue: queue.Queue[PageResult],
    run_id: str,
    store: V2StateStore,
    limiter: RateLimiter,
    max_retries: int,
    stats: PipelineStats,
) -> None:
    while True:
        candidate = input_queue.get()
        try:
            if candidate is None:
                return

            last_error = "unknown error"
            for attempt in range(1, max_retries + 1):
                try:
                    limiter.wait_for_slot()
                    page = Page.from_id(int(candidate.page_id))
                    if page.title == "Page not accessible":
                        last_error = f"Page {candidate.page_id} not accessible"
                        if attempt >= max_retries:
                            _mark_page_failed(
                                run_id=run_id,
                                candidate=candidate,
                                stage=PAGE_STAGE_FETCHED,
                                attempt=attempt,
                                error=last_error,
                                store=store,
                                results_queue=results_queue,
                                stats=stats,
                            )
                            break
                        time.sleep(_compute_backoff_seconds(attempt))
                        continue
                    store.mark_stage(run_id, candidate.page_id, PAGE_STAGE_FETCHED, attempt)
                    stats.inc_fetched()
                    output_queue.put(FetchedPage(candidate=candidate, page=page))
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    if attempt >= max_retries:
                        _mark_page_failed(
                            run_id=run_id,
                            candidate=candidate,
                            stage=PAGE_STAGE_FETCHED,
                            attempt=attempt,
                            error=last_error,
                            store=store,
                            results_queue=results_queue,
                            stats=stats,
                        )
                        break
                    time.sleep(_compute_backoff_seconds(attempt))
        finally:
            input_queue.task_done()


def _convert_worker(
    *,
    input_queue: queue.Queue[FetchedPage | None],
    output_queue: queue.Queue[ConvertedPage],
    results_queue: queue.Queue[PageResult],
    run_id: str,
    store: V2StateStore,
    max_retries: int,
    stats: PipelineStats,
) -> None:
    while True:
        fetched = input_queue.get()
        try:
            if fetched is None:
                return

            candidate = fetched.candidate
            last_error = "unknown error"
            for attempt in range(1, max_retries + 1):
                try:
                    markdown = fetched.page.markdown
                    store.mark_stage(run_id, candidate.page_id, PAGE_STAGE_CONVERTED, attempt)
                    stats.inc_converted()
                    output_queue.put(
                        ConvertedPage(candidate=candidate, page=fetched.page, markdown=markdown)
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    if attempt >= max_retries:
                        _mark_page_failed(
                            run_id=run_id,
                            candidate=candidate,
                            stage=PAGE_STAGE_CONVERTED,
                            attempt=attempt,
                            error=last_error,
                            store=store,
                            results_queue=results_queue,
                            stats=stats,
                        )
                        break
                    time.sleep(_compute_backoff_seconds(attempt))
        finally:
            input_queue.task_done()


def _write_worker(
    *,
    input_queue: queue.Queue[ConvertedPage | None],
    results_queue: queue.Queue[PageResult],
    run_id: str,
    store: V2StateStore,
    export_root: Path,
    max_retries: int,
    stats: PipelineStats,
) -> None:
    while True:
        converted = input_queue.get()
        try:
            if converted is None:
                return

            candidate = converted.candidate
            last_error = "unknown error"
            for attempt in range(1, max_retries + 1):
                try:
                    converted.page.export_attachments()
                    _atomic_write(export_root / converted.page.export_path, converted.markdown)
                    store.mark_stage(run_id, candidate.page_id, PAGE_STAGE_WRITTEN, attempt)
                    store.mark_success(candidate.page_id)
                    stats.inc_written()
                    results_queue.put(
                        item=PageResult(
                            page_id=candidate.page_id,
                            success=True,
                            stage=PAGE_STAGE_WRITTEN,
                            attempt=attempt,
                        )
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    if attempt >= max_retries:
                        _mark_page_failed(
                            run_id=run_id,
                            candidate=candidate,
                            stage=PAGE_STAGE_WRITTEN,
                            attempt=attempt,
                            error=last_error,
                            store=store,
                            results_queue=results_queue,
                            stats=stats,
                        )
                        break
                    time.sleep(_compute_backoff_seconds(attempt))
        finally:
            input_queue.task_done()


def _collect_results(
    *,
    results_queue: queue.Queue[PageResult],
    expected: int,
    timeout_seconds: int,
    stats: PipelineStats,
) -> tuple[int, int, int, list[PageResult]]:
    start = time.monotonic()
    processed = 0
    updated = 0
    failed = 0
    failures: list[PageResult] = []

    with tqdm(total=expected, smoothing=0.05, desc="V2 sync") as pbar:
        while processed < expected:
            elapsed = time.monotonic() - start
            remaining = timeout_seconds - elapsed
            if remaining <= 0:
                msg = f"V2 sync timed out after {timeout_seconds} seconds"
                raise TimeoutError(msg)

            result = results_queue.get(timeout=min(1.0, remaining))
            processed += 1
            if result.success:
                updated += 1
            else:
                failed += 1
                failures.append(result)

            fetched, converted, written, failed_stage = stats.snapshot()
            pbar.set_postfix_str(
                f"fetched={fetched} converted={converted} written={written} failed={failed_stage}"
            )
            pbar.update(1)

    return processed, updated, failed, failures


def _run_pipeline(
    *,
    queue_items: list[PageCandidate],
    run_id: str,
    store: V2StateStore,
    limiter: RateLimiter,
    max_retries: int,
    export_root: Path,
    max_fetch_workers: int,
    max_convert_workers: int,
    max_write_workers: int,
    timeout_seconds: int,
) -> tuple[int, int, int, list[PageResult]]:
    fetch_input: queue.Queue[PageCandidate | None] = queue.Queue()
    fetched_queue: queue.Queue[FetchedPage | None] = queue.Queue()
    converted_queue: queue.Queue[ConvertedPage | None] = queue.Queue()
    results_queue: queue.Queue[PageResult] = queue.Queue()
    stats = PipelineStats()

    fetch_workers = max(1, max_fetch_workers)
    convert_workers = max(1, max_convert_workers)
    write_workers = max(1, max_write_workers)

    fetch_threads = [
        threading.Thread(
            target=_fetch_worker,
            kwargs={
                "input_queue": fetch_input,
                "output_queue": fetched_queue,
                "results_queue": results_queue,
                "run_id": run_id,
                "store": store,
                "limiter": limiter,
                "max_retries": max_retries,
                "stats": stats,
            },
            daemon=True,
        )
        for _ in range(fetch_workers)
    ]
    convert_threads = [
        threading.Thread(
            target=_convert_worker,
            kwargs={
                "input_queue": fetched_queue,
                "output_queue": converted_queue,
                "results_queue": results_queue,
                "run_id": run_id,
                "store": store,
                "max_retries": max_retries,
                "stats": stats,
            },
            daemon=True,
        )
        for _ in range(convert_workers)
    ]
    write_threads = [
        threading.Thread(
            target=_write_worker,
            kwargs={
                "input_queue": converted_queue,
                "results_queue": results_queue,
                "run_id": run_id,
                "store": store,
                "export_root": export_root,
                "max_retries": max_retries,
                "stats": stats,
            },
            daemon=True,
        )
        for _ in range(write_workers)
    ]

    for thread in [*fetch_threads, *convert_threads, *write_threads]:
        thread.start()

    for candidate in queue_items:
        fetch_input.put(candidate)
    for _ in range(fetch_workers):
        fetch_input.put(None)

    processed, updated, failed, failures = _collect_results(
        results_queue=results_queue,
        expected=len(queue_items),
        timeout_seconds=timeout_seconds,
        stats=stats,
    )

    fetch_input.join()
    for _ in range(convert_workers):
        fetched_queue.put(None)
    fetched_queue.join()
    for _ in range(write_workers):
        converted_queue.put(None)
    converted_queue.join()

    for thread in [*fetch_threads, *convert_threads, *write_threads]:
        thread.join(timeout=1.0)

    return processed, updated, failed, failures


def _artifacts_root(export_root: Path) -> Path:
    return export_root / ".cme-v2" / "meta"


def _write_failed_tsv(
    *,
    export_root: Path,
    run_id: str,
    failures: list[PageResult],
) -> Path:
    logs_dir = _artifacts_root(export_root) / "import-logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    failed_tsv_path = logs_dir / f"{run_id}.failed.tsv"

    rows = sorted(failures, key=lambda item: (item.page_id, item.stage, item.attempt))
    lines = ["page_id\tstage\tattempt\terror"]
    for item in rows:
        error = (item.error or "").replace("\t", " ").replace("\n", " ").strip()
        lines.append(f"{item.page_id}\t{item.stage}\t{item.attempt}\t{error}")
    _atomic_write(failed_tsv_path, "\n".join(lines) + "\n")
    return failed_tsv_path


def _write_manifest(
    *,
    export_root: Path,
    run_id: str,
    manifest: dict[str, Any],
) -> Path:
    manifest_dir = _artifacts_root(export_root) / "run-manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{run_id}.manifest.json"
    manifest_content = json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True)
    _atomic_write(manifest_path, manifest_content + "\n")
    return manifest_path


def _resolve_from_ts(mode: str, requested_from_ts: str, store: V2StateStore) -> str | None:
    if mode == "full":
        return None
    if requested_from_ts != "auto":
        return _parse_timestamp_input(requested_from_ts)
    return store.get_last_success_to_ts()


def _discover_for_mode(
    *,
    mode: str,
    from_ts: str | None,
    space_keys: list[str],
    store: V2StateStore,
) -> list[PageCandidate]:
    discovered: list[PageCandidate]
    if mode == "resume":
        previous_run_id = store.get_latest_incomplete_run_id()
        if previous_run_id:
            resume_page_ids = set(store.get_resume_page_ids(previous_run_id))
            if resume_page_ids:
                all_candidates = _discover_pages(space_keys=space_keys, from_ts=None)
                discovered = [c for c in all_candidates if c.page_id in resume_page_ids]
                if discovered:
                    logger.info(
                        "Resume mode: continuing %d pages from run %s",
                        len(discovered),
                        previous_run_id,
                    )
                    return discovered

    try:
        discovered = _discover_pages(space_keys=space_keys, from_ts=from_ts)
    except Exception:
        if from_ts is None:
            raise
        logger.warning(
            "Incremental discover with from_ts=%s failed. Falling back to full-scope discover.",
            from_ts,
        )
        discovered = _discover_pages(space_keys=space_keys, from_ts=None)
    return discovered


def run_v2_sync(
    *,
    mode: str | None = None,
    from_ts: str | None = None,
    space_keys: list[str] | None = None,
    state_db_path: Path | None = None,
    max_fetch_workers: int | None = None,
    max_convert_workers: int | None = None,
    max_attachment_workers: int | None = None,
    global_rps: float | None = None,
    max_retries: int | None = None,
    timeout_seconds: int | None = None,
) -> SyncResult:
    """Execute a V2 sync run and return aggregate metrics."""
    settings = get_settings()
    v2_config = settings.v2

    resolved_mode = mode or v2_config.mode
    resolved_space_keys = space_keys if space_keys is not None else v2_config.space_keys
    db_path = state_db_path or v2_config.state_db_path
    resolved_global_rps = global_rps if global_rps is not None else v2_config.global_rps
    resolved_max_retries = max_retries if max_retries is not None else v2_config.max_retries
    resolved_timeout_seconds = (
        timeout_seconds if timeout_seconds is not None else v2_config.timeout_seconds
    )
    effective_fetch = (
        max_fetch_workers if max_fetch_workers is not None else v2_config.max_fetch_workers
    )
    effective_convert = (
        max_convert_workers if max_convert_workers is not None else v2_config.max_convert_workers
    )
    effective_write = (
        max_attachment_workers
        if max_attachment_workers is not None
        else v2_config.max_attachment_workers
    )

    requested_from_ts = from_ts if from_ts is not None else v2_config.from_ts

    store = V2StateStore(db_path)
    started_at = _utc_now_iso()
    to_ts = started_at
    resolved_from_ts = _resolve_from_ts(resolved_mode, requested_from_ts, store)
    run_id = store.start_run(mode=resolved_mode, from_ts=resolved_from_ts, to_ts=to_ts)
    started_monotonic = time.monotonic()

    processed = 0
    updated = 0
    failed = 0
    failures: list[PageResult] = []
    run_status = "failed"

    try:
        logger.info(
            "V2 run %s: stage=discover mode=%s from_ts=%s space_keys=%s",
            run_id,
            resolved_mode,
            resolved_from_ts or "none",
            ",".join(resolved_space_keys) if resolved_space_keys else "all",
        )
        discovered = _discover_for_mode(
            mode=resolved_mode,
            from_ts=resolved_from_ts,
            space_keys=resolved_space_keys,
            store=store,
        )

        queue = _filter_changed_candidates(mode=resolved_mode, candidates=discovered, store=store)
        logger.info(
            "V2 run %s: discovered=%d enqueued=%d",
            run_id,
            len(discovered),
            len(queue),
        )
        for candidate in queue:
            store.mark_discovered(run_id, candidate)

        limiter = RateLimiter(resolved_global_rps)
        logger.info(
            "V2 run %s: stage=pipeline fetch_workers=%d convert_workers=%d write_workers=%d "
            "global_rps=%.2f max_retries=%d timeout=%ds",
            run_id,
            effective_fetch,
            effective_convert,
            effective_write,
            resolved_global_rps,
            resolved_max_retries,
            resolved_timeout_seconds,
        )
        processed, updated, failed, failures = _run_pipeline(
            queue_items=queue,
            run_id=run_id,
            store=store,
            limiter=limiter,
            max_retries=resolved_max_retries,
            export_root=settings.export.output_path,
            max_fetch_workers=effective_fetch,
            max_convert_workers=effective_convert,
            max_write_workers=effective_write,
            timeout_seconds=resolved_timeout_seconds,
        )

        run_status = "success" if failed == 0 else "failed"
        logger.info(
            "V2 run %s: completed status=%s processed=%d updated=%d failed=%d",
            run_id,
            run_status,
            processed,
            updated,
            failed,
        )
        store.finalize_run(
            run_id,
            status=run_status,
            processed=processed,
            updated=updated,
            failed=failed,
        )

        failed_tsv_path = _write_failed_tsv(
            export_root=settings.export.output_path,
            run_id=run_id,
            failures=failures,
        )
        _write_manifest(
            export_root=settings.export.output_path,
            run_id=run_id,
            manifest={
                "run_id": run_id,
                "status": run_status,
                "mode": resolved_mode,
                "from_ts": resolved_from_ts,
                "to_ts": to_ts,
                "started_at": started_at,
                "duration_seconds": round(time.monotonic() - started_monotonic, 3),
                "counts": {
                    "discovered": len(discovered),
                    "enqueued": len(queue),
                    "processed": processed,
                    "updated": updated,
                    "failed": failed,
                },
                "failed_tsv": str(failed_tsv_path),
                "failed_pages": [
                    {
                        "page_id": item.page_id,
                        "stage": item.stage,
                        "attempt": item.attempt,
                        "error": item.error,
                    }
                    for item in sorted(
                        failures,
                        key=lambda item: (item.page_id, item.stage, item.attempt),
                    )
                ],
                "config": {
                    "state_db_path": str(db_path),
                    "space_keys": resolved_space_keys,
                    "max_fetch_workers": effective_fetch,
                    "max_convert_workers": effective_convert,
                    "max_attachment_workers": effective_write,
                    "global_rps": resolved_global_rps,
                    "max_retries": resolved_max_retries,
                    "timeout_seconds": resolved_timeout_seconds,
                },
            },
        )

        return SyncResult(
            run_id=run_id,
            mode=resolved_mode,
            discovered=len(discovered),
            enqueued=len(queue),
            processed=processed,
            updated=updated,
            failed=failed,
            from_ts=resolved_from_ts,
            to_ts=to_ts,
        )
    except Exception:
        store.finalize_run(
            run_id,
            status="failed",
            processed=processed,
            updated=updated,
            failed=max(1, failed),
        )
        _write_failed_tsv(
            export_root=settings.export.output_path,
            run_id=run_id,
            failures=failures,
        )
        _write_manifest(
            export_root=settings.export.output_path,
            run_id=run_id,
            manifest={
                "run_id": run_id,
                "status": "failed",
                "mode": resolved_mode,
                "from_ts": resolved_from_ts,
                "to_ts": to_ts,
                "started_at": started_at,
                "duration_seconds": round(time.monotonic() - started_monotonic, 3),
                "counts": {
                    "processed": processed,
                    "updated": updated,
                    "failed": max(1, failed),
                },
                "failed_pages": [
                    {
                        "page_id": item.page_id,
                        "stage": item.stage,
                        "attempt": item.attempt,
                        "error": item.error,
                    }
                    for item in sorted(
                        failures,
                        key=lambda item: (item.page_id, item.stage, item.attempt),
                    )
                ],
            },
        )
        raise
    finally:
        store.close()
