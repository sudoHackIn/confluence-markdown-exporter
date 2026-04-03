"""Unit tests for V2 sync orchestrator."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from confluence_markdown_exporter.v2_sync import PAGE_STAGE_DISCOVERED
from confluence_markdown_exporter.v2_sync import PAGE_STAGE_WRITTEN
from confluence_markdown_exporter.v2_sync import PageCandidate
from confluence_markdown_exporter.v2_sync import V2StateStore
from confluence_markdown_exporter.v2_sync import _build_discover_cql
from confluence_markdown_exporter.v2_sync import _filter_changed_candidates
from confluence_markdown_exporter.v2_sync import export_state_snapshot
from confluence_markdown_exporter.v2_sync import import_state_snapshot
from confluence_markdown_exporter.v2_sync import run_v2_sync


class TestDiscoverCql:
    """Test CQL builder for V2 discover."""

    def test_build_cql_without_filters(self) -> None:
        cql = _build_discover_cql(space_keys=[], from_ts=None)
        assert cql == "type=page"

    def test_build_cql_with_space_and_from_ts(self) -> None:
        cql = _build_discover_cql(
            space_keys=["ENG", "DOCS"],
            from_ts="2026-04-01T09:30:00+00:00",
        )
        assert "type=page" in cql
        assert 'space in ("ENG","DOCS")' in cql
        assert 'lastmodified >= "2026-04-01 09:30"' in cql


class TestStateStore:
    """Test SQLite state helpers."""

    def test_start_and_finalize_run(self, tmp_path: Path) -> None:
        db_path = tmp_path / "state.db"
        store = V2StateStore(db_path)

        run_id = store.start_run(
            mode="incremental",
            from_ts=None,
            to_ts="2026-04-03T00:00:00+00:00",
        )
        assert run_id.startswith("run-")

        store.finalize_run(run_id, status="success", processed=2, updated=2, failed=0)
        assert store.get_last_success_to_ts() == "2026-04-03T00:00:00+00:00"
        store.close()

    def test_resume_ids_excludes_written(self, tmp_path: Path) -> None:
        db_path = tmp_path / "state.db"
        store = V2StateStore(db_path)
        run_id = store.start_run(mode="resume", from_ts=None, to_ts="2026-04-03T00:00:00+00:00")

        candidate_written = PageCandidate("1", "ENG", 3, "2026-04-03T00:00:00+00:00")
        candidate_pending = PageCandidate("2", "ENG", 3, "2026-04-03T00:00:00+00:00")

        store.mark_discovered(run_id, candidate_written)
        store.mark_discovered(run_id, candidate_pending)
        store.mark_stage(run_id, "1", PAGE_STAGE_WRITTEN, attempt=1)

        resume_ids = store.get_resume_page_ids(run_id)
        assert resume_ids == ["2"]
        store.close()


class TestCandidateFiltering:
    """Test candidate diffing for incremental mode."""

    def test_incremental_filters_unchanged(self, tmp_path: Path) -> None:
        store = V2StateStore(tmp_path / "state.db")
        run_id = store.start_run(
            mode="incremental",
            from_ts=None,
            to_ts="2026-04-03T00:00:00+00:00",
        )

        unchanged = PageCandidate("1", "ENG", 1, "2026-04-03T00:00:00+00:00")
        changed = PageCandidate("2", "ENG", 2, "2026-04-03T00:00:00+00:00")

        store.mark_discovered(run_id, unchanged)
        store.mark_stage(run_id, unchanged.page_id, PAGE_STAGE_WRITTEN, 1)

        queue = _filter_changed_candidates("incremental", [unchanged, changed], store)
        assert [c.page_id for c in queue] == ["2"]
        store.close()


class TestRunV2Sync:
    """Test high-level orchestration with mocked processing."""

    def test_run_v2_sync_metrics(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(
            "confluence_markdown_exporter.v2_sync.get_settings",
            lambda: MagicMock(
                export=MagicMock(output_path=tmp_path / "out"),
                v2=MagicMock(
                    mode="incremental",
                    from_ts="auto",
                    state_db_path=tmp_path / "state.db",
                    artifacts_path=None,
                    max_fetch_workers=2,
                    max_convert_workers=2,
                    max_attachment_workers=2,
                    global_rps=5.0,
                    max_retries=2,
                    timeout_seconds=60,
                    space_keys=[],
                ),
            ),
        )
        monkeypatch.setattr(
            "confluence_markdown_exporter.v2_sync._discover_for_mode",
            lambda **_kwargs: [
                PageCandidate("100", "ENG", 1, "2026-04-03T00:00:00+00:00"),
                PageCandidate("200", "ENG", 1, "2026-04-03T00:00:00+00:00"),
            ],
        )
        monkeypatch.setattr(
            "confluence_markdown_exporter.v2_sync._run_pipeline",
            lambda **_kwargs: (
                2,
                1,
                1,
                [],
            ),
        )

        result = run_v2_sync()

        assert result.discovered == 2
        assert result.enqueued == 2
        assert result.processed == 2
        assert result.updated == 1
        assert result.failed == 1
        manifests = list((tmp_path / "out" / ".cme-v2" / "meta" / "run-manifests").glob("*.json"))
        failed_logs = list((tmp_path / "out" / ".cme-v2" / "meta" / "import-logs").glob("*.tsv"))
        assert len(manifests) == 1
        assert len(failed_logs) == 1

    def test_run_v2_sync_writes_artifacts_to_custom_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setattr(
            "confluence_markdown_exporter.v2_sync.get_settings",
            lambda: MagicMock(
                export=MagicMock(output_path=tmp_path / "out"),
                v2=MagicMock(
                    mode="incremental",
                    from_ts="auto",
                    state_db_path=tmp_path / "state.db",
                    artifacts_path=tmp_path / "artifacts",
                    max_fetch_workers=1,
                    max_convert_workers=1,
                    max_attachment_workers=1,
                    global_rps=5.0,
                    max_retries=1,
                    timeout_seconds=60,
                    space_keys=[],
                ),
            ),
        )
        monkeypatch.setattr(
            "confluence_markdown_exporter.v2_sync._discover_for_mode",
            lambda **_kwargs: [PageCandidate("100", "ENG", 1, "2026-04-03T00:00:00+00:00")],
        )
        monkeypatch.setattr(
            "confluence_markdown_exporter.v2_sync._run_pipeline",
            lambda **_kwargs: (1, 1, 0, []),
        )

        run_v2_sync()

        assert len(list((tmp_path / "artifacts" / "run-manifests").glob("*.json"))) == 1
        assert len(list((tmp_path / "artifacts" / "import-logs").glob("*.tsv"))) == 1


class TestStateSnapshot:
    """Test exporting/importing V2 SQLite state snapshots."""

    def test_export_and_import_snapshot_roundtrip(self, tmp_path: Path) -> None:
        src_db = tmp_path / "source.db"
        snapshot_path = tmp_path / "state-snapshot.json"
        dst_db = tmp_path / "target.db"

        src_store = V2StateStore(src_db)
        run_id = src_store.start_run("full", None, "2026-04-03T00:00:00+00:00")
        candidate = PageCandidate("100", "ENG", 2, "2026-04-03T00:00:00+00:00")
        src_store.mark_discovered(run_id, candidate)
        src_store.mark_stage(run_id, "100", PAGE_STAGE_DISCOVERED, 1)
        src_store.finalize_run(run_id, status="success", processed=1, updated=1, failed=0)
        src_store.close()

        payload = export_state_snapshot(db_path=src_db, snapshot_path=snapshot_path)
        assert snapshot_path.exists()
        assert payload["snapshot_version"] == 1
        assert len(payload["tables"]["runs"]) == 1

        counts = import_state_snapshot(db_path=dst_db, snapshot_path=snapshot_path)
        assert counts["runs"] == 1
        assert counts["pages_state"] == 1
        assert counts["queue_checkpoint"] == 1

        dst_store = V2StateStore(dst_db)
        assert dst_store.get_last_success_to_ts() == "2026-04-03T00:00:00+00:00"
        page_state = dst_store.get_pages_state(["100"])
        assert "100" in page_state
        dst_store.close()

    def test_export_snapshot_file_contains_tables(self, tmp_path: Path) -> None:
        db = tmp_path / "db.sqlite"
        snapshot_path = tmp_path / "snapshot.json"
        store = V2StateStore(db)
        run_id = store.start_run("incremental", None, "2026-04-03T00:00:00+00:00")
        store.finalize_run(run_id, status="failed", processed=0, updated=0, failed=1)
        store.close()

        export_state_snapshot(db_path=db, snapshot_path=snapshot_path)
        parsed = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert set(parsed["tables"].keys()) == {"runs", "pages_state", "queue_checkpoint"}
