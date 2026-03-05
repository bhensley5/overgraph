import os

import pytest
from overgraph import OverGraph, OverGraphError


class TestOpen:
    def test_open_creates_directory(self, db_path):
        db = OverGraph.open(db_path)
        assert os.path.isdir(db_path)
        db.close()

    def test_open_with_options(self, db_path):
        db = OverGraph.open(
            db_path,
            create_if_missing=True,
            edge_uniqueness=True,
            memtable_flush_threshold=500,
        )
        db.close()

    def test_open_group_commit_mode(self, db_path):
        db = OverGraph.open(
            db_path,
            wal_sync_mode="group_commit",
            group_commit_interval_ms=5,
        )
        s = db.stats()
        assert "group" in s.wal_sync_mode.lower()
        db.close()

    def test_open_immediate_mode(self, db_path):
        db = OverGraph.open(db_path, wal_sync_mode="immediate")
        s = db.stats()
        assert "immediate" in s.wal_sync_mode.lower() or "Immediate" in s.wal_sync_mode
        db.close()

    def test_open_invalid_sync_mode(self, db_path):
        with pytest.raises(ValueError, match="Invalid wal_sync_mode"):
            OverGraph.open(db_path, wal_sync_mode="banana")

    def test_open_unknown_kwarg(self, db_path):
        with pytest.raises(ValueError, match="Unknown option"):
            OverGraph.open(db_path, create_if_mising=True)  # typo

    def test_open_creates_nested_dirs(self, tmp_dir):
        nested_path = os.path.join(tmp_dir, "deep", "nested", "db")
        db = OverGraph.open(nested_path)
        assert os.path.isdir(nested_path)
        db.close()


class TestClose:
    def test_close(self, db_path):
        db = OverGraph.open(db_path)
        db.close()

    def test_close_force(self, db_path):
        db = OverGraph.open(db_path)
        db.close(force=True)

    def test_double_close_is_safe(self, db_path):
        db = OverGraph.open(db_path)
        db.close()
        db.close()  # Should not raise

    def test_operations_after_close_raise(self, db_path):
        db = OverGraph.open(db_path)
        db.close()
        with pytest.raises(OverGraphError):
            db.upsert_node(1, "k")


class TestContextManager:
    def test_context_manager_basic(self, db_path):
        with OverGraph.open(db_path) as db:
            nid = db.upsert_node(1, "hello")
            assert nid > 0

    def test_context_manager_closes_on_exit(self, db_path):
        db = OverGraph.open(db_path)
        with db:
            db.upsert_node(1, "hello")
        with pytest.raises(OverGraphError):
            db.upsert_node(1, "world")

    def test_context_manager_closes_on_exception(self, db_path):
        db = OverGraph.open(db_path)
        try:
            with db:
                db.upsert_node(1, "hello")
                raise ValueError("boom")
        except ValueError:
            pass
        with pytest.raises(OverGraphError):
            db.upsert_node(1, "world")


class TestStats:
    def test_stats_initial(self, db):
        s = db.stats()
        assert s.segment_count >= 0
        assert s.pending_wal_bytes >= 0
        assert s.node_tombstone_count == 0
        assert s.edge_tombstone_count == 0

    def test_stats_repr(self, db):
        s = db.stats()
        r = repr(s)
        assert "DbStats" in r

    def test_stats_after_writes(self, db):
        db.upsert_node(1, "a")
        db.upsert_node(1, "b")
        s = db.stats()
        assert s.pending_wal_bytes > 0
