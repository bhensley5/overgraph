from overgraph import OverGraph, OverGraphError


class TestSync:
    def test_sync(self, db):
        db.upsert_node(1, "a")
        db.sync()  # Should not raise


class TestFlush:
    def test_flush_empty(self, db):
        result = db.flush()
        assert result is None

    def test_flush_with_data(self, db):
        for i in range(10):
            db.upsert_node(1, f"n{i}")
        result = db.flush()
        assert result is not None
        assert result.node_count == 10
        assert result.edge_count == 0
        assert "SegmentInfo" in repr(result)


class TestCompact:
    def test_compact_no_segments(self, db):
        result = db.compact()
        assert result is None

    def test_compact_with_segments(self, db):
        # Create enough data to flush at least twice
        for i in range(50):
            db.upsert_node(1, f"n{i}")
        db.flush()
        for i in range(50, 100):
            db.upsert_node(1, f"n{i}")
        db.flush()
        result = db.compact()
        assert result is not None
        assert result.segments_merged == 2
        assert result.nodes_kept == 100
        assert result.duration_ms >= 0
        assert "CompactionStats" in repr(result)


class TestCompactWithProgress:
    def test_progress_callback(self, db):
        # Create enough data to flush at least twice
        for i in range(50):
            db.upsert_node(1, f"n{i}")
        db.flush()
        for i in range(50, 100):
            db.upsert_node(1, f"n{i}")
        db.flush()

        progress_calls = []

        def on_progress(p):
            progress_calls.append({
                "phase": p.phase,
                "records": p.records_processed,
                "total": p.total_records,
            })
            return True  # Continue

        result = db.compact_with_progress(on_progress)
        if result is not None:
            assert len(progress_calls) > 0
            assert result.segments_merged >= 2

    def test_cancel_via_callback(self, db):
        for i in range(50):
            db.upsert_node(1, f"n{i}")
        db.flush()
        for i in range(50, 100):
            db.upsert_node(1, f"n{i}")
        db.flush()

        def cancel_immediately(_p):
            return False  # Cancel

        # Cancelled compaction may return None or raise
        try:
            result = db.compact_with_progress(cancel_immediately)
            # If it returns, it's fine (cancelled early)
        except OverGraphError:
            pass  # CompactionCancelled is expected

    def test_callback_exception_reraises(self, db):
        """Callback exceptions should propagate, not be swallowed."""
        import pytest

        for i in range(50):
            db.upsert_node(1, f"n{i}")
        db.flush()
        for i in range(50, 100):
            db.upsert_node(1, f"n{i}")
        db.flush()

        def buggy_callback(_p):
            raise ValueError("oops")

        with pytest.raises(ValueError, match="oops"):
            db.compact_with_progress(buggy_callback)
