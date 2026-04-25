import time
import os
import pytest
from overgraph import OverGraph


class TestDegree:
    def test_outgoing(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.upsert_edge(a, c, 20, weight=3.0)
        assert db.degree(a) == 2
        assert db.degree(b) == 0

    def test_incoming(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 20)
        assert db.degree(a, direction="incoming") == 0
        assert db.degree(b, direction="incoming") == 1

    def test_both(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 20)
        assert db.degree(a, direction="both") == 2
        assert db.degree(b, direction="both") == 1

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 20)
        assert db.degree(a, direction="outgoing", type_filter=[10]) == 1
        assert db.degree(a, direction="outgoing", type_filter=[20]) == 1
        assert db.degree(a, direction="outgoing", type_filter=[10, 20]) == 2
        assert db.degree(a, direction="outgoing", type_filter=[99]) == 0

    def test_nonexistent_node(self, db):
        assert db.degree(999999) == 0

    def test_matches_neighbors_length(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.upsert_edge(a, c, 20, weight=3.0)
        db.upsert_edge(b, c, 10, weight=1.0)
        for direction in ["outgoing", "incoming", "both"]:
            for nid in [a, b, c]:
                deg = db.degree(nid, direction=direction)
                nbrs = db.neighbors(nid, direction=direction)
                assert deg == len(nbrs), (
                    f"mismatch node={nid} dir={direction}: degree={deg} neighbors={len(nbrs)}"
                )


class TestSumEdgeWeights:
    def test_sum(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.upsert_edge(a, c, 10, weight=3.0)
        assert abs(db.sum_edge_weights(a) - 5.0) < 1e-6

    def test_zero_for_no_edges(self, db):
        assert db.sum_edge_weights(999999) == 0.0

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.upsert_edge(a, c, 20, weight=5.0)
        assert abs(db.sum_edge_weights(a, type_filter=[10]) - 2.0) < 1e-6
        assert abs(db.sum_edge_weights(a, type_filter=[20]) - 5.0) < 1e-6

    def test_at_epoch(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10, weight=3.0, valid_from=100, valid_to=200)
        assert abs(db.sum_edge_weights(a, at_epoch=150) - 3.0) < 1e-6
        assert db.sum_edge_weights(a, at_epoch=250) == 0.0


class TestAvgEdgeWeight:
    def test_avg(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.upsert_edge(a, c, 10, weight=4.0)
        avg = db.avg_edge_weight(a)
        assert avg is not None
        assert abs(avg - 3.0) < 1e-6

    def test_none_for_no_edges(self, db):
        assert db.avg_edge_weight(999999) is None

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.upsert_edge(a, c, 20, weight=6.0)
        avg = db.avg_edge_weight(a, type_filter=[10])
        assert avg is not None
        assert abs(avg - 2.0) < 1e-6

    def test_at_epoch(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10, weight=4.0, valid_from=100, valid_to=200)
        avg = db.avg_edge_weight(a, at_epoch=150)
        assert avg is not None
        assert abs(avg - 4.0) < 1e-6
        assert db.avg_edge_weight(a, at_epoch=250) is None


class TestDegreesBatch:
    def test_batch(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 10)
        db.upsert_edge(b, c, 10)
        result = db.degrees([a, b, c])
        assert isinstance(result, dict)
        assert result[a] == 2
        assert result[b] == 1
        assert result.get(c, 0) == 0

    def test_empty_input(self, db):
        result = db.degrees([])
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 20)
        result = db.degrees([a], type_filter=[10])
        assert result[a] == 1

    def test_at_epoch(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10, valid_from=100, valid_to=200)
        result = db.degrees([a], at_epoch=150)
        assert result[a] == 1
        result2 = db.degrees([a], at_epoch=250)
        assert result2.get(a, 0) == 0


class TestDegreeSidecars:
    def test_flush_compact_reopen_preserves_degree_family_results(self, db_path):
        db = OverGraph.open(db_path)
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=2.0)
        db.flush()
        db.upsert_edge(a, c, 10, weight=4.0)
        db.flush()
        db.compact()

        assert db.degree(a) == 2
        assert abs(db.sum_edge_weights(a) - 6.0) < 1e-6
        assert abs(db.avg_edge_weight(a) - 3.0) < 1e-6
        assert db.degrees([a, b, c])[a] == 2
        db.close()

        reopened = OverGraph.open(db_path)
        assert reopened.degree(a) == 2
        assert abs(reopened.sum_edge_weights(a) - 6.0) < 1e-6
        assert abs(reopened.avg_edge_weight(a) - 3.0) < 1e-6
        assert reopened.degrees([a, b, c])[a] == 2
        reopened.close()

    def test_corrupt_degree_sidecar_falls_back(self, db_path):
        db = OverGraph.open(db_path)
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10, weight=5.0)
        db.flush()
        db.close()

        segments_dir = os.path.join(db_path, "segments")
        sidecars = [
            os.path.join(segments_dir, name, "degree_delta.dat")
            for name in os.listdir(segments_dir)
            if name.startswith("seg_")
        ]
        assert len(sidecars) == 1
        with open(sidecars[0], "wb") as f:
            f.write(b"not a degree sidecar")

        reopened = OverGraph.open(db_path)
        assert reopened.degree(a) == 1
        assert abs(reopened.sum_edge_weights(a) - 5.0) < 1e-6
        assert abs(reopened.avg_edge_weight(a) - 5.0) < 1e-6
        assert reopened.degrees([a, b])[a] == 1
        assert len(reopened.neighbors(a)) == 1
        reopened.close()


class TestDegreeTemporal:
    def test_ignores_expired_edge(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        now = int(time.time() * 1000)
        db.upsert_edge(a, b, 10, valid_from=now - 2000, valid_to=now - 1000)
        assert db.degree(a) == 0

    def test_ignores_future_edge(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        now = int(time.time() * 1000)
        db.upsert_edge(a, b, 10, valid_from=now + 10000)
        assert db.degree(a) == 0

    def test_at_epoch(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10, valid_from=100, valid_to=200)
        assert db.degree(a, at_epoch=150) == 1
        assert db.degree(a, at_epoch=250) == 0
        assert db.degree(a, at_epoch=50) == 0
