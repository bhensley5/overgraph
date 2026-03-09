import time
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
        assert db.degree(a, "incoming") == 0
        assert db.degree(b, "incoming") == 1

    def test_both(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 20)
        assert db.degree(a, "both") == 2
        assert db.degree(b, "both") == 1

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(a, c, 20)
        assert db.degree(a, "outgoing", type_filter=[10]) == 1
        assert db.degree(a, "outgoing", type_filter=[20]) == 1
        assert db.degree(a, "outgoing", type_filter=[10, 20]) == 2
        assert db.degree(a, "outgoing", type_filter=[99]) == 0

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
                deg = db.degree(nid, direction)
                nbrs = db.neighbors(nid, direction)
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
