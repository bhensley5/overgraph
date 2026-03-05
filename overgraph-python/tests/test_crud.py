import pytest
from overgraph import OverGraph, OverGraphError


class TestUpsertNode:
    def test_upsert_returns_id(self, db):
        nid = db.upsert_node(1, "alice")
        assert isinstance(nid, int)
        assert nid > 0

    def test_upsert_same_key_returns_same_id(self, db):
        id1 = db.upsert_node(1, "alice")
        id2 = db.upsert_node(1, "alice")
        assert id1 == id2

    def test_upsert_different_keys(self, db):
        id1 = db.upsert_node(1, "alice")
        id2 = db.upsert_node(1, "bob")
        assert id1 != id2

    def test_upsert_with_props(self, db):
        nid = db.upsert_node(1, "alice", props={"age": 30, "name": "Alice"})
        node = db.get_node(nid)
        assert node.props["age"] == 30
        assert node.props["name"] == "Alice"

    def test_upsert_with_weight(self, db):
        nid = db.upsert_node(1, "alice", weight=2.5)
        node = db.get_node(nid)
        assert abs(node.weight - 2.5) < 0.01

    def test_upsert_with_nested_props(self, db):
        nid = db.upsert_node(1, "alice", props={
            "tags": ["a", "b"],
            "meta": {"nested": True, "count": 42},
        })
        node = db.get_node(nid)
        assert node.props["tags"] == ["a", "b"]
        assert node.props["meta"]["nested"] is True
        assert node.props["meta"]["count"] == 42

    def test_upsert_updates_props(self, db):
        nid = db.upsert_node(1, "alice", props={"v": 1})
        db.upsert_node(1, "alice", props={"v": 2})
        node = db.get_node(nid)
        assert node.props["v"] == 2


class TestGetNode:
    def test_get_existing(self, db):
        nid = db.upsert_node(1, "alice")
        node = db.get_node(nid)
        assert node is not None
        assert node.id == nid
        assert node.type_id == 1
        assert node.key == "alice"
        assert node.created_at > 0
        assert node.updated_at > 0

    def test_get_nonexistent(self, db):
        node = db.get_node(999999)
        assert node is None

    def test_node_repr(self, db):
        nid = db.upsert_node(5, "bob")
        node = db.get_node(nid)
        r = repr(node)
        assert "NodeRecord" in r
        assert "bob" in r


class TestGetNodeByKey:
    def test_get_by_key(self, db):
        nid = db.upsert_node(1, "alice")
        node = db.get_node_by_key(1, "alice")
        assert node is not None
        assert node.id == nid

    def test_get_by_key_wrong_type(self, db):
        db.upsert_node(1, "alice")
        node = db.get_node_by_key(2, "alice")
        assert node is None

    def test_get_by_key_nonexistent(self, db):
        node = db.get_node_by_key(1, "nobody")
        assert node is None


class TestDeleteNode:
    def test_delete_node(self, db):
        nid = db.upsert_node(1, "alice")
        db.delete_node(nid)
        assert db.get_node(nid) is None

    def test_delete_nonexistent(self, db):
        # Should not raise
        db.delete_node(999999)


class TestUpsertEdge:
    def test_upsert_returns_id(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        assert isinstance(eid, int)
        assert eid > 0

    def test_upsert_with_props(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, props={"rel": "friend"})
        edge = db.get_edge(eid)
        assert edge.props["rel"] == "friend"

    def test_upsert_with_weight(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, weight=3.14)
        edge = db.get_edge(eid)
        assert abs(edge.weight - 3.14) < 0.01

    def test_upsert_with_temporal(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, valid_from=1000, valid_to=2000)
        edge = db.get_edge(eid)
        assert edge.valid_from == 1000
        assert edge.valid_to == 2000


class TestGetEdge:
    def test_get_existing(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        edge = db.get_edge(eid)
        assert edge is not None
        assert edge.id == eid
        assert edge.from_id == n1
        assert edge.to_id == n2
        assert edge.type_id == 10

    def test_get_nonexistent(self, db):
        assert db.get_edge(999999) is None

    def test_edge_repr(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        edge = db.get_edge(eid)
        r = repr(edge)
        assert "EdgeRecord" in r


class TestGetEdgeByTriple:
    def test_get_by_triple(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        edge = db.get_edge_by_triple(n1, n2, 10)
        assert edge is not None
        assert edge.id == eid

    def test_get_by_triple_wrong_type(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.upsert_edge(n1, n2, 10)
        assert db.get_edge_by_triple(n1, n2, 99) is None


class TestDeleteEdge:
    def test_delete_edge(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        db.delete_edge(eid)
        assert db.get_edge(eid) is None


class TestInvalidateEdge:
    def test_invalidate(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        result = db.invalidate_edge(eid, 5000)
        assert result is not None
        assert result.valid_to == 5000

    def test_invalidate_nonexistent(self, db):
        result = db.invalidate_edge(999999, 5000)
        assert result is None


class TestPropertyTypes:
    """Test all supported property types roundtrip through Python -> Rust -> Python."""

    def test_string(self, db):
        nid = db.upsert_node(1, "k", props={"s": "hello"})
        assert db.get_node(nid).props["s"] == "hello"

    def test_int(self, db):
        nid = db.upsert_node(1, "k", props={"i": 42})
        assert db.get_node(nid).props["i"] == 42

    def test_float(self, db):
        nid = db.upsert_node(1, "k", props={"f": 3.14})
        assert abs(db.get_node(nid).props["f"] - 3.14) < 0.001

    def test_bool(self, db):
        nid = db.upsert_node(1, "k", props={"b": True, "c": False})
        p = db.get_node(nid).props
        assert p["b"] is True
        assert p["c"] is False

    def test_null(self, db):
        nid = db.upsert_node(1, "k", props={"n": None})
        assert db.get_node(nid).props["n"] is None

    def test_bytes(self, db):
        nid = db.upsert_node(1, "k", props={"data": b"\x00\x01\x02"})
        assert db.get_node(nid).props["data"] == b"\x00\x01\x02"

    def test_array(self, db):
        nid = db.upsert_node(1, "k", props={"arr": [1, "two", 3.0]})
        p = db.get_node(nid).props
        assert p["arr"][0] == 1
        assert p["arr"][1] == "two"
        assert abs(p["arr"][2] - 3.0) < 0.001

    def test_nested_map(self, db):
        nid = db.upsert_node(1, "k", props={"m": {"a": 1, "b": {"c": 2}}})
        p = db.get_node(nid).props
        assert p["m"]["a"] == 1
        assert p["m"]["b"]["c"] == 2

    def test_empty_props(self, db):
        nid = db.upsert_node(1, "k", props={})
        assert db.get_node(nid).props == {}

    def test_no_props(self, db):
        nid = db.upsert_node(1, "k")
        assert db.get_node(nid).props == {}
