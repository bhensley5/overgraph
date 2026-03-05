import struct

import pytest
from overgraph import OverGraph, OverGraphError


class TestBatchUpsertNodes:
    def test_batch_upsert(self, db):
        ids = db.batch_upsert_nodes([
            {"type_id": 1, "key": "a"},
            {"type_id": 1, "key": "b"},
            {"type_id": 2, "key": "c"},
        ])
        assert len(ids) == 3
        assert len(set(ids)) == 3  # All unique

    def test_batch_upsert_with_props(self, db):
        ids = db.batch_upsert_nodes([
            {"type_id": 1, "key": "a", "props": {"name": "Alice"}},
            {"type_id": 1, "key": "b", "props": {"name": "Bob"}},
        ])
        assert db.get_node(ids[0]).props["name"] == "Alice"
        assert db.get_node(ids[1]).props["name"] == "Bob"

    def test_batch_upsert_with_weight(self, db):
        ids = db.batch_upsert_nodes([
            {"type_id": 1, "key": "a", "weight": 2.0},
        ])
        assert abs(db.get_node(ids[0]).weight - 2.0) < 0.01

    def test_batch_upsert_empty(self, db):
        ids = db.batch_upsert_nodes([])
        assert ids == []

    def test_batch_upsert_idempotent(self, db):
        ids1 = db.batch_upsert_nodes([
            {"type_id": 1, "key": "a"},
            {"type_id": 1, "key": "b"},
        ])
        ids2 = db.batch_upsert_nodes([
            {"type_id": 1, "key": "a"},
            {"type_id": 1, "key": "b"},
        ])
        assert ids1 == ids2

    def test_batch_upsert_missing_field(self, db):
        with pytest.raises(Exception):
            db.batch_upsert_nodes([{"type_id": 1}])  # missing 'key'


class TestBatchUpsertEdges:
    def test_batch_upsert(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        ids = db.batch_upsert_edges([
            {"from_id": n1, "to_id": n2, "type_id": 10},
            {"from_id": n2, "to_id": n3, "type_id": 10},
        ])
        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_batch_upsert_with_props(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        ids = db.batch_upsert_edges([
            {"from_id": n1, "to_id": n2, "type_id": 10, "props": {"rel": "friend"}},
        ])
        assert db.get_edge(ids[0]).props["rel"] == "friend"

    def test_batch_upsert_with_temporal(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        ids = db.batch_upsert_edges([
            {"from_id": n1, "to_id": n2, "type_id": 10, "valid_from": 100, "valid_to": 200},
        ])
        edge = db.get_edge(ids[0])
        assert edge.valid_from == 100
        assert edge.valid_to == 200

    def test_batch_upsert_empty(self, db):
        ids = db.batch_upsert_edges([])
        assert ids == []


class TestGetNodes:
    def test_get_multiple(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        results = db.get_nodes([n1, n2])
        assert len(results) == 2
        assert results[0].id == n1
        assert results[1].id == n2

    def test_get_with_missing(self, db):
        n1 = db.upsert_node(1, "a")
        results = db.get_nodes([n1, 999999])
        assert len(results) == 2
        assert results[0] is not None
        assert results[1] is None

    def test_get_empty(self, db):
        results = db.get_nodes([])
        assert results == []


class TestGetEdges:
    def test_get_multiple(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        e1 = db.upsert_edge(n1, n2, 10)
        e2 = db.upsert_edge(n2, n3, 10)
        results = db.get_edges([e1, e2])
        assert len(results) == 2
        assert results[0].id == e1
        assert results[1].id == e2

    def test_get_with_missing(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        e1 = db.upsert_edge(n1, n2, 10)
        results = db.get_edges([e1, 999999])
        assert results[0] is not None
        assert results[1] is None

    def test_get_empty(self, db):
        results = db.get_edges([])
        assert results == []


class TestGraphPatch:
    def test_upsert_nodes_only(self, db):
        result = db.graph_patch({
            "upsert_nodes": [
                {"type_id": 1, "key": "a"},
                {"type_id": 1, "key": "b"},
            ],
        })
        assert len(result.node_ids) == 2
        assert len(result.edge_ids) == 0

    def test_upsert_edges_only(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        result = db.graph_patch({
            "upsert_edges": [
                {"from_id": n1, "to_id": n2, "type_id": 10},
            ],
        })
        assert len(result.node_ids) == 0
        assert len(result.edge_ids) == 1

    def test_mixed_patch(self, db):
        result = db.graph_patch({
            "upsert_nodes": [
                {"type_id": 1, "key": "x"},
                {"type_id": 1, "key": "y"},
            ],
        })
        n1, n2 = result.node_ids
        result2 = db.graph_patch({
            "upsert_edges": [
                {"from_id": n1, "to_id": n2, "type_id": 10},
            ],
        })
        assert len(result2.edge_ids) == 1
        assert db.get_edge(result2.edge_ids[0]) is not None

    def test_delete_nodes(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.graph_patch({"delete_node_ids": [n1]})
        assert db.get_node(n1) is None
        assert db.get_node(n2) is not None

    def test_delete_edges(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        db.graph_patch({"delete_edge_ids": [eid]})
        assert db.get_edge(eid) is None

    def test_invalidate_edges(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        db.graph_patch({
            "invalidate_edges": [{"edge_id": eid, "valid_to": 5000}],
        })
        edge = db.get_edge(eid)
        assert edge.valid_to == 5000

    def test_empty_patch(self, db):
        result = db.graph_patch({})
        assert result.node_ids == []
        assert result.edge_ids == []

    def test_patch_result_repr(self, db):
        result = db.graph_patch({
            "upsert_nodes": [{"type_id": 1, "key": "a"}],
        })
        r = repr(result)
        assert "PatchResult" in r


# ============================================================
# Helper: pack binary batches using struct
# ============================================================

def pack_node_batch(nodes):
    """Pack a list of node dicts into the binary wire format.

    Format: [count:u32] per node: [type_id:u32][weight:f32][key_len:u16][key:utf8][props_len:u32][props:json]
    """
    import json
    buf = struct.pack("<I", len(nodes))
    for n in nodes:
        type_id = n.get("type_id", 0)
        weight = n.get("weight", 1.0)
        key = n.get("key", "").encode("utf-8")
        props_json = json.dumps(n.get("props", {})).encode("utf-8") if n.get("props") else b""
        buf += struct.pack("<IfH", type_id, weight, len(key))
        buf += key
        buf += struct.pack("<I", len(props_json))
        buf += props_json
    return buf


def pack_edge_batch(edges):
    """Pack a list of edge dicts into the binary wire format.

    Format: [count:u32] per edge: [from:u64][to:u64][type_id:u32][weight:f32]
            [valid_from:i64][valid_to:i64][props_len:u32][props:json]
    """
    import json
    buf = struct.pack("<I", len(edges))
    for e in edges:
        from_id = e["from_id"]
        to_id = e["to_id"]
        type_id = e.get("type_id", 0)
        weight = e.get("weight", 1.0)
        valid_from = e.get("valid_from", 0)
        valid_to = e.get("valid_to", 0)
        props_json = json.dumps(e.get("props", {})).encode("utf-8") if e.get("props") else b""
        buf += struct.pack("<QQIf", from_id, to_id, type_id, weight)
        buf += struct.pack("<qq", valid_from, valid_to)
        buf += struct.pack("<I", len(props_json))
        buf += props_json
    return buf


class TestBatchUpsertNodesBinary:
    def test_basic(self, db):
        buf = pack_node_batch([
            {"type_id": 1, "key": "a"},
            {"type_id": 1, "key": "b"},
        ])
        ids = db.batch_upsert_nodes_binary(buf)
        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_with_props_and_weight(self, db):
        buf = pack_node_batch([
            {"type_id": 1, "key": "check", "props": {"color": "red", "score": 42}, "weight": 0.8},
        ])
        ids = db.batch_upsert_nodes_binary(buf)
        n = db.get_node(ids[0])
        assert n.key == "check"
        assert n.props["color"] == "red"
        assert n.props["score"] == 42
        assert abs(n.weight - 0.8) < 0.01

    def test_empty(self, db):
        buf = pack_node_batch([])
        ids = db.batch_upsert_nodes_binary(buf)
        assert ids == []

    def test_dedup(self, db):
        buf = pack_node_batch([
            {"type_id": 1, "key": "dup", "props": {"v": 1}},
            {"type_id": 1, "key": "dup", "props": {"v": 2}},
        ])
        ids = db.batch_upsert_nodes_binary(buf)
        assert ids[0] == ids[1]
        n = db.get_node(ids[0])
        assert n.props["v"] == 2  # last write wins

    def test_truncated_buffer(self, db):
        buf = pack_node_batch([{"type_id": 1, "key": "a"}])
        with pytest.raises(ValueError, match="truncated"):
            db.batch_upsert_nodes_binary(buf[:5])

    def test_trailing_bytes(self, db):
        buf = pack_node_batch([{"type_id": 1, "key": "a"}])
        with pytest.raises(ValueError, match="trailing"):
            db.batch_upsert_nodes_binary(buf + b"\x00")


class TestBatchUpsertEdgesBinary:
    def test_basic(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        buf = pack_edge_batch([
            {"from_id": n1, "to_id": n2, "type_id": 10},
        ])
        ids = db.batch_upsert_edges_binary(buf)
        assert len(ids) == 1
        e = db.get_edge(ids[0])
        assert e.from_id == n1
        assert e.to_id == n2
        assert e.type_id == 10

    def test_with_props_and_weight(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        buf = pack_edge_batch([
            {"from_id": n1, "to_id": n2, "type_id": 10, "props": {"label": "knows"}, "weight": 2.5},
        ])
        ids = db.batch_upsert_edges_binary(buf)
        e = db.get_edge(ids[0])
        assert e.props["label"] == "knows"
        assert abs(e.weight - 2.5) < 0.01

    def test_empty(self, db):
        buf = pack_edge_batch([])
        ids = db.batch_upsert_edges_binary(buf)
        assert ids == []

    def test_truncated_buffer(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        buf = pack_edge_batch([{"from_id": n1, "to_id": n2, "type_id": 10}])
        with pytest.raises(ValueError, match="truncated"):
            db.batch_upsert_edges_binary(buf[:10])

    def test_trailing_bytes(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        buf = pack_edge_batch([{"from_id": n1, "to_id": n2, "type_id": 10}])
        with pytest.raises(ValueError, match="trailing"):
            db.batch_upsert_edges_binary(buf + b"\x00")
