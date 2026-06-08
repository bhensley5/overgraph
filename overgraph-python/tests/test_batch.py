import struct

import pytest
from overgraph import OverGraph, OverGraphError


class TestBatchUpsertNodes:
    def test_batch_upsert(self, db):
        ids = db.batch_upsert_nodes([
            {"labels": ["Person"], "key": "a"},
            {"labels": ["Person"], "key": "b"},
            {"labels": ["Company"], "key": "c"},
        ])
        assert len(ids) == 3
        assert len(set(ids)) == 3  # All unique

    def test_batch_upsert_with_props(self, db):
        ids = db.batch_upsert_nodes([
            {"labels": ["Person"], "key": "a", "props": {"name": "Alice"}},
            {"labels": ["Person"], "key": "b", "props": {"name": "Bob"}},
        ])
        assert db.get_node(ids[0]).props["name"] == "Alice"
        assert db.get_node(ids[1]).props["name"] == "Bob"

    def test_batch_upsert_with_weight(self, db):
        ids = db.batch_upsert_nodes([
            {"labels": ["Person"], "key": "a", "weight": 2.0},
        ])
        assert abs(db.get_node(ids[0]).weight - 2.0) < 0.01

    def test_batch_upsert_empty(self, db):
        ids = db.batch_upsert_nodes([])
        assert ids == []

    def test_batch_upsert_idempotent(self, db):
        ids1 = db.batch_upsert_nodes([
            {"labels": ["Person"], "key": "a"},
            {"labels": ["Person"], "key": "b"},
        ])
        ids2 = db.batch_upsert_nodes([
            {"labels": ["Person"], "key": "a"},
            {"labels": ["Person"], "key": "b"},
        ])
        assert ids1 == ids2

    def test_batch_upsert_missing_field(self, db):
        with pytest.raises(Exception):
            db.batch_upsert_nodes([{"labels": ["Person"]}])  # missing 'key'

class TestBatchUpsertEdges:
    def test_batch_upsert(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        n3 = db.upsert_node("Person", "c")
        ids = db.batch_upsert_edges([
            {"from_id": n1, "to_id": n2, "label": "RELATES_TO"},
            {"from_id": n2, "to_id": n3, "label": "RELATES_TO"},
        ])
        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_batch_upsert_with_props(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        ids = db.batch_upsert_edges([
            {"from_id": n1, "to_id": n2, "label": "RELATES_TO", "props": {"rel": "friend"}},
        ])
        assert db.get_edge(ids[0]).props["rel"] == "friend"

    def test_batch_upsert_with_temporal(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        ids = db.batch_upsert_edges([
            {"from_id": n1, "to_id": n2, "label": "RELATES_TO", "valid_from": 100, "valid_to": 200},
        ])
        edge = db.get_edge(ids[0])
        assert edge.valid_from == 100
        assert edge.valid_to == 200

    def test_batch_upsert_empty(self, db):
        ids = db.batch_upsert_edges([])
        assert ids == []

class TestGetNodes:
    def test_get_multiple(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        results = db.get_nodes([n1, n2])
        assert len(results) == 2
        assert results[0].id == n1
        assert results[1].id == n2

    def test_get_with_missing(self, db):
        n1 = db.upsert_node("Person", "a")
        results = db.get_nodes([n1, 999999])
        assert len(results) == 2
        assert results[0] is not None
        assert results[1] is None

    def test_get_empty(self, db):
        results = db.get_nodes([])
        assert results == []


class TestGetNodesByKeys:
    def test_basic(self, db):
        db.upsert_node("Person", "alice")
        db.upsert_node("Person", "bob")
        db.upsert_node("Company", "charlie")
        results = db.get_nodes_by_keys([
            {"labels": ["Person"], "key": "alice"},
            {"labels": ["Person"], "key": "bob"},
            {"labels": ["Company"], "key": "charlie"},
        ])
        assert len(results) == 3
        assert results[0].key == "alice"
        assert results[1].key == "bob"
        assert results[2].key == "charlie"

    def test_mixed_found_missing(self, db):
        db.upsert_node("Person", "a")
        bid = db.upsert_node("Person", "b")
        db.delete_node(bid)
        results = db.get_nodes_by_keys([
            {"labels": ["Person"], "key": "a"},
            {"labels": ["Person"], "key": "b"},
            {"labels": ["Person"], "key": "nonexistent"},
        ])
        assert len(results) == 3
        assert results[0] is not None
        assert results[1] is None
        assert results[2] is None

    def test_empty(self, db):
        results = db.get_nodes_by_keys([])
        assert results == []

class TestGetEdges:
    def test_get_multiple(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        n3 = db.upsert_node("Person", "c")
        e1 = db.upsert_edge(n1, n2, "RELATES_TO")
        e2 = db.upsert_edge(n2, n3, "RELATES_TO")
        results = db.get_edges([e1, e2])
        assert len(results) == 2
        assert results[0].id == e1
        assert results[1].id == e2

    def test_get_with_missing(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        e1 = db.upsert_edge(n1, n2, "RELATES_TO")
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
                {"labels": ["Person"], "key": "a"},
                {"labels": ["Person"], "key": "b"},
            ],
        })
        assert len(result.node_ids) == 2
        assert len(result.edge_ids) == 0

    def test_upsert_edges_only(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        result = db.graph_patch({
            "upsert_edges": [
                {"from_id": n1, "to_id": n2, "label": "RELATES_TO"},
            ],
        })
        assert len(result.node_ids) == 0
        assert len(result.edge_ids) == 1

    def test_mixed_patch(self, db):
        result = db.graph_patch({
            "upsert_nodes": [
                {"labels": ["Person"], "key": "x"},
                {"labels": ["Person"], "key": "y"},
            ],
        })
        n1, n2 = result.node_ids
        result2 = db.graph_patch({
            "upsert_edges": [
                {"from_id": n1, "to_id": n2, "label": "RELATES_TO"},
            ],
        })
        assert len(result2.edge_ids) == 1
        assert db.get_edge(result2.edge_ids[0]) is not None

    def test_delete_nodes(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        db.graph_patch({"delete_node_ids": [n1]})
        assert db.get_node(n1) is None
        assert db.get_node(n2) is not None

    def test_delete_edges(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        db.graph_patch({"delete_edge_ids": [eid]})
        assert db.get_edge(eid) is None

    def test_invalidate_edges(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
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
            "upsert_nodes": [{"labels": ["Person"], "key": "a"}],
        })
        r = repr(result)
        assert "PatchResult" in r


# ============================================================
# Helper: pack binary batches using struct
# ============================================================

def pack_node_batch(nodes):
    """Pack a list of node dicts into the binary wire format.

    Format: [magic:4][version:u16][count:u32] per node:
            [label_count:u8] repeated [label_len:u16][label:utf8]
            [weight:f32][key_len:u16][key:utf8][props_len:u32][props:json]
    """
    import json
    buf = b"OGNB" + struct.pack("<HI", 2, len(nodes))
    for n in nodes:
        labels = n.get("labels", [])
        weight = n.get("weight", 1.0)
        key = n.get("key", "").encode("utf-8")
        props_json = json.dumps(n.get("props", {})).encode("utf-8") if n.get("props") else b""
        buf += struct.pack("<B", len(labels))
        for label in labels:
            encoded = label.encode("utf-8")
            buf += struct.pack("<H", len(encoded))
            buf += encoded
        buf += struct.pack("<fH", weight, len(key))
        buf += key
        buf += struct.pack("<I", len(props_json))
        buf += props_json
    return buf


def pack_edge_batch(edges):
    """Pack a list of edge dicts into the binary wire format.

    Format: [count:u32] per edge: [from:u64][to:u64][label_len:u16]
            [label:utf8][weight:f32][valid_from:i64][valid_to:i64]
            [props_len:u32][props:json]
    """
    import json
    buf = struct.pack("<I", len(edges))
    for e in edges:
        from_id = e["from_id"]
        to_id = e["to_id"]
        label = e.get("label", "").encode("utf-8")
        weight = e.get("weight", 1.0)
        valid_from = e.get("valid_from", 0)
        valid_to = e.get("valid_to", 0)
        props_json = json.dumps(e.get("props", {})).encode("utf-8") if e.get("props") else b""
        buf += struct.pack("<QQH", from_id, to_id, len(label))
        buf += label
        buf += struct.pack("<f", weight)
        buf += struct.pack("<qq", valid_from, valid_to)
        buf += struct.pack("<I", len(props_json))
        buf += props_json
    return buf


class TestBatchUpsertNodesBinary:
    def test_basic(self, db):
        buf = pack_node_batch([
            {"labels": ["Person"], "key": "a"},
            {"labels": ["Person"], "key": "b"},
        ])
        ids = db.batch_upsert_nodes_binary(buf)
        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_with_props_and_weight(self, db):
        buf = pack_node_batch([
            {"labels": ["Person", "Admin"], "key": "check", "props": {"color": "red", "score": 42}, "weight": 0.8},
        ])
        ids = db.batch_upsert_nodes_binary(buf)
        n = db.get_node(ids[0])
        assert n.key == "check"
        assert n.labels == ["Person", "Admin"]
        assert n.props["color"] == "red"
        assert n.props["score"] == 42
        assert abs(n.weight - 0.8) < 0.01

    def test_empty(self, db):
        buf = pack_node_batch([])
        ids = db.batch_upsert_nodes_binary(buf)
        assert ids == []

    def test_dedup(self, db):
        buf = pack_node_batch([
            {"labels": ["Person"], "key": "dup", "props": {"v": 1}},
            {"labels": ["Person"], "key": "dup", "props": {"v": 2}},
        ])
        ids = db.batch_upsert_nodes_binary(buf)
        assert ids[0] == ids[1]
        n = db.get_node(ids[0])
        assert n.props["v"] == 2  # last write wins

    def test_truncated_buffer(self, db):
        buf = pack_node_batch([{"labels": ["Person"], "key": "a"}])
        with pytest.raises(ValueError, match="truncated"):
            db.batch_upsert_nodes_binary(buf[:5])

    def test_trailing_bytes(self, db):
        buf = pack_node_batch([{"labels": ["Person"], "key": "a"}])
        with pytest.raises(ValueError, match="trailing"):
            db.batch_upsert_nodes_binary(buf + b"\x00")

    def test_rejects_missing_label_token(self, db):
        buf = b"OGNB" + struct.pack("<HI", 2, 1)
        buf += struct.pack("<B", 0)
        buf += struct.pack("<fH", 1.0, 1)
        buf += b"a"
        buf += struct.pack("<I", 0)
        with pytest.raises(ValueError, match="label"):
            db.batch_upsert_nodes_binary(buf)

    def test_rejects_malformed_label_record(self, db):
        buf = b"OGNB" + struct.pack("<HI", 2, 1)
        buf += struct.pack("<BI", 1, 10)
        buf += b"a"
        with pytest.raises(ValueError):
            db.batch_upsert_nodes_binary(buf)

    def test_rejects_legacy_v1_buffer(self, db):
        label = b"Person"
        key = b"legacy"
        buf = struct.pack("<I", 1)
        buf += struct.pack("<H", len(label))
        buf += label
        buf += struct.pack("<fH", 1.0, len(key))
        buf += key
        buf += struct.pack("<I", 0)
        with pytest.raises(ValueError, match="version 2|version 1"):
            db.batch_upsert_nodes_binary(buf)

    def test_schema_rejects_binary_node_batch(self, db):
        db.set_node_schema(
            "BinarySchemaPerson",
            {
                "properties": {
                    "name": {"required": True, "nullable": False, "types": ["string"]}
                }
            },
        )
        buf = pack_node_batch([{"labels": ["BinarySchemaPerson"], "key": "bad"}])
        with pytest.raises(OverGraphError, match="schema violation"):
            db.batch_upsert_nodes_binary(buf)
        assert db.count_nodes_by_labels("BinarySchemaPerson") == 0


class TestBatchUpsertEdgesBinary:
    def test_basic(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        buf = pack_edge_batch([
            {"from_id": n1, "to_id": n2, "label": "RELATES_TO"},
        ])
        ids = db.batch_upsert_edges_binary(buf)
        assert len(ids) == 1
        e = db.get_edge(ids[0])
        assert e.from_id == n1
        assert e.to_id == n2
        assert e.label == "RELATES_TO"

    def test_with_props_and_weight(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        buf = pack_edge_batch([
            {"from_id": n1, "to_id": n2, "label": "RELATES_TO", "props": {"label": "knows"}, "weight": 2.5},
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
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        buf = pack_edge_batch([{"from_id": n1, "to_id": n2, "label": "RELATES_TO"}])
        with pytest.raises(ValueError, match="truncated"):
            db.batch_upsert_edges_binary(buf[:10])

    def test_trailing_bytes(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        buf = pack_edge_batch([{"from_id": n1, "to_id": n2, "label": "RELATES_TO"}])
        with pytest.raises(ValueError, match="trailing"):
            db.batch_upsert_edges_binary(buf + b"\x00")

    def test_rejects_missing_label_token(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        buf = struct.pack("<I", 1)
        buf += struct.pack("<QQH", n1, n2, 0)
        buf += struct.pack("<fqqI", 1.0, 0, 0, 0)
        with pytest.raises(ValueError, match="label"):
            db.batch_upsert_edges_binary(buf)

    def test_rejects_malformed_label_record(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        buf = struct.pack("<I", 1)
        buf += struct.pack("<QQIf", n1, n2, 10, 1.0)
        buf += struct.pack("<qqI", 0, 0, 0)
        with pytest.raises(ValueError):
            db.batch_upsert_edges_binary(buf)

    def test_schema_rejects_binary_edge_batch(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        db.set_edge_schema(
            "BINARY_SCHEMA_EDGE",
            {
                "properties": {
                    "role": {"required": True, "nullable": False, "types": ["string"]}
                }
            },
        )
        buf = pack_edge_batch([{"from_id": n1, "to_id": n2, "label": "BINARY_SCHEMA_EDGE"}])
        with pytest.raises(OverGraphError, match="schema violation"):
            db.batch_upsert_edges_binary(buf)
        assert db.count_edges_by_label("BINARY_SCHEMA_EDGE") == 0
