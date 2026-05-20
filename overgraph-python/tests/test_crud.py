import pytest
from overgraph import OverGraph, OverGraphError


class TestCatalogApi:
    def test_node_label_and_label_catalog(self, db):
        person_id = db.ensure_node_label("Person")
        company_id = db.ensure_node_label("Company")
        relates_to_id = db.ensure_edge_label("RELATES_TO")
        works_at_id = db.ensure_edge_label("WORKS_AT")

        assert db.ensure_node_label("Person") == person_id
        assert db.ensure_edge_label("RELATES_TO") == relates_to_id
        assert db.get_node_label_id("Person") == person_id
        assert db.get_node_label_id("Company") == company_id
        assert db.get_edge_label_id("RELATES_TO") == relates_to_id
        assert db.get_edge_label_id("WORKS_AT") == works_at_id
        assert db.get_node_label(person_id) == "Person"
        assert db.get_node_label(company_id) == "Company"
        assert db.get_edge_label(relates_to_id) == "RELATES_TO"
        assert db.get_edge_label(works_at_id) == "WORKS_AT"
        assert db.get_edge_label(label_id=relates_to_id) == "RELATES_TO"
        assert db.get_node_label_id("Document") is None
        assert db.get_edge_label_id("LIKES") is None
        assert db.get_node_label(999999) is None
        assert db.get_edge_label(999999) is None

        old_field_name = "type" + "_id"
        with pytest.raises(TypeError):
            db.get_edge_label(**{old_field_name: relates_to_id})

        node_labels = {entry.label: entry.label_id for entry in db.list_node_labels()}
        edge_label_entries = db.list_edge_labels()
        edge_labels = {entry.label: entry.label_id for entry in edge_label_entries}
        assert node_labels["Person"] == person_id
        assert node_labels["Company"] == company_id
        assert edge_labels["RELATES_TO"] == relates_to_id
        assert edge_labels["WORKS_AT"] == works_at_id
        for entry in edge_label_entries:
            assert not hasattr(entry, old_field_name)
        assert "label_id=" in repr(edge_label_entries[0])


class TestUpsertNode:
    def test_upsert_returns_id(self, db):
        nid = db.upsert_node("Person", "alice")
        assert isinstance(nid, int)
        assert nid > 0

    def test_upsert_same_key_returns_same_id(self, db):
        id1 = db.upsert_node("Person", "alice")
        id2 = db.upsert_node("Person", "alice")
        assert id1 == id2

    def test_upsert_different_keys(self, db):
        id1 = db.upsert_node("Person", "alice")
        id2 = db.upsert_node("Person", "bob")
        assert id1 != id2

    def test_upsert_with_props(self, db):
        nid = db.upsert_node("Person", "alice", props={"age": 30, "name": "Alice"})
        node = db.get_node(nid)
        assert node.props["age"] == 30
        assert node.props["name"] == "Alice"

    def test_upsert_with_weight(self, db):
        nid = db.upsert_node("Person", "alice", weight=2.5)
        node = db.get_node(nid)
        assert abs(node.weight - 2.5) < 0.01

    def test_upsert_with_nested_props(self, db):
        nid = db.upsert_node("Person", "alice", props={
            "tags": ["a", "b"],
            "meta": {"nested": True, "count": 42},
        })
        node = db.get_node(nid)
        assert node.props["tags"] == ["a", "b"]
        assert node.props["meta"]["nested"] is True
        assert node.props["meta"]["count"] == 42

    def test_upsert_updates_props(self, db):
        nid = db.upsert_node("Person", "alice", props={"v": 1})
        db.upsert_node("Person", "alice", props={"v": 2})
        node = db.get_node(nid)
        assert node.props["v"] == 2

    def test_upsert_accepts_label_string_or_sequence(self, db):
        single = db.upsert_node("Person", "alice")
        multi = db.upsert_node(["Person", "Admin"], "root")

        assert db.get_node(single).labels == ["Person"]
        assert db.get_node(multi).labels == ["Person", "Admin"]
        assert db.count_nodes_by_labels("Person") == 2
        assert db.nodes_by_labels(["Person", "Admin"]).to_list() == [multi]

    def test_add_remove_node_label_returns_changed(self, db):
        nid = db.upsert_node("Person", "alice")

        assert db.add_node_label(nid, "Admin") is True
        assert db.add_node_label(nid, "Admin") is False
        assert db.get_node(nid).labels == ["Person", "Admin"]
        assert db.remove_node_label(nid, "Admin") is True
        assert db.remove_node_label(nid, "Admin") is False
        assert db.get_node(nid).labels == ["Person"]

    def test_remove_last_node_label_rejected(self, db):
        nid = db.upsert_node("Person", "alice")

        with pytest.raises(OverGraphError, match="last node label"):
            db.remove_node_label(nid, "Person")

        assert db.get_node(nid).labels == ["Person"]

    def test_add_node_label_conflict_rejected(self, db):
        alice = db.upsert_node("Person", "shared")
        other = db.upsert_node("Admin", "shared")

        with pytest.raises(OverGraphError, match="node key conflict"):
            db.add_node_label(alice, "Admin")

        assert db.get_node(alice).labels == ["Person"]
        assert db.get_node(other).labels == ["Admin"]


class TestGetNode:
    def test_get_existing(self, db):
        nid = db.upsert_node("Person", "alice")
        node = db.get_node(nid)
        assert node is not None
        assert node.id == nid
        assert node.labels == ["Person"]
        assert node.key == "alice"
        assert node.created_at > 0
        assert node.updated_at > 0

    def test_get_nonexistent(self, db):
        node = db.get_node(999999)
        assert node is None

    def test_node_repr(self, db):
        nid = db.upsert_node("Person", "bob")
        node = db.get_node(nid)
        r = repr(node)
        assert "NodeView" in r
        assert "bob" in r


class TestGetNodeByKey:
    def test_get_by_key(self, db):
        nid = db.upsert_node("Person", "alice")
        node = db.get_node_by_key("Person", "alice")
        assert node is not None
        assert node.id == nid

    def test_get_by_key_wrong_type(self, db):
        db.upsert_node("Person", "alice")
        node = db.get_node_by_key("Company", "alice")
        assert node is None

    def test_get_by_key_nonexistent(self, db):
        node = db.get_node_by_key("Person", "nobody")
        assert node is None


class TestDeleteNode:
    def test_delete_node(self, db):
        nid = db.upsert_node("Person", "alice")
        db.delete_node(nid)
        assert db.get_node(nid) is None

    def test_delete_nonexistent(self, db):
        # Should not raise
        db.delete_node(999999)


class TestUpsertEdge:
    def test_upsert_returns_id(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        assert isinstance(eid, int)
        assert eid > 0

    def test_upsert_with_props(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO", props={"rel": "friend"})
        edge = db.get_edge(eid)
        assert edge.props["rel"] == "friend"

    def test_upsert_with_weight(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO", weight=3.14)
        edge = db.get_edge(eid)
        assert abs(edge.weight - 3.14) < 0.01

    def test_upsert_with_temporal(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO", valid_from=1000, valid_to=2000)
        edge = db.get_edge(eid)
        assert edge.valid_from == 1000
        assert edge.valid_to == 2000


class TestGetEdge:
    def test_get_existing(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        edge = db.get_edge(eid)
        assert edge is not None
        assert edge.id == eid
        assert edge.from_id == n1
        assert edge.to_id == n2
        assert edge.label == "RELATES_TO"

    def test_get_nonexistent(self, db):
        assert db.get_edge(999999) is None

    def test_edge_repr(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        edge = db.get_edge(eid)
        r = repr(edge)
        assert "EdgeView" in r


class TestGetEdgeByTriple:
    def test_get_by_triple(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        edge = db.get_edge_by_triple(n1, n2, "RELATES_TO")
        assert edge is not None
        assert edge.id == eid

    def test_get_by_triple_wrong_type(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        db.upsert_edge(n1, n2, "RELATES_TO")
        assert db.get_edge_by_triple(n1, n2, "DOES_NOT_EXIST") is None


class TestDeleteEdge:
    def test_delete_edge(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        db.delete_edge(eid)
        assert db.get_edge(eid) is None


class TestInvalidateEdge:
    def test_invalidate(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        eid = db.upsert_edge(n1, n2, "RELATES_TO")
        result = db.invalidate_edge(eid, 5000)
        assert result is not None
        assert result.valid_to == 5000

    def test_invalidate_nonexistent(self, db):
        result = db.invalidate_edge(999999, 5000)
        assert result is None


class TestPropertyTypes:
    """Test all supported property types roundtrip through Python -> Rust -> Python."""

    def test_string(self, db):
        nid = db.upsert_node("Person", "k", props={"s": "hello"})
        assert db.get_node(nid).props["s"] == "hello"

    def test_int(self, db):
        nid = db.upsert_node("Person", "k", props={"i": 42})
        assert db.get_node(nid).props["i"] == 42

    def test_float(self, db):
        nid = db.upsert_node("Person", "k", props={"f": 3.14})
        assert abs(db.get_node(nid).props["f"] - 3.14) < 0.001

    def test_bool(self, db):
        nid = db.upsert_node("Person", "k", props={"b": True, "c": False})
        p = db.get_node(nid).props
        assert p["b"] is True
        assert p["c"] is False

    def test_null(self, db):
        nid = db.upsert_node("Person", "k", props={"n": None})
        assert db.get_node(nid).props["n"] is None

    def test_bytes(self, db):
        nid = db.upsert_node("Person", "k", props={"data": b"\x00\x01\x02"})
        assert db.get_node(nid).props["data"] == b"\x00\x01\x02"

    def test_array(self, db):
        nid = db.upsert_node("Person", "k", props={"arr": [1, "two", 3.0]})
        p = db.get_node(nid).props
        assert p["arr"][0] == 1
        assert p["arr"][1] == "two"
        assert abs(p["arr"][2] - 3.0) < 0.001

    def test_nested_map(self, db):
        nid = db.upsert_node("Person", "k", props={"m": {"a": 1, "b": {"c": 2}}})
        p = db.get_node(nid).props
        assert p["m"]["a"] == 1
        assert p["m"]["b"]["c"] == 2

    def test_empty_props(self, db):
        nid = db.upsert_node("Person", "k", props={})
        assert db.get_node(nid).props == {}

    def test_no_props(self, db):
        nid = db.upsert_node("Person", "k")
        assert db.get_node(nid).props == {}
