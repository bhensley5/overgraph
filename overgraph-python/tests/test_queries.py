from overgraph import OverGraph


class TestFindNodes:
    def test_find_by_prop(self, db):
        db.upsert_node("Person", "a", props={"color": "red"})
        db.upsert_node("Person", "b", props={"color": "blue"})
        db.upsert_node("Person", "c", props={"color": "red"})
        ids = db.find_nodes("Person", "color", "red")
        assert len(ids) == 2

    def test_find_no_match(self, db):
        db.upsert_node("Person", "a", props={"color": "red"})
        ids = db.find_nodes("Person", "color", "green")
        assert len(ids) == 0

    def test_find_wrong_type(self, db):
        db.upsert_node("Person", "a", props={"color": "red"})
        ids = db.find_nodes("Company", "color", "red")
        assert len(ids) == 0

    def test_find_int_prop(self, db):
        db.upsert_node("Person", "a", props={"level": 5})
        db.upsert_node("Person", "b", props={"level": 10})
        ids = db.find_nodes("Person", "level", 5)
        assert len(ids) == 1


class TestNodesByType:
    def test_nodes_by_labels(self, db):
        db.upsert_node("Person", "a")
        db.upsert_node("Person", "b")
        db.upsert_node("Company", "c")
        ids = db.nodes_by_labels("Person")
        assert len(ids) == 2

    def test_nodes_by_labels_empty(self, db):
        ids = db.nodes_by_labels("Missing")
        assert len(ids) == 0

    def test_nodes_by_labels_list_requires_all_labels(self, db):
        admin = db.upsert_node(["Person", "Admin"], "admin")
        db.upsert_node("Person", "regular")

        assert db.nodes_by_labels(["Person", "Admin"]).to_list() == [admin]
        assert not hasattr(db, "nodes_by_label")


class TestGetNodesByType:
    def test_get_nodes_by_labels(self, db):
        db.upsert_node("Person", "a", props={"name": "Alice"})
        db.upsert_node("Person", "b", props={"name": "Bob"})
        db.upsert_node("Company", "c")
        nodes = db.get_nodes_by_labels("Person")
        assert len(nodes) == 2
        assert all(n.labels == ["Person"] for n in nodes)

    def test_get_nodes_by_labels_empty(self, db):
        assert db.get_nodes_by_labels("Missing") == []


class TestEdgesByType:
    def test_edges_by_label(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        n3 = db.upsert_node("Person", "c")
        db.upsert_edge(n1, n2, "RELATES_TO")
        db.upsert_edge(n2, n3, "WORKS_AT")
        db.upsert_edge(n1, n3, "RELATES_TO")
        ids = db.edges_by_label("RELATES_TO")
        assert len(ids) == 2

    def test_edges_by_label_empty(self, db):
        assert len(db.edges_by_label("FOLLOWS")) == 0


class TestGetEdgesByType:
    def test_get_edges_by_label(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        db.upsert_edge(n1, n2, "RELATES_TO", props={"rel": "friend"})
        edges = db.get_edges_by_label("RELATES_TO")
        assert len(edges) == 1
        assert edges[0].props["rel"] == "friend"

    def test_get_edges_by_label_empty(self, db):
        assert db.get_edges_by_label("FOLLOWS") == []


class TestCountByType:
    def test_count_nodes(self, db):
        db.upsert_node("Person", "a")
        db.upsert_node("Person", "b")
        db.upsert_node("Company", "c")
        assert db.count_nodes_by_labels("Person") == 2
        assert db.count_nodes_by_labels("Company") == 1
        assert db.count_nodes_by_labels("Missing") == 0
        assert not hasattr(db, "count_nodes_by_label")

    def test_count_edges(self, db):
        n1 = db.upsert_node("Person", "a")
        n2 = db.upsert_node("Person", "b")
        db.upsert_edge(n1, n2, "RELATES_TO")
        db.upsert_edge(n1, n2, "WORKS_AT")
        assert db.count_edges_by_label("RELATES_TO") == 1
        assert db.count_edges_by_label("WORKS_AT") == 1
        assert db.count_edges_by_label("FOLLOWS") == 0


class TestFindNodesByTimeRange:
    def test_time_range(self, db):
        id1 = db.upsert_node("Person", "a")
        id2 = db.upsert_node("Person", "b")
        # Both nodes were just created, so their timestamps should be recent
        node1 = db.get_node(id1)
        node2 = db.get_node(id2)
        # Search with a range that covers both
        lo = min(node1.updated_at, node2.updated_at) - 1000
        hi = max(node1.updated_at, node2.updated_at) + 1000
        ids = db.find_nodes_by_time_range("Person", lo, hi)
        assert len(ids) == 2

    def test_time_range_empty(self, db):
        db.upsert_node("Person", "a")
        # Search far in the past
        ids = db.find_nodes_by_time_range("Person", 0, 1)
        assert len(ids) == 0
