from overgraph import OverGraph


class TestFindNodes:
    def test_find_by_prop(self, db):
        db.upsert_node(1, "a", props={"color": "red"})
        db.upsert_node(1, "b", props={"color": "blue"})
        db.upsert_node(1, "c", props={"color": "red"})
        ids = db.find_nodes(1, "color", "red")
        assert len(ids) == 2

    def test_find_no_match(self, db):
        db.upsert_node(1, "a", props={"color": "red"})
        ids = db.find_nodes(1, "color", "green")
        assert len(ids) == 0

    def test_find_wrong_type(self, db):
        db.upsert_node(1, "a", props={"color": "red"})
        ids = db.find_nodes(2, "color", "red")
        assert len(ids) == 0

    def test_find_int_prop(self, db):
        db.upsert_node(1, "a", props={"level": 5})
        db.upsert_node(1, "b", props={"level": 10})
        ids = db.find_nodes(1, "level", 5)
        assert len(ids) == 1


class TestNodesByType:
    def test_nodes_by_type(self, db):
        db.upsert_node(1, "a")
        db.upsert_node(1, "b")
        db.upsert_node(2, "c")
        ids = db.nodes_by_type(1)
        assert len(ids) == 2

    def test_nodes_by_type_empty(self, db):
        ids = db.nodes_by_type(99)
        assert len(ids) == 0


class TestGetNodesByType:
    def test_get_nodes_by_type(self, db):
        db.upsert_node(1, "a", props={"name": "Alice"})
        db.upsert_node(1, "b", props={"name": "Bob"})
        db.upsert_node(2, "c")
        nodes = db.get_nodes_by_type(1)
        assert len(nodes) == 2
        assert all(n.type_id == 1 for n in nodes)

    def test_get_nodes_by_type_empty(self, db):
        assert db.get_nodes_by_type(99) == []


class TestEdgesByType:
    def test_edges_by_type(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n2, n3, 20)
        db.upsert_edge(n1, n3, 10)
        ids = db.edges_by_type(10)
        assert len(ids) == 2

    def test_edges_by_type_empty(self, db):
        assert len(db.edges_by_type(99)) == 0


class TestGetEdgesByType:
    def test_get_edges_by_type(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.upsert_edge(n1, n2, 10, props={"rel": "friend"})
        edges = db.get_edges_by_type(10)
        assert len(edges) == 1
        assert edges[0].props["rel"] == "friend"

    def test_get_edges_by_type_empty(self, db):
        assert db.get_edges_by_type(99) == []


class TestCountByType:
    def test_count_nodes(self, db):
        db.upsert_node(1, "a")
        db.upsert_node(1, "b")
        db.upsert_node(2, "c")
        assert db.count_nodes_by_type(1) == 2
        assert db.count_nodes_by_type(2) == 1
        assert db.count_nodes_by_type(99) == 0

    def test_count_edges(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n2, 20)
        assert db.count_edges_by_type(10) == 1
        assert db.count_edges_by_type(20) == 1
        assert db.count_edges_by_type(99) == 0


class TestFindNodesByTimeRange:
    def test_time_range(self, db):
        id1 = db.upsert_node(1, "a")
        id2 = db.upsert_node(1, "b")
        # Both nodes were just created, so their timestamps should be recent
        node1 = db.get_node(id1)
        node2 = db.get_node(id2)
        # Search with a range that covers both
        lo = min(node1.updated_at, node2.updated_at) - 1000
        hi = max(node1.updated_at, node2.updated_at) + 1000
        ids = db.find_nodes_by_time_range(1, lo, hi)
        assert len(ids) == 2

    def test_time_range_empty(self, db):
        db.upsert_node(1, "a")
        # Search far in the past
        ids = db.find_nodes_by_time_range(1, 0, 1)
        assert len(ids) == 0
