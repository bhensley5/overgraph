from conftest import make_chain, make_star


class TestNodesByTypePaged:
    def test_single_page(self, db):
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        page = db.nodes_by_type_paged(1)
        assert len(page.items) == 5
        assert len(page) == 5
        assert page  # __bool__ truthy
        assert page.next_cursor is None
        assert "IdPageResult" in repr(page)

    def test_empty_page_is_falsy(self, db):
        page = db.nodes_by_type_paged(99)
        assert len(page) == 0
        assert not page  # __bool__ falsy

    def test_pagination(self, db):
        for i in range(10):
            db.upsert_node(1, f"n{i}")
        p1 = db.nodes_by_type_paged(1, limit=3)
        assert len(p1.items) == 3
        assert p1.next_cursor is not None
        p2 = db.nodes_by_type_paged(1, limit=3, after=p1.next_cursor)
        assert len(p2.items) == 3
        # No overlap
        assert set(p1.items).isdisjoint(set(p2.items))

    def test_empty(self, db):
        page = db.nodes_by_type_paged(99)
        assert len(page.items) == 0
        assert page.next_cursor is None

    def test_exhaust_all_pages(self, db):
        for i in range(7):
            db.upsert_node(1, f"n{i}")
        all_ids = []
        cursor = None
        while True:
            page = db.nodes_by_type_paged(1, limit=3, after=cursor)
            all_ids.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        assert len(all_ids) == 7


class TestEdgesByTypePaged:
    def test_single_page(self, db):
        nodes, edges = make_chain(db, 5)
        page = db.edges_by_type_paged(10)
        assert len(page.items) == 4  # 5 nodes, 4 edges
        assert page.next_cursor is None

    def test_pagination(self, db):
        nodes, edges = make_chain(db, 6)
        p1 = db.edges_by_type_paged(10, limit=2)
        assert len(p1.items) == 2
        assert p1.next_cursor is not None


class TestGetNodesByTypePaged:
    def test_returns_records(self, db):
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        page = db.get_nodes_by_type_paged(1)
        assert len(page.items) == 5
        assert page.next_cursor is None
        assert "NodePageResult" in repr(page)
        # Items are full records
        for item in page.items:
            assert item.type_id == 1
            assert item.key.startswith("n")

    def test_pagination(self, db):
        for i in range(8):
            db.upsert_node(1, f"n{i}")
        p1 = db.get_nodes_by_type_paged(1, limit=3)
        assert len(p1.items) == 3
        assert p1.next_cursor is not None
        p2 = db.get_nodes_by_type_paged(1, limit=3, after=p1.next_cursor)
        assert len(p2.items) == 3
        ids1 = {r.id for r in p1.items}
        ids2 = {r.id for r in p2.items}
        assert ids1.isdisjoint(ids2)


class TestGetEdgesByTypePaged:
    def test_returns_records(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, weight=2.5)
        page = db.get_edges_by_type_paged(10)
        assert len(page.items) == 1
        assert page.next_cursor is None
        assert "EdgePageResult" in repr(page)
        e = page.items[0]
        assert e.id == eid
        assert e.from_id == n1
        assert e.to_id == n2

    def test_pagination(self, db):
        nodes, edges = make_chain(db, 6)
        p1 = db.get_edges_by_type_paged(10, limit=2)
        assert len(p1.items) == 2
        assert p1.next_cursor is not None


class TestFindNodesPaged:
    def test_find_paged(self, db):
        for i in range(6):
            db.upsert_node(1, f"n{i}", props={"color": "red"})
        page = db.find_nodes_paged(1, "color", "red")
        assert len(page.items) == 6
        assert page.next_cursor is None

    def test_find_paged_with_limit(self, db):
        for i in range(6):
            db.upsert_node(1, f"n{i}", props={"color": "red"})
        p1 = db.find_nodes_paged(1, "color", "red", limit=2)
        assert len(p1.items) == 2
        assert p1.next_cursor is not None

    def test_find_paged_no_match(self, db):
        db.upsert_node(1, "a", props={"color": "blue"})
        page = db.find_nodes_paged(1, "color", "red")
        assert len(page.items) == 0


class TestFindNodesByTimeRangePaged:
    def test_time_range_paged(self, db):
        import time
        start = int(time.time() * 1000) - 1000
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        end = int(time.time() * 1000) + 1000
        page = db.find_nodes_by_time_range_paged(1, start, end)
        assert len(page.items) == 5

    def test_time_range_paged_with_limit(self, db):
        import time
        start = int(time.time() * 1000) - 1000
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        end = int(time.time() * 1000) + 1000
        p1 = db.find_nodes_by_time_range_paged(1, start, end, limit=2)
        assert len(p1.items) == 2
        assert p1.next_cursor is not None


class TestNeighborsPaged:
    def test_single_page(self, db):
        center, spokes = make_star(db)
        page = db.neighbors_paged(center, "outgoing")
        assert len(page.items) == 5
        assert page.next_cursor is None
        assert "NeighborPageResult" in repr(page)

    def test_pagination(self, db):
        center, spokes = make_star(db, spokes=10)
        p1 = db.neighbors_paged(center, "outgoing", limit=3)
        assert len(p1.items) == 3
        assert p1.next_cursor is not None
        p2 = db.neighbors_paged(center, "outgoing", limit=3, after=p1.next_cursor)
        assert len(p2.items) == 3
        ids1 = {n.node_id for n in p1.items}
        ids2 = {n.node_id for n in p2.items}
        assert ids1.isdisjoint(ids2)

    def test_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        page = db.neighbors_paged(n1, "outgoing", type_filter=[10])
        assert len(page.items) == 1
        assert page.items[0].node_id == n2


class TestNeighbors2HopPaged:
    def test_paged(self, db):
        nodes, _ = make_chain(db, 4)
        page = db.neighbors_2hop_paged(nodes[0], "outgoing")
        node_ids = {n.node_id for n in page.items}
        assert nodes[2] in node_ids
        assert nodes[0] not in node_ids

    def test_pagination(self, db):
        # Create a hub-and-spoke at each layer for more 2-hop results
        center = db.upsert_node(1, "center")
        middles = []
        for i in range(3):
            m = db.upsert_node(1, f"mid_{i}")
            db.upsert_edge(center, m, 10)
            middles.append(m)
        for m in middles:
            for j in range(3):
                leaf = db.upsert_node(1, f"leaf_{m}_{j}")
                db.upsert_edge(m, leaf, 10)
        # 9 leaves reachable at 2 hops
        page = db.neighbors_2hop_paged(center, "outgoing", limit=4)
        assert len(page.items) == 4
        assert page.next_cursor is not None


class TestNeighbors2HopConstrainedPaged:
    def test_paged(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(2, "b")
        n3 = db.upsert_node(3, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n2, n3, 10)
        page = db.neighbors_2hop_constrained_paged(
            n1, "outgoing",
            traverse_edge_types=[10],
            target_node_types=[3],
        )
        node_ids = {n.node_id for n in page.items}
        assert n3 in node_ids
        assert n2 not in node_ids
