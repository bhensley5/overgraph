import pytest
from overgraph import OverGraph, AsyncOverGraph, PyShortestPath


def build_chain(db, n=5, edge_type=10):
    """Create a chain: n0 -> n1 -> ... -> n(n-1). Returns node IDs."""
    nodes = []
    for i in range(n):
        nodes.append(db.upsert_node(1, f"n{i}"))
    for i in range(n - 1):
        db.upsert_edge(nodes[i], nodes[i + 1], edge_type)
    return nodes


def build_diamond(db):
    """A -> B -> D, A -> C -> D."""
    a = db.upsert_node(1, "a")
    b = db.upsert_node(1, "b")
    c = db.upsert_node(1, "c")
    d = db.upsert_node(1, "d")
    db.upsert_edge(a, b, 10)
    db.upsert_edge(a, c, 10)
    db.upsert_edge(b, d, 10)
    db.upsert_edge(c, d, 10)
    return a, b, c, d


class TestShortestPath:
    def test_direct_neighbor(self, db):
        nodes = build_chain(db, 2)
        result = db.shortest_path(nodes[0], nodes[1])
        assert result is not None
        assert result.nodes == nodes
        assert len(result.edges) == 1
        assert result.total_cost == 1.0

    def test_multi_hop(self, db):
        nodes = build_chain(db, 5)
        result = db.shortest_path(nodes[0], nodes[4])
        assert result is not None
        assert result.nodes == nodes
        assert len(result.edges) == 4
        assert result.total_cost == 4.0

    def test_disconnected(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        assert db.shortest_path(a, b) is None

    def test_self_path(self, db):
        a = db.upsert_node(1, "a")
        result = db.shortest_path(a, a)
        assert result is not None
        assert result.nodes == [a]
        assert result.edges == []
        assert result.total_cost == 0.0

    def test_direction_incoming(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        # outgoing from b to a: no path
        assert db.shortest_path(b, a) is None
        # incoming: b has incoming from a, so b<-a should work
        result = db.shortest_path(b, a, direction="incoming")
        assert result is not None
        assert result.nodes == [b, a]

    def test_direction_both(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        result = db.shortest_path(b, a, direction="both")
        assert result is not None
        assert result.nodes == [b, a]

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 20)
        # Only type 10: can't reach c
        assert db.shortest_path(a, c, type_filter=[10]) is None
        # Both types: can reach c
        result = db.shortest_path(a, c, type_filter=[10, 20])
        assert result is not None

    def test_max_depth(self, db):
        nodes = build_chain(db, 5)
        assert db.shortest_path(nodes[0], nodes[4], max_depth=2) is None
        result = db.shortest_path(nodes[0], nodes[4], max_depth=4)
        assert result is not None

    def test_nonexistent_nodes(self, db):
        assert db.shortest_path(999998, 999999) is None

    def test_repr(self, db):
        a = db.upsert_node(1, "a")
        result = db.shortest_path(a, a)
        assert "ShortestPath" in repr(result)


class TestShortestPathWeighted:
    def test_weighted_shortest(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=1.0)
        db.upsert_edge(a, c, 10, weight=10.0)
        db.upsert_edge(b, c, 10, weight=1.0)
        result = db.shortest_path(a, c, weight_field="weight")
        assert result is not None
        assert result.nodes == [a, b, c]
        assert abs(result.total_cost - 2.0) < 1e-6

    def test_max_cost(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, weight=5.0)
        db.upsert_edge(b, c, 10, weight=5.0)
        assert db.shortest_path(a, c, weight_field="weight", max_cost=8.0) is None
        result = db.shortest_path(a, c, weight_field="weight", max_cost=10.0)
        assert result is not None

    def test_max_depth_uses_best_constrained_weighted_path(self, db):
        s = db.upsert_node(1, "s")
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        t = db.upsert_node(1, "t")
        db.upsert_edge(s, a, 10, weight=1.0)
        db.upsert_edge(a, b, 10, weight=1.0)
        db.upsert_edge(b, t, 10, weight=1.0)
        db.upsert_edge(s, c, 10, weight=3.0)
        db.upsert_edge(c, t, 10, weight=3.0)

        result = db.shortest_path(s, t, weight_field="weight", max_depth=2)
        assert result is not None
        assert result.nodes == [s, c, t]
        assert abs(result.total_cost - 6.0) < 1e-6


class TestIsConnected:
    def test_connected(self, db):
        nodes = build_chain(db, 3)
        assert db.is_connected(nodes[0], nodes[2]) is True

    def test_disconnected(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        assert db.is_connected(a, b) is False

    def test_self(self, db):
        a = db.upsert_node(1, "a")
        assert db.is_connected(a, a) is True

    def test_direction(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        assert db.is_connected(b, a) is False
        assert db.is_connected(b, a, direction="incoming") is True
        assert db.is_connected(b, a, direction="both") is True

    def test_max_depth(self, db):
        nodes = build_chain(db, 5)
        assert db.is_connected(nodes[0], nodes[4], max_depth=2) is False
        assert db.is_connected(nodes[0], nodes[4], max_depth=4) is True

    def test_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 20)
        assert db.is_connected(a, c, type_filter=[10]) is False
        assert db.is_connected(a, c, type_filter=[10, 20]) is True


class TestAllShortestPaths:
    def test_diamond(self, db):
        a, b, c, d = build_diamond(db)
        paths = db.all_shortest_paths(a, d)
        assert len(paths) == 2
        for p in paths:
            assert p.nodes[0] == a
            assert p.nodes[-1] == d
            assert p.total_cost == 2.0
            assert len(p.edges) == 2

    def test_disconnected(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        paths = db.all_shortest_paths(a, b)
        assert isinstance(paths, list)
        assert len(paths) == 0

    def test_max_paths(self, db):
        a, b, c, d = build_diamond(db)
        paths = db.all_shortest_paths(a, d, max_paths=1)
        assert len(paths) == 1

    def test_weighted(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        d = db.upsert_node(1, "d")
        db.upsert_edge(a, b, 10, weight=1.0)
        db.upsert_edge(a, c, 10, weight=1.0)
        db.upsert_edge(b, d, 10, weight=1.0)
        db.upsert_edge(c, d, 10, weight=1.0)
        paths = db.all_shortest_paths(a, d, weight_field="weight")
        assert len(paths) == 2
        for p in paths:
            assert abs(p.total_cost - 2.0) < 1e-6

    def test_weighted_max_depth_uses_best_constrained_cost(self, db):
        s = db.upsert_node(1, "s")
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        t = db.upsert_node(1, "t")
        db.upsert_edge(s, a, 10, weight=1.0)
        db.upsert_edge(a, b, 10, weight=1.0)
        db.upsert_edge(b, t, 10, weight=1.0)
        db.upsert_edge(s, c, 10, weight=3.0)
        db.upsert_edge(c, t, 10, weight=3.0)

        paths = db.all_shortest_paths(s, t, weight_field="weight", max_depth=2)
        assert len(paths) == 1
        assert paths[0].nodes == [s, c, t]
        assert abs(paths[0].total_cost - 6.0) < 1e-6

    def test_self_path(self, db):
        a = db.upsert_node(1, "a")
        paths = db.all_shortest_paths(a, a)
        assert len(paths) == 1
        assert paths[0].nodes == [a]
        assert paths[0].total_cost == 0.0


class TestShortestPathTemporal:
    def test_at_epoch_excludes_expired(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, valid_from=100, valid_to=200)
        db.upsert_edge(b, c, 10, valid_from=100, valid_to=200)
        assert db.shortest_path(a, c, at_epoch=150) is not None
        assert db.shortest_path(a, c, at_epoch=250) is None

    def test_is_connected_temporal(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10, valid_from=100, valid_to=200)
        assert db.is_connected(a, b, at_epoch=150) is True
        assert db.is_connected(a, b, at_epoch=250) is False


@pytest.mark.asyncio
class TestShortestPathAsync:
    async def test_shortest_path(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        c = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(a, b, 10)
        await async_db.upsert_edge(b, c, 10)
        result = await async_db.shortest_path(a, c)
        assert result is not None
        assert result.nodes == [a, b, c]
        assert result.total_cost == 2.0

    async def test_disconnected(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        assert await async_db.shortest_path(a, b) is None

    async def test_is_connected(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(a, b, 10)
        assert await async_db.is_connected(a, b) is True
        assert await async_db.is_connected(b, a) is False

    async def test_all_shortest_paths(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        c = await async_db.upsert_node(1, "c")
        d = await async_db.upsert_node(1, "d")
        await async_db.upsert_edge(a, b, 10)
        await async_db.upsert_edge(a, c, 10)
        await async_db.upsert_edge(b, d, 10)
        await async_db.upsert_edge(c, d, 10)
        paths = await async_db.all_shortest_paths(a, d)
        assert len(paths) == 2
