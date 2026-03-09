import pytest
from overgraph import OverGraph


class TestConnectedComponents:
    def test_single_component(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)

        comps = db.connected_components()
        assert len(comps) == 3
        assert comps[a] == a  # min node id
        assert comps[b] == a
        assert comps[c] == a

    def test_multiple_components(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        d = db.upsert_node(1, "d")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(c, d, 10)

        comps = db.connected_components()
        assert len(comps) == 4
        assert comps[a] == comps[b]
        assert comps[c] == comps[d]
        assert comps[a] != comps[c]

    def test_isolated_nodes(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        comps = db.connected_components()
        assert comps[a] == a
        assert comps[b] == b

    def test_self_loop(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, a, 10)
        comps = db.connected_components()
        assert comps[a] == a
        assert comps[b] == b

    def test_direction_ignored(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)  # directed A→B
        db.upsert_edge(c, b, 10)  # directed C→B
        comps = db.connected_components()
        assert comps[a] == comps[b] == comps[c]

    def test_deleted_node(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        db.delete_node(b)
        comps = db.connected_components()
        assert b not in comps
        assert comps[a] == a
        assert comps[c] == c

    def test_deleted_edge(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        e1 = db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        db.delete_edge(e1)
        comps = db.connected_components()
        assert comps[a] != comps[b]
        assert comps[b] == comps[c]

    def test_edge_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 20)
        comps = db.connected_components(edge_type_filter=[10])
        assert comps[a] == comps[b]
        assert comps[b] != comps[c]

    def test_node_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(2, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        comps = db.connected_components(node_type_filter=[1])
        assert a in comps
        assert c in comps
        assert b not in comps

    def test_after_flush(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        db.flush()
        c = db.upsert_node(1, "c")
        db.upsert_edge(b, c, 10)
        comps = db.connected_components()
        assert comps[a] == comps[b] == comps[c]

    def test_after_compaction(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        db.flush()
        c = db.upsert_node(1, "c")
        db.upsert_edge(b, c, 10)
        db.flush()
        db.compact()
        comps = db.connected_components()
        assert comps[a] == comps[b] == comps[c]

    def test_close_reopen(self, tmp_dir):
        import os
        path = os.path.join(tmp_dir, "reopen")
        db = OverGraph.open(path)
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        db.flush()
        db.close()

        db2 = OverGraph.open(path)
        comps = db2.connected_components()
        assert len(comps) == 2
        assert len(set(comps.values())) == 1
        db2.close()

    def test_empty_graph(self, db):
        comps = db.connected_components()
        assert comps == {}

    def test_deterministic(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(c, b, 10)
        db.upsert_edge(b, a, 10)
        comps1 = db.connected_components()
        comps2 = db.connected_components()
        assert comps1 == comps2
        assert comps1[a] == a  # min ID

    def test_prune_policy(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b", weight=0.1)
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        db.set_prune_policy("low", max_weight=0.5)
        comps = db.connected_components()
        assert b not in comps
        assert comps[a] == a
        assert comps[c] == c


class TestComponentOf:
    def test_basic(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        d = db.upsert_node(1, "d")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        members = db.component_of(a)
        assert sorted(members) == sorted([a, b, c])
        assert db.component_of(d) == [d]

    def test_missing_node(self, db):
        assert db.component_of(999999) == []

    def test_deleted_node(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        db.delete_node(a)
        assert db.component_of(a) == []
        assert db.component_of(b) == [b]

    def test_edge_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 20)
        members = db.component_of(a, edge_type_filter=[10])
        assert sorted(members) == sorted([a, b])

    def test_node_type_filter(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(2, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        members = db.component_of(a, node_type_filter=[1])
        assert members == [a]

    def test_start_excluded_by_type_filter(self, db):
        a = db.upsert_node(1, "a")
        assert db.component_of(a, node_type_filter=[99]) == []

    def test_undirected_reachability(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        # B should find A via undirected walk
        members = db.component_of(b)
        assert sorted(members) == sorted([a, b])

    def test_self_loop(self, db):
        a = db.upsert_node(1, "a")
        db.upsert_edge(a, a, 10)
        assert db.component_of(a) == [a]

    def test_after_flush(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, b, 10)
        db.flush()
        c = db.upsert_node(1, "c")
        db.upsert_edge(b, c, 10)
        assert sorted(db.component_of(a)) == sorted([a, b, c])

    def test_prune_policy(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b", weight=0.1)
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        db.set_prune_policy("low", max_weight=0.5)
        assert db.component_of(a) == [a]
        assert db.component_of(b) == []
        assert db.component_of(c) == [c]

    def test_agrees_with_wcc(self, db):
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        d = db.upsert_node(1, "d")
        e = db.upsert_node(1, "e")
        db.upsert_edge(a, b, 10)
        db.upsert_edge(b, c, 10)
        db.upsert_edge(d, e, 10)

        comps = db.connected_components()
        for node in [a, b, c, d, e]:
            members = db.component_of(node)
            comp_id = comps[node]
            for member in members:
                assert comps[member] == comp_id
            wcc_count = sum(1 for v in comps.values() if v == comp_id)
            assert wcc_count == len(members)


class TestConnectedComponentsAsync:
    @pytest.mark.asyncio
    async def test_basic(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        c = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(a, b, 10)
        comps = await async_db.connected_components()
        assert comps[a] == comps[b]
        assert comps[c] == c

    @pytest.mark.asyncio
    async def test_component_of(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(a, b, 10)
        members = await async_db.component_of(a)
        assert sorted(members) == sorted([a, b])

    @pytest.mark.asyncio
    async def test_missing_node(self, async_db):
        assert await async_db.component_of(999999) == []
