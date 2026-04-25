"""Tests for AsyncOverGraph wrapper."""

import asyncio
import os
import shutil
import tempfile

import pytest
import pytest_asyncio
from overgraph import AsyncOverGraph


@pytest_asyncio.fixture
async def async_db():
    d = tempfile.mkdtemp(prefix="egtest_async_")
    path = os.path.join(d, "testdb")
    db = await AsyncOverGraph.open(path)
    yield db
    try:
        await db.close()
    except Exception:
        pass
    shutil.rmtree(d, ignore_errors=True)


class TestAsyncLifecycle:
    @pytest.mark.asyncio
    async def test_open_close(self):
        d = tempfile.mkdtemp(prefix="egtest_async_")
        path = os.path.join(d, "testdb")
        try:
            db = await AsyncOverGraph.open(path)
            s = await db.stats()
            assert s.segment_count == 0
            await db.close()
        finally:
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_context_manager(self):
        d = tempfile.mkdtemp(prefix="egtest_async_")
        path = os.path.join(d, "testdb")
        try:
            async with await AsyncOverGraph.open(path) as db:
                nid = await db.upsert_node(1, "test")
                assert nid > 0
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestAsyncCrud:
    @pytest.mark.asyncio
    async def test_upsert_get_node(self, async_db):
        nid = await async_db.upsert_node(1, "hello", props={"x": 42})
        node = await async_db.get_node(nid)
        assert node is not None
        assert node.key == "hello"
        assert node.type_id == 1

    @pytest.mark.asyncio
    async def test_upsert_get_edge(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        eid = await async_db.upsert_edge(n1, n2, 10, weight=2.5)
        edge = await async_db.get_edge(eid)
        assert edge is not None
        assert edge.from_id == n1
        assert edge.to_id == n2

    @pytest.mark.asyncio
    async def test_delete_node(self, async_db):
        nid = await async_db.upsert_node(1, "bye")
        await async_db.delete_node(nid)
        assert await async_db.get_node(nid) is None

    @pytest.mark.asyncio
    async def test_delete_edge(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        eid = await async_db.upsert_edge(n1, n2, 10)
        await async_db.delete_edge(eid)
        assert await async_db.get_edge(eid) is None

    @pytest.mark.asyncio
    async def test_get_node_by_key(self, async_db):
        nid = await async_db.upsert_node(1, "mykey")
        node = await async_db.get_node_by_key(1, "mykey")
        assert node is not None
        assert node.id == nid

    @pytest.mark.asyncio
    async def test_get_edge_by_triple(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        eid = await async_db.upsert_edge(n1, n2, 10)
        edge = await async_db.get_edge_by_triple(n1, n2, 10)
        assert edge is not None
        assert edge.id == eid

    @pytest.mark.asyncio
    async def test_invalidate_edge(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        eid = await async_db.upsert_edge(n1, n2, 10)
        result = await async_db.invalidate_edge(eid, 1000)
        assert result is not None  # returns updated EdgeRecord


class TestAsyncBatch:
    @pytest.mark.asyncio
    async def test_batch_upsert_nodes(self, async_db):
        nodes = [{"type_id": 1, "key": f"n{i}"} for i in range(5)]
        ids = await async_db.batch_upsert_nodes(nodes)
        assert len(ids) == 5

    @pytest.mark.asyncio
    async def test_batch_upsert_edges(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        edges = [
            {"from_id": n1, "to_id": n2, "type_id": 10},
            {"from_id": n2, "to_id": n3, "type_id": 10},
        ]
        ids = await async_db.batch_upsert_edges(edges)
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_get_nodes(self, async_db):
        nids = [await async_db.upsert_node(1, f"n{i}") for i in range(3)]
        nodes = await async_db.get_nodes(nids)
        assert len(nodes) == 3
        assert all(n is not None for n in nodes)

    @pytest.mark.asyncio
    async def test_get_edges(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        eid = await async_db.upsert_edge(n1, n2, 10)
        edges = await async_db.get_edges([eid])
        assert len(edges) == 1
        assert edges[0] is not None
        assert edges[0].from_id == n1

    @pytest.mark.asyncio
    async def test_get_nodes_by_keys(self, async_db):
        await async_db.upsert_node(1, "alice")
        await async_db.upsert_node(1, "bob")
        results = await async_db.get_nodes_by_keys([(1, "alice"), (1, "bob"), (1, "missing")])
        assert len(results) == 3
        assert results[0].key == "alice"
        assert results[1].key == "bob"
        assert results[2] is None

    @pytest.mark.asyncio
    async def test_graph_patch(self, async_db):
        result = await async_db.graph_patch({
            "upsert_nodes": [
                {"type_id": 1, "key": "a"},
                {"type_id": 1, "key": "b"},
            ],
        })
        assert len(result.node_ids) == 2


class TestAsyncTransactions:
    @pytest.mark.asyncio
    async def test_async_stage_read_and_commit(self, async_db):
        txn = await async_db.begin_write_txn()
        await txn.stage(
            [
                {
                    "op": "upsert_node",
                    "alias": "alice",
                    "type_id": 1,
                    "key": "alice",
                    "props": {"name": "Alice"},
                },
                {"op": "upsert_node", "alias": "bob", "type_id": 1, "key": "bob"},
                {
                    "op": "upsert_edge",
                    "alias": "knows",
                    "from": {"local": "alice"},
                    "to": {"local": "bob"},
                    "type_id": 7,
                },
            ]
        )

        staged = await txn.get_node({"local": "alice"})
        assert staged is not None
        assert staged["id"] is None
        assert staged["props"]["name"] == "Alice"

        result = await txn.commit()
        assert result.node_aliases["alice"] == result.node_ids[0]
        assert result.node_aliases["bob"] == result.node_ids[1]
        assert result.edge_aliases["knows"] == result.edge_ids[0]
        assert await async_db.get_node(result.node_aliases["alice"]) is not None

    @pytest.mark.asyncio
    async def test_async_builders_and_rollback(self, async_db):
        txn = await async_db.begin_write_txn()
        alice = await txn.upsert_node_as("alice", 1, "alice", props={"mood": "staged"})
        bob = await txn.upsert_node_as("bob", 1, "bob")
        await txn.upsert_edge_as("knows", alice, bob, 9)

        staged = await txn.get_node_by_key(1, "alice")
        assert staged is not None
        assert staged["props"]["mood"] == "staged"

        await txn.rollback()
        assert await async_db.get_node_by_key(1, "alice") is None

    @pytest.mark.asyncio
    async def test_async_transaction_operations_preserve_call_order(self, async_db):
        txn = await async_db.begin_write_txn()
        stage_task = asyncio.create_task(
            txn.stage(
                [
                    {
                        "op": "upsert_node",
                        "alias": "queued",
                        "type_id": 1,
                        "key": "queued",
                    }
                ]
            )
        )
        read_task = asyncio.create_task(txn.get_node({"local": "queued"}))
        commit_task = asyncio.create_task(txn.commit())

        await stage_task
        staged = await read_task
        result = await commit_task

        assert staged is not None
        assert staged["local"] == "queued"
        assert result.node_aliases["queued"] == result.node_ids[0]


class TestAsyncQueries:
    @pytest.mark.asyncio
    async def test_find_nodes(self, async_db):
        await async_db.upsert_node(1, "x", props={"color": "red"})
        ids = await async_db.find_nodes(1, "color", "red")
        assert len(ids) == 1

    @pytest.mark.asyncio
    async def test_count_by_type(self, async_db):
        for i in range(3):
            await async_db.upsert_node(1, f"n{i}")
        count = await async_db.count_nodes_by_type(1)
        assert count == 3


class TestAsyncTraversal:
    @pytest.mark.asyncio
    async def test_neighbors(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        nbrs = await async_db.neighbors(n1, direction="outgoing")
        assert len(nbrs) == 1
        assert nbrs[0].node_id == n2

    @pytest.mark.asyncio
    async def test_top_k_neighbors(self, async_db):
        center = await async_db.upsert_node(1, "center")
        for i in range(3):
            s = await async_db.upsert_node(1, f"s{i}")
            await async_db.upsert_edge(center, s, 10, weight=float(i + 1))
        top = await async_db.top_k_neighbors(center, k=2, scoring="weight")
        assert len(top) == 2

    @pytest.mark.asyncio
    async def test_extract_subgraph(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        sg = await async_db.extract_subgraph(n1, 1)
        assert len(sg.nodes) == 2
        assert len(sg.edges) == 1


class TestAsyncRetention:
    @pytest.mark.asyncio
    async def test_prune(self, async_db):
        await async_db.upsert_node(1, "low", weight=0.1)
        await async_db.upsert_node(1, "high", weight=5.0)
        result = await async_db.prune(max_weight=0.5)
        assert result.nodes_pruned == 1

    @pytest.mark.asyncio
    async def test_prune_policies(self, async_db):
        await async_db.set_prune_policy("p1", max_weight=0.5)
        policies = await async_db.list_prune_policies()
        assert len(policies) == 1
        removed = await async_db.remove_prune_policy("p1")
        assert removed is True


class TestAsyncTimeRange:
    @pytest.mark.asyncio
    async def test_find_nodes_by_time_range(self, async_db):
        await async_db.upsert_node(1, "a")
        await async_db.upsert_node(1, "b")
        # Use a wide range to catch all nodes
        ids = await async_db.find_nodes_by_time_range(1, 0, 2**53)
        assert len(ids) == 2


class TestAsyncMaintenance:
    @pytest.mark.asyncio
    async def test_sync_flush(self, async_db):
        await async_db.upsert_node(1, "a")
        await async_db.sync()
        result = await async_db.flush()
        assert result is not None

    @pytest.mark.asyncio
    async def test_compact(self, async_db):
        await async_db.upsert_node(1, "a")
        await async_db.flush()
        await async_db.upsert_node(1, "b")
        await async_db.flush()
        result = await async_db.compact()
        assert result is not None


class TestAsyncPagination:
    @pytest.mark.asyncio
    async def test_nodes_by_type_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node(1, f"n{i}")
        page = await async_db.nodes_by_type_paged(1, limit=3)
        assert len(page.items) == 3
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_neighbors_paged(self, async_db):
        center = await async_db.upsert_node(1, "center")
        for i in range(5):
            s = await async_db.upsert_node(1, f"s{i}")
            await async_db.upsert_edge(center, s, 10)
        page = await async_db.neighbors_paged(center, direction="outgoing", limit=3)
        assert len(page.items) == 3


class TestAsyncTraversal2:
    @pytest.mark.asyncio
    async def test_traverse(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n2, n3, 10)
        page = await async_db.traverse(n1, 2, min_depth=2, direction="outgoing")
        assert [(hit.node_id, hit.depth) for hit in page.items] == [(n3, 2)]

    @pytest.mark.asyncio
    async def test_traverse_node_type_filter(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(2, "b")
        n3 = await async_db.upsert_node(3, "c")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n2, n3, 10)
        page = await async_db.traverse(
            n1,
            2,
            min_depth=2,
            direction="outgoing",
            edge_type_filter=[10],
            node_type_filter=[3],
        )
        assert [(hit.node_id, hit.depth) for hit in page.items] == [(n3, 2)]

    @pytest.mark.asyncio
    async def test_removed_two_hop_async_apis_stay_absent(self, async_db):
        assert not hasattr(async_db, "neighbors_2hop")
        assert not hasattr(async_db, "neighbors_2hop_paged")
        assert not hasattr(async_db, "neighbors_2hop_constrained")
        assert not hasattr(async_db, "neighbors_2hop_constrained_paged")

    @pytest.mark.asyncio
    async def test_neighbors_batch(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n1, n3, 20)
        result = await async_db.neighbors_batch([n1])
        assert n1 in result
        assert len(result[n1]) == 2


class TestAsyncQueries2:
    @pytest.mark.asyncio
    async def test_count_edges_by_type(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        count = await async_db.count_edges_by_type(10)
        assert count == 1

    @pytest.mark.asyncio
    async def test_nodes_by_type(self, async_db):
        await async_db.upsert_node(1, "a")
        await async_db.upsert_node(1, "b")
        ids = await async_db.nodes_by_type(1)
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_edges_by_type(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        ids = await async_db.edges_by_type(10)
        assert len(ids) == 1

    @pytest.mark.asyncio
    async def test_get_nodes_by_type(self, async_db):
        await async_db.upsert_node(1, "a")
        await async_db.upsert_node(1, "b")
        nodes = await async_db.get_nodes_by_type(1)
        assert len(nodes) == 2
        keys = {n.key for n in nodes}
        assert keys == {"a", "b"}

    @pytest.mark.asyncio
    async def test_get_edges_by_type(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        edges = await async_db.get_edges_by_type(10)
        assert len(edges) == 1
        assert edges[0].from_id == n1


class TestAsyncPagination2:
    @pytest.mark.asyncio
    async def test_edges_by_type_paged(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n1, n3, 10)
        await async_db.upsert_edge(n2, n3, 10)
        page = await async_db.edges_by_type_paged(10, limit=2)
        assert len(page.items) == 2
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_get_nodes_by_type_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node(1, f"n{i}")
        page = await async_db.get_nodes_by_type_paged(1, limit=3)
        assert len(page.items) == 3
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_get_edges_by_type_paged(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n2, n3, 10)
        page = await async_db.get_edges_by_type_paged(10, limit=1)
        assert len(page.items) == 1
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_find_nodes_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node(1, f"fp{i}", props={"color": "blue"})
        page = await async_db.find_nodes_paged(1, "color", "blue", limit=3)
        assert len(page.items) == 3
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_find_nodes_by_time_range_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node(1, f"tr{i}")
        page = await async_db.find_nodes_by_time_range_paged(1, 0, 2**53, limit=3)
        assert len(page.items) == 3

    @pytest.mark.asyncio
    async def test_traverse_paged(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n2, n3, 10)
        page = await async_db.traverse(n1, 2, min_depth=2, direction="outgoing")
        assert [(hit.node_id, hit.depth) for hit in page.items] == [(n3, 2)]

    @pytest.mark.asyncio
    async def test_traverse_cursor_roundtrip(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        n3 = await async_db.upsert_node(1, "c")
        n4 = await async_db.upsert_node(1, "d")
        await async_db.upsert_edge(n1, n2, 10)
        await async_db.upsert_edge(n2, n3, 10)
        await async_db.upsert_edge(n2, n4, 10)
        p1 = await async_db.traverse(n1, 2, min_depth=2, direction="outgoing", limit=1)
        assert len(p1.items) == 1
        assert p1.next_cursor is not None
        p2 = await async_db.traverse(
            n1,
            2,
            min_depth=2,
            direction="outgoing",
            limit=1,
            cursor=p1.next_cursor,
        )
        assert len(p2.items) == 1
        assert p1.items[0].node_id != p2.items[0].node_id


class TestAsyncMaintenance2:
    @pytest.mark.asyncio
    async def test_compact_with_progress(self, async_db):
        await async_db.upsert_node(1, "a")
        await async_db.flush()
        await async_db.upsert_node(1, "b")
        await async_db.flush()
        events = []
        result = await async_db.compact_with_progress(lambda p: (events.append(p) or True))
        assert result is not None
        assert len(events) > 0


class TestAsyncBatch2:
    @pytest.mark.asyncio
    async def test_batch_upsert_nodes_binary(self, async_db):
        import struct
        import json
        buf = struct.pack("<I", 2)
        for key in ("a", "b"):
            kb = key.encode("utf-8")
            buf += struct.pack("<IfH", 1, 1.0, len(kb))
            buf += kb
            buf += struct.pack("<I", 0)
        ids = await async_db.batch_upsert_nodes_binary(buf)
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_batch_upsert_edges_binary(self, async_db):
        import struct
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        buf = struct.pack("<I", 1)
        buf += struct.pack("<QQIf", n1, n2, 10, 1.0)
        buf += struct.pack("<qq", 0, 0)
        buf += struct.pack("<I", 0)
        ids = await async_db.batch_upsert_edges_binary(buf)
        assert len(ids) == 1


class TestAsyncAnalytics:
    @pytest.mark.asyncio
    async def test_personalized_pagerank(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        result = await async_db.personalized_pagerank([n1])
        assert len(result.node_ids) > 0
        assert n1 in result.node_ids

    @pytest.mark.asyncio
    async def test_personalized_pagerank_approx(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        result = await async_db.personalized_pagerank(
            [n1],
            algorithm="approx",
            approx_residual_tolerance=1e-6,
        )
        assert result.algorithm == "approx"
        assert result.approx is not None
        assert result.approx.residual_tolerance == 1e-6

    @pytest.mark.asyncio
    async def test_export_adjacency(self, async_db):
        n1 = await async_db.upsert_node(1, "a")
        n2 = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(n1, n2, 10)
        export = await async_db.export_adjacency()
        assert len(export.node_ids) == 2
        assert len(export.edges) == 1


class TestAsyncDegree:
    @pytest.mark.asyncio
    async def test_degree(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(a, b, 10, weight=5.0)
        assert await async_db.degree(a) == 1
        assert await async_db.degree(b) == 0

    @pytest.mark.asyncio
    async def test_sum_edge_weights(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(a, b, 10, weight=5.0)
        s = await async_db.sum_edge_weights(a)
        assert abs(s - 5.0) < 1e-6

    @pytest.mark.asyncio
    async def test_avg_edge_weight(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(a, b, 10, weight=5.0)
        avg = await async_db.avg_edge_weight(a)
        assert avg is not None
        assert abs(avg - 5.0) < 1e-6
        assert await async_db.avg_edge_weight(999999) is None

    @pytest.mark.asyncio
    async def test_degrees_batch(self, async_db):
        a = await async_db.upsert_node(1, "a")
        b = await async_db.upsert_node(1, "b")
        await async_db.upsert_edge(a, b, 10)
        result = await async_db.degrees([a, b])
        assert isinstance(result, dict)
        assert result[a] == 1
