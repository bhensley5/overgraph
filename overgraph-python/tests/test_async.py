"""Tests for AsyncOverGraph wrapper."""

import asyncio
import os
import shutil
import tempfile

import pytest
import pytest_asyncio
from overgraph import AsyncOverGraph, OverGraphError


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
                nid = await db.upsert_node("Person", "test")
                assert nid > 0
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestAsyncCrud:
    @pytest.mark.asyncio
    async def test_catalog_apis(self, async_db):
        person_id = await async_db.ensure_node_label("Person")
        company_id = await async_db.ensure_node_label("Company")
        relates_to_id = await async_db.ensure_edge_label("RELATES_TO")
        works_at_id = await async_db.ensure_edge_label("WORKS_AT")

        assert await async_db.ensure_node_label("Person") == person_id
        assert await async_db.ensure_edge_label("RELATES_TO") == relates_to_id
        assert await async_db.get_node_label_id("Person") == person_id
        assert await async_db.get_node_label_id("Company") == company_id
        assert await async_db.get_edge_label_id("RELATES_TO") == relates_to_id
        assert await async_db.get_edge_label_id("WORKS_AT") == works_at_id
        assert await async_db.get_node_label(person_id) == "Person"
        assert await async_db.get_node_label(company_id) == "Company"
        assert await async_db.get_edge_label(relates_to_id) == "RELATES_TO"
        assert await async_db.get_edge_label(works_at_id) == "WORKS_AT"
        assert await async_db.get_edge_label(label_id=relates_to_id) == "RELATES_TO"
        assert await async_db.get_node_label_id("Document") is None
        assert await async_db.get_edge_label_id("LIKES") is None
        assert await async_db.get_node_label(999999) is None
        assert await async_db.get_edge_label(999999) is None

        old_field_name = "type" + "_id"
        with pytest.raises(TypeError):
            await async_db.get_edge_label(**{old_field_name: relates_to_id})

        node_labels = {
            entry.label: entry.label_id
            for entry in await async_db.list_node_labels()
        }
        edge_label_entries = await async_db.list_edge_labels()
        edge_labels = {
            entry.label: entry.label_id
            for entry in edge_label_entries
        }
        assert node_labels["Person"] == person_id
        assert node_labels["Company"] == company_id
        assert edge_labels["RELATES_TO"] == relates_to_id
        assert edge_labels["WORKS_AT"] == works_at_id
        for entry in edge_label_entries:
            assert not hasattr(entry, old_field_name)
        assert "label_id=" in repr(edge_label_entries[0])

    @pytest.mark.asyncio
    async def test_upsert_get_node(self, async_db):
        nid = await async_db.upsert_node("Person", "hello", props={"x": 42})
        node = await async_db.get_node(nid)
        assert node is not None
        assert node.key == "hello"
        assert node.labels == ["Person"]
        assert await async_db.add_node_label(nid, "Admin") is True
        assert await async_db.add_node_label(nid, "Admin") is False
        assert (await async_db.get_node(nid)).labels == ["Person", "Admin"]
        assert await async_db.remove_node_label(nid, "Admin") is True
        assert await async_db.remove_node_label(nid, "Admin") is False

    @pytest.mark.asyncio
    async def test_node_label_failure_paths(self, async_db):
        solo = await async_db.upsert_node("Person", "solo")
        with pytest.raises(OverGraphError, match="last node label"):
            await async_db.remove_node_label(solo, "Person")
        assert (await async_db.get_node(solo)).labels == ["Person"]

        alice = await async_db.upsert_node("Person", "shared")
        other = await async_db.upsert_node("Admin", "shared")
        with pytest.raises(OverGraphError, match="node key conflict"):
            await async_db.add_node_label(alice, "Admin")

        assert (await async_db.get_node(alice)).labels == ["Person"]
        assert (await async_db.get_node(other)).labels == ["Admin"]

    @pytest.mark.asyncio
    async def test_upsert_get_edge(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        eid = await async_db.upsert_edge(n1, n2, "RELATES_TO", weight=2.5)
        edge = await async_db.get_edge(eid)
        assert edge is not None
        assert edge.from_id == n1
        assert edge.to_id == n2

    @pytest.mark.asyncio
    async def test_delete_node(self, async_db):
        nid = await async_db.upsert_node("Person", "bye")
        await async_db.delete_node(nid)
        assert await async_db.get_node(nid) is None

    @pytest.mark.asyncio
    async def test_delete_edge(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        eid = await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.delete_edge(eid)
        assert await async_db.get_edge(eid) is None

    @pytest.mark.asyncio
    async def test_get_node_by_key(self, async_db):
        nid = await async_db.upsert_node("Person", "mykey")
        node = await async_db.get_node_by_key("Person", "mykey")
        assert node is not None
        assert node.id == nid

    @pytest.mark.asyncio
    async def test_get_edge_by_triple(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        eid = await async_db.upsert_edge(n1, n2, "RELATES_TO")
        edge = await async_db.get_edge_by_triple(n1, n2, "RELATES_TO")
        assert edge is not None
        assert edge.id == eid

    @pytest.mark.asyncio
    async def test_invalidate_edge(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        eid = await async_db.upsert_edge(n1, n2, "RELATES_TO")
        result = await async_db.invalidate_edge(eid, 1000)
        assert result is not None  # returns updated EdgeView


@pytest.mark.asyncio
async def test_async_schema_management_wrapper_parity(async_db):
    schema = {
        "properties": {
            "name": {"required": True, "nullable": False, "types": ["string"]}
        }
    }

    info = await async_db.set_node_schema(
        "AsyncSchemaPerson",
        schema,
        max_violations=1,
        chunk_size=1,
        scan_limit=None,
    )
    assert info.label == "AsyncSchemaPerson"
    assert info.schema["properties"]["name"]["required"] is True

    report = await async_db.check_node_schema(
        "AsyncSchemaPerson",
        schema,
        max_violations=100,
        chunk_size=2,
        scan_limit=None,
    )
    assert report.checked_records == 0
    assert report.violation_count == 0

    with pytest.raises(OverGraphError, match="schema violation"):
        await async_db.upsert_node("AsyncSchemaPerson", "bad", props={})

    assert (await async_db.get_node_schema("AsyncSchemaPerson")).label == "AsyncSchemaPerson"
    assert [item.label for item in await async_db.list_node_schemas()] == ["AsyncSchemaPerson"]
    assert await async_db.drop_node_schema("AsyncSchemaPerson") is True
    assert await async_db.get_node_schema("AsyncSchemaPerson") is None

    edge_info = await async_db.set_edge_schema(
        "ASYNC_SCHEMA_EDGE",
        {"properties": {"role": {"required": True, "nullable": False, "types": ["string"]}}},
    )
    assert edge_info.label == "ASYNC_SCHEMA_EDGE"
    edge_report = await async_db.check_edge_schema(
        "ASYNC_SCHEMA_EDGE",
        {"properties": {"role": {"required": True, "nullable": False, "types": ["string"]}}},
        max_violations=100,
        chunk_size=2,
        scan_limit=None,
    )
    assert edge_report.checked_records == 0
    assert edge_report.violation_count == 0
    assert (await async_db.get_edge_schema("ASYNC_SCHEMA_EDGE")).label == "ASYNC_SCHEMA_EDGE"
    assert [item.label for item in await async_db.list_edge_schemas()] == ["ASYNC_SCHEMA_EDGE"]
    assert await async_db.drop_edge_schema("ASYNC_SCHEMA_EDGE") is True


@pytest.mark.asyncio
async def test_async_bulk_graph_schema_management_wrapper_parity(async_db):
    schema = {
        "node_schemas": [
            {
                "label": "AsyncBulkPerson",
                "schema": {
                    "properties": {
                        "name": {"required": True, "nullable": False, "types": ["string"]}
                    }
                },
            }
        ],
        "edge_schemas": [{"label": "ASYNC_BULK_EDGE", "schema": {"properties": {}}}],
    }
    published = await async_db.set_graph_schema(schema)
    assert published.operation == "set"
    assert published.targets_published == 2
    assert [item.label for item in await async_db.list_node_schemas()] == ["AsyncBulkPerson"]

    added = await async_db.alter_graph_schema(
        [
            {
                "kind": "set_node",
                "label": "AsyncBulkCompany",
                "schema": {"properties": {}},
            }
        ]
    )
    assert added.operation == "add"
    assert added.targets_published == 1
    assert [item.label for item in await async_db.list_node_schemas()] == [
        "AsyncBulkCompany",
        "AsyncBulkPerson",
    ]

    check = await async_db.check_graph_schema_add(
        {
            "node_schemas": [
                {"label": "AsyncBulkDryRun", "schema": {"properties": {}}}
            ]
        }
    )
    assert check.operation == "check_add"
    assert check.entries[0].label == "AsyncBulkDryRun"
    assert await async_db.get_node_schema("AsyncBulkDryRun") is None

    check_set = await async_db.check_graph_schema_set({"node_schemas": []})
    assert check_set.operation == "check_set"
    assert check_set.entries == []
    assert [item.label for item in await async_db.list_node_schemas()] == [
        "AsyncBulkCompany",
        "AsyncBulkPerson",
    ]

    dropped = await async_db.alter_graph_schema(
        [
            {"kind": "drop_node", "label": "AsyncBulkCompany"},
            {"kind": "drop_node", "label": "AsyncBulkPerson"},
            {"kind": "drop_edge", "label": "ASYNC_BULK_MISSING"},
        ]
    )
    assert [target.action for target in dropped.drop_targets] == [
        "dropped",
        "dropped",
        "not_found",
    ]
    assert dropped.targets_dropped == 2
    assert await async_db.get_node_schema("AsyncBulkCompany") is None
    assert await async_db.get_node_schema("AsyncBulkPerson") is None

    drop_all = await async_db.drop_graph_schema()
    assert drop_all.edge_schemas_dropped == 1
    assert await async_db.list_edge_schemas() == []


class TestAsyncBatch:
    @pytest.mark.asyncio
    async def test_batch_upsert_nodes(self, async_db):
        nodes = [{"labels": ["Person"], "key": f"n{i}"} for i in range(5)]
        ids = await async_db.batch_upsert_nodes(nodes)
        assert len(ids) == 5

    @pytest.mark.asyncio
    async def test_batch_upsert_edges(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        edges = [
            {"from_id": n1, "to_id": n2, "label": "RELATES_TO"},
            {"from_id": n2, "to_id": n3, "label": "RELATES_TO"},
        ]
        ids = await async_db.batch_upsert_edges(edges)
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_get_nodes(self, async_db):
        nids = [await async_db.upsert_node("Person", f"n{i}") for i in range(3)]
        nodes = await async_db.get_nodes(nids)
        assert len(nodes) == 3
        assert all(n is not None for n in nodes)

    @pytest.mark.asyncio
    async def test_get_edges(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        eid = await async_db.upsert_edge(n1, n2, "RELATES_TO")
        edges = await async_db.get_edges([eid])
        assert len(edges) == 1
        assert edges[0] is not None
        assert edges[0].from_id == n1

    @pytest.mark.asyncio
    async def test_get_nodes_by_keys(self, async_db):
        await async_db.upsert_node("Person", "alice")
        await async_db.upsert_node("Person", "bob")
        results = await async_db.get_nodes_by_keys([
            {"labels": ["Person"], "key": "alice"},
            {"labels": ["Person"], "key": "bob"},
            {"labels": ["Person"], "key": "missing"},
        ])
        assert len(results) == 3
        assert results[0].key == "alice"
        assert results[1].key == "bob"
        assert results[2] is None

    @pytest.mark.asyncio
    async def test_graph_patch(self, async_db):
        result = await async_db.graph_patch({
            "upsert_nodes": [
                {"labels": ["Person"], "key": "a"},
                {"labels": ["Person"], "key": "b"},
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
                    "labels": ["Person"],
                    "key": "alice",
                    "props": {"name": "Alice"},
                },
                {"op": "upsert_node", "alias": "bob", "labels": ["Person"], "key": "bob"},
                {
                    "op": "upsert_edge",
                    "alias": "knows",
                    "from": {"local": "alice"},
                    "to": {"local": "bob"},
                    "label": "KNOWS",
                },
            ]
        )

        staged = await txn.get_node({"local": "alice"})
        assert staged is not None
        assert staged["id"] is None
        assert staged["labels"] == ["Person"]
        assert staged["props"]["name"] == "Alice"

        result = await txn.commit()
        assert result.node_aliases["alice"] == result.node_ids[0]
        assert result.node_aliases["bob"] == result.node_ids[1]
        assert result.edge_aliases["knows"] == result.edge_ids[0]
        assert await async_db.get_node(result.node_aliases["alice"]) is not None

    @pytest.mark.asyncio
    async def test_async_builders_and_rollback(self, async_db):
        txn = await async_db.begin_write_txn()
        alice = await txn.upsert_node_as("alice", "Person", "alice", props={"mood": "staged"})
        bob = await txn.upsert_node_as("bob", "Person", "bob")
        await txn.upsert_edge_as("knows", alice, bob, "KNOWS")

        staged = await txn.get_node_by_key("Person", "alice")
        assert staged is not None
        assert staged["props"]["mood"] == "staged"

        await txn.rollback()
        assert await async_db.get_node_by_key("Person", "alice") is None

    @pytest.mark.asyncio
    async def test_async_transaction_add_remove_node_label(self, async_db):
        node_id = await async_db.upsert_node("Person", "alice")

        txn = await async_db.begin_write_txn()
        assert await txn.add_node_label({"id": node_id}, "Admin") is True
        assert await txn.add_node_label({"id": node_id}, "Admin") is False
        assert (await txn.get_node({"id": node_id}))["labels"] == ["Person", "Admin"]
        assert await txn.remove_node_label({"id": node_id}, "Admin") is True
        assert await txn.remove_node_label({"id": node_id}, "Admin") is False
        await txn.commit()

        assert (await async_db.get_node(node_id)).labels == ["Person"]

    @pytest.mark.asyncio
    async def test_async_transaction_node_label_failure_paths(self, async_db):
        solo = await async_db.upsert_node("Person", "solo")
        txn = await async_db.begin_write_txn()
        with pytest.raises(OverGraphError, match="last node label"):
            await txn.remove_node_label({"id": solo}, "Person")
        await txn.rollback()

        alice = await async_db.upsert_node("Person", "shared")
        other = await async_db.upsert_node("Admin", "shared")
        conflict_txn = await async_db.begin_write_txn()
        with pytest.raises(OverGraphError, match="node key conflict"):
            await conflict_txn.add_node_label({"id": alice}, "Admin")
        await conflict_txn.rollback()

        assert (await async_db.get_node(alice)).labels == ["Person"]
        assert (await async_db.get_node(other)).labels == ["Admin"]

    @pytest.mark.asyncio
    async def test_async_transaction_operations_preserve_call_order(self, async_db):
        txn = await async_db.begin_write_txn()
        stage_task = asyncio.create_task(
            txn.stage(
                [
                    {
                        "op": "upsert_node",
                        "alias": "queued",
                        "labels": ["Person"],
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
        await async_db.upsert_node("Person", "x", props={"color": "red"})
        ids = await async_db.find_nodes("Person", "color", "red")
        assert len(ids) == 1

    @pytest.mark.asyncio
    async def test_gql_phase34_options_and_compact_rows(self, async_db):
        await async_db.upsert_node(
            "PyAsyncPhase34",
            "ada",
            props={"name": "Ada", "group": "core", "rank": 2},
        )
        await async_db.upsert_node(
            "PyAsyncPhase34",
            "ben",
            props={"name": "Ben", "group": "core", "rank": 1},
        )
        await async_db.upsert_node(
            "PyAsyncPhase34",
            "cy",
            props={"name": "Cy", "group": "ops", "rank": 3},
        )

        options = {
            "max_pipeline_rows": 16,
            "max_groups": 8,
            "max_collect_items": 8,
            "max_union_branches": 4,
            "max_subquery_invocations": 8,
            "max_subquery_depth": 1,
            "max_shortest_path_pairs": 8,
        }
        query = """
            MATCH (n:PyAsyncPhase34)
            WITH n.group AS grp, count(*) AS count, collect(n.name) AS names
            WHERE count > 1
            RETURN grp, count, names
            """
        result = await async_db.execute_gql(query, compact_rows=True, **options)
        assert result["columns"] == ["grp", "count", "names"]
        assert result["rows"] == [["core", 2, ["Ada", "Ben"]]]

        explain = await async_db.explain_gql(query, **options)
        assert explain["read"]["target"] == "graph_pipeline_query"
        for name, value in options.items():
            assert explain["caps"][name] == value

    @pytest.mark.asyncio
    async def test_count_by_type(self, async_db):
        for i in range(3):
            await async_db.upsert_node("Person", f"n{i}")
        count = await async_db.count_nodes_by_labels("Person")
        assert count == 3


class TestAsyncTraversal:
    @pytest.mark.asyncio
    async def test_neighbors(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        nbrs = await async_db.neighbors(n1, direction="outgoing")
        assert len(nbrs) == 1
        assert nbrs[0].node_id == n2

    @pytest.mark.asyncio
    async def test_top_k_neighbors(self, async_db):
        center = await async_db.upsert_node("Person", "center")
        for i in range(3):
            s = await async_db.upsert_node("Person", f"s{i}")
            await async_db.upsert_edge(center, s, "RELATES_TO", weight=float(i + 1))
        top = await async_db.top_k_neighbors(center, k=2, scoring="weight")
        assert len(top) == 2

    @pytest.mark.asyncio
    async def test_extract_subgraph(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        sg = await async_db.extract_subgraph(n1, 1)
        assert len(sg.nodes) == 2
        assert len(sg.edges) == 1


class TestAsyncRetention:
    @pytest.mark.asyncio
    async def test_prune(self, async_db):
        await async_db.upsert_node("Person", "low", weight=0.1)
        await async_db.upsert_node("Person", "high", weight=5.0)
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
        await async_db.upsert_node("Person", "a")
        await async_db.upsert_node("Person", "b")
        # Use a wide range to catch all nodes
        ids = await async_db.find_nodes_by_time_range("Person", 0, 2**53)
        assert len(ids) == 2


class TestAsyncMaintenance:
    @pytest.mark.asyncio
    async def test_sync_flush(self, async_db):
        await async_db.upsert_node("Person", "a")
        await async_db.sync()
        result = await async_db.flush()
        assert result is not None

    @pytest.mark.asyncio
    async def test_compact(self, async_db):
        await async_db.upsert_node("Person", "a")
        await async_db.flush()
        await async_db.upsert_node("Person", "b")
        await async_db.flush()
        result = await async_db.compact()
        assert result is not None


class TestAsyncPagination:
    @pytest.mark.asyncio
    async def test_nodes_by_labels_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node("Person", f"n{i}")
        page = await async_db.nodes_by_labels_paged("Person", limit=3)
        assert len(page.items) == 3
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_neighbors_paged(self, async_db):
        center = await async_db.upsert_node("Person", "center")
        for i in range(5):
            s = await async_db.upsert_node("Person", f"s{i}")
            await async_db.upsert_edge(center, s, "RELATES_TO")
        page = await async_db.neighbors_paged(center, direction="outgoing", limit=3)
        assert len(page.items) == 3


class TestAsyncTraversal2:
    @pytest.mark.asyncio
    async def test_traverse(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n2, n3, "RELATES_TO")
        page = await async_db.traverse(n1, 2, min_depth=2, direction="outgoing")
        assert [(hit.node_id, hit.depth) for hit in page.items] == [(n3, 2)]

    @pytest.mark.asyncio
    async def test_traverse_node_label_filter(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Company", "b")
        n3 = await async_db.upsert_node("Document", "c")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n2, n3, "RELATES_TO")
        page = await async_db.traverse(
            n1,
            2,
            min_depth=2,
            direction="outgoing",
            edge_label_filter=["RELATES_TO"],
            emit_node_label_filter={"labels": ["Document"], "mode": "all"},
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
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n1, n3, "WORKS_AT")
        result = await async_db.neighbors_batch([n1])
        assert n1 in result
        assert len(result[n1]) == 2


class TestAsyncQueries2:
    @pytest.mark.asyncio
    async def test_count_edges_by_label(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        count = await async_db.count_edges_by_label("RELATES_TO")
        assert count == 1

    @pytest.mark.asyncio
    async def test_nodes_by_labels(self, async_db):
        await async_db.upsert_node("Person", "a")
        await async_db.upsert_node("Person", "b")
        ids = await async_db.nodes_by_labels("Person")
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_edges_by_label(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        ids = await async_db.edges_by_label("RELATES_TO")
        assert len(ids) == 1

    @pytest.mark.asyncio
    async def test_get_nodes_by_labels(self, async_db):
        await async_db.upsert_node("Person", "a")
        await async_db.upsert_node("Person", "b")
        nodes = await async_db.get_nodes_by_labels("Person")
        assert len(nodes) == 2
        keys = {n.key for n in nodes}
        assert keys == {"a", "b"}

    @pytest.mark.asyncio
    async def test_get_edges_by_label(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        edges = await async_db.get_edges_by_label("RELATES_TO")
        assert len(edges) == 1
        assert edges[0].from_id == n1


class TestAsyncPagination2:
    @pytest.mark.asyncio
    async def test_edges_by_label_paged(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n1, n3, "RELATES_TO")
        await async_db.upsert_edge(n2, n3, "RELATES_TO")
        page = await async_db.edges_by_label_paged("RELATES_TO", limit=2)
        assert len(page.items) == 2
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_get_nodes_by_labels_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node("Person", f"n{i}")
        page = await async_db.get_nodes_by_labels_paged("Person", limit=3)
        assert len(page.items) == 3
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_get_edges_by_label_paged(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n2, n3, "RELATES_TO")
        page = await async_db.get_edges_by_label_paged("RELATES_TO", limit=1)
        assert len(page.items) == 1
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_find_nodes_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node("Person", f"fp{i}", props={"color": "blue"})
        page = await async_db.find_nodes_paged("Person", "color", "blue", limit=3)
        assert len(page.items) == 3
        assert page.next_cursor is not None

    @pytest.mark.asyncio
    async def test_find_nodes_by_time_range_paged(self, async_db):
        for i in range(5):
            await async_db.upsert_node("Person", f"tr{i}")
        page = await async_db.find_nodes_by_time_range_paged("Person", 0, 2**53, limit=3)
        assert len(page.items) == 3

    @pytest.mark.asyncio
    async def test_traverse_paged(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n2, n3, "RELATES_TO")
        page = await async_db.traverse(n1, 2, min_depth=2, direction="outgoing")
        assert [(hit.node_id, hit.depth) for hit in page.items] == [(n3, 2)]

    @pytest.mark.asyncio
    async def test_traverse_cursor_roundtrip(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        n3 = await async_db.upsert_node("Person", "c")
        n4 = await async_db.upsert_node("Person", "d")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        await async_db.upsert_edge(n2, n3, "RELATES_TO")
        await async_db.upsert_edge(n2, n4, "RELATES_TO")
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
        await async_db.upsert_node("Person", "a")
        await async_db.flush()
        await async_db.upsert_node("Person", "b")
        await async_db.flush()
        events = []
        result = await async_db.compact_with_progress(lambda p: (events.append(p) or True))
        assert result is not None
        assert len(events) > 0


class TestAsyncBatch2:
    @pytest.mark.asyncio
    async def test_batch_upsert_nodes_binary(self, async_db):
        import struct
        buf = b"OGNB" + struct.pack("<HI", 2, 2)
        for key in ("a", "b"):
            label = "Person".encode("utf-8")
            kb = key.encode("utf-8")
            buf += struct.pack("<B", 1)
            buf += struct.pack("<H", len(label))
            buf += label
            buf += struct.pack("<fH", 1.0, len(kb))
            buf += kb
            buf += struct.pack("<I", 0)
        ids = await async_db.batch_upsert_nodes_binary(buf)
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_batch_upsert_edges_binary(self, async_db):
        import struct
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        buf = struct.pack("<I", 1)
        label = "RELATES_TO".encode("utf-8")
        buf += struct.pack("<QQH", n1, n2, len(label))
        buf += label
        buf += struct.pack("<fqqI", 1.0, 0, 0, 0)
        ids = await async_db.batch_upsert_edges_binary(buf)
        assert len(ids) == 1


class TestAsyncAnalytics:
    @pytest.mark.asyncio
    async def test_personalized_pagerank(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        result = await async_db.personalized_pagerank([n1])
        assert len(result.node_ids) > 0
        assert n1 in result.node_ids

    @pytest.mark.asyncio
    async def test_personalized_pagerank_approx(self, async_db):
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
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
        n1 = await async_db.upsert_node("Person", "a")
        n2 = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(n1, n2, "RELATES_TO")
        export = await async_db.export_adjacency()
        assert len(export.node_ids) == 2
        assert len(export.edges) == 1


class TestAsyncDegree:
    @pytest.mark.asyncio
    async def test_degree(self, async_db):
        a = await async_db.upsert_node("Person", "a")
        b = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(a, b, "RELATES_TO", weight=5.0)
        assert await async_db.degree(a) == 1
        assert await async_db.degree(b) == 0

    @pytest.mark.asyncio
    async def test_sum_edge_weights(self, async_db):
        a = await async_db.upsert_node("Person", "a")
        b = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(a, b, "RELATES_TO", weight=5.0)
        s = await async_db.sum_edge_weights(a)
        assert abs(s - 5.0) < 1e-6

    @pytest.mark.asyncio
    async def test_avg_edge_weight(self, async_db):
        a = await async_db.upsert_node("Person", "a")
        b = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(a, b, "RELATES_TO", weight=5.0)
        avg = await async_db.avg_edge_weight(a)
        assert avg is not None
        assert abs(avg - 5.0) < 1e-6
        assert await async_db.avg_edge_weight(999999) is None

    @pytest.mark.asyncio
    async def test_degrees_batch(self, async_db):
        a = await async_db.upsert_node("Person", "a")
        b = await async_db.upsert_node("Person", "b")
        await async_db.upsert_edge(a, b, "RELATES_TO")
        result = await async_db.degrees([a, b])
        assert isinstance(result, dict)
        assert result[a] == 1
