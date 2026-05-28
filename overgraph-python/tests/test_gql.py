import inspect
import os

import pytest

from overgraph import AsyncOverGraph, OverGraph


def seed(db, include_vectors=False):
    ada_kwargs = {
        "props": {"name": "Ada", "status": "active", "rank": 2, "group": "core"},
    }
    if include_vectors:
        ada_kwargs["dense_vector"] = [0.1, 0.2, 0.3]
        ada_kwargs["sparse_vector"] = [(7, 1.5)]
    ada = db.upsert_node("Person", "ada", **ada_kwargs)
    ben = db.upsert_node(
        "Person",
        "ben",
        props={"name": "Ben", "status": "active", "rank": 1, "group": "core"},
    )
    cy = db.upsert_node(
        "Person",
        "cy",
        props={"name": "Cy", "status": "inactive", "rank": 3, "group": "ops"},
    )
    acme = db.upsert_node("Company", "acme", props={"name": "Acme"})
    works_at = db.upsert_edge(ada, acme, "WORKS_AT", props={"role": "engineer", "since": 2020})
    return {"ada": ada, "ben": ben, "cy": cy, "acme": acme, "works_at": works_at}


async def seed_async(db):
    ada = await db.upsert_node(
        "Person",
        "ada",
        props={"name": "Ada", "status": "active", "rank": 2, "group": "core"},
    )
    ben = await db.upsert_node(
        "Person",
        "ben",
        props={"name": "Ben", "status": "active", "rank": 1, "group": "core"},
    )
    cy = await db.upsert_node(
        "Person",
        "cy",
        props={"name": "Cy", "status": "inactive", "rank": 3, "group": "ops"},
    )
    acme = await db.upsert_node("Company", "acme", props={"name": "Acme"})
    works_at = await db.upsert_edge(
        ada,
        acme,
        "WORKS_AT",
        props={"role": "engineer", "since": 2020},
    )
    return {"ada": ada, "ben": ben, "cy": cy, "acme": acme, "works_at": works_at}


def open_vector_db(tmp_dir):
    return OverGraph.open(os.path.join(tmp_dir, "gql_vector_db"), dense_vector_dimension=3)


def test_execute_gql_params_and_bytes_round_trip(db):
    seed(db)
    result = db.execute_gql(
        """
        MATCH (n:Person {name: $name})
        RETURN $nil AS nil, $flag AS flag, $neg AS neg, $pos AS pos,
               $float AS float, $text AS text, $blob AS blob,
               $list AS list, $map AS map
        """,
        {
            "name": "Ada",
            "nil": None,
            "flag": True,
            "neg": -7,
            "pos": 9,
            "float": 1.25,
            "text": "ok",
            "blob": b"\x01\x02\x03",
            "list": [False, 4, "x"],
            "map": {"nested": "value", "bytes": b"b"},
        },
    )

    assert result["kind"] == "query"
    assert result["next_cursor"] is None
    assert result["mutation_stats"] is None
    assert result["plan"] is None
    assert result["columns"] == ["nil", "flag", "neg", "pos", "float", "text", "blob", "list", "map"]
    row = result["rows"][0]
    assert row["nil"] is None
    assert row["flag"] is True
    assert row["neg"] == -7
    assert row["pos"] == 9
    assert row["float"] == 1.25
    assert row["text"] == "ok"
    assert row["blob"] == b"\x01\x02\x03"
    assert row["list"] == [False, 4, "x"]
    assert row["map"]["bytes"] == b"b"


@pytest.mark.asyncio
async def test_async_execute_gql_and_compact_row_parity(async_db):
    await seed_async(async_db)
    query = """
        MATCH (n:Person)
        RETURN n.name AS name, n.rank AS rank
        ORDER BY n.rank SKIP 1 LIMIT 1
    """
    object_rows = await async_db.execute_gql(query)
    compact_rows = await async_db.execute_gql(query, compact_rows=True)

    assert object_rows["rows"] == [{"name": "Ada", "rank": 2}]
    assert compact_rows["columns"] == object_rows["columns"]
    assert compact_rows["rows"] == [["Ada", 2]]
    assert compact_rows["stats"]["rows_returned"] == object_rows["stats"]["rows_returned"]
    assert object_rows["kind"] == "query"
    assert compact_rows["kind"] == "query"


def test_gql_node_edge_and_vector_option(tmp_dir):
    db = open_vector_db(tmp_dir)
    try:
        ids = seed(db, include_vectors=True)
        default_node = db.execute_gql("MATCH (n:Person) WHERE n.name = 'Ada' RETURN n")["rows"][0]["n"]
        assert default_node["id"] == ids["ada"]
        assert default_node["labels"] == ["Person"]
        assert default_node["props"]["name"] == "Ada"
        assert "dense_vector" not in default_node
        assert "sparse_vector" not in default_node

        vector_node = db.execute_gql(
            "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n",
            include_vectors=True,
        )["rows"][0]["n"]
        assert vector_node["dense_vector"] == pytest.approx([0.1, 0.2, 0.3])
        assert vector_node["sparse_vector"] == [(7, 1.5)]

        edge = db.execute_gql(
            "MATCH (a:Person)-[r:WORKS_AT]->(c:Company) WHERE a.name = 'Ada' RETURN r"
        )["rows"][0]["r"]
        assert edge["id"] == ids["works_at"]
        assert edge["from_id"] == ids["ada"]
        assert edge["to_id"] == ids["acme"]
        assert edge["label"] == "WORKS_AT"
        assert edge["props"]["role"] == "engineer"
    finally:
        db.close()


def test_gql_caps_full_scan_row_ops_and_profile(db):
    ids = seed(db)

    with pytest.raises(Exception, match="full[- ]scan|allow_full_scan"):
        db.execute_gql("MATCH (n) RETURN id(n) AS id")

    full_scan = db.execute_gql(
        "MATCH (n) RETURN id(n) AS id ORDER BY id(n) LIMIT 10",
        allow_full_scan=True,
        include_plan=True,
        profile=True,
    )
    assert sorted(row["id"] for row in full_scan["rows"]) == sorted(
        [ids["ada"], ids["ben"], ids["cy"], ids["acme"]]
    )
    assert full_scan["plan"]["caps"]["allow_full_scan"] is True
    assert full_scan["plan"]["kind"] == "query"
    assert "sort" in full_scan["plan"]["read"]["row_ops"]
    assert full_scan["plan"]["mutation"] is None
    assert isinstance(full_scan["stats"]["elapsed_us"], int)
    assert full_scan["stats"]["rows_returned"] == 4

    with pytest.raises(Exception, match="max_skip|SKIP|skip"):
        db.execute_gql("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 100001")

    capped_explain = db.explain_gql(
        "MATCH (n:Person) RETURN id(n) LIMIT 1",
        max_query_bytes=128,
        max_param_bytes=9,
        max_ast_depth=4,
        max_literal_items=3,
    )
    assert capped_explain["caps"]["max_query_bytes"] == 128
    assert capped_explain["caps"]["max_param_bytes"] == 9
    assert capped_explain["caps"]["max_ast_depth"] == 4
    assert capped_explain["caps"]["max_literal_items"] == 3

    unused_oversized = db.execute_gql(
        "MATCH (n:Person) RETURN id(n) LIMIT 1",
        {"unused": [1, 2, 3]},
        max_literal_items=1,
    )
    assert len(unused_oversized["rows"]) == 1

    with pytest.raises(Exception, match="max_literal_items"):
        db.execute_gql(
            "MATCH (n:Person) RETURN $ids LIMIT 0",
            {"ids": [1, 2]},
            max_literal_items=1,
        )
    with pytest.raises(Exception, match="max_ast_depth"):
        db.execute_gql(
            "MATCH (n:Person) RETURN $payload LIMIT 0",
            {"payload": [[1]]},
            max_ast_depth=1,
        )
    with pytest.raises(Exception, match="max_param_bytes"):
        db.execute_gql(
            "MATCH (n:Person) RETURN $payload LIMIT 0",
            {"payload": "toolong"},
            max_param_bytes=4,
        )
    with pytest.raises(Exception, match="max_param_bytes"):
        db.execute_gql(
            "MATCH (n:Person) RETURN $payload LIMIT 0",
            {"payload": b"\x01\x02\x03"},
            max_param_bytes=2,
        )
    with pytest.raises(Exception, match="max_param_bytes"):
        db.execute_gql(
            "MATCH (n:Person) RETURN $payload LIMIT 0",
            {"payload": {"oversized": 1}},
            max_param_bytes=4,
        )

    boundary_bytes = db.execute_gql(
        "MATCH (n:Person) RETURN $payload AS payload LIMIT 1",
        {"payload": {"abc": "de"}},
        max_param_bytes=5,
        max_ast_depth=1,
        max_literal_items=1,
    )
    assert boundary_bytes["rows"][0]["payload"] == {"abc": "de"}


def test_gql_explain_sync(db):
    seed(db)
    explain = db.explain_gql(
        "MATCH (n:Person) WHERE n.status = 'active' RETURN n.name ORDER BY n.rank LIMIT 1",
        include_plan=True,
        profile=True,
    )
    assert explain["kind"] == "query"
    assert explain["read"]["target"] == "graph_row_query"
    assert "sort" in explain["read"]["row_ops"]
    assert explain["mutation"] is None
    assert explain["columns"] == ["n.name"]


@pytest.mark.asyncio
async def test_gql_explain_async(async_db):
    await seed_async(async_db)
    explain = await async_db.explain_gql(
        "MATCH (n:Person) WHERE n.status = 'active' RETURN n.name ORDER BY n.rank LIMIT 1",
        include_plan=True,
    )
    assert explain["kind"] == "query"
    assert explain["read"]["target"] == "graph_row_query"
    assert explain["mutation"] is None
    assert explain["columns"] == ["n.name"]


def test_gql_sync_create_return_mutation_stats_bytes_and_plan(db):
    result = db.execute_gql(
        """
        CREATE (n:PyCreateReturn {key: 'created-one', name: $name, payload: $payload})
        RETURN n.key AS key, n.name AS name, n.payload AS payload, n
        """,
        {"name": "Created", "payload": b"\x09\x08\x07"},
        include_plan=True,
    )

    assert result["kind"] == "mutation"
    assert result["columns"] == ["key", "name", "payload", "n"]
    assert result["next_cursor"] is None
    assert len(result["rows"]) == 1
    assert result["rows"][0]["payload"] == b"\x09\x08\x07"
    assert result["rows"][0]["n"]["key"] == "created-one"
    assert result["rows"][0]["n"]["props"]["name"] == "Created"
    assert result["mutation_stats"]["rows_matched"] == 1
    assert result["mutation_stats"]["mutation_rows"] == 1
    assert result["mutation_stats"]["nodes_created"] == 1
    assert result["mutation_stats"]["mutation_ops"] == 1
    assert result["plan"]["kind"] == "mutation"
    assert result["plan"]["read"] is None
    assert result["plan"]["mutation"]["uses_write_txn"] is True
    assert result["plan"]["mutation"]["return_plan"]["columns"] == result["columns"]


def test_gql_sync_set_remove_return_row_ops(db):
    db.upsert_node("PySetRemoveReturn", "a", props={"rank": 1, "group": "old", "status": "old"})
    db.upsert_node("PySetRemoveReturn", "b", props={"rank": 2, "group": "old", "status": "old"})
    db.upsert_node("PySetRemoveReturn", "c", props={"rank": 3, "group": "old", "status": "old"})

    result = db.execute_gql(
        """
        MATCH (n:PySetRemoveReturn)
        SET n.status = $status
        REMOVE n.group
        RETURN n.key AS key, n.status AS status, n.group AS group
        ORDER BY n.rank SKIP 1 LIMIT 1
        """,
        {"status": "new"},
    )

    assert result["kind"] == "mutation"
    assert result["rows"] == [{"key": "b", "status": "new", "group": None}]
    assert result["mutation_stats"]["rows_matched"] == 3
    assert result["mutation_stats"]["mutation_rows"] == 3
    assert result["mutation_stats"]["nodes_updated"] == 3
    assert result["mutation_stats"]["properties_set"] == 3
    assert result["mutation_stats"]["properties_removed"] == 3


def test_gql_delete_and_detach_delete_no_return_stats(db):
    source = db.upsert_node("PyDeleteSource", "source")
    target = db.upsert_node("PyDeleteTarget", "target")
    db.upsert_edge(source, target, "PY_DELETE_ME")

    edge_delete = db.execute_gql(
        """
        MATCH (a:PyDeleteSource)-[r:PY_DELETE_ME]->(b:PyDeleteTarget)
        DELETE r
        """
    )
    assert edge_delete["kind"] == "mutation"
    assert edge_delete["rows"] == []
    assert edge_delete["mutation_stats"]["edges_deleted"] == 1
    assert edge_delete["mutation_stats"]["mutation_ops"] == 1

    hub = db.upsert_node("PyDetachDelete", "hub")
    leaf = db.upsert_node("PyDetachDelete", "leaf")
    db.upsert_edge(hub, leaf, "PY_DETACH_ME")

    detach_delete = db.execute_gql(
        """
        MATCH (n:PyDetachDelete)
        WHERE n.key = 'hub'
        DETACH DELETE n
        """
    )
    assert detach_delete["kind"] == "mutation"
    assert detach_delete["rows"] == []
    assert detach_delete["mutation_stats"]["nodes_deleted"] == 1
    assert detach_delete["mutation_stats"]["edges_deleted"] == 1
    assert detach_delete["mutation_stats"]["mutation_ops"] == 2


def test_gql_read_only_mode_and_mode_validation(db):
    seed(db)
    read = db.execute_gql(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank LIMIT 1",
        mode="read_only",
    )
    assert read["kind"] == "query"
    assert read["rows"] == [{"name": "Ben"}]

    with pytest.raises(Exception, match="read.?only|ReadOnly"):
        db.execute_gql("CREATE (n:PyReadOnly {key: 'blocked'})", mode="read_only")

    with pytest.raises(Exception, match="mode.*auto.*read_only"):
        db.execute_gql("MATCH (n:Person) RETURN n LIMIT 1", mode="readonly")


def test_gql_mutation_compact_rows_and_include_vectors(tmp_dir):
    db = open_vector_db(tmp_dir)
    try:
        seed(db, include_vectors=True)
        compact = db.execute_gql(
            """
            CREATE (n:PyCompactMutation {key: 'compact', name: 'Compact'})
            RETURN n.key AS key, n.name AS name
            """,
            compact_rows=True,
        )
        assert compact["kind"] == "mutation"
        assert compact["columns"] == ["key", "name"]
        assert compact["rows"] == [["compact", "Compact"]]

        vector_node = db.execute_gql(
            """
            MATCH (n:Person {name: 'Ada'})
            SET n.status = 'vector-return'
            RETURN n
            """,
            include_vectors=True,
        )["rows"][0]["n"]
        assert vector_node["dense_vector"] == pytest.approx([0.1, 0.2, 0.3])
        assert vector_node["sparse_vector"] == [(7, 1.5)]
    finally:
        db.close()


def test_gql_mutation_explain_is_side_effect_free(db):
    seed(db)
    explain = db.explain_gql(
        """
        MATCH (n:Person {name: 'Ada'})
        SET n.planned = 'explained'
        RETURN n.planned AS planned
        """
    )
    assert explain["kind"] == "mutation"
    assert explain["columns"] == ["planned"]
    assert explain["read"]["target"] == "graph_row_query"
    assert explain["mutation"]["uses_transaction_snapshot"] is True
    assert explain["mutation"]["uses_write_txn"] is True
    assert explain["mutation"]["atomic_commit"] is True
    assert any(op["op"] == "SET PROPERTY" for op in explain["mutation"]["operations"])
    assert explain["mutation"]["return_plan"]["columns"] == ["planned"]

    unchanged = db.execute_gql("MATCH (n:Person {name: 'Ada'}) RETURN n.planned AS planned")
    assert unchanged["rows"] == [{"planned": None}]


@pytest.mark.asyncio
async def test_async_execute_gql_mutation_and_explain(async_db):
    result = await async_db.execute_gql(
        """
        CREATE (n:PyAsyncMutation {key: 'once', name: 'Async'})
        RETURN n.name AS name
        """
    )
    assert result["kind"] == "mutation"
    assert result["rows"] == [{"name": "Async"}]
    assert result["mutation_stats"]["nodes_created"] == 1

    read_back = await async_db.execute_gql(
        "MATCH (n:PyAsyncMutation) WHERE n.key = 'once' RETURN n.name AS name"
    )
    assert read_back["rows"] == [{"name": "Async"}]

    await async_db.upsert_node(
        "PyAsyncUpdate",
        "target",
        props={"status": "old", "drop": "remove-me"},
    )
    update = await async_db.execute_gql(
        """
        MATCH (n:PyAsyncUpdate)
        WHERE n.key = 'target'
        SET n.status = 'new'
        REMOVE n.drop
        RETURN n.status AS status, n.drop AS dropped
        """
    )
    assert update["kind"] == "mutation"
    assert update["rows"] == [{"status": "new", "dropped": None}]
    assert update["mutation_stats"]["nodes_updated"] == 1
    assert update["mutation_stats"]["properties_set"] == 1
    assert update["mutation_stats"]["properties_removed"] == 1

    hub = await async_db.upsert_node("PyAsyncDetach", "hub")
    leaf = await async_db.upsert_node("PyAsyncDetach", "leaf")
    await async_db.upsert_edge(hub, leaf, "PY_ASYNC_DETACH")
    detached = await async_db.execute_gql(
        """
        MATCH (n:PyAsyncDetach)
        WHERE n.key = 'hub'
        DETACH DELETE n
        """
    )
    assert detached["kind"] == "mutation"
    assert detached["rows"] == []
    assert detached["mutation_stats"]["nodes_deleted"] == 1
    assert detached["mutation_stats"]["edges_deleted"] == 1

    with pytest.raises(Exception, match="read.?only|ReadOnly"):
        await async_db.execute_gql(
            "CREATE (n:PyAsyncReadOnly {key: 'blocked'})",
            mode="read_only",
        )

    explain = await async_db.explain_gql(
        "CREATE (n:PyAsyncExplain {key: 'planned'}) RETURN n.key AS key"
    )
    assert explain["kind"] == "mutation"
    assert explain["read"] is None
    assert explain["mutation"]["return_plan"]["columns"] == ["key"]


def test_gql_volatile_metadata_order_by_rejection_surfaces(db):
    with pytest.raises(Exception, match="ORDER BY|commit|metadata|before commit|volatile"):
        db.execute_gql("CREATE (n:PyVolatileOrder {key: 'bad-order'}) RETURN n.key ORDER BY n.id")


def test_gql_forwards_mutation_order_and_path_caps(db):
    seed(db)
    with pytest.raises(Exception, match="max_mutation_rows"):
        db.execute_gql("CREATE (n:PyCapMutationRows {key: 'row-cap'})", max_mutation_rows=0)

    with pytest.raises(Exception, match="max_mutation_ops"):
        db.execute_gql("CREATE (n:PyCapMutationOps {key: 'op-cap'})", max_mutation_ops=0)

    with pytest.raises(
        Exception,
        match="max_order_materialization|order materialization",
    ):
        db.execute_gql(
            "MATCH (n:Person) SET n.cap_probe = true RETURN n.name ORDER BY n.name",
            max_order_materialization=1,
        )

    with pytest.raises(Exception, match="max_path_hops|path hops|upper bound"):
        db.execute_gql(
            """
            MATCH p = (a:Person)-[:WORKS_AT*1..1]->(c:Company)
            WHERE a.name = 'Ada'
            RETURN p
            """,
            max_path_hops=0,
        )


def test_gql_rejects_deferred_or_unsupported_syntax(db):
    seed(db)
    with pytest.raises(Exception, match="ORDER BY|scalar|labels"):
        db.execute_gql("MATCH (n:Person) RETURN n ORDER BY labels(n)")


def test_gql_optional_vlp_paths_and_cursor_through_python(db):
    ids = seed(db)
    ben = ids["ben"]
    extra = db.upsert_node("Company", "extra", props={"name": "Extra"})
    extra_edge = db.upsert_edge(ben, extra, "WORKS_AT", props={"role": "analyst"})
    knows = db.upsert_edge(ids["ada"], ben, "KNOWS")

    optional = db.execute_gql(
        f"""
        MATCH (p:Person)
        WHERE id(p) = {ben}
        OPTIONAL MATCH (p)-[r:REPORTS_TO]->(m:Person)
        RETURN id(p) AS p, id(r) AS r, id(m) AS m
        """
    )
    assert optional["rows"] == [{"p": ben, "r": None, "m": None}]

    path = db.execute_gql(
        f"""
        MATCH path = (a)-[:KNOWS*1..1]->(b)
        WHERE id(a) = {ids["ada"]}
        RETURN path, node_ids(path) AS node_ids, edge_ids(path) AS edge_ids
        """
    )
    row = path["rows"][0]
    assert row["path"]["node_ids"] == [ids["ada"], ben]
    assert row["path"]["edge_ids"] == [knows]
    assert row["node_ids"] == [ids["ada"], ben]
    assert row["edge_ids"] == [knows]

    cursor_query = (
        "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
        "RETURN id(c) AS c ORDER BY id(c) LIMIT 2"
    )
    first = db.execute_gql(cursor_query, max_rows=1)
    assert first["next_cursor"]
    second = db.execute_gql(
        cursor_query,
        cursor=first["next_cursor"],
        max_rows=1,
    )
    assert [first["rows"][0]["c"], second["rows"][0]["c"]] == sorted([ids["acme"], extra])

    with pytest.raises(Exception, match="cursor|max_cursor_bytes|too large|exceeds"):
        db.execute_gql(
            cursor_query,
            cursor=first["next_cursor"],
            max_cursor_bytes=4,
        )


def test_gql_stub_and_signature_smoke():
    try:
        signature = str(inspect.signature(OverGraph.execute_gql))
    except (TypeError, ValueError):
        signature = getattr(OverGraph.execute_gql, "__text_signature__", "")
    assert "query" in signature
    assert "params" in signature

    stub_path = os.path.join(os.path.dirname(__file__), "..", "python", "overgraph", "__init__.pyi")
    with open(stub_path, encoding="utf-8") as stub:
        text = stub.read()
    assert "def execute_gql" in text
    assert "mode: Literal[\"auto\", \"read_only\"]" in text
    assert "cursor: str | None" in text
    assert "max_cursor_bytes: int | None" in text
    assert "max_mutation_rows: int | None" in text
    assert "class GqlExecutionResult" in text
    assert "class GqlExecutionExplain" in text
    assert "class GqlMutationStats" in text
    assert "async def execute_gql" in text
    assert "gql_query" not in text
    assert "explain_gql_query" not in text
    assert "class GqlResult" not in text
    assert "truncated" not in text
