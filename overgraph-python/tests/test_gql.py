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
    assert "sort" in full_scan["plan"]["row_ops"]
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
    assert explain["target"] == "graph_row_query"
    assert "sort" in explain["row_ops"]
    assert explain["columns"] == ["n.name"]


@pytest.mark.asyncio
async def test_gql_explain_async(async_db):
    await seed_async(async_db)
    explain = await async_db.explain_gql(
        "MATCH (n:Person) WHERE n.status = 'active' RETURN n.name ORDER BY n.rank LIMIT 1",
        include_plan=True,
    )
    assert explain["target"] == "graph_row_query"
    assert explain["columns"] == ["n.name"]


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
    assert "cursor: str | None" in text
    assert "max_cursor_bytes: int | None" in text
    assert "async def execute_gql" in text
    assert "class GqlExplain" in text
