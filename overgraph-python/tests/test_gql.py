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


def property_index_row_field(key):
    return {"source": "property", "key": key}


def metadata_index_row_field(field):
    return {"source": "metadata", "field": field}


def property_index_explain_field(key):
    return {"source": "property", "key": key, "field": None}


def metadata_index_explain_field(field):
    return {"source": "metadata", "key": None, "field": field}


def seed_phase34(db, person_label="PyPhase34Person"):
    ada = db.upsert_node(
        person_label,
        "ada",
        props={
            "name": "Ada",
            "status": "active",
            "rank": 2,
            "group": "core",
            "age": 37,
            "target": "acct-a",
        },
    )
    ben = db.upsert_node(
        person_label,
        "ben",
        props={
            "name": "Ben",
            "status": "active",
            "rank": 1,
            "group": "core",
            "age": 29,
            "target": "acct-a",
        },
    )
    cy = db.upsert_node(
        person_label,
        "cy",
        props={
            "name": "Cy",
            "status": "inactive",
            "rank": 3,
            "group": "ops",
            "age": 41,
            "target": "acct-c",
        },
    )
    acme = db.upsert_node("PyPhase34Company", "acme", props={"name": "Acme"})
    knows_ab = db.upsert_edge(ada, ben, "PY34_KNOWS", props={"weight": 1})
    knows_bc = db.upsert_edge(ben, cy, "PY34_KNOWS", props={"weight": 1})
    works_at = db.upsert_edge(ada, acme, "PY34_WORKS_AT", props={"role": "engineer"})
    return {
        "ada": ada,
        "ben": ben,
        "cy": cy,
        "acme": acme,
        "knows_ab": knows_ab,
        "knows_bc": knows_bc,
        "works_at": works_at,
    }


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
    assert result["schema_stats"] is None
    assert result["index_stats"] is None
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


def test_gql_schema_execute_explain_and_tagged_rows(tmp_dir):
    path = os.path.join(tmp_dir, "gql_schema_db")
    db = OverGraph.open(path)
    try:
        alter = db.execute_gql(
            """
            ALTER CURRENT GRAPH TYPE SET {
              NODE Tagged = {
                properties: {
                  payload: {
                    enum_values: [
                      { type: 'uint', value: '18446744073709551615' },
                      { type: 'bytes', value: [0, 1, 255] }
                    ]
                  }
                }
              }
            }
            """,
            include_plan=True,
        )
        assert alter["kind"] == "schema"
        assert alter["mutation_stats"] is None
        assert alter["index_stats"] is None
        assert alter["schema_stats"]["operation"] == "alter_graph_type_set"
        assert alter["schema_stats"]["targets_published"] == 1
        assert alter["plan"]["kind"] == "schema"
        assert alter["plan"]["read"] is None
        assert alter["plan"]["mutation"] is None
        assert alter["plan"]["index"] is None
        assert alter["plan"]["schema"]["operation"] == "alter_graph_type_set"
        assert alter["plan"]["schema"]["uses_core_write_queue"] is True

        show = db.execute_gql("SHOW CURRENT GRAPH TYPE")
        assert show["kind"] == "schema"
        assert show["schema_stats"]["operation"] == "show_current_graph_type"
        assert show["rows"][0]["target_kind"] == "node"
        enum_values = show["rows"][0]["schema"]["properties"]["payload"]["enum_values"]
        assert enum_values[0] == {
            "type": "uint",
            "value": "18446744073709551615",
        }
        assert enum_values[1] == {"type": "bytes", "value": [0, 1, 255]}

        db.upsert_node("PyNeedsName", "missing")
        check = db.execute_gql(
            """
            CHECK CURRENT GRAPH TYPE ADD {
              NODE PyNeedsName = {
                properties: {
                  name: { required: true, nullable: false, types: ['string'] }
                }
              }
            }
            """
        )
        assert check["kind"] == "schema"
        assert check["schema_stats"]["operation"] == "check_graph_type_add"
        assert check["schema_stats"]["violation_count"] == 1
        assert check["rows"][0]["violations"][0]["target"]["id"] == {
            "type": "uint",
            "value": "1",
        }
        assert db.get_node_schema("PyNeedsName") is None

        explain = db.explain_gql(
            """
            CHECK CURRENT GRAPH TYPE SET {
              NODE Tagged = { properties: {} }
            }
            """
        )
        assert explain["kind"] == "schema"
        assert explain["read"] is None
        assert explain["mutation"] is None
        assert explain["index"] is None
        assert explain["schema"]["operation"] == "check_graph_type_set"
        assert explain["schema"]["side_effect_free"] is True

        read = db.execute_gql("MATCH (n:PyNeedsName) RETURN elementKey(n) AS key")
        assert read["kind"] == "query"
        assert read["schema_stats"] is None
        assert read["index_stats"] is None

        mutation = db.execute_gql(
            "CREATE (n:PyUnconstrained {elementKey: 'one'}) RETURN elementKey(n) AS key"
        )
        assert mutation["kind"] == "mutation"
        assert mutation["schema_stats"] is None
        assert mutation["index_stats"] is None
    finally:
        db.close()


def test_gql_property_index_connector_payloads(tmp_dir):
    path = os.path.join(tmp_dir, "gql_index_db")
    db = OverGraph.open(path)
    try:
        person_index = "CREATE PROPERTY INDEX FOR (n:PyIndexPerson) ON (n.status) KIND EQUALITY"
        edge_index = (
            "CREATE PROPERTY INDEX FOR ()-[r:PY_INDEX_WORKS_AT]-() ON (r.since) KIND RANGE"
        )
        compound_index = (
            "CREATE PROPERTY INDEX FOR (n:PyIndexPerson) ON (n.group, updatedAt(n)) KIND RANGE"
        )
        drop_edge = (
            "DROP PROPERTY INDEX FOR ()-[r:PY_INDEX_WORKS_AT]-() ON (r.since) KIND RANGE"
        )

        create = db.execute_gql(person_index)
        assert create["kind"] == "index"
        assert create["columns"] == [
            "operation",
            "target_kind",
            "label",
            "fields",
            "kind",
            "action",
            "state",
            "index_id",
            "last_error",
            "compound",
            "field_count",
        ]
        assert create["mutation_stats"] is None
        assert create["schema_stats"] is None
        assert create["index_stats"]["operation"] == "create_property_index"
        assert create["index_stats"]["indexes_ensured"] == 1
        assert create["index_stats"]["indexes_dropped"] == 0
        assert create["index_stats"]["indexes_returned"] == 0
        assert create["index_stats"]["elapsed_us"] is None
        assert create["index_stats"]["warnings"] == []
        assert create["rows"][0]["operation"] == "create_property_index"
        assert create["rows"][0]["target_kind"] == "node"
        assert create["rows"][0]["label"] == "PyIndexPerson"
        assert create["rows"][0]["fields"] == [property_index_row_field("status")]
        assert create["rows"][0]["kind"] == "equality"
        assert create["rows"][0]["action"] == "ensured"
        assert create["rows"][0]["state"] in {"building", "ready", "failed"}
        assert isinstance(create["rows"][0]["index_id"], int)
        assert create["rows"][0]["last_error"] is None
        assert create["rows"][0]["compound"] is False
        assert create["rows"][0]["field_count"] == 1

        planned = db.execute_gql(person_index, include_plan=True, profile=True)
        assert isinstance(planned["index_stats"]["elapsed_us"], int)
        assert planned["plan"]["kind"] == "index"
        assert planned["plan"]["read"] is None
        assert planned["plan"]["mutation"] is None
        assert planned["plan"]["schema"] is None
        assert planned["plan"]["index"]["operation"] == "create_property_index"
        assert planned["plan"]["index"]["targets"] == [
            {
                "target_kind": "node",
                "label": "PyIndexPerson",
                "fields": [property_index_explain_field("status")],
                "kind": "equality",
                "action": "ensure",
                "compound": False,
            }
        ]
        assert planned["plan"]["index"]["uses_core_write_queue"] is True
        assert planned["plan"]["index"]["publishes_manifest"] is True
        assert planned["plan"]["index"]["creates_labels"] is True
        assert planned["plan"]["index"]["schedules_background_build"] is True
        assert planned["plan"]["index"]["drops_index_data_async"] is False
        assert planned["plan"]["index"]["side_effect_free"] is False

        create_explain = db.explain_gql(person_index)
        assert create_explain["kind"] == "index"
        assert create_explain["read"] is None
        assert create_explain["mutation"] is None
        assert create_explain["schema"] is None
        assert create_explain["index"]["operation"] == "create_property_index"
        assert create_explain["index"]["targets"][0] == planned["plan"]["index"]["targets"][0]

        edge_create = db.execute_gql(edge_index)
        assert edge_create["kind"] == "index"
        assert edge_create["index_stats"]["operation"] == "create_property_index"
        assert edge_create["index_stats"]["indexes_ensured"] == 1
        assert edge_create["mutation_stats"] is None
        assert edge_create["schema_stats"] is None

        compound_create = db.execute_gql(compound_index)
        assert compound_create["kind"] == "index"
        assert compound_create["rows"][0]["fields"] == [
            property_index_row_field("group"),
            metadata_index_row_field("updatedAt"),
        ]
        assert compound_create["rows"][0]["compound"] is True
        assert compound_create["rows"][0]["field_count"] == 2

        compound_explain = db.explain_gql(compound_index)
        assert compound_explain["index"]["targets"][0] == {
            "target_kind": "node",
            "label": "PyIndexPerson",
            "fields": [
                property_index_explain_field("group"),
                metadata_index_explain_field("updatedAt"),
            ],
            "kind": "range",
            "action": "ensure",
            "compound": True,
        }

        show = db.execute_gql("SHOW PROPERTY INDEXES")
        assert show["kind"] == "index"
        assert show["index_stats"]["operation"] == "show_property_indexes"
        assert show["index_stats"]["indexes_returned"] == 3
        assert [
            (
                row["target_kind"],
                row["label"],
                row["fields"],
                row["kind"],
                row["compound"],
                row["field_count"],
            )
            for row in show["rows"]
        ] == [
            (
                "node",
                "PyIndexPerson",
                [property_index_row_field("group"), metadata_index_row_field("updatedAt")],
                "range",
                True,
                2,
            ),
            ("node", "PyIndexPerson", [property_index_row_field("status")], "equality", False, 1),
            ("edge", "PY_INDEX_WORKS_AT", [property_index_row_field("since")], "range", False, 1),
        ]
        assert all(row["state"] in {"building", "ready", "failed"} for row in show["rows"])
        assert all(row["last_error"] is None for row in show["rows"])

        show_explain = db.explain_gql("SHOW PROPERTY INDEXES")
        assert show_explain["index"]["operation"] == "show_property_indexes"
        assert show_explain["index"]["targets"] == [
            {
                "target_kind": "property_index_catalog",
                "label": None,
                "fields": [],
                "kind": None,
                "action": "show",
                "compound": False,
            }
        ]
        assert show_explain["index"]["uses_core_write_queue"] is False
        assert show_explain["index"]["publishes_manifest"] is False
        assert show_explain["index"]["creates_labels"] is False
        assert show_explain["index"]["schedules_background_build"] is False
        assert show_explain["index"]["drops_index_data_async"] is False
        assert show_explain["index"]["side_effect_free"] is True

        drop_explain = db.explain_gql(drop_edge)
        assert drop_explain["index"]["operation"] == "drop_property_index"
        assert drop_explain["index"]["targets"] == [
            {
                "target_kind": "edge",
                "label": "PY_INDEX_WORKS_AT",
                "fields": [property_index_explain_field("since")],
                "kind": "range",
                "action": "drop",
                "compound": False,
            }
        ]
        assert drop_explain["index"]["uses_core_write_queue"] is True
        assert drop_explain["index"]["publishes_manifest"] is True
        assert drop_explain["index"]["creates_labels"] is False
        assert drop_explain["index"]["schedules_background_build"] is False
        assert drop_explain["index"]["drops_index_data_async"] is True
        assert drop_explain["index"]["side_effect_free"] is False

        with pytest.raises(Exception, match="GQL index management is not allowed in ReadOnly mode"):
            db.execute_gql(person_index, mode="read_only")
        with pytest.raises(Exception, match="GQL index management is not allowed in ReadOnly mode"):
            db.execute_gql(drop_edge, mode="read_only")
        read_only_show = db.execute_gql("SHOW PROPERTY INDEXES", mode="read_only")
        assert len(read_only_show["rows"]) == 3
        with pytest.raises(Exception, match="GQL index statements do not accept cursors"):
            db.execute_gql("SHOW PROPERTY INDEXES", cursor="locked")
    finally:
        db.close()


@pytest.mark.asyncio
async def test_async_gql_property_index_payloads(tmp_dir):
    async with await AsyncOverGraph.open(os.path.join(tmp_dir, "gql_index_async_db")) as db:
        await db.execute_gql(
            "CREATE PROPERTY INDEX FOR (n:PyAsyncIndexPerson) ON (n.status) KIND EQUALITY"
        )
        show = await db.execute_gql("SHOW PROPERTY INDEXES")
        assert show["kind"] == "index"
        assert show["index_stats"]["operation"] == "show_property_indexes"
        assert show["index_stats"]["indexes_returned"] == 1
        explain = await db.explain_gql("SHOW PROPERTY INDEXES")
        assert explain["kind"] == "index"
        assert explain["index"]["operation"] == "show_property_indexes"
        assert explain["index"]["targets"][0]["target_kind"] == "property_index_catalog"


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
        max_pipeline_rows=11,
        max_groups=12,
        max_collect_items=13,
        max_union_branches=2,
        max_subquery_invocations=14,
        max_subquery_depth=1,
        max_shortest_path_pairs=15,
    )
    assert capped_explain["caps"]["max_query_bytes"] == 128
    assert capped_explain["caps"]["max_param_bytes"] == 9
    assert capped_explain["caps"]["max_ast_depth"] == 4
    assert capped_explain["caps"]["max_literal_items"] == 3
    assert capped_explain["caps"]["max_pipeline_rows"] == 11
    assert capped_explain["caps"]["max_groups"] == 12
    assert capped_explain["caps"]["max_collect_items"] == 13
    assert capped_explain["caps"]["max_union_branches"] == 2
    assert capped_explain["caps"]["max_subquery_invocations"] == 14
    assert capped_explain["caps"]["max_subquery_depth"] == 1
    assert capped_explain["caps"]["max_shortest_path_pairs"] == 15
    assert capped_explain["index"] is None

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


def test_gql_phase34_with_distinct_aggregation_and_compact_rows(db):
    seed_phase34(db)

    rich = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WITH n.name AS name,
             lower(n.name) AS slug,
             n.rank + 10 AS adjusted,
             CASE WHEN n.age > 35 THEN upper(n.name) ELSE 'young' END AS bucket
        WHERE slug STARTS WITH 'a'
        RETURN name, slug, adjusted, bucket
        """,
        include_plan=True,
    )
    assert rich["rows"] == [
        {"name": "Ada", "slug": "ada", "adjusted": 12, "bucket": "ADA"}
    ]
    assert rich["plan"]["read"]["target"] == "graph_pipeline_query"
    assert any("Project(With)" in item for item in rich["plan"]["read"]["projection"])

    distinct = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WITH DISTINCT n.group AS grp
        RETURN grp
        ORDER BY grp
        """
    )
    assert distinct["rows"] == [{"grp": "core"}, {"grp": "ops"}]

    aggregate = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        RETURN n.group AS grp,
               count(*) AS count,
               sum(n.rank) AS total,
               avg(n.rank) AS avg,
               collect(n.name) AS names
        ORDER BY grp
        """,
        include_plan=True,
    )
    assert aggregate["columns"] == ["grp", "count", "total", "avg", "names"]
    assert aggregate["rows"][0]["grp"] == "core"
    assert aggregate["rows"][0]["count"] == 2
    assert aggregate["rows"][0]["total"] == 3
    assert aggregate["rows"][0]["avg"] == 1.5
    assert sorted(aggregate["rows"][0]["names"]) == ["Ada", "Ben"]
    assert aggregate["rows"][1] == {
        "grp": "ops",
        "count": 1,
        "total": 3,
        "avg": 3.0,
        "names": ["Cy"],
    }
    assert any("Aggregate" in item for item in aggregate["plan"]["read"]["projection"])

    nulls = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WHERE n.missing IS NULL
        RETURN count(n.missing) AS count,
               sum(n.missing) AS total,
               avg(n.missing) AS avg,
               collect(n.missing) AS values
        """
    )
    assert nulls["rows"] == [{"count": 0, "total": None, "avg": None, "values": []}]

    compact = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WITH n.group AS grp, count(*) AS count
        WHERE count > 1
        RETURN grp, count
        """,
        compact_rows=True,
    )
    assert compact["columns"] == ["grp", "count"]
    assert compact["rows"] == [["core", 2]]


def test_gql_phase34_union_and_read_only_subqueries(db):
    seed_phase34(db)

    union_all = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WHERE n.group = 'core'
        RETURN n.name AS name
        ORDER BY name
        UNION ALL
        MATCH (n:PyPhase34Person)
        WHERE n.group = 'ops'
        RETURN n.name AS name
        """
    )
    assert union_all["rows"] == [{"name": "Ada"}, {"name": "Ben"}, {"name": "Cy"}]

    union = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WHERE n.group = 'core'
        RETURN n.group AS grp
        UNION
        MATCH (n:PyPhase34Person)
        RETURN n.group AS grp
        ORDER BY grp
        """,
        include_plan=True,
    )
    assert union["rows"] == [{"grp": "core"}, {"grp": "ops"}]
    assert any("Union" in item for item in union["plan"]["read"]["projection"])

    subquery = db.execute_gql(
        """
        MATCH (n:PyPhase34Person)
        WHERE EXISTS {
          MATCH (n)-[:PY34_WORKS_AT]->(c:PyPhase34Company)
          RETURN c
        }
        WITH n
        CALL {
          MATCH (n)-[:PY34_WORKS_AT]->(c:PyPhase34Company)
          RETURN c.name AS company
        }
        RETURN n.name AS name, company
        """,
        include_plan=True,
    )
    assert subquery["rows"] == [{"name": "Ada", "company": "Acme"}]
    assert any("EXISTS subquery" in note for note in subquery["plan"]["notes"])
    assert any("CallSubquery" in item for item in subquery["plan"]["read"]["projection"])


def test_gql_phase34_shortest_path_and_nested_value_conversion(db):
    ids = seed_phase34(db)

    path_result = db.execute_gql(
        f"""
        MATCH (a:PyPhase34Person)
        WHERE id(a) = {ids["ada"]}
        WITH a
        MATCH (b:PyPhase34Person)
        WHERE id(b) = {ids["cy"]}
        WITH a, b
        MATCH p = shortestPath((a)-[:PY34_KNOWS*1..3]->(b))
        RETURN p,
               nodeIds(p) AS node_ids,
               edgeIds(p) AS edge_ids,
               length(p) AS length,
               nodes(p) AS nodes,
               relationships(p) AS relationships,
               [p] AS path_list,
               {{path: p, nested: [nodeIds(p), {{edges: edgeIds(p)}}]}} AS wrapped
        """,
        include_plan=True,
    )
    row = path_result["rows"][0]
    assert row["p"]["node_ids"] == [ids["ada"], ids["ben"], ids["cy"]]
    assert row["p"]["edge_ids"] == [ids["knows_ab"], ids["knows_bc"]]
    assert row["p"]["nodes"][0]["props"]["name"] == "Ada"
    assert row["p"]["edges"][0]["label"] == "PY34_KNOWS"
    assert row["node_ids"] == [ids["ada"], ids["ben"], ids["cy"]]
    assert row["edge_ids"] == [ids["knows_ab"], ids["knows_bc"]]
    assert row["length"] == 2
    assert row["nodes"] == [ids["ada"], ids["ben"], ids["cy"]]
    assert row["relationships"] == [ids["knows_ab"], ids["knows_bc"]]
    assert row["path_list"][0]["node_ids"] == [ids["ada"], ids["ben"], ids["cy"]]
    assert row["wrapped"]["path"]["edge_ids"] == [ids["knows_ab"], ids["knows_bc"]]
    assert row["wrapped"]["nested"][1]["edges"] == [ids["knows_ab"], ids["knows_bc"]]
    assert any("ShortestPath" in item for item in path_result["plan"]["read"]["projection"])

    collected = db.execute_gql("MATCH (n:PyPhase34Person) RETURN collect(n) AS people")
    assert sorted(collected["rows"][0]["people"]) == sorted([ids["ada"], ids["ben"], ids["cy"]])


def test_gql_phase34_keyed_merge_on_create_on_match_stats(db):
    db.upsert_node("PyPhase34MergeSource", "source", props={"target": "acct-a"})

    query = """
        MATCH (s:PyPhase34MergeSource)
        WITH s.target AS target
        MERGE (a:PyPhase34Account {elementKey: target})
        ON CREATE SET a.status = 'created', a.count = 1
        ON MATCH SET a.status = 'matched', a.count = coalesce(a.count, 0) + 1
        RETURN elementKey(a) AS key, a.status AS status, a.count AS count
        """
    created = db.execute_gql(query, include_plan=True)
    assert created["kind"] == "mutation"
    assert created["rows"] == [{"key": "acct-a", "status": "created", "count": 1}]
    assert created["mutation_stats"]["nodes_created"] == 1
    assert created["mutation_stats"]["nodes_updated"] == 0
    assert created["mutation_stats"]["mutation_rows"] == 1
    assert created["plan"]["mutation"]["uses_write_txn"] is True
    assert any(op["op"] == "MERGE NODE" for op in created["plan"]["mutation"]["operations"])

    matched = db.execute_gql(query)
    assert matched["rows"] == [{"key": "acct-a", "status": "matched", "count": 2}]
    assert matched["mutation_stats"]["nodes_created"] == 0
    assert matched["mutation_stats"]["nodes_updated"] == 1
    assert matched["mutation_stats"]["properties_set"] == 2


def test_gql_phase34_cap_forwarding_and_explain_fields(db):
    seed_phase34(db, person_label="PyPhase34CapPerson")
    explain = db.explain_gql(
        """
        MATCH (n:PyPhase34CapPerson)
        WITH n.group AS grp, count(*) AS count
        WHERE count > 1
        RETURN grp, count
        """,
        max_pipeline_rows=7,
        max_groups=8,
        max_collect_items=9,
        max_union_branches=10,
        max_subquery_invocations=11,
        max_subquery_depth=1,
        max_shortest_path_pairs=12,
    )
    assert explain["read"]["target"] == "graph_pipeline_query"
    assert explain["caps"]["max_pipeline_rows"] == 7
    assert explain["caps"]["max_groups"] == 8
    assert explain["caps"]["max_collect_items"] == 9
    assert explain["caps"]["max_union_branches"] == 10
    assert explain["caps"]["max_subquery_invocations"] == 11
    assert explain["caps"]["max_subquery_depth"] == 1
    assert explain["caps"]["max_shortest_path_pairs"] == 12
    assert any("Aggregate" in item for item in explain["read"]["projection"])

    with pytest.raises(Exception, match="cap 1|max_intermediate_bindings|max_pipeline"):
        db.execute_gql(
            "MATCH (n:PyPhase34CapPerson) WITH n RETURN n",
            max_pipeline_rows=1,
        )
    with pytest.raises(Exception, match="max_groups"):
        db.execute_gql(
            "MATCH (n:PyPhase34CapPerson) RETURN n.group AS grp, count(*) AS count",
            max_groups=1,
        )
    with pytest.raises(Exception, match="max_collect_items"):
        db.execute_gql(
            "MATCH (n:PyPhase34CapPerson) RETURN collect(n.name) AS names",
            max_collect_items=1,
        )
    with pytest.raises(Exception, match="max_union_branches"):
        db.execute_gql(
            """
            MATCH (n:PyPhase34CapPerson) RETURN n.name AS name
            UNION ALL
            MATCH (n:PyPhase34CapPerson) RETURN n.name AS name
            """,
            max_union_branches=1,
        )
    with pytest.raises(Exception, match="max_subquery_invocations"):
        db.execute_gql(
            """
            MATCH (n:PyPhase34CapPerson)
            WHERE EXISTS {
              MATCH (m:PyPhase34CapPerson)
              WHERE m.group = n.group
              RETURN m
            }
            RETURN n.name AS name
            """,
            max_subquery_invocations=1,
        )
    with pytest.raises(Exception, match="max_subquery_depth"):
        db.execute_gql(
            """
            MATCH (n:PyPhase34CapPerson)
            WHERE EXISTS { MATCH (m:PyPhase34CapPerson) RETURN m }
            RETURN n.name AS name
            """,
            max_subquery_depth=0,
        )
    with pytest.raises(Exception, match="max_shortest_path_pairs"):
        db.execute_gql(
            f"""
            MATCH (a:PyPhase34CapPerson)
            WITH a
            MATCH (b:PyPhase34CapPerson)
            WITH a, b
            MATCH p = shortestPath((a)-[:PY34_KNOWS*1..3]->(b))
            RETURN p
            """,
            max_shortest_path_pairs=1,
        )


def test_gql_sync_create_return_mutation_stats_bytes_and_plan(db):
    result = db.execute_gql(
        """
        CREATE (n:PyCreateReturn {elementKey: 'created-one', name: $name, payload: $payload})
        RETURN elementKey(n) AS key, n.name AS name, n.payload AS payload, n
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


def test_gql_schema_mutation_failures_are_rejected(db):
    db.set_node_schema(
        "PyGqlSchemaPerson",
        {
            "properties": {
                "name": {"required": True, "nullable": False, "types": ["string"]}
            }
        },
    )

    with pytest.raises(Exception, match="schema violation"):
        db.execute_gql("CREATE (n:PyGqlSchemaPerson {elementKey: 'bad'}) RETURN n")
    assert db.get_node_by_key("PyGqlSchemaPerson", "bad") is None

    db.upsert_node("PyGqlSchemaPerson", "good", props={"name": "Ada"})
    with pytest.raises(Exception, match="schema violation"):
        db.execute_gql(
            "MATCH (n:PyGqlSchemaPerson) WHERE elementKey(n) = 'good' REMOVE n.name"
        )
    assert db.get_node_by_key("PyGqlSchemaPerson", "good").props["name"] == "Ada"


def test_gql_sync_set_remove_return_row_ops(db):
    db.upsert_node("PySetRemoveReturn", "a", props={"rank": 1, "group": "old", "status": "old"})
    db.upsert_node("PySetRemoveReturn", "b", props={"rank": 2, "group": "old", "status": "old"})
    db.upsert_node("PySetRemoveReturn", "c", props={"rank": 3, "group": "old", "status": "old"})

    result = db.execute_gql(
        """
        MATCH (n:PySetRemoveReturn)
        SET n.status = $status
        REMOVE n.group
        RETURN elementKey(n) AS key, n.status AS status, n.group AS group
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
        WHERE elementKey(n) = 'hub'
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
        db.execute_gql("CREATE (n:PyReadOnly {elementKey: 'blocked'})", mode="read_only")

    with pytest.raises(Exception, match="mode.*auto.*read_only"):
        db.execute_gql("MATCH (n:Person) RETURN n LIMIT 1", mode="readonly")


def test_gql_mutation_compact_rows_and_include_vectors(tmp_dir):
    db = open_vector_db(tmp_dir)
    try:
        seed(db, include_vectors=True)
        compact = db.execute_gql(
            """
            CREATE (n:PyCompactMutation {elementKey: 'compact', name: 'Compact'})
            RETURN elementKey(n) AS key, n.name AS name
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
        CREATE (n:PyAsyncMutation {elementKey: 'once', name: 'Async'})
        RETURN n.name AS name
        """
    )
    assert result["kind"] == "mutation"
    assert result["rows"] == [{"name": "Async"}]
    assert result["mutation_stats"]["nodes_created"] == 1

    read_back = await async_db.execute_gql(
        "MATCH (n:PyAsyncMutation) WHERE elementKey(n) = 'once' RETURN n.name AS name"
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
        WHERE elementKey(n) = 'target'
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
        WHERE elementKey(n) = 'hub'
        DETACH DELETE n
        """
    )
    assert detached["kind"] == "mutation"
    assert detached["rows"] == []
    assert detached["mutation_stats"]["nodes_deleted"] == 1
    assert detached["mutation_stats"]["edges_deleted"] == 1

    with pytest.raises(Exception, match="read.?only|ReadOnly"):
        await async_db.execute_gql(
            "CREATE (n:PyAsyncReadOnly {elementKey: 'blocked'})",
            mode="read_only",
        )

    explain = await async_db.explain_gql(
        "CREATE (n:PyAsyncExplain {elementKey: 'planned'}) RETURN elementKey(n) AS key"
    )
    assert explain["kind"] == "mutation"
    assert explain["read"] is None
    assert explain["mutation"]["return_plan"]["columns"] == ["key"]


def test_gql_volatile_metadata_order_by_rejection_surfaces(db):
    with pytest.raises(Exception, match="ORDER BY|commit|metadata|before commit|volatile"):
        db.execute_gql("CREATE (n:PyVolatileOrder {elementKey: 'bad-order'}) RETURN elementKey(n) ORDER BY id(n)")


def test_gql_forwards_mutation_order_and_path_caps(db):
    seed(db)
    with pytest.raises(Exception, match="max_mutation_rows"):
        db.execute_gql("CREATE (n:PyCapMutationRows {elementKey: 'row-cap'})", max_mutation_rows=0)

    with pytest.raises(Exception, match="max_mutation_ops"):
        db.execute_gql("CREATE (n:PyCapMutationOps {elementKey: 'op-cap'})", max_mutation_ops=0)

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
        RETURN path, nodeIds(path) AS node_ids, edgeIds(path) AS edge_ids
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
    assert "max_pipeline_rows: int | None" in text
    assert "max_groups: int | None" in text
    assert "max_collect_items: int | None" in text
    assert "max_union_branches: int | None" in text
    assert "max_subquery_invocations: int | None" in text
    assert "max_subquery_depth: int | None" in text
    assert "max_shortest_path_pairs: int | None" in text
    assert "class QueryPlanCompoundIndexDetails" in text
    assert "compound_index_prefix_not_satisfied" in text
    assert "compound_equality_index" in text
    assert "compound_range_index" in text
    assert "fields: list[GqlIndexExplainField]" in text
    assert "compound: bool" in text
    assert "class GqlExecutionResult" in text
    assert "class GqlExecutionExplain" in text
    assert "class GqlMutationStats" in text
    assert "class GqlSchemaStats" in text
    assert "class GqlIndexStats" in text
    assert "class GqlSchemaExplain" in text
    assert "class GqlIndexExplain" in text
    assert "schema_stats: GqlSchemaStats | None" in text
    assert "index_stats: GqlIndexStats | None" in text
    assert "schema: GqlSchemaExplain | None" in text
    assert "index: GqlIndexExplain | None" in text
    assert "Literal[\"query\", \"mutation\", \"schema\", \"index\"]" in text
    assert "async def execute_gql" in text
    assert "def query_graph_pipeline" in text
    assert "def explain_graph_pipeline" in text
    assert "async def query_graph_pipeline" in text
    assert hasattr(OverGraph, "query_graph_pipeline")
    assert hasattr(OverGraph, "explain_graph_pipeline")
    assert "gql_query" not in text
    assert "explain_gql_query" not in text
    assert "class GqlResult" not in text
