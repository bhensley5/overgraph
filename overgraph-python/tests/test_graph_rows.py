import os
from pathlib import Path

import pytest

from overgraph import AsyncOverGraph, OverGraph


def lf(*labels, mode="all"):
    return {"labels": list(labels), "mode": mode}


def seed_graph(db, include_vectors=False):
    ada_kwargs = {"props": {"name": "Ada", "rank": 2, "status": "active"}}
    if include_vectors:
        ada_kwargs["dense_vector"] = [0.1, 0.2, 0.3]
        ada_kwargs["sparse_vector"] = [(4, 0.5)]
    ada = db.upsert_node("Person", "ada", **ada_kwargs)
    ben = db.upsert_node("Person", "ben", props={"name": "Ben", "rank": 1, "status": "active"})
    cy = db.upsert_node("Person", "cy", props={"name": "Cy", "rank": 3, "status": "inactive"})
    acme = db.upsert_node("Company", "acme", props={"name": "Acme"})
    works = db.upsert_edge(ada, acme, "WORKS_AT", props={"role": "engineer"})
    reports = db.upsert_edge(ada, ben, "REPORTS_TO")
    knows_ab = db.upsert_edge(ada, ben, "KNOWS")
    knows_bc = db.upsert_edge(ben, cy, "KNOWS")
    return {
        "ada": ada,
        "ben": ben,
        "cy": cy,
        "acme": acme,
        "works": works,
        "reports": reports,
        "knows_ab": knows_ab,
        "knows_bc": knows_bc,
    }


async def seed_graph_async(db):
    ada = await db.upsert_node("Person", "ada", props={"name": "Ada", "rank": 2, "status": "active"})
    ben = await db.upsert_node("Person", "ben", props={"name": "Ben", "rank": 1, "status": "active"})
    cy = await db.upsert_node("Person", "cy", props={"name": "Cy", "rank": 3, "status": "inactive"})
    acme = await db.upsert_node("Company", "acme", props={"name": "Acme"})
    works = await db.upsert_edge(ada, acme, "WORKS_AT", props={"role": "engineer"})
    reports = await db.upsert_edge(ada, ben, "REPORTS_TO")
    knows_ab = await db.upsert_edge(ada, ben, "KNOWS")
    knows_bc = await db.upsert_edge(ben, cy, "KNOWS")
    return {
        "ada": ada,
        "ben": ben,
        "cy": cy,
        "acme": acme,
        "works": works,
        "reports": reports,
        "knows_ab": knows_ab,
        "knows_bc": knows_bc,
    }


def fixed_request(ids):
    return {
        "nodes": [
            {"alias": "p", "ids": [ids["ada"]]},
            {"alias": "c", "label_filter": lf("Company"), "keys": ["acme"]},
        ],
        "pieces": [
            {
                "kind": "edge",
                "alias": "r",
                "from": "p",
                "to": "c",
                "labels": ["WORKS_AT"],
                "filter": {"property": "role", "eq": "engineer"},
            }
        ],
        "return": [
            {"expr": {"binding": "p"}, "as": "person"},
            {"expr": {"binding": "r"}, "as": "rel"},
            {"expr": {"binding": "c"}, "as": "company"},
        ],
        "limit": 10,
    }


def test_query_graph_rows_fixed_basic(db):
    ids = seed_graph(db)
    result = db.query_graph_rows(fixed_request(ids))
    assert result["columns"] == ["person", "rel", "company"]
    assert result["rows"] == [{"person": ids["ada"], "rel": ids["works"], "company": ids["acme"]}]
    assert result["next_cursor"] is None
    assert result["stats"]["rows_returned"] == 1
    assert result["stats"]["planning_ns"] is None
    assert result["stats"]["execution_ns"] is None

    profiled = db.query_graph_rows(
        {**fixed_request(ids), "options": {"profile": True}}
    )
    assert isinstance(profiled["stats"]["planning_ns"], int)
    assert isinstance(profiled["stats"]["execution_ns"], int)

    fast_profiled = db.query_graph_rows(
        {
            "nodes": [{"alias": "n", "ids": [ids["ada"]]}],
            "return": [{"expr": {"binding": "n"}, "as": "n"}],
            "limit": 1,
            "options": {"profile": True},
        }
    )
    assert isinstance(fast_profiled["stats"]["planning_ns"], int)
    assert isinstance(fast_profiled["stats"]["execution_ns"], int)


def test_query_graph_rows_optional_hit_and_miss(db):
    ids = seed_graph(db)
    ben_works = db.upsert_edge(ids["ben"], ids["acme"], "WORKS_AT")
    base = {
        "nodes": [
            {"alias": "p", "label_filter": lf("Person")},
            {"alias": "c", "label_filter": lf("Company")},
            {"alias": "m", "label_filter": lf("Person")},
        ],
        "pieces": [
            {"kind": "edge", "alias": "works", "from": "p", "to": "c", "labels": ["WORKS_AT"]},
            {
                "kind": "optional",
                "pieces": [
                    {"kind": "edge", "alias": "r", "from": "p", "to": "m", "labels": ["REPORTS_TO"]}
                ],
            }
        ],
        "return": [
            {"expr": {"binding": "p"}, "as": "p"},
            {"expr": {"binding": "m"}, "as": "m"},
            {"expr": {"binding": "r"}, "as": "r"},
        ],
        "order_by": [{"expr": {"property": {"alias": "p", "key": "rank"}}, "direction": "asc"}],
        "limit": 10,
    }

    hit = db.query_graph_rows(
        {**base, "nodes": [{**base["nodes"][0], "ids": [ids["ada"]]}, *base["nodes"][1:]]}
    )
    assert hit["rows"] == [{"p": ids["ada"], "m": ids["ben"], "r": ids["reports"]}]

    miss = db.query_graph_rows(
        {**base, "nodes": [{**base["nodes"][0], "ids": [ids["ben"]]}, *base["nodes"][1:]]}
    )
    assert miss["rows"] == [{"p": ids["ben"], "m": None, "r": None}]
    assert ben_works


def test_query_graph_rows_vlp_path_and_hydration_vectors(tmp_dir):
    db = OverGraph.open(os.path.join(tmp_dir, "graph_rows_vectors"), dense_vector_dimension=3)
    try:
        ids = seed_graph(db, include_vectors=True)
        request = {
            "nodes": [
                {"alias": "a", "ids": [ids["ada"]]},
                {"alias": "z", "label_filter": lf("Person")},
            ],
            "pieces": [
                {
                    "kind": "variable_length",
                    "path_alias": "path",
                    "from": "a",
                    "to": "z",
                    "labels": ["KNOWS"],
                    "min_hops": 1,
                    "max_hops": 2,
                }
            ],
            "return": [{"expr": {"binding": "path"}, "as": "path"}],
            "order_by": [{"expr": {"path_field": {"alias": "path", "field": "length"}}}],
            "limit": 10,
        }
        ids_only = db.query_graph_rows(request)
        paths = [row["path"] for row in ids_only["rows"]]
        assert {"node_ids": [ids["ada"], ids["ben"]], "edge_ids": [ids["knows_ab"]]} in paths
        assert {
            "node_ids": [ids["ada"], ids["ben"], ids["cy"]],
            "edge_ids": [ids["knows_ab"], ids["knows_bc"]],
        } in paths

        hydrated = db.query_graph_rows({**request, "output": {"mode": "elements"}})
        first = hydrated["rows"][0]["path"]
        assert "nodes" in first and "edges" in first
        assert "dense_vector" not in first["nodes"][0]

        with_vectors = db.query_graph_rows(
            {**request, "output": {"mode": "elements", "include_vectors": True}}
        )
        vector_path = with_vectors["rows"][0]["path"]
        assert vector_path["nodes"][0]["dense_vector"] == pytest.approx([0.1, 0.2, 0.3])
        assert vector_path["nodes"][0]["sparse_vector"] == [(4, 0.5)]
    finally:
        db.close()


def test_query_graph_rows_order_page_cursor_compact_and_errors(tmp_dir):
    seed_graph(db := OverGraph.open(os.path.join(tmp_dir, "cursor_test")))
    try:
        request = {
            "nodes": [{"alias": "p", "label_filter": lf("Person")}],
            "return": [{"expr": {"property": {"alias": "p", "key": "name"}}, "as": "name"}],
            "order_by": [{"expr": {"property": {"alias": "p", "key": "rank"}}, "direction": "asc"}],
            "skip": 0,
            "limit": 1,
        }
        first = db.query_graph_rows(request)
        assert first["rows"] == [{"name": "Ben"}]
        assert first["next_cursor"]
        second = db.query_graph_rows({**request, "cursor": first["next_cursor"]})
        assert second["rows"] == [{"name": "Ada"}]

        compact = db.query_graph_rows({**request, "output": {"compact_rows": True}})
        assert compact["columns"] == ["name"]
        assert compact["rows"] == [["Ben"]]

        with pytest.raises(Exception, match="cursor|fingerprint|mismatch"):
            db.query_graph_rows(
                {
                    **request,
                    "cursor": first["next_cursor"],
                    "order_by": [
                        {"expr": {"property": {"alias": "p", "key": "name"}}, "direction": "asc"}
                    ],
                }
            )
        with pytest.raises(Exception, match="cursor|invalid|decode|malformed"):
            db.query_graph_rows({**request, "cursor": "not-a-valid-cursor"})
        with pytest.raises(Exception, match="cursor|max_cursor_bytes|too large|exceeds"):
            db.query_graph_rows(
                {
                    **request,
                    "cursor": first["next_cursor"],
                    "options": {"max_cursor_bytes": 4},
                }
            )
    finally:
        db.close(force=True)


def test_query_graph_rows_omitted_limit_defaults_to_1000(tmp_dir):
    db = OverGraph.open(os.path.join(tmp_dir, "default_limit"))
    try:
        for index in range(1002):
            db.upsert_node("Bulk", f"n-{index:04d}")
        result = db.query_graph_rows({"nodes": [{"alias": "n", "label_filter": lf("Bulk")}]})
        assert result["columns"] == ["n"]
        assert len(result["rows"]) == 1000
        assert result["next_cursor"]

        capped = db.query_graph_rows(
            {
                "nodes": [{"alias": "n", "label_filter": lf("Bulk")}],
                "options": {"max_page_limit": 3},
            }
        )
        assert len(capped["rows"]) == 3
    finally:
        db.close(force=True)


def test_query_graph_rows_explain_params_and_expression_tags(tmp_dir):
    ids = seed_graph(db := OverGraph.open(os.path.join(tmp_dir, "expr_test")))
    try:
        request = {
            "nodes": [{"alias": "p", "label_filter": lf("Person")}],
            "where": {
                "op": "=",
                "left": {"property": {"alias": "p", "key": "status"}},
                "right": {"param": "status"},
            },
            "return": [
                {"expr": {"property": {"alias": "p", "key": "name"}}, "as": "name"},
                {"expr": {"list": [{"param": "status"}, {"bytes": [1, 2]}]}, "as": "items"},
                {"expr": {"map": {"k": {"param": "status"}}}, "as": "payload"},
            ],
            "params": {"status": "active"},
            "order_by": [{"expr": {"property": {"alias": "p", "key": "rank"}}}],
            "limit": 10,
        }
        result = db.query_graph_rows(request)
        assert [row["name"] for row in result["rows"]] == ["Ben", "Ada"]
        assert result["rows"][0]["items"] == ["active", b"\x01\x02"]
        assert result["rows"][0]["payload"] == {"k": "active"}

        explain = db.explain_graph_rows(
            {
                **request,
                "pieces": [
                    {
                        "kind": "optional",
                        "pieces": [
                            {"kind": "edge", "from": "p", "to": "m", "labels": ["REPORTS_TO"]}
                        ],
                    },
                    {
                        "kind": "variable_length",
                        "path_alias": "path",
                        "from": "p",
                        "to": "z",
                        "labels": ["KNOWS"],
                        "min_hops": 1,
                        "max_hops": 2,
                    },
                ],
                "nodes": [
                    {"alias": "p", "ids": [ids["ada"]]},
                    {"alias": "m", "label_filter": lf("Person")},
                    {"alias": "z", "label_filter": lf("Person")},
                ],
                "return": [{"expr": {"binding": "path"}, "as": "path"}],
                "options": {"include_plan": True},
            }
        )
        assert explain["projection"]["output_mode"] == "ids"
        assert explain["cursor"]["supplied"] is False
        assert any("optional" in node["kind"].lower() or "optional" in node["detail"].lower() for node in explain["plan"])
        assert any(
            "variable" in node["kind"].lower() or "path" in node["detail"].lower()
            for node in explain["plan"]
        )

        with pytest.raises(Exception, match="exactly one known discriminant"):
            db.query_graph_rows({**request, "where": {"param": "status", "binding": "p"}})
        with pytest.raises(Exception, match="exactly one known discriminant"):
            db.query_graph_rows({**request, "where": {"unexpected": {"x": 1}}})
        for malformed in (
            {"bytes": None},
            {"param": None},
            {"list": None},
            {"map": None},
        ):
            with pytest.raises(Exception):
                db.query_graph_rows({**request, "where": malformed})
        assert len(db.query_graph_rows({**request, "where": {"is_null": None}})["rows"]) == 3
        with pytest.raises(Exception, match="as.*alias|alias.*as"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [
                        {"expr": {"binding": "p"}, "as": "p", "alias": "also_p"},
                    ],
                }
            )
        with pytest.raises(Exception, match="alias"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [{"expr": {"binding": "p"}, "alias": 3}],
                }
            )
        with pytest.raises(Exception, match="exactly one"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [
                        {
                            "expr": {"binding": "p"},
                            "projection": {"element": "full", "selected": {"node": {"id": True}}},
                        }
                    ],
                }
            )
        with pytest.raises(Exception, match="exactly one"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [
                        {
                            "expr": {"binding": "p"},
                            "projection": {
                                "selected": {
                                    "node": {"id": True},
                                    "edge": {"id": True},
                                }
                            },
                        }
                    ],
                }
            )
        with pytest.raises(Exception, match="from_id.*from|from.*from_id"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [
                        {
                            "expr": {"binding": "p"},
                            "projection": {
                                "selected": {
                                    "edge": {"from_id": True, "from": True},
                                }
                            },
                        }
                    ],
                }
            )
        with pytest.raises(Exception, match="does not accept field"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [
                        {
                            "expr": {"binding": "path"},
                            "projection": {
                                "selected": {
                                    "path": {"node_ids": True, "unknown": True},
                                }
                            },
                        }
                    ],
                }
            )
        with pytest.raises(Exception, match="does not accept field"):
            db.query_graph_rows(
                {
                    **request,
                    "return": [
                        {
                            "expr": {
                                "node_field": {
                                    "alias": "p",
                                    "field": "id",
                                    "extra": "rejected",
                                }
                            }
                        }
                    ],
                }
            )
    finally:
        db.close(force=True)


@pytest.mark.asyncio
async def test_query_graph_pipeline_sync_and_async_connector_boundary(tmp_dir):
    db = OverGraph.open(os.path.join(tmp_dir, "graph_pipeline"))
    try:
        seed_graph(db)
        pipeline = {
            "stages": [
                {
                    "kind": "match",
                    "nodes": [{"alias": "n", "label_filter": lf("Person")}],
                },
                {
                    "kind": "project",
                    "project_kind": "with",
                    "items": [
                        {"expr": {"property": {"alias": "n", "key": "name"}}, "as": "name"},
                        {"expr": {"property": {"alias": "n", "key": "rank"}}, "as": "rank"},
                        {"expr": {"property": {"alias": "n", "key": "status"}}, "as": "status"},
                    ],
                    "where": {"op": "=", "left": {"binding": "status"}, "right": "active"},
                    "order_by": [{"expr": {"binding": "rank"}, "direction": "desc"}],
                    "limit": 3,
                },
                {
                    "kind": "project",
                    "project_kind": "return",
                    "items": [
                        {"expr": {"binding": "name"}, "as": "name"},
                        {"expr": {"op": "+", "left": {"binding": "rank"}, "right": 10}, "as": "score"},
                    ],
                    "order_by": [{"expr": {"binding": "score"}, "direction": "desc"}],
                },
            ],
            "limit": 10,
            "options": {"include_plan": True, "profile": True},
        }

        result = db.query_graph_pipeline(pipeline)
        assert result["columns"] == ["name", "score"]
        assert result["rows"] == [{"name": "Ada", "score": 12}, {"name": "Ben", "score": 11}]
        assert result["next_cursor"] is None
        assert result["stats"]["rows_returned"] == 2
        assert len(result["plan"]["stages"]) == 3
        assert result["plan"]["caps"]["max_pipeline_rows"] == 65536

        async_db = await AsyncOverGraph.open(os.path.join(tmp_dir, "graph_pipeline_async"))
        try:
            await seed_graph_async(async_db)
            compact = await async_db.query_graph_pipeline(
                {**pipeline, "output": {"compact_rows": True}}
            )
            assert compact["rows"] == [["Ada", 12], ["Ben", 11]]
            explain = await async_db.explain_graph_pipeline(pipeline)
            assert explain["columns"] == ["name", "score"]
            assert len(explain["stages"]) == 3
        finally:
            await async_db.close(force=True)
    finally:
        db.close(force=True)


def test_query_graph_pipeline_aggregate_projection(db):
    seed_graph(db)
    result = db.query_graph_pipeline(
        {
            "stages": [
                {
                    "kind": "match",
                    "nodes": [{"alias": "n", "label_filter": lf("Person")}],
                },
                {
                    "kind": "return",
                    "items": [
                        {"expr": {"aggregate": {"function": "count"}}, "as": "people"},
                        {
                            "expr": {
                                "aggregate": {
                                    "function": "collect",
                                    "arg": {"property": {"alias": "n", "key": "status"}},
                                    "distinct": True,
                                }
                            },
                            "as": "statuses",
                        },
                    ],
                },
            ],
            "limit": 10,
            "options": {"include_plan": True},
        }
    )
    assert result["rows"][0]["people"] == 3
    assert set(result["rows"][0]["statuses"]) == {"active", "inactive"}
    assert result["plan"]["stats"]["groups"] == 1


def test_old_pattern_surface_is_explicitly_unsupported(db):
    seed_graph(db)
    with pytest.raises(Exception, match="unsupported; use query_graph_rows"):
        db.query_pattern({"nodes": [], "edges": [], "limit": 1})
    with pytest.raises(Exception, match="unsupported; use explain_graph_rows"):
        db.explain_pattern_query({"nodes": [], "edges": [], "limit": 1})


def test_python_stub_names_graph_rows_not_old_patterns():
    stub = Path(__file__).parents[1] / "python" / "overgraph" / "__init__.pyi"
    text = stub.read_text()
    assert "def query_graph_rows" in text
    assert "def explain_graph_rows" in text
    assert "GraphRowResult" in text
    assert "GraphPathValue" in text
    assert "def query_pattern" not in text
    assert "GraphPatternRequest" not in text
    assert "class GraphNodePattern" not in text
    assert "class GraphEdgePattern" not in text


@pytest.mark.asyncio
async def test_async_graph_row_wrappers(tmp_dir):
    async with await AsyncOverGraph.open(os.path.join(tmp_dir, "async_graph_rows")) as adb:
        ada = await adb.upsert_node("Person", "ada", props={"name": "Ada"})
        result = await adb.query_graph_rows(
            {
                "nodes": [{"alias": "p", "ids": [ada]}],
                "return": [{"expr": {"binding": "p"}, "as": "p"}],
                "limit": 1,
            }
        )
        assert result["rows"] == [{"p": ada}]
        explain = await adb.explain_graph_rows(
            {"nodes": [{"alias": "p", "ids": [ada]}], "limit": 1}
        )
        assert explain["columns"] == ["p"]
