import pytest
import time

from overgraph import GraphEdgePattern, GraphNodePattern, GraphPatternRequest, NodeQueryRequest


def plan_has_kind(node, kind):
    if not node:
        return False
    if node["kind"] == kind:
        return True
    if "input" in node and plan_has_kind(node["input"], kind):
        return True
    return any(plan_has_kind(child, kind) for child in node.get("inputs", []))


def wait_for_index_state(db, predicate, expected_state="ready", timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = predicate(db.list_node_property_indexes())
        if info is not None and info.state == expected_state:
            return info
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for secondary index state '{expected_state}'")


def seed_query_graph(db):
    active_high = db.upsert_node(
        1, "active-high", props={"status": "active", "score": 90, "team": "core"}
    )
    active_low = db.upsert_node(
        1, "active-low", props={"status": "active", "score": 40, "team": "core"}
    )
    inactive = db.upsert_node(
        1, "inactive", props={"status": "inactive", "score": 95, "team": "core"}
    )
    literal_updated_at = db.upsert_node(
        1,
        "literal-updated-at",
        props={"updated_at": "literal-property-value", "status": "active", "score": 70},
    )
    null_tag = db.upsert_node(
        1,
        "null-tag",
        props={"status": "nullish", "tag": None, "score": 10},
    )
    nested = db.upsert_node(
        1,
        "nested",
        props={"status": "nested", "payload": {"items": [1, "1", None]}, "score": 15},
    )
    acme = db.upsert_node(2, "acme", props={"status": "customer"})
    beta = db.upsert_node(2, "beta", props={"status": "prospect"})
    works_at = db.upsert_edge(
        active_high,
        acme,
        10,
        props={"role": "engineer", "since": 2020, "updated_at": "edge-literal"},
    )
    inactive_works_at = db.upsert_edge(
        inactive,
        beta,
        10,
        props={"role": "engineer", "since": 2022, "updated_at": "edge-literal"},
    )
    return {
        "active_high": active_high,
        "active_low": active_low,
        "inactive": inactive,
        "literal_updated_at": literal_updated_at,
        "null_tag": null_tag,
        "nested": nested,
        "acme": acme,
        "beta": beta,
        "works_at": works_at,
        "inactive_works_at": inactive_works_at,
    }


def test_query_node_ids_and_hydrated_nodes(db):
    ids = seed_query_graph(db)
    request = {
        "type_id": 1,
        "filter": {
            "and": [
                {"property": "status", "eq": "active"},
                {"property": "score", "gte": 50},
            ]
        },
        "limit": 0,
    }

    result = db.query_node_ids(request)
    assert sorted(result.items.to_list()) == [ids["active_high"], ids["literal_updated_at"]]

    nodes = db.query_nodes({**request, "limit": 1})
    assert [node.id for node in nodes.items] == [ids["active_high"]]
    assert nodes.next_cursor == ids["active_high"]
    assert nodes.items[0].props["status"] == "active"


def test_query_predicates_and_literal_builtin_name_collision(db):
    ids = seed_query_graph(db)

    result = db.query_node_ids(
        {
            "type_id": 1,
            "filter": {
                "and": [
                    {"property": "status", "eq": "active"},
                    {"property": "updated_at", "eq": "literal-property-value"},
                ]
            },
        }
    )
    assert result.items.to_list() == [ids["literal_updated_at"]]

    updated_at = db.get_node(ids["active_high"]).updated_at
    timestamp_result = db.query_node_ids(
        {
            "type_id": 1,
            "filter": {
                "and": [
                    {"updated_at": {"gte": updated_at - 1000}},
                    {"property": "status", "eq": "active"},
                ]
            },
        }
    )
    assert ids["active_high"] in timestamp_result.items.to_list()


def test_query_updated_at_exclusive_boundaries(db):
    ids = seed_query_graph(db)
    updated_at = db.get_node(ids["active_high"]).updated_at

    assert (
        db.query_node_ids(
            {"ids": [ids["active_high"]], "filter": {"updated_at": {"gt": updated_at}}}
        ).items.to_list()
        == []
    )
    assert (
        db.query_node_ids(
            {"ids": [ids["active_high"]], "filter": {"updated_at": {"lt": updated_at}}}
        ).items.to_list()
        == []
    )
    assert db.query_node_ids(
        {
            "ids": [ids["active_high"]],
            "filter": {"updated_at": {"gte": updated_at, "lte": updated_at}},
        }
    ).items.to_list() == [ids["active_high"]]


def test_query_updated_at_exclusive_overflow_is_empty_result(db):
    ids = seed_query_graph(db)

    gt_result = db.query_node_ids(
        {
            "ids": [ids["active_high"]],
            "filter": {"updated_at": {"gt": 9223372036854775807}},
        }
    )
    assert gt_result.items.to_list() == []
    gt_plan = db.explain_node_query(
        {
            "ids": [ids["active_high"]],
            "filter": {"updated_at": {"gt": 9223372036854775807}},
        }
    )
    assert plan_has_kind(gt_plan["root"], "empty_result")

    lt_result = db.query_node_ids(
        {
            "ids": [ids["active_high"]],
            "filter": {"updated_at": {"lt": -9223372036854775808}},
        }
    )
    assert lt_result.items.to_list() == []
    lt_plan = db.explain_node_query(
        {
            "ids": [ids["active_high"]],
            "filter": {"updated_at": {"lt": -9223372036854775808}},
        }
    )
    assert plan_has_kind(lt_plan["root"], "empty_result")


def test_query_pattern_and_edge_literal_updated_at(db):
    ids = seed_query_graph(db)

    result = db.query_pattern(
        {
            "nodes": [
                {
                    "alias": "person",
                    "type_id": 1,
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "type_id": 2, "keys": ["acme"]},
            ],
            "edges": [
                {
                    "alias": "employment",
                    "from_alias": "person",
                    "to_alias": "company",
                    "direction": "outgoing",
                    "type_filter": [10],
                    "where": {
                        "role": {"op": "eq", "value": "engineer"},
                        "updated_at": {"op": "eq", "value": "edge-literal"},
                    },
                    "predicates": [
                        {"property": {"key": "since", "op": "range", "lte": 2021}}
                    ],
                }
            ],
            "limit": 10,
        }
    )

    assert result == {
        "matches": [
            {
                "nodes": {"company": ids["acme"], "person": ids["active_high"]},
                "edges": {"employment": ids["works_at"]},
            }
        ],
        "truncated": False,
    }
    assert ids["inactive_works_at"] != ids["works_at"]


def test_query_request_helpers_are_directly_usable(db):
    ids = seed_query_graph(db)

    request = NodeQueryRequest(
        type_id=1,
        filter={
            "and": [
                {"property": "status", "eq": "active"},
                {"property": "score", "gte": 50},
            ]
        },
    )
    result = db.query_node_ids(request)
    assert sorted(result.items.to_list()) == [ids["active_high"], ids["literal_updated_at"]]
    assert db.explain_node_query(request)["kind"] == "node_query"

    pattern = GraphPatternRequest(
        nodes=[
            GraphNodePattern(
                "person",
                type_id=1,
                filter={"property": "status", "eq": "active"},
            ),
            GraphNodePattern("company", type_id=2, keys=["acme"]),
        ],
        edges=[
            GraphEdgePattern(
                "person",
                "company",
                alias="employment",
                type_filter=[10],
                where={"role": {"eq": "engineer"}},
            )
        ],
        limit=10,
    )
    assert db.query_pattern(pattern)["matches"][0] == {
        "nodes": {"company": ids["acme"], "person": ids["active_high"]},
        "edges": {"employment": ids["works_at"]},
    }


def test_query_request_helpers_reject_string_list_fields(db):
    with pytest.raises(TypeError, match="keys"):
        db.query_node_ids(NodeQueryRequest(type_id=1, keys="acme"))

    with pytest.raises(TypeError, match="keys"):
        GraphNodePattern("company", type_id=2, keys="acme").to_dict()


def test_query_explain_uses_lower_snake_recursive_strings(db):
    seed_query_graph(db)

    node_plan = db.explain_node_query(
        {"type_id": 1, "filter": {"property": "status", "eq": "active"}}
    )
    assert node_plan["kind"] == "node_query"
    assert plan_has_kind(node_plan["root"], "fallback_type_scan")
    assert all(warning.replace("_", "").islower() for warning in node_plan["warnings"])
    assert "using_fallback_scan" in node_plan["warnings"]

    pattern_plan = db.explain_pattern_query(
        {
            "nodes": [
                {
                    "alias": "person",
                    "type_id": 1,
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "type_id": 2, "keys": ["acme"]},
            ],
            "edges": [
                {
                    "alias": "employment",
                    "from_alias": "person",
                    "to_alias": "company",
                    "type_filter": [10],
                    "where": {"role": {"op": "eq", "value": "engineer"}},
                }
            ],
            "limit": 10,
        }
    )
    assert pattern_plan["kind"] == "pattern_query"
    assert plan_has_kind(pattern_plan["root"], "pattern_expand")
    assert plan_has_kind(pattern_plan["root"], "verify_edge_predicates")
    assert "edge_property_post_filter" in pattern_plan["warnings"]


def test_query_validation_errors(db):
    seed_query_graph(db)

    with pytest.raises(Exception, match="use filter"):
        db.query_node_ids(
            {"type_id": 1, "predicates": [{"property": {"key": "status", "op": "eq"}}]}
        )
    with pytest.raises(Exception, match="both gt and gte"):
        db.query_node_ids({"type_id": 1, "filter": {"property": "score", "gt": 1, "gte": 2}})
    with pytest.raises(Exception, match="use filter"):
        db.query_node_ids({"type_id": 1, "where": {"status": {"eq": "active"}}})
    with pytest.raises(Exception, match="use filter"):
        db.query_pattern({"nodes": [{"alias": "a", "where": {"status": {"eq": "active"}}}], "edges": [], "limit": 1})
    with pytest.raises(Exception, match="edge pattern filter is not supported"):
        db.query_pattern(
            {
                "nodes": [{"alias": "a"}],
                "edges": [
                    {
                        "from_alias": "a",
                        "to_alias": "b",
                        "filter": {"property": "role", "eq": "engineer"},
                    }
                ],
                "limit": 1,
            }
        )
    with pytest.raises(Exception, match="positive limit|limit must be > 0"):
        db.query_pattern({"nodes": [], "edges": [], "limit": 0})


def test_query_numeric_fields_reject_bool(db):
    seed_query_graph(db)

    invalid_node_requests = [
        {"type_id": True},
        {"ids": [True]},
        {"type_id": 1, "after": True},
        {"type_id": 1, "limit": True},
        {"type_id": 1, "filter": {"updated_at": {"gte": True}}},
    ]
    for request in invalid_node_requests:
        with pytest.raises(TypeError, match="bool"):
            db.query_node_ids(request)

    invalid_pattern_requests = [
        {"nodes": [], "edges": [], "limit": True},
        {"nodes": [], "edges": [], "limit": 1, "at_epoch": True},
        {"nodes": [{"alias": "a", "type_id": True}], "edges": [], "limit": 1},
        {"nodes": [{"alias": "a", "ids": [True]}], "edges": [], "limit": 1},
        {
            "nodes": [],
            "edges": [{"from_alias": "a", "to_alias": "b", "type_filter": [True]}],
            "limit": 1,
        },
    ]
    for request in invalid_pattern_requests:
        with pytest.raises(TypeError, match="bool"):
            db.query_pattern(request)


def test_query_boolean_filter_and_value_semantics(db):
    ids = seed_query_graph(db)

    assert sorted(
        db.query_node_ids(
            {
                "type_id": 1,
                "filter": {
                    "or": [
                        {"property": "status", "eq": "active"},
                        {"property": "status", "eq": "nullish"},
                    ]
                },
            }
        ).items.to_list()
    ) == [
        ids["active_high"],
        ids["active_low"],
        ids["literal_updated_at"],
        ids["null_tag"],
    ]

    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "status", "in": ["nested"]}}
    ).items.to_list() == [ids["nested"]]
    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "tag", "eq": None}}
    ).items.to_list() == [ids["null_tag"]]
    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "tag", "in": [None]}}
    ).items.to_list() == [ids["null_tag"]]
    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "tag", "exists": True}}
    ).items.to_list() == [ids["null_tag"]]
    assert ids["null_tag"] not in db.query_node_ids(
        {"type_id": 1, "filter": {"property": "tag", "missing": True}}
    ).items.to_list()

    assert db.query_node_ids(
        {
            "type_id": 1,
            "filter": {"property": "payload", "eq": {"items": [1, "1", None]}},
        }
    ).items.to_list() == [ids["nested"]]
    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "status", "eq": "1"}}
    ).items.to_list() == []

    int_node = db.upsert_node(1, "int-value", props={"kind": 1})
    float_node = db.upsert_node(1, "float-value", props={"kind": 1.0})
    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "kind", "eq": 1}}
    ).items.to_list() == [int_node]
    assert db.query_node_ids(
        {"type_id": 1, "filter": {"property": "kind", "eq": 1.0}}
    ).items.to_list() == [float_node]


def test_query_invalid_canonical_filter_shapes(db):
    seed_query_graph(db)

    invalid_filters = [
        ({}, "empty object"),
        ({"and": []}, "at least one"),
        ({"or": []}, "at least one"),
        ({"not": None}, "dict|object"),
        ({"AND": []}, "exactly one|uppercase"),
        (
            {"and": [{"property": "x", "eq": 1}], "or": [{"property": "x", "eq": 2}]},
            "exactly one",
        ),
        ({"property": "", "eq": 1}, "non-empty"),
        ({"property": "x", "in": []}, "at least one"),
        ({"property": "x", "eq": 1, "in": [1]}, "exactly one operator family"),
        ({"property": "x", "exists": False}, "must be true"),
        ({"property": "x", "missing": False}, "must be true"),
        ({"eq": 1}, "exactly one|selector"),
        ({"property": "x"}, "exactly one operator family"),
    ]
    for filter_expr, pattern in invalid_filters:
        with pytest.raises(Exception, match=pattern):
            db.query_node_ids({"type_id": 1, "filter": filter_expr})

    with pytest.raises(Exception, match="use filter"):
        db.query_pattern(
            {
                "nodes": [
                    {
                        "alias": "a",
                        "predicates": [
                            {"property": {"key": "status", "op": "eq", "value": "active"}}
                        ],
                    }
                ],
                "edges": [],
                "limit": 1,
            }
        )


def test_query_boolean_explain_serialization(db):
    seed_query_graph(db)
    db.ensure_node_property_index(1, "status", "equality")
    wait_for_index_state(
        db,
        lambda infos: next(
            (info for info in infos if info.type_id == 1 and info.prop_key == "status"),
            None,
        ),
    )

    indexed_or = db.explain_node_query(
        {
            "type_id": 1,
            "filter": {
                "or": [
                    {"property": "status", "eq": "active"},
                    {"property": "status", "eq": "nullish"},
                ]
            },
        }
    )
    assert plan_has_kind(indexed_or["root"], "union")
    assert plan_has_kind(indexed_or["root"], "verify_node_filter")

    fallback_or = db.explain_node_query(
        {
            "type_id": 1,
            "filter": {
                "or": [
                    {"property": "status", "eq": "active"},
                    {"property": "tag", "missing": True},
                ]
            },
        }
    )
    assert "boolean_branch_fallback" in fallback_or["warnings"]
    assert "verify_only_filter" in fallback_or["warnings"]

    empty = db.explain_node_query(
        {
            "type_id": 1,
            "filter": {
                "and": [
                    {"property": "status", "eq": "active"},
                    {"property": "status", "eq": "inactive"},
                ]
            },
        }
    )
    assert plan_has_kind(empty["root"], "empty_result")


@pytest.mark.asyncio
async def test_async_query_parity(async_db):
    active_high = await async_db.upsert_node(
        1, "active-high", props={"status": "active", "score": 90}
    )
    inactive = await async_db.upsert_node(
        1, "inactive", props={"status": "inactive", "score": 95}
    )
    acme = await async_db.upsert_node(2, "acme")
    await async_db.upsert_edge(active_high, acme, 10)

    ids = await async_db.query_node_ids(
        NodeQueryRequest(type_id=1, filter={"property": "status", "eq": "active"})
    )
    assert ids.items.to_list() == [active_high]

    nodes = await async_db.query_nodes(
        {"type_id": 1, "filter": {"property": "score", "gte": 80}}
    )
    assert [node.id for node in nodes.items] == [active_high, inactive]

    plan = await async_db.explain_node_query(
        {"type_id": 1, "filter": {"property": "status", "eq": "active"}}
    )
    assert plan["kind"] == "node_query"

    pattern = await async_db.query_pattern(
        {
            "nodes": [
                {
                    "alias": "person",
                    "ids": [active_high],
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "type_id": 2, "keys": ["acme"]},
            ],
            "edges": [
                {
                    "alias": "employment",
                    "from_alias": "person",
                    "to_alias": "company",
                    "type_filter": [10],
                }
            ],
            "limit": 10,
        }
    )
    assert pattern["matches"][0]["nodes"] == {"company": acme, "person": active_high}

    pattern_plan = await async_db.explain_pattern_query(
        {
            "nodes": [
                {
                    "alias": "person",
                    "ids": [active_high],
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "type_id": 2, "keys": ["acme"]},
            ],
            "edges": [
                {
                    "alias": "employment",
                    "from_alias": "person",
                    "to_alias": "company",
                    "type_filter": [10],
                }
            ],
            "limit": 10,
        }
    )
    assert pattern_plan["kind"] == "pattern_query"
    assert plan_has_kind(pattern_plan["root"], "verify_node_filter")
