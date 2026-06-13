import pytest
import time

from overgraph import (
    EdgeQueryRequest,
    NodeQueryRequest,
)


def plan_has_kind(node, kind):
    if not node:
        return False
    if node["kind"] == kind:
        return True
    if "input" in node and plan_has_kind(node["input"], kind):
        return True
    return any(plan_has_kind(child, kind) for child in node.get("inputs", []))


def node_lf(*labels, mode="all"):
    return {"labels": list(labels), "mode": mode}


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
        "Person", "active-high", props={"status": "active", "score": 90, "team": "core"}
    )
    active_low = db.upsert_node(
        "Person", "active-low", props={"status": "active", "score": 40, "team": "core"}
    )
    inactive = db.upsert_node(
        "Person", "inactive", props={"status": "inactive", "score": 95, "team": "core"}
    )
    literal_updated_at = db.upsert_node(
        "Person",
        "literal-updated-at",
        props={"updated_at": "literal-property-value", "status": "active", "score": 70},
    )
    null_tag = db.upsert_node(
        "Person",
        "null-tag",
        props={"status": "nullish", "tag": None, "score": 10},
    )
    nested = db.upsert_node(
        "Person",
        "nested",
        props={"status": "nested", "payload": {"items": [1, "1", None]}, "score": 15},
    )
    acme = db.upsert_node("Company", "acme", props={"status": "customer"})
    beta = db.upsert_node("Company", "beta", props={"status": "prospect"})
    works_at = db.upsert_edge(
        active_high,
        acme,
        "WORKS_AT",
        props={"role": "engineer", "since": 2020, "updated_at": "edge-literal"},
    )
    inactive_works_at = db.upsert_edge(
        inactive,
        beta,
        "WORKS_AT",
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
        "label_filter": node_lf("Person"),
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
            "label_filter": node_lf("Person"),
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
            "label_filter": node_lf("Person"),
            "filter": {
                "and": [
                    {"updated_at": {"gte": updated_at - 1000}},
                    {"property": "status", "eq": "active"},
                ]
            },
        }
    )
    assert ids["active_high"] in timestamp_result.items.to_list()


def test_query_node_label_filter_any_and_all_modes(db):
    admin = db.upsert_node(["Person", "Admin"], "admin")
    person = db.upsert_node("Person", "person")
    company = db.upsert_node("Company", "company")

    assert db.query_node_ids({"label_filter": node_lf("Person", "Admin")}).items.to_list() == [
        admin
    ]
    assert sorted(
        db.query_node_ids({"label_filter": node_lf("Admin", "Company", mode="any")}).items.to_list()
    ) == [admin, company]
    assert person not in db.query_node_ids(
        {"label_filter": node_lf("Admin", "Company", mode="any")}
    ).items.to_list()


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


def test_query_graph_rows_and_edge_literal_updated_at(db):
    ids = seed_query_graph(db)

    result = db.query_graph_rows(
        {
            "nodes": [
                {
                    "alias": "person",
                    "label_filter": node_lf("Person"),
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "label_filter": node_lf("Company"), "keys": ["acme"]},
            ],
            "pieces": [
                {
                    "alias": "employment",
                    "kind": "edge",
                    "from": "person",
                    "to": "company",
                    "direction": "outgoing",
                    "label_filter": ["WORKS_AT"],
                    "filter": {
                        "and": [
                            {"property": "role", "eq": "engineer"},
                            {"property": "updated_at", "eq": "edge-literal"},
                            {"property": "since", "lte": 2021},
                        ]
                    },
                }
            ],
            "return": [
                {"expr": {"binding": "person"}, "as": "person"},
                {"expr": {"binding": "company"}, "as": "company"},
                {"expr": {"binding": "employment"}, "as": "employment"},
            ],
            "limit": 10,
        }
    )

    assert result == {
        "columns": ["person", "company", "employment"],
        "rows": [{"person": ids["active_high"], "company": ids["acme"], "employment": ids["works_at"]}],
        "next_cursor": None,
        "stats": result["stats"],
        "plan": None,
    }
    assert ids["inactive_works_at"] != ids["works_at"]


def test_direct_edge_queries_and_canonical_pattern_filter(db):
    ids = seed_query_graph(db)
    edge = db.get_edge(ids["works_at"])
    request = EdgeQueryRequest(
        label="WORKS_AT",
        from_ids=[ids["active_high"]],
        filter={
            "and": [
                {"weight": {"gte": 1.0}},
                {"valid_at": int(time.time() * 1000)},
                {"updated_at": {"gte": edge.updated_at - 1000}},
                {"property": "role", "eq": "engineer"},
            ]
        },
        limit=0,
    )

    edge_ids = db.query_edge_ids(request)
    assert edge_ids.items.to_list() == [ids["works_at"]]

    edges = db.query_edges({**request.to_dict(), "limit": 1})
    assert [edge.id for edge in edges.items] == [ids["works_at"]]
    assert edges.next_cursor is None
    assert edges.items[0].props["role"] == "engineer"

    plan = db.explain_edge_query(request)
    assert plan["kind"] == "edge_query"
    assert plan_has_kind(plan["root"], "verify_edge_filter")
    assert "edge_property_post_filter" in plan["warnings"]

    graph_rows = db.query_graph_rows(
        {
            "nodes": [
                {"alias": "person", "ids": [ids["active_high"]]},
                {"alias": "company", "label_filter": node_lf("Company"), "keys": ["acme"]},
            ],
            "pieces": [
                {
                    "kind": "edge",
                    "alias": "employment",
                    "from": "person",
                    "to": "company",
                    "label_filter": ["WORKS_AT"],
                    "filter": {
                        "and": [
                            {"valid_at": int(time.time() * 1000)},
                            {"property": "role", "eq": "engineer"},
                        ]
                    },
                }
            ],
            "return": [
                {"expr": {"binding": "person"}, "as": "person"},
                {"expr": {"binding": "company"}, "as": "company"},
                {"expr": {"binding": "employment"}, "as": "employment"},
            ],
            "limit": 10,
        }
    )
    assert graph_rows["rows"] == [
        {"company": ids["acme"], "person": ids["active_high"], "employment": ids["works_at"]}
    ]


def test_query_request_helpers_are_directly_usable(db):
    ids = seed_query_graph(db)

    request = NodeQueryRequest(
        label_filter=node_lf("Person"),
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

    graph_rows = db.query_graph_rows(
        {
            "nodes": [
                {
                    "alias": "person",
                    "label_filter": node_lf("Person"),
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "label_filter": node_lf("Company"), "keys": ["acme"]},
            ],
            "pieces": [
                {
                    "kind": "edge",
                    "alias": "employment",
                    "from": "person",
                    "to": "company",
                    "label_filter": ["WORKS_AT"],
                    "filter": {"property": "role", "eq": "engineer"},
                }
            ],
            "return": [
                {"expr": {"binding": "person"}, "as": "person"},
                {"expr": {"binding": "company"}, "as": "company"},
                {"expr": {"binding": "employment"}, "as": "employment"},
            ],
            "limit": 10,
        }
    )
    assert graph_rows["rows"][0] == {
        "company": ids["acme"],
        "person": ids["active_high"],
        "employment": ids["works_at"],
    }


def test_query_request_helpers_reject_string_list_fields(db):
    with pytest.raises(TypeError, match="keys"):
        db.query_node_ids(NodeQueryRequest(label_filter=node_lf("Person"), keys="acme"))


def test_query_explain_uses_lower_snake_recursive_strings(db):
    ids = seed_query_graph(db)

    node_plan = db.explain_node_query(
        {"label_filter": node_lf("Person"), "filter": {"property": "status", "eq": "active"}}
    )
    assert node_plan["kind"] == "node_query"
    assert plan_has_kind(node_plan["root"], "fallback_node_label_scan")
    assert all(warning.replace("_", "").islower() for warning in node_plan["warnings"])
    assert "using_fallback_scan" in node_plan["warnings"]
    assert "stale_node_label_membership_verification" in node_plan["notes"]
    assert node_plan["public_inputs"]["node_labels"] == [
        {"alias": None, "name": "Person", "known": True, "mode": "all"}
    ]
    assert node_plan["public_inputs"]["edge_labels"] == []

    db.upsert_node(["Person", "Admin"], "admin")
    any_plan = db.explain_node_query({"label_filter": node_lf("Person", "Admin", mode="any")})
    assert "node_label_any_final_verification" in any_plan["notes"]
    assert any_plan["public_inputs"]["node_labels"] == [
        {"alias": None, "name": "Person", "known": True, "mode": "any"},
        {"alias": None, "name": "Admin", "known": True, "mode": "any"},
    ]

    graph_plan = db.explain_graph_rows(
        {
            "nodes": [
                {
                    "alias": "person",
                    "label_filter": node_lf("Person"),
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "label_filter": node_lf("Company"), "keys": ["acme"]},
            ],
            "pieces": [
                {
                    "alias": "employment",
                    "kind": "edge",
                    "from": "person",
                    "to": "company",
                    "label_filter": ["WORKS_AT"],
                    "filter": {"property": "role", "eq": "engineer"},
                }
            ],
            "limit": 10,
        }
    )
    assert graph_plan["columns"] == ["person", "company", "employment"]
    assert graph_plan["projection"]["output_mode"] == "ids"
    assert graph_plan["caps"]["max_page_limit"] >= 10

    full_edge_plan = db.explain_edge_query(
        {
            "filter": {"property": "role", "eq": "engineer"},
            "allow_full_scan": True,
        }
    )
    assert plan_has_kind(full_edge_plan["root"], "fallback_full_edge_scan")

    missing_edge_plan = db.explain_edge_query(
        {"label": "MISSING", "from_ids": [ids["active_high"]]}
    )
    assert "unknown_edge_label" in missing_edge_plan["warnings"]
    assert missing_edge_plan["public_inputs"]["edge_labels"] == [
        {"alias": None, "name": "MISSING", "known": False, "mode": None}
    ]


def test_query_validation_errors(db):
    seed_query_graph(db)

    with pytest.raises(Exception, match="use filter"):
        db.query_node_ids(
            {"label_filter": node_lf("Person"), "predicates": [{"property": {"key": "status", "op": "eq"}}]}
        )
    with pytest.raises(Exception, match="both gt and gte"):
        db.query_node_ids({"label_filter": node_lf("Person"), "filter": {"property": "score", "gt": 1, "gte": 2}})
    with pytest.raises(Exception, match="use filter"):
        db.query_node_ids({"label_filter": node_lf("Person"), "where": {"status": {"eq": "active"}}})
    with pytest.raises(Exception, match="unsupported; use query_graph_rows"):
        db.query_pattern({"nodes": [], "edges": [], "limit": 1})
    with pytest.raises(Exception, match="full scan|anchor|allow_full_scan"):
        db.query_edge_ids({"filter": {"property": "role", "eq": "engineer"}})
    with pytest.raises(Exception, match="both gt and gte"):
        db.query_edge_ids({"label": "WORKS_AT", "filter": {"weight": {"gt": 1, "gte": 2}}})
    for field in ("where", "predicates"):
        with pytest.raises(Exception, match="use filter"):
            db.query_edge_ids({"label": "WORKS_AT", field: {"role": {"eq": "engineer"}}})
        with pytest.raises(Exception, match="use filter"):
            db.query_edges({"label": "WORKS_AT", field: {"role": {"eq": "engineer"}}})
        with pytest.raises(Exception, match="use filter"):
            db.explain_edge_query({"label": "WORKS_AT", field: {"role": {"eq": "engineer"}}})
    with pytest.raises(Exception, match="use filter"):
        db.query_graph_rows(
            {
                "nodes": [{"alias": "a"}],
                "pieces": [
                    {
                        "kind": "edge",
                        "from": "a",
                        "to": "b",
                        "filter": {"property": "role", "eq": "engineer"},
                        "where": {"role": {"eq": "engineer"}},
                    }
                ],
                "limit": 1,
            }
        )
    with pytest.raises(Exception, match="positive limit|limit must be > 0"):
        db.query_graph_rows({"nodes": [], "pieces": [], "limit": 0})


def test_query_numeric_fields_reject_bool(db):
    seed_query_graph(db)

    invalid_node_requests = [
        {"ids": [True]},
        {"label_filter": node_lf("Person"), "after": True},
        {"label_filter": node_lf("Person"), "limit": True},
        {"label_filter": node_lf("Person"), "filter": {"updated_at": {"gte": True}}},
    ]
    for request in invalid_node_requests:
        with pytest.raises(TypeError, match="bool"):
            db.query_node_ids(request)

    invalid_edge_requests = [
        {"label": True},
        {"ids": [True]},
        {"from_ids": [True]},
        {"to_ids": [True]},
        {"endpoint_ids": [True]},
        {"label": "WORKS_AT", "after": True},
        {"label": "WORKS_AT", "limit": True},
        {"label": "WORKS_AT", "filter": {"valid_at": True}},
        {"label": "WORKS_AT", "filter": {"weight": {"gte": True}}},
    ]
    for request in invalid_edge_requests:
        with pytest.raises(TypeError, match="bool"):
            db.query_edge_ids(request)

    invalid_graph_row_bool_requests = [
        {"nodes": [], "pieces": [], "limit": True},
        {"nodes": [], "pieces": [], "limit": 1, "at_epoch": True},
        {"nodes": [{"alias": "a", "ids": [True]}], "pieces": [], "limit": 1},
    ]
    for request in invalid_graph_row_bool_requests:
        with pytest.raises(TypeError, match="bool"):
            db.query_graph_rows(request)
    with pytest.raises(TypeError, match="str"):
        db.query_node_ids({"label_filter": {"labels": [True], "mode": "all"}})
    with pytest.raises(TypeError, match="str"):
        db.query_graph_rows(
            {
                "nodes": [
                    {"alias": "a", "label_filter": {"labels": [True], "mode": "all"}}
                ],
                "pieces": [],
                "limit": 1,
            }
        )
    with pytest.raises(TypeError, match="str"):
        db.query_graph_rows(
            {
                "nodes": [],
                "pieces": [{"kind": "edge", "from": "a", "to": "b", "label_filter": [True]}],
                "limit": 1,
            }
        )

    with pytest.raises(ValueError, match="label.*label_filter"):
        db.query_node_ids({"label": "Person"})
    with pytest.raises(ValueError, match="label.*label_filter"):
        db.query_graph_rows({"nodes": [{"alias": "a", "label": "Person"}], "pieces": [], "limit": 1})


def test_query_boolean_filter_and_value_semantics(db):
    ids = seed_query_graph(db)

    assert sorted(
        db.query_node_ids(
            {
                "label_filter": node_lf("Person"),
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
        {"label_filter": node_lf("Person"), "filter": {"property": "status", "in": ["nested"]}}
    ).items.to_list() == [ids["nested"]]
    assert db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "tag", "eq": None}}
    ).items.to_list() == [ids["null_tag"]]
    assert db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "tag", "in": [None]}}
    ).items.to_list() == [ids["null_tag"]]
    assert db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "tag", "exists": True}}
    ).items.to_list() == [ids["null_tag"]]
    assert ids["null_tag"] not in db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "tag", "missing": True}}
    ).items.to_list()

    assert db.query_node_ids(
        {
            "label_filter": node_lf("Person"),
            "filter": {"property": "payload", "eq": {"items": [1, "1", None]}},
        }
    ).items.to_list() == [ids["nested"]]
    assert db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "status", "eq": "1"}}
    ).items.to_list() == []

    int_node = db.upsert_node("Person", "int-value", props={"kind": 1})
    float_node = db.upsert_node("Person", "float-value", props={"kind": 1.0})
    assert db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "kind", "eq": 1}}
    ).items.to_list() == [int_node, float_node]
    assert db.query_node_ids(
        {"label_filter": node_lf("Person"), "filter": {"property": "kind", "eq": 1.0}}
    ).items.to_list() == [int_node, float_node]


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
            db.query_node_ids({"label_filter": node_lf("Person"), "filter": filter_expr})

    with pytest.raises(Exception, match="use filter"):
        db.query_graph_rows(
            {
                "nodes": [
                    {
                        "alias": "a",
                        "predicates": [
                            {"property": {"key": "status", "op": "eq", "value": "active"}}
                        ],
                    }
                ],
                "pieces": [],
                "limit": 1,
            }
        )


def test_query_boolean_explain_serialization(db):
    seed_query_graph(db)
    db.ensure_node_property_index(
        "Person",
        {"kind": "equality", "fields": [{"source": "property", "key": "status"}]},
    )
    wait_for_index_state(
        db,
        lambda infos: next(
            (
                info
                for info in infos
                if info.label == "Person"
                and info.fields == [{"source": "property", "key": "status"}]
            ),
            None,
        ),
    )

    indexed_or = db.explain_node_query(
        {
            "label_filter": node_lf("Person"),
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
            "label_filter": node_lf("Person"),
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
            "label_filter": node_lf("Person"),
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
        "Person", "active-high", props={"status": "active", "score": 90}
    )
    inactive = await async_db.upsert_node(
        "Person", "inactive", props={"status": "inactive", "score": 95}
    )
    acme = await async_db.upsert_node("Company", "acme")
    works_at = await async_db.upsert_edge(
        active_high, acme, "WORKS_AT", props={"role": "engineer"}
    )

    ids = await async_db.query_node_ids(
        NodeQueryRequest(label_filter=node_lf("Person"), filter={"property": "status", "eq": "active"})
    )
    assert ids.items.to_list() == [active_high]

    nodes = await async_db.query_nodes(
        {"label_filter": node_lf("Person"), "filter": {"property": "score", "gte": 80}}
    )
    assert [node.id for node in nodes.items] == [active_high, inactive]

    plan = await async_db.explain_node_query(
        {"label_filter": node_lf("Person"), "filter": {"property": "status", "eq": "active"}}
    )
    assert plan["kind"] == "node_query"

    edge_ids = await async_db.query_edge_ids(
        {"from_ids": [active_high], "filter": {"property": "role", "eq": "engineer"}}
    )
    assert edge_ids.items.to_list() == [works_at]

    edges = await async_db.query_edges(
        {"ids": [works_at], "filter": {"valid_at": int(time.time() * 1000)}}
    )
    assert [edge.id for edge in edges.items] == [works_at]

    edge_plan = await async_db.explain_edge_query({"ids": [works_at]})
    assert edge_plan["kind"] == "edge_query"

    with pytest.raises(Exception, match="use filter"):
        await async_db.query_edge_ids(
            {"label": "WORKS_AT", "where": {"role": {"eq": "engineer"}}}
        )
    with pytest.raises(Exception, match="use filter"):
        await async_db.query_edges(
            {"label": "WORKS_AT", "predicates": {"role": {"eq": "engineer"}}}
        )
    with pytest.raises(Exception, match="use filter"):
        await async_db.explain_edge_query(
            {"label": "WORKS_AT", "where": {"role": {"eq": "engineer"}}}
        )

    graph_rows = await async_db.query_graph_rows(
        {
            "nodes": [
                {
                    "alias": "person",
                    "ids": [active_high],
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "label_filter": node_lf("Company"), "keys": ["acme"]},
            ],
            "pieces": [
                {
                    "kind": "edge",
                    "alias": "employment",
                    "from": "person",
                    "to": "company",
                    "label_filter": ["WORKS_AT"],
                }
            ],
            "return": [
                {"expr": {"binding": "person"}, "as": "person"},
                {"expr": {"binding": "company"}, "as": "company"},
                {"expr": {"binding": "employment"}, "as": "employment"},
            ],
            "limit": 10,
        }
    )
    assert graph_rows["rows"][0] == {
        "company": acme,
        "person": active_high,
        "employment": works_at,
    }

    graph_plan = await async_db.explain_graph_rows(
        {
            "nodes": [
                {
                    "alias": "person",
                    "ids": [active_high],
                    "filter": {"property": "status", "eq": "active"},
                },
                {"alias": "company", "label_filter": node_lf("Company"), "keys": ["acme"]},
            ],
            "pieces": [
                {
                    "kind": "edge",
                    "alias": "employment",
                    "from": "person",
                    "to": "company",
                    "label_filter": ["WORKS_AT"],
                }
            ],
            "limit": 10,
        }
    )
    assert graph_plan["projection"]["output_mode"] == "ids"
