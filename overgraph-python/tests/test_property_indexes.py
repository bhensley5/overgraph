import time

import pytest

from overgraph import OverGraphError, PropertyRangeBound, PropertyRangeCursor


def wait_for_index_state(db, predicate, expected_state="ready", timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = predicate(db.list_node_property_indexes())
        if info is not None and info.state == expected_state:
            return info
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for secondary index state '{expected_state}'")


def wait_for_edge_index_state(db, predicate, expected_state="ready", timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = predicate(db.list_edge_property_indexes())
        if info is not None and info.state == expected_state:
            return info
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for edge secondary index state '{expected_state}'")


def plan_has_kind(node, kind):
    if not node:
        return False
    if node.get("kind") == kind:
        return True
    if "input" in node and plan_has_kind(node["input"], kind):
        return True
    return any(plan_has_kind(child, kind) for child in node.get("inputs", []))


def graph_explain_has_text(nodes, text):
    text = text.lower()
    for node in nodes:
        if text in node.get("kind", "").lower() or text in node.get("detail", "").lower():
            return True
        if graph_explain_has_text(node.get("children", []), text):
            return True
    return False


class TestPropertyIndexes:
    def test_ensure_list_drop(self, db):
        for i in range(6):
            db.upsert_node(
                "Person",
                f"node-{i}",
                props={
                    "color": "red" if i % 2 == 0 else "blue",
                    "score": (i + 1) * 10,
                    "temp": (i + 1) * 5,
                },
            )

        eq = db.ensure_node_property_index("Person", "color", "equality")
        assert eq.kind == "equality"
        assert not hasattr(eq, "domain")
        assert eq.state == "building"

        range_info = db.ensure_node_property_index("Person", "score", "range")
        assert range_info.kind == "range"
        assert not hasattr(range_info, "domain")
        assert range_info.state == "building"

        wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "color" and info.kind == "equality"),
                None,
            ),
        )
        ready_range = wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "score" and info.kind == "range"),
                None,
            ),
        )
        assert not hasattr(ready_range, "domain")

        listed = db.list_node_property_indexes()
        assert sorted((info.prop_key, info.kind, hasattr(info, "domain"), info.state) for info in listed) == [
            ("color", "equality", False, "ready"),
            ("score", "range", False, "ready"),
        ]

        assert db.drop_node_property_index("Person", "color", "equality") is True
        assert db.drop_node_property_index("Person", "color", "equality") is False

    def test_range_queries_and_paging(self, db):
        inserted = []
        for i in range(6):
            inserted.append(
                db.upsert_node(
                    "Person",
                    f"node-{i}",
                    props={"score": (i + 1) * 10, "temp": (i + 1) * 5},
                )
            )

        db.ensure_node_property_index("Person", "score", "range")
        wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "score" and info.kind == "range"),
                None,
            ),
        )

        lower = PropertyRangeBound(20, domain="int")
        upper = PropertyRangeBound(50, inclusive=False, domain="int")
        all_ids = db.find_nodes_range("Person", "score", lower, upper).to_list()
        assert len(all_ids) == 3

        first = db.find_nodes_range_paged("Person", "score", lower, upper, limit=2)
        assert first.items.to_list() == all_ids[:2]
        assert first.next_cursor is not None
        assert first.next_cursor.domain == "int"
        assert isinstance(first.next_cursor.node_id, int)

        second = db.find_nodes_range_paged(
            "Person",
            "score",
            lower,
            upper,
            limit=2,
            after=first.next_cursor,
        )
        assert second.items.to_list() == all_ids[2:]
        assert second.next_cursor is None

        fallback = db.find_nodes_range(
            "Person",
            "temp",
            PropertyRangeBound(10, domain="int"),
            PropertyRangeBound(25, domain="int"),
        )
        assert len(fallback) == 4

        mixed_bounds = db.find_nodes_range(
            "Person",
            "score",
            PropertyRangeBound(20, domain="int"),
            PropertyRangeBound(40.0, domain="float"),
        )
        assert mixed_bounds.to_list() == inserted[1:4]

        mixed_cursor = db.find_nodes_range_paged(
            "Person",
            "score",
            PropertyRangeBound(20, domain="int"),
            PropertyRangeBound(40, domain="int"),
            limit=10,
            after=PropertyRangeCursor(20.0, inserted[1], domain="float"),
        )
        assert mixed_cursor.items.to_list() == inserted[2:4]

    def test_binding_validation_errors(self, db):
        with pytest.raises(ValueError, match="Invalid index kind"):
            db.ensure_node_property_index("Person", "score", "bogus")

        assert db.ensure_node_property_index("Person", "score", "range").kind == "range"

        with pytest.raises(ValueError, match="Invalid range value type annotation"):
            PropertyRangeBound(10, domain="bogus")

        assert len(
            db.find_nodes_range(
                "Person",
                "score",
                PropertyRangeBound(10, domain="int"),
                PropertyRangeBound(20.0, domain="float"),
            )
        ) == 0

        page = db.find_nodes_range_paged(
            "Person",
            "score",
            PropertyRangeBound(10, domain="int"),
            PropertyRangeBound(20, domain="int"),
            limit=2,
            after=PropertyRangeCursor(15.0, 1, domain="float"),
        )
        assert len(page.items) == 0


@pytest.mark.asyncio
class TestPropertyIndexesAsync:
    async def test_async_property_index_and_range_apis(self, async_db):
        for i in range(6):
            await async_db.upsert_node(
                "Person",
                f"node-{i}",
                props={"score": (i + 1) * 10, "temp": (i + 1) * 5},
            )

        eq = await async_db.ensure_node_property_index("Person", "temp", "equality")
        assert eq.kind == "equality"

        deadline = time.time() + 5.0
        while time.time() < deadline:
            infos = await async_db.list_node_property_indexes()
            ready = next(
                (info for info in infos if info.prop_key == "temp" and info.kind == "equality"),
                None,
            )
            if ready is not None and ready.state == "ready":
                break
            await __import__("asyncio").sleep(0.02)
        else:
            raise AssertionError("timed out waiting for async equality index to become ready")

        ids = await async_db.find_nodes_range(
            "Person",
            "score",
            PropertyRangeBound(20, domain="int"),
            PropertyRangeBound(30, domain="int"),
        )
        assert ids.to_list() and len(ids) == 2

        page = await async_db.find_nodes_range_paged(
            "Person",
            "score",
            PropertyRangeBound(20, domain="int"),
            PropertyRangeBound(40, domain="int"),
            limit=2,
        )
        assert len(page.items) == 2
        assert page.next_cursor is not None
        assert page.next_cursor.domain == "int"

        assert await async_db.drop_node_property_index("Person", "temp", "equality") is True


class TestEdgePropertyIndexes:
    def test_ensure_list_validate_and_drop_edge_property_indexes(self, db):
        eq = db.ensure_edge_property_index("RELATES_TO", "status", "equality")
        assert eq.kind == "equality"
        assert not hasattr(eq, "domain")
        assert eq.state == "building"

        range_info = db.ensure_edge_property_index("RELATES_TO", "score", "range")
        assert range_info.kind == "range"
        assert not hasattr(range_info, "domain")
        assert range_info.state == "building"

        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "status" and info.kind == "equality"),
                None,
            ),
        )
        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "score" and info.kind == "range"),
                None,
            ),
        )

        listed = db.list_edge_property_indexes()
        assert sorted((info.prop_key, info.kind, hasattr(info, "domain"), info.state) for info in listed) == [
            ("score", "range", False, "ready"),
            ("status", "equality", False, "ready"),
        ]

        assert db.ensure_edge_property_index("RELATES_TO", "score", "range").kind == "range"

        assert db.drop_edge_property_index("RELATES_TO", "missing", "equality") is False

    def test_edge_property_index_queries_and_pattern_explain(self, db):
        db.ensure_edge_property_index("RELATES_TO", "status", "equality")
        db.ensure_edge_property_index("RELATES_TO", "score", "range")
        source = db.upsert_node("Person", "source")
        hot_target = db.upsert_node("Company", "hot-target")
        cold_target = db.upsert_node("Company", "cold-target")
        hot_edge = db.upsert_edge(source, hot_target, "RELATES_TO", props={"status": "hot", "score": 90})
        db.upsert_edge(source, cold_target, "RELATES_TO", props={"status": "cold", "score": 10})

        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "status" and info.kind == "equality"),
                None,
            ),
        )
        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "score" and info.kind == "range"),
                None,
            ),
        )

        direct = db.query_edge_ids(
            {
                "label": "RELATES_TO",
                "from_ids": [source],
                "filter": {"property": "status", "eq": "hot"},
                "limit": 10,
            }
        )
        assert direct.items.to_list() == [hot_edge]

        direct_plan = db.explain_edge_query(
            {
                "label": "RELATES_TO",
                "from_ids": [source],
                "filter": {"property": "status", "eq": "hot"},
                "limit": 10,
            }
        )
        assert plan_has_kind(direct_plan["root"], "edge_property_equality_index")

        direct_range = db.query_edge_ids(
            {
                "label": "RELATES_TO",
                "from_ids": [source],
                "filter": {"property": "score", "gte": 80},
                "limit": 10,
            }
        )
        assert direct_range.items.to_list() == [hot_edge]

        direct_range_plan = db.explain_edge_query(
            {
                "label": "RELATES_TO",
                "from_ids": [source],
                "filter": {"property": "score", "gte": 80},
                "limit": 10,
            }
        )
        assert plan_has_kind(direct_range_plan["root"], "edge_property_range_index")

        pattern = {
            "nodes": [
                {"alias": "a", "label_filter": {"labels": ["Person"], "mode": "all"}},
                {"alias": "b", "label_filter": {"labels": ["Company"], "mode": "all"}},
            ],
            "pieces": [
                {
                    "kind": "edge",
                    "alias": "e",
                    "from": "a",
                    "to": "b",
                    "direction": "outgoing",
                    "label_filter": ["RELATES_TO"],
                    "filter": {"property": "status", "eq": "hot"},
                }
            ],
            "return": [
                {"expr": {"binding": "a"}, "as": "a"},
                {"expr": {"binding": "b"}, "as": "b"},
                {"expr": {"binding": "e"}, "as": "e"},
            ],
            "limit": 10,
        }
        assert db.query_graph_rows(pattern)["rows"] == [
            {"a": source, "b": hot_target, "e": hot_edge}
        ]
        pattern_plan = db.explain_graph_rows(pattern)
        assert pattern_plan["projection"]["output_mode"] == "ids"
        assert graph_explain_has_text(pattern_plan["plan"], "EdgePropertyEqualityIndex")

        range_pattern = {
            **pattern,
            "pieces": [
                {
                    **pattern["pieces"][0],
                    "filter": {"property": "score", "gte": 80},
                }
            ],
        }
        assert db.query_graph_rows(range_pattern)["rows"] == [
            {"a": source, "b": hot_target, "e": hot_edge}
        ]
        range_pattern_plan = db.explain_graph_rows(range_pattern)
        assert range_pattern_plan["projection"]["output_mode"] == "ids"
        assert graph_explain_has_text(range_pattern_plan["plan"], "EdgePropertyRangeIndex")


@pytest.mark.asyncio
class TestEdgePropertyIndexesAsync:
    async def test_async_edge_property_index_apis(self, async_db):
        info = await async_db.ensure_edge_property_index("RELATES_TO", "temp", "equality")
        assert info.kind == "equality"

        deadline = time.time() + 5.0
        while time.time() < deadline:
            infos = await async_db.list_edge_property_indexes()
            ready = next(
                (info for info in infos if info.prop_key == "temp" and info.kind == "equality"),
                None,
            )
            if ready is not None and ready.state == "ready":
                break
            await __import__("asyncio").sleep(0.02)
        else:
            raise AssertionError("timed out waiting for async edge equality index to become ready")

        listed = await async_db.list_edge_property_indexes()
        assert any(info.prop_key == "temp" and info.kind == "equality" for info in listed)

        assert await async_db.drop_edge_property_index("RELATES_TO", "temp", "equality") is True
