import asyncio
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


async def wait_for_async_index_state(db, predicate, expected_state="ready", timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = predicate(await db.list_node_property_indexes())
        if info is not None and info.state == expected_state:
            return info
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for secondary index state '{expected_state}'")


async def wait_for_async_edge_index_state(db, predicate, expected_state="ready", timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = predicate(await db.list_edge_property_indexes())
        if info is not None and info.state == expected_state:
            return info
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for edge secondary index state '{expected_state}'")


def plan_has_kind(node, kind):
    if not node:
        return False
    if node.get("kind") == kind:
        return True
    if "input" in node and plan_has_kind(node["input"], kind):
        return True
    return any(plan_has_kind(child, kind) for child in node.get("inputs", []))


def property_index_spec(prop_key, kind):
    return {"kind": kind, "fields": [{"source": "property", "key": prop_key}]}


def has_property_field(info, prop_key):
    return (
        len(info.fields) == 1
        and info.fields[0]["source"] == "property"
        and info.fields[0]["key"] == prop_key
    )


def same_fields(actual, expected):
    return actual == expected


def property_field_key(info):
    assert len(info.fields) == 1
    assert info.fields[0]["source"] == "property"
    return info.fields[0]["key"]


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

        color_spec = property_index_spec("color", "equality")
        score_spec = property_index_spec("score", "range")
        eq = db.ensure_node_property_index("Person", color_spec)
        assert eq.kind == "equality"
        assert not hasattr(eq, "domain")
        assert eq.state == "building"
        assert eq.fields == color_spec["fields"]
        assert eq.compound is False

        range_info = db.ensure_node_property_index("Person", score_spec)
        assert range_info.kind == "range"
        assert not hasattr(range_info, "domain")
        assert range_info.state == "building"
        assert range_info.fields == score_spec["fields"]

        wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "color") and info.kind == "equality"),
                None,
            ),
        )
        ready_range = wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "score") and info.kind == "range"),
                None,
            ),
        )
        assert not hasattr(ready_range, "domain")

        listed = db.list_node_property_indexes()
        assert sorted((property_field_key(info), info.kind, hasattr(info, "domain"), info.state, info.compound) for info in listed) == [
            ("color", "equality", False, "ready", False),
            ("score", "range", False, "ready", False),
        ]

        assert db.drop_node_property_index("Person", color_spec) is True
        assert db.drop_node_property_index("Person", color_spec) is False

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

        db.ensure_node_property_index("Person", property_index_spec("score", "range"))
        wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "score") and info.kind == "range"),
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
        with pytest.raises(TypeError, match="invalid secondary index: spec must be a mapping"):
            db.ensure_node_property_index("Person", ["not", "a", "mapping"])

        with pytest.raises(ValueError, match="invalid secondary index: secondary index spec does not accept field 'name'"):
            db.ensure_node_property_index(
                "Person",
                {"kind": "equality", "fields": [{"source": "property", "key": "score"}], "name": "idx"},
            )

        with pytest.raises(ValueError, match="invalid secondary index: secondary index field does not accept field 'order'"):
            db.ensure_node_property_index(
                "Person",
                {"kind": "equality", "fields": [{"source": "property", "key": "score", "order": "asc"}]},
            )

        with pytest.raises(ValueError, match="invalid secondary index: kind is required"):
            db.ensure_node_property_index("Person", {"fields": [{"source": "property", "key": "score"}]})

        with pytest.raises(ValueError, match="invalid secondary index: fields are required"):
            db.ensure_node_property_index("Person", {"kind": "equality"})

        with pytest.raises(ValueError, match="invalid secondary index: field source is required"):
            db.ensure_node_property_index("Person", {"kind": "equality", "fields": [{"key": "score"}]})

        with pytest.raises(ValueError, match="invalid secondary index"):
            db.ensure_node_property_index("Person", property_index_spec("score", "bogus"))

        assert db.ensure_node_property_index("Person", property_index_spec("score", "range")).kind == "range"

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

    def test_declares_uses_and_drops_compound_field_list_indexes(self, db):
        for i in range(6):
            db.upsert_node(
                "Person",
                f"compound-node-{i}",
                props={"color": "red" if i % 2 == 0 else "blue"},
            )

        compound_spec = {
            "kind": "range",
            "fields": [
                {"source": "property", "key": "color"},
                {"source": "metadata", "field": "updated_at"},
            ],
        }
        info = db.ensure_node_property_index("Person", compound_spec)
        assert info.compound is True
        assert info.fields == compound_spec["fields"]
        ready = wait_for_index_state(
            db,
            lambda infos: next(
                (
                    index
                    for index in infos
                    if index.label == "Person"
                    and index.kind == "range"
                    and same_fields(index.fields, compound_spec["fields"])
                ),
                None,
            ),
        )
        assert ready.state == "ready"
        assert ready.compound is True

        query = {
            "label_filter": {"labels": ["Person"], "mode": "all"},
            "filter": {
                "and": [
                    {"property": "color", "eq": "red"},
                    {"updated_at": {"gte": 0}},
                ]
            },
            "limit": 10,
        }
        assert len(db.query_node_ids(query).items) == 3
        plan = db.explain_node_query(query)
        assert plan_has_kind(plan["root"], "compound_range_index")
        assert db.drop_node_property_index("Person", compound_spec) is True
        assert db.drop_node_property_index("Person", compound_spec) is False


@pytest.mark.asyncio
class TestPropertyIndexesAsync:
    async def test_async_property_index_and_range_apis(self, async_db):
        for i in range(6):
            await async_db.upsert_node(
                "Person",
                f"node-{i}",
                props={"score": (i + 1) * 10, "temp": (i + 1) * 5},
            )

        temp_spec = property_index_spec("temp", "equality")
        eq = await async_db.ensure_node_property_index("Person", temp_spec)
        assert eq.kind == "equality"

        await wait_for_async_index_state(
            async_db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "temp") and info.kind == "equality"),
                None,
            ),
        )

        compound_spec = {
            "kind": "range",
            "fields": [
                {"source": "property", "key": "score"},
                {"source": "metadata", "field": "updated_at"},
            ],
        }
        compound = await async_db.ensure_node_property_index("Person", compound_spec)
        assert compound.compound is True
        assert compound.fields == compound_spec["fields"]
        await wait_for_async_index_state(
            async_db,
            lambda infos: next(
                (
                    info
                    for info in infos
                    if info.label == "Person"
                    and info.kind == "range"
                    and same_fields(info.fields, compound_spec["fields"])
                ),
                None,
            ),
        )

        listed = await async_db.list_node_property_indexes()
        assert any(info.compound and same_fields(info.fields, compound_spec["fields"]) for info in listed)

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

        assert await async_db.drop_node_property_index("Person", temp_spec) is True
        assert await async_db.drop_node_property_index("Person", compound_spec) is True


class TestEdgePropertyIndexes:
    def test_ensure_list_validate_and_drop_edge_property_indexes(self, db):
        status_spec = property_index_spec("status", "equality")
        score_spec = property_index_spec("score", "range")
        eq = db.ensure_edge_property_index("RELATES_TO", status_spec)
        assert eq.kind == "equality"
        assert not hasattr(eq, "domain")
        assert eq.state == "building"

        range_info = db.ensure_edge_property_index("RELATES_TO", score_spec)
        assert range_info.kind == "range"
        assert not hasattr(range_info, "domain")
        assert range_info.state == "building"

        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "status") and info.kind == "equality"),
                None,
            ),
        )
        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "score") and info.kind == "range"),
                None,
            ),
        )

        listed = db.list_edge_property_indexes()
        assert sorted((property_field_key(info), info.kind, hasattr(info, "domain"), info.state, info.compound) for info in listed) == [
            ("score", "range", False, "ready", False),
            ("status", "equality", False, "ready", False),
        ]

        assert db.ensure_edge_property_index("RELATES_TO", score_spec).kind == "range"

        assert db.drop_edge_property_index("RELATES_TO", property_index_spec("missing", "equality")) is False

    def test_declares_uses_and_drops_edge_field_list_indexes(self, db):
        compound_spec = {
            "kind": "range",
            "fields": [
                {"source": "metadata", "field": "from"},
                {"source": "property", "key": "score"},
            ],
        }
        source = db.upsert_node("Person", "compound-source")
        hot_target = db.upsert_node("Company", "compound-hot-target")
        cold_target = db.upsert_node("Company", "compound-cold-target")
        hot_edge = db.upsert_edge(source, hot_target, "COMPOUND_RELATES_TO", props={"score": 90})
        db.upsert_edge(source, cold_target, "COMPOUND_RELATES_TO", props={"score": 10})

        info = db.ensure_edge_property_index("COMPOUND_RELATES_TO", compound_spec)
        assert info.compound is True
        assert info.fields == compound_spec["fields"]
        ready = wait_for_edge_index_state(
            db,
            lambda infos: next(
                (
                    index
                    for index in infos
                    if index.label == "COMPOUND_RELATES_TO"
                    and index.kind == "range"
                    and same_fields(index.fields, compound_spec["fields"])
                ),
                None,
            ),
        )
        assert ready.state == "ready"
        assert ready.compound is True

        query = {
            "label": "COMPOUND_RELATES_TO",
            "from_ids": [source],
            "filter": {"property": "score", "gte": 80},
            "limit": 10,
        }
        assert db.query_edge_ids(query).items.to_list() == [hot_edge]
        plan = db.explain_edge_query(query)
        assert plan_has_kind(plan["root"], "compound_range_index")
        assert db.drop_edge_property_index("COMPOUND_RELATES_TO", compound_spec) is True
        assert db.drop_edge_property_index("COMPOUND_RELATES_TO", compound_spec) is False

    def test_edge_property_index_queries_and_pattern_explain(self, db):
        db.ensure_edge_property_index("RELATES_TO", property_index_spec("status", "equality"))
        db.ensure_edge_property_index("RELATES_TO", property_index_spec("score", "range"))
        source = db.upsert_node("Person", "source")
        hot_target = db.upsert_node("Company", "hot-target")
        cold_target = db.upsert_node("Company", "cold-target")
        hot_edge = db.upsert_edge(source, hot_target, "RELATES_TO", props={"status": "hot", "score": 90})
        db.upsert_edge(source, cold_target, "RELATES_TO", props={"status": "cold", "score": 10})

        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "status") and info.kind == "equality"),
                None,
            ),
        )
        wait_for_edge_index_state(
            db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "score") and info.kind == "range"),
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
        temp_spec = property_index_spec("temp", "equality")
        info = await async_db.ensure_edge_property_index("RELATES_TO", temp_spec)
        assert info.kind == "equality"

        await wait_for_async_edge_index_state(
            async_db,
            lambda infos: next(
                (info for info in infos if has_property_field(info, "temp") and info.kind == "equality"),
                None,
            ),
        )

        listed = await async_db.list_edge_property_indexes()
        assert any(has_property_field(info, "temp") and info.kind == "equality" for info in listed)

        compound_spec = {
            "kind": "range",
            "fields": [
                {"source": "metadata", "field": "from"},
                {"source": "property", "key": "temp"},
            ],
        }
        compound = await async_db.ensure_edge_property_index("RELATES_TO", compound_spec)
        assert compound.compound is True
        assert compound.fields == compound_spec["fields"]
        await wait_for_async_edge_index_state(
            async_db,
            lambda infos: next(
                (
                    info
                    for info in infos
                    if info.label == "RELATES_TO"
                    and info.kind == "range"
                    and same_fields(info.fields, compound_spec["fields"])
                ),
                None,
            ),
        )

        listed = await async_db.list_edge_property_indexes()
        assert any(info.compound and same_fields(info.fields, compound_spec["fields"]) for info in listed)

        assert await async_db.drop_edge_property_index("RELATES_TO", temp_spec) is True
        assert await async_db.drop_edge_property_index("RELATES_TO", compound_spec) is True
