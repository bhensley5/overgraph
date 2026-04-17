import time

import pytest

from overgraph import OverGraphError, PyPropertyRangeBound, PyPropertyRangeCursor


def wait_for_index_state(db, predicate, expected_state="ready", timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = predicate(db.list_node_property_indexes())
        if info is not None and info.state == expected_state:
            return info
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for secondary index state '{expected_state}'")


class TestPropertyIndexes:
    def test_ensure_list_drop(self, db):
        for i in range(6):
            db.upsert_node(
                1,
                f"node-{i}",
                props={
                    "color": "red" if i % 2 == 0 else "blue",
                    "score": (i + 1) * 10,
                    "temp": (i + 1) * 5,
                },
            )

        eq = db.ensure_node_property_index(1, "color", "equality")
        assert eq.kind == "equality"
        assert eq.domain is None
        assert eq.state == "building"

        range_info = db.ensure_node_property_index(1, "score", "range", domain="int")
        assert range_info.kind == "range"
        assert range_info.domain == "int"
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
        assert ready_range.domain == "int"

        listed = db.list_node_property_indexes()
        assert sorted((info.prop_key, info.kind, info.domain, info.state) for info in listed) == [
            ("color", "equality", None, "ready"),
            ("score", "range", "int", "ready"),
        ]

        with pytest.raises(OverGraphError, match="different domain"):
            db.ensure_node_property_index(1, "score", "range", domain="float")

        assert db.drop_node_property_index(1, "color", "equality") is True
        assert db.drop_node_property_index(1, "color", "equality") is False

    def test_range_queries_and_paging(self, db):
        inserted = []
        for i in range(6):
            inserted.append(
                db.upsert_node(
                    1,
                    f"node-{i}",
                    props={"score": (i + 1) * 10, "temp": (i + 1) * 5},
                )
            )

        db.ensure_node_property_index(1, "score", "range", domain="int")
        wait_for_index_state(
            db,
            lambda infos: next(
                (info for info in infos if info.prop_key == "score" and info.kind == "range"),
                None,
            ),
        )

        lower = PyPropertyRangeBound(20, domain="int")
        upper = PyPropertyRangeBound(50, inclusive=False, domain="int")
        all_ids = db.find_nodes_range(1, "score", lower, upper).to_list()
        assert len(all_ids) == 3

        first = db.find_nodes_range_paged(1, "score", lower, upper, limit=2)
        assert first.items.to_list() == all_ids[:2]
        assert first.next_cursor is not None
        assert first.next_cursor.domain == "int"
        assert isinstance(first.next_cursor.node_id, int)

        second = db.find_nodes_range_paged(
            1,
            "score",
            lower,
            upper,
            limit=2,
            after=first.next_cursor,
        )
        assert second.items.to_list() == all_ids[2:]
        assert second.next_cursor is None

        fallback = db.find_nodes_range(
            1,
            "temp",
            PyPropertyRangeBound(10, domain="int"),
            PyPropertyRangeBound(25, domain="int"),
        )
        assert len(fallback) == 4

    def test_binding_validation_errors(self, db):
        with pytest.raises(ValueError, match="Invalid index kind"):
            db.ensure_node_property_index(1, "score", "bogus")

        with pytest.raises(ValueError, match="require domain"):
            db.ensure_node_property_index(1, "score", "range")

        with pytest.raises(ValueError, match="do not accept a range domain"):
            db.ensure_node_property_index(1, "score", "equality", domain="int")

        with pytest.raises(ValueError, match="Invalid range domain"):
            PyPropertyRangeBound(10, domain="bogus")

        with pytest.raises(OverGraphError, match="same PropValue variant"):
            db.find_nodes_range(
                1,
                "score",
                PyPropertyRangeBound(10, domain="int"),
                PyPropertyRangeBound(20.0, domain="float"),
            )

        with pytest.raises(OverGraphError, match="cursor must use the same PropValue variant"):
            db.find_nodes_range_paged(
                1,
                "score",
                PyPropertyRangeBound(10, domain="int"),
                PyPropertyRangeBound(20, domain="int"),
                limit=2,
                after=PyPropertyRangeCursor(15.0, 1, domain="float"),
            )


@pytest.mark.asyncio
class TestPropertyIndexesAsync:
    async def test_async_property_index_and_range_apis(self, async_db):
        for i in range(6):
            await async_db.upsert_node(
                1,
                f"node-{i}",
                props={"score": (i + 1) * 10, "temp": (i + 1) * 5},
            )

        eq = await async_db.ensure_node_property_index(1, "temp", "equality")
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
            1,
            "score",
            PyPropertyRangeBound(20, domain="int"),
            PyPropertyRangeBound(30, domain="int"),
        )
        assert ids.to_list() and len(ids) == 2

        page = await async_db.find_nodes_range_paged(
            1,
            "score",
            PyPropertyRangeBound(20, domain="int"),
            PyPropertyRangeBound(40, domain="int"),
            limit=2,
        )
        assert len(page.items) == 2
        assert page.next_cursor is not None
        assert page.next_cursor.domain == "int"

        assert await async_db.drop_node_property_index(1, "temp", "equality") is True
