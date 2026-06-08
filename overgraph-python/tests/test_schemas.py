import inspect
import os

import pytest

from overgraph import OverGraph, OverGraphError


def required_string_schema():
    return {
        "additional_properties": "allow",
        "properties": {
            "name": {
                "required": True,
                "nullable": False,
                "types": ["string"],
            }
        },
    }


def full_node_schema():
    return {
        "additional_properties": "reject",
        "properties": {
            "name": {
                "required": True,
                "nullable": False,
                "types": ["string"],
                "string_min_bytes": 1,
                "string_max_bytes": 64,
                "enum_values": ["Ada"],
            },
            "payload": {
                "required": False,
                "nullable": True,
                "types": [
                    "bool",
                    "int",
                    "uint",
                    "float",
                    "number",
                    "string",
                    "bytes",
                    "array",
                    "map",
                ],
                "numeric_min": {"value": {"type": "uint", "value": "1"}, "inclusive": True},
                "numeric_max": {"value": {"type": "uint", "value": 10}, "inclusive": False},
                "string_min_bytes": 1,
                "string_max_bytes": 20,
                "bytes_min_len": 1,
                "bytes_max_len": 8,
                "array_min_items": 1,
                "array_max_items": 4,
                "map_min_entries": 1,
                "map_max_entries": 4,
                "enum_values": [
                    b"raw",
                    bytearray(b"mutable"),
                    {"type": "uint", "value": "7"},
                    [{"nested": {"type": "uint", "value": 9}}, b"x"],
                    {"blob": bytearray(b"y")},
                ],
            },
        },
        "key": {"min_bytes": 1, "max_bytes": 128, "enum_values": ["ada"]},
        "label_constraints": {
            "all_of": ["Entity"],
            "any_of": ["Person", "User"],
            "none_of": ["Deleted"],
        },
        "weight": {
            "min": {"value": 0.0, "inclusive": True},
            "max": {"value": 100.0, "inclusive": False},
            "finite": True,
        },
        "dense_vector": {"presence": "optional", "dimension": 3},
        "sparse_vector": {
            "presence": "forbidden",
            "min_entries": 1,
            "max_entries": 8,
            "max_dimension_id": 100,
        },
    }


def full_edge_schema():
    return {
        "additional_properties": "reject",
        "properties": {
            "role": {
                "required": True,
                "nullable": False,
                "types": ["string"],
                "enum_values": ["engineer"],
            }
        },
        "from": {"all_of": ["Person"], "any_of": ["Employee"], "none_of": ["Blocked"]},
        "to": {"all_of": ["Company"], "any_of": ["Org"], "none_of": ["Archived"]},
        "allow_self_loops": False,
        "weight": {
            "min": {"value": 0, "inclusive": True},
            "max": {"value": 10, "inclusive": True},
            "finite": True,
        },
        "validity": {
            "require_valid_from_before_valid_to": True,
            "valid_from_min": 0,
            "valid_from_max": 1000,
            "valid_to_min": 1,
            "valid_to_max": 2000,
            "allow_open_ended_valid_to": False,
        },
    }


def test_schema_dto_round_trip_covers_every_field(db):
    node_info = db.set_node_schema("Person", full_node_schema())
    node_schema = node_info.schema

    assert node_info.label == "Person"
    assert node_schema["additional_properties"] == "reject"
    assert node_schema["properties"]["name"]["required"] is True
    assert node_schema["properties"]["payload"]["types"] == [
        "bool",
        "int",
        "uint",
        "float",
        "number",
        "string",
        "bytes",
        "array",
        "map",
    ]
    assert node_schema["properties"]["payload"]["numeric_min"] == {
        "value": {"type": "uint", "value": 1},
        "inclusive": True,
    }
    assert node_schema["properties"]["payload"]["numeric_max"] == {
        "value": {"type": "uint", "value": 10},
        "inclusive": False,
    }
    assert node_schema["properties"]["payload"]["bytes_min_len"] == 1
    assert node_schema["properties"]["payload"]["array_max_items"] == 4
    assert node_schema["properties"]["payload"]["map_max_entries"] == 4
    assert node_schema["key"] == {"min_bytes": 1, "max_bytes": 128, "enum_values": ["ada"]}
    assert node_schema["label_constraints"] == {
        "all_of": ["Entity"],
        "any_of": ["Person", "User"],
        "none_of": ["Deleted"],
    }
    assert node_schema["weight"]["min"] == {"value": 0.0, "inclusive": True}
    assert node_schema["weight"]["max"] == {"value": 100.0, "inclusive": False}
    assert node_schema["dense_vector"] == {"presence": "optional", "dimension": 3}
    assert node_schema["sparse_vector"] == {
        "presence": "forbidden",
        "min_entries": 1,
        "max_entries": 8,
        "max_dimension_id": 100,
    }

    edge_info = db.set_edge_schema("WORKS_AT", full_edge_schema())
    edge_schema = edge_info.schema

    assert edge_info.label == "WORKS_AT"
    assert edge_schema["additional_properties"] == "reject"
    assert edge_schema["properties"]["role"]["enum_values"] == ["engineer"]
    assert edge_schema["from"] == {
        "all_of": ["Person"],
        "any_of": ["Employee"],
        "none_of": ["Blocked"],
    }
    assert edge_schema["to"] == {
        "all_of": ["Company"],
        "any_of": ["Org"],
        "none_of": ["Archived"],
    }
    assert edge_schema["allow_self_loops"] is False
    assert edge_schema["weight"] == {
        "min": {"value": 0, "inclusive": True},
        "max": {"value": 10, "inclusive": True},
        "finite": True,
    }
    assert edge_schema["validity"] == {
        "require_valid_from_before_valid_to": True,
        "valid_from_min": 0,
        "valid_from_max": 1000,
        "valid_to_min": 1,
        "valid_to_max": 2000,
        "allow_open_ended_valid_to": False,
    }

    assert [info.label for info in db.list_node_schemas()] == ["Person"]
    assert [info.label for info in db.list_edge_schemas()] == ["WORKS_AT"]
    assert db.drop_node_schema("Person") is True
    assert db.drop_node_schema("Person") is False
    assert db.get_node_schema("Person") is None


def test_schema_literal_uint_and_bytes_return_canonically(db):
    db.set_node_schema(
        "LiteralNode",
        {
            "properties": {
                "payload": {
                    "enum_values": [
                        b"bytes",
                        bytearray(b"bytearray"),
                        {"type": "uint", "value": "18446744073709551615"},
                        [{"type": "uint", "value": 4}, bytearray(b"nested")],
                        {"blob": b"map-bytes", "count": {"type": "uint", "value": 5}},
                    ]
                }
            }
        },
    )

    enum_values = db.get_node_schema("LiteralNode").schema["properties"]["payload"]["enum_values"]
    assert enum_values[0] == b"bytes"
    assert enum_values[1] == b"bytearray"
    assert enum_values[2] == {"type": "uint", "value": 18446744073709551615}
    assert enum_values[3] == [{"type": "uint", "value": 4}, b"nested"]
    assert enum_values[4] == {"blob": b"map-bytes", "count": {"type": "uint", "value": 5}}
    assert db.list_node_schemas()[0].schema["properties"]["payload"]["enum_values"] == enum_values


def test_schema_check_options_forward_scan_limit_max_violations_and_chunk_size(db):
    for index in range(3):
        db.upsert_node("OptionNode", f"bad-{index}", props={})

    report = db.check_node_schema(
        "OptionNode",
        required_string_schema(),
        max_violations=1,
        chunk_size=1,
        scan_limit=2,
    )
    assert report.checked_records == 2
    assert report.violation_count == 2
    assert len(report.violations) == 1
    assert report.truncated is True
    assert report.scan_limit_hit is True
    assert report.violations[0].target["kind"] == "node"
    assert report.violations[0].path == "properties.name"

    with pytest.raises(OverGraphError, match="scan limit"):
        db.set_node_schema(
            "OptionNode",
            required_string_schema(),
            max_violations=1,
            chunk_size=1,
            scan_limit=2,
        )
    assert db.get_node_schema("OptionNode") is None


def test_edge_schema_check_options_forward_scan_limit_max_violations_and_chunk_size(db):
    source = db.upsert_node("Endpoint", "source")
    for index in range(3):
        target = db.upsert_node("Endpoint", f"target-{index}")
        db.upsert_edge(source, target, "OPTION_EDGE", props={})

    report = db.check_edge_schema(
        "OPTION_EDGE",
        {
            "properties": {
                "role": {"required": True, "nullable": False, "types": ["string"]}
            }
        },
        max_violations=1,
        chunk_size=1,
        scan_limit=2,
    )
    assert report.checked_records == 2
    assert report.violation_count == 2
    assert len(report.violations) == 1
    assert report.truncated is True
    assert report.scan_limit_hit is True
    assert report.violations[0].target["kind"] == "edge"
    assert report.violations[0].path == "properties.role"


def test_schema_plain_oversized_int_literals_are_rejected(db):
    for value in (2**63, -(2**63) - 1):
        with pytest.raises(ValueError, match="fit signed i64|uint"):
            db.check_node_schema(
                "OversizedLiteral",
                {"properties": {"value": {"enum_values": [value]}}},
            )


def test_schema_persists_after_close_reopen(db_path):
    db = OverGraph.open(db_path)
    try:
        db.set_node_schema("PersistentNode", required_string_schema())
    finally:
        db.close()

    reopened = OverGraph.open(db_path)
    try:
        info = reopened.get_node_schema("PersistentNode")
        assert info is not None
        assert info.schema["properties"]["name"]["types"] == ["string"]
    finally:
        reopened.close()


def test_bulk_graph_schema_apis_publish_check_replace_drop_and_remain_atomic(db):
    db.upsert_node("PyBulkPerson", "ada", props={"name": "Ada"})
    db.upsert_node("PyBulkCompany", "acme")

    node_schema = required_string_schema()
    edge_schema = {
        "properties": {"since": {"required": True, "nullable": False, "types": ["int"]}},
        "from": {"any_of": ["PyBulkPerson"]},
        "to": {"any_of": ["PyBulkCompany"]},
    }

    published = db.set_graph_schema(
        {
            "node_schemas": [{"label": "PyBulkPerson", "schema": node_schema}],
            "edge_schemas": [{"label": "PY_BULK_WORKS_AT", "schema": edge_schema}],
        }
    )
    assert published.operation == "set"
    assert published.targets_published == 2
    assert published.targets_dropped == 0
    assert len(published.validation.entries) == 2
    assert [info.label for info in db.list_node_schemas()] == ["PyBulkPerson"]
    assert [info.label for info in db.list_edge_schemas()] == ["PY_BULK_WORKS_AT"]

    added = db.alter_graph_schema(
        [
            {
                "kind": "set_node",
                "label": "PyBulkCompany",
                "schema": {"properties": {"name": {"types": ["string"]}}},
            }
        ]
    )
    assert added.operation == "add"
    assert added.targets_published == 1
    assert [info.label for info in db.list_node_schemas()] == [
        "PyBulkCompany",
        "PyBulkPerson",
    ]

    check_add = db.check_graph_schema_add(
        {
            "node_schemas": [
                {"label": "PyDryRunOnly", "schema": required_string_schema()}
            ]
        }
    )
    assert check_add.operation == "check_add"
    assert check_add.entries[0].target_kind == "node"
    assert db.get_node_schema("PyDryRunOnly") is None

    check_set = db.check_graph_schema_set({"node_schemas": []})
    assert check_set.operation == "check_set"
    assert check_set.entries == []
    assert [info.label for info in db.list_node_schemas()] == [
        "PyBulkCompany",
        "PyBulkPerson",
    ]

    drop_selected = db.alter_graph_schema(
        [
            {"kind": "drop_node", "label": "PyBulkCompany"},
            {"kind": "drop_edge", "label": "PY_MISSING_EDGE"},
            {"kind": "drop_edge", "label": "PY_BULK_WORKS_AT"},
        ]
    )
    assert drop_selected.operation == "drop"
    assert [
        (target.target_kind, target.label, target.action)
        for target in drop_selected.drop_targets
    ] == [
        ("node", "PyBulkCompany", "dropped"),
        ("edge", "PY_MISSING_EDGE", "not_found"),
        ("edge", "PY_BULK_WORKS_AT", "dropped"),
    ]
    assert drop_selected.targets_dropped == 2
    assert [info.label for info in db.list_node_schemas()] == ["PyBulkPerson"]
    assert db.list_edge_schemas() == []

    replaced = db.set_graph_schema(
        {"edge_schemas": [{"label": "PY_REPLACEMENT_EDGE", "schema": {"properties": {}}}]}
    )
    assert replaced.operation == "set"
    assert replaced.targets_published == 1
    assert replaced.targets_dropped == 1
    assert replaced.node_schemas_dropped == 1
    assert replaced.edge_schemas_dropped == 0
    assert db.list_node_schemas() == []
    assert [info.label for info in db.list_edge_schemas()] == ["PY_REPLACEMENT_EDGE"]

    dropped = db.drop_graph_schema()
    assert dropped.operation == "drop_all"
    assert dropped.targets_dropped == 1
    assert dropped.node_schemas_dropped == 0
    assert dropped.edge_schemas_dropped == 1
    assert db.list_node_schemas() == []
    assert db.list_edge_schemas() == []

    db.upsert_node("PyViolatingBulk", "bad", props={})
    with pytest.raises(OverGraphError, match="schema violation|validation"):
        db.set_graph_schema(
            {
                "node_schemas": [
                    {"label": "PyCleanBulk", "schema": {"properties": {}}},
                    {"label": "PyViolatingBulk", "schema": required_string_schema()},
                ]
            }
        )
    assert db.get_node_schema("PyCleanBulk") is None
    assert db.get_node_schema("PyViolatingBulk") is None


def test_schema_enforcement_reaches_native_python_write_paths(db):
    db.set_node_schema("StrictPerson", required_string_schema())

    with pytest.raises(OverGraphError, match="schema violation"):
        db.upsert_node("StrictPerson", "bad-upsert", props={})

    with pytest.raises(OverGraphError, match="schema violation"):
        db.batch_upsert_nodes([{"labels": ["StrictPerson"], "key": "bad-batch"}])
    assert db.get_node_by_key("StrictPerson", "bad-batch") is None

    with pytest.raises(OverGraphError, match="schema violation"):
        db.graph_patch({"upsert_nodes": [{"labels": ["StrictPerson"], "key": "bad-patch"}]})
    assert db.get_node_by_key("StrictPerson", "bad-patch") is None

    txn = db.begin_write_txn()
    txn.upsert_node_as("bad", "StrictPerson", "bad-txn", props={})
    with pytest.raises(OverGraphError, match="schema violation"):
        txn.commit()
    assert db.get_node_by_key("StrictPerson", "bad-txn") is None


def test_schema_stub_and_signature_smoke():
    assert hasattr(OverGraph, "set_node_schema")
    assert hasattr(OverGraph, "check_edge_schema")
    try:
        signature = str(inspect.signature(OverGraph.set_node_schema))
    except (TypeError, ValueError):
        signature = getattr(OverGraph.set_node_schema, "__text_signature__", "")
    assert "max_violations" in signature
    assert "chunk_size" in signature
    assert "scan_limit" in signature

    stub_path = os.path.join(os.path.dirname(__file__), "..", "python", "overgraph", "__init__.pyi")
    with open(stub_path, encoding="utf-8") as stub:
        text = stub.read()
    assert "class NodeSchemaInfo" in text
    assert "class SchemaValidationReport" in text
    assert "class GraphSchemaPublishResult" in text
    assert "def set_graph_schema" in text
    assert "class SchemaUIntLiteral" in text
    assert "def set_node_schema" in text
    assert "async def set_edge_schema" in text
    assert "async def alter_graph_schema" in text
    assert "scan_limit: int | None = None" in text
