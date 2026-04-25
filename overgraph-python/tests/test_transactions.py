import os

import pytest
from overgraph import OverGraph, OverGraphError


def test_stage_alias_payload_commits_atomically(db):
    txn = db.begin_write_txn()
    txn.stage(
        [
            {
                "op": "upsert_node",
                "alias": "alice",
                "type_id": 1,
                "key": "alice",
                "props": {"name": "Alice"},
            },
            {"op": "upsert_node", "alias": "bob", "type_id": 1, "key": "bob"},
            {
                "op": "upsert_edge",
                "alias": "knows",
                "from": {"local": "alice"},
                "to": {"local": "bob"},
                "type_id": 7,
                "props": {"since": 2026},
            },
        ]
    )

    staged_alice = txn.get_node({"local": "alice"})
    assert staged_alice["id"] is None
    assert staged_alice["local"] == "alice"
    assert staged_alice["props"]["name"] == "Alice"

    staged_edge = txn.get_edge({"local": "knows"})
    assert staged_edge["id"] is None
    assert staged_edge["from"]["local"] == "alice"
    assert staged_edge["to"]["local"] == "bob"

    result = txn.commit()
    assert len(result.node_ids) == 2
    assert len(result.edge_ids) == 1
    assert result.node_aliases["alice"] == result.node_ids[0]
    assert result.node_aliases["bob"] == result.node_ids[1]
    assert result.edge_aliases["knows"] == result.edge_ids[0]

    edge = db.get_edge(result.edge_aliases["knows"])
    assert edge.from_id == result.node_aliases["alice"]
    assert edge.to_id == result.node_aliases["bob"]


def test_builder_aliases_read_own_writes_and_rollback(db):
    txn = db.begin_write_txn()
    alice = txn.upsert_node_as("alice", 1, "alice", props={"mood": "staged"})
    bob = txn.upsert_node_as("bob", 1, "bob")
    txn.upsert_edge_as("knows", alice, bob, 9)

    assert txn.get_node_by_key(1, "alice")["props"]["mood"] == "staged"
    txn.rollback()
    assert db.get_node_by_key(1, "alice") is None
    with pytest.raises(OverGraphError, match="transaction is closed"):
        txn.commit()


def test_unaliased_builder_refs_create_and_connect(db):
    txn = db.begin_write_txn()
    alice = txn.upsert_node(1, "alice")
    bob = txn.upsert_node(1, "bob")
    edge_ref = txn.upsert_edge(alice, bob, 7)

    assert alice == {"type_id": 1, "key": "alice"}
    assert bob == {"type_id": 1, "key": "bob"}
    assert txn.get_edge(edge_ref)["type_id"] == 7

    result = txn.commit()
    edge = db.get_edge(result.edge_ids[0])
    assert edge.from_id == result.node_ids[0]
    assert edge.to_id == result.node_ids[1]


def test_stage_delete_and_invalidate_operations_read_own_writes(db):
    a, b, c, d = db.batch_upsert_nodes(
        [
            {"type_id": 1, "key": "a"},
            {"type_id": 1, "key": "b"},
            {"type_id": 1, "key": "c"},
            {"type_id": 1, "key": "d"},
        ]
    )
    active_edge, deleted_edge, cascaded_edge = db.batch_upsert_edges(
        [
            {"from_id": a, "to_id": b, "type_id": 7},
            {"from_id": b, "to_id": c, "type_id": 8},
            {"from_id": c, "to_id": d, "type_id": 9},
        ]
    )

    txn = db.begin_write_txn()
    txn.stage(
        [
            {"op": "invalidate_edge", "target": {"id": active_edge}, "valid_to": 12345},
            {"op": "delete_edge", "target": {"id": deleted_edge}},
            {"op": "delete_node", "target": {"id": d}},
        ]
    )

    assert txn.get_edge({"id": active_edge})["valid_to"] == 12345
    assert txn.get_edge({"id": deleted_edge}) is None
    assert txn.get_node({"id": d}) is None
    assert txn.get_edge({"id": cascaded_edge}) is None

    txn.commit()
    assert db.get_edge(active_edge).valid_to == 12345
    assert db.get_edge(deleted_edge) is None
    assert db.get_node(d) is None
    assert db.get_edge(cascaded_edge) is None


def test_rejects_malformed_refs_missing_fields_and_duplicate_aliases(db):
    malformed_ref_txn = db.begin_write_txn()
    with pytest.raises(ValueError, match="node ref must be exactly one"):
        malformed_ref_txn.get_node({"id": 1, "local": "mixed"})
    malformed_ref_txn.rollback()

    missing_field_txn = db.begin_write_txn()
    with pytest.raises(ValueError, match="upsert_node requires key"):
        missing_field_txn.stage([{"op": "upsert_node", "type_id": 1}])
    missing_field_txn.rollback()

    duplicate_txn = db.begin_write_txn()
    with pytest.raises(OverGraphError, match="duplicate transaction node alias"):
        duplicate_txn.stage(
            [
                {"op": "upsert_node", "alias": "n", "type_id": 1, "key": "n1"},
                {"op": "upsert_node", "alias": "n", "type_id": 1, "key": "n2"},
            ]
        )
    duplicate_txn.rollback()


def test_conflict_with_implicit_write_closes_transaction(db):
    db.upsert_node(1, "base", props={"v": 1})

    txn = db.begin_write_txn()
    txn.upsert_node(1, "base", props={"v": 2})
    db.upsert_node(1, "base", props={"v": 3})

    with pytest.raises(OverGraphError, match="transaction conflict"):
        txn.commit()
    with pytest.raises(OverGraphError, match="transaction is closed"):
        txn.rollback()
    assert db.get_node_by_key(1, "base").props["v"] == 3


def test_reopen_and_closed_state_behavior(db_path):
    db = OverGraph.open(db_path)
    txn = db.begin_write_txn()
    txn.stage([{"op": "upsert_node", "alias": "n", "type_id": 1, "key": "n"}])
    committed = txn.commit()
    node_id = committed.node_aliases["n"]
    db.close()

    reopened = OverGraph.open(db_path)
    assert reopened.get_node(node_id).key == "n"

    commit_txn = reopened.begin_write_txn()
    commit_txn.upsert_node_as("m", 1, "m")
    rollback_txn = reopened.begin_write_txn()
    rollback_txn.upsert_node_as("r", 1, "r")
    reopened.close()

    with pytest.raises(OverGraphError, match="[Dd]atabase is closed"):
        reopened.begin_write_txn()
    with pytest.raises(OverGraphError, match="[Dd]atabase is closed"):
        commit_txn.commit()
    rollback_txn.rollback()

    assert os.path.exists(db_path)
