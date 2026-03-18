"""WAL replay and persistence round-trip tests.

Pattern: open → write → close → reopen → verify → close
"""

import os
import shutil
import tempfile

import pytest
from overgraph import OverGraph


@pytest.fixture
def db_dir():
    """Provide a fresh temporary directory, cleaned up after test."""
    d = tempfile.mkdtemp(prefix="egtest_persist_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def open_db(db_dir, name="testdb"):
    return OverGraph.open(os.path.join(db_dir, name))


class TestWalReplay:
    def test_nodes_survive_reopen(self, db_dir):
        db = open_db(db_dir)
        n1 = db.upsert_node(1, "alice", props={"age": 30})
        n2 = db.upsert_node(2, "bob")
        db.close()

        db2 = open_db(db_dir)
        node = db2.get_node(n1)
        assert node is not None
        assert node.key == "alice"
        assert node.props["age"] == 30
        assert db2.get_node(n2) is not None
        db2.close()

    def test_edges_survive_reopen(self, db_dir):
        db = open_db(db_dir)
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, props={"rel": "friend"}, weight=2.5)
        db.close()

        db2 = open_db(db_dir)
        edge = db2.get_edge(eid)
        assert edge is not None
        assert edge.from_id == n1
        assert edge.to_id == n2
        assert edge.type_id == 10
        assert edge.props["rel"] == "friend"
        assert abs(edge.weight - 2.5) < 0.01
        db2.close()

    def test_temporal_edges_survive_reopen(self, db_dir):
        db = open_db(db_dir)
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, valid_from=100, valid_to=200)
        db.close()

        db2 = open_db(db_dir)
        edge = db2.get_edge(eid)
        assert edge.valid_from == 100
        assert edge.valid_to == 200
        db2.close()

    def test_deletes_persist(self, db_dir):
        db = open_db(db_dir)
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.delete_node(n1)
        db.close()

        db2 = open_db(db_dir)
        assert db2.get_node(n1) is None
        assert db2.get_node(n2) is not None
        db2.close()

    def test_neighbors_work_after_reopen(self, db_dir):
        db = open_db(db_dir)
        n1 = db.upsert_node(1, "center")
        spokes = []
        for i in range(3):
            s = db.upsert_node(1, f"spoke_{i}")
            db.upsert_edge(n1, s, 10)
            spokes.append(s)
        db.close()

        db2 = open_db(db_dir)
        nbrs = db2.neighbors(n1, direction="outgoing")
        assert len(nbrs) == 3
        node_ids = {n.node_id for n in nbrs}
        assert node_ids == set(spokes)
        db2.close()

    def test_find_nodes_works_after_reopen(self, db_dir):
        db = open_db(db_dir)
        db.upsert_node(1, "x", props={"color": "red"})
        db.upsert_node(1, "y", props={"color": "blue"})
        db.close()

        db2 = open_db(db_dir)
        ids = db2.find_nodes(1, "color", "red")
        assert len(ids) == 1
        db2.close()

    def test_key_lookup_after_reopen(self, db_dir):
        db = open_db(db_dir)
        nid = db.upsert_node(1, "mykey")
        db.close()

        db2 = open_db(db_dir)
        node = db2.get_node_by_key(1, "mykey")
        assert node is not None
        assert node.id == nid
        db2.close()


class TestFlushReopen:
    def test_flushed_data_survives(self, db_dir):
        db = open_db(db_dir)
        n1 = db.upsert_node(1, "a", props={"v": 1})
        db.flush()
        db.close()

        db2 = open_db(db_dir)
        node = db2.get_node(n1)
        assert node is not None
        assert node.props["v"] == 1
        db2.close()

    def test_compact_reopen(self, db_dir):
        db = open_db(db_dir)
        # Create two segments
        for i in range(10):
            db.upsert_node(1, f"seg1_{i}")
        db.flush()
        for i in range(10):
            db.upsert_node(1, f"seg2_{i}")
        db.flush()
        db.compact()
        db.close()

        db2 = open_db(db_dir)
        # All 20 nodes should be present
        count = db2.count_nodes_by_type(1)
        assert count == 20
        db2.close()

    def test_multi_cycle_flush_compact_reopen(self, db_dir):
        db = open_db(db_dir)
        # Cycle 1
        for i in range(5):
            db.upsert_node(1, f"c1_{i}")
        db.flush()
        for i in range(5):
            db.upsert_node(1, f"c2_{i}")
        db.flush()
        db.compact()

        # Cycle 2: add more data after compaction
        for i in range(5):
            db.upsert_node(1, f"c3_{i}")
        db.flush()
        db.close()

        db2 = open_db(db_dir)
        count = db2.count_nodes_by_type(1)
        assert count == 15
        db2.close()


class TestMixedPersistence:
    def test_wal_and_segment_merged_on_reopen(self, db_dir):
        db = open_db(db_dir)
        # Segment data
        for i in range(5):
            db.upsert_node(1, f"seg_{i}")
        db.flush()
        # WAL-only data (not flushed)
        for i in range(3):
            db.upsert_node(1, f"wal_{i}")
        db.close()

        db2 = open_db(db_dir)
        count = db2.count_nodes_by_type(1)
        assert count == 8
        db2.close()

    def test_batch_upsert_survives(self, db_dir):
        db = open_db(db_dir)
        ids = db.batch_upsert_nodes([
            {"type_id": 1, "key": "a"},
            {"type_id": 1, "key": "b"},
            {"type_id": 1, "key": "c"},
        ])
        db.close()

        db2 = open_db(db_dir)
        for nid in ids:
            assert db2.get_node(nid) is not None
        db2.close()

    def test_graph_patch_survives(self, db_dir):
        db = open_db(db_dir)
        result = db.graph_patch({
            "upsert_nodes": [
                {"type_id": 1, "key": "p1"},
                {"type_id": 1, "key": "p2"},
            ],
        })
        n1, n2 = result.node_ids
        result2 = db.graph_patch({
            "upsert_edges": [
                {"from_id": n1, "to_id": n2, "type_id": 10},
            ],
        })
        eid = result2.edge_ids[0]
        db.close()

        db2 = open_db(db_dir)
        assert db2.get_node(n1) is not None
        assert db2.get_node(n2) is not None
        assert db2.get_edge(eid) is not None
        db2.close()
