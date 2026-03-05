import os
import shutil
import tempfile

import pytest
from overgraph import OverGraph


@pytest.fixture
def tmp_dir():
    """Provide a fresh temporary directory, cleaned up after test."""
    d = tempfile.mkdtemp(prefix="egtest_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def db_path(tmp_dir):
    """Return a path for a new database inside the temp dir."""
    return os.path.join(tmp_dir, "testdb")


@pytest.fixture
def db(db_path):
    """Open a database, yield it, then close."""
    database = OverGraph.open(db_path)
    yield database
    database.close()


def make_chain(db, n=5, edge_type=10):
    """Create a chain: n0 -> n1 -> n2 -> ... -> n(n-1)"""
    nodes = []
    for i in range(n):
        nid = db.upsert_node(1, f"node_{i}")
        nodes.append(nid)
    edges = []
    for i in range(len(nodes) - 1):
        eid = db.upsert_edge(nodes[i], nodes[i + 1], edge_type)
        edges.append(eid)
    return nodes, edges


def make_star(db, center_key="center", spokes=5, edge_type=10):
    """Create a star: center -> spoke_0, center -> spoke_1, ..."""
    center = db.upsert_node(1, center_key)
    spoke_ids = []
    for i in range(spokes):
        sid = db.upsert_node(1, f"spoke_{i}")
        db.upsert_edge(center, sid, edge_type)
        spoke_ids.append(sid)
    return center, spoke_ids
