import os
import shutil
import tempfile

import pytest
import pytest_asyncio
from overgraph import OverGraph, AsyncOverGraph


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
    drain_after_close(database)


@pytest_asyncio.fixture
async def async_db(tmp_dir):
    """Open an AsyncOverGraph, yield it, then close."""
    path = os.path.join(tmp_dir, "async_testdb")
    database = await AsyncOverGraph.open(path)
    yield database
    await database.close()
    drain_after_close(database)


def make_chain(db, n=5, label="RELATES_TO"):
    """Create a chain: n0 -> n1 -> n2 -> ... -> n(n-1)"""
    nodes = []
    for i in range(n):
        nid = db.upsert_node("Person", f"node_{i}")
        nodes.append(nid)
    edges = []
    for i in range(len(nodes) - 1):
        eid = db.upsert_edge(nodes[i], nodes[i + 1], label)
        edges.append(eid)
    return nodes, edges


def make_star(db, center_key="center", spokes=5, label="RELATES_TO"):
    """Create a star: center -> spoke_0, center -> spoke_1, ..."""
    center = db.upsert_node("Person", center_key)
    spoke_ids = []
    for i in range(spokes):
        sid = db.upsert_node("Person", f"spoke_{i}")
        db.upsert_edge(center, sid, label)
        spoke_ids.append(sid)
    return center, spoke_ids


def drain_after_close(db):
    """Wait for actual candidate domain exit before reopen or fixture deletion."""
    import time
    if isinstance(db,AsyncOverGraph):
        db=db._db
    method=getattr(db,"reclamation_stats",None)
    if method is None:
        return
    try:
        stats=method()
    except ValueError as error:
        if str(error)=="reclamation_stats requires production-concurrency":
            return
        raise
    until=time.monotonic()+15
    while stats.close_state!="drained":
        assert time.monotonic()<until,"native fixture drain timed out"
        time.sleep(.001)
        stats=method()
