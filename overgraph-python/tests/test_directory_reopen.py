"""Immediate reopen needs no diagnostic drain or retry in user code."""
import pytest
from overgraph import OverGraph, AsyncOverGraph
from conftest import drain_after_close


@pytest.mark.parametrize('force', [False, True])
def test_immediate_reopen_after_close(tmp_path, force):
    path = str(tmp_path / 'db')
    db = None
    try:
        for i in range(12):
            db = OverGraph.open(path)
            db.upsert_node('N', str(i), props={'value': i})
            db.close(force=force)
            db = OverGraph.open(path, create_if_missing=False)
            assert db.get_node_by_key('N', str(i)).props['value'] == i
            db.close()
    finally:
        if db is not None:
            db.close(force=True)
            drain_after_close(db)


@pytest.mark.asyncio
@pytest.mark.parametrize('force', [False, True])
async def test_immediate_reopen_after_async_close(tmp_path, force):
    path = str(tmp_path / 'db')
    db = None
    try:
        for i in range(12):
            db = await AsyncOverGraph.open(path)
            await db.upsert_node('N', str(i), props={'value': i})
            await db.close(force=force)
            db = await AsyncOverGraph.open(path, create_if_missing=False)
            assert (await db.get_node_by_key('N', str(i))).props['value'] == i
            await db.close()
    finally:
        if db is not None:
            await db.close(force=True)
            drain_after_close(db)
