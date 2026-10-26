"""Installed ordinary/candidate contracts and deterministic native boundaries."""
import asyncio
import gc
import os
import time
from collections import UserDict
from pathlib import Path

import pytest
from overgraph import OverGraph, OverGraphError, AsyncOverGraph

# Ordinary packages select production; use 0 only for an explicit legacy build.
CANDIDATE = os.environ.get('OVERGRAPH_TEST_CANDIDATE', '1') != '0'
HOOKS = os.environ.get('OVERGRAPH_TEST_HOOKS') == '1'

def drain(db):
    until = time.monotonic() + 15
    while db.reclamation_stats().close_state != 'drained':
        assert time.monotonic() < until, 'native drain'
        time.sleep(.001)

def reopen_and_drain(path):
    until=time.monotonic()+15
    while True:
        try:
            reopened=OverGraph.open(str(path))
            break
        except OverGraphError:
            assert time.monotonic()<until
            time.sleep(.001)
    cleanup(reopened)


def cleanup(db):
    if db is not None:
        try:
            db.close(force=True)
        except OverGraphError:
            pass
        if CANDIDATE:
            drain(db)


def test_concurrency_validation_precedes_files(tmp_path):
    path = tmp_path / 'db'
    for options in [{'unknown':1}, {'max_batch_operations':True}, {'max_batch_bytes':-1}, {'generation_wal_bytes':1.5}, {'admission_timeout_ms':float('inf')}, {'max_batch_operations':0}, {'generation_operation_limit':2**100}, {'max_batch_operations':2,'generation_operation_limit':1}, {'max_batch_bytes':float('nan')}]:
        with pytest.raises((ValueError, TypeError, OverflowError, OverGraphError)):
            OverGraph.open(str(path), concurrency_options=options)
        assert not path.exists()
    if not CANDIDATE:
        with pytest.raises(ValueError, match='production-concurrency'):
            OverGraph.open(str(path), concurrency_options={})
        assert not path.exists()


@pytest.mark.skipif(not CANDIDATE, reason='candidate package')
@pytest.mark.asyncio
async def test_options_structured_errors_and_root_free_stats(tmp_path):
    db = None
    try:
        db = OverGraph.open(str(tmp_path/'db'), concurrency_options=UserDict(max_batch_operations=2,max_batch_bytes=1024*1024,generation_operation_limit=100,generation_wal_bytes=1024*1024,admission_timeout_ms=1000))
        partial=OverGraph.open(str(tmp_path/'partial'),concurrency_options={'admission_timeout_ms':100})
        cleanup(partial)
        inputs=[{'labels':['Item'],'key':str(i)} for i in range(3)]
        for asynchronous in [False,True]:
            with pytest.raises(OverGraphError) as caught:
                if asynchronous:
                    await AsyncOverGraph(db).batch_upsert_nodes(inputs)
                else:
                    db.batch_upsert_nodes(inputs)
            error=caught.value
            assert (error.code,error.resource,error.requested,error.limit,error.retryable)==('resource_limit','max_batch_operations',3,2,False)
        assert db.get_node_by_key('Item','0') is None
        for asynchronous in [False,True]:
            txn=db.begin_write_txn()
            for i in range(3):txn.upsert_node('Item',f'txn-{asynchronous}-{i}')
            with pytest.raises(OverGraphError) as caught:
                if asynchronous:await asyncio.to_thread(txn.commit)
                else:txn.commit()
            assert (caught.value.code,caught.value.requested,caught.value.limit)==('resource_limit',3,2)

        stats=db.reclamation_stats()
        expected=['registered_objects','retired_objects','dropping_objects','active_generations','frozen_generations','leased_participants','participant_limit','process_participants','active_sessions','draining_sessions','close_state','oldest_retirement_age_us','scan_lag_us','release_to_eligibility_us','eligibility_to_drop_us','worker_wall_us','worker_cpu_us']
        assert all(hasattr(stats,name) for name in expected)
        assert stats.close_state=='open' and stats.registered_objects>0
        assert stats.worker_cpu_us is None
        for h in [stats.release_to_eligibility_us,stats.eligibility_to_drop_us]:
            assert len(h.buckets)==64 and all(isinstance(n,int) for n in h.buckets)
            assert h.max_us is None or isinstance(h.max_us,int)
        nid=db.upsert_node('Item','owned',props={'value':7})
        value=db.get_node(nid)
        await AsyncOverGraph(db).close()
        drain(db)
        assert value.props['value']==7
        db.close()
    finally:
        cleanup(db)


@pytest.mark.skipif(not HOOKS, reason='separate instrumented wheel')
@pytest.mark.asyncio
@pytest.mark.parametrize('point',['write','accepted'])
async def test_repeated_transaction_cancellation_keeps_native_order(tmp_path,point):
    db=OverGraph.open(str(tmp_path/'db'))
    hold=tmp_path/point
    committing=None
    try:
        txn=await AsyncOverGraph(db).begin_write_txn()
        await txn.upsert_node('Item','cancelled',props={'value':7})
        db.binding_test_hold(point,str(hold))
        committing=asyncio.create_task(txn.commit())
        until=time.monotonic()+15
        while not hold.with_suffix('.ready').exists():
            assert time.monotonic()<until
            await asyncio.sleep(.001)
        committing.cancel()
        await asyncio.sleep(0)
        committing.cancel()
        following=asyncio.create_task(txn.rollback())
        for _ in range(4):await asyncio.sleep(0)
        assert not committing.done() and not following.done()
        hold.with_suffix('.release').write_text('go')
        with pytest.raises(asyncio.CancelledError):await committing
        with pytest.raises(OverGraphError,match='transaction is closed'):await following
        assert db.get_node_by_key('Item','cancelled').props['value']==7
    finally:
        hold.with_suffix('.release').write_text('go')
        if committing is not None and not committing.done():
            try:await committing
            except BaseException:pass
        cleanup(db)


@pytest.mark.skipif(not HOOKS, reason='separate instrumented wheel')
@pytest.mark.asyncio
async def test_native_handoff_releases_service_before_python_delivery(tmp_path):
    db=OverGraph.open(str(tmp_path/'db'))
    hold=tmp_path/'handoff'
    result=None
    try:
        nid=db.upsert_node('Item','snapshot',props={'value':'original'})
        db.binding_test_hold('handoff',str(hold))
        result=asyncio.create_task(AsyncOverGraph(db).get_node(nid))
        until=time.monotonic()+15
        while not hold.with_suffix('.ready').exists():
            assert time.monotonic()<until
            await asyncio.sleep(.001)
        db.upsert_node('Item','snapshot',props={'value':'newer'})
        db.close()
        drain(db)
        hold.with_suffix('.release').write_text('go')
        assert (await result).props['value']=='original'
    finally:
        hold.with_suffix('.release').write_text('go')
        if result is not None:await result
        cleanup(db)


@pytest.mark.skipif(not HOOKS, reason='separate instrumented wheel')
@pytest.mark.asyncio
async def test_real_accepted_outcomes_are_structured(tmp_path):
    for kind,code in [('committed','commit_committed'),('indeterminate','commit_indeterminate')]:
        for asynchronous in [False,True]:
            path=tmp_path/f'{kind}-{asynchronous}'
            db=OverGraph.open(str(path))
            try:
                db.binding_test_fault(kind)
                with pytest.raises(OverGraphError) as caught:
                    if asynchronous:await AsyncOverGraph(db).upsert_node('Item','accepted')
                    else:db.upsert_node('Item','accepted')
                assert caught.value.code==code
            finally:cleanup(db)
            reopened=OverGraph.open(str(path))
            try:assert reopened.get_node_by_key('Item','accepted') is not None
            finally:cleanup(reopened)


@pytest.mark.skipif(not HOOKS, reason='separate instrumented wheel')
def test_native_close_and_abandonment_progress_with_gil_held(tmp_path):
    import subprocess
    import sys
    import threading
    from overgraph.overgraph import binding_test_hold_gil
    for abandoned in [False, True]:
        root=tmp_path/str(abandoned)
        root.mkdir()
        db=OverGraph.open(str(root/'db'))
        db.upsert_node('Item','value')
        shutdown=root/'shutdown'
        completed=root/'completed'
        gil=root/'gil'
        db.binding_test_hold('shutdown',str(shutdown))
        db.binding_test_hold('completed',str(completed))
        code="""import pathlib,sys,time
r=pathlib.Path(sys.argv[1]);end=time.monotonic()+20
def wait(name):
 while not (r/name).exists():
  assert time.monotonic()<end,name
  time.sleep(.001)
wait('gil.ready')
(r/'shutdown.release').write_text('GIL is held')
wait('completed.ready')
(r/'completed.release').write_text('native completion observed with GIL held')
(r/'gil.release').write_text('allow Python delivery')
"""
        child=subprocess.Popen([sys.executable,'-c',code,str(root)])
        errors=[]
        def close():
            try: db.close()
            except BaseException as error:errors.append(error)
        try:
            if abandoned:
                del db
                gc.collect()  # returns while the native shutdown boundary is held
            else:
                thread=threading.Thread(target=close)
                thread.start()
            until=time.monotonic()+15
            while not shutdown.with_suffix('.ready').exists():
                assert time.monotonic()<until
                time.sleep(.001)
            binding_test_hold_gil(str(gil))
            assert completed.with_suffix('.ready').exists()
            assert (root/'gil.release').exists()
        finally:
            for name in ['shutdown','completed','gil']:
                (root/(name+'.release')).write_text('cleanup')
            assert child.wait(timeout=25)==0
            if not abandoned:
                thread.join(timeout=10)
                assert not thread.is_alive() and not errors
                cleanup(db)
            else:
                reopen_and_drain(root/"db")


@pytest.mark.skipif(not CANDIDATE, reason='candidate package')
@pytest.mark.asyncio
async def test_cancel_before_transaction_lock_submits_no_mutation(tmp_path):
    db=OverGraph.open(str(tmp_path/'db'))
    try:
        txn=await AsyncOverGraph(db).begin_write_txn()
        await txn._lock.acquire()
        waiting=asyncio.create_task(txn.upsert_node('Item','must-not-submit'))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):await waiting
        txn._lock.release()
        await txn.commit()
        assert db.get_node_by_key('Item','must-not-submit') is None
    finally:cleanup(db)


@pytest.mark.skipif(not HOOKS, reason='separate instrumented wheel')
@pytest.mark.asyncio
async def test_exact_mutation_return_survives_intervening_commit(tmp_path):
    db=OverGraph.open(str(tmp_path/'db'))
    hold=tmp_path/'return'
    task=None
    try:
        db.binding_test_hold('handoff',str(hold))
        task=asyncio.create_task(AsyncOverGraph(db).execute_gql("CREATE (n:Item {elementKey: 'return', value: 'original'}) RETURN n.value AS value"))
        until=time.monotonic()+15
        while not hold.with_suffix('.ready').exists():
            assert time.monotonic()<until
            await asyncio.sleep(.001)
        db.upsert_node('Item','return',props={'value':'later'})
        db.close()
        drain(db)
        hold.with_suffix('.release').write_text('go')
        assert (await task)['rows']==[{'value':'original'}]
    finally:
        hold.with_suffix('.release').write_text('go')
        if task is not None:await task
        cleanup(db)


@pytest.mark.skipif(not HOOKS, reason='separate instrumented wheel')
def test_open_transaction_and_database_drop_orders(tmp_path):
    for txn_first in [False,True]:
        root=tmp_path/str(txn_first)
        db=OverGraph.open(str(root/'db'))
        txn=db.begin_write_txn()
        txn.upsert_node('Item','staged-only')
        completed=root/'completed'
        db.binding_test_hold('completed',str(completed))
        if txn_first:
            txn.rollback()
            del txn
            del db
        else:
            del db
            gc.collect()
            assert not completed.with_suffix('.ready').exists()
            txn.rollback()
            del txn
        gc.collect()
        until=time.monotonic()+15
        try:
            while not completed.with_suffix('.ready').exists():
                assert time.monotonic()<until
                time.sleep(.001)
        finally:
            completed.with_suffix('.release').write_text('cleanup')
            reopen_and_drain(root/'db')
