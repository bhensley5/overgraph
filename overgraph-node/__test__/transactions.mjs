import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb() {
  const tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-txn-'));
  const db = OverGraph.open(join(tmpDir, 'db'));
  return { tmpDir, db };
}

describe('write transactions', () => {
  let current = null;

  afterEach(() => {
    if (current) {
      current.db.close();
      rmSync(current.tmpDir, { recursive: true, force: true });
      current = null;
    }
  });

  it('stages ordered alias payloads and commits atomically', () => {
    current = freshDb();
    const { db } = current;
    const txn = db.beginWriteTxn();

    txn.stage([
      { op: 'upsertNode', alias: 'alice', typeId: 1, key: 'alice', props: { name: 'Alice' } },
      { op: 'upsertNode', alias: 'bob', typeId: 1, key: 'bob' },
      {
        op: 'upsertEdge',
        alias: 'knows',
        from: { local: 'alice' },
        to: { local: 'bob' },
        typeId: 7,
        props: { since: 2026 },
      },
    ]);

    const stagedAlice = txn.getNode({ local: 'alice' });
    assert.equal(stagedAlice.id, undefined);
    assert.equal(stagedAlice.local, 'alice');
    assert.equal(stagedAlice.props.name, 'Alice');

    const stagedEdge = txn.getEdge({ local: 'knows' });
    assert.equal(stagedEdge.id, undefined);
    assert.equal(stagedEdge.from.local, 'alice');
    assert.equal(stagedEdge.to.local, 'bob');

    const result = txn.commit();
    assert.ok(result.nodeIds instanceof Float64Array);
    assert.equal(result.nodeIds.length, 2);
    assert.equal(result.edgeIds.length, 1);
    assert.equal(result.nodeAliases.alice, result.nodeIds[0]);
    assert.equal(result.nodeAliases.bob, result.nodeIds[1]);
    assert.equal(result.edgeAliases.knows, result.edgeIds[0]);

    const edge = db.getEdge(result.edgeAliases.knows);
    assert.equal(edge.from, result.nodeAliases.alice);
    assert.equal(edge.to, result.nodeAliases.bob);
  });

  it('builder aliases support read-own-writes and rollback no-trace', () => {
    current = freshDb();
    const { db } = current;
    const txn = db.beginWriteTxn();

    const alice = txn.upsertNodeAs('alice', 1, 'alice', { props: { mood: 'staged' } });
    const bob = txn.upsertNodeAs('bob', 1, 'bob');
    txn.upsertEdgeAs('knows', alice, bob, 9);

    assert.equal(txn.getNodeByKey(1, 'alice').props.mood, 'staged');
    txn.rollback();
    assert.equal(db.getNodeByKey(1, 'alice'), null);
    assert.throws(() => txn.commit(), /transaction is closed/);
  });

  it('unaliased builder refs can create and connect in one transaction', () => {
    current = freshDb();
    const { db } = current;
    const txn = db.beginWriteTxn();

    const alice = txn.upsertNode(1, 'alice');
    const bob = txn.upsertNode(1, 'bob');
    const edgeRef = txn.upsertEdge(alice, bob, 7);

    assert.deepEqual(alice, { typeId: 1, key: 'alice' });
    assert.deepEqual(bob, { typeId: 1, key: 'bob' });
    assert.equal(txn.getEdge(edgeRef).typeId, 7);

    const result = txn.commit();
    const edge = db.getEdge(result.edgeIds[0]);
    assert.equal(edge.from, result.nodeIds[0]);
    assert.equal(edge.to, result.nodeIds[1]);
  });

  it('stages delete and invalidate operations with read-own-writes', () => {
    current = freshDb();
    const { db } = current;
    const [a, b, c, d] = Array.from(db.batchUpsertNodes([
      { typeId: 1, key: 'a' },
      { typeId: 1, key: 'b' },
      { typeId: 1, key: 'c' },
      { typeId: 1, key: 'd' },
    ]));
    const [activeEdge, deletedEdge, cascadedEdge] = Array.from(db.batchUpsertEdges([
      { from: a, to: b, typeId: 7 },
      { from: b, to: c, typeId: 8 },
      { from: c, to: d, typeId: 9 },
    ]));

    const txn = db.beginWriteTxn();
    txn.stage([
      { op: 'invalidateEdge', target: { id: activeEdge }, validTo: 12345 },
      { op: 'deleteEdge', target: { id: deletedEdge } },
      { op: 'deleteNode', target: { id: d } },
    ]);

    assert.equal(txn.getEdge({ id: activeEdge }).validTo, 12345);
    assert.equal(txn.getEdge({ id: deletedEdge }), null);
    assert.equal(txn.getNode({ id: d }), null);
    assert.equal(txn.getEdge({ id: cascadedEdge }), null);

    txn.commit();
    assert.equal(db.getEdge(activeEdge).validTo, 12345);
    assert.equal(db.getEdge(deletedEdge), null);
    assert.equal(db.getNode(d), null);
    assert.equal(db.getEdge(cascadedEdge), null);
  });

  it('rejects malformed refs, missing op fields, and duplicate aliases', () => {
    current = freshDb();
    const { db } = current;

    const malformedRefTxn = db.beginWriteTxn();
    assert.throws(
      () => malformedRefTxn.getNode({ id: 1, local: 'mixed' }),
      /node ref must be exactly one/,
    );
    malformedRefTxn.rollback();

    const missingFieldTxn = db.beginWriteTxn();
    assert.throws(
      () => missingFieldTxn.stage([{ op: 'upsertNode', typeId: 1 }]),
      /upsertNode requires key/,
    );
    missingFieldTxn.rollback();

    const duplicateTxn = db.beginWriteTxn();
    assert.throws(
      () => duplicateTxn.stage([
        { op: 'upsertNode', alias: 'n', typeId: 1, key: 'n1' },
        { op: 'upsertNode', alias: 'n', typeId: 1, key: 'n2' },
      ]),
      /duplicate transaction node alias/,
    );
    duplicateTxn.rollback();
  });

  it('conflicts with implicit writes and closes after failed commit', () => {
    current = freshDb();
    const { db } = current;
    db.upsertNode(1, 'base', { props: { v: 1 } });

    const txn = db.beginWriteTxn();
    txn.upsertNode(1, 'base', { props: { v: 2 } });
    db.upsertNode(1, 'base', { props: { v: 3 } });

    assert.throws(() => txn.commit(), /transaction conflict/);
    assert.throws(() => txn.rollback(), /transaction is closed/);
    assert.equal(db.getNodeByKey(1, 'base').props.v, 3);
  });

  it('begin and commit respect database and transaction close states', () => {
    current = freshDb();
    const { db } = current;
    const commitTxn = db.beginWriteTxn();
    commitTxn.upsertNodeAs('n', 1, 'n');
    const rollbackTxn = db.beginWriteTxn();
    rollbackTxn.upsertNodeAs('m', 1, 'm');
    db.close();

    assert.throws(() => db.beginWriteTxn(), /Database is closed/);
    assert.throws(() => commitTxn.commit(), /Database is closed|database is closed/);
    rollbackTxn.rollback();
  });
});
