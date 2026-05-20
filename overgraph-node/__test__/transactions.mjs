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
      { op: 'upsertNode', alias: 'alice', labels: ['Person'], key: 'alice', props: { name: 'Alice' } },
      { op: 'upsertNode', alias: 'bob', labels: ['Person'], key: 'bob' },
      {
        op: 'upsertEdge',
        alias: 'knows',
        from: { local: 'alice' },
        to: { local: 'bob' },
        label: 'KNOWS',
        props: { since: 2026 },
      },
    ]);

    const stagedAlice = txn.getNode({ local: 'alice' });
    assert.equal(stagedAlice.id, undefined);
    assert.equal(stagedAlice.local, 'alice');
    assert.deepEqual(stagedAlice.labels, ['Person']);
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

    const alice = txn.upsertNodeAs('alice', 'Person', 'alice', { props: { mood: 'staged' } });
    const bob = txn.upsertNodeAs('bob', 'Person', 'bob');
    txn.upsertEdgeAs('knows', alice, bob, 'FOLLOWS');

    assert.equal(txn.getNodeByKey('Person', 'alice').props.mood, 'staged');
    txn.rollback();
    assert.equal(db.getNodeByKey('Person', 'alice'), null);
    assert.throws(() => txn.commit(), /transaction is closed/);
  });

  it('stages node label mutations inside a transaction', () => {
    current = freshDb();
    const { db } = current;
    const id = db.upsertNode('Person', 'labeled');
    const txn = db.beginWriteTxn();

    assert.equal(txn.addNodeLabel({ id }, 'Admin'), true);
    assert.equal(txn.addNodeLabel({ id }, 'Admin'), false);
    assert.deepEqual([...txn.getNode({ id }).labels].sort(), ['Admin', 'Person']);
    assert.equal(txn.removeNodeLabel({ id }, 'Admin'), true);
    assert.equal(txn.removeNodeLabel({ id }, 'Admin'), false);
    assert.deepEqual(txn.getNode({ id }).labels, ['Person']);
    assert.throws(
      () => txn.removeNodeLabel({ id }, 'Person'),
      /cannot remove the last node label/,
    );

    txn.commit();
    assert.deepEqual(db.getNode(id).labels, ['Person']);
  });

  it('async transaction node label mutations preserve order', async () => {
    current = freshDb();
    const { db } = current;
    const id = db.upsertNode('Person', 'async-labeled');
    const txn = db.beginWriteTxn();

    assert.equal(await txn.addNodeLabelAsync({ id }, 'Admin'), true);
    assert.deepEqual([...(await txn.getNodeAsync({ id })).labels].sort(), ['Admin', 'Person']);
    assert.equal(await txn.removeNodeLabelAsync({ id }, 'Admin'), true);
    await txn.commitAsync();
    assert.deepEqual(db.getNode(id).labels, ['Person']);
  });

  it('unaliased builder refs can create and connect in one transaction', () => {
    current = freshDb();
    const { db } = current;
    const txn = db.beginWriteTxn();

    const alice = txn.upsertNode('Person', 'alice');
    const bob = txn.upsertNode('Person', 'bob');
    const edgeRef = txn.upsertEdge(alice, bob, 'KNOWS');

    assert.deepEqual(alice, { labels: ['Person'], key: 'alice' });
    assert.deepEqual(bob, { labels: ['Person'], key: 'bob' });
    assert.equal(txn.getEdge(edgeRef).label, 'KNOWS');

    const result = txn.commit();
    const edge = db.getEdge(result.edgeIds[0]);
    assert.equal(edge.from, result.nodeIds[0]);
    assert.equal(edge.to, result.nodeIds[1]);
  });

  it('stages delete and invalidate operations with read-own-writes', () => {
    current = freshDb();
    const { db } = current;
    const [a, b, c, d] = Array.from(db.batchUpsertNodes([
      { labels: ['Person'], key: 'a' },
      { labels: ['Person'], key: 'b' },
      { labels: ['Person'], key: 'c' },
      { labels: ['Person'], key: 'd' },
    ]));
    const [activeEdge, deletedEdge, cascadedEdge] = Array.from(db.batchUpsertEdges([
      { from: a, to: b, label: 'KNOWS'},
      { from: b, to: c, label: 'PARENT_OF'},
      { from: c, to: d, label: 'FOLLOWS'},
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
      () => missingFieldTxn.stage([{ op: 'upsertNode', labels: ['Person'] }]),
      /upsertNode requires key/,
    );
    missingFieldTxn.rollback();

    const duplicateTxn = db.beginWriteTxn();
    assert.throws(
      () => duplicateTxn.stage([
        { op: 'upsertNode', alias: 'n', labels: ['Person'], key: 'n1' },
        { op: 'upsertNode', alias: 'n', labels: ['Person'], key: 'n2' },
      ]),
      /duplicate transaction node alias/,
    );
    duplicateTxn.rollback();
  });

  it('conflicts with implicit writes and closes after failed commit', () => {
    current = freshDb();
    const { db } = current;
    db.upsertNode('Person', 'base', { props: { v: 1 } });

    const txn = db.beginWriteTxn();
    txn.upsertNode('Person', 'base', { props: { v: 2 } });
    db.upsertNode('Person', 'base', { props: { v: 3 } });

    assert.throws(() => txn.commit(), /transaction conflict/);
    assert.throws(() => txn.rollback(), /transaction is closed/);
    assert.equal(db.getNodeByKey('Person', 'base').props.v, 3);
  });

  it('begin and commit respect database and transaction close states', () => {
    current = freshDb();
    const { db } = current;
    const commitTxn = db.beginWriteTxn();
    commitTxn.upsertNodeAs('n', 'Person', 'n');
    const rollbackTxn = db.beginWriteTxn();
    rollbackTxn.upsertNodeAs('m', 'Person', 'm');
    db.close();

    assert.throws(() => db.beginWriteTxn(), /Database is closed/);
    assert.throws(() => commitTxn.commit(), /Database is closed|database is closed/);
    rollbackTxn.rollback();
  });
});
