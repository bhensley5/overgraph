import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

// Phase 10: Agent API tests

describe('getNodeByKey', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-key-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns node by type+key', () => {
    const id = db.upsertNode('Person', 'alice', { props: { name: 'Alice' }, weight: 1.0 });
    const node = db.getNodeByKey('Person', 'alice');
    assert.ok(node);
    assert.equal(node.id, id);
    assert.deepEqual(node.labels, ['Person']);
    assert.equal(node.key, 'alice');
    assert.equal(node.props.name, 'Alice');
  });

  it('returns null for missing key', () => {
    assert.equal(db.getNodeByKey('Person', 'nonexistent'), null);
  });

  it('returns null for wrong label', () => {
    db.upsertNode('User', 'typed', { props: {} });
    assert.equal(db.getNodeByKey('MissingLabel', 'typed'), null);
  });

  it('returns null for deleted node', () => {
    const id = db.upsertNode('Person', 'to-delete', { props: {} });
    db.deleteNode(id);
    assert.equal(db.getNodeByKey('Person', 'to-delete'), null);
  });

  it('works after flush (segment source)', () => {
    const id = db.upsertNode('Person', 'flushed', { props: { v: 1 } });
    db.flush();
    const node = db.getNodeByKey('Person', 'flushed');
    assert.ok(node);
    assert.equal(node.id, id);
  });
});

describe('getEdgeByTriple', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-triple-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns edge by from+to+type', () => {
    const a = db.upsertNode('Person', 'a', { props: {} });
    const b = db.upsertNode('Person', 'b', { props: {} });
    const eid = db.upsertEdge(a, b, 'WORKS_AT', { props: { rel: 'knows' }, weight: 0.5 });
    const edge = db.getEdgeByTriple(a, b, 'WORKS_AT');
    assert.ok(edge);
    assert.equal(edge.id, eid);
    assert.equal(edge.from, a);
    assert.equal(edge.to, b);
    assert.equal(edge.label, 'WORKS_AT');
    assert.equal(edge.props.rel, 'knows');
  });

  it('returns null for wrong edge label', () => {
    const a = db.upsertNode('Person', 'c', { props: {} });
    const b = db.upsertNode('Person', 'd', { props: {} });
    db.upsertEdge(a, b, 'WORKS_AT', { props: {} });
    assert.equal(db.getEdgeByTriple(a, b, 'MISSING_EDGE_TYPE'), null);
  });

  it('returns null for reversed direction', () => {
    const a = db.upsertNode('Person', 'e', { props: {} });
    const b = db.upsertNode('Person', 'f', { props: {} });
    db.upsertEdge(a, b, 'WORKS_AT', { props: {} });
    assert.equal(db.getEdgeByTriple(b, a, 'WORKS_AT'), null);
  });

  it('returns null after delete', () => {
    const a = db.upsertNode('Person', 'g', { props: {} });
    const b = db.upsertNode('Person', 'h', { props: {} });
    const eid = db.upsertEdge(a, b, 'WORKS_AT', { props: {} });
    db.deleteEdge(eid);
    assert.equal(db.getEdgeByTriple(a, b, 'WORKS_AT'), null);
  });

  it('works after flush', () => {
    const a = db.upsertNode('Person', 'i', { props: {} });
    const b = db.upsertNode('Person', 'j', { props: {} });
    const eid = db.upsertEdge(a, b, 'MENTIONS', { props: {} });
    db.flush();
    const edge = db.getEdgeByTriple(a, b, 'MENTIONS');
    assert.ok(edge);
    assert.equal(edge.id, eid);
  });
});

describe('extractSubgraph', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-subgraph-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('extracts a simple chain', () => {
    const a = db.upsertNode('Person', 'sg-a', { props: { name: 'A' } });
    const b = db.upsertNode('Person', 'sg-b', { props: { name: 'B' } });
    const c = db.upsertNode('Person', 'sg-c', { props: { name: 'C' } });
    db.upsertEdge(a, b, 'LINKS_TO');
    db.upsertEdge(b, c, 'LINKS_TO');

    const sg = db.extractSubgraph(a, 2);
    assert.ok(sg);
    assert.equal(sg.nodes.length, 3);
    assert.equal(sg.edges.length, 2);
  });

  it('respects maxDepth', () => {
    const sg = db.extractSubgraph(db.upsertNode('Person', 'sg-a', { props: {} }), 0);
    assert.equal(sg.nodes.length, 1);
    assert.equal(sg.edges.length, 0);
  });
});

describe('getNodes / getEdges (bulk)', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bulk-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('getNodes returns array matching input order', () => {
    const a = db.upsertNode('Person', 'bulk-a', { props: { n: 'A' } });
    const b = db.upsertNode('Person', 'bulk-b', { props: { n: 'B' } });
    const c = db.upsertNode('Person', 'bulk-c', { props: { n: 'C' } });
    const results = db.getNodes([a, b, c]);
    assert.equal(results.length, 3);
    assert.equal(results[0].key, 'bulk-a');
    assert.equal(results[1].key, 'bulk-b');
    assert.equal(results[2].key, 'bulk-c');
  });

  it('getNodes returns null for missing/deleted', () => {
    const a = db.upsertNode('Person', 'bulk-d', { props: {} });
    const b = db.upsertNode('Person', 'bulk-e', { props: {} });
    db.deleteNode(b);
    const results = db.getNodes([a, b, 99999]);
    assert.equal(results.length, 3);
    assert.ok(results[0]);
    assert.equal(results[1], null);
    assert.equal(results[2], null);
  });

  it('getNodes empty array returns empty', () => {
    const results = db.getNodes([]);
    assert.equal(results.length, 0);
  });

  it('getEdges returns array matching input order', () => {
    const a = db.upsertNode('Person', 'bulk-ea', { props: {} });
    const b = db.upsertNode('Person', 'bulk-eb', { props: {} });
    const c = db.upsertNode('Person', 'bulk-ec', { props: {} });
    const e1 = db.upsertEdge(a, b, 'LINKS_TO');
    const e2 = db.upsertEdge(b, c, 'LINKS_TO');
    const results = db.getEdges([e1, e2, 99999]);
    assert.equal(results.length, 3);
    assert.equal(results[0].from, a);
    assert.equal(results[1].from, b);
    assert.equal(results[2], null);
  });

  it('getNodes works cross-source (memtable + segment)', () => {
    const a = db.upsertNode('Person', 'bulk-xa', { props: {} });
    db.flush();
    const b = db.upsertNode('Person', 'bulk-xb', { props: {} });
    const results = db.getNodes([a, b]);
    assert.equal(results[0].key, 'bulk-xa');
    assert.equal(results[1].key, 'bulk-xb');
  });
});

describe('getNodesByKeys (bulk key lookup)', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bykeys-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns matching nodes in input order', () => {
    db.upsertNode('Person', 'ka', { props: { n: 'A' } });
    db.upsertNode('Person', 'kb', { props: { n: 'B' } });
    db.upsertNode('Company', 'kc', { props: { n: 'C' } });
    const results = db.getNodesByKeys([
      { label: 'Person', key: 'ka' },
      { label: 'Person', key: 'kb' },
      { label: 'Company', key: 'kc' },
    ]);
    assert.equal(results.length, 3);
    assert.equal(results[0].key, 'ka');
    assert.equal(results[1].key, 'kb');
    assert.equal(results[2].key, 'kc');
  });

  it('returns null for missing and deleted keys', () => {
    db.upsertNode('Person', 'kd');
    const bid = db.upsertNode('Person', 'ke');
    db.deleteNode(bid);
    const results = db.getNodesByKeys([
      { label: 'Person', key: 'kd' },
      { label: 'Person', key: 'ke' },
      { label: 'Person', key: 'nonexistent' },
    ]);
    assert.equal(results.length, 3);
    assert.ok(results[0]);
    assert.equal(results[1], null);
    assert.equal(results[2], null);
  });

  it('returns empty for empty input', () => {
    const results = db.getNodesByKeys([]);
    assert.equal(results.length, 0);
  });

  it('async variant works', async () => {
    db.upsertNode('Person', 'kf');
    const results = await db.getNodesByKeysAsync([{ label: 'Person', key: 'kf' }]);
    assert.equal(results.length, 1);
    assert.equal(results[0].key, 'kf');
  });
});

describe('async variants of new APIs', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-new-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('getNodeByKeyAsync works', async () => {
    const id = db.upsertNode('Person', 'async-key', { props: { x: 1 } });
    const node = await db.getNodeByKeyAsync('Person', 'async-key');
    assert.ok(node);
    assert.equal(node.id, id);
  });

  it('getEdgeByTripleAsync works', async () => {
    const a = db.upsertNode('Person', 'at-a', { props: {} });
    const b = db.upsertNode('Person', 'at-b', { props: {} });
    const eid = db.upsertEdge(a, b, 'DEPENDS_ON');
    const edge = await db.getEdgeByTripleAsync(a, b, 'DEPENDS_ON');
    assert.ok(edge);
    assert.equal(edge.id, eid);
  });

  it('getNodesAsync works', async () => {
    const a = db.upsertNode('Person', 'an-a', { props: {} });
    const b = db.upsertNode('Person', 'an-b', { props: {} });
    const results = await db.getNodesAsync([a, b]);
    assert.equal(results.length, 2);
    assert.equal(results[0].key, 'an-a');
  });

  it('getEdgesAsync works', async () => {
    const a = db.upsertNode('Person', 'ae-a', { props: {} });
    const b = db.upsertNode('Person', 'ae-b', { props: {} });
    const e = db.upsertEdge(a, b, 'LINKS_TO');
    const results = await db.getEdgesAsync([e]);
    assert.equal(results.length, 1);
    assert.equal(results[0].from, a);
  });

  it('extractSubgraphAsync works', async () => {
    const a = db.upsertNode('Person', 'esg-a', { props: {} });
    const b = db.upsertNode('Person', 'esg-b', { props: {} });
    db.upsertEdge(a, b, 'LINKS_TO');
    const sg = await db.extractSubgraphAsync(a, 1);
    assert.ok(sg);
    assert.ok(sg.nodes.length >= 2);
    assert.ok(sg.edges.length >= 1);
  });
});

// ========== P10-005: graphPatch ==========

describe('graphPatch', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-patch-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('mixed ops in a single call', () => {
    const a = db.upsertNode('Person', 'pa', { props: {} });
    const b = db.upsertNode('Person', 'pb', { props: {} });
    const e1 = db.upsertEdge(a, b, 'LINKS_TO', { props: { v: 'old' } });

    const result = db.graphPatch({
      upsertNodes: [{ labels: ['Person'], key: 'pc', props: { role: 'new' } }],
      upsertEdges: [{ from: a, to: b, label: 'REFERENCES'}],
      invalidateEdges: [{ edgeId: e1, validTo: 1000 }],
      deleteEdgeIds: [],
      deleteNodeIds: [],
    });

    assert.equal(result.nodeIds.length, 1);
    assert.equal(result.edgeIds.length, 1);

    // New node created
    const c = db.getNode(result.nodeIds[0]);
    assert.ok(c);
    assert.equal(c.key, 'pc');

    // New edge created
    const e2 = db.getEdge(result.edgeIds[0]);
    assert.ok(e2);
    assert.equal(e2.label, 'REFERENCES');

    // e1 invalidated
    const inv = db.getEdge(e1);
    assert.ok(inv);
    assert.equal(inv.validTo, 1000);
  });

  it('empty patch is valid', () => {
    const result = db.graphPatch({});
    assert.equal(result.nodeIds.length, 0);
    assert.equal(result.edgeIds.length, 0);
  });

  it('uses NodeInput labels shape in patch node upserts', () => {
    const result = db.graphPatch({
      upsertNodes: [
        { labels: 'Person', key: 'patch-string-label' },
        { labels: ['Person', 'Admin'], key: 'patch-multi-label' },
      ],
    });

    assert.deepEqual(db.getNode(result.nodeIds[0]).labels, ['Person']);
    assert.deepEqual([...db.getNode(result.nodeIds[1]).labels].sort(), ['Admin', 'Person']);
    assert.throws(
      () => db.graphPatch({ upsertNodes: [{ labels: [], key: 'patch-empty-labels' }] }),
      /node label set must contain at least one label/,
    );
  });

  it('delete cascades edges', () => {
    const x = db.upsertNode('Person', 'dx', { props: {} });
    const y = db.upsertNode('Person', 'dy', { props: {} });
    const e = db.upsertEdge(x, y, 'LINKS_TO');

    db.graphPatch({ deleteNodeIds: [x] });

    assert.equal(db.getNode(x), null);
    assert.equal(db.getEdge(e), null);
    assert.ok(db.getNode(y)); // y survives
  });

  it('deduplicates node upserts within patch', () => {
    const existing = db.upsertNode('Person', 'dup-node', { props: { v: '1' } });

    const result = db.graphPatch({
      upsertNodes: [
        { labels: ['Person'], key: 'dup-node', props: { v: '2' } },
        { labels: ['Person'], key: 'dup-node', props: { v: '3' } },
      ],
    });

    // Both get the existing ID
    assert.equal(result.nodeIds[0], existing);
    assert.equal(result.nodeIds[1], existing);

    // Last write wins
    const node = db.getNode(existing);
    assert.equal(node.props.v, '3');
  });

  it('upsert then delete in same patch (delete wins)', () => {
    const n = db.upsertNode('Person', 'ud', { props: {} });

    db.graphPatch({
      upsertNodes: [{ labels: ['Person'], key: 'ud', props: { v: 'updated' } }],
      deleteNodeIds: [n],
    });

    // Delete ordering wins
    assert.equal(db.getNode(n), null);
  });

  it('survives WAL replay', () => {
    // Use a fresh DB for restart test
    const tmpDir2 = mkdtempSync(join(tmpdir(), 'overgraph-patch-wal-'));
    const dbPath = join(tmpDir2, 'db');
    let db2 = OverGraph.open(dbPath, { walSyncMode: 'immediate' });

    const a = db2.upsertNode('Person', 'wa', { props: {} });
    const b = db2.upsertNode('Person', 'wb', { props: {} });

    const result = db2.graphPatch({
      upsertNodes: [{ labels: ['Person'], key: 'wc', props: { role: 'new' } }],
      upsertEdges: [{ from: a, to: b, label: 'DEPENDS_ON'}],
    });
    const nodeId = result.nodeIds[0];
    const edgeId = result.edgeIds[0];
    db2.close();

    // Reopen with WAL replay
    db2 = OverGraph.open(dbPath, { walSyncMode: 'immediate' });
    const node = db2.getNode(nodeId);
    assert.ok(node);
    assert.equal(node.key, 'wc');

    const edge = db2.getEdge(edgeId);
    assert.ok(edge);
    assert.equal(edge.label, 'DEPENDS_ON');

    db2.close();
    rmSync(tmpDir2, { recursive: true, force: true });
  });
});

describe('graphPatchAsync', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-patch-async-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('async patch works', async () => {
    const a = db.upsertNode('Person', 'ap-a', { props: {} });
    const b = db.upsertNode('Person', 'ap-b', { props: {} });

    const result = await db.graphPatchAsync({
      upsertNodes: [{ labels: ['Person'], key: 'ap-c' }],
      upsertEdges: [{ from: a, to: b, label: 'PATCH_EDGE'}],
    });

    assert.equal(result.nodeIds.length, 1);
    assert.equal(result.edgeIds.length, 1);
    assert.ok(db.getNode(result.nodeIds[0]));
    assert.ok(db.getEdge(result.edgeIds[0]));
  });
});

// ========== P10-006: prune(policy) ==========

describe('prune', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-prune-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('empty policy is rejected', () => {
    db.upsertNode('Person', 'safe', { props: {}, weight: 1.0 });
    assert.throws(() => db.prune({}), /at least max_age_ms or max_weight/);
  });

  it('prunes by weight', () => {
    const low = db.upsertNode('Person', 'pr-low', { props: {}, weight: 0.1 });
    const high = db.upsertNode('Person', 'pr-high', { props: {}, weight: 0.9 });

    const result = db.prune({ maxWeight: 0.5 });
    assert.ok(result.nodesPruned >= 1);
    assert.equal(db.getNode(low), null);
    assert.ok(db.getNode(high));
  });

  it('prunes by weight, boundary (<=)', () => {
    const exact = db.upsertNode('Person', 'pr-exact', { props: {}, weight: 0.5 });
    const above = db.upsertNode('Person', 'pr-above', { props: {}, weight: 0.500001 });

    db.prune({ maxWeight: 0.5 });
    assert.equal(db.getNode(exact), null);
    assert.ok(db.getNode(above));
  });

  it('cascade deletes edges of pruned nodes', () => {
    const a = db.upsertNode('Person', 'pr-ca', { props: {}, weight: 0.1 });
    const b = db.upsertNode('Person', 'pr-cb', { props: {}, weight: 0.9 });
    const e = db.upsertEdge(a, b, 'LINKS_TO');

    const result = db.prune({ maxWeight: 0.5 });
    assert.equal(db.getNode(a), null);
    assert.equal(db.getEdge(e), null);
    assert.ok(db.getNode(b)); // survives
    assert.ok(result.edgesPruned >= 1);
  });

  it('type-scoped prune', () => {
    const t1 = db.upsertNode('PruneTarget', 'pr-t1', { props: {}, weight: 0.1 });
    const t2 = db.upsertNode('PruneOther', 'pr-t2', { props: {}, weight: 0.1 });

    db.prune({ maxWeight: 0.5, label: 'PruneTarget'});
    assert.equal(db.getNode(t1), null);
    assert.ok(db.getNode(t2)); // different type, survives
  });

  it('no matches returns zero counts', () => {
    db.upsertNode('Person', 'pr-nomatch', { props: {}, weight: 0.9 });
    const result = db.prune({ maxWeight: 0.01 });
    assert.equal(result.nodesPruned, 0);
    assert.equal(result.edgesPruned, 0);
  });

  it('works after flush (segment source)', () => {
    const a = db.upsertNode('Person', 'pr-seg', { props: {}, weight: 0.1 });
    const b = db.upsertNode('Person', 'pr-seg-keep', { props: {}, weight: 0.9 });
    const e = db.upsertEdge(a, b, 'LINKS_TO');
    db.flush();

    const result = db.prune({ maxWeight: 0.5 });
    assert.ok(result.nodesPruned >= 1);
    assert.equal(db.getNode(a), null);
    assert.equal(db.getEdge(e), null);
    assert.ok(db.getNode(b));
  });

  it('survives WAL replay', () => {
    const tmpDir2 = mkdtempSync(join(tmpdir(), 'overgraph-prune-wal-'));
    const dbPath = join(tmpDir2, 'db');
    let db2 = OverGraph.open(dbPath, { walSyncMode: 'immediate' });

    const a = db2.upsertNode('Person', 'pr-wal-a', { props: {}, weight: 0.1 });
    const b = db2.upsertNode('Person', 'pr-wal-b', { props: {}, weight: 0.9 });
    const e = db2.upsertEdge(a, b, 'LINKS_TO');

    db2.prune({ maxWeight: 0.5 });
    db2.close();

    // Reopen with WAL replay
    db2 = OverGraph.open(dbPath, { walSyncMode: 'immediate' });
    assert.equal(db2.getNode(a), null);
    assert.ok(db2.getNode(b));
    assert.equal(db2.getEdge(e), null);

    db2.close();
    rmSync(tmpDir2, { recursive: true, force: true });
  });
});

describe('pruneAsync', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-prune-async-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('async prune works', async () => {
    const a = db.upsertNode('Person', 'ap-low', { props: {}, weight: 0.1 });
    const b = db.upsertNode('Person', 'ap-high', { props: {}, weight: 0.9 });

    const result = await db.pruneAsync({ maxWeight: 0.5 });
    assert.ok(result.nodesPruned >= 1);
    assert.equal(db.getNode(a), null);
    assert.ok(db.getNode(b));
  });
});

// ============================================================
// FO-005: Named prune policies (compaction-filter auto-prune)
// ============================================================

describe('setPrunePolicy / listPrunePolicies / removePrunePolicy', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-policy-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('initially empty', () => {
    assert.deepEqual(db.listPrunePolicies(), []);
  });

  it('set and list a policy', () => {
    db.setPrunePolicy('low-weight', { maxWeight: 0.5 });
    const list = db.listPrunePolicies();
    assert.equal(list.length, 1);
    assert.equal(list[0].name, 'low-weight');
    assert.equal(list[0].policy.maxWeight, 0.5);
  });

  it('overwrite a policy', () => {
    db.setPrunePolicy('low-weight', { maxAgeMs: 60000 });
    const list = db.listPrunePolicies();
    assert.equal(list.length, 1);
    assert.equal(list[0].policy.maxAgeMs, 60000);
  });

  it('remove a policy', () => {
    assert.equal(db.removePrunePolicy('low-weight'), true);
    assert.deepEqual(db.listPrunePolicies(), []);
    assert.equal(db.removePrunePolicy('nonexistent'), false);
  });

  it('rejects empty policy', () => {
    assert.throws(() => db.setPrunePolicy('bad', {}), /at least/i);
  });
});

describe('prune policy survives close/reopen', () => {
  it('policies persist in manifest', () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-policy-persist-'));
    const dbPath = join(tmpDir, 'db');

    let db = OverGraph.open(dbPath, { walSyncMode: 'immediate' });
    db.setPrunePolicy('age', { maxAgeMs: 30000 });
    db.setPrunePolicy('weight', { maxWeight: 0.1, label: 'User'});
    db.close();

    db = OverGraph.open(dbPath, { walSyncMode: 'immediate' });
    const list = db.listPrunePolicies();
    assert.equal(list.length, 2);
    // BTreeMap ordering: 'age' < 'weight'
    assert.equal(list[0].name, 'age');
    assert.equal(list[0].policy.maxAgeMs, 30000);
    assert.equal(list[1].name, 'weight');
    assert.ok(Math.abs(list[1].policy.maxWeight - 0.1) < 1e-6);
    assert.equal(list[1].policy.label, 'User');
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('compaction auto-prune', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-auto-prune-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('prunes matching nodes and cascade-drops edges during compaction', () => {
    const a = db.upsertNode('Person', 'a', { props: {}, weight: 0.1 });  // will be pruned
    const b = db.upsertNode('Person', 'b', { props: {}, weight: 0.9 });
    const c = db.upsertNode('Person', 'c', { props: {}, weight: 0.9 });
    const e1 = db.upsertEdge(a, b, 'LINKS_TO');
    const e2 = db.upsertEdge(b, c, 'LINKS_TO');
    db.flush();

    db.upsertNode('Person', 'b', { props: {}, weight: 0.9 });  // overlap → forces standard compaction path
    db.flush();

    db.setPrunePolicy('low', { maxWeight: 0.5 });
    const stats = db.compact();

    assert.equal(stats.nodesAutoPruned, 1);
    assert.equal(stats.edgesAutoPruned, 1);
    assert.equal(db.getNode(a), null);
    assert.ok(db.getNode(b));
    assert.ok(db.getNode(c));
    assert.equal(db.getEdge(e1), null);   // cascade
    assert.ok(db.getEdge(e2));             // survives
  });
});

describe('compaction auto-prune type-scoped', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-type-prune-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('only prunes the targeted type', () => {
    const t1 = db.upsertNode('Person', 't1-low', { props: {}, weight: 0.1 });
    const t2 = db.upsertNode('Company', 't2-low', { props: {}, weight: 0.1 });
    db.flush();
    db.upsertNode('Person', 't1-low', { props: {}, weight: 0.1 });  // overlap → forces standard compaction path
    db.flush();

    db.setPrunePolicy('type1', { maxWeight: 0.5, label: 'Person'});
    const stats = db.compact();

    assert.equal(stats.nodesAutoPruned, 1);
    assert.equal(db.getNode(t1), null);
    assert.ok(db.getNode(t2));  // type 2, out of scope
  });
});

describe('setPrunePolicyAsync / removePrunePolicyAsync / listPrunePoliciesAsync', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-policy-async-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('async set, list, and remove work', async () => {
    await db.setPrunePolicyAsync('p1', { maxWeight: 0.3 });
    const list = await db.listPrunePoliciesAsync();
    assert.equal(list.length, 1);
    assert.equal(list[0].name, 'p1');

    const removed = await db.removePrunePolicyAsync('p1');
    assert.equal(removed, true);

    const list2 = await db.listPrunePoliciesAsync();
    assert.equal(list2.length, 0);
  });
});

// FO-005a: Read-time prune policy filtering tests

describe('read-time policy filtering, getNode', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-rtf-get-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('policy-excluded node returns null from getNode', () => {
    const lo = db.upsertNode('Person', 'lo', { props: {}, weight: 0.2 });
    const hi = db.upsertNode('Person', 'hi', { props: {}, weight: 0.9 });

    // Before policy: both visible
    assert.ok(db.getNode(lo));
    assert.ok(db.getNode(hi));

    db.setPrunePolicy('hide-low', { maxWeight: 0.5 });

    // After policy: lo hidden, hi visible
    assert.equal(db.getNode(lo), null);
    assert.ok(db.getNode(hi));

    // getNodeByKey also filtered
    assert.equal(db.getNodeByKey('Person', 'lo'), null);
    assert.ok(db.getNodeByKey('Person', 'hi'));

    // Remove policy → visible again
    db.removePrunePolicy('hide-low');
    assert.ok(db.getNode(lo));
  });
});

describe('read-time policy filtering, neighbors', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-rtf-nbrs-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('policy-excluded neighbor omitted from results', () => {
    const a = db.upsertNode('Person', 'a', { props: {}, weight: 0.9 });
    const b = db.upsertNode('Person', 'b', { props: {}, weight: 0.2 }); // will be excluded
    const c = db.upsertNode('Person', 'c', { props: {}, weight: 0.8 });

    db.upsertEdge(a, b, 'LINKS_TO', { props: {}, weight: 1.0 });
    db.upsertEdge(a, c, 'LINKS_TO', { props: {}, weight: 1.0 });

    // Before policy: 2 neighbors
    let result = db.neighbors(a, { direction: 'outgoing' });
    assert.equal(result.length, 2);

    db.setPrunePolicy('p', { maxWeight: 0.5 });

    // After policy: only c visible
    result = db.neighbors(a, { direction: 'outgoing' });
    assert.equal(result.length, 1);
    assert.equal(result[0].nodeId, c);
  });
});

describe('read-time policy filtering, toggle visibility', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-rtf-toggle-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate', compactAfterNFlushes: 0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('add policy hides node, remove policy reveals it', () => {
    const id = db.upsertNode('Person', 'target', { props: {}, weight: 0.3 });

    assert.ok(db.getNode(id));

    db.setPrunePolicy('p', { maxWeight: 0.5 });
    assert.equal(db.getNode(id), null);

    db.removePrunePolicy('p');
    assert.ok(db.getNode(id));
  });

  it('upsert dedup works through policy (no duplicate IDs)', () => {
    const id1 = db.upsertNode('Person', 'dedup-test', { props: {}, weight: 0.2 });
    db.setPrunePolicy('p', { maxWeight: 0.5 });

    // Hidden from reads
    assert.equal(db.getNode(id1), null);

    // Upsert same key; must reuse ID
    const id2 = db.upsertNode('Person', 'dedup-test', { props: {}, weight: 0.8 });
    assert.equal(id1, id2);

    // Now weight 0.8 > 0.5, visible again
    assert.ok(db.getNode(id2));

    db.removePrunePolicy('p');
  });
});

// Phase 12: API Completeness tests

describe('getNodesByLabels', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nbt-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns hydrated node records by type', () => {
    db.upsertNode('Person', 'alice', { props: { name: 'Alice' }, weight: 0.9 });
    db.upsertNode('Person', 'bob', { props: { name: 'Bob' }, weight: 0.8 });
    db.upsertNode('Company', 'charlie', { props: {}, weight: 0.7 });

    const type1 = db.getNodesByLabels('Person');
    assert.equal(type1.length, 2);
    assert.ok(type1.every(n => n.labels.includes('Person')));
    const keys = type1.map(n => n.key);
    assert.ok(keys.includes('alice'));
    assert.ok(keys.includes('bob'));

    // Verify full record hydration
    const alice = type1.find(n => n.key === 'alice');
    assert.equal(alice.props.name, 'Alice');
    assert.ok(alice.weight > 0);
    assert.ok(alice.createdAt > 0);
  });

  it('returns empty array for non-existent type', () => {
    assert.deepEqual(db.getNodesByLabels('MissingLabel'), []);
  });

  it('excludes deleted nodes', () => {
    const id = db.upsertNode('Document', 'to-delete', { props: {} });
    db.upsertNode('Document', 'keeper', { props: {} });
    db.deleteNode(id);

    const type3 = db.getNodesByLabels('Document');
    assert.equal(type3.length, 1);
    assert.equal(type3[0].key, 'keeper');
  });

  it('works across memtable and segments', () => {
    db.upsertNode('Post', 'seg1', { props: {} });
    db.flush();
    db.upsertNode('Post', 'mem1', { props: {} });

    const type4 = db.getNodesByLabels('Post');
    assert.equal(type4.length, 2);
  });
});

describe('getEdgesByLabel', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ebt-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns hydrated edge records by type', () => {
    const a = db.upsertNode('Person', 'a', { props: {} });
    const b = db.upsertNode('Person', 'b', { props: {} });
    const c = db.upsertNode('Person', 'c', { props: {} });

    db.upsertEdge(a, b, 'WORKS_AT', { props: { rel: 'knows' }, weight: 0.9 });
    db.upsertEdge(b, c, 'WORKS_AT', { props: { rel: 'likes' }, weight: 0.8 });
    db.upsertEdge(a, c, 'MENTIONS', { props: {}, weight: 0.5 });

    const type10 = db.getEdgesByLabel('WORKS_AT');
    assert.equal(type10.length, 2);
    assert.ok(type10.every(e => e.label === 'WORKS_AT'));
    assert.ok(type10.every(e => e.weight > 0));
  });

  it('returns empty array for non-existent type', () => {
    assert.deepEqual(db.getEdgesByLabel('MISSING_EDGE_TYPE'), []);
  });

  it('works across memtable and segments', () => {
    const x = db.upsertNode('Person', 'x', { props: {} });
    const y = db.upsertNode('Person', 'y', { props: {} });
    db.upsertEdge(x, y, 'OWNS', { props: {}, weight: 1.0 });
    db.flush();
    const z = db.upsertNode('Person', 'z', { props: {} });
    db.upsertEdge(y, z, 'OWNS', { props: {}, weight: 1.0 });

    const type30 = db.getEdgesByLabel('OWNS');
    assert.equal(type30.length, 2);
  });
});

describe('countNodesByLabels / countEdgesByLabel', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-cnt-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('counts nodes by type without hydrating records', () => {
    db.upsertNode('Person', 'n1', { props: {} });
    db.upsertNode('Person', 'n2', { props: {} });
    db.upsertNode('Company', 'n3', { props: {} });

    assert.equal(db.countNodesByLabels('Person'), 2);
    assert.equal(db.countNodesByLabels('Company'), 1);
    assert.equal(db.countNodesByLabels('MissingLabel'), 0);
  });

  it('counts edges by type', () => {
    const a = db.upsertNode('Person', 'ca', { props: {} });
    const b = db.upsertNode('Person', 'cb', { props: {} });
    const c = db.upsertNode('Person', 'cc', { props: {} });
    db.upsertEdge(a, b, 'WORKS_AT', { props: {}, weight: 1.0 });
    db.upsertEdge(b, c, 'WORKS_AT', { props: {}, weight: 1.0 });
    db.upsertEdge(a, c, 'MENTIONS', { props: {}, weight: 1.0 });

    assert.equal(db.countEdgesByLabel('WORKS_AT'), 2);
    assert.equal(db.countEdgesByLabel('MENTIONS'), 1);
    assert.equal(db.countEdgesByLabel('MISSING_EDGE_TYPE'), 0);
  });

  it('counts respect tombstones', () => {
    const id = db.upsertNode('User', 'temp', { props: {} });
    assert.equal(db.countNodesByLabels('User'), 1);
    db.deleteNode(id);
    assert.equal(db.countNodesByLabels('User'), 0);
  });

  it('counts work across memtable and segments', () => {
    db.upsertNode('Location', 's1', { props: {} });
    db.flush();
    db.upsertNode('Location', 'm1', { props: {} });

    assert.equal(db.countNodesByLabels('Location'), 2);
  });
});

describe('async variants of type query APIs', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-tqa-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });
    db.upsertNode('Person', 'a', { props: { v: 1 }, weight: 0.9 });
    db.upsertNode('Person', 'b', { props: { v: 2 }, weight: 0.8 });
    db.upsertNode('Company', 'c', { props: {}, weight: 0.7 });
    const na = db.upsertNode('Person', 'a', { props: { v: 1 }, weight: 0.9 }); // already exists, same id
    const nb = db.upsertNode('Person', 'b', { props: { v: 2 }, weight: 0.8 });
    db.upsertEdge(na, nb, 'WORKS_AT', { props: {}, weight: 1.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('getNodesByLabelsAsync returns hydrated records', async () => {
    const records = await db.getNodesByLabelsAsync('Person');
    assert.equal(records.length, 2);
    assert.ok(records.every(n => n.labels.includes('Person')));
  });

  it('getEdgesByLabelAsync returns hydrated records', async () => {
    const records = await db.getEdgesByLabelAsync('WORKS_AT');
    assert.equal(records.length, 1);
    assert.equal(records[0].label, 'WORKS_AT');
  });

  it('countNodesByLabelsAsync returns correct count', async () => {
    const count = await db.countNodesByLabelsAsync('Person');
    assert.equal(count, 2);
    const zero = await db.countNodesByLabelsAsync('MissingLabel');
    assert.equal(zero, 0);
  });

  it('countEdgesByLabelAsync returns correct count', async () => {
    const count = await db.countEdgesByLabelAsync('WORKS_AT');
    assert.equal(count, 1);
    const zero = await db.countEdgesByLabelAsync('MISSING_EDGE_TYPE');
    assert.equal(zero, 0);
  });
});
