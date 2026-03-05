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
    const id = db.upsertNode(1, 'alice', { name: 'Alice' }, 1.0);
    const node = db.getNodeByKey(1, 'alice');
    assert.ok(node);
    assert.equal(node.id, id);
    assert.equal(node.typeId, 1);
    assert.equal(node.key, 'alice');
    assert.equal(node.props.name, 'Alice');
  });

  it('returns null for missing key', () => {
    assert.equal(db.getNodeByKey(1, 'nonexistent'), null);
  });

  it('returns null for wrong type_id', () => {
    db.upsertNode(5, 'typed', {});
    assert.equal(db.getNodeByKey(99, 'typed'), null);
  });

  it('returns null for deleted node', () => {
    const id = db.upsertNode(1, 'to-delete', {});
    db.deleteNode(id);
    assert.equal(db.getNodeByKey(1, 'to-delete'), null);
  });

  it('works after flush (segment source)', () => {
    const id = db.upsertNode(1, 'flushed', { v: 1 });
    db.flush();
    const node = db.getNodeByKey(1, 'flushed');
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
    const a = db.upsertNode(1, 'a', {});
    const b = db.upsertNode(1, 'b', {});
    const eid = db.upsertEdge(a, b, 10, { rel: 'knows' }, 0.5);
    const edge = db.getEdgeByTriple(a, b, 10);
    assert.ok(edge);
    assert.equal(edge.id, eid);
    assert.equal(edge.from, a);
    assert.equal(edge.to, b);
    assert.equal(edge.typeId, 10);
    assert.equal(edge.props.rel, 'knows');
  });

  it('returns null for wrong type_id', () => {
    const a = db.upsertNode(1, 'c', {});
    const b = db.upsertNode(1, 'd', {});
    db.upsertEdge(a, b, 10, {});
    assert.equal(db.getEdgeByTriple(a, b, 99), null);
  });

  it('returns null for reversed direction', () => {
    const a = db.upsertNode(1, 'e', {});
    const b = db.upsertNode(1, 'f', {});
    db.upsertEdge(a, b, 10, {});
    assert.equal(db.getEdgeByTriple(b, a, 10), null);
  });

  it('returns null after delete', () => {
    const a = db.upsertNode(1, 'g', {});
    const b = db.upsertNode(1, 'h', {});
    const eid = db.upsertEdge(a, b, 10, {});
    db.deleteEdge(eid);
    assert.equal(db.getEdgeByTriple(a, b, 10), null);
  });

  it('works after flush', () => {
    const a = db.upsertNode(1, 'i', {});
    const b = db.upsertNode(1, 'j', {});
    const eid = db.upsertEdge(a, b, 20, {});
    db.flush();
    const edge = db.getEdgeByTriple(a, b, 20);
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
    const a = db.upsertNode(1, 'sg-a', { name: 'A' });
    const b = db.upsertNode(1, 'sg-b', { name: 'B' });
    const c = db.upsertNode(1, 'sg-c', { name: 'C' });
    db.upsertEdge(a, b, 1);
    db.upsertEdge(b, c, 1);

    const sg = db.extractSubgraph(a, 2);
    assert.ok(sg);
    assert.equal(sg.nodes.length, 3);
    assert.equal(sg.edges.length, 2);
  });

  it('respects maxDepth', () => {
    const sg = db.extractSubgraph(db.upsertNode(1, 'sg-a', {}), 0);
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
    const a = db.upsertNode(1, 'bulk-a', { n: 'A' });
    const b = db.upsertNode(1, 'bulk-b', { n: 'B' });
    const c = db.upsertNode(1, 'bulk-c', { n: 'C' });
    const results = db.getNodes([a, b, c]);
    assert.equal(results.length, 3);
    assert.equal(results[0].key, 'bulk-a');
    assert.equal(results[1].key, 'bulk-b');
    assert.equal(results[2].key, 'bulk-c');
  });

  it('getNodes returns null for missing/deleted', () => {
    const a = db.upsertNode(1, 'bulk-d', {});
    const b = db.upsertNode(1, 'bulk-e', {});
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
    const a = db.upsertNode(1, 'bulk-ea', {});
    const b = db.upsertNode(1, 'bulk-eb', {});
    const c = db.upsertNode(1, 'bulk-ec', {});
    const e1 = db.upsertEdge(a, b, 1);
    const e2 = db.upsertEdge(b, c, 1);
    const results = db.getEdges([e1, e2, 99999]);
    assert.equal(results.length, 3);
    assert.equal(results[0].from, a);
    assert.equal(results[1].from, b);
    assert.equal(results[2], null);
  });

  it('getNodes works cross-source (memtable + segment)', () => {
    const a = db.upsertNode(1, 'bulk-xa', {});
    db.flush();
    const b = db.upsertNode(1, 'bulk-xb', {});
    const results = db.getNodes([a, b]);
    assert.equal(results[0].key, 'bulk-xa');
    assert.equal(results[1].key, 'bulk-xb');
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
    const id = db.upsertNode(1, 'async-key', { x: 1 });
    const node = await db.getNodeByKeyAsync(1, 'async-key');
    assert.ok(node);
    assert.equal(node.id, id);
  });

  it('getEdgeByTripleAsync works', async () => {
    const a = db.upsertNode(1, 'at-a', {});
    const b = db.upsertNode(1, 'at-b', {});
    const eid = db.upsertEdge(a, b, 5);
    const edge = await db.getEdgeByTripleAsync(a, b, 5);
    assert.ok(edge);
    assert.equal(edge.id, eid);
  });

  it('getNodesAsync works', async () => {
    const a = db.upsertNode(1, 'an-a', {});
    const b = db.upsertNode(1, 'an-b', {});
    const results = await db.getNodesAsync([a, b]);
    assert.equal(results.length, 2);
    assert.equal(results[0].key, 'an-a');
  });

  it('getEdgesAsync works', async () => {
    const a = db.upsertNode(1, 'ae-a', {});
    const b = db.upsertNode(1, 'ae-b', {});
    const e = db.upsertEdge(a, b, 1);
    const results = await db.getEdgesAsync([e]);
    assert.equal(results.length, 1);
    assert.equal(results[0].from, a);
  });

  it('extractSubgraphAsync works', async () => {
    const a = db.upsertNode(1, 'esg-a', {});
    const b = db.upsertNode(1, 'esg-b', {});
    db.upsertEdge(a, b, 1);
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
    const a = db.upsertNode(1, 'pa', {});
    const b = db.upsertNode(1, 'pb', {});
    const e1 = db.upsertEdge(a, b, 1, { v: 'old' });

    const result = db.graphPatch({
      upsertNodes: [{ typeId: 1, key: 'pc', props: { role: 'new' } }],
      upsertEdges: [{ from: a, to: b, typeId: 2 }],
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
    assert.equal(e2.typeId, 2);

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

  it('delete cascades edges', () => {
    const x = db.upsertNode(1, 'dx', {});
    const y = db.upsertNode(1, 'dy', {});
    const e = db.upsertEdge(x, y, 1);

    db.graphPatch({ deleteNodeIds: [x] });

    assert.equal(db.getNode(x), null);
    assert.equal(db.getEdge(e), null);
    assert.ok(db.getNode(y)); // y survives
  });

  it('deduplicates node upserts within patch', () => {
    const existing = db.upsertNode(1, 'dup-node', { v: '1' });

    const result = db.graphPatch({
      upsertNodes: [
        { typeId: 1, key: 'dup-node', props: { v: '2' } },
        { typeId: 1, key: 'dup-node', props: { v: '3' } },
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
    const n = db.upsertNode(1, 'ud', {});

    db.graphPatch({
      upsertNodes: [{ typeId: 1, key: 'ud', props: { v: 'updated' } }],
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

    const a = db2.upsertNode(1, 'wa', {});
    const b = db2.upsertNode(1, 'wb', {});

    const result = db2.graphPatch({
      upsertNodes: [{ typeId: 1, key: 'wc', props: { role: 'new' } }],
      upsertEdges: [{ from: a, to: b, typeId: 5 }],
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
    assert.equal(edge.typeId, 5);

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
    const a = db.upsertNode(1, 'ap-a', {});
    const b = db.upsertNode(1, 'ap-b', {});

    const result = await db.graphPatchAsync({
      upsertNodes: [{ typeId: 1, key: 'ap-c' }],
      upsertEdges: [{ from: a, to: b, typeId: 3 }],
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

  it('empty policy is a no-op', () => {
    db.upsertNode(1, 'safe', {}, 1.0);
    const result = db.prune({});
    assert.equal(result.nodesPruned, 0);
    assert.equal(result.edgesPruned, 0);
  });

  it('prunes by weight', () => {
    const low = db.upsertNode(1, 'pr-low', {}, 0.1);
    const high = db.upsertNode(1, 'pr-high', {}, 0.9);

    const result = db.prune({ maxWeight: 0.5 });
    assert.ok(result.nodesPruned >= 1);
    assert.equal(db.getNode(low), null);
    assert.ok(db.getNode(high));
  });

  it('prunes by weight, boundary (<=)', () => {
    const exact = db.upsertNode(1, 'pr-exact', {}, 0.5);
    const above = db.upsertNode(1, 'pr-above', {}, 0.500001);

    db.prune({ maxWeight: 0.5 });
    assert.equal(db.getNode(exact), null);
    assert.ok(db.getNode(above));
  });

  it('cascade deletes edges of pruned nodes', () => {
    const a = db.upsertNode(1, 'pr-ca', {}, 0.1);
    const b = db.upsertNode(1, 'pr-cb', {}, 0.9);
    const e = db.upsertEdge(a, b, 1);

    const result = db.prune({ maxWeight: 0.5 });
    assert.equal(db.getNode(a), null);
    assert.equal(db.getEdge(e), null);
    assert.ok(db.getNode(b)); // survives
    assert.ok(result.edgesPruned >= 1);
  });

  it('type-scoped prune', () => {
    const t1 = db.upsertNode(10, 'pr-t1', {}, 0.1);
    const t2 = db.upsertNode(20, 'pr-t2', {}, 0.1);

    db.prune({ maxWeight: 0.5, typeId: 10 });
    assert.equal(db.getNode(t1), null);
    assert.ok(db.getNode(t2)); // different type, survives
  });

  it('no matches returns zero counts', () => {
    db.upsertNode(1, 'pr-nomatch', {}, 0.9);
    const result = db.prune({ maxWeight: 0.01 });
    assert.equal(result.nodesPruned, 0);
    assert.equal(result.edgesPruned, 0);
  });

  it('works after flush (segment source)', () => {
    const a = db.upsertNode(1, 'pr-seg', {}, 0.1);
    const b = db.upsertNode(1, 'pr-seg-keep', {}, 0.9);
    const e = db.upsertEdge(a, b, 1);
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

    const a = db2.upsertNode(1, 'pr-wal-a', {}, 0.1);
    const b = db2.upsertNode(1, 'pr-wal-b', {}, 0.9);
    const e = db2.upsertEdge(a, b, 1);

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
    const a = db.upsertNode(1, 'ap-low', {}, 0.1);
    const b = db.upsertNode(1, 'ap-high', {}, 0.9);

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
    db.setPrunePolicy('weight', { maxWeight: 0.1, typeId: 5 });
    db.close();

    db = OverGraph.open(dbPath, { walSyncMode: 'immediate' });
    const list = db.listPrunePolicies();
    assert.equal(list.length, 2);
    // BTreeMap ordering: 'age' < 'weight'
    assert.equal(list[0].name, 'age');
    assert.equal(list[0].policy.maxAgeMs, 30000);
    assert.equal(list[1].name, 'weight');
    assert.ok(Math.abs(list[1].policy.maxWeight - 0.1) < 1e-6);
    assert.equal(list[1].policy.typeId, 5);
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
    const a = db.upsertNode(1, 'a', {}, 0.1);  // will be pruned
    const b = db.upsertNode(1, 'b', {}, 0.9);
    const c = db.upsertNode(1, 'c', {}, 0.9);
    const e1 = db.upsertEdge(a, b, 1);
    const e2 = db.upsertEdge(b, c, 1);
    db.flush();

    db.upsertNode(1, 'b', {}, 0.9);  // overlap → forces standard compaction path
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
    const t1 = db.upsertNode(1, 't1-low', {}, 0.1);
    const t2 = db.upsertNode(2, 't2-low', {}, 0.1);
    db.flush();
    db.upsertNode(1, 't1-low', {}, 0.1);  // overlap → forces standard compaction path
    db.flush();

    db.setPrunePolicy('type1', { maxWeight: 0.5, typeId: 1 });
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
    const lo = db.upsertNode(1, 'lo', {}, 0.2);
    const hi = db.upsertNode(1, 'hi', {}, 0.9);

    // Before policy: both visible
    assert.ok(db.getNode(lo));
    assert.ok(db.getNode(hi));

    db.setPrunePolicy('hide-low', { maxWeight: 0.5 });

    // After policy: lo hidden, hi visible
    assert.equal(db.getNode(lo), null);
    assert.ok(db.getNode(hi));

    // getNodeByKey also filtered
    assert.equal(db.getNodeByKey(1, 'lo'), null);
    assert.ok(db.getNodeByKey(1, 'hi'));

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
    const a = db.upsertNode(1, 'a', {}, 0.9);
    const b = db.upsertNode(1, 'b', {}, 0.2); // will be excluded
    const c = db.upsertNode(1, 'c', {}, 0.8);

    db.upsertEdge(a, b, 1, {}, 1.0);
    db.upsertEdge(a, c, 1, {}, 1.0);

    // Before policy: 2 neighbors
    let result = db.neighbors(a, 'outgoing');
    assert.equal(result.length, 2);

    db.setPrunePolicy('p', { maxWeight: 0.5 });

    // After policy: only c visible
    result = db.neighbors(a, 'outgoing');
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), c);
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
    const id = db.upsertNode(1, 'target', {}, 0.3);

    assert.ok(db.getNode(id));

    db.setPrunePolicy('p', { maxWeight: 0.5 });
    assert.equal(db.getNode(id), null);

    db.removePrunePolicy('p');
    assert.ok(db.getNode(id));
  });

  it('upsert dedup works through policy (no duplicate IDs)', () => {
    const id1 = db.upsertNode(1, 'dedup-test', {}, 0.2);
    db.setPrunePolicy('p', { maxWeight: 0.5 });

    // Hidden from reads
    assert.equal(db.getNode(id1), null);

    // Upsert same key; must reuse ID
    const id2 = db.upsertNode(1, 'dedup-test', {}, 0.8);
    assert.equal(id1, id2);

    // Now weight 0.8 > 0.5, visible again
    assert.ok(db.getNode(id2));

    db.removePrunePolicy('p');
  });
});

// Phase 12: API Completeness tests

describe('getNodesByType', () => {
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
    db.upsertNode(1, 'alice', { name: 'Alice' }, 0.9);
    db.upsertNode(1, 'bob', { name: 'Bob' }, 0.8);
    db.upsertNode(2, 'charlie', {}, 0.7);

    const type1 = db.getNodesByType(1);
    assert.equal(type1.length, 2);
    assert.ok(type1.every(n => n.typeId === 1));
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
    assert.deepEqual(db.getNodesByType(99), []);
  });

  it('excludes deleted nodes', () => {
    const id = db.upsertNode(3, 'to-delete', {});
    db.upsertNode(3, 'keeper', {});
    db.deleteNode(id);

    const type3 = db.getNodesByType(3);
    assert.equal(type3.length, 1);
    assert.equal(type3[0].key, 'keeper');
  });

  it('works across memtable and segments', () => {
    db.upsertNode(4, 'seg1', {});
    db.flush();
    db.upsertNode(4, 'mem1', {});

    const type4 = db.getNodesByType(4);
    assert.equal(type4.length, 2);
  });
});

describe('getEdgesByType', () => {
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
    const a = db.upsertNode(1, 'a', {});
    const b = db.upsertNode(1, 'b', {});
    const c = db.upsertNode(1, 'c', {});

    db.upsertEdge(a, b, 10, { rel: 'knows' }, 0.9);
    db.upsertEdge(b, c, 10, { rel: 'likes' }, 0.8);
    db.upsertEdge(a, c, 20, {}, 0.5);

    const type10 = db.getEdgesByType(10);
    assert.equal(type10.length, 2);
    assert.ok(type10.every(e => e.typeId === 10));
    assert.ok(type10.every(e => e.weight > 0));
  });

  it('returns empty array for non-existent type', () => {
    assert.deepEqual(db.getEdgesByType(99), []);
  });

  it('works across memtable and segments', () => {
    const x = db.upsertNode(1, 'x', {});
    const y = db.upsertNode(1, 'y', {});
    db.upsertEdge(x, y, 30, {}, 1.0);
    db.flush();
    const z = db.upsertNode(1, 'z', {});
    db.upsertEdge(y, z, 30, {}, 1.0);

    const type30 = db.getEdgesByType(30);
    assert.equal(type30.length, 2);
  });
});

describe('countNodesByType / countEdgesByType', () => {
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
    db.upsertNode(1, 'n1', {});
    db.upsertNode(1, 'n2', {});
    db.upsertNode(2, 'n3', {});

    assert.equal(db.countNodesByType(1), 2);
    assert.equal(db.countNodesByType(2), 1);
    assert.equal(db.countNodesByType(99), 0);
  });

  it('counts edges by type', () => {
    const a = db.upsertNode(1, 'ca', {});
    const b = db.upsertNode(1, 'cb', {});
    const c = db.upsertNode(1, 'cc', {});
    db.upsertEdge(a, b, 10, {}, 1.0);
    db.upsertEdge(b, c, 10, {}, 1.0);
    db.upsertEdge(a, c, 20, {}, 1.0);

    assert.equal(db.countEdgesByType(10), 2);
    assert.equal(db.countEdgesByType(20), 1);
    assert.equal(db.countEdgesByType(99), 0);
  });

  it('counts respect tombstones', () => {
    const id = db.upsertNode(5, 'temp', {});
    assert.equal(db.countNodesByType(5), 1);
    db.deleteNode(id);
    assert.equal(db.countNodesByType(5), 0);
  });

  it('counts work across memtable and segments', () => {
    db.upsertNode(6, 's1', {});
    db.flush();
    db.upsertNode(6, 'm1', {});

    assert.equal(db.countNodesByType(6), 2);
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
    db.upsertNode(1, 'a', { v: 1 }, 0.9);
    db.upsertNode(1, 'b', { v: 2 }, 0.8);
    db.upsertNode(2, 'c', {}, 0.7);
    const na = db.upsertNode(1, 'a', { v: 1 }, 0.9); // already exists, same id
    const nb = db.upsertNode(1, 'b', { v: 2 }, 0.8);
    db.upsertEdge(na, nb, 10, {}, 1.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('getNodesByTypeAsync returns hydrated records', async () => {
    const records = await db.getNodesByTypeAsync(1);
    assert.equal(records.length, 2);
    assert.ok(records.every(n => n.typeId === 1));
  });

  it('getEdgesByTypeAsync returns hydrated records', async () => {
    const records = await db.getEdgesByTypeAsync(10);
    assert.equal(records.length, 1);
    assert.equal(records[0].typeId, 10);
  });

  it('countNodesByTypeAsync returns correct count', async () => {
    const count = await db.countNodesByTypeAsync(1);
    assert.equal(count, 2);
    const zero = await db.countNodesByTypeAsync(99);
    assert.equal(zero, 0);
  });

  it('countEdgesByTypeAsync returns correct count', async () => {
    const count = await db.countEdgesByTypeAsync(10);
    assert.equal(count, 1);
    const zero = await db.countEdgesByTypeAsync(99);
    assert.equal(zero, 0);
  });
});
