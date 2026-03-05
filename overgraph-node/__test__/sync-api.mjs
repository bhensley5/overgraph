import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';
import { packNodeBatch, packEdgeBatch } from '../helpers/pack-binary.mjs';

// Helper: create a fresh DB for each suite
function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

describe('upsert_node / upsert_edge', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-sync-'));
    db = freshDb(tmpDir, 'upsert');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('upsertNode returns a number id', () => {
    const id = db.upsertNode(1, 'alice');
    assert.equal(typeof id, 'number');
    assert.ok(id > 0);
  });

  it('upsertNode with props and weight', () => {
    const id = db.upsertNode(1, 'bob', { age: 30, name: 'Bob' }, 0.8);
    assert.equal(typeof id, 'number');
    assert.ok(id > 0);
  });

  it('upsertNode deduplicates by (type_id, key)', () => {
    const id1 = db.upsertNode(2, 'same-key');
    const id2 = db.upsertNode(2, 'same-key');
    assert.equal(id1, id2);
  });

  it('upsertEdge returns a number id', () => {
    const a = db.upsertNode(1, 'src');
    const b = db.upsertNode(1, 'dst');
    const eid = db.upsertEdge(a, b, 10);
    assert.equal(typeof eid, 'number');
    assert.ok(eid > 0);
  });

  it('upsertEdge with props and weight', () => {
    const a = db.upsertNode(1, 'e-src');
    const b = db.upsertNode(1, 'e-dst');
    const eid = db.upsertEdge(a, b, 10, { strength: 0.9 }, 2.5);
    assert.ok(eid > 0);
  });
});

describe('batch_upsert_nodes / batch_upsert_edges', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-batch-'));
    db = freshDb(tmpDir, 'batch');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('batchUpsertNodes returns Float64Array of ids', () => {
    const ids = db.batchUpsertNodes([
      { typeId: 1, key: 'n1' },
      { typeId: 1, key: 'n2', props: { x: 1 }, weight: 0.5 },
      { typeId: 2, key: 'n3' },
    ]);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 3);
    // All ids should be positive
    for (const id of ids) assert.ok(id > 0);
  });

  it('batchUpsertEdges returns Float64Array of ids', () => {
    const [a, b, c] = db.batchUpsertNodes([
      { typeId: 1, key: 'ba' },
      { typeId: 1, key: 'bb' },
      { typeId: 1, key: 'bc' },
    ]);
    const eids = db.batchUpsertEdges([
      { from: a, to: b, typeId: 5 },
      { from: b, to: c, typeId: 5, props: { label: 'knows' }, weight: 1.2 },
    ]);
    assert.ok(eids instanceof Float64Array);
    assert.equal(eids.length, 2);
    for (const id of eids) assert.ok(id > 0);
  });
});

describe('batch_upsert_nodes_binary / batch_upsert_edges_binary', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-binbatch-'));
    db = freshDb(tmpDir, 'binbatch');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('batchUpsertNodesBinary returns Float64Array of ids', () => {
    const buf = packNodeBatch([
      { typeId: 1, key: 'bn1' },
      { typeId: 1, key: 'bn2', props: { x: 1 }, weight: 0.5 },
      { typeId: 2, key: 'bn3' },
    ]);
    const ids = db.batchUpsertNodesBinary(buf);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 3);
    for (const id of ids) assert.ok(id > 0);
  });

  it('binary node upsert data matches getNode', () => {
    const buf = packNodeBatch([
      { typeId: 3, key: 'check-me', props: { color: 'red', score: 42 }, weight: 0.8 },
    ]);
    const [id] = db.batchUpsertNodesBinary(buf);
    const n = db.getNode(id);
    assert.ok(n);
    assert.equal(n.typeId, 3);
    assert.equal(n.key, 'check-me');
    assert.equal(n.props.color, 'red');
    assert.equal(n.props.score, 42);
    assert.ok(Math.abs(n.weight - 0.8) < 0.01);
  });

  it('binary node upsert deduplicates by (type_id, key)', () => {
    const buf = packNodeBatch([
      { typeId: 4, key: 'dedup-bin', props: { v: 1 } },
      { typeId: 4, key: 'dedup-bin', props: { v: 2 } },
    ]);
    const ids = db.batchUpsertNodesBinary(buf);
    assert.equal(ids[0], ids[1]);
    const n = db.getNode(ids[0]);
    assert.equal(n.props.v, 2); // last write wins
  });

  it('batchUpsertEdgesBinary returns Float64Array of ids', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([
      { typeId: 1, key: 'be-a' },
      { typeId: 1, key: 'be-b' },
      { typeId: 1, key: 'be-c' },
    ]));
    const buf = packEdgeBatch([
      { from: nodeIds[0], to: nodeIds[1], typeId: 5 },
      { from: nodeIds[1], to: nodeIds[2], typeId: 5, props: { label: 'knows' }, weight: 1.2 },
    ]);
    const eids = db.batchUpsertEdgesBinary(buf);
    assert.ok(eids instanceof Float64Array);
    assert.equal(eids.length, 2);
    for (const id of eids) assert.ok(id > 0);
  });

  it('binary edge upsert data matches getEdge', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([
      { typeId: 1, key: 'edge-src' },
      { typeId: 1, key: 'edge-dst' },
    ]));
    const buf = packEdgeBatch([
      { from: nodeIds[0], to: nodeIds[1], typeId: 7, props: { kind: 'test' }, weight: 2.5 },
    ]);
    const [eid] = db.batchUpsertEdgesBinary(buf);
    const e = db.getEdge(eid);
    assert.ok(e);
    assert.equal(e.from, nodeIds[0]);
    assert.equal(e.to, nodeIds[1]);
    assert.equal(e.typeId, 7);
    assert.equal(e.props.kind, 'test');
    assert.ok(Math.abs(e.weight - 2.5) < 0.01);
  });

  it('binary batch with empty array', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([]));
    assert.equal(nodeIds.length, 0);
    const edgeIds = db.batchUpsertEdgesBinary(packEdgeBatch([]));
    assert.equal(edgeIds.length, 0);
  });

  it('neighbors work with binary-inserted edges', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([
      { typeId: 1, key: 'nbr-hub' },
      { typeId: 1, key: 'nbr-leaf1' },
      { typeId: 1, key: 'nbr-leaf2' },
    ]));
    db.batchUpsertEdgesBinary(packEdgeBatch([
      { from: nodeIds[0], to: nodeIds[1], typeId: 10 },
      { from: nodeIds[0], to: nodeIds[2], typeId: 10 },
    ]));
    const result = db.neighbors(nodeIds[0], 'outgoing');
    assert.equal(result.length, 2);
  });
});

describe('get_node / get_edge', () => {
  let tmpDir, db, nodeId, edgeId;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-get-'));
    db = freshDb(tmpDir, 'get');
    nodeId = db.upsertNode(3, 'getme', { color: 'blue', score: 42 }, 0.7);
    const dst = db.upsertNode(3, 'dst');
    edgeId = db.upsertEdge(nodeId, dst, 8, { rel: 'parent' }, 1.5);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('getNode returns full record', () => {
    const n = db.getNode(nodeId);
    assert.ok(n);
    assert.equal(n.id, nodeId);
    assert.equal(n.typeId, 3);
    assert.equal(n.key, 'getme');
    assert.equal(n.props.color, 'blue');
    assert.equal(n.props.score, 42);
    assert.equal(typeof n.createdAt, 'number');
    assert.equal(typeof n.updatedAt, 'number');
    assert.ok(Math.abs(n.weight - 0.7) < 0.01);
  });

  it('getNode returns null for missing id', () => {
    assert.equal(db.getNode(999999), null);
  });

  it('getEdge returns full record', () => {
    const e = db.getEdge(edgeId);
    assert.ok(e);
    assert.equal(e.id, edgeId);
    assert.equal(e.from, nodeId);
    assert.equal(e.typeId, 8);
    assert.equal(e.props.rel, 'parent');
    assert.ok(Math.abs(e.weight - 1.5) < 0.01);
  });

  it('getEdge returns null for missing id', () => {
    assert.equal(db.getEdge(999999), null);
  });
});

describe('delete_node / delete_edge', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-del-'));
    db = freshDb(tmpDir, 'del');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('deleteNode removes a node', () => {
    const id = db.upsertNode(1, 'todelete');
    assert.ok(db.getNode(id));
    db.deleteNode(id);
    assert.equal(db.getNode(id), null);
  });

  it('deleteEdge removes an edge', () => {
    const a = db.upsertNode(1, 'da');
    const b = db.upsertNode(1, 'db');
    const eid = db.upsertEdge(a, b, 1);
    assert.ok(db.getEdge(eid));
    db.deleteEdge(eid);
    assert.equal(db.getEdge(eid), null);
  });
});

describe('neighbors / neighbors_2hop', () => {
  let tmpDir, db, center, n1, n2, n3;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nbr-'));
    db = freshDb(tmpDir, 'nbr');
    center = db.upsertNode(1, 'center');
    n1 = db.upsertNode(1, 'n1');
    n2 = db.upsertNode(1, 'n2');
    n3 = db.upsertNode(1, 'n3');
    // center -> n1 (type 10), center -> n2 (type 20), n1 -> n3 (type 10)
    db.upsertEdge(center, n1, 10, null, 1.0);
    db.upsertEdge(center, n2, 20, null, 2.0);
    db.upsertEdge(n1, n3, 10, null, 3.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('neighbors returns outgoing neighbors', () => {
    const result = db.neighbors(center, 'outgoing');
    assert.equal(typeof result.length, 'number');
    assert.equal(result.length, 2);
    const nodeSet = new Set([result.nodeId(0), result.nodeId(1)]);
    assert.ok(nodeSet.has(n1));
    assert.ok(nodeSet.has(n2));
  });

  it('neighbors with type filter', () => {
    const result = db.neighbors(center, 'outgoing', [10]);
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), n1);
  });

  it('neighbors with limit', () => {
    const result = db.neighbors(center, 'outgoing', null, 1);
    assert.equal(result.length, 1);
  });

  it('neighbors incoming', () => {
    const result = db.neighbors(n1, 'incoming');
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), center);
  });

  it('neighbors both', () => {
    // n1 has incoming from center and outgoing to n3
    const result = db.neighbors(n1, 'both');
    assert.equal(result.length, 2);
  });

  it('neighbors_2hop returns only 2nd-hop nodes', () => {
    // From center: 1-hop = {n1, n2}, 2nd-hop-only = {n3}
    const result = db.neighbors2Hop(center, 'outgoing');
    const nodeSet = new Set(result.toArray().map(e => e.nodeId));
    // n3 is the only node exactly 2 hops from center
    assert.ok(nodeSet.has(n3));
    // 1-hop nodes are excluded
    assert.ok(!nodeSet.has(n1));
    assert.ok(!nodeSet.has(n2));
  });

  it('neighbors_2hop with type filter', () => {
    const result = db.neighbors2Hop(center, 'outgoing', [10]);
    const nodeSet = new Set(result.toArray().map(e => e.nodeId));
    // Only type-10 edges: center->n1->n3, so n3 is the 2nd-hop result
    assert.ok(nodeSet.has(n3));
    assert.equal(result.length, 1);
  });

  it('neighbor weights are returned', () => {
    const result = db.neighbors(center, 'outgoing');
    assert.equal(result.length, 2);
    // All weights should be numbers > 0
    for (let i = 0; i < result.length; i++) {
      const w = result.weight(i);
      assert.equal(typeof w, 'number');
      assert.ok(w > 0);
    }
  });
});

describe('find_nodes', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-find-'));
    db = freshDb(tmpDir, 'find');
    db.upsertNode(5, 'alice', { city: 'NYC', active: true });
    db.upsertNode(5, 'bob', { city: 'NYC', active: false });
    db.upsertNode(5, 'carol', { city: 'LA', active: true });
    db.upsertNode(6, 'dave', { city: 'NYC' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('findNodes by string property', () => {
    const ids = db.findNodes(5, 'city', 'NYC');
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
  });

  it('findNodes by boolean property', () => {
    const ids = db.findNodes(5, 'active', true);
    assert.equal(ids.length, 2);
  });

  it('findNodes respects type_id', () => {
    // type 6 has one NYC node
    const ids = db.findNodes(6, 'city', 'NYC');
    assert.equal(ids.length, 1);
  });

  it('findNodes returns empty for no match', () => {
    const ids = db.findNodes(5, 'city', 'Chicago');
    assert.equal(ids.length, 0);
  });
});

describe('flush / compact', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-maint-'));
    db = freshDb(tmpDir, 'maint');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('flush does not throw', () => {
    db.upsertNode(1, 'flushed');
    db.flush();
  });

  it('compact returns null when < 2 segments', () => {
    const result = db.compact();
    assert.equal(result, null);
  });

  it('compact returns stats when there are segments to merge', () => {
    // Create enough data to flush twice
    for (let i = 0; i < 50; i++) {
      db.upsertNode(1, `node-${i}`, { idx: i });
    }
    db.flush();
    for (let i = 50; i < 100; i++) {
      db.upsertNode(1, `node-${i}`, { idx: i });
    }
    db.flush();

    const stats = db.compact();
    assert.ok(stats);
    assert.equal(typeof stats.segmentsMerged, 'number');
    assert.ok(stats.segmentsMerged >= 2);
    assert.equal(typeof stats.nodesKept, 'number');
    assert.equal(typeof stats.edgesKept, 'number');
  });
});

describe('edge cases', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-edge-'));
    db = freshDb(tmpDir, 'edge');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('upsertNode with empty props object', () => {
    const id = db.upsertNode(1, 'empty-props', {});
    const n = db.getNode(id);
    assert.ok(n);
    assert.deepEqual(n.props, {});
  });

  it('upsertNode with null/undefined props', () => {
    const id1 = db.upsertNode(1, 'null-props', null);
    const id2 = db.upsertNode(1, 'undef-props', undefined);
    assert.ok(db.getNode(id1));
    assert.ok(db.getNode(id2));
  });

  it('findNodes with integer property value', () => {
    db.upsertNode(9, 'scored', { score: 100 });
    db.upsertNode(9, 'scored2', { score: 200 });
    const ids = db.findNodes(9, 'score', 100);
    assert.equal(ids.length, 1);
  });

  it('batchUpsertNodes with empty array', () => {
    const ids = db.batchUpsertNodes([]);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 0);
  });

  it('batchUpsertEdges with empty array', () => {
    const ids = db.batchUpsertEdges([]);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 0);
  });

  it('neighbors on node with no edges returns empty', () => {
    const id = db.upsertNode(1, 'loner');
    const result = db.neighbors(id, 'outgoing');
    assert.equal(result.length, 0);
  });
});

describe('error handling', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-err-'));
    db = freshDb(tmpDir, 'err');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('throws on invalid direction string', () => {
    const id = db.upsertNode(1, 'x');
    assert.throws(() => db.neighbors(id, 'sideways'), /Invalid direction/);
  });

  it('rejects negative IDs', () => {
    assert.throws(() => db.getNode(-1), /safe non-negative/);
    assert.throws(() => db.deleteNode(-5), /safe non-negative/);
  });

  it('rejects NaN IDs', () => {
    assert.throws(() => db.getNode(NaN), /safe non-negative/);
  });

  it('rejects Infinity IDs', () => {
    assert.throws(() => db.getNode(Infinity), /safe non-negative/);
  });

  it('rejects fractional IDs', () => {
    assert.throws(() => db.getNode(1.5), /safe non-negative/);
  });

  it('rejects IDs beyond MAX_SAFE_INTEGER', () => {
    assert.throws(() => db.getNode(Number.MAX_SAFE_INTEGER + 1), /safe non-negative/);
  });

  it('operations on closed db throw', () => {
    const dbPath = join(tmpDir, 'closed');
    const closedDb = OverGraph.open(dbPath);
    closedDb.close();
    assert.throws(() => closedDb.upsertNode(1, 'fail'), /closed/i);
  });
});

describe('persistence round-trip', () => {
  let tmpDir;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-persist-'));
  });
  after(() => { rmSync(tmpDir, { recursive: true, force: true }); });

  it('data survives close and reopen', () => {
    const dbPath = join(tmpDir, 'roundtrip');

    // Write phase
    const db1 = OverGraph.open(dbPath);
    const nid = db1.upsertNode(1, 'persist-me', { val: 'hello' });
    const dst = db1.upsertNode(1, 'persist-dst');
    const eid = db1.upsertEdge(nid, dst, 5, { kind: 'test' });
    db1.close();

    // Read phase
    const db2 = OverGraph.open(dbPath);
    const n = db2.getNode(nid);
    assert.ok(n);
    assert.equal(n.key, 'persist-me');
    assert.equal(n.props.val, 'hello');

    const e = db2.getEdge(eid);
    assert.ok(e);
    assert.equal(e.typeId, 5);
    assert.equal(e.props.kind, 'test');

    db2.close();
  });
});

describe('nodesByType / edgesByType (ID-only)', () => {
  let tmpDir, db;
  let n1, n2, n3, e1, e2;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bytype-'));
    db = freshDb(tmpDir, 'bytype');
    n1 = db.upsertNode(1, 'a');
    n2 = db.upsertNode(1, 'b');
    n3 = db.upsertNode(2, 'c');
    e1 = db.upsertEdge(n1, n2, 10);
    e2 = db.upsertEdge(n1, n3, 20);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByType returns Float64Array of node IDs', () => {
    const ids = db.nodesByType(1);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
    const idSet = new Set(Array.from(ids));
    assert.ok(idSet.has(n1));
    assert.ok(idSet.has(n2));
  });

  it('nodesByType returns empty for unused type', () => {
    const ids = db.nodesByType(999);
    assert.equal(ids.length, 0);
  });

  it('edgesByType returns Float64Array of edge IDs', () => {
    const ids = db.edgesByType(10);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 1);
    assert.equal(ids[0], e1);
  });

  it('edgesByType returns empty for unused type', () => {
    const ids = db.edgesByType(999);
    assert.equal(ids.length, 0);
  });

  it('nodesByType distinguishes types', () => {
    const type1Ids = db.nodesByType(1);
    const type2Ids = db.nodesByType(2);
    assert.equal(type1Ids.length, 2);
    assert.equal(type2Ids.length, 1);
    assert.equal(type2Ids[0], n3);
  });

  it('edgesByType covers both types', () => {
    const type10Ids = db.edgesByType(10);
    const type20Ids = db.edgesByType(20);
    assert.equal(type10Ids.length, 1);
    assert.equal(type10Ids[0], e1);
    assert.equal(type20Ids.length, 1);
    assert.equal(type20Ids[0], e2);
  });
});

describe('nodesByTypeAsync / edgesByTypeAsync', () => {
  let tmpDir, db;
  let n1, n2, e1;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bytype-async-'));
    db = freshDb(tmpDir, 'bytype-async');
    n1 = db.upsertNode(1, 'a');
    n2 = db.upsertNode(1, 'b');
    e1 = db.upsertEdge(n1, n2, 10);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByTypeAsync returns same IDs as sync', async () => {
    const syncIds = db.nodesByType(1);
    const asyncIds = await db.nodesByTypeAsync(1);
    assert.deepEqual([...asyncIds].sort(), [...syncIds].sort());
  });

  it('edgesByTypeAsync returns same IDs as sync', async () => {
    const syncIds = db.edgesByType(10);
    const asyncIds = await db.edgesByTypeAsync(10);
    assert.deepEqual([...asyncIds].sort(), [...syncIds].sort());
  });
});
