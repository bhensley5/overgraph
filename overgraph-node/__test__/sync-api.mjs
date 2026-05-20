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

function legacyNodeBinaryBuffer() {
  const key = Buffer.from('legacy-node', 'utf8');
  const buf = Buffer.alloc(4 + 4 + 4 + 2 + key.length + 4);
  let off = 0;
  buf.writeUInt32LE(1, off); off += 4;
  buf.writeUInt32LE(0x4e500002, off); off += 4; // was accepted as label "PN" without a magic header
  buf.writeFloatLE(1.0, off); off += 4;
  buf.writeUInt16LE(key.length, off); off += 2;
  key.copy(buf, off); off += key.length;
  buf.writeUInt32LE(0, off);
  return buf;
}

function legacyNodeBinaryV1Buffer() {
  const buf = Buffer.alloc(10);
  let off = 0;
  Buffer.from('OGNB').copy(buf, off); off += 4;
  buf.writeUInt16LE(1, off); off += 2;
  buf.writeUInt32LE(0, off);
  return buf;
}

function legacyEdgeBinaryBuffer(from, to) {
  const buf = Buffer.alloc(4 + 8 + 8 + 4 + 4 + 8 + 8 + 4);
  let off = 0;
  buf.writeUInt32LE(1, off); off += 4;
  buf.writeBigUInt64LE(BigInt(from), off); off += 8;
  buf.writeBigUInt64LE(BigInt(to), off); off += 8;
  buf.writeUInt32LE(0x544c0002, off); off += 4; // was accepted as edge label "LT" without a magic header
  buf.writeFloatLE(1.0, off); off += 4;
  buf.writeBigInt64LE(0n, off); off += 8;
  buf.writeBigInt64LE(0n, off); off += 8;
  buf.writeUInt32LE(0, off);
  return buf;
}

describe('catalog diagnostics', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-catalog-'));
    db = freshDb(tmpDir, 'catalog');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('ensures, gets, and lists node labels and edge labels', () => {
    assert.deepEqual(db.listNodeLabels(), []);
    assert.deepEqual(db.listEdgeLabels(), []);

    const personId = db.ensureNodeLabel('Person');
    const companyId = db.ensureNodeLabel('Company');
    const worksAtId = db.ensureEdgeLabel('WORKS_AT');
    const knowsId = db.ensureEdgeLabel('KNOWS');

    assert.equal(db.ensureNodeLabel('Person'), personId);
    assert.equal(db.ensureEdgeLabel('WORKS_AT'), worksAtId);
    assert.equal(db.getNodeLabelId('Person'), personId);
    assert.equal(db.getNodeLabelId('MissingLabel'), null);
    assert.equal(db.getEdgeLabelId('WORKS_AT'), worksAtId);
    assert.equal(db.getEdgeLabelId('MISSING_EDGE'), null);
    assert.equal(db.getNodeLabel(personId), 'Person');
    assert.equal(db.getNodeLabel(999_999), null);
    assert.equal(db.getEdgeLabel(worksAtId), 'WORKS_AT');
    assert.equal(db.getEdgeLabel(999_999), null);

    assert.deepEqual(
      db.listNodeLabels().sort((a, b) => a.labelId - b.labelId),
      [
        { label: 'Person', labelId: personId },
        { label: 'Company', labelId: companyId },
      ],
    );
    assert.deepEqual(
      db.listEdgeLabels().sort((a, b) => a.labelId - b.labelId),
      [
        { label: 'WORKS_AT', labelId: worksAtId },
        { label: 'KNOWS', labelId: knowsId },
      ],
    );
  });
});

describe('upsert_node / upsert_edge', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-sync-'));
    db = freshDb(tmpDir, 'upsert');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('upsertNode returns a number id', () => {
    const id = db.upsertNode('Person', 'alice');
    assert.equal(typeof id, 'number');
    assert.ok(id > 0);
  });

  it('upsertNode with props and weight', () => {
    const id = db.upsertNode('Person', 'bob', { props: { age: 30, name: 'Bob' }, weight: 0.8 });
    assert.equal(typeof id, 'number');
    assert.ok(id > 0);
  });

  it('upsertNode deduplicates by (label, key)', () => {
    const id1 = db.upsertNode('Company', 'same-key');
    const id2 = db.upsertNode('Company', 'same-key');
    assert.equal(id1, id2);
  });

  it('upsertEdge returns a number id', () => {
    const a = db.upsertNode('Person', 'src');
    const b = db.upsertNode('Person', 'dst');
    const eid = db.upsertEdge(a, b, 'WORKS_AT');
    assert.equal(typeof eid, 'number');
    assert.ok(eid > 0);
  });

  it('upsertEdge with props and weight', () => {
    const a = db.upsertNode('Person', 'e-src');
    const b = db.upsertNode('Person', 'e-dst');
    const eid = db.upsertEdge(a, b, 'WORKS_AT', { props: { strength: 0.9 }, weight: 2.5 });
    assert.ok(eid > 0);
  });
});

describe('node label mutations', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-node-label-mutate-'));
    db = freshDb(tmpDir, 'labels');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('adds and removes node labels with changed flags', () => {
    const id = db.upsertNode(['Person'], 'multi-label');
    assert.equal(db.addNodeLabel(id, 'Admin'), true);
    assert.equal(db.addNodeLabel(id, 'Admin'), false);
    assert.deepEqual([...db.getNode(id).labels].sort(), ['Admin', 'Person']);
    assert.deepEqual(Array.from(db.nodesByLabels(['Person', 'Admin'])), [id]);

    assert.equal(db.removeNodeLabel(id, 'Admin'), true);
    assert.equal(db.removeNodeLabel(id, 'Admin'), false);
    assert.deepEqual(db.getNode(id).labels, ['Person']);
    assert.throws(
      () => db.removeNodeLabel(id, 'Person'),
      /cannot remove the last node label/,
    );
  });
});

describe('multi-label node APIs', () => {
  let tmpDir, db;
  let both, personOnly, adminOnly;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-node-multi-label-'));
    db = freshDb(tmpDir, 'multi-labels');
    both = db.upsertNode(['Person', 'Admin'], 'both');
    personOnly = db.upsertNode('Person', 'person-only');
    adminOnly = db.upsertNode('Admin', 'admin-only');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('upsertNode accepts a label array and hydrates labels', () => {
    assert.deepEqual([...db.getNode(both).labels].sort(), ['Admin', 'Person']);
  });

  it('plural convenience APIs treat arrays as All', () => {
    assert.deepEqual(Array.from(db.nodesByLabels(['Person', 'Admin'])), [both]);
    assert.deepEqual(db.getNodesByLabels(['Person', 'Admin']).map(node => node.id), [both]);
    assert.equal(db.countNodesByLabels(['Person', 'Admin']), 1);
    assert.equal(db.countNodesByLabels('Person'), 2);
    assert.equal(db.countNodesByLabels('Admin'), 2);
  });

  it('paged plural convenience APIs treat arrays as All', () => {
    const idPage = db.nodesByLabelsPaged(['Person', 'Admin'], 10);
    assert.deepEqual(Array.from(idPage.items), [both]);
    assert.equal(idPage.nextCursor ?? null, null);

    const nodePage = db.getNodesByLabelsPaged(['Person', 'Admin'], 10);
    assert.deepEqual(nodePage.items.map(node => node.id), [both]);
    assert.equal(nodePage.nextCursor ?? null, null);
  });

  it('async plural convenience APIs preserve All semantics', async () => {
    assert.deepEqual(Array.from(await db.nodesByLabelsAsync(['Person', 'Admin'])), [both]);
    assert.deepEqual((await db.getNodesByLabelsAsync(['Person', 'Admin'])).map(node => node.id), [both]);
    assert.equal(await db.countNodesByLabelsAsync(['Person', 'Admin']), 1);
    assert.deepEqual(Array.from((await db.nodesByLabelsPagedAsync(['Person', 'Admin'], 10)).items), [both]);
    assert.deepEqual((await db.getNodesByLabelsPagedAsync(['Person', 'Admin'], 10)).items.map(node => node.id), [both]);
  });

  it('single-label queries still include multi-label nodes', () => {
    assert.deepEqual(new Set(Array.from(db.nodesByLabels('Person'))), new Set([both, personOnly]));
    assert.deepEqual(new Set(Array.from(db.nodesByLabels('Admin'))), new Set([both, adminOnly]));
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
      { labels: ['Person'], key: 'n1' },
      { labels: ['Person'], key: 'n2', props: { x: 1 }, weight: 0.5 },
      { labels: ['Company'], key: 'n3' },
    ]);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 3);
    // All ids should be positive
    for (const id of ids) assert.ok(id > 0);
  });

  it('batchUpsertEdges returns Float64Array of ids', () => {
    const [a, b, c] = db.batchUpsertNodes([
      { labels: 'Person', key: 'ba' },
      { labels: 'Person', key: 'bb' },
      { labels: 'Person', key: 'bc' },
    ]);
    const eids = db.batchUpsertEdges([
      { from: a, to: b, label: 'DEPENDS_ON'},
      { from: b, to: c, label: 'DEPENDS_ON', props: { label: 'knows' }, weight: 1.2 },
    ]);
    assert.ok(eids instanceof Float64Array);
    assert.equal(eids.length, 2);
    for (const id of eids) assert.ok(id > 0);
  });
});

describe('vector hydration', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-vector-hydration-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      denseVector: { dimension: 2, metric: 'cosine' },
    });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('hydrates node vectors on full record paths', () => {
    const id = db.upsertNode('VectorDoc', 'full-record-vector', {
      denseVector: [1, 0],
      sparseVector: [{ dimension: 0, value: 2.5 }, { dimension: 3, value: 1.25 }],
    });

    const node = db.getNode(id);
    assert.deepEqual(node.denseVector, [1, 0]);
    assert.deepEqual(node.sparseVector, [
      { dimension: 0, value: 2.5 },
      { dimension: 3, value: 1.25 },
    ]);

    const page = db.queryNodes({
      labelFilter: { labels: ['VectorDoc'], mode: 'all' },
      allowFullScan: true,
    });
    const hydrated = page.items.find((candidate) => candidate.id === id);
    assert.ok(hydrated);
    assert.deepEqual(hydrated.denseVector, [1, 0]);
    assert.deepEqual(hydrated.sparseVector, [
      { dimension: 0, value: 2.5 },
      { dimension: 3, value: 1.25 },
    ]);
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
      { labels: ['Person'], key: 'bn1' },
      { labels: ['Person'], key: 'bn2', props: { x: 1 }, weight: 0.5 },
      { labels: ['Company'], key: 'bn3' },
    ]);
    const ids = db.batchUpsertNodesBinary(buf);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 3);
    for (const id of ids) assert.ok(id > 0);
  });

  it('binary node upsert data matches getNode', () => {
    const buf = packNodeBatch([
      { labels: ['Document'], key: 'check-me', props: { color: 'red', score: 42 }, weight: 0.8 },
    ]);
    const [id] = db.batchUpsertNodesBinary(buf);
    const n = db.getNode(id);
    assert.ok(n);
    assert.deepEqual(n.labels, ['Document']);
    assert.equal(n.key, 'check-me');
    assert.equal(n.props.color, 'red');
    assert.equal(n.props.score, 42);
    assert.ok(Math.abs(n.weight - 0.8) < 0.01);
  });

  it('binary node upsert preserves multiple labels', () => {
    const [id] = db.batchUpsertNodesBinary(packNodeBatch([
      { labels: ['Person', 'Admin'], key: 'multi-bin', props: { role: 'owner' } },
    ]));
    const node = db.getNode(id);
    assert.deepEqual([...node.labels].sort(), ['Admin', 'Person']);
    assert.equal(node.props.role, 'owner');
    assert.deepEqual(Array.from(db.nodesByLabels(['Person', 'Admin'])), [id]);
  });

  it('binary node upsert deduplicates by (label, key)', () => {
    const buf = packNodeBatch([
      { labels: ['Post'], key: 'dedup-bin', props: { v: 1 } },
      { labels: ['Post'], key: 'dedup-bin', props: { v: 2 } },
    ]);
    const ids = db.batchUpsertNodesBinary(buf);
    assert.equal(ids[0], ids[1]);
    const n = db.getNode(ids[0]);
    assert.equal(n.props.v, 2); // last write wins
  });

  it('batchUpsertEdgesBinary returns Float64Array of ids', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([
      { labels: ['Person'], key: 'be-a' },
      { labels: ['Person'], key: 'be-b' },
      { labels: ['Person'], key: 'be-c' },
    ]));
    const buf = packEdgeBatch([
      { from: nodeIds[0], to: nodeIds[1], label: 'DEPENDS_ON'},
      { from: nodeIds[1], to: nodeIds[2], label: 'DEPENDS_ON', props: { label: 'knows' }, weight: 1.2 },
    ]);
    const eids = db.batchUpsertEdgesBinary(buf);
    assert.ok(eids instanceof Float64Array);
    assert.equal(eids.length, 2);
    for (const id of eids) assert.ok(id > 0);
  });

  it('binary edge upsert data matches getEdge', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([
      { labels: ['Person'], key: 'edge-src' },
      { labels: ['Person'], key: 'edge-dst' },
    ]));
    const buf = packEdgeBatch([
      { from: nodeIds[0], to: nodeIds[1], label: 'KNOWS', props: { kind: 'test' }, weight: 2.5 },
    ]);
    const [eid] = db.batchUpsertEdgesBinary(buf);
    const e = db.getEdge(eid);
    assert.ok(e);
    assert.equal(e.from, nodeIds[0]);
    assert.equal(e.to, nodeIds[1]);
    assert.equal(e.label, 'KNOWS');
    assert.equal(e.props.kind, 'test');
    assert.ok(Math.abs(e.weight - 2.5) < 0.01);
  });

  it('binary batch with empty array', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([]));
    assert.equal(nodeIds.length, 0);
    const edgeIds = db.batchUpsertEdgesBinary(packEdgeBatch([]));
    assert.equal(edgeIds.length, 0);
  });

  it('rejects old count-first numeric node and edge binary buffers', () => {
    assert.throws(
      () => db.batchUpsertNodesBinary(legacyNodeBinaryBuffer()),
      /missing magic header/,
    );

    const [from, to] = db.batchUpsertNodes([
      { labels: ['Person'], key: 'legacy-edge-from' },
      { labels: ['Person'], key: 'legacy-edge-to' },
    ]);
    assert.throws(
      () => db.batchUpsertEdgesBinary(legacyEdgeBinaryBuffer(from, to)),
      /missing magic header/,
    );
  });

  it('rejects OGNB v1 single-label binary buffers clearly', () => {
    assert.throws(
      () => db.batchUpsertNodesBinary(legacyNodeBinaryV1Buffer()),
      /OGNB v1 single-label buffers are no longer supported/,
    );
  });

  it('neighbors work with binary-inserted edges', () => {
    const nodeIds = db.batchUpsertNodesBinary(packNodeBatch([
      { labels: ['Person'], key: 'nbr-hub' },
      { labels: ['Person'], key: 'nbr-leaf1' },
      { labels: ['Person'], key: 'nbr-leaf2' },
    ]));
    db.batchUpsertEdgesBinary(packEdgeBatch([
      { from: nodeIds[0], to: nodeIds[1], label: 'WORKS_AT'},
      { from: nodeIds[0], to: nodeIds[2], label: 'WORKS_AT'},
    ]));
    const result = db.neighbors(nodeIds[0], { direction: 'outgoing' });
    assert.equal(result.length, 2);
  });
});

describe('get_node / get_edge', () => {
  let tmpDir, db, nodeId, edgeId;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-get-'));
    db = freshDb(tmpDir, 'get');
    nodeId = db.upsertNode('Document', 'getme', { props: { color: 'blue', score: 42 }, weight: 0.7 });
    const dst = db.upsertNode('Document', 'dst');
    edgeId = db.upsertEdge(nodeId, dst, 'PARENT_OF', { props: { rel: 'parent' }, weight: 1.5 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('getNode returns full record', () => {
    const n = db.getNode(nodeId);
    assert.ok(n);
    assert.equal(n.id, nodeId);
    assert.deepEqual(n.labels, ['Document']);
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
    assert.equal(e.label, 'PARENT_OF');
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
    const id = db.upsertNode('Person', 'todelete');
    assert.ok(db.getNode(id));
    db.deleteNode(id);
    assert.equal(db.getNode(id), null);
  });

  it('deleteEdge removes an edge', () => {
    const a = db.upsertNode('Person', 'da');
    const b = db.upsertNode('Person', 'db');
    const eid = db.upsertEdge(a, b, 'LINKS_TO');
    assert.ok(db.getEdge(eid));
    db.deleteEdge(eid);
    assert.equal(db.getEdge(eid), null);
  });
});

describe('neighbors / traverse', () => {
  let tmpDir, db, center, n1, n2, n3;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nbr-'));
    db = freshDb(tmpDir, 'nbr');
    center = db.upsertNode('Person', 'center');
    n1 = db.upsertNode('Person', 'n1');
    n2 = db.upsertNode('Person', 'n2');
    n3 = db.upsertNode('Person', 'n3');
    // center -> n1 (type 10), center -> n2 (type 20), n1 -> n3 (type 10)
    db.upsertEdge(center, n1, 'WORKS_AT', { weight: 1.0 });
    db.upsertEdge(center, n2, 'MENTIONS', { weight: 2.0 });
    db.upsertEdge(n1, n3, 'WORKS_AT', { weight: 3.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('neighbors returns outgoing neighbors', () => {
    const result = db.neighbors(center); // direction defaults to outgoing
    assert.equal(typeof result.length, 'number');
    assert.equal(result.length, 2);
    const nodeSet = new Set([result[0].nodeId, result[1].nodeId]);
    assert.ok(nodeSet.has(n1));
    assert.ok(nodeSet.has(n2));
  });

  it('neighbors with type filter', () => {
    const result = db.neighbors(center, { direction: 'outgoing', edgeLabelFilter: ['WORKS_AT'] });
    assert.equal(result.length, 1);
    assert.equal(result[0].nodeId, n1);
  });

  it('topKNeighbors returns plain neighbor entry arrays', () => {
    const result = db.topKNeighbors(center, 2, { direction: 'outgoing', scoring: 'weight' });
    assert.ok(Array.isArray(result));
    assert.equal(result.length, 2);
    assert.equal(result[0].nodeId, n2);
    assert.equal(result[0].weight, 2);
    assert.equal(result[1].nodeId, n1);
    assert.equal(result[1].weight, 1);
  });

  it('neighbors with limit', () => {
    const result = db.neighbors(center, { direction: 'outgoing', limit: 1 });
    assert.equal(result.length, 1);
  });

  it('neighbors incoming', () => {
    const result = db.neighbors(n1, { direction: 'incoming' });
    assert.equal(result.length, 1);
    assert.equal(result[0].nodeId, center);
  });

  it('neighbors both', () => {
    // n1 has incoming from center and outgoing to n3
    const result = db.neighbors(n1, { direction: 'both' });
    assert.equal(result.length, 2);
  });

  it('traverse can return only 2nd-hop nodes', () => {
    // From center: 1-hop = {n1, n2}, 2nd-hop-only = {n3}
    const page = db.traverse(center, 2, { minDepth: 2, direction: 'outgoing' });
    const nodeSet = new Set(page.items.map(hit => hit.nodeId));
    assert.ok(nodeSet.has(n3));
    assert.ok(!nodeSet.has(n1));
    assert.ok(!nodeSet.has(n2));
    assert.deepEqual(page.items.map(hit => hit.depth), [2]);
  });

  it('traverse supports edge filters and deterministic hit fields', () => {
    const page = db.traverse(center, 2, { minDepth: 2, direction: 'outgoing', edgeLabelFilter: ['WORKS_AT'] });
    const nodeSet = new Set(page.items.map(hit => hit.nodeId));
    assert.ok(nodeSet.has(n3));
    assert.equal(page.items.length, 1);
    assert.equal(page.items[0].depth, 2);
    assert.equal(typeof page.items[0].viaEdgeId, 'number');
  });

  it('removed neighbors2Hop APIs stay absent', () => {
    assert.equal(typeof db.neighbors2Hop, 'undefined');
    assert.equal(typeof db.neighbors2HopPaged, 'undefined');
    assert.equal(typeof db.neighbors2HopConstrained, 'undefined');
    assert.equal(typeof db.neighbors2HopConstrainedPaged, 'undefined');
  });

  it('neighbor weights are returned', () => {
    const result = db.neighbors(center, { direction: 'outgoing' });
    assert.equal(result.length, 2);
    // All weights should be numbers > 0
    for (const entry of result) {
      const w = entry.weight;
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
    db.upsertNode('User', 'alice', { props: { city: 'NYC', active: true } });
    db.upsertNode('User', 'bob', { props: { city: 'NYC', active: false } });
    db.upsertNode('User', 'carol', { props: { city: 'LA', active: true } });
    db.upsertNode('Location', 'dave', { props: { city: 'NYC' } });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('findNodes by string property', () => {
    const ids = db.findNodes('User', 'city', 'NYC');
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
  });

  it('findNodes by boolean property', () => {
    const ids = db.findNodes('User', 'active', true);
    assert.equal(ids.length, 2);
  });

  it('findNodes respects label', () => {
    // type 6 has one NYC node
    const ids = db.findNodes('Location', 'city', 'NYC');
    assert.equal(ids.length, 1);
  });

  it('findNodes returns empty for no match', () => {
    const ids = db.findNodes('User', 'city', 'Chicago');
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
    db.upsertNode('Person', 'flushed');
    db.flush();
  });

  it('compact returns null when < 2 segments', () => {
    const result = db.compact();
    assert.equal(result, null);
  });

  it('compact returns stats when there are segments to merge', () => {
    const testDir = mkdtempSync(join(tmpdir(), 'overgraph-compact-'));
    const testDb = OverGraph.open(join(testDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });

    try {
      for (let i = 0; i < 50; i++) {
        testDb.upsertNode('Person', `node-${i}`, { props: { idx: i } });
      }
      testDb.flush();
      for (let i = 50; i < 100; i++) {
        testDb.upsertNode('Person', `node-${i}`, { props: { idx: i } });
      }
      testDb.flush();

      assert.ok(testDb.stats().segmentCount >= 2);
      const stats = testDb.compact();
      assert.ok(stats);
      assert.equal(typeof stats.segmentsMerged, 'number');
      assert.ok(stats.segmentsMerged >= 2);
      assert.equal(typeof stats.nodesKept, 'number');
      assert.equal(typeof stats.edgesKept, 'number');
    } finally {
      testDb.close();
      rmSync(testDir, { recursive: true, force: true });
    }
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
    const id = db.upsertNode('Person', 'empty-props', { props: {} });
    const n = db.getNode(id);
    assert.ok(n);
    assert.deepEqual(n.props, {});
  });

  it('upsertNode with null/undefined props', () => {
    const id1 = db.upsertNode('Person', 'null-props');
    const id2 = db.upsertNode('Person', 'undef-props');
    assert.ok(db.getNode(id1));
    assert.ok(db.getNode(id2));
  });

  it('findNodes with integer property value', () => {
    db.upsertNode('ScoredNode', 'scored', { props: { score: 100 } });
    db.upsertNode('ScoredNode', 'scored2', { props: { score: 200 } });
    const ids = db.findNodes('ScoredNode', 'score', 100);
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
    const id = db.upsertNode('Person', 'loner');
    const result = db.neighbors(id, { direction: 'outgoing' });
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
    const id = db.upsertNode('Person', 'x');
    assert.throws(() => db.neighbors(id, { direction: 'sideways' }), /Invalid direction/);
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
    assert.throws(() => closedDb.upsertNode('Person', 'fail'), /closed/i);
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
    const nid = db1.upsertNode('Person', 'persist-me', { props: { val: 'hello' } });
    const dst = db1.upsertNode('Person', 'persist-dst');
    const eid = db1.upsertEdge(nid, dst, 'DEPENDS_ON', { props: { kind: 'test' } });
    db1.close();

    // Read phase
    const db2 = OverGraph.open(dbPath);
    const n = db2.getNode(nid);
    assert.ok(n);
    assert.equal(n.key, 'persist-me');
    assert.equal(n.props.val, 'hello');

    const e = db2.getEdge(eid);
    assert.ok(e);
    assert.equal(e.label, 'DEPENDS_ON');
    assert.equal(e.props.kind, 'test');

    db2.close();
  });
});

describe('nodesByLabels / edgesByLabel (ID-only)', () => {
  let tmpDir, db;
  let n1, n2, n3, e1, e2;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bytype-'));
    db = freshDb(tmpDir, 'bytype');
    n1 = db.upsertNode('Person', 'a');
    n2 = db.upsertNode('Person', 'b');
    n3 = db.upsertNode('Company', 'c');
    e1 = db.upsertEdge(n1, n2, 'WORKS_AT');
    e2 = db.upsertEdge(n1, n3, 'MENTIONS');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByLabels returns Float64Array of node IDs', () => {
    const ids = db.nodesByLabels('Person');
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
    const idSet = new Set(Array.from(ids));
    assert.ok(idSet.has(n1));
    assert.ok(idSet.has(n2));
  });

  it('nodesByLabels returns empty for unused type', () => {
    const ids = db.nodesByLabels('UnusedLabel');
    assert.equal(ids.length, 0);
  });

  it('edgesByLabel returns Float64Array of edge IDs', () => {
    const ids = db.edgesByLabel('WORKS_AT');
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 1);
    assert.equal(ids[0], e1);
  });

  it('edgesByLabel returns empty for unused type', () => {
    const ids = db.edgesByLabel('UNUSED_EDGE_TYPE');
    assert.equal(ids.length, 0);
  });

  it('nodesByLabels distinguishes types', () => {
    const type1Ids = db.nodesByLabels('Person');
    const type2Ids = db.nodesByLabels('Company');
    assert.equal(type1Ids.length, 2);
    assert.equal(type2Ids.length, 1);
    assert.equal(type2Ids[0], n3);
  });

  it('does not expose singular node-label convenience aliases', () => {
    for (const name of [
      'nodesByLabel',
      'getNodesByLabel',
      'countNodesByLabel',
      'nodesByLabelPaged',
      'getNodesByLabelPaged',
      'nodesByLabelAsync',
      'getNodesByLabelAsync',
      'countNodesByLabelAsync',
      'nodesByLabelPagedAsync',
      'getNodesByLabelPagedAsync',
    ]) {
      assert.equal(db[name], undefined, `${name} should not be exported`);
    }
  });

  it('edgesByLabel covers both types', () => {
    const type10Ids = db.edgesByLabel('WORKS_AT');
    const type20Ids = db.edgesByLabel('MENTIONS');
    assert.equal(type10Ids.length, 1);
    assert.equal(type10Ids[0], e1);
    assert.equal(type20Ids.length, 1);
    assert.equal(type20Ids[0], e2);
  });
});

describe('nodesByLabelsAsync / edgesByLabelAsync', () => {
  let tmpDir, db;
  let n1, n2, e1;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bytype-async-'));
    db = freshDb(tmpDir, 'bytype-async');
    n1 = db.upsertNode('Person', 'a');
    n2 = db.upsertNode('Person', 'b');
    e1 = db.upsertEdge(n1, n2, 'WORKS_AT');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByLabelsAsync returns same IDs as sync', async () => {
    const syncIds = db.nodesByLabels('Person');
    const asyncIds = await db.nodesByLabelsAsync('Person');
    assert.deepEqual([...asyncIds].sort(), [...syncIds].sort());
  });

  it('edgesByLabelAsync returns same IDs as sync', async () => {
    const syncIds = db.edgesByLabel('WORKS_AT');
    const asyncIds = await db.edgesByLabelAsync('WORKS_AT');
    assert.deepEqual([...asyncIds].sort(), [...syncIds].sort());
  });
});
