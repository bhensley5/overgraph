import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

describe('degree (sync)', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-degree-'));
    db = freshDb(tmpDir, 'deg');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10, undefined, 2.0);
    db.upsertEdge(a, c, 20, undefined, 3.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns outgoing degree', () => {
    assert.equal(db.degree(a), 2);
    assert.equal(db.degree(b), 0);
  });

  it('returns incoming degree', () => {
    assert.equal(db.degree(a, 'incoming'), 0);
    assert.equal(db.degree(b, 'incoming'), 1);
  });

  it('returns both direction degree', () => {
    assert.equal(db.degree(a, 'both'), 2);
    assert.equal(db.degree(b, 'both'), 1);
  });

  it('filters by type', () => {
    assert.equal(db.degree(a, 'outgoing', [10]), 1);
    assert.equal(db.degree(a, 'outgoing', [20]), 1);
    assert.equal(db.degree(a, 'outgoing', [10, 20]), 2);
    assert.equal(db.degree(a, 'outgoing', [99]), 0);
  });

  it('returns 0 for nonexistent node', () => {
    assert.equal(db.degree(999999), 0);
  });
});

describe('sumEdgeWeights (sync)', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-sum-'));
    db = freshDb(tmpDir, 'sum');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10, undefined, 2.0);
    db.upsertEdge(a, c, 10, undefined, 3.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns sum of edge weights', () => {
    assert.ok(Math.abs(db.sumEdgeWeights(a) - 5.0) < 1e-6);
  });

  it('returns 0.0 for zero-degree node', () => {
    assert.equal(db.sumEdgeWeights(999999), 0.0);
  });

  it('filters by type', () => {
    const db2 = freshDb(tmpDir, 'sum-tf');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    const z = db2.upsertNode(1, 'z');
    db2.upsertEdge(x, y, 10, undefined, 2.0);
    db2.upsertEdge(x, z, 20, undefined, 5.0);
    assert.ok(Math.abs(db2.sumEdgeWeights(x, 'outgoing', [10]) - 2.0) < 1e-6);
    assert.ok(Math.abs(db2.sumEdgeWeights(x, 'outgoing', [20]) - 5.0) < 1e-6);
    db2.close();
  });

  it('respects at_epoch', () => {
    const db2 = freshDb(tmpDir, 'sum-ep');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    db2.upsertEdge(x, y, 10, undefined, 3.0, 100, 200);
    assert.ok(Math.abs(db2.sumEdgeWeights(x, 'outgoing', null, 150) - 3.0) < 1e-6);
    assert.equal(db2.sumEdgeWeights(x, 'outgoing', null, 250), 0.0);
    db2.close();
  });
});

describe('avgEdgeWeight (sync)', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-avg-'));
    db = freshDb(tmpDir, 'avg');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10, undefined, 2.0);
    db.upsertEdge(a, c, 10, undefined, 4.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns average edge weight', () => {
    const avg = db.avgEdgeWeight(a);
    assert.ok(avg !== null && avg !== undefined);
    assert.ok(Math.abs(avg - 3.0) < 1e-6);
  });

  it('returns null for zero-degree node', () => {
    assert.equal(db.avgEdgeWeight(999999), null);
  });

  it('filters by type', () => {
    const db2 = freshDb(tmpDir, 'avg-tf');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    const z = db2.upsertNode(1, 'z');
    db2.upsertEdge(x, y, 10, undefined, 2.0);
    db2.upsertEdge(x, z, 20, undefined, 6.0);
    const avg = db2.avgEdgeWeight(x, 'outgoing', [10]);
    assert.ok(avg !== null);
    assert.ok(Math.abs(avg - 2.0) < 1e-6);
    db2.close();
  });

  it('respects at_epoch', () => {
    const db2 = freshDb(tmpDir, 'avg-ep');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    db2.upsertEdge(x, y, 10, undefined, 4.0, 100, 200);
    const avg = db2.avgEdgeWeight(x, 'outgoing', null, 150);
    assert.ok(avg !== null);
    assert.ok(Math.abs(avg - 4.0) < 1e-6);
    assert.equal(db2.avgEdgeWeight(x, 'outgoing', null, 250), null);
    db2.close();
  });
});

describe('degrees batch (sync)', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-degs-'));
    db = freshDb(tmpDir, 'degs');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10);
    db.upsertEdge(a, c, 10);
    db.upsertEdge(b, c, 10);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns batch degree counts', () => {
    const results = db.degrees([a, b, c]);
    assert.ok(Array.isArray(results));
    const degA = results.find(r => r.nodeId === a);
    const degB = results.find(r => r.nodeId === b);
    assert.ok(degA, 'should have entry for a');
    assert.ok(degB, 'should have entry for b');
    assert.equal(degA.degree, 2);
    assert.equal(degB.degree, 1);
  });

  it('returns empty array for empty input', () => {
    const results = db.degrees([]);
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 0);
  });

  it('filters by type', () => {
    const db2 = freshDb(tmpDir, 'degs-tf');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    const z = db2.upsertNode(1, 'z');
    db2.upsertEdge(x, y, 10);
    db2.upsertEdge(x, z, 20);
    const results = db2.degrees([x], 'outgoing', [10]);
    const degX = results.find(r => r.nodeId === x);
    assert.ok(degX);
    assert.equal(degX.degree, 1);
    db2.close();
  });

  it('respects at_epoch', () => {
    const db2 = freshDb(tmpDir, 'degs-ep');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    db2.upsertEdge(x, y, 10, undefined, 1.0, 100, 200);
    const at150 = db2.degrees([x], 'outgoing', null, 150);
    const degAt150 = at150.find(r => r.nodeId === x);
    assert.ok(degAt150);
    assert.equal(degAt150.degree, 1);
    const at250 = db2.degrees([x], 'outgoing', null, 250);
    assert.equal(at250.length, 0);
    db2.close();
  });
});

describe('degree matches neighbors length', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-dparity-'));
    db = freshDb(tmpDir, 'dparity');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10, undefined, 2.0);
    db.upsertEdge(a, c, 20, undefined, 3.0);
    db.upsertEdge(b, c, 10, undefined, 1.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('degree equals neighbors().length for each direction', () => {
    for (const dir of ['outgoing', 'incoming', 'both']) {
      for (const nid of [a, b, c]) {
        const deg = db.degree(nid, dir);
        const nbrs = db.neighbors(nid, dir);
        assert.equal(deg, nbrs.length,
          `mismatch node=${nid} dir=${dir}: degree=${deg} neighbors=${nbrs.length}`);
      }
    }
  });
});

describe('degree async', () => {
  let tmpDir, db;
  let a, b;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-degasync-'));
    db = freshDb(tmpDir, 'degasync');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 10, undefined, 5.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('degreeAsync returns correct count', async () => {
    const deg = await db.degreeAsync(a);
    assert.equal(deg, 1);
  });

  it('sumEdgeWeightsAsync returns correct sum', async () => {
    const sum = await db.sumEdgeWeightsAsync(a);
    assert.ok(Math.abs(sum - 5.0) < 1e-6);
  });

  it('avgEdgeWeightAsync returns correct avg', async () => {
    const avg = await db.avgEdgeWeightAsync(a);
    assert.ok(avg !== null);
    assert.ok(Math.abs(avg - 5.0) < 1e-6);
  });

  it('avgEdgeWeightAsync returns null for zero-degree', async () => {
    const avg = await db.avgEdgeWeightAsync(999999);
    assert.equal(avg, null);
  });

  it('degreesAsync returns batch results', async () => {
    const results = await db.degreesAsync([a, b]);
    assert.ok(Array.isArray(results));
    const degA = results.find(r => r.nodeId === a);
    assert.ok(degA);
    assert.equal(degA.degree, 1);
  });
});

describe('degree temporal', () => {
  let tmpDir, db;
  let a, b;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-degtemporal-'));
    db = freshDb(tmpDir, 'degtemporal');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('ignores expired edge', () => {
    const now = Date.now();
    db.upsertEdge(a, b, 10, undefined, 1.0, now - 2000, now - 1000);
    assert.equal(db.degree(a), 0);
  });

  it('at_epoch selects valid window', () => {
    const c = db.upsertNode(1, 'c');
    db.upsertEdge(a, c, 20, undefined, 1.0, 100, 200);
    assert.equal(db.degree(a, 'outgoing', null, 150), 1);
    assert.equal(db.degree(a, 'outgoing', null, 250), 0);
    assert.equal(db.degree(a, 'outgoing', null, 50), 0);
  });
});
