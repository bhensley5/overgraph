import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('exportAdjacency (sync)', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-export-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns empty result for empty db', () => {
    const r = db.exportAdjacency();
    assert.equal(r.nodeIds.length, 0);
    assert.equal(r.edgeFrom.length, 0);
    assert.equal(r.edgeTo.length, 0);
    assert.equal(r.edgeTypeIds.length, 0);
    // Default include_weights=true, so weights typed array is present (but empty)
    assert.ok(r.edgeWeights != null);
    assert.equal(r.edgeWeights.length, 0);
  });
});

describe('exportAdjacency, full graph', () => {
  let tmpDir, db, a, b, c;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-export-graph-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 1, null, 2.0);
    db.upsertEdge(b, c, 1, null, 3.0);
    db.upsertEdge(c, a, 2, null, 1.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('exports all nodes and edges with weights', () => {
    const r = db.exportAdjacency({ includeWeights: true });
    assert.equal(r.nodeIds.length, 3);
    assert.equal(r.edgeFrom.length, 3);
    assert.equal(r.edgeTo.length, 3);
    assert.equal(r.edgeTypeIds.length, 3);
    assert.ok(r.edgeWeights !== null);
    assert.equal(r.edgeWeights.length, 3);
  });

  it('node IDs are sorted', () => {
    const r = db.exportAdjacency();
    for (let i = 1; i < r.nodeIds.length; i++) {
      assert.ok(r.nodeIds[i] > r.nodeIds[i - 1], 'node IDs must be sorted');
    }
  });

  it('edge data is correct', () => {
    const r = db.exportAdjacency({ includeWeights: true });
    // Find the a→b edge
    let found = false;
    for (let i = 0; i < r.edgeFrom.length; i++) {
      if (r.edgeFrom[i] === a && r.edgeTo[i] === b) {
        assert.equal(r.edgeTypeIds[i], 1);
        assert.ok(Math.abs(r.edgeWeights[i] - 2.0) < 1e-6);
        found = true;
      }
    }
    assert.ok(found, 'should find a→b edge');
  });
});

describe('exportAdjacency, filters', () => {
  let tmpDir, db, a, b, c;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-export-filter-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(2, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 1, null, 1.0);
    db.upsertEdge(a, c, 1, null, 1.0);
    db.upsertEdge(a, c, 2, null, 2.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('node type filter excludes nodes and their edges', () => {
    const r = db.exportAdjacency({ nodeTypeFilter: [1], includeWeights: true });
    assert.equal(r.nodeIds.length, 2); // a and c
    // Edge a→b excluded (b is type 2)
    for (let i = 0; i < r.edgeFrom.length; i++) {
      assert.notEqual(r.edgeTo[i], b);
    }
  });

  it('edge type filter restricts edges', () => {
    const r = db.exportAdjacency({ edgeTypeFilter: [2], includeWeights: true });
    assert.equal(r.edgeFrom.length, 1);
    assert.equal(r.edgeTypeIds[0], 2);
  });

  it('includeWeights false gives null weights', () => {
    const r = db.exportAdjacency({ includeWeights: false });
    assert.equal(r.edgeWeights, undefined);
  });
});

describe('exportAdjacency, across flush', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-export-flush-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 1, null, 1.0);
    db.flush();
    const c = db.upsertNode(1, 'c');
    db.upsertEdge(b, c, 1, null, 2.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds nodes and edges across memtable and segment', () => {
    const r = db.exportAdjacency({ includeWeights: true });
    assert.equal(r.nodeIds.length, 3);
    assert.equal(r.edgeFrom.length, 2);
  });
});

describe('exportAdjacencyAsync', () => {
  let tmpDir, db, a, b;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-export-async-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 1, null, 1.0);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('async variant returns same shape', async () => {
    const r = await db.exportAdjacencyAsync({ includeWeights: true });
    assert.equal(r.nodeIds.length, 2);
    assert.equal(r.edgeFrom.length, 1);
    assert.equal(r.edgeTo.length, 1);
    assert.equal(r.edgeTypeIds.length, 1);
    assert.ok(r.edgeWeights !== null);
  });
});
