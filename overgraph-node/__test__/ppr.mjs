import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('personalizedPagerank (sync)', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ppr-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns empty result for empty seeds', () => {
    const r = db.personalizedPagerank([]);
    assert.equal(r.nodeIds.length, 0);
    assert.equal(r.scores.length, 0);
    assert.equal(r.iterations, 0);
    assert.equal(r.converged, true);
  });

  it('single seed no edges returns seed with rank 1.0', () => {
    const id = db.upsertNode(1, 'lonely');
    const r = db.personalizedPagerank([id]);
    assert.equal(r.nodeIds.length, 1);
    assert.equal(r.nodeIds[0], id);
    assert.ok(Math.abs(r.scores[0] - 1.0) < 1e-4);
    assert.equal(r.converged, true);
  });
});

describe('personalizedPagerank, graph queries', () => {
  let tmpDir, db, a, b, c;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ppr-graph-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 1, { weight: 1.0 });
    db.upsertEdge(b, c, 1, { weight: 1.0 });
    db.upsertEdge(c, a, 1, { weight: 1.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('chain produces descending scores from seed', () => {
    const r = db.personalizedPagerank([a], { maxIterations: 100 });
    assert.ok(r.converged);
    const scores = new Map();
    for (let i = 0; i < r.nodeIds.length; i++) scores.set(r.nodeIds[i], r.scores[i]);
    assert.ok(scores.get(a) > scores.get(b), 'seed A should rank higher than B');
    // Total should sum to ~1.0
    const total = r.scores.reduce((s, v) => s + v, 0);
    assert.ok(Math.abs(total - 1.0) < 1e-4, `total rank ${total} should be ~1.0`);
  });

  it('max_results limits output', () => {
    const r = db.personalizedPagerank([a], { maxResults: 2, maxIterations: 100 });
    assert.ok(r.nodeIds.length <= 2);
  });

  it('edge type filter restricts walk', () => {
    // Only type-1 edges exist, filter to type-99 should give no neighbors
    const r = db.personalizedPagerank([a], {
      edgeTypeFilter: [99],
      maxIterations: 100,
    });
    // Only seed should have rank (no edges to walk)
    assert.equal(r.nodeIds.length, 1);
    assert.equal(r.nodeIds[0], a);
  });
});

describe('personalizedPagerank, weighted edges', () => {
  let tmpDir, db, a, b, c;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ppr-weight-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 1, { weight: 1.0 });
    db.upsertEdge(a, c, 1, { weight: 9.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('higher weight edge gives neighbor more rank', () => {
    const r = db.personalizedPagerank([a], { maxIterations: 100 });
    const scores = new Map();
    for (let i = 0; i < r.nodeIds.length; i++) scores.set(r.nodeIds[i], r.scores[i]);
    assert.ok(scores.get(c) > scores.get(b) * 3.0,
      `C (${scores.get(c)}) should rank much higher than B (${scores.get(b)})`);
  });
});

describe('personalizedPagerank across flush', () => {
  let tmpDir, db, a, b, c;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ppr-flush-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 1, { weight: 1.0 });
    db.flush();
    c = db.upsertNode(1, 'c');
    db.upsertEdge(b, c, 1, { weight: 1.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds nodes across memtable and segment', () => {
    const r = db.personalizedPagerank([a], { maxIterations: 100 });
    assert.ok(r.nodeIds.length >= 3, `expected >= 3 nodes, got ${r.nodeIds.length}`);
  });
});

describe('personalizedPagerankAsync', () => {
  let tmpDir, db, a, b;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ppr-async-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 1, { weight: 1.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('async variant returns same shape', async () => {
    const r = await db.personalizedPagerankAsync([a], { maxIterations: 100 });
    assert.ok(r.nodeIds.length >= 2);
    assert.equal(r.scores.length, r.nodeIds.length);
    assert.equal(typeof r.iterations, 'number');
    assert.equal(typeof r.converged, 'boolean');
  });
});
