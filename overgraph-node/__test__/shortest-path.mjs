import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

// Helper: build a linear chain  n0 -> n1 -> n2 -> ... -> n(len-1)
function buildChain(db, len, edgeType = 10) {
  const nodes = [];
  for (let i = 0; i < len; i++) {
    nodes.push(db.upsertNode(1, `n${i}`));
  }
  for (let i = 0; i < len - 1; i++) {
    db.upsertEdge(nodes[i], nodes[i + 1], edgeType);
  }
  return nodes;
}

// Helper: build a diamond  A -> B -> D, A -> C -> D
function buildDiamond(db) {
  const a = db.upsertNode(1, 'a');
  const b = db.upsertNode(1, 'b');
  const c = db.upsertNode(1, 'c');
  const d = db.upsertNode(1, 'd');
  db.upsertEdge(a, b, 10);
  db.upsertEdge(a, c, 10);
  db.upsertEdge(b, d, 10);
  db.upsertEdge(c, d, 10);
  return { a, b, c, d };
}

describe('shortestPath (sync)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-sp-'));
    db = freshDb(tmpDir, 'sp');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds direct neighbor path', () => {
    const [a, b] = buildChain(db, 2);
    const result = db.shortestPath(a, b);
    assert.ok(result);
    assert.deepEqual(result.nodes, [a, b]);
    assert.equal(result.edges.length, 1);
    assert.equal(result.totalCost, 1.0);
  });

  it('finds multi-hop path', () => {
    const db2 = freshDb(tmpDir, 'sp-multi');
    const nodes = buildChain(db2, 5);
    const result = db2.shortestPath(nodes[0], nodes[4]);
    assert.ok(result);
    assert.deepEqual(result.nodes, nodes);
    assert.equal(result.edges.length, 4);
    assert.equal(result.totalCost, 4.0);
    db2.close();
  });

  it('returns null for disconnected nodes', () => {
    const db2 = freshDb(tmpDir, 'sp-disc');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const result = db2.shortestPath(a, b);
    assert.equal(result, null);
    db2.close();
  });

  it('returns self-path for from == to', () => {
    const db2 = freshDb(tmpDir, 'sp-self');
    const a = db2.upsertNode(1, 'a');
    const result = db2.shortestPath(a, a);
    assert.ok(result);
    assert.deepEqual(result.nodes, [a]);
    assert.deepEqual(result.edges, []);
    assert.equal(result.totalCost, 0.0);
    db2.close();
  });

  it('respects direction (incoming)', () => {
    const db2 = freshDb(tmpDir, 'sp-dir');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    db2.upsertEdge(a, b, 10); // a -> b
    // outgoing from b: no path to a
    assert.equal(db2.shortestPath(b, a), null);
    // incoming from perspective of a: follow incoming edges = reverse, so b->a should work
    // Actually: direction=incoming means follow incoming edges from "from" node
    // b has incoming from a, so shortestPath(b, a, 'incoming') should find b<-a
    const result = db2.shortestPath(b, a, { direction: 'incoming' });
    assert.ok(result);
    assert.deepEqual(result.nodes, [b, a]);
    db2.close();
  });

  it('respects direction (both)', () => {
    const db2 = freshDb(tmpDir, 'sp-both');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    db2.upsertEdge(a, b, 10);
    // With 'both', b can reach a
    const result = db2.shortestPath(b, a, { direction: 'both' });
    assert.ok(result);
    assert.deepEqual(result.nodes, [b, a]);
    db2.close();
  });

  it('filters by edge type', () => {
    const db2 = freshDb(tmpDir, 'sp-tf');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const c = db2.upsertNode(1, 'c');
    db2.upsertEdge(a, b, 10);
    db2.upsertEdge(b, c, 20);
    // Only type 10 edges: can reach b but not c
    const result = db2.shortestPath(a, c, { typeFilter: [10] });
    assert.equal(result, null);
    // Both types: can reach c
    const result2 = db2.shortestPath(a, c, { typeFilter: [10, 20] });
    assert.ok(result2);
    db2.close();
  });

  it('respects maxDepth', () => {
    const db2 = freshDb(tmpDir, 'sp-depth');
    const nodes = buildChain(db2, 5);
    // maxDepth=2 can't reach node at distance 4
    const result = db2.shortestPath(nodes[0], nodes[4], { maxDepth: 2 });
    assert.equal(result, null);
    // maxDepth=4 can reach it
    const result2 = db2.shortestPath(nodes[0], nodes[4], { maxDepth: 4 });
    assert.ok(result2);
    db2.close();
  });

  it('returns null for nonexistent nodes', () => {
    const result = db.shortestPath(999998, 999999);
    assert.equal(result, null);
  });
});

describe('shortestPath weighted (sync)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-spw-'));
    db = freshDb(tmpDir, 'spw');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds weighted shortest path via weightField', () => {
    // A -> B (weight 1), A -> C (weight 10), B -> C (weight 1)
    // Shortest by weight: A->B->C (cost 2) not A->C (cost 10)
    const a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    const c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10, { weight: 1.0 });
    db.upsertEdge(a, c, 10, { weight: 10.0 });
    db.upsertEdge(b, c, 10, { weight: 1.0 });
    const result = db.shortestPath(a, c, { weightField: 'weight' });
    assert.ok(result);
    assert.deepEqual(result.nodes, [a, b, c]);
    assert.ok(Math.abs(result.totalCost - 2.0) < 1e-6);
  });

  it('respects maxCost', () => {
    const db2 = freshDb(tmpDir, 'spw-maxc');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const c = db2.upsertNode(1, 'c');
    db2.upsertEdge(a, b, 10, { weight: 5.0 });
    db2.upsertEdge(b, c, 10, { weight: 5.0 });
    // maxCost=8: can't afford A->B->C (cost 10)
    const result = db2.shortestPath(a, c, { weightField: 'weight', maxCost: 8.0 });
    assert.equal(result, null);
    // maxCost=10: can afford it
    const result2 = db2.shortestPath(a, c, { weightField: 'weight', maxCost: 10.0 });
    assert.ok(result2);
    db2.close();
  });

  it('uses the best constrained weighted path under maxDepth', () => {
    const db2 = freshDb(tmpDir, 'spw-depth');
    const s = db2.upsertNode(1, 's');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const c = db2.upsertNode(1, 'c');
    const t = db2.upsertNode(1, 't');
    db2.upsertEdge(s, a, 10, { weight: 1.0 });
    db2.upsertEdge(a, b, 10, { weight: 1.0 });
    db2.upsertEdge(b, t, 10, { weight: 1.0 });
    db2.upsertEdge(s, c, 10, { weight: 3.0 });
    db2.upsertEdge(c, t, 10, { weight: 3.0 });

    const result = db2.shortestPath(s, t, {
      weightField: 'weight',
      maxDepth: 2,
    });
    assert.ok(result);
    assert.deepEqual(result.nodes, [s, c, t]);
    assert.ok(Math.abs(result.totalCost - 6.0) < 1e-6);
    db2.close();
  });
});

describe('isConnected (sync)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ic-'));
    db = freshDb(tmpDir, 'ic');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns true for connected nodes', () => {
    const [a, , c] = buildChain(db, 3);
    assert.equal(db.isConnected(a, c), true);
  });

  it('returns false for disconnected nodes', () => {
    const db2 = freshDb(tmpDir, 'ic-disc');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    assert.equal(db2.isConnected(a, b), false);
    db2.close();
  });

  it('returns true for self', () => {
    const db2 = freshDb(tmpDir, 'ic-self');
    const a = db2.upsertNode(1, 'a');
    assert.equal(db2.isConnected(a, a), true);
    db2.close();
  });

  it('respects direction', () => {
    const db2 = freshDb(tmpDir, 'ic-dir');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    db2.upsertEdge(a, b, 10);
    assert.equal(db2.isConnected(b, a, { direction: 'outgoing' }), false);
    assert.equal(db2.isConnected(b, a, { direction: 'incoming' }), true);
    assert.equal(db2.isConnected(b, a, { direction: 'both' }), true);
    db2.close();
  });

  it('respects maxDepth', () => {
    const db2 = freshDb(tmpDir, 'ic-depth');
    const nodes = buildChain(db2, 5);
    assert.equal(db2.isConnected(nodes[0], nodes[4], { maxDepth: 2 }), false);
    assert.equal(db2.isConnected(nodes[0], nodes[4], { maxDepth: 4 }), true);
    db2.close();
  });

  it('filters by type', () => {
    const db2 = freshDb(tmpDir, 'ic-tf');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const c = db2.upsertNode(1, 'c');
    db2.upsertEdge(a, b, 10);
    db2.upsertEdge(b, c, 20);
    assert.equal(db2.isConnected(a, c, { typeFilter: [10] }), false);
    assert.equal(db2.isConnected(a, c, { typeFilter: [10, 20] }), true);
    db2.close();
  });
});

describe('allShortestPaths (sync)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-asp-'));
    db = freshDb(tmpDir, 'asp');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds multiple equal-cost paths (diamond)', () => {
    const { a, d } = buildDiamond(db);
    const paths = db.allShortestPaths(a, d);
    assert.equal(paths.length, 2);
    for (const p of paths) {
      assert.equal(p.nodes[0], a);
      assert.equal(p.nodes[p.nodes.length - 1], d);
      assert.equal(p.totalCost, 2.0);
      assert.equal(p.edges.length, 2);
    }
  });

  it('returns empty array for disconnected', () => {
    const db2 = freshDb(tmpDir, 'asp-disc');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const paths = db2.allShortestPaths(a, b);
    assert.ok(Array.isArray(paths));
    assert.equal(paths.length, 0);
    db2.close();
  });

  it('respects maxPaths', () => {
    const db2 = freshDb(tmpDir, 'asp-max');
    const { a, d } = buildDiamond(db2);
    const paths = db2.allShortestPaths(a, d, { maxPaths: 1 });
    assert.equal(paths.length, 1);
    db2.close();
  });

  it('works with weighted paths', () => {
    const db2 = freshDb(tmpDir, 'asp-w');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const c = db2.upsertNode(1, 'c');
    const d = db2.upsertNode(1, 'd');
    db2.upsertEdge(a, b, 10, { weight: 1.0 });
    db2.upsertEdge(a, c, 10, { weight: 1.0 });
    db2.upsertEdge(b, d, 10, { weight: 1.0 });
    db2.upsertEdge(c, d, 10, { weight: 1.0 });
    const paths = db2.allShortestPaths(a, d, { weightField: 'weight' });
    assert.equal(paths.length, 2);
    for (const p of paths) {
      assert.ok(Math.abs(p.totalCost - 2.0) < 1e-6);
    }
    db2.close();
  });

  it('uses the best constrained weighted cost under maxDepth', () => {
    const db2 = freshDb(tmpDir, 'asp-w-depth');
    const s = db2.upsertNode(1, 's');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const c = db2.upsertNode(1, 'c');
    const t = db2.upsertNode(1, 't');
    db2.upsertEdge(s, a, 10, { weight: 1.0 });
    db2.upsertEdge(a, b, 10, { weight: 1.0 });
    db2.upsertEdge(b, t, 10, { weight: 1.0 });
    db2.upsertEdge(s, c, 10, { weight: 3.0 });
    db2.upsertEdge(c, t, 10, { weight: 3.0 });

    const paths = db2.allShortestPaths(s, t, {
      weightField: 'weight',
      maxDepth: 2,
    });
    assert.equal(paths.length, 1);
    assert.deepEqual(paths[0].nodes, [s, c, t]);
    assert.ok(Math.abs(paths[0].totalCost - 6.0) < 1e-6);
    db2.close();
  });

  it('returns single path for from == to', () => {
    const db2 = freshDb(tmpDir, 'asp-self');
    const a = db2.upsertNode(1, 'a');
    const paths = db2.allShortestPaths(a, a);
    assert.equal(paths.length, 1);
    assert.deepEqual(paths[0].nodes, [a]);
    assert.equal(paths[0].totalCost, 0.0);
    db2.close();
  });
});

describe('shortest path async', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-spa-'));
    db = freshDb(tmpDir, 'spa');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('shortestPathAsync returns correct path', async () => {
    const [a, b, c] = buildChain(db, 3);
    const result = await db.shortestPathAsync(a, c);
    assert.ok(result);
    assert.deepEqual(result.nodes, [a, b, c]);
    assert.equal(result.totalCost, 2.0);
  });

  it('shortestPathAsync returns null for disconnected', async () => {
    const db2 = freshDb(tmpDir, 'spa-disc');
    const a = db2.upsertNode(1, 'a');
    const b = db2.upsertNode(1, 'b');
    const result = await db2.shortestPathAsync(a, b);
    assert.equal(result, null);
    db2.close();
  });

  it('isConnectedAsync returns correct bool', async () => {
    const [a, , c] = buildChain(db, 3);
    assert.equal(await db.isConnectedAsync(a, c), true);
    const db2 = freshDb(tmpDir, 'spa-ic');
    const x = db2.upsertNode(1, 'x');
    const y = db2.upsertNode(1, 'y');
    assert.equal(await db2.isConnectedAsync(x, y), false);
    db2.close();
  });

  it('allShortestPathsAsync returns multiple paths', async () => {
    const db2 = freshDb(tmpDir, 'spa-asp');
    const { a, d } = buildDiamond(db2);
    const paths = await db2.allShortestPathsAsync(a, d);
    assert.equal(paths.length, 2);
    db2.close();
  });
});

describe('shortest path temporal', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-spt-'));
    db = freshDb(tmpDir, 'spt');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('atEpoch excludes expired edges', () => {
    const a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    const c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10, { weight: 1.0, validFrom: 100, validTo: 200 });
    db.upsertEdge(b, c, 10, { weight: 1.0, validFrom: 100, validTo: 200 });
    // At epoch 150: edges valid
    const r1 = db.shortestPath(a, c, { atEpoch: 150 });
    assert.ok(r1);
    // At epoch 250: edges expired
    const r2 = db.shortestPath(a, c, { atEpoch: 250 });
    assert.equal(r2, null);
  });
});
