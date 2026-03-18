import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

// =============================================================================
// 1. decay_lambda on neighbors
// =============================================================================

describe('neighbors, decay_lambda', () => {
  let tmpDir, db;
  let center, spoke1, spoke2;
  // Use explicit timestamps: spoke1 "recent" (1 hour ago), spoke2 "old" (100 hours ago)
  const now = Date.now();
  const oneHourMs = 3_600_000;
  const recentFrom = now - oneHourMs;        // 1 hour ago
  const oldFrom = now - 100 * oneHourMs;     // 100 hours ago

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-decay-'));
    db = freshDb(tmpDir, 'decay');

    center = db.upsertNode(1, 'center');
    spoke1 = db.upsertNode(1, 'spoke1');
    spoke2 = db.upsertNode(1, 'spoke2');

    // spoke1: recent edge (1 hour old)
    db.upsertEdge(center, spoke1, 10, { weight: 1.0, validFrom: recentFrom });
    // spoke2: old edge (100 hours old)
    db.upsertEdge(center, spoke2, 10, { weight: 1.0, validFrom: oldFrom });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns both neighbors with decay_lambda applied', () => {
    const result = db.neighbors(center, { direction: 'outgoing', atEpoch: now, decayLambda: 0.01 });
    assert.equal(result.length, 2);
  });

  it('recent edge has higher adjusted weight than old edge', () => {
    const result = db.neighbors(center, { direction: 'outgoing', atEpoch: now, decayLambda: 0.01 });
    const arr = result.toArray();

    const spoke1Entry = arr.find(e => e.nodeId === spoke1);
    const spoke2Entry = arr.find(e => e.nodeId === spoke2);
    assert.ok(spoke1Entry, 'spoke1 should be in results');
    assert.ok(spoke2Entry, 'spoke2 should be in results');

    // Recent edge decays less: weight * exp(-0.01 * 1) vs weight * exp(-0.01 * 100)
    assert.ok(spoke1Entry.weight > 0, 'recent edge should have positive weight');
    assert.ok(
      spoke1Entry.weight > spoke2Entry.weight,
      `recent weight (${spoke1Entry.weight}) should exceed old weight (${spoke2Entry.weight})`,
    );
  });

  it('negative decay_lambda throws an error', () => {
    assert.throws(
      () => db.neighbors(center, { direction: 'outgoing', decayLambda: -0.5 }),
      /decay/i,
    );
  });
});

// =============================================================================
// 2. at_epoch temporal filtering on neighbors (non-batch)
// =============================================================================

describe('neighbors, at_epoch temporal filtering', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-epoch-'));
    db = freshDb(tmpDir, 'epoch');

    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');

    // A->B valid [1000, 5000)
    db.upsertEdge(a, b, 10, { weight: 1.0, validFrom: 1000, validTo: 5000 });
    // A->C valid [3000, 9000)
    db.upsertEdge(a, c, 10, { weight: 1.0, validFrom: 3000, validTo: 9000 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('at_epoch=2000 returns only B', () => {
    const result = db.neighbors(a, { direction: 'outgoing', atEpoch: 2000 });
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), b);
  });

  it('at_epoch=4000 returns both B and C', () => {
    const result = db.neighbors(a, { direction: 'outgoing', atEpoch: 4000 });
    assert.equal(result.length, 2);
    const ids = new Set([result.nodeId(0), result.nodeId(1)]);
    assert.ok(ids.has(b));
    assert.ok(ids.has(c));
  });

  it('at_epoch=6000 returns only C (B expired)', () => {
    const result = db.neighbors(a, { direction: 'outgoing', atEpoch: 6000 });
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), c);
  });

  it('at_epoch=99999 returns neither (both expired)', () => {
    const result = db.neighbors(a, { direction: 'outgoing', atEpoch: 99999 });
    assert.equal(result.length, 0);
  });
});

// =============================================================================
// 3. Prune by max_age_ms
// =============================================================================

describe('prune, maxAgeMs', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-prune-age-'));
    db = freshDb(tmpDir, 'prune-age');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('maxAgeMs prunes nodes older than the threshold', async () => {
    const id1 = db.upsertNode(1, 'age-a', { weight: 1.0 });
    const id2 = db.upsertNode(1, 'age-b', { weight: 1.0 });

    // Wait so nodes are clearly older than the threshold
    await sleep(50);

    const result = db.prune({ maxAgeMs: 10 });
    assert.ok(result.nodesPruned >= 2, `expected >= 2 pruned, got ${result.nodesPruned}`);
    assert.equal(db.getNode(id1), null);
    assert.equal(db.getNode(id2), null);
  });

  it('maxAgeMs=999999999 keeps everything (nothing is that old)', () => {
    const id1 = db.upsertNode(1, 'age-c', { weight: 1.0 });
    const id2 = db.upsertNode(1, 'age-d', { weight: 1.0 });

    const result = db.prune({ maxAgeMs: 999999999 });
    assert.equal(result.nodesPruned, 0);
    assert.ok(db.getNode(id1));
    assert.ok(db.getNode(id2));
  });
});

// =============================================================================
// 4. Prune combined (maxAgeMs AND maxWeight, AND semantics)
// =============================================================================

describe('prune, combined maxAgeMs AND maxWeight', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-prune-combo-'));
    db = freshDb(tmpDir, 'prune-combo');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('only prunes nodes matching BOTH criteria', async () => {
    // Low weight node (matches weight criterion)
    const lowWeight = db.upsertNode(1, 'low-w', { weight: 0.1 });
    // High weight node (does not match weight criterion)
    const highWeight = db.upsertNode(1, 'high-w', { weight: 0.9 });

    // Wait so nodes are old enough
    await sleep(50);

    // maxAgeMs: 10 (everything old enough) + maxWeight: 0.5 (only low weight)
    // AND semantics: both conditions must be true
    const result = db.prune({ maxAgeMs: 10, maxWeight: 0.5 });

    // lowWeight matches both: old enough AND weight <= 0.5
    assert.equal(db.getNode(lowWeight), null, 'low-weight node should be pruned');
    // highWeight: old enough but weight > 0.5, survives
    assert.ok(db.getNode(highWeight), 'high-weight node should survive');
  });

  it('very large maxAgeMs prevents pruning even when weight matches', () => {
    const node = db.upsertNode(1, 'combo-safe', { weight: 0.1 });

    // Weight criterion matches (0.1 <= 0.5), but maxAgeMs is huge so node is too young
    const result = db.prune({ maxAgeMs: 999999999999, maxWeight: 0.5 });
    assert.equal(result.nodesPruned, 0);
    assert.ok(db.getNode(node));
  });
});

// =============================================================================
// 5. Binary batch format errors
// =============================================================================

describe('batchUpsertNodesBinary, format errors', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-binerr-'));
    db = freshDb(tmpDir, 'binerr');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('throws on truncated buffer (less than 4 bytes for count)', () => {
    assert.throws(
      () => db.batchUpsertNodesBinary(Buffer.alloc(2)),
    );
  });

  it('throws on absurd count header', () => {
    assert.throws(
      () => db.batchUpsertNodesBinary(Buffer.from([0xff, 0xff, 0xff, 0x7f])),
    );
  });
});

// =============================================================================
// 6. PPR damping_factor edge cases
// =============================================================================

describe('personalizedPagerank, dampingFactor edge cases', () => {
  let tmpDir, db;
  let a, b, c;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-ppr-damp-'));
    db = freshDb(tmpDir, 'ppr-damp');

    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 1, { weight: 1.0 });
    db.upsertEdge(b, c, 1, { weight: 1.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('very low dampingFactor (0.01): seed dominates hugely', () => {
    const r = db.personalizedPagerank([a], {
      dampingFactor: 0.01,
      maxIterations: 100,
    });
    const scores = new Map();
    for (let i = 0; i < r.nodeIds.length; i++) {
      scores.set(r.nodeIds[i], r.scores[i]);
    }
    const seedScore = scores.get(a) ?? 0;
    const bScore = scores.get(b) ?? 0;
    const cScore = scores.get(c) ?? 0;

    // With damping ~0, almost all rank stays at the seed
    assert.ok(seedScore > 0.9, `seed should dominate with low damping, got ${seedScore}`);
    assert.ok(seedScore > bScore * 10, 'seed should be >> B');
    assert.ok(seedScore > cScore * 10, 'seed should be >> C');
  });

  it('very high dampingFactor (0.99): rank spreads more evenly', () => {
    const r = db.personalizedPagerank([a], {
      dampingFactor: 0.99,
      maxIterations: 200,
    });
    const scores = new Map();
    for (let i = 0; i < r.nodeIds.length; i++) {
      scores.set(r.nodeIds[i], r.scores[i]);
    }
    const seedScore = scores.get(a) ?? 0;
    const bScore = scores.get(b) ?? 0;

    // With damping ~1, rank should spread significantly
    // B should receive a meaningful share of the rank
    assert.ok(bScore > 0.05, `B should get meaningful rank with high damping, got ${bScore}`);
    // The seed's share should be notably smaller than with low damping
    assert.ok(seedScore < 0.9, `seed should not dominate with high damping, got ${seedScore}`);
  });

  it('relative distribution changes with damping factor', () => {
    const rLow = db.personalizedPagerank([a], {
      dampingFactor: 0.01,
      maxIterations: 100,
    });
    const rHigh = db.personalizedPagerank([a], {
      dampingFactor: 0.99,
      maxIterations: 200,
    });

    const lowScores = new Map();
    for (let i = 0; i < rLow.nodeIds.length; i++) {
      lowScores.set(rLow.nodeIds[i], rLow.scores[i]);
    }
    const highScores = new Map();
    for (let i = 0; i < rHigh.nodeIds.length; i++) {
      highScores.set(rHigh.nodeIds[i], rHigh.scores[i]);
    }

    const lowSeedShare = lowScores.get(a) ?? 0;
    const highSeedShare = highScores.get(a) ?? 0;

    // Seed's share should be larger with low damping than with high damping
    assert.ok(
      lowSeedShare > highSeedShare,
      `low-damping seed share (${lowSeedShare}) should exceed high-damping seed share (${highSeedShare})`,
    );
  });
});

// =============================================================================
// 7. Pagination cursor tampering
// =============================================================================

describe('nodesByTypePaged, cursor edge cases', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-cursor-'));
    db = freshDb(tmpDir, 'cursor');
    for (let i = 0; i < 10; i++) {
      db.upsertNode(1, `node-${i}`);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('cursor past all IDs returns empty result', () => {
    const page = db.nodesByTypePaged(1, 3, 999999999);
    assert.equal(page.items.length, 0);
    assert.equal(page.nextCursor, undefined);
  });

  it('cursor=0 works like no cursor (returns first page)', () => {
    const pageNoCursor = db.nodesByTypePaged(1, 3);
    const pageCursor0 = db.nodesByTypePaged(1, 3, 0);

    assert.equal(pageCursor0.items.length, pageNoCursor.items.length);
    // Both should return the same set of IDs
    const noCursorIds = Array.from(pageNoCursor.items).sort();
    const cursor0Ids = Array.from(pageCursor0.items).sort();
    assert.deepEqual(cursor0Ids, noCursorIds);
  });
});

// =============================================================================
// 8. Self-loops
// =============================================================================

describe('self-loops', () => {
  let tmpDir, db;
  let a;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-selfloop-'));
    db = freshDb(tmpDir, 'selfloop');
    a = db.upsertNode(1, 'self');
    db.upsertEdge(a, a, 10, { weight: 1.0 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('outgoing neighbors include self-loop', () => {
    const result = db.neighbors(a, { direction: 'outgoing' });
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), a);
  });

  it('incoming neighbors include self-loop', () => {
    const result = db.neighbors(a, { direction: 'incoming' });
    assert.equal(result.length, 1);
    assert.equal(result.nodeId(0), a);
  });

  it('direction=both does not duplicate self-loop', () => {
    const result = db.neighbors(a, { direction: 'both' });
    // A self-loop should appear exactly once, not twice
    assert.equal(result.length, 1, `expected 1 neighbor for self-loop with 'both', got ${result.length}`);
    assert.equal(result.nodeId(0), a);
  });
});

// =============================================================================
// 9. Concurrent async stress
// =============================================================================

describe('concurrent async stress', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-stress-'));
    db = freshDb(tmpDir, 'stress');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('50 parallel upsertNodeAsync calls all succeed', async () => {
    const promises = [];
    for (let i = 0; i < 50; i++) {
      promises.push(db.upsertNodeAsync(1, `stress-${i}`, { props: { idx: i } }));
    }
    const ids = await Promise.all(promises);

    // All 50 should return valid IDs
    assert.equal(ids.length, 50);
    for (const id of ids) {
      assert.equal(typeof id, 'number');
      assert.ok(id > 0, `expected positive id, got ${id}`);
    }

    // All IDs should be unique
    const uniqueIds = new Set(ids);
    assert.equal(uniqueIds.size, 50, 'all 50 IDs should be unique');

    // Each node should be retrievable
    for (const id of ids) {
      const node = db.getNode(id);
      assert.ok(node, `node ${id} should be retrievable`);
      assert.equal(node.typeId, 1);
    }
  });
});
