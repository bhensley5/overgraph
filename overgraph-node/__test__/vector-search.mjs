import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(dir, name) {
  return OverGraph.open(join(dir, name), {
    denseVector: { dimension: 4, metric: 'cosine' },
  });
}

function setupHybridDb(dir) {
  const db = freshDb(dir, 'hybrid');
  // Node 1: dense rank #1, sparse rank #4
  const n1 = db.upsertNode(1, 'n1', { weight: 1.0,
    denseVector: [0.95, 0.05, 0.05, 0.05], sparseVector: [{ dimension: 0, value: 0.2 }, { dimension: 1, value: 0.1 }] });
  // Node 2: dense rank #4, sparse rank #1
  const n2 = db.upsertNode(1, 'n2', { weight: 1.0,
    denseVector: [0.3, 0.5, 0.5, 0.5], sparseVector: [{ dimension: 0, value: 0.9 }, { dimension: 1, value: 0.8 }, { dimension: 2, value: 0.7 }] });
  // Node 3: dense rank #2, sparse rank #2 (balanced)
  const n3 = db.upsertNode(1, 'n3', { weight: 1.0,
    denseVector: [0.85, 0.1, 0.1, 0.1], sparseVector: [{ dimension: 0, value: 0.7 }, { dimension: 1, value: 0.6 }] });
  // Node 4: dense rank #3, sparse rank #3
  const n4 = db.upsertNode(1, 'n4', { weight: 1.0,
    denseVector: [0.6, 0.3, 0.3, 0.3], sparseVector: [{ dimension: 0, value: 0.5 }, { dimension: 2, value: 0.3 }] });
  // Node 5: dense rank #5, sparse rank #5
  const n5 = db.upsertNode(1, 'n5', { weight: 1.0,
    denseVector: [0.1, 0.4, 0.6, 0.6], sparseVector: [{ dimension: 1, value: 0.1 }] });
  db.flush();
  return { db, ids: [n1, n2, n3, n4, n5] };
}

describe('vectorSearch dense (sync)', () => {
  let tmpDir, db, ids;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'og-vs-'));
    const setup = setupHybridDb(tmpDir);
    db = setup.db;
    ids = setup.ids;
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true }); });

  it('returns dense results sorted by score', () => {
    const hits = db.vectorSearch('dense', { k: 5, denseQuery: [1, 0, 0, 0] });
    assert.equal(hits.length, 5);
    assert.equal(hits[0].nodeId, ids[0]); // highest cosine sim to [1,0,0,0]
    for (let i = 1; i < hits.length; i++) {
      assert.ok(hits[i - 1].score >= hits[i].score, 'results should be sorted desc');
    }
  });

  it('returns sparse results', () => {
    const hits = db.vectorSearch('sparse', { k: 5,
      sparseQuery: [{ dimension: 0, value: 1.0 }, { dimension: 1, value: 0.5 }] });
    assert.equal(hits.length, 5);
    assert.equal(hits[0].nodeId, ids[1]); // highest sparse dot product
  });
});

describe('vectorSearch hybrid (sync)', () => {
  let tmpDir, db, ids;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'og-hyb-'));
    const setup = setupHybridDb(tmpDir);
    db = setup.db;
    ids = setup.ids;
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true }); });

  it('weighted rank fusion promotes balanced node', () => {
    const hits = db.vectorSearch('hybrid', { k: 5,
      denseQuery: [1, 0, 0, 0],
      sparseQuery: [{ dimension: 0, value: 1.0 }, { dimension: 1, value: 0.5 }, { dimension: 2, value: 0.3 }],
      denseWeight: 1.0, sparseWeight: 1.0, fusionMode: 'weighted_rank' });
    assert.equal(hits.length, 5);
    assert.equal(hits[0].nodeId, ids[2]); // balanced node
  });

  it('heavy dense weight promotes dense #1', () => {
    const hits = db.vectorSearch('hybrid', { k: 5,
      denseQuery: [1, 0, 0, 0],
      sparseQuery: [{ dimension: 0, value: 1.0 }, { dimension: 1, value: 0.5 }],
      denseWeight: 5.0, sparseWeight: 1.0, fusionMode: 'weighted_rank' });
    assert.equal(hits[0].nodeId, ids[0]);
  });

  it('reciprocal rank fusion ignores weights', () => {
    const a = db.vectorSearch('hybrid', { k: 5,
      denseQuery: [1, 0, 0, 0],
      sparseQuery: [{ dimension: 0, value: 1.0 }, { dimension: 1, value: 0.5 }],
      fusionMode: 'reciprocal_rank' });
    const b = db.vectorSearch('hybrid', { k: 5,
      denseQuery: [1, 0, 0, 0],
      sparseQuery: [{ dimension: 0, value: 1.0 }, { dimension: 1, value: 0.5 }],
      denseWeight: 99.0, sparseWeight: 0.01, fusionMode: 'reciprocal_rank' });
    assert.equal(a.length, b.length);
    for (let i = 0; i < a.length; i++) {
      assert.equal(a[i].nodeId, b[i].nodeId);
    }
  });

  it('weighted score fusion returns results', () => {
    const hits = db.vectorSearch('hybrid', { k: 5,
      denseQuery: [1, 0, 0, 0],
      sparseQuery: [{ dimension: 0, value: 1.0 }],
      denseWeight: 1.0, sparseWeight: 1.0, fusionMode: 'weighted_score' });
    assert.equal(hits.length, 5);
    for (let i = 1; i < hits.length; i++) {
      assert.ok(hits[i - 1].score >= hits[i].score);
    }
  });

  it('degeneration: hybrid with only dense query matches pure dense', () => {
    const dense = db.vectorSearch('dense', { k: 5, denseQuery: [1, 0, 0, 0] });
    const hybrid = db.vectorSearch('hybrid', { k: 5, denseQuery: [1, 0, 0, 0] });
    assert.equal(dense.length, hybrid.length);
    for (let i = 0; i < dense.length; i++) {
      assert.equal(dense[i].nodeId, hybrid[i].nodeId);
    }
  });

  it('hybrid with no queries errors', () => {
    assert.throws(() => db.vectorSearch('hybrid', { k: 5 }), /requires at least one/);
  });

  it('k=0 returns empty', () => {
    const hits = db.vectorSearch('hybrid', { k: 0, denseQuery: [1, 0, 0, 0], sparseQuery: [{ dimension: 0, value: 1.0 }] });
    assert.equal(hits.length, 0);
  });
});

describe('vectorSearch async', () => {
  let tmpDir, db, ids;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'og-vsa-'));
    const setup = setupHybridDb(tmpDir);
    db = setup.db;
    ids = setup.ids;
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true }); });

  it('dense async matches sync', async () => {
    const sync = db.vectorSearch('dense', { k: 5, denseQuery: [1, 0, 0, 0] });
    const async_ = await db.vectorSearchAsync('dense', { k: 5, denseQuery: [1, 0, 0, 0] });
    assert.equal(sync.length, async_.length);
    for (let i = 0; i < sync.length; i++) {
      assert.equal(sync[i].nodeId, async_[i].nodeId);
    }
  });

  it('hybrid async returns results', async () => {
    const hits = await db.vectorSearchAsync('hybrid', { k: 5,
      denseQuery: [1, 0, 0, 0],
      sparseQuery: [{ dimension: 0, value: 1.0 }] });
    assert.equal(hits.length, 5);
  });
});

describe('vectorSearch with scope (sync)', () => {
  let tmpDir, db, ids;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'og-vsc-'));
    db = freshDb(tmpDir, 'scope');
    ids = [];
    for (let i = 0; i < 4; i++) {
      ids.push(db.upsertNode(1, `n${i}`, { weight: 1.0,
        denseVector: [1, 0, 0, 0], sparseVector: [{ dimension: 0, value: (i + 1) * 0.3 }] }));
    }
    db.upsertEdge(ids[0], ids[1], 1);
    db.upsertEdge(ids[1], ids[2], 1);
    db.flush();
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true }); });

  it('scope limits results to reachable nodes', () => {
    const hits = db.vectorSearch('hybrid', { k: 10,
      denseQuery: [1, 0, 0, 0], sparseQuery: [{ dimension: 0, value: 1.0 }],
      scope: { startNodeId: ids[0], maxDepth: 1, direction: 'outgoing' } });
    const hitIds = hits.map(h => h.nodeId);
    assert.ok(hitIds.includes(ids[0]));
    assert.ok(hitIds.includes(ids[1]));
    assert.ok(!hitIds.includes(ids[2])); // depth 2
    assert.ok(!hitIds.includes(ids[3])); // disconnected
  });
});
