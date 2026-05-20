import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

// Helper: collect all pages into a flat array of items
function collectIdPages(db, method, ...args) {
  const allItems = [];
  let cursor = null;
  const pageSize = args.pop(); // last arg is page size
  for (;;) {
    const page = db[method](...args, pageSize, cursor);
    for (let i = 0; i < page.items.length; i++) {
      allItems.push(page.items[i]);
    }
    cursor = page.nextCursor ?? null;
    if (cursor === null || cursor === undefined) break;
  }
  return allItems;
}

describe('nodesByLabelsPaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    // Create 10 nodes of type 1
    for (let i = 0; i < 10; i++) {
      db.upsertNode('Person', `node-${i}`);
    }
    // 3 nodes of type 2
    for (let i = 0; i < 3; i++) {
      db.upsertNode('Company', `other-${i}`);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns first page with correct size', () => {
    const page = db.nodesByLabelsPaged('Person', 3);
    assert.equal(page.items.length, 3);
    assert.ok(page.nextCursor !== null && page.nextCursor !== undefined);
    assert.equal(typeof page.nextCursor, 'number');
  });

  it('round-trip pagination collects all items', () => {
    const all = collectIdPages(db, 'nodesByLabelsPaged', 'Person', 3);
    assert.equal(all.length, 10);
  });

  it('no limit returns all', () => {
    const page = db.nodesByLabelsPaged('Person');
    assert.equal(page.items.length, 10);
    assert.equal(page.nextCursor, undefined);
  });

  it('empty type returns empty page', () => {
    const page = db.nodesByLabelsPaged('UnusedLabel', 5);
    assert.equal(page.items.length, 0);
    assert.equal(page.nextCursor, undefined);
  });
});

describe('edgesByLabelPaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const nodes = [];
    for (let i = 0; i < 6; i++) {
      nodes.push(db.upsertNode('Person', `n${i}`));
    }
    // 5 edges of type 10
    for (let i = 0; i < 5; i++) {
      db.upsertEdge(nodes[i], nodes[i + 1], 'WORKS_AT');
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('round-trip pagination', () => {
    const all = collectIdPages(db, 'edgesByLabelPaged', 'WORKS_AT', 2);
    assert.equal(all.length, 5);
  });
});

describe('getNodesByLabelsPaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 8; i++) {
      db.upsertNode('Person', `node-${i}`, { props: { idx: i } });
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns hydrated records with pagination', () => {
    const page = db.getNodesByLabelsPaged('Person', 3);
    assert.equal(page.items.length, 3);
    // Each item should be a full record
    for (const item of page.items) {
      assert.equal(typeof item.id, 'number');
      assert.equal(typeof item.key, 'string');
      assert.ok(item.props !== undefined);
      assert.ok(item.labels.includes('Person'));
    }
    assert.ok(page.nextCursor != null);
  });

  it('round-trip collects all records', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.getNodesByLabelsPaged('Person', 3, cursor);
      allItems.push(...page.items);
      cursor = page.nextCursor;
      if (cursor == null) break;
    }
    assert.equal(allItems.length, 8);
    // All keys should be unique
    const keys = new Set(allItems.map(n => n.key));
    assert.equal(keys.size, 8);
  });
});

describe('getEdgesByLabelPaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const n1 = db.upsertNode('Person', 'a');
    const n2 = db.upsertNode('Person', 'b');
    for (let i = 0; i < 6; i++) {
      db.upsertEdge(n1, n2, 'WORKS_AT', { weight: 0.5 + i * 0.1 });
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns hydrated edge records with cursor', () => {
    const page = db.getEdgesByLabelPaged('WORKS_AT', 2);
    assert.equal(page.items.length, 2);
    for (const item of page.items) {
      assert.equal(typeof item.id, 'number');
      assert.equal(item.label, 'WORKS_AT');
    }
    assert.ok(page.nextCursor != null);
  });
});

describe('findNodesPaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 7; i++) {
      db.upsertNode('Person', `node-${i}`, { props: { color: 'red' } });
    }
    db.upsertNode('Person', 'blue-node', { props: { color: 'blue' } });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('paginates property-matched nodes', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.findNodesPaged('Person', 'color', 'red', { limit: 3, after: cursor ?? undefined });
      for (let i = 0; i < page.items.length; i++) {
        allItems.push(page.items[i]);
      }
      cursor = page.nextCursor ?? null;
      if (cursor === null || cursor === undefined) break;
    }
    assert.equal(allItems.length, 7);
  });

  it('no matches returns empty', () => {
    const page = db.findNodesPaged('Person', 'color', 'green', { limit: 10 });
    assert.equal(page.items.length, 0);
    assert.equal(page.nextCursor, undefined);
  });
});

describe('neighborsPaged', () => {
  let tmpDir, db, center;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    center = db.upsertNode('Person', 'center');
    for (let i = 0; i < 12; i++) {
      const n = db.upsertNode('Person', `n${i}`);
      db.upsertEdge(center, n, 'WORKS_AT');
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns array page with nextCursor', () => {
    const page = db.neighborsPaged(center, { direction: 'outgoing', limit: 4 });
    assert.equal(page.items.length, 4);
    assert.equal(typeof page.items[0].nodeId, 'number');
    assert.equal(typeof page.items[0].edgeId, 'number');
    assert.equal(typeof page.items[0].label, 'string');
    assert.equal(typeof page.items[0].weight, 'number');
    assert.ok(page.nextCursor != null);
    assert.equal(typeof page.nextCursor, 'number');
  });

  it('round-trip matches unpaginated neighbors', () => {
    const allEdgeIds = [];
    let cursor = null;
    for (;;) {
      const page = db.neighborsPaged(center, { direction: 'outgoing', limit: 4, after: cursor ?? undefined });
      const items = page.items;
      allEdgeIds.push(...items.map(item => item.edgeId));
      cursor = page.nextCursor;
      if (cursor == null) break;
    }
    assert.equal(allEdgeIds.length, 12);

    // Compare with unpaginated
    const unpaged = db.neighbors(center, { direction: 'outgoing' });
    assert.equal(unpaged.length, 12);
  });

  it('no limit returns all', () => {
    const page = db.neighborsPaged(center, { direction: 'outgoing' });
    assert.equal(page.items.length, 12);
    assert.equal(page.nextCursor, null);
  });
});

describe('traverse pagination', () => {
  let tmpDir, db, a;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    // a -> b1, b2, b3; each bi -> ci1, ci2 (6 2-hop targets)
    a = db.upsertNode('Person', 'a');
    for (let i = 0; i < 3; i++) {
      const b = db.upsertNode('Person', `b${i}`);
      db.upsertEdge(a, b, 'WORKS_AT');
      for (let j = 0; j < 2; j++) {
        const c = db.upsertNode('Person', `c${i}_${j}`);
        db.upsertEdge(b, c, 'WORKS_AT');
      }
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('paginates exact-depth traversal results with traversal cursor objects', () => {
    const p1 = db.traverse(a, 2, { minDepth: 2, direction: 'outgoing', limit: 2 });
    assert.equal(p1.items.length, 2);
    assert.ok(p1.nextCursor != null);
    assert.equal(typeof p1.nextCursor.depth, 'number');
    assert.equal(typeof p1.nextCursor.lastNodeId, 'number');

    const p2 = db.traverse(a, 2, { minDepth: 2, direction: 'outgoing', limit: 2, cursor: p1.nextCursor });
    assert.equal(p2.items.length, 2);
    assert.ok(p2.nextCursor != null);

    const p3 = db.traverse(a, 2, { minDepth: 2, direction: 'outgoing', limit: 2, cursor: p2.nextCursor });
    assert.equal(p3.items.length, 2);
    assert.equal(p3.nextCursor, undefined);
  });

  it('round-trip collects all exact-depth traversal hits in deterministic order', () => {
    const allNodeIds = [];
    let cursor = null;
    for (;;) {
      const page = db.traverse(a, 2, { minDepth: 2, direction: 'outgoing', limit: 3, cursor: cursor ?? undefined });
      allNodeIds.push(...page.items.map(hit => hit.nodeId));
      cursor = page.nextCursor ?? null;
      if (cursor === null || cursor === undefined) break;
    }
    assert.equal(allNodeIds.length, 6);
    assert.equal(new Set(allNodeIds.map(id => id.toString())).size, 6);
    assert.deepEqual([...allNodeIds], [...allNodeIds].sort((lhs, rhs) => lhs - rhs));
  });

  it('rejects raw ID cursors for traversal', () => {
    assert.throws(
      () => db.traverse(a, 2, { minDepth: 2, direction: 'outgoing', limit: 2, cursor: 123 }),
    );
  });
});

describe('traverse node-label filtering', () => {
  let tmpDir, db, a;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode('Person', 'a');
    const b = db.upsertNode('Person', 'b');
    db.upsertEdge(a, b, 'WORKS_AT');
    // 2-hop targets: 3 Company nodes, 2 Document nodes
    for (let i = 0; i < 3; i++) {
      const c = db.upsertNode('Company', `c-type2-${i}`);
      db.upsertEdge(b, c, 'WORKS_AT');
    }
    for (let i = 0; i < 2; i++) {
      const d = db.upsertNode('Document', `d-type3-${i}`);
      db.upsertEdge(b, d, 'WORKS_AT');
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('filters by target node label with pagination', () => {
    const allNodeIds = [];
    let cursor = null;
    for (;;) {
      const page = db.traverse(a, 2, {
        minDepth: 2,
        direction: 'outgoing',
        emitNodeLabelFilter: { labels: ['Company'], mode: 'all' },
        limit: 2,
        cursor: cursor ?? undefined,
      });
      allNodeIds.push(...page.items.map(hit => hit.nodeId));
      cursor = page.nextCursor ?? null;
      if (cursor === null || cursor === undefined) break;
    }
    assert.equal(allNodeIds.length, 3);
  });
});

describe('pagination cross-segment', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });
    // Create nodes in two segments
    for (let i = 0; i < 5; i++) {
      db.upsertNode('Person', `seg1-${i}`);
    }
    db.flush();
    for (let i = 0; i < 5; i++) {
      db.upsertNode('Person', `seg2-${i}`);
    }
    // 5 in memtable, 5 in segment
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByLabelsPaged merges across memtable and segment', () => {
    const all = collectIdPages(db, 'nodesByLabelsPaged', 'Person', 3);
    assert.equal(all.length, 10);
  });

  it('getNodesByLabelsPaged merges across sources', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.getNodesByLabelsPaged('Person', 4, cursor);
      allItems.push(...page.items);
      cursor = page.nextCursor ?? null;
      if (cursor === null || cursor === undefined) break;
    }
    assert.equal(allItems.length, 10);
    const keys = new Set(allItems.map(n => n.key));
    assert.equal(keys.size, 10);
  });
});

describe('pagination async variants', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 6; i++) {
      db.upsertNode('Person', `n${i}`, { props: { tag: 'a' } });
    }
    const n0 = db.findNodes('Person', 'tag', 'a')[0];
    const n1 = db.findNodes('Person', 'tag', 'a')[1];
    db.upsertEdge(n0, n1, 'WORKS_AT');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByLabelsPagedAsync returns promise', async () => {
    const page = await db.nodesByLabelsPagedAsync('Person', 2);
    assert.equal(page.items.length, 2);
    assert.ok(page.nextCursor !== undefined);
  });

  it('getNodesByLabelsPagedAsync returns hydrated records', async () => {
    const page = await db.getNodesByLabelsPagedAsync('Person', 3);
    assert.equal(page.items.length, 3);
    for (const item of page.items) {
      assert.equal(typeof item.id, 'number');
      assert.equal(typeof item.key, 'string');
    }
  });

  it('findNodesPagedAsync works', async () => {
    const page = await db.findNodesPagedAsync('Person', 'tag', 'a', { limit: 4 });
    assert.equal(page.items.length, 4);
    assert.ok(page.nextCursor !== undefined);
  });

  it('edgesByLabelPagedAsync works', async () => {
    const page = await db.edgesByLabelPagedAsync('WORKS_AT', 10);
    assert.equal(page.items.length, 1);
    assert.equal(page.nextCursor, undefined);
  });

  it('neighborsPagedAsync returns array result', async () => {
    const page = await db.neighborsPagedAsync(db.findNodes('Person', 'tag', 'a')[0], { direction: 'outgoing', limit: 10 });
    assert.equal(typeof page.items.length, 'number');
    assert.equal(page.nextCursor, null);
  });

  it('traverseAsync paginates exact-depth traversal hits', async () => {
    const page = await db.traverseAsync(db.findNodes('Person', 'tag', 'a')[0], 2, { direction: 'outgoing', limit: 10 });
    assert.equal(typeof page.items.length, 'number');
  });

  it('traverseAsync accepts node label filters and traversal cursor args', async () => {
    const page = await db.traverseAsync(db.findNodes('Person', 'tag', 'a')[0], 2, {
      direction: 'outgoing',
      emitNodeLabelFilter: { labels: ['Person'], mode: 'all' },
      limit: 10,
    });
    assert.equal(typeof page.items.length, 'number');
  });
});

describe('pagination single-item pages', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 4; i++) {
      db.upsertNode('Person', `n${i}`);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('page size 1 yields one item per page', () => {
    const all = collectIdPages(db, 'nodesByLabelsPaged', 'Person', 1);
    assert.equal(all.length, 4);
  });
});

describe('pagination cursor at end', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    db.upsertNode('Person', 'only');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('cursor past all IDs returns empty page', () => {
    // Get the one item, then use its cursor
    const p1 = db.nodesByLabelsPaged('Person', 1);
    assert.equal(p1.items.length, 1);
    // nextCursor should be undefined since there's only one item
    assert.equal(p1.nextCursor, undefined);
  });

  it('large cursor returns empty', () => {
    const page = db.nodesByLabelsPaged('Person', 10, 9999999999999);
    assert.equal(page.items.length, 0);
    assert.equal(page.nextCursor, undefined);
  });
});

describe('pagination with deletions', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const ids = [];
    for (let i = 0; i < 5; i++) {
      ids.push(db.upsertNode('Person', `n${i}`));
    }
    // Delete node at index 2
    db.deleteNode(ids[2]);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('deleted nodes excluded from paginated results', () => {
    const all = collectIdPages(db, 'nodesByLabelsPaged', 'Person', 2);
    assert.equal(all.length, 4); // 5 - 1 deleted
  });
});
