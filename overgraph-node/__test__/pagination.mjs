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

describe('nodesByTypePaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    // Create 10 nodes of type 1
    for (let i = 0; i < 10; i++) {
      db.upsertNode(1, `node-${i}`);
    }
    // 3 nodes of type 2
    for (let i = 0; i < 3; i++) {
      db.upsertNode(2, `other-${i}`);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns first page with correct size', () => {
    const page = db.nodesByTypePaged(1, 3);
    assert.equal(page.items.length, 3);
    assert.ok(page.nextCursor !== null && page.nextCursor !== undefined);
    assert.equal(typeof page.nextCursor, 'number');
  });

  it('round-trip pagination collects all items', () => {
    const all = collectIdPages(db, 'nodesByTypePaged', 1, 3);
    assert.equal(all.length, 10);
  });

  it('no limit returns all', () => {
    const page = db.nodesByTypePaged(1);
    assert.equal(page.items.length, 10);
    assert.equal(page.nextCursor, undefined);
  });

  it('empty type returns empty page', () => {
    const page = db.nodesByTypePaged(999, 5);
    assert.equal(page.items.length, 0);
    assert.equal(page.nextCursor, undefined);
  });
});

describe('edgesByTypePaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const nodes = [];
    for (let i = 0; i < 6; i++) {
      nodes.push(db.upsertNode(1, `n${i}`));
    }
    // 5 edges of type 10
    for (let i = 0; i < 5; i++) {
      db.upsertEdge(nodes[i], nodes[i + 1], 10);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('round-trip pagination', () => {
    const all = collectIdPages(db, 'edgesByTypePaged', 10, 2);
    assert.equal(all.length, 5);
  });
});

describe('getNodesByTypePaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 8; i++) {
      db.upsertNode(1, `node-${i}`, { props: { idx: i } });
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns hydrated records with pagination', () => {
    const page = db.getNodesByTypePaged(1, 3);
    assert.equal(page.items.length, 3);
    // Each item should be a full record
    for (const item of page.items) {
      assert.equal(typeof item.id, 'number');
      assert.equal(typeof item.key, 'string');
      assert.ok(item.props !== undefined);
      assert.equal(item.typeId, 1);
    }
    assert.ok(page.nextCursor != null);
  });

  it('round-trip collects all records', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.getNodesByTypePaged(1, 3, cursor);
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

describe('getEdgesByTypePaged', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const n1 = db.upsertNode(1, 'a');
    const n2 = db.upsertNode(1, 'b');
    for (let i = 0; i < 6; i++) {
      db.upsertEdge(n1, n2, 10, { weight: 0.5 + i * 0.1 });
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns hydrated edge records with cursor', () => {
    const page = db.getEdgesByTypePaged(10, 2);
    assert.equal(page.items.length, 2);
    for (const item of page.items) {
      assert.equal(typeof item.id, 'number');
      assert.equal(item.typeId, 10);
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
      db.upsertNode(1, `node-${i}`, { props: { color: 'red' } });
    }
    db.upsertNode(1, 'blue-node', { props: { color: 'blue' } });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('paginates property-matched nodes', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.findNodesPaged(1, 'color', 'red', { limit: 3, after: cursor ?? undefined });
      for (let i = 0; i < page.items.length; i++) {
        allItems.push(page.items[i]);
      }
      cursor = page.nextCursor ?? null;
      if (cursor === null || cursor === undefined) break;
    }
    assert.equal(allItems.length, 7);
  });

  it('no matches returns empty', () => {
    const page = db.findNodesPaged(1, 'color', 'green', { limit: 10 });
    assert.equal(page.items.length, 0);
    assert.equal(page.nextCursor, undefined);
  });
});

describe('neighborsPaged', () => {
  let tmpDir, db, center;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    center = db.upsertNode(1, 'center');
    for (let i = 0; i < 12; i++) {
      const n = db.upsertNode(1, `n${i}`);
      db.upsertEdge(center, n, 10);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns lazy page with nextCursor', () => {
    const page = db.neighborsPaged(center, { direction: 'outgoing', limit: 4 });
    assert.equal(page.items.length, 4);
    assert.equal(typeof page.items.nodeId(0), 'number');
    assert.equal(typeof page.items.edgeId(0), 'number');
    assert.equal(typeof page.items.edgeTypeId(0), 'number');
    assert.equal(typeof page.items.weight(0), 'number');
    assert.ok(page.nextCursor != null);
    assert.equal(typeof page.nextCursor, 'number');
  });

  it('round-trip matches unpaginated neighbors', () => {
    const allEdgeIds = [];
    let cursor = null;
    for (;;) {
      const page = db.neighborsPaged(center, { direction: 'outgoing', limit: 4, after: cursor ?? undefined });
      const items = page.items;
      for (let i = 0; i < items.length; i++) {
        allEdgeIds.push(items.edgeId(i));
      }
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
    a = db.upsertNode(1, 'a');
    for (let i = 0; i < 3; i++) {
      const b = db.upsertNode(1, `b${i}`);
      db.upsertEdge(a, b, 10);
      for (let j = 0; j < 2; j++) {
        const c = db.upsertNode(1, `c${i}_${j}`);
        db.upsertEdge(b, c, 10);
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

describe('traverse node-type filtering', () => {
  let tmpDir, db, a;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 10);
    // 2-hop targets: 3 of type 2, 2 of type 3
    for (let i = 0; i < 3; i++) {
      const c = db.upsertNode(2, `c-type2-${i}`);
      db.upsertEdge(b, c, 10);
    }
    for (let i = 0; i < 2; i++) {
      const d = db.upsertNode(3, `d-type3-${i}`);
      db.upsertEdge(b, d, 10);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('filters by target node type with pagination', () => {
    const allNodeIds = [];
    let cursor = null;
    for (;;) {
      const page = db.traverse(a, 2, {
        minDepth: 2, direction: 'outgoing', nodeTypeFilter: [2], limit: 2, cursor: cursor ?? undefined
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
      db.upsertNode(1, `seg1-${i}`);
    }
    db.flush();
    for (let i = 0; i < 5; i++) {
      db.upsertNode(1, `seg2-${i}`);
    }
    // 5 in memtable, 5 in segment
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByTypePaged merges across memtable and segment', () => {
    const all = collectIdPages(db, 'nodesByTypePaged', 1, 3);
    assert.equal(all.length, 10);
  });

  it('getNodesByTypePaged merges across sources', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.getNodesByTypePaged(1, 4, cursor);
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
      db.upsertNode(1, `n${i}`, { props: { tag: 'a' } });
    }
    const n0 = db.findNodes(1, 'tag', 'a')[0];
    const n1 = db.findNodes(1, 'tag', 'a')[1];
    db.upsertEdge(n0, n1, 10);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('nodesByTypePagedAsync returns promise', async () => {
    const page = await db.nodesByTypePagedAsync(1, 2);
    assert.equal(page.items.length, 2);
    assert.ok(page.nextCursor !== undefined);
  });

  it('getNodesByTypePagedAsync returns hydrated records', async () => {
    const page = await db.getNodesByTypePagedAsync(1, 3);
    assert.equal(page.items.length, 3);
    for (const item of page.items) {
      assert.equal(typeof item.id, 'number');
      assert.equal(typeof item.key, 'string');
    }
  });

  it('findNodesPagedAsync works', async () => {
    const page = await db.findNodesPagedAsync(1, 'tag', 'a', { limit: 4 });
    assert.equal(page.items.length, 4);
    assert.ok(page.nextCursor !== undefined);
  });

  it('edgesByTypePagedAsync works', async () => {
    const page = await db.edgesByTypePagedAsync(10, 10);
    assert.equal(page.items.length, 1);
    assert.equal(page.nextCursor, undefined);
  });

  it('neighborsPagedAsync returns lazy result', async () => {
    const page = await db.neighborsPagedAsync(db.findNodes(1, 'tag', 'a')[0], { direction: 'outgoing', limit: 10 });
    assert.equal(typeof page.items.length, 'number');
    assert.equal(page.nextCursor, null);
  });

  it('traverseAsync paginates exact-depth traversal hits', async () => {
    const page = await db.traverseAsync(db.findNodes(1, 'tag', 'a')[0], 2, { direction: 'outgoing', limit: 10 });
    assert.equal(typeof page.items.length, 'number');
  });

  it('traverseAsync accepts node type filters and traversal cursor args', async () => {
    const page = await db.traverseAsync(db.findNodes(1, 'tag', 'a')[0], 2, {
      direction: 'outgoing', nodeTypeFilter: [1], limit: 10
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
      db.upsertNode(1, `n${i}`);
    }
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('page size 1 yields one item per page', () => {
    const all = collectIdPages(db, 'nodesByTypePaged', 1, 1);
    assert.equal(all.length, 4);
  });
});

describe('pagination cursor at end', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-page-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    db.upsertNode(1, 'only');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('cursor past all IDs returns empty page', () => {
    // Get the one item, then use its cursor
    const p1 = db.nodesByTypePaged(1, 1);
    assert.equal(p1.items.length, 1);
    // nextCursor should be undefined since there's only one item
    assert.equal(p1.nextCursor, undefined);
  });

  it('large cursor returns empty', () => {
    const page = db.nodesByTypePaged(1, 10, 9999999999999);
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
      ids.push(db.upsertNode(1, `n${i}`));
    }
    // Delete node at index 2
    db.deleteNode(ids[2]);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('deleted nodes excluded from paginated results', () => {
    const all = collectIdPages(db, 'nodesByTypePaged', 1, 2);
    assert.equal(all.length, 4); // 5 - 1 deleted
  });
});
