import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('findNodesByTimeRange (sync)', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-time-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    // Create nodes: type 1 at different times
    db.upsertNode(1, 'node-a');
    db.upsertNode(1, 'node-b');
    db.upsertNode(1, 'node-c');
    db.upsertNode(2, 'node-d'); // different type
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns all nodes in a wide range', () => {
    const ids = db.findNodesByTimeRange(1, 0, Number.MAX_SAFE_INTEGER);
    assert.ok(ids.length >= 3, `expected >= 3 nodes, got ${ids.length}`);
  });

  it('returns empty for non-existent type', () => {
    const ids = db.findNodesByTimeRange(99, 0, Number.MAX_SAFE_INTEGER);
    assert.equal(ids.length, 0);
  });

  it('type filtering works', () => {
    const type1 = db.findNodesByTimeRange(1, 0, Number.MAX_SAFE_INTEGER);
    const type2 = db.findNodesByTimeRange(2, 0, Number.MAX_SAFE_INTEGER);
    assert.ok(type1.length >= 3);
    assert.ok(type2.length >= 1);
  });
});

describe('findNodesByTimeRange across flush', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-time-flush-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    // Batch 1: flush to segment
    db.upsertNode(1, 'seg-a');
    db.upsertNode(1, 'seg-b');
    db.flush();
    // Batch 2: in memtable
    db.upsertNode(1, 'mem-c');
    db.upsertNode(1, 'mem-d');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds nodes across memtable and segments', () => {
    const ids = db.findNodesByTimeRange(1, 0, Number.MAX_SAFE_INTEGER);
    assert.equal(ids.length, 4);
  });
});

describe('findNodesByTimeRange survives compaction', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-time-compact-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 5; i++) db.upsertNode(1, `node-${i}`);
    db.flush();
    for (let i = 5; i < 10; i++) db.upsertNode(1, `node-${i}`);
    db.flush();
    db.compact();
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns correct results after compaction', () => {
    const ids = db.findNodesByTimeRange(1, 0, Number.MAX_SAFE_INTEGER);
    assert.equal(ids.length, 10);
  });
});

describe('findNodesByTimeRange respects tombstones', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-time-tomb-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    const id1 = db.upsertNode(1, 'keep');
    const id2 = db.upsertNode(1, 'delete-me');
    db.flush();
    db.deleteNode(id2);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('excludes deleted nodes', () => {
    const ids = db.findNodesByTimeRange(1, 0, Number.MAX_SAFE_INTEGER);
    assert.equal(ids.length, 1);
  });
});

describe('findNodesByTimeRangePaged (sync)', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-time-paged-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 10; i++) db.upsertNode(1, `node-${i}`);
    db.flush();
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('paginates correctly', () => {
    const allItems = [];
    let cursor = null;
    for (;;) {
      const page = db.findNodesByTimeRangePaged(1, 0, Number.MAX_SAFE_INTEGER, { limit: 3, after: cursor ?? undefined });
      for (let i = 0; i < page.items.length; i++) allItems.push(page.items[i]);
      cursor = page.nextCursor ?? null;
      if (cursor === null || cursor === undefined) break;
    }
    assert.equal(allItems.length, 10);
  });

  it('first page has correct size', () => {
    const page = db.findNodesByTimeRangePaged(1, 0, Number.MAX_SAFE_INTEGER, { limit: 3 });
    assert.equal(page.items.length, 3);
    assert.ok(page.nextCursor !== null && page.nextCursor !== undefined);
  });

  it('unlimited page returns all', () => {
    const page = db.findNodesByTimeRangePaged(1, 0, Number.MAX_SAFE_INTEGER);
    assert.equal(page.items.length, 10);
    assert.ok(page.nextCursor === null || page.nextCursor === undefined);
  });
});

describe('findNodesByTimeRange async', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-time-async-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 5; i++) db.upsertNode(1, `node-${i}`);
    db.flush();
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('findNodesByTimeRangeAsync returns results', async () => {
    const ids = await db.findNodesByTimeRangeAsync(1, 0, Number.MAX_SAFE_INTEGER);
    assert.equal(ids.length, 5);
  });

  it('findNodesByTimeRangePagedAsync paginates', async () => {
    const page = await db.findNodesByTimeRangePagedAsync(1, 0, Number.MAX_SAFE_INTEGER, { limit: 2 });
    assert.equal(page.items.length, 2);
    assert.ok(page.nextCursor !== null && page.nextCursor !== undefined);
  });
});
