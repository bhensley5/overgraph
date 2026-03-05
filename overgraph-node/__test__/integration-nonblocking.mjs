/**
 * P6-010: Async non-blocking integration test.
 *
 * Verifies that async operations run on the libuv thread pool
 * and do not block the Node.js event loop.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('Async non-blocking verification', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nonblock-'));
    db = OverGraph.open(join(tmpDir, 'nb'));

    // Pre-populate with enough data to make operations take measurable time
    for (let i = 0; i < 500; i++) {
      db.upsertNode(1, `node-${i}`, {
        payload: 'x'.repeat(200),
        idx: i,
        tags: ['a', 'b', 'c'],
      });
    }
    // Create edges
    for (let i = 0; i < 400; i++) {
      const from = db.upsertNode(1, `node-${i}`);
      const to = db.upsertNode(1, `node-${(i + 1) % 500}`);
      db.upsertEdge(from, to, 1, { order: i });
    }
  });

  after(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('event loop stays responsive during async flush', async () => {
    const intervals = [];
    let ticks = 0;

    // Start a rapid setInterval to measure event loop responsiveness
    const interval = setInterval(() => {
      ticks++;
      intervals.push(Date.now());
    }, 5);

    // Run a heavy flush on the thread pool
    await db.flushAsync();

    clearInterval(interval);

    // The event loop should have ticked multiple times during the flush
    assert.ok(
      ticks >= 2,
      `Event loop should tick during flush, got ${ticks} ticks`
    );
  });

  it('event loop stays responsive during async compact', async () => {
    // Insert more data and flush again to create a second segment
    for (let i = 500; i < 800; i++) {
      db.upsertNode(1, `node-${i}`, { payload: 'y'.repeat(200) });
    }
    await db.flushAsync();

    let ticks = 0;
    const interval = setInterval(() => { ticks++; }, 5);

    await db.compactAsync();

    clearInterval(interval);

    assert.ok(
      ticks >= 1,
      `Event loop should tick during compact, got ${ticks} ticks`
    );
  });

  it('concurrent async operations resolve independently', async () => {
    // Fire multiple async operations at once. They should all resolve
    const results = await Promise.all([
      db.getNodeAsync(1),
      db.findNodesAsync(1, 'idx', 0),
      db.neighborsAsync(1, 'outgoing'),
      db.upsertNodeAsync(99, 'concurrent-test', { ts: Date.now() }),
    ]);

    // getNode
    assert.ok(results[0] === null || results[0].id !== undefined);
    // findNodes
    assert.ok(results[1] instanceof Float64Array);
    // neighbors (lazy wrapper)
    assert.equal(typeof results[2].length, 'number');
    // upsertNode
    assert.equal(typeof results[3], 'number');
  });

  it('async and sync can interleave without corruption', async () => {
    // Mix sync writes with async reads
    const id1 = db.upsertNode(50, 'sync-write', { val: 1 });
    const asyncRead = db.getNodeAsync(id1);

    const id2 = db.upsertNode(50, 'sync-write-2', { val: 2 });

    const node = await asyncRead;
    assert.ok(node);
    assert.equal(node.key, 'sync-write');

    // Verify the sync write after async didn't corrupt anything
    const node2 = db.getNode(id2);
    assert.ok(node2);
    assert.equal(node2.key, 'sync-write-2');
  });
});
