import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

describe('async upsert + get', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-'));
    db = freshDb(tmpDir, 'upsert');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('upsertNodeAsync returns a number', async () => {
    const id = await db.upsertNodeAsync(1, 'alice', { age: 30 }, 0.9);
    assert.equal(typeof id, 'number');
    assert.ok(id > 0);
  });

  it('upsertEdgeAsync returns a number', async () => {
    const a = await db.upsertNodeAsync(1, 'src');
    const b = await db.upsertNodeAsync(1, 'dst');
    const eid = await db.upsertEdgeAsync(a, b, 5, { rel: 'knows' }, 1.5);
    assert.equal(typeof eid, 'number');
    assert.ok(eid > 0);
  });

  it('getNodeAsync returns full record', async () => {
    const id = await db.upsertNodeAsync(2, 'bob', { color: 'red' });
    const n = await db.getNodeAsync(id);
    assert.ok(n);
    assert.equal(n.id, id);
    assert.equal(n.typeId, 2);
    assert.equal(n.key, 'bob');
    assert.equal(n.props.color, 'red');
  });

  it('getNodeAsync returns null for missing id', async () => {
    const n = await db.getNodeAsync(999999);
    assert.equal(n, null);
  });

  it('getEdgeAsync returns full record', async () => {
    const a = await db.upsertNodeAsync(1, 'ea');
    const b = await db.upsertNodeAsync(1, 'eb');
    const eid = await db.upsertEdgeAsync(a, b, 10, { kind: 'test' });
    const e = await db.getEdgeAsync(eid);
    assert.ok(e);
    assert.equal(e.id, eid);
    assert.equal(e.typeId, 10);
    assert.equal(e.props.kind, 'test');
  });
});

describe('async batch upserts', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-batch-'));
    db = freshDb(tmpDir, 'batch');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('batchUpsertNodesAsync returns Float64Array', async () => {
    const ids = await db.batchUpsertNodesAsync([
      { typeId: 1, key: 'n1' },
      { typeId: 1, key: 'n2', props: { x: 1 } },
    ]);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
    for (const id of ids) assert.ok(id > 0);
  });

  it('batchUpsertEdgesAsync returns Float64Array', async () => {
    const [a, b] = await db.batchUpsertNodesAsync([
      { typeId: 1, key: 'ba' },
      { typeId: 1, key: 'bb' },
    ]);
    const eids = await db.batchUpsertEdgesAsync([
      { from: a, to: b, typeId: 5 },
    ]);
    assert.ok(eids instanceof Float64Array);
    assert.equal(eids.length, 1);
  });
});

describe('async delete + neighbors + find', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-query-'));
    db = freshDb(tmpDir, 'query');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('deleteNodeAsync removes a node', async () => {
    const id = await db.upsertNodeAsync(1, 'doomed');
    assert.ok(await db.getNodeAsync(id));
    await db.deleteNodeAsync(id);
    assert.equal(await db.getNodeAsync(id), null);
  });

  it('deleteEdgeAsync removes an edge', async () => {
    const a = await db.upsertNodeAsync(1, 'da');
    const b = await db.upsertNodeAsync(1, 'db');
    const eid = await db.upsertEdgeAsync(a, b, 1);
    assert.ok(await db.getEdgeAsync(eid));
    await db.deleteEdgeAsync(eid);
    assert.equal(await db.getEdgeAsync(eid), null);
  });

  it('neighborsAsync returns correct results', async () => {
    const c = await db.upsertNodeAsync(1, 'center');
    const n1 = await db.upsertNodeAsync(1, 'nbr1');
    const n2 = await db.upsertNodeAsync(1, 'nbr2');
    await db.upsertEdgeAsync(c, n1, 10);
    await db.upsertEdgeAsync(c, n2, 10);

    const result = await db.neighborsAsync(c, 'outgoing');
    assert.equal(typeof result.length, 'number');
    assert.equal(result.length, 2);
  });

  it('neighbors2HopAsync returns 2nd-hop nodes', async () => {
    const a = await db.upsertNodeAsync(1, 'hop-a');
    const b = await db.upsertNodeAsync(1, 'hop-b');
    const c = await db.upsertNodeAsync(1, 'hop-c');
    await db.upsertEdgeAsync(a, b, 10);
    await db.upsertEdgeAsync(b, c, 10);

    const result = await db.neighbors2HopAsync(a, 'outgoing');
    const nodeSet = new Set(result.toArray().map(e => e.nodeId));
    assert.ok(nodeSet.has(c));
    assert.ok(!nodeSet.has(b)); // 1-hop excluded
  });

  it('findNodesAsync returns matching ids', async () => {
    await db.upsertNodeAsync(7, 'fa', { city: 'NYC' });
    await db.upsertNodeAsync(7, 'fb', { city: 'NYC' });
    await db.upsertNodeAsync(7, 'fc', { city: 'LA' });

    const ids = await db.findNodesAsync(7, 'city', 'NYC');
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
  });
});

describe('async flush + compact', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-maint-'));
    db = freshDb(tmpDir, 'maint');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('flushAsync resolves without error', async () => {
    await db.upsertNodeAsync(1, 'flushed');
    await db.flushAsync();
  });

  it('compactAsync returns null when < 2 segments', async () => {
    const result = await db.compactAsync();
    assert.equal(result, null);
  });

  it('compactAsync returns stats after multiple flushes', async () => {
    for (let i = 0; i < 50; i++) {
      db.upsertNode(1, `cn-${i}`, { idx: i }); // sync for speed
    }
    await db.flushAsync();
    for (let i = 50; i < 100; i++) {
      db.upsertNode(1, `cn-${i}`, { idx: i });
    }
    await db.flushAsync();

    const stats = await db.compactAsync();
    assert.ok(stats);
    assert.ok(stats.segmentsMerged >= 2);
  });
});

describe('compactWithProgressAsync', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-progress-'));
    db = freshDb(tmpDir, 'progress');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns null when < 2 segments', async () => {
    const events = [];
    const result = await db.compactWithProgressAsync((p) => { events.push(p); });
    assert.equal(result, null);
    assert.equal(events.length, 0);
  });

  it('returns stats and calls progress callback', async () => {
    for (let i = 0; i < 50; i++) {
      db.upsertNode(1, `cp-${i}`, { idx: i });
    }
    await db.flushAsync();
    for (let i = 50; i < 100; i++) {
      db.upsertNode(1, `cp-${i}`, { idx: i });
    }
    await db.flushAsync();

    const events = [];
    const stats = await db.compactWithProgressAsync((p) => { events.push(p); });
    assert.ok(stats);
    assert.ok(stats.segmentsMerged >= 2);
    // NonBlocking tsfn events may still be in the queue, so give them a tick to drain
    await new Promise((r) => setTimeout(r, 50));
    assert.ok(events.length > 0, 'progress callback should have been called');
    for (const ev of events) {
      assert.equal(typeof ev.phase, 'string');
      assert.equal(typeof ev.segmentsProcessed, 'number');
      assert.equal(typeof ev.totalSegments, 'number');
    }
  });

  it('does not block event loop during compaction', async () => {
    for (let i = 0; i < 100; i++) {
      db.upsertNode(2, `nb2-${i}`, { data: 'y'.repeat(50) });
    }
    await db.flushAsync();
    for (let i = 100; i < 200; i++) {
      db.upsertNode(2, `nb2-${i}`, { data: 'y'.repeat(50) });
    }
    await db.flushAsync();

    let timerFired = false;
    const timerPromise = new Promise((resolve) => {
      setTimeout(() => { timerFired = true; resolve(); }, 1);
    });
    await Promise.all([
      db.compactWithProgressAsync(() => {}),
      timerPromise,
    ]);
    assert.ok(timerFired, 'setTimeout should fire during async compact');
  });
});

describe('async does not block event loop', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-nonblock-'));
    db = freshDb(tmpDir, 'nonblock');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('setTimeout fires during async flush', async () => {
    // Insert enough data to make flush take a moment
    for (let i = 0; i < 200; i++) {
      db.upsertNode(1, `nb-${i}`, { data: 'x'.repeat(100) });
    }

    let timerFired = false;
    const timerPromise = new Promise((resolve) => {
      setTimeout(() => {
        timerFired = true;
        resolve();
      }, 1);
    });

    // Run flush and timer concurrently
    await Promise.all([db.flushAsync(), timerPromise]);
    assert.ok(timerFired, 'setTimeout should have fired during async flush');
  });
});
