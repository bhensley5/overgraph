import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

describe('async catalog diagnostics', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-catalog-'));
    db = freshDb(tmpDir, 'catalog');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('ensures, gets, and lists node labels and edge labels', async () => {
    assert.deepEqual(await db.listNodeLabelsAsync(), []);
    assert.deepEqual(await db.listEdgeLabelsAsync(), []);

    const personId = await db.ensureNodeLabelAsync('Person');
    const companyId = await db.ensureNodeLabelAsync('Company');
    const worksAtId = await db.ensureEdgeLabelAsync('WORKS_AT');
    const knowsId = await db.ensureEdgeLabelAsync('KNOWS');

    assert.equal(await db.ensureNodeLabelAsync('Person'), personId);
    assert.equal(await db.ensureEdgeLabelAsync('WORKS_AT'), worksAtId);
    assert.equal(await db.getNodeLabelIdAsync('Person'), personId);
    assert.equal(await db.getNodeLabelIdAsync('MissingLabel'), null);
    assert.equal(await db.getEdgeLabelIdAsync('WORKS_AT'), worksAtId);
    assert.equal(await db.getEdgeLabelIdAsync('MISSING_EDGE'), null);
    assert.equal(await db.getNodeLabelAsync(personId), 'Person');
    assert.equal(await db.getNodeLabelAsync(999_999), null);
    assert.equal(await db.getEdgeLabelAsync(worksAtId), 'WORKS_AT');
    assert.equal(await db.getEdgeLabelAsync(999_999), null);

    assert.deepEqual(
      (await db.listNodeLabelsAsync()).sort((a, b) => a.labelId - b.labelId),
      [
        { label: 'Person', labelId: personId },
        { label: 'Company', labelId: companyId },
      ],
    );
    assert.deepEqual(
      (await db.listEdgeLabelsAsync()).sort((a, b) => a.labelId - b.labelId),
      [
        { label: 'WORKS_AT', labelId: worksAtId },
        { label: 'KNOWS', labelId: knowsId },
      ],
    );
  });
});

describe('async upsert + get', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-'));
    db = freshDb(tmpDir, 'upsert');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('upsertNodeAsync returns a number', async () => {
    const id = await db.upsertNodeAsync('Person', 'alice', { props: { age: 30 }, weight: 0.9 });
    assert.equal(typeof id, 'number');
    assert.ok(id > 0);
  });

  it('upsertEdgeAsync returns a number', async () => {
    const a = await db.upsertNodeAsync('Person', 'src');
    const b = await db.upsertNodeAsync('Person', 'dst');
    const eid = await db.upsertEdgeAsync(a, b, 'DEPENDS_ON', { props: { rel: 'knows' }, weight: 1.5 });
    assert.equal(typeof eid, 'number');
    assert.ok(eid > 0);
  });

  it('getNodeAsync returns full record', async () => {
    const id = await db.upsertNodeAsync('Company', 'bob', { props: { color: 'red' } });
    const n = await db.getNodeAsync(id);
    assert.ok(n);
    assert.equal(n.id, id);
    assert.deepEqual(n.labels, ['Company']);
    assert.equal(n.key, 'bob');
    assert.equal(n.props.color, 'red');
  });

  it('getNodeAsync returns null for missing id', async () => {
    const n = await db.getNodeAsync(999999);
    assert.equal(n, null);
  });

  it('getEdgeAsync returns full record', async () => {
    const a = await db.upsertNodeAsync('Person', 'ea');
    const b = await db.upsertNodeAsync('Person', 'eb');
    const eid = await db.upsertEdgeAsync(a, b, 'WORKS_AT', { props: { kind: 'test' } });
    const e = await db.getEdgeAsync(eid);
    assert.ok(e);
    assert.equal(e.id, eid);
    assert.equal(e.label, 'WORKS_AT');
    assert.equal(e.props.kind, 'test');
  });

  it('addNodeLabelAsync and removeNodeLabelAsync return changed flags', async () => {
    const id = await db.upsertNodeAsync(['Person'], 'label-mutation');
    assert.equal(await db.addNodeLabelAsync(id, 'Admin'), true);
    assert.equal(await db.addNodeLabelAsync(id, 'Admin'), false);
    const withLabel = await db.getNodeAsync(id);
    assert.deepEqual([...withLabel.labels].sort(), ['Admin', 'Person']);
    assert.equal(await db.removeNodeLabelAsync(id, 'Admin'), true);
    assert.equal(await db.removeNodeLabelAsync(id, 'Admin'), false);
    assert.deepEqual((await db.getNodeAsync(id)).labels, ['Person']);
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
      { labels: ['Person'], key: 'n1' },
      { labels: ['Person'], key: 'n2', props: { x: 1 } },
    ]);
    assert.ok(ids instanceof Float64Array);
    assert.equal(ids.length, 2);
    for (const id of ids) assert.ok(id > 0);
  });

  it('batchUpsertEdgesAsync returns Float64Array', async () => {
    const [a, b] = await db.batchUpsertNodesAsync([
      { labels: ['Person'], key: 'ba' },
      { labels: ['Person'], key: 'bb' },
    ]);
    const eids = await db.batchUpsertEdgesAsync([
      { from: a, to: b, label: 'DEPENDS_ON'},
    ]);
    assert.ok(eids instanceof Float64Array);
    assert.equal(eids.length, 1);
  });
});

describe('async write transactions', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-txn-'));
    db = freshDb(tmpDir, 'txn');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('stages, reads, and commits asynchronously', async () => {
    const txn = await db.beginWriteTxnAsync();
    await txn.stageAsync([
      { op: 'upsertNode', alias: 'alice', labels: ['Person'], key: 'alice', props: { name: 'Alice' } },
      { op: 'upsertNode', alias: 'bob', labels: ['Person'], key: 'bob' },
      { op: 'upsertEdge', alias: 'knows', from: { local: 'alice' }, to: { local: 'bob' }, label: 'KNOWS'},
    ]);

    const staged = await txn.getNodeAsync({ local: 'alice' });
    assert.ok(staged);
    assert.equal(staged.id, undefined);
    assert.deepEqual(staged.labels, ['Person']);
    assert.equal(staged.props.name, 'Alice');

    const result = await txn.commitAsync();
    assert.equal(result.nodeAliases.alice, result.nodeIds[0]);
    assert.equal(result.nodeAliases.bob, result.nodeIds[1]);
    assert.equal(result.edgeAliases.knows, result.edgeIds[0]);
    assert.ok(await db.getNodeAsync(result.nodeAliases.alice));
  });

  it('supports async builders and rollback', async () => {
    const txn = db.beginWriteTxn();
    const alice = await txn.upsertNodeAsAsync('async-alice', 'Person', 'async-alice', {
      props: { mood: 'staged' },
    });
    const bob = await txn.upsertNodeAsAsync('async-bob', 'Person', 'async-bob');
    await txn.upsertEdgeAsAsync('async-knows', alice, bob, 'FOLLOWS');

    const staged = await txn.getNodeByKeyAsync('Person', 'async-alice');
    assert.ok(staged);
    assert.equal(staged.props.mood, 'staged');

    await txn.rollbackAsync();
    assert.equal(await db.getNodeByKeyAsync('Person', 'async-alice'), null);
  });

  it('preserves async transaction call order when promises are started together', async () => {
    const txn = await db.beginWriteTxnAsync();
    const stage = txn.stageAsync([
      { op: 'upsertNode', alias: 'queued', labels: ['Person'], key: 'queued' },
    ]);
    const read = txn.getNodeAsync({ local: 'queued' });
    const commit = txn.commitAsync();

    await stage;
    const staged = await read;
    const result = await commit;

    assert.ok(staged);
    assert.equal(staged.local, 'queued');
    assert.equal(result.nodeAliases.queued, result.nodeIds[0]);
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
    const id = await db.upsertNodeAsync('Person', 'doomed');
    assert.ok(await db.getNodeAsync(id));
    await db.deleteNodeAsync(id);
    assert.equal(await db.getNodeAsync(id), null);
  });

  it('deleteEdgeAsync removes an edge', async () => {
    const a = await db.upsertNodeAsync('Person', 'da');
    const b = await db.upsertNodeAsync('Person', 'db');
    const eid = await db.upsertEdgeAsync(a, b, 'LINKS_TO');
    assert.ok(await db.getEdgeAsync(eid));
    await db.deleteEdgeAsync(eid);
    assert.equal(await db.getEdgeAsync(eid), null);
  });

  it('neighborsAsync returns correct results', async () => {
    const c = await db.upsertNodeAsync('Person', 'center');
    const n1 = await db.upsertNodeAsync('Person', 'nbr1');
    const n2 = await db.upsertNodeAsync('Person', 'nbr2');
    await db.upsertEdgeAsync(c, n1, 'WORKS_AT', { weight: 1 });
    await db.upsertEdgeAsync(c, n2, 'WORKS_AT', { weight: 2 });

    const result = await db.neighborsAsync(c, { direction: 'outgoing' });
    assert.ok(Array.isArray(result));
    assert.equal(typeof result.length, 'number');
    assert.equal(result.length, 2);
  });

  it('topKNeighborsAsync returns plain neighbor entry arrays', async () => {
    const c = await db.upsertNodeAsync('Person', 'topk-center');
    const n1 = await db.upsertNodeAsync('Person', 'topk-nbr1');
    const n2 = await db.upsertNodeAsync('Person', 'topk-nbr2');
    await db.upsertEdgeAsync(c, n1, 'WORKS_AT', { weight: 1 });
    await db.upsertEdgeAsync(c, n2, 'WORKS_AT', { weight: 2 });

    const result = await db.topKNeighborsAsync(c, 2, { direction: 'outgoing', scoring: 'weight' });
    assert.ok(Array.isArray(result));
    assert.equal(result.length, 2);
    assert.equal(result[0].nodeId, n2);
    assert.equal(result[0].weight, 2);
    assert.equal(result[1].nodeId, n1);
    assert.equal(result[1].weight, 1);
  });

  it('traverseAsync returns 2nd-hop nodes', async () => {
    const a = await db.upsertNodeAsync('Person', 'hop-a');
    const b = await db.upsertNodeAsync('Person', 'hop-b');
    const c = await db.upsertNodeAsync('Person', 'hop-c');
    await db.upsertEdgeAsync(a, b, 'WORKS_AT');
    await db.upsertEdgeAsync(b, c, 'WORKS_AT');

    const page = await db.traverseAsync(a, 2, { minDepth: 2, direction: 'outgoing' });
    const nodeSet = new Set(page.items.map(hit => hit.nodeId));
    assert.ok(nodeSet.has(c));
    assert.ok(!nodeSet.has(b)); // 1-hop excluded
    assert.deepEqual(page.items.map(hit => hit.depth), [2]);
  });

  it('findNodesAsync returns matching ids', async () => {
    await db.upsertNodeAsync('CityResident', 'fa', { props: { city: 'NYC' } });
    await db.upsertNodeAsync('CityResident', 'fb', { props: { city: 'NYC' } });
    await db.upsertNodeAsync('CityResident', 'fc', { props: { city: 'LA' } });

    const ids = await db.findNodesAsync('CityResident', 'city', 'NYC');
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
    await db.upsertNodeAsync('Person', 'flushed');
    await db.flushAsync();
  });

  it('compactAsync returns null when < 2 segments', async () => {
    const result = await db.compactAsync();
    assert.equal(result, null);
  });

  it('compactAsync returns stats after multiple flushes', async () => {
    const testDir = mkdtempSync(join(tmpdir(), 'overgraph-compact-async-'));
    const testDb = OverGraph.open(join(testDir, 'db'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });

    try {
      for (let i = 0; i < 50; i++) {
        testDb.upsertNode('Person', `cn-${i}`, { props: { idx: i } });
      }
      await testDb.flushAsync();
      for (let i = 50; i < 100; i++) {
        testDb.upsertNode('Person', `cn-${i}`, { props: { idx: i } });
      }
      await testDb.flushAsync();

      assert.ok(testDb.stats().segmentCount >= 2);
      const stats = await testDb.compactAsync();
      assert.ok(stats);
      assert.ok(stats.segmentsMerged >= 2);
    } finally {
      await testDb.closeAsync();
      rmSync(testDir, { recursive: true, force: true });
    }
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
      db.upsertNode('Person', `cp-${i}`, { props: { idx: i } });
    }
    await db.flushAsync();
    for (let i = 50; i < 100; i++) {
      db.upsertNode('Person', `cp-${i}`, { props: { idx: i } });
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
      db.upsertNode('Company', `nb2-${i}`, { props: { data: 'y'.repeat(50) } });
    }
    await db.flushAsync();
    for (let i = 100; i < 200; i++) {
      db.upsertNode('Company', `nb2-${i}`, { props: { data: 'y'.repeat(50) } });
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
      db.upsertNode('Person', `nb-${i}`, { props: { data: 'x'.repeat(100) } });
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

describe('removed async 2-hop APIs', () => {
  let tmpDir, db;
  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-async-removed-'));
    db = freshDb(tmpDir, 'removed');
  });
  after(async () => { await db.closeAsync(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('keeps neighbors2Hop async wrappers absent', () => {
    assert.equal(typeof db.neighbors2HopAsync, 'undefined');
    assert.equal(typeof db.neighbors2HopPagedAsync, 'undefined');
    assert.equal(typeof db.neighbors2HopConstrainedAsync, 'undefined');
    assert.equal(typeof db.neighbors2HopConstrainedPagedAsync, 'undefined');
  });
});
