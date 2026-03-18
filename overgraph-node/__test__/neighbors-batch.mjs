import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

describe('neighborsBatch (sync)', () => {
  let tmpDir, db;
  let n1, n2, n3, n4;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nbatch-'));
    db = freshDb(tmpDir, 'nbatch');
    // Build a small graph: n1->n2 (type 10), n1->n3 (type 20), n2->n4 (type 10)
    n1 = db.upsertNode(1, 'n1');
    n2 = db.upsertNode(1, 'n2');
    n3 = db.upsertNode(1, 'n3');
    n4 = db.upsertNode(1, 'n4');
    db.upsertEdge(n1, n2, 10);
    db.upsertEdge(n1, n3, 20);
    db.upsertEdge(n2, n4, 10);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns batch results for multiple nodes', () => {
    const results = db.neighborsBatch([n1, n2]);
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 2);

    // Results are sorted by queryNodeId
    const r1 = results.find(r => r.queryNodeId === n1);
    const r2 = results.find(r => r.queryNodeId === n2);
    assert.ok(r1, 'should have entry for n1');
    assert.ok(r2, 'should have entry for n2');

    // n1 has 2 outgoing neighbors (n2, n3)
    assert.equal(r1.neighbors.length, 2);
    // n2 has 1 outgoing neighbor (n4)
    assert.equal(r2.neighbors.length, 1);
  });

  it('respects direction parameter', () => {
    const results = db.neighborsBatch([n2], { direction: 'incoming' });
    assert.equal(results.length, 1);
    assert.equal(results[0].queryNodeId, n2);
    // n2 has 1 incoming neighbor (n1)
    assert.equal(results[0].neighbors.length, 1);
    assert.equal(results[0].neighbors.nodeId(0), n1);
  });

  it('respects type filter', () => {
    const results = db.neighborsBatch([n1], { direction: 'outgoing', typeFilter: [10] });
    assert.equal(results.length, 1);
    // n1 has 1 outgoing edge of type 10 (to n2)
    assert.equal(results[0].neighbors.length, 1);
    assert.equal(results[0].neighbors.nodeId(0), n2);
  });

  it('handles empty input', () => {
    const results = db.neighborsBatch([]);
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 0);
  });

  it('handles node with no neighbors', () => {
    const lonely = db.upsertNode(1, 'lonely');
    const results = db.neighborsBatch([lonely]);
    // Engine filters empty entries: nodes with no neighbors are omitted
    assert.equal(results.length, 0);
  });

  it('matches individual neighbors calls', () => {
    const batchResults = db.neighborsBatch([n1, n2], { direction: 'outgoing' });
    const n1Individual = db.neighbors(n1, { direction: 'outgoing' });
    const n2Individual = db.neighbors(n2, { direction: 'outgoing' });

    const batchN1 = batchResults.find(r => r.queryNodeId === n1);
    const batchN2 = batchResults.find(r => r.queryNodeId === n2);

    // Compare node IDs (as sorted arrays)
    const batchN1Ids = batchN1.neighbors.toArray().map(e => e.nodeId).sort();
    const indivN1Ids = n1Individual.toArray().map(e => e.nodeId).sort();
    assert.deepEqual(batchN1Ids, indivN1Ids);

    const batchN2Ids = batchN2.neighbors.toArray().map(e => e.nodeId).sort();
    const indivN2Ids = n2Individual.toArray().map(e => e.nodeId).sort();
    assert.deepEqual(batchN2Ids, indivN2Ids);
  });
});

describe('neighborsBatch, temporal (at_epoch)', () => {
  let tmpDir, db;
  let n1, n2;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nbatch-temp-'));
    db = freshDb(tmpDir, 'nbatch-temp');
    n1 = db.upsertNode(1, 'a');
    n2 = db.upsertNode(1, 'b');
    // Edge valid from 1000 to 2000
    db.upsertEdge(n1, n2, 10, { weight: 1.0, validFrom: 1000, validTo: 2000 });
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns edge within valid window', () => {
    const results = db.neighborsBatch([n1], { direction: 'outgoing', atEpoch: 1500 });
    assert.equal(results.length, 1);
    assert.equal(results[0].neighbors.length, 1);
  });

  it('excludes edge outside valid window', () => {
    const results = db.neighborsBatch([n1], { direction: 'outgoing', atEpoch: 3000 });
    assert.ok(results.length === 0 || results[0].neighbors.length === 0);
  });
});

describe('neighborsBatchAsync', () => {
  let tmpDir, db;
  let n1, n2, n3;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-nbatch-async-'));
    db = freshDb(tmpDir, 'nbatch-async');
    n1 = db.upsertNode(1, 'x1');
    n2 = db.upsertNode(1, 'x2');
    n3 = db.upsertNode(1, 'x3');
    db.upsertEdge(n1, n2, 10);
    db.upsertEdge(n1, n3, 10);
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns same results as sync version', async () => {
    const syncResults = db.neighborsBatch([n1]);
    const asyncResults = await db.neighborsBatchAsync([n1]);

    assert.equal(asyncResults.length, syncResults.length);
    assert.equal(
      asyncResults[0].neighbors.length,
      syncResults[0].neighbors.length,
    );
  });
});
