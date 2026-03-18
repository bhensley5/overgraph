import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function freshDb(tmpDir, name) {
  return OverGraph.open(join(tmpDir, name));
}

describe('connectedComponents (sync)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-wcc-'));
    db = freshDb(tmpDir, 'wcc');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds single component', () => {
    const a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    const c = db.upsertNode(1, 'c');
    db.upsertEdge(a, b, 10);
    db.upsertEdge(b, c, 10);

    const comps = db.connectedComponents();
    assert.equal(comps.length, 3);
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[a], a); // min node ID
    assert.equal(map[b], a);
    assert.equal(map[c], a);
  });

  it('finds multiple components', () => {
    const d = db.upsertNode(1, 'd');
    const e = db.upsertNode(1, 'e');
    db.upsertEdge(d, e, 10);

    const comps = db.connectedComponents();
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[d], d);
    assert.equal(map[e], d);
    // d-e are in a different component than a-b-c (which share component a)
    const a = comps[0].nodeId; // first node from earlier test
    assert.notEqual(map[d], map[a]);
  });

  it('handles self-loops', () => {
    const s = db.upsertNode(1, 'selfloop');
    db.upsertEdge(s, s, 10);
    const comps = db.connectedComponents();
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[s], s); // self-loop doesn't change membership
  });

  it('handles parallel edges', () => {
    const p1 = db.upsertNode(1, 'par1');
    const p2 = db.upsertNode(1, 'par2');
    const p3 = db.upsertNode(1, 'par3');
    db.upsertEdge(p1, p2, 10);
    db.upsertEdge(p1, p2, 20); // parallel
    db.upsertEdge(p2, p1, 10); // reverse parallel
    const comps = db.connectedComponents();
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[p1], map[p2]);
    assert.notEqual(map[p1], map[p3]); // p3 isolated
  });

  it('isolated nodes are singletons', () => {
    const f = db.upsertNode(1, 'f_isolated');
    const comps = db.connectedComponents();
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[f], f);
  });

  it('respects edge type filter', () => {
    const g = db.upsertNode(1, 'g');
    const h = db.upsertNode(1, 'h');
    const i = db.upsertNode(1, 'i');
    db.upsertEdge(g, h, 10);
    db.upsertEdge(h, i, 20);

    const comps = db.connectedComponents({ edgeTypeFilter: [10] });
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[g], map[h]); // connected via type 10
    assert.notEqual(map[h], map[i]); // type 20 excluded
  });

  it('respects node type filter', () => {
    const j = db.upsertNode(1, 'j');
    const k = db.upsertNode(2, 'k');
    const l = db.upsertNode(1, 'l');
    db.upsertEdge(j, k, 10);
    db.upsertEdge(k, l, 10);

    const comps = db.connectedComponents({ nodeTypeFilter: [1] });
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.ok(map[j] !== undefined);
    assert.ok(map[l] !== undefined);
    assert.equal(map[k], undefined); // type 2 filtered out
  });

  it('returns sorted by nodeId', () => {
    const comps = db.connectedComponents();
    for (let i = 1; i < comps.length; i++) {
      assert.ok(comps[i].nodeId > comps[i - 1].nodeId);
    }
  });

  it('returns empty for empty graph', () => {
    const db2 = freshDb(tmpDir, 'empty');
    const comps = db2.connectedComponents();
    assert.equal(comps.length, 0);
    db2.close();
  });
});

describe('componentOf (sync)', () => {
  let tmpDir, db;
  let a, b, c, d;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-comp-'));
    db = freshDb(tmpDir, 'comp');
    a = db.upsertNode(1, 'a');
    b = db.upsertNode(1, 'b');
    c = db.upsertNode(1, 'c');
    d = db.upsertNode(1, 'd');
    db.upsertEdge(a, b, 10);
    db.upsertEdge(b, c, 10);
    // d is isolated
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns component members sorted', () => {
    const members = db.componentOf(a);
    assert.deepEqual(Array.from(members).sort((x, y) => x - y), [a, b, c].sort((x, y) => x - y));
  });

  it('returns same component from any member', () => {
    const fromA = Array.from(db.componentOf(a)).sort((x, y) => x - y);
    const fromB = Array.from(db.componentOf(b)).sort((x, y) => x - y);
    const fromC = Array.from(db.componentOf(c)).sort((x, y) => x - y);
    assert.deepEqual(fromA, fromB);
    assert.deepEqual(fromB, fromC);
  });

  it('isolated node returns singleton', () => {
    const members = Array.from(db.componentOf(d));
    assert.deepEqual(members, [d]);
  });

  it('missing node returns empty', () => {
    const members = Array.from(db.componentOf(999999));
    assert.equal(members.length, 0);
  });

  it('respects edge type filter', () => {
    const e = db.upsertNode(1, 'e');
    const f = db.upsertNode(1, 'f');
    db.upsertEdge(e, f, 20);

    const members = Array.from(db.componentOf(e, { edgeTypeFilter: [10] }));
    // Edge type 20 excluded, so e is isolated.
    assert.deepEqual(members, [e]);
  });

  it('respects node type filter', () => {
    const g = db.upsertNode(1, 'g');
    const h = db.upsertNode(2, 'h');
    db.upsertEdge(g, h, 10);

    const members = Array.from(db.componentOf(g, { nodeTypeFilter: [1] }));
    // h is type 2, filtered out
    assert.deepEqual(members, [g]);
  });

  it('returns empty for start node excluded by type filter', () => {
    const members = Array.from(db.componentOf(a, { nodeTypeFilter: [99] }));
    assert.equal(members.length, 0);
  });

  it('handles self-loop', () => {
    const s = db.upsertNode(1, 'selfloop_comp');
    db.upsertEdge(s, s, 10);
    const members = Array.from(db.componentOf(s));
    assert.deepEqual(members, [s]);
  });
});

describe('connectedComponents (async)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-wcc-async-'));
    db = freshDb(tmpDir, 'wcc');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds components async', async () => {
    const a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 10);
    const c = db.upsertNode(1, 'c'); // isolated

    const comps = await db.connectedComponentsAsync();
    const map = Object.fromEntries(comps.map(e => [e.nodeId, e.componentId]));
    assert.equal(map[a], a);
    assert.equal(map[b], a);
    assert.equal(map[c], c);
  });
});

describe('componentOf (async)', () => {
  let tmpDir, db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-comp-async-'));
    db = freshDb(tmpDir, 'comp');
  });
  after(() => { db.close(); rmSync(tmpDir, { recursive: true, force: true }); });

  it('finds component members async', async () => {
    const a = db.upsertNode(1, 'a');
    const b = db.upsertNode(1, 'b');
    db.upsertEdge(a, b, 10);

    const members = await db.componentOfAsync(a);
    assert.deepEqual(Array.from(members).sort((x, y) => x - y), [a, b].sort((x, y) => x - y));
  });

  it('missing node returns empty async', async () => {
    const members = await db.componentOfAsync(999999);
    assert.equal(members.length, 0);
  });
});
