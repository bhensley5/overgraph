import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { OverGraph } from '../index.js';

async function waitForIndexState(db, predicate, expectedState = 'ready', timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const info = predicate(db.listNodePropertyIndexes());
    if (info?.state === expectedState) {
      return info;
    }
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for secondary index state '${expectedState}'`);
    }
    await delay(20);
  }
}

async function waitForEdgeIndexState(db, predicate, expectedState = 'ready', timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const info = predicate(db.listEdgePropertyIndexes());
    if (info?.state === expectedState) {
      return info;
    }
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for edge secondary index state '${expectedState}'`);
    }
    await delay(20);
  }
}

function planHasKind(node, kind) {
  if (!node) return false;
  if (node.kind === kind) return true;
  if (node.input && planHasKind(node.input, kind)) return true;
  return Array.isArray(node.inputs) && node.inputs.some(input => planHasKind(input, kind));
}

async function ensureRangeIndexReady(db, propKey = 'score') {
  db.ensureNodePropertyIndex('Person', propKey, { kind: 'range', domain: 'int' });
  return waitForIndexState(
    db,
    infos => infos.find(info => info.label === 'Person' && info.propKey === propKey && info.kind === 'range')
  );
}

describe('node property index APIs', () => {
  let tmpDir;
  let db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-prop-index-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 6; i++) {
      db.upsertNode('Person', `node-${i}`, {
        props: {
          color: i % 2 === 0 ? 'red' : 'blue',
          score: (i + 1) * 10,
          temp: (i + 1) * 5,
        },
      });
    }
  });

  after(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('ensures, lists, and drops declared property indexes', async () => {
    const eq = db.ensureNodePropertyIndex('Person', 'color', { kind: 'equality' });
    assert.equal(eq.kind, 'equality');
    assert.equal(eq.domain, undefined);
    assert.equal(eq.state, 'building');

    const range = db.ensureNodePropertyIndex('Person', 'score', { kind: 'range', domain: 'int' });
    assert.equal(range.kind, 'range');
    assert.equal(range.domain, 'int');
    assert.equal(range.state, 'building');

    await waitForIndexState(
      db,
      infos => infos.find(info => info.label === 'Person' && info.propKey === 'color' && info.kind === 'equality')
    );
    const readyRange = await waitForIndexState(
      db,
      infos => infos.find(info => info.label === 'Person' && info.propKey === 'score' && info.kind === 'range')
    );
    assert.equal(readyRange.domain, 'int');

    const listed = db.listNodePropertyIndexes();
    assert.equal(listed.length, 2);
    assert.deepEqual(
      listed.map(info => [info.propKey, info.kind, info.domain ?? null, info.state]).sort(),
      [
        ['color', 'equality', null, 'ready'],
        ['score', 'range', 'int', 'ready'],
      ]
    );

    assert.throws(
      () => db.ensureNodePropertyIndex('Person', 'score', { kind: 'range', domain: 'float' }),
      /different domain/i
    );

    assert.equal(db.dropNodePropertyIndex('Person', 'color', { kind: 'equality' }), true);
    assert.equal(db.dropNodePropertyIndex('Person', 'color', { kind: 'equality' }), false);
  });

  it('runs range queries and paging through the public API', async () => {
    await ensureRangeIndexReady(db);

    const all = Array.from(
      db.findNodesRange('Person',
        'score',
        { value: 20, inclusive: true, domain: 'int' },
        { value: 50, inclusive: false, domain: 'int' }
      )
    );
    assert.equal(all.length, 3);

    const first = db.findNodesRangePaged('Person',
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 50, inclusive: false, domain: 'int' },
      { limit: 2 }
    );
    assert.deepEqual(Array.from(first.items), all.slice(0, 2));
    assert.equal(first.nextCursor?.domain, 'int');
    assert.equal(typeof first.nextCursor?.value, 'number');
    assert.equal(typeof first.nextCursor?.nodeId, 'number');

    const second = db.findNodesRangePaged('Person',
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 50, inclusive: false, domain: 'int' },
      { limit: 2, after: first.nextCursor }
    );
    assert.deepEqual(Array.from(second.items), all.slice(2));
    assert.ok(second.nextCursor == null);

    const fallback = Array.from(
      db.findNodesRange('Person',
        'temp',
        { value: 10, inclusive: true, domain: 'int' },
        { value: 25, inclusive: true, domain: 'int' }
      )
    );
    assert.equal(fallback.length, 4);
  });

  it('validates kind and domain inputs at the binding boundary', () => {
    assert.throws(
      () => db.ensureNodePropertyIndex('Person', 'score', { kind: 'bogus' }),
      /Invalid index kind/i
    );
    assert.throws(
      () => db.ensureNodePropertyIndex('Person', 'score', { kind: 'range' }),
      /Range indexes require domain/i
    );
    assert.throws(
      () => db.ensureNodePropertyIndex('Person', 'score', { kind: 'equality', domain: 'int' }),
      /do not accept a range domain/i
    );
    assert.throws(
      () => db.findNodesRange('Person', 'score', { value: 10, inclusive: true, domain: 'bogus' }),
      /Invalid range domain/i
    );
    assert.throws(
      () =>
        db.findNodesRange('Person',
          'score',
          { value: 10, inclusive: true, domain: 'int' },
          { value: 20, inclusive: true, domain: 'float' }
        ),
      /same PropValue variant/i
    );
    assert.throws(
      () =>
        db.findNodesRangePaged('Person',
          'score',
          { value: 10, inclusive: true, domain: 'int' },
          { value: 20, inclusive: true, domain: 'int' },
          {
            limit: 2,
            after: { value: 15, nodeId: 1, domain: 'float' },
          }
        ),
      /cursor must use the same PropValue variant/i
    );
  });

  it('supports async property index and range APIs', async () => {
    const asyncEq = await db.ensureNodePropertyIndexAsync('Person', 'temp', { kind: 'equality' });
    assert.equal(asyncEq.kind, 'equality');
    await waitForIndexState(
      db,
      infos => infos.find(info => info.label === 'Person' && info.propKey === 'temp' && info.kind === 'equality')
    );
    await ensureRangeIndexReady(db);

    const listed = await db.listNodePropertyIndexesAsync();
    assert.ok(listed.some(info => info.propKey === 'temp' && info.kind === 'equality'));

    const ids = await db.findNodesRangeAsync('Person',
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 30, inclusive: true, domain: 'int' }
    );
    assert.equal(ids.length, 2);

    const page = await db.findNodesRangePagedAsync('Person',
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 40, inclusive: true, domain: 'int' },
      { limit: 2 }
    );
    assert.equal(page.items.length, 2);
    assert.equal(page.nextCursor?.domain, 'int');

    assert.equal(await db.dropNodePropertyIndexAsync('Person', 'temp', { kind: 'equality' }), true);
  });
});

describe('edge property index APIs', () => {
  let tmpDir;
  let db;
  let source;
  let hotTarget;
  let coldTarget;
  let hotEdge;

  before(async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-edge-prop-index-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });

    const eq = db.ensureEdgePropertyIndex('WORKS_AT', 'status', { kind: 'equality' });
    assert.equal(eq.kind, 'equality');
    assert.equal(eq.domain, undefined);
    assert.equal(eq.state, 'building');

    const range = db.ensureEdgePropertyIndex('WORKS_AT', 'score', { kind: 'range', domain: 'int' });
    assert.equal(range.kind, 'range');
    assert.equal(range.domain, 'int');
    assert.equal(range.state, 'building');

    source = db.upsertNode('Person', 'source');
    hotTarget = db.upsertNode('Company', 'hot-target');
    coldTarget = db.upsertNode('Company', 'cold-target');
    hotEdge = db.upsertEdge(source, hotTarget, 'WORKS_AT', {
      props: { status: 'hot', score: 90 },
      weight: 2.0,
    });
    db.upsertEdge(source, coldTarget, 'WORKS_AT', {
      props: { status: 'cold', score: 10 },
      weight: 1.0,
    });

    await waitForEdgeIndexState(
      db,
      infos => infos.find(info => info.label === 'WORKS_AT' && info.propKey === 'status' && info.kind === 'equality')
    );
    await waitForEdgeIndexState(
      db,
      infos => infos.find(info => info.label === 'WORKS_AT' && info.propKey === 'score' && info.kind === 'range')
    );
  });

  after(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('ensures, lists, validates, and drops declared edge property indexes', () => {
    const listed = db.listEdgePropertyIndexes();
    assert.deepEqual(
      listed.map(info => [info.propKey, info.kind, info.domain ?? null, info.state]).sort(),
      [
        ['score', 'range', 'int', 'ready'],
        ['status', 'equality', null, 'ready'],
      ]
    );

    assert.throws(
      () => db.ensureEdgePropertyIndex('WORKS_AT', 'score', { kind: 'range', domain: 'float' }),
      /different domain/i
    );
    assert.throws(
      () => db.ensureEdgePropertyIndex('WORKS_AT', 'score', { kind: 'range' }),
      /Range indexes require domain/i
    );
    assert.throws(
      () => db.ensureEdgePropertyIndex('WORKS_AT', 'status', { kind: 'equality', domain: 'int' }),
      /do not accept a range domain/i
    );

    assert.equal(db.dropEdgePropertyIndex('WORKS_AT', 'missing', { kind: 'equality' }), false);
  });

  it('uses edge property indexes from direct edge queries and pattern explain', () => {
    const direct = db.queryEdgeIds({
      label: 'WORKS_AT',
      fromIds: [source],
      filter: { property: 'status', eq: 'hot' },
      limit: 10,
    });
    assert.deepEqual(Array.from(direct.items), [hotEdge]);

    const directPlan = db.explainEdgeQuery({
      label: 'WORKS_AT',
      fromIds: [source],
      filter: { property: 'status', eq: 'hot' },
      limit: 10,
    });
    assert.ok(planHasKind(directPlan.root, 'edge_property_equality_index'));

    const directRange = db.queryEdgeIds({
      label: 'WORKS_AT',
      fromIds: [source],
      filter: { property: 'score', gte: 80 },
      limit: 10,
    });
    assert.deepEqual(Array.from(directRange.items), [hotEdge]);

    const directRangePlan = db.explainEdgeQuery({
      label: 'WORKS_AT',
      fromIds: [source],
      filter: { property: 'score', gte: 80 },
      limit: 10,
    });
    assert.ok(planHasKind(directRangePlan.root, 'edge_property_range_index'));

    const pattern = {
      nodes: [
        { alias: 'a', labelFilter: { labels: ['Person'], mode: 'all' } },
        { alias: 'b', labelFilter: { labels: ['Company'], mode: 'all' } },
      ],
      edges: [
        {
          alias: 'e',
          fromAlias: 'a',
          toAlias: 'b',
          direction: 'outgoing',
          labelFilter: ['WORKS_AT'],
          filter: { property: 'status', eq: 'hot' },
        },
      ],
      limit: 10,
    };
    assert.deepEqual(db.queryPattern(pattern).matches, [
      { nodes: { a: source, b: hotTarget }, edges: { e: hotEdge } },
    ]);
    const patternPlan = db.explainPatternQuery(pattern);
    assert.ok(planHasKind(patternPlan.root, 'pattern_edge_anchor'));
    assert.ok(planHasKind(patternPlan.root, 'edge_property_equality_index'));

    const rangePattern = {
      ...pattern,
      edges: [
        {
          ...pattern.edges[0],
          filter: { property: 'score', gte: 80 },
        },
      ],
    };
    assert.deepEqual(db.queryPattern(rangePattern).matches, [
      { nodes: { a: source, b: hotTarget }, edges: { e: hotEdge } },
    ]);
    const rangePatternPlan = db.explainPatternQuery(rangePattern);
    assert.ok(planHasKind(rangePatternPlan.root, 'pattern_edge_anchor'));
    assert.ok(planHasKind(rangePatternPlan.root, 'edge_property_range_index'));
  });

  it('supports async edge property index APIs', async () => {
    const asyncEq = await db.ensureEdgePropertyIndexAsync('WORKS_AT', 'temp', { kind: 'equality' });
    assert.equal(asyncEq.kind, 'equality');
    await waitForEdgeIndexState(
      db,
      infos => infos.find(info => info.label === 'WORKS_AT' && info.propKey === 'temp' && info.kind === 'equality')
    );

    const listed = await db.listEdgePropertyIndexesAsync();
    assert.ok(listed.some(info => info.propKey === 'temp' && info.kind === 'equality'));

    assert.equal(await db.dropEdgePropertyIndexAsync('WORKS_AT', 'temp', { kind: 'equality' }), true);
  });
});
