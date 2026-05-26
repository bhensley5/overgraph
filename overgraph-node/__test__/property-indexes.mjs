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
  db.ensureNodePropertyIndex('Person', propKey, 'range');
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
    const eq = db.ensureNodePropertyIndex('Person', 'color', 'equality');
    assert.equal(eq.kind, 'equality');
    assert.equal('domain' in eq, false);
    assert.equal(eq.state, 'building');

    const range = db.ensureNodePropertyIndex('Person', 'score', 'range');
    assert.equal(range.kind, 'range');
    assert.equal('domain' in range, false);
    assert.equal(range.state, 'building');

    await waitForIndexState(
      db,
      infos => infos.find(info => info.label === 'Person' && info.propKey === 'color' && info.kind === 'equality')
    );
    const readyRange = await waitForIndexState(
      db,
      infos => infos.find(info => info.label === 'Person' && info.propKey === 'score' && info.kind === 'range')
    );
    assert.equal('domain' in readyRange, false);

    const listed = db.listNodePropertyIndexes();
    assert.equal(listed.length, 2);
    assert.deepEqual(
      listed.map(info => [info.propKey, info.kind, 'domain' in info, info.state]).sort(),
      [
        ['color', 'equality', false, 'ready'],
        ['score', 'range', false, 'ready'],
      ]
    );

    assert.equal(db.dropNodePropertyIndex('Person', 'color', 'equality'), true);
    assert.equal(db.dropNodePropertyIndex('Person', 'color', 'equality'), false);
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

  it('validates kind and range-bound inputs at the binding boundary', () => {
    assert.throws(
      () => db.ensureNodePropertyIndex('Person', 'score', 'bogus'),
      /Invalid index kind/i
    );
    assert.equal(db.ensureNodePropertyIndex('Person', 'score', 'range').kind, 'range');
    assert.throws(
      () => db.findNodesRange('Person', 'score', { value: 10, inclusive: true, domain: 'bogus' }),
      /Invalid range value type annotation/i
    );
    assert.equal(
      db.findNodesRange('Person',
        'score',
        { value: 10, inclusive: true, domain: 'int' },
        { value: 20, inclusive: true, domain: 'float' }
      ).length,
      2
    );
    assert.equal(
      db.findNodesRangePaged('Person',
        'score',
        { value: 10, inclusive: true, domain: 'int' },
        { value: 20, inclusive: true, domain: 'int' },
        {
          limit: 2,
          after: { value: 15, nodeId: 1, domain: 'float' },
        }
      ).items.length,
      1
    );
  });

  it('supports async property index and range APIs', async () => {
    const asyncEq = await db.ensureNodePropertyIndexAsync('Person', 'temp', 'equality');
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

    assert.equal(await db.dropNodePropertyIndexAsync('Person', 'temp', 'equality'), true);
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

    const eq = db.ensureEdgePropertyIndex('WORKS_AT', 'status', 'equality');
    assert.equal(eq.kind, 'equality');
    assert.equal('domain' in eq, false);
    assert.equal(eq.state, 'building');

    const range = db.ensureEdgePropertyIndex('WORKS_AT', 'score', 'range');
    assert.equal(range.kind, 'range');
    assert.equal('domain' in range, false);
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
      listed.map(info => [info.propKey, info.kind, 'domain' in info, info.state]).sort(),
      [
        ['score', 'range', false, 'ready'],
        ['status', 'equality', false, 'ready'],
      ]
    );

    assert.equal(db.dropEdgePropertyIndex('WORKS_AT', 'missing', 'equality'), false);
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
      pieces: [
        {
          kind: 'edge',
          alias: 'e',
          fromAlias: 'a',
          toAlias: 'b',
          direction: 'outgoing',
          labelFilter: ['WORKS_AT'],
          filter: { property: 'status', eq: 'hot' },
        },
      ],
      return: [
        { expr: { binding: 'a' }, as: 'a' },
        { expr: { binding: 'b' }, as: 'b' },
        { expr: { binding: 'e' }, as: 'e' },
      ],
      limit: 10,
    };
    assert.deepEqual(db.queryGraphRows(pattern).rows, [
      { a: source, b: hotTarget, e: hotEdge },
    ]);
    const patternPlan = db.explainGraphRows(pattern);
    assert.deepEqual(patternPlan.columns, ['a', 'b', 'e']);
    assert.ok(patternPlan.plan.length > 0);
    assert.match(JSON.stringify(patternPlan.plan), /EdgePropertyEqualityIndex/);

    const rangePattern = {
      ...pattern,
      pieces: [
        {
          ...pattern.pieces[0],
          filter: { property: 'score', gte: 80 },
        },
      ],
    };
    assert.deepEqual(db.queryGraphRows(rangePattern).rows, [
      { a: source, b: hotTarget, e: hotEdge },
    ]);
    const rangePatternPlan = db.explainGraphRows(rangePattern);
    assert.deepEqual(rangePatternPlan.columns, ['a', 'b', 'e']);
    assert.ok(rangePatternPlan.plan.length > 0);
    assert.match(JSON.stringify(rangePatternPlan.plan), /EdgePropertyRangeIndex/);
  });

  it('supports async edge property index APIs', async () => {
    const asyncEq = await db.ensureEdgePropertyIndexAsync('WORKS_AT', 'temp', 'equality');
    assert.equal(asyncEq.kind, 'equality');
    await waitForEdgeIndexState(
      db,
      infos => infos.find(info => info.label === 'WORKS_AT' && info.propKey === 'temp' && info.kind === 'equality')
    );

    const listed = await db.listEdgePropertyIndexesAsync();
    assert.ok(listed.some(info => info.propKey === 'temp' && info.kind === 'equality'));

    assert.equal(await db.dropEdgePropertyIndexAsync('WORKS_AT', 'temp', 'equality'), true);
  });
});
