import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { OverGraph } from '../index.js';

function planHasKind(node, kind) {
  if (!node) return false;
  if (node.kind === kind) return true;
  if (node.input && planHasKind(node.input, kind)) return true;
  return Array.isArray(node.inputs) && node.inputs.some(input => planHasKind(input, kind));
}

function sortedIds(page) {
  return Array.from(page.items).sort((a, b) => a - b);
}

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

describe('query API parity', () => {
  let tmpDir;
  let db;
  let activeHigh;
  let activeLow;
  let inactive;
  let literalUpdatedAt;
  let nullTag;
  let nested;
  let acme;
  let beta;
  let worksAt;
  let inactiveWorksAt;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-query-node-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });

    activeHigh = db.upsertNode(1, 'active-high', {
      props: { status: 'active', score: 90, team: 'core' },
    });
    activeLow = db.upsertNode(1, 'active-low', {
      props: { status: 'active', score: 40, team: 'core' },
    });
    inactive = db.upsertNode(1, 'inactive', {
      props: { status: 'inactive', score: 95, team: 'core' },
    });
    literalUpdatedAt = db.upsertNode(1, 'literal-updated-at', {
      props: { updatedAt: 'literal-property-value', status: 'active', score: 70 },
    });
    nullTag = db.upsertNode(1, 'null-tag', {
      props: { status: 'nullish', tag: null, score: 10 },
    });
    nested = db.upsertNode(1, 'nested', {
      props: { status: 'nested', payload: { items: [1, '1', null] }, score: 15 },
    });
    acme = db.upsertNode(2, 'acme', { props: { status: 'customer' } });
    beta = db.upsertNode(2, 'beta', { props: { status: 'prospect' } });
    worksAt = db.upsertEdge(activeHigh, acme, 10, {
      props: { role: 'engineer', since: 2020, updatedAt: 'edge-literal' },
    });
    inactiveWorksAt = db.upsertEdge(inactive, beta, 10, {
      props: { role: 'engineer', since: 2022, updatedAt: 'edge-literal' },
    });
  });

  after(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('runs ID-only and hydrated compound node queries', () => {
    const request = {
      typeId: 1,
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'score', gte: 50 },
        ],
      },
      limit: 0,
    };

    assert.deepEqual(sortedIds(db.queryNodeIds(request)), [activeHigh, literalUpdatedAt]);

    const nodes = db.queryNodes({ ...request, limit: 1 });
    assert.deepEqual(nodes.items.map(node => node.id), [activeHigh]);
    assert.equal(nodes.nextCursor, activeHigh);
    assert.equal(nodes.items[0].props.status, 'active');
  });

  it('ANDs filters while preserving literal built-in property names', () => {
    const result = db.queryNodeIds({
      typeId: 1,
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'updatedAt', eq: 'literal-property-value' },
        ],
      },
    });

    assert.deepEqual(Array.from(result.items), [literalUpdatedAt]);

    const timestampResult = db.queryNodeIds({
      typeId: 1,
      filter: {
        and: [
          { updatedAt: { gte: db.getNode(activeHigh).updatedAt - 1000 } },
          { property: 'status', eq: 'active' },
        ],
      },
    });
    assert.ok(Array.from(timestampResult.items).includes(activeHigh));
  });

  it('honors exclusive updatedAt range boundaries', () => {
    const updatedAt = db.getNode(activeHigh).updatedAt;

    assert.deepEqual(
      Array.from(db.queryNodeIds({ ids: [activeHigh], filter: { updatedAt: { gt: updatedAt } } }).items),
      []
    );
    assert.deepEqual(
      Array.from(db.queryNodeIds({ ids: [activeHigh], filter: { updatedAt: { lt: updatedAt } } }).items),
      []
    );
    assert.deepEqual(
      Array.from(
        db.queryNodeIds({
          ids: [activeHigh],
          filter: { updatedAt: { gte: updatedAt, lte: updatedAt } },
        }).items
      ),
      [activeHigh]
    );
  });

  it('normalizes exclusive updatedAt overflow to empty results', () => {
    const gtRequest = {
      ids: [activeHigh],
      filter: { updatedAt: { gt: 9223372036854776000 } },
    };
    assert.deepEqual(Array.from(db.queryNodeIds(gtRequest).items), []);
    assert.ok(planHasKind(db.explainNodeQuery(gtRequest).root, 'empty_result'));

    const ltRequest = {
      ids: [activeHigh],
      filter: { updatedAt: { lt: -9223372036854776000 } },
    };
    assert.deepEqual(Array.from(db.queryNodeIds(ltRequest).items), []);
    assert.ok(planHasKind(db.explainNodeQuery(ltRequest).root, 'empty_result'));
  });

  it('matches graph patterns and treats edge updatedAt as a literal property', () => {
    const result = db.queryPattern({
      nodes: [
        { alias: 'person', typeId: 1, filter: { property: 'status', eq: 'active' } },
        { alias: 'company', typeId: 2, keys: ['acme'] },
      ],
      edges: [
        {
          alias: 'employment',
          fromAlias: 'person',
          toAlias: 'company',
          direction: 'outgoing',
          typeFilter: [10],
          where: {
            role: { op: 'eq', value: 'engineer' },
            updatedAt: { op: 'eq', value: 'edge-literal' },
          },
          predicates: [{ property: { key: 'since', op: 'range', lte: 2021 } }],
        },
      ],
      limit: 10,
    });

    assert.equal(result.truncated, false);
    assert.deepEqual(result.matches, [
      {
        nodes: { company: acme, person: activeHigh },
        edges: { employment: worksAt },
      },
    ]);
    assert.notEqual(inactiveWorksAt, worksAt);
  });

  it('serializes explain output with recursive lower_snake kinds and warnings', () => {
    const nodePlan = db.explainNodeQuery({
      typeId: 1,
      filter: { property: 'status', eq: 'active' },
    });
    assert.equal(nodePlan.kind, 'node_query');
    assert.ok(planHasKind(nodePlan.root, 'fallback_type_scan'));
    assert.ok(nodePlan.warnings.every(warning => /^[a-z_]+$/.test(warning)));
    assert.ok(nodePlan.warnings.includes('using_fallback_scan'));

    const patternPlan = db.explainPatternQuery({
      nodes: [
        { alias: 'person', typeId: 1, filter: { property: 'status', eq: 'active' } },
        { alias: 'company', typeId: 2, keys: ['acme'] },
      ],
      edges: [
        {
          alias: 'employment',
          fromAlias: 'person',
          toAlias: 'company',
          direction: 'outgoing',
          typeFilter: [10],
          where: { role: { op: 'eq', value: 'engineer' } },
        },
      ],
      limit: 10,
    });
    assert.equal(patternPlan.kind, 'pattern_query');
    assert.ok(planHasKind(patternPlan.root, 'pattern_expand'));
    assert.ok(planHasKind(patternPlan.root, 'verify_edge_predicates'));
    assert.ok(patternPlan.warnings.includes('edge_property_post_filter'));
  });

  it('rejects invalid predicate and pattern shapes at the binding boundary', () => {
    assert.throws(
      () => db.queryNodeIds({ typeId: 1, predicates: [{ property: { key: 'status', op: 'eq' } }] }),
      /use filter/i
    );
    assert.throws(
      () => db.queryNodeIds({ typeId: 1, filter: { property: 'score', gt: 1, gte: 2 } }),
      /both gt and gte/i
    );
    assert.throws(
      () => db.queryNodeIds({ typeId: 1, where: { status: { eq: 'active' } } }),
      /use filter/i
    );
    assert.throws(
      () => db.queryPattern({ nodes: [{ alias: 'a', where: { status: { eq: 'active' } } }], edges: [], limit: 1 }),
      /use filter/i
    );
    assert.throws(
      () => db.queryPattern({ nodes: [{ alias: 'a' }], edges: [{ fromAlias: 'a', toAlias: 'b', filter: { property: 'role', eq: 'engineer' } }], limit: 1 }),
      /edge pattern filter is not supported/i
    );
    assert.throws(
      () => db.queryPattern({ nodes: [], edges: [], limit: 0 }),
      /positive limit|limit must be > 0/i
    );
  });

  it('supports async query and explain parity', async () => {
    const ids = await db.queryNodeIdsAsync({
      typeId: 1,
      filter: { property: 'status', eq: 'active' },
    });
    assert.ok(Array.from(ids.items).includes(activeHigh));

    const nodes = await db.queryNodesAsync({
      typeId: 1,
      filter: { property: 'score', gte: 80 },
    });
    assert.deepEqual(nodes.items.map(node => node.id), [activeHigh, inactive]);

    const plan = await db.explainNodeQueryAsync({
      typeId: 1,
      filter: { property: 'status', eq: 'active' },
    });
    assert.equal(plan.kind, 'node_query');

    const pattern = await db.queryPatternAsync({
      nodes: [
        { alias: 'person', ids: [activeHigh], filter: { property: 'status', eq: 'active' } },
        { alias: 'company', typeId: 2, keys: ['acme'] },
      ],
      edges: [
        { alias: 'employment', fromAlias: 'person', toAlias: 'company', typeFilter: [10] },
      ],
      limit: 10,
    });
    assert.deepEqual(pattern.matches[0].nodes, { company: acme, person: activeHigh });

    const patternPlan = await db.explainPatternQueryAsync({
      nodes: [
        { alias: 'person', ids: [activeHigh], filter: { property: 'status', eq: 'active' } },
        { alias: 'company', typeId: 2, keys: ['acme'] },
      ],
      edges: [
        { alias: 'employment', fromAlias: 'person', toAlias: 'company', typeFilter: [10] },
      ],
      limit: 10,
    });
    assert.equal(patternPlan.kind, 'pattern_query');
    assert.ok(planHasKind(patternPlan.root, 'verify_node_filter'));
  });

  it('supports boolean filters, null presence semantics, and nested values', () => {
    assert.deepEqual(sortedIds(db.queryNodeIds({
      typeId: 1,
      filter: { or: [{ property: 'status', eq: 'active' }, { property: 'status', eq: 'nullish' }] },
    })), [activeHigh, activeLow, literalUpdatedAt, nullTag]);

    assert.deepEqual(Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'status', in: ['nested'] },
    }).items), [nested]);

    assert.deepEqual(Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'tag', eq: null },
    }).items), [nullTag]);
    assert.deepEqual(Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'tag', in: [null] },
    }).items), [nullTag]);
    assert.deepEqual(Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'tag', exists: true },
    }).items), [nullTag]);
    assert.ok(!Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'tag', missing: true },
    }).items).includes(nullTag));

    assert.deepEqual(Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'payload', eq: { items: [1, '1', null] } },
    }).items), [nested]);
    assert.deepEqual(Array.from(db.queryNodeIds({
      typeId: 1,
      filter: { property: 'status', eq: '1' },
    }).items), []);
  });

  it('rejects invalid canonical filter shapes', () => {
    const invalid = [
      [{}, /empty object/i],
      [{ and: [] }, /at least one/i],
      [{ or: [] }, /at least one/i],
      [{ not: null }, /must be an object/i],
      [{ AND: [] }, /exactly one|uppercase/i],
      [{ and: [{ property: 'x', eq: 1 }], or: [{ property: 'x', eq: 2 }] }, /exactly one/i],
      [{ property: '', eq: 1 }, /non-empty/i],
      [{ property: 'x', in: [] }, /at least one/i],
      [{ property: 'x', eq: 1, in: [1] }, /exactly one operator family/i],
      [{ property: 'x', exists: false }, /must be true/i],
      [{ property: 'x', missing: false }, /must be true/i],
      [{ eq: 1 }, /exactly one|selector/i],
      [{ property: 'x' }, /exactly one operator family/i],
    ];
    for (const [filter, pattern] of invalid) {
      assert.throws(() => db.queryNodeIds({ typeId: 1, filter }), pattern);
    }
  });

  it('serializes boolean explain plans with lower_snake physical nodes and warnings', async () => {
    db.ensureNodePropertyIndex(1, 'status', { kind: 'equality' });
    await waitForIndexState(
      db,
      infos => infos.find(info => info.typeId === 1 && info.propKey === 'status' && info.kind === 'equality')
    );

    const indexedOr = db.explainNodeQuery({
      typeId: 1,
      filter: { or: [{ property: 'status', eq: 'active' }, { property: 'status', eq: 'nullish' }] },
    });
    assert.ok(planHasKind(indexedOr.root, 'union'));
    assert.ok(planHasKind(indexedOr.root, 'verify_node_filter'));

    const fallbackOr = db.explainNodeQuery({
      typeId: 1,
      filter: { or: [{ property: 'status', eq: 'active' }, { property: 'tag', missing: true }] },
    });
    assert.ok(fallbackOr.warnings.includes('boolean_branch_fallback'));
    assert.ok(fallbackOr.warnings.includes('verify_only_filter'));

    const empty = db.explainNodeQuery({
      typeId: 1,
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'status', eq: 'inactive' },
        ],
      },
    });
    assert.ok(planHasKind(empty.root, 'empty_result'));

    const asyncPlan = await db.explainNodeQueryAsync({
      typeId: 1,
      filter: { property: 'status', eq: 'active' },
    });
    assert.equal(asyncPlan.kind, 'node_query');
    assert.ok(asyncPlan.warnings.every(warning => /^[a-z_]+$/.test(warning)));
  });
});
