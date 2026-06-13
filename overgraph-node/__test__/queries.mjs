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

function nodeLabels(label) {
  return { labels: [label], mode: 'all' };
}

function sortedIds(page) {
  return Array.from(page.items).sort((a, b) => a - b);
}

function propertyIndexSpec(propKey, kind) {
  return { kind, fields: [{ source: 'property', key: propKey }] };
}

function hasPropertyField(info, propKey) {
  return info.fields.length === 1
    && info.fields[0].source === 'property'
    && info.fields[0].key === propKey;
}

async function rejectsOrThrows(fn, pattern) {
  let result;
  try {
    result = fn();
  } catch (err) {
    assert.match(String(err?.message ?? err), pattern);
    return;
  }
  await assert.rejects(result, pattern);
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

    activeHigh = db.upsertNode('Person', 'active-high', {
      props: { status: 'active', score: 90, team: 'core' },
    });
    activeLow = db.upsertNode('Person', 'active-low', {
      props: { status: 'active', score: 40, team: 'core' },
    });
    inactive = db.upsertNode('Person', 'inactive', {
      props: { status: 'inactive', score: 95, team: 'core' },
    });
    literalUpdatedAt = db.upsertNode('Person', 'literal-updated-at', {
      props: { updatedAt: 'literal-property-value', status: 'active', score: 70 },
    });
    nullTag = db.upsertNode('Person', 'null-tag', {
      props: { status: 'nullish', tag: null, score: 10 },
    });
    nested = db.upsertNode('Person', 'nested', {
      props: { status: 'nested', payload: { items: [1, '1', null] }, score: 15 },
    });
    acme = db.upsertNode('Company', 'acme', { props: { status: 'customer' } });
    beta = db.upsertNode('Company', 'beta', { props: { status: 'prospect' } });
    worksAt = db.upsertEdge(activeHigh, acme, 'WORKS_AT', {
      props: { role: 'engineer', since: 2020, updatedAt: 'edge-literal' },
    });
    inactiveWorksAt = db.upsertEdge(inactive, beta, 'WORKS_AT', {
      props: { role: 'engineer', since: 2022, updatedAt: 'edge-literal' },
    });
  });

  after(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('runs ID-only and hydrated compound node queries', () => {
    const request = {
      labelFilter: nodeLabels('Person'),
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

  it('honors NodeLabelFilter Any versus All semantics', () => {
    const alpha = db.upsertNode('AnyAlpha', 'any-alpha');
    const beta = db.upsertNode('AnyBeta', 'any-beta');
    const both = db.upsertNode(['AnyAlpha', 'AnyBeta'], 'any-both');

    const any = sortedIds(db.queryNodeIds({
      labelFilter: { labels: ['AnyAlpha', 'AnyBeta'], mode: 'any' },
    }));
    assert.deepEqual(any, [alpha, beta, both].sort((a, b) => a - b));

    const all = sortedIds(db.queryNodeIds({
      labelFilter: { labels: ['AnyAlpha', 'AnyBeta'], mode: 'all' },
    }));
    assert.deepEqual(all, [both]);
  });

  it('ANDs filters while preserving literal built-in property names', () => {
    const result = db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'updatedAt', eq: 'literal-property-value' },
        ],
      },
    });

    assert.deepEqual(Array.from(result.items), [literalUpdatedAt]);

    const timestampResult = db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
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

  it('matches graph rows and treats edge updatedAt as a literal property', () => {
    const result = db.queryGraphRows({
      nodes: [
        { alias: 'person', labelFilter: nodeLabels('Person'), filter: { property: 'status', eq: 'active' } },
        { alias: 'company', labelFilter: nodeLabels('Company'), keys: [{ label: 'Company', key: 'acme' }] },
      ],
      pieces: [
        {
          kind: 'edge',
          alias: 'employment',
          fromAlias: 'person',
          toAlias: 'company',
          direction: 'outgoing',
          labelFilter: ['WORKS_AT'],
          filter: {
            and: [
              { property: 'role', eq: 'engineer' },
              { property: 'updatedAt', eq: 'edge-literal' },
              { property: 'since', lte: 2021 },
            ],
          },
        },
      ],
      return: [
        { expr: { binding: 'company' }, as: 'company' },
        { expr: { binding: 'person' }, as: 'person' },
        { expr: { binding: 'employment' }, as: 'employment' },
      ],
      limit: 10,
    });

    assert.equal(result.nextCursor, null);
    assert.deepEqual(result.rows, [{ company: acme, person: activeHigh, employment: worksAt }]);
    assert.notEqual(inactiveWorksAt, worksAt);
  });

  it('runs direct edge ID and hydrated edge queries', () => {
    const edge = db.getEdge(worksAt);
    const request = {
      label: 'WORKS_AT',
      fromIds: [activeHigh],
      filter: {
        and: [
          { weight: { gte: 1.0 } },
          { validAt: Date.now() },
          { updatedAt: { gte: edge.updatedAt - 1000 } },
          { property: 'role', eq: 'engineer' },
        ],
      },
      limit: 0,
    };

    assert.deepEqual(Array.from(db.queryEdgeIds(request).items), [worksAt]);

    const edges = db.queryEdges({ ...request, limit: 1 });
    assert.deepEqual(edges.items.map(item => item.id), [worksAt]);
    assert.equal(edges.nextCursor, null);
    assert.equal(edges.items[0].props.role, 'engineer');

    const plan = db.explainEdgeQuery(request);
    assert.equal(plan.kind, 'edge_query');
    assert.ok(planHasKind(plan.root, 'verify_edge_filter'));
    assert.ok(plan.warnings.includes('edge_property_post_filter'));
  });

  it('accepts canonical graph edge filters', () => {
    const result = db.queryGraphRows({
      nodes: [
        { alias: 'person', ids: [activeHigh] },
        { alias: 'company', labelFilter: nodeLabels('Company'), keys: [{ label: 'Company', key: 'acme' }] },
      ],
      pieces: [
        {
          kind: 'edge',
          alias: 'employment',
          fromAlias: 'person',
          toAlias: 'company',
          direction: 'outgoing',
          labelFilter: ['WORKS_AT'],
          filter: {
            and: [
              { validAt: Date.now() },
              { property: 'role', eq: 'engineer' },
            ],
          },
        },
      ],
      return: [
        { expr: { binding: 'company' }, as: 'company' },
        { expr: { binding: 'person' }, as: 'person' },
        { expr: { binding: 'employment' }, as: 'employment' },
      ],
      limit: 10,
    });

    assert.deepEqual(result.rows, [{ company: acme, person: activeHigh, employment: worksAt }]);
  });

  it('serializes explain output with recursive lower_snake kinds and warnings', () => {
    const nodePlan = db.explainNodeQuery({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'status', eq: 'active' },
    });
    assert.equal(nodePlan.kind, 'node_query');
    assert.ok(planHasKind(nodePlan.root, 'fallback_node_label_scan'));
    assert.ok(nodePlan.warnings.every(warning => /^[a-z_]+$/.test(warning)));
    assert.ok(nodePlan.warnings.includes('using_fallback_scan'));

    const graphPlan = db.explainGraphRows({
      nodes: [
        { alias: 'person', labelFilter: nodeLabels('Person'), filter: { property: 'status', eq: 'active' } },
        { alias: 'company', labelFilter: nodeLabels('Company'), keys: [{ label: 'Company', key: 'acme' }] },
      ],
      pieces: [
        {
          kind: 'edge',
          alias: 'employment',
          fromAlias: 'person',
          toAlias: 'company',
          direction: 'outgoing',
          labelFilter: ['WORKS_AT'],
          filter: { property: 'role', eq: 'engineer' },
        },
      ],
      return: [{ expr: { binding: 'employment' }, as: 'employment' }],
      limit: 10,
    });
    assert.deepEqual(graphPlan.columns, ['employment']);
    assert.equal(graphPlan.projection.outputMode, 'ids');
    assert.ok(graphPlan.plan.length > 0);
  });

  it('rejects invalid predicate and pattern shapes at the binding boundary', () => {
    assert.throws(
      () => db.queryNodeIds({ labelFilter: nodeLabels('Person'), predicates: [{ property: { key: 'status', op: 'eq' } }] }),
      /use filter/i
    );
    assert.throws(
      () => db.queryNodeIds({ labelFilter: nodeLabels('Person'), filter: { property: 'score', gt: 1, gte: 2 } }),
      /both gt and gte/i
    );
    assert.throws(
      () => db.queryNodeIds({ labelFilter: nodeLabels('Person'), where: { status: { eq: 'active' } } }),
      /use filter/i
    );
    assert.throws(
      () => db.queryGraphRows({ nodes: [{ alias: 'a', where: { status: { eq: 'active' } } }], pieces: [], limit: 1 }),
      /use filter/i
    );
    assert.throws(
      () => db.queryEdgeIds({ filter: { property: 'role', eq: 'engineer' } }),
      /full scan|anchor|allow_full_scan/i
    );
    assert.throws(
      () => db.queryEdgeIds({ label: 'WORKS_AT', filter: { weight: { gt: 1, gte: 2 } } }),
      /both gt and gte/i
    );
    for (const field of ['where', 'predicates']) {
      assert.throws(
        () => db.queryEdgeIds({ label: 'WORKS_AT', [field]: { role: { eq: 'engineer' } } }),
        /use filter/i
      );
      assert.throws(
        () => db.queryEdges({ label: 'WORKS_AT', [field]: { role: { eq: 'engineer' } } }),
        /use filter/i
      );
      assert.throws(
        () => db.explainEdgeQuery({ label: 'WORKS_AT', [field]: { role: { eq: 'engineer' } } }),
        /use filter/i
      );
    }
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'a' }],
        pieces: [{
          kind: 'edge',
          fromAlias: 'a',
          toAlias: 'b',
          filter: { property: 'role', eq: 'engineer' },
          where: { role: { eq: 'engineer' } },
        }],
        limit: 1,
      }),
      /where|does not accept field/i
    );
    assert.throws(
      () => db.queryGraphRows({ nodes: [], pieces: [], limit: 0 }),
      /positive limit|limit must be > 0/i
    );
  });

  it('supports async query and explain parity', async () => {
    const ids = await db.queryNodeIdsAsync({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'status', eq: 'active' },
    });
    assert.ok(Array.from(ids.items).includes(activeHigh));

    const nodes = await db.queryNodesAsync({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'score', gte: 80 },
    });
    assert.deepEqual(nodes.items.map(node => node.id), [activeHigh, inactive]);

    const plan = await db.explainNodeQueryAsync({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'status', eq: 'active' },
    });
    assert.equal(plan.kind, 'node_query');

    const edgeIds = await db.queryEdgeIdsAsync({
      fromIds: [activeHigh],
      filter: { property: 'role', eq: 'engineer' },
    });
    assert.deepEqual(Array.from(edgeIds.items), [worksAt]);

    const edges = await db.queryEdgesAsync({
      ids: [worksAt],
      filter: { validAt: Date.now() },
    });
    assert.deepEqual(edges.items.map(edge => edge.id), [worksAt]);

    const edgePlan = await db.explainEdgeQueryAsync({ ids: [worksAt] });
    assert.equal(edgePlan.kind, 'edge_query');

    await rejectsOrThrows(
      () => db.queryEdgeIdsAsync({ label: 'WORKS_AT', where: { role: { eq: 'engineer' } } }),
      /use filter/i
    );
    await rejectsOrThrows(
      () => db.queryEdgesAsync({ label: 'WORKS_AT', predicates: { role: { eq: 'engineer' } } }),
      /use filter/i
    );
    await rejectsOrThrows(
      () => db.explainEdgeQueryAsync({ label: 'WORKS_AT', where: { role: { eq: 'engineer' } } }),
      /use filter/i
    );

    const pattern = await db.queryGraphRowsAsync({
      nodes: [
        { alias: 'person', ids: [activeHigh], filter: { property: 'status', eq: 'active' } },
        { alias: 'company', labelFilter: nodeLabels('Company'), keys: [{ label: 'Company', key: 'acme' }] },
      ],
      pieces: [
        { kind: 'edge', alias: 'employment', fromAlias: 'person', toAlias: 'company', labelFilter: ['WORKS_AT'] },
      ],
      return: [
        { expr: { binding: 'company' }, as: 'company' },
        { expr: { binding: 'person' }, as: 'person' },
      ],
      limit: 10,
    });
    assert.deepEqual(pattern.rows[0], { company: acme, person: activeHigh });

    const patternPlan = await db.explainGraphRowsAsync({
      nodes: [
        { alias: 'person', ids: [activeHigh], filter: { property: 'status', eq: 'active' } },
        { alias: 'company', labelFilter: nodeLabels('Company'), keys: [{ label: 'Company', key: 'acme' }] },
      ],
      pieces: [
        { kind: 'edge', alias: 'employment', fromAlias: 'person', toAlias: 'company', labelFilter: ['WORKS_AT'] },
      ],
      return: [{ expr: { binding: 'employment' }, as: 'employment' }],
      limit: 10,
    });
    assert.deepEqual(patternPlan.columns, ['employment']);
    assert.ok(patternPlan.plan.length > 0);
  });

  it('supports boolean filters, null presence semantics, and nested values', () => {
    assert.deepEqual(sortedIds(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { or: [{ property: 'status', eq: 'active' }, { property: 'status', eq: 'nullish' }] },
    })), [activeHigh, activeLow, literalUpdatedAt, nullTag]);

    assert.deepEqual(Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'status', in: ['nested'] },
    }).items), [nested]);

    assert.deepEqual(Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'tag', eq: null },
    }).items), [nullTag]);
    assert.deepEqual(Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'tag', in: [null] },
    }).items), [nullTag]);
    assert.deepEqual(Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'tag', exists: true },
    }).items), [nullTag]);
    assert.ok(!Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'tag', missing: true },
    }).items).includes(nullTag));

    assert.deepEqual(Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'payload', eq: { items: [1, '1', null] } },
    }).items), [nested]);
    assert.deepEqual(Array.from(db.queryNodeIds({
      labelFilter: nodeLabels('Person'),
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
      assert.throws(() => db.queryNodeIds({ labelFilter: nodeLabels('Person'), filter }), pattern);
    }
  });

  it('serializes boolean explain plans with lower_snake physical nodes and warnings', async () => {
    db.ensureNodePropertyIndex('Person', propertyIndexSpec('status', 'equality'));
    await waitForIndexState(
      db,
      infos => infos.find(info => info.label === 'Person' && hasPropertyField(info, 'status') && info.kind === 'equality')
    );

    const indexedOr = db.explainNodeQuery({
      labelFilter: nodeLabels('Person'),
      filter: { or: [{ property: 'status', eq: 'active' }, { property: 'status', eq: 'nullish' }] },
    });
    assert.ok(planHasKind(indexedOr.root, 'union'));
    assert.ok(planHasKind(indexedOr.root, 'verify_node_filter'));

    const fallbackOr = db.explainNodeQuery({
      labelFilter: nodeLabels('Person'),
      filter: { or: [{ property: 'status', eq: 'active' }, { property: 'tag', missing: true }] },
    });
    assert.ok(fallbackOr.warnings.includes('boolean_branch_fallback'));
    assert.ok(fallbackOr.warnings.includes('verify_only_filter'));

    const empty = db.explainNodeQuery({
      labelFilter: nodeLabels('Person'),
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'status', eq: 'inactive' },
        ],
      },
    });
    assert.ok(planHasKind(empty.root, 'empty_result'));

    const asyncPlan = await db.explainNodeQueryAsync({
      labelFilter: nodeLabels('Person'),
      filter: { property: 'status', eq: 'active' },
    });
    assert.equal(asyncPlan.kind, 'node_query');
    assert.ok(asyncPlan.warnings.every(warning => /^[a-z_]+$/.test(warning)));
  });
});
