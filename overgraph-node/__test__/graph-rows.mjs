import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function nodeLabels(label) {
  return { labels: [label], mode: 'all' };
}

function closeDir(db, tmpDir) {
  db?.close();
  rmSync(tmpDir, { recursive: true, force: true });
}

function approxArray(actual, expected) {
  assert.equal(actual.length, expected.length);
  for (let i = 0; i < actual.length; i += 1) {
    assert.ok(Math.abs(actual[i] - expected[i]) < 0.00001);
  }
}

describe('graph row connector API', () => {
  let tmpDir;
  let db;
  let ids;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-graph-row-node-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      denseVector: { dimension: 3 },
    });
    const ada = db.upsertNode('Person', 'ada', {
      props: { name: 'Ada', status: 'active', rank: 1 },
      denseVector: [0.1, 0.2, 0.3],
      sparseVector: [{ dimension: 2, value: 0.75 }],
    });
    const ben = db.upsertNode('Person', 'ben', {
      props: { name: 'Ben', status: 'active', rank: 2 },
    });
    const cy = db.upsertNode('Person', 'cy', {
      props: { name: 'Cy', status: 'active', rank: 3 },
    });
    const dee = db.upsertNode('Person', 'dee', {
      props: { name: 'Dee', status: 'inactive', rank: 4 },
    });
    const root = db.upsertNode('Root', 'root');
    const ab = db.upsertEdge(ada, ben, 'KNOWS', { props: { strength: 7 } });
    const bc = db.upsertEdge(ben, cy, 'KNOWS', { props: { strength: 5 } });
    const ad = db.upsertEdge(ada, dee, 'KNOWS', { props: { strength: 1 } });
    const rootAda = db.upsertEdge(root, ada, 'SEED');
    const rootCy = db.upsertEdge(root, cy, 'SEED');
    ids = { ada, ben, cy, dee, root, ab, bc, ad, rootAda, rootCy };
  });

  after(() => closeDir(db, tmpDir));

  it('does not expose the removed pattern API surface', () => {
    assert.equal(db.queryPattern, undefined);
    assert.equal(db.explainPatternQuery, undefined);
    assert.equal(db.queryPatternAsync, undefined);
    assert.equal(db.explainPatternQueryAsync, undefined);
  });

  it('runs fixed graph-row patterns and compact rows', () => {
    const request = {
      nodes: [
        { alias: 'a', ids: [ids.ada] },
        { alias: 'b', labelFilter: nodeLabels('Person') },
      ],
      pieces: [{ kind: 'edge', alias: 'r', fromAlias: 'a', toAlias: 'b', labelFilter: ['KNOWS'] }],
      return: [
        { expr: { binding: 'a' }, as: 'a' },
        { expr: { binding: 'r' }, as: 'r' },
        { expr: { binding: 'b' }, as: 'b' },
      ],
      orderBy: [{ expr: { property: { alias: 'b', key: 'rank' } }, direction: 'asc' }],
      limit: 10,
    };

    const plain = db.queryGraphRows(request);
    assert.equal(plain.stats.planningNs, null);
    assert.equal(plain.stats.executionNs, null);
    assert.deepEqual(plain.rows, [
      { a: ids.ada, r: ids.ab, b: ids.ben },
      { a: ids.ada, r: ids.ad, b: ids.dee },
    ]);

    const profiled = db.queryGraphRows({
      ...request,
      options: { profile: true },
    });
    assert.equal(typeof profiled.stats.planningNs, 'number');
    assert.equal(typeof profiled.stats.executionNs, 'number');

    const fastProfiled = db.queryGraphRows({
      nodes: [{ alias: 'n', ids: [ids.ada] }],
      return: [{ expr: { binding: 'n' }, as: 'n' }],
      limit: 1,
      options: { profile: true },
    });
    assert.equal(typeof fastProfiled.stats.planningNs, 'number');
    assert.equal(typeof fastProfiled.stats.executionNs, 'number');

    const compact = db.queryGraphRows({
      ...request,
      output: { compactRows: true },
    });
    assert.deepEqual(compact.columns, ['a', 'r', 'b']);
    assert.deepEqual(compact.rows, [
      [ids.ada, ids.ab, ids.ben],
      [ids.ada, ids.ad, ids.dee],
    ]);
  });

  it('returns null for optional misses and values for optional hits', () => {
    const request = {
      nodes: [
        { alias: 'root', ids: [ids.root] },
        { alias: 'a', labelFilter: nodeLabels('Person') },
        { alias: 'm', labelFilter: nodeLabels('Person') },
      ],
      pieces: [
        { kind: 'edge', alias: 'seed', fromAlias: 'root', toAlias: 'a', labelFilter: ['SEED'] },
        {
          kind: 'optional',
          pieces: [{ kind: 'edge', alias: 'r', fromAlias: 'a', toAlias: 'm', labelFilter: ['KNOWS'] }],
        },
      ],
      return: [
        { expr: { property: { alias: 'a', key: 'name' } }, as: 'name' },
        { expr: { binding: 'm' }, as: 'maybeFriend' },
        { expr: { binding: 'r' }, as: 'maybeEdge' },
      ],
      orderBy: [{ expr: { property: { alias: 'a', key: 'name' } }, direction: 'asc' }],
      limit: 10,
      options: { allowFullScan: true },
    };

    assert.deepEqual(db.queryGraphRows(request).rows, [
      { name: 'Ada', maybeFriend: ids.ben, maybeEdge: ids.ab },
      { name: 'Ada', maybeFriend: ids.dee, maybeEdge: ids.ad },
      { name: 'Cy', maybeFriend: null, maybeEdge: null },
    ]);
  });

  it('returns path identities and hydrated paths with vector policy', () => {
    const base = {
      nodes: [
        { alias: 'a', ids: [ids.ada] },
        { alias: 'c', ids: [ids.cy] },
      ],
      pieces: [
        {
          kind: 'variableLength',
          pathAlias: 'p',
          fromAlias: 'a',
          toAlias: 'c',
          labelFilter: ['KNOWS'],
          minHops: 1,
          maxHops: 2,
        },
      ],
      return: [{ expr: { binding: 'p' }, as: 'p' }],
      limit: 10,
    };

    const idPath = db.queryGraphRows(base).rows[0].p;
    assert.deepEqual(idPath.nodeIds, [ids.ada, ids.ben, ids.cy]);
    assert.deepEqual(idPath.edgeIds, [ids.ab, ids.bc]);
    assert.equal(idPath.nodes, undefined);

    const hydrated = db.queryGraphRows({ ...base, output: { mode: 'elements' } }).rows[0].p;
    assert.deepEqual(hydrated.nodes.map(node => node.id), [ids.ada, ids.ben, ids.cy]);
    assert.deepEqual(hydrated.edges.map(edge => edge.id), [ids.ab, ids.bc]);
    assert.equal(hydrated.nodes[0].denseVector, undefined);

    const vectors = db.queryGraphRows({
      ...base,
      output: { mode: 'elements', includeVectors: true },
    }).rows[0].p;
    approxArray(vectors.nodes[0].denseVector, [0.1, 0.2, 0.3]);
    assert.deepEqual(vectors.nodes[0].sparseVector, [{ dimension: 2, value: 0.75 }]);
  });

  it('round-trips order, skip, limit, and cursors', () => {
    const defaultLimit = db.queryGraphRows({
      nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
      return: [{ expr: { property: { alias: 'n', key: 'name' } }, as: 'name' }],
      orderBy: [{ expr: { property: { alias: 'n', key: 'rank' } }, direction: 'asc' }],
      options: { maxPageLimit: 2 },
    });
    assert.deepEqual(defaultLimit.rows, [{ name: 'Ada' }, { name: 'Ben' }]);
    assert.equal(typeof defaultLimit.nextCursor, 'string');

    const first = db.queryGraphRows({
      nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
      return: [{ expr: { property: { alias: 'n', key: 'name' } }, as: 'name' }],
      orderBy: [{ expr: { property: { alias: 'n', key: 'rank' } }, direction: 'asc' }],
      skip: 1,
      limit: 1,
    });
    assert.deepEqual(first.rows, [{ name: 'Ben' }]);
    assert.equal(typeof first.nextCursor, 'string');

    const second = db.queryGraphRows({
      nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
      return: [{ expr: { property: { alias: 'n', key: 'name' } }, as: 'name' }],
      orderBy: [{ expr: { property: { alias: 'n', key: 'rank' } }, direction: 'asc' }],
      cursor: first.nextCursor,
      limit: 2,
    });
    assert.deepEqual(second.rows, [{ name: 'Cy' }, { name: 'Dee' }]);

    assert.throws(
      () => db.queryGraphRows({ nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }], cursor: 'not-a-cursor', limit: 1 }),
      /invalid cursor/i
    );
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        return: [{ expr: { property: { alias: 'n', key: 'rank' } }, as: 'rank' }],
        orderBy: [{ expr: { property: { alias: 'n', key: 'rank' } }, direction: 'asc' }],
        cursor: first.nextCursor,
        limit: 1,
      }),
      /invalid cursor|fingerprint/i
    );
  });

  it('parses params, lists, maps, and reports graph-row explain facts', async () => {
    const result = db.queryGraphRows({
      nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
      where: { op: '=', left: { property: { alias: 'n', key: 'status' } }, right: { param: 'status' } },
      return: [
        { expr: { property: { alias: 'n', key: 'name' } }, as: 'name' },
        { expr: { list: [{ param: 'status' }, { map: { nested: { param: 'status' } } }] }, as: 'payload' },
      ],
      orderBy: [{ expr: { property: { alias: 'n', key: 'rank' } } }],
      params: { status: 'active' },
      limit: 1,
    });
    assert.deepEqual(result.rows, [{ name: 'Ada', payload: ['active', { nested: 'active' }] }]);

    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        where: { nested: 1 },
        limit: 1,
      }),
      /expression object must contain exactly one known expression tag/i
    );
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        where: { param: 'status', binding: 'n' },
        params: { status: 'active' },
        limit: 1,
      }),
      /expression object must contain exactly one known expression tag/i
    );
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        where: { param: null },
        params: { status: 'active' },
        limit: 1,
      }),
      /param must be a string/i
    );
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        return: [{ expr: { binding: 'n' }, as: 7 }],
        limit: 1,
      }),
      /graph row return\[0\] as must be a string/i
    );
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        return: [
          {
            expr: { binding: 'n' },
            as: 'n',
            projection: {
              element: 'full',
              selected: { node: { id: true } },
            },
          },
        ],
        limit: 1,
      }),
      /projection must contain exactly one of 'element' or 'selected'/i
    );
    assert.throws(
      () => db.queryGraphRows({
        nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        where: { nodeField: { alias: 'n', field: 'id', extra: true } },
        limit: 1,
      }),
      /does not accept field 'extra'/i
    );

    const explain = await db.explainGraphRowsAsync({
      nodes: [
        { alias: 'a', ids: [ids.ada] },
        { alias: 'c', ids: [ids.cy] },
        { alias: 'm', labelFilter: nodeLabels('Person') },
      ],
      pieces: [
        {
          kind: 'variableLength',
          pathAlias: 'p',
          fromAlias: 'a',
          toAlias: 'c',
          labelFilter: ['KNOWS'],
          minHops: 1,
          maxHops: 2,
        },
        {
          kind: 'optional',
          pieces: [{ kind: 'edge', alias: 'r', fromAlias: 'c', toAlias: 'm', labelFilter: ['KNOWS'] }],
        },
      ],
      return: [{ expr: { binding: 'p' }, as: 'p' }],
      limit: 10,
      options: { includePlan: true },
    });
    const explainText = JSON.stringify(explain);
    assert.equal(explain.projection.outputMode, 'ids');
    assert.match(explainText, /VariableLength|variable/i);
    assert.match(explainText, /Optional|optional/i);
    assert.equal(explain.cursor.codecImplemented, true);
  });

  it('runs structured graph pipeline queries through sync and async connector APIs', async () => {
    const pipeline = {
      stages: [
        {
          kind: 'match',
          nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        },
        {
          kind: 'project',
          projectKind: 'with',
          items: [
            { expr: { property: { alias: 'n', key: 'name' } }, as: 'name' },
            { expr: { property: { alias: 'n', key: 'rank' } }, as: 'rank' },
            { expr: { property: { alias: 'n', key: 'status' } }, as: 'status' },
          ],
          where: { op: '=', left: { binding: 'status' }, right: 'active' },
          orderBy: [{ expr: { binding: 'rank' }, direction: 'desc' }],
          limit: 3,
        },
        {
          kind: 'project',
          projectKind: 'return',
          items: [
            { expr: { binding: 'name' }, as: 'name' },
            { expr: { op: '+', left: { binding: 'rank' }, right: 10 }, as: 'score' },
          ],
          orderBy: [{ expr: { binding: 'score' }, direction: 'desc' }],
        },
      ],
      limit: 10,
      options: { includePlan: true, profile: true },
    };

    const result = db.queryGraphPipeline(pipeline);
    assert.deepEqual(result.columns, ['name', 'score']);
    assert.deepEqual(result.rows, [
      { name: 'Cy', score: 13 },
      { name: 'Ben', score: 12 },
    ]);
    assert.equal(result.nextCursor, null);
    assert.equal(result.stats.rowsReturned, 2);
    assert.equal(result.plan.stages.length, 3);
    assert.equal(result.plan.caps.maxPipelineRows, 65536);

    const compact = await db.queryGraphPipelineAsync({
      ...pipeline,
      output: { compactRows: true },
    });
    assert.deepEqual(compact.rows, [
      ['Cy', 13],
      ['Ben', 12],
    ]);

    const explain = await db.explainGraphPipelineAsync(pipeline);
    assert.deepEqual(explain.columns, ['name', 'score']);
    assert.equal(explain.stages.length, 3);
    assert.equal(explain.projection.compactRows, false);
  });

  it('runs structured graph pipeline aggregate projections', () => {
    const result = db.queryGraphPipeline({
      stages: [
        {
          kind: 'match',
          nodes: [{ alias: 'n', labelFilter: nodeLabels('Person') }],
        },
        {
          kind: 'return',
          items: [
            { expr: { aggregate: { function: 'count' } }, as: 'people' },
            { expr: { aggregate: { function: 'collect', arg: { property: { alias: 'n', key: 'status' } }, distinct: true } }, as: 'statuses' },
          ],
        },
      ],
      limit: 10,
      options: { includePlan: true },
    });

    assert.equal(result.rows[0].people, 4);
    assert.deepEqual(new Set(result.rows[0].statuses), new Set(['active', 'inactive']));
    assert.equal(result.plan.stats.groups, 1);
  });
});
