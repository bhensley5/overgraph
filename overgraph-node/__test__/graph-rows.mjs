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

function graphRowPlanDetail(result, kind) {
  const node = result.plan?.plan.find(candidate => candidate.kind === kind);
  assert.ok(node, `expected ${kind} in graph-row plan`);
  return node.detail;
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

  it('keeps ordered adjacency pages exact across active and flushed public execution', () => {
    const localTmpDir = mkdtempSync(join(tmpdir(), 'overgraph-graph-row-node-adjacency-'));
    const localDb = OverGraph.open(join(localTmpDir, 'db'), {
      walSyncMode: 'group-commit',
      groupCommitIntervalMs: 1,
    });
    try {
      const expected = [];
      for (let ordinal = 0; ordinal < 1_000; ordinal += 1) {
        const sourceId = localDb.upsertNode(
          'ConnectorAdjacencySource',
          `connector-adjacency-source-${ordinal.toString().padStart(4, '0')}`
        );
        const targetId = localDb.upsertNode(
          'ConnectorAdjacencyTarget',
          `connector-adjacency-target-${ordinal.toString().padStart(4, '0')}`
        );
        const edgeId = localDb.upsertEdge(
          sourceId,
          targetId,
          'CONNECTOR_ADJACENCY_EDGE'
        );
        expected.push({ source_id: sourceId, edge_id: edgeId, target_id: targetId });
      }

      const request = {
        nodes: [{ alias: 'source' }, { alias: 'target' }],
        pieces: [{
          kind: 'edge',
          alias: 'edge',
          fromAlias: 'source',
          toAlias: 'target',
          labelFilter: ['CONNECTOR_ADJACENCY_EDGE'],
        }],
        return: [
          { expr: { binding: 'source' }, as: 'source_id' },
          { expr: { binding: 'edge' }, as: 'edge_id' },
          { expr: { binding: 'target' }, as: 'target_id' },
        ],
        limit: 10,
        options: {
          includePlan: true,
          maxFrontier: 1_024,
          maxIntermediateBindings: 1_024,
        },
      };

      const active = localDb.queryGraphRows(request);
      assert.deepEqual(active.rows, expected.slice(0, 10));
      assert.equal(typeof active.nextCursor, 'string');
      assert.match(graphRowPlanDetail(active, 'GraphRowSourceRead'), /choice=EdgePullChunk/);
      assert.match(graphRowPlanDetail(active, 'ChunkedRowProductionRuntime'), /source=EdgePull\{/);
      assert.ok(
        active.plan.plan.some(node => (
          node.kind === 'GraphRowPlanAlternative'
          && /kind=AdjacencyPull/.test(node.detail)
          && /reason=mutable_active_memtable/.test(node.detail)
        )),
        'expected active adjacency alternative rejection with mutable_active_memtable'
      );

      const pinnedRequest = { ...request, atEpoch: active.stats.effectiveAtEpoch };
      localDb.flush();
      const first = localDb.queryGraphRows(pinnedRequest);
      assert.deepEqual(first.rows, expected.slice(0, 10));
      assert.equal(first.nextCursor, active.nextCursor);
      assert.equal(first.plan.fingerprint, active.plan.fingerprint);
      assert.equal(first.stats.effectiveAtEpoch, active.stats.effectiveAtEpoch);
      assert.match(graphRowPlanDetail(first, 'GraphRowSourceRead'), /choice=AdjacencyPullChunk/);
      const firstRuntime = graphRowPlanDetail(first, 'ChunkedRowProductionRuntime');
      for (const fact of [
        /source=AdjacencyPull\{/,
        /source_order=logical_from_group_asc/,
        /proof_boundary=completed_owner_group/,
        /early_exit=true/,
        /cursor_seek=none/,
      ]) {
        assert.match(firstRuntime, fact);
      }

      const repeatedFirst = localDb.queryGraphRows(pinnedRequest);
      assert.deepEqual(repeatedFirst.rows, first.rows);
      assert.equal(repeatedFirst.nextCursor, first.nextCursor);
      assert.equal(repeatedFirst.plan.fingerprint, first.plan.fingerprint);
      assert.equal(repeatedFirst.stats.effectiveAtEpoch, first.stats.effectiveAtEpoch);

      const cursorRequest = { ...pinnedRequest, cursor: first.nextCursor };
      const second = localDb.queryGraphRows(cursorRequest);
      assert.deepEqual(second.rows, expected.slice(10, 20));
      assert.equal(typeof second.nextCursor, 'string');
      assert.notEqual(second.nextCursor, first.nextCursor);
      assert.equal(second.plan.fingerprint, first.plan.fingerprint);
      assert.equal(second.stats.effectiveAtEpoch, first.stats.effectiveAtEpoch);
      assert.equal(second.plan.cursor.supplied, true);
      assert.match(graphRowPlanDetail(second, 'GraphRowSourceRead'), /choice=AdjacencyPullChunk/);
      const secondRuntime = graphRowPlanDetail(second, 'ChunkedRowProductionRuntime');
      assert.match(secondRuntime, /source=AdjacencyPull\{/);
      assert.match(secondRuntime, /source_order=logical_from_group_asc/);
      assert.match(secondRuntime, /proof_boundary=completed_owner_group/);
      assert.match(secondRuntime, /early_exit=true/);
      assert.match(secondRuntime, /cursor_seek=anchor_ge:/);

      const repeatedSecond = localDb.queryGraphRows(cursorRequest);
      assert.deepEqual(repeatedSecond.rows, second.rows);
      assert.equal(repeatedSecond.nextCursor, second.nextCursor);
      assert.equal(repeatedSecond.plan.fingerprint, second.plan.fingerprint);
      assert.equal(repeatedSecond.stats.effectiveAtEpoch, second.stats.effectiveAtEpoch);
    } finally {
      closeDir(localDb, localTmpDir);
    }
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
