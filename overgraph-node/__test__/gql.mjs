import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

function closeDir(db, tmpDir) {
  db?.close();
  rmSync(tmpDir, { recursive: true, force: true });
}

function seed(db) {
  const ada = db.upsertNode('Person', 'ada', {
    props: { name: 'Ada', status: 'active', rank: 2, group: 'core' },
    denseVector: [0.1, 0.2, 0.3],
    sparseVector: [{ dimension: 7, value: 1.5 }],
  });
  const ben = db.upsertNode('Person', 'ben', {
    props: { name: 'Ben', status: 'active', rank: 1, group: 'core' },
  });
  const cy = db.upsertNode('Person', 'cy', {
    props: { name: 'Cy', status: 'inactive', rank: 3, group: 'ops' },
  });
  const acme = db.upsertNode('Company', 'acme', {
    props: { name: 'Acme' },
  });
  const worksAt = db.upsertEdge(ada, acme, 'WORKS_AT', {
    props: { role: 'engineer', since: 2020 },
  });
  return { ada, ben, cy, acme, worksAt };
}

function approxArray(actual, expected) {
  assert.equal(actual.length, expected.length);
  for (let i = 0; i < actual.length; i += 1) {
    assert.ok(Math.abs(actual[i] - expected[i]) < 0.00001);
  }
}

describe('GQL connector API', () => {
  let tmpDir;
  let db;
  let ids;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-gql-node-'));
    db = OverGraph.open(join(tmpDir, 'db'), {
      walSyncMode: 'immediate',
      denseVector: { dimension: 3 },
    });
    ids = seed(db);
  });

  after(() => closeDir(db, tmpDir));

  it('runs sync GQL queries with params and byte round trips', () => {
    const result = db.executeGql(
      `MATCH (n:Person {name: $name})
       RETURN $nil AS nil, $flag AS flag, $neg AS neg, $pos AS pos,
              $float AS float, $text AS text, $blob AS blob,
              $list AS list, $map AS map`,
      {
        name: 'Ada',
        nil: null,
        flag: true,
        neg: -7,
        pos: 9,
        float: 1.25,
        text: 'ok',
        blob: Buffer.from([1, 2, 3]),
        list: [false, 4, 'x'],
        map: { nested: 'value', bytes: Buffer.from('b') },
      }
    );

    assert.equal(result.kind, 'query');
    assert.equal(result.nextCursor, null);
    assert.equal(result.mutationStats, null);
    assert.equal(result.plan, null);
    assert.deepEqual(result.columns, ['nil', 'flag', 'neg', 'pos', 'float', 'text', 'blob', 'list', 'map']);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].nil, null);
    assert.equal(result.rows[0].flag, true);
    assert.equal(result.rows[0].neg, -7);
    assert.equal(result.rows[0].pos, 9);
    assert.equal(result.rows[0].float, 1.25);
    assert.equal(result.rows[0].text, 'ok');
    assert.deepEqual(result.rows[0].blob, Buffer.from([1, 2, 3]));
    assert.deepEqual(result.rows[0].list, [false, 4, 'x']);
    assert.deepEqual(result.rows[0].map.bytes, Buffer.from('b'));
  });

  it('runs async GQL queries and preserves compact row parity', async () => {
    const query = `MATCH (n:Person)
                   RETURN n.name AS name, n.rank AS rank
                   ORDER BY n.rank SKIP 1 LIMIT 1`;
    const objectRows = db.executeGql(query);
    const compactRows = await db.executeGqlAsync(query, null, { compactRows: true });

    assert.deepEqual(objectRows.rows, [{ name: 'Ada', rank: 2 }]);
    assert.deepEqual(compactRows.columns, objectRows.columns);
    assert.deepEqual(compactRows.rows, [['Ada', 2]]);
    assert.equal(objectRows.stats.rowsReturned, compactRows.stats.rowsReturned);
    assert.equal(objectRows.kind, 'query');
    assert.equal(compactRows.kind, 'query');
  });

  it('returns nodes and edges without vectors by default and with vectors on opt-in', () => {
    const defaultNode = db.executeGql(
      "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n"
    ).rows[0].n;
    assert.equal(defaultNode.id, ids.ada);
    assert.deepEqual(defaultNode.labels, ['Person']);
    assert.equal(defaultNode.props.name, 'Ada');
    assert.equal(defaultNode.denseVector, undefined);
    assert.equal(defaultNode.sparseVector, undefined);

    const vectorNode = db.executeGql(
      "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n",
      null,
      { includeVectors: true }
    ).rows[0].n;
    approxArray(vectorNode.denseVector, [0.1, 0.2, 0.3]);
    assert.deepEqual(vectorNode.sparseVector, [{ dimension: 7, value: 1.5 }]);

    const edge = db.executeGql(
      "MATCH (a:Person)-[r:WORKS_AT]->(c:Company) WHERE a.name = 'Ada' RETURN r"
    ).rows[0].r;
    assert.equal(edge.id, ids.worksAt);
    assert.equal(edge.from, ids.ada);
    assert.equal(edge.to, ids.acme);
    assert.equal(edge.label, 'WORKS_AT');
    assert.equal(edge.props.role, 'engineer');
  });

  it('enforces caps, full-scan opt-in, row ops, and profile stats through connectors', () => {
    assert.throws(
      () => db.executeGql('MATCH (n) RETURN id(n) AS id'),
      /full[- ]scan|allow_full_scan/i
    );

    const fullScan = db.executeGql(
      'MATCH (n) RETURN id(n) AS id ORDER BY id(n) LIMIT 10',
      null,
      { allowFullScan: true, includePlan: true, profile: true }
    );
    assert.deepEqual(fullScan.rows.map(row => row.id).sort((a, b) => a - b), [
      ids.ada,
      ids.ben,
      ids.cy,
      ids.acme,
    ].sort((a, b) => a - b));
    assert.equal(fullScan.plan.caps.allowFullScan, true);
    assert.equal(fullScan.plan.kind, 'query');
    assert.ok(fullScan.plan.read.rowOps.includes('sort'));
    assert.equal(fullScan.plan.mutation, null);
    assert.equal(typeof fullScan.stats.elapsedUs, 'number');
    assert.equal(fullScan.stats.rowsReturned, 4);

    assert.throws(
      () => db.executeGql('MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 100001'),
      /maxSkip|skip/i
    );

    const cappedExplain = db.explainGql(
      'MATCH (n:Person) RETURN id(n) LIMIT 1',
      null,
      {
        maxQueryBytes: 128,
        maxParamBytes: 9,
        maxAstDepth: 4,
        maxLiteralItems: 3,
      }
    );
    assert.equal(cappedExplain.caps.maxQueryBytes, 128);
    assert.equal(cappedExplain.caps.maxParamBytes, 9);
    assert.equal(cappedExplain.caps.maxAstDepth, 4);
    assert.equal(cappedExplain.caps.maxLiteralItems, 3);

    const unusedOversized = db.executeGql(
      'MATCH (n:Person) RETURN id(n) LIMIT 1',
      { unused: [1, 2, 3] },
      { maxLiteralItems: 1 }
    );
    assert.equal(unusedOversized.rows.length, 1);

    assert.throws(
      () => db.executeGql(
        'MATCH (n:Person) RETURN $ids LIMIT 0',
        { ids: [1, 2] },
        { maxLiteralItems: 1 }
      ),
      /maxLiteralItems|max_literal_items/i
    );
    assert.throws(
      () => db.executeGql(
        'MATCH (n:Person) RETURN $payload LIMIT 0',
        { payload: [[1]] },
        { maxAstDepth: 1 }
      ),
      /maxAstDepth|max_ast_depth/i
    );
    assert.throws(
      () => db.executeGql(
        'MATCH (n:Person) RETURN $payload LIMIT 0',
        { payload: 'toolong' },
        { maxParamBytes: 4 }
      ),
      /maxParamBytes|max_param_bytes/i
    );
    assert.throws(
      () => db.executeGql(
        'MATCH (n:Person) RETURN $payload LIMIT 0',
        { payload: Buffer.from([1, 2, 3]) },
        { maxParamBytes: 2 }
      ),
      /maxParamBytes|max_param_bytes/i
    );
    assert.throws(
      () => db.executeGql(
        'MATCH (n:Person) RETURN $payload LIMIT 0',
        { payload: { oversized: 1 } },
        { maxParamBytes: 4 }
      ),
      /maxParamBytes|max_param_bytes/i
    );

    const boundaryBytes = db.executeGql(
      'MATCH (n:Person) RETURN $payload AS payload LIMIT 1',
      { payload: { abc: 'de' } },
      { maxParamBytes: 5, maxAstDepth: 1, maxLiteralItems: 1 }
    );
    assert.deepEqual(boundaryBytes.rows[0].payload, { abc: 'de' });
  });

  it('explains sync and async GQL plans', async () => {
    const query = "MATCH (n:Person) WHERE n.status = 'active' RETURN n.name ORDER BY n.rank LIMIT 1";
    const syncExplain = db.explainGql(query, null, { includePlan: true, profile: true });
    const asyncExplain = await db.explainGqlAsync(query, null, { includePlan: true });

    assert.equal(syncExplain.kind, 'query');
    assert.equal(syncExplain.read.target, 'graph_row_query');
    assert.ok(syncExplain.read.rowOps.includes('sort'));
    assert.equal(syncExplain.mutation, null);
    assert.deepEqual(asyncExplain.columns, ['n.name']);
    assert.equal(asyncExplain.kind, 'query');
    assert.equal(asyncExplain.read.target, 'graph_row_query');
  });

  it('executes sync CREATE RETURN with mutation stats, bytes, and embedded plan', () => {
    const result = db.executeGql(
      `CREATE (n:NodeCreateReturn {key: 'created-one', name: $name, payload: $payload})
       RETURN n.key AS key, n.name AS name, n.payload AS payload, n`,
      { name: 'Created', payload: Buffer.from([9, 8, 7]) },
      { includePlan: true }
    );

    assert.equal(result.kind, 'mutation');
    assert.deepEqual(result.columns, ['key', 'name', 'payload', 'n']);
    assert.equal(result.nextCursor, null);
    assert.equal(result.rows.length, 1);
    assert.deepEqual(result.rows[0].payload, Buffer.from([9, 8, 7]));
    assert.equal(result.rows[0].n.key, 'created-one');
    assert.equal(result.rows[0].n.props.name, 'Created');
    assert.equal(result.mutationStats.rowsMatched, 1);
    assert.equal(result.mutationStats.mutationRows, 1);
    assert.equal(result.mutationStats.nodesCreated, 1);
    assert.equal(result.mutationStats.mutationOps, 1);
    assert.equal(result.plan.kind, 'mutation');
    assert.equal(result.plan.read, null);
    assert.equal(result.plan.mutation.usesWriteTxn, true);
    assert.deepEqual(result.plan.mutation.returnPlan.columns, result.columns);
  });

  it('executes SET and REMOVE RETURN row operations through sync GQL', () => {
    db.upsertNode('NodeSetRemoveReturn', 'a', {
      props: { rank: 1, group: 'old', status: 'old' },
    });
    db.upsertNode('NodeSetRemoveReturn', 'b', {
      props: { rank: 2, group: 'old', status: 'old' },
    });
    db.upsertNode('NodeSetRemoveReturn', 'c', {
      props: { rank: 3, group: 'old', status: 'old' },
    });

    const result = db.executeGql(
      `MATCH (n:NodeSetRemoveReturn)
       SET n.status = $status
       REMOVE n.group
       RETURN n.key AS key, n.status AS status, n.group AS group
       ORDER BY n.rank SKIP 1 LIMIT 1`,
      { status: 'new' }
    );

    assert.equal(result.kind, 'mutation');
    assert.deepEqual(result.rows, [{ key: 'b', status: 'new', group: null }]);
    assert.equal(result.mutationStats.rowsMatched, 3);
    assert.equal(result.mutationStats.mutationRows, 3);
    assert.equal(result.mutationStats.nodesUpdated, 3);
    assert.equal(result.mutationStats.propertiesSet, 3);
    assert.equal(result.mutationStats.propertiesRemoved, 3);
  });

  it('executes DELETE and DETACH DELETE without RETURN and reports stats', () => {
    const source = db.upsertNode('NodeDeleteSource', 'source');
    const target = db.upsertNode('NodeDeleteTarget', 'target');
    db.upsertEdge(source, target, 'DELETE_ME');

    const edgeDelete = db.executeGql(
      `MATCH (a:NodeDeleteSource)-[r:DELETE_ME]->(b:NodeDeleteTarget)
       DELETE r`
    );
    assert.equal(edgeDelete.kind, 'mutation');
    assert.deepEqual(edgeDelete.rows, []);
    assert.equal(edgeDelete.mutationStats.edgesDeleted, 1);
    assert.equal(edgeDelete.mutationStats.mutationOps, 1);

    const hub = db.upsertNode('NodeDetachDelete', 'hub');
    const leaf = db.upsertNode('NodeDetachDelete', 'leaf');
    db.upsertEdge(hub, leaf, 'DETACH_ME');

    const detachDelete = db.executeGql(
      `MATCH (n:NodeDetachDelete)
       WHERE n.key = 'hub'
       DETACH DELETE n`
    );
    assert.equal(detachDelete.kind, 'mutation');
    assert.deepEqual(detachDelete.rows, []);
    assert.equal(detachDelete.mutationStats.nodesDeleted, 1);
    assert.equal(detachDelete.mutationStats.edgesDeleted, 1);
    assert.equal(detachDelete.mutationStats.mutationOps, 2);
  });

  it('runs async mutation execution once and returns the final shape', async () => {
    const result = await db.executeGqlAsync(
      `CREATE (n:NodeAsyncMutation {key: 'once', name: 'Async'})
       RETURN n.name AS name`
    );

    assert.equal(result.kind, 'mutation');
    assert.deepEqual(result.rows, [{ name: 'Async' }]);
    assert.equal(result.mutationStats.nodesCreated, 1);

    const readBack = db.executeGql(
      "MATCH (n:NodeAsyncMutation) WHERE n.key = 'once' RETURN n.name AS name"
    );
    assert.deepEqual(readBack.rows, [{ name: 'Async' }]);

    db.upsertNode('NodeAsyncUpdate', 'target', {
      props: { status: 'old', drop: 'remove-me' },
    });
    const update = await db.executeGqlAsync(
      `MATCH (n:NodeAsyncUpdate)
       WHERE n.key = 'target'
       SET n.status = 'new'
       REMOVE n.drop
       RETURN n.status AS status, n.drop AS dropped`
    );
    assert.equal(update.kind, 'mutation');
    assert.deepEqual(update.rows, [{ status: 'new', dropped: null }]);
    assert.equal(update.mutationStats.nodesUpdated, 1);
    assert.equal(update.mutationStats.propertiesSet, 1);
    assert.equal(update.mutationStats.propertiesRemoved, 1);

    const hub = db.upsertNode('NodeAsyncDetach', 'hub');
    const leaf = db.upsertNode('NodeAsyncDetach', 'leaf');
    db.upsertEdge(hub, leaf, 'ASYNC_DETACH');
    const detached = await db.executeGqlAsync(
      `MATCH (n:NodeAsyncDetach)
       WHERE n.key = 'hub'
       DETACH DELETE n`
    );
    assert.equal(detached.kind, 'mutation');
    assert.deepEqual(detached.rows, []);
    assert.equal(detached.mutationStats.nodesDeleted, 1);
    assert.equal(detached.mutationStats.edgesDeleted, 1);

    await assert.rejects(
      db.executeGqlAsync("CREATE (n:NodeAsyncReadOnly {key: 'blocked'})", null, { mode: 'readOnly' }),
      /read.?only|ReadOnly/i
    );
  });

  it('enforces readOnly mode and validates mode values before execution', () => {
    const read = db.executeGql(
      'MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank LIMIT 1',
      null,
      { mode: 'readOnly' }
    );
    assert.equal(read.kind, 'query');
    assert.deepEqual(read.rows, [{ name: 'Ben' }]);

    assert.throws(
      () => db.executeGql("CREATE (n:NodeReadOnly {key: 'blocked'})", null, { mode: 'readOnly' }),
      /read.?only|ReadOnly/i
    );
    assert.throws(
      () => db.executeGql('MATCH (n:Person) RETURN n LIMIT 1', null, { mode: 'readonly' }),
      /mode.*auto.*readOnly/i
    );
  });

  it('forwards mutation, order, and path caps through Node options', () => {
    assert.throws(
      () => db.executeGql("CREATE (n:NodeCapMutationRows {key: 'row-cap'})", null, { maxMutationRows: 0 }),
      /maxMutationRows|max_mutation_rows/i
    );
    assert.throws(
      () => db.executeGql("CREATE (n:NodeCapMutationOps {key: 'op-cap'})", null, { maxMutationOps: 0 }),
      /maxMutationOps|max_mutation_ops/i
    );
    assert.throws(
      () => db.executeGql(
        'MATCH (n:Person) SET n.cap_probe = true RETURN n.name ORDER BY n.name',
        null,
        { maxOrderMaterialization: 1 }
      ),
      /maxOrderMaterialization|max_order_materialization|order materialization/i
    );
    assert.throws(
      () => db.executeGql(
        `MATCH p = (a:Person)-[:WORKS_AT*1..1]->(c:Company)
         WHERE a.name = 'Ada'
         RETURN p`,
        null,
        { maxPathHops: 0 }
      ),
      /maxPathHops|max_path_hops|path hops|upper bound/i
    );
  });

  it('returns compact rows and vectors for mutation RETURN when requested', () => {
    const compact = db.executeGql(
      `CREATE (n:NodeCompactMutation {key: 'compact', name: 'Compact'})
       RETURN n.key AS key, n.name AS name`,
      null,
      { compactRows: true }
    );
    assert.equal(compact.kind, 'mutation');
    assert.deepEqual(compact.columns, ['key', 'name']);
    assert.deepEqual(compact.rows, [['compact', 'Compact']]);

    const vector = db.executeGql(
      `MATCH (n:Person {name: 'Ada'})
       SET n.status = 'vector-return'
       RETURN n`,
      null,
      { includeVectors: true }
    ).rows[0].n;
    approxArray(vector.denseVector, [0.1, 0.2, 0.3]);
    assert.deepEqual(vector.sparseVector, [{ dimension: 7, value: 1.5 }]);
  });

  it('explains mutations without side effects', async () => {
    const explain = db.explainGql(
      `MATCH (n:Person {name: 'Ada'})
       SET n.planned = 'explained'
       RETURN n.planned AS planned`
    );
    assert.equal(explain.kind, 'mutation');
    assert.deepEqual(explain.columns, ['planned']);
    assert.equal(explain.read.target, 'graph_row_query');
    assert.equal(explain.mutation.usesTransactionSnapshot, true);
    assert.equal(explain.mutation.usesWriteTxn, true);
    assert.equal(explain.mutation.atomicCommit, true);
    assert.ok(explain.mutation.operations.some(op => op.op === 'SET PROPERTY'));
    assert.deepEqual(explain.mutation.returnPlan.columns, ['planned']);

    const unchanged = db.executeGql(
      "MATCH (n:Person {name: 'Ada'}) RETURN n.planned AS planned"
    );
    assert.deepEqual(unchanged.rows, [{ planned: null }]);

    const asyncExplain = await db.explainGqlAsync(
      "CREATE (n:NodeAsyncExplain {key: 'planned'}) RETURN n.key AS key"
    );
    assert.equal(asyncExplain.kind, 'mutation');
    assert.equal(asyncExplain.read, null);
    assert.equal(asyncExplain.mutation.returnPlan.columns[0], 'key');
  });

  it('surfaces volatile mutation RETURN ORDER BY rejection through Node', () => {
    assert.throws(
      () => db.executeGql(
        "CREATE (n:NodeVolatileOrder {key: 'bad-order'}) RETURN n.key ORDER BY n.id"
      ),
      /ORDER BY|commit|metadata|before commit|volatile/i
    );
  });

  it('round-trips GQL optional, path, and cursor results through Node', async () => {
    const optional = db.executeGql(
      `MATCH (n:Person)
       OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company)
       RETURN n.name AS name, c.name AS company
       ORDER BY n.rank`,
      null,
      { allowFullScan: true }
    );
    assert.deepEqual(optional.rows, [
      { name: 'Ben', company: null },
      { name: 'Ada', company: 'Acme' },
      { name: 'Cy', company: null },
    ]);

    const path = db.executeGql(
      `MATCH p = (a:Person)-[:WORKS_AT*1..1]->(c:Company)
       WHERE a.name = 'Ada'
       RETURN p`
    ).rows[0].p;
    assert.deepEqual(path.nodeIds, [ids.ada, ids.acme]);
    assert.deepEqual(path.edgeIds, [ids.worksAt]);
    assert.deepEqual(path.nodes.map(node => node.id), [ids.ada, ids.acme]);
    assert.equal(path.nodes[0].denseVector, undefined);

    const cursorQuery = 'MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank LIMIT 3';
    const first = db.executeGql(
      cursorQuery,
      null,
      { maxRows: 1 }
    );
    assert.deepEqual(first.rows, [{ name: 'Ben' }]);
    assert.equal(typeof first.nextCursor, 'string');
    const second = db.executeGql(
      cursorQuery,
      null,
      { cursor: first.nextCursor, maxRows: 2, maxCursorBytes: 65536 }
    );
    assert.deepEqual(second.rows, [{ name: 'Ada' }, { name: 'Cy' }]);

    await assert.rejects(
      db.executeGqlAsync('MATCH (n:Person) RETURN n ORDER BY labels(n)'),
      /ORDER BY|scalar|labels/i
    );
  });

  it('rejects unsupported GQL syntax through connector APIs', async () => {
    assert.throws(
      () => db.executeGql('MATCH (n:Person)-[*]->(m) RETURN n'),
      /unsupported|variable|upper bound|unbounded/i
    );
    await assert.rejects(
      db.executeGqlAsync('MATCH (n:Person) RETURN n ORDER BY labels(n)'),
      /ORDER BY|scalar|labels/i
    );
  });
});
