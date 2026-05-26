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
    assert.ok(fullScan.plan.rowOps.includes('sort'));
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

    assert.equal(syncExplain.target, 'graph_row_query');
    assert.ok(syncExplain.rowOps.includes('sort'));
    assert.deepEqual(asyncExplain.columns, ['n.name']);
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
