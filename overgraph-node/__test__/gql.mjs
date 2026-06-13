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

function byName(row) {
  return row.name;
}

function propertyIndexRowField(key) {
  return { source: 'property', key };
}

function metadataIndexRowField(field) {
  return { source: 'metadata', field };
}

function propertyIndexExplainField(key) {
  return { source: 'property', key, field: null };
}

function metadataIndexExplainField(field) {
  return { source: 'metadata', key: null, field };
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
    assert.equal(result.schemaStats, null);
    assert.equal(result.indexStats, null);
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

  it('executes and explains GQL schema statements with tagged schema rows', () => {
    const localTmpDir = mkdtempSync(join(tmpdir(), 'overgraph-gql-node-schema-'));
    const localDb = OverGraph.open(join(localTmpDir, 'db'), { walSyncMode: 'immediate' });
    try {
      const alter = localDb.executeGql(
        `ALTER CURRENT GRAPH TYPE SET {
           NODE Tagged = {
             properties: {
               payload: {
                 enum_values: [
                   { type: 'uint', value: '18446744073709551615' },
                   { type: 'bytes', value: [0, 1, 255] }
                 ]
               }
             }
           }
         }`,
        null,
        { includePlan: true }
      );
      assert.equal(alter.kind, 'schema');
      assert.equal(alter.mutationStats, null);
      assert.equal(alter.indexStats, null);
      assert.equal(alter.schemaStats.operation, 'alter_graph_type_set');
      assert.equal(alter.schemaStats.targetsPublished, 1);
      assert.equal(alter.plan.kind, 'schema');
      assert.equal(alter.plan.read, null);
      assert.equal(alter.plan.mutation, null);
      assert.equal(alter.plan.index, null);
      assert.equal(alter.plan.schema.operation, 'alter_graph_type_set');
      assert.equal(alter.plan.schema.usesCoreWriteQueue, true);

      const show = localDb.executeGql('SHOW CURRENT GRAPH TYPE');
      assert.equal(show.kind, 'schema');
      assert.equal(show.schemaStats.operation, 'show_current_graph_type');
      assert.equal(show.rows[0].target_kind, 'node');
      const enumValues = show.rows[0].schema.properties.payload.enum_values;
      assert.deepEqual(enumValues[0], {
        type: 'uint',
        value: '18446744073709551615',
      });
      assert.deepEqual(enumValues[1], {
        type: 'bytes',
        value: [0, 1, 255],
      });

      localDb.upsertNode('NeedsName', 'missing');
      const check = localDb.executeGql(
        `CHECK CURRENT GRAPH TYPE ADD {
           NODE NeedsName = {
             properties: {
               name: { required: true, nullable: false, types: ['string'] }
             }
           }
         }`
      );
      assert.equal(check.kind, 'schema');
      assert.equal(check.schemaStats.operation, 'check_graph_type_add');
      assert.equal(check.schemaStats.violationCount, 1);
      assert.equal(check.rows[0].violations[0].target.id.type, 'uint');
      assert.equal(check.rows[0].violations[0].target.id.value, '1');
      assert.equal(localDb.getNodeSchema('NeedsName'), null);

      const explain = localDb.explainGql(
        `CHECK CURRENT GRAPH TYPE SET {
           NODE Tagged = { properties: {} }
         }`
      );
      assert.equal(explain.kind, 'schema');
      assert.equal(explain.read, null);
      assert.equal(explain.mutation, null);
      assert.equal(explain.index, null);
      assert.equal(explain.schema.operation, 'check_graph_type_set');
      assert.equal(explain.schema.sideEffectFree, true);

      const read = localDb.executeGql('MATCH (n:NeedsName) RETURN elementKey(n) AS key');
      assert.equal(read.kind, 'query');
      assert.equal(read.schemaStats, null);
      assert.equal(read.indexStats, null);

      const mutation = localDb.executeGql(
        "CREATE (n:Unconstrained {elementKey: 'one'}) RETURN elementKey(n) AS key"
      );
      assert.equal(mutation.kind, 'mutation');
      assert.equal(mutation.schemaStats, null);
      assert.equal(mutation.indexStats, null);
    } finally {
      closeDir(localDb, localTmpDir);
    }
  });

  it('executes and explains GQL property-index DDL through connectors', async () => {
    const localTmpDir = mkdtempSync(join(tmpdir(), 'overgraph-gql-node-index-'));
    const localDb = OverGraph.open(join(localTmpDir, 'db'), { walSyncMode: 'immediate' });
    try {
      const personIndex =
        'CREATE PROPERTY INDEX FOR (n:IndexPerson) ON (n.status) KIND EQUALITY';
      const edgeIndex =
        'CREATE PROPERTY INDEX FOR ()-[r:INDEX_WORKS_AT]-() ON (r.since) KIND RANGE';
      const compoundIndex =
        'CREATE PROPERTY INDEX FOR (n:IndexPerson) ON (n.group, updatedAt(n)) KIND RANGE';
      const dropEdge =
        'DROP PROPERTY INDEX FOR ()-[r:INDEX_WORKS_AT]-() ON (r.since) KIND RANGE';

      const create = localDb.executeGql(personIndex);
      assert.equal(create.kind, 'index');
      assert.deepEqual(create.columns, [
        'operation',
        'target_kind',
        'label',
        'fields',
        'kind',
        'action',
        'state',
        'index_id',
        'last_error',
        'compound',
        'field_count',
      ]);
      assert.equal(create.mutationStats, null);
      assert.equal(create.schemaStats, null);
      assert.equal(create.indexStats.operation, 'create_property_index');
      assert.equal(create.indexStats.indexesEnsured, 1);
      assert.equal(create.indexStats.indexesDropped, 0);
      assert.equal(create.indexStats.indexesReturned, 0);
      assert.equal(create.indexStats.elapsedUs, null);
      assert.deepEqual(create.indexStats.warnings, []);
      assert.equal(create.rows[0].operation, 'create_property_index');
      assert.equal(create.rows[0].target_kind, 'node');
      assert.equal(create.rows[0].label, 'IndexPerson');
      assert.deepEqual(create.rows[0].fields, [propertyIndexRowField('status')]);
      assert.equal(create.rows[0].kind, 'equality');
      assert.equal(create.rows[0].action, 'ensured');
      assert.ok(['building', 'ready', 'failed'].includes(create.rows[0].state));
      assert.equal(typeof create.rows[0].index_id, 'number');
      assert.equal(create.rows[0].last_error, null);
      assert.equal(create.rows[0].compound, false);
      assert.equal(create.rows[0].field_count, 1);

      const planned = localDb.executeGql(personIndex, null, {
        includePlan: true,
        profile: true,
      });
      assert.equal(typeof planned.indexStats.elapsedUs, 'number');
      assert.equal(planned.plan.kind, 'index');
      assert.equal(planned.plan.read, null);
      assert.equal(planned.plan.mutation, null);
      assert.equal(planned.plan.schema, null);
      assert.equal(planned.plan.index.operation, 'create_property_index');
      assert.deepEqual(planned.plan.index.targets, [
        {
          targetKind: 'node',
          label: 'IndexPerson',
          fields: [propertyIndexExplainField('status')],
          kind: 'equality',
          action: 'ensure',
          compound: false,
        },
      ]);
      assert.equal(planned.plan.index.usesCoreWriteQueue, true);
      assert.equal(planned.plan.index.publishesManifest, true);
      assert.equal(planned.plan.index.createsLabels, true);
      assert.equal(planned.plan.index.schedulesBackgroundBuild, true);
      assert.equal(planned.plan.index.dropsIndexDataAsync, false);
      assert.equal(planned.plan.index.sideEffectFree, false);

      const createExplain = localDb.explainGql(personIndex);
      assert.equal(createExplain.kind, 'index');
      assert.equal(createExplain.read, null);
      assert.equal(createExplain.mutation, null);
      assert.equal(createExplain.schema, null);
      assert.equal(createExplain.index.operation, 'create_property_index');
      assert.deepEqual(createExplain.index.targets[0], planned.plan.index.targets[0]);

      const edgeCreate = localDb.executeGql(edgeIndex);
      assert.equal(edgeCreate.kind, 'index');
      assert.equal(edgeCreate.indexStats.operation, 'create_property_index');
      assert.equal(edgeCreate.indexStats.indexesEnsured, 1);
      assert.equal(edgeCreate.mutationStats, null);
      assert.equal(edgeCreate.schemaStats, null);

      const compoundCreate = localDb.executeGql(compoundIndex);
      assert.equal(compoundCreate.kind, 'index');
      assert.deepEqual(compoundCreate.rows[0].fields, [
        propertyIndexRowField('group'),
        metadataIndexRowField('updatedAt'),
      ]);
      assert.equal(compoundCreate.rows[0].compound, true);
      assert.equal(compoundCreate.rows[0].field_count, 2);

      const compoundExplain = localDb.explainGql(compoundIndex);
      assert.deepEqual(compoundExplain.index.targets[0], {
        targetKind: 'node',
        label: 'IndexPerson',
        fields: [propertyIndexExplainField('group'), metadataIndexExplainField('updatedAt')],
        kind: 'range',
        action: 'ensure',
        compound: true,
      });

      const show = localDb.executeGql('SHOW PROPERTY INDEXES');
      assert.equal(show.kind, 'index');
      assert.equal(show.indexStats.operation, 'show_property_indexes');
      assert.equal(show.indexStats.indexesReturned, 3);
      assert.deepEqual(
        show.rows.map(row => [row.target_kind, row.label, row.fields, row.kind, row.compound, row.field_count]),
        [
          ['node', 'IndexPerson', [propertyIndexRowField('group'), metadataIndexRowField('updatedAt')], 'range', true, 2],
          ['node', 'IndexPerson', [propertyIndexRowField('status')], 'equality', false, 1],
          ['edge', 'INDEX_WORKS_AT', [propertyIndexRowField('since')], 'range', false, 1],
        ]
      );
      assert.ok(show.rows.every(row => ['building', 'ready', 'failed'].includes(row.state)));
      assert.ok(show.rows.every(row => row.last_error === null));

      const showExplain = localDb.explainGql('SHOW PROPERTY INDEXES');
      assert.equal(showExplain.index.operation, 'show_property_indexes');
      assert.deepEqual(showExplain.index.targets, [
        {
          targetKind: 'property_index_catalog',
          label: null,
          fields: [],
          kind: null,
          action: 'show',
          compound: false,
        },
      ]);
      assert.equal(showExplain.index.usesCoreWriteQueue, false);
      assert.equal(showExplain.index.publishesManifest, false);
      assert.equal(showExplain.index.createsLabels, false);
      assert.equal(showExplain.index.schedulesBackgroundBuild, false);
      assert.equal(showExplain.index.dropsIndexDataAsync, false);
      assert.equal(showExplain.index.sideEffectFree, true);

      const dropExplain = localDb.explainGql(dropEdge);
      assert.equal(dropExplain.index.operation, 'drop_property_index');
      assert.deepEqual(dropExplain.index.targets, [
        {
          targetKind: 'edge',
          label: 'INDEX_WORKS_AT',
          fields: [propertyIndexExplainField('since')],
          kind: 'range',
          action: 'drop',
          compound: false,
        },
      ]);
      assert.equal(dropExplain.index.usesCoreWriteQueue, true);
      assert.equal(dropExplain.index.publishesManifest, true);
      assert.equal(dropExplain.index.createsLabels, false);
      assert.equal(dropExplain.index.schedulesBackgroundBuild, false);
      assert.equal(dropExplain.index.dropsIndexDataAsync, true);
      assert.equal(dropExplain.index.sideEffectFree, false);

      assert.throws(
        () => localDb.executeGql(personIndex, null, { mode: 'readOnly' }),
        /GQL index management is not allowed in ReadOnly mode/
      );
      assert.throws(
        () => localDb.executeGql(dropEdge, null, { mode: 'readOnly' }),
        /GQL index management is not allowed in ReadOnly mode/
      );
      const readOnlyShow = localDb.executeGql('SHOW PROPERTY INDEXES', null, {
        mode: 'readOnly',
      });
      assert.equal(readOnlyShow.rows.length, 3);
      assert.throws(
        () => localDb.executeGql('SHOW PROPERTY INDEXES', null, { cursor: 'locked' }),
        /GQL index statements do not accept cursors/
      );

      const asyncShow = await localDb.executeGqlAsync('SHOW PROPERTY INDEXES');
      assert.equal(asyncShow.kind, 'index');
      assert.equal(asyncShow.indexStats.operation, 'show_property_indexes');
      assert.equal(asyncShow.indexStats.indexesReturned, 3);
      const asyncExplain = await localDb.explainGqlAsync('SHOW PROPERTY INDEXES');
      assert.equal(asyncExplain.kind, 'index');
      assert.equal(asyncExplain.index.operation, 'show_property_indexes');
      assert.equal(asyncExplain.index.targets[0].targetKind, 'property_index_catalog');
    } finally {
      closeDir(localDb, localTmpDir);
    }
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
        maxPipelineRows: 11,
        maxGroups: 12,
        maxCollectItems: 13,
        maxUnionBranches: 2,
        maxSubqueryInvocations: 14,
        maxSubqueryDepth: 1,
        maxShortestPathPairs: 15,
      }
    );
    assert.equal(cappedExplain.caps.maxQueryBytes, 128);
    assert.equal(cappedExplain.caps.maxParamBytes, 9);
    assert.equal(cappedExplain.caps.maxAstDepth, 4);
    assert.equal(cappedExplain.caps.maxLiteralItems, 3);
    assert.equal(cappedExplain.caps.maxPipelineRows, 11);
    assert.equal(cappedExplain.caps.maxGroups, 12);
    assert.equal(cappedExplain.caps.maxCollectItems, 13);
    assert.equal(cappedExplain.caps.maxUnionBranches, 2);
    assert.equal(cappedExplain.caps.maxSubqueryInvocations, 14);
    assert.equal(cappedExplain.caps.maxSubqueryDepth, 1);
    assert.equal(cappedExplain.caps.maxShortestPathPairs, 15);
    assert.equal(cappedExplain.index, null);

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

  it('executes Phase 34 WITH, rich expressions, DISTINCT, aggregation, and compact rows', () => {
    const rich = db.executeGql(
      `MATCH (n:Person)
       WITH n,
            lower(trim(n.name)) AS slug,
            n.rank + 2 AS boosted,
            -n.rank AS negRank,
            CASE WHEN n.rank > 1 THEN upper(n.status) ELSE 'LOW' END AS bucket,
            {name: n.name, scores: [n.rank, n.rank + 1], active: n.status = 'active'} AS payload
       WHERE slug STARTS WITH 'a'
       RETURN n.name AS name, slug, boosted, negRank, bucket, payload`
    );
    assert.deepEqual(rich.columns, ['name', 'slug', 'boosted', 'negRank', 'bucket', 'payload']);
    assert.deepEqual(rich.rows, [{
      name: 'Ada',
      slug: 'ada',
      boosted: 4,
      negRank: -2,
      bucket: 'ACTIVE',
      payload: { active: true, name: 'Ada', scores: [2, 3] },
    }]);

    const distinct = db.executeGql(
      'MATCH (n:Person) RETURN DISTINCT n.group AS group ORDER BY group'
    );
    assert.deepEqual(distinct.rows, [{ group: 'core' }, { group: 'ops' }]);

    const compactAgg = db.executeGql(
      `MATCH (n:Person)
       WITH n.group AS group, count(*) AS total, sum(n.rank) AS sumRank,
            avg(n.rank) AS avgRank, collect(n.name) AS names
       RETURN group, total, sumRank, avgRank, names
       ORDER BY group`,
      null,
      { compactRows: true }
    );
    assert.deepEqual(compactAgg.columns, ['group', 'total', 'sumRank', 'avgRank', 'names']);
    assert.equal(compactAgg.kind, 'query');
    assert.deepEqual(compactAgg.rows[0].slice(0, 4), ['core', 2, 3, 1.5]);
    assert.deepEqual([...compactAgg.rows[0][4]].sort(), ['Ada', 'Ben']);
    assert.deepEqual(compactAgg.rows[1], ['ops', 1, 3, 3, ['Cy']]);

    const collectedNodeIds = db.executeGql(
      'MATCH (n:Person) RETURN collect(n) AS people',
      null,
      { includeVectors: true }
    ).rows[0].people;
    assert.deepEqual(
      [...collectedNodeIds].sort((a, b) => a - b),
      [ids.ada, ids.ben, ids.cy].sort((a, b) => a - b)
    );
  });

  it('executes Phase 34 UNION variants and read-only subqueries', () => {
    const unionAll = db.executeGql(
      `MATCH (n:Person) WHERE n.group = 'core' RETURN n.name AS name ORDER BY name
       UNION ALL
       MATCH (m:Person) WHERE m.status = 'active' RETURN m.name AS name ORDER BY name`
    );
    assert.deepEqual(unionAll.rows.map(byName), ['Ada', 'Ben', 'Ada', 'Ben']);

    const union = db.executeGql(
      `MATCH (n:Person) WHERE n.group = 'core' RETURN n.name AS name ORDER BY name
       UNION
       MATCH (m:Person) WHERE m.status = 'active' RETURN m.name AS name ORDER BY name`
    );
    assert.deepEqual(union.rows.map(byName), ['Ada', 'Ben']);

    const exists = db.executeGql(
      `MATCH (n:Person)
       WHERE EXISTS { MATCH (n)-[:WORKS_AT]->(c:Company) RETURN c }
       RETURN n.name AS name`
    );
    assert.deepEqual(exists.rows, [{ name: 'Ada' }]);

    const call = db.executeGql(
      `MATCH (n:Person)
       CALL { MATCH (n)-[:WORKS_AT]->(c:Company) RETURN c.name AS company }
       RETURN n.name AS name, company`
    );
    assert.deepEqual(call.rows, [{ name: 'Ada', company: 'Acme' }]);
  });

  it('returns shortest path objects and helper values through Node', () => {
    const result = db.executeGql(
      `MATCH (a:Person) WHERE a.name = 'Ada'
       WITH a
       MATCH (c:Company) WHERE c.name = 'Acme'
       WITH a, c
       MATCH p = shortestPath((a)-[:WORKS_AT*1..1]->(c))
       RETURN p,
              nodeIds(p) AS nodeIds,
              edgeIds(p) AS edgeIds,
              length(p) AS hops,
              nodes(p) AS nodeHelper,
              relationships(p) AS relationshipHelper,
              [p] AS pathList,
              {path: p, nodes: nodes(p), relationships: relationships(p)} AS nested`,
      null,
      { includePlan: true }
    );

    assert.equal(result.rows.length, 1);
    assert.equal(result.plan.read.target, 'graph_pipeline_query');
    assert.ok(result.plan.read.projection.some(item => item.includes('ShortestPath')));
    const row = result.rows[0];
    assert.deepEqual(row.p.nodeIds, [ids.ada, ids.acme]);
    assert.deepEqual(row.p.edgeIds, [ids.worksAt]);
    assert.deepEqual(row.p.nodes.map(node => node.id), [ids.ada, ids.acme]);
    assert.equal(row.p.edges[0].id, ids.worksAt);
    assert.deepEqual(row.nodeIds, [ids.ada, ids.acme]);
    assert.deepEqual(row.edgeIds, [ids.worksAt]);
    assert.equal(row.hops, 1);
    assert.deepEqual(row.nodeHelper, [ids.ada, ids.acme]);
    assert.deepEqual(row.relationshipHelper, [ids.worksAt]);
    assert.deepEqual(row.pathList[0].nodeIds, [ids.ada, ids.acme]);
    assert.deepEqual(row.pathList[0].edgeIds, [ids.worksAt]);
    assert.deepEqual(row.nested.path.nodeIds, [ids.ada, ids.acme]);
    assert.deepEqual(row.nested.path.edgeIds, [ids.worksAt]);
    assert.deepEqual(row.nested.nodes, [ids.ada, ids.acme]);
    assert.deepEqual(row.nested.relationships, [ids.worksAt]);
  });

  it('executes keyed MERGE actions with mutation stats and result shape', () => {
    const created = db.executeGql(
      `MERGE (n:NodeMergeParity {elementKey: 'node'})
       ON CREATE SET n.status = 'created', n.count = 1
       ON MATCH SET n.status = 'matched', n.count = n.count + 1
       RETURN elementKey(n) AS key, n.status AS status, n.count AS count`,
      null,
      { includePlan: true, profile: true }
    );
    assert.equal(created.kind, 'mutation');
    assert.deepEqual(created.rows, [{ key: 'node', status: 'created', count: 1 }]);
    assert.equal(created.mutationStats.nodesCreated, 1);
    assert.equal(created.mutationStats.nodesUpdated, 0);
    assert.equal(created.mutationStats.mutationRows, 1);
    assert.equal(created.plan.mutation.usesWriteTxn, true);

    const matched = db.executeGql(
      `MERGE (n:NodeMergeParity {elementKey: 'node'})
       ON CREATE SET n.status = 'created-again', n.count = 1
       ON MATCH SET n.status = 'matched', n.count = n.count + 1
       RETURN elementKey(n) AS key, n.status AS status, n.count AS count`
    );
    assert.equal(matched.kind, 'mutation');
    assert.deepEqual(matched.rows, [{ key: 'node', status: 'matched', count: 2 }]);
    assert.equal(matched.mutationStats.nodesCreated, 0);
    assert.equal(matched.mutationStats.nodesUpdated, 1);
    assert.equal(matched.mutationStats.propertiesSet, 2);
  });

  it('forwards Phase 34 caps and graph-pipeline explain fields through sync and async GQL', async () => {
    const options = {
      maxPipelineRows: 64,
      maxGroups: 8,
      maxCollectItems: 8,
      maxUnionBranches: 4,
      maxSubqueryInvocations: 16,
      maxSubqueryDepth: 2,
      maxShortestPathPairs: 8,
      includePlan: true,
      profile: true,
    };
    const query = `MATCH (n:Person)
                   WITH n.group AS group, count(*) AS total
                   RETURN group, total
                   ORDER BY group`;

    const result = db.executeGql(query, null, options);
    assert.equal(result.plan.read.target, 'graph_pipeline_query');
    assert.equal(result.plan.caps.maxPipelineRows, 64);
    assert.equal(result.plan.caps.maxGroups, 8);
    assert.equal(result.plan.caps.maxCollectItems, 8);
    assert.equal(result.plan.caps.maxUnionBranches, 4);
    assert.equal(result.plan.caps.maxSubqueryInvocations, 16);
    assert.equal(result.plan.caps.maxSubqueryDepth, 2);
    assert.equal(result.plan.caps.maxShortestPathPairs, 8);
    assert.equal(result.stats.rowsReturned, 2);
    assert.equal(typeof result.stats.dbHits, 'number');
    assert.equal(typeof result.stats.elapsedUs, 'number');
    assert.ok(result.plan.read.projection.some(item => item.includes('graph pipeline stage')));

    const explain = db.explainGql(query, null, options);
    assert.equal(explain.read.target, 'graph_pipeline_query');
    assert.equal(explain.caps.maxPipelineRows, 64);
    assert.equal(explain.caps.maxGroups, 8);
    assert.equal(explain.caps.maxCollectItems, 8);
    assert.equal(explain.caps.maxUnionBranches, 4);
    assert.equal(explain.caps.maxSubqueryInvocations, 16);
    assert.equal(explain.caps.maxSubqueryDepth, 2);
    assert.equal(explain.caps.maxShortestPathPairs, 8);

    const asyncRows = await db.executeGqlAsync(query, null, { ...options, compactRows: true });
    assert.deepEqual(asyncRows.rows, [['core', 2], ['ops', 1]]);
    const asyncExplain = await db.explainGqlAsync(query, null, options);
    assert.equal(asyncExplain.read.target, 'graph_pipeline_query');
    assert.equal(asyncExplain.caps.maxPipelineRows, 64);
    assert.equal(asyncExplain.caps.maxGroups, 8);
    assert.equal(asyncExplain.caps.maxCollectItems, 8);
    assert.equal(asyncExplain.caps.maxUnionBranches, 4);
    assert.equal(asyncExplain.caps.maxSubqueryInvocations, 16);
    assert.equal(asyncExplain.caps.maxSubqueryDepth, 2);
    assert.equal(asyncExplain.caps.maxShortestPathPairs, 8);

    assert.throws(
      () => db.executeGql('MATCH (n:Person) RETURN collect(n.name) AS names', null, { maxCollectItems: 1 }),
      /maxCollectItems|max_collect_items/i
    );
    assert.throws(
      () => db.executeGql(
        `MATCH (n:Person) RETURN n.name AS name
         UNION ALL
         MATCH (m:Company) RETURN m.name AS name`,
        null,
        { maxUnionBranches: 1 }
      ),
      /maxUnionBranches|max_union_branches/i
    );
    assert.throws(
      () => db.executeGql(
        `MATCH (a:Person) WHERE a.name = 'Ada'
         WITH a
         MATCH (c:Company) WHERE c.name = 'Acme'
         WITH a, c
         MATCH p = shortestPath((a)-[:WORKS_AT*1..1]->(c))
         RETURN p`,
        null,
        { maxShortestPathPairs: 0 }
      ),
      /maxShortestPathPairs|max_shortest_path_pairs|path caps/i
    );
  });

  it('executes sync CREATE RETURN with mutation stats, bytes, and embedded plan', () => {
    const result = db.executeGql(
      `CREATE (n:NodeCreateReturn {elementKey: 'created-one', name: $name, payload: $payload})
       RETURN elementKey(n) AS key, n.name AS name, n.payload AS payload, n`,
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
       RETURN elementKey(n) AS key, n.status AS status, n.group AS group
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
       WHERE elementKey(n) = 'hub'
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
      `CREATE (n:NodeAsyncMutation {elementKey: 'once', name: 'Async'})
       RETURN n.name AS name`
    );

    assert.equal(result.kind, 'mutation');
    assert.deepEqual(result.rows, [{ name: 'Async' }]);
    assert.equal(result.mutationStats.nodesCreated, 1);

    const readBack = db.executeGql(
      "MATCH (n:NodeAsyncMutation) WHERE elementKey(n) = 'once' RETURN n.name AS name"
    );
    assert.deepEqual(readBack.rows, [{ name: 'Async' }]);

    db.upsertNode('NodeAsyncUpdate', 'target', {
      props: { status: 'old', drop: 'remove-me' },
    });
    const update = await db.executeGqlAsync(
      `MATCH (n:NodeAsyncUpdate)
       WHERE elementKey(n) = 'target'
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
       WHERE elementKey(n) = 'hub'
       DETACH DELETE n`
    );
    assert.equal(detached.kind, 'mutation');
    assert.deepEqual(detached.rows, []);
    assert.equal(detached.mutationStats.nodesDeleted, 1);
    assert.equal(detached.mutationStats.edgesDeleted, 1);

    await assert.rejects(
      db.executeGqlAsync("CREATE (n:NodeAsyncReadOnly {elementKey: 'blocked'})", null, { mode: 'readOnly' }),
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
      () => db.executeGql("CREATE (n:NodeReadOnly {elementKey: 'blocked'})", null, { mode: 'readOnly' }),
      /read.?only|ReadOnly/i
    );
    assert.throws(
      () => db.executeGql('MATCH (n:Person) RETURN n LIMIT 1', null, { mode: 'readonly' }),
      /mode.*auto.*readOnly/i
    );
  });

  it('forwards mutation, order, and path caps through Node options', () => {
    assert.throws(
      () => db.executeGql("CREATE (n:NodeCapMutationRows {elementKey: 'row-cap'})", null, { maxMutationRows: 0 }),
      /maxMutationRows|max_mutation_rows/i
    );
    assert.throws(
      () => db.executeGql("CREATE (n:NodeCapMutationOps {elementKey: 'op-cap'})", null, { maxMutationOps: 0 }),
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
      `CREATE (n:NodeCompactMutation {elementKey: 'compact', name: 'Compact'})
       RETURN elementKey(n) AS key, n.name AS name`,
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
      "CREATE (n:NodeAsyncExplain {elementKey: 'planned'}) RETURN elementKey(n) AS key"
    );
    assert.equal(asyncExplain.kind, 'mutation');
    assert.equal(asyncExplain.read, null);
    assert.equal(asyncExplain.mutation.returnPlan.columns[0], 'key');
  });

  it('surfaces volatile mutation RETURN ORDER BY rejection through Node', () => {
    assert.throws(
      () => db.executeGql(
        "CREATE (n:NodeVolatileOrder {elementKey: 'bad-order'}) RETURN elementKey(n) ORDER BY id(n)"
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
