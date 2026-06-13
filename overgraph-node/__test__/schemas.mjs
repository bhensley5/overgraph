import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';
import { packNodeBatch, packEdgeBatch } from '../helpers/pack-binary.mjs';

function tempDb(name, options = undefined) {
  const tmpDir = mkdtempSync(join(tmpdir(), `overgraph-node-schema-${name}-`));
  const db = OverGraph.open(join(tmpDir, 'db'), options);
  return {
    tmpDir,
    db,
    close() {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    },
  };
}

function assertSchemaViolation(fn) {
  assert.throws(fn, /schema violation:/i);
}

function requiredStringProperty() {
  return {
    required: true,
    nullable: false,
    types: ['string'],
  };
}

describe('schema management APIs', () => {
  const cleanup = [];
  after(() => {
    for (const entry of cleanup.splice(0).reverse()) {
      entry.close();
    }
  });

  it('round-trips node and edge DTOs with canonical schema literals', () => {
    const entry = tempDb('roundtrip', {
      walSyncMode: 'immediate',
      denseVector: { dimension: 3, metric: 'cosine' },
    });
    cleanup.push(entry);
    const { db } = entry;

    const nodeSchema = {
      additionalProperties: 'reject',
      properties: {
        name: {
          required: true,
          nullable: false,
          types: ['string'],
          stringMinBytes: 1,
          stringMaxBytes: 64,
          enumValues: ['Alice'],
        },
        age: {
          types: ['int', 'uint', 'number'],
          numericMin: { value: -5, inclusive: false },
          numericMax: { value: { type: 'uint', value: '42' }, inclusive: true },
        },
        blob: {
          types: ['bytes'],
          bytesMinLen: 2,
          bytesMaxLen: 4,
          enumValues: [{ type: 'bytes', value: new Uint8Array([1, 2, 3]) }],
        },
        tags: {
          types: ['array'],
          arrayMinItems: 1,
          arrayMaxItems: 3,
        },
        meta: {
          types: ['map'],
          mapMinEntries: 1,
          mapMaxEntries: 2,
          enumValues: [
            {
              nested: [
                { type: 'uint', value: '9007199254740993' },
                { type: 'bytes', value: Buffer.from([7, 8]) },
              ],
            },
          ],
        },
        markerMap: {
          types: ['map'],
          enumValues: [
            { type: 'map', value: { type: 'uint', value: '123' } },
            { type: 'map', value: { type: 'bytes', value: { type: 'bytes', value: Buffer.from([9]) } } },
            { type: 'map', value: { type: 'map', value: { nested: true } } },
            { type: 'map', value: { type: 1 } },
          ],
        },
      },
      key: { minBytes: 3, maxBytes: 32, enumValues: ['alice-key'] },
      labelConstraints: {
        allOf: ['Entity'],
        anyOf: ['Employee', 'Customer'],
        noneOf: ['Archived'],
      },
      weight: {
        min: { value: 0, inclusive: true },
        max: { value: 10.5, inclusive: false },
        finite: true,
      },
      denseVector: { presence: 'required', dimension: 3 },
      sparseVector: {
        presence: 'optional',
        minEntries: 1,
        maxEntries: 4,
        maxDimensionId: 1024,
      },
    };

    const nodeInfo = db.setNodeSchema('FullNode', nodeSchema, {
      maxViolations: 1,
      chunkSize: 2,
      scanLimit: null,
    });
    assert.equal(nodeInfo.label, 'FullNode');
    assert.equal(nodeInfo.schema.additionalProperties, 'reject');
    assert.deepEqual(nodeInfo.schema.properties.name.types, ['string']);
    assert.equal(nodeInfo.schema.properties.age.numericMin.inclusive, false);
    assert.deepEqual(nodeInfo.schema.properties.age.numericMax.value, {
      type: 'uint',
      value: '42',
    });
    assert.equal(nodeInfo.schema.properties.blob.enumValues[0].type, 'bytes');
    assert.ok(Buffer.isBuffer(nodeInfo.schema.properties.blob.enumValues[0].value));
    assert.deepEqual([...nodeInfo.schema.properties.blob.enumValues[0].value], [1, 2, 3]);
    assert.deepEqual(nodeInfo.schema.properties.meta.enumValues[0].nested[0], {
      type: 'uint',
      value: '9007199254740993',
    });
    assert.equal(nodeInfo.schema.properties.meta.enumValues[0].nested[1].type, 'bytes');
    assert.ok(Buffer.isBuffer(nodeInfo.schema.properties.meta.enumValues[0].nested[1].value));
    assert.deepEqual(nodeInfo.schema.properties.markerMap.enumValues[0], {
      type: 'map',
      value: { type: 'uint', value: '123' },
    });
    assert.equal(nodeInfo.schema.properties.markerMap.enumValues[1].value.type, 'bytes');
    assert.equal(nodeInfo.schema.properties.markerMap.enumValues[1].value.value.type, 'bytes');
    assert.ok(Buffer.isBuffer(nodeInfo.schema.properties.markerMap.enumValues[1].value.value.value));
    assert.deepEqual([...nodeInfo.schema.properties.markerMap.enumValues[1].value.value.value], [9]);
    assert.deepEqual(nodeInfo.schema.properties.markerMap.enumValues[2], {
      type: 'map',
      value: { type: 'map', value: { nested: true } },
    });
    assert.deepEqual(nodeInfo.schema.properties.markerMap.enumValues[3], { type: 1 });
    assert.deepEqual(nodeInfo.schema.labelConstraints.allOf, ['Entity']);
    assert.equal(nodeInfo.schema.denseVector.presence, 'required');
    assert.equal(nodeInfo.schema.sparseVector.maxDimensionId, 1024);
    assert.deepEqual(
      db.setNodeSchema('FullNode', nodeInfo.schema).schema.properties.markerMap.enumValues,
      nodeInfo.schema.properties.markerMap.enumValues
    );

    const edgeSchema = {
      additionalProperties: 'allow',
      properties: {
        since: {
          required: true,
          nullable: false,
          types: ['int'],
          numericMin: { value: 2000, inclusive: true },
        },
      },
      from: { allOf: ['Person'], anyOf: ['Employee'], noneOf: ['Archived'] },
      to: { allOf: ['Company'], anyOf: ['Sponsor'], noneOf: ['Closed'] },
      allowSelfLoops: false,
      weight: {
        min: { value: 0, inclusive: true },
        max: { value: 5, inclusive: true },
        finite: true,
      },
      validity: {
        requireValidFromBeforeValidTo: true,
        validFromMin: 1,
        validFromMax: 100,
        validToMin: 2,
        validToMax: 200,
        allowOpenEndedValidTo: false,
      },
    };
    const edgeInfo = db.setEdgeSchema('WORKS_AT_FULL', edgeSchema);
    assert.equal(edgeInfo.label, 'WORKS_AT_FULL');
    assert.equal(edgeInfo.schema.allowSelfLoops, false);
    assert.deepEqual(edgeInfo.schema.from.allOf, ['Person']);
    assert.equal(edgeInfo.schema.validity.requireValidFromBeforeValidTo, true);
    assert.equal(edgeInfo.schema.validity.allowOpenEndedValidTo, false);

    assert.equal(db.getNodeSchema('FullNode').schema.properties.blob.enumValues[0].type, 'bytes');
    assert.deepEqual(db.listNodeSchemas().map(info => info.label), ['FullNode']);
    assert.deepEqual(db.listEdgeSchemas().map(info => info.label), ['WORKS_AT_FULL']);
  });

  it('maps check options and reports max-violation truncation and scan limits', () => {
    const entry = tempDb('options', { walSyncMode: 'immediate' });
    cleanup.push(entry);
    const { db } = entry;

    db.upsertNode('OptionNode', 'bad-1', { props: { age: 'old' } });
    db.upsertNode('OptionNode', 'bad-2', { props: { age: 'older' } });
    const schema = {
      properties: {
        age: { required: true, nullable: false, types: ['int'] },
      },
    };

    const truncated = db.checkNodeSchema('OptionNode', schema, {
      maxViolations: 1,
      chunkSize: 1,
    });
    assert.equal(truncated.checkedRecords, 2);
    assert.equal(truncated.violationCount, 2);
    assert.equal(truncated.violations.length, 1);
    assert.equal(truncated.truncated, true);
    assert.equal(truncated.scanLimitHit, false);
    assert.equal(truncated.violations[0].target.kind, 'node');

    const limited = db.checkNodeSchema('OptionNode', schema, {
      maxViolations: 10,
      chunkSize: 1,
      scanLimit: 1,
    });
    assert.equal(limited.checkedRecords, 1);
    assert.equal(limited.scanLimitHit, true);
  });

  it('persists schemas across close and reopen', () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-node-schema-persist-'));
    cleanup.push({
      close() {
        rmSync(tmpDir, { recursive: true, force: true });
      },
    });

    let db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    db.setNodeSchema('PersistedNode', {
      properties: { name: requiredStringProperty() },
    });
    db.setEdgeSchema('PersistedEdge', {
      properties: { since: { required: true, nullable: false, types: ['int'] } },
    });
    db.close();

    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    assert.equal(db.getNodeSchema('PersistedNode').schema.properties.name.required, true);
    assert.equal(db.getEdgeSchema('PersistedEdge').schema.properties.since.types[0], 'int');
    db.close();
  });

  it('publishes, alters, checks, replaces, and drops graph schemas in bulk', () => {
    const entry = tempDb('bulk-sync', { walSyncMode: 'immediate' });
    cleanup.push(entry);
    const { db } = entry;

    const nodeSchema = {
      properties: { name: requiredStringProperty() },
    };
    const edgeSchema = {
      properties: { since: { required: true, nullable: false, types: ['int'] } },
      from: { anyOf: ['BulkPerson'] },
      to: { anyOf: ['BulkCompany'] },
    };

    db.upsertNode('BulkPerson', 'ada', { props: { name: 'Ada' } });
    db.upsertNode('BulkCompany', 'acme');

    const set = db.setGraphSchema({
      nodeSchemas: [{ label: 'BulkPerson', schema: nodeSchema }],
      edgeSchemas: [{ label: 'BULK_WORKS_AT', schema: edgeSchema }],
    });
    assert.equal(set.operation, 'set');
    assert.equal(set.targetsPublished, 2);
    assert.equal(set.targetsDropped, 0);
    assert.equal(set.validation.entries.length, 2);
    assert.deepEqual(db.listNodeSchemas().map(info => info.label), ['BulkPerson']);
    assert.deepEqual(db.listEdgeSchemas().map(info => info.label), ['BULK_WORKS_AT']);

    const add = db.alterGraphSchema([
      {
        kind: 'setNode',
        label: 'BulkCompany',
        schema: { properties: { name: { types: ['string'] } } },
      },
    ]);
    assert.equal(add.operation, 'add');
    assert.equal(add.targetsPublished, 1);
    assert.deepEqual(db.listNodeSchemas().map(info => info.label), ['BulkCompany', 'BulkPerson']);

    const checkAdd = db.checkGraphSchemaAdd({
      nodeSchemas: [{ label: 'DryRunOnly', schema: { properties: { name: requiredStringProperty() } } }],
    });
    assert.equal(checkAdd.operation, 'checkAdd');
    assert.equal(checkAdd.entries[0].targetKind, 'node');
    assert.equal(db.getNodeSchema('DryRunOnly'), null);

    const checkSet = db.checkGraphSchemaSet({ nodeSchemas: [] });
    assert.equal(checkSet.operation, 'checkSet');
    assert.equal(checkSet.entries.length, 0);
    assert.deepEqual(db.listNodeSchemas().map(info => info.label), ['BulkCompany', 'BulkPerson']);

    const dropSelected = db.alterGraphSchema([
      { kind: 'dropNode', label: 'BulkCompany' },
      { kind: 'dropEdge', label: 'MISSING_EDGE' },
      { kind: 'dropEdge', label: 'BULK_WORKS_AT' },
    ]);
    assert.equal(dropSelected.operation, 'drop');
    assert.deepEqual(
      dropSelected.dropTargets.map(target => [target.targetKind, target.label, target.action]),
      [
        ['node', 'BulkCompany', 'dropped'],
        ['edge', 'MISSING_EDGE', 'notFound'],
        ['edge', 'BULK_WORKS_AT', 'dropped'],
      ]
    );
    assert.equal(dropSelected.targetsDropped, 2);
    assert.deepEqual(db.listNodeSchemas().map(info => info.label), ['BulkPerson']);
    assert.deepEqual(db.listEdgeSchemas(), []);

    const replace = db.setGraphSchema({
      edgeSchemas: [{ label: 'REPLACEMENT_EDGE', schema: { properties: {} } }],
    });
    assert.equal(replace.operation, 'set');
    assert.equal(replace.targetsPublished, 1);
    assert.equal(replace.targetsDropped, 1);
    assert.equal(replace.nodeSchemasDropped, 1);
    assert.equal(replace.edgeSchemasDropped, 0);
    assert.deepEqual(db.listNodeSchemas(), []);
    assert.deepEqual(db.listEdgeSchemas().map(info => info.label), ['REPLACEMENT_EDGE']);

    const dropAll = db.dropGraphSchema();
    assert.equal(dropAll.operation, 'dropAll');
    assert.equal(dropAll.targetsDropped, 1);
    assert.equal(dropAll.nodeSchemasDropped, 0);
    assert.equal(dropAll.edgeSchemasDropped, 1);
    assert.deepEqual(db.listNodeSchemas(), []);
    assert.deepEqual(db.listEdgeSchemas(), []);
  });

  it('does not partially publish failed graph-schema batches', () => {
    const entry = tempDb('bulk-atomic', { walSyncMode: 'immediate' });
    cleanup.push(entry);
    const { db } = entry;

    db.upsertNode('ViolatingBulk', 'bad', { props: {} });
    assert.throws(
      () =>
        db.setGraphSchema({
          nodeSchemas: [
            { label: 'CleanBulk', schema: { properties: {} } },
            { label: 'ViolatingBulk', schema: { properties: { name: requiredStringProperty() } } },
          ],
        }),
      /schema violation|validation/i
    );
    assert.equal(db.getNodeSchema('CleanBulk'), null);
    assert.equal(db.getNodeSchema('ViolatingBulk'), null);
    assert.deepEqual(db.listNodeSchemas(), []);
  });

  it('exposes async schema management parity', async () => {
    const entry = tempDb('async', { walSyncMode: 'immediate' });
    cleanup.push(entry);
    const { db } = entry;
    db.upsertNode('Endpoint', 'async-from');
    db.upsertNode('Endpoint', 'async-to');

    const nodeInfo = await db.setNodeSchemaAsync('AsyncNode', {
      properties: { name: requiredStringProperty() },
    });
    assert.equal(nodeInfo.label, 'AsyncNode');
    assert.equal((await db.getNodeSchemaAsync('AsyncNode')).schema.properties.name.required, true);
    assert.equal((await db.listNodeSchemasAsync()).length, 1);

    const check = await db.checkNodeSchemaAsync('AsyncNode', {
      properties: { name: requiredStringProperty() },
    });
    assert.equal(check.violationCount, 0);

    const edgeInfo = await db.setEdgeSchemaAsync('AsyncEdge', {
      properties: { since: { required: true, nullable: false, types: ['int'] } },
    });
    assert.equal(edgeInfo.label, 'AsyncEdge');
    assert.equal((await db.getEdgeSchemaAsync('AsyncEdge')).schema.properties.since.required, true);
    assert.equal((await db.listEdgeSchemasAsync()).length, 1);
    const edgeCheck = await db.checkEdgeSchemaAsync('AsyncEdge', {
      properties: { since: { required: true, nullable: false, types: ['int'] } },
      from: { anyOf: ['Endpoint'] },
      to: { anyOf: ['Endpoint'] },
    });
    assert.equal(edgeCheck.violationCount, 0);
    assert.equal(await db.dropNodeSchemaAsync('AsyncNode'), true);
    assert.equal(await db.dropEdgeSchemaAsync('AsyncEdge'), true);
    assert.equal(await db.getNodeSchemaAsync('AsyncNode'), null);
    assert.equal(await db.getEdgeSchemaAsync('AsyncEdge'), null);
  });

  it('exposes async bulk graph-schema management parity', async () => {
    const entry = tempDb('bulk-async', { walSyncMode: 'immediate' });
    cleanup.push(entry);
    const { db } = entry;

    const set = await db.setGraphSchemaAsync({
      nodeSchemas: [{ label: 'AsyncBulkNode', schema: { properties: { name: requiredStringProperty() } } }],
      edgeSchemas: [{ label: 'ASYNC_BULK_EDGE', schema: { properties: {} } }],
    });
    assert.equal(set.operation, 'set');
    assert.equal(set.targetsPublished, 2);
    assert.equal((await db.listNodeSchemasAsync()).length, 1);

    const added = await db.alterGraphSchemaAsync([
      { kind: 'setNode', label: 'AsyncBulkCompany', schema: { properties: {} } },
    ]);
    assert.equal(added.operation, 'add');
    assert.equal(added.targetsPublished, 1);
    assert.deepEqual(
      (await db.listNodeSchemasAsync()).map(info => info.label),
      ['AsyncBulkCompany', 'AsyncBulkNode']
    );

    const check = await db.checkGraphSchemaAddAsync({
      nodeSchemas: [{ label: 'AsyncDryRun', schema: { properties: {} } }],
    });
    assert.equal(check.operation, 'checkAdd');
    assert.equal(check.entries[0].label, 'AsyncDryRun');
    assert.equal(await db.getNodeSchemaAsync('AsyncDryRun'), null);

    const checkSet = await db.checkGraphSchemaSetAsync({ nodeSchemas: [] });
    assert.equal(checkSet.operation, 'checkSet');
    assert.deepEqual(checkSet.entries, []);
    assert.deepEqual(
      (await db.listNodeSchemasAsync()).map(info => info.label),
      ['AsyncBulkCompany', 'AsyncBulkNode']
    );

    const dropSelected = await db.alterGraphSchemaAsync([
      { kind: 'dropNode', label: 'AsyncBulkCompany' },
      { kind: 'dropNode', label: 'AsyncBulkNode' },
      { kind: 'dropEdge', label: 'MISSING_ASYNC_EDGE' },
    ]);
    assert.deepEqual(
      dropSelected.dropTargets.map(target => target.action),
      ['dropped', 'dropped', 'notFound']
    );
    assert.equal(dropSelected.targetsDropped, 2);
    assert.equal(await db.getNodeSchemaAsync('AsyncBulkCompany'), null);
    assert.equal(await db.getNodeSchemaAsync('AsyncBulkNode'), null);

    const dropAll = await db.dropGraphSchemaAsync();
    assert.equal(dropAll.edgeSchemasDropped, 1);
    assert.deepEqual(await db.listEdgeSchemasAsync(), []);
  });

  it('surfaces Rust schema enforcement through Node write APIs', () => {
    const entry = tempDb('enforcement', { walSyncMode: 'immediate' });
    cleanup.push(entry);
    const { db } = entry;

    db.setNodeSchema('StrictNode', {
      properties: { name: requiredStringProperty() },
    });
    db.setEdgeSchema('StrictEdge', {
      properties: { since: { required: true, nullable: false, types: ['int'] } },
    });

    assertSchemaViolation(() =>
      db.upsertNode('StrictNode', 'bad-native', { props: { name: 1 } })
    );
    assert.equal(db.getNodeByKey('StrictNode', 'bad-native'), null);

    assertSchemaViolation(() =>
      db.batchUpsertNodes([{ labels: 'StrictNode', key: 'bad-batch', props: { name: 1 } }])
    );
    assert.equal(db.getNodeByKey('StrictNode', 'bad-batch'), null);

    assertSchemaViolation(() =>
      db.batchUpsertNodesBinary(packNodeBatch([
        { labels: 'StrictNode', key: 'bad-binary', props: { name: 1 } },
      ]))
    );
    assert.equal(db.getNodeByKey('StrictNode', 'bad-binary'), null);

    assertSchemaViolation(() =>
      db.graphPatch({ upsertNodes: [{ labels: 'StrictNode', key: 'bad-patch', props: { name: 1 } }] })
    );
    assert.equal(db.getNodeByKey('StrictNode', 'bad-patch'), null);

    const txn = db.beginWriteTxn();
    txn.upsertNode('StrictNode', 'bad-txn', { props: { name: 1 } });
    assertSchemaViolation(() => txn.commit());
    assert.equal(db.getNodeByKey('StrictNode', 'bad-txn'), null);

    assertSchemaViolation(() =>
      db.executeGql("CREATE (n:StrictNode {elementKey: 'bad-gql', name: 1}) RETURN n")
    );
    assert.equal(db.getNodeByKey('StrictNode', 'bad-gql'), null);

    const from = db.upsertNode('Endpoint', 'from', { props: { name: 'from' } });
    const to = db.upsertNode('Endpoint', 'to', { props: { name: 'to' } });
    assertSchemaViolation(() =>
      db.upsertEdge(from, to, 'StrictEdge', { props: { since: 'now' } })
    );
    assert.equal(db.getEdgeByTriple(from, to, 'StrictEdge'), null);

    assertSchemaViolation(() =>
      db.batchUpsertEdges([{ from, to, label: 'StrictEdge', props: { since: 'now' } }])
    );
    assert.equal(db.getEdgeByTriple(from, to, 'StrictEdge'), null);

    assertSchemaViolation(() =>
      db.batchUpsertEdgesBinary(packEdgeBatch([
        { from, to, label: 'StrictEdge', props: { since: 'now' } },
      ]))
    );
    assert.equal(db.getEdgeByTriple(from, to, 'StrictEdge'), null);

    assertSchemaViolation(() =>
      db.graphPatch({ upsertEdges: [{ from, to, label: 'StrictEdge', props: { since: 'now' } }] })
    );
    assert.equal(db.getEdgeByTriple(from, to, 'StrictEdge'), null);

    const edgeTxn = db.beginWriteTxn();
    edgeTxn.upsertEdge({ id: from }, { id: to }, 'StrictEdge', { props: { since: 'now' } });
    assertSchemaViolation(() => edgeTxn.commit());
    assert.equal(db.getEdgeByTriple(from, to, 'StrictEdge'), null);

    assertSchemaViolation(() =>
      db.executeGql(
        "MATCH (a:Endpoint) WHERE elementKey(a) = 'from' MATCH (b:Endpoint) WHERE elementKey(b) = 'to' CREATE (a)-[:StrictEdge {since: 'now'}]->(b)"
      )
    );
    assert.equal(db.getEdgeByTriple(from, to, 'StrictEdge'), null);
  });
});
