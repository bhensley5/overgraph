import type {
  EdgeInput,
  EdgeLabelInfo,
  EdgeSchema,
  EdgeSchemaInfo,
  CanonicalSchemaLiteral,
  GraphSchema,
  GraphSchemaCheckReport,
  GraphSchemaDropTargetResult,
  GraphSchemaOperation,
  GraphSchemaOperationKind,
  GraphSchemaPublishResult,
  GraphSchemaValidationReportEntry,
  NeighborEntry,
  NeighborsOptions,
  NodeInput,
  NodeLabelFilter,
  NodeLabelInfo,
  NodeSchema,
  NodeSchemaInfo,
  OverGraph,
  SchemaCheckOptions,
  SchemaLiteral,
  SchemaSetOptions,
  SchemaValidationReport,
} from '../../index.js'
import type {
  GraphPathValue,
  GraphPipelineRequest,
  GraphPipelineResult,
  GraphRowRequest,
  GraphRowResult,
  GqlEdge,
  GqlExecutionExplain,
  GqlExecutionOptions,
  GqlExecutionResult,
  GqlIndexExplain,
  GqlIndexStats,
  GqlLoweringTarget,
  GqlNode,
  GqlPath,
  GqlStatementKind,
  GqlValue,
  QueryEdgeRequest,
  QueryPlanNode,
} from '../../query-types.js'
import type * as QueryTypes from '../../query-types.js'

declare const db: OverGraph

const edgeInput: EdgeInput = {
  from: 1,
  to: 2,
  label: 'WORKS_AT',
  props: { since: 2026 },
}

const edgeInfo: EdgeLabelInfo = {
  label: 'WORKS_AT',
  labelId: 1,
}

const nodeInfo: NodeLabelInfo = {
  label: 'Person',
  labelId: 1,
}

const schemaLiteral: SchemaLiteral = {
  nested: [
    { type: 'uint', value: '18446744073709551615' },
    { type: 'bytes', value: Buffer.from('abc') },
    { type: 'map', value: { type: 'uint', value: '123' } },
  ],
}

const schemaSetOptions: SchemaSetOptions = {
  maxViolations: 1,
  chunkSize: 4096,
  scanLimit: null,
}

const schemaCheckOptions: SchemaCheckOptions = {
  maxViolations: 100,
  chunkSize: 128,
  scanLimit: 10,
}

const nodeSchema: NodeSchema = {
  additionalProperties: 'reject',
  properties: {
    name: {
      required: true,
      nullable: false,
      types: ['string'],
      enumValues: ['Alice', schemaLiteral],
    },
    age: {
      types: ['int', 'uint', 'number'],
      numericMin: { value: 0 },
      numericMax: { value: { type: 'uint', value: '120' }, inclusive: true },
    },
    payload: {
      types: ['bytes'],
      bytesMinLen: 1,
      bytesMaxLen: 1024,
      enumValues: [{ type: 'bytes', value: new Uint8Array([1, 2, 3]) }],
    },
  },
  key: { minBytes: 1, maxBytes: 128, enumValues: ['alice'] },
  labelConstraints: { allOf: ['Entity'], anyOf: ['Person'], noneOf: ['Archived'] },
  weight: { min: { value: 0 }, max: { value: 1.0 }, finite: true },
  denseVector: { presence: 'optional', dimension: 3 },
  sparseVector: { presence: 'forbidden', maxDimensionId: 1024 },
}

const edgeSchema: EdgeSchema = {
  additionalProperties: 'allow',
  properties: {
    since: { required: true, nullable: false, types: ['int'] },
  },
  from: { allOf: ['Person'] },
  to: { anyOf: ['Company', 'Team'] },
  allowSelfLoops: false,
  validity: {
    requireValidFromBeforeValidTo: true,
    allowOpenEndedValidTo: false,
  },
}

const setNodeSchemaInfo: NodeSchemaInfo = db.setNodeSchema('Person', nodeSchema, schemaSetOptions)
const checkNodeSchemaReport: SchemaValidationReport = db.checkNodeSchema('Person', nodeSchema, schemaCheckOptions)
const maybeNodeSchemaInfo: NodeSchemaInfo | null = db.getNodeSchema('Person')
const listedNodeSchemas: Array<NodeSchemaInfo> = db.listNodeSchemas()
const setEdgeSchemaInfo: EdgeSchemaInfo = db.setEdgeSchema('WORKS_AT', edgeSchema, schemaSetOptions)
const checkEdgeSchemaReport: SchemaValidationReport = db.checkEdgeSchema('WORKS_AT', edgeSchema, schemaCheckOptions)
const maybeEdgeSchemaInfo: EdgeSchemaInfo | null = db.getEdgeSchema('WORKS_AT')
const listedEdgeSchemas: Array<EdgeSchemaInfo> = db.listEdgeSchemas()
const droppedNodeSchema: boolean = db.dropNodeSchema('Person')
const droppedEdgeSchema: boolean = db.dropEdgeSchema('WORKS_AT')
const asyncNodeSchemaInfo: Promise<NodeSchemaInfo> = db.setNodeSchemaAsync('Person', nodeSchema)
const asyncNodeSchemaReport: Promise<SchemaValidationReport> = db.checkNodeSchemaAsync('Person', nodeSchema)
const asyncMaybeNodeSchemaInfo: Promise<NodeSchemaInfo | null> = db.getNodeSchemaAsync('Person')
const asyncListedNodeSchemas: Promise<Array<NodeSchemaInfo>> = db.listNodeSchemasAsync()
const asyncEdgeSchemaInfo: Promise<EdgeSchemaInfo> = db.setEdgeSchemaAsync('WORKS_AT', edgeSchema)
const asyncEdgeSchemaReport: Promise<SchemaValidationReport> = db.checkEdgeSchemaAsync('WORKS_AT', edgeSchema)
const asyncMaybeEdgeSchemaInfo: Promise<EdgeSchemaInfo | null> = db.getEdgeSchemaAsync('WORKS_AT')
const asyncListedEdgeSchemas: Promise<Array<EdgeSchemaInfo>> = db.listEdgeSchemasAsync()
const asyncDroppedNodeSchema: Promise<boolean> = db.dropNodeSchemaAsync('Person')
const asyncDroppedEdgeSchema: Promise<boolean> = db.dropEdgeSchemaAsync('WORKS_AT')
const canonicalUIntValue: CanonicalSchemaLiteral | undefined =
  setNodeSchemaInfo.schema.properties?.age?.numericMax?.value
const graphSchema: GraphSchema = {
  nodeSchemas: [{ label: 'Person', schema: nodeSchema }],
  edgeSchemas: [{ label: 'WORKS_AT', schema: edgeSchema }],
}
const graphSchemaOperations: Array<GraphSchemaOperation> = [
  { kind: 'setNode', label: 'Person', schema: nodeSchema },
  { kind: 'setEdge', label: 'WORKS_AT', schema: edgeSchema },
  { kind: 'dropNode', label: 'OldPerson' },
  { kind: 'dropEdge', label: 'OLD_EDGE' },
]
const graphSchemaPublish: GraphSchemaPublishResult = db.setGraphSchema(graphSchema, schemaSetOptions)
const graphSchemaAlter: GraphSchemaPublishResult = db.alterGraphSchema(graphSchemaOperations, schemaSetOptions)
const graphSchemaCheckSet: GraphSchemaCheckReport = db.checkGraphSchemaSet(graphSchema, schemaCheckOptions)
const graphSchemaCheckAdd: GraphSchemaCheckReport = db.checkGraphSchemaAdd(graphSchema, schemaCheckOptions)
const graphSchemaDrop: GraphSchemaPublishResult = db.dropGraphSchema()
const graphSchemaValidationEntry: GraphSchemaValidationReportEntry | undefined = graphSchemaCheckSet.entries[0]
const graphSchemaDropTarget: GraphSchemaDropTargetResult | undefined = graphSchemaAlter.dropTargets[0]
const graphSchemaOperationKind: GraphSchemaOperationKind = graphSchemaPublish.operation
const asyncGraphSchemaPublish: Promise<GraphSchemaPublishResult> = db.setGraphSchemaAsync(graphSchema)
const asyncGraphSchemaAlter: Promise<GraphSchemaPublishResult> = db.alterGraphSchemaAsync(graphSchemaOperations)
const asyncGraphSchemaCheckSet: Promise<GraphSchemaCheckReport> = db.checkGraphSchemaSetAsync(graphSchema)
const asyncGraphSchemaCheckAdd: Promise<GraphSchemaCheckReport> = db.checkGraphSchemaAddAsync(graphSchema)
const asyncGraphSchemaDrop: Promise<GraphSchemaPublishResult> = db.dropGraphSchemaAsync()

const neighbor: NeighborEntry = {
  nodeId: 2,
  edgeId: 3,
  label: 'WORKS_AT',
  weight: 1,
  validFrom: 0,
  validTo: 0,
}

const neighborOptions: NeighborsOptions = {
  direction: 'outgoing',
  edgeLabelFilter: ['WORKS_AT'],
}

const edgeQuery: QueryEdgeRequest = {
  label: 'WORKS_AT',
  allowFullScan: true,
}

const nodeInput: NodeInput = {
  labels: ['Person', 'Admin'],
  key: 'alice',
  props: { active: true },
}

const nodeLabelFilter: NodeLabelFilter = {
  labels: ['Person'],
  mode: 'all',
}

const graphRows: GraphRowRequest = {
  nodes: [
    { alias: 'person', labelFilter: { labels: ['Person'], mode: 'all' } },
    { alias: 'company', labelFilter: { labels: ['Company'], mode: 'all' } },
  ],
  pieces: [
    {
      kind: 'edge',
      alias: 'employment',
      fromAlias: 'person',
      toAlias: 'company',
      labelFilter: ['WORKS_AT'],
    },
  ],
  where: { op: '=', left: { property: { alias: 'person', key: 'active' } }, right: { param: 'active' } },
  return: [
    { expr: { binding: 'person' }, as: 'person' },
    { expr: { fn: 'nodeIds', args: [{ binding: 'path' }] }, as: 'nodeIds' },
  ],
  params: { active: true, payload: { list: [{ map: { ok: true } }] } },
  output: { mode: 'ids', compactRows: false, includeVectors: false },
  options: { allowFullScan: true, maxCursorBytes: 4096 },
  limit: 10,
}

const graphRowResult: GraphRowResult = db.queryGraphRows(graphRows)
const graphRowAsyncResult: Promise<GraphRowResult> = db.queryGraphRowsAsync(graphRows)
const graphRowExplain = db.explainGraphRows(graphRows)
const graphRowExplainAsync = db.explainGraphRowsAsync(graphRows)

const graphPipeline: GraphPipelineRequest = {
  stages: [
    { kind: 'match', nodes: [{ alias: 'person', labelFilter: { labels: ['Person'], mode: 'all' } }] },
    {
      kind: 'return',
      items: [
        { expr: { property: { alias: 'person', key: 'name' } }, as: 'name' },
        { expr: { aggregate: { function: 'count' } }, as: 'count' },
      ],
    },
  ],
  output: { compactRows: true },
  options: { allowFullScan: true, maxPipelineRows: 1024, includePlan: true },
  limit: 10,
}

const graphPipelineResult: GraphPipelineResult = db.queryGraphPipeline(graphPipeline)
const graphPipelineAsyncResult: Promise<GraphPipelineResult> = db.queryGraphPipelineAsync(graphPipeline)
const graphPipelineExplain = db.explainGraphPipeline(graphPipeline)
const graphPipelineExplainAsync = db.explainGraphPipelineAsync(graphPipeline)

// @ts-expect-error Old pattern request types are intentionally not exported.
type RemovedGraphNodePattern = QueryTypes.GraphNodePattern
// @ts-expect-error Old pattern query APIs are intentionally not exposed.
db.queryPattern({})

const pathValue: GraphPathValue = {
  nodeIds: [1, 2],
  edgeIds: [3],
  nodes: [{ id: 1, labels: ['Person'], denseVector: [0.1] }],
}

const fallbackEdgeLabelScan: QueryPlanNode = {
  kind: 'fallback_edge_label_scan',
}

const gqlOptions: GqlExecutionOptions = {
  mode: 'readOnly',
  allowFullScan: true,
  maxQueryBytes: 1024,
  cursor: null,
  maxCursorBytes: 4096,
  maxMutationRows: 10,
  maxMutationOps: 20,
  maxPipelineRows: 512,
  maxGroups: 128,
  maxCollectItems: 64,
  maxUnionBranches: 4,
  maxSubqueryInvocations: 32,
  maxSubqueryDepth: 2,
  maxShortestPathPairs: 16,
  maxParamBytes: 1024,
  maxAstDepth: 32,
  maxLiteralItems: 128,
  maxIntermediateBindings: 256,
  maxFrontier: 64,
  maxPathHops: 4,
  maxPathsPerStart: 16,
  maxOrderMaterialization: 128,
  includePlan: true,
  profile: true,
  compactRows: false,
  includeVectors: false,
}

const gqlParams = {
  name: 'alice',
  active: true,
  blob: Buffer.from('payload'),
  list: [1, null, 'x'],
  map: { nested: false },
}

const gqlResult: GqlExecutionResult = db.executeGql(
  'MATCH p = (n:Person {name: $name})-[:KNOWS*0..1]->(m) RETURN p',
  gqlParams,
  gqlOptions,
)
const gqlAsyncResult: Promise<GqlExecutionResult> = db.executeGqlAsync(
  'MATCH (n:Person {name: $name}) RETURN n.name',
  gqlParams,
  { compactRows: true },
)
const gqlExplain: GqlExecutionExplain = db.explainGql('MATCH (n:Person) RETURN n', null, { allowFullScan: true })
const gqlReadTarget: GqlLoweringTarget | undefined = gqlExplain.read?.target
const gqlExplainSchemaOperation: string | undefined = gqlExplain.schema?.operation
const gqlExplainSchemaTargetLabel: string | null | undefined = gqlExplain.schema?.targets[0]?.label
const gqlExplainSchemaScanLimit: number | null | undefined = gqlExplain.schema?.options.scanLimit
const gqlExplainIndexOperation: string | undefined = gqlExplain.index?.operation
const gqlExplainIndexTargetProp: string | null | undefined = gqlExplain.index?.targets[0]?.propKey
const gqlPipelineRowCap: number = gqlExplain.caps.maxPipelineRows
const gqlGroupCap: number = gqlExplain.caps.maxGroups
const gqlCollectCap: number = gqlExplain.caps.maxCollectItems
const gqlUnionCap: number = gqlExplain.caps.maxUnionBranches
const gqlSubqueryInvocationCap: number = gqlExplain.caps.maxSubqueryInvocations
const gqlSubqueryDepthCap: number = gqlExplain.caps.maxSubqueryDepth
const gqlShortestPathPairCap: number = gqlExplain.caps.maxShortestPathPairs
const gqlReadRowCap: number | undefined = gqlExplain.read?.caps.maxRows
const gqlExplainAsync = db.explainGqlAsync('MATCH (n:Person) RETURN n', null, {
  allowFullScan: true,
  maxPipelineRows: 512,
  maxGroups: 128,
  maxCollectItems: 64,
  maxUnionBranches: 4,
  maxSubqueryInvocations: 32,
  maxSubqueryDepth: 2,
  maxShortestPathPairs: 16,
})
const gqlMutationResult: GqlExecutionResult = db.executeGql(
  "CREATE (n:Person {key: 'new-person', name: 'New'}) RETURN n.name AS name",
  null,
  { maxMutationRows: 1, maxMutationOps: 1 },
)
const compactRows = gqlMutationResult.rows as Array<Array<GqlValue>>
const gqlSchemaStatsOperation: string | undefined = gqlResult.schemaStats?.operation
const gqlSchemaStatsWarnings: Array<string> | undefined = gqlResult.schemaStats?.warnings
const gqlMutationSchemaStats: QueryTypes.GqlSchemaStats | null = gqlMutationResult.schemaStats
const gqlStatementKindIndex: GqlStatementKind = 'index'
const gqlIndexStats: GqlIndexStats = {
  operation: 'create_property_index',
  indexesEnsured: 1,
  indexesDropped: 0,
  indexesReturned: 0,
  elapsedUs: null,
  warnings: [],
}
const gqlIndexExplain: GqlIndexExplain = {
  operation: 'show_property_indexes',
  targets: [
    {
      targetKind: 'property_index_catalog',
      label: null,
      propKey: null,
      kind: null,
      action: 'show',
    },
  ],
  usesCoreWriteQueue: false,
  publishesManifest: false,
  createsLabels: false,
  schedulesBackgroundBuild: false,
  dropsIndexDataAsync: false,
  sideEffectFree: true,
}
const gqlResultIndexStats: QueryTypes.GqlIndexStats | null = gqlResult.indexStats
const gqlNodeValue: GqlNode = {
  id: 1,
  labels: ['Person'],
  props: {
    nested: { scores: [1, null, { ok: true }] },
  },
}
const gqlEdgeValue: GqlEdge = {
  id: 3,
  from: 1,
  to: 2,
  label: 'KNOWS',
  props: {
    weights: [0.5, 1],
  },
}
const gqlPathValue: GqlPath = {
  nodeIds: [1, 2],
  edgeIds: [3],
  nodes: [gqlNodeValue],
  edges: [gqlEdgeValue],
}
const gqlNestedValue: GqlValue = {
  collect: [gqlNodeValue],
  path: gqlPathValue,
  helpers: {
    nodes: gqlPathValue.nodeIds,
    relationships: gqlPathValue.edgeIds,
  },
}
const mutationStats = gqlMutationResult.mutationStats?.nodesCreated
const mutationExplain = db.explainGql(
  "CREATE (n:Person {key: 'planned-person'}) RETURN n.key AS key",
)
const mutationOperation = mutationExplain.mutation?.operations[0]?.op
const mutationReturnColumns = mutationExplain.mutation?.returnPlan?.columns

// @ts-expect-error Old GQL options type is intentionally not exported.
type RemovedGqlQueryOptions = QueryTypes.GqlQueryOptions
// @ts-expect-error Old GQL result type is intentionally not exported.
type RemovedGqlResult = QueryTypes.GqlResult
// @ts-expect-error Old top-level read-only GQL explain type is intentionally not exported.
type RemovedGqlExplain = QueryTypes.GqlExplain

void db
void edgeInput
void edgeInfo
void nodeInfo
void schemaLiteral
void schemaSetOptions
void schemaCheckOptions
void nodeSchema
void edgeSchema
void setNodeSchemaInfo
void checkNodeSchemaReport
void maybeNodeSchemaInfo
void listedNodeSchemas
void setEdgeSchemaInfo
void checkEdgeSchemaReport
void maybeEdgeSchemaInfo
void listedEdgeSchemas
void droppedNodeSchema
void droppedEdgeSchema
void asyncNodeSchemaInfo
void asyncNodeSchemaReport
void asyncMaybeNodeSchemaInfo
void asyncListedNodeSchemas
void asyncEdgeSchemaInfo
void asyncEdgeSchemaReport
void asyncMaybeEdgeSchemaInfo
void asyncListedEdgeSchemas
void asyncDroppedNodeSchema
void asyncDroppedEdgeSchema
void canonicalUIntValue
void graphSchema
void graphSchemaOperations
void graphSchemaPublish
void graphSchemaAlter
void graphSchemaCheckSet
void graphSchemaCheckAdd
void graphSchemaDrop
void graphSchemaValidationEntry
void graphSchemaDropTarget
void graphSchemaOperationKind
void asyncGraphSchemaPublish
void asyncGraphSchemaAlter
void asyncGraphSchemaCheckSet
void asyncGraphSchemaCheckAdd
void asyncGraphSchemaDrop
void neighbor
void neighborOptions
void edgeQuery
void nodeInput
void nodeLabelFilter
void graphRows
void graphRowResult
void graphRowAsyncResult
void graphRowExplain
void graphRowExplainAsync
void pathValue
void fallbackEdgeLabelScan
void gqlResult
void gqlAsyncResult
void gqlExplain
void gqlReadTarget
void gqlExplainSchemaOperation
void gqlExplainSchemaTargetLabel
void gqlExplainSchemaScanLimit
void gqlExplainAsync
void gqlMutationResult
void compactRows
void gqlSchemaStatsOperation
void gqlSchemaStatsWarnings
void gqlMutationSchemaStats
void gqlReadRowCap
void gqlNestedValue
void mutationStats
void gqlStatementKindIndex
void gqlIndexStats
void gqlIndexExplain
void gqlResultIndexStats
void gqlExplainIndexOperation
void gqlExplainIndexTargetProp
void mutationOperation
void mutationReturnColumns
