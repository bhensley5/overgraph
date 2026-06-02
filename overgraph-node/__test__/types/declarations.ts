import type {
  EdgeInput,
  EdgeLabelInfo,
  NeighborEntry,
  NeighborsOptions,
  NodeInput,
  NodeLabelFilter,
  NodeLabelInfo,
  OverGraph,
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
  GqlLoweringTarget,
  GqlNode,
  GqlPath,
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
void gqlExplainAsync
void gqlMutationResult
void compactRows
void gqlReadRowCap
void gqlNestedValue
void mutationStats
void mutationOperation
void mutationReturnColumns
