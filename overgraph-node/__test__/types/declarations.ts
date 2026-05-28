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
  GraphRowRequest,
  GraphRowResult,
  GqlExecutionExplain,
  GqlExecutionOptions,
  GqlExecutionResult,
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
const gqlExplainAsync = db.explainGqlAsync('MATCH (n:Person) RETURN n', null, {
  allowFullScan: true,
})
const gqlMutationResult: GqlExecutionResult = db.executeGql(
  "CREATE (n:Person {key: 'new-person', name: 'New'}) RETURN n.name AS name",
  null,
  { maxMutationRows: 1, maxMutationOps: 1 },
)
const compactRows = gqlMutationResult.rows as Array<Array<GqlValue>>
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
void gqlExplainAsync
void gqlMutationResult
void compactRows
void mutationStats
void mutationOperation
void mutationReturnColumns
