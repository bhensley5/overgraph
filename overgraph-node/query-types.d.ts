export type QueryPredicateOp = 'eq' | 'range'

export type NonEmptyArray<T> = [T, ...T[]]

export type QueryNodeFilter =
  | { and: NonEmptyArray<QueryNodeFilter>; or?: never; not?: never; property?: never; updatedAt?: never }
  | { or: NonEmptyArray<QueryNodeFilter>; and?: never; not?: never; property?: never; updatedAt?: never }
  | { not: QueryNodeFilter; and?: never; or?: never; property?: never; updatedAt?: never }
  | QueryNodePropertyFilter
  | QueryNodeUpdatedAtFilter

export type QueryEdgeFilter =
  | {
      and: NonEmptyArray<QueryEdgeFilter>
      or?: never
      not?: never
      property?: never
      weight?: never
      updatedAt?: never
      validAt?: never
      validFrom?: never
      validTo?: never
    }
  | {
      or: NonEmptyArray<QueryEdgeFilter>
      and?: never
      not?: never
      property?: never
      weight?: never
      updatedAt?: never
      validAt?: never
      validFrom?: never
      validTo?: never
    }
  | {
      not: QueryEdgeFilter
      and?: never
      or?: never
      property?: never
      weight?: never
      updatedAt?: never
      validAt?: never
      validFrom?: never
      validTo?: never
    }
  | QueryEdgePropertyFilter
  | QueryEdgeWeightFilter
  | QueryEdgeUpdatedAtFilter
  | QueryEdgeValidAtFilter
  | QueryEdgeValidFromFilter
  | QueryEdgeValidToFilter

export type QueryNodePropertyFilter =
  | {
      property: string
      eq: any
      in?: never
      exists?: never
      missing?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }
  | {
      property: string
      in: NonEmptyArray<any>
      eq?: never
      exists?: never
      missing?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }
  | ({ property: string; eq?: never; in?: never; exists?: never; missing?: never } & QueryNodeRangePredicate)
  | {
      property: string
      exists: true
      eq?: never
      in?: never
      missing?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }
  | {
      property: string
      missing: true
      eq?: never
      in?: never
      exists?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }

export interface QueryNodeUpdatedAtFilter {
  updatedAt: QueryNodeRangePredicate
  and?: never
  or?: never
  not?: never
  property?: never
}

export type QueryEdgePropertyFilter =
  | {
      property: string
      eq: any
      in?: never
      exists?: never
      missing?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }
  | {
      property: string
      in: NonEmptyArray<any>
      eq?: never
      exists?: never
      missing?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }
  | ({ property: string; eq?: never; in?: never; exists?: never; missing?: never } & QueryNodeRangePredicate)
  | {
      property: string
      exists: true
      eq?: never
      in?: never
      missing?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }
  | {
      property: string
      missing: true
      eq?: never
      in?: never
      exists?: never
      gt?: never
      gte?: never
      lt?: never
      lte?: never
    }

export interface QueryEdgeWeightFilter {
  weight: QueryNodeRangePredicate
  and?: never
  or?: never
  not?: never
  property?: never
}

export interface QueryEdgeUpdatedAtFilter {
  updatedAt: QueryNodeRangePredicate
  and?: never
  or?: never
  not?: never
  property?: never
}

export interface QueryEdgeValidAtFilter {
  validAt: number
  and?: never
  or?: never
  not?: never
  property?: never
}

export interface QueryEdgeValidFromFilter {
  validFrom: QueryNodeRangePredicate
  and?: never
  or?: never
  not?: never
  property?: never
}

export interface QueryEdgeValidToFilter {
  validTo: QueryNodeRangePredicate
  and?: never
  or?: never
  not?: never
  property?: never
}

export type QueryLowerBound =
  | { gt: any; gte?: never }
  | { gte: any; gt?: never }
  | { gt?: never; gte?: never }

export type QueryUpperBound =
  | { lt: any; lte?: never }
  | { lte: any; lt?: never }
  | { lt?: never; lte?: never }

export type QueryPresentLowerBound =
  | { gt: any; gte?: never }
  | { gte: any; gt?: never }

export type QueryPresentUpperBound =
  | { lt: any; lte?: never }
  | { lte: any; lt?: never }

export type QueryRangePredicate =
  | ({ op?: 'range' } & QueryPresentLowerBound & QueryUpperBound)
  | ({ op?: 'range' } & QueryLowerBound & QueryPresentUpperBound)

export type QueryNodeRangePredicate =
  | (QueryPresentLowerBound & QueryUpperBound)
  | (QueryLowerBound & QueryPresentUpperBound)

export type QueryEqualsPredicate =
  | { op: 'eq'; value: any; eq?: never; gt?: never; gte?: never; lt?: never; lte?: never }
  | { eq: any; op?: never; value?: never; gt?: never; gte?: never; lt?: never; lte?: never }

export type QueryWherePredicate = QueryRangePredicate | QueryEqualsPredicate

export type QueryPropertyPredicatePayload = {
  key: string
} & QueryWherePredicate

export interface QueryPropertyPredicate {
  property: QueryPropertyPredicatePayload
}

export interface QueryUpdatedAtPredicate {
  updatedAt: QueryRangePredicate
}

export type QueryPredicate = QueryPropertyPredicate | QueryUpdatedAtPredicate

export type LabelMatchMode = 'any' | 'all'

export interface NodeLabelFilter {
  labels: Array<string>
  mode: LabelMatchMode
}

export interface QueryNodeRequest {
  labelFilter?: NodeLabelFilter
  ids?: Array<number>
  keys?: Array<string>
  filter?: QueryNodeFilter | null
  orderBy?: 'nodeIdAsc' | 'node_id_asc'
  limit?: number | null
  after?: number
  allowFullScan?: boolean
}

export interface QueryEdgeRequest {
  label?: string
  ids?: Array<number>
  fromIds?: Array<number>
  toIds?: Array<number>
  endpointIds?: Array<number>
  filter?: QueryEdgeFilter | null
  limit?: number | null
  after?: number
  allowFullScan?: boolean
}

export interface GraphRowNodePattern {
  alias: string
  labelFilter?: NodeLabelFilter
  ids?: Array<number>
  keys?: Array<string | { label: string; key: string }>
  filter?: QueryNodeFilter | null
}

export interface GraphRowEdgePiece {
  kind: 'edge'
  alias?: string
  fromAlias: string
  toAlias: string
  direction?: 'outgoing' | 'incoming' | 'both'
  labelFilter?: Array<string>
  filter?: QueryEdgeFilter | null
}

export interface GraphRowOptionalPiece {
  kind: 'optional'
  pieces: Array<GraphRowPatternPiece>
  where?: GraphExpr | null
}

export interface GraphRowVariableLengthPiece {
  kind: 'variableLength'
  pathAlias?: string
  edgeAlias?: string
  fromAlias: string
  toAlias: string
  direction?: 'outgoing' | 'incoming' | 'both'
  labelFilter?: Array<string>
  filter?: QueryEdgeFilter | null
  minHops: number
  maxHops: number
}

export type GraphRowPatternPiece =
  | GraphRowEdgePiece
  | GraphRowOptionalPiece
  | GraphRowVariableLengthPiece

export type GraphScalar = null | boolean | number | string

export type GraphParamValue =
  | GraphScalar
  | { bytes: Array<number> }
  | { list: Array<GraphParamValue> }
  | { map: Record<string, GraphParamValue> }

export type GraphExpr =
  | GraphScalar
  | { bytes: Array<number> }
  | { list: Array<GraphExpr> }
  | { map: Record<string, GraphExpr> }
  | { param: string }
  | { binding: string }
  | { property: { alias: string; key: string } }
  | { nodeField: { alias: string; field: 'id' | 'labels' | 'key' | 'weight' | 'createdAt' | 'updatedAt' } }
  | { edgeField: { alias: string; field: 'id' | 'from' | 'to' | 'label' | 'weight' | 'createdAt' | 'updatedAt' | 'validFrom' | 'validTo' } }
  | { pathField: { alias: string; field: 'nodeIds' | 'edgeIds' | 'length' } }
  | { fn: GraphFunctionName; args: Array<GraphExpr> }
  | { aggregate: { function: GraphAggregateFunction; distinct?: boolean; arg?: GraphExpr | null } }
  | { exists: GraphPipelineCallPayload }
  | { op: GraphBinaryOperator; left: GraphExpr; right: GraphExpr }
  | { op: 'not' | 'neg' | '-'; expr: GraphExpr }
  | { case: { operand?: GraphExpr | null; branches: Array<{ when: GraphExpr; then: GraphExpr }>; else?: GraphExpr | null } }
  | { isNull: GraphExpr }
  | { isNotNull: GraphExpr }

export type GraphFunctionName =
  | 'id'
  | 'labels'
  | 'type'
  | 'length'
  | 'startNode'
  | 'endNode'
  | 'nodes'
  | 'relationships'
  | 'nodeIds'
  | 'edgeIds'
  | 'coalesce'
  | 'toString'
  | 'toInteger'
  | 'toFloat'
  | 'abs'
  | 'floor'
  | 'ceil'
  | 'round'
  | 'lower'
  | 'upper'
  | 'trim'
  | 'substring'
  | 'size'
  | 'head'
  | 'last'

export type GraphAggregateFunction = 'count' | 'sum' | 'avg' | 'min' | 'max' | 'collect'

export type GraphBinaryOperator =
  | 'and'
  | 'or'
  | '='
  | '=='
  | 'eq'
  | '<>'
  | '!='
  | 'neq'
  | '<'
  | 'lt'
  | '<='
  | 'lte'
  | '>'
  | 'gt'
  | '>='
  | 'gte'
  | 'in'
  | '+'
  | 'add'
  | '-'
  | 'sub'
  | '*'
  | 'mul'
  | '/'
  | 'div'
  | 'startsWith'
  | 'endsWith'
  | 'contains'

export type GraphElementProjection = 'idOnly' | 'compact' | 'full'
export type GraphPropertySelection = boolean | 'all' | 'none' | Array<string>
export type GraphVectorSelection = boolean | 'none' | 'dense' | 'sparse' | 'both'

export interface GraphSelectedNodeProjection {
  id?: boolean
  labels?: boolean
  key?: boolean
  props?: GraphPropertySelection
  weight?: boolean
  createdAt?: boolean
  updatedAt?: boolean
  vectors?: GraphVectorSelection
}

export interface GraphSelectedEdgeProjection {
  id?: boolean
  from?: boolean
  to?: boolean
  label?: boolean
  props?: GraphPropertySelection
  weight?: boolean
  createdAt?: boolean
  updatedAt?: boolean
  validFrom?: boolean
  validTo?: boolean
}

export interface GraphSelectedPathProjection {
  nodeIds?: boolean
  edgeIds?: boolean
  nodes?: GraphSelectedNodeProjection
  edges?: GraphSelectedEdgeProjection
}

export type GraphSelectedProjection =
  | { node: GraphSelectedNodeProjection }
  | { edge: GraphSelectedEdgeProjection }
  | { path: GraphSelectedPathProjection }

export type GraphReturnProjection =
  | 'auto'
  | 'idOnly'
  | 'id'
  | 'element'
  | 'compact'
  | 'full'
  | { element: GraphElementProjection }
  | { selected: GraphSelectedProjection }

export interface GraphReturnItem {
  expr: GraphExpr
  as?: string
  projection?: GraphReturnProjection
}

export interface GraphOrderItem {
  expr: GraphExpr
  direction?: 'asc' | 'desc'
}

export interface GraphOutputOptions {
  mode?: 'ids' | 'elements' | 'projected'
  compactRows?: boolean
  includeVectors?: boolean
}

export interface GraphQueryOptions {
  allowFullScan?: boolean
  maxIntermediateBindings?: number
  maxFrontier?: number
  maxPathHops?: number
  maxPathsPerStart?: number
  maxPageLimit?: number
  maxOrderMaterialization?: number
  maxCursorBytes?: number
  maxQueryBytes?: number
  includePlan?: boolean
  profile?: boolean
}

export interface GraphRowRequest {
  nodes?: Array<GraphRowNodePattern>
  pieces?: Array<GraphRowPatternPiece>
  where?: GraphExpr | null
  return?: Array<GraphReturnItem>
  orderBy?: Array<GraphOrderItem>
  skip?: number
  limit?: number
  cursor?: string | null
  atEpoch?: number | null
  params?: Record<string, GraphParamValue>
  output?: GraphOutputOptions
  options?: GraphQueryOptions
}

export type GraphValue =
  | GraphScalar
  | Buffer
  | Array<GraphValue>
  | { [key: string]: GraphValue }
  | GraphNodeValue
  | GraphEdgeValue
  | GraphPathValue

export interface GraphNodeValue {
  id?: number
  labels?: Array<string>
  key?: string
  props?: Record<string, GraphValue>
  weight?: number
  createdAt?: number
  updatedAt?: number
  denseVector?: Array<number>
  sparseVector?: Array<{ dimension: number; value: number }>
}

export interface GraphEdgeValue {
  id?: number
  from?: number
  to?: number
  label?: string
  props?: Record<string, GraphValue>
  weight?: number
  createdAt?: number
  updatedAt?: number
  validFrom?: number
  validTo?: number
}

export interface GraphPathValue {
  nodeIds: Array<number>
  edgeIds: Array<number>
  nodes?: Array<GraphNodeValue>
  edges?: Array<GraphEdgeValue>
}

export interface GraphRowStats {
  rowsReturned: number
  rowsAfterFilter: number
  rowsSeenForPage: number
  intermediateBindingsPeak: number
  frontierPeak: number
  pathsEnumerated: number
  dbHits: number
  elapsedUs: number | null
  effectiveAtEpoch: number
  warnings: Array<string>
}

export interface GraphExplainNode {
  kind: string
  detail: string
  children: Array<GraphExplainNode>
}

export interface GraphRowOperationExplain {
  kind: string
  detail: string
}

export interface GraphRowExplain {
  columns: Array<string>
  effectiveAtEpoch: number | null
  fingerprint: string
  plan: Array<GraphExplainNode>
  rowOps: Array<GraphRowOperationExplain>
  order: { explicit: boolean; items: number; stableLogicalRowKey: boolean }
  cursor: { supplied: boolean; codecImplemented: boolean; message: string | null }
  projection: { columns: Array<string>; outputMode: 'ids' | 'elements' | 'projected'; includeVectors: boolean; compactRows: boolean }
  caps: {
    allowFullScan: boolean
    maxIntermediateBindings: number
    maxFrontier: number
    maxPathHops: number
    maxPathsPerStart: number
    maxPageLimit: number
    maxOrderMaterialization: number
    maxCursorBytes: number
    maxQueryBytes: number
  }
  summaries: { validationOnly: boolean; rowsPlanned: number; warnings: Array<string> }
  warnings: Array<string>
  notes: Array<string>
}

export interface GraphObjectRowsResult {
  columns: Array<string>
  rows: Array<Record<string, GraphValue>>
  nextCursor: string | null
  stats: GraphRowStats
  plan: GraphRowExplain | null
}

export interface GraphCompactRowsResult {
  columns: Array<string>
  rows: Array<Array<GraphValue>>
  nextCursor: string | null
  stats: GraphRowStats
  plan: GraphRowExplain | null
}

export type GraphRowResult = GraphObjectRowsResult | GraphCompactRowsResult

export interface GraphPipelineMatchStage {
  kind: 'match'
  optional?: boolean
  nodes?: Array<GraphRowNodePattern>
  pieces?: Array<GraphRowPatternPiece>
  where?: GraphExpr | null
  optionalCandidateWhere?: GraphExpr | null
}

export type GraphProjectionItems = 'star' | '*' | Array<GraphProjectItem>

export interface GraphProjectItem {
  expr: GraphExpr
  as?: string
  projection?: GraphReturnProjection
}

export interface GraphPipelineProjectStage {
  kind: 'project' | 'with' | 'return'
  projectKind?: 'with' | 'return'
  items?: GraphProjectionItems
  distinct?: boolean
  where?: GraphExpr | null
  orderBy?: Array<GraphOrderItem>
  skip?: GraphExpr | null
  limit?: GraphExpr | null
}

export type GraphShortestPathEndpoint =
  | string
  | number
  | { alias: string }
  | { nodeId: number }
  | { nodeKey: { label: string; key: string } }
  | { expr: GraphExpr }

export interface GraphPipelineShortestPathStage {
  kind: 'shortestPath' | 'shortest_path'
  optional?: boolean
  outputPathAlias: string
  mode?: 'one' | 'all'
  from: GraphShortestPathEndpoint
  to: GraphShortestPathEndpoint
  direction?: 'outgoing' | 'incoming' | 'both'
  edgeLabelFilter?: Array<string>
  minHops: number
  maxHops: number
  weightField?: string | null
  maxCost?: number | null
  maxPaths?: number | null
}

export interface GraphPipelineCallPayload {
  query: GraphPipelineRequest
  importAliases?: Array<string>
}

export interface GraphPipelineCallStage extends GraphPipelineCallPayload {
  kind: 'call'
}

export interface GraphPipelineUnionStage {
  kind: 'union'
  branches: Array<GraphPipelineRequest>
  all?: boolean
}

export type GraphPipelineStage =
  | GraphPipelineMatchStage
  | GraphPipelineProjectStage
  | GraphPipelineShortestPathStage
  | GraphPipelineCallStage
  | GraphPipelineUnionStage

export interface GraphPipelineOptions {
  allowFullScan?: boolean
  maxRows?: number
  maxPipelineRows?: number
  maxGroups?: number
  maxCollectItems?: number
  maxUnionBranches?: number
  maxSubqueryInvocations?: number
  maxSubqueryDepth?: number
  maxShortestPathPairs?: number
  maxIntermediateBindings?: number
  maxFrontier?: number
  maxPathHops?: number
  maxPathsPerStart?: number
  maxOrderMaterialization?: number
  maxSkip?: number
  maxCursorBytes?: number
  maxQueryBytes?: number
  maxParamBytes?: number
  maxAstDepth?: number
  maxLiteralItems?: number
  includePlan?: boolean
  profile?: boolean
}

export interface GraphPipelineRequest {
  stages: Array<GraphPipelineStage>
  params?: Record<string, GraphParamValue>
  atEpoch?: number | null
  skip?: number
  limit?: number
  cursor?: string | null
  output?: GraphOutputOptions
  options?: GraphPipelineOptions
}

export interface GraphPipelineStats {
  rowsReturned: number
  rowsEnteredPipeline: number
  rowsAfterFilter: number
  intermediateRows: number
  pipelineRowsMaterialized: number
  groups: number
  collectItems: number
  unionBranches: number
  unionDedupKeys: number
  subqueryInvocations: number
  subqueryCacheHits: number
  shortestPathPairs: number
  shortestPathCacheHits: number
  dbHits: number
  elapsedUs: number | null
  effectiveAtEpoch: number
  warnings: Array<string>
}

export interface GraphPipelineStageExplain {
  index: number
  kind: string
  detail: string
  columns: Array<string>
  graphRow: GraphRowExplain | null
  warnings: Array<string>
  notes: Array<string>
}

export interface GraphPipelineExplain {
  columns: Array<string>
  effectiveAtEpoch: number | null
  fingerprint: string
  stages: Array<GraphPipelineStageExplain>
  rowOps: Array<GraphRowOperationExplain>
  order: { explicit: boolean; items: number; stableLogicalRowKey: boolean }
  cursor: { supplied: boolean; codecImplemented: boolean; message: string | null }
  projection: { columns: Array<string>; outputMode: 'ids' | 'elements' | 'projected'; includeVectors: boolean; compactRows: boolean }
  caps: {
    allowFullScan: boolean
    maxRows: number
    maxPipelineRows: number
    maxGroups: number
    maxCollectItems: number
    maxUnionBranches: number
    maxSubqueryInvocations: number
    maxSubqueryDepth: number
    maxShortestPathPairs: number
    maxIntermediateBindings: number
    maxFrontier: number
    maxPathHops: number
    maxPathsPerStart: number
    maxOrderMaterialization: number
    maxSkip: number
    maxCursorBytes: number
    maxQueryBytes: number
    maxParamBytes: number
    maxAstDepth: number
    maxLiteralItems: number
  }
  summaries: { validationOnly: boolean; rowsPlanned: number; warnings: Array<string> }
  stats: GraphPipelineStats
  warnings: Array<string>
  notes: Array<string>
}

export interface GraphPipelineObjectRowsResult {
  columns: Array<string>
  rows: Array<Record<string, GraphValue>>
  nextCursor: string | null
  stats: GraphPipelineStats
  plan: GraphPipelineExplain | null
}

export interface GraphPipelineCompactRowsResult {
  columns: Array<string>
  rows: Array<Array<GraphValue>>
  nextCursor: string | null
  stats: GraphPipelineStats
  plan: GraphPipelineExplain | null
}

export type GraphPipelineResult = GraphPipelineObjectRowsResult | GraphPipelineCompactRowsResult

export type QueryPlanKind = 'node_query' | 'edge_query'

export type QueryPlanWarning =
  | 'missing_ready_index'
  | 'using_fallback_scan'
  | 'full_scan_requires_opt_in'
  | 'full_scan_explicitly_allowed'
  | 'edge_property_post_filter'
  | 'index_skipped_as_broad'
  | 'candidate_cap_exceeded'
  | 'range_candidate_cap_exceeded'
  | 'timestamp_candidate_cap_exceeded'
  | 'verify_only_filter'
  | 'boolean_branch_fallback'
  | 'planning_probe_budget_exceeded'
  | 'compound_index_prefix_not_satisfied'
  | 'unknown_node_label'
  | 'unknown_edge_label'

export type QueryPlanNote =
  | 'node_label_any_dedupe_before_pagination'
  | 'node_label_any_final_verification'
  | 'node_label_all_superset_verification'
  | 'stale_node_label_membership_verification'

export interface QueryPlanPublicName {
  alias?: string | null
  name: string
  known: boolean
  mode?: LabelMatchMode | null
}

export interface QueryPlanPublicInputs {
  nodeLabels: Array<QueryPlanPublicName>
  edgeLabels: Array<QueryPlanPublicName>
}

export type QueryPlanCompoundTargetKind = 'node' | 'edge'
export type QueryPlanSecondaryIndexKind = 'equality' | 'range'
export type QueryPlanNodeMetadataIndexField = 'id' | 'key' | 'weight' | 'created_at' | 'updated_at'
export type QueryPlanEdgeMetadataIndexField =
  | 'id'
  | 'from'
  | 'to'
  | 'weight'
  | 'created_at'
  | 'updated_at'
  | 'valid_from'
  | 'valid_to'

export type QueryPlanSecondaryIndexField =
  | { source: 'property'; key: string; field?: null }
  | { source: 'metadata'; key?: null; field: QueryPlanNodeMetadataIndexField | QueryPlanEdgeMetadataIndexField }

export interface QueryPlanCompoundIndexDetails {
  indexId: number
  targetKind: QueryPlanCompoundTargetKind
  label: string | null
  kind: QueryPlanSecondaryIndexKind
  fields: Array<QueryPlanSecondaryIndexField>
  compound: boolean
  matchedPrefixLen: number
  rangeField: QueryPlanSecondaryIndexField | null
  inExpansions: number
  estimatedCandidates: number | null
  coverage: string
  residualPredicates: number
  finalVerification: boolean
  fallbackReason: string | null
}

export type QueryPlanNode =
  | { kind: 'explicit_ids' }
  | { kind: 'key_lookup' }
  | { kind: 'node_label_index' }
  | { kind: 'node_label_any_index' }
  | { kind: 'compound_equality_index'; details: QueryPlanCompoundIndexDetails }
  | { kind: 'compound_range_index'; details: QueryPlanCompoundIndexDetails }
  | { kind: 'property_equality_index' }
  | { kind: 'property_range_index' }
  | { kind: 'timestamp_index' }
  | { kind: 'adjacency_expansion' }
  | { kind: 'explicit_edge_ids' }
  | { kind: 'edge_label_index' }
  | { kind: 'edge_triple_index' }
  | { kind: 'edge_endpoint_adjacency' }
  | { kind: 'edge_weight_index' }
  | { kind: 'edge_updated_at_index' }
  | { kind: 'edge_validity_index' }
  | { kind: 'edge_metadata_scan' }
  | { kind: 'edge_property_equality_index' }
  | { kind: 'edge_property_range_index' }
  | { kind: 'intersect'; inputs: Array<QueryPlanNode> }
  | { kind: 'union'; inputs: Array<QueryPlanNode> }
  | { kind: 'verify_node_filter'; input: QueryPlanNode }
  | { kind: 'verify_edge_filter'; input: QueryPlanNode }
  | { kind: 'verify_edge_predicates'; input: QueryPlanNode }
  | { kind: 'fallback_node_label_scan' }
  | { kind: 'fallback_full_node_scan' }
  | { kind: 'fallback_edge_label_scan' }
  | { kind: 'fallback_full_edge_scan' }
  | { kind: 'empty_result' }

export interface QueryPlan {
  kind: QueryPlanKind
  root: QueryPlanNode
  estimatedCandidates: number | null
  warnings: Array<QueryPlanWarning>
  notes: Array<QueryPlanNote>
  publicInputs: QueryPlanPublicInputs
}

export type GqlParam =
  | null
  | boolean
  | number
  | string
  | Buffer
  | Array<GqlParam>
  | { [key: string]: GqlParam }

export type GqlParams = Record<string, GqlParam>

export type GqlExecutionMode = 'auto' | 'readOnly'

export type GqlStatementKind = 'query' | 'mutation' | 'schema' | 'index'

export interface GqlExecutionOptions {
  mode?: GqlExecutionMode
  allowFullScan?: boolean
  maxRows?: number
  cursor?: string | null
  maxCursorBytes?: number
  maxMutationRows?: number
  maxMutationOps?: number
  maxPipelineRows?: number
  maxGroups?: number
  maxCollectItems?: number
  maxUnionBranches?: number
  maxSubqueryInvocations?: number
  maxSubqueryDepth?: number
  maxShortestPathPairs?: number
  maxIntermediateBindings?: number
  maxFrontier?: number
  maxPathHops?: number
  maxPathsPerStart?: number
  maxOrderMaterialization?: number
  maxSkip?: number
  maxQueryBytes?: number
  maxParamBytes?: number
  maxAstDepth?: number
  maxLiteralItems?: number
  includePlan?: boolean
  profile?: boolean
  compactRows?: boolean
  includeVectors?: boolean
}

export type GqlScalar = null | boolean | number | string | Buffer

export type GqlValue =
  | GqlScalar
  | Array<GqlValue>
  | { [key: string]: GqlValue }
  | GqlNode
  | GqlEdge
  | GqlPath

export interface GqlNode {
  id?: number
  labels?: Array<string>
  key?: string
  props?: Record<string, GqlValue>
  weight?: number
  createdAt?: number
  updatedAt?: number
  denseVector?: Array<number>
  sparseVector?: Array<{ dimension: number; value: number }>
}

export interface GqlEdge {
  id?: number
  from?: number
  to?: number
  label?: string
  props?: Record<string, GqlValue>
  weight?: number
  createdAt?: number
  updatedAt?: number
  validFrom?: number
  validTo?: number
}

export interface GqlPath {
  nodeIds: Array<number>
  edgeIds: Array<number>
  nodes?: Array<GqlNode>
  edges?: Array<GqlEdge>
}

export interface GqlExecutionStats {
  rowsReturned: number
  rowsMatched: number
  rowsAfterFilter: number
  intermediateBindings: number
  dbHits: number
  elapsedUs: number | null
  warnings: Array<string>
}

export interface GqlCapSummary {
  allowFullScan: boolean
  maxRows: number
  maxIntermediateBindings: number
  maxSkip: number
  maxQueryBytes: number
  maxParamBytes: number
  maxAstDepth: number
  maxLiteralItems: number
}

export interface GqlExecutionCapSummary {
  allowFullScan: boolean
  maxRows: number
  maxCursorBytes: number
  maxMutationRows: number
  maxMutationOps: number
  maxPipelineRows: number
  maxGroups: number
  maxCollectItems: number
  maxUnionBranches: number
  maxSubqueryInvocations: number
  maxSubqueryDepth: number
  maxShortestPathPairs: number
  maxQueryBytes: number
  maxParamBytes: number
  maxAstDepth: number
  maxLiteralItems: number
  maxIntermediateBindings: number
  maxFrontier: number
  maxPathHops: number
  maxPathsPerStart: number
  maxOrderMaterialization: number
  maxSkip: number
}

export type GqlLoweringTarget = 'node_query' | 'edge_query' | 'graph_row_query' | 'graph_pipeline_query'

export type GqlRowOperation = 'residual_filter' | 'projection' | 'sort' | 'skip' | 'limit'

export interface GqlReadExplain {
  columns: Array<string>
  target: GqlLoweringTarget
  nativePlan: QueryPlan | null
  pushedDown: Array<string>
  residual: Array<string>
  projection: Array<string>
  rowOps: Array<GqlRowOperation>
  caps: GqlCapSummary
  warnings: Array<string>
}

export interface GqlMutationReadPrefixExplain {
  graphRowTarget: GqlReadExplain
  internalColumns: Array<string>
  targetAliases: Array<string>
  expressionColumns: number
}

export interface GqlMutationOperationExplain {
  op: string
  targetAlias: string | null
  rowMultiplicity: string
  detail: string
}

export interface GqlMutationReturnExplain {
  columns: Array<string>
  orderItems: number
  skip: number
  limit: number | null
  postCommitHydration: string
}

export interface GqlMutationExplain {
  readPrefix: GqlMutationReadPrefixExplain | null
  operations: Array<GqlMutationOperationExplain>
  returnPlan: GqlMutationReturnExplain | null
  wouldCreateNodeLabels: Array<string>
  wouldCreateEdgeLabels: Array<string>
  usesTransactionSnapshot: boolean
  usesWriteTxn: boolean
  replacementAdapters: boolean
  atomicCommit: boolean
}

export interface GqlSchemaExplainTarget {
  targetKind: string
  label: string | null
  action: string | null
}

export interface GqlSchemaExplainOptions {
  maxViolations: number | null
  chunkSize: number | null
  scanLimit: number | null
}

export interface GqlSchemaExplain {
  operation: string
  targets: Array<GqlSchemaExplainTarget>
  replacesEntireCatalog: boolean
  publishesManifest: boolean
  validatesExistingData: boolean
  usesCoreWriteQueue: boolean
  sideEffectFree: boolean
  options: GqlSchemaExplainOptions
}

export interface GqlIndexExplainField {
  source: string
  key: string | null
  field: string | null
}

export interface GqlIndexExplainTarget {
  targetKind: string
  label: string | null
  fields: Array<GqlIndexExplainField>
  kind: string | null
  action: string | null
  compound: boolean
}

export interface GqlIndexExplain {
  operation: string
  targets: Array<GqlIndexExplainTarget>
  usesCoreWriteQueue: boolean
  publishesManifest: boolean
  createsLabels: boolean
  schedulesBackgroundBuild: boolean
  dropsIndexDataAsync: boolean
  sideEffectFree: boolean
}

export interface GqlExecutionExplain {
  kind: GqlStatementKind
  columns: Array<string>
  read: GqlReadExplain | null
  mutation: GqlMutationExplain | null
  schema: GqlSchemaExplain | null
  index: GqlIndexExplain | null
  caps: GqlExecutionCapSummary
  warnings: Array<string>
  notes: Array<string>
}

export interface GqlMutationStats {
  rowsMatched: number
  mutationRows: number
  mutationOps: number
  nodesCreated: number
  nodesUpdated: number
  nodesDeleted: number
  edgesCreated: number
  edgesUpdated: number
  edgesDeleted: number
  labelsAdded: number
  labelsRemoved: number
  propertiesSet: number
  propertiesRemoved: number
  skippedNullTargets: number
  duplicateTargets: number
  dbHits: number
  elapsedUs: number | null
  warnings: Array<string>
}

export interface GqlSchemaStats {
  operation: string
  targetsChecked: number
  targetsPublished: number
  targetsDropped: number
  checkedRecords: number
  violationCount: number
  truncated: boolean
  scanLimitHit: boolean
  elapsedUs: number | null
  warnings: Array<string>
}

export interface GqlIndexStats {
  operation: string
  indexesEnsured: number
  indexesDropped: number
  indexesReturned: number
  elapsedUs: number | null
  warnings: Array<string>
}

export interface GqlObjectRowsExecutionResult {
  kind: GqlStatementKind
  columns: Array<string>
  rows: Array<Record<string, GqlValue>>
  nextCursor: string | null
  stats: GqlExecutionStats
  mutationStats: GqlMutationStats | null
  schemaStats: GqlSchemaStats | null
  indexStats: GqlIndexStats | null
  plan: GqlExecutionExplain | null
}

export interface GqlCompactRowsExecutionResult {
  kind: GqlStatementKind
  columns: Array<string>
  rows: Array<Array<GqlValue>>
  nextCursor: string | null
  stats: GqlExecutionStats
  mutationStats: GqlMutationStats | null
  schemaStats: GqlSchemaStats | null
  indexStats: GqlIndexStats | null
  plan: GqlExecutionExplain | null
}

export type GqlExecutionResult = GqlObjectRowsExecutionResult | GqlCompactRowsExecutionResult
