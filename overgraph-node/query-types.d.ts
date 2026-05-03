export type QueryPredicateOp = 'eq' | 'range'

export type NonEmptyArray<T> = [T, ...T[]]

export type QueryNodeFilter =
  | { and: NonEmptyArray<QueryNodeFilter>; or?: never; not?: never; property?: never; updatedAt?: never }
  | { or: NonEmptyArray<QueryNodeFilter>; and?: never; not?: never; property?: never; updatedAt?: never }
  | { not: QueryNodeFilter; and?: never; or?: never; property?: never; updatedAt?: never }
  | QueryNodePropertyFilter
  | QueryNodeUpdatedAtFilter

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

export interface QueryNodeRequest {
  typeId?: number
  ids?: Array<number>
  keys?: Array<string>
  filter?: QueryNodeFilter | null
  orderBy?: 'nodeIdAsc' | 'node_id_asc'
  limit?: number | null
  after?: number
  allowFullScan?: boolean
}

export interface GraphNodePattern {
  alias: string
  typeId?: number
  ids?: Array<number>
  keys?: Array<string>
  filter?: QueryNodeFilter | null
}

export interface EdgePropertyPredicate {
  property: QueryPropertyPredicatePayload
}

export interface GraphEdgePattern {
  alias?: string
  fromAlias: string
  toAlias: string
  direction?: 'outgoing' | 'incoming' | 'both'
  typeFilter?: Array<number>
  where?: Record<string, QueryWherePredicate>
  predicates?: Array<EdgePropertyPredicate>
}

export interface GraphPatternRequest {
  nodes: Array<GraphNodePattern>
  edges: Array<GraphEdgePattern>
  atEpoch?: number
  limit: number
}

export interface QueryMatch {
  nodes: Record<string, number>
  edges: Record<string, number>
}

export interface QueryPatternResult {
  matches: Array<QueryMatch>
  truncated: boolean
}

export type QueryPlanKind = 'node_query' | 'pattern_query'

export type QueryPlanWarning =
  | 'missing_ready_index'
  | 'using_fallback_scan'
  | 'full_scan_requires_opt_in'
  | 'full_scan_explicitly_allowed'
  | 'unbounded_pattern_rejected'
  | 'edge_property_post_filter'
  | 'index_skipped_as_broad'
  | 'candidate_cap_exceeded'
  | 'range_candidate_cap_exceeded'
  | 'timestamp_candidate_cap_exceeded'
  | 'verify_only_filter'
  | 'boolean_branch_fallback'
  | 'planning_probe_budget_exceeded'

export type QueryPlanNode =
  | { kind: 'explicit_ids' }
  | { kind: 'key_lookup' }
  | { kind: 'node_type_index' }
  | { kind: 'property_equality_index' }
  | { kind: 'property_range_index' }
  | { kind: 'timestamp_index' }
  | { kind: 'adjacency_expansion' }
  | { kind: 'intersect'; inputs: Array<QueryPlanNode> }
  | { kind: 'union'; inputs: Array<QueryPlanNode> }
  | { kind: 'verify_node_filter'; input: QueryPlanNode }
  | { kind: 'verify_edge_predicates'; input: QueryPlanNode }
  | { kind: 'pattern_expand'; anchorAlias: string; input: QueryPlanNode }
  | { kind: 'fallback_type_scan' }
  | { kind: 'fallback_full_node_scan' }
  | { kind: 'empty_result' }

export interface QueryPlan {
  kind: QueryPlanKind
  root: QueryPlanNode
  estimatedCandidates: number | null
  warnings: Array<QueryPlanWarning>
}
