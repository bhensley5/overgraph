#![allow(dead_code)]

use crate::engine::GraphFixedPathBinding;
use crate::error::EngineError;
use crate::gql::ast::*;
use crate::gql::metadata::{GqlElementMapMetadataKey, GqlMetadataFunction};
use crate::gql::params::{validate_referenced_gql_mutation_params, validate_referenced_gql_params};
use crate::gql::semantic::{
    bind_mutation, bind_query, bind_subquery_pipeline_for_outer_aliases, edge_endpoint_id_call,
    expression_output_name, gql_semantic_error, variable_name, GqlAliasBinding, GqlAliasKind,
    GqlAliasOrigin,
    GqlAliasTable, GqlBoundCallSubquery, GqlBoundCreateEdge, GqlBoundCreateNode,
    GqlBoundEdgePattern, GqlBoundMatchClause, GqlBoundMergeClause, GqlBoundMergePattern,
    GqlBoundMutationClause, GqlBoundNodePattern, GqlBoundPattern, GqlBoundPipelineClause,
    GqlBoundProjectionClause, GqlBoundRemoveItem, GqlBoundSetItem, GqlBoundShortestPathClause,
    GqlMutationSemanticPlan, GqlReturnPlan, GqlSemanticPlan,
};
use crate::row_projection::{DIRECT_EDGE_ALIAS, DIRECT_NODE_ALIAS};
use crate::types::{
    Direction, EdgeFilterExpr, GqlExecutionOptions, GqlParamValue, GqlParams, GqlSemanticErrorCode,
    GraphAggregateFunction, GraphBinaryOp, GraphCaseBranch, GraphEdgeField, GraphEdgePattern,
    GraphElementProjection, GraphExpr, GraphFunction, GraphNodeField, GraphNodePattern,
    GraphOptionalGroup, GraphOrderDirection, GraphOrderItem, GraphOutputMode, GraphOutputOptions,
    GraphPageRequest, GraphParamValue, GraphPathField, GraphPatternPiece, GraphPipelineMatchStage,
    GraphPipelineOptions, GraphPipelineQuery, GraphPipelineStage, GraphProjectItem,
    GraphProjectKind, GraphProjectStage, GraphProjectionItems, GraphQueryOptions, GraphReturnItem,
    GraphReturnProjection, GraphRowQuery, GraphShortestPathEndpoint, GraphShortestPathMode,
    GraphShortestPathStage, GraphSubqueryStage, GraphUnaryOp, GraphUnionStage,
    GraphVariableLengthPattern, LabelMatchMode, NodeFilterExpr, NodeKeyQuery, NodeLabelFilter,
    PropValue, PropertyRangeBound, SourceSpan,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlNativeTargetKind {
    GraphRows,
    GraphPipeline,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlNativeTarget {
    GraphRows { query: GraphRowQueryTarget },
    GraphPipeline { query: GraphPipelineQuery },
}

impl GqlNativeTarget {
    pub(crate) fn kind(&self) -> GqlNativeTargetKind {
        match self {
            Self::GraphRows { .. } => GqlNativeTargetKind::GraphRows,
            Self::GraphPipeline { .. } => GqlNativeTargetKind::GraphPipeline,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphRowQueryTarget {
    pub(crate) query: GraphRowQuery,
    pub(crate) fixed_paths: Vec<GraphFixedPathBinding>,
    pub(crate) edge_id_constraints: BTreeMap<String, Vec<u64>>,
    pub(crate) logical_limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlPushedPredicate {
    pub(crate) alias: String,
    pub(crate) target_kind: GqlAliasKind,
    pub(crate) summary: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlLoweredPlan {
    pub(crate) semantic: GqlSemanticPlan,
    pub(crate) native_target: GqlNativeTarget,
    pub(crate) residual_predicates: Vec<Expr>,
    pub(crate) order_by: Vec<OrderItem>,
    pub(crate) skip: Option<Expr>,
    pub(crate) limit: Option<Expr>,
    pub(crate) pushed_down: Vec<GqlPushedPredicate>,
    pub(crate) warnings: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationPlan {
    pub(crate) semantic: GqlMutationSemanticPlan,
    pub(crate) read_prefix: Option<GqlMutationReadPrefixPlan>,
    pub(crate) clauses: Vec<GqlMutationClausePlan>,
    pub(crate) return_plan: Option<GqlMutationReturnPlan>,
    pub(crate) operation_exprs: Vec<GqlMutationExprPlan>,
    pub(crate) params_used: Vec<String>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationReadPrefixPlan {
    pub(crate) graph_row: Option<GraphRowQueryTarget>,
    pub(crate) lowered: Box<GqlLoweredPlan>,
    pub(crate) internal_columns: Vec<GqlMutationInternalColumn>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlMutationInternalColumn {
    TargetId { alias: String, kind: GqlAliasKind },
    TargetPath { alias: String },
    ScalarValue { alias: String, expr: GraphExpr },
    ExprValue { id: usize, expr: GraphExpr },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationExprPlan {
    pub(crate) id: usize,
    pub(crate) expr: GraphExpr,
    pub(crate) source: Expr,
    pub(crate) late: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct GqlMutationOperationExpr {
    expr: Expr,
    late: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlMutationExprRef {
    pub(crate) id: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlMutationClausePlan {
    Create(Vec<GqlCreatePatternPlan>),
    Merge(GqlMergePlan),
    Set(Vec<GqlSetItemPlan>),
    Remove(Vec<GqlRemoveItemPlan>),
    Delete {
        detach: bool,
        targets: Vec<GqlDeleteTargetPlan>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMergePlan {
    pub(crate) pattern: GqlMergePatternPlan,
    pub(crate) on_create: Vec<GqlSetItemPlan>,
    pub(crate) on_match: Vec<GqlSetItemPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlMergePatternPlan {
    Node {
        alias: String,
        label: String,
        key: GqlMutationExprRef,
    },
    Relationship {
        alias: String,
        from_alias: String,
        to_alias: String,
        label: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlCreatePatternPlan {
    pub(crate) nodes: Vec<GqlCreateNodePlan>,
    pub(crate) edges: Vec<GqlCreateEdgePlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlCreateNodePlan {
    pub(crate) alias: String,
    pub(crate) labels: Vec<String>,
    pub(crate) element_key: Option<GqlMutationExprRef>,
    pub(crate) weight: Option<GqlMutationExprRef>,
    pub(crate) property_keys: Vec<String>,
    pub(crate) property_values: BTreeMap<String, GqlMutationExprRef>,
    pub(crate) created: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlCreateEdgePlan {
    pub(crate) alias: Option<String>,
    pub(crate) from_alias: String,
    pub(crate) to_alias: String,
    pub(crate) label: String,
    pub(crate) weight: Option<GqlMutationExprRef>,
    pub(crate) valid_from: Option<GqlMutationExprRef>,
    pub(crate) valid_to: Option<GqlMutationExprRef>,
    pub(crate) property_keys: Vec<String>,
    pub(crate) property_values: BTreeMap<String, GqlMutationExprRef>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlSetItemPlan {
    Property {
        alias: String,
        kind: GqlAliasKind,
        property: String,
        value: GqlMutationExprRef,
    },
    Metadata {
        alias: String,
        kind: GqlAliasKind,
        field: GqlMetadataFunction,
        value: GqlMutationExprRef,
    },
    MapMerge {
        alias: String,
        kind: GqlAliasKind,
        value: GqlMutationExprRef,
    },
    NodeLabel {
        alias: String,
        label: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlRemoveItemPlan {
    Property {
        alias: String,
        kind: GqlAliasKind,
        property: String,
    },
    NodeLabel {
        alias: String,
        label: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlDeleteTargetPlan {
    pub(crate) alias: String,
    pub(crate) kind: GqlAliasKind,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationReturnPlan {
    pub(crate) columns: Vec<String>,
    pub(crate) distinct: bool,
    pub(crate) order_items: usize,
    pub(crate) skip: Option<Expr>,
    pub(crate) limit: Option<Expr>,
}

pub(crate) fn lower_query(
    query: GqlQuery,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlLoweredPlan, EngineError> {
    let semantic = bind_query(query, params)?;
    validate_referenced_gql_params(&semantic, params, options)?;
    lower_semantic_plan(semantic, params, options)
}

pub(crate) fn lower_mutation(
    mutation: GqlMutationStatement,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlMutationPlan, EngineError> {
    let semantic = bind_mutation(mutation, params)?;
    validate_referenced_gql_mutation_params(&semantic, params, options)?;
    lower_mutation_semantic_plan(semantic, params, options)
}

pub(crate) fn lower_semantic_plan(
    semantic: GqlSemanticPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlLoweredPlan, EngineError> {
    if semantic.query.requires_deferred_pipeline_execution()
        || gql_query_contains_aggregate(&semantic.query)
        || gql_query_contains_subquery(&semantic.query)
    {
        return lower_read_pipeline_semantic_plan(semantic, params, options);
    }

    let mut state = LoweringState::new(params, &semantic);
    let mut graph_nodes = Vec::new();
    let mut node_indexes = BTreeMap::new();
    let mut pieces = Vec::new();
    let mut fixed_paths = Vec::new();
    let mut required_where = Vec::new();

    for clause in &semantic.clauses {
        if clause.patterns.len() != 1 {
            return Err(EngineError::GqlUnsupported {
                feature: "multiple MATCH patterns".to_string(),
                message: "multiple comma-separated MATCH patterns are not supported".to_string(),
                span: clause.span.clone(),
            });
        }
        let pattern = &clause.patterns[0];
        reject_unsupported_pure_edge_label_or(&semantic.clauses, pattern, params)?;
        let reused_node_constraints =
            state.collect_graph_nodes(pattern, &mut graph_nodes, &mut node_indexes)?;
        let materialize_node_only =
            clause.optional || semantic.clauses.len() > 1 || pattern.path_alias.is_some();
        let clause_pieces = state.lower_pattern_pieces(pattern, materialize_node_only)?;
        if clause.optional {
            let optional_piece_index = pieces.len();
            fixed_paths.extend(state.fixed_paths_for_pattern(
                pattern,
                &clause_pieces,
                vec![optional_piece_index],
                0,
            )?);
            let mut local_where = reused_node_constraints;
            if let Some(where_clause) = clause.where_clause.as_ref() {
                local_where.push(where_clause.clone());
            }
            let where_ = combine_gql_predicates(local_where)
                .map(|expr| gql_expr_to_graph_expr(&expr, &state.alias_kinds))
                .transpose()?;
            pieces.push(GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: clause_pieces,
                where_,
            }));
        } else {
            let base_piece_index = pieces.len();
            fixed_paths.extend(state.fixed_paths_for_pattern(
                pattern,
                &clause_pieces,
                Vec::new(),
                base_piece_index,
            )?);
            pieces.extend(clause_pieces);
            required_where.extend(reused_node_constraints);
            if let Some(where_clause) = clause.where_clause.as_ref() {
                required_where.push(where_clause.clone());
            }
        }
    }

    let fixed_edge_indexes = graph_fixed_edge_indexes(&pieces);
    for where_clause in &required_where {
        state.apply_where_to_graph_pattern(
            where_clause,
            &mut graph_nodes,
            &mut pieces,
            &node_indexes,
            &fixed_edge_indexes,
        )?;
    }

    if options.allow_full_scan && !pattern_has_anchor(&graph_nodes, &graph_fixed_edges(&pieces)) {
        state
            .warnings
            .push("full scan explicitly allowed for unanchored graph pattern".to_string());
    }

    let mut native_target = GqlNativeTarget::GraphRows {
        query: GraphRowQueryTarget {
            query: state.base_graph_row_query(graph_nodes, pieces, options),
            fixed_paths,
            edge_id_constraints: state.edge_id_constraints.clone(),
            logical_limit: None,
        },
    };
    state.finalize_graph_row_target(&semantic, options, &mut native_target)?;

    let order_by = semantic.query.order_by.clone();
    let skip = semantic.query.skip.clone();
    let limit = semantic.query.limit.clone();

    Ok(GqlLoweredPlan {
        semantic,
        native_target,
        residual_predicates: state.residual_predicates,
        order_by,
        skip,
        limit,
        pushed_down: state.pushed_down,
        warnings: state.warnings,
        notes: state.notes,
    })
}

fn lower_read_pipeline_semantic_plan(
    semantic: GqlSemanticPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlLoweredPlan, EngineError> {
    let mut lowered = lower_bound_read_pipeline(&semantic.pipeline, params, options, 0)?;

    lowered.warnings.sort();
    lowered.warnings.dedup();
    lowered.notes.sort();
    lowered.notes.dedup();

    Ok(GqlLoweredPlan {
        semantic,
        native_target: GqlNativeTarget::GraphPipeline {
            query: GraphPipelineQuery {
                stages: lowered.stages,
                params: gql_params_to_graph_params(params),
                at_epoch: None,
                page: GraphPageRequest {
                    skip: 0,
                    limit: options.max_rows.max(1),
                    cursor: options.cursor.clone(),
                },
                output: GraphOutputOptions {
                    mode: GraphOutputMode::Ids,
                    compact_rows: options.compact_rows,
                    include_vectors: options.include_vectors,
                },
                options: gql_pipeline_options(options),
            },
        },
        residual_predicates: lowered.residual_predicates,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        pushed_down: lowered.pushed_down,
        warnings: lowered.warnings,
        notes: lowered.notes,
    })
}

#[derive(Default)]
struct LoweredReadPipelineStages {
    stages: Vec<GraphPipelineStage>,
    residual_predicates: Vec<Expr>,
    pushed_down: Vec<GqlPushedPredicate>,
    warnings: Vec<String>,
    notes: Vec<String>,
}

fn lower_bound_read_pipeline(
    pipeline: &crate::gql::semantic::GqlBoundReadPipeline,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
) -> Result<LoweredReadPipelineStages, EngineError> {
    lower_bound_read_pipeline_with_alias_kinds(
        pipeline,
        params,
        options,
        subquery_depth,
        BTreeMap::new(),
    )
}

fn lower_bound_read_pipeline_with_alias_kinds(
    pipeline: &crate::gql::semantic::GqlBoundReadPipeline,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
    initial_alias_kinds: BTreeMap<String, GqlAliasKind>,
) -> Result<LoweredReadPipelineStages, EngineError> {
    if pipeline.union_branches.is_empty() {
        lower_bound_read_pipeline_clauses(
            &pipeline.clauses,
            params,
            options,
            subquery_depth,
            initial_alias_kinds,
        )
    } else {
        lower_union_read_pipeline(
            pipeline,
            params,
            options,
            subquery_depth,
            initial_alias_kinds,
        )
    }
}

fn lower_union_read_pipeline(
    pipeline: &crate::gql::semantic::GqlBoundReadPipeline,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
    initial_alias_kinds: BTreeMap<String, GqlAliasKind>,
) -> Result<LoweredReadPipelineStages, EngineError> {
    let branch_count = 1 + pipeline.union_branches.len();
    if branch_count > options.max_union_branches {
        return Err(EngineError::InvalidOperation(format!(
            "GQL UNION has {branch_count} branch(es), exceeding max_union_branches {}",
            options.max_union_branches
        )));
    }
    let union_modifier = pipeline
        .union_branches
        .first()
        .map(|branch| branch.modifier)
        .expect("union branch exists");
    if pipeline
        .union_branches
        .iter()
        .any(|branch| branch.modifier != union_modifier)
    {
        return Err(EngineError::GqlUnsupported {
            feature: "mixed UNION modifiers".to_string(),
            message:
                "mixing UNION and UNION ALL in one statement is not supported in this checkpoint"
                    .to_string(),
            span: pipeline
                .union_branches
                .iter()
                .find(|branch| branch.modifier != union_modifier)
                .map(|branch| branch.union_span.clone())
                .unwrap_or_else(|| SourceSpan::new(0, 0, 1, 1)),
        });
    }

    let mut combined = LoweredReadPipelineStages::default();
    let mut branches = Vec::with_capacity(branch_count);
    let first = lower_bound_read_pipeline_clauses(
        &pipeline.clauses,
        params,
        options,
        subquery_depth,
        initial_alias_kinds.clone(),
    )?;
    combined.merge_from(&first);
    branches.push(lowered_branch_query(first, params, options));
    for branch in &pipeline.union_branches {
        let lowered = lower_bound_read_pipeline_clauses(
            &branch.clauses,
            params,
            options,
            subquery_depth,
            initial_alias_kinds.clone(),
        )?;
        combined.merge_from(&lowered);
        branches.push(lowered_branch_query(lowered, params, options));
    }
    combined.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches,
        all: union_modifier == GqlUnionModifier::All,
    })];
    Ok(combined)
}

impl LoweredReadPipelineStages {
    fn merge_from(&mut self, other: &LoweredReadPipelineStages) {
        self.residual_predicates
            .extend(other.residual_predicates.iter().cloned());
        self.pushed_down.extend(other.pushed_down.iter().cloned());
        self.warnings.extend(other.warnings.iter().cloned());
        self.notes.extend(other.notes.iter().cloned());
    }
}

fn lowered_branch_query(
    lowered: LoweredReadPipelineStages,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> GraphPipelineQuery {
    GraphPipelineQuery {
        stages: lowered.stages,
        params: gql_params_to_graph_params(params),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: options.max_rows.max(1),
            cursor: None,
        },
        output: GraphOutputOptions {
            mode: GraphOutputMode::Ids,
            compact_rows: options.compact_rows,
            include_vectors: options.include_vectors,
        },
        options: gql_pipeline_options(options),
    }
}

fn lower_bound_read_pipeline_clauses(
    clauses: &[GqlBoundPipelineClause],
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
    initial_alias_kinds: BTreeMap<String, GqlAliasKind>,
) -> Result<LoweredReadPipelineStages, EngineError> {
    let mut lowered = LoweredReadPipelineStages::default();
    let mut current_alias_kinds = initial_alias_kinds;
    let mut saw_terminal_return = false;
    let branch_match_clauses = clauses
        .iter()
        .flat_map(|clause| match clause {
            GqlBoundPipelineClause::Match(clauses) => clauses.clone(),
            GqlBoundPipelineClause::ShortestPath(_) => Vec::new(),
            GqlBoundPipelineClause::Call(_) => Vec::new(),
            GqlBoundPipelineClause::Projection(_) => Vec::new(),
        })
        .collect::<Vec<_>>();

    for clause in clauses {
        if saw_terminal_return {
            return Err(EngineError::InvalidOperation(
                "GQL read pipeline has stages after terminal RETURN".to_string(),
            ));
        }
        match clause {
            GqlBoundPipelineClause::Match(clauses) => {
                for match_clause in clauses {
                    let mut stage_clause = match_clause.clone();
                    let (match_filter, subquery_filter) =
                        split_match_where_for_subquery_filter(stage_clause.where_clause.take());
                    stage_clause.where_clause = match_filter;
                    let (mut stage, stage_state, next_alias_kinds) = lower_pipeline_match_clause(
                        &branch_match_clauses,
                        &stage_clause,
                        params,
                        &current_alias_kinds,
                    )?;
                    lowered
                        .residual_predicates
                        .extend(stage_state.residual_predicates);
                    lowered.pushed_down.extend(stage_state.pushed_down);
                    lowered.warnings.extend(stage_state.warnings);
                    lowered.notes.extend(stage_state.notes);
                    let subquery_filter = if let Some(filter) = subquery_filter {
                        if match_clause.optional {
                            stage.optional_candidate_where =
                                Some(gql_expr_to_graph_expr_for_pipeline(
                                    &filter,
                                    &next_alias_kinds,
                                    params,
                                    options,
                                    subquery_depth,
                                )?);
                            None
                        } else {
                            Some(filter)
                        }
                    } else {
                        None
                    };
                    current_alias_kinds = next_alias_kinds;
                    lowered.stages.push(GraphPipelineStage::Match(stage));
                    if let Some(filter) = subquery_filter {
                        lowered
                            .stages
                            .push(GraphPipelineStage::Project(GraphProjectStage {
                                kind: GraphProjectKind::With,
                                items: GraphProjectionItems::Star,
                                distinct: false,
                                where_: Some(gql_expr_to_graph_expr_for_pipeline(
                                    &filter,
                                    &current_alias_kinds,
                                    params,
                                    options,
                                    subquery_depth,
                                )?),
                                order_by: Vec::new(),
                                skip: None,
                                limit: None,
                            }));
                    }
                }
            }
            GqlBoundPipelineClause::ShortestPath(shortest) => {
                let stage = lower_pipeline_shortest_path_clause(shortest)?;
                current_alias_kinds.insert(shortest.output_path_alias.clone(), GqlAliasKind::Path);
                lowered.stages.push(GraphPipelineStage::ShortestPath(stage));
            }
            GqlBoundPipelineClause::Call(call) => {
                let stage = lower_pipeline_call_subquery(
                    call,
                    params,
                    options,
                    subquery_depth,
                    &current_alias_kinds,
                )?;
                for output in &call.output_aliases {
                    current_alias_kinds.insert(output.name.clone(), output.kind);
                }
                lowered.stages.push(GraphPipelineStage::Call(stage));
            }
            GqlBoundPipelineClause::Projection(projection) => {
                let output_alias_kinds = projection_alias_kinds(projection);
                let stage = lower_pipeline_projection_clause(
                    projection,
                    &current_alias_kinds,
                    &output_alias_kinds,
                    params,
                    options,
                    subquery_depth,
                )?;
                if projection.kind == GqlProjectionKind::Return {
                    saw_terminal_return = true;
                } else {
                    current_alias_kinds = output_alias_kinds;
                }
                lowered.stages.push(GraphPipelineStage::Project(stage));
            }
        }
    }

    if !saw_terminal_return {
        return Err(EngineError::InvalidOperation(
            "GQL read pipeline must end in RETURN".to_string(),
        ));
    }
    Ok(lowered)
}

fn gql_query_contains_aggregate(query: &GqlQuery) -> bool {
    gql_read_pipeline_contains_aggregate(&query.pipeline)
}

fn gql_read_pipeline_contains_aggregate(pipeline: &GqlReadPipeline) -> bool {
    pipeline
        .clauses
        .iter()
        .any(gql_pipeline_clause_contains_aggregate)
        || pipeline.union_branches.iter().any(|branch| {
            branch
                .clauses
                .iter()
                .any(gql_pipeline_clause_contains_aggregate)
        })
}

fn gql_pipeline_clause_contains_aggregate(clause: &GqlPipelineClause) -> bool {
    match clause {
        GqlPipelineClause::Match(clauses) => clauses.iter().any(|clause| {
            clause
                .where_clause
                .as_ref()
                .is_some_and(gql_expr_contains_aggregate)
                || clause.patterns.iter().any(gql_pattern_contains_aggregate)
        }),
        GqlPipelineClause::ShortestPath(_) => false,
        GqlPipelineClause::Call(call) => gql_read_pipeline_contains_aggregate(&call.pipeline),
        GqlPipelineClause::Projection(projection) => {
            gql_return_body_contains_aggregate(&projection.body)
                || projection
                    .where_clause
                    .as_ref()
                    .is_some_and(gql_expr_contains_aggregate)
                || projection
                    .order_by
                    .iter()
                    .any(|item| gql_expr_contains_aggregate(&item.expr))
                || projection
                    .skip
                    .as_ref()
                    .is_some_and(gql_expr_contains_aggregate)
                || projection
                    .limit
                    .as_ref()
                    .is_some_and(gql_expr_contains_aggregate)
        }
    }
}

fn gql_query_contains_subquery(query: &GqlQuery) -> bool {
    query
        .pipeline
        .clauses
        .iter()
        .any(gql_pipeline_clause_contains_subquery)
        || query.pipeline.union_branches.iter().any(|branch| {
            branch
                .clauses
                .iter()
                .any(gql_pipeline_clause_contains_subquery)
        })
}

fn gql_pipeline_clause_contains_subquery(clause: &GqlPipelineClause) -> bool {
    match clause {
        GqlPipelineClause::Match(clauses) => clauses.iter().any(|clause| {
            clause
                .where_clause
                .as_ref()
                .is_some_and(gql_expr_contains_subquery)
                || clause.patterns.iter().any(gql_pattern_contains_subquery)
        }),
        GqlPipelineClause::ShortestPath(_) => false,
        GqlPipelineClause::Call(_) => true,
        GqlPipelineClause::Projection(projection) => {
            gql_return_body_contains_subquery(&projection.body)
                || projection
                    .where_clause
                    .as_ref()
                    .is_some_and(gql_expr_contains_subquery)
                || projection
                    .order_by
                    .iter()
                    .any(|item| gql_expr_contains_subquery(&item.expr))
                || projection
                    .skip
                    .as_ref()
                    .is_some_and(gql_expr_contains_subquery)
                || projection
                    .limit
                    .as_ref()
                    .is_some_and(gql_expr_contains_subquery)
        }
    }
}

fn gql_return_body_contains_subquery(body: &ReturnBody) -> bool {
    match body {
        ReturnBody::All(_) => false,
        ReturnBody::AllAndItems { items, .. } | ReturnBody::Items(items) => items
            .iter()
            .any(|item| gql_expr_contains_subquery(&item.expr)),
    }
}

fn gql_pattern_contains_subquery(pattern: &Pattern) -> bool {
    pattern
        .start
        .properties
        .as_ref()
        .is_some_and(gql_map_contains_subquery)
        || pattern.chains.iter().any(|chain| {
            chain
                .relationship
                .properties
                .as_ref()
                .is_some_and(gql_map_contains_subquery)
                || chain
                    .node
                    .properties
                    .as_ref()
                    .is_some_and(gql_map_contains_subquery)
        })
}

fn gql_map_contains_subquery(map: &MapLiteral) -> bool {
    map.entries
        .iter()
        .any(|entry| gql_expr_contains_subquery(&entry.value))
}

fn gql_expr_contains_subquery(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::ExistsSubquery(_) => true,
        ExprKind::PropertyAccess { object, .. } => gql_expr_contains_subquery(object),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            gql_expr_contains_subquery(expr)
        }
        ExprKind::Binary { left, right, .. } => {
            gql_expr_contains_subquery(left) || gql_expr_contains_subquery(right)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            args.iter().any(gql_expr_contains_subquery)
        }
        ExprKind::AggregateCall { arg, .. } => arg
            .as_ref()
            .is_some_and(|arg| gql_expr_contains_subquery(arg)),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| gql_expr_contains_subquery(expr))
                || branches.iter().any(|branch| {
                    gql_expr_contains_subquery(&branch.when)
                        || gql_expr_contains_subquery(&branch.then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| gql_expr_contains_subquery(expr))
        }
        ExprKind::Map(map) => gql_map_contains_subquery(map),
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => false,
    }
}

fn split_match_where_for_subquery_filter(
    where_clause: Option<Expr>,
) -> (Option<Expr>, Option<Expr>) {
    let Some(where_clause) = where_clause else {
        return (None, None);
    };
    if !gql_expr_contains_subquery(&where_clause) {
        return (Some(where_clause), None);
    }

    let mut conjuncts = Vec::new();
    flatten_gql_and_conjuncts(where_clause, &mut conjuncts);
    let mut match_conjuncts = Vec::new();
    let mut subquery_conjuncts = Vec::new();
    for conjunct in conjuncts {
        if gql_expr_contains_subquery(&conjunct) {
            subquery_conjuncts.push(conjunct);
        } else {
            match_conjuncts.push(conjunct);
        }
    }
    (
        combine_gql_and_conjuncts(match_conjuncts),
        combine_gql_and_conjuncts(subquery_conjuncts),
    )
}

fn flatten_gql_and_conjuncts(expr: Expr, out: &mut Vec<Expr>) {
    match expr.kind {
        ExprKind::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            flatten_gql_and_conjuncts(*left, out);
            flatten_gql_and_conjuncts(*right, out);
        }
        _ => out.push(expr),
    }
}

fn combine_gql_and_conjuncts(mut conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut combined = conjuncts.remove(0);
    for conjunct in conjuncts {
        let span = combine_expr_spans(&combined.span, &conjunct.span);
        combined = Expr {
            kind: ExprKind::Binary {
                op: BinaryOp::And,
                left: Box::new(combined),
                right: Box::new(conjunct),
            },
            span,
        };
    }
    Some(combined)
}

fn combine_expr_spans(left: &SourceSpan, right: &SourceSpan) -> SourceSpan {
    let start = left.offset.min(right.offset);
    let end = left.end_offset().max(right.end_offset());
    let (line, column) = if left.offset <= right.offset {
        (left.line, left.column)
    } else {
        (right.line, right.column)
    };
    SourceSpan::new(start, end.saturating_sub(start), line, column)
}

fn gql_return_body_contains_aggregate(body: &ReturnBody) -> bool {
    match body {
        ReturnBody::All(_) => false,
        ReturnBody::AllAndItems { items, .. } | ReturnBody::Items(items) => items
            .iter()
            .any(|item| gql_expr_contains_aggregate(&item.expr)),
    }
}

fn gql_pattern_contains_aggregate(pattern: &Pattern) -> bool {
    pattern
        .start
        .properties
        .as_ref()
        .is_some_and(gql_map_contains_aggregate)
        || pattern.chains.iter().any(|chain| {
            chain
                .relationship
                .properties
                .as_ref()
                .is_some_and(gql_map_contains_aggregate)
                || chain
                    .node
                    .properties
                    .as_ref()
                    .is_some_and(gql_map_contains_aggregate)
        })
}

fn gql_map_contains_aggregate(map: &MapLiteral) -> bool {
    map.entries
        .iter()
        .any(|entry| gql_expr_contains_aggregate(&entry.value))
}

fn gql_expr_contains_aggregate(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::AggregateCall { .. } => true,
        ExprKind::PropertyAccess { object, .. } => gql_expr_contains_aggregate(object),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            gql_expr_contains_aggregate(expr)
        }
        ExprKind::Binary { left, right, .. } => {
            gql_expr_contains_aggregate(left) || gql_expr_contains_aggregate(right)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            args.iter().any(gql_expr_contains_aggregate)
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| gql_expr_contains_aggregate(expr))
                || branches.iter().any(|branch| {
                    gql_expr_contains_aggregate(&branch.when)
                        || gql_expr_contains_aggregate(&branch.then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| gql_expr_contains_aggregate(expr))
        }
        ExprKind::Map(map) => gql_map_contains_aggregate(map),
        ExprKind::ExistsSubquery(pipeline) => gql_read_pipeline_contains_aggregate(pipeline),
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => false,
    }
}

fn lower_pipeline_match_clause<'a>(
    branch_clauses: &[GqlBoundMatchClause],
    clause: &GqlBoundMatchClause,
    params: &'a GqlParams,
    input_alias_kinds: &BTreeMap<String, GqlAliasKind>,
) -> Result<
    (
        GraphPipelineMatchStage,
        LoweringState<'a>,
        BTreeMap<String, GqlAliasKind>,
    ),
    EngineError,
> {
    if clause.patterns.len() != 1 {
        return Err(EngineError::GqlUnsupported {
            feature: "multiple MATCH patterns".to_string(),
            message: "multiple comma-separated MATCH patterns are not supported".to_string(),
            span: clause.span.clone(),
        });
    }
    let pattern = &clause.patterns[0];
    reject_unsupported_pure_edge_label_or(branch_clauses, pattern, params)?;
    if pattern.path_alias.is_some()
        && pattern.edges.len() > 1
        && pattern.edges.iter().all(|edge| edge.quantifier.is_none())
    {
        return Err(EngineError::GqlUnsupported {
            feature: "path assignment in WITH pipelines".to_string(),
            message: "path assignment over multiple fixed relationship patterns in WITH pipelines is deferred".to_string(),
            span: pattern
                .path_span
                .clone()
                .unwrap_or_else(|| pattern.span.clone()),
        });
    }

    let mut state = LoweringState::new_with_alias_kinds(params, input_alias_kinds.clone());
    add_pipeline_pattern_alias_kinds(pattern, &mut state.alias_kinds);
    let mut graph_nodes = Vec::new();
    let mut node_indexes = BTreeMap::new();
    let mut pieces = Vec::new();
    let mut required_where =
        collect_pipeline_graph_nodes(&mut state, pattern, &mut graph_nodes, &mut node_indexes)?;
    pieces.extend(state.lower_pattern_pieces(pattern, true)?);
    if let Some(where_clause) = clause.where_clause.as_ref() {
        required_where.push(where_clause.clone());
    }
    let fixed_edge_indexes = graph_fixed_edge_indexes(&pieces);
    for where_clause in &required_where {
        state.apply_where_to_graph_pattern(
            where_clause,
            &mut graph_nodes,
            &mut pieces,
            &node_indexes,
            &fixed_edge_indexes,
        )?;
    }
    let where_ = combine_pipeline_match_where_with_edge_id_constraints(
        state.graph_residual_expr()?,
        take_edge_id_constraint_residuals(&mut state),
    );

    let mut output_alias_kinds = input_alias_kinds.clone();
    add_pipeline_pattern_alias_kinds(pattern, &mut output_alias_kinds);
    Ok((
        GraphPipelineMatchStage {
            optional: clause.optional,
            nodes: graph_nodes,
            pieces,
            where_,
            optional_candidate_where: None,
        },
        state,
        output_alias_kinds,
    ))
}

fn lower_pipeline_projection_clause(
    projection: &GqlBoundProjectionClause,
    input_alias_kinds: &BTreeMap<String, GqlAliasKind>,
    output_alias_kinds: &BTreeMap<String, GqlAliasKind>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
) -> Result<GraphProjectStage, EngineError> {
    let items = match &projection.returns {
        GqlReturnPlan::Star { .. } => GraphProjectionItems::Star,
        GqlReturnPlan::Items(items) => GraphProjectionItems::Items(
            items
                .iter()
                .map(|item| {
                    Ok(GraphProjectItem {
                        expr: gql_expr_to_graph_expr_for_pipeline(
                            &item.expr,
                            input_alias_kinds,
                            params,
                            options,
                            subquery_depth,
                        )?,
                        alias: Some(item.output_name.clone()),
                        projection: gql_projection_for_expr(&item.expr, input_alias_kinds),
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
        ),
    };
    Ok(GraphProjectStage {
        kind: match projection.kind {
            GqlProjectionKind::With => GraphProjectKind::With,
            GqlProjectionKind::Return => GraphProjectKind::Return,
        },
        items,
        distinct: projection.distinct,
        where_: projection
            .where_clause
            .as_ref()
            .map(|expr| {
                gql_expr_to_graph_expr_for_pipeline(
                    expr,
                    output_alias_kinds,
                    params,
                    options,
                    subquery_depth,
                )
            })
            .transpose()?,
        order_by: projection
            .order_by
            .iter()
            .map(|item| {
                Ok(GraphOrderItem {
                    expr: gql_expr_to_graph_expr_for_pipeline(
                        &item.expr,
                        output_alias_kinds,
                        params,
                        options,
                        subquery_depth,
                    )?,
                    direction: gql_order_direction_to_graph(item.direction),
                })
            })
            .collect::<Result<Vec<_>, EngineError>>()?,
        skip: projection
            .skip
            .as_ref()
            .map(|expr| {
                gql_expr_to_graph_expr_for_pipeline(
                    expr,
                    output_alias_kinds,
                    params,
                    options,
                    subquery_depth,
                )
            })
            .transpose()?,
        limit: projection
            .limit
            .as_ref()
            .map(|expr| {
                gql_expr_to_graph_expr_for_pipeline(
                    expr,
                    output_alias_kinds,
                    params,
                    options,
                    subquery_depth,
                )
            })
            .transpose()?,
    })
}

fn lower_pipeline_shortest_path_clause(
    clause: &GqlBoundShortestPathClause,
) -> Result<GraphShortestPathStage, EngineError> {
    Ok(GraphShortestPathStage {
        optional: clause.optional,
        output_path_alias: clause.output_path_alias.clone(),
        mode: match clause.mode {
            GqlShortestPathMode::One => GraphShortestPathMode::One,
            GqlShortestPathMode::All => GraphShortestPathMode::All,
        },
        from: GraphShortestPathEndpoint::Alias(clause.from_alias.clone()),
        to: GraphShortestPathEndpoint::Alias(clause.to_alias.clone()),
        direction: native_direction(clause.direction),
        edge_label_filter: clause
            .rel_types
            .iter()
            .map(|label| label.name.clone())
            .collect(),
        min_hops: clause.min_hops,
        max_hops: clause.max_hops,
        weight_field: None,
        max_cost: None,
        max_paths: None,
    })
}

fn lower_pipeline_call_subquery(
    call: &GqlBoundCallSubquery,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
    input_alias_kinds: &BTreeMap<String, GqlAliasKind>,
) -> Result<GraphSubqueryStage, EngineError> {
    let next_depth = subquery_depth.saturating_add(1);
    if next_depth > options.max_subquery_depth {
        return Err(EngineError::InvalidOperation(format!(
            "GQL subquery depth {next_depth} exceeds max_subquery_depth {}",
            options.max_subquery_depth
        )));
    }
    let import_alias_kinds = call
        .import_aliases
        .iter()
        .filter_map(|alias| {
            input_alias_kinds
                .get(alias)
                .copied()
                .map(|kind| (alias.clone(), kind))
        })
        .collect::<BTreeMap<_, _>>();
    let lowered = lower_bound_read_pipeline_with_alias_kinds(
        &call.pipeline,
        params,
        options,
        next_depth,
        import_alias_kinds,
    )?;
    Ok(GraphSubqueryStage {
        query: Box::new(GraphPipelineQuery {
            stages: lowered.stages,
            params: gql_params_to_graph_params(params),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: options.max_rows.max(1),
                cursor: None,
            },
            output: GraphOutputOptions {
                mode: GraphOutputMode::Ids,
                compact_rows: false,
                include_vectors: false,
            },
            options: gql_pipeline_options(options),
        }),
        import_aliases: call.import_aliases.clone(),
    })
}

fn collect_pipeline_graph_nodes(
    state: &mut LoweringState<'_>,
    pattern: &GqlBoundPattern,
    nodes: &mut Vec<GraphNodePattern>,
    node_indexes: &mut BTreeMap<String, usize>,
) -> Result<Vec<Expr>, EngineError> {
    let mut reused_constraints = Vec::new();
    for node in &pattern.nodes {
        if node_indexes.contains_key(&node.alias) {
            reused_constraints.extend(reused_node_constraint_exprs(node)?);
        } else {
            let index = nodes.len();
            node_indexes.insert(node.alias.clone(), index);
            nodes.push(state.lower_graph_node_pattern(node)?);
        }
    }
    Ok(reused_constraints)
}

fn projection_alias_kinds(projection: &GqlBoundProjectionClause) -> BTreeMap<String, GqlAliasKind> {
    projection
        .output_aliases
        .iter()
        .map(|alias| (alias.name.clone(), alias.kind))
        .collect()
}

fn add_pipeline_pattern_alias_kinds(
    pattern: &GqlBoundPattern,
    alias_kinds: &mut BTreeMap<String, GqlAliasKind>,
) {
    for node in &pattern.nodes {
        alias_kinds.insert(node.alias.clone(), GqlAliasKind::Node);
    }
    for edge in &pattern.edges {
        if let Some(alias) = edge.alias.as_ref() {
            alias_kinds.insert(alias.clone(), GqlAliasKind::Edge);
        }
    }
    if let Some(alias) = pattern.path_alias.as_ref() {
        alias_kinds.insert(alias.clone(), GqlAliasKind::Path);
    }
}

fn take_edge_id_constraint_residuals(state: &mut LoweringState<'_>) -> Vec<GraphExpr> {
    let constraints = std::mem::take(&mut state.edge_id_constraints);
    constraints
        .into_iter()
        .map(|(alias, ids)| edge_id_constraint_graph_expr(&alias, &ids))
        .collect()
}

fn combine_pipeline_match_where_with_edge_id_constraints(
    mut where_: Option<GraphExpr>,
    constraints: Vec<GraphExpr>,
) -> Option<GraphExpr> {
    for constraint in constraints {
        where_ = Some(match where_ {
            Some(existing) => GraphExpr::Binary {
                left: Box::new(existing),
                op: GraphBinaryOp::And,
                right: Box::new(constraint),
            },
            None => constraint,
        });
    }
    where_
}

fn edge_id_constraint_graph_expr(alias: &str, ids: &[u64]) -> GraphExpr {
    let id_expr = GraphExpr::Function {
        name: GraphFunction::Id,
        args: vec![GraphExpr::Binding(alias.to_string())],
    };
    if let [id] = ids {
        return GraphExpr::Binary {
            left: Box::new(id_expr),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::UInt(*id)),
        };
    }
    GraphExpr::Binary {
        left: Box::new(id_expr),
        op: GraphBinaryOp::In,
        right: Box::new(GraphExpr::List(
            ids.iter().copied().map(GraphExpr::UInt).collect(),
        )),
    }
}

fn gql_projection_for_expr(
    expr: &Expr,
    alias_kinds: &BTreeMap<String, GqlAliasKind>,
) -> GraphReturnProjection {
    match &expr.kind {
        ExprKind::Variable(alias)
            if matches!(
                alias_kinds.get(alias),
                Some(GqlAliasKind::Node | GqlAliasKind::Edge | GqlAliasKind::Path)
            ) =>
        {
            GraphReturnProjection::Element(GraphElementProjection::Full)
        }
        _ => GraphReturnProjection::Auto,
    }
}

fn gql_pipeline_options(options: &GqlExecutionOptions) -> GraphPipelineOptions {
    GraphPipelineOptions {
        allow_full_scan: options.allow_full_scan,
        max_rows: options.max_rows,
        max_pipeline_rows: options.max_pipeline_rows,
        max_groups: options.max_groups,
        max_collect_items: options.max_collect_items,
        max_union_branches: options.max_union_branches,
        max_subquery_invocations: options.max_subquery_invocations,
        max_subquery_depth: options.max_subquery_depth,
        max_shortest_path_pairs: options.max_shortest_path_pairs,
        max_intermediate_bindings: options.max_intermediate_bindings,
        max_frontier: options.max_frontier,
        max_path_hops: options.max_path_hops,
        max_paths_per_start: options.max_paths_per_start,
        max_order_materialization: options.max_order_materialization,
        max_skip: options.max_skip,
        max_cursor_bytes: options.max_cursor_bytes,
        max_query_bytes: options.max_query_bytes,
        max_param_bytes: options.max_param_bytes,
        max_ast_depth: options.max_ast_depth,
        max_literal_items: options.max_literal_items,
        include_plan: options.include_plan,
        profile: options.profile,
    }
}

pub(crate) fn lower_mutation_semantic_plan(
    semantic: GqlMutationSemanticPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlMutationPlan, EngineError> {
    let operation_exprs = mutation_operation_exprs(&semantic);
    let alias_kinds = semantic
        .aliases
        .iter()
        .map(|(alias, binding)| (alias.clone(), binding.kind))
        .collect::<BTreeMap<_, _>>();
    let operation_expr_plans = operation_exprs
        .iter()
        .enumerate()
        .map(|(id, operation_expr)| {
            Ok(GqlMutationExprPlan {
                id,
                expr: gql_expr_to_graph_expr(&operation_expr.expr, &alias_kinds)?,
                source: operation_expr.expr.clone(),
                late: operation_expr.late,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let mut expr_cursor = 0;
    let clauses = semantic
        .clauses
        .iter()
        .map(|clause| lower_mutation_clause(clause, &mut expr_cursor))
        .collect::<Vec<_>>();
    debug_assert_eq!(expr_cursor, operation_expr_plans.len());
    let internal_columns =
        mutation_internal_columns(&semantic, &operation_exprs, &operation_expr_plans);

    let read_prefix = lower_mutation_read_prefix(
        &semantic,
        &operation_exprs,
        internal_columns,
        params,
        options,
    )?;
    let return_plan = semantic
        .statement
        .return_tail
        .as_ref()
        .map(|tail| GqlMutationReturnPlan {
            columns: mutation_return_columns(&semantic),
            distinct: tail.return_clause.distinct,
            order_items: tail.order_by.len(),
            skip: tail.skip.clone(),
            limit: tail.limit.clone(),
        });
    let mut warnings = Vec::new();
    if let Some(read_prefix) = read_prefix.as_ref() {
        warnings.extend(read_prefix.lowered.warnings.iter().cloned());
    }
    warnings.sort();
    warnings.dedup();
    Ok(GqlMutationPlan {
        params_used: semantic.parameters.clone(),
        semantic,
        read_prefix,
        clauses,
        return_plan,
        operation_exprs: operation_expr_plans,
        warnings,
    })
}

fn lower_mutation_read_prefix(
    semantic: &GqlMutationSemanticPlan,
    operation_exprs: &[GqlMutationOperationExpr],
    internal_columns: Vec<GqlMutationInternalColumn>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<Option<GqlMutationReadPrefixPlan>, EngineError> {
    if !mutation_statement_has_read_prefix(&semantic.statement) {
        return Ok(None);
    }
    let read_query = mutation_read_prefix_query(semantic, operation_exprs, &internal_columns);
    let read_semantic = bind_query(read_query, params)?;
    validate_referenced_gql_params(&read_semantic, params, options)?;
    let mut read_options = options.clone();
    read_options.cursor = None;
    read_options.max_rows = options.max_mutation_rows.saturating_add(1).max(1);
    let lowered = lower_semantic_plan(read_semantic, params, &read_options)?;
    let graph_row = match &lowered.native_target {
        GqlNativeTarget::GraphRows { query } => Some(query.clone()),
        GqlNativeTarget::GraphPipeline { .. } => None,
    };
    Ok(Some(GqlMutationReadPrefixPlan {
        graph_row,
        lowered: Box::new(lowered),
        internal_columns,
    }))
}

fn mutation_read_prefix_query(
    semantic: &GqlMutationSemanticPlan,
    operation_exprs: &[GqlMutationOperationExpr],
    internal_columns: &[GqlMutationInternalColumn],
) -> GqlQuery {
    let mut items = Vec::new();
    for column in internal_columns {
        match column {
            GqlMutationInternalColumn::TargetId { alias, .. } => {
                let Some(binding) = semantic.aliases.get(alias) else {
                    continue;
                };
                items.push(internal_return_item(
                    id_function_expr(alias, &binding.span),
                    format!("_gql_mut_id_{alias}"),
                    &binding.span,
                ));
            }
            GqlMutationInternalColumn::TargetPath { alias } => {
                let Some(binding) = semantic.aliases.get(alias) else {
                    continue;
                };
                items.push(internal_return_item(
                    path_function_expr("nodeIds", alias, &binding.span),
                    format!("_gql_mut_path_nodes_{alias}"),
                    &binding.span,
                ));
                items.push(internal_return_item(
                    path_function_expr("edgeIds", alias, &binding.span),
                    format!("_gql_mut_path_edges_{alias}"),
                    &binding.span,
                ));
            }
            GqlMutationInternalColumn::ScalarValue { alias, .. } => {
                let Some(binding) = semantic.aliases.get(alias) else {
                    continue;
                };
                items.push(internal_return_item(
                    Expr {
                        kind: ExprKind::Variable(alias.clone()),
                        span: binding.span.clone(),
                    },
                    format!("_gql_mut_scalar_{alias}"),
                    &binding.span,
                ));
            }
            GqlMutationInternalColumn::ExprValue { id, .. } => {
                let expr = &operation_exprs[*id].expr;
                items.push(internal_return_item(
                    expr.clone(),
                    format!("_gql_mut_expr_{id}"),
                    &expr.span,
                ));
            }
        };
    }
    if items.is_empty() {
        items.push(internal_return_item(
            Expr {
                kind: ExprKind::Literal(Literal::Int(1)),
                span: semantic.statement.span.clone(),
            },
            "_gql_mut_row".to_string(),
            &semantic.statement.span,
        ));
    }
    let return_clause = ReturnClause {
        body: ReturnBody::Items(items.clone()),
        distinct: false,
        distinct_span: None,
        span: semantic.statement.span.clone(),
    };
    let return_projection = GqlProjectionClause {
        kind: GqlProjectionKind::Return,
        distinct: false,
        distinct_span: None,
        body: ReturnBody::Items(items),
        where_clause: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        span: semantic.statement.span.clone(),
    };
    let pipeline = if let Some(prefix) = semantic.statement.read_prefix_pipeline.as_ref() {
        let mut pipeline = prefix.clone();
        pipeline
            .clauses
            .push(GqlPipelineClause::Projection(return_projection));
        pipeline.span = semantic.statement.span.clone();
        pipeline
    } else {
        GqlReadPipeline {
            clauses: vec![
                GqlPipelineClause::Match(semantic.statement.read_prefix.clone()),
                GqlPipelineClause::Projection(return_projection),
            ],
            union_branches: Vec::new(),
            span: semantic.statement.span.clone(),
        }
    };
    GqlQuery {
        match_clauses: semantic.statement.read_prefix.clone(),
        return_clause,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        pipeline,
        span: semantic.statement.span.clone(),
    }
}

fn mutation_statement_has_read_prefix(statement: &GqlMutationStatement) -> bool {
    statement.read_prefix_pipeline.is_some() || !statement.read_prefix.is_empty()
}

fn internal_return_item(expr: Expr, alias: String, span: &SourceSpan) -> ReturnItem {
    ReturnItem {
        span: expr.span.clone(),
        expr,
        alias: Some(Ident {
            name: alias,
            span: span.clone(),
        }),
    }
}

fn id_function_expr(alias: &str, span: &SourceSpan) -> Expr {
    Expr {
        kind: ExprKind::FunctionCall {
            name: Ident {
                name: "id".to_string(),
                span: span.clone(),
            },
            args: vec![Expr {
                kind: ExprKind::Variable(alias.to_string()),
                span: span.clone(),
            }],
        },
        span: span.clone(),
    }
}

fn path_function_expr(function: &str, alias: &str, span: &SourceSpan) -> Expr {
    Expr {
        kind: ExprKind::FunctionCall {
            name: Ident {
                name: function.to_string(),
                span: span.clone(),
            },
            args: vec![Expr {
                kind: ExprKind::Variable(alias.to_string()),
                span: span.clone(),
            }],
        },
        span: span.clone(),
    }
}

fn mutation_internal_columns(
    semantic: &GqlMutationSemanticPlan,
    operation_exprs: &[GqlMutationOperationExpr],
    lowered_exprs: &[GqlMutationExprPlan],
) -> Vec<GqlMutationInternalColumn> {
    let mut required_aliases = BTreeSet::new();
    collect_mutation_target_aliases(semantic, &mut required_aliases);
    collect_return_identity_aliases(semantic, &mut required_aliases);
    collect_late_expr_scalar_aliases(semantic, operation_exprs, &mut required_aliases);

    let mut columns = Vec::new();
    for alias in semantic.user_order.iter() {
        if !required_aliases.contains(alias) {
            continue;
        }
        let Some(binding) = semantic.aliases.get(alias) else {
            continue;
        };
        if binding.origin != GqlAliasOrigin::ReadPrefix {
            continue;
        }
        match binding.kind {
            GqlAliasKind::Node | GqlAliasKind::Edge => {
                columns.push(GqlMutationInternalColumn::TargetId {
                    alias: alias.clone(),
                    kind: binding.kind,
                });
            }
            GqlAliasKind::Path => {
                columns.push(GqlMutationInternalColumn::TargetPath {
                    alias: alias.clone(),
                });
            }
            GqlAliasKind::Scalar => {
                columns.push(GqlMutationInternalColumn::ScalarValue {
                    alias: alias.clone(),
                    expr: GraphExpr::Binding(alias.clone()),
                });
            }
        }
    }

    for (id, expr) in operation_exprs.iter().enumerate() {
        if !expr.late && expr_references_read_prefix_alias(&expr.expr, semantic) {
            columns.push(GqlMutationInternalColumn::ExprValue {
                id,
                expr: lowered_exprs[id].expr.clone(),
            });
        }
    }

    columns
}

fn collect_mutation_target_aliases(
    semantic: &GqlMutationSemanticPlan,
    required_aliases: &mut BTreeSet<String>,
) {
    for clause in &semantic.clauses {
        match clause {
            GqlBoundMutationClause::Create(create) => {
                for pattern in &create.patterns {
                    for node in &pattern.nodes {
                        if !node.created {
                            maybe_insert_read_prefix_alias(semantic, &node.alias, required_aliases);
                        }
                    }
                }
            }
            GqlBoundMutationClause::Merge(merge) => {
                match &merge.pattern {
                    GqlBoundMergePattern::Node(node) => {
                        collect_read_prefix_aliases_from_expr(
                            semantic,
                            &node.key,
                            required_aliases,
                        );
                    }
                    GqlBoundMergePattern::Relationship(rel) => {
                        maybe_insert_read_prefix_alias(semantic, &rel.from_alias, required_aliases);
                        maybe_insert_read_prefix_alias(semantic, &rel.to_alias, required_aliases);
                    }
                }
                collect_set_target_aliases(semantic, &merge.on_create.items, required_aliases);
                collect_set_target_aliases(semantic, &merge.on_match.items, required_aliases);
            }
            GqlBoundMutationClause::Set(set) => {
                collect_set_target_aliases(semantic, &set.items, required_aliases);
            }
            GqlBoundMutationClause::Remove(remove) => {
                for item in &remove.items {
                    match item {
                        GqlBoundRemoveItem::Property { alias, .. }
                        | GqlBoundRemoveItem::NodeLabel { alias, .. } => {
                            maybe_insert_read_prefix_alias(semantic, alias, required_aliases);
                        }
                    }
                }
            }
            GqlBoundMutationClause::Delete(delete) => {
                for target in &delete.targets {
                    maybe_insert_read_prefix_alias(semantic, &target.alias, required_aliases);
                }
            }
        }
    }
}

fn collect_late_expr_scalar_aliases(
    semantic: &GqlMutationSemanticPlan,
    operation_exprs: &[GqlMutationOperationExpr],
    required_aliases: &mut BTreeSet<String>,
) {
    for expr in operation_exprs.iter().filter(|expr| expr.late) {
        collect_read_prefix_scalar_aliases_in_expr(semantic, &expr.expr, required_aliases);
    }
}

fn collect_read_prefix_scalar_aliases_in_expr(
    semantic: &GqlMutationSemanticPlan,
    expr: &Expr,
    aliases: &mut BTreeSet<String>,
) {
    match &expr.kind {
        ExprKind::Variable(name) => {
            if semantic.aliases.get(name).is_some_and(|binding| {
                binding.origin == GqlAliasOrigin::ReadPrefix && binding.kind == GqlAliasKind::Scalar
            }) {
                aliases.insert(name.clone());
            }
        }
        ExprKind::PropertyAccess { object, .. }
        | ExprKind::Unary { expr: object, .. }
        | ExprKind::IsNull { expr: object, .. } => {
            collect_read_prefix_scalar_aliases_in_expr(semantic, object, aliases);
        }
        ExprKind::Binary { left, right, .. } => {
            collect_read_prefix_scalar_aliases_in_expr(semantic, left, aliases);
            collect_read_prefix_scalar_aliases_in_expr(semantic, right, aliases);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                collect_read_prefix_scalar_aliases_in_expr(semantic, arg, aliases);
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_read_prefix_scalar_aliases_in_expr(semantic, arg, aliases);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_read_prefix_scalar_aliases_in_expr(semantic, operand, aliases);
            }
            for branch in branches {
                collect_read_prefix_scalar_aliases_in_expr(semantic, &branch.when, aliases);
                collect_read_prefix_scalar_aliases_in_expr(semantic, &branch.then, aliases);
            }
            if let Some(else_expr) = else_expr.as_ref() {
                collect_read_prefix_scalar_aliases_in_expr(semantic, else_expr, aliases);
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_read_prefix_scalar_aliases_in_expr(semantic, &entry.value, aliases);
            }
        }
        ExprKind::ExistsSubquery(_) | ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
}

fn collect_set_target_aliases(
    semantic: &GqlMutationSemanticPlan,
    items: &[GqlBoundSetItem],
    required_aliases: &mut BTreeSet<String>,
) {
    for item in items {
        match item {
            GqlBoundSetItem::Property { alias, value, .. }
            | GqlBoundSetItem::Metadata { alias, value, .. }
            | GqlBoundSetItem::MapMerge { alias, value, .. } => {
                maybe_insert_read_prefix_alias(semantic, alias, required_aliases);
                collect_read_prefix_aliases_from_expr(semantic, value, required_aliases);
            }
            GqlBoundSetItem::NodeLabel { alias, .. } => {
                maybe_insert_read_prefix_alias(semantic, alias, required_aliases);
            }
        }
    }
}

fn collect_return_identity_aliases(
    semantic: &GqlMutationSemanticPlan,
    required_aliases: &mut BTreeSet<String>,
) {
    if let Some(returns) = semantic.returns.as_ref() {
        match returns {
            GqlReturnPlan::Star {
                expanded_aliases, ..
            } => {
                for alias in expanded_aliases {
                    maybe_insert_read_prefix_alias(semantic, alias, required_aliases);
                }
            }
            GqlReturnPlan::Items(items) => {
                for item in items {
                    collect_read_prefix_aliases_from_expr(semantic, &item.expr, required_aliases);
                }
            }
        }
    }
    if let Some(tail) = semantic.statement.return_tail.as_ref() {
        for item in &tail.order_by {
            collect_read_prefix_aliases_from_expr(semantic, &item.expr, required_aliases);
        }
        if let Some(skip) = tail.skip.as_ref() {
            collect_read_prefix_aliases_from_expr(semantic, skip, required_aliases);
        }
        if let Some(limit) = tail.limit.as_ref() {
            collect_read_prefix_aliases_from_expr(semantic, limit, required_aliases);
        }
    }
}

fn collect_read_prefix_aliases_from_expr(
    semantic: &GqlMutationSemanticPlan,
    expr: &Expr,
    aliases: &mut BTreeSet<String>,
) {
    match &expr.kind {
        ExprKind::Variable(alias) => {
            maybe_insert_read_prefix_alias(semantic, alias, aliases);
        }
        ExprKind::PropertyAccess { object, .. } => {
            collect_read_prefix_aliases_from_expr(semantic, object, aliases);
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            collect_read_prefix_aliases_from_expr(semantic, expr, aliases);
        }
        ExprKind::Binary { left, right, .. } => {
            collect_read_prefix_aliases_from_expr(semantic, left, aliases);
            collect_read_prefix_aliases_from_expr(semantic, right, aliases);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                collect_read_prefix_aliases_from_expr(semantic, arg, aliases);
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_read_prefix_aliases_from_expr(semantic, arg, aliases);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_read_prefix_aliases_from_expr(semantic, operand, aliases);
            }
            for branch in branches {
                collect_read_prefix_aliases_from_expr(semantic, &branch.when, aliases);
                collect_read_prefix_aliases_from_expr(semantic, &branch.then, aliases);
            }
            if let Some(else_expr) = else_expr.as_ref() {
                collect_read_prefix_aliases_from_expr(semantic, else_expr, aliases);
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_read_prefix_aliases_from_expr(semantic, &entry.value, aliases);
            }
        }
        ExprKind::ExistsSubquery(_) => {}
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
}

fn maybe_insert_read_prefix_alias(
    semantic: &GqlMutationSemanticPlan,
    alias: &str,
    aliases: &mut BTreeSet<String>,
) {
    if semantic
        .aliases
        .get(alias)
        .is_some_and(|binding| binding.origin == GqlAliasOrigin::ReadPrefix)
    {
        aliases.insert(alias.to_string());
    }
}

fn expr_references_read_prefix_alias(expr: &Expr, semantic: &GqlMutationSemanticPlan) -> bool {
    let mut aliases = BTreeSet::new();
    collect_read_prefix_aliases_from_expr(semantic, expr, &mut aliases);
    !aliases.is_empty()
}

fn mutation_operation_exprs(semantic: &GqlMutationSemanticPlan) -> Vec<GqlMutationOperationExpr> {
    let mut exprs = Vec::new();
    for clause in &semantic.clauses {
        match clause {
            GqlBoundMutationClause::Create(create) => {
                for pattern in &create.patterns {
                    for node in &pattern.nodes {
                        collect_map_value_exprs(node.properties.as_ref(), &mut exprs, false);
                    }
                    for edge in &pattern.edges {
                        collect_map_value_exprs(edge.properties.as_ref(), &mut exprs, false);
                    }
                }
            }
            GqlBoundMutationClause::Merge(merge) => {
                if let GqlBoundMergePattern::Node(node) = &merge.pattern {
                    exprs.push(GqlMutationOperationExpr {
                        expr: node.key.clone(),
                        late: false,
                    });
                }
                collect_set_value_exprs(&merge.on_create.items, &mut exprs, semantic, true);
                collect_set_value_exprs(&merge.on_match.items, &mut exprs, semantic, true);
            }
            GqlBoundMutationClause::Set(set) => {
                collect_set_value_exprs(&set.items, &mut exprs, semantic, false);
            }
            GqlBoundMutationClause::Remove(_) | GqlBoundMutationClause::Delete(_) => {}
        }
    }
    exprs
}

fn collect_set_value_exprs(
    items: &[GqlBoundSetItem],
    exprs: &mut Vec<GqlMutationOperationExpr>,
    semantic: &GqlMutationSemanticPlan,
    allow_late: bool,
) {
    for item in items {
        match item {
            GqlBoundSetItem::Property { value, .. }
            | GqlBoundSetItem::Metadata { value, .. }
            | GqlBoundSetItem::MapMerge { value, .. } => {
                exprs.push(GqlMutationOperationExpr {
                    expr: value.clone(),
                    late: allow_late && expr_references_created_or_merged_alias(value, semantic),
                })
            }
            GqlBoundSetItem::NodeLabel { .. } => {}
        }
    }
}

fn collect_map_value_exprs(
    map: Option<&MapLiteral>,
    exprs: &mut Vec<GqlMutationOperationExpr>,
    late: bool,
) {
    if let Some(map) = map {
        exprs.extend(map.entries.iter().map(|entry| GqlMutationOperationExpr {
            expr: entry.value.clone(),
            late,
        }));
    }
}

fn expr_references_created_or_merged_alias(
    expr: &Expr,
    semantic: &GqlMutationSemanticPlan,
) -> bool {
    match &expr.kind {
        ExprKind::Variable(name) => semantic.aliases.get(name).is_some_and(|binding| {
            matches!(
                binding.origin,
                GqlAliasOrigin::Created | GqlAliasOrigin::Merged
            )
        }),
        ExprKind::PropertyAccess { object, .. }
        | ExprKind::Unary { expr: object, .. }
        | ExprKind::IsNull { expr: object, .. } => {
            expr_references_created_or_merged_alias(object, semantic)
        }
        ExprKind::Binary { left, right, .. } => {
            expr_references_created_or_merged_alias(left, semantic)
                || expr_references_created_or_merged_alias(right, semantic)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => args
            .iter()
            .any(|arg| expr_references_created_or_merged_alias(arg, semantic)),
        ExprKind::AggregateCall { arg, .. } => arg
            .as_ref()
            .is_some_and(|arg| expr_references_created_or_merged_alias(arg, semantic)),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_references_created_or_merged_alias(expr, semantic))
                || branches.iter().any(|branch| {
                    expr_references_created_or_merged_alias(&branch.when, semantic)
                        || expr_references_created_or_merged_alias(&branch.then, semantic)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_references_created_or_merged_alias(expr, semantic))
        }
        ExprKind::Map(map) => map
            .entries
            .iter()
            .any(|entry| expr_references_created_or_merged_alias(&entry.value, semantic)),
        ExprKind::ExistsSubquery(_) | ExprKind::Literal(_) | ExprKind::Parameter(_) => false,
    }
}

fn lower_mutation_clause(
    clause: &GqlBoundMutationClause,
    expr_cursor: &mut usize,
) -> GqlMutationClausePlan {
    match clause {
        GqlBoundMutationClause::Create(create) => GqlMutationClausePlan::Create(
            create
                .patterns
                .iter()
                .map(|pattern| GqlCreatePatternPlan {
                    nodes: pattern
                        .nodes
                        .iter()
                        .map(|node| lower_create_node(node, expr_cursor))
                        .collect(),
                    edges: pattern
                        .edges
                        .iter()
                        .map(|edge| lower_create_edge(edge, expr_cursor))
                        .collect(),
                })
                .collect(),
        ),
        GqlBoundMutationClause::Merge(merge) => {
            GqlMutationClausePlan::Merge(lower_merge_clause(merge, expr_cursor))
        }
        GqlBoundMutationClause::Set(set) => GqlMutationClausePlan::Set(
            set.items
                .iter()
                .map(|item| lower_set_item(item, expr_cursor))
                .collect(),
        ),
        GqlBoundMutationClause::Remove(remove) => {
            GqlMutationClausePlan::Remove(remove.items.iter().map(lower_remove_item).collect())
        }
        GqlBoundMutationClause::Delete(delete) => GqlMutationClausePlan::Delete {
            detach: delete.detach,
            targets: delete
                .targets
                .iter()
                .map(|target| GqlDeleteTargetPlan {
                    alias: target.alias.clone(),
                    kind: target.kind,
                })
                .collect(),
        },
    }
}

fn lower_merge_clause(merge: &GqlBoundMergeClause, expr_cursor: &mut usize) -> GqlMergePlan {
    let pattern = match &merge.pattern {
        GqlBoundMergePattern::Node(node) => GqlMergePatternPlan::Node {
            alias: node.alias.clone(),
            label: node.label.name.clone(),
            key: next_expr_ref(expr_cursor),
        },
        GqlBoundMergePattern::Relationship(rel) => GqlMergePatternPlan::Relationship {
            alias: rel.alias.clone(),
            from_alias: rel.from_alias.clone(),
            to_alias: rel.to_alias.clone(),
            label: rel.rel_type.name.clone(),
        },
    };
    GqlMergePlan {
        pattern,
        on_create: merge
            .on_create
            .items
            .iter()
            .map(|item| lower_set_item(item, expr_cursor))
            .collect(),
        on_match: merge
            .on_match
            .items
            .iter()
            .map(|item| lower_set_item(item, expr_cursor))
            .collect(),
    }
}

/// Splits a CREATE/MERGE element map into metadata expr refs and user-property entries.
/// Expr refs MUST be assigned in map-entry order to stay aligned with
/// `collect_map_value_exprs`, regardless of whether an entry is metadata or a property.
struct LoweredElementMap {
    element_key: Option<GqlMutationExprRef>,
    weight: Option<GqlMutationExprRef>,
    valid_from: Option<GqlMutationExprRef>,
    valid_to: Option<GqlMutationExprRef>,
    property_keys: Vec<String>,
    property_values: BTreeMap<String, GqlMutationExprRef>,
}

fn lower_element_map(map: Option<&MapLiteral>, expr_cursor: &mut usize) -> LoweredElementMap {
    let mut lowered = LoweredElementMap {
        element_key: None,
        weight: None,
        valid_from: None,
        valid_to: None,
        property_keys: Vec::new(),
        property_values: BTreeMap::new(),
    };
    let Some(map) = map else {
        return lowered;
    };
    for entry in &map.entries {
        let expr_ref = next_expr_ref(expr_cursor);
        match GqlElementMapMetadataKey::from_key(&entry.key.name) {
            Some(GqlElementMapMetadataKey::ElementKey) => lowered.element_key = Some(expr_ref),
            Some(GqlElementMapMetadataKey::Weight) => lowered.weight = Some(expr_ref),
            Some(GqlElementMapMetadataKey::ValidFrom) => lowered.valid_from = Some(expr_ref),
            Some(GqlElementMapMetadataKey::ValidTo) => lowered.valid_to = Some(expr_ref),
            None => {
                lowered.property_keys.push(entry.key.name.clone());
                lowered.property_values.insert(entry.key.name.clone(), expr_ref);
            }
        }
    }
    lowered
}

fn lower_create_node(node: &GqlBoundCreateNode, expr_cursor: &mut usize) -> GqlCreateNodePlan {
    let map = lower_element_map(node.properties.as_ref(), expr_cursor);
    GqlCreateNodePlan {
        alias: node.alias.clone(),
        labels: node.labels.iter().map(|label| label.name.clone()).collect(),
        element_key: map.element_key,
        weight: map.weight,
        property_keys: map.property_keys,
        property_values: map.property_values,
        created: node.created,
    }
}

fn lower_create_edge(edge: &GqlBoundCreateEdge, expr_cursor: &mut usize) -> GqlCreateEdgePlan {
    let map = lower_element_map(edge.properties.as_ref(), expr_cursor);
    GqlCreateEdgePlan {
        alias: edge.alias.clone(),
        from_alias: edge.from_alias.clone(),
        to_alias: edge.to_alias.clone(),
        label: edge.rel_type.name.clone(),
        weight: map.weight,
        valid_from: map.valid_from,
        valid_to: map.valid_to,
        property_keys: map.property_keys,
        property_values: map.property_values,
    }
}

fn lower_set_item(item: &GqlBoundSetItem, expr_cursor: &mut usize) -> GqlSetItemPlan {
    match item {
        GqlBoundSetItem::Property {
            alias,
            target_kind,
            property,
            ..
        } => GqlSetItemPlan::Property {
            alias: alias.clone(),
            kind: *target_kind,
            property: property.name.clone(),
            value: next_expr_ref(expr_cursor),
        },
        GqlBoundSetItem::Metadata {
            alias,
            target_kind,
            field,
            ..
        } => GqlSetItemPlan::Metadata {
            alias: alias.clone(),
            kind: *target_kind,
            field: *field,
            value: next_expr_ref(expr_cursor),
        },
        GqlBoundSetItem::MapMerge {
            alias, target_kind, ..
        } => GqlSetItemPlan::MapMerge {
            alias: alias.clone(),
            kind: *target_kind,
            value: next_expr_ref(expr_cursor),
        },
        GqlBoundSetItem::NodeLabel { alias, label, .. } => GqlSetItemPlan::NodeLabel {
            alias: alias.clone(),
            label: label.name.clone(),
        },
    }
}

fn lower_remove_item(item: &GqlBoundRemoveItem) -> GqlRemoveItemPlan {
    match item {
        GqlBoundRemoveItem::Property {
            alias,
            target_kind,
            property,
            ..
        } => GqlRemoveItemPlan::Property {
            alias: alias.clone(),
            kind: *target_kind,
            property: property.name.clone(),
        },
        GqlBoundRemoveItem::NodeLabel { alias, label, .. } => GqlRemoveItemPlan::NodeLabel {
            alias: alias.clone(),
            label: label.name.clone(),
        },
    }
}

fn next_expr_ref(expr_cursor: &mut usize) -> GqlMutationExprRef {
    let id = *expr_cursor;
    *expr_cursor += 1;
    GqlMutationExprRef { id }
}

fn mutation_return_columns(semantic: &GqlMutationSemanticPlan) -> Vec<String> {
    match semantic.returns.as_ref() {
        None => Vec::new(),
        Some(GqlReturnPlan::Star {
            expanded_aliases, ..
        }) => expanded_aliases.clone(),
        Some(GqlReturnPlan::Items(items)) => items
            .iter()
            .map(|item| {
                item.explicit_alias
                    .clone()
                    .unwrap_or_else(|| expression_output_name(&item.expr))
            })
            .collect(),
    }
}

struct LoweringState<'a> {
    params: &'a GqlParams,
    alias_kinds: BTreeMap<String, GqlAliasKind>,
    edge_id_constraints: BTreeMap<String, Vec<u64>>,
    residual_predicates: Vec<Expr>,
    pushed_down: Vec<GqlPushedPredicate>,
    warnings: Vec<String>,
    notes: Vec<String>,
}

impl<'a> LoweringState<'a> {
    fn new(params: &'a GqlParams, semantic: &GqlSemanticPlan) -> Self {
        Self {
            params,
            alias_kinds: semantic
                .aliases
                .by_name
                .iter()
                .map(|(alias, binding)| (alias.clone(), binding.kind))
                .collect(),
            edge_id_constraints: BTreeMap::new(),
            residual_predicates: Vec::new(),
            pushed_down: Vec::new(),
            warnings: Vec::new(),
            notes: Vec::new(),
        }
    }

    fn new_with_alias_kinds(
        params: &'a GqlParams,
        alias_kinds: BTreeMap<String, GqlAliasKind>,
    ) -> Self {
        Self {
            params,
            alias_kinds,
            edge_id_constraints: BTreeMap::new(),
            residual_predicates: Vec::new(),
            pushed_down: Vec::new(),
            warnings: Vec::new(),
            notes: Vec::new(),
        }
    }

    fn collect_graph_nodes(
        &mut self,
        pattern: &GqlBoundPattern,
        nodes: &mut Vec<GraphNodePattern>,
        node_indexes: &mut BTreeMap<String, usize>,
    ) -> Result<Vec<Expr>, EngineError> {
        let mut reused_constraints = Vec::new();
        for node in &pattern.nodes {
            if node_indexes.contains_key(&node.alias) {
                reused_constraints.extend(reused_node_constraint_exprs(node)?);
            } else {
                let index = nodes.len();
                node_indexes.insert(node.alias.clone(), index);
                nodes.push(self.lower_graph_node_pattern(node)?);
            }
        }
        Ok(reused_constraints)
    }

    fn lower_pattern_pieces(
        &mut self,
        pattern: &GqlBoundPattern,
        materialize_node_only: bool,
    ) -> Result<Vec<GraphPatternPiece>, EngineError> {
        if pattern.edges.is_empty() {
            if pattern.path_alias.is_some() || materialize_node_only {
                let start = pattern
                    .nodes
                    .first()
                    .ok_or_else(|| {
                        gql_semantic_error(
                            GqlSemanticErrorCode::InvalidReturnExpression,
                            "node-only pattern requires a node pattern".to_string(),
                            pattern.span.clone(),
                        )
                    })?
                    .alias
                    .clone();
                return Ok(vec![GraphPatternPiece::VariableLength(
                    GraphVariableLengthPattern {
                        path_alias: pattern.path_alias.clone(),
                        edge_alias: None,
                        from_alias: start.clone(),
                        to_alias: start,
                        direction: Direction::Outgoing,
                        label_filter: Vec::new(),
                        filter: None,
                        min_hops: 0,
                        max_hops: 0,
                    },
                )]);
            }
            return Ok(Vec::new());
        }

        let mut pieces = Vec::with_capacity(pattern.edges.len());
        let fixed_multi_hop_path = pattern.path_alias.is_some()
            && pattern.edges.len() > 1
            && pattern.edges.iter().all(|edge| edge.quantifier.is_none());
        for edge in &pattern.edges {
            let use_path_substrate = edge.quantifier.is_some()
                || (pattern.path_alias.is_some() && !fixed_multi_hop_path);
            if use_path_substrate {
                pieces.push(GraphPatternPiece::VariableLength(
                    self.lower_graph_variable_length_pattern(edge, pattern.path_alias.as_deref())?,
                ));
            } else {
                pieces.push(GraphPatternPiece::Edge(
                    self.lower_graph_edge_pattern(edge)?,
                ));
            }
        }
        Ok(pieces)
    }

    fn fixed_paths_for_pattern(
        &self,
        pattern: &GqlBoundPattern,
        pieces: &[GraphPatternPiece],
        scope: Vec<usize>,
        base_piece_index: usize,
    ) -> Result<Vec<GraphFixedPathBinding>, EngineError> {
        let Some(alias) = pattern.path_alias.clone() else {
            return Ok(Vec::new());
        };
        if pattern.edges.len() <= 1 {
            return Ok(Vec::new());
        }
        if pattern.edges.iter().any(|edge| edge.quantifier.is_some()) {
            return Err(EngineError::GqlUnsupported {
                feature: "path assignment".to_string(),
                message: "path assignment across multiple relationship patterns with a variable-length segment is not supported; use one bounded relationship pattern for variable-length path assignment".to_string(),
                span: pattern
                    .path_span
                    .clone()
                    .unwrap_or_else(|| pattern.span.clone()),
            });
        }
        if pieces.len() != pattern.edges.len()
            || !pieces
                .iter()
                .all(|piece| matches!(piece, GraphPatternPiece::Edge(_)))
        {
            return Err(EngineError::InvalidOperation(
                "GQL fixed path lowering expected fixed edge pieces".to_string(),
            ));
        }
        let node_aliases = pattern
            .nodes
            .iter()
            .map(|node| node.alias.clone())
            .collect::<Vec<_>>();
        let edge_piece_indices = (0..pattern.edges.len())
            .map(|index| base_piece_index + index)
            .collect::<Vec<_>>();
        Ok(vec![GraphFixedPathBinding {
            scope,
            alias,
            node_aliases,
            edge_piece_indices,
            after_piece_index: base_piece_index + pattern.edges.len() - 1,
        }])
    }

    fn lower_graph_variable_length_pattern(
        &mut self,
        edge: &GqlBoundEdgePattern,
        path_alias: Option<&str>,
    ) -> Result<GraphVariableLengthPattern, EngineError> {
        let (min_hops, max_hops) = edge
            .quantifier
            .as_ref()
            .map(|quantifier| (quantifier.min_hops, quantifier.max_hops))
            .unwrap_or((1, 1));
        if edge.alias.is_some() && (min_hops != 1 || max_hops != 1) {
            return Err(EngineError::GqlUnsupported {
                feature: "multi-hop relationship-list aliases".to_string(),
                message: "relationship aliases on variable-length patterns are supported only for exactly 1..1; return the path alias and inspect edge_ids instead".to_string(),
                span: edge.span.clone(),
            });
        }
        let mut filter_parts = Vec::new();
        if let Some(alias) = edge.alias.as_ref() {
            self.push_edge_property_map_filters(
                alias,
                edge.properties.as_ref(),
                &mut filter_parts,
            )?;
        } else {
            self.push_edge_property_map_filters(
                DIRECT_EDGE_ALIAS,
                edge.properties.as_ref(),
                &mut filter_parts,
            )?;
        }
        Ok(GraphVariableLengthPattern {
            path_alias: path_alias.map(str::to_string),
            edge_alias: edge.alias.clone(),
            from_alias: edge.from_alias.clone(),
            to_alias: edge.to_alias.clone(),
            direction: native_direction(edge.direction),
            label_filter: edge
                .rel_types
                .iter()
                .map(|label| label.name.clone())
                .collect(),
            filter: combine_edge_filters(filter_parts),
            min_hops,
            max_hops,
        })
    }

    fn lower_graph_node_pattern(
        &mut self,
        node: &GqlBoundNodePattern,
    ) -> Result<GraphNodePattern, EngineError> {
        let mut filter_parts = Vec::new();
        self.push_node_property_map_filters(
            &node.alias,
            node.properties.as_ref(),
            &mut filter_parts,
        )?;
        Ok(GraphNodePattern {
            alias: node.alias.clone(),
            label_filter: node_label_filter(&node.labels),
            ids: Vec::new(),
            keys: Vec::new(),
            filter: combine_node_filters(filter_parts),
        })
    }

    fn lower_graph_edge_pattern(
        &mut self,
        edge: &GqlBoundEdgePattern,
    ) -> Result<GraphEdgePattern, EngineError> {
        let mut filter_parts = Vec::new();
        if let Some(alias) = edge.alias.as_ref() {
            self.push_edge_property_map_filters(
                alias,
                edge.properties.as_ref(),
                &mut filter_parts,
            )?;
        } else if edge.properties.is_some() {
            self.push_edge_property_map_filters(
                DIRECT_EDGE_ALIAS,
                edge.properties.as_ref(),
                &mut filter_parts,
            )?;
        }
        Ok(GraphEdgePattern {
            alias: edge.alias.clone(),
            from_alias: edge.from_alias.clone(),
            to_alias: edge.to_alias.clone(),
            direction: native_direction(edge.direction),
            label_filter: edge
                .rel_types
                .iter()
                .map(|label| label.name.clone())
                .collect(),
            filter: combine_edge_filters(filter_parts),
        })
    }

    fn base_graph_row_query(
        &self,
        nodes: Vec<GraphNodePattern>,
        pieces: Vec<GraphPatternPiece>,
        options: &GqlExecutionOptions,
    ) -> GraphRowQuery {
        let execution_limit = options.max_intermediate_bindings.max(1);
        GraphRowQuery {
            nodes,
            pieces,
            where_: None,
            return_items: None,
            order_by: Vec::new(),
            page: GraphPageRequest {
                skip: 0,
                limit: options.max_rows.max(1),
                cursor: options.cursor.clone(),
            },
            at_epoch: None,
            params: gql_params_to_graph_params(self.params),
            output: GraphOutputOptions {
                mode: GraphOutputMode::Ids,
                compact_rows: options.compact_rows,
                include_vectors: options.include_vectors,
            },
            options: GraphQueryOptions {
                allow_full_scan: options.allow_full_scan,
                max_intermediate_bindings: execution_limit,
                max_frontier: options.max_frontier,
                max_path_hops: options.max_path_hops,
                max_paths_per_start: options.max_paths_per_start,
                max_page_limit: execution_limit,
                max_order_materialization: options.max_order_materialization,
                max_cursor_bytes: options.max_cursor_bytes,
                max_query_bytes: options.max_query_bytes,
                include_plan: options.include_plan,
                profile: options.profile,
            },
        }
    }

    fn finalize_graph_row_target(
        &self,
        semantic: &GqlSemanticPlan,
        options: &GqlExecutionOptions,
        target: &mut GqlNativeTarget,
    ) -> Result<(), EngineError> {
        let GqlNativeTarget::GraphRows { query } = target else {
            return Err(EngineError::InvalidOperation(
                "GQL graph-row finalization received a non-graph-row target".to_string(),
            ));
        };
        query.query.where_ = self.graph_residual_expr()?;
        query.query.return_items = Some(gql_graph_return_items(semantic)?);
        query.query.options.include_plan = options.include_plan;
        Ok(())
    }

    fn graph_residual_expr(&self) -> Result<Option<GraphExpr>, EngineError> {
        let mut exprs = self
            .residual_predicates
            .iter()
            .map(|expr| gql_expr_to_graph_expr(expr, &self.alias_kinds))
            .collect::<Result<Vec<_>, _>>()?;
        if exprs.is_empty() {
            return Ok(None);
        }
        let mut combined = exprs.remove(0);
        for expr in exprs {
            combined = GraphExpr::Binary {
                left: Box::new(combined),
                op: GraphBinaryOp::And,
                right: Box::new(expr),
            };
        }
        Ok(Some(combined))
    }

    fn push_node_property_map_filters(
        &mut self,
        alias: &str,
        properties: Option<&MapLiteral>,
        filter_parts: &mut Vec<NodeFilterExpr>,
    ) -> Result<(), EngineError> {
        let Some(properties) = properties else {
            return Ok(());
        };
        for entry in &properties.entries {
            if let Some(metadata) = GqlElementMapMetadataKey::from_key(&entry.key.name) {
                if !metadata.valid_for_node() {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "element map metadata '{}' is valid only for relationships",
                            metadata.canonical_name()
                        ),
                        entry.key.span.clone(),
                    ));
                }
                match (metadata, constant_prop_value(&entry.value, self.params)?) {
                    (GqlElementMapMetadataKey::ElementKey, Some(PropValue::String(value))) => {
                        filter_parts.push(NodeFilterExpr::KeyEquals(value.clone()));
                        self.pushed_down.push(GqlPushedPredicate {
                            alias: alias.to_string(),
                            target_kind: GqlAliasKind::Node,
                            summary: format!("elementKey({alias}) = {value:?}"),
                        });
                    }
                    _ => self
                        .residual_predicates
                        .push(metadata_map_entry_predicate(alias, metadata, entry)),
                }
                continue;
            }
            match constant_prop_value(&entry.value, self.params)? {
                Some(value) if !matches!(value, PropValue::Null) => {
                    filter_parts.push(NodeFilterExpr::PropertyEquals {
                        key: entry.key.name.clone(),
                        value: value.clone(),
                    });
                    self.pushed_down.push(GqlPushedPredicate {
                        alias: alias.to_string(),
                        target_kind: GqlAliasKind::Node,
                        summary: format!("{}.{} = {:?}", alias, entry.key.name, value),
                    });
                }
                _ => self
                    .residual_predicates
                    .push(property_map_residual(alias, entry)),
            }
        }
        Ok(())
    }

    fn push_edge_property_map_filters(
        &mut self,
        alias: &str,
        properties: Option<&MapLiteral>,
        filter_parts: &mut Vec<EdgeFilterExpr>,
    ) -> Result<(), EngineError> {
        let Some(properties) = properties else {
            return Ok(());
        };
        for entry in &properties.entries {
            if let Some(metadata) = GqlElementMapMetadataKey::from_key(&entry.key.name) {
                if !metadata.valid_for_edge() {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "element map metadata '{}' is valid only for nodes",
                            metadata.canonical_name()
                        ),
                        entry.key.span.clone(),
                    ));
                }
                let field = match metadata {
                    GqlElementMapMetadataKey::Weight => EdgeMetadataField::Weight,
                    GqlElementMapMetadataKey::ValidFrom => EdgeMetadataField::ValidFrom,
                    GqlElementMapMetadataKey::ValidTo => EdgeMetadataField::ValidTo,
                    GqlElementMapMetadataKey::ElementKey => {
                        unreachable!("elementKey rejected for edges above")
                    }
                };
                let constant = constant_prop_value(&entry.value, self.params)?;
                let eq_filter = constant
                    .as_ref()
                    .and_then(|value| edge_metadata_eq_filter(field, value));
                match eq_filter {
                    Some(filter) => {
                        filter_parts.push(filter);
                        let explain_alias = edge_explain_alias(alias);
                        let value = constant.expect("eq filter requires a constant value");
                        self.pushed_down.push(GqlPushedPredicate {
                            alias: explain_alias.to_string(),
                            target_kind: GqlAliasKind::Edge,
                            summary: format!("{} = {:?}", field.summary_expr(explain_alias), value),
                        });
                    }
                    None if alias == DIRECT_EDGE_ALIAS => {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidPropertyAccess,
                            "anonymous relationship property constraints must lower to native filters"
                                .to_string(),
                            entry.span.clone(),
                        ));
                    }
                    None => self
                        .residual_predicates
                        .push(metadata_map_entry_predicate(alias, metadata, entry)),
                }
                continue;
            }
            match constant_prop_value(&entry.value, self.params)? {
                Some(value) if !matches!(value, PropValue::Null) => {
                    filter_parts.push(EdgeFilterExpr::PropertyEquals {
                        key: entry.key.name.clone(),
                        value: value.clone(),
                    });
                    let explain_alias = edge_explain_alias(alias);
                    self.pushed_down.push(GqlPushedPredicate {
                        alias: explain_alias.to_string(),
                        target_kind: GqlAliasKind::Edge,
                        summary: format!("{}.{} = {:?}", explain_alias, entry.key.name, value),
                    });
                }
                _ if alias == DIRECT_EDGE_ALIAS => {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        "anonymous relationship property constraints must lower to native filters"
                            .to_string(),
                        entry.span.clone(),
                    ));
                }
                _ => self
                    .residual_predicates
                    .push(property_map_residual(alias, entry)),
            }
        }
        Ok(())
    }

    fn apply_where_to_graph_node(
        &mut self,
        expr: &Expr,
        alias: &str,
        node: &mut GraphNodePattern,
    ) -> Result<(), EngineError> {
        let mut filters = Vec::new();
        let mut ids = Vec::new();
        let mut keys = Vec::new();
        let allow_key_pushdown = node_key_pushdown_supported(node.label_filter.as_ref());
        self.collect_node_predicate_filters(
            expr,
            alias,
            &mut filters,
            &mut ids,
            &mut keys,
            allow_key_pushdown,
        )?;
        node.filter = merge_node_filter(node.filter.take(), combine_node_filters(filters));
        node.ids.extend(ids);
        if allow_key_pushdown {
            node.keys.extend(keys.into_iter().map(|key| {
                NodeKeyQuery {
                    label: node
                        .label_filter
                        .as_ref()
                        .and_then(|filter| filter.labels.first())
                        .cloned()
                        .unwrap_or_default(),
                    key,
                }
            }));
        }
        Ok(())
    }

    fn apply_where_to_graph_pattern(
        &mut self,
        expr: &Expr,
        nodes: &mut [GraphNodePattern],
        pieces: &mut [GraphPatternPiece],
        node_indexes: &BTreeMap<String, usize>,
        edge_indexes: &BTreeMap<String, usize>,
    ) -> Result<(), EngineError> {
        if let ExprKind::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = &expr.kind
        {
            self.apply_where_to_graph_pattern(left, nodes, pieces, node_indexes, edge_indexes)?;
            self.apply_where_to_graph_pattern(right, nodes, pieces, node_indexes, edge_indexes)?;
            return Ok(());
        }

        let pushed_before = self.pushed_down.len();
        match self.try_push_predicate(expr)? {
            Some(PushFilter::Node { alias, filter }) => {
                let Some(index) = node_indexes.get(&alias).copied() else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                let node = &mut nodes[index];
                node.filter = merge_node_filter(node.filter.take(), Some(filter));
            }
            Some(PushFilter::NodeIds { alias, ids }) => {
                let Some(index) = node_indexes.get(&alias).copied() else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                let node = &mut nodes[index];
                node.ids.extend(ids);
            }
            Some(PushFilter::NodeKeys { alias, keys }) => {
                let Some(index) = node_indexes.get(&alias).copied() else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                let node = &mut nodes[index];
                if node_key_pushdown_supported(node.label_filter.as_ref()) && node.keys.is_empty() {
                    let summary = node_key_summary(&alias, &keys);
                    self.record_node_push(alias, summary);
                    node.keys.extend(keys.into_iter().map(|key| {
                        NodeKeyQuery {
                            label: node
                                .label_filter
                                .as_ref()
                                .and_then(|filter| filter.labels.first())
                                .cloned()
                                .unwrap_or_default(),
                            key,
                        }
                    }));
                } else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                }
            }
            Some(PushFilter::Edge { alias, filter }) => {
                let Some(index) = edge_indexes.get(&alias).copied() else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                let Some(edge) = graph_fixed_edge_mut(pieces, index) else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                edge.filter = merge_edge_filter(edge.filter.take(), Some(filter));
            }
            Some(PushFilter::EdgeLabels {
                alias,
                labels,
                summary,
            }) => {
                let Some(index) = edge_indexes.get(&alias).copied() else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                let Some(edge) = graph_fixed_edge_mut(pieces, index) else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                    return Ok(());
                };
                if merge_edge_label_filter(&mut edge.label_filter, &labels) {
                    self.record_edge_push(alias, summary);
                } else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                }
            }
            Some(PushFilter::EdgeIds { alias, ids }) => {
                if edge_indexes.contains_key(&alias) {
                    if self.edge_id_constraints.contains_key(&alias) {
                        self.record_residual_after_failed_push(expr, pushed_before);
                    } else {
                        let summary = edge_id_summary(&alias, &ids);
                        self.edge_id_constraints.insert(alias.clone(), ids);
                        self.record_edge_push(alias, summary);
                    }
                } else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                }
            }
            Some(PushFilter::EdgeEndpointIds { alias, field, ids }) => {
                let summary = edge_endpoint_summary(&alias, field, &ids);
                if self.apply_edge_endpoint_ids_to_pattern(
                    &alias,
                    field,
                    &ids,
                    nodes,
                    pieces,
                    node_indexes,
                    edge_indexes,
                ) {
                    self.record_edge_push(alias, summary);
                } else {
                    self.record_residual_after_failed_push(expr, pushed_before);
                }
            }
            Some(PushFilter::Noop) => self.record_residual_after_failed_push(expr, pushed_before),
            None => self.record_residual_after_failed_push(expr, pushed_before),
        }
        Ok(())
    }

    fn collect_node_predicate_filters(
        &mut self,
        expr: &Expr,
        allowed_alias: &str,
        filters: &mut Vec<NodeFilterExpr>,
        ids: &mut Vec<u64>,
        keys: &mut Vec<String>,
        allow_key_pushdown: bool,
    ) -> Result<(), EngineError> {
        match &expr.kind {
            ExprKind::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                self.collect_node_predicate_filters(
                    left,
                    allowed_alias,
                    filters,
                    ids,
                    keys,
                    allow_key_pushdown,
                )?;
                self.collect_node_predicate_filters(
                    right,
                    allowed_alias,
                    filters,
                    ids,
                    keys,
                    allow_key_pushdown,
                )?;
            }
            _ => match self.try_push_predicate(expr)? {
                Some(PushFilter::Node { alias, filter }) if alias == allowed_alias => {
                    filters.push(filter)
                }
                Some(PushFilter::NodeIds {
                    alias,
                    ids: pushed_ids,
                }) if alias == allowed_alias => ids.extend(pushed_ids),
                Some(PushFilter::NodeKeys {
                    alias,
                    keys: pushed_keys,
                }) if alias == allowed_alias && allow_key_pushdown && keys.is_empty() => {
                    let summary = node_key_summary(&alias, &pushed_keys);
                    self.record_node_push(alias, summary);
                    keys.extend(pushed_keys);
                }
                _ => self.residual_predicates.push(expr.clone()),
            },
        }
        Ok(())
    }

    fn try_push_predicate(&mut self, expr: &Expr) -> Result<Option<PushFilter>, EngineError> {
        match &expr.kind {
            ExprKind::Binary { op, left, right } => match op {
                BinaryOp::Eq => self
                    .try_push_eq(left, right)
                    .or_else(|| self.try_push_eq(right, left))
                    .transpose(),
                BinaryOp::In => self.try_push_in(left, right).transpose(),
                BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => self
                    .try_push_range(*op, left, right)
                    .or_else(|| {
                        reverse_range_op(*op).and_then(|op| self.try_push_range(op, right, left))
                    })
                    .transpose(),
                BinaryOp::And
                | BinaryOp::Or
                | BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Div
                | BinaryOp::Neq
                | BinaryOp::StartsWith
                | BinaryOp::EndsWith
                | BinaryOp::Contains => Ok(None),
            },
            ExprKind::IsNull { .. } | ExprKind::Unary { .. } | ExprKind::Case { .. } => Ok(None),
            _ => Ok(None),
        }
    }

    fn try_push_eq(
        &mut self,
        left: &Expr,
        right: &Expr,
    ) -> Option<Result<PushFilter, EngineError>> {
        let reference = entity_value_ref(left, &self.alias_kinds)?;
        let value = match constant_prop_value(right, self.params) {
            Ok(Some(value)) if !matches!(value, PropValue::Null) => value,
            Ok(_) => return None,
            Err(err) => return Some(Err(err)),
        };
        Some(self.eq_filter(reference, value, right))
    }

    fn try_push_in(
        &mut self,
        left: &Expr,
        right: &Expr,
    ) -> Option<Result<PushFilter, EngineError>> {
        let reference = entity_value_ref(left, &self.alias_kinds)?;
        if matches!(reference, EntityValueRef::EdgeMetadata { .. }) {
            return None;
        }
        let values = match constant_list_values(right, self.params) {
            Ok(Some(values))
                if !values.is_empty()
                    && values.iter().all(|value| !matches!(value, PropValue::Null)) =>
            {
                values
            }
            Ok(_) => return None,
            Err(err) => return Some(Err(err)),
        };
        Some(self.in_filter(reference, values))
    }

    fn try_push_range(
        &mut self,
        op: BinaryOp,
        left: &Expr,
        right: &Expr,
    ) -> Option<Result<PushFilter, EngineError>> {
        let reference = entity_value_ref(left, &self.alias_kinds)?;
        let value = match constant_prop_value(right, self.params) {
            Ok(Some(value)) if range_pushdown_compatible(&reference, &value) => value,
            Ok(_) => return None,
            Err(err) => return Some(Err(err)),
        };
        Some(self.range_filter(reference, op, value))
    }

    fn eq_filter(
        &mut self,
        reference: EntityValueRef,
        value: PropValue,
        value_expr: &Expr,
    ) -> Result<PushFilter, EngineError> {
        match reference {
            EntityValueRef::NodeProperty { alias, key } => {
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Node,
                    summary: format!("{}.{} = {:?}", alias, key, value),
                });
                Ok(PushFilter::Node {
                    alias,
                    filter: NodeFilterExpr::PropertyEquals { key, value },
                })
            }
            EntityValueRef::NodeMetadata { alias, field } => match field {
                NodeMetadataField::Id => {
                    let id = match id_value_for_eq(&value, value_expr, "node id")? {
                        IdValueMatch::Id(id) => id,
                        IdValueMatch::Impossible => {
                            return Ok(false_node_push(alias));
                        }
                        IdValueMatch::Residual => return Ok(PushFilter::Noop),
                    };
                    self.pushed_down.push(GqlPushedPredicate {
                        alias: alias.clone(),
                        target_kind: GqlAliasKind::Node,
                        summary: format!("{} = {id}", field.summary_expr(&alias)),
                    });
                    Ok(PushFilter::NodeIds {
                        alias,
                        ids: vec![id],
                    })
                }
                NodeMetadataField::Key => {
                    let Some(key) = prop_value_to_key(&value) else {
                        return Ok(PushFilter::Noop);
                    };
                    Ok(PushFilter::NodeKeys {
                        alias,
                        keys: vec![key],
                    })
                }
                NodeMetadataField::UpdatedAt => {
                    let Some(value) = prop_value_to_i64(&value) else {
                        return Ok(PushFilter::Noop);
                    };
                    self.pushed_down.push(GqlPushedPredicate {
                        alias: alias.clone(),
                        target_kind: GqlAliasKind::Node,
                        summary: format!("{} = {value}", field.summary_expr(&alias)),
                    });
                    Ok(PushFilter::Node {
                        alias,
                        filter: NodeFilterExpr::UpdatedAtRange {
                            lower_ms: Some(value),
                            upper_ms: Some(value),
                        },
                    })
                }
                NodeMetadataField::Labels
                | NodeMetadataField::Weight
                | NodeMetadataField::CreatedAt => Ok(PushFilter::Noop),
            },
            EntityValueRef::EdgeProperty { alias, key } => {
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Edge,
                    summary: format!("{}.{} = {:?}", alias, key, value),
                });
                Ok(PushFilter::Edge {
                    alias,
                    filter: EdgeFilterExpr::PropertyEquals { key, value },
                })
            }
            EntityValueRef::EdgeEndpoint { alias, field } => {
                let id = match id_value_for_eq(&value, value_expr, "edge endpoint id")? {
                    IdValueMatch::Id(id) => id,
                    IdValueMatch::Impossible => {
                        return Ok(false_edge_push(alias));
                    }
                    IdValueMatch::Residual => return Ok(PushFilter::Noop),
                };
                Ok(PushFilter::EdgeEndpointIds {
                    alias,
                    field,
                    ids: vec![id],
                })
            }
            EntityValueRef::EdgeMetadata { alias, field } => {
                let Some(filter) = edge_metadata_eq_filter(field, &value) else {
                    return Ok(PushFilter::Noop);
                };
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Edge,
                    summary: format!("{} = {:?}", field.summary_expr(&alias), value),
                });
                Ok(PushFilter::Edge { alias, filter })
            }
            EntityValueRef::NodeId { alias } => {
                let id = match id_value_for_eq(&value, value_expr, "node id")? {
                    IdValueMatch::Id(id) => id,
                    IdValueMatch::Impossible => {
                        return Ok(false_node_push(alias));
                    }
                    IdValueMatch::Residual => return Ok(PushFilter::Noop),
                };
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Node,
                    summary: format!("id({alias}) = {id}"),
                });
                Ok(PushFilter::NodeIds {
                    alias,
                    ids: vec![id],
                })
            }
            EntityValueRef::EdgeId { alias } => {
                let id = match id_value_for_eq(&value, value_expr, "edge id")? {
                    IdValueMatch::Id(id) => id,
                    IdValueMatch::Impossible => {
                        return Ok(false_edge_push(alias));
                    }
                    IdValueMatch::Residual => return Ok(PushFilter::Noop),
                };
                Ok(PushFilter::EdgeIds {
                    alias,
                    ids: vec![id],
                })
            }
            EntityValueRef::RelationshipLabelFunction { alias } => {
                let Some(label) = prop_value_to_label(&value) else {
                    return Ok(PushFilter::Noop);
                };
                Ok(PushFilter::EdgeLabels {
                    alias,
                    labels: vec![label.clone()],
                    summary: format!("type() = {:?}", label),
                })
            }
        }
    }

    fn in_filter(
        &mut self,
        reference: EntityValueRef,
        values: Vec<PropValue>,
    ) -> Result<PushFilter, EngineError> {
        match reference {
            EntityValueRef::NodeProperty { alias, key } => {
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Node,
                    summary: format!("{}.{} IN {:?}", alias, key, values),
                });
                Ok(PushFilter::Node {
                    alias,
                    filter: NodeFilterExpr::PropertyIn { key, values },
                })
            }
            EntityValueRef::NodeMetadata { alias, field } => match field {
                NodeMetadataField::Id => {
                    let ids = match id_values_for_in(&values) {
                        IdListMatch::Ids(ids) => ids,
                        IdListMatch::Impossible => {
                            return Ok(false_node_push(alias));
                        }
                        IdListMatch::Residual => return Ok(PushFilter::Noop),
                    };
                    self.pushed_down.push(GqlPushedPredicate {
                        alias: alias.clone(),
                        target_kind: GqlAliasKind::Node,
                        summary: format!("{} IN {:?}", field.summary_expr(&alias), ids),
                    });
                    Ok(PushFilter::NodeIds { alias, ids })
                }
                NodeMetadataField::Key => {
                    let Some(keys) = prop_values_to_keys(&values) else {
                        return Ok(PushFilter::Noop);
                    };
                    Ok(PushFilter::NodeKeys { alias, keys })
                }
                NodeMetadataField::Labels
                | NodeMetadataField::Weight
                | NodeMetadataField::CreatedAt
                | NodeMetadataField::UpdatedAt => Ok(PushFilter::Noop),
            },
            EntityValueRef::EdgeProperty { alias, key } => {
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Edge,
                    summary: format!("{}.{} IN {:?}", alias, key, values),
                });
                Ok(PushFilter::Edge {
                    alias,
                    filter: EdgeFilterExpr::PropertyIn { key, values },
                })
            }
            EntityValueRef::EdgeEndpoint { alias, field } => {
                let ids = match id_values_for_in(&values) {
                    IdListMatch::Ids(ids) => ids,
                    IdListMatch::Impossible => {
                        return Ok(false_edge_push(alias));
                    }
                    IdListMatch::Residual => return Ok(PushFilter::Noop),
                };
                Ok(PushFilter::EdgeEndpointIds { alias, field, ids })
            }
            EntityValueRef::EdgeMetadata { .. } => Ok(PushFilter::Noop),
            EntityValueRef::NodeId { alias } => {
                let ids = match id_values_for_in(&values) {
                    IdListMatch::Ids(ids) => ids,
                    IdListMatch::Impossible => {
                        return Ok(false_node_push(alias));
                    }
                    IdListMatch::Residual => return Ok(PushFilter::Noop),
                };
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Node,
                    summary: format!("id({alias}) IN {:?}", ids),
                });
                Ok(PushFilter::NodeIds { alias, ids })
            }
            EntityValueRef::EdgeId { alias } => {
                let ids = match id_values_for_in(&values) {
                    IdListMatch::Ids(ids) => ids,
                    IdListMatch::Impossible => {
                        return Ok(false_edge_push(alias));
                    }
                    IdListMatch::Residual => return Ok(PushFilter::Noop),
                };
                Ok(PushFilter::EdgeIds { alias, ids })
            }
            EntityValueRef::RelationshipLabelFunction { alias } => {
                let Some(labels) = prop_values_to_labels(&values) else {
                    return Ok(PushFilter::Noop);
                };
                Ok(PushFilter::EdgeLabels {
                    alias,
                    summary: format!("type() IN {:?}", labels),
                    labels,
                })
            }
        }
    }

    fn range_filter(
        &mut self,
        reference: EntityValueRef,
        op: BinaryOp,
        value: PropValue,
    ) -> Result<PushFilter, EngineError> {
        match reference {
            EntityValueRef::NodeProperty { alias, key } => {
                let (lower, upper, op_text) = range_bounds(op, value);
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Node,
                    summary: format!("{}.{} {}", alias, key, op_text),
                });
                Ok(PushFilter::Node {
                    alias,
                    filter: NodeFilterExpr::PropertyRange { key, lower, upper },
                })
            }
            EntityValueRef::NodeMetadata { alias, field } => {
                let Some(filter) = node_metadata_range_filter(field, op, &value) else {
                    return Ok(PushFilter::Noop);
                };
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Node,
                    summary: format!("{} {}", field.summary_expr(&alias), range_op_text(op)),
                });
                Ok(PushFilter::Node { alias, filter })
            }
            EntityValueRef::EdgeProperty { alias, key } => {
                let (lower, upper, op_text) = range_bounds(op, value);
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Edge,
                    summary: format!("{}.{} {}", alias, key, op_text),
                });
                Ok(PushFilter::Edge {
                    alias,
                    filter: EdgeFilterExpr::PropertyRange { key, lower, upper },
                })
            }
            EntityValueRef::EdgeEndpoint { .. } => Ok(PushFilter::Noop),
            EntityValueRef::EdgeMetadata { alias, field } => {
                let Some(filter) = edge_metadata_range_filter(field, op, &value) else {
                    return Ok(PushFilter::Noop);
                };
                self.pushed_down.push(GqlPushedPredicate {
                    alias: alias.clone(),
                    target_kind: GqlAliasKind::Edge,
                    summary: format!("{} {}", field.summary_expr(&alias), range_op_text(op)),
                });
                Ok(PushFilter::Edge { alias, filter })
            }
            EntityValueRef::NodeId { .. }
            | EntityValueRef::EdgeId { .. }
            | EntityValueRef::RelationshipLabelFunction { .. } => Ok(PushFilter::Noop),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_edge_endpoint_ids_to_pattern(
        &mut self,
        alias: &str,
        field: EdgeEndpointField,
        ids: &[u64],
        nodes: &mut [GraphNodePattern],
        pieces: &[GraphPatternPiece],
        node_indexes: &BTreeMap<String, usize>,
        edge_indexes: &BTreeMap<String, usize>,
    ) -> bool {
        let Some(edge_index) = edge_indexes.get(alias).copied() else {
            return false;
        };
        let Some(GraphPatternPiece::Edge(edge)) = pieces.get(edge_index) else {
            return false;
        };
        let endpoint_alias = match (field, edge.direction) {
            (EdgeEndpointField::From, Direction::Outgoing)
            | (EdgeEndpointField::To, Direction::Incoming) => edge.from_alias.as_str(),
            (EdgeEndpointField::To, Direction::Outgoing)
            | (EdgeEndpointField::From, Direction::Incoming) => edge.to_alias.as_str(),
            (_, Direction::Both) => return false,
        };
        let Some(node_index) = node_indexes.get(endpoint_alias).copied() else {
            return false;
        };
        if !nodes[node_index].ids.is_empty() {
            return false;
        }
        nodes[node_index].ids.extend(ids.iter().copied());
        true
    }

    fn record_edge_push(&mut self, alias: String, summary: String) {
        self.pushed_down.push(GqlPushedPredicate {
            alias,
            target_kind: GqlAliasKind::Edge,
            summary,
        });
    }

    fn record_node_push(&mut self, alias: String, summary: String) {
        self.pushed_down.push(GqlPushedPredicate {
            alias,
            target_kind: GqlAliasKind::Node,
            summary,
        });
    }

    fn record_residual_after_failed_push(&mut self, expr: &Expr, pushed_before: usize) {
        self.pushed_down.truncate(pushed_before);
        self.residual_predicates.push(expr.clone());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum EntityValueRef {
    NodeProperty {
        alias: String,
        key: String,
    },
    NodeMetadata {
        alias: String,
        field: NodeMetadataField,
    },
    EdgeProperty {
        alias: String,
        key: String,
    },
    EdgeEndpoint {
        alias: String,
        field: EdgeEndpointField,
    },
    EdgeMetadata {
        alias: String,
        field: EdgeMetadataField,
    },
    NodeId {
        alias: String,
    },
    EdgeId {
        alias: String,
    },
    RelationshipLabelFunction {
        alias: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeMetadataField {
    Id,
    Labels,
    Key,
    Weight,
    CreatedAt,
    UpdatedAt,
}

impl NodeMetadataField {
    fn summary_expr(self, alias: &str) -> String {
        let function = match self {
            Self::Id => "id",
            Self::Labels => "labels",
            Self::Key => "elementKey",
            Self::Weight => "weight",
            Self::CreatedAt => "createdAt",
            Self::UpdatedAt => "updatedAt",
        };
        format!("{function}({alias})")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeEndpointField {
    From,
    To,
}

impl EdgeEndpointField {
    fn summary_expr(self, alias: &str) -> String {
        match self {
            Self::From => format!("id(startNode({alias}))"),
            Self::To => format!("id(endNode({alias}))"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeMetadataField {
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo,
}

impl EdgeMetadataField {
    fn summary_expr(self, alias: &str) -> String {
        let function = match self {
            Self::Weight => "weight",
            Self::CreatedAt => "createdAt",
            Self::UpdatedAt => "updatedAt",
            Self::ValidFrom => "validFrom",
            Self::ValidTo => "validTo",
        };
        format!("{function}({alias})")
    }
}

#[derive(Clone, Debug, PartialEq)]
enum PushFilter {
    Node {
        alias: String,
        filter: NodeFilterExpr,
    },
    NodeIds {
        alias: String,
        ids: Vec<u64>,
    },
    NodeKeys {
        alias: String,
        keys: Vec<String>,
    },
    Edge {
        alias: String,
        filter: EdgeFilterExpr,
    },
    EdgeIds {
        alias: String,
        ids: Vec<u64>,
    },
    EdgeEndpointIds {
        alias: String,
        field: EdgeEndpointField,
        ids: Vec<u64>,
    },
    EdgeLabels {
        alias: String,
        labels: Vec<String>,
        summary: String,
    },
    Noop,
}

fn reject_unsupported_pure_edge_label_or(
    clauses: &[GqlBoundMatchClause],
    pattern: &GqlBoundPattern,
    params: &GqlParams,
) -> Result<(), EngineError> {
    if !has_unconstrained_anonymous_edge_endpoints(pattern) {
        return Ok(());
    }
    let edge = &pattern.edges[0];
    if edge.rel_types.len() > 1 {
        return Err(unsupported_pure_edge_label_or(
            edge.span.clone(),
            "relationship type alternatives on a pure edge match require graph-row pure-edge label-alternative support",
        ));
    }
    let Some(edge_alias) = edge.alias.as_deref() else {
        return Ok(());
    };
    if edge.rel_types.is_empty()
        && where_has_multi_label_constraint_for_edge(
            clauses
                .iter()
                .find_map(|clause| clause.where_clause.as_ref()),
            edge_alias,
            params,
        )?
    {
        return Err(unsupported_pure_edge_label_or(
            clauses
                .iter()
                .find_map(|clause| clause.where_clause.as_ref())
                .map(|expr| expr.span.clone())
                .unwrap_or_else(|| edge.span.clone()),
            "type(r) IN with multiple relationship labels on a pure edge match requires graph-row pure-edge label-alternative support",
        ));
    }
    Ok(())
}

fn has_unconstrained_anonymous_edge_endpoints(pattern: &GqlBoundPattern) -> bool {
    pattern.nodes.len() == 2
        && pattern.edges.len() == 1
        && pattern.nodes.iter().all(|node| {
            node.user_alias.is_none() && node.labels.is_empty() && node.properties.is_none()
        })
}

fn unsupported_pure_edge_label_or(span: SourceSpan, message: &str) -> EngineError {
    EngineError::GqlUnsupported {
        feature: "edge label alternatives".to_string(),
        message: format!(
            "{message}; tracked for a future graph-row pure-edge enhancement so self-loop semantics stay exact"
        ),
        span,
    }
}

fn is_pure_edge_query_shape(
    semantic: &GqlSemanticPlan,
    pattern: &GqlBoundPattern,
    params: &GqlParams,
) -> Result<bool, EngineError> {
    if pattern.nodes.len() != 2 || pattern.edges.len() != 1 {
        return Ok(false);
    }
    let edge = &pattern.edges[0];
    if edge.rel_types.len() > 1 {
        return Ok(false);
    }
    if pattern.nodes.iter().any(|node| {
        node.user_alias.is_some() || !node.labels.is_empty() || node.properties.is_some()
    }) {
        return Ok(false);
    }
    let Some(edge_alias) = edge.alias.as_deref() else {
        return Ok(true);
    };
    if edge.rel_types.is_empty()
        && where_has_multi_label_constraint_for_edge(
            semantic
                .clauses
                .iter()
                .find_map(|clause| clause.where_clause.as_ref()),
            edge_alias,
            params,
        )?
    {
        return Ok(false);
    }
    Ok(references_only_edge_alias(semantic, edge_alias))
}

fn references_only_edge_alias(semantic: &GqlSemanticPlan, edge_alias: &str) -> bool {
    let mut variables = BTreeSet::new();
    for clause in &semantic.clauses {
        if let Some(expr) = clause.where_clause.as_ref() {
            collect_expr_variables(expr, &mut variables);
        }
    }
    let return_aliases = return_aliases(semantic);
    for item in &semantic.query.order_by {
        collect_expr_pattern_variables(semantic, &return_aliases, &item.expr, &mut variables);
    }
    if let Some(expr) = semantic.query.skip.as_ref() {
        collect_expr_pattern_variables(semantic, &return_aliases, expr, &mut variables);
    }
    if let Some(expr) = semantic.query.limit.as_ref() {
        collect_expr_pattern_variables(semantic, &return_aliases, expr, &mut variables);
    }
    match &semantic.returns {
        GqlReturnPlan::Star { .. } => {}
        GqlReturnPlan::Items(items) => {
            for item in items {
                collect_expr_variables(&item.expr, &mut variables);
            }
        }
    }
    variables.iter().all(|variable| variable == edge_alias)
}

fn return_aliases(semantic: &GqlSemanticPlan) -> BTreeSet<String> {
    match &semantic.returns {
        GqlReturnPlan::Items(items) => items
            .iter()
            .filter_map(|item| item.explicit_alias.clone())
            .collect(),
        GqlReturnPlan::Star { .. } => BTreeSet::new(),
    }
}

fn collect_expr_pattern_variables(
    semantic: &GqlSemanticPlan,
    return_aliases: &BTreeSet<String>,
    expr: &Expr,
    out: &mut BTreeSet<String>,
) {
    match &expr.kind {
        ExprKind::Variable(name) => {
            if semantic.aliases.contains(name) || !return_aliases.contains(name) {
                out.insert(name.clone());
            }
        }
        ExprKind::PropertyAccess { object, .. } => {
            collect_expr_pattern_variables(semantic, return_aliases, object, out)
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            collect_expr_pattern_variables(semantic, return_aliases, expr, out);
        }
        ExprKind::Binary { left, right, .. } => {
            collect_expr_pattern_variables(semantic, return_aliases, left, out);
            collect_expr_pattern_variables(semantic, return_aliases, right, out);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                collect_expr_pattern_variables(semantic, return_aliases, arg, out);
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_expr_pattern_variables(semantic, return_aliases, arg, out);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_expr_pattern_variables(semantic, return_aliases, operand, out);
            }
            for branch in branches {
                collect_expr_pattern_variables(semantic, return_aliases, &branch.when, out);
                collect_expr_pattern_variables(semantic, return_aliases, &branch.then, out);
            }
            if let Some(else_expr) = else_expr.as_ref() {
                collect_expr_pattern_variables(semantic, return_aliases, else_expr, out);
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_expr_pattern_variables(semantic, return_aliases, &entry.value, out);
            }
        }
        ExprKind::ExistsSubquery(_) => {}
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
}

fn collect_expr_variables(expr: &Expr, out: &mut BTreeSet<String>) {
    match &expr.kind {
        ExprKind::Variable(name) => {
            out.insert(name.clone());
        }
        ExprKind::PropertyAccess { object, .. } => collect_expr_variables(object, out),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            collect_expr_variables(expr, out);
        }
        ExprKind::Binary { left, right, .. } => {
            collect_expr_variables(left, out);
            collect_expr_variables(right, out);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                collect_expr_variables(arg, out);
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_expr_variables(arg, out);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_expr_variables(operand, out);
            }
            for branch in branches {
                collect_expr_variables(&branch.when, out);
                collect_expr_variables(&branch.then, out);
            }
            if let Some(else_expr) = else_expr.as_ref() {
                collect_expr_variables(else_expr, out);
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_expr_variables(&entry.value, out);
            }
        }
        ExprKind::ExistsSubquery(_) => {}
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
}

fn where_has_multi_label_constraint_for_edge(
    expr: Option<&Expr>,
    edge_alias: &str,
    params: &GqlParams,
) -> Result<bool, EngineError> {
    let Some(expr) = expr else {
        return Ok(false);
    };
    match &expr.kind {
        ExprKind::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => Ok(
            where_has_multi_label_constraint_for_edge(Some(left), edge_alias, params)?
                || where_has_multi_label_constraint_for_edge(Some(right), edge_alias, params)?,
        ),
        ExprKind::Binary {
            op: BinaryOp::In,
            left,
            right,
        } if is_type_call_for_alias(left, edge_alias) => {
            let Some(values) = constant_list_values(right, params)? else {
                return Ok(false);
            };
            Ok(prop_values_to_labels(&values).is_some_and(|labels| labels.len() > 1))
        }
        _ => Ok(false),
    }
}

fn is_type_call_for_alias(expr: &Expr, edge_alias: &str) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args } if args.len() == 1 => {
            name.name.eq_ignore_ascii_case("type")
                && variable_name(&args[0]).is_some_and(|alias| alias == edge_alias)
        }
        _ => false,
    }
}

fn gql_params_to_graph_params(params: &GqlParams) -> BTreeMap<String, GraphParamValue> {
    params
        .iter()
        .map(|(key, value)| (key.clone(), gql_param_to_graph_param(value)))
        .collect()
}

fn gql_param_to_graph_param(value: &GqlParamValue) -> GraphParamValue {
    match value {
        GqlParamValue::Null => GraphParamValue::Null,
        GqlParamValue::Bool(value) => GraphParamValue::Bool(*value),
        GqlParamValue::Int(value) => GraphParamValue::Int(*value),
        GqlParamValue::UInt(value) => GraphParamValue::UInt(*value),
        GqlParamValue::Float(value) => GraphParamValue::Float(*value),
        GqlParamValue::String(value) => GraphParamValue::String(value.clone()),
        GqlParamValue::Bytes(value) => GraphParamValue::Bytes(value.clone()),
        GqlParamValue::List(values) => {
            GraphParamValue::List(values.iter().map(gql_param_to_graph_param).collect())
        }
        GqlParamValue::Map(values) => GraphParamValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), gql_param_to_graph_param(value)))
                .collect(),
        ),
    }
}

fn gql_graph_return_items(semantic: &GqlSemanticPlan) -> Result<Vec<GraphReturnItem>, EngineError> {
    match &semantic.returns {
        GqlReturnPlan::Star {
            expanded_aliases, ..
        } => expanded_aliases
            .iter()
            .map(|alias| {
                let expr = Expr {
                    kind: ExprKind::Variable(alias.clone()),
                    span: semantic
                        .aliases
                        .get(alias)
                        .map(|binding| binding.span.clone())
                        .unwrap_or_else(|| semantic.query.return_clause.span.clone()),
                };
                gql_return_item_from_expr(&expr, alias.clone(), semantic)
            })
            .collect(),
        GqlReturnPlan::Items(items) => items
            .iter()
            .map(|item| gql_return_item_from_expr(&item.expr, item.output_name.clone(), semantic))
            .collect(),
    }
}

fn gql_return_item_from_expr(
    expr: &Expr,
    output_name: String,
    semantic: &GqlSemanticPlan,
) -> Result<GraphReturnItem, EngineError> {
    let projection = match &expr.kind {
        ExprKind::Variable(alias) if semantic.aliases.contains(alias) => {
            GraphReturnProjection::Element(GraphElementProjection::Full)
        }
        _ => GraphReturnProjection::Auto,
    };
    Ok(GraphReturnItem {
        expr: gql_expr_to_graph_expr(
            expr,
            &semantic
                .aliases
                .by_name
                .iter()
                .map(|(alias, binding)| (alias.clone(), binding.kind))
                .collect(),
        )?,
        alias: Some(output_name),
        projection,
    })
}

pub(crate) fn gql_expr_to_graph_expr(
    expr: &Expr,
    alias_kinds: &BTreeMap<String, GqlAliasKind>,
) -> Result<GraphExpr, EngineError> {
    Ok(match &expr.kind {
        ExprKind::Literal(literal) => gql_literal_to_graph_expr(literal),
        ExprKind::Parameter(name) => GraphExpr::Param(name.clone()),
        ExprKind::Variable(alias) => GraphExpr::Binding(alias.clone()),
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                if let Some(kind) = alias_kinds.get(alias).copied() {
                    return gql_alias_property_to_graph_expr(alias, kind, property);
                }
            }
            GraphExpr::Property {
                alias: gql_property_object_alias(object)?,
                key: property.name.clone(),
            }
        }
        ExprKind::Unary { op, expr } => GraphExpr::Unary {
            op: gql_unary_op_to_graph_op(*op),
            expr: Box::new(gql_expr_to_graph_expr(expr, alias_kinds)?),
        },
        ExprKind::Binary { op, left, right } => GraphExpr::Binary {
            left: Box::new(gql_expr_to_graph_expr(left, alias_kinds)?),
            op: gql_binary_op_to_graph_op(*op),
            right: Box::new(gql_expr_to_graph_expr(right, alias_kinds)?),
        },
        ExprKind::IsNull { expr, negated } => {
            let inner = Box::new(gql_expr_to_graph_expr(expr, alias_kinds)?);
            if *negated {
                GraphExpr::IsNotNull(inner)
            } else {
                GraphExpr::IsNull(inner)
            }
        }
        ExprKind::FunctionCall { name, args } => {
            if let Some(metadata_expr) = gql_metadata_call_to_graph_expr(name, args, alias_kinds)? {
                metadata_expr
            } else if name.name.eq_ignore_ascii_case("nodeids")
                || name.name.eq_ignore_ascii_case("edgeids")
            {
                if args.len() != 1 {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!("function '{}' expects exactly one argument", name.name),
                        name.span.clone(),
                    ));
                }
                let alias = variable_name(&args[0]).ok_or_else(|| {
                    gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!("function '{}' expects a path alias argument", name.name),
                        args[0].span.clone(),
                    )
                })?;
                GraphExpr::PathField {
                    alias: alias.to_string(),
                    field: if name.name.eq_ignore_ascii_case("nodeids") {
                        GraphPathField::NodeIds
                    } else {
                        GraphPathField::EdgeIds
                    },
                }
            } else {
                GraphExpr::Function {
                    name: gql_function_to_graph_function(&name.name, &name.span)?,
                    args: args
                        .iter()
                        .map(|arg| gql_expr_to_graph_expr(arg, alias_kinds))
                        .collect::<Result<Vec<_>, _>>()?,
                }
            }
        }
        ExprKind::AggregateCall {
            function,
            distinct,
            arg,
            ..
        } => GraphExpr::AggregateCall {
            function: gql_aggregate_function_to_graph(*function),
            distinct: *distinct,
            arg: arg
                .as_ref()
                .map(|arg| gql_expr_to_graph_expr(arg, alias_kinds).map(Box::new))
                .transpose()?,
        },
        ExprKind::ExistsSubquery(_) => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "EXISTS subqueries are supported only in graph pipeline predicate execution"
                    .to_string(),
                expr.span.clone(),
            ));
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => GraphExpr::Case {
            operand: operand
                .as_ref()
                .map(|operand| gql_expr_to_graph_expr(operand, alias_kinds).map(Box::new))
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(GraphCaseBranch {
                        when: gql_expr_to_graph_expr(&branch.when, alias_kinds)?,
                        then: gql_expr_to_graph_expr(&branch.then, alias_kinds)?,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| gql_expr_to_graph_expr(else_expr, alias_kinds).map(Box::new))
                .transpose()?,
        },
        ExprKind::List(items) => GraphExpr::List(
            items
                .iter()
                .map(|item| gql_expr_to_graph_expr(item, alias_kinds))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ExprKind::Map(map) => GraphExpr::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        entry.key.name.clone(),
                        gql_expr_to_graph_expr(&entry.value, alias_kinds)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
    })
}

fn gql_expr_to_graph_expr_for_pipeline(
    expr: &Expr,
    alias_kinds: &BTreeMap<String, GqlAliasKind>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
) -> Result<GraphExpr, EngineError> {
    Ok(match &expr.kind {
        ExprKind::ExistsSubquery(pipeline) => {
            lower_exists_subquery_expr(pipeline, alias_kinds, params, options, subquery_depth)?
        }
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                if let Some(kind) = alias_kinds.get(alias).copied() {
                    return gql_alias_property_to_graph_expr(alias, kind, property);
                }
            }
            GraphExpr::Property {
                alias: gql_property_object_alias(object)?,
                key: property.name.clone(),
            }
        }
        ExprKind::Unary { op, expr } => GraphExpr::Unary {
            op: gql_unary_op_to_graph_op(*op),
            expr: Box::new(gql_expr_to_graph_expr_for_pipeline(
                expr,
                alias_kinds,
                params,
                options,
                subquery_depth,
            )?),
        },
        ExprKind::Binary { op, left, right } => GraphExpr::Binary {
            left: Box::new(gql_expr_to_graph_expr_for_pipeline(
                left,
                alias_kinds,
                params,
                options,
                subquery_depth,
            )?),
            op: gql_binary_op_to_graph_op(*op),
            right: Box::new(gql_expr_to_graph_expr_for_pipeline(
                right,
                alias_kinds,
                params,
                options,
                subquery_depth,
            )?),
        },
        ExprKind::IsNull { expr, negated } => {
            let inner = Box::new(gql_expr_to_graph_expr_for_pipeline(
                expr,
                alias_kinds,
                params,
                options,
                subquery_depth,
            )?);
            if *negated {
                GraphExpr::IsNotNull(inner)
            } else {
                GraphExpr::IsNull(inner)
            }
        }
        ExprKind::FunctionCall { name, args } => {
            if let Some(metadata_expr) = gql_metadata_call_to_graph_expr(name, args, alias_kinds)? {
                metadata_expr
            } else if name.name.eq_ignore_ascii_case("nodeids")
                || name.name.eq_ignore_ascii_case("edgeids")
            {
                if args.len() != 1 {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!("function '{}' expects exactly one argument", name.name),
                        name.span.clone(),
                    ));
                }
                let alias = variable_name(&args[0]).ok_or_else(|| {
                    gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!("function '{}' expects a path alias argument", name.name),
                        args[0].span.clone(),
                    )
                })?;
                GraphExpr::PathField {
                    alias: alias.to_string(),
                    field: if name.name.eq_ignore_ascii_case("nodeids") {
                        GraphPathField::NodeIds
                    } else {
                        GraphPathField::EdgeIds
                    },
                }
            } else {
                GraphExpr::Function {
                    name: gql_function_to_graph_function(&name.name, &name.span)?,
                    args: args
                        .iter()
                        .map(|arg| {
                            gql_expr_to_graph_expr_for_pipeline(
                                arg,
                                alias_kinds,
                                params,
                                options,
                                subquery_depth,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                }
            }
        }
        ExprKind::AggregateCall {
            function,
            distinct,
            arg,
            ..
        } => GraphExpr::AggregateCall {
            function: gql_aggregate_function_to_graph(*function),
            distinct: *distinct,
            arg: arg
                .as_ref()
                .map(|arg| {
                    gql_expr_to_graph_expr_for_pipeline(
                        arg,
                        alias_kinds,
                        params,
                        options,
                        subquery_depth,
                    )
                    .map(Box::new)
                })
                .transpose()?,
        },
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => GraphExpr::Case {
            operand: operand
                .as_ref()
                .map(|operand| {
                    gql_expr_to_graph_expr_for_pipeline(
                        operand,
                        alias_kinds,
                        params,
                        options,
                        subquery_depth,
                    )
                    .map(Box::new)
                })
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(GraphCaseBranch {
                        when: gql_expr_to_graph_expr_for_pipeline(
                            &branch.when,
                            alias_kinds,
                            params,
                            options,
                            subquery_depth,
                        )?,
                        then: gql_expr_to_graph_expr_for_pipeline(
                            &branch.then,
                            alias_kinds,
                            params,
                            options,
                            subquery_depth,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| {
                    gql_expr_to_graph_expr_for_pipeline(
                        else_expr,
                        alias_kinds,
                        params,
                        options,
                        subquery_depth,
                    )
                    .map(Box::new)
                })
                .transpose()?,
        },
        ExprKind::List(items) => GraphExpr::List(
            items
                .iter()
                .map(|item| {
                    gql_expr_to_graph_expr_for_pipeline(
                        item,
                        alias_kinds,
                        params,
                        options,
                        subquery_depth,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ExprKind::Map(map) => GraphExpr::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        entry.key.name.clone(),
                        gql_expr_to_graph_expr_for_pipeline(
                            &entry.value,
                            alias_kinds,
                            params,
                            options,
                            subquery_depth,
                        )?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => {
            gql_expr_to_graph_expr(expr, alias_kinds)?
        }
    })
}

fn lower_exists_subquery_expr(
    pipeline: &GqlReadPipeline,
    alias_kinds: &BTreeMap<String, GqlAliasKind>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    subquery_depth: usize,
) -> Result<GraphExpr, EngineError> {
    let next_depth = subquery_depth.saturating_add(1);
    if next_depth > options.max_subquery_depth {
        return Err(EngineError::InvalidOperation(format!(
            "GQL subquery depth {next_depth} exceeds max_subquery_depth {}",
            options.max_subquery_depth
        )));
    }
    let outer_aliases = alias_table_from_kinds(alias_kinds);
    let (bound, import_aliases, _) =
        bind_subquery_pipeline_for_outer_aliases(pipeline, &outer_aliases, params)?;
    let import_alias_kinds = import_aliases
        .iter()
        .filter_map(|alias| {
            alias_kinds
                .get(alias)
                .copied()
                .map(|kind| (alias.clone(), kind))
        })
        .collect::<BTreeMap<_, _>>();
    let lowered = lower_bound_read_pipeline_with_alias_kinds(
        &bound,
        params,
        options,
        next_depth,
        import_alias_kinds,
    )?;
    let mut stages = lowered.stages;
    inject_exists_internal_limit(&mut stages);
    Ok(GraphExpr::ExistsSubquery(GraphSubqueryStage {
        query: Box::new(GraphPipelineQuery {
            stages,
            params: gql_params_to_graph_params(params),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: 1,
                cursor: None,
            },
            output: GraphOutputOptions {
                mode: GraphOutputMode::Ids,
                compact_rows: false,
                include_vectors: false,
            },
            options: gql_pipeline_options(options),
        }),
        import_aliases,
    }))
}

fn inject_exists_internal_limit(stages: &mut [GraphPipelineStage]) {
    for stage in stages.iter_mut() {
        if let GraphPipelineStage::Union(union) = stage {
            for branch in &mut union.branches {
                inject_exists_internal_limit(&mut branch.stages);
            }
        }
    }
    if let Some(GraphPipelineStage::Project(project)) = stages.iter_mut().rev().find(|stage| {
        matches!(
            stage,
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                ..
            })
        )
    }) {
        if project.limit.is_none() {
            project.limit = Some(GraphExpr::UInt(1));
        }
    }
}

fn alias_table_from_kinds(alias_kinds: &BTreeMap<String, GqlAliasKind>) -> GqlAliasTable {
    let mut table = GqlAliasTable::default();
    for (name, kind) in alias_kinds {
        table.by_name.insert(
            name.clone(),
            GqlAliasBinding {
                name: name.clone(),
                kind: *kind,
                span: SourceSpan::new(0, 0, 1, 1),
                user_visible: true,
            },
        );
        table.user_order.push(name.clone());
    }
    table
}

fn gql_unary_op_to_graph_op(op: UnaryOp) -> GraphUnaryOp {
    match op {
        UnaryOp::Not => GraphUnaryOp::Not,
        UnaryOp::Neg => GraphUnaryOp::Neg,
    }
}

fn gql_property_object_alias(object: &Expr) -> Result<String, EngineError> {
    if let ExprKind::Variable(alias) = &object.kind {
        return Ok(alias.clone());
    }
    Err(gql_semantic_error(
        GqlSemanticErrorCode::InvalidPropertyAccess,
        "graph-row lowering supports property access only on bound aliases".to_string(),
        object.span.clone(),
    ))
}

fn gql_alias_property_to_graph_expr(
    alias: &str,
    kind: GqlAliasKind,
    property: &Ident,
) -> Result<GraphExpr, EngineError> {
    match kind {
        GqlAliasKind::Node | GqlAliasKind::Edge => Ok(GraphExpr::Property {
            alias: alias.to_string(),
            key: property.name.clone(),
        }),
        GqlAliasKind::Path => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidPropertyAccess,
            format!(
                "paths do not have properties; use length(p), nodeIds(p), or edgeIds(p) instead of '.{}'",
                property.name
            ),
            property.span.clone(),
        )),
        GqlAliasKind::Scalar => Ok(GraphExpr::Binding(alias.to_string())),
    }
}

/// Lowers a metadata function call over a bound node/edge alias to the corresponding
/// engine field expression. Returns None when the call is not a metadata access (the
/// caller falls through to general function lowering).
fn gql_metadata_call_to_graph_expr(
    name: &Ident,
    args: &[Expr],
    alias_kinds: &BTreeMap<String, GqlAliasKind>,
) -> Result<Option<GraphExpr>, EngineError> {
    if let Some((endpoint, endpoint_arg)) = edge_endpoint_id_call(name, args) {
        let alias = variable_name(endpoint_arg).expect("endpoint call shape checked");
        if alias_kinds.get(alias).copied() == Some(GqlAliasKind::Edge) {
            return Ok(Some(GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: match endpoint {
                    crate::gql::metadata::GqlEndpointFunction::StartNode => GraphEdgeField::From,
                    crate::gql::metadata::GqlEndpointFunction::EndNode => GraphEdgeField::To,
                },
            }));
        }
        return Ok(None);
    }
    let Some(metadata) = GqlMetadataFunction::from_lower(&name.name.to_ascii_lowercase()) else {
        return Ok(None);
    };
    if args.len() != 1 {
        return Ok(None);
    }
    let Some(alias) = variable_name(&args[0]) else {
        return Ok(None);
    };
    let Some(kind) = alias_kinds.get(alias).copied() else {
        return Ok(None);
    };
    let expr = match kind {
        GqlAliasKind::Node => {
            let field = match metadata {
                GqlMetadataFunction::Id => GraphNodeField::Id,
                GqlMetadataFunction::ElementKey => GraphNodeField::Key,
                GqlMetadataFunction::Weight => GraphNodeField::Weight,
                GqlMetadataFunction::CreatedAt => GraphNodeField::CreatedAt,
                GqlMetadataFunction::UpdatedAt => GraphNodeField::UpdatedAt,
                GqlMetadataFunction::ValidFrom | GqlMetadataFunction::ValidTo => return Ok(None),
            };
            GraphExpr::NodeField {
                alias: alias.to_string(),
                field,
            }
        }
        GqlAliasKind::Edge => {
            let field = match metadata {
                GqlMetadataFunction::Id => GraphEdgeField::Id,
                GqlMetadataFunction::Weight => GraphEdgeField::Weight,
                GqlMetadataFunction::CreatedAt => GraphEdgeField::CreatedAt,
                GqlMetadataFunction::UpdatedAt => GraphEdgeField::UpdatedAt,
                GqlMetadataFunction::ValidFrom => GraphEdgeField::ValidFrom,
                GqlMetadataFunction::ValidTo => GraphEdgeField::ValidTo,
                GqlMetadataFunction::ElementKey => return Ok(None),
            };
            GraphExpr::EdgeField {
                alias: alias.to_string(),
                field,
            }
        }
        GqlAliasKind::Path | GqlAliasKind::Scalar => return Ok(None),
    };
    Ok(Some(expr))
}

fn gql_literal_to_graph_expr(literal: &Literal) -> GraphExpr {
    match literal {
        Literal::Null => GraphExpr::Null,
        Literal::Bool(value) => GraphExpr::Bool(*value),
        Literal::Int(value) => GraphExpr::Int(*value),
        Literal::Float(value) => GraphExpr::Float(*value),
        Literal::String(value) => GraphExpr::String(value.clone()),
    }
}

fn gql_binary_op_to_graph_op(op: BinaryOp) -> GraphBinaryOp {
    match op {
        BinaryOp::Or => GraphBinaryOp::Or,
        BinaryOp::And => GraphBinaryOp::And,
        BinaryOp::Add => GraphBinaryOp::Add,
        BinaryOp::Sub => GraphBinaryOp::Sub,
        BinaryOp::Mul => GraphBinaryOp::Mul,
        BinaryOp::Div => GraphBinaryOp::Div,
        BinaryOp::Eq => GraphBinaryOp::Eq,
        BinaryOp::Neq => GraphBinaryOp::Neq,
        BinaryOp::Lt => GraphBinaryOp::Lt,
        BinaryOp::Le => GraphBinaryOp::Le,
        BinaryOp::Gt => GraphBinaryOp::Gt,
        BinaryOp::Ge => GraphBinaryOp::Ge,
        BinaryOp::In => GraphBinaryOp::In,
        BinaryOp::StartsWith => GraphBinaryOp::StartsWith,
        BinaryOp::EndsWith => GraphBinaryOp::EndsWith,
        BinaryOp::Contains => GraphBinaryOp::Contains,
    }
}

fn gql_function_to_graph_function(
    name: &str,
    span: &SourceSpan,
) -> Result<GraphFunction, EngineError> {
    match name.to_ascii_lowercase().as_str() {
        "id" => Ok(GraphFunction::Id),
        "labels" => Ok(GraphFunction::Labels),
        "type" => Ok(GraphFunction::Type),
        "length" => Ok(GraphFunction::Length),
        "startnode" => Ok(GraphFunction::StartNode),
        "endnode" => Ok(GraphFunction::EndNode),
        "nodes" => Ok(GraphFunction::Nodes),
        "relationships" => Ok(GraphFunction::Relationships),
        "coalesce" => Ok(GraphFunction::Coalesce),
        "tostring" => Ok(GraphFunction::ToString),
        "tointeger" => Ok(GraphFunction::ToInteger),
        "tofloat" => Ok(GraphFunction::ToFloat),
        "abs" => Ok(GraphFunction::Abs),
        "floor" => Ok(GraphFunction::Floor),
        "ceil" => Ok(GraphFunction::Ceil),
        "round" => Ok(GraphFunction::Round),
        "lower" => Ok(GraphFunction::Lower),
        "upper" => Ok(GraphFunction::Upper),
        "trim" => Ok(GraphFunction::Trim),
        "substring" => Ok(GraphFunction::Substring),
        "size" => Ok(GraphFunction::Size),
        "head" => Ok(GraphFunction::Head),
        "last" => Ok(GraphFunction::Last),
        _ => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "unsupported GQL scalar function".to_string(),
            span.clone(),
        )),
    }
}

fn gql_aggregate_function_to_graph(function: AggregateFunction) -> GraphAggregateFunction {
    match function {
        AggregateFunction::Count => GraphAggregateFunction::Count,
        AggregateFunction::Sum => GraphAggregateFunction::Sum,
        AggregateFunction::Avg => GraphAggregateFunction::Avg,
        AggregateFunction::Min => GraphAggregateFunction::Min,
        AggregateFunction::Max => GraphAggregateFunction::Max,
        AggregateFunction::Collect => GraphAggregateFunction::Collect,
    }
}

pub(crate) fn gql_order_direction_to_graph(direction: OrderDirection) -> GraphOrderDirection {
    match direction {
        OrderDirection::Asc => GraphOrderDirection::Asc,
        OrderDirection::Desc => GraphOrderDirection::Desc,
    }
}

fn node_key_pushdown_supported(label_filter: Option<&NodeLabelFilter>) -> bool {
    matches!(
        label_filter,
        Some(NodeLabelFilter {
            labels,
            mode: LabelMatchMode::All,
        }) if labels.len() == 1
    )
}

fn node_key_summary(alias: &str, keys: &[String]) -> String {
    match keys {
        [key] => format!("elementKey({alias}) = {key:?}"),
        _ => format!("elementKey({alias}) IN {:?}", keys),
    }
}

fn edge_id_summary(alias: &str, ids: &[u64]) -> String {
    match ids {
        [id] => format!("id({alias}) = {id}"),
        _ => format!("id({alias}) IN {:?}", ids),
    }
}

fn edge_endpoint_summary(alias: &str, field: EdgeEndpointField, ids: &[u64]) -> String {
    match ids {
        [id] => format!("{} = {id}", field.summary_expr(alias)),
        _ => format!("{} IN {:?}", field.summary_expr(alias), ids),
    }
}

fn merge_edge_label_filter(existing: &mut Vec<String>, labels: &[String]) -> bool {
    if labels.is_empty() {
        return false;
    }
    if existing.is_empty() {
        existing.extend(labels.iter().cloned());
        return true;
    }

    let incoming = labels.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let narrowed = existing
        .iter()
        .filter(|label| incoming.contains(label.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if narrowed.is_empty() {
        false
    } else {
        *existing = narrowed;
        true
    }
}

fn edge_explain_alias(alias: &str) -> &str {
    if alias == DIRECT_EDGE_ALIAS {
        "<anonymous relationship>"
    } else {
        alias
    }
}

fn node_label_filter(labels: &[Ident]) -> Option<NodeLabelFilter> {
    if labels.is_empty() {
        None
    } else {
        Some(NodeLabelFilter {
            labels: labels.iter().map(|label| label.name.clone()).collect(),
            mode: LabelMatchMode::All,
        })
    }
}

fn native_direction(direction: RelationshipDirection) -> Direction {
    match direction {
        RelationshipDirection::LeftToRight => Direction::Outgoing,
        RelationshipDirection::RightToLeft => Direction::Incoming,
        RelationshipDirection::Undirected => Direction::Both,
    }
}

fn combine_node_filters(filters: Vec<NodeFilterExpr>) -> Option<NodeFilterExpr> {
    match filters.len() {
        0 => None,
        1 => filters.into_iter().next(),
        _ => Some(NodeFilterExpr::And(filters)),
    }
}

fn combine_edge_filters(filters: Vec<EdgeFilterExpr>) -> Option<EdgeFilterExpr> {
    match filters.len() {
        0 => None,
        1 => filters.into_iter().next(),
        _ => Some(EdgeFilterExpr::And(filters)),
    }
}

fn merge_node_filter(
    existing: Option<NodeFilterExpr>,
    added: Option<NodeFilterExpr>,
) -> Option<NodeFilterExpr> {
    match (existing, added) {
        (None, filter) | (filter, None) => filter,
        (Some(left), Some(right)) => Some(NodeFilterExpr::And(vec![left, right])),
    }
}

fn merge_edge_filter(
    existing: Option<EdgeFilterExpr>,
    added: Option<EdgeFilterExpr>,
) -> Option<EdgeFilterExpr> {
    match (existing, added) {
        (None, filter) | (filter, None) => filter,
        (Some(left), Some(right)) => Some(EdgeFilterExpr::And(vec![left, right])),
    }
}

fn merge_graph_node_pattern(target: &mut GraphNodePattern, added: GraphNodePattern) {
    target.label_filter = merge_node_label_filter(target.label_filter.take(), added.label_filter);
    target.ids.extend(added.ids);
    target.keys.extend(added.keys);
    target.filter = merge_node_filter(target.filter.take(), added.filter);
}

fn merge_node_label_filter(
    existing: Option<NodeLabelFilter>,
    added: Option<NodeLabelFilter>,
) -> Option<NodeLabelFilter> {
    match (existing, added) {
        (None, filter) | (filter, None) => filter,
        (Some(mut left), Some(right)) => {
            left.labels.extend(right.labels);
            left.labels.sort();
            left.labels.dedup();
            left.mode = LabelMatchMode::All;
            Some(left)
        }
    }
}

fn graph_fixed_edge_indexes(pieces: &[GraphPatternPiece]) -> BTreeMap<String, usize> {
    pieces
        .iter()
        .enumerate()
        .filter_map(|(index, piece)| match piece {
            GraphPatternPiece::Edge(edge) => {
                edge.alias.as_ref().map(|alias| (alias.clone(), index))
            }
            GraphPatternPiece::Optional(_) | GraphPatternPiece::VariableLength(_) => None,
        })
        .collect()
}

fn graph_fixed_edges(pieces: &[GraphPatternPiece]) -> Vec<GraphEdgePattern> {
    pieces
        .iter()
        .filter_map(|piece| match piece {
            GraphPatternPiece::Edge(edge) => Some(edge.clone()),
            GraphPatternPiece::Optional(_) | GraphPatternPiece::VariableLength(_) => None,
        })
        .collect()
}

fn graph_fixed_edge_mut(
    pieces: &mut [GraphPatternPiece],
    index: usize,
) -> Option<&mut GraphEdgePattern> {
    match pieces.get_mut(index) {
        Some(GraphPatternPiece::Edge(edge)) => Some(edge),
        Some(GraphPatternPiece::Optional(_) | GraphPatternPiece::VariableLength(_)) | None => None,
    }
}

fn entity_value_ref(
    expr: &Expr,
    alias_kinds: &BTreeMap<String, GqlAliasKind>,
) -> Option<EntityValueRef> {
    match &expr.kind {
        ExprKind::PropertyAccess { object, property } => {
            let alias = variable_name(object)?.to_string();
            let kind = alias_kinds.get(&alias).copied().or_else(|| {
                if alias == DIRECT_EDGE_ALIAS {
                    Some(GqlAliasKind::Edge)
                } else if alias == DIRECT_NODE_ALIAS {
                    Some(GqlAliasKind::Node)
                } else {
                    None
                }
            })?;
            match kind {
                GqlAliasKind::Node => Some(EntityValueRef::NodeProperty {
                    alias,
                    key: property.name.clone(),
                }),
                GqlAliasKind::Edge => Some(EntityValueRef::EdgeProperty {
                    alias,
                    key: property.name.clone(),
                }),
                GqlAliasKind::Path | GqlAliasKind::Scalar => None,
            }
        }
        ExprKind::FunctionCall { name, args } => {
            if let Some((endpoint, endpoint_arg)) = edge_endpoint_id_call(name, args) {
                let alias = variable_name(endpoint_arg)?.to_string();
                let kind = alias_kinds.get(&alias).copied().or_else(|| {
                    (alias == DIRECT_EDGE_ALIAS).then_some(GqlAliasKind::Edge)
                })?;
                if kind != GqlAliasKind::Edge {
                    return None;
                }
                return Some(EntityValueRef::EdgeEndpoint {
                    alias,
                    field: match endpoint {
                        crate::gql::metadata::GqlEndpointFunction::StartNode => {
                            EdgeEndpointField::From
                        }
                        crate::gql::metadata::GqlEndpointFunction::EndNode => EdgeEndpointField::To,
                    },
                });
            }
            if args.len() != 1 {
                return None;
            }
            let alias = variable_name(&args[0])?.to_string();
            let kind = alias_kinds.get(&alias).copied().or_else(|| {
                if alias == DIRECT_EDGE_ALIAS {
                    Some(GqlAliasKind::Edge)
                } else if alias == DIRECT_NODE_ALIAS {
                    Some(GqlAliasKind::Node)
                } else {
                    None
                }
            });
            let lower = name.name.to_ascii_lowercase();
            if lower == "type" {
                return Some(EntityValueRef::RelationshipLabelFunction { alias });
            }
            if lower == "labels" {
                return (kind == Some(GqlAliasKind::Node)).then_some(EntityValueRef::NodeMetadata {
                    alias,
                    field: NodeMetadataField::Labels,
                });
            }
            let metadata = GqlMetadataFunction::from_lower(&lower)?;
            match (metadata, kind?) {
                (GqlMetadataFunction::Id, GqlAliasKind::Node) => {
                    Some(EntityValueRef::NodeId { alias })
                }
                (GqlMetadataFunction::Id, GqlAliasKind::Edge) => {
                    Some(EntityValueRef::EdgeId { alias })
                }
                (GqlMetadataFunction::ElementKey, GqlAliasKind::Node) => {
                    Some(EntityValueRef::NodeMetadata {
                        alias,
                        field: NodeMetadataField::Key,
                    })
                }
                (GqlMetadataFunction::Weight, GqlAliasKind::Node) => {
                    Some(EntityValueRef::NodeMetadata {
                        alias,
                        field: NodeMetadataField::Weight,
                    })
                }
                (GqlMetadataFunction::CreatedAt, GqlAliasKind::Node) => {
                    Some(EntityValueRef::NodeMetadata {
                        alias,
                        field: NodeMetadataField::CreatedAt,
                    })
                }
                (GqlMetadataFunction::UpdatedAt, GqlAliasKind::Node) => {
                    Some(EntityValueRef::NodeMetadata {
                        alias,
                        field: NodeMetadataField::UpdatedAt,
                    })
                }
                (GqlMetadataFunction::Weight, GqlAliasKind::Edge) => {
                    Some(EntityValueRef::EdgeMetadata {
                        alias,
                        field: EdgeMetadataField::Weight,
                    })
                }
                (GqlMetadataFunction::CreatedAt, GqlAliasKind::Edge) => {
                    Some(EntityValueRef::EdgeMetadata {
                        alias,
                        field: EdgeMetadataField::CreatedAt,
                    })
                }
                (GqlMetadataFunction::UpdatedAt, GqlAliasKind::Edge) => {
                    Some(EntityValueRef::EdgeMetadata {
                        alias,
                        field: EdgeMetadataField::UpdatedAt,
                    })
                }
                (GqlMetadataFunction::ValidFrom, GqlAliasKind::Edge) => {
                    Some(EntityValueRef::EdgeMetadata {
                        alias,
                        field: EdgeMetadataField::ValidFrom,
                    })
                }
                (GqlMetadataFunction::ValidTo, GqlAliasKind::Edge) => {
                    Some(EntityValueRef::EdgeMetadata {
                        alias,
                        field: EdgeMetadataField::ValidTo,
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn constant_prop_value(expr: &Expr, params: &GqlParams) -> Result<Option<PropValue>, EngineError> {
    match &expr.kind {
        ExprKind::Literal(literal) => Ok(Some(literal_to_prop_value(literal))),
        ExprKind::Parameter(name) => {
            let value = params.get(name).ok_or_else(|| EngineError::GqlParameter {
                name: name.clone(),
                expected: "GqlParamValue".to_string(),
                message: format!("missing parameter '${name}'"),
                span: expr.span.clone(),
            })?;
            Ok(Some(param_to_prop_value(value)))
        }
        ExprKind::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                let Some(value) = constant_prop_value(item, params)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(PropValue::Array(values)))
        }
        ExprKind::Map(map) => {
            let mut values = BTreeMap::new();
            for entry in &map.entries {
                let Some(value) = constant_prop_value(&entry.value, params)? else {
                    return Ok(None);
                };
                values.insert(entry.key.name.clone(), value);
            }
            Ok(Some(PropValue::Map(values)))
        }
        _ => Ok(None),
    }
}

fn constant_list_values(
    expr: &Expr,
    params: &GqlParams,
) -> Result<Option<Vec<PropValue>>, EngineError> {
    match &expr.kind {
        ExprKind::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                let Some(value) = constant_prop_value(item, params)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(values))
        }
        ExprKind::Parameter(name) => {
            let value = params.get(name).ok_or_else(|| EngineError::GqlParameter {
                name: name.clone(),
                expected: "list".to_string(),
                message: format!("missing parameter '${name}'"),
                span: expr.span.clone(),
            })?;
            match value {
                GqlParamValue::List(items) => Ok(Some(
                    items
                        .iter()
                        .map(param_to_prop_value)
                        .collect::<Vec<PropValue>>(),
                )),
                _ => Err(EngineError::GqlParameter {
                    name: name.clone(),
                    expected: "list".to_string(),
                    message: format!("parameter '${name}' must be a list for IN"),
                    span: expr.span.clone(),
                }),
            }
        }
        _ => Ok(None),
    }
}

fn literal_to_prop_value(literal: &Literal) -> PropValue {
    match literal {
        Literal::Null => PropValue::Null,
        Literal::Bool(value) => PropValue::Bool(*value),
        Literal::Int(value) => PropValue::Int(*value),
        Literal::Float(value) => PropValue::Float(*value),
        Literal::String(value) => PropValue::String(value.clone()),
    }
}

fn param_to_prop_value(value: &GqlParamValue) -> PropValue {
    match value {
        GqlParamValue::Null => PropValue::Null,
        GqlParamValue::Bool(value) => PropValue::Bool(*value),
        GqlParamValue::Int(value) => PropValue::Int(*value),
        GqlParamValue::UInt(value) => PropValue::UInt(*value),
        GqlParamValue::Float(value) => PropValue::Float(*value),
        GqlParamValue::String(value) => PropValue::String(value.clone()),
        GqlParamValue::Bytes(value) => PropValue::Bytes(value.clone()),
        GqlParamValue::List(values) => {
            PropValue::Array(values.iter().map(param_to_prop_value).collect())
        }
        GqlParamValue::Map(values) => PropValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), param_to_prop_value(value)))
                .collect(),
        ),
    }
}

/// Builds the residual predicate `metadataFn(alias) = value` for an element-map metadata
/// entry that could not be lowered to a native filter.
fn metadata_map_entry_predicate(
    alias: &str,
    metadata: GqlElementMapMetadataKey,
    entry: &MapEntry,
) -> Expr {
    let arg = Expr {
        kind: ExprKind::Variable(alias.to_string()),
        span: entry.key.span.clone(),
    };
    let left = Expr {
        kind: ExprKind::FunctionCall {
            name: Ident {
                name: metadata.canonical_name().to_string(),
                span: entry.key.span.clone(),
            },
            args: vec![arg],
        },
        span: entry.key.span.clone(),
    };
    Expr {
        kind: ExprKind::Binary {
            op: BinaryOp::Eq,
            left: Box::new(left),
            right: Box::new(entry.value.clone()),
        },
        span: entry.span.clone(),
    }
}

fn property_map_residual(alias: &str, entry: &MapEntry) -> Expr {
    let object = Expr {
        kind: ExprKind::Variable(alias.to_string()),
        span: entry.span.clone(),
    };
    let left = Expr {
        kind: ExprKind::PropertyAccess {
            object: Box::new(object),
            property: Ident {
                name: entry.key.name.clone(),
                span: entry.key.span.clone(),
            },
        },
        span: entry.key.span.clone(),
    };
    Expr {
        kind: ExprKind::Binary {
            op: BinaryOp::Eq,
            left: Box::new(left),
            right: Box::new(entry.value.clone()),
        },
        span: entry.span.clone(),
    }
}

fn reused_node_constraint_exprs(node: &GqlBoundNodePattern) -> Result<Vec<Expr>, EngineError> {
    let mut constraints = Vec::new();
    for label in &node.labels {
        constraints.push(node_label_membership_expr(&node.alias, label));
    }
    if let Some(properties) = node.properties.as_ref() {
        for entry in &properties.entries {
            match GqlElementMapMetadataKey::from_key(&entry.key.name) {
                Some(metadata) => {
                    if !metadata.valid_for_node() {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidPropertyAccess,
                            format!(
                                "element map metadata '{}' is valid only for relationships",
                                metadata.canonical_name()
                            ),
                            entry.key.span.clone(),
                        ));
                    }
                    constraints.push(metadata_map_entry_predicate(&node.alias, metadata, entry));
                }
                None => constraints.push(property_map_residual(&node.alias, entry)),
            }
        }
    }
    Ok(constraints)
}

fn node_label_membership_expr(alias: &str, label: &Ident) -> Expr {
    let left = Expr {
        kind: ExprKind::Literal(Literal::String(label.name.clone())),
        span: label.span.clone(),
    };
    let variable = Expr {
        kind: ExprKind::Variable(alias.to_string()),
        span: label.span.clone(),
    };
    let right = Expr {
        kind: ExprKind::FunctionCall {
            name: Ident {
                name: "labels".to_string(),
                span: label.span.clone(),
            },
            args: vec![variable],
        },
        span: label.span.clone(),
    };
    Expr {
        kind: ExprKind::Binary {
            op: BinaryOp::In,
            left: Box::new(left),
            right: Box::new(right),
        },
        span: label.span.clone(),
    }
}

fn combine_gql_predicates(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let mut combined = exprs.remove(0);
    for expr in exprs {
        let span = SourceSpan::new(
            combined.span.offset,
            expr.span
                .offset
                .saturating_add(expr.span.length)
                .saturating_sub(combined.span.offset),
            combined.span.line,
            combined.span.column,
        );
        combined = Expr {
            kind: ExprKind::Binary {
                op: BinaryOp::And,
                left: Box::new(combined),
                right: Box::new(expr),
            },
            span,
        };
    }
    Some(combined)
}

fn range_pushdown_compatible(reference: &EntityValueRef, value: &PropValue) -> bool {
    match reference {
        EntityValueRef::NodeProperty { .. } | EntityValueRef::EdgeProperty { .. } => {
            range_compatible(value)
        }
        EntityValueRef::NodeMetadata { field, .. } => node_metadata_value_compatible(*field, value),
        EntityValueRef::EdgeEndpoint { .. } => false,
        EntityValueRef::EdgeMetadata { field, .. } => edge_metadata_value_compatible(*field, value),
        EntityValueRef::NodeId { .. }
        | EntityValueRef::EdgeId { .. }
        | EntityValueRef::RelationshipLabelFunction { .. } => false,
    }
}

fn range_compatible(value: &PropValue) -> bool {
    match value {
        PropValue::Int(_) | PropValue::UInt(_) => true,
        PropValue::Float(value) => value.is_finite(),
        _ => false,
    }
}

fn range_bounds(
    op: BinaryOp,
    value: PropValue,
) -> (
    Option<PropertyRangeBound>,
    Option<PropertyRangeBound>,
    &'static str,
) {
    match op {
        BinaryOp::Gt => (
            Some(PropertyRangeBound::Excluded(value)),
            None,
            "> lower-bound",
        ),
        BinaryOp::Ge => (
            Some(PropertyRangeBound::Included(value)),
            None,
            ">= lower-bound",
        ),
        BinaryOp::Lt => (
            None,
            Some(PropertyRangeBound::Excluded(value)),
            "< upper-bound",
        ),
        BinaryOp::Le => (
            None,
            Some(PropertyRangeBound::Included(value)),
            "<= upper-bound",
        ),
        _ => unreachable!("range_bounds called for non-range operator"),
    }
}

fn range_op_text(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Gt => "> lower-bound",
        BinaryOp::Ge => ">= lower-bound",
        BinaryOp::Lt => "< upper-bound",
        BinaryOp::Le => "<= upper-bound",
        _ => unreachable!("range_op_text called for non-range operator"),
    }
}

fn reverse_range_op(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Le => Some(BinaryOp::Ge),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Ge => Some(BinaryOp::Le),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdValueMatch {
    Id(u64),
    Impossible,
    Residual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IdListMatch {
    Ids(Vec<u64>),
    Impossible,
    Residual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RawIdValueMatch {
    Id(u64),
    Impossible,
    Residual,
    Invalid,
}

fn false_node_push(alias: String) -> PushFilter {
    PushFilter::Node {
        alias,
        filter: NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(1),
            upper_ms: Some(0),
        },
    }
}

fn false_edge_push(alias: String) -> PushFilter {
    PushFilter::Edge {
        alias,
        filter: EdgeFilterExpr::UpdatedAtRange {
            lower_ms: Some(1),
            upper_ms: Some(0),
        },
    }
}

fn id_value_for_eq(
    value: &PropValue,
    value_expr: &Expr,
    noun: &str,
) -> Result<IdValueMatch, EngineError> {
    match raw_id_value_match(value) {
        RawIdValueMatch::Id(id) => Ok(IdValueMatch::Id(id)),
        RawIdValueMatch::Impossible => Ok(IdValueMatch::Impossible),
        RawIdValueMatch::Residual => Ok(IdValueMatch::Residual),
        RawIdValueMatch::Invalid => Err(parameter_or_semantic_type_error(
            parameter_name(value_expr).unwrap_or(""),
            &format!("{noun} must be a non-negative integer"),
            value_expr.span.clone(),
        )),
    }
}

fn id_values_for_in(values: &[PropValue]) -> IdListMatch {
    let mut ids = Vec::new();
    for value in values {
        match raw_id_value_match(value) {
            RawIdValueMatch::Id(id) => ids.push(id),
            RawIdValueMatch::Impossible => {}
            RawIdValueMatch::Residual | RawIdValueMatch::Invalid => return IdListMatch::Residual,
        }
    }
    if ids.is_empty() {
        IdListMatch::Impossible
    } else {
        IdListMatch::Ids(ids)
    }
}

fn raw_id_value_match(value: &PropValue) -> RawIdValueMatch {
    match value {
        PropValue::UInt(value) => RawIdValueMatch::Id(*value),
        PropValue::Int(value) if *value >= 0 => RawIdValueMatch::Id(*value as u64),
        PropValue::Int(_) => RawIdValueMatch::Impossible,
        PropValue::Float(value) if !value.is_finite() => RawIdValueMatch::Residual,
        PropValue::Float(value) if *value < 0.0 => RawIdValueMatch::Impossible,
        PropValue::Float(value) => match nonnegative_integral_f64_to_u128(*value) {
            Some(value) => match u64::try_from(value) {
                Ok(value) => RawIdValueMatch::Id(value),
                Err(_) => RawIdValueMatch::Impossible,
            },
            None => RawIdValueMatch::Impossible,
        },
        PropValue::Null => RawIdValueMatch::Residual,
        _ => RawIdValueMatch::Invalid,
    }
}

fn nonnegative_integral_f64_to_u128(value: f64) -> Option<u128> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    if value == 0.0 {
        return Some(0);
    }

    let bits = value.to_bits();
    let exp_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (significand, exponent) = if exp_bits == 0 {
        (fraction as u128, 1 - 1023 - 52)
    } else {
        (((1_u64 << 52) | fraction) as u128, exp_bits - 1023 - 52)
    };

    if exponent >= 0 {
        significand.checked_shl(exponent as u32)
    } else {
        let shift = (-exponent) as u32;
        if shift >= 128 {
            return None;
        }
        let divisor = 1_u128 << shift;
        (significand % divisor == 0).then_some(significand >> shift)
    }
}

fn parameter_name(expr: &Expr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Parameter(name) => Some(name.as_str()),
        _ => None,
    }
}

fn prop_value_to_key(value: &PropValue) -> Option<String> {
    match value {
        PropValue::String(key) => Some(key.clone()),
        _ => None,
    }
}

fn prop_values_to_keys(values: &[PropValue]) -> Option<Vec<String>> {
    let mut keys = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let key = prop_value_to_key(value)?;
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    (!keys.is_empty()).then_some(keys)
}

fn prop_value_to_label(value: &PropValue) -> Option<String> {
    match value {
        PropValue::String(label) if !label.is_empty() => Some(label.clone()),
        _ => None,
    }
}

fn prop_values_to_labels(values: &[PropValue]) -> Option<Vec<String>> {
    let mut labels = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let label = prop_value_to_label(value)?;
        if seen.insert(label.clone()) {
            labels.push(label);
        }
    }
    (!labels.is_empty()).then_some(labels)
}

fn prop_value_to_exact_f32(value: &PropValue) -> Option<f32> {
    match value {
        PropValue::Float(value) if value.is_finite() => {
            let narrowed = *value as f32;
            (f64::from(narrowed) == *value).then_some(narrowed)
        }
        PropValue::Int(value) => {
            let narrowed = *value as f32;
            ((narrowed as i64) == *value).then_some(narrowed)
        }
        PropValue::UInt(value) => {
            let narrowed = *value as f32;
            ((narrowed as u64) == *value).then_some(narrowed)
        }
        _ => None,
    }
}

fn prop_value_to_i64(value: &PropValue) -> Option<i64> {
    match value {
        PropValue::Int(value) => Some(*value),
        PropValue::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn node_metadata_value_compatible(field: NodeMetadataField, value: &PropValue) -> bool {
    match field {
        NodeMetadataField::UpdatedAt => prop_value_to_i64(value).is_some(),
        NodeMetadataField::Id
        | NodeMetadataField::Labels
        | NodeMetadataField::Key
        | NodeMetadataField::Weight
        | NodeMetadataField::CreatedAt => false,
    }
}

fn node_metadata_range_filter(
    field: NodeMetadataField,
    op: BinaryOp,
    value: &PropValue,
) -> Option<NodeFilterExpr> {
    match field {
        NodeMetadataField::UpdatedAt => {
            let value = prop_value_to_i64(value)?;
            let (lower_ms, upper_ms) = i64_range_bounds(op, value)?;
            Some(NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms })
        }
        NodeMetadataField::Id
        | NodeMetadataField::Labels
        | NodeMetadataField::Key
        | NodeMetadataField::Weight
        | NodeMetadataField::CreatedAt => None,
    }
}

fn edge_metadata_value_compatible(field: EdgeMetadataField, value: &PropValue) -> bool {
    match field {
        EdgeMetadataField::Weight => prop_value_to_exact_f32(value).is_some(),
        EdgeMetadataField::UpdatedAt
        | EdgeMetadataField::ValidFrom
        | EdgeMetadataField::ValidTo => prop_value_to_i64(value).is_some(),
        EdgeMetadataField::CreatedAt => false,
    }
}

fn edge_metadata_eq_filter(field: EdgeMetadataField, value: &PropValue) -> Option<EdgeFilterExpr> {
    match field {
        EdgeMetadataField::Weight => {
            let value = prop_value_to_exact_f32(value)?;
            Some(EdgeFilterExpr::WeightRange {
                lower: Some(value),
                upper: Some(value),
            })
        }
        EdgeMetadataField::UpdatedAt => {
            let value = prop_value_to_i64(value)?;
            Some(EdgeFilterExpr::UpdatedAtRange {
                lower_ms: Some(value),
                upper_ms: Some(value),
            })
        }
        EdgeMetadataField::ValidFrom => {
            let value = prop_value_to_i64(value)?;
            Some(EdgeFilterExpr::ValidFromRange {
                lower_ms: Some(value),
                upper_ms: Some(value),
            })
        }
        EdgeMetadataField::ValidTo => {
            let value = prop_value_to_i64(value)?;
            Some(EdgeFilterExpr::ValidToRange {
                lower_ms: Some(value),
                upper_ms: Some(value),
            })
        }
        EdgeMetadataField::CreatedAt => None,
    }
}

fn edge_metadata_range_filter(
    field: EdgeMetadataField,
    op: BinaryOp,
    value: &PropValue,
) -> Option<EdgeFilterExpr> {
    match field {
        EdgeMetadataField::Weight => {
            let value = prop_value_to_exact_f32(value)?;
            let (lower, upper) = f32_range_bounds(op, value)?;
            Some(EdgeFilterExpr::WeightRange { lower, upper })
        }
        EdgeMetadataField::UpdatedAt => {
            let value = prop_value_to_i64(value)?;
            let (lower_ms, upper_ms) = i64_range_bounds(op, value)?;
            Some(EdgeFilterExpr::UpdatedAtRange { lower_ms, upper_ms })
        }
        EdgeMetadataField::ValidFrom => {
            let value = prop_value_to_i64(value)?;
            let (lower_ms, upper_ms) = i64_range_bounds(op, value)?;
            Some(EdgeFilterExpr::ValidFromRange { lower_ms, upper_ms })
        }
        EdgeMetadataField::ValidTo => {
            let value = prop_value_to_i64(value)?;
            let (lower_ms, upper_ms) = i64_range_bounds(op, value)?;
            Some(EdgeFilterExpr::ValidToRange { lower_ms, upper_ms })
        }
        EdgeMetadataField::CreatedAt => None,
    }
}

fn f32_range_bounds(op: BinaryOp, value: f32) -> Option<(Option<f32>, Option<f32>)> {
    match op {
        BinaryOp::Gt => Some((Some(next_f32_up(value)?), None)),
        BinaryOp::Ge => Some((Some(value), None)),
        BinaryOp::Lt => Some((None, Some(next_f32_down(value)?))),
        BinaryOp::Le => Some((None, Some(value))),
        _ => None,
    }
}

fn i64_range_bounds(op: BinaryOp, value: i64) -> Option<(Option<i64>, Option<i64>)> {
    match op {
        BinaryOp::Gt => Some((Some(value.checked_add(1)?), None)),
        BinaryOp::Ge => Some((Some(value), None)),
        BinaryOp::Lt => Some((None, Some(value.checked_sub(1)?))),
        BinaryOp::Le => Some((None, Some(value))),
        _ => None,
    }
}

fn next_f32_up(value: f32) -> Option<f32> {
    if !value.is_finite() || value == f32::MAX {
        return None;
    }
    if value == 0.0 {
        return Some(f32::from_bits(1));
    }
    let bits = value.to_bits();
    Some(if value > 0.0 {
        f32::from_bits(bits + 1)
    } else {
        f32::from_bits(bits - 1)
    })
}

fn next_f32_down(value: f32) -> Option<f32> {
    if !value.is_finite() || value == -f32::MAX {
        return None;
    }
    if value == 0.0 {
        return Some(f32::from_bits(0x8000_0001));
    }
    let bits = value.to_bits();
    Some(if value > 0.0 {
        f32::from_bits(bits - 1)
    } else {
        f32::from_bits(bits + 1)
    })
}

fn pattern_has_anchor(nodes: &[GraphNodePattern], edges: &[GraphEdgePattern]) -> bool {
    nodes.iter().any(|node| {
        node.label_filter.is_some()
            || !node.ids.is_empty()
            || !node.keys.is_empty()
            || node
                .filter
                .as_ref()
                .is_some_and(node_filter_is_proven_false)
    }) || edges.iter().any(|edge| {
        !edge.label_filter.is_empty()
            || edge
                .filter
                .as_ref()
                .is_some_and(edge_filter_has_metadata_anchor)
    })
}

fn node_filter_is_proven_false(filter: &NodeFilterExpr) -> bool {
    match filter {
        NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(lower),
            upper_ms: Some(upper),
        } => lower > upper,
        NodeFilterExpr::And(children) => children.iter().any(node_filter_is_proven_false),
        NodeFilterExpr::Or(children) => {
            !children.is_empty() && children.iter().all(node_filter_is_proven_false)
        }
        NodeFilterExpr::Not(_)
        | NodeFilterExpr::IdRange { .. }
        | NodeFilterExpr::KeyEquals(_)
        | NodeFilterExpr::KeyIn(_)
        | NodeFilterExpr::PropertyEquals { .. }
        | NodeFilterExpr::PropertyIn { .. }
        | NodeFilterExpr::PropertyRange { .. }
        | NodeFilterExpr::PropertyExists { .. }
        | NodeFilterExpr::PropertyMissing { .. }
        | NodeFilterExpr::WeightRange { .. }
        | NodeFilterExpr::CreatedAtRange { .. }
        | NodeFilterExpr::UpdatedAtRange { .. } => false,
    }
}

fn edge_filter_has_metadata_anchor(filter: &EdgeFilterExpr) -> bool {
    match filter {
        EdgeFilterExpr::IdRange { .. }
        | EdgeFilterExpr::WeightRange { .. }
        | EdgeFilterExpr::UpdatedAtRange { .. }
        | EdgeFilterExpr::CreatedAtRange { .. }
        | EdgeFilterExpr::ValidAt { .. }
        | EdgeFilterExpr::ValidFromRange { .. }
        | EdgeFilterExpr::ValidToRange { .. } => true,
        EdgeFilterExpr::And(children) => children.iter().any(edge_filter_has_metadata_anchor),
        EdgeFilterExpr::Or(children) => {
            !children.is_empty() && children.iter().all(edge_filter_has_metadata_anchor)
        }
        EdgeFilterExpr::Not(_) => false,
        EdgeFilterExpr::PropertyEquals { .. }
        | EdgeFilterExpr::PropertyIn { .. }
        | EdgeFilterExpr::PropertyRange { .. }
        | EdgeFilterExpr::PropertyExists { .. }
        | EdgeFilterExpr::PropertyMissing { .. } => false,
    }
}

fn full_scan_not_allowed(span: SourceSpan, message: &str) -> EngineError {
    gql_semantic_error(
        GqlSemanticErrorCode::FullScanNotAllowed,
        message.to_string(),
        span,
    )
}

fn parameter_or_semantic_type_error(name: &str, message: &str, span: SourceSpan) -> EngineError {
    if name.is_empty() {
        gql_semantic_error(
            GqlSemanticErrorCode::ParameterTypeMismatch,
            message.to_string(),
            span,
        )
    } else {
        EngineError::GqlParameter {
            name: name.to_string(),
            expected: "compatible scalar".to_string(),
            message: message.to_string(),
            span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parser::{parse_query, parse_statement, GqlParseOptions};
    use crate::{DatabaseEngine, DbOptions};
    use tempfile::TempDir;

    fn params() -> GqlParams {
        BTreeMap::new()
    }

    fn params_with(name: &str, value: GqlParamValue) -> GqlParams {
        BTreeMap::from([(name.to_string(), value)])
    }

    fn parse(source: &str) -> GqlQuery {
        parse_query(source, &GqlParseOptions::default()).unwrap()
    }

    fn lower(source: &str) -> Result<GqlLoweredPlan, EngineError> {
        lower_query(parse(source), &params(), &GqlExecutionOptions::default())
    }

    fn lower_with_options(source: &str, options: GqlExecutionOptions) -> GqlLoweredPlan {
        lower_query(parse(source), &params(), &options).unwrap()
    }

    fn lower_result_with_options(
        source: &str,
        options: GqlExecutionOptions,
    ) -> Result<GqlLoweredPlan, EngineError> {
        lower_query(parse(source), &params(), &options)
    }

    fn lower_with_params(source: &str, params: GqlParams) -> Result<GqlLoweredPlan, EngineError> {
        lower_query(parse(source), &params, &GqlExecutionOptions::default())
    }

    fn lower_mut(source: &str) -> Result<GqlMutationPlan, EngineError> {
        let statement = parse_statement(source, &GqlParseOptions::default()).unwrap();
        let GqlStatementBody::Mutation(mutation) = statement.body else {
            panic!("expected mutation statement");
        };
        lower_mutation(mutation, &params(), &allow_full_scan())
    }

    fn allow_full_scan() -> GqlExecutionOptions {
        GqlExecutionOptions {
            allow_full_scan: true,
            ..GqlExecutionOptions::default()
        }
    }

    fn expect_semantic_code(err: EngineError, code: GqlSemanticErrorCode) {
        match err {
            EngineError::GqlSemantic { code: actual, .. } => assert_eq!(actual, code),
            other => panic!("expected semantic error {code:?}, got {other:?}"),
        }
    }

    fn node_filter_contains(filter: &Option<NodeFilterExpr>, expected: &NodeFilterExpr) -> bool {
        match filter {
            Some(actual) if actual == expected => true,
            Some(NodeFilterExpr::And(children)) | Some(NodeFilterExpr::Or(children)) => children
                .iter()
                .any(|child| node_filter_contains(&Some(child.clone()), expected)),
            Some(NodeFilterExpr::Not(child)) => {
                node_filter_contains(&Some(*child.clone()), expected)
            }
            _ => false,
        }
    }

    fn edge_filter_contains(filter: &Option<EdgeFilterExpr>, expected: &EdgeFilterExpr) -> bool {
        match filter {
            Some(actual) if actual == expected => true,
            Some(EdgeFilterExpr::And(children)) | Some(EdgeFilterExpr::Or(children)) => children
                .iter()
                .any(|child| edge_filter_contains(&Some(child.clone()), expected)),
            Some(EdgeFilterExpr::Not(child)) => {
                edge_filter_contains(&Some(*child.clone()), expected)
            }
            _ => false,
        }
    }

    fn graph_target(plan: &GqlLoweredPlan) -> &GraphRowQueryTarget {
        let GqlNativeTarget::GraphRows { query } = &plan.native_target else {
            panic!(
                "expected graph-row target, got {:?}",
                plan.native_target.kind()
            );
        };
        query
    }

    fn pipeline_target(plan: &GqlLoweredPlan) -> &GraphPipelineQuery {
        let GqlNativeTarget::GraphPipeline { query } = &plan.native_target else {
            panic!(
                "expected graph-pipeline target, got {:?}",
                plan.native_target.kind()
            );
        };
        query
    }

    fn graph_edge(query: &GraphRowQuery, index: usize) -> &GraphEdgePattern {
        match &query.pieces[index] {
            GraphPatternPiece::Edge(edge) => edge,
            other => panic!("expected fixed edge piece, got {other:?}"),
        }
    }

    fn graph_variable_length(query: &GraphRowQuery, index: usize) -> &GraphVariableLengthPattern {
        match &query.pieces[index] {
            GraphPatternPiece::VariableLength(path) => path,
            other => panic!("expected variable-length piece, got {other:?}"),
        }
    }

    #[test]
    fn lowers_create_only_mutation_without_read_prefix() {
        let plan = lower_mut("CREATE (n:Person {elementKey: 'ada', name: 'Ada'}) RETURN n").unwrap();
        assert!(plan.read_prefix.is_none());
        assert_eq!(plan.return_plan.as_ref().unwrap().columns, vec!["n"]);
        let [GqlMutationClausePlan::Create(patterns)] = plan.clauses.as_slice() else {
            panic!("expected CREATE plan");
        };
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].nodes[0].alias, "n");
        assert_eq!(patterns[0].nodes[0].labels, vec!["Person"]);
        // Metadata map entries split into dedicated plan fields; property_values holds
        // only user properties. Expr refs are assigned in map-entry order across both.
        assert_eq!(
            patterns[0].nodes[0].element_key.as_ref().map(|key| key.id),
            Some(0)
        );
        assert!(patterns[0].nodes[0].weight.is_none());
        assert_eq!(patterns[0].nodes[0].property_keys, vec!["name".to_string()]);
        assert_eq!(plan.operation_exprs.len(), 2);
        assert_eq!(patterns[0].nodes[0].property_values["name"].id, 1);
    }

    #[test]
    fn create_plans_split_metadata_map_entries_from_user_properties() {
        let plan = lower_mut(
            "CREATE (a:Person {elementKey: 'a', weight: 1.5, name: 'Ada'})-[r:R {since: 1, weight: 0.5, validFrom: 10, validTo: 20}]->(b:Person {elementKey: 'b'}) RETURN r",
        )
        .unwrap();
        let [GqlMutationClausePlan::Create(patterns)] = plan.clauses.as_slice() else {
            panic!("expected CREATE plan");
        };
        let node_a = &patterns[0].nodes[0];
        assert_eq!(node_a.element_key.as_ref().map(|key| key.id), Some(0));
        assert_eq!(node_a.weight.as_ref().map(|weight| weight.id), Some(1));
        assert_eq!(node_a.property_keys, vec!["name".to_string()]);
        assert_eq!(node_a.property_values["name"].id, 2);

        // Expr refs cover all node maps first, then edge maps, in map-entry order.
        let node_b = &patterns[0].nodes[1];
        assert_eq!(node_b.element_key.as_ref().map(|key| key.id), Some(3));

        let edge = &patterns[0].edges[0];
        assert_eq!(edge.property_keys, vec!["since".to_string()]);
        assert_eq!(edge.property_values["since"].id, 4);
        assert_eq!(edge.weight.as_ref().map(|weight| weight.id), Some(5));
        assert_eq!(edge.valid_from.as_ref().map(|from| from.id), Some(6));
        assert_eq!(edge.valid_to.as_ref().map(|to| to.id), Some(7));
        assert_eq!(plan.operation_exprs.len(), 8);
    }

    #[test]
    fn lowers_match_backed_mutation_read_prefix_to_graph_rows() {
        let plan = lower_mut(
            "MATCH (n:Person {elementKey: 'ada'}) OPTIONAL MATCH p = (n)-[r:KNOWS*1..1]->(m) SET n.name = m.name RETURN n, p",
        )
        .unwrap();
        let read = plan.read_prefix.as_ref().expect("read prefix should lower");
        assert!(matches!(
            read.internal_columns[0],
            GqlMutationInternalColumn::TargetId {
                ref alias,
                kind: GqlAliasKind::Node
            } if alias == "n"
        ));
        assert!(read.internal_columns.iter().any(|column| {
            matches!(column, GqlMutationInternalColumn::TargetPath { alias } if alias == "p")
        }));
        assert!(read
            .internal_columns
            .iter()
            .any(|column| { matches!(column, GqlMutationInternalColumn::ExprValue { .. }) }));
        assert_eq!(
            read.graph_row
                .as_ref()
                .unwrap()
                .query
                .return_items
                .as_ref()
                .unwrap()
                .len(),
            5
        );
        assert_eq!(plan.operation_exprs.len(), 1);
        let [GqlMutationClausePlan::Set(items)] = plan.clauses.as_slice() else {
            panic!("expected SET plan");
        };
        assert!(matches!(
            &items[0],
            GqlSetItemPlan::Property {
                alias,
                kind: GqlAliasKind::Node,
                property,
                value,
            } if alias == "n" && property == "name" && value.id == 0
        ));
    }

    #[test]
    fn lowers_keyed_node_merge_actions_and_expr_order() {
        let plan = lower_mut(
            "MERGE (n:Person {elementKey: 'ada'}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'seen' RETURN n",
        )
        .unwrap();
        assert!(plan.read_prefix.is_none());
        assert_eq!(plan.operation_exprs.len(), 3);
        let [GqlMutationClausePlan::Merge(merge)] = plan.clauses.as_slice() else {
            panic!("expected MERGE plan");
        };
        assert!(matches!(
            &merge.pattern,
            GqlMergePatternPlan::Node { alias, label, key }
                if alias == "n" && label == "Person" && key.id == 0
        ));
        assert!(matches!(
            &merge.on_create[0],
            GqlSetItemPlan::Property { alias, property, value, .. }
                if alias == "n" && property == "status" && value.id == 1
        ));
        assert!(matches!(
            &merge.on_match[0],
            GqlSetItemPlan::Property { alias, property, value, .. }
                if alias == "n" && property == "status" && value.id == 2
        ));

        let late = lower_mut(
            "MERGE (n:Person {elementKey: 'ada'}) ON MATCH SET n.count = coalesce(n.count, 0) + 1",
        )
        .unwrap();
        assert!(late.operation_exprs[1].late);
    }

    #[test]
    fn lowers_metadata_set_items_to_explicit_metadata_plans() {
        let plan = lower_mut(
            "MATCH (a)-[r:KNOWS]->(b) SET weight(r) = 0.5, validFrom(r) = 10, validTo(r) = 20, weight(a) = 1.5, r.weight = 7",
        )
        .unwrap();
        let [GqlMutationClausePlan::Set(items)] = plan.clauses.as_slice() else {
            panic!("expected SET plan");
        };
        assert!(matches!(
            &items[0],
            GqlSetItemPlan::Metadata {
                alias,
                kind: GqlAliasKind::Edge,
                field: GqlMetadataFunction::Weight,
                value,
            } if alias == "r" && value.id == 0
        ));
        assert!(matches!(
            &items[1],
            GqlSetItemPlan::Metadata {
                alias,
                kind: GqlAliasKind::Edge,
                field: GqlMetadataFunction::ValidFrom,
                value,
            } if alias == "r" && value.id == 1
        ));
        assert!(matches!(
            &items[2],
            GqlSetItemPlan::Metadata {
                alias,
                kind: GqlAliasKind::Edge,
                field: GqlMetadataFunction::ValidTo,
                value,
            } if alias == "r" && value.id == 2
        ));
        assert!(matches!(
            &items[3],
            GqlSetItemPlan::Metadata {
                alias,
                kind: GqlAliasKind::Node,
                field: GqlMetadataFunction::Weight,
                value,
            } if alias == "a" && value.id == 3
        ));
        // Dot SET on a metadata-sounding name is a plain user property plan.
        assert!(matches!(
            &items[4],
            GqlSetItemPlan::Property {
                alias,
                kind: GqlAliasKind::Edge,
                property,
                value,
            } if alias == "r" && property == "weight" && value.id == 4
        ));
    }

    #[test]
    fn lowers_relationship_merge_and_with_read_prefix_to_pipeline() {
        let relationship =
            lower_mut("MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS]->(b) RETURN r")
                .unwrap();
        let read = relationship
            .read_prefix
            .as_ref()
            .expect("relationship endpoints require read prefix");
        assert!(read.graph_row.is_some());
        let [GqlMutationClausePlan::Merge(merge)] = relationship.clauses.as_slice() else {
            panic!("expected relationship MERGE plan");
        };
        assert!(matches!(
            &merge.pattern,
            GqlMergePatternPlan::Relationship { alias, from_alias, to_alias, label }
                if alias == "r" && from_alias == "a" && to_alias == "b" && label == "KNOWS"
        ));

        let with_prefix =
            lower_mut("MATCH (s:Person) WITH s MERGE (n:GqlMergeWith {elementKey: s.key}) RETURN n")
                .unwrap();
        let read = with_prefix
            .read_prefix
            .as_ref()
            .expect("WITH prefix should lower");
        assert!(read.graph_row.is_none());
        assert!(matches!(
            read.lowered.native_target,
            GqlNativeTarget::GraphPipeline { .. }
        ));
    }

    #[test]
    fn rejects_duplicate_and_reserved_aliases() {
        let duplicate =
            lower_result_with_options("MATCH (n)-[n:KNOWS]->(m) RETURN n", allow_full_scan())
                .expect_err("duplicate alias should fail");
        expect_semantic_code(duplicate, GqlSemanticErrorCode::DuplicateAlias);

        let reserved =
            lower_result_with_options("MATCH (__node:Person) RETURN __node", allow_full_scan())
                .expect_err("reserved alias should fail");
        expect_semantic_code(reserved, GqlSemanticErrorCode::DuplicateAlias);

        let internal_prefix = lower_result_with_options(
            "MATCH (__gql_anon_node_0:Person) RETURN __gql_anon_node_0",
            allow_full_scan(),
        )
        .expect_err("internal alias prefix should fail");
        expect_semantic_code(internal_prefix, GqlSemanticErrorCode::DuplicateAlias);

        let reserved_return_alias = lower_result_with_options(
            "MATCH (n:Person) RETURN n AS __gql_output",
            allow_full_scan(),
        )
        .expect_err("reserved return alias should fail");
        expect_semantic_code(reserved_return_alias, GqlSemanticErrorCode::DuplicateAlias);
    }

    #[test]
    fn rejects_unknown_variables_in_where_return_and_order_by() {
        for source in [
            "MATCH (n:Person) WHERE x.name = 'Ada' RETURN n",
            "MATCH (n:Person) RETURN x",
            "MATCH (n:Person) RETURN n ORDER BY x",
            "MATCH (n:Person) RETURN n SKIP x",
            "MATCH (n:Person) RETURN n LIMIT x",
        ] {
            let err = lower(source).expect_err("unknown variable should fail");
            expect_semantic_code(err, GqlSemanticErrorCode::UnknownVariable);
        }
    }

    #[test]
    fn validates_missing_and_mismatched_parameters() {
        let missing = lower("MATCH (n:Person {elementKey: $key}) RETURN n")
            .expect_err("missing parameter should fail");
        assert!(matches!(
            missing,
            EngineError::GqlParameter { ref name, .. } if name == "key"
        ));

        let mismatch = lower_with_params(
            "MATCH (n:Person) WHERE n.status IN $status RETURN n",
            params_with("status", GqlParamValue::String("active".to_string())),
        )
        .expect_err("IN parameter must be a list");
        assert!(matches!(
            mismatch,
            EngineError::GqlParameter { ref name, expected, .. }
                if name == "status" && expected == "list"
        ));

        let bad_id = lower_with_params(
            "MATCH (n:Person) WHERE id(n) = $bad RETURN n",
            params_with("bad", GqlParamValue::String("not an id".to_string())),
        )
        .expect_err("bad id parameter should report the parameter span");
        assert!(matches!(
            bad_id,
            EngineError::GqlParameter { ref name, ref span, .. }
                if name == "bad" && span.offset > 0
        ));
    }

    #[test]
    fn keeps_unsafe_parameter_range_predicate_residual() {
        let plan = lower_with_params(
            "MATCH (n:Person) WHERE n.age > $age RETURN n",
            params_with("age", GqlParamValue::String("old".to_string())),
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        assert!(query.nodes[0].filter.is_none());
        assert_eq!(plan.pushed_down.len(), 0);
        assert_eq!(plan.residual_predicates.len(), 1);
    }

    #[test]
    fn unsupported_syntax_still_rejects_before_lowering() {
        for (source, feature) in [
            ("CREATE (n) RETURN n", "write clauses"),
            ("MATCH (n:$(label)) RETURN n", "dynamic labels"),
            (
                "MATCH (n)-[r:$(rel_label)]->(m) RETURN r",
                "dynamic relationship types",
            ),
        ] {
            let err = parse_query(source, &GqlParseOptions::default()).unwrap_err();
            match err {
                EngineError::GqlUnsupported {
                    feature: actual, ..
                } => assert_eq!(actual, feature),
                other => panic!("expected unsupported {feature}, got {other:?}"),
            }
        }
    }

    #[test]
    fn node_only_match_lowers_to_node_query_with_all_label_filter() {
        let plan = lower("MATCH (n:Person:Researcher) RETURN n").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        let query = &graph_target(&plan).query;
        assert_eq!(query.nodes[0].alias, "n");
        assert_eq!(
            query.nodes[0].label_filter,
            Some(NodeLabelFilter {
                labels: vec!["Person".to_string(), "Researcher".to_string()],
                mode: LabelMatchMode::All,
            })
        );
    }

    #[test]
    fn graph_row_target_inherits_gql_intermediate_cap_without_max_rows_inflation() {
        let plan = lower_with_options(
            "MATCH (n:Person) RETURN n",
            GqlExecutionOptions {
                max_rows: 100,
                max_intermediate_bindings: 3,
                max_frontier: 3,
                max_order_materialization: 3,
                ..GqlExecutionOptions::default()
            },
        );
        let options = &graph_target(&plan).query.options;
        assert_eq!(options.max_intermediate_bindings, 3);
        assert_eq!(options.max_frontier, 3);
        assert_eq!(options.max_order_materialization, 3);
        assert_eq!(options.max_page_limit, 3);
    }

    #[test]
    fn node_property_maps_and_where_predicates_push_down() {
        let plan = lower(
            "MATCH (n:Person {status: 'active'}) \
             WHERE n.age >= 18 AND n.score IN [1, 2] RETURN n",
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        assert!(node_filter_contains(
            &query.nodes[0].filter,
            &NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }
        ));
        assert!(node_filter_contains(
            &query.nodes[0].filter,
            &NodeFilterExpr::PropertyRange {
                key: "age".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(18))),
                upper: None,
            }
        ));
        assert!(node_filter_contains(
            &query.nodes[0].filter,
            &NodeFilterExpr::PropertyIn {
                key: "score".to_string(),
                values: vec![PropValue::Int(1), PropValue::Int(2)],
            }
        ));
        assert!(plan.residual_predicates.is_empty());
    }

    #[test]
    fn reused_alias_map_rejects_kind_invalid_metadata_key() {
        // Regression: a reused alias used to route edge-only metadata map keys into a
        // residual validFrom(n) predicate that failed later with a generic
        // unsupported-function error instead of the element-map kind error.
        let err = lower("MATCH (n:Person {elementKey: 'x'}) MATCH (n {validFrom: 1}) RETURN n")
            .expect_err("validFrom is edge-only metadata in element maps");
        match err {
            EngineError::GqlSemantic { code, message, .. } => {
                assert_eq!(code, GqlSemanticErrorCode::InvalidPropertyAccess);
                assert!(message.contains("valid only for relationships"), "{message}");
            }
            other => panic!("expected semantic error, got {other:?}"),
        }
    }

    #[test]
    fn with_pipeline_later_match_pushes_predicates_onto_carried_nodes() {
        let plan = lower(
            "MATCH (n:SeedSource) \
             WITH n \
             MATCH (n)-[:SEEDED_REL]->(m:SeedTarget) \
             WHERE n.status = 'active' \
             RETURN id(m) AS id",
        )
        .unwrap();
        let pipeline = pipeline_target(&plan);
        let GraphPipelineStage::Match(stage) = &pipeline.stages[2] else {
            panic!("expected later MATCH stage, got {:?}", pipeline.stages[2]);
        };
        let carried = stage
            .nodes
            .iter()
            .find(|node| node.alias == "n")
            .expect("carried node alias should be present in graph-row stage");
        assert!(node_filter_contains(
            &carried.filter,
            &NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }
        ));
        assert!(stage.where_.is_none());
        assert!(plan.residual_predicates.is_empty());
        assert!(plan.pushed_down.iter().any(|push| {
            push.alias == "n"
                && push.target_kind == GqlAliasKind::Node
                && push.summary.contains("n.status")
        }));
    }

    #[test]
    fn lowers_shortest_path_pipeline_stage_to_native_stage() {
        let plan = lower(
            "MATCH (a) WITH a MATCH (b) WITH a, b \
             MATCH p = allShortestPaths((a)-[:R*2..4]-(b)) \
             RETURN p",
        )
        .unwrap();
        let pipeline = pipeline_target(&plan);
        let GraphPipelineStage::ShortestPath(stage) = &pipeline.stages[4] else {
            panic!("expected shortest-path stage, got {:?}", pipeline.stages[4]);
        };
        assert!(!stage.optional);
        assert_eq!(stage.output_path_alias, "p");
        assert_eq!(stage.mode, GraphShortestPathMode::All);
        assert_eq!(
            stage.from,
            GraphShortestPathEndpoint::Alias("a".to_string())
        );
        assert_eq!(stage.to, GraphShortestPathEndpoint::Alias("b".to_string()));
        assert_eq!(stage.direction, Direction::Both);
        assert_eq!(stage.edge_label_filter, vec!["R"]);
        assert_eq!(stage.min_hops, 2);
        assert_eq!(stage.max_hops, 4);
        assert_eq!(stage.weight_field, None);
        assert_eq!(stage.max_cost, None);
    }

    #[test]
    fn union_pipeline_lowers_to_native_union_stage() {
        let plan = lower(
            "MATCH (n:UnionLower) RETURN n.name AS name \
             UNION ALL \
             MATCH (m:UnionLower) RETURN m.name AS name",
        )
        .unwrap();
        let pipeline = pipeline_target(&plan);
        assert_eq!(pipeline.stages.len(), 1);
        let GraphPipelineStage::Union(union) = &pipeline.stages[0] else {
            panic!("expected union stage, got {:?}", pipeline.stages[0]);
        };
        assert!(union.all);
        assert_eq!(union.branches.len(), 2);
        for branch in &union.branches {
            assert_eq!(branch.page.skip, 0);
            assert!(branch.page.cursor.is_none());
            assert!(matches!(
                branch.stages.last(),
                Some(GraphPipelineStage::Project(GraphProjectStage {
                    kind: GraphProjectKind::Return,
                    ..
                }))
            ));
        }
    }

    #[test]
    fn union_lowers_caps_and_rejects_mixed_modifiers() {
        let capped = lower_result_with_options(
            "MATCH (n:UnionLower) RETURN n.name AS name \
             UNION ALL MATCH (m:UnionLower) RETURN m.name AS name \
             UNION ALL MATCH (x:UnionLower) RETURN x.name AS name",
            GqlExecutionOptions {
                max_union_branches: 2,
                ..GqlExecutionOptions::default()
            },
        );
        assert!(matches!(
            capped,
            Err(EngineError::InvalidOperation(message)) if message.contains("max_union_branches")
        ));

        let mixed = lower(
            "MATCH (n:UnionLower) RETURN n.name AS name \
             UNION ALL MATCH (m:UnionLower) RETURN m.name AS name \
             UNION MATCH (x:UnionLower) RETURN x.name AS name",
        );
        assert!(
            matches!(mixed, Err(EngineError::GqlUnsupported { feature, .. }) if feature == "mixed UNION modifiers")
        );
    }

    #[test]
    fn distinct_and_aggregate_pipeline_shape_is_preserved_in_lowered_ir() {
        fn has_aggregate(
            expr: &GraphExpr,
            function: GraphAggregateFunction,
            distinct: bool,
            arg_present: bool,
        ) -> bool {
            match expr {
                GraphExpr::AggregateCall {
                    function: actual_function,
                    distinct: actual_distinct,
                    arg,
                } => {
                    *actual_function == function
                        && *actual_distinct == distinct
                        && arg.is_some() == arg_present
                }
                GraphExpr::ExistsSubquery(stage) => stage.query.stages.iter().any(|stage| {
                    graph_pipeline_stage_has_aggregate(stage, function, distinct, arg_present)
                }),
                GraphExpr::List(items) => items
                    .iter()
                    .any(|expr| has_aggregate(expr, function, distinct, arg_present)),
                GraphExpr::Map(items) => items
                    .values()
                    .any(|expr| has_aggregate(expr, function, distinct, arg_present)),
                GraphExpr::Function { args, .. } => args
                    .iter()
                    .any(|expr| has_aggregate(expr, function, distinct, arg_present)),
                GraphExpr::Unary { expr, .. }
                | GraphExpr::IsNull(expr)
                | GraphExpr::IsNotNull(expr) => {
                    has_aggregate(expr, function, distinct, arg_present)
                }
                GraphExpr::Binary { left, right, .. } => {
                    has_aggregate(left, function, distinct, arg_present)
                        || has_aggregate(right, function, distinct, arg_present)
                }
                GraphExpr::Case {
                    operand,
                    branches,
                    else_expr,
                } => {
                    operand
                        .as_deref()
                        .is_some_and(|expr| has_aggregate(expr, function, distinct, arg_present))
                        || branches.iter().any(|branch| {
                            has_aggregate(&branch.when, function, distinct, arg_present)
                                || has_aggregate(&branch.then, function, distinct, arg_present)
                        })
                        || else_expr.as_deref().is_some_and(|expr| {
                            has_aggregate(expr, function, distinct, arg_present)
                        })
                }
                GraphExpr::Null
                | GraphExpr::Bool(_)
                | GraphExpr::Int(_)
                | GraphExpr::UInt(_)
                | GraphExpr::Float(_)
                | GraphExpr::String(_)
                | GraphExpr::Bytes(_)
                | GraphExpr::Param(_)
                | GraphExpr::Binding(_)
                | GraphExpr::Property { .. }
                | GraphExpr::NodeField { .. }
                | GraphExpr::EdgeField { .. }
                | GraphExpr::PathField { .. } => false,
            }
        }

        fn graph_pipeline_stage_has_aggregate(
            stage: &GraphPipelineStage,
            function: GraphAggregateFunction,
            distinct: bool,
            arg_present: bool,
        ) -> bool {
            match stage {
                GraphPipelineStage::Match(stage) => stage
                    .where_
                    .as_ref()
                    .is_some_and(|expr| has_aggregate(expr, function, distinct, arg_present)),
                GraphPipelineStage::Project(stage) => {
                    let items = match &stage.items {
                        GraphProjectionItems::Star => false,
                        GraphProjectionItems::Items(items) => items
                            .iter()
                            .any(|item| has_aggregate(&item.expr, function, distinct, arg_present)),
                    };
                    items
                        || stage.where_.as_ref().is_some_and(|expr| {
                            has_aggregate(expr, function, distinct, arg_present)
                        })
                        || stage
                            .order_by
                            .iter()
                            .any(|item| has_aggregate(&item.expr, function, distinct, arg_present))
                        || stage.skip.as_ref().is_some_and(|expr| {
                            has_aggregate(expr, function, distinct, arg_present)
                        })
                        || stage.limit.as_ref().is_some_and(|expr| {
                            has_aggregate(expr, function, distinct, arg_present)
                        })
                }
                GraphPipelineStage::Call(stage) => stage.query.stages.iter().any(|stage| {
                    graph_pipeline_stage_has_aggregate(stage, function, distinct, arg_present)
                }),
                GraphPipelineStage::Union(stage) => stage.branches.iter().any(|branch| {
                    branch.stages.iter().any(|stage| {
                        graph_pipeline_stage_has_aggregate(stage, function, distinct, arg_present)
                    })
                }),
                GraphPipelineStage::ShortestPath(_) => false,
            }
        }

        let with = lower("MATCH (n:Person) WITH DISTINCT n.kind AS k RETURN k").unwrap();
        let with_pipeline = pipeline_target(&with);
        let GraphPipelineStage::Project(with_stage) = &with_pipeline.stages[1] else {
            panic!("expected WITH project stage");
        };
        assert_eq!(with_stage.kind, GraphProjectKind::With);
        assert!(with_stage.distinct);

        let return_distinct = lower(
            "MATCH (n:Person) RETURN DISTINCT n.kind AS k, count(*) + 1 AS total ORDER BY count(*) DESC",
        )
        .unwrap();
        let return_pipeline = pipeline_target(&return_distinct);
        let GraphPipelineStage::Project(return_stage) = &return_pipeline.stages[1] else {
            panic!("expected RETURN project stage");
        };
        assert_eq!(return_stage.kind, GraphProjectKind::Return);
        assert!(return_stage.distinct);
        let GraphProjectionItems::Items(items) = &return_stage.items else {
            panic!("expected explicit RETURN items");
        };
        assert!(has_aggregate(
            &items[1].expr,
            GraphAggregateFunction::Count,
            false,
            false
        ));
        assert!(has_aggregate(
            &return_stage.order_by[0].expr,
            GraphAggregateFunction::Count,
            false,
            false
        ));

        let collect = lower("MATCH (n:Person) RETURN collect(DISTINCT n.kind) AS kinds").unwrap();
        let collect_pipeline = pipeline_target(&collect);
        let GraphPipelineStage::Project(collect_stage) = &collect_pipeline.stages[1] else {
            panic!("expected collect RETURN stage");
        };
        let GraphProjectionItems::Items(items) = &collect_stage.items else {
            panic!("expected explicit collect items");
        };
        assert!(has_aggregate(
            &items[0].expr,
            GraphAggregateFunction::Collect,
            true,
            true
        ));
    }

    #[test]
    fn node_metadata_predicates_push_down_only_when_native_semantics_match() {
        let plan = lower(
            "MATCH (n:Person) \
             WHERE elementKey(n) = 'alice' AND updatedAt(n) >= 100 RETURN n",
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        assert_eq!(
            query.nodes[0].keys,
            vec![NodeKeyQuery {
                label: "Person".to_string(),
                key: "alice".to_string()
            }]
        );
        assert!(node_filter_contains(
            &query.nodes[0].filter,
            &NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(100),
                upper_ms: None,
            }
        ));
        assert!(plan.residual_predicates.is_empty());
        assert!(plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "elementKey(n) = \"alice\""));

        let residual_key = lower_result_with_options(
            "MATCH (n) WHERE elementKey(n) = 'alice' RETURN n",
            allow_full_scan(),
        )
        .unwrap();
        let query = &graph_target(&residual_key).query;
        assert!(query.nodes[0].keys.is_empty());
        assert_eq!(residual_key.residual_predicates.len(), 1);
        assert!(!residual_key
            .pushed_down
            .iter()
            .any(|push| push.summary.starts_with("elementKey(n)")));

        // Dot access on a metadata-sounding name is a plain property predicate now and
        // must NOT reach the native key constraint.
        let property_dot = lower("MATCH (n:Person) WHERE n.key = 'alice' RETURN n").unwrap();
        let query = &graph_target(&property_dot).query;
        assert!(query.nodes[0].keys.is_empty());
        assert!(node_filter_contains(
            &query.nodes[0].filter,
            &NodeFilterExpr::PropertyEquals {
                key: "key".to_string(),
                value: PropValue::String("alice".to_string()),
            }
        ));
    }

    #[test]
    fn direct_id_predicates_lower_to_native_id_constraints() {
        let node_plan = lower("MATCH (n) WHERE id(n) = 42 RETURN n").unwrap();
        let query = &graph_target(&node_plan).query;
        assert_eq!(query.nodes[0].ids, vec![42]);
        assert!(!query.options.allow_full_scan);

        let edge_plan = lower("MATCH ()-[r]->() WHERE id(r) IN [7, 9] RETURN r").unwrap();
        let target = graph_target(&edge_plan);
        assert_eq!(target.edge_id_constraints.get("r"), Some(&vec![7, 9]));
        assert!(!target.query.options.allow_full_scan);
        assert!(edge_plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "id(r) IN [7, 9]"));
    }

    #[test]
    fn pattern_edge_id_predicates_with_labels_become_candidate_constraints() {
        let plan = lower("MATCH (a)-[r:LIKES]->(b) WHERE id(r) = 7 RETURN r").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        assert_eq!(
            graph_target(&plan).edge_id_constraints.get("r"),
            Some(&vec![7])
        );
        assert!(plan.residual_predicates.is_empty());
        assert!(plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "id(r) = 7"));
    }

    #[test]
    fn null_sensitive_predicates_remain_residual() {
        let plan =
            lower("MATCH (n:Person) WHERE n.deleted IS NULL AND n.status = 'active' RETURN n")
                .unwrap();
        let query = &graph_target(&plan).query;
        assert!(node_filter_contains(
            &query.nodes[0].filter,
            &NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }
        ));
        assert_eq!(plan.residual_predicates.len(), 1);
    }

    #[test]
    fn pure_edge_shape_lowers_to_edge_query_when_legal() {
        let plan = lower("MATCH ()-[r:LIKES]->() RETURN r").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        let query = &graph_target(&plan).query;
        let edge = graph_edge(query, 0);
        assert_eq!(edge.alias.as_deref(), Some("r"));
        assert_eq!(edge.label_filter, vec!["LIKES".to_string()]);
    }

    #[test]
    fn anonymous_edge_without_binding_still_lowers_to_edge_query() {
        let plan = lower("MATCH ()-[:LIKES]->() RETURN 1").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        let query = &graph_target(&plan).query;
        let edge = graph_edge(query, 0);
        assert_eq!(edge.alias, None);
        assert_eq!(edge.label_filter, vec!["LIKES".to_string()]);
    }

    #[test]
    fn edge_property_maps_and_where_predicates_push_down() {
        let plan = lower(
            "MATCH ()-[r:LIKES {kind: 'post'}]->() \
             WHERE r.since >= 2020 RETURN r",
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        let edge = graph_edge(query, 0);
        assert!(edge_filter_contains(
            &edge.filter,
            &EdgeFilterExpr::PropertyEquals {
                key: "kind".to_string(),
                value: PropValue::String("post".to_string()),
            }
        ));
        assert!(edge_filter_contains(
            &edge.filter,
            &EdgeFilterExpr::PropertyRange {
                key: "since".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(2020))),
                upper: None,
            }
        ));
    }

    #[test]
    fn edge_metadata_and_type_predicates_push_down() {
        let weight_plan = lower("MATCH ()-[r]->() WHERE weight(r) > 0.5 RETURN r.since").unwrap();
        let query = &graph_target(&weight_plan).query;
        let edge = graph_edge(query, 0);
        assert!(edge_filter_contains(
            &edge.filter,
            &EdgeFilterExpr::WeightRange {
                lower: Some(next_f32_up(0.5).unwrap()),
                upper: None,
            }
        ));
        assert!(weight_plan.residual_predicates.is_empty());

        let label_plan = lower("MATCH ()-[r]->() WHERE type(r) = 'LIKES' RETURN r").unwrap();
        let query = &graph_target(&label_plan).query;
        assert_eq!(graph_edge(query, 0).label_filter, vec!["LIKES".to_string()]);

        let pure_multi_label =
            lower("MATCH ()-[r]->() WHERE type(r) IN ['LIKES', 'FOLLOWS'] RETURN r")
                .expect_err("pure anonymous edge label alternatives remain unsupported");
        match pure_multi_label {
            EngineError::GqlUnsupported {
                ref feature,
                ref message,
                ..
            } if feature == "edge label alternatives" => {
                assert!(message.contains("graph-row pure-edge"));
                assert!(!message.contains("EdgeQuery"));
            }
            err => panic!("unexpected error: {err}"),
        }

        let multi_label_plan =
            lower("MATCH (a)-[r]->(b) WHERE type(r) IN ['LIKES', 'FOLLOWS'] RETURN r").unwrap();
        assert_eq!(
            graph_edge(&graph_target(&multi_label_plan).query, 0).label_filter,
            vec!["LIKES".to_string(), "FOLLOWS".to_string()]
        );
    }

    #[test]
    fn direct_edge_endpoint_metadata_predicates_push_down_to_endpoint_ids() {
        let plan = lower(
            "MATCH ()-[r]->() WHERE id(startNode(r)) = 42 AND id(endNode(r)) IN [7, 8] RETURN r",
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        assert_eq!(query.nodes[0].ids, vec![42]);
        assert_eq!(query.nodes[1].ids, vec![7, 8]);
        assert!(graph_edge(query, 0).filter.is_none());
        assert!(plan.residual_predicates.is_empty());
        assert!(plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "id(startNode(r)) = 42"));
        assert!(plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "id(endNode(r)) IN [7, 8]"));
    }

    #[test]
    fn return_alias_order_by_preserves_edge_fast_path() {
        let plan = lower("MATCH ()-[r:LIKES]->() RETURN r.since AS s ORDER BY s").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
    }

    #[test]
    fn fixed_relationship_directions_lower_to_graph_row_query() {
        let directed = lower("MATCH (a)-[r:KNOWS]->(b) RETURN r").unwrap();
        let query = &graph_target(&directed).query;
        assert_eq!(graph_edge(query, 0).from_alias, "a");
        assert_eq!(graph_edge(query, 0).to_alias, "b");
        assert_eq!(graph_edge(query, 0).direction, Direction::Outgoing);

        let reverse = lower("MATCH (a)<-[r:KNOWS]-(b) RETURN r").unwrap();
        let query = &graph_target(&reverse).query;
        assert_eq!(graph_edge(query, 0).direction, Direction::Incoming);

        let undirected = lower("MATCH (a)-[r:KNOWS]-(b) RETURN r").unwrap();
        let query = &graph_target(&undirected).query;
        assert_eq!(graph_edge(query, 0).direction, Direction::Both);
    }

    #[test]
    fn pattern_metadata_pushdown_is_truthful_for_supported_endpoint_orientation() {
        let directed = lower(
            "MATCH (a:Person)-[r:KNOWS]->(b) \
             WHERE id(startNode(r)) = 42 AND updatedAt(b) < 100 RETURN r",
        )
        .unwrap();
        let query = &graph_target(&directed).query;
        assert_eq!(query.nodes[0].ids, vec![42]);
        assert!(node_filter_contains(
            &query.nodes[1].filter,
            &NodeFilterExpr::UpdatedAtRange {
                lower_ms: None,
                upper_ms: Some(99),
            }
        ));
        assert!(directed.residual_predicates.is_empty());

        let reverse =
            lower("MATCH (a)<-[r:KNOWS]-(b) WHERE id(startNode(r)) = 42 RETURN r").unwrap();
        let query = &graph_target(&reverse).query;
        assert!(query.nodes[0].ids.is_empty());
        assert_eq!(query.nodes[1].ids, vec![42]);

        let undirected =
            lower("MATCH (a)-[r:KNOWS]-(b) WHERE id(startNode(r)) = 42 RETURN r").unwrap();
        let query = &graph_target(&undirected).query;
        assert!(query.nodes.iter().all(|node| node.ids.is_empty()));
        assert_eq!(undirected.residual_predicates.len(), 1);
        assert!(!undirected
            .pushed_down
            .iter()
            .any(|push| push.summary.starts_with("id(startNode(r))")));
    }

    #[test]
    fn pattern_edge_id_predicates_become_graph_row_candidate_constraints() {
        let plan = lower("MATCH (a)-[r]->(b) WHERE id(r) IN [7, 8] RETURN id(r)").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        assert_eq!(
            graph_target(&plan).edge_id_constraints.get("r"),
            Some(&vec![7, 8])
        );
        assert!(plan.residual_predicates.is_empty());
        assert!(plan
            .pushed_down
            .iter()
            .any(|predicate| predicate.summary == "id(r) IN [7, 8]"));

        let repeated =
            lower("MATCH (a)-[r]->(b) WHERE id(r) = 7 AND id(r) = 8 RETURN id(r)").unwrap();
        assert_eq!(
            graph_target(&repeated).edge_id_constraints.get("r"),
            Some(&vec![7])
        );
        assert_eq!(repeated.residual_predicates.len(), 1);
    }

    #[test]
    fn chained_patterns_and_endpoint_aliases_force_graph_row_query() {
        let chained = lower("MATCH (a)-[r:KNOWS]->(b)-[s:LIKES]->(c) RETURN *").unwrap();
        let query = &graph_target(&chained).query;
        assert_eq!(query.nodes.len(), 3);
        assert_eq!(query.pieces.len(), 2);

        let endpoint_alias = lower("MATCH (a)-[r:LIKES]->() RETURN r").unwrap();
        assert_eq!(
            endpoint_alias.native_target.kind(),
            GqlNativeTargetKind::GraphRows
        );
    }

    #[test]
    fn fixed_multi_hop_path_assignment_lowers_to_fixed_path_composition() {
        let plan = lower(
            "MATCH p = (a)-[:R {kind: 'first'}]->(b)<-[s:S]-(c) \
             RETURN p, nodeIds(p), edgeIds(p), length(p)",
        )
        .unwrap();
        let target = graph_target(&plan);
        let query = &target.query;
        assert_eq!(query.pieces.len(), 2);
        assert!(matches!(query.pieces[0], GraphPatternPiece::Edge(_)));
        assert!(matches!(query.pieces[1], GraphPatternPiece::Edge(_)));
        assert_eq!(target.fixed_paths.len(), 1);
        let fixed_path = &target.fixed_paths[0];
        assert_eq!(fixed_path.scope, Vec::<usize>::new());
        assert_eq!(fixed_path.alias, "p");
        assert_eq!(
            fixed_path.node_aliases,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert_eq!(fixed_path.edge_piece_indices, vec![0, 1]);
        assert_eq!(fixed_path.after_piece_index, 1);
        assert_eq!(graph_edge(query, 1).direction, Direction::Incoming);
        assert!(matches!(
            query.return_items.as_ref().unwrap()[1].expr,
            GraphExpr::PathField {
                field: GraphPathField::NodeIds,
                ..
            }
        ));

        let mixed = lower("MATCH p = (a)-[:R]->(b)-[:S*1..2]->(c) RETURN p")
            .expect_err("mixed fixed/VLP path assignment should stay unsupported");
        match mixed {
            EngineError::GqlUnsupported { feature, .. } => {
                assert_eq!(feature, "path assignment");
            }
            other => panic!("expected unsupported path assignment, got {other:?}"),
        }
    }

    #[test]
    fn relationship_type_alternatives_use_graph_row_query() {
        let pure_edge = lower("MATCH ()-[r:A|B]->() RETURN r")
            .expect_err("pure anonymous edge label alternatives remain unsupported");
        match pure_edge {
            EngineError::GqlUnsupported {
                ref feature,
                ref message,
                ..
            } if feature == "edge label alternatives" => {
                assert!(message.contains("graph-row pure-edge"));
                assert!(!message.contains("EdgeQuery"));
            }
            err => panic!("unexpected error: {err}"),
        }

        let plan = lower("MATCH (a)-[r:A|B]->(b) RETURN r").unwrap();
        assert_eq!(
            graph_edge(&graph_target(&plan).query, 0).label_filter,
            vec!["A".to_string(), "B".to_string()]
        );
    }

    #[test]
    fn anonymous_nodes_are_internal_and_return_star_uses_user_order() {
        let plan = lower("MATCH (:Person)-[r:LIKES]->(:Post) RETURN *").unwrap();
        let query = &graph_target(&plan).query;
        assert_eq!(query.nodes[0].alias, "__gql_anon_node_0");
        assert_eq!(query.nodes[1].alias, "__gql_anon_node_1");
        let GqlReturnPlan::Star {
            expanded_aliases, ..
        } = &plan.semantic.returns
        else {
            panic!("expected RETURN *");
        };
        assert_eq!(expanded_aliases, &vec!["r".to_string()]);
    }

    #[test]
    fn anonymous_edge_constraints_have_no_user_visible_binding() {
        let plan = lower("MATCH (a)-[:LIKES {kind: 'post'}]->(b) RETURN *").unwrap();
        let query = &graph_target(&plan).query;
        let edge = graph_edge(query, 0);
        assert_eq!(edge.alias, None);
        assert!(edge_filter_contains(
            &edge.filter,
            &EdgeFilterExpr::PropertyEquals {
                key: "kind".to_string(),
                value: PropValue::String("post".to_string()),
            }
        ));
        assert!(plan
            .pushed_down
            .iter()
            .any(|predicate| predicate.summary.contains("<anonymous relationship>.kind")));
        assert!(plan
            .pushed_down
            .iter()
            .all(|predicate| !predicate.summary.contains(DIRECT_EDGE_ALIAS)));
        let GqlReturnPlan::Star {
            expanded_aliases, ..
        } = &plan.semantic.returns
        else {
            panic!("expected RETURN *");
        };
        assert_eq!(expanded_aliases, &vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn return_star_expansion_follows_semantic_binding_order() {
        let plan = lower("MATCH (a)-[r:KNOWS]->(b)-[s:LIKES]->(c) RETURN *").unwrap();
        let GqlReturnPlan::Star {
            expanded_aliases, ..
        } = &plan.semantic.returns
        else {
            panic!("expected RETURN *");
        };
        assert_eq!(
            expanded_aliases,
            &vec![
                "a".to_string(),
                "r".to_string(),
                "b".to_string(),
                "s".to_string(),
                "c".to_string()
            ]
        );
    }

    #[test]
    fn optional_vlp_and_path_aliases_lower_to_graph_row_ir() {
        let plan = lower(
            "MATCH (a:Person) \
             OPTIONAL MATCH p = (a)-[:KNOWS*0..2]->(b:Person) WHERE length(p) >= 1 \
             RETURN * ORDER BY nodeIds(p)",
        )
        .unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        let query = &graph_target(&plan).query;
        assert_eq!(query.nodes.len(), 2);
        assert_eq!(query.pieces.len(), 2);

        let required_anchor = graph_variable_length(query, 0);
        assert_eq!(required_anchor.path_alias, None);
        assert_eq!(required_anchor.from_alias, "a");
        assert_eq!(required_anchor.to_alias, "a");
        assert_eq!((required_anchor.min_hops, required_anchor.max_hops), (0, 0));

        let GraphPatternPiece::Optional(group) = &query.pieces[1] else {
            panic!("expected optional group");
        };
        assert!(group.where_.is_some());
        let [GraphPatternPiece::VariableLength(path)] = group.pieces.as_slice() else {
            panic!("expected optional VLP piece, got {:?}", group.pieces);
        };
        assert_eq!(path.path_alias.as_deref(), Some("p"));
        assert_eq!(path.edge_alias, None);
        assert_eq!(path.from_alias, "a");
        assert_eq!(path.to_alias, "b");
        assert_eq!(path.label_filter, vec!["KNOWS".to_string()]);
        assert_eq!((path.min_hops, path.max_hops), (0, 2));

        let GqlReturnPlan::Star {
            expanded_aliases, ..
        } = &plan.semantic.returns
        else {
            panic!("expected RETURN *");
        };
        assert_eq!(
            expanded_aliases,
            &vec!["a".to_string(), "p".to_string(), "b".to_string()]
        );
        assert!(matches!(
            &plan.order_by[0].expr.kind,
            ExprKind::FunctionCall { name, .. } if name.name == "nodeIds"
        ));
    }

    #[test]
    fn reused_node_constraints_stay_clause_local_for_optionals() {
        let plan = lower(
            "MATCH (a:Person) \
             OPTIONAL MATCH (a)-[:EMPLOYS]->(b:Company) \
             OPTIONAL MATCH (b:Person)-[:KNOWS]->(c) \
             RETURN id(b), id(c)",
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        let b = query
            .nodes
            .iter()
            .find(|node| node.alias == "b")
            .expect("b node should be present");
        assert_eq!(
            b.label_filter.as_ref().map(|filter| &filter.labels),
            Some(&vec!["Company".to_string()])
        );
        let GraphPatternPiece::Optional(second_optional) = &query.pieces[2] else {
            panic!("expected second optional group, got {:?}", query.pieces);
        };
        assert!(
            second_optional.where_.is_some(),
            "reused b:Person constraint should be optional-local"
        );
    }

    #[test]
    fn path_functions_lower_to_graph_row_expressions() {
        let plan = lower(
            "MATCH p = (a)-[:KNOWS*1..3]->(b) \
             WHERE length(p) > 0 \
             RETURN length(p) AS hops, startNode(p) AS first, endNode(p) AS last, \
                    nodes(p) AS ns, relationships(p) AS rs, nodeIds(p) AS node_ids, edgeIds(p) AS edge_ids \
             ORDER BY edgeIds(p)",
        )
        .unwrap();
        let query = &graph_target(&plan).query;
        let path = graph_variable_length(query, 0);
        assert_eq!(path.path_alias.as_deref(), Some("p"));
        assert_eq!((path.min_hops, path.max_hops), (1, 3));
        assert!(matches!(
            query.where_.as_ref(),
            Some(GraphExpr::Binary { left, .. })
                if matches!(left.as_ref(), GraphExpr::Function { name: GraphFunction::Length, .. })
        ));
        let items = query.return_items.as_ref().unwrap();
        assert!(matches!(
            items[0].expr,
            GraphExpr::Function {
                name: GraphFunction::Length,
                ..
            }
        ));
        assert!(matches!(
            items[1].expr,
            GraphExpr::Function {
                name: GraphFunction::StartNode,
                ..
            }
        ));
        assert!(matches!(
            items[2].expr,
            GraphExpr::Function {
                name: GraphFunction::EndNode,
                ..
            }
        ));
        assert!(matches!(
            items[3].expr,
            GraphExpr::Function {
                name: GraphFunction::Nodes,
                ..
            }
        ));
        assert!(matches!(
            items[4].expr,
            GraphExpr::Function {
                name: GraphFunction::Relationships,
                ..
            }
        ));
        assert!(matches!(
            items[5].expr,
            GraphExpr::PathField {
                field: GraphPathField::NodeIds,
                ..
            }
        ));
        assert!(matches!(
            items[6].expr,
            GraphExpr::PathField {
                field: GraphPathField::EdgeIds,
                ..
            }
        ));
        assert!(matches!(
            &plan.order_by[0].expr.kind,
            ExprKind::FunctionCall { name, .. } if name.name == "edgeIds"
        ));
    }

    #[test]
    fn path_semantic_errors_keep_structured_spans() {
        let multi_hop_edge_alias = lower("MATCH p = (a)-[r:KNOWS*1..3]->(b) RETURN p")
            .expect_err("multi-hop edge alias should fail");
        match multi_hop_edge_alias {
            EngineError::GqlUnsupported { feature, span, .. } => {
                assert_eq!(feature, "multi-hop relationship-list aliases");
                assert!(span.length > 0);
            }
            other => panic!("expected unsupported relationship-list alias, got {other:?}"),
        }

        let wrong_kind = lower("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN length(a)")
            .expect_err("path function on node should fail");
        expect_semantic_code(wrong_kind, GqlSemanticErrorCode::InvalidReturnExpression);

        let wrong_arity = lower("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN length(p, p)")
            .expect_err("path function arity should fail");
        expect_semantic_code(wrong_arity, GqlSemanticErrorCode::InvalidReturnExpression);

        let path_id = lower("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN id(p)")
            .expect_err("id on path should fail");
        expect_semantic_code(path_id, GqlSemanticErrorCode::InvalidReturnExpression);
    }

    #[test]
    fn full_scan_rejection_and_allowance_are_explicit() {
        let lowered = lower("MATCH (n) RETURN n").unwrap();
        assert!(!graph_target(&lowered).query.options.allow_full_scan);

        let allowed = lower_with_options("MATCH (n) RETURN n", allow_full_scan());
        assert!(graph_target(&allowed).query.options.allow_full_scan);
    }

    #[test]
    fn lowerer_does_not_reserve_unknown_catalog_labels() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("gql_catalog_db");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(engine.list_node_labels().unwrap().is_empty());
        assert!(engine.list_edge_labels().unwrap().is_empty());

        let plan = lower("MATCH (a:Missing)-[r:MISSING]->(b:Other) RETURN *").unwrap();
        assert_eq!(plan.native_target.kind(), GqlNativeTargetKind::GraphRows);
        assert_eq!(engine.get_node_label_id("Missing").unwrap(), None);
        assert_eq!(engine.get_node_label_id("Other").unwrap(), None);
        assert_eq!(engine.get_edge_label_id("MISSING").unwrap(), None);
        assert!(engine.list_node_labels().unwrap().is_empty());
        assert!(engine.list_edge_labels().unwrap().is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn large_boolean_predicate_lowers_without_quadratic_behavior() {
        let mut where_clause = String::new();
        for i in 0..100 {
            if i > 0 {
                where_clause.push_str(" AND ");
            }
            where_clause.push_str(&format!("n.p{} = {}", i, i));
        }
        let source = format!("MATCH (n:Person) WHERE {where_clause} RETURN n");
        let plan = lower(&source).unwrap();
        assert_eq!(plan.pushed_down.len(), 100);
        assert!(plan.residual_predicates.is_empty());
        let query = &graph_target(&plan).query;
        assert!(matches!(
            query.nodes[0].filter,
            Some(NodeFilterExpr::And(_))
        ));
    }

    #[test]
    fn large_relationship_label_in_list_dedupes_without_quadratic_lookup() {
        let labels = (0..128)
            .flat_map(|idx| [format!("'REL_{idx}'"), format!("'REL_{idx}'")])
            .collect::<Vec<_>>()
            .join(", ");
        let source = format!("MATCH (a)-[r]->(b) WHERE type(r) IN [{labels}] RETURN r");
        let plan = lower(&source).unwrap();
        let edge = graph_edge(&graph_target(&plan).query, 0);
        assert_eq!(edge.label_filter.len(), 128);
        assert_eq!(edge.label_filter[0], "REL_0");
        assert_eq!(edge.label_filter[127], "REL_127");
    }

    #[test]
    fn large_chained_pattern_predicates_use_alias_indexes() {
        let mut pattern = "MATCH (n0:L0)".to_string();
        for idx in 0..48 {
            pattern.push_str(&format!("-[r{idx}:REL{idx}]->(n{}:L{})", idx + 1, idx + 1));
        }

        let mut predicates = Vec::new();
        for idx in 0..49 {
            predicates.push(format!("n{idx}.p = {idx}"));
        }
        for idx in 0..48 {
            predicates.push(format!("r{idx}.score = {idx}"));
        }

        let source = format!("{pattern} WHERE {} RETURN *", predicates.join(" AND "));
        let plan = lower(&source).unwrap();
        assert_eq!(plan.pushed_down.len(), 97);
        assert!(plan.residual_predicates.is_empty());
        let query = &graph_target(&plan).query;
        assert_eq!(query.nodes.len(), 49);
        assert_eq!(query.pieces.len(), 48);
        assert!(node_filter_contains(
            &query.nodes[48].filter,
            &NodeFilterExpr::PropertyEquals {
                key: "p".to_string(),
                value: PropValue::Int(48),
            }
        ));
        assert!(edge_filter_contains(
            &graph_edge(query, 47).filter,
            &EdgeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Int(47),
            }
        ));
    }

    #[test]
    fn relationship_label_filter_intersection_preserves_existing_order() {
        const PATTERN_LABEL_COUNT: usize = 128;
        const OVERLAP_START: usize = 34;
        const INCOMING_LABEL_END: usize = 160;
        let pattern_labels = (0..PATTERN_LABEL_COUNT)
            .map(|idx| format!("REL_{idx}"))
            .collect::<Vec<_>>()
            .join("|");
        let incoming_labels = (OVERLAP_START..INCOMING_LABEL_END)
            .rev()
            .flat_map(|idx| [format!("'REL_{idx}'"), format!("'REL_{idx}'")])
            .collect::<Vec<_>>()
            .join(", ");
        let source = format!(
            "MATCH (a)-[r:{pattern_labels}]->(b) WHERE type(r) IN [{incoming_labels}] RETURN r"
        );
        let plan = lower(&source).unwrap();
        let edge = graph_edge(&graph_target(&plan).query, 0);
        let expected_len = PATTERN_LABEL_COUNT - OVERLAP_START;
        assert_eq!(edge.label_filter.len(), expected_len);
        assert_eq!(edge.label_filter[0], format!("REL_{OVERLAP_START}"));
        assert_eq!(
            edge.label_filter[expected_len - 1],
            format!("REL_{}", PATTERN_LABEL_COUNT - 1)
        );
    }
}
