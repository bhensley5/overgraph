#![allow(dead_code)]

use crate::engine::GraphFixedPathBinding;
use crate::error::EngineError;
use crate::gql::ast::*;
use crate::gql::params::{validate_referenced_gql_mutation_params, validate_referenced_gql_params};
use crate::gql::semantic::{
    bind_mutation, bind_query, expression_output_name, gql_semantic_error, variable_name,
    GqlAliasKind, GqlAliasOrigin, GqlBoundCreateEdge, GqlBoundCreateNode, GqlBoundEdgePattern,
    GqlBoundMutationClause, GqlBoundNodePattern, GqlBoundPattern, GqlBoundRemoveItem,
    GqlBoundSetItem, GqlMutationSemanticPlan, GqlReturnPlan, GqlSemanticPlan,
};
use crate::row_projection::{DIRECT_EDGE_ALIAS, DIRECT_NODE_ALIAS};
use crate::types::{
    Direction, EdgeFilterExpr, GqlExecutionOptions, GqlParamValue, GqlParams, GqlSemanticErrorCode,
    GraphBinaryOp, GraphEdgeField, GraphEdgePattern, GraphElementProjection, GraphExpr,
    GraphFunction, GraphNodeField, GraphNodePattern, GraphOptionalGroup, GraphOrderDirection,
    GraphOutputMode, GraphOutputOptions, GraphPageRequest, GraphParamValue, GraphPathField,
    GraphPatternPiece, GraphQueryOptions, GraphReturnItem, GraphReturnProjection, GraphRowQuery,
    GraphVariableLengthPattern, LabelMatchMode, NodeFilterExpr, NodeKeyQuery, NodeLabelFilter,
    PropValue, PropertyRangeBound, SourceSpan,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlNativeTargetKind {
    GraphRows,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlNativeTarget {
    GraphRows { query: GraphRowQueryTarget },
}

impl GqlNativeTarget {
    pub(crate) fn kind(&self) -> GqlNativeTargetKind {
        match self {
            Self::GraphRows { .. } => GqlNativeTargetKind::GraphRows,
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
    pub(crate) graph_row: GraphRowQueryTarget,
    pub(crate) lowered: Box<GqlLoweredPlan>,
    pub(crate) internal_columns: Vec<GqlMutationInternalColumn>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlMutationInternalColumn {
    TargetId { alias: String, kind: GqlAliasKind },
    TargetPath { alias: String },
    ExprValue { id: usize, expr: GraphExpr },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationExprPlan {
    pub(crate) id: usize,
    pub(crate) expr: GraphExpr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlMutationExprRef {
    pub(crate) id: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlMutationClausePlan {
    Create(Vec<GqlCreatePatternPlan>),
    Set(Vec<GqlSetItemPlan>),
    Remove(Vec<GqlRemoveItemPlan>),
    Delete {
        detach: bool,
        targets: Vec<GqlDeleteTargetPlan>,
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
        reject_unsupported_pure_edge_label_or(&semantic, pattern, params)?;
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
        .map(|(id, expr)| {
            Ok(GqlMutationExprPlan {
                id,
                expr: gql_expr_to_graph_expr(expr, &alias_kinds)?,
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
    operation_exprs: &[Expr],
    internal_columns: Vec<GqlMutationInternalColumn>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<Option<GqlMutationReadPrefixPlan>, EngineError> {
    if semantic.statement.read_prefix.is_empty() {
        return Ok(None);
    }
    let read_query = mutation_read_prefix_query(semantic, operation_exprs, &internal_columns);
    let read_semantic = bind_query(read_query, params)?;
    validate_referenced_gql_params(&read_semantic, params, options)?;
    let mut read_options = options.clone();
    read_options.cursor = None;
    read_options.max_rows = options.max_mutation_rows.saturating_add(1).max(1);
    let lowered = lower_semantic_plan(read_semantic, params, &read_options)?;
    let GqlNativeTarget::GraphRows { query } = &lowered.native_target;
    Ok(Some(GqlMutationReadPrefixPlan {
        graph_row: query.clone(),
        lowered: Box::new(lowered),
        internal_columns,
    }))
}

fn mutation_read_prefix_query(
    semantic: &GqlMutationSemanticPlan,
    operation_exprs: &[Expr],
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
                    path_function_expr("node_ids", alias, &binding.span),
                    format!("_gql_mut_path_nodes_{alias}"),
                    &binding.span,
                ));
                items.push(internal_return_item(
                    path_function_expr("edge_ids", alias, &binding.span),
                    format!("_gql_mut_path_edges_{alias}"),
                    &binding.span,
                ));
            }
            GqlMutationInternalColumn::ExprValue { id, .. } => {
                let expr = &operation_exprs[*id];
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
    GqlQuery {
        match_clauses: semantic.statement.read_prefix.clone(),
        return_clause: ReturnClause {
            body: ReturnBody::Items(items),
            span: semantic.statement.span.clone(),
        },
        order_by: Vec::new(),
        skip: None,
        limit: None,
        span: semantic.statement.span.clone(),
    }
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
    operation_exprs: &[Expr],
    lowered_exprs: &[GqlMutationExprPlan],
) -> Vec<GqlMutationInternalColumn> {
    let mut required_aliases = BTreeSet::new();
    collect_mutation_target_aliases(semantic, &mut required_aliases);
    collect_return_identity_aliases(semantic, &mut required_aliases);

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
        }
    }

    for (id, expr) in operation_exprs.iter().enumerate() {
        if expr_references_read_prefix_alias(expr, semantic) {
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
            GqlBoundMutationClause::Set(set) => {
                for item in &set.items {
                    match item {
                        GqlBoundSetItem::Property { alias, .. }
                        | GqlBoundSetItem::MapMerge { alias, .. }
                        | GqlBoundSetItem::NodeLabel { alias, .. } => {
                            maybe_insert_read_prefix_alias(semantic, alias, required_aliases);
                        }
                    }
                }
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
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_read_prefix_aliases_from_expr(semantic, &entry.value, aliases);
            }
        }
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

fn mutation_operation_exprs(semantic: &GqlMutationSemanticPlan) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for clause in &semantic.clauses {
        match clause {
            GqlBoundMutationClause::Create(create) => {
                for pattern in &create.patterns {
                    for node in &pattern.nodes {
                        collect_map_value_exprs(node.properties.as_ref(), &mut exprs);
                    }
                    for edge in &pattern.edges {
                        collect_map_value_exprs(edge.properties.as_ref(), &mut exprs);
                    }
                }
            }
            GqlBoundMutationClause::Set(set) => {
                for item in &set.items {
                    match item {
                        GqlBoundSetItem::Property { value, .. }
                        | GqlBoundSetItem::MapMerge { value, .. } => exprs.push(value.clone()),
                        GqlBoundSetItem::NodeLabel { .. } => {}
                    }
                }
            }
            GqlBoundMutationClause::Remove(_) | GqlBoundMutationClause::Delete(_) => {}
        }
    }
    exprs
}

fn collect_map_value_exprs(map: Option<&MapLiteral>, exprs: &mut Vec<Expr>) {
    if let Some(map) = map {
        exprs.extend(map.entries.iter().map(|entry| entry.value.clone()));
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

fn lower_create_node(node: &GqlBoundCreateNode, expr_cursor: &mut usize) -> GqlCreateNodePlan {
    GqlCreateNodePlan {
        alias: node.alias.clone(),
        labels: node.labels.iter().map(|label| label.name.clone()).collect(),
        property_keys: map_property_keys(node.properties.as_ref()),
        property_values: map_property_values(node.properties.as_ref(), expr_cursor),
        created: node.created,
    }
}

fn lower_create_edge(edge: &GqlBoundCreateEdge, expr_cursor: &mut usize) -> GqlCreateEdgePlan {
    GqlCreateEdgePlan {
        alias: edge.alias.clone(),
        from_alias: edge.from_alias.clone(),
        to_alias: edge.to_alias.clone(),
        label: edge.rel_type.name.clone(),
        property_keys: map_property_keys(edge.properties.as_ref()),
        property_values: map_property_values(edge.properties.as_ref(), expr_cursor),
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

fn map_property_keys(map: Option<&MapLiteral>) -> Vec<String> {
    map.map(|map| {
        map.entries
            .iter()
            .map(|entry| entry.key.name.clone())
            .collect()
    })
    .unwrap_or_default()
}

fn map_property_values(
    map: Option<&MapLiteral>,
    expr_cursor: &mut usize,
) -> BTreeMap<String, GqlMutationExprRef> {
    map.map(|map| {
        map.entries
            .iter()
            .map(|entry| (entry.key.name.clone(), next_expr_ref(expr_cursor)))
            .collect()
    })
    .unwrap_or_default()
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

    fn collect_graph_nodes(
        &mut self,
        pattern: &GqlBoundPattern,
        nodes: &mut Vec<GraphNodePattern>,
        node_indexes: &mut BTreeMap<String, usize>,
    ) -> Result<Vec<Expr>, EngineError> {
        let mut reused_constraints = Vec::new();
        for node in &pattern.nodes {
            if node_indexes.contains_key(&node.alias) {
                reused_constraints.extend(reused_node_constraint_exprs(node));
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
        let GqlNativeTarget::GraphRows { query } = target;
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

        match self.try_push_predicate(expr)? {
            Some(PushFilter::Node { alias, filter }) => {
                let Some(index) = node_indexes.get(&alias).copied() else {
                    self.residual_predicates.push(expr.clone());
                    return Ok(());
                };
                let node = &mut nodes[index];
                node.filter = merge_node_filter(node.filter.take(), Some(filter));
            }
            Some(PushFilter::NodeIds { alias, ids }) => {
                let Some(index) = node_indexes.get(&alias).copied() else {
                    self.residual_predicates.push(expr.clone());
                    return Ok(());
                };
                let node = &mut nodes[index];
                node.ids.extend(ids);
            }
            Some(PushFilter::NodeKeys { alias, keys }) => {
                let Some(index) = node_indexes.get(&alias).copied() else {
                    self.residual_predicates.push(expr.clone());
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
                    self.residual_predicates.push(expr.clone());
                }
            }
            Some(PushFilter::Edge { alias, filter }) => {
                let Some(index) = edge_indexes.get(&alias).copied() else {
                    self.residual_predicates.push(expr.clone());
                    return Ok(());
                };
                let Some(edge) = graph_fixed_edge_mut(pieces, index) else {
                    self.residual_predicates.push(expr.clone());
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
                    self.residual_predicates.push(expr.clone());
                    return Ok(());
                };
                let Some(edge) = graph_fixed_edge_mut(pieces, index) else {
                    self.residual_predicates.push(expr.clone());
                    return Ok(());
                };
                if merge_edge_label_filter(&mut edge.label_filter, &labels) {
                    self.record_edge_push(alias, summary);
                } else {
                    self.residual_predicates.push(expr.clone());
                }
            }
            Some(PushFilter::EdgeIds { alias, ids }) => {
                if edge_indexes.contains_key(&alias) {
                    if self.edge_id_constraints.contains_key(&alias) {
                        self.residual_predicates.push(expr.clone());
                    } else {
                        let summary = edge_id_summary(&alias, &ids);
                        self.edge_id_constraints.insert(alias.clone(), ids);
                        self.record_edge_push(alias, summary);
                    }
                } else {
                    self.residual_predicates.push(expr.clone());
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
                    self.residual_predicates.push(expr.clone());
                }
            }
            Some(PushFilter::Noop) => self.residual_predicates.push(expr.clone()),
            None => self.residual_predicates.push(expr.clone()),
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
                BinaryOp::And | BinaryOp::Or | BinaryOp::Neq => Ok(None),
            },
            ExprKind::IsNull { .. } | ExprKind::Unary { .. } => Ok(None),
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
                        summary: format!("{}.{} = {id}", alias, field.as_str()),
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
                        summary: format!("{}.{} = {value}", alias, field.as_str()),
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
                    summary: format!("{}.{} = {:?}", alias, field.as_str(), value),
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
                        summary: format!("{}.{} IN {:?}", alias, field.as_str(), ids),
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
                    summary: format!("{}.{} {}", alias, field.as_str(), range_op_text(op)),
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
                    summary: format!("{}.{} {}", alias, field.as_str(), range_op_text(op)),
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
    fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Labels => "labels",
            Self::Key => "key",
            Self::Weight => "weight",
            Self::CreatedAt => "created_at",
            Self::UpdatedAt => "updated_at",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeEndpointField {
    From,
    To,
}

impl EdgeEndpointField {
    fn as_str(self) -> &'static str {
        match self {
            Self::From => "from",
            Self::To => "to",
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
    fn as_str(self) -> &'static str {
        match self {
            Self::Weight => "weight",
            Self::CreatedAt => "created_at",
            Self::UpdatedAt => "updated_at",
            Self::ValidFrom => "valid_from",
            Self::ValidTo => "valid_to",
        }
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
    semantic: &GqlSemanticPlan,
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
            semantic
                .clauses
                .iter()
                .find_map(|clause| clause.where_clause.as_ref()),
            edge_alias,
            params,
        )?
    {
        return Err(unsupported_pure_edge_label_or(
            semantic
                .clauses
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
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_expr_pattern_variables(semantic, return_aliases, &entry.value, out);
            }
        }
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
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_expr_variables(&entry.value, out);
            }
        }
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
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } => GraphExpr::Unary {
            op: crate::types::GraphUnaryOp::Not,
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
            if name.name.eq_ignore_ascii_case("node_ids")
                || name.name.eq_ignore_ascii_case("edge_ids")
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
                    field: if name.name.eq_ignore_ascii_case("node_ids") {
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
        GqlAliasKind::Node => Ok(match property.name.as_str() {
            "id" => GraphExpr::NodeField {
                alias: alias.to_string(),
                field: GraphNodeField::Id,
            },
            "labels" => GraphExpr::NodeField {
                alias: alias.to_string(),
                field: GraphNodeField::Labels,
            },
            "key" => GraphExpr::NodeField {
                alias: alias.to_string(),
                field: GraphNodeField::Key,
            },
            "weight" => GraphExpr::NodeField {
                alias: alias.to_string(),
                field: GraphNodeField::Weight,
            },
            "created_at" => GraphExpr::NodeField {
                alias: alias.to_string(),
                field: GraphNodeField::CreatedAt,
            },
            "updated_at" => GraphExpr::NodeField {
                alias: alias.to_string(),
                field: GraphNodeField::UpdatedAt,
            },
            _ => GraphExpr::Property {
                alias: alias.to_string(),
                key: property.name.clone(),
            },
        }),
        GqlAliasKind::Edge => Ok(match property.name.as_str() {
            "from" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::From,
            },
            "to" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::To,
            },
            "weight" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::Weight,
            },
            "created_at" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::CreatedAt,
            },
            "updated_at" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::UpdatedAt,
            },
            "valid_from" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::ValidFrom,
            },
            "valid_to" => GraphExpr::EdgeField {
                alias: alias.to_string(),
                field: GraphEdgeField::ValidTo,
            },
            _ => GraphExpr::Property {
                alias: alias.to_string(),
                key: property.name.clone(),
            },
        }),
        GqlAliasKind::Path => Ok(match property.name.as_str() {
            "node_ids" => GraphExpr::PathField {
                alias: alias.to_string(),
                field: GraphPathField::NodeIds,
            },
            "edge_ids" => GraphExpr::PathField {
                alias: alias.to_string(),
                field: GraphPathField::EdgeIds,
            },
            "length" => GraphExpr::PathField {
                alias: alias.to_string(),
                field: GraphPathField::Length,
            },
            _ => {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidPropertyAccess,
                    format!("unsupported path property '{}'", property.name),
                    property.span.clone(),
                ));
            }
        }),
    }
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
        BinaryOp::Eq => GraphBinaryOp::Eq,
        BinaryOp::Neq => GraphBinaryOp::Neq,
        BinaryOp::Lt => GraphBinaryOp::Lt,
        BinaryOp::Le => GraphBinaryOp::Le,
        BinaryOp::Gt => GraphBinaryOp::Gt,
        BinaryOp::Ge => GraphBinaryOp::Ge,
        BinaryOp::In => GraphBinaryOp::In,
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
        "start_node" => Ok(GraphFunction::StartNode),
        "end_node" => Ok(GraphFunction::EndNode),
        "nodes" => Ok(GraphFunction::Nodes),
        "relationships" => Ok(GraphFunction::Relationships),
        _ => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "unsupported GQL scalar function".to_string(),
            span.clone(),
        )),
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
        [key] => format!("{alias}.key = {key:?}"),
        _ => format!("{alias}.key IN {:?}", keys),
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
        [id] => format!("{}.{} = {id}", alias, field.as_str()),
        _ => format!("{}.{} IN {:?}", alias, field.as_str(), ids),
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
                GqlAliasKind::Node => node_metadata_field(&property.name)
                    .map(|field| EntityValueRef::NodeMetadata {
                        alias: alias.clone(),
                        field,
                    })
                    .or_else(|| {
                        Some(EntityValueRef::NodeProperty {
                            alias,
                            key: property.name.clone(),
                        })
                    }),
                GqlAliasKind::Edge => edge_endpoint_field(&property.name)
                    .map(|field| EntityValueRef::EdgeEndpoint {
                        alias: alias.clone(),
                        field,
                    })
                    .or_else(|| {
                        edge_metadata_field(&property.name).map(|field| {
                            EntityValueRef::EdgeMetadata {
                                alias: alias.clone(),
                                field,
                            }
                        })
                    })
                    .or_else(|| {
                        Some(EntityValueRef::EdgeProperty {
                            alias,
                            key: property.name.clone(),
                        })
                    }),
                GqlAliasKind::Path => None,
            }
        }
        ExprKind::FunctionCall { name, args } if args.len() == 1 => {
            let alias = variable_name(&args[0])?.to_string();
            match name.name.to_ascii_lowercase().as_str() {
                "id" => match alias_kinds.get(&alias).copied() {
                    Some(GqlAliasKind::Node) => Some(EntityValueRef::NodeId { alias }),
                    Some(GqlAliasKind::Edge) => Some(EntityValueRef::EdgeId { alias }),
                    Some(GqlAliasKind::Path) | None => None,
                },
                "type" => Some(EntityValueRef::RelationshipLabelFunction { alias }),
                _ => None,
            }
        }
        _ => None,
    }
}

fn node_metadata_field(name: &str) -> Option<NodeMetadataField> {
    match name {
        "id" => Some(NodeMetadataField::Id),
        "labels" => Some(NodeMetadataField::Labels),
        "key" => Some(NodeMetadataField::Key),
        "weight" => Some(NodeMetadataField::Weight),
        "created_at" => Some(NodeMetadataField::CreatedAt),
        "updated_at" => Some(NodeMetadataField::UpdatedAt),
        _ => None,
    }
}

fn edge_endpoint_field(name: &str) -> Option<EdgeEndpointField> {
    match name {
        "from" => Some(EdgeEndpointField::From),
        "to" => Some(EdgeEndpointField::To),
        _ => None,
    }
}

fn edge_metadata_field(name: &str) -> Option<EdgeMetadataField> {
    match name {
        "weight" => Some(EdgeMetadataField::Weight),
        "created_at" => Some(EdgeMetadataField::CreatedAt),
        "updated_at" => Some(EdgeMetadataField::UpdatedAt),
        "valid_from" => Some(EdgeMetadataField::ValidFrom),
        "valid_to" => Some(EdgeMetadataField::ValidTo),
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

fn reused_node_constraint_exprs(node: &GqlBoundNodePattern) -> Vec<Expr> {
    let mut constraints = Vec::new();
    for label in &node.labels {
        constraints.push(node_label_membership_expr(&node.alias, label));
    }
    if let Some(properties) = node.properties.as_ref() {
        constraints.extend(
            properties
                .entries
                .iter()
                .map(|entry| property_map_residual(&node.alias, entry)),
        );
    }
    constraints
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
        | NodeFilterExpr::PropertyEquals { .. }
        | NodeFilterExpr::PropertyIn { .. }
        | NodeFilterExpr::PropertyRange { .. }
        | NodeFilterExpr::PropertyExists { .. }
        | NodeFilterExpr::PropertyMissing { .. }
        | NodeFilterExpr::UpdatedAtRange { .. } => false,
    }
}

fn edge_filter_has_metadata_anchor(filter: &EdgeFilterExpr) -> bool {
    match filter {
        EdgeFilterExpr::WeightRange { .. }
        | EdgeFilterExpr::UpdatedAtRange { .. }
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
        let GqlNativeTarget::GraphRows { query } = &plan.native_target;
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
        let plan = lower_mut("CREATE (n:Person {key: 'ada', name: 'Ada'}) RETURN n").unwrap();
        assert!(plan.read_prefix.is_none());
        assert_eq!(plan.return_plan.as_ref().unwrap().columns, vec!["n"]);
        let [GqlMutationClausePlan::Create(patterns)] = plan.clauses.as_slice() else {
            panic!("expected CREATE plan");
        };
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].nodes[0].alias, "n");
        assert_eq!(patterns[0].nodes[0].labels, vec!["Person"]);
        assert_eq!(
            patterns[0].nodes[0].property_keys,
            vec!["key".to_string(), "name".to_string()]
        );
        assert_eq!(plan.operation_exprs.len(), 2);
        assert_eq!(patterns[0].nodes[0].property_values["key"].id, 0);
        assert_eq!(patterns[0].nodes[0].property_values["name"].id, 1);
    }

    #[test]
    fn lowers_match_backed_mutation_read_prefix_to_graph_rows() {
        let plan = lower_mut(
            "MATCH (n:Person {key: 'ada'}) OPTIONAL MATCH p = (n)-[r:KNOWS*1..1]->(m) SET n.name = m.name RETURN n, p",
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
        assert_eq!(read.graph_row.query.return_items.as_ref().unwrap().len(), 4);
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
        let missing = lower("MATCH (n:Person {key: $key}) RETURN n")
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
    fn node_metadata_predicates_push_down_only_when_native_semantics_match() {
        let plan = lower(
            "MATCH (n:Person) \
             WHERE n.key = 'alice' AND n.updated_at >= 100 RETURN n",
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
            .any(|push| push.summary == "n.key = \"alice\""));

        let residual_key = lower_result_with_options(
            "MATCH (n) WHERE n.key = 'alice' RETURN n",
            allow_full_scan(),
        )
        .unwrap();
        let query = &graph_target(&residual_key).query;
        assert!(query.nodes[0].keys.is_empty());
        assert_eq!(residual_key.residual_predicates.len(), 1);
        assert!(!residual_key
            .pushed_down
            .iter()
            .any(|push| push.summary.starts_with("n.key")));
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
        let weight_plan = lower("MATCH ()-[r]->() WHERE r.weight > 0.5 RETURN r.since").unwrap();
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
        let plan = lower("MATCH ()-[r]->() WHERE r.from = 42 AND r.to IN [7, 8] RETURN r").unwrap();
        let query = &graph_target(&plan).query;
        assert_eq!(query.nodes[0].ids, vec![42]);
        assert_eq!(query.nodes[1].ids, vec![7, 8]);
        assert!(graph_edge(query, 0).filter.is_none());
        assert!(plan.residual_predicates.is_empty());
        assert!(plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "r.from = 42"));
        assert!(plan
            .pushed_down
            .iter()
            .any(|push| push.summary == "r.to IN [7, 8]"));
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
             WHERE r.from = 42 AND b.updated_at < 100 RETURN r",
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

        let reverse = lower("MATCH (a)<-[r:KNOWS]-(b) WHERE r.from = 42 RETURN r").unwrap();
        let query = &graph_target(&reverse).query;
        assert!(query.nodes[0].ids.is_empty());
        assert_eq!(query.nodes[1].ids, vec![42]);

        let undirected = lower("MATCH (a)-[r:KNOWS]-(b) WHERE r.from = 42 RETURN r").unwrap();
        let query = &graph_target(&undirected).query;
        assert!(query.nodes.iter().all(|node| node.ids.is_empty()));
        assert_eq!(undirected.residual_predicates.len(), 1);
        assert!(!undirected
            .pushed_down
            .iter()
            .any(|push| push.summary.starts_with("r.from")));
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
             RETURN p, node_ids(p), edge_ids(p), length(p)",
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
             RETURN * ORDER BY node_ids(p)",
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
            ExprKind::FunctionCall { name, .. } if name.name == "node_ids"
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
             RETURN length(p) AS hops, start_node(p) AS first, end_node(p) AS last, \
                    nodes(p) AS ns, relationships(p) AS rs, node_ids(p) AS node_ids, edge_ids(p) AS edge_ids \
             ORDER BY edge_ids(p)",
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
            ExprKind::FunctionCall { name, .. } if name.name == "edge_ids"
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
