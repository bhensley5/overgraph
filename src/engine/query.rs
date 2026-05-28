use crate::gql::eval::{
    build_runtime_projection, eval_expr_against_context, return_exprs, GqlEvalContext,
    GqlReturnExpr,
};
use crate::gql::ast::{
    BinaryOp, Expr, ExprKind, GqlMutationStatement, GqlQuery, GqlStatementBody, Literal,
    OrderDirection, UnaryOp,
};
use crate::gql::lower::{
    gql_expr_to_graph_expr, gql_order_direction_to_graph, lower_mutation, lower_semantic_plan,
    GqlCreateEdgePlan, GqlCreateNodePlan, GqlCreatePatternPlan, GqlDeleteTargetPlan,
    GqlLoweredPlan, GqlMutationClausePlan, GqlMutationInternalColumn, GqlMutationPlan,
    GqlNativeTarget, GqlNativeTargetKind, GqlRemoveItemPlan, GqlSetItemPlan,
};
use crate::gql::parser::{parse_statement, GqlParseOptions};
use crate::gql::params::validate_referenced_gql_params;
use crate::gql::result::graph_value_to_gql_value;
use crate::gql::semantic::{
    bind_query, gql_semantic_error, GqlAliasKind, GqlAliasOrigin, GqlReturnPlan,
    GqlSemanticPlan,
};
use crate::graph_row::{eval_graph_expr, GraphBindingSchema, GraphEvalContext, GraphEvalValue};
use std::time::Instant;

impl DatabaseEngine {
    pub fn execute_gql(
        &self,
        query: &str,
        params: &GqlParams,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionResult, EngineError> {
        let started_at = Instant::now();
        let parse_options = GqlParseOptions {
            max_query_bytes: options.max_query_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        };
        let statement = parse_statement(query, &parse_options)?;
        match statement.body {
            GqlStatementBody::Query(query) => {
                self.execute_gql_query(query, params, options, started_at)
            }
            GqlStatementBody::Mutation(mutation) => {
                self.execute_gql_mutation(mutation, params, options, started_at)
            }
        }
    }

    pub fn explain_gql(
        &self,
        query: &str,
        params: &GqlParams,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionExplain, EngineError> {
        let parse_options = GqlParseOptions {
            max_query_bytes: options.max_query_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        };
        let statement = parse_statement(query, &parse_options)?;
        match statement.body {
            GqlStatementBody::Query(query) => self.explain_gql_query(query, params, options),
            GqlStatementBody::Mutation(mutation) => {
                explain_gql_mutation(self, mutation, params, options)
            }
        }
    }

    fn execute_gql_query(
        &self,
        ast: GqlQuery,
        params: &GqlParams,
        options: &GqlExecutionOptions,
        started_at: Instant,
    ) -> Result<GqlExecutionResult, EngineError> {
        let semantic = bind_query(ast, params)?;
        validate_referenced_gql_params(&semantic, params, options)?;
        let mut lowered = lower_semantic_plan(semantic, params, options)?;
        let return_exprs = return_exprs(&lowered.semantic);
        let resolved_order_by = resolve_order_by_return_aliases(&lowered)?;
        validate_gql_row_independent_order_keys(&resolved_order_by, &lowered, params)?;
        let row_counts = evaluate_gql_row_counts(&lowered, params, options)?;
        configure_gql_graph_row_target(&mut lowered, &resolved_order_by, &row_counts, options)?;
        let warnings = lowered.warnings.clone();

        if row_counts.limit == Some(0) {
            let plan = if options.include_plan {
                Some(wrap_read_gql_explain(build_gql_limit_zero_explain(
                    &lowered,
                    &return_exprs,
                    &resolved_order_by,
                    options,
                ), options))
            } else {
                None
            };
            let elapsed_us = if options.profile {
                started_at.elapsed().as_micros().try_into().ok()
            } else {
                None
            };
            return Ok(GqlExecutionResult {
                kind: GqlStatementKind::Query,
                columns: return_exprs
                    .iter()
                    .map(|expr| expr.output_name.clone())
                    .collect(),
                rows: Vec::new(),
                next_cursor: None,
                stats: GqlExecutionStats {
                    rows_returned: 0,
                    rows_matched: 0,
                    rows_after_filter: 0,
                    intermediate_bindings: 0,
                    db_hits: 0,
                    elapsed_us,
                    warnings,
                },
                mutation_stats: None,
                plan,
            });
        }

        let (_guard, published) = self.runtime.published_snapshot()?;
        let plan = if options.include_plan {
            Some(wrap_read_gql_explain(build_gql_explain(
                &published.view,
                &lowered,
                &return_exprs,
                &resolved_order_by,
                options,
            )?, options))
        } else {
            None
        };
        let mut warnings = warnings;

        let graph_rows = execute_gql_graph_row_target(&published.view, &lowered)?;
        for followup in graph_rows.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        let graph_result = graph_rows.value;
        warnings.extend(graph_result.stats.warnings.iter().cloned());

        let effective_row_cap = options.max_rows.min(options.max_intermediate_bindings).max(1);
        let truncated_by_row_cap = graph_result.next_cursor.is_some()
            && row_counts
                .limit
                .is_none_or(|limit| limit > effective_row_cap);
        if truncated_by_row_cap {
            let (cap_name, cap_value) = if options.max_intermediate_bindings < options.max_rows {
                ("max_intermediate_bindings", options.max_intermediate_bindings)
            } else {
                ("max_rows", options.max_rows)
            };
            if !resolved_order_by.is_empty() {
                warnings.push(format!(
                    "GQL ORDER BY evaluated over capped rows at {cap_name}={cap_value}; ordered results may be incomplete"
                ));
            } else {
                warnings.push(format!("GQL result rows capped at {cap_name}={cap_value}"));
            }
        }

        let projected = graph_result
            .rows
            .into_iter()
            .map(|row| {
                Ok(GqlRow {
                    values: row
                        .values
                        .into_iter()
                        .map(graph_value_to_gql_value)
                        .collect::<Result<Vec<_>, EngineError>>()?,
                })
            })
            .collect::<Result<Vec<_>, EngineError>>()?;

        let rows_returned = projected.len();
        let elapsed_us = if options.profile {
            started_at.elapsed().as_micros().try_into().ok()
        } else {
            None
        };
        let mut rows_matched = graph_result
            .stats
            .intermediate_bindings_peak
            .max(graph_result.stats.rows_after_filter);
        if graph_result.stats.rows_after_filter == 0 && rows_matched == 1 {
            rows_matched = 0;
        }
        if truncated_by_row_cap && row_counts.limit.is_none() {
            rows_matched = rows_returned;
        }
        Ok(GqlExecutionResult {
            kind: GqlStatementKind::Query,
            columns: return_exprs
                .iter()
                .map(|expr| expr.output_name.clone())
                .collect(),
            rows: projected,
            next_cursor: graph_result.next_cursor,
            stats: GqlExecutionStats {
                rows_returned,
                rows_matched,
                rows_after_filter: graph_result.stats.rows_after_filter,
                intermediate_bindings: graph_result.stats.intermediate_bindings_peak,
                db_hits: if options.profile {
                    rows_matched
                } else {
                    0
                },
                elapsed_us,
                warnings,
            },
            mutation_stats: None,
            plan,
        })
    }

    fn explain_gql_query(
        &self,
        ast: GqlQuery,
        params: &GqlParams,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionExplain, EngineError> {
        let semantic = bind_query(ast, params)?;
        validate_referenced_gql_params(&semantic, params, options)?;
        let mut lowered = lower_semantic_plan(semantic, params, options)?;
        let return_exprs = return_exprs(&lowered.semantic);
        let resolved_order_by = resolve_order_by_return_aliases(&lowered)?;
        validate_gql_row_independent_order_keys(&resolved_order_by, &lowered, params)?;
        let row_counts = evaluate_gql_row_counts(&lowered, params, options)?;
        configure_gql_graph_row_target(&mut lowered, &resolved_order_by, &row_counts, options)?;
        if row_counts.limit == Some(0) {
            return Ok(wrap_read_gql_explain(build_gql_limit_zero_explain(
                &lowered,
                &return_exprs,
                &resolved_order_by,
                options,
            ), options));
        }

        let (_guard, published) = self.runtime.published_snapshot()?;
        Ok(wrap_read_gql_explain(build_gql_explain(
            &published.view,
            &lowered,
            &return_exprs,
            &resolved_order_by,
            options,
        )?, options))
    }

    pub fn query_node_ids(
        &self,
        query: &NodeQuery,
    ) -> Result<QueryNodeIdsResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        #[cfg(test)]
        published
            .view
            .query_execution_counters
            .public_node_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let outcome = published.view.query_node_ids_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn query_nodes(&self, query: &NodeQuery) -> Result<QueryNodesResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        #[cfg(test)]
        published
            .view
            .query_execution_counters
            .public_node_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let outcome = published.view.query_nodes_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn explain_node_query(&self, query: &NodeQuery) -> Result<QueryPlan, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.explain_node_query(query)
    }

    pub fn query_edge_ids(
        &self,
        query: &EdgeQuery,
    ) -> Result<QueryEdgeIdsResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        #[cfg(test)]
        published
            .view
            .query_execution_counters
            .public_edge_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let outcome = published.view.query_edge_ids_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn query_edges(&self, query: &EdgeQuery) -> Result<QueryEdgesResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        #[cfg(test)]
        published
            .view
            .query_execution_counters
            .public_edge_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let outcome = published.view.query_edges_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn explain_edge_query(&self, query: &EdgeQuery) -> Result<QueryPlan, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.explain_edge_query(query)
    }

    pub fn query_graph_rows(
        &self,
        query: &GraphRowQuery,
    ) -> Result<GraphRowResult, EngineError> {
        let decoded_cursor = graph_row_decode_request_cursor(&query.page, &query.options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let cursor_state =
            graph_row_cursor_state_from_decoded(decoded_cursor, &query.page, query.at_epoch)?;
        let normalized = normalize_graph_row_query(query)?;
        let outcome = published
            .view
            .query_graph_rows_outcome(&normalized, cursor_state)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn explain_graph_rows(
        &self,
        query: &GraphRowQuery,
    ) -> Result<GraphRowExplain, EngineError> {
        let decoded_cursor = graph_row_decode_request_cursor(&query.page, &query.options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let cursor_state =
            graph_row_cursor_state_from_decoded(decoded_cursor, &query.page, query.at_epoch)?;
        let normalized = normalize_graph_row_query(query)?;
        published
            .view
            .explain_graph_rows_normalized(&normalized, cursor_state)
    }

}

fn execute_gql_mutation_unsupported_error(plan: &GqlMutationPlan) -> EngineError {
    EngineError::GqlUnsupported {
        feature: "GQL mutation execution".to_string(),
        message: "GQL mutation execution for the supplied clause combination is not supported by the current implementation".to_string(),
        span: plan.semantic.statement.span.clone(),
    }
}

impl DatabaseEngine {
    fn execute_gql_mutation(
        &self,
        mutation: GqlMutationStatement,
        params: &GqlParams,
        options: &GqlExecutionOptions,
        started_at: Instant,
    ) -> Result<GqlExecutionResult, EngineError> {
        if options.cursor.is_some() {
            return Err(EngineError::InvalidCursor {
                message: "GQL mutation statements do not accept cursors".into(),
            });
        }
        if options.mode == GqlExecutionMode::ReadOnly {
            return Err(gql_read_only_mutation_error(&mutation.span));
        }
        let plan = lower_mutation(mutation, params, options)?;
        validate_gql_mutation_plan_for_execution(&plan)?;
        if !gql_mutation_plan_is_executable(&plan) {
            return Err(execute_gql_mutation_unsupported_error(&plan));
        }
        self.execute_gql_create_mutation(&plan, params, options, started_at)
    }
}

fn gql_mutation_plan_is_executable(plan: &GqlMutationPlan) -> bool {
    !plan.clauses.is_empty()
        && plan
            .clauses
            .iter()
            .all(|clause| {
                matches!(
                    clause,
                    GqlMutationClausePlan::Create(_)
                        | GqlMutationClausePlan::Set(_)
                        | GqlMutationClausePlan::Remove(_)
                        | GqlMutationClausePlan::Delete { .. }
                )
            })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GqlCreateEndpointKey {
    Id(u64),
    Local(TxnLocalRef),
}

#[derive(Clone)]
struct GqlCreateExecutionRow {
    read_nodes: BTreeMap<String, Option<u64>>,
    read_edges: BTreeMap<String, Option<u64>>,
    read_paths: BTreeMap<String, Option<GqlPathIdentity>>,
    expr_values: Vec<Option<GraphValue>>,
    created_nodes: BTreeMap<String, usize>,
    created_edges: BTreeMap<String, usize>,
    produced_write: bool,
}

struct GqlMutationInputRows {
    rows: Vec<GqlCreateExecutionRow>,
    db_hits: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GqlPathIdentity {
    node_ids: Vec<u64>,
    edge_ids: Vec<u64>,
}

struct GqlCreatedNodeExecution {
    local: TxnLocalRef,
    labels: Vec<String>,
    key: String,
    props: BTreeMap<String, PropValue>,
    weight: f32,
}

struct GqlCreatedEdgeExecution {
    alias: Option<String>,
    local: Option<TxnLocalRef>,
    from: TxnNodeRef,
    to: TxnNodeRef,
    label: String,
    props: BTreeMap<String, PropValue>,
    weight: f32,
    valid_from: Option<i64>,
    valid_to: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GqlMutationTargetKey {
    CreatedNode(usize),
    CreatedEdge(usize),
    ExistingNode(u64),
    ExistingEdge(u64),
}

struct GqlExistingNodeExecution {
    original: NodeRecord,
    original_labels: Vec<String>,
    labels: Vec<String>,
    props: BTreeMap<String, PropValue>,
    weight: f32,
    dense_vector: Option<DenseVector>,
    sparse_vector: Option<SparseVector>,
}

struct GqlExistingEdgeExecution {
    original: EdgeRecord,
    label: String,
    props: BTreeMap<String, PropValue>,
    weight: f32,
    valid_from: i64,
    valid_to: i64,
}

struct GqlCreateMaterialization {
    rows: Vec<GqlCreateExecutionRow>,
    intents: Vec<TxnIntent>,
    record_replacements: Vec<TxnRecordReplacement>,
    nodes: Vec<GqlCreatedNodeExecution>,
    edges: Vec<GqlCreatedEdgeExecution>,
    existing_nodes: BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: BTreeMap<u64, GqlExistingEdgeExecution>,
    node_precheck_keys: BTreeSet<(String, String)>,
    edge_precheck_triples: BTreeSet<(u64, u64, String)>,
    mutation_rows: usize,
    mutation_ops: usize,
    nodes_created: usize,
    nodes_updated: usize,
    nodes_deleted: usize,
    edges_created: usize,
    edges_updated: usize,
    edges_deleted: usize,
    properties_set: usize,
    properties_removed: usize,
    labels_added: usize,
    labels_removed: usize,
    skipped_null_targets: usize,
    duplicate_targets: usize,
    db_hits: usize,
}

impl DatabaseEngine {
    fn execute_gql_create_mutation(
        &self,
        plan: &GqlMutationPlan,
        params: &GqlParams,
        options: &GqlExecutionOptions,
        started_at: Instant,
    ) -> Result<GqlExecutionResult, EngineError> {
        let return_static = build_gql_mutation_return_static_plan(plan, params, options)?;
        let mut txn = self.begin_write_txn()?;
        let snapshot = txn.gql_snapshot()?;
        let explain = if options.include_plan {
            Some(build_gql_mutation_explain_with_snapshot(
                snapshot.as_ref(),
                plan,
                params,
                options,
            )?)
        } else {
            None
        };
        let edge_uniqueness = txn.gql_edge_uniqueness()?;
        let input = self.gql_create_input_rows(plan, params, options, snapshot.as_ref())?;
        let mutation_timestamp = now_millis();
        let mut materialized = materialize_gql_create(
            plan,
            input.rows,
            edge_uniqueness,
            options.max_mutation_ops,
            mutation_timestamp,
            snapshot.as_ref(),
        )?;
        let return_execution = build_gql_mutation_return_execution_plan(
            plan,
            return_static,
            params,
            options,
            &materialized,
            snapshot.as_ref(),
        )?;
        precheck_gql_create_conflicts(&txn, &materialized, edge_uniqueness)?;
        if let Some(return_execution) = return_execution.as_ref() {
            txn.gql_validate_return_read_set(return_execution.read_set.clone())?;
        }
        txn.gql_apply_mutation_op_budget(options.max_mutation_ops)?;
        #[cfg(test)]
        self.runtime.pause_gql_mutation_before_commit_for_test();

        let intents = std::mem::take(&mut materialized.intents);
        let replacements = std::mem::take(&mut materialized.record_replacements);
        txn.stage_intents(intents)?;
        txn.stage_record_replacements(replacements)?;
        let needs_return_view = return_execution
            .as_ref()
            .is_some_and(gql_mutation_return_needs_committed_view);
        let (commit, return_view) = if needs_return_view {
            let (commit, view) = txn.commit_with_gql_return_view()?;
            (commit, Some(view))
        } else {
            (txn.commit()?, None)
        };
        let result_rows = build_gql_mutation_return_rows(
            plan,
            params,
            &materialized,
            &commit,
            return_execution.as_ref(),
            return_view.as_deref(),
            options,
        )?;
        let rows_returned = result_rows.len();
        let elapsed_us = if options.profile {
            started_at.elapsed().as_micros().try_into().ok()
        } else {
            None
        };
        let db_hits = gql_mutation_profile_db_hits(
            options,
            input.db_hits,
            materialized.db_hits,
            return_execution.as_ref(),
        );
        let warnings = plan.warnings.clone();
        Ok(GqlExecutionResult {
            kind: GqlStatementKind::Mutation,
            columns: plan
                .return_plan
                .as_ref()
                .map(|return_plan| return_plan.columns.clone())
                .unwrap_or_default(),
            rows: result_rows,
            next_cursor: None,
            stats: GqlExecutionStats {
                rows_returned,
                rows_matched: materialized.rows.len(),
                rows_after_filter: materialized.rows.len(),
                intermediate_bindings: materialized.rows.len(),
                db_hits,
                elapsed_us,
                warnings: warnings.clone(),
            },
            mutation_stats: Some(GqlMutationStats {
                rows_matched: materialized.rows.len(),
                mutation_rows: materialized.mutation_rows,
                mutation_ops: materialized.mutation_ops,
                nodes_created: materialized.nodes_created,
                nodes_updated: materialized.nodes_updated,
                nodes_deleted: materialized.nodes_deleted,
                edges_created: materialized.edges_created,
                edges_updated: materialized.edges_updated,
                edges_deleted: materialized.edges_deleted,
                labels_added: materialized.labels_added,
                labels_removed: materialized.labels_removed,
                properties_set: materialized.properties_set,
                properties_removed: materialized.properties_removed,
                skipped_null_targets: materialized.skipped_null_targets,
                duplicate_targets: materialized.duplicate_targets,
                db_hits,
                elapsed_us,
                warnings,
            }),
            plan: explain,
        })
    }

    fn gql_create_input_rows(
        &self,
        plan: &GqlMutationPlan,
        params: &GqlParams,
        options: &GqlExecutionOptions,
        snapshot: &ReadView,
    ) -> Result<GqlMutationInputRows, EngineError> {
        let missing_expr_ids = gql_create_missing_operation_expr_ids(plan);
        let graph_params =
            gql_params_to_graph_params_for_mutation(params, plan, &missing_expr_ids);
        if let Some(read_prefix) = plan.read_prefix.as_ref() {
            let outcome = execute_gql_graph_row_target(snapshot, &read_prefix.lowered)?;
            for followup in outcome.followups {
                self.runtime.enqueue_secondary_index_read_followup(followup);
            }
            let graph_result = outcome.value;
            if graph_result.rows.len() > options.max_mutation_rows {
                return Err(gql_mutation_cap_error(
                    "max_mutation_rows",
                    graph_result.rows.len(),
                    options.max_mutation_rows,
                ));
            }
            if graph_result.next_cursor.is_some() {
                let (cap_name, cap_value) =
                    if options.max_intermediate_bindings <= options.max_mutation_rows {
                        ("max_intermediate_bindings", options.max_intermediate_bindings)
                    } else {
                        ("max_mutation_rows", options.max_mutation_rows)
                    };
                return Err(gql_mutation_cap_error(
                    cap_name,
                    graph_result.rows.len().saturating_add(1),
                    cap_value,
                ));
            }
            let db_hits = if options.profile {
                gql_profile_graph_row_db_hits(&graph_result.stats)
            } else {
                0
            };
            let rows = graph_result
                .rows
                .into_iter()
                .map(|row| {
                    gql_create_input_row_from_graph_row(
                        plan,
                        row.values,
                        &missing_expr_ids,
                        &graph_params,
                    )
                })
                .collect::<Result<Vec<_>, EngineError>>()?;
            Ok(GqlMutationInputRows { rows, db_hits })
        } else {
            if options.max_mutation_rows == 0 {
                return Err(gql_mutation_cap_error("max_mutation_rows", 1, 0));
            }
            let mut row = GqlCreateExecutionRow {
                read_nodes: BTreeMap::new(),
                read_edges: BTreeMap::new(),
                read_paths: BTreeMap::new(),
                expr_values: vec![None; plan.operation_exprs.len()],
                created_nodes: BTreeMap::new(),
                created_edges: BTreeMap::new(),
                produced_write: false,
            };
            fill_missing_gql_create_expr_values(
                plan,
                &mut row,
                &missing_expr_ids,
                &graph_params,
            )?;
            Ok(GqlMutationInputRows {
                rows: vec![row],
                db_hits: 0,
            })
        }
    }
}

fn gql_profile_graph_row_db_hits(stats: &GraphRowStats) -> usize {
    stats
        .db_hits
        .max(stats.intermediate_bindings_peak)
        .max(stats.rows_after_filter)
        .max(stats.rows_returned)
}

fn gql_mutation_profile_db_hits(
    options: &GqlExecutionOptions,
    input_db_hits: usize,
    materialization_db_hits: usize,
    return_execution: Option<&GqlMutationReturnExecutionPlan>,
) -> usize {
    if !options.profile {
        return 0;
    }
    input_db_hits
        .saturating_add(materialization_db_hits)
        .saturating_add(
            return_execution
                .map(gql_mutation_return_profile_db_hits)
                .unwrap_or(0),
        )
}

fn gql_create_input_row_from_graph_row(
    plan: &GqlMutationPlan,
    values: Vec<GraphValue>,
    missing_expr_ids: &[usize],
    graph_params: &BTreeMap<String, GraphParamValue>,
) -> Result<GqlCreateExecutionRow, EngineError> {
    let mut row = GqlCreateExecutionRow {
        read_nodes: BTreeMap::new(),
        read_edges: BTreeMap::new(),
        read_paths: BTreeMap::new(),
        expr_values: vec![None; plan.operation_exprs.len()],
        created_nodes: BTreeMap::new(),
        created_edges: BTreeMap::new(),
        produced_write: false,
    };
    let Some(read_prefix) = plan.read_prefix.as_ref() else {
        fill_missing_gql_create_expr_values(plan, &mut row, missing_expr_ids, graph_params)?;
        return Ok(row);
    };
    let mut value_index = 0usize;
    for column in &read_prefix.internal_columns {
        match column {
            GqlMutationInternalColumn::TargetId { alias, kind } => {
                let value = values.get(value_index).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "mutation read prefix returned fewer internal columns than planned"
                            .to_string(),
                    )
                })?;
                let id = gql_internal_id_value(value, alias)?;
                match kind {
                    GqlAliasKind::Node => {
                        row.read_nodes.insert(alias.clone(), id);
                    }
                    GqlAliasKind::Edge => {
                        row.read_edges.insert(alias.clone(), id);
                    }
                    GqlAliasKind::Path => {
                        return Err(EngineError::InvalidOperation(
                            "path aliases are not scalar mutation targets".to_string(),
                        ));
                    }
                }
                value_index += 1;
            }
            GqlMutationInternalColumn::TargetPath { alias } => {
                let node_value = values.get(value_index).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "mutation read prefix returned fewer path node columns than planned"
                            .to_string(),
                    )
                })?;
                let edge_value = values.get(value_index + 1).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "mutation read prefix returned fewer path edge columns than planned"
                            .to_string(),
                    )
                })?;
                let identity = gql_internal_path_identity(node_value, edge_value, alias)?;
                row.read_paths.insert(alias.clone(), identity);
                value_index += 2;
            }
            GqlMutationInternalColumn::ExprValue { id, .. } => {
                let value = values.get(value_index).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "mutation read prefix returned fewer expression columns than planned"
                            .to_string(),
                    )
                })?;
                if let Some(slot) = row.expr_values.get_mut(*id) {
                    *slot = Some(value.clone());
                }
                value_index += 1;
            }
        }
    }
    fill_missing_gql_create_expr_values(plan, &mut row, missing_expr_ids, graph_params)?;
    Ok(row)
}

fn gql_create_missing_operation_expr_ids(plan: &GqlMutationPlan) -> Vec<usize> {
    let mut supplied_by_read_prefix = BTreeSet::new();
    if let Some(read_prefix) = plan.read_prefix.as_ref() {
        for column in &read_prefix.internal_columns {
            if let GqlMutationInternalColumn::ExprValue { id, .. } = column {
                supplied_by_read_prefix.insert(*id);
            }
        }
    }
    plan.operation_exprs
        .iter()
        .filter_map(|expr| {
            if supplied_by_read_prefix.contains(&expr.id) {
                None
            } else {
                Some(expr.id)
            }
        })
        .collect()
}

fn fill_missing_gql_create_expr_values(
    plan: &GqlMutationPlan,
    row: &mut GqlCreateExecutionRow,
    missing_expr_ids: &[usize],
    graph_params: &BTreeMap<String, GraphParamValue>,
) -> Result<(), EngineError> {
    if missing_expr_ids.iter().all(|id| {
        row.expr_values
            .get(*id)
            .is_some_and(|value| value.is_some())
    }) {
        return Ok(());
    }
    let schema = GraphBindingSchema::new();
    let empty_row = schema.empty_row();
    let context = GraphEvalContext {
        schema: &schema,
        row: &empty_row,
        params: graph_params,
    };
    for &expr_id in missing_expr_ids {
        if row
            .expr_values
            .get(expr_id)
            .is_some_and(|value| value.is_some())
        {
            continue;
        }
        let expr = plan.operation_exprs.get(expr_id).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "mutation operation expression id {expr_id} is not available"
            ))
        })?;
        let value = eval_graph_expr(&expr.expr, &context)?;
        row.expr_values[expr.id] = Some(graph_eval_value_to_graph_value(value)?);
    }
    Ok(())
}

fn materialize_gql_create(
    plan: &GqlMutationPlan,
    mut rows: Vec<GqlCreateExecutionRow>,
    edge_uniqueness: bool,
    max_mutation_ops: usize,
    default_valid_from: i64,
    snapshot: &ReadView,
) -> Result<GqlCreateMaterialization, EngineError> {
    let (existing_node_ids, existing_edge_ids) =
        collect_gql_existing_update_targets(plan, &rows);
    let mut existing_nodes = hydrate_gql_existing_node_targets(snapshot, &existing_node_ids)?;
    let mut existing_edges = hydrate_gql_existing_edge_targets(snapshot, &existing_edge_ids)?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut edge_precheck_triples = BTreeSet::new();
    let mut seen_edge_triples = BTreeSet::new();
    let mut target_applications: BTreeMap<GqlMutationTargetKey, usize> = BTreeMap::new();
    let mut skipped_null_targets = 0usize;
    let mut first_existing_node_update_order = Vec::new();
    let mut first_existing_edge_update_order = Vec::new();
    let mut seen_existing_node_updates = BTreeSet::new();
    let mut seen_existing_edge_updates = BTreeSet::new();
    let mut existing_node_deletes = BTreeSet::new();
    let mut direct_existing_edge_deletes = BTreeSet::new();
    let mut created_node_deletes = BTreeSet::new();
    let mut direct_created_edge_deletes = BTreeSet::new();
    let mut op_budget = GqlMaterializationOpBudget::new(max_mutation_ops);

    for (row_index, row) in rows.iter_mut().enumerate() {
        for clause in &plan.clauses {
            match clause {
                GqlMutationClausePlan::Create(patterns) => {
                    for (pattern_index, pattern) in patterns.iter().enumerate() {
                        if gql_create_pattern_has_null_read_endpoint(plan, pattern, row) {
                            skipped_null_targets += 1;
                            continue;
                        }
                        materialize_gql_create_pattern(
                            pattern,
                            row,
                            row_index,
                            pattern_index,
                            default_valid_from,
                            &mut nodes,
                            &mut edges,
                            &mut edge_precheck_triples,
                            &mut seen_edge_triples,
                            edge_uniqueness,
                            &mut op_budget,
                        )?;
                    }
                }
                GqlMutationClausePlan::Set(items) => {
                    apply_gql_set_items(
                        plan,
                        items,
                        row,
                        &mut nodes,
                        &mut edges,
                        &mut existing_nodes,
                        &mut existing_edges,
                        &mut target_applications,
                        &mut skipped_null_targets,
                        &mut first_existing_node_update_order,
                        &mut first_existing_edge_update_order,
                        &mut seen_existing_node_updates,
                        &mut seen_existing_edge_updates,
                    )?;
                }
                GqlMutationClausePlan::Remove(items) => {
                    apply_gql_remove_items(
                        plan,
                        items,
                        row,
                        &mut nodes,
                        &mut edges,
                        &mut existing_nodes,
                        &mut existing_edges,
                        &mut target_applications,
                        &mut skipped_null_targets,
                        &mut first_existing_node_update_order,
                        &mut first_existing_edge_update_order,
                        &mut seen_existing_node_updates,
                        &mut seen_existing_edge_updates,
                    )?;
                }
                GqlMutationClausePlan::Delete { .. } => {
                    apply_gql_delete_targets(
                        clause,
                        row,
                        &mut target_applications,
                        &mut skipped_null_targets,
                        &mut existing_node_deletes,
                        &mut direct_existing_edge_deletes,
                        &mut created_node_deletes,
                        &mut direct_created_edge_deletes,
                        &mut op_budget,
                    )?;
                }
            }
        }
    }

    validate_gql_edge_update_windows(&edges, &existing_edges)?;
    let node_precheck_keys = build_gql_final_created_node_precheck_keys(&nodes)?;
    let cascade = plan_gql_detach_delete_cascades(
        snapshot,
        &nodes,
        &edges,
        &existing_node_deletes,
        &direct_existing_edge_deletes,
        &created_node_deletes,
        &direct_created_edge_deletes,
        &mut target_applications,
        &mut op_budget,
    )?;
    let existing_edge_deletes = direct_existing_edge_deletes
        .union(&cascade.existing_edges)
        .copied()
        .collect::<BTreeSet<_>>();
    let created_edge_deletes = direct_created_edge_deletes
        .union(&cascade.created_edges)
        .copied()
        .collect::<BTreeSet<_>>();
    let stats = compute_gql_mutation_stats(
        &nodes,
        &edges,
        &existing_nodes,
        &existing_edges,
        &existing_node_deletes,
        &existing_edge_deletes,
        &created_node_deletes,
        &created_edge_deletes,
        skipped_null_targets,
        &target_applications,
    );

    for row in &mut rows {
        row.produced_write = gql_row_produced_effective_write(
            row,
            &nodes,
            &edges,
            &existing_nodes,
            &existing_edges,
            &existing_node_deletes,
            &existing_edge_deletes,
            &created_node_deletes,
            &created_edge_deletes,
        );
    }
    let mutation_rows = rows.iter().filter(|row| row.produced_write).count();
    let mut intents = build_gql_create_intents(&nodes, &edges);
    intents.extend(build_gql_delete_intents(
        &nodes,
        &edges,
        &existing_node_deletes,
        &direct_existing_edge_deletes,
        &created_node_deletes,
        &direct_created_edge_deletes,
        &cascade.existing_edges,
        &cascade.created_edges,
    )?);
    let record_replacements = build_gql_record_replacements(
        &existing_nodes,
        &existing_edges,
        &first_existing_node_update_order,
        &first_existing_edge_update_order,
        &existing_node_deletes,
        &existing_edge_deletes,
        &mut op_budget,
    )?;
    let mutation_ops = nodes.len()
        + edges.len()
        + record_replacements.len()
        + existing_node_deletes.len()
        + created_node_deletes.len()
        + existing_edge_deletes.len()
        + created_edge_deletes.len();
    let db_hits = existing_node_ids
        .len()
        .saturating_add(existing_edge_ids.len())
        .saturating_add(existing_node_deletes.len())
        .saturating_add(existing_edge_deletes.len());
    if mutation_ops > max_mutation_ops {
        return Err(gql_mutation_cap_error(
            "max_mutation_ops",
            mutation_ops,
            max_mutation_ops,
        ));
    }

    Ok(GqlCreateMaterialization {
        rows,
        intents,
        record_replacements,
        nodes,
        edges,
        existing_nodes,
        existing_edges,
        node_precheck_keys,
        edge_precheck_triples,
        mutation_rows,
        mutation_ops,
        nodes_created: stats.nodes_created,
        nodes_updated: stats.nodes_updated,
        nodes_deleted: stats.nodes_deleted,
        edges_created: stats.edges_created,
        edges_updated: stats.edges_updated,
        edges_deleted: stats.edges_deleted,
        properties_set: stats.properties_set,
        properties_removed: stats.properties_removed,
        labels_added: stats.labels_added,
        labels_removed: stats.labels_removed,
        skipped_null_targets: stats.skipped_null_targets,
        duplicate_targets: stats.duplicate_targets,
        db_hits,
    })
}

#[derive(Default)]
struct GqlMutationComputedStats {
    nodes_created: usize,
    nodes_updated: usize,
    nodes_deleted: usize,
    edges_created: usize,
    edges_updated: usize,
    edges_deleted: usize,
    properties_set: usize,
    properties_removed: usize,
    labels_added: usize,
    labels_removed: usize,
    skipped_null_targets: usize,
    duplicate_targets: usize,
}

#[allow(clippy::too_many_arguments)]
fn materialize_gql_create_pattern(
    pattern: &GqlCreatePatternPlan,
    row: &mut GqlCreateExecutionRow,
    row_index: usize,
    pattern_index: usize,
    default_valid_from: i64,
    nodes: &mut Vec<GqlCreatedNodeExecution>,
    edges: &mut Vec<GqlCreatedEdgeExecution>,
    edge_precheck_triples: &mut BTreeSet<(u64, u64, String)>,
    seen_edge_triples: &mut BTreeSet<(GqlCreateEndpointKey, GqlCreateEndpointKey, String)>,
    edge_uniqueness: bool,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<(), EngineError> {
    for node in &pattern.nodes {
        if !node.created {
            continue;
        }
        op_budget.reserve(1)?;
        let local_alias = format!("__gql_create_node_{row_index}_{pattern_index}_{}", node.alias);
        let local = TxnLocalRef::Alias(local_alias);
        let created = materialize_gql_create_node(node, row, local)?;
        row.created_nodes.insert(node.alias.clone(), nodes.len());
        row.produced_write = true;
        nodes.push(created);
    }

    for (edge_index, edge) in pattern.edges.iter().enumerate() {
        let Some(from) = gql_create_node_ref_for_alias(row, &edge.from_alias, nodes)? else {
            continue;
        };
        let Some(to) = gql_create_node_ref_for_alias(row, &edge.to_alias, nodes)? else {
            continue;
        };
        let local = edge.alias.as_ref().map(|alias| {
            TxnLocalRef::Alias(format!(
                "__gql_create_edge_{row_index}_{pattern_index}_{edge_index}_{alias}"
            ))
        });
        op_budget.reserve(1)?;
        let created = materialize_gql_create_edge(
            edge,
            row,
            from.clone(),
            to.clone(),
            local.clone(),
                    default_valid_from,
        )?;
        if edge_uniqueness {
            let triple = (
                gql_create_endpoint_key(&from),
                gql_create_endpoint_key(&to),
                edge.label.clone(),
            );
            if !seen_edge_triples.insert(triple) {
                return Err(gql_create_conflict_error(format!(
                    "duplicate edge CREATE target ({:?}, {:?}, {}) in one statement",
                    from, to, edge.label
                )));
            }
            if let (TxnNodeRef::Id(from_id), TxnNodeRef::Id(to_id)) = (&from, &to) {
                edge_precheck_triples.insert((*from_id, *to_id, edge.label.clone()));
            }
        }
        if let (Some(alias), Some(_)) = (&created.alias, &created.local) {
            row.created_edges.insert(alias.clone(), edges.len());
        }
        row.produced_write = true;
        edges.push(created);
    }
    Ok(())
}

fn build_gql_final_created_node_precheck_keys(
    nodes: &[GqlCreatedNodeExecution],
) -> Result<BTreeSet<(String, String)>, EngineError> {
    let mut precheck_keys = BTreeSet::new();
    for node in nodes {
        for label in &node.labels {
            let key = (label.clone(), node.key.clone());
            if !precheck_keys.insert(key) {
                return Err(gql_create_conflict_error(format!(
                    "duplicate node CREATE target ({}, {}) in one statement",
                    label, node.key
                )));
            }
        }
    }
    Ok(precheck_keys)
}

fn collect_gql_existing_update_targets(
    plan: &GqlMutationPlan,
    rows: &[GqlCreateExecutionRow],
) -> (BTreeSet<u64>, BTreeSet<u64>) {
    let mut nodes = BTreeSet::new();
    let mut edges = BTreeSet::new();
    for row in rows {
        for clause in &plan.clauses {
            match clause {
                GqlMutationClausePlan::Set(items) => {
                    for item in items {
                        match item {
                            GqlSetItemPlan::Property { alias, kind, .. }
                            | GqlSetItemPlan::MapMerge { alias, kind, .. } => {
                                collect_gql_existing_update_target(
                                    plan, row, alias, *kind, &mut nodes, &mut edges,
                                );
                            }
                            GqlSetItemPlan::NodeLabel { alias, .. } => {
                                collect_gql_existing_update_target(
                                    plan,
                                    row,
                                    alias,
                                    GqlAliasKind::Node,
                                    &mut nodes,
                                    &mut edges,
                                );
                            }
                        }
                    }
                }
                GqlMutationClausePlan::Remove(items) => {
                    for item in items {
                        match item {
                            GqlRemoveItemPlan::Property { alias, kind, .. } => {
                                collect_gql_existing_update_target(
                                    plan, row, alias, *kind, &mut nodes, &mut edges,
                                );
                            }
                            GqlRemoveItemPlan::NodeLabel { alias, .. } => {
                                collect_gql_existing_update_target(
                                    plan,
                                    row,
                                    alias,
                                    GqlAliasKind::Node,
                                    &mut nodes,
                                    &mut edges,
                                );
                            }
                        }
                    }
                }
                GqlMutationClausePlan::Create(_) | GqlMutationClausePlan::Delete { .. } => {}
            }
        }
    }
    (nodes, edges)
}

fn collect_gql_existing_update_target(
    plan: &GqlMutationPlan,
    row: &GqlCreateExecutionRow,
    alias: &str,
    kind: GqlAliasKind,
    nodes: &mut BTreeSet<u64>,
    edges: &mut BTreeSet<u64>,
) {
    if plan
        .semantic
        .aliases
        .get(alias)
        .is_some_and(|binding| binding.origin != GqlAliasOrigin::ReadPrefix)
    {
        return;
    }
    match kind {
        GqlAliasKind::Node => {
            if let Some(Some(id)) = row.read_nodes.get(alias) {
                nodes.insert(*id);
            }
        }
        GqlAliasKind::Edge => {
            if let Some(Some(id)) = row.read_edges.get(alias) {
                edges.insert(*id);
            }
        }
        GqlAliasKind::Path => {}
    }
}

fn hydrate_gql_existing_node_targets(
    snapshot: &ReadView,
    node_ids: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, GqlExistingNodeExecution>, EngineError> {
    let ids: Vec<u64> = node_ids.iter().copied().collect();
    let records = snapshot.get_nodes_raw(&ids)?;
    ids.into_iter()
        .zip(records)
        .map(|(id, record)| {
            let record = record.ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GQL mutation target node {id} was not found in the transaction snapshot"
                ))
            })?;
            let labels = txn_labels_from_record(&record, snapshot.label_catalog.as_ref())?;
            Ok((
                id,
                GqlExistingNodeExecution {
                    props: record.props.clone(),
                    weight: record.weight,
                    dense_vector: record.dense_vector.clone(),
                    sparse_vector: record.sparse_vector.clone(),
                    original: record,
                    original_labels: labels.clone(),
                    labels,
                },
            ))
        })
        .collect()
}

fn hydrate_gql_existing_edge_targets(
    snapshot: &ReadView,
    edge_ids: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, GqlExistingEdgeExecution>, EngineError> {
    let ids: Vec<u64> = edge_ids.iter().copied().collect();
    let records = snapshot.get_edges(&ids)?;
    ids.into_iter()
        .zip(records)
        .map(|(id, record)| {
            let record = record.ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GQL mutation target edge {id} was not found in the transaction snapshot"
                ))
            })?;
            let label = snapshot
                .label_catalog
                .edge_label(record.label_id)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "edge record {id} references missing edge-label label_id {}",
                        record.label_id
                    ))
                })?
                .to_string();
            Ok((
                id,
                GqlExistingEdgeExecution {
                    props: record.props.clone(),
                    weight: record.weight,
                    valid_from: record.valid_from,
                    valid_to: record.valid_to,
                    original: record,
                    label,
                },
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn apply_gql_delete_targets(
    clause: &GqlMutationClausePlan,
    row: &mut GqlCreateExecutionRow,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    existing_node_deletes: &mut BTreeSet<u64>,
    existing_edge_deletes: &mut BTreeSet<u64>,
    created_node_deletes: &mut BTreeSet<usize>,
    created_edge_deletes: &mut BTreeSet<usize>,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<(), EngineError> {
    let GqlMutationClausePlan::Delete { detach, targets } = clause else {
        return Err(EngineError::InvalidOperation(
            "GQL delete materialization received a non-delete clause".to_string(),
        ));
    };
    for target in targets {
        let Some(resolved) =
            gql_delete_target_for_alias(row, &target.alias, target.kind, skipped_null_targets)?
        else {
            continue;
        };
        *target_applications.entry(resolved.clone()).or_default() += 1;
        match (*detach, resolved) {
            (true, GqlMutationTargetKey::CreatedNode(index)) => {
                if !created_node_deletes.contains(&index) {
                    op_budget.reserve(1)?;
                }
                if created_node_deletes.insert(index) {
                    row.produced_write = true;
                }
            }
            (true, GqlMutationTargetKey::ExistingNode(id)) => {
                if !existing_node_deletes.contains(&id) {
                    op_budget.reserve(1)?;
                }
                if existing_node_deletes.insert(id) {
                    row.produced_write = true;
                }
            }
            (false, GqlMutationTargetKey::CreatedEdge(index)) => {
                if !created_edge_deletes.contains(&index) {
                    op_budget.reserve(1)?;
                }
                if created_edge_deletes.insert(index) {
                    row.produced_write = true;
                }
            }
            (false, GqlMutationTargetKey::ExistingEdge(id)) => {
                if !existing_edge_deletes.contains(&id) {
                    op_budget.reserve(1)?;
                }
                if existing_edge_deletes.insert(id) {
                    row.produced_write = true;
                }
            }
            (true, GqlMutationTargetKey::CreatedEdge(_))
            | (true, GqlMutationTargetKey::ExistingEdge(_))
            | (false, GqlMutationTargetKey::CreatedNode(_))
            | (false, GqlMutationTargetKey::ExistingNode(_)) => {
                return Err(EngineError::InvalidOperation(
                    "GQL delete target kind passed semantic validation but was incompatible at execution".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn gql_delete_target_for_alias(
    row: &GqlCreateExecutionRow,
    alias: &str,
    kind: GqlAliasKind,
    skipped_null_targets: &mut usize,
) -> Result<Option<GqlMutationTargetKey>, EngineError> {
    match kind {
        GqlAliasKind::Node => {
            if let Some(&index) = row.created_nodes.get(alias) {
                return Ok(Some(GqlMutationTargetKey::CreatedNode(index)));
            }
            let Some(id) = row.read_nodes.get(alias) else {
                return Err(EngineError::InvalidOperation(format!(
                    "GQL DELETE node target alias '{alias}' was not materialized"
                )));
            };
            let Some(id) = id else {
                *skipped_null_targets += 1;
                return Ok(None);
            };
            Ok(Some(GqlMutationTargetKey::ExistingNode(*id)))
        }
        GqlAliasKind::Edge => {
            if let Some(&index) = row.created_edges.get(alias) {
                return Ok(Some(GqlMutationTargetKey::CreatedEdge(index)));
            }
            let Some(id) = row.read_edges.get(alias) else {
                return Err(EngineError::InvalidOperation(format!(
                    "GQL DELETE edge target alias '{alias}' was not materialized"
                )));
            };
            let Some(id) = id else {
                *skipped_null_targets += 1;
                return Ok(None);
            };
            Ok(Some(GqlMutationTargetKey::ExistingEdge(*id)))
        }
        GqlAliasKind::Path => Err(EngineError::InvalidOperation(
            "path aliases are not scalar mutation targets".to_string(),
        )),
    }
}

#[derive(Default)]
struct GqlDetachCascadePlan {
    existing_edges: BTreeSet<u64>,
    created_edges: BTreeSet<usize>,
}

struct GqlMaterializationOpBudget {
    max_ops: usize,
    ops: usize,
}

impl GqlMaterializationOpBudget {
    fn new(max_ops: usize) -> Self {
        Self { max_ops, ops: 0 }
    }

    fn reserve(&mut self, count: usize) -> Result<(), EngineError> {
        if count == 0 {
            return Ok(());
        }
        let next = self.ops.saturating_add(count);
        if next > self.max_ops {
            return Err(gql_mutation_cap_error(
                "max_mutation_ops",
                next,
                self.max_ops,
            ));
        }
        self.ops = next;
        Ok(())
    }

    fn remaining(&self) -> usize {
        self.max_ops.saturating_sub(self.ops)
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_gql_detach_delete_cascades(
    snapshot: &ReadView,
    nodes: &[GqlCreatedNodeExecution],
    edges: &[GqlCreatedEdgeExecution],
    existing_node_deletes: &BTreeSet<u64>,
    direct_existing_edge_deletes: &BTreeSet<u64>,
    created_node_deletes: &BTreeSet<usize>,
    direct_created_edge_deletes: &BTreeSet<usize>,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<GqlDetachCascadePlan, EngineError> {
    let mut plan = GqlDetachCascadePlan::default();
    let deleted_created_node_locals = created_node_deletes
        .iter()
        .filter_map(|index| nodes.get(*index).map(|node| node.local.clone()))
        .collect::<BTreeSet<_>>();
    for (edge_index, edge) in edges.iter().enumerate() {
        if gql_created_edge_incident_to_deleted_node(
            edge,
            existing_node_deletes,
            &deleted_created_node_locals,
        ) {
            plan.created_edges.insert(edge_index);
            *target_applications
                .entry(GqlMutationTargetKey::CreatedEdge(edge_index))
                .or_default() += 1;
            if !direct_created_edge_deletes.contains(&edge_index) {
                op_budget.reserve(1)?;
            }
        }
    }

    let existing_node_ids = existing_node_deletes.iter().copied().collect::<Vec<_>>();
    let existing_scan_limit = op_budget
        .remaining()
        .saturating_add(direct_existing_edge_deletes.len())
        .saturating_add(1);
    for edge_id in
        snapshot.txn_delete_incident_edge_ids_limited(&existing_node_ids, existing_scan_limit)?
    {
        let inserted = plan.existing_edges.insert(edge_id);
        *target_applications
            .entry(GqlMutationTargetKey::ExistingEdge(edge_id))
            .or_default() += 1;
        if inserted && !direct_existing_edge_deletes.contains(&edge_id) {
            op_budget.reserve(1)?;
        }
    }
    Ok(plan)
}

fn gql_created_edge_incident_to_deleted_node(
    edge: &GqlCreatedEdgeExecution,
    existing_node_deletes: &BTreeSet<u64>,
    deleted_created_node_locals: &BTreeSet<TxnLocalRef>,
) -> bool {
    gql_node_ref_matches_deleted_node(
        &edge.from,
        existing_node_deletes,
        deleted_created_node_locals,
    ) || gql_node_ref_matches_deleted_node(
        &edge.to,
        existing_node_deletes,
        deleted_created_node_locals,
    )
}

fn gql_node_ref_matches_deleted_node(
    target: &TxnNodeRef,
    existing_node_deletes: &BTreeSet<u64>,
    deleted_created_node_locals: &BTreeSet<TxnLocalRef>,
) -> bool {
    match target {
        TxnNodeRef::Id(id) => existing_node_deletes.contains(id),
        TxnNodeRef::Local(local) => deleted_created_node_locals.contains(local),
        TxnNodeRef::Key { .. } => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_gql_set_items(
    plan: &GqlMutationPlan,
    items: &[GqlSetItemPlan],
    row: &mut GqlCreateExecutionRow,
    nodes: &mut [GqlCreatedNodeExecution],
    edges: &mut [GqlCreatedEdgeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    first_existing_node_update_order: &mut Vec<u64>,
    first_existing_edge_update_order: &mut Vec<u64>,
    seen_existing_node_updates: &mut BTreeSet<u64>,
    seen_existing_edge_updates: &mut BTreeSet<u64>,
) -> Result<(), EngineError> {
    for item in items {
        match item {
            GqlSetItemPlan::Property {
                alias,
                kind,
                property,
                value,
            } => {
                let value = gql_create_expr_value(row, value.id)?.clone();
                let Some(target) = gql_mutation_target_for_alias(
                    row,
                    alias,
                    *kind,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?
                else {
                    continue;
                };
                if apply_gql_set_property(
                    target,
                    property,
                    &value,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )? {
                    row.produced_write = true;
                }
            }
            GqlSetItemPlan::MapMerge { alias, kind, value } => {
                let value = gql_create_expr_value(row, value.id)?.clone();
                let Some(target) = gql_mutation_target_for_alias(
                    row,
                    alias,
                    *kind,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?
                else {
                    continue;
                };
                if apply_gql_map_merge(
                    target,
                    &value,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )? {
                    row.produced_write = true;
                }
            }
            GqlSetItemPlan::NodeLabel { alias, label } => {
                let Some(target) = gql_mutation_target_for_alias(
                    row,
                    alias,
                    GqlAliasKind::Node,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?
                else {
                    continue;
                };
                if apply_gql_add_node_label(target, label, nodes, existing_nodes)? {
                    row.produced_write = true;
                }
            }
        }
    }
    let _ = plan;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_gql_remove_items(
    plan: &GqlMutationPlan,
    items: &[GqlRemoveItemPlan],
    row: &mut GqlCreateExecutionRow,
    nodes: &mut [GqlCreatedNodeExecution],
    edges: &mut [GqlCreatedEdgeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    first_existing_node_update_order: &mut Vec<u64>,
    first_existing_edge_update_order: &mut Vec<u64>,
    seen_existing_node_updates: &mut BTreeSet<u64>,
    seen_existing_edge_updates: &mut BTreeSet<u64>,
) -> Result<(), EngineError> {
    for item in items {
        match item {
            GqlRemoveItemPlan::Property {
                alias,
                kind,
                property,
            } => {
                let Some(target) = gql_mutation_target_for_alias(
                    row,
                    alias,
                    *kind,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?
                else {
                    continue;
                };
                if apply_gql_remove_property(
                    target,
                    property,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )? {
                    row.produced_write = true;
                }
            }
            GqlRemoveItemPlan::NodeLabel { alias, label } => {
                let Some(target) = gql_mutation_target_for_alias(
                    row,
                    alias,
                    GqlAliasKind::Node,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?
                else {
                    continue;
                };
                if apply_gql_remove_node_label(target, label, nodes, existing_nodes)? {
                    row.produced_write = true;
                }
            }
        }
    }
    let _ = plan;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn gql_mutation_target_for_alias(
    row: &GqlCreateExecutionRow,
    alias: &str,
    kind: GqlAliasKind,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    first_existing_node_update_order: &mut Vec<u64>,
    first_existing_edge_update_order: &mut Vec<u64>,
    seen_existing_node_updates: &mut BTreeSet<u64>,
    seen_existing_edge_updates: &mut BTreeSet<u64>,
) -> Result<Option<GqlMutationTargetKey>, EngineError> {
    let target = match kind {
        GqlAliasKind::Node => {
            if let Some(&index) = row.created_nodes.get(alias) {
                GqlMutationTargetKey::CreatedNode(index)
            } else {
                let Some(id) = row.read_nodes.get(alias) else {
                    return Err(EngineError::InvalidOperation(format!(
                        "GQL mutation node target alias '{alias}' was not materialized"
                    )));
                };
                let Some(id) = id else {
                    *skipped_null_targets += 1;
                    return Ok(None);
                };
                if seen_existing_node_updates.insert(*id) {
                    first_existing_node_update_order.push(*id);
                }
                GqlMutationTargetKey::ExistingNode(*id)
            }
        }
        GqlAliasKind::Edge => {
            if let Some(&index) = row.created_edges.get(alias) {
                GqlMutationTargetKey::CreatedEdge(index)
            } else {
                let Some(id) = row.read_edges.get(alias) else {
                    return Err(EngineError::InvalidOperation(format!(
                        "GQL mutation edge target alias '{alias}' was not materialized"
                    )));
                };
                let Some(id) = id else {
                    *skipped_null_targets += 1;
                    return Ok(None);
                };
                if seen_existing_edge_updates.insert(*id) {
                    first_existing_edge_update_order.push(*id);
                }
                GqlMutationTargetKey::ExistingEdge(*id)
            }
        }
        GqlAliasKind::Path => {
            return Err(EngineError::InvalidOperation(
                "path aliases are not scalar mutation targets".to_string(),
            ));
        }
    };
    *target_applications.entry(target.clone()).or_default() += 1;
    Ok(Some(target))
}

fn apply_gql_set_property(
    target: GqlMutationTargetKey,
    property: &str,
    value: &GraphValue,
    nodes: &mut [GqlCreatedNodeExecution],
    edges: &mut [GqlCreatedEdgeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
) -> Result<bool, EngineError> {
    match target {
        GqlMutationTargetKey::CreatedNode(index) => {
            let node = nodes.get_mut(index).ok_or_else(gql_missing_created_target)?;
            apply_gql_set_node_property(&mut node.props, &mut node.weight, property, value)
        }
        GqlMutationTargetKey::ExistingNode(id) => {
            let node = existing_nodes.get_mut(&id).ok_or_else(gql_missing_existing_target)?;
            apply_gql_set_node_property(&mut node.props, &mut node.weight, property, value)
        }
        GqlMutationTargetKey::CreatedEdge(index) => {
            let edge = edges.get_mut(index).ok_or_else(gql_missing_created_target)?;
            apply_gql_set_edge_property(
                &mut edge.props,
                &mut edge.weight,
                edge.valid_from.get_or_insert(0),
                edge.valid_to.get_or_insert(i64::MAX),
                property,
                value,
            )
        }
        GqlMutationTargetKey::ExistingEdge(id) => {
            let edge = existing_edges.get_mut(&id).ok_or_else(gql_missing_existing_target)?;
            apply_gql_set_edge_property(
                &mut edge.props,
                &mut edge.weight,
                &mut edge.valid_from,
                &mut edge.valid_to,
                property,
                value,
            )
        }
    }
}

fn apply_gql_set_node_property(
    props: &mut BTreeMap<String, PropValue>,
    weight: &mut f32,
    property: &str,
    value: &GraphValue,
) -> Result<bool, EngineError> {
    if property == "weight" {
        let next = gql_mutation_weight(value, "node weight")?;
        let changed = *weight != next;
        *weight = next;
        return Ok(changed);
    }
    gql_set_stored_property(props, property, value)
}

fn apply_gql_set_edge_property(
    props: &mut BTreeMap<String, PropValue>,
    weight: &mut f32,
    valid_from: &mut i64,
    valid_to: &mut i64,
    property: &str,
    value: &GraphValue,
) -> Result<bool, EngineError> {
    match property {
        "weight" => {
            let next = gql_mutation_weight(value, "edge weight")?;
            let changed = *weight != next;
            *weight = next;
            Ok(changed)
        }
        "valid_from" => {
            let next = gql_mutation_i64(value, "valid_from")?;
            let changed = *valid_from != next;
            *valid_from = next;
            Ok(changed)
        }
        "valid_to" => {
            let next = gql_mutation_i64(value, "valid_to")?;
            let changed = *valid_to != next;
            *valid_to = next;
            Ok(changed)
        }
        _ => gql_set_stored_property(props, property, value),
    }
}

fn gql_set_stored_property(
    props: &mut BTreeMap<String, PropValue>,
    property: &str,
    value: &GraphValue,
) -> Result<bool, EngineError> {
    if matches!(value, GraphValue::Null) {
        return Ok(props.remove(property).is_some());
    }
    let prop = gql_graph_value_to_prop(value)?;
    let changed = props.get(property) != Some(&prop);
    props.insert(property.to_string(), prop);
    Ok(changed)
}

fn apply_gql_map_merge(
    target: GqlMutationTargetKey,
    value: &GraphValue,
    nodes: &mut [GqlCreatedNodeExecution],
    edges: &mut [GqlCreatedEdgeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
) -> Result<bool, EngineError> {
    let GraphValue::Map(values) = value else {
        return Err(gql_create_invalid_value("GQL SET += requires a map value"));
    };
    match target {
        GqlMutationTargetKey::CreatedNode(index) => {
            let node = nodes.get_mut(index).ok_or_else(gql_missing_created_target)?;
            reject_reserved_gql_map_merge_keys(GqlAliasKind::Node, values)?;
            gql_merge_stored_properties(&mut node.props, values)
        }
        GqlMutationTargetKey::ExistingNode(id) => {
            let node = existing_nodes.get_mut(&id).ok_or_else(gql_missing_existing_target)?;
            reject_reserved_gql_map_merge_keys(GqlAliasKind::Node, values)?;
            gql_merge_stored_properties(&mut node.props, values)
        }
        GqlMutationTargetKey::CreatedEdge(index) => {
            let edge = edges.get_mut(index).ok_or_else(gql_missing_created_target)?;
            reject_reserved_gql_map_merge_keys(GqlAliasKind::Edge, values)?;
            gql_merge_stored_properties(&mut edge.props, values)
        }
        GqlMutationTargetKey::ExistingEdge(id) => {
            let edge = existing_edges.get_mut(&id).ok_or_else(gql_missing_existing_target)?;
            reject_reserved_gql_map_merge_keys(GqlAliasKind::Edge, values)?;
            gql_merge_stored_properties(&mut edge.props, values)
        }
    }
}

fn reject_reserved_gql_map_merge_keys(
    kind: GqlAliasKind,
    values: &BTreeMap<String, GraphValue>,
) -> Result<(), EngineError> {
    for key in values.keys() {
        let reserved = match kind {
            GqlAliasKind::Node => matches!(
                key.as_str(),
                "id"
                    | "labels"
                    | "key"
                    | "created_at"
                    | "updated_at"
                    | "dense_vector"
                    | "sparse_vector"
            ),
            GqlAliasKind::Edge => matches!(
                key.as_str(),
                "id" | "from" | "to" | "label" | "type" | "created_at" | "updated_at"
            ),
            GqlAliasKind::Path => true,
        };
        if reserved {
            return Err(EngineError::InvalidOperation(format!(
                "SET += map key '{key}' is reserved metadata"
            )));
        }
    }
    Ok(())
}

fn gql_merge_stored_properties(
    props: &mut BTreeMap<String, PropValue>,
    values: &BTreeMap<String, GraphValue>,
) -> Result<bool, EngineError> {
    let mut changed = false;
    for (key, value) in values {
        if matches!(value, GraphValue::Null) {
            changed |= props.remove(key).is_some();
            continue;
        }
        let prop = gql_graph_value_to_prop(value)?;
        changed |= props.get(key) != Some(&prop);
        props.insert(key.clone(), prop);
    }
    Ok(changed)
}

fn apply_gql_remove_property(
    target: GqlMutationTargetKey,
    property: &str,
    nodes: &mut [GqlCreatedNodeExecution],
    edges: &mut [GqlCreatedEdgeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
) -> Result<bool, EngineError> {
    match target {
        GqlMutationTargetKey::CreatedNode(index) => {
            let node = nodes.get_mut(index).ok_or_else(gql_missing_created_target)?;
            Ok(node.props.remove(property).is_some())
        }
        GqlMutationTargetKey::ExistingNode(id) => {
            let node = existing_nodes.get_mut(&id).ok_or_else(gql_missing_existing_target)?;
            Ok(node.props.remove(property).is_some())
        }
        GqlMutationTargetKey::CreatedEdge(index) => {
            let edge = edges.get_mut(index).ok_or_else(gql_missing_created_target)?;
            Ok(edge.props.remove(property).is_some())
        }
        GqlMutationTargetKey::ExistingEdge(id) => {
            let edge = existing_edges.get_mut(&id).ok_or_else(gql_missing_existing_target)?;
            Ok(edge.props.remove(property).is_some())
        }
    }
}

fn apply_gql_add_node_label(
    target: GqlMutationTargetKey,
    label: &str,
    nodes: &mut [GqlCreatedNodeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
) -> Result<bool, EngineError> {
    let labels = match target {
        GqlMutationTargetKey::CreatedNode(index) => {
            &mut nodes.get_mut(index).ok_or_else(gql_missing_created_target)?.labels
        }
        GqlMutationTargetKey::ExistingNode(id) => {
            &mut existing_nodes
                .get_mut(&id)
                .ok_or_else(gql_missing_existing_target)?
                .labels
        }
        GqlMutationTargetKey::CreatedEdge(_) | GqlMutationTargetKey::ExistingEdge(_) => {
            return Err(EngineError::InvalidOperation(
                "SET node labels require a node target".to_string(),
            ));
        }
    };
    if labels.iter().any(|existing| existing == label) {
        return Ok(false);
    }
    labels.push(label.to_string());
    Ok(true)
}

fn apply_gql_remove_node_label(
    target: GqlMutationTargetKey,
    label: &str,
    nodes: &mut [GqlCreatedNodeExecution],
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
) -> Result<bool, EngineError> {
    let labels = match target {
        GqlMutationTargetKey::CreatedNode(index) => {
            &mut nodes.get_mut(index).ok_or_else(gql_missing_created_target)?.labels
        }
        GqlMutationTargetKey::ExistingNode(id) => {
            &mut existing_nodes
                .get_mut(&id)
                .ok_or_else(gql_missing_existing_target)?
                .labels
        }
        GqlMutationTargetKey::CreatedEdge(_) | GqlMutationTargetKey::ExistingEdge(_) => {
            return Err(EngineError::InvalidOperation(
                "REMOVE node labels require a node target".to_string(),
            ));
        }
    };
    if !labels.iter().any(|existing| existing == label) {
        return Ok(false);
    }
    if labels.len() == 1 {
        return Err(EngineError::InvalidOperation(
            "cannot remove the last node label".to_string(),
        ));
    }
    labels.retain(|existing| existing != label);
    Ok(true)
}

fn validate_gql_edge_update_windows(
    created_edges: &[GqlCreatedEdgeExecution],
    existing_edges: &BTreeMap<u64, GqlExistingEdgeExecution>,
) -> Result<(), EngineError> {
    for edge in created_edges {
        let valid_from = edge.valid_from.unwrap_or(0);
        let valid_to = edge.valid_to.unwrap_or(i64::MAX);
        if valid_from >= valid_to {
            return Err(gql_create_invalid_value(
                "GQL SET edge validity window requires valid_from < valid_to",
            ));
        }
    }
    for edge in existing_edges.values() {
        if edge.valid_from >= edge.valid_to {
            return Err(gql_create_invalid_value(
                "GQL SET edge validity window requires valid_from < valid_to",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compute_gql_mutation_stats(
    created_nodes: &[GqlCreatedNodeExecution],
    created_edges: &[GqlCreatedEdgeExecution],
    existing_nodes: &BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &BTreeMap<u64, GqlExistingEdgeExecution>,
    existing_node_deletes: &BTreeSet<u64>,
    existing_edge_deletes: &BTreeSet<u64>,
    created_node_deletes: &BTreeSet<usize>,
    created_edge_deletes: &BTreeSet<usize>,
    skipped_null_targets: usize,
    target_applications: &BTreeMap<GqlMutationTargetKey, usize>,
) -> GqlMutationComputedStats {
    let mut stats = GqlMutationComputedStats {
        skipped_null_targets,
        duplicate_targets: target_applications
            .values()
            .map(|count| count.saturating_sub(1))
            .sum(),
        ..GqlMutationComputedStats::default()
    };
    stats.nodes_created = created_nodes
        .len()
        .saturating_sub(created_node_deletes.len());
    stats.edges_created = created_edges
        .len()
        .saturating_sub(created_edge_deletes.len());
    stats.nodes_deleted = existing_node_deletes.len();
    stats.edges_deleted = existing_edge_deletes.len();
    for (index, node) in created_nodes.iter().enumerate() {
        if created_node_deletes.contains(&index) {
            continue;
        }
        stats.labels_added += node.labels.len();
        stats.properties_set += node.props.len();
    }
    for (index, edge) in created_edges.iter().enumerate() {
        if created_edge_deletes.contains(&index) {
            continue;
        }
        stats.properties_set += edge.props.len();
    }
    for (id, node) in existing_nodes {
        if existing_node_deletes.contains(id) {
            continue;
        }
        if gql_existing_node_changed(node) {
            stats.nodes_updated += 1;
        }
        stats.labels_added += node
            .labels
            .iter()
            .filter(|label| !node.original_labels.iter().any(|original| original == *label))
            .count();
        stats.labels_removed += node
            .original_labels
            .iter()
            .filter(|label| !node.labels.iter().any(|current| current == *label))
            .count();
        stats.properties_set += node
            .props
            .iter()
            .filter(|(key, value)| node.original.props.get(*key) != Some(*value))
            .count();
        stats.properties_removed += node
            .original
            .props
            .keys()
            .filter(|key| !node.props.contains_key(*key))
            .count();
    }
    for (id, edge) in existing_edges {
        if existing_edge_deletes.contains(id) {
            continue;
        }
        if gql_existing_edge_changed(edge) {
            stats.edges_updated += 1;
        }
        stats.properties_set += edge
            .props
            .iter()
            .filter(|(key, value)| edge.original.props.get(*key) != Some(*value))
            .count();
        stats.properties_removed += edge
            .original
            .props
            .keys()
            .filter(|key| !edge.props.contains_key(*key))
            .count();
    }
    stats
}

fn gql_existing_node_changed(node: &GqlExistingNodeExecution) -> bool {
    node.props != node.original.props
        || node.weight != node.original.weight
        || node.dense_vector != node.original.dense_vector
        || node.sparse_vector != node.original.sparse_vector
        || !gql_label_name_sets_equal(&node.labels, &node.original_labels)
}

fn gql_label_name_sets_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|label| right.iter().any(|other| other == label))
}

fn gql_existing_edge_changed(edge: &GqlExistingEdgeExecution) -> bool {
    edge.props != edge.original.props
        || edge.weight != edge.original.weight
        || edge.valid_from != edge.original.valid_from
        || edge.valid_to != edge.original.valid_to
}

#[allow(clippy::too_many_arguments)]
fn gql_row_produced_effective_write(
    row: &GqlCreateExecutionRow,
    nodes: &[GqlCreatedNodeExecution],
    edges: &[GqlCreatedEdgeExecution],
    existing_nodes: &BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &BTreeMap<u64, GqlExistingEdgeExecution>,
    existing_node_deletes: &BTreeSet<u64>,
    existing_edge_deletes: &BTreeSet<u64>,
    created_node_deletes: &BTreeSet<usize>,
    created_edge_deletes: &BTreeSet<usize>,
) -> bool {
    row.created_nodes.values().any(|&index| nodes.get(index).is_some())
        || row.created_edges.values().any(|&index| edges.get(index).is_some())
        || row
            .read_nodes
            .values()
            .flatten()
            .any(|id| {
                existing_node_deletes.contains(id)
                    || existing_nodes
                        .get(id)
                        .is_some_and(|node| !existing_node_deletes.contains(id) && gql_existing_node_changed(node))
            })
        || row
            .read_edges
            .values()
            .flatten()
            .any(|id| {
                existing_edge_deletes.contains(id)
                    || existing_edges
                        .get(id)
                        .is_some_and(|edge| !existing_edge_deletes.contains(id) && gql_existing_edge_changed(edge))
            })
        || row
            .created_nodes
            .values()
            .any(|index| created_node_deletes.contains(index))
        || row
            .created_edges
            .values()
            .any(|index| created_edge_deletes.contains(index))
}

fn build_gql_create_intents(
    nodes: &[GqlCreatedNodeExecution],
    edges: &[GqlCreatedEdgeExecution],
) -> Vec<TxnIntent> {
    let mut intents = Vec::with_capacity(nodes.len() + edges.len());
    for node in nodes {
        intents.push(TxnIntent::UpsertNode {
            alias: Some(match &node.local {
                TxnLocalRef::Alias(alias) => alias.clone(),
                TxnLocalRef::Slot(_) => unreachable!("GQL CREATE uses alias locals"),
            }),
            labels: node.labels.clone(),
            key: node.key.clone(),
            options: UpsertNodeOptions {
                props: node.props.clone(),
                weight: node.weight,
                dense_vector: None,
                sparse_vector: None,
            },
        });
    }
    for edge in edges {
        intents.push(TxnIntent::UpsertEdge {
            alias: edge.local.as_ref().map(|local| match local {
                TxnLocalRef::Alias(alias) => alias.clone(),
                TxnLocalRef::Slot(_) => unreachable!("GQL CREATE uses alias locals"),
            }),
            from: edge.from.clone(),
            to: edge.to.clone(),
            label: edge.label.clone(),
            options: UpsertEdgeOptions {
                props: edge.props.clone(),
                weight: edge.weight,
                valid_from: edge.valid_from,
                valid_to: edge.valid_to,
            },
        });
    }
    intents
}

#[allow(clippy::too_many_arguments)]
fn build_gql_delete_intents(
    nodes: &[GqlCreatedNodeExecution],
    edges: &[GqlCreatedEdgeExecution],
    existing_node_deletes: &BTreeSet<u64>,
    direct_existing_edge_deletes: &BTreeSet<u64>,
    created_node_deletes: &BTreeSet<usize>,
    direct_created_edge_deletes: &BTreeSet<usize>,
    cascade_existing_edge_deletes: &BTreeSet<u64>,
    cascade_created_edge_deletes: &BTreeSet<usize>,
) -> Result<Vec<TxnIntent>, EngineError> {
    let mut intents = Vec::new();
    for id in direct_existing_edge_deletes {
        if cascade_existing_edge_deletes.contains(id) {
            continue;
        }
        intents.push(TxnIntent::DeleteEdge {
            target: TxnEdgeRef::Id(*id),
        });
    }
    for index in direct_created_edge_deletes {
        if cascade_created_edge_deletes.contains(index) {
            continue;
        }
        let edge = edges.get(*index).ok_or_else(gql_missing_created_target)?;
        let local = edge.local.clone().ok_or_else(|| {
            EngineError::InvalidOperation(
                "GQL DELETE of a created edge requires an aliased transaction local".to_string(),
            )
        })?;
        intents.push(TxnIntent::DeleteEdge {
            target: TxnEdgeRef::Local(local),
        });
    }
    for id in existing_node_deletes {
        intents.push(TxnIntent::DeleteNode {
            target: TxnNodeRef::Id(*id),
        });
    }
    for index in created_node_deletes {
        let node = nodes.get(*index).ok_or_else(gql_missing_created_target)?;
        intents.push(TxnIntent::DeleteNode {
            target: TxnNodeRef::Local(node.local.clone()),
        });
    }
    Ok(intents)
}

fn build_gql_record_replacements(
    existing_nodes: &BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &BTreeMap<u64, GqlExistingEdgeExecution>,
    node_order: &[u64],
    edge_order: &[u64],
    deleted_nodes: &BTreeSet<u64>,
    deleted_edges: &BTreeSet<u64>,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<Vec<TxnRecordReplacement>, EngineError> {
    let mut replacements = Vec::new();
    for id in node_order {
        if deleted_nodes.contains(id) {
            continue;
        }
        let Some(node) = existing_nodes.get(id).filter(|node| gql_existing_node_changed(node))
        else {
            continue;
        };
        op_budget.reserve(1)?;
        replacements.push(TxnRecordReplacement::Node(TxnNodeRecordReplacement {
            id: *id,
            labels: node.labels.clone(),
            key: node.original.key.clone(),
            props: node.props.clone(),
            created_at: node.original.created_at,
            weight: node.weight,
            dense_vector: node.dense_vector.clone(),
            sparse_vector: node.sparse_vector.clone(),
        }));
    }
    for id in edge_order {
        if deleted_edges.contains(id) {
            continue;
        }
        let Some(edge) = existing_edges.get(id).filter(|edge| gql_existing_edge_changed(edge))
        else {
            continue;
        };
        op_budget.reserve(1)?;
        replacements.push(TxnRecordReplacement::Edge(TxnEdgeRecordReplacement {
            id: *id,
            from: edge.original.from,
            to: edge.original.to,
            label: edge.label.clone(),
            props: edge.props.clone(),
            created_at: edge.original.created_at,
            weight: edge.weight,
            valid_from: edge.valid_from,
            valid_to: edge.valid_to,
        }));
    }
    Ok(replacements)
}

fn gql_missing_created_target() -> EngineError {
    EngineError::InvalidOperation("GQL created mutation target is missing".to_string())
}

fn gql_missing_existing_target() -> EngineError {
    EngineError::InvalidOperation("GQL existing mutation target is missing".to_string())
}

fn materialize_gql_create_node(
    node: &GqlCreateNodePlan,
    row: &GqlCreateExecutionRow,
    local: TxnLocalRef,
) -> Result<GqlCreatedNodeExecution, EngineError> {
    let key = node
        .property_values
        .get("key")
        .ok_or_else(|| gql_create_invalid_value("CREATE node requires key metadata"))?;
    let key = gql_create_string_key(gql_create_expr_value(row, key.id)?)?;
    let mut weight = 1.0f32;
    if let Some(weight_ref) = node.property_values.get("weight") {
        weight = gql_create_weight(gql_create_expr_value(row, weight_ref.id)?, "node weight")?;
    }
    let mut props = BTreeMap::new();
    for (property, expr_ref) in &node.property_values {
        if property == "key" || property == "weight" {
            continue;
        }
        props.insert(
            property.clone(),
            gql_graph_value_to_prop(gql_create_expr_value(row, expr_ref.id)?)?,
        );
    }
    Ok(GqlCreatedNodeExecution {
        local,
        labels: node.labels.clone(),
        key,
        weight,
        props,
    })
}

fn materialize_gql_create_edge(
    edge: &GqlCreateEdgePlan,
    row: &GqlCreateExecutionRow,
    from: TxnNodeRef,
    to: TxnNodeRef,
    local: Option<TxnLocalRef>,
    default_valid_from: i64,
) -> Result<GqlCreatedEdgeExecution, EngineError> {
    let mut weight = 1.0f32;
    if let Some(weight_ref) = edge.property_values.get("weight") {
        weight = gql_create_weight(gql_create_expr_value(row, weight_ref.id)?, "edge weight")?;
    }
    let valid_from = edge
        .property_values
        .get("valid_from")
        .map(|expr_ref| gql_create_i64(gql_create_expr_value(row, expr_ref.id)?, "valid_from"))
        .transpose()?
        .unwrap_or(default_valid_from);
    let valid_to = edge
        .property_values
        .get("valid_to")
        .map(|expr_ref| gql_create_i64(gql_create_expr_value(row, expr_ref.id)?, "valid_to"))
        .transpose()?
        .unwrap_or(i64::MAX);
    if valid_from >= valid_to {
        return Err(gql_create_invalid_value(
            "GQL CREATE edge validity window requires valid_from < valid_to",
        ));
    }
    let mut props = BTreeMap::new();
    for (property, expr_ref) in &edge.property_values {
        if matches!(property.as_str(), "weight" | "valid_from" | "valid_to") {
            continue;
        }
        props.insert(
            property.clone(),
            gql_graph_value_to_prop(gql_create_expr_value(row, expr_ref.id)?)?,
        );
    }
    Ok(GqlCreatedEdgeExecution {
        alias: edge.alias.clone(),
        local,
        from,
        to,
        label: edge.label.clone(),
        weight,
        valid_from: Some(valid_from),
        valid_to: Some(valid_to),
        props,
    })
}

fn precheck_gql_create_conflicts(
    txn: &WriteTxn,
    materialized: &GqlCreateMaterialization,
    edge_uniqueness: bool,
) -> Result<(), EngineError> {
    if let Some((label, key)) =
        txn.gql_first_existing_node_key(&materialized.node_precheck_keys)?
    {
        return Err(gql_create_conflict_error(format!(
            "GQL CREATE node target ({label}, {key}) already exists"
        )));
    }
    if edge_uniqueness {
        if let Some((from, to, label)) =
            txn.gql_first_existing_edge_triple(&materialized.edge_precheck_triples)?
        {
            return Err(gql_create_conflict_error(format!(
                "GQL CREATE edge target ({from}, {to}, {label}) already exists"
            )));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct GqlMutationReturnStaticPlan {
    exprs: Vec<GqlReturnExpr>,
    order_by: Vec<GqlMutationResolvedOrderItem>,
    skip: usize,
    limit: Option<usize>,
}

#[derive(Clone)]
struct GqlMutationResolvedOrderItem {
    expr: Expr,
    direction: OrderDirection,
    span: SourceSpan,
}

struct GqlMutationReturnExecutionPlan {
    static_plan: GqlMutationReturnStaticPlan,
    ordered_rows: Vec<GqlMutationReturnOrderedRow>,
    output_hydration_needs: GqlMutationReturnHydrationNeeds,
    read_set: TxnReturnReadSet,
}

struct GqlMutationOrderedRowsPrecommit {
    rows: Vec<GqlMutationReturnOrderedRow>,
    read_set: TxnReturnReadSet,
}

struct GqlMutationReturnOrderedRow {
    row_index: usize,
    order_keys: Vec<GqlMutationReturnOrderKey>,
}

struct GqlMutationReturnOrderKey {
    atom: GqlMutationSortAtom,
    direction: OrderDirection,
}

#[derive(Clone, Default)]
struct GqlMutationReturnHydrationNeeds {
    node_ids: BTreeSet<u64>,
    edge_ids: BTreeSet<u64>,
    created_node_indices: BTreeSet<usize>,
    created_edge_indices: BTreeSet<usize>,
}

fn gql_mutation_return_hydration_need_count(needs: &GqlMutationReturnHydrationNeeds) -> usize {
    needs
        .node_ids
        .len()
        .saturating_add(needs.edge_ids.len())
        .saturating_add(needs.created_node_indices.len())
        .saturating_add(needs.created_edge_indices.len())
}

fn gql_txn_return_read_set_count(read_set: &TxnReturnReadSet) -> usize {
    read_set
        .node_ids
        .len()
        .saturating_add(read_set.edge_ids.len())
}

fn gql_mutation_return_profile_db_hits(
    return_execution: &GqlMutationReturnExecutionPlan,
) -> usize {
    gql_txn_return_read_set_count(&return_execution.read_set).saturating_add(
        gql_mutation_return_hydration_need_count(&return_execution.output_hydration_needs),
    )
}

#[derive(Default)]
struct GqlMutationHydratedRecords {
    nodes: BTreeMap<u64, GqlHydratedNode>,
    edges: BTreeMap<u64, GqlHydratedEdge>,
}

struct GqlHydratedNode {
    record: NodeRecord,
    labels: Vec<String>,
}

struct GqlHydratedEdge {
    record: EdgeRecord,
    label: String,
}

fn build_gql_mutation_return_static_plan(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<Option<GqlMutationReturnStaticPlan>, EngineError> {
    let Some(return_plan) = plan.return_plan.as_ref() else {
        return Ok(None);
    };
    let exprs = gql_mutation_return_exprs(plan);
    let order_by = resolve_gql_mutation_order_by_return_aliases(plan)?;
    validate_gql_mutation_return_exprs_static(plan, &exprs)?;
    validate_gql_mutation_order_exprs_static(plan, &order_by)?;
    let skip = return_plan
        .skip
        .as_ref()
        .map(|expr| evaluate_gql_mutation_count_expr(expr, plan, params, options, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    if skip > options.max_skip {
        return Err(gql_row_count_error(
            return_plan.skip.as_ref().expect("skip checked above"),
            format!("SKIP/OFFSET value {skip} exceeds max_skip={}", options.max_skip),
        ));
    }
    let limit = return_plan
        .limit
        .as_ref()
        .map(|expr| evaluate_gql_mutation_count_expr(expr, plan, params, options, "LIMIT"))
        .transpose()?;
    Ok(Some(GqlMutationReturnStaticPlan {
        exprs,
        order_by,
        skip,
        limit,
    }))
}

fn build_gql_mutation_return_execution_plan(
    plan: &GqlMutationPlan,
    static_plan: Option<GqlMutationReturnStaticPlan>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    materialized: &GqlCreateMaterialization,
    snapshot: &ReadView,
) -> Result<Option<GqlMutationReturnExecutionPlan>, EngineError> {
    let Some(static_plan) = static_plan else {
        return Ok(None);
    };
    if !static_plan.order_by.is_empty()
        && materialized.rows.len() > options.max_order_materialization
    {
        return Err(gql_mutation_cap_error(
            "max_order_materialization",
            materialized.rows.len(),
            options.max_order_materialization,
        ));
    }
    let returned_count =
        gql_mutation_return_count_after_row_ops(materialized.rows.len(), &static_plan);
    if returned_count > options.max_rows {
        return Err(gql_mutation_cap_error(
            "max_rows",
            returned_count,
            options.max_rows,
        ));
    }
    validate_gql_mutation_order_exprs_materialized(plan, &static_plan.order_by, materialized)?;

    let ordered_precommit = build_gql_mutation_ordered_rows_precommit(
        plan,
        &static_plan,
        params,
        materialized,
        snapshot,
        options,
    )?;
    let selected_rows =
        selected_gql_mutation_return_rows(&ordered_precommit.rows, static_plan.skip, static_plan.limit);
    let mut output_hydration_needs = GqlMutationReturnHydrationNeeds::default();
    let mut read_set = ordered_precommit.read_set;
    for item in &static_plan.exprs {
        collect_gql_mutation_return_expr_ids(
            plan,
            materialized,
            &item.expr,
            &selected_rows,
            GqlMutationReturnUse::Output,
            None,
            &mut output_hydration_needs,
            &mut read_set,
        );
    }
    let hydrated = hydrate_gql_mutation_return_records(snapshot, &output_hydration_needs)?;
    validate_gql_mutation_return_output_values_precommit(
        plan,
        &static_plan,
        params,
        materialized,
        &selected_rows,
        &hydrated,
        options,
    )?;
    Ok(Some(GqlMutationReturnExecutionPlan {
        static_plan,
        ordered_rows: ordered_precommit.rows,
        output_hydration_needs,
        read_set,
    }))
}

fn gql_mutation_return_count_after_row_ops(
    row_count: usize,
    plan: &GqlMutationReturnStaticPlan,
) -> usize {
    let after_skip = row_count.saturating_sub(plan.skip);
    plan.limit.map_or(after_skip, |limit| after_skip.min(limit))
}

fn gql_mutation_return_exprs(plan: &GqlMutationPlan) -> Vec<GqlReturnExpr> {
    match plan.semantic.returns.as_ref() {
        Some(GqlReturnPlan::Star {
            expanded_aliases, ..
        }) => expanded_aliases
            .iter()
            .map(|alias| GqlReturnExpr {
                expr: Expr {
                    kind: ExprKind::Variable(alias.clone()),
                    span: plan
                        .semantic
                        .aliases
                        .get(alias)
                        .map(|binding| binding.span.clone())
                        .unwrap_or_else(|| plan.semantic.statement.span.clone()),
                },
                output_name: alias.clone(),
            })
            .collect(),
        Some(GqlReturnPlan::Items(items)) => items
            .iter()
            .map(|item| GqlReturnExpr {
                expr: item.expr.clone(),
                output_name: item.output_name.clone(),
            })
            .collect(),
        None => Vec::new(),
    }
}

fn build_gql_mutation_ordered_rows_precommit(
    plan: &GqlMutationPlan,
    static_plan: &GqlMutationReturnStaticPlan,
    params: &GqlParams,
    materialized: &GqlCreateMaterialization,
    snapshot: &ReadView,
    options: &GqlExecutionOptions,
) -> Result<GqlMutationOrderedRowsPrecommit, EngineError> {
    let mut order_hydration_needs = GqlMutationReturnHydrationNeeds::default();
    let mut read_set = TxnReturnReadSet::default();
    let all_rows = (0..materialized.rows.len()).collect::<Vec<_>>();
    for item in &static_plan.order_by {
        collect_gql_mutation_return_expr_ids(
            plan,
            materialized,
            &item.expr,
            &all_rows,
            GqlMutationReturnUse::Order,
            None,
            &mut order_hydration_needs,
            &mut read_set,
        );
    }
    let hydrated = hydrate_gql_mutation_return_records(snapshot, &order_hydration_needs)?;
    let mut rows = all_rows
        .into_iter()
        .map(|row_index| {
            let row = &materialized.rows[row_index];
            let context = GqlMutationReturnEvalContext {
                plan,
                row,
                materialized,
                commit: None,
                hydrated: &hydrated,
                include_vectors: options.include_vectors,
                path_id_only: true,
            };
            let order_keys = static_plan
                .order_by
                .iter()
                .map(|item| {
                    let value = gql_mutation_return_expr_value(&item.expr, params, &context)?;
                    Ok(GqlMutationReturnOrderKey {
                        atom: gql_mutation_sort_atom_for_value(&value, &item.span)?,
                        direction: item.direction,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?;
            Ok(GqlMutationReturnOrderedRow {
                row_index,
                order_keys,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    if !static_plan.order_by.is_empty() {
        rows.sort_by(|left, right| {
            for (left_key, right_key) in left.order_keys.iter().zip(&right.order_keys) {
                let mut ordering =
                    compare_gql_mutation_sort_atoms(&left_key.atom, &right_key.atom);
                if left_key.direction == OrderDirection::Desc
                    && !matches!(left_key.atom, GqlMutationSortAtom::Null)
                    && !matches!(right_key.atom, GqlMutationSortAtom::Null)
                {
                    ordering = ordering.reverse();
                }
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            left.row_index.cmp(&right.row_index)
        });
    }
    Ok(GqlMutationOrderedRowsPrecommit { rows, read_set })
}

fn selected_gql_mutation_return_rows(
    ordered_rows: &[GqlMutationReturnOrderedRow],
    skip: usize,
    limit: Option<usize>,
) -> Vec<usize> {
    let iter = ordered_rows.iter().skip(skip).map(|row| row.row_index);
    match limit {
        Some(limit) => iter.take(limit).collect(),
        None => iter.collect(),
    }
}

fn gql_mutation_return_selected_row_count(
    ordered_rows: &[GqlMutationReturnOrderedRow],
    skip: usize,
    limit: Option<usize>,
) -> usize {
    let after_skip = ordered_rows.len().saturating_sub(skip);
    limit.map_or(after_skip, |limit| after_skip.min(limit))
}

fn gql_mutation_return_needs_committed_view(
    return_execution: &GqlMutationReturnExecutionPlan,
) -> bool {
    gql_mutation_return_selected_row_count(
        &return_execution.ordered_rows,
        return_execution.static_plan.skip,
        return_execution.static_plan.limit,
    ) > 0
}

fn validate_gql_mutation_return_output_values_precommit(
    plan: &GqlMutationPlan,
    static_plan: &GqlMutationReturnStaticPlan,
    params: &GqlParams,
    materialized: &GqlCreateMaterialization,
    selected_rows: &[usize],
    hydrated: &GqlMutationHydratedRecords,
    options: &GqlExecutionOptions,
) -> Result<(), EngineError> {
    for &row_index in selected_rows {
        let row = materialized.rows.get(row_index).ok_or_else(|| {
            EngineError::InvalidOperation(
                "GQL mutation RETURN selected row index is out of bounds".to_string(),
            )
        })?;
        let context = GqlMutationReturnEvalContext {
            plan,
            row,
            materialized,
            commit: None,
            hydrated,
            include_vectors: options.include_vectors,
            path_id_only: false,
        };
        for item in &static_plan.exprs {
            let _ = gql_mutation_return_expr_value(&item.expr, params, &context)?;
        }
    }
    Ok(())
}

fn build_gql_mutation_return_rows(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    materialized: &GqlCreateMaterialization,
    commit: &TxnCommitResult,
    return_execution: Option<&GqlMutationReturnExecutionPlan>,
    return_view: Option<&ReadView>,
    options: &GqlExecutionOptions,
) -> Result<Vec<GqlRow>, EngineError> {
    let Some(return_execution) = return_execution else {
        return Ok(Vec::new());
    };
    let selected = selected_gql_mutation_return_rows(
        &return_execution.ordered_rows,
        return_execution.static_plan.skip,
        return_execution.static_plan.limit,
    );
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let view = return_view.ok_or_else(|| {
        EngineError::InvalidOperation(
            "GQL mutation RETURN projection requires the committed read view".to_string(),
        )
    })?;
    let ids = realize_gql_mutation_return_hydration_ids(
        &return_execution.output_hydration_needs,
        materialized,
        commit,
    );
    let hydrated = hydrate_gql_mutation_return_records(view, &ids)?;
    selected
        .into_iter()
        .map(|row_index| {
            let row = &materialized.rows[row_index];
            let context = GqlMutationReturnEvalContext {
                plan,
                row,
                materialized,
                commit: Some(commit),
                hydrated: &hydrated,
                include_vectors: options.include_vectors,
                path_id_only: false,
            };
            let values = return_execution
                .static_plan
                .exprs
                .iter()
                .map(|item| gql_mutation_return_expr_value(&item.expr, params, &context))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(GqlRow { values })
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GqlMutationReturnUse {
    Output,
    Order,
}

struct GqlMutationReturnEvalContext<'a> {
    plan: &'a GqlMutationPlan,
    row: &'a GqlCreateExecutionRow,
    materialized: &'a GqlCreateMaterialization,
    commit: Option<&'a TxnCommitResult>,
    hydrated: &'a GqlMutationHydratedRecords,
    include_vectors: bool,
    path_id_only: bool,
}

fn resolve_gql_mutation_order_by_return_aliases(
    plan: &GqlMutationPlan,
) -> Result<Vec<GqlMutationResolvedOrderItem>, EngineError> {
    let return_aliases = gql_mutation_return_alias_exprs(plan);
    let Some(tail) = plan.semantic.statement.return_tail.as_ref() else {
        return Ok(Vec::new());
    };
    tail.order_by
        .iter()
        .map(|item| {
            let expr = resolve_mutation_return_aliases_in_expr(
                &item.expr,
                &return_aliases,
                plan,
            )?;
            Ok(GqlMutationResolvedOrderItem {
                expr,
                direction: item.direction,
                span: item.span.clone(),
            })
        })
        .collect()
}

fn gql_mutation_return_alias_exprs(
    plan: &GqlMutationPlan,
) -> BTreeMap<String, GqlReturnAliasResolution> {
    let mut aliases = BTreeMap::new();
    if let Some(GqlReturnPlan::Items(items)) = &plan.semantic.returns {
        for item in items {
            if let Some(alias) = item.explicit_alias.as_ref() {
                aliases
                    .entry(alias.clone())
                    .and_modify(|resolution| *resolution = GqlReturnAliasResolution::Ambiguous)
                    .or_insert_with(|| GqlReturnAliasResolution::Unique(item.expr.clone()));
            }
        }
    }
    aliases
}

fn resolve_mutation_return_aliases_in_expr(
    expr: &Expr,
    return_aliases: &BTreeMap<String, GqlReturnAliasResolution>,
    plan: &GqlMutationPlan,
) -> Result<Expr, EngineError> {
    let kind = match &expr.kind {
        ExprKind::Variable(name)
            if !plan.semantic.aliases.contains_key(name) && return_aliases.contains_key(name) =>
        {
            return match return_aliases.get(name).expect("checked above") {
                GqlReturnAliasResolution::Unique(expr) => Ok(expr.clone()),
                GqlReturnAliasResolution::Ambiguous => {
                    Err(gql_ambiguous_return_alias_error(name, &expr.span))
                }
            };
        }
        ExprKind::PropertyAccess { object, property } => ExprKind::PropertyAccess {
            object: Box::new(resolve_mutation_return_aliases_in_expr(
                object,
                return_aliases,
                plan,
            )?),
            property: property.clone(),
        },
        ExprKind::Unary { op, expr } => ExprKind::Unary {
            op: *op,
            expr: Box::new(resolve_mutation_return_aliases_in_expr(
                expr,
                return_aliases,
                plan,
            )?),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: Box::new(resolve_mutation_return_aliases_in_expr(
                left,
                return_aliases,
                plan,
            )?),
            right: Box::new(resolve_mutation_return_aliases_in_expr(
                right,
                return_aliases,
                plan,
            )?),
        },
        ExprKind::IsNull { expr, negated } => ExprKind::IsNull {
            expr: Box::new(resolve_mutation_return_aliases_in_expr(
                expr,
                return_aliases,
                plan,
            )?),
            negated: *negated,
        },
        ExprKind::FunctionCall { name, args } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_mutation_return_aliases_in_expr(arg, return_aliases, plan))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ExprKind::List(items) => ExprKind::List(
            items
                .iter()
                .map(|item| resolve_mutation_return_aliases_in_expr(item, return_aliases, plan))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ExprKind::Map(map) => {
            let mut resolved = map.clone();
            for entry in &mut resolved.entries {
                entry.value =
                    resolve_mutation_return_aliases_in_expr(&entry.value, return_aliases, plan)?;
            }
            ExprKind::Map(resolved)
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => {
            return Ok(expr.clone())
        }
    };
    Ok(Expr {
        kind,
        span: expr.span.clone(),
    })
}

fn validate_gql_mutation_return_exprs_static(
    plan: &GqlMutationPlan,
    exprs: &[GqlReturnExpr],
) -> Result<(), EngineError> {
    for expr in exprs {
        validate_gql_mutation_return_expr_static(plan, &expr.expr)?;
    }
    Ok(())
}

fn validate_gql_mutation_return_expr_static(
    plan: &GqlMutationPlan,
    expr: &Expr,
) -> Result<(), EngineError> {
    match &expr.kind {
        ExprKind::Variable(alias) => {
            if !plan.semantic.aliases.contains_key(alias) {
                return Err(gql_create_return_unsupported(
                    "GQL mutation RETURN references an unknown alias",
                    &expr.span,
                ));
            }
        }
        ExprKind::PropertyAccess { object, .. } => {
            validate_gql_mutation_return_property_access_static(plan, object, &expr.span)?
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            validate_gql_mutation_return_expr_static(plan, expr)?
        }
        ExprKind::Binary { left, right, .. } => {
            validate_gql_mutation_return_expr_static(plan, left)?;
            validate_gql_mutation_return_expr_static(plan, right)?;
        }
        ExprKind::FunctionCall { name, args } => {
            validate_gql_mutation_return_function_static(plan, &name.name, args, &expr.span)?
        }
        ExprKind::List(args) => {
            for arg in args {
                validate_gql_mutation_return_expr_static(plan, arg)?;
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                validate_gql_mutation_return_expr_static(plan, &entry.value)?;
            }
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
    Ok(())
}

fn validate_gql_mutation_return_property_access_static(
    plan: &GqlMutationPlan,
    object: &Expr,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    match &object.kind {
        ExprKind::Variable(alias) => {
            if !plan.semantic.aliases.contains_key(alias) {
                return Err(gql_create_return_unsupported(
                    "GQL mutation RETURN references an unknown alias",
                    &object.span,
                ));
            }
            Ok(())
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                validate_gql_mutation_return_expr_static(plan, &entry.value)?;
            }
            Ok(())
        }
        _ => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidPropertyAccess,
            "GQL mutation RETURN property access supports only bound aliases".to_string(),
            span.clone(),
        )),
    }
}

fn validate_gql_mutation_return_function_static(
    plan: &GqlMutationPlan,
    function: &str,
    args: &[Expr],
    span: &SourceSpan,
) -> Result<(), EngineError> {
    let [arg] = args else {
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            format!("function '{function}' expects exactly one argument"),
            span.clone(),
        ));
    };
    let ExprKind::Variable(alias) = &arg.kind else {
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            format!("function '{function}' expects a bound alias argument"),
            arg.span.clone(),
        ));
    };
    let Some(binding) = plan.semantic.aliases.get(alias) else {
        return Err(gql_create_return_unsupported(
            "GQL mutation RETURN references an unknown alias",
            &arg.span,
        ));
    };
    let function = function.to_ascii_lowercase();
    let valid = match function.as_str() {
        "id" => matches!(binding.kind, GqlAliasKind::Node | GqlAliasKind::Edge),
        "labels" => binding.kind == GqlAliasKind::Node,
        "type" => binding.kind == GqlAliasKind::Edge,
        "length" | "node_ids" | "edge_ids" | "start_node" | "end_node" | "nodes"
        | "relationships" => binding.kind == GqlAliasKind::Path,
        _ => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "unsupported GQL scalar function".to_string(),
                span.clone(),
            ))
        }
    };
    if valid {
        Ok(())
    } else {
        Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            format!("function '{}' received an unsupported alias kind", function),
            span.clone(),
        ))
    }
}

fn validate_gql_mutation_order_exprs_static(
    plan: &GqlMutationPlan,
    order_by: &[GqlMutationResolvedOrderItem],
) -> Result<(), EngineError> {
    for item in order_by {
        validate_gql_mutation_order_expr_static(plan, &item.expr, &item.span)?;
    }
    Ok(())
}

fn validate_gql_mutation_order_expr_static(
    plan: &GqlMutationPlan,
    expr: &Expr,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    match &expr.kind {
        ExprKind::Variable(alias) => match plan.semantic.aliases.get(alias).map(|binding| binding.kind) {
            Some(GqlAliasKind::Path) => Ok(()),
            Some(GqlAliasKind::Node | GqlAliasKind::Edge) => Err(gql_order_key_error(span)),
            None => Ok(()),
        },
        ExprKind::FunctionCall { name, args } => {
            let function = name.name.to_ascii_lowercase();
            if matches!(
                function.as_str(),
                "labels"
                    | "start_node"
                    | "end_node"
                    | "nodes"
                    | "relationships"
                    | "node_ids"
                    | "edge_ids"
            ) {
                return Err(gql_order_key_error(span));
            }
            for arg in args {
                validate_gql_mutation_return_expr_static(plan, arg)?;
            }
            Ok(())
        }
        ExprKind::PropertyAccess { object, property } => {
            if matches!(property.name.as_str(), "labels" | "node_ids" | "edge_ids") {
                return Err(gql_order_key_error(span));
            }
            if matches!(object.kind, ExprKind::Variable(_)) {
                Ok(())
            } else {
                validate_gql_mutation_order_expr_static(plan, object, span)
            }
        }
        ExprKind::List(_) | ExprKind::Map(_) => Err(gql_order_key_error(span)),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            validate_gql_mutation_order_expr_static(plan, expr, span)
        }
        ExprKind::Binary { left, right, .. } => {
            validate_gql_mutation_order_expr_static(plan, left, span)?;
            validate_gql_mutation_order_expr_static(plan, right, span)
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) => Ok(()),
    }
}

fn validate_gql_mutation_order_exprs_materialized(
    plan: &GqlMutationPlan,
    order_by: &[GqlMutationResolvedOrderItem],
    materialized: &GqlCreateMaterialization,
) -> Result<(), EngineError> {
    for item in order_by {
        validate_gql_mutation_order_expr_materialized(
            plan,
            materialized,
            &item.expr,
            &item.span,
        )?;
    }
    Ok(())
}

fn validate_gql_mutation_order_expr_materialized(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    expr: &Expr,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    match &expr.kind {
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                validate_gql_mutation_order_alias_property_materialized(
                    plan,
                    materialized,
                    alias,
                    &property.name,
                    span,
                )?;
            } else {
                validate_gql_mutation_order_expr_materialized(plan, materialized, object, span)?;
            }
            Ok(())
        }
        ExprKind::FunctionCall { name, args } => {
            if name.name.eq_ignore_ascii_case("id") {
                if let Some(Expr {
                    kind: ExprKind::Variable(alias),
                    ..
                }) = args.first()
                {
                    if plan.semantic.aliases.get(alias).is_some_and(|binding| {
                        binding.origin == GqlAliasOrigin::Created
                            && matches!(binding.kind, GqlAliasKind::Node | GqlAliasKind::Edge)
                    }) {
                        return Err(gql_order_key_error(span));
                    }
                }
            }
            for arg in args {
                validate_gql_mutation_order_expr_materialized(plan, materialized, arg, span)?;
            }
            Ok(())
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            validate_gql_mutation_order_expr_materialized(plan, materialized, expr, span)
        }
        ExprKind::Binary { left, right, .. } => {
            validate_gql_mutation_order_expr_materialized(plan, materialized, left, span)?;
            validate_gql_mutation_order_expr_materialized(plan, materialized, right, span)
        }
        ExprKind::List(items) => {
            for item in items {
                validate_gql_mutation_order_expr_materialized(plan, materialized, item, span)?;
            }
            Ok(())
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                validate_gql_mutation_order_expr_materialized(
                    plan,
                    materialized,
                    &entry.value,
                    span,
                )?;
            }
            Ok(())
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => Ok(()),
    }
}

fn validate_gql_mutation_order_alias_property_materialized(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    alias: &str,
    property: &str,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    let Some(binding) = plan.semantic.aliases.get(alias) else {
        return Ok(());
    };
    if binding.origin == GqlAliasOrigin::Created {
        let volatile = match binding.kind {
            GqlAliasKind::Node => matches!(property, "id" | "created_at" | "updated_at"),
            GqlAliasKind::Edge => {
                matches!(property, "id" | "from" | "to" | "created_at" | "updated_at")
            }
            GqlAliasKind::Path => false,
        };
        if volatile {
            return Err(gql_order_key_error(span));
        }
    }
    if property == "updated_at"
        && binding.origin == GqlAliasOrigin::ReadPrefix
        && gql_mutation_order_alias_has_changed_target(materialized, alias, binding.kind)
    {
        return Err(gql_order_key_error(span));
    }
    Ok(())
}

fn gql_mutation_order_alias_has_changed_target(
    materialized: &GqlCreateMaterialization,
    alias: &str,
    kind: GqlAliasKind,
) -> bool {
    materialized.rows.iter().any(|row| match kind {
        GqlAliasKind::Node => row
            .read_nodes
            .get(alias)
            .and_then(|id| *id)
            .and_then(|id| materialized.existing_nodes.get(&id))
            .is_some_and(gql_existing_node_changed),
        GqlAliasKind::Edge => row
            .read_edges
            .get(alias)
            .and_then(|id| *id)
            .and_then(|id| materialized.existing_edges.get(&id))
            .is_some_and(gql_existing_edge_changed),
        GqlAliasKind::Path => false,
    })
}

fn evaluate_gql_mutation_count_expr(
    expr: &Expr,
    plan: &GqlMutationPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
    clause: &str,
) -> Result<usize, EngineError> {
    let resolved = resolve_mutation_return_aliases_in_expr(
        expr,
        &gql_mutation_return_alias_exprs(plan),
        plan,
    )?;
    if gql_mutation_expr_depends_on_alias(&resolved, plan) {
        return Err(gql_row_count_error(
            expr,
            format!("{clause} must be a row-independent non-negative integer"),
        ));
    }
    let empty_materialized = GqlMutationHydratedRecords::default();
    let empty_row = GqlCreateExecutionRow {
        read_nodes: BTreeMap::new(),
        read_edges: BTreeMap::new(),
        read_paths: BTreeMap::new(),
        expr_values: Vec::new(),
        created_nodes: BTreeMap::new(),
        created_edges: BTreeMap::new(),
        produced_write: false,
    };
    let empty_materialization = GqlCreateMaterialization {
        rows: Vec::new(),
        intents: Vec::new(),
        record_replacements: Vec::new(),
        nodes: Vec::new(),
        edges: Vec::new(),
        existing_nodes: BTreeMap::new(),
        existing_edges: BTreeMap::new(),
        node_precheck_keys: BTreeSet::new(),
        edge_precheck_triples: BTreeSet::new(),
        mutation_rows: 0,
        mutation_ops: 0,
        nodes_created: 0,
        nodes_updated: 0,
        nodes_deleted: 0,
        edges_created: 0,
        edges_updated: 0,
        edges_deleted: 0,
        properties_set: 0,
        properties_removed: 0,
        labels_added: 0,
        labels_removed: 0,
        skipped_null_targets: 0,
        duplicate_targets: 0,
        db_hits: 0,
    };
    let context = GqlMutationReturnEvalContext {
        plan,
        row: &empty_row,
        materialized: &empty_materialization,
        commit: None,
        hydrated: &empty_materialized,
        include_vectors: options.include_vectors,
        path_id_only: false,
    };
    let value = gql_mutation_return_expr_value(&resolved, params, &context)?;
    match value {
        GqlValue::Int(value) if value >= 0 => usize::try_from(value).map_err(|_| {
            gql_row_count_error(expr, format!("{clause} value is too large for this platform"))
        }),
        GqlValue::UInt(value) => usize::try_from(value).map_err(|_| {
            gql_row_count_error(expr, format!("{clause} value is too large for this platform"))
        }),
        _ => Err(gql_row_count_error(
            expr,
            format!("{clause} must evaluate to a non-negative integer"),
        )),
    }
}

fn gql_mutation_expr_depends_on_alias(expr: &Expr, plan: &GqlMutationPlan) -> bool {
    match &expr.kind {
        ExprKind::Variable(alias) => plan.semantic.aliases.contains_key(alias),
        ExprKind::PropertyAccess { object, .. } => {
            gql_mutation_expr_depends_on_alias(object, plan)
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            gql_mutation_expr_depends_on_alias(expr, plan)
        }
        ExprKind::Binary { left, right, .. } => {
            gql_mutation_expr_depends_on_alias(left, plan)
                || gql_mutation_expr_depends_on_alias(right, plan)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => args
            .iter()
            .any(|arg| gql_mutation_expr_depends_on_alias(arg, plan)),
        ExprKind::Map(map) => map
            .entries
            .iter()
            .any(|entry| gql_mutation_expr_depends_on_alias(&entry.value, plan)),
        ExprKind::Literal(_) | ExprKind::Parameter(_) => false,
    }
}

fn hydrate_gql_mutation_return_records(
    view: &ReadView,
    ids: &GqlMutationReturnHydrationNeeds,
) -> Result<GqlMutationHydratedRecords, EngineError> {
    let node_ids = ids.node_ids.iter().copied().collect::<Vec<_>>();
    let edge_ids = ids.edge_ids.iter().copied().collect::<Vec<_>>();
    let node_records = view.get_nodes_raw(&node_ids)?;
    let edge_records = view.get_edges(&edge_ids)?;
    let nodes = node_ids
        .into_iter()
        .zip(node_records)
        .map(|(id, record)| {
            let record = record.ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GQL mutation RETURN node {id} was not visible in the projection view"
                ))
            })?;
            let labels = txn_labels_from_record(&record, view.label_catalog.as_ref())?;
            Ok((id, GqlHydratedNode { record, labels }))
        })
        .collect::<Result<BTreeMap<_, _>, EngineError>>()?;
    let edges = edge_ids
        .into_iter()
        .zip(edge_records)
        .map(|(id, record)| {
            let record = record.ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GQL mutation RETURN edge {id} was not visible in the projection view"
                ))
            })?;
            let label = view
                .label_catalog
                .edge_label(record.label_id)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "edge record {id} references missing edge-label label_id {}",
                        record.label_id
                    ))
                })?
                .to_string();
            Ok((id, GqlHydratedEdge { record, label }))
        })
        .collect::<Result<BTreeMap<_, _>, EngineError>>()?;
    Ok(GqlMutationHydratedRecords { nodes, edges })
}

fn realize_gql_mutation_return_hydration_ids(
    needs: &GqlMutationReturnHydrationNeeds,
    materialized: &GqlCreateMaterialization,
    commit: &TxnCommitResult,
) -> GqlMutationReturnHydrationNeeds {
    let mut ids = GqlMutationReturnHydrationNeeds {
        node_ids: needs.node_ids.clone(),
        edge_ids: needs.edge_ids.clone(),
        created_node_indices: BTreeSet::new(),
        created_edge_indices: BTreeSet::new(),
    };
    for &index in &needs.created_node_indices {
        if let Some(node) = materialized.nodes.get(index) {
            if let Some(&id) = commit.local_node_ids.get(&node.local) {
                ids.node_ids.insert(id);
            }
        }
    }
    for &index in &needs.created_edge_indices {
        if let Some(edge) = materialized.edges.get(index) {
            if let Some(id) = edge.local.as_ref().and_then(|local| commit.local_edge_ids.get(local))
            {
                ids.edge_ids.insert(*id);
            }
        }
    }
    ids
}

#[allow(clippy::too_many_arguments)]
fn collect_gql_mutation_return_expr_ids(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    expr: &Expr,
    row_indices: &[usize],
    use_: GqlMutationReturnUse,
    commit: Option<&TxnCommitResult>,
    ids: &mut GqlMutationReturnHydrationNeeds,
    read_set: &mut TxnReturnReadSet,
) {
    match &expr.kind {
        ExprKind::Variable(alias) => {
            collect_gql_mutation_alias_ids(
                plan,
                materialized,
                alias,
                row_indices,
                use_,
                commit,
                ids,
                read_set,
            )
        }
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                if plan
                    .semantic
                    .aliases
                    .get(alias)
                    .is_some_and(|binding| binding.kind == GqlAliasKind::Path)
                    && matches!(property.name.as_str(), "node_ids" | "edge_ids" | "length")
                {
                    return;
                }
                collect_gql_mutation_alias_ids(
                    plan,
                    materialized,
                    alias,
                    row_indices,
                    use_,
                    commit,
                    ids,
                    read_set,
                );
            } else {
                collect_gql_mutation_return_expr_ids(
                    plan, materialized, object, row_indices, use_, commit, ids, read_set,
                );
            }
        }
        ExprKind::FunctionCall { name, args } => {
            if let Some(Expr {
                kind: ExprKind::Variable(alias),
                ..
            }) = args.first()
            {
                let function = name.name.to_ascii_lowercase();
                match function.as_str() {
                    "labels" | "type" => {
                        collect_gql_mutation_alias_ids(
                            plan,
                            materialized,
                            alias,
                            row_indices,
                            use_,
                            commit,
                            ids,
                            read_set,
                        );
                    }
                    "start_node" | "end_node" | "nodes" | "relationships"
                        if use_ == GqlMutationReturnUse::Output =>
                    {
                        collect_gql_mutation_path_helper_ids(
                            plan,
                            materialized,
                            alias,
                            row_indices,
                            function.as_str(),
                            ids,
                            read_set,
                        );
                    }
                    _ => {}
                }
                return;
            }
            for arg in args {
                collect_gql_mutation_return_expr_ids(
                    plan, materialized, arg, row_indices, use_, commit, ids, read_set,
                );
            }
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            collect_gql_mutation_return_expr_ids(
                plan, materialized, expr, row_indices, use_, commit, ids, read_set,
            )
        }
        ExprKind::Binary { left, right, .. } => {
            collect_gql_mutation_return_expr_ids(
                plan, materialized, left, row_indices, use_, commit, ids, read_set,
            );
            collect_gql_mutation_return_expr_ids(
                plan, materialized, right, row_indices, use_, commit, ids, read_set,
            );
        }
        ExprKind::List(items) => {
            for item in items {
                collect_gql_mutation_return_expr_ids(
                    plan, materialized, item, row_indices, use_, commit, ids, read_set,
                );
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_gql_mutation_return_expr_ids(
                    plan, materialized, &entry.value, row_indices, use_, commit, ids, read_set,
                );
            }
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
}

fn collect_gql_mutation_path_helper_ids(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    alias: &str,
    row_indices: &[usize],
    function: &str,
    ids: &mut GqlMutationReturnHydrationNeeds,
    read_set: &mut TxnReturnReadSet,
) {
    if !plan
        .semantic
        .aliases
        .get(alias)
        .is_some_and(|binding| binding.kind == GqlAliasKind::Path)
    {
        return;
    }
    for &row_index in row_indices {
        let Some(row) = materialized.rows.get(row_index) else {
            continue;
        };
        let Some(Some(path)) = row.read_paths.get(alias) else {
            continue;
        };
        match function {
            "start_node" => {
                if let Some(&id) = path.node_ids.first() {
                    ids.node_ids.insert(id);
                    read_set.node_ids.insert(id);
                }
            }
            "end_node" => {
                if let Some(&id) = path.node_ids.last() {
                    ids.node_ids.insert(id);
                    read_set.node_ids.insert(id);
                }
            }
            "nodes" => {
                ids.node_ids.extend(path.node_ids.iter().copied());
                read_set.node_ids.extend(path.node_ids.iter().copied());
            }
            "relationships" => {
                ids.edge_ids.extend(path.edge_ids.iter().copied());
                read_set.edge_ids.extend(path.edge_ids.iter().copied());
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_gql_mutation_alias_ids(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    alias: &str,
    row_indices: &[usize],
    use_: GqlMutationReturnUse,
    commit: Option<&TxnCommitResult>,
    ids: &mut GqlMutationReturnHydrationNeeds,
    read_set: &mut TxnReturnReadSet,
) {
    let Some(binding) = plan.semantic.aliases.get(alias) else {
        return;
    };
    for &row_index in row_indices {
        let Some(row) = materialized.rows.get(row_index) else {
            continue;
        };
        match binding.kind {
            GqlAliasKind::Node => {
                if binding.origin == GqlAliasOrigin::Created && commit.is_none() {
                    if use_ == GqlMutationReturnUse::Output {
                        if let Some(&index) = row.created_nodes.get(alias) {
                            ids.created_node_indices.insert(index);
                        }
                    }
                    continue;
                }
                if let Some(id) =
                    gql_mutation_node_id_for_alias(alias, row, materialized, commit)
                {
                    if binding.origin == GqlAliasOrigin::ReadPrefix
                        && matches!(use_, GqlMutationReturnUse::Output | GqlMutationReturnUse::Order)
                    {
                        read_set.node_ids.insert(id);
                    }
                    if matches!(use_, GqlMutationReturnUse::Output | GqlMutationReturnUse::Order) {
                        ids.node_ids.insert(id);
                    }
                }
            }
            GqlAliasKind::Edge => {
                if binding.origin == GqlAliasOrigin::Created && commit.is_none() {
                    if use_ == GqlMutationReturnUse::Output {
                        if let Some(&index) = row.created_edges.get(alias) {
                            ids.created_edge_indices.insert(index);
                        }
                    }
                    continue;
                }
                if let Some(id) =
                    gql_mutation_edge_id_for_alias(alias, row, materialized, commit)
                {
                    if binding.origin == GqlAliasOrigin::ReadPrefix
                        && matches!(use_, GqlMutationReturnUse::Output | GqlMutationReturnUse::Order)
                    {
                        read_set.edge_ids.insert(id);
                    }
                    if matches!(use_, GqlMutationReturnUse::Output | GqlMutationReturnUse::Order) {
                        ids.edge_ids.insert(id);
                    }
                }
            }
            GqlAliasKind::Path => {
                let Some(Some(path)) = row.read_paths.get(alias) else {
                    continue;
                };
                if use_ == GqlMutationReturnUse::Output {
                    ids.node_ids.extend(path.node_ids.iter().copied());
                    ids.edge_ids.extend(path.edge_ids.iter().copied());
                    read_set.node_ids.extend(path.node_ids.iter().copied());
                    read_set.edge_ids.extend(path.edge_ids.iter().copied());
                }
            }
        }
    }
}

fn gql_mutation_return_expr_value(
    expr: &Expr,
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<GqlValue, EngineError> {
    match &expr.kind {
        ExprKind::Literal(literal) => Ok(gql_literal_to_value(literal)),
        ExprKind::Parameter(name) => params
            .get(name)
            .map(gql_param_to_value)
            .ok_or_else(|| EngineError::GqlParameter {
                name: name.clone(),
                expected: "GqlParamValue".to_string(),
                message: format!("missing parameter '${name}'"),
                span: expr.span.clone(),
            }),
        ExprKind::Variable(alias) => gql_mutation_alias_value(alias, context),
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                return gql_mutation_alias_property_value(alias, &property.name, context);
            }
            let object = gql_mutation_return_expr_value(object, params, context)?;
            match object {
                GqlValue::Map(map) => Ok(map.get(&property.name).cloned().unwrap_or(GqlValue::Null)),
                GqlValue::Null => Ok(GqlValue::Null),
                _ => Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidPropertyAccess,
                    "property access requires a map or bound alias".to_string(),
                    expr.span.clone(),
                )),
            }
        }
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } => match gql_mutation_return_expr_value(expr, params, context)? {
            GqlValue::Bool(value) => Ok(GqlValue::Bool(!value)),
            GqlValue::Null => Ok(GqlValue::Null),
            _ => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "NOT requires a boolean or null operand".to_string(),
                expr.span.clone(),
            )),
        },
        ExprKind::Binary { op, left, right } => {
            gql_mutation_eval_binary(*op, left, right, params, context)
        }
        ExprKind::IsNull { expr, negated } => {
            let is_null = matches!(
                gql_mutation_return_expr_value(expr, params, context)?,
                GqlValue::Null
            );
            Ok(GqlValue::Bool(if *negated { !is_null } else { is_null }))
        }
        ExprKind::FunctionCall { name, args } => {
            let Some(Expr {
                kind: ExprKind::Variable(alias),
                ..
            }) = args.first()
            else {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!("function '{}' expects a bound alias argument", name.name),
                    expr.span.clone(),
                ));
            };
            gql_mutation_function_value(&name.name, alias, context, &expr.span)
        }
        ExprKind::List(items) => Ok(GqlValue::List(
            items
                .iter()
                .map(|item| gql_mutation_return_expr_value(item, params, context))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ExprKind::Map(map) => Ok(GqlValue::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        entry.key.name.clone(),
                        gql_mutation_return_expr_value(&entry.value, params, context)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        )),
    }
}

fn gql_mutation_eval_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<GqlValue, EngineError> {
    match op {
        BinaryOp::And => {
            let left = gql_mutation_bool_or_null(left, params, context)?;
            if left == Some(false) {
                return Ok(GqlValue::Bool(false));
            }
            let right = gql_mutation_bool_or_null(right, params, context)?;
            Ok(match (left, right) {
                (_, Some(false)) => GqlValue::Bool(false),
                (Some(true), Some(true)) => GqlValue::Bool(true),
                _ => GqlValue::Null,
            })
        }
        BinaryOp::Or => {
            let left = gql_mutation_bool_or_null(left, params, context)?;
            if left == Some(true) {
                return Ok(GqlValue::Bool(true));
            }
            let right = gql_mutation_bool_or_null(right, params, context)?;
            Ok(match (left, right) {
                (_, Some(true)) => GqlValue::Bool(true),
                (Some(false), Some(false)) => GqlValue::Bool(false),
                _ => GqlValue::Null,
            })
        }
        BinaryOp::Eq
        | BinaryOp::Neq
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::In => {
            let left_value = gql_mutation_return_expr_value(left, params, context)?;
            let right_value = gql_mutation_return_expr_value(right, params, context)?;
            Ok(gql_mutation_compare_values(op, left_value, right_value))
        }
    }
}

fn gql_mutation_bool_or_null(
    expr: &Expr,
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<Option<bool>, EngineError> {
    match gql_mutation_return_expr_value(expr, params, context)? {
        GqlValue::Bool(value) => Ok(Some(value)),
        GqlValue::Null => Ok(None),
        _ => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "boolean operators require boolean or null operands".to_string(),
            expr.span.clone(),
        )),
    }
}

fn gql_mutation_compare_values(op: BinaryOp, left: GqlValue, right: GqlValue) -> GqlValue {
    if matches!(left, GqlValue::Null) || matches!(right, GqlValue::Null) {
        return GqlValue::Null;
    }
    match op {
        BinaryOp::Eq => GqlValue::Bool(gql_values_equal_for_mutation(&left, &right)),
        BinaryOp::Neq => GqlValue::Bool(!gql_values_equal_for_mutation(&left, &right)),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            match partial_cmp_gql_mutation_values(&left, &right) {
                Some(ordering) => GqlValue::Bool(match op {
                    BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
                    BinaryOp::Le => matches!(
                        ordering,
                        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                    ),
                    BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
                    BinaryOp::Ge => matches!(
                        ordering,
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                    ),
                    _ => unreachable!(),
                }),
                None => GqlValue::Null,
            }
        }
        BinaryOp::In => match right {
            GqlValue::List(values) => {
                let mut saw_null = false;
                for value in values {
                    if matches!(value, GqlValue::Null) {
                        saw_null = true;
                    } else if gql_values_equal_for_mutation(&left, &value) {
                        return GqlValue::Bool(true);
                    }
                }
                if saw_null {
                    GqlValue::Null
                } else {
                    GqlValue::Bool(false)
                }
            }
            _ => GqlValue::Null,
        },
        BinaryOp::And | BinaryOp::Or => unreachable!(),
    }
}

fn gql_mutation_alias_value(
    alias: &str,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<GqlValue, EngineError> {
    let Some(binding) = context.plan.semantic.aliases.get(alias) else {
        return Ok(GqlValue::Null);
    };
    Ok(match binding.kind {
        GqlAliasKind::Node => {
            if context.commit.is_none() {
                if let Some(&index) = context.row.created_nodes.get(alias) {
                    GqlValue::Node(gql_node_from_created_execution(
                        &context.materialized.nodes[index],
                        context.commit,
                        context.include_vectors,
                    ))
                } else {
                    context
                        .node_id(alias)
                        .map(|id| context.node_value(id))
                        .transpose()?
                        .unwrap_or(GqlValue::Null)
                }
            } else {
                context
                    .node_id(alias)
                    .map(|id| context.node_value(id))
                    .transpose()?
                    .unwrap_or(GqlValue::Null)
            }
        }
        GqlAliasKind::Edge => {
            if context.commit.is_none() {
                if let Some(&index) = context.row.created_edges.get(alias) {
                    GqlValue::Edge(gql_edge_from_created_execution(
                        &context.materialized.edges[index],
                        context.commit,
                    ))
                } else {
                    context
                        .edge_id(alias)
                        .map(|id| context.edge_value(id))
                        .transpose()?
                        .unwrap_or(GqlValue::Null)
                }
            } else {
                context
                    .edge_id(alias)
                    .map(|id| context.edge_value(id))
                    .transpose()?
                    .unwrap_or(GqlValue::Null)
            }
        }
        GqlAliasKind::Path => context
            .path(alias)
            .map(|path| {
                if context.path_id_only {
                    Ok(gql_path_identity_value(path))
                } else {
                    context.path_value(path)
                }
            })
            .transpose()?
            .unwrap_or(GqlValue::Null),
    })
}

fn gql_mutation_alias_property_value(
    alias: &str,
    property: &str,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<GqlValue, EngineError> {
    let Some(binding) = context.plan.semantic.aliases.get(alias) else {
        return Ok(GqlValue::Null);
    };
    match binding.kind {
        GqlAliasKind::Node => {
            if context.commit.is_none() {
                if let Some(&index) = context.row.created_nodes.get(alias) {
                    let node = gql_node_from_created_execution(
                        &context.materialized.nodes[index],
                        context.commit,
                        context.include_vectors,
                    );
                    Ok(gql_node_property_from_value(node, property))
                } else {
                    context
                        .node_id(alias)
                        .map(|id| context.node_property_value(id, property))
                        .transpose()
                        .map(|value| value.unwrap_or(GqlValue::Null))
                }
            } else {
                context
                    .node_id(alias)
                    .map(|id| context.node_property_value(id, property))
                    .transpose()
                    .map(|value| value.unwrap_or(GqlValue::Null))
            }
        }
        GqlAliasKind::Edge => {
            if context.commit.is_none() {
                if let Some(&index) = context.row.created_edges.get(alias) {
                    let edge = gql_edge_from_created_execution(
                        &context.materialized.edges[index],
                        context.commit,
                    );
                    Ok(gql_edge_property_from_value(edge, property))
                } else {
                    context
                        .edge_id(alias)
                        .map(|id| context.edge_property_value(id, property))
                        .transpose()
                        .map(|value| value.unwrap_or(GqlValue::Null))
                }
            } else {
                context
                    .edge_id(alias)
                    .map(|id| context.edge_property_value(id, property))
                    .transpose()
                    .map(|value| value.unwrap_or(GqlValue::Null))
            }
        }
        GqlAliasKind::Path => Ok(context
            .path(alias)
            .map(|path| match property {
                "node_ids" => gql_id_list_value(&path.node_ids),
                "edge_ids" => gql_id_list_value(&path.edge_ids),
                "length" => GqlValue::UInt(path.edge_ids.len() as u64),
                _ => GqlValue::Null,
            })
            .unwrap_or(GqlValue::Null)),
    }
}

fn gql_mutation_function_value(
    function: &str,
    alias: &str,
    context: &GqlMutationReturnEvalContext<'_>,
    span: &SourceSpan,
) -> Result<GqlValue, EngineError> {
    match function.to_ascii_lowercase().as_str() {
        "id" => {
            if let Some(id) = context.node_id(alias) {
                return Ok(GqlValue::UInt(id));
            }
            if let Some(id) = context.edge_id(alias) {
                return Ok(GqlValue::UInt(id));
            }
            Ok(GqlValue::Null)
        }
        "labels" => context
            .row
            .created_nodes
            .get(alias)
            .map(|&index| {
                GqlValue::List(
                    context.materialized.nodes[index]
                        .labels
                        .iter()
                        .cloned()
                        .map(GqlValue::String)
                        .collect(),
                )
            })
            .map(Ok)
            .unwrap_or_else(|| {
                context
                    .node_id(alias)
                    .map(|id| context.node_labels_value(id))
                    .transpose()
                    .map(|value| value.unwrap_or(GqlValue::Null))
            }),
        "type" => context
            .row
            .created_edges
            .get(alias)
            .map(|&index| GqlValue::String(context.materialized.edges[index].label.clone()))
            .map(Ok)
            .unwrap_or_else(|| {
                context
                    .edge_id(alias)
                    .map(|id| context.edge_label_value(id))
                    .transpose()
                    .map(|value| value.unwrap_or(GqlValue::Null))
            }),
        "length" => Ok(context
            .path(alias)
            .map(|path| GqlValue::UInt(path.edge_ids.len() as u64))
            .unwrap_or(GqlValue::Null)),
        "node_ids" => Ok(context
            .path(alias)
            .map(|path| gql_id_list_value(&path.node_ids))
            .unwrap_or(GqlValue::Null)),
        "edge_ids" => Ok(context
            .path(alias)
            .map(|path| gql_id_list_value(&path.edge_ids))
            .unwrap_or(GqlValue::Null)),
        "start_node" => Ok(context
            .path(alias)
            .and_then(|path| path.node_ids.first().copied())
            .map(|id| {
                if context.path_id_only {
                    Ok(GqlValue::UInt(id))
                } else {
                    context.node_value(id)
                }
            })
            .transpose()?
            .unwrap_or(GqlValue::Null)),
        "end_node" => Ok(context
            .path(alias)
            .and_then(|path| path.node_ids.last().copied())
            .map(|id| {
                if context.path_id_only {
                    Ok(GqlValue::UInt(id))
                } else {
                    context.node_value(id)
                }
            })
            .transpose()?
            .unwrap_or(GqlValue::Null)),
        "nodes" => match context.path(alias) {
            Some(path) if context.path_id_only => Ok(gql_id_list_value(&path.node_ids)),
            Some(path) => Ok(GqlValue::List(
                path.node_ids
                    .iter()
                    .copied()
                    .map(|id| context.node_value(id))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            None => Ok(GqlValue::Null),
        },
        "relationships" => match context.path(alias) {
            Some(path) if context.path_id_only => Ok(gql_id_list_value(&path.edge_ids)),
            Some(path) => Ok(GqlValue::List(
                path.edge_ids
                    .iter()
                    .copied()
                    .map(|id| context.edge_value(id))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            None => Ok(GqlValue::Null),
        },
        _ => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "unsupported GQL scalar function".to_string(),
            span.clone(),
        )),
    }
}

impl<'a> GqlMutationReturnEvalContext<'a> {
    fn node_id(&self, alias: &str) -> Option<u64> {
        gql_mutation_node_id_for_alias(alias, self.row, self.materialized, self.commit)
    }

    fn edge_id(&self, alias: &str) -> Option<u64> {
        gql_mutation_edge_id_for_alias(alias, self.row, self.materialized, self.commit)
    }

    fn path(&self, alias: &str) -> Option<&GqlPathIdentity> {
        self.row.read_paths.get(alias).and_then(Option::as_ref)
    }

    fn node_value(&self, id: u64) -> Result<GqlValue, EngineError> {
        Ok(GqlValue::Node(self.gql_node(id)?))
    }

    fn edge_value(&self, id: u64) -> Result<GqlValue, EngineError> {
        Ok(GqlValue::Edge(self.gql_edge(id)?))
    }

    fn path_value(&self, path: &GqlPathIdentity) -> Result<GqlValue, EngineError> {
        Ok(GqlValue::Path(GqlPath {
            node_ids: path.node_ids.clone(),
            edge_ids: path.edge_ids.clone(),
            nodes: Some(
                path.node_ids
                    .iter()
                    .copied()
                    .map(|id| self.gql_node(id))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            edges: Some(
                path.edge_ids
                    .iter()
                    .copied()
                    .map(|id| self.gql_edge(id))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        }))
    }

    fn node_property_value(&self, id: u64, property: &str) -> Result<GqlValue, EngineError> {
        let node = self.gql_node(id)?;
        Ok(gql_node_property_from_value(node, property))
    }

    fn edge_property_value(&self, id: u64, property: &str) -> Result<GqlValue, EngineError> {
        let edge = self.gql_edge(id)?;
        Ok(gql_edge_property_from_value(edge, property))
    }

    fn node_labels_value(&self, id: u64) -> Result<GqlValue, EngineError> {
        Ok(GqlValue::List(
            self.gql_node(id)?
                .labels
                .unwrap_or_default()
                .into_iter()
                .map(GqlValue::String)
                .collect(),
        ))
    }

    fn edge_label_value(&self, id: u64) -> Result<GqlValue, EngineError> {
        Ok(self
            .gql_edge(id)?
            .label
            .map(GqlValue::String)
            .unwrap_or(GqlValue::Null))
    }

    fn gql_node(&self, id: u64) -> Result<GqlNode, EngineError> {
        if self.commit.is_none() {
            if let Some(node) = self.materialized.existing_nodes.get(&id) {
                return Ok(gql_node_from_existing_execution(id, node, self.include_vectors));
            }
        }
        if let Some(node) = self.hydrated.nodes.get(&id) {
            return Ok(gql_node_from_record(&node.record, &node.labels, self.include_vectors));
        }
        if let Some(node) = self.materialized.existing_nodes.get(&id) {
            return Ok(gql_node_from_existing_execution(id, node, self.include_vectors));
        }
        if let Some(node) = self.materialized.nodes.iter().find(|node| {
            self.commit
                .and_then(|commit| commit.local_node_ids.get(&node.local).copied())
                .is_some_and(|committed_id| committed_id == id)
        })
        {
            return Ok(gql_node_from_created_execution(
                node,
                self.commit,
                self.include_vectors,
            ));
        }
        Err(EngineError::InvalidOperation(format!(
            "GQL mutation RETURN node {id} was not hydrated"
        )))
    }

    fn gql_edge(&self, id: u64) -> Result<GqlEdge, EngineError> {
        if self.commit.is_none() {
            if let Some(edge) = self.materialized.existing_edges.get(&id) {
                return Ok(gql_edge_from_existing_execution(id, edge));
            }
        }
        if let Some(edge) = self.hydrated.edges.get(&id) {
            return Ok(gql_edge_from_record(&edge.record, &edge.label));
        }
        if let Some(edge) = self.materialized.existing_edges.get(&id) {
            return Ok(gql_edge_from_existing_execution(id, edge));
        }
        if let Some(edge) = self.materialized.edges.iter().find(|edge| {
            edge.local
                .as_ref()
                .and_then(|local| {
                    self.commit
                        .and_then(|commit| commit.local_edge_ids.get(local).copied())
                })
                .is_some_and(|committed_id| committed_id == id)
        })
        {
            return Ok(gql_edge_from_created_execution(edge, self.commit));
        }
        Err(EngineError::InvalidOperation(format!(
            "GQL mutation RETURN edge {id} was not hydrated"
        )))
    }
}

fn gql_node_property_from_value(node: GqlNode, property: &str) -> GqlValue {
    match property {
        "id" => node.id.map(GqlValue::UInt).unwrap_or(GqlValue::Null),
        "labels" => node
            .labels
            .map(|labels| GqlValue::List(labels.into_iter().map(GqlValue::String).collect()))
            .unwrap_or(GqlValue::Null),
        "key" => node.key.map(GqlValue::String).unwrap_or(GqlValue::Null),
        "weight" => node
            .weight
            .map(|value| GqlValue::Float(value as f64))
            .unwrap_or(GqlValue::Null),
        "created_at" => node.created_at.map(GqlValue::Int).unwrap_or(GqlValue::Null),
        "updated_at" => node.updated_at.map(GqlValue::Int).unwrap_or(GqlValue::Null),
        other => node
            .props
            .and_then(|props| props.get(other).cloned())
            .unwrap_or(GqlValue::Null),
    }
}

fn gql_edge_property_from_value(edge: GqlEdge, property: &str) -> GqlValue {
    match property {
        "id" => edge.id.map(GqlValue::UInt).unwrap_or(GqlValue::Null),
        "from" => edge.from.map(GqlValue::UInt).unwrap_or(GqlValue::Null),
        "to" => edge.to.map(GqlValue::UInt).unwrap_or(GqlValue::Null),
        "label" | "type" => edge.label.map(GqlValue::String).unwrap_or(GqlValue::Null),
        "weight" => edge
            .weight
            .map(|value| GqlValue::Float(value as f64))
            .unwrap_or(GqlValue::Null),
        "created_at" => edge.created_at.map(GqlValue::Int).unwrap_or(GqlValue::Null),
        "updated_at" => edge.updated_at.map(GqlValue::Int).unwrap_or(GqlValue::Null),
        "valid_from" => edge.valid_from.map(GqlValue::Int).unwrap_or(GqlValue::Null),
        "valid_to" => edge.valid_to.map(GqlValue::Int).unwrap_or(GqlValue::Null),
        other => edge
            .props
            .and_then(|props| props.get(other).cloned())
            .unwrap_or(GqlValue::Null),
    }
}

fn gql_mutation_node_id_for_alias(
    alias: &str,
    row: &GqlCreateExecutionRow,
    materialized: &GqlCreateMaterialization,
    commit: Option<&TxnCommitResult>,
) -> Option<u64> {
    if let Some(&node_index) = row.created_nodes.get(alias) {
        let node = &materialized.nodes[node_index];
        return commit.and_then(|commit| commit.local_node_ids.get(&node.local).copied());
    }
    row.read_nodes.get(alias).copied().flatten()
}

fn gql_mutation_edge_id_for_alias(
    alias: &str,
    row: &GqlCreateExecutionRow,
    materialized: &GqlCreateMaterialization,
    commit: Option<&TxnCommitResult>,
) -> Option<u64> {
    if let Some(&edge_index) = row.created_edges.get(alias) {
        let edge = &materialized.edges[edge_index];
        return edge
            .local
            .as_ref()
            .and_then(|local| commit.and_then(|commit| commit.local_edge_ids.get(local).copied()));
    }
    row.read_edges.get(alias).copied().flatten()
}

fn gql_node_from_record(record: &NodeRecord, labels: &[String], include_vectors: bool) -> GqlNode {
    GqlNode {
        id: Some(record.id),
        labels: Some(labels.to_vec()),
        key: Some(record.key.clone()),
        props: Some(gql_props_from_prop_map(&record.props)),
        weight: Some(record.weight),
        created_at: Some(record.created_at),
        updated_at: Some(record.updated_at),
        dense_vector: include_vectors.then(|| record.dense_vector.clone()).flatten(),
        sparse_vector: include_vectors.then(|| record.sparse_vector.clone()).flatten(),
    }
}

fn gql_edge_from_record(record: &EdgeRecord, label: &str) -> GqlEdge {
    GqlEdge {
        id: Some(record.id),
        from: Some(record.from),
        to: Some(record.to),
        label: Some(label.to_string()),
        props: Some(gql_props_from_prop_map(&record.props)),
        weight: Some(record.weight),
        created_at: Some(record.created_at),
        updated_at: Some(record.updated_at),
        valid_from: Some(record.valid_from),
        valid_to: Some(record.valid_to),
    }
}

fn gql_node_from_existing_execution(
    id: u64,
    node: &GqlExistingNodeExecution,
    include_vectors: bool,
) -> GqlNode {
    GqlNode {
        id: Some(id),
        labels: Some(node.labels.clone()),
        key: Some(node.original.key.clone()),
        props: Some(gql_props_from_prop_map(&node.props)),
        weight: Some(node.weight),
        created_at: Some(node.original.created_at),
        updated_at: Some(node.original.updated_at),
        dense_vector: include_vectors.then(|| node.dense_vector.clone()).flatten(),
        sparse_vector: include_vectors.then(|| node.sparse_vector.clone()).flatten(),
    }
}

fn gql_edge_from_existing_execution(id: u64, edge: &GqlExistingEdgeExecution) -> GqlEdge {
    GqlEdge {
        id: Some(id),
        from: Some(edge.original.from),
        to: Some(edge.original.to),
        label: Some(edge.label.clone()),
        props: Some(gql_props_from_prop_map(&edge.props)),
        weight: Some(edge.weight),
        created_at: Some(edge.original.created_at),
        updated_at: Some(edge.original.updated_at),
        valid_from: Some(edge.valid_from),
        valid_to: Some(edge.valid_to),
    }
}

fn gql_node_from_created_execution(
    node: &GqlCreatedNodeExecution,
    commit: Option<&TxnCommitResult>,
    include_vectors: bool,
) -> GqlNode {
    GqlNode {
        id: commit.and_then(|commit| commit.local_node_ids.get(&node.local).copied()),
        labels: Some(node.labels.clone()),
        key: Some(node.key.clone()),
        props: Some(gql_props_from_prop_map(&node.props)),
        weight: Some(node.weight),
        created_at: None,
        updated_at: None,
        dense_vector: include_vectors.then_some(None).flatten(),
        sparse_vector: include_vectors.then_some(None).flatten(),
    }
}

fn gql_edge_from_created_execution(
    edge: &GqlCreatedEdgeExecution,
    commit: Option<&TxnCommitResult>,
) -> GqlEdge {
    GqlEdge {
        id: edge
            .local
            .as_ref()
            .and_then(|local| commit.and_then(|commit| commit.local_edge_ids.get(local).copied())),
        from: gql_txn_node_ref_id(&edge.from, commit),
        to: gql_txn_node_ref_id(&edge.to, commit),
        label: Some(edge.label.clone()),
        props: Some(gql_props_from_prop_map(&edge.props)),
        weight: Some(edge.weight),
        created_at: None,
        updated_at: None,
        valid_from: edge.valid_from,
        valid_to: Some(edge.valid_to.unwrap_or(i64::MAX)),
    }
}

fn gql_id_list_value(ids: &[u64]) -> GqlValue {
    GqlValue::List(ids.iter().copied().map(GqlValue::UInt).collect())
}

fn gql_path_identity_value(path: &GqlPathIdentity) -> GqlValue {
    GqlValue::Path(GqlPath {
        node_ids: path.node_ids.clone(),
        edge_ids: path.edge_ids.clone(),
        nodes: None,
        edges: None,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GqlMutationSortAtom {
    Null,
    Bool(bool),
    Number(NumericRangeSortKey),
    String(Vec<u8>),
    Bytes(Vec<u8>),
    Path {
        hop_count: usize,
        nodes: Vec<u64>,
        edges: Vec<u64>,
    },
}

fn gql_mutation_sort_atom_for_value(
    value: &GqlValue,
    span: &SourceSpan,
) -> Result<GqlMutationSortAtom, EngineError> {
    Ok(match value {
        GqlValue::Null => GqlMutationSortAtom::Null,
        GqlValue::Bool(value) => GqlMutationSortAtom::Bool(*value),
        GqlValue::Int(value) => GqlMutationSortAtom::Number(
            crate::property_value_semantics::numeric_range_sort_key(
                crate::property_value_semantics::numeric_key_from_i64(*value),
            ),
        ),
        GqlValue::UInt(value) => GqlMutationSortAtom::Number(
            crate::property_value_semantics::numeric_range_sort_key(
                crate::property_value_semantics::numeric_key_from_u64(*value),
            ),
        ),
        GqlValue::Float(value) => GqlMutationSortAtom::Number(
            crate::property_value_semantics::numeric_range_sort_key(
                crate::property_value_semantics::numeric_key_from_f64(*value)
                    .ok_or_else(|| gql_order_key_error(span))?,
            ),
        ),
        GqlValue::String(value) => GqlMutationSortAtom::String(value.as_bytes().to_vec()),
        GqlValue::Bytes(value) => GqlMutationSortAtom::Bytes(value.clone()),
        GqlValue::Node(_) | GqlValue::Edge(_) => return Err(gql_order_key_error(span)),
        GqlValue::Path(path) => GqlMutationSortAtom::Path {
            hop_count: path.edge_ids.len(),
            nodes: path.node_ids.clone(),
            edges: path.edge_ids.clone(),
        },
        GqlValue::List(_) | GqlValue::Map(_) => return Err(gql_order_key_error(span)),
    })
}

fn compare_gql_mutation_sort_atoms(
    left: &GqlMutationSortAtom,
    right: &GqlMutationSortAtom,
) -> std::cmp::Ordering {
    match (left, right) {
        (GqlMutationSortAtom::Null, GqlMutationSortAtom::Null) => std::cmp::Ordering::Equal,
        (GqlMutationSortAtom::Null, _) => std::cmp::Ordering::Greater,
        (_, GqlMutationSortAtom::Null) => std::cmp::Ordering::Less,
        _ => gql_mutation_sort_rank(left)
            .cmp(&gql_mutation_sort_rank(right))
            .then_with(|| left.cmp(right)),
    }
}

fn gql_mutation_sort_rank(value: &GqlMutationSortAtom) -> u8 {
    match value {
        GqlMutationSortAtom::Null => 255,
        GqlMutationSortAtom::Bool(_) => 0,
        GqlMutationSortAtom::Number(_) => 1,
        GqlMutationSortAtom::String(_) => 2,
        GqlMutationSortAtom::Bytes(_) => 3,
        GqlMutationSortAtom::Path { .. } => 4,
    }
}

fn gql_values_equal_for_mutation(left: &GqlValue, right: &GqlValue) -> bool {
    if let Some(ordering) = partial_cmp_gql_mutation_numbers(left, right) {
        return ordering == std::cmp::Ordering::Equal;
    }
    match (left, right) {
        (GqlValue::Null, GqlValue::Null) => true,
        (GqlValue::Bool(left), GqlValue::Bool(right)) => left == right,
        (GqlValue::String(left), GqlValue::String(right)) => left == right,
        (GqlValue::Bytes(left), GqlValue::Bytes(right)) => left == right,
        (GqlValue::Node(left), GqlValue::Node(right)) => left.id == right.id,
        (GqlValue::Edge(left), GqlValue::Edge(right)) => left.id == right.id,
        (GqlValue::Path(left), GqlValue::Path(right)) => {
            left.node_ids == right.node_ids && left.edge_ids == right.edge_ids
        }
        (GqlValue::List(left), GqlValue::List(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| gql_values_equal_for_mutation(left, right))
        }
        (GqlValue::Map(left), GqlValue::Map(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| gql_values_equal_for_mutation(left, right))
                })
        }
        _ => false,
    }
}

fn partial_cmp_gql_mutation_values(
    left: &GqlValue,
    right: &GqlValue,
) -> Option<std::cmp::Ordering> {
    partial_cmp_gql_mutation_numbers(left, right).or_else(|| match (left, right) {
        (GqlValue::String(left), GqlValue::String(right)) => Some(left.cmp(right)),
        _ => None,
    })
}

fn partial_cmp_gql_mutation_numbers(
    left: &GqlValue,
    right: &GqlValue,
) -> Option<std::cmp::Ordering> {
    Some(crate::property_value_semantics::compare_numeric_keys(
        gql_numeric_key(left)?,
        gql_numeric_key(right)?,
    ))
}

fn gql_numeric_key(value: &GqlValue) -> Option<crate::property_value_semantics::NumericScalarKey> {
    match value {
        GqlValue::Int(value) => Some(crate::property_value_semantics::numeric_key_from_i64(*value)),
        GqlValue::UInt(value) => Some(crate::property_value_semantics::numeric_key_from_u64(*value)),
        GqlValue::Float(value) => crate::property_value_semantics::numeric_key_from_f64(*value),
        _ => None,
    }
}

fn gql_create_pattern_has_null_read_endpoint(
    plan: &GqlMutationPlan,
    pattern: &GqlCreatePatternPlan,
    row: &GqlCreateExecutionRow,
) -> bool {
    pattern.nodes.iter().any(|node| {
        !node.created
            && plan
                .semantic
                .aliases
                .get(&node.alias)
                .is_some_and(|binding| binding.origin == GqlAliasOrigin::ReadPrefix)
            && row
                .read_nodes
                .get(&node.alias)
                .is_some_and(|id| id.is_none())
    })
}

fn gql_create_node_ref_for_alias(
    row: &GqlCreateExecutionRow,
    alias: &str,
    nodes: &[GqlCreatedNodeExecution],
) -> Result<Option<TxnNodeRef>, EngineError> {
    if let Some(&node_index) = row.created_nodes.get(alias) {
        return Ok(Some(TxnNodeRef::Local(nodes[node_index].local.clone())));
    }
    if let Some(id) = row.read_nodes.get(alias) {
        return Ok(id.map(TxnNodeRef::Id));
    }
    Err(EngineError::InvalidOperation(format!(
        "GQL CREATE endpoint alias '{alias}' was not materialized"
    )))
}

fn gql_create_endpoint_key(target: &TxnNodeRef) -> GqlCreateEndpointKey {
    match target {
        TxnNodeRef::Id(id) => GqlCreateEndpointKey::Id(*id),
        TxnNodeRef::Local(local) => GqlCreateEndpointKey::Local(local.clone()),
        TxnNodeRef::Key { label, key } => {
            GqlCreateEndpointKey::Local(TxnLocalRef::Alias(format!("{label}:{key}")))
        }
    }
}

fn gql_create_expr_value(row: &GqlCreateExecutionRow, expr_id: usize) -> Result<&GraphValue, EngineError> {
    row.expr_values
        .get(expr_id)
        .and_then(|value| value.as_ref())
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GQL mutation expression ref #{expr_id} is missing from execution row"
            ))
        })
}

fn gql_internal_id_value(value: &GraphValue, alias: &str) -> Result<Option<u64>, EngineError> {
    match value {
        GraphValue::Null => Ok(None),
        GraphValue::UInt(value) | GraphValue::NodeId(value) | GraphValue::EdgeId(value) => {
            Ok(Some(*value))
        }
        GraphValue::Int(value) if *value >= 0 => Ok(u64::try_from(*value).ok()),
        other => Err(EngineError::InvalidOperation(format!(
            "mutation read-prefix alias '{alias}' returned non-id value {other:?}"
        ))),
    }
}

fn gql_internal_path_identity(
    node_value: &GraphValue,
    edge_value: &GraphValue,
    alias: &str,
) -> Result<Option<GqlPathIdentity>, EngineError> {
    if matches!(node_value, GraphValue::Null) || matches!(edge_value, GraphValue::Null) {
        return Ok(None);
    }
    Ok(Some(GqlPathIdentity {
        node_ids: gql_internal_id_list(node_value, alias, "node_ids")?,
        edge_ids: gql_internal_id_list(edge_value, alias, "edge_ids")?,
    }))
}

fn gql_internal_id_list(
    value: &GraphValue,
    alias: &str,
    field: &str,
) -> Result<Vec<u64>, EngineError> {
    let GraphValue::List(values) = value else {
        return Err(EngineError::InvalidOperation(format!(
            "mutation read-prefix path alias '{alias}' returned non-list {field} value {value:?}"
        )));
    };
    values
        .iter()
        .map(|value| {
            gql_internal_id_value(value, alias)?.ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "mutation read-prefix path alias '{alias}' returned null {field} entry"
                ))
            })
        })
        .collect()
}

fn gql_create_string_key(value: &GraphValue) -> Result<String, EngineError> {
    match value {
        GraphValue::String(value) if !value.is_empty() => Ok(value.clone()),
        _ => Err(gql_create_invalid_value(
            "GQL CREATE node key must be a non-empty string",
        )),
    }
}

fn gql_create_weight(value: &GraphValue, field: &str) -> Result<f32, EngineError> {
    let as_f64 = match value {
        GraphValue::Int(value) => *value as f64,
        GraphValue::UInt(value) => *value as f64,
        GraphValue::Float(value) => *value,
        _ => {
            return Err(gql_create_invalid_value(format!(
                "GQL CREATE {field} must be a finite number"
            )))
        }
    };
    if !as_f64.is_finite() {
        return Err(gql_create_invalid_value(format!(
            "GQL CREATE {field} must be finite"
        )));
    }
    let as_f32 = as_f64 as f32;
    if !as_f32.is_finite() {
        return Err(gql_create_invalid_value(format!(
            "GQL CREATE {field} is out of f32 range"
        )));
    }
    Ok(as_f32)
}

fn gql_create_i64(value: &GraphValue, field: &str) -> Result<i64, EngineError> {
    match value {
        GraphValue::Int(value) => Ok(*value),
        GraphValue::UInt(value) => i64::try_from(*value).map_err(|_| {
            gql_create_invalid_value(format!("GQL CREATE {field} is out of i64 range"))
        }),
        _ => Err(gql_create_invalid_value(format!(
            "GQL CREATE {field} must be an integer epoch millis value"
        ))),
    }
}

fn gql_mutation_weight(value: &GraphValue, field: &str) -> Result<f32, EngineError> {
    let as_f64 = match value {
        GraphValue::Int(value) => *value as f64,
        GraphValue::UInt(value) => *value as f64,
        GraphValue::Float(value) => *value,
        _ => {
            return Err(gql_create_invalid_value(format!(
                "GQL SET {field} must be a finite number"
            )))
        }
    };
    if !as_f64.is_finite() {
        return Err(gql_create_invalid_value(format!(
            "GQL SET {field} must be finite"
        )));
    }
    let as_f32 = as_f64 as f32;
    if !as_f32.is_finite() {
        return Err(gql_create_invalid_value(format!(
            "GQL SET {field} is out of f32 range"
        )));
    }
    Ok(as_f32)
}

fn gql_mutation_i64(value: &GraphValue, field: &str) -> Result<i64, EngineError> {
    match value {
        GraphValue::Int(value) => Ok(*value),
        GraphValue::UInt(value) => i64::try_from(*value)
            .map_err(|_| gql_create_invalid_value(format!("GQL SET {field} is out of i64 range"))),
        _ => Err(gql_create_invalid_value(format!(
            "GQL SET {field} must be an integer epoch millis value"
        ))),
    }
}

fn gql_graph_value_to_prop(value: &GraphValue) -> Result<PropValue, EngineError> {
    match value {
        GraphValue::Null => Ok(PropValue::Null),
        GraphValue::Bool(value) => Ok(PropValue::Bool(*value)),
        GraphValue::Int(value) => Ok(PropValue::Int(*value)),
        GraphValue::UInt(value) | GraphValue::NodeId(value) | GraphValue::EdgeId(value) => {
            Ok(PropValue::UInt(*value))
        }
        GraphValue::Float(value) if value.is_finite() => Ok(PropValue::Float(*value)),
        GraphValue::Float(_) => Err(gql_create_invalid_value(
            "GQL CREATE property floats must be finite",
        )),
        GraphValue::String(value) => Ok(PropValue::String(value.clone())),
        GraphValue::Bytes(value) => Ok(PropValue::Bytes(value.clone())),
        GraphValue::List(values) => values
            .iter()
            .map(gql_graph_value_to_prop)
            .collect::<Result<Vec<_>, _>>()
            .map(PropValue::Array),
        GraphValue::Map(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), gql_graph_value_to_prop(value)?)))
            .collect::<Result<BTreeMap<_, _>, EngineError>>()
            .map(PropValue::Map),
        GraphValue::Node(_) | GraphValue::Edge(_) | GraphValue::Path(_) => Err(
            gql_create_invalid_value("GQL CREATE properties cannot store graph elements or paths"),
        ),
    }
}

fn graph_eval_value_to_graph_value(value: GraphEvalValue) -> Result<GraphValue, EngineError> {
    match value {
        GraphEvalValue::Null => Ok(GraphValue::Null),
        GraphEvalValue::Bool(value) => Ok(GraphValue::Bool(value)),
        GraphEvalValue::Int(value) => Ok(GraphValue::Int(value)),
        GraphEvalValue::UInt(value) => Ok(GraphValue::UInt(value)),
        GraphEvalValue::Float(value) => Ok(GraphValue::Float(value)),
        GraphEvalValue::String(value) => Ok(GraphValue::String(value)),
        GraphEvalValue::Bytes(value) => Ok(GraphValue::Bytes(value)),
        GraphEvalValue::List(values) => values
            .into_iter()
            .map(graph_eval_value_to_graph_value)
            .collect::<Result<Vec<_>, _>>()
            .map(GraphValue::List),
        GraphEvalValue::Map(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, graph_eval_value_to_graph_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, EngineError>>()
            .map(GraphValue::Map),
        GraphEvalValue::Node(_) | GraphEvalValue::Edge(_) | GraphEvalValue::Path(_) => Err(
            gql_create_invalid_value("GQL CREATE operation expression cannot produce graph elements"),
        ),
    }
}

fn gql_params_to_graph_params_for_mutation(
    params: &GqlParams,
    plan: &GqlMutationPlan,
    expr_ids: &[usize],
) -> BTreeMap<String, GraphParamValue> {
    let mut referenced = BTreeSet::new();
    for &expr_id in expr_ids {
        if let Some(expr) = plan.operation_exprs.get(expr_id) {
            collect_graph_expr_param_names(&expr.expr, &mut referenced);
        }
    }
    referenced
        .into_iter()
        .filter_map(|key| {
            params
                .get(&key)
                .map(|value| (key, gql_param_to_graph_param_for_mutation(value)))
        })
        .collect()
}

fn gql_param_to_graph_param_for_mutation(value: &GqlParamValue) -> GraphParamValue {
    match value {
        GqlParamValue::Null => GraphParamValue::Null,
        GqlParamValue::Bool(value) => GraphParamValue::Bool(*value),
        GqlParamValue::Int(value) => GraphParamValue::Int(*value),
        GqlParamValue::UInt(value) => GraphParamValue::UInt(*value),
        GqlParamValue::Float(value) => GraphParamValue::Float(*value),
        GqlParamValue::String(value) => GraphParamValue::String(value.clone()),
        GqlParamValue::Bytes(value) => GraphParamValue::Bytes(value.clone()),
        GqlParamValue::List(values) => {
            GraphParamValue::List(values.iter().map(gql_param_to_graph_param_for_mutation).collect())
        }
        GqlParamValue::Map(values) => GraphParamValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), gql_param_to_graph_param_for_mutation(value)))
                .collect(),
        ),
    }
}

fn gql_props_from_prop_map(props: &BTreeMap<String, PropValue>) -> BTreeMap<String, GqlValue> {
    props
        .iter()
        .map(|(key, value)| (key.clone(), gql_value_from_prop(value)))
        .collect()
}

fn gql_value_from_prop(value: &PropValue) -> GqlValue {
    match value {
        PropValue::Null => GqlValue::Null,
        PropValue::Bool(value) => GqlValue::Bool(*value),
        PropValue::Int(value) => GqlValue::Int(*value),
        PropValue::UInt(value) => GqlValue::UInt(*value),
        PropValue::Float(value) => GqlValue::Float(*value),
        PropValue::String(value) => GqlValue::String(value.clone()),
        PropValue::Bytes(value) => GqlValue::Bytes(value.clone()),
        PropValue::Array(values) => {
            GqlValue::List(values.iter().map(gql_value_from_prop).collect())
        }
        PropValue::Map(values) => GqlValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), gql_value_from_prop(value)))
                .collect(),
        ),
    }
}

fn gql_literal_to_value(literal: &Literal) -> GqlValue {
    match literal {
        Literal::Null => GqlValue::Null,
        Literal::Bool(value) => GqlValue::Bool(*value),
        Literal::Int(value) => GqlValue::Int(*value),
        Literal::Float(value) => GqlValue::Float(*value),
        Literal::String(value) => GqlValue::String(value.clone()),
    }
}

fn gql_param_to_value(value: &GqlParamValue) -> GqlValue {
    match value {
        GqlParamValue::Null => GqlValue::Null,
        GqlParamValue::Bool(value) => GqlValue::Bool(*value),
        GqlParamValue::Int(value) => GqlValue::Int(*value),
        GqlParamValue::UInt(value) => GqlValue::UInt(*value),
        GqlParamValue::Float(value) => GqlValue::Float(*value),
        GqlParamValue::String(value) => GqlValue::String(value.clone()),
        GqlParamValue::Bytes(value) => GqlValue::Bytes(value.clone()),
        GqlParamValue::List(values) => {
            GqlValue::List(values.iter().map(gql_param_to_value).collect())
        }
        GqlParamValue::Map(values) => GqlValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), gql_param_to_value(value)))
                .collect(),
        ),
    }
}

fn gql_txn_node_ref_id(
    target: &TxnNodeRef,
    commit: Option<&TxnCommitResult>,
) -> Option<u64> {
    match target {
        TxnNodeRef::Id(id) => Some(*id),
        TxnNodeRef::Local(local) => {
            commit.and_then(|commit| commit.local_node_ids.get(local).copied())
        }
        TxnNodeRef::Key { .. } => None,
    }
}

fn gql_create_invalid_value(message: impl Into<String>) -> EngineError {
    EngineError::InvalidOperation(message.into())
}

fn gql_create_conflict_error(message: impl Into<String>) -> EngineError {
    EngineError::InvalidOperation(message.into())
}

fn gql_create_return_unsupported(message: &str, span: &SourceSpan) -> EngineError {
    EngineError::GqlUnsupported {
        feature: "GQL mutation RETURN".to_string(),
        message: message.to_string(),
        span: span.clone(),
    }
}

fn gql_mutation_cap_error(name: &str, actual: usize, cap: usize) -> EngineError {
    EngineError::InvalidOperation(format!(
        "GQL mutation {name} exceeded: attempted {actual}, cap {cap}"
    ))
}

fn explain_gql_mutation(
    engine: &DatabaseEngine,
    mutation: GqlMutationStatement,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlExecutionExplain, EngineError> {
    if options.cursor.is_some() {
        return Err(EngineError::InvalidCursor {
            message: "GQL mutation statements do not accept cursors".into(),
        });
    }
    if options.mode == GqlExecutionMode::ReadOnly {
        return Err(gql_read_only_mutation_error(&mutation.span));
    }
    let plan = lower_mutation(mutation, params, options)?;
    let (_guard, published) = engine.runtime.published_snapshot()?;
    build_gql_mutation_explain_with_snapshot(published.view.as_ref(), &plan, params, options)
}

fn gql_read_only_mutation_error(span: &SourceSpan) -> EngineError {
    gql_semantic_error(
        GqlSemanticErrorCode::ReadOnlyViolation,
        "GQL mutation statements are not allowed when mode is ReadOnly".to_string(),
        span.clone(),
    )
}

fn wrap_read_gql_explain(
    read: GqlExplain,
    options: &GqlExecutionOptions,
) -> GqlExecutionExplain {
    GqlExecutionExplain {
        kind: GqlStatementKind::Query,
        columns: read.columns.clone(),
        warnings: read.warnings.clone(),
        read: Some(read),
        mutation: None,
        caps: gql_execution_cap_summary(options),
        notes: Vec::new(),
    }
}

fn validate_gql_mutation_plan_for_execution(plan: &GqlMutationPlan) -> Result<(), EngineError> {
    if let Some(read_prefix) = plan.read_prefix.as_ref() {
        normalize_gql_graph_row_target(&read_prefix.lowered)?;
    }
    Ok(())
}

fn build_gql_mutation_explain_with_snapshot(
    snapshot: &ReadView,
    plan: &GqlMutationPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlExecutionExplain, EngineError> {
    let columns = plan
        .return_plan
        .as_ref()
        .map(|return_plan| return_plan.columns.clone())
        .unwrap_or_default();
    let read_prefix = if let Some(read_prefix) = plan.read_prefix.as_ref() {
        let returns = return_exprs(&read_prefix.lowered.semantic);
        let graph_row_target = build_gql_explain(
            snapshot,
            &read_prefix.lowered,
            &returns,
            &[],
            options,
        )?;
        Some(GqlMutationReadPrefixExplain {
            graph_row_target,
            internal_columns: read_prefix
                .internal_columns
                .iter()
                .map(mutation_internal_column_summary)
                .collect(),
            target_aliases: read_prefix
                .internal_columns
                .iter()
                .filter_map(|column| match column {
                    GqlMutationInternalColumn::TargetId { alias, .. }
                    | GqlMutationInternalColumn::TargetPath { alias } => Some(alias.clone()),
                    GqlMutationInternalColumn::ExprValue { .. } => None,
                })
                .collect(),
            expression_columns: read_prefix
                .internal_columns
                .iter()
                .filter(|column| matches!(column, GqlMutationInternalColumn::ExprValue { .. }))
                .count(),
        })
    } else {
        None
    };
    let read = read_prefix
        .as_ref()
        .map(|prefix| prefix.graph_row_target.clone());
    let mut warnings = plan.warnings.clone();
    if let Some(read) = read_prefix.as_ref() {
        warnings.extend(read.graph_row_target.warnings.iter().cloned());
    }
    warnings.sort();
    warnings.dedup();
    let notes = vec![
        "Mutation explain is side-effect-free and does not open write transactions, allocate label tokens, append WAL records, mutate memtables, or enqueue index followups".to_string(),
        "CREATE, SET, REMOVE, DELETE, and DETACH DELETE execution are supported through one WriteTxn; SET/REMOVE use crate-private by-ID record replacement adapters and DETACH DELETE reuses transaction cascade planning".to_string(),
        "Mutation RETURN supports row operations, compact-row-compatible Rust rows, include-vectors projection, post-commit batch hydration, and crate-private returned-alias read-set validation for CREATE/SET/REMOVE; DELETE/DETACH RETURN remains rejected".to_string(),
    ];
    let return_explain = plan
        .return_plan
        .as_ref()
        .map(|return_plan| -> Result<GqlMutationReturnExplain, EngineError> {
            let skip = return_plan
                .skip
                .as_ref()
                .map(|expr| evaluate_gql_mutation_count_expr(expr, plan, params, options, "SKIP"))
                .transpose()?
                .unwrap_or(0);
            if skip > options.max_skip {
                return Err(gql_row_count_error(
                    return_plan.skip.as_ref().expect("skip checked above"),
                    format!("SKIP/OFFSET value {skip} exceeds max_skip={}", options.max_skip),
                ));
            }
            let limit = return_plan
                .limit
                .as_ref()
                .map(|expr| evaluate_gql_mutation_count_expr(expr, plan, params, options, "LIMIT"))
                .transpose()?;
            Ok(GqlMutationReturnExplain {
                columns: columns.clone(),
                order_items: return_plan.order_items,
                skip,
                limit,
                post_commit_hydration: "Mutation RETURN prevalidates expressions and row operations before staging, applies ORDER BY/SKIP/LIMIT to returned rows only, guards returned existing aliases and hydrated path elements with a crate-private read-set before commit, then performs deterministic post-commit batch projection".to_string(),
            })
        })
        .transpose()?;
    Ok(GqlExecutionExplain {
        kind: GqlStatementKind::Mutation,
        columns: columns.clone(),
        read,
        mutation: Some(GqlMutationExplain {
            read_prefix,
            operations: mutation_operation_explains(plan),
            return_plan: return_explain,
            would_create_node_labels: mutation_create_node_labels(plan),
            would_create_edge_labels: mutation_create_edge_labels(plan),
            uses_transaction_snapshot: plan.read_prefix.is_some(),
            uses_write_txn: true,
            replacement_adapters: mutation_uses_replacement_adapters(plan),
            atomic_commit: true,
        }),
        caps: gql_execution_cap_summary(options),
        warnings,
        notes,
    })
}

fn gql_execution_cap_summary(options: &GqlExecutionOptions) -> GqlExecutionCapSummary {
    GqlExecutionCapSummary {
        allow_full_scan: options.allow_full_scan,
        max_rows: options.max_rows,
        max_cursor_bytes: options.max_cursor_bytes,
        max_mutation_rows: options.max_mutation_rows,
        max_mutation_ops: options.max_mutation_ops,
        max_query_bytes: options.max_query_bytes,
        max_param_bytes: options.max_param_bytes,
        max_ast_depth: options.max_ast_depth,
        max_literal_items: options.max_literal_items,
        max_intermediate_bindings: options.max_intermediate_bindings,
        max_frontier: options.max_frontier,
        max_path_hops: options.max_path_hops,
        max_paths_per_start: options.max_paths_per_start,
        max_order_materialization: options.max_order_materialization,
        max_skip: options.max_skip,
    }
}

fn mutation_operation_explains(plan: &GqlMutationPlan) -> Vec<GqlMutationOperationExplain> {
    plan
        .clauses
        .iter()
        .flat_map(|clause| match clause {
            GqlMutationClausePlan::Create(patterns) => patterns
                .iter()
                .flat_map(|pattern| {
                    let node_ops = pattern.nodes.iter().map(create_node_operation_explain);
                    let edge_ops = pattern.edges.iter().map(create_edge_operation_explain);
                    node_ops.chain(edge_ops).collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            GqlMutationClausePlan::Set(items) => items.iter().map(set_operation_explain).collect(),
            GqlMutationClausePlan::Remove(items) => {
                items.iter().map(remove_operation_explain).collect()
            }
            GqlMutationClausePlan::Delete { detach, targets } => targets
                .iter()
                .map(|target| delete_operation_explain(*detach, target))
                .collect(),
        })
        .collect()
}

fn create_node_operation_explain(node: &GqlCreateNodePlan) -> GqlMutationOperationExplain {
    GqlMutationOperationExplain {
        op: if node.created {
            "CREATE NODE".to_string()
        } else {
            "USE NODE".to_string()
        },
        target_alias: Some(node.alias.clone()),
        row_multiplicity: "per mutation input row".to_string(),
        detail: format!(
            "labels={:?}; properties={:?}; staged through WriteTxn during execution",
            node.labels, node.property_keys
        ),
    }
}

fn create_edge_operation_explain(edge: &GqlCreateEdgePlan) -> GqlMutationOperationExplain {
    GqlMutationOperationExplain {
        op: "CREATE EDGE".to_string(),
        target_alias: edge.alias.clone(),
        row_multiplicity: "per mutation input row".to_string(),
        detail: format!(
            "{} -[:{}]-> {}; properties={:?}; staged through WriteTxn during execution",
            edge.from_alias, edge.label, edge.to_alias, edge.property_keys
        ),
    }
}

fn set_operation_explain(item: &GqlSetItemPlan) -> GqlMutationOperationExplain {
    match item {
        GqlSetItemPlan::Property {
            alias,
            kind,
            property,
            value,
        } => GqlMutationOperationExplain {
            op: "SET PROPERTY".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row".to_string(),
            detail: format!(
                "{kind:?}.{property} = expr #{}; by-ID replacement adapter required",
                value.id
            ),
        },
        GqlSetItemPlan::MapMerge { alias, kind, value } => GqlMutationOperationExplain {
            op: "SET MAP MERGE".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row".to_string(),
            detail: format!(
                "{kind:?} map merge from expr #{}; by-ID replacement adapter required",
                value.id
            ),
        },
        GqlSetItemPlan::NodeLabel { alias, label } => GqlMutationOperationExplain {
            op: "SET NODE LABEL".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row".to_string(),
            detail: format!("add label {label:?}; by-ID replacement adapter required"),
        },
    }
}

fn remove_operation_explain(item: &GqlRemoveItemPlan) -> GqlMutationOperationExplain {
    match item {
        GqlRemoveItemPlan::Property {
            alias,
            kind,
            property,
        } => GqlMutationOperationExplain {
            op: "REMOVE PROPERTY".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row".to_string(),
            detail: format!("{kind:?}.{property}; by-ID replacement adapter required"),
        },
        GqlRemoveItemPlan::NodeLabel { alias, label } => GqlMutationOperationExplain {
            op: "REMOVE NODE LABEL".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row".to_string(),
            detail: format!("remove label {label:?}; by-ID replacement adapter required"),
        },
    }
}

fn delete_operation_explain(
    detach: bool,
    target: &GqlDeleteTargetPlan,
) -> GqlMutationOperationExplain {
    GqlMutationOperationExplain {
        op: if detach {
            "DETACH DELETE".to_string()
        } else {
            "DELETE".to_string()
        },
        target_alias: Some(target.alias.clone()),
        row_multiplicity: "per mutation input row".to_string(),
        detail: format!(
            "{:?} target; staged through WriteTxn delete intents during execution",
            target.kind
        ),
    }
}

fn mutation_create_node_labels(plan: &GqlMutationPlan) -> Vec<String> {
    let mut labels = BTreeSet::new();
    for clause in &plan.clauses {
        if let GqlMutationClausePlan::Create(patterns) = clause {
            for pattern in patterns {
                for node in &pattern.nodes {
                    if node.created {
                        labels.extend(node.labels.iter().cloned());
                    }
                }
            }
        }
    }
    labels.into_iter().collect()
}

fn mutation_create_edge_labels(plan: &GqlMutationPlan) -> Vec<String> {
    let mut labels = BTreeSet::new();
    for clause in &plan.clauses {
        if let GqlMutationClausePlan::Create(patterns) = clause {
            for pattern in patterns {
                labels.extend(pattern.edges.iter().map(|edge| edge.label.clone()));
            }
        }
    }
    labels.into_iter().collect()
}

fn mutation_uses_replacement_adapters(plan: &GqlMutationPlan) -> bool {
    plan.clauses
        .iter()
        .any(|clause| matches!(clause, GqlMutationClausePlan::Set(_) | GqlMutationClausePlan::Remove(_)))
}

fn mutation_internal_column_summary(column: &GqlMutationInternalColumn) -> String {
    match column {
        GqlMutationInternalColumn::TargetId { alias, kind } => {
            format!("target id: {alias} ({kind:?})")
        }
        GqlMutationInternalColumn::TargetPath { alias } => {
            format!("target path identity: {alias}")
        }
        GqlMutationInternalColumn::ExprValue { id, expr } => {
            format!("expr value #{id}: {expr:?}")
        }
    }
}

impl ReadView {
    fn explain_graph_rows_normalized(
        &self,
        query: &NormalizedGraphRowQuery,
        cursor_state: GraphRowCursorState,
    ) -> Result<GraphRowExplain, EngineError> {
        let fingerprints =
            graph_row_cursor_fingerprints(query, cursor_state.effective_at_epoch, cursor_state.original_skip);
        if let Some(cursor) = cursor_state.decoded.as_ref() {
            graph_row_validate_cursor_fingerprints(cursor, &fingerprints)?;
            graph_row_validate_cursor_shape(query, cursor)?;
        }

        let mut trace = GraphRowExplainTrace::default();
        self.populate_graph_row_explain_trace(query, &cursor_state, &mut trace)?;
        Ok(build_graph_row_explain(
            query,
            Some(cursor_state.effective_at_epoch),
            &cursor_state,
            Some(trace),
            None,
        ))
    }

    fn populate_graph_row_explain_trace(
        &self,
        query: &NormalizedGraphRowQuery,
        cursor_state: &GraphRowCursorState,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<(), EngineError> {
        let runtime = self.normalize_graph_row_explain_runtime_plan(query, trace)?;
        let physical_plan = self.plan_graph_row_physical(query, &runtime)?;
        self.populate_graph_row_explain_trace_from_runtime(
            query,
            cursor_state,
            &runtime,
            &physical_plan,
            trace,
        )
    }

    fn populate_graph_row_explain_trace_from_runtime(
        &self,
        query: &NormalizedGraphRowQuery,
        cursor_state: &GraphRowCursorState,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<(), EngineError> {
        trace.record_plan(
            "GraphRowPhysicalPlan",
            format!(
                "fanout-aware fixed required executor; nodes={}, fixed_edges={}, fixed_path_compositions={}, optional_apply_groups={}, variable_length_paths={}; initial_driver={}; physical_edge_order={:?}; final logical row order/cursor pipeline remains independent of physical order",
                query.nodes.len(),
                query
                    .pieces
                    .iter()
                    .filter(|piece| matches!(piece, GraphPatternPiece::Edge(_)))
                    .count(),
                query.fixed_paths.len(),
                query
                    .pieces
                    .iter()
                    .filter(|piece| matches!(piece, GraphPatternPiece::Optional(_)))
                    .count(),
                query
                    .pieces
                    .iter()
                    .filter(|piece| matches!(piece, GraphPatternPiece::VariableLength(_)))
                    .count(),
                graph_row_initial_driver_detail(physical_plan),
                graph_row_physical_edge_order_detail(runtime, &physical_plan.edge_order)
            ),
        );
        trace.record_physical_plan(runtime, physical_plan);

        if graph_row_node_only_default_order_fast_path(query, runtime) {
            let anchor = &runtime.nodes[0];
            let mut anchor_query = anchor.query.clone();
            if let Some(cursor) = cursor_state.decoded.as_ref() {
                if let [crate::graph_row::GraphSortAtom::Node(id)] =
                    cursor.last_logical_row_key.as_slice()
                {
                    anchor_query.page.after = Some(*id);
                }
            }
            let selection_capacity = graph_row_selection_capacity(query, cursor_state)?;
            anchor_query.page.limit = Some(selection_capacity);
            let planned = self.plan_normalized_node_query(&anchor_query)?;
            trace.record_node_plan(
                &anchor.alias,
                "node-only default-order fast path candidate source",
                &planned,
            );
            trace.mark_bound_node_alias(&anchor.alias, Some(anchor.query.ids.as_slice()));
        } else if physical_plan.segments.is_empty() {
            match &physical_plan.initial_driver {
                GraphRowInitialDriver::Node { node_index, .. } => {
                    let anchor = &runtime.nodes[*node_index];
                    let mut anchor_query = anchor.query.clone();
                    anchor_query.page.limit =
                        Some(query.options.max_intermediate_bindings.saturating_add(1));
                    let planned = self.plan_normalized_node_query(&anchor_query)?;
                    trace.record_node_plan(&anchor.alias, "physical initial node driver", &planned);
                    trace.mark_bound_node_alias(&anchor.alias, Some(anchor.query.ids.as_slice()));
                }
                GraphRowInitialDriver::Empty { .. } | GraphRowInitialDriver::Edge { .. } => {
                    trace.record_plan(
                        "InitialRows",
                        "starts from one empty binding row because the chosen physical driver is an edge source or deterministic no-anchor fallback",
                    );
                }
            }
        }

        if !graph_row_node_only_default_order_fast_path(query, runtime) {
            for node in &runtime.nodes {
                if trace.is_bound(&node.alias) || !graph_row_node_query_has_anchor(&node.query) {
                    continue;
                }
                let mut candidate_query = node.query.clone();
                candidate_query.page.limit =
                    Some(query.options.max_intermediate_bindings.saturating_add(1));
                if let Ok(planned) = self.plan_normalized_node_query(&candidate_query) {
                    trace.record_node_plan(
                        &node.alias,
                        "considered physical node anchor alternative",
                        &planned,
                    );
                }
            }
        }

        if physical_plan.segments.is_empty() {
            for &edge_index in &physical_plan.edge_order {
                self.populate_graph_row_physical_edge_explain(
                    query,
                    runtime,
                    physical_plan,
                    edge_index,
                    trace,
                )?;
            }
        } else {
            for segment in &physical_plan.segments {
                if !segment.barriers_before.is_empty() {
                    trace.record_plan(
                        "RequiredSegmentBarrier",
                        format!(
                            "segment={}; barriers_before={}; required fixed pieces on either side are planned independently and never reordered across this boundary",
                            segment.segment_index,
                            graph_row_barriers_detail(&segment.barriers_before)
                        ),
                    );
                }
                match &segment.initial_driver {
                    GraphRowInitialDriver::Node { node_index, .. } => {
                        let anchor = &runtime.nodes[*node_index];
                        if !trace.is_bound(&anchor.alias) {
                            let mut anchor_query = anchor.query.clone();
                            anchor_query.page.limit =
                                Some(query.options.max_intermediate_bindings.saturating_add(1));
                            let planned = self.plan_normalized_node_query(&anchor_query)?;
                            trace.record_node_plan(
                                &anchor.alias,
                                &format!(
                                    "physical initial node driver for required segment {}",
                                    segment.segment_index
                                ),
                                &planned,
                            );
                            trace.mark_bound_node_alias(
                                &anchor.alias,
                                Some(anchor.query.ids.as_slice()),
                            );
                        }
                    }
                    GraphRowInitialDriver::Empty { .. } | GraphRowInitialDriver::Edge { .. } => {
                        trace.record_plan(
                            "InitialRows",
                            format!(
                                "segment={}; starts from current binding frontier because the chosen physical driver is an edge source or deterministic no-anchor fallback",
                                segment.segment_index
                            ),
                        );
                    }
                }
                for &edge_index in &segment.edge_order {
                    self.populate_graph_row_physical_edge_explain(
                        query,
                        runtime,
                        physical_plan,
                        edge_index,
                        trace,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn populate_graph_row_physical_edge_explain(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
        edge_index: usize,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<(), EngineError> {
        let edge = &runtime.edges[edge_index];
        let planned_source_choice = physical_plan
            .edge_source_choices
            .get(edge_index)
            .and_then(|choice| *choice);
        self.populate_graph_row_edge_explain(query, edge, planned_source_choice, trace)?;
        trace.mark_bound_node_alias(
            &edge.from_alias,
            graph_row_runtime_node_explicit_ids(runtime, &edge.from_alias),
        );
        trace.mark_bound_node_alias(
            &edge.to_alias,
            graph_row_runtime_node_explicit_ids(runtime, &edge.to_alias),
        );
        if let Some(edge_alias) = edge.edge_alias() {
            trace.mark_bound_alias(edge_alias);
        }
        Ok(())
    }

    fn normalize_graph_row_explain_runtime_plan(
        &self,
        query: &NormalizedGraphRowQuery,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<GraphRowRuntimePlan, EngineError> {
        let runtime = self.normalize_graph_row_runtime_plan(query)?;
        for warning in &runtime.warnings {
            trace.record_warning(format!("{warning:?}"));
        }
        self.record_graph_row_static_step_explain(query, &runtime, trace)?;
        Ok(runtime)
    }

    fn record_graph_row_static_step_explain(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<(), EngineError> {
        for step in &runtime.steps {
            match step {
                GraphRowRuntimeStep::RequiredSegment(_) => {}
                GraphRowRuntimeStep::FixedPath(path) => {
                    trace.record_plan(
                        "FixedPathCompose",
                        format!(
                            "path={}; nodes={}; edges={}; stores ID vectors only and hydrates path elements after final page selection",
                            path.alias,
                            path.node_slots.len(),
                            path.edge_slots.len()
                        ),
                    );
                    trace.mark_bound_alias(&path.alias);
                }
                GraphRowRuntimeStep::Optional(group) => {
                    trace.record_plan(
                        "OptionalApply",
                        format!(
                            "piece_index={}; pieces={}; introduced_slots={}; dependency_slots={}; left_slots={}; left_outer=true; barrier=true; where_present={}; optional filters affect matching only; misses null-extend introduced aliases without overwriting outer aliases",
                            group.piece_index,
                            group.pieces_len,
                            graph_row_slot_list_detail(query, &group.introduced_slots),
                            graph_row_slot_list_detail(query, &group.dependency_slots),
                            graph_row_slot_list_detail(query, &group.left_slots),
                            group.where_present
                        ),
                    );
                    let group_physical_plan = self.plan_graph_row_physical(query, &group.runtime)?;
                    trace.record_physical_plan(&group.runtime, &group_physical_plan);
                    self.record_graph_row_static_step_explain(query, &group.runtime, trace)?;
                }
                GraphRowRuntimeStep::VariableLength(path) => {
                    trace.record_plan(
                        "VariableLengthPath",
                        format!(
                            "piece_index={}; path={}; from={}; to={}; min_hops={}; max_hops={}; direction={:?}; path_alias={}; edge_alias={}; relationship_simple=true; caps=max_frontier:{} max_paths_per_start:{} max_intermediate_bindings:{}; source_verification=latest_visible_edges_properties_temporal_prune_tombstone_shadow_and_endpoint_visibility",
                            path.piece_index,
                            graph_row_vlp_context(path),
                            path.from_alias,
                            path.to_alias,
                            path.min_hops,
                            path.max_hops,
                            path.direction,
                            path.path_alias.is_some(),
                            path.edge_alias.is_some(),
                            query.options.max_frontier,
                            query.options.max_paths_per_start,
                            query.options.max_intermediate_bindings
                        ),
                    );
                    trace.mark_bound_node_alias(
                        &path.from_alias,
                        graph_row_runtime_node_explicit_ids(runtime, &path.from_alias),
                    );
                    trace.mark_bound_node_alias(
                        &path.to_alias,
                        graph_row_runtime_node_explicit_ids(runtime, &path.to_alias),
                    );
                    if let Some(alias) = path.edge_alias.as_deref() {
                        trace.mark_bound_alias(alias);
                    }
                    if let Some(alias) = path.path_alias.as_deref() {
                        trace.mark_bound_alias(alias);
                    }
                }
            }
        }
        Ok(())
    }

    fn populate_graph_row_edge_explain(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        planned_source_choice: Option<GraphRowEdgeCandidateSourceChoice>,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<(), EngineError> {
        let edge_name = edge.explain_name();
        let from_bound = trace.is_bound(&edge.from_alias);
        let to_bound = trace.is_bound(&edge.to_alias);
        let has_bound_endpoint = from_bound || to_bound;
        let has_unbound_endpoint_pair = !from_bound && !to_bound;

        if !edge.candidate_edge_ids.is_empty() {
            trace.record_plan(
                "EdgeCandidateSource",
                format!(
                    "edge={edge_name}; source=ExplicitEdgeIds; ids={}; labels={}; current executor verifies latest visible edge metadata/properties/endpoints after candidate lookup",
                    edge.candidate_edge_ids.len(),
                    graph_row_label_filter_detail(edge.label_filter_ids.as_deref())
                ),
            );
        } else if has_bound_endpoint {
            let source_choice = self
                .graph_row_bound_endpoint_source_choice_for_explain(
                query, edge, trace, from_bound, to_bound,
            )?
                .or(planned_source_choice);
            match source_choice {
                Some(GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource) => {
                    match edge.label_filter_ids.as_deref() {
                        Some([]) => {
                            trace.record_plan(
                                "EdgeCandidateSource",
                                format!(
                                    "edge={edge_name}; source=EmptyResult; reason=unknown edge label"
                                ),
                            );
                        }
                        Some(label_ids) => {
                            for &label_id in label_ids {
                                self.record_unbound_edge_candidate_plan(
                                    query,
                                    edge,
                                    Some(label_id),
                                    "bound endpoint selective edge candidate source",
                                    trace,
                                )?;
                            }
                        }
                        None => self.record_unbound_edge_candidate_plan(
                            query,
                            edge,
                            None,
                            "bound endpoint selective edge candidate source",
                            trace,
                        )?,
                    }
                }
                Some(GraphRowEdgeCandidateSourceChoice::ExplicitIds) => {
                    trace.record_plan(
                        "EdgeCandidateSource",
                        format!(
                            "edge={edge_name}; source=ExplicitEdgeIds; ids={}; labels={}; current executor verifies latest visible edge metadata/properties/endpoints after candidate lookup",
                            edge.candidate_edge_ids.len(),
                            graph_row_label_filter_detail(edge.label_filter_ids.as_deref())
                        ),
                    );
                }
                Some(GraphRowEdgeCandidateSourceChoice::EmptyResult) => {
                    trace.record_plan(
                        "EdgeCandidateSource",
                        format!("edge={edge_name}; source=EmptyResult; reason=planned empty edge source"),
                    );
                }
                _ => {
                    for direction in
                        graph_row_adjacency_directions_for_bound_edge(edge, from_bound, to_bound)
                    {
                        trace.record_plan(
                            "AdjacencyExpansion",
                            format!(
                                "edge={edge_name}; source=EndpointAdjacency; direction={direction:?}; from_alias={} bound={from_bound}; to_alias={} bound={to_bound}; labels={}; uses SourceList adjacency over the current ReadView",
                                edge.from_alias,
                                edge.to_alias,
                                graph_row_label_filter_detail(edge.label_filter_ids.as_deref())
                            ),
                        );
                    }
                }
            }
        }

        if has_unbound_endpoint_pair {
            let label_ids = match edge.label_filter_ids.as_deref() {
                Some([]) => {
                    trace.record_plan(
                        "EdgeCandidateSource",
                        format!("edge={edge_name}; source=EmptyResult; reason=unknown edge label"),
                    );
                    return Ok(());
                }
                Some(label_ids) => Some(label_ids),
                None => None,
            };
            if edge.candidate_edge_ids.is_empty()
                && label_ids.is_none()
                && edge.filter.is_always_true()
                && !query.options.allow_full_scan
            {
                trace.record_warning(
                    "UnanchoredRequiredEdgeWouldNeedFullScanOptIn".to_string(),
                );
                trace.record_plan(
                    "EdgeCandidateSource",
                    format!(
                        "edge={edge_name}; source=RejectedUnanchoredEdge; current execution rejects this shape without allow_full_scan=true"
                    ),
                );
            } else if let Some(label_ids) = label_ids {
                for &label_id in label_ids {
                    self.record_unbound_edge_candidate_plan(
                        query,
                        edge,
                        Some(label_id),
                        "unbound required edge candidate source",
                        trace,
                    )?;
                }
            } else {
                self.record_unbound_edge_candidate_plan(
                    query,
                    edge,
                    None,
                    "unbound required edge candidate source",
                    trace,
                )?;
            }
        }

        trace.record_plan(
            "EdgeVerification",
            format!(
                "edge={edge_name}; verifies label membership, temporal validity at effective_at_epoch, endpoint visibility, tombstones/shadows, prune policy, stale index candidates/hash collisions, semantic numeric equality/range equivalence, metadata predicates, and property predicates{}",
                graph_row_edge_filter_detail(&edge.filter)
            ),
        );
        trace.record_plan(
            "EndpointNodeVerification",
            format!(
                "edge={edge_name}; verifies endpoint node aliases {} and {} after binding using latest visible node metadata and selected verifier fields; key constraints are normalized to candidate IDs without public hydration",
                edge.from_alias, edge.to_alias
            ),
        );
        Ok(())
    }

    fn graph_row_bound_endpoint_source_choice_for_explain(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        trace: &GraphRowExplainTrace,
        from_bound: bool,
        to_bound: bool,
    ) -> Result<Option<GraphRowEdgeCandidateSourceChoice>, EngineError> {
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();
        let mut both = Vec::new();
        if from_bound {
            if let Some(ids) = trace.bound_node_ids(&edge.from_alias) {
                for &node_id in ids {
                    graph_row_collect_endpoint_sources(
                        edge.direction,
                        true,
                        Some(node_id),
                        &mut outgoing,
                        &mut incoming,
                        &mut both,
                    );
                }
            }
        }
        if to_bound {
            if let Some(ids) = trace.bound_node_ids(&edge.to_alias) {
                for &node_id in ids {
                    graph_row_collect_endpoint_sources(
                        edge.direction,
                        false,
                        Some(node_id),
                        &mut outgoing,
                        &mut incoming,
                        &mut both,
                    );
                }
            }
        }
        if outgoing.is_empty() && incoming.is_empty() && both.is_empty() {
            return Ok(None);
        }
        self.graph_row_choose_bound_edge_source(query, edge, &outgoing, &incoming, &both)
            .map(Some)
    }

    fn record_unbound_edge_candidate_plan(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        label_id: Option<u32>,
        context: &str,
        trace: &mut GraphRowExplainTrace,
    ) -> Result<(), EngineError> {
        let normalized = NormalizedEdgeQuery {
            label_id,
            ids: edge.candidate_edge_ids.clone(),
            from_ids: Vec::new(),
            to_ids: Vec::new(),
            endpoint_ids: Vec::new(),
            filter: edge.filter.clone(),
            allow_full_scan: query.options.allow_full_scan,
            page: PageRequest {
                limit: Some(query.options.max_frontier.saturating_add(1)),
                after: None,
            },
            warnings: Vec::new(),
        };
        let planned = self.plan_normalized_edge_query(&normalized)?;
        trace.record_edge_plan(&edge.explain_name(), context, label_id, &planned);
        Ok(())
    }
}

fn build_graph_row_explain(
    query: &NormalizedGraphRowQuery,
    effective_at_epoch: Option<i64>,
    cursor_state: &GraphRowCursorState,
    trace: Option<GraphRowExplainTrace>,
    runtime_stats: Option<GraphRowExplainRuntimeStats>,
) -> GraphRowExplain {
    let fingerprints = graph_row_cursor_fingerprints(
        query,
        cursor_state.effective_at_epoch,
        cursor_state.original_skip,
    );
    let mut trace = trace.unwrap_or_default();
    append_graph_row_projection_plan(query, &mut trace);
    append_graph_row_standard_row_ops(query, cursor_state, &mut trace);
    append_graph_row_standard_notes(query, cursor_state, runtime_stats.as_ref(), &mut trace);
    let rows_planned = query.nodes.len() + query.pieces.len();
    let mut warnings = trace.warnings.clone();
    warnings.sort();
    warnings.dedup();
    GraphRowExplain {
        columns: query.columns.clone(),
        effective_at_epoch,
        fingerprint: format!("{:032x}", fingerprints.query),
        plan: trace.plan,
        row_ops: trace.row_ops,
        order: GraphOrderExplain {
            explicit: !query.bound_order_by.is_empty(),
            items: query.order_by.len(),
            stable_logical_row_key: true,
        },
        cursor: GraphCursorExplain {
            supplied: query.page.cursor.is_some(),
            codec_implemented: true,
            message: Some(graph_row_cursor_explain_message(cursor_state)),
        },
        projection: GraphProjectionExplain {
            columns: query.columns.clone(),
            output_mode: query.output.mode.clone(),
            include_vectors: query.output.include_vectors,
            compact_rows: query.output.compact_rows,
        },
        caps: GraphCapExplain {
            allow_full_scan: query.options.allow_full_scan,
            max_intermediate_bindings: query.options.max_intermediate_bindings,
            max_frontier: query.options.max_frontier,
            max_path_hops: query.options.max_path_hops,
            max_paths_per_start: query.options.max_paths_per_start,
            max_page_limit: query.options.max_page_limit,
            max_order_materialization: query.options.max_order_materialization,
            max_cursor_bytes: query.options.max_cursor_bytes,
            max_query_bytes: query.options.max_query_bytes,
        },
        summaries: GraphExecutionSummaries {
            validation_only: false,
            rows_planned,
            warnings: warnings.clone(),
        },
        warnings,
        notes: trace.notes,
    }
}

fn graph_row_need_group_count(needs: &crate::row_projection::EntityProjectionNeeds) -> usize {
    needs.nodes.len()
        + needs.edges.len()
        + needs.paths.len()
        + needs.hidden_edges.len()
        + needs.hidden_paths.len()
}

#[derive(Clone, Default)]
struct GraphRowExplainTrace {
    plan: Vec<GraphExplainNode>,
    row_ops: Vec<GraphRowOperationExplain>,
    notes: Vec<String>,
    warnings: Vec<String>,
    bound_aliases: BTreeSet<String>,
    bound_node_ids: BTreeMap<String, Vec<u64>>,
}

#[derive(Clone, Copy)]
struct GraphRowExplainRuntimeStats {
    rows_returned: usize,
    rows_after_filter: usize,
    rows_seen_for_page: usize,
    intermediate_bindings_peak: usize,
    frontier_peak: usize,
    paths_enumerated: usize,
    next_cursor: bool,
}

impl GraphRowExplainTrace {
    fn record_plan(&mut self, kind: impl Into<String>, detail: impl Into<String>) {
        self.plan.push(GraphExplainNode {
            kind: kind.into(),
            detail: detail.into(),
            children: Vec::new(),
        });
    }

    fn record_row_op(&mut self, kind: impl Into<String>, detail: impl Into<String>) {
        self.row_ops.push(GraphRowOperationExplain {
            kind: kind.into(),
            detail: detail.into(),
        });
    }

    fn record_note(&mut self, note: impl Into<String>) {
        let note = note.into();
        if !self.notes.contains(&note) {
            self.notes.push(note);
        }
    }

    fn record_warning(&mut self, warning: impl Into<String>) {
        let warning = warning.into();
        if !self.warnings.contains(&warning) {
            self.warnings.push(warning);
        }
    }

    fn mark_bound_alias(&mut self, alias: &str) {
        self.bound_aliases.insert(alias.to_string());
    }

    fn mark_bound_node_alias(&mut self, alias: &str, ids: Option<&[u64]>) {
        self.mark_bound_alias(alias);
        let Some(ids) = ids.filter(|ids| !ids.is_empty()) else {
            return;
        };
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        self.bound_node_ids.insert(alias.to_string(), ids);
    }

    fn is_bound(&self, alias: &str) -> bool {
        self.bound_aliases.contains(alias)
    }

    fn bound_node_ids(&self, alias: &str) -> Option<&[u64]> {
        self.bound_node_ids.get(alias).map(Vec::as_slice)
    }

    fn record_node_plan(
        &mut self,
        alias: &str,
        context: &str,
        planned: &PlannedNodeQuery,
    ) {
        let root = planned.driver.plan_node();
        self.record_plan(
            "NodeCandidateSource",
            format!(
                "alias={alias}; context={context}; source={root:?}; estimated_candidates={:?}; warnings={:?}; secondary_index_followups={}",
                planned.estimated_candidate_count(),
                planned.warnings,
                planned.followups.len()
            ),
        );
        for warning in &planned.warnings {
            self.record_warning(format!("{warning:?}"));
        }
        if !planned.followups.is_empty() {
            self.record_note(format!(
                "alias={alias}; node candidate planning recorded {} secondary-index read followup(s); execution will enqueue followups from the actual read path",
                planned.followups.len()
            ));
        }
        self.record_plan(
            "NodeVerification",
            format!(
                "alias={alias}; verifies latest visible node metadata/properties, label filters, keys normalized to IDs, tombstones/shadows, prune policy, and stale index candidates"
            ),
        );
    }

    fn record_edge_plan(
        &mut self,
        edge_name: &str,
        context: &str,
        label_id: Option<u32>,
        planned: &PlannedEdgeQuery,
    ) {
        let root = planned.driver.plan_node();
        self.record_plan(
            "EdgeCandidateSource",
            format!(
                "edge={edge_name}; context={context}; label_id={label_id:?}; source={root:?}; estimated_candidates={:?}; warnings={:?}; secondary_index_followups={}",
                planned.estimated_candidate_count(),
                planned.warnings,
                planned.followups.len()
            ),
        );
        for warning in &planned.warnings {
            self.record_warning(format!("{warning:?}"));
        }
        if !planned.followups.is_empty() {
            self.record_note(format!(
                "edge={edge_name}; edge candidate planning recorded {} secondary-index read followup(s); execution will enqueue followups from the actual read path",
                planned.followups.len()
            ));
        }
    }

    fn record_physical_plan(
        &mut self,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
    ) {
        for segment in &physical_plan.segments {
            self.record_plan(
                "GraphRowRequiredSegment",
                format!(
                    "segment={}; barriers_before={}; initial_driver={}; physical_edge_order={:?}; segment-local fanout planning never reorders required fixed pieces across optional/VLP barriers",
                    segment.segment_index,
                    graph_row_barriers_detail(&segment.barriers_before),
                    graph_row_initial_driver_detail_for_driver(&segment.initial_driver),
                    graph_row_physical_edge_order_detail(runtime, &segment.edge_order)
                ),
            );
        }
        for alternative in &physical_plan.alternatives {
            let chosen = if alternative.chosen { "chosen" } else { "rejected" };
            let decision = alternative
                .decision
                .as_deref()
                .unwrap_or("decision=not_costed");
            let cost = alternative
                .cost
                .as_ref()
                .map(graph_row_physical_cost_detail)
                .unwrap_or_else(|| "cost=unavailable".to_string());
            self.record_plan(
                "GraphRowPlanAlternative",
                format!(
                    "{chosen}; kind={}; {}; {decision}; {cost}",
                    alternative.kind, alternative.detail
                ),
            );
        }
        for note in &physical_plan.notes {
            self.record_note(note.clone());
        }
        self.record_note(
            "graph-row physical planner considers node anchors, edge anchors, endpoint adjacency, edge candidate/index sources, reverse expansion, target selectivity, fanout rollups, hub-risk, stale/missing stats, and deterministic tie-breakers".to_string(),
        );
    }

    fn record_runtime_edge_source(
        &mut self,
        edge: &GraphRowRuntimeEdge,
        choice: GraphRowEdgeCandidateSourceChoice,
        detail: impl Into<String>,
        candidate_count: usize,
    ) {
        self.record_plan(
            "GraphRowSourceRead",
            format!(
                "edge={}; choice={choice:?}; {}; candidate_ids_after_source={candidate_count}; every candidate is still latest-record verified before binding",
                edge.explain_name(),
                detail.into()
            ),
        );
    }
}

fn graph_row_physical_cost_detail(cost: &GraphRowPlanCost) -> String {
    format!(
        "cost_work={}; simulated_frontier={}; fanout_complete={}; confidence_rank={}; stale_risk_rank={}; hub_risk_rank={}; frontier_capped={}; source_rank={}; canonical_key={}",
        cost.estimated_work,
        cost.simulated_frontier,
        cost.fanout_complete,
        cost.confidence_rank,
        cost.stale_risk_rank,
        cost.hub_risk_rank,
        cost.frontier_capped,
        cost.source_rank,
        cost.canonical_key
    )
}

fn graph_row_initial_driver_detail(physical_plan: &GraphRowPhysicalPlan) -> String {
    graph_row_initial_driver_detail_for_driver(&physical_plan.initial_driver)
}

fn graph_row_initial_driver_detail_for_driver(driver: &GraphRowInitialDriver) -> String {
    match driver {
        GraphRowInitialDriver::Empty { reason } => {
            format!("EmptyBindingRow({reason})")
        }
        GraphRowInitialDriver::Node { alias, node_index } => {
            format!("NodeAnchor(alias={alias}, index={node_index})")
        }
        GraphRowInitialDriver::Edge {
            edge_name,
            edge_index,
        } => format!("EdgeAnchor(edge={edge_name}, index={edge_index})"),
    }
}

fn graph_row_physical_edge_order_detail(
    runtime: &GraphRowRuntimePlan,
    edge_order: &[usize],
) -> Vec<String> {
    edge_order
        .iter()
        .map(|edge_index| runtime.edges[*edge_index].explain_name())
        .collect()
}

fn graph_row_barriers_detail(barriers: &[GraphRowPlanBarrier]) -> String {
    if barriers.is_empty() {
        return "none".to_string();
    }
    barriers
        .iter()
        .map(|barrier| format!("{:?}@piece{}", barrier.kind, barrier.piece_index))
        .collect::<Vec<_>>()
        .join("|")
}

impl GraphRowRuntimeEdge {
    fn edge_alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    fn explain_name(&self) -> String {
        self.alias
            .as_deref()
            .map(|alias| format!("alias:{alias}"))
            .unwrap_or_else(|| "hidden-edge".to_string())
    }
}

fn graph_row_runtime_node_explicit_ids<'a>(
    runtime: &'a GraphRowRuntimePlan,
    alias: &str,
) -> Option<&'a [u64]> {
    let node = runtime
        .node_by_alias
        .get(alias)
        .and_then(|index| runtime.nodes.get(*index))?;
    (!node.query.ids.is_empty()).then_some(node.query.ids.as_slice())
}

fn graph_row_selection_capacity(
    query: &NormalizedGraphRowQuery,
    cursor_state: &GraphRowCursorState,
) -> Result<usize, EngineError> {
    let page_start = if cursor_state.is_cursor_page() {
        0
    } else {
        query.page.skip
    };
    let effective_page_limit = graph_row_effective_page_limit(query, cursor_state);
    let proof_row = graph_row_page_needs_continuation_proof(query, cursor_state);
    let selection_capacity = page_start
        .checked_add(effective_page_limit)
        .and_then(|value| value.checked_add(usize::from(proof_row)))
        .ok_or_else(|| {
            EngineError::InvalidOperation(
                "graph row page skip and limit overflow order materialization bounds".to_string(),
            )
        })?;
    if selection_capacity > query.options.max_order_materialization {
        return Err(graph_row_cap_error(
            "max_order_materialization",
            query.options.max_order_materialization,
        ));
    }
    Ok(selection_capacity)
}

fn graph_row_remaining_logical_limit(
    query: &NormalizedGraphRowQuery,
    cursor_state: &GraphRowCursorState,
) -> Option<usize> {
    query.logical_limit.map(|limit| {
        let emitted = usize::try_from(cursor_state.rows_emitted_after_skip)
            .unwrap_or(usize::MAX);
        limit.saturating_sub(emitted)
    })
}

fn graph_row_effective_page_limit(
    query: &NormalizedGraphRowQuery,
    cursor_state: &GraphRowCursorState,
) -> usize {
    graph_row_remaining_logical_limit(query, cursor_state)
        .map_or(query.page.limit, |remaining| query.page.limit.min(remaining))
}

fn graph_row_page_needs_continuation_proof(
    query: &NormalizedGraphRowQuery,
    cursor_state: &GraphRowCursorState,
) -> bool {
    graph_row_remaining_logical_limit(query, cursor_state)
        .is_none_or(|remaining| remaining > query.page.limit)
}

fn graph_row_rows_emitted_after_page(
    cursor_state: &GraphRowCursorState,
    rows_returned: usize,
) -> Result<u64, EngineError> {
    cursor_state
        .rows_emitted_after_skip
        .checked_add(rows_returned as u64)
        .ok_or_else(|| {
            EngineError::InvalidOperation(
                "graph row cursor emitted row count overflowed".to_string(),
            )
        })
}

fn graph_row_logical_limit_exhausted(
    query: &NormalizedGraphRowQuery,
    rows_emitted_after_page: u64,
) -> bool {
    query
        .logical_limit
        .is_some_and(|limit| rows_emitted_after_page >= limit as u64)
}

fn graph_row_node_only_default_order_fast_path(
    query: &NormalizedGraphRowQuery,
    runtime: &GraphRowRuntimePlan,
) -> bool {
    query.pieces.is_empty()
        && runtime.nodes.len() == 1
        && query.bound_order_by.is_empty()
        && query.bound_where.is_none()
        && query.edge_id_constraints.is_empty()
}

fn graph_row_optional_group_count(pieces: &[GraphPatternPiece]) -> usize {
    pieces
        .iter()
        .map(|piece| match piece {
            GraphPatternPiece::Optional(group) => {
                1 + graph_row_optional_group_count(&group.pieces)
            }
            GraphPatternPiece::Edge(_) | GraphPatternPiece::VariableLength(_) => 0,
        })
        .sum()
}

fn graph_row_variable_length_count(pieces: &[GraphPatternPiece]) -> usize {
    pieces
        .iter()
        .map(|piece| match piece {
            GraphPatternPiece::VariableLength(_) => 1,
            GraphPatternPiece::Optional(group) => graph_row_variable_length_count(&group.pieces),
            GraphPatternPiece::Edge(_) => 0,
        })
        .sum()
}

fn graph_row_adjacency_directions_for_bound_edge(
    edge: &GraphRowRuntimeEdge,
    from_bound: bool,
    to_bound: bool,
) -> Vec<Direction> {
    let mut directions = Vec::new();
    if from_bound {
        match edge.direction {
            Direction::Outgoing => directions.push(Direction::Outgoing),
            Direction::Incoming => directions.push(Direction::Incoming),
            Direction::Both => directions.push(Direction::Both),
        }
    }
    if to_bound {
        match edge.direction {
            Direction::Outgoing => directions.push(Direction::Incoming),
            Direction::Incoming => directions.push(Direction::Outgoing),
            Direction::Both => directions.push(Direction::Both),
        }
    }
    directions.sort_by_key(|direction| match direction {
        Direction::Outgoing => 0,
        Direction::Incoming => 1,
        Direction::Both => 2,
    });
    directions.dedup();
    directions
}

fn graph_row_label_filter_detail(label_ids: Option<&[u32]>) -> String {
    match label_ids {
        None => "unconstrained".to_string(),
        Some([]) => "empty/unknown-label".to_string(),
        Some(ids) => format!("resolved_token_ids={ids:?}"),
    }
}

fn graph_row_edge_filter_detail(filter: &NormalizedEdgeFilter) -> String {
    let mut details = Vec::new();
    collect_graph_row_edge_filter_detail(filter, &mut details);
    if details.is_empty() {
        String::new()
    } else {
        format!("; filter_verification={}", details.join(","))
    }
}

fn collect_graph_row_edge_filter_detail(filter: &NormalizedEdgeFilter, details: &mut Vec<&'static str>) {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => {}
        NormalizedEdgeFilter::AlwaysFalse => details.push("always_false"),
        NormalizedEdgeFilter::WeightRange { .. }
        | NormalizedEdgeFilter::UpdatedAtRange { .. }
        | NormalizedEdgeFilter::ValidAt { .. }
        | NormalizedEdgeFilter::ValidFromRange { .. }
        | NormalizedEdgeFilter::ValidToRange { .. } => details.push("metadata_only"),
        NormalizedEdgeFilter::PropertyEquals { .. }
        | NormalizedEdgeFilter::PropertyIn { .. }
        | NormalizedEdgeFilter::PropertyRange { .. }
        | NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. } => details.push("edge_property_projection"),
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            for child in children {
                collect_graph_row_edge_filter_detail(child, details);
            }
        }
        NormalizedEdgeFilter::Not(child) => {
            details.push("negated_filter");
            collect_graph_row_edge_filter_detail(child, details);
        }
    }
    details.sort_unstable();
    details.dedup();
}

fn graph_row_cursor_explain_message(cursor_state: &GraphRowCursorState) -> String {
    match cursor_state.decoded.as_ref() {
        Some(cursor) => format!(
            "final-row cursor supplied; page_sequence={}, original_skip={}, rows_emitted_after_skip={}, effective_at_epoch={} came from cursor and fingerprints were validated",
            cursor.page_sequence,
            cursor.original_skip,
            cursor.rows_emitted_after_skip,
            cursor.effective_at_epoch
        ),
        None => "no cursor supplied; first page uses final logical row ordering and emitted cursors store order atoms plus logical row key".to_string(),
    }
}

fn append_graph_row_projection_plan(
    query: &NormalizedGraphRowQuery,
    trace: &mut GraphRowExplainTrace,
) {
    let groups = [
        ("verifier", &query.projection_needs.verifier),
        ("residual", &query.projection_needs.residual),
        ("order", &query.projection_needs.order),
        ("output", &query.projection_needs.output),
    ];
    for (need_class, needs) in groups {
        trace.record_plan(
            "ProjectionNeeds",
            format!(
                "need_class={need_class}; {}",
                graph_row_projection_needs_detail(needs)
            ),
        );
    }
    trace.record_plan(
        "FinalHydrationProjection",
        format!(
            "final page hydration/projection only; output_mode={:?}; include_vectors={}; compact_rows={}; columns={:?}",
            query.output.mode,
            query.output.include_vectors,
            query.output.compact_rows,
            query.columns
        ),
    );
}

fn graph_row_projection_needs_detail(
    needs: &crate::row_projection::EntityProjectionNeeds,
) -> String {
    format!(
        "node_aliases={:?}; edge_aliases={:?}; path_aliases={:?}; hidden_edges={:?}; hidden_paths={:?}; groups={}",
        needs.nodes.keys().collect::<Vec<_>>(),
        needs.edges.keys().collect::<Vec<_>>(),
        needs.paths.keys().collect::<Vec<_>>(),
        needs.hidden_edges.keys().collect::<Vec<_>>(),
        needs.hidden_paths.keys().collect::<Vec<_>>(),
        graph_row_need_group_count(needs)
    )
}

fn append_graph_row_standard_row_ops(
    query: &NormalizedGraphRowQuery,
    cursor_state: &GraphRowCursorState,
    trace: &mut GraphRowExplainTrace,
) {
    let optional_groups = graph_row_optional_group_count(&query.pieces);
    if optional_groups > 0 {
        trace.record_row_op(
            "OptionalApply",
            format!(
                "groups={optional_groups}; left-outer apply preserves each incoming row on misses, null-extends introduced aliases, and never permits required-piece reordering across optional barriers"
            ),
        );
    }
    let variable_length_paths = graph_row_variable_length_count(&query.pieces);
    if variable_length_paths > 0 {
        trace.record_row_op(
            "VariableLengthPath",
            format!(
                "pieces={variable_length_paths}; bounded relationship-simple path expansion stores ID vectors only and hydrates path elements after final page selection"
            ),
        );
    }
    if !query.fixed_paths.is_empty() {
        trace.record_row_op(
            "FixedPathCompose",
            format!(
                "paths={}; composes path ID vectors from already-bound fixed node/edge slots without new index scans",
                query.fixed_paths.len()
            ),
        );
    }
    if query.bound_where.is_some() {
        trace.record_row_op(
            "ResidualFilter",
            "evaluates normalized graph-row WHERE after required and optional expansion and before final ordering/page selection",
        );
    } else {
        trace.record_row_op("ResidualFilter", "none");
    }
    trace.record_row_op(
        "Order",
        format!(
            "explicit_order={}; order_items={}; stable logical row key is always the deterministic tie-breaker",
            !query.bound_order_by.is_empty(),
            query.bound_order_by.len()
        ),
    );
    trace.record_row_op(
        "CursorSeek",
        format!(
            "cursor_supplied={}; seek compares final (order_atoms, logical_row_key), not physical frontier state",
            cursor_state.is_cursor_page()
        ),
    );
    trace.record_row_op(
        "SkipLimit",
        format!(
            "skip={}, logical_limit={:?}, rows_emitted_before_page={}, effective_page_limit={}, requested_page_limit={}, max_page_limit={}",
            if cursor_state.is_cursor_page() { 0 } else { query.page.skip },
            query.logical_limit,
            cursor_state.rows_emitted_after_skip,
            graph_row_effective_page_limit(query, cursor_state),
            query.page.limit,
            query.options.max_page_limit
        ),
    );
    trace.record_row_op(
        "FinalProjection",
        format!(
            "hydrates/projects only final page rows; output need groups={}",
            graph_row_need_group_count(&query.projection_needs.output)
        ),
    );
}

fn append_graph_row_standard_notes(
    query: &NormalizedGraphRowQuery,
    cursor_state: &GraphRowCursorState,
    runtime_stats: Option<&GraphRowExplainRuntimeStats>,
    trace: &mut GraphRowExplainTrace,
) {
    trace.record_note("GraphRowExplain is the root explain shape for graph rows; embedded node/edge source summaries are advisory and not old graph-pattern explain roots".to_string());
    trace.record_note(format!(
        "normalized return items={}, binding slots={}, projection need groups: verifier={} residual={} order={} output={}",
        query.return_items.len(),
        query.binding_schema.slots().len(),
        graph_row_need_group_count(&query.projection_needs.verifier),
        graph_row_need_group_count(&query.projection_needs.residual),
        graph_row_need_group_count(&query.projection_needs.order),
        graph_row_need_group_count(&query.projection_needs.output)
    ));
    trace.record_note(match cursor_state.decoded.as_ref() {
        Some(_) => "effective_at_epoch source: cursor payload".to_string(),
        None if query.at_epoch.is_some() => "effective_at_epoch source: explicit request at_epoch".to_string(),
        None => "effective_at_epoch source: resolved once at operation start for page 1".to_string(),
    });
    trace.record_note(
        "source correctness: one ReadView is used for graph-row planning/execution/explain; active memtable wins, immutable memtables and segments are read newest-to-oldest, newer shadows older records, tombstones hide older records, prune policies apply at read time, temporal edge validity uses effective_at_epoch, and stale index candidates are finally verified".to_string(),
    );
    trace.record_note(
        "planner statistics and candidate indexes are advisory only; latest visible SourceList verification remains the correctness boundary".to_string(),
    );
    trace.record_note(
        "fanout-aware physical source choice is advisory only; final logical result order, explicit ORDER BY, cursor seek, and page boundaries are applied after fixed-row verification".to_string(),
    );
    trace.record_note(format!(
        "caps: max_frontier={}, max_intermediate_bindings={}, max_order_materialization={}, max_page_limit={}, max_cursor_bytes={}, effective_page_limit={}",
        query.options.max_frontier,
        query.options.max_intermediate_bindings,
        query.options.max_order_materialization,
        query.options.max_page_limit,
        query.options.max_cursor_bytes,
        graph_row_effective_page_limit(query, cursor_state)
    ));
    match runtime_stats {
        Some(stats) => trace.record_note(format!(
            "cap pressure: frontier_peak={}, intermediate_bindings_peak={}, paths_enumerated={}, rows_after_filter={}, rows_seen_for_page={}, rows_returned={}, next_cursor={}",
            stats.frontier_peak,
            stats.intermediate_bindings_peak,
            stats.paths_enumerated,
            stats.rows_after_filter,
            stats.rows_seen_for_page,
            stats.rows_returned,
            stats.next_cursor
        )),
        None => trace.record_note(
            "cap pressure: standalone explain reports configured caps and planned operations without materializing rows".to_string(),
        ),
    }
}

#[derive(Clone, Debug)]
struct GqlResolvedOrderItem {
    expr: Expr,
    direction: OrderDirection,
    span: SourceSpan,
}

#[derive(Clone, Copy)]
struct GqlRowCounts {
    skip: usize,
    limit: Option<usize>,
}

fn configure_gql_graph_row_target(
    lowered: &mut GqlLoweredPlan,
    order_by: &[GqlResolvedOrderItem],
    row_counts: &GqlRowCounts,
    options: &GqlExecutionOptions,
) -> Result<(), EngineError> {
    let GqlNativeTarget::GraphRows { query } = &mut lowered.native_target;
    query.query.order_by = order_by
        .iter()
        .map(|item| {
            Ok(GraphOrderItem {
                expr: gql_expr_to_graph_expr(
                    &item.expr,
                    &lowered
                        .semantic
                        .aliases
                        .by_name
                        .iter()
                        .map(|(alias, binding)| (alias.clone(), binding.kind))
                        .collect(),
                )?,
                direction: gql_order_direction_to_graph(item.direction),
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    query.query.page.skip = row_counts.skip;
    let effective_row_cap = options.max_rows.min(options.max_intermediate_bindings).max(1);
    query.logical_limit = row_counts.limit;
    query.query.page.limit = row_counts
        .limit
        .unwrap_or(effective_row_cap)
        .min(effective_row_cap)
        .max(1);
    query.query.options.max_page_limit = query
        .query
        .options
        .max_page_limit
        .max(query.query.page.limit);
    query.query.options.max_order_materialization = query
        .query
        .options
        .max_order_materialization
        .max(gql_graph_row_order_materialization_floor(
            query.query.page.skip,
            query.query.page.limit,
            row_counts.limit,
        ));
    Ok(())
}

fn gql_graph_row_order_materialization_floor(
    skip: usize,
    page_limit: usize,
    logical_limit: Option<usize>,
) -> usize {
    let proof_row = logical_limit.is_none_or(|limit| limit > page_limit);
    skip.saturating_add(page_limit)
        .saturating_add(usize::from(proof_row))
}

fn execute_gql_graph_row_target(
    view: &ReadView,
    lowered: &GqlLoweredPlan,
) -> Result<QueryExecutionOutcome<GraphRowResult>, EngineError> {
    let normalized = normalize_gql_graph_row_target(lowered)?;
    let cursor_state = graph_row_prepare_cursor_state(
        &normalized.page,
        normalized.at_epoch,
        &normalized.options,
    )?;
    view.query_graph_rows_outcome(&normalized, cursor_state)
        .map_err(|err| graph_row_execution_error_to_gql(err, lowered))
}

fn normalize_gql_graph_row_target(
    lowered: &GqlLoweredPlan,
) -> Result<NormalizedGraphRowQuery, EngineError> {
    let GqlNativeTarget::GraphRows { query } = &lowered.native_target;
    let fallback_span = lowered
        .semantic
        .query
        .match_clauses
        .first()
        .map(|clause| clause.span.clone())
        .unwrap_or_else(|| lowered.semantic.query.return_clause.span.clone());
    let normalized = normalize_graph_row_query_with_gql_fixed_paths(
        &query.query,
        &query.edge_id_constraints,
        query.logical_limit,
        &query.fixed_paths,
    )
    .map_err(|err| graph_row_normalization_error_to_gql(err, &fallback_span))?;
    Ok(normalized)
}

fn graph_row_normalization_error_to_gql(err: EngineError, span: &SourceSpan) -> EngineError {
    match err {
        EngineError::InvalidOperation(message) if graph_row_full_scan_error_message(&message) => {
            gql_semantic_error(GqlSemanticErrorCode::FullScanNotAllowed, message, span.clone())
        }
        other => other,
    }
}

fn graph_row_execution_error_to_gql(err: EngineError, lowered: &GqlLoweredPlan) -> EngineError {
    match err {
        EngineError::InvalidOperation(message) if graph_row_full_scan_error_message(&message) => {
            let span = lowered
                .semantic
                .clauses
                .first()
                .map(|clause| clause.span.clone())
                .unwrap_or_else(|| lowered.semantic.query.return_clause.span.clone());
            gql_semantic_error(GqlSemanticErrorCode::FullScanNotAllowed, message, span)
        }
        EngineError::InvalidOperation(message)
            if message.contains("ORDER BY")
                || message.contains("order contexts")
                || message.contains("orderable") =>
        {
            let span = lowered
                .order_by
                .first()
                .map(|item| item.span.clone())
                .unwrap_or_else(|| lowered.semantic.query.return_clause.span.clone());
            gql_order_key_error(&span)
        }
        other => other,
    }
}

fn graph_row_full_scan_error_message(message: &str) -> bool {
    message.contains("allow_full_scan=true") || message.contains("or allow_full_scan")
}

fn gql_alias_projection_map(plan: &GqlLoweredPlan) -> BTreeMap<String, String> {
    plan
        .semantic
        .aliases
        .by_name
        .keys()
        .map(|alias| (alias.clone(), alias.clone()))
        .collect::<BTreeMap<_, _>>()
}

fn resolve_order_by_return_aliases(
    lowered: &GqlLoweredPlan,
) -> Result<Vec<GqlResolvedOrderItem>, EngineError> {
    let return_aliases = gql_return_alias_exprs(&lowered.semantic);
    lowered
        .order_by
        .iter()
        .map(|item| {
            let expr =
                resolve_return_aliases_in_expr(&item.expr, &return_aliases, &lowered.semantic)?;
            validate_gql_order_expr_static(&expr, &lowered.semantic, &item.span)?;
            Ok(GqlResolvedOrderItem {
                expr,
                direction: item.direction,
                span: item.span.clone(),
            })
        })
        .collect()
}

fn validate_gql_order_expr_static(
    expr: &Expr,
    plan: &GqlSemanticPlan,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    match &expr.kind {
        ExprKind::FunctionCall { name, .. } if name.name.eq_ignore_ascii_case("labels") => {
            Err(gql_order_key_error(span))
        }
        ExprKind::FunctionCall { name, .. }
            if matches!(
                name.name.to_ascii_lowercase().as_str(),
                "nodes" | "relationships" | "node_ids" | "edge_ids"
            ) =>
        {
            Err(gql_order_key_error(span))
        }
        ExprKind::PropertyAccess { object, property }
            if property.name == "labels"
                && matches!(
                    &object.kind,
                    ExprKind::Variable(name)
                        if plan
                            .aliases
                            .get(name)
                            .is_some_and(|binding| binding.kind == GqlAliasKind::Node)
                ) =>
        {
            Err(gql_order_key_error(span))
        }
        ExprKind::PropertyAccess { object, property }
            if matches!(property.name.as_str(), "node_ids" | "edge_ids")
                && matches!(
                    &object.kind,
                    ExprKind::Variable(name)
                        if plan
                            .aliases
                            .get(name)
                            .is_some_and(|binding| binding.kind == GqlAliasKind::Path)
                ) =>
        {
            Err(gql_order_key_error(span))
        }
        ExprKind::List(_) | ExprKind::Map(_) => Err(gql_order_key_error(span)),
        _ => Ok(()),
    }
}

fn validate_gql_row_independent_order_keys(
    order_by: &[GqlResolvedOrderItem],
    lowered: &GqlLoweredPlan,
    params: &GqlParams,
) -> Result<(), EngineError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let projection = build_runtime_projection(
        &[],
        &lowered.semantic,
        &BTreeMap::new(),
        false,
        false,
    )?;
    let row = ProjectedRow { values: Vec::new() };
    let context = GqlEvalContext::new(&projection, &row, &lowered.semantic, params);
    for item in order_by {
        if gql_expr_depends_on_alias(&item.expr, &lowered.semantic) {
            continue;
        }
        let value = eval_expr_against_context(&item.expr, &context)
            .map_err(|_| gql_order_key_error(&item.span))?;
        validate_gql_row_independent_order_value(value, &item.span)?;
    }
    Ok(())
}

fn gql_expr_depends_on_alias(expr: &Expr, plan: &GqlSemanticPlan) -> bool {
    match &expr.kind {
        ExprKind::Variable(name) => plan.aliases.contains(name),
        ExprKind::PropertyAccess { object, .. } => gql_expr_depends_on_alias(object, plan),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            gql_expr_depends_on_alias(expr, plan)
        }
        ExprKind::Binary { left, right, .. } => {
            gql_expr_depends_on_alias(left, plan) || gql_expr_depends_on_alias(right, plan)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            args.iter().any(|arg| gql_expr_depends_on_alias(arg, plan))
        }
        ExprKind::Map(map) => map
            .entries
            .iter()
            .any(|entry| gql_expr_depends_on_alias(&entry.value, plan)),
        ExprKind::Literal(_) | ExprKind::Parameter(_) => false,
    }
}

#[derive(Clone)]
enum GqlReturnAliasResolution {
    Unique(Expr),
    Ambiguous,
}

fn gql_return_alias_exprs(plan: &GqlSemanticPlan) -> BTreeMap<String, GqlReturnAliasResolution> {
    let mut aliases = BTreeMap::new();
    if let GqlReturnPlan::Items(items) = &plan.returns {
        for item in items {
            if let Some(alias) = item.explicit_alias.as_ref() {
                aliases
                    .entry(alias.clone())
                    .and_modify(|resolution| {
                        *resolution = GqlReturnAliasResolution::Ambiguous;
                    })
                    .or_insert_with(|| GqlReturnAliasResolution::Unique(item.expr.clone()));
            }
        }
    }
    aliases
}

fn resolve_return_aliases_in_expr(
    expr: &Expr,
    return_aliases: &BTreeMap<String, GqlReturnAliasResolution>,
    plan: &GqlSemanticPlan,
) -> Result<Expr, EngineError> {
    let kind = match &expr.kind {
        ExprKind::Variable(name)
            if !plan.aliases.contains(name) && return_aliases.contains_key(name) =>
        {
            return match return_aliases.get(name).expect("checked above") {
                GqlReturnAliasResolution::Unique(expr) => Ok(expr.clone()),
                GqlReturnAliasResolution::Ambiguous => {
                    Err(gql_ambiguous_return_alias_error(name, &expr.span))
                }
            };
        }
        ExprKind::PropertyAccess { object, property } => ExprKind::PropertyAccess {
            object: Box::new(resolve_return_aliases_in_expr(
                object,
                return_aliases,
                plan,
            )?),
            property: property.clone(),
        },
        ExprKind::Unary { op, expr } => ExprKind::Unary {
            op: *op,
            expr: Box::new(resolve_return_aliases_in_expr(
                expr,
                return_aliases,
                plan,
            )?),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: Box::new(resolve_return_aliases_in_expr(
                left,
                return_aliases,
                plan,
            )?),
            right: Box::new(resolve_return_aliases_in_expr(
                right,
                return_aliases,
                plan,
            )?),
        },
        ExprKind::IsNull { expr, negated } => ExprKind::IsNull {
            expr: Box::new(resolve_return_aliases_in_expr(
                expr,
                return_aliases,
                plan,
            )?),
            negated: *negated,
        },
        ExprKind::FunctionCall { name, args } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_return_aliases_in_expr(arg, return_aliases, plan))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ExprKind::List(items) => ExprKind::List(
            items
                .iter()
                .map(|item| resolve_return_aliases_in_expr(item, return_aliases, plan))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ExprKind::Map(map) => {
            let mut resolved = map.clone();
            for entry in &mut resolved.entries {
                entry.value = resolve_return_aliases_in_expr(
                    &entry.value,
                    return_aliases,
                    plan,
                )?;
            }
            ExprKind::Map(resolved)
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => {
            return Ok(expr.clone())
        }
    };
    Ok(Expr {
        kind,
        span: expr.span.clone(),
    })
}

fn gql_ambiguous_return_alias_error(alias: &str, span: &SourceSpan) -> EngineError {
    EngineError::GqlSemantic {
        code: GqlSemanticErrorCode::InvalidReturnExpression,
        message: format!(
            "return alias '{alias}' is ambiguous because multiple RETURN items use it"
        ),
        span: span.clone(),
    }
}

fn evaluate_gql_row_counts(
    lowered: &GqlLoweredPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<GqlRowCounts, EngineError> {
    let alias_projection = gql_alias_projection_map(lowered);
    let return_aliases = gql_return_alias_exprs(&lowered.semantic);
    let skip = match lowered.skip.as_ref() {
        Some(expr) => {
            let resolved =
                resolve_return_aliases_in_expr(expr, &return_aliases, &lowered.semantic)?;
            let skip =
                evaluate_gql_count_expr(&resolved, lowered, &alias_projection, params, "SKIP")?;
            if skip > options.max_skip {
                return Err(gql_row_count_error(
                    expr,
                    format!("SKIP/OFFSET value {skip} exceeds max_skip={}", options.max_skip),
                ));
            }
            skip
        }
        None => 0,
    };
    let limit = lowered
        .limit
        .as_ref()
        .map(|expr| {
            let resolved =
                resolve_return_aliases_in_expr(expr, &return_aliases, &lowered.semantic)?;
            evaluate_gql_count_expr(&resolved, lowered, &alias_projection, params, "LIMIT")
        })
        .transpose()?;
    Ok(GqlRowCounts { skip, limit })
}

fn evaluate_gql_count_expr(
    expr: &Expr,
    lowered: &GqlLoweredPlan,
    alias_projection: &BTreeMap<String, String>,
    params: &GqlParams,
    clause: &str,
) -> Result<usize, EngineError> {
    let projection = build_runtime_projection(
        std::slice::from_ref(expr),
        &lowered.semantic,
        alias_projection,
        false,
        false,
    )?;
    if !projection.keys.is_empty() {
        return Err(gql_row_count_error(
            expr,
            format!("{clause} must be a row-independent non-negative integer"),
        ));
    }
    let empty_row = ProjectedRow { values: Vec::new() };
    let context = GqlEvalContext::new(&projection, &empty_row, &lowered.semantic, params);
    let value = eval_expr_against_context(expr, &context)?;
    match value {
        ProjectedValue::Int(value) if value >= 0 => usize::try_from(value).map_err(|_| {
            gql_row_count_error(expr, format!("{clause} value is too large for this platform"))
        }),
        ProjectedValue::UInt(value) => usize::try_from(value).map_err(|_| {
            gql_row_count_error(expr, format!("{clause} value is too large for this platform"))
        }),
        _ => Err(gql_row_count_error(
            expr,
            format!("{clause} must evaluate to a non-negative integer"),
        )),
    }
}

fn gql_row_count_error(expr: &Expr, message: String) -> EngineError {
    EngineError::GqlSemantic {
        code: GqlSemanticErrorCode::InvalidReturnExpression,
        message,
        span: expr.span.clone(),
    }
}

fn validate_gql_row_independent_order_value(
    value: ProjectedValue,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    match value {
        ProjectedValue::Null
        | ProjectedValue::Bool(_)
        | ProjectedValue::Int(_)
        | ProjectedValue::UInt(_)
        | ProjectedValue::String(_)
        | ProjectedValue::Bytes(_) => Ok(()),
        ProjectedValue::Float(value) if value.is_finite() => Ok(()),
        _ => Err(gql_order_key_error(span)),
    }
}

fn gql_order_key_error(span: &SourceSpan) -> EngineError {
    EngineError::GqlSemantic {
        code: GqlSemanticErrorCode::InvalidReturnExpression,
        message: "ORDER BY keys must be null or supported graph-row order atoms; lists, maps, and non-finite floats are not orderable".to_string(),
        span: span.clone(),
    }
}

fn build_gql_explain(
    view: &ReadView,
    lowered: &GqlLoweredPlan,
    returns: &[GqlReturnExpr],
    order_by: &[GqlResolvedOrderItem],
    options: &GqlExecutionOptions,
) -> Result<GqlExplain, EngineError> {
    let mut warnings = lowered.warnings.clone();
    let normalized = normalize_gql_graph_row_target(lowered)?;
    let cursor_state = graph_row_prepare_cursor_state(
        &normalized.page,
        normalized.at_epoch,
        &normalized.options,
    )?;
    let graph_row_explain = view.explain_graph_rows_normalized(&normalized, cursor_state)?;
    warnings.extend(graph_row_explain.warnings.iter().cloned());
    warnings.sort();
    warnings.dedup();
    let mut projection =
        gql_projection_summaries(&normalized, returns, order_by, options.include_vectors);
    projection.extend(graph_row_explain.plan.iter().map(|node| {
        format!("graph row plan: {}: {}", node.kind, node.detail)
    }));
    projection.extend(graph_row_explain.row_ops.iter().map(|op| {
        format!("graph row row op: {}: {}", op.kind, op.detail)
    }));
    projection.push(format!(
        "graph row order: explicit={}, items={}, stable_logical_row_key={}",
        graph_row_explain.order.explicit,
        graph_row_explain.order.items,
        graph_row_explain.order.stable_logical_row_key
    ));
    projection.push(format!(
        "graph row cursor: supplied={}, codec_implemented={}, message={}",
        graph_row_explain.cursor.supplied,
        graph_row_explain.cursor.codec_implemented,
        graph_row_explain
            .cursor
            .message
            .as_deref()
            .unwrap_or("none")
    ));
    projection.push(format!(
        "graph row caps: allow_full_scan={}, max_frontier={}, max_intermediate_bindings={}, max_order_materialization={}, max_page_limit={}, max_cursor_bytes={}, max_query_bytes={}",
        graph_row_explain.caps.allow_full_scan,
        graph_row_explain.caps.max_frontier,
        graph_row_explain.caps.max_intermediate_bindings,
        graph_row_explain.caps.max_order_materialization,
        graph_row_explain.caps.max_page_limit,
        graph_row_explain.caps.max_cursor_bytes,
        graph_row_explain.caps.max_query_bytes
    ));
    projection.extend(
        graph_row_explain
            .notes
            .iter()
            .map(|note| format!("graph row note: {note}")),
    );
    Ok(GqlExplain {
        columns: returns
            .iter()
            .map(|return_expr| return_expr.output_name.clone())
            .collect(),
        target: gql_explain_target(lowered.native_target.kind()),
        native_plan: None,
        pushed_down: lowered
            .pushed_down
            .iter()
            .map(|predicate| predicate.summary.clone())
            .collect(),
        residual: lowered
            .residual_predicates
            .iter()
            .map(|expr| format!("residual filter: {}", gql_expr_summary(expr)))
            .collect(),
        projection,
        row_ops: gql_row_ops(lowered),
        caps: GqlCapSummary {
            allow_full_scan: options.allow_full_scan,
            max_rows: options.max_rows,
            max_intermediate_bindings: options.max_intermediate_bindings,
            max_skip: options.max_skip,
            max_query_bytes: options.max_query_bytes,
            max_param_bytes: options.max_param_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        },
        warnings,
    })
}

fn build_gql_limit_zero_explain(
    lowered: &GqlLoweredPlan,
    returns: &[GqlReturnExpr],
    order_by: &[GqlResolvedOrderItem],
    options: &GqlExecutionOptions,
) -> GqlExplain {
    GqlExplain {
        columns: returns
            .iter()
            .map(|return_expr| return_expr.output_name.clone())
            .collect(),
        target: gql_explain_target(lowered.native_target.kind()),
        native_plan: None,
        pushed_down: lowered
            .pushed_down
            .iter()
            .map(|predicate| predicate.summary.clone())
            .collect(),
        residual: lowered
            .residual_predicates
            .iter()
            .map(|expr| format!("residual filter: {}", gql_expr_summary(expr)))
            .collect(),
        projection: gql_limit_zero_projection_summaries(returns, order_by),
        row_ops: gql_row_ops(lowered),
        caps: GqlCapSummary {
            allow_full_scan: options.allow_full_scan,
            max_rows: options.max_rows,
            max_intermediate_bindings: options.max_intermediate_bindings,
            max_skip: options.max_skip,
            max_query_bytes: options.max_query_bytes,
            max_param_bytes: options.max_param_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        },
        warnings: lowered.warnings.clone(),
    }
}

fn gql_limit_zero_projection_summaries(
    returns: &[GqlReturnExpr],
    order_by: &[GqlResolvedOrderItem],
) -> Vec<String> {
    let mut summaries = returns
        .iter()
        .map(|return_expr| format!("output column: {}", return_expr.output_name))
        .collect::<Vec<_>>();
    summaries.extend(order_by.iter().enumerate().map(|(index, item)| {
        format!(
            "order key {}: {} {:?}",
            index + 1,
            gql_expr_summary(&item.expr),
            item.direction
        )
    }));
    summaries
}

fn gql_explain_target(kind: GqlNativeTargetKind) -> GqlLoweringTarget {
    match kind {
        GqlNativeTargetKind::GraphRows => GqlLoweringTarget::GraphRowQuery,
    }
}

fn gql_row_ops(lowered: &GqlLoweredPlan) -> Vec<GqlRowOperation> {
    let mut ops = Vec::new();
    if !lowered.residual_predicates.is_empty() {
        ops.push(GqlRowOperation::ResidualFilter);
    }
    if !lowered.order_by.is_empty() {
        ops.push(GqlRowOperation::Sort);
    }
    if lowered.skip.is_some() {
        ops.push(GqlRowOperation::Skip);
    }
    if lowered.limit.is_some() {
        ops.push(GqlRowOperation::Limit);
    }
    ops.push(GqlRowOperation::Projection);
    ops
}

fn gql_projection_summaries(
    normalized: &NormalizedGraphRowQuery,
    returns: &[GqlReturnExpr],
    order_by: &[GqlResolvedOrderItem],
    include_vectors: bool,
) -> Vec<String> {
    let mut summaries = Vec::new();
    gql_append_projection_need_summaries(
        &mut summaries,
        "residual selected field",
        &normalized.projection_needs.residual,
        include_vectors,
    );
    gql_append_projection_need_summaries(
        &mut summaries,
        "order selected field",
        &normalized.projection_needs.order,
        include_vectors,
    );
    gql_append_projection_need_summaries(
        &mut summaries,
        "output selected field",
        &normalized.projection_needs.output,
        include_vectors,
    );
    gql_append_graph_row_element_output_summaries(&mut summaries, normalized, include_vectors);
    summaries.extend(
        returns
            .iter()
            .map(|return_expr| format!("output column: {}", return_expr.output_name)),
    );
    if !order_by.is_empty() {
        summaries.extend(order_by.iter().enumerate().map(|(index, item)| {
            format!(
                "order key {}: {} {:?}",
                index + 1,
                gql_expr_summary(&item.expr),
                item.direction
            )
        }));
    }
    summaries.sort();
    summaries.dedup();
    summaries
}

fn gql_append_projection_need_summaries(
    summaries: &mut Vec<String>,
    prefix: &str,
    needs: &EntityProjectionNeeds,
    include_vectors: bool,
) {
    for (alias, node_needs) in &needs.nodes {
        if node_needs.key {
            summaries.push(format!("{prefix}: {alias}.key"));
        }
        if node_needs.created_at {
            summaries.push(format!("{prefix}: {alias}.created_at"));
        }
        gql_append_property_selection_summaries(summaries, prefix, alias, &node_needs.props);
        match node_needs.vectors {
            VectorSelection::Dense => summaries.push(format!("{prefix}: {alias}.dense_vector")),
            VectorSelection::Sparse => summaries.push(format!("{prefix}: {alias}.sparse_vector")),
            VectorSelection::Both => {
                summaries.push(format!("{prefix}: {alias}.dense_vector"));
                summaries.push(format!("{prefix}: {alias}.sparse_vector"));
            }
            VectorSelection::None => {}
        }
        if include_vectors && !matches!(node_needs.vectors, VectorSelection::None) {
            summaries.push(format!("{prefix}: node element {alias} (vectors included)"));
        }
    }
    for (alias, edge_needs) in &needs.edges {
        if edge_needs.created_at {
            summaries.push(format!("{prefix}: {alias}.created_at"));
        }
        gql_append_property_selection_summaries(summaries, prefix, alias, &edge_needs.props);
    }
    for (alias, path_needs) in &needs.paths {
        if path_needs.node_ids {
            summaries.push(format!("{prefix}: {alias}.node_ids"));
        }
        if path_needs.edge_ids {
            summaries.push(format!("{prefix}: {alias}.edge_ids"));
        }
    }
    for (slot, edge_needs) in &needs.hidden_edges {
        if edge_needs.created_at {
            summaries.push(format!("{prefix}: hidden_edge[{slot}].created_at"));
        }
        gql_append_property_selection_summaries(
            summaries,
            prefix,
            &format!("hidden_edge[{slot}]"),
            &edge_needs.props,
        );
    }
    for (slot, path_needs) in &needs.hidden_paths {
        if path_needs.node_ids {
            summaries.push(format!("{prefix}: hidden_path[{slot}].node_ids"));
        }
        if path_needs.edge_ids {
            summaries.push(format!("{prefix}: hidden_path[{slot}].edge_ids"));
        }
    }
}

fn gql_append_property_selection_summaries(
    summaries: &mut Vec<String>,
    prefix: &str,
    alias: &str,
    props: &PropertySelection,
) {
    match props {
        PropertySelection::None => {}
        PropertySelection::Keys(keys) => {
            summaries.extend(keys.iter().map(|key| format!("{prefix}: {alias}.{key}")));
        }
        PropertySelection::All => summaries.push(format!("{prefix}: {alias}.props[*]")),
    }
}

fn gql_append_graph_row_element_output_summaries(
    summaries: &mut Vec<String>,
    normalized: &NormalizedGraphRowQuery,
    include_vectors: bool,
) {
    for item in &normalized.return_items {
        gql_append_graph_expr_output_summaries(summaries, &item.expr);
        if let GraphReturnProjection::Selected(selected) = &item.projection {
            gql_append_selected_return_projection_summaries(summaries, &item.expr, selected);
        }
        if let GraphExpr::Binding(alias) = &item.expr {
            let Some(slot) = normalized.binding_schema.slot_for_alias(alias) else {
                continue;
            };
            match slot.kind {
                crate::graph_row::GraphBindingSlotKind::Node => {
                    summaries.push(format!(
                        "output selected field: node element {alias} ({})",
                        if include_vectors {
                            "vectors included"
                        } else {
                            "vectors omitted"
                        }
                    ));
                }
                crate::graph_row::GraphBindingSlotKind::Edge => {
                    summaries.push(format!("output selected field: edge element {alias}"));
                }
                crate::graph_row::GraphBindingSlotKind::Path => {
                    summaries.push(format!("output selected field: path element {alias}"));
                }
                crate::graph_row::GraphBindingSlotKind::Scalar
                | crate::graph_row::GraphBindingSlotKind::HiddenOccurrence => {}
            }
        }
    }
}

fn gql_append_graph_expr_output_summaries(summaries: &mut Vec<String>, expr: &GraphExpr) {
    match expr {
        GraphExpr::NodeField { alias, field } => {
            summaries.push(format!(
                "output selected field: {alias}.{}",
                gql_graph_node_field_name(*field)
            ));
        }
        GraphExpr::EdgeField { alias, field } => {
            summaries.push(format!(
                "output selected field: {alias}.{}",
                gql_graph_edge_field_name(*field)
            ));
        }
        GraphExpr::PathField { alias, field } => {
            summaries.push(format!(
                "output selected field: {alias}.{}",
                gql_graph_path_field_name(*field)
            ));
        }
        GraphExpr::Function { name, args } => {
            if let Some(GraphExpr::Binding(alias)) = args.first() {
                match name {
                    GraphFunction::Id => summaries.push(format!("output selected field: {alias}.id")),
                    GraphFunction::Labels => {
                        summaries.push(format!("output selected field: {alias}.labels"))
                    }
                    GraphFunction::Type => {
                        summaries.push(format!("output selected field: {alias}.label"))
                    }
                    GraphFunction::Length => {
                        summaries.push(format!("output selected field: {alias}.length"))
                    }
                    GraphFunction::StartNode => {
                        summaries.push(format!("output selected field: {alias}.start_node"))
                    }
                    GraphFunction::EndNode => {
                        summaries.push(format!("output selected field: {alias}.end_node"))
                    }
                    GraphFunction::Nodes => {
                        summaries.push(format!("output selected field: {alias}.nodes"))
                    }
                    GraphFunction::Relationships => {
                        summaries.push(format!("output selected field: {alias}.relationships"))
                    }
                }
            }
        }
        GraphExpr::List(items) => {
            for item in items {
                gql_append_graph_expr_output_summaries(summaries, item);
            }
        }
        GraphExpr::Map(items) => {
            for item in items.values() {
                gql_append_graph_expr_output_summaries(summaries, item);
            }
        }
        GraphExpr::Unary { expr, .. }
        | GraphExpr::IsNull(expr)
        | GraphExpr::IsNotNull(expr) => gql_append_graph_expr_output_summaries(summaries, expr),
        GraphExpr::Binary { left, right, .. } => {
            gql_append_graph_expr_output_summaries(summaries, left);
            gql_append_graph_expr_output_summaries(summaries, right);
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
        | GraphExpr::Property { .. } => {}
    }
}

fn gql_graph_node_field_name(field: GraphNodeField) -> &'static str {
    match field {
        GraphNodeField::Id => "id",
        GraphNodeField::Labels => "labels",
        GraphNodeField::Key => "key",
        GraphNodeField::Weight => "weight",
        GraphNodeField::CreatedAt => "created_at",
        GraphNodeField::UpdatedAt => "updated_at",
    }
}

fn gql_graph_edge_field_name(field: GraphEdgeField) -> &'static str {
    match field {
        GraphEdgeField::Id => "id",
        GraphEdgeField::From => "from",
        GraphEdgeField::To => "to",
        GraphEdgeField::Label => "label",
        GraphEdgeField::Weight => "weight",
        GraphEdgeField::CreatedAt => "created_at",
        GraphEdgeField::UpdatedAt => "updated_at",
        GraphEdgeField::ValidFrom => "valid_from",
        GraphEdgeField::ValidTo => "valid_to",
    }
}

fn gql_graph_path_field_name(field: GraphPathField) -> &'static str {
    match field {
        GraphPathField::NodeIds => "node_ids",
        GraphPathField::EdgeIds => "edge_ids",
        GraphPathField::Length => "length",
    }
}

fn gql_append_selected_return_projection_summaries(
    summaries: &mut Vec<String>,
    expr: &GraphExpr,
    selected: &GraphSelectedProjection,
) {
    let alias = match expr {
        GraphExpr::Binding(alias)
        | GraphExpr::Property { alias, .. }
        | GraphExpr::NodeField { alias, .. }
        | GraphExpr::EdgeField { alias, .. }
        | GraphExpr::PathField { alias, .. } => alias.as_str(),
        _ => "expr",
    };
    match selected {
        GraphSelectedProjection::Node(node) => {
            if node.id {
                summaries.push(format!("output selected field: {alias}.id"));
            }
            if node.labels {
                summaries.push(format!("output selected field: {alias}.labels"));
            }
            if node.key {
                summaries.push(format!("output selected field: {alias}.key"));
            }
            gql_append_graph_property_selection_summaries(
                summaries,
                "output selected field",
                alias,
                &node.props,
            );
            if node.weight {
                summaries.push(format!("output selected field: {alias}.weight"));
            }
            if node.created_at {
                summaries.push(format!("output selected field: {alias}.created_at"));
            }
            if node.updated_at {
                summaries.push(format!("output selected field: {alias}.updated_at"));
            }
            match node.vectors {
                GraphVectorSelection::Dense => {
                    summaries.push(format!("output selected field: {alias}.dense_vector"))
                }
                GraphVectorSelection::Sparse => {
                    summaries.push(format!("output selected field: {alias}.sparse_vector"))
                }
                GraphVectorSelection::Both => {
                    summaries.push(format!("output selected field: {alias}.dense_vector"));
                    summaries.push(format!("output selected field: {alias}.sparse_vector"));
                }
                GraphVectorSelection::None => {}
            }
        }
        GraphSelectedProjection::Edge(edge) => {
            if edge.id {
                summaries.push(format!("output selected field: {alias}.id"));
            }
            if edge.from {
                summaries.push(format!("output selected field: {alias}.from"));
            }
            if edge.to {
                summaries.push(format!("output selected field: {alias}.to"));
            }
            if edge.label {
                summaries.push(format!("output selected field: {alias}.label"));
            }
            gql_append_graph_property_selection_summaries(
                summaries,
                "output selected field",
                alias,
                &edge.props,
            );
            if edge.weight {
                summaries.push(format!("output selected field: {alias}.weight"));
            }
            if edge.created_at {
                summaries.push(format!("output selected field: {alias}.created_at"));
            }
            if edge.updated_at {
                summaries.push(format!("output selected field: {alias}.updated_at"));
            }
            if edge.valid_from {
                summaries.push(format!("output selected field: {alias}.valid_from"));
            }
            if edge.valid_to {
                summaries.push(format!("output selected field: {alias}.valid_to"));
            }
        }
        GraphSelectedProjection::Path(path) => {
            if path.node_ids {
                summaries.push(format!("output selected field: {alias}.node_ids"));
            }
            if path.edge_ids {
                summaries.push(format!("output selected field: {alias}.edge_ids"));
            }
            if path.nodes.is_some() {
                summaries.push(format!("output selected field: {alias}.nodes"));
            }
            if path.edges.is_some() {
                summaries.push(format!("output selected field: {alias}.relationships"));
            }
        }
    }
}

fn gql_append_graph_property_selection_summaries(
    summaries: &mut Vec<String>,
    prefix: &str,
    alias: &str,
    props: &GraphPropertySelection,
) {
    match props {
        GraphPropertySelection::None => {}
        GraphPropertySelection::Keys(keys) => {
            summaries.extend(keys.iter().map(|key| format!("{prefix}: {alias}.{key}")));
        }
        GraphPropertySelection::All => summaries.push(format!("{prefix}: {alias}.props[*]")),
    }
}

fn gql_expr_summary(expr: &Expr) -> String {
    match &expr.kind {
        ExprKind::Literal(literal) => gql_literal_summary(literal),
        ExprKind::Parameter(name) => format!("${name}"),
        ExprKind::Variable(name) => name.clone(),
        ExprKind::PropertyAccess { object, property } => {
            format!("{}.{}", gql_expr_summary(object), property.name)
        }
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } => format!("NOT {}", gql_expr_summary(expr)),
        ExprKind::Binary { op, left, right } => format!(
            "{} {} {}",
            gql_expr_summary(left),
            gql_binary_op_summary(*op),
            gql_expr_summary(right)
        ),
        ExprKind::IsNull { expr, negated } => {
            if *negated {
                format!("{} IS NOT NULL", gql_expr_summary(expr))
            } else {
                format!("{} IS NULL", gql_expr_summary(expr))
            }
        }
        ExprKind::FunctionCall { name, args } => format!(
            "{}({})",
            name.name,
            args.iter()
                .map(gql_expr_summary)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ExprKind::List(_) => "list".to_string(),
        ExprKind::Map(_) => "map".to_string(),
    }
}

fn gql_literal_summary(literal: &Literal) -> String {
    match literal {
        Literal::Null => "null".to_string(),
        Literal::Bool(value) => value.to_string(),
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => value.to_string(),
        Literal::String(value) => format!("{value:?}"),
    }
}

fn gql_binary_op_summary(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "OR",
        BinaryOp::And => "AND",
        BinaryOp::Eq => "=",
        BinaryOp::Neq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::In => "IN",
    }
}
