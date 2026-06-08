use crate::gql::eval::{
    build_runtime_projection, eval_expr_against_context, return_exprs, GqlEvalContext,
    GqlReturnExpr,
};
use crate::gql::ast::{
    BinaryOp, Expr, ExprKind, GqlGraphTypeAlterMode, GqlGraphTypeCheckMode, GqlIndexStatement,
    GqlMutationStatement, GqlQuery, GqlSchemaStatement, GqlShowPropertyIndexScope,
    GqlShowSchemaKind, GqlStatementBody, Literal, OrderDirection, UnaryOp,
};
use crate::gql::index::{
    bind_index_statement, gql_index_target_kind_name, index_statement_is_mutating,
    GqlBoundPropertyIndexStatement, GqlIndexSemanticPlan, GqlPropertyIndexTargetKind,
};
use crate::gql::lower::{
    gql_expr_to_graph_expr, gql_order_direction_to_graph, lower_mutation, lower_semantic_plan,
    GqlCreateEdgePlan, GqlCreateNodePlan, GqlCreatePatternPlan, GqlDeleteTargetPlan,
    GqlLoweredPlan, GqlMutationClausePlan, GqlMutationInternalColumn, GqlMutationPlan,
    GqlMergePatternPlan, GqlMergePlan, GqlMutationExprRef, GqlNativeTarget, GqlNativeTargetKind,
    GqlRemoveItemPlan, GqlSetItemPlan,
};
use crate::gql::parser::{parse_statement, GqlParseOptions};
use crate::gql::params::{
    validate_referenced_gql_params, validate_referenced_gql_schema_ast_params,
};
use crate::gql::result::graph_value_to_gql_value;
use crate::gql::schema::{
    bind_schema_statement, gql_schema_target_kind_name, gql_value_from_edge_schema,
    gql_value_from_node_schema, gql_value_from_schema_violation, schema_statement_is_mutating,
    GqlBoundAlterGraphTypeStatement, GqlBoundCheckGraphTypeStatement, GqlSchemaSemanticPlan,
};
use crate::gql::semantic::{
    bind_query, gql_semantic_error, GqlAliasKind, GqlAliasOrigin, GqlReturnPlan,
    GqlSemanticPlan,
};
use crate::graph_row::{
    eval_graph_binary_values, eval_graph_expr, eval_graph_scalar_function_values,
    eval_graph_unary_value, graph_canonical_key_for_value, GraphBindingSchema, GraphCanonicalKey,
    GraphEvalContext, GraphEvalValue,
};
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
            GqlStatementBody::Schema(schema) => {
                self.execute_gql_schema(schema, params, options, started_at)
            }
            GqlStatementBody::Index(index) => self.execute_gql_index(index, options, started_at),
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
            GqlStatementBody::Schema(schema) => self.explain_gql_schema(schema, params, options),
            GqlStatementBody::Index(index) => self.explain_gql_index(index, options),
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
        if matches!(&lowered.native_target, GqlNativeTarget::GraphPipeline { .. }) {
            return self.execute_gql_pipeline_target(lowered, started_at, options);
        }
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
                schema_stats: None,
                index_stats: None,
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
            schema_stats: None,
            index_stats: None,
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
        if matches!(&lowered.native_target, GqlNativeTarget::GraphPipeline { .. }) {
            return self.explain_gql_pipeline_target(&lowered, options);
        }
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

    fn execute_gql_pipeline_target(
        &self,
        lowered: GqlLoweredPlan,
        started_at: Instant,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionResult, EngineError> {
        let GqlNativeTarget::GraphPipeline { query } = &lowered.native_target else {
            return Err(EngineError::InvalidOperation(
                "GQL pipeline execution received a non-pipeline target".to_string(),
            ));
        };
        let graph_result = self
            .query_graph_pipeline(query)
            .map_err(|err| graph_pipeline_execution_error_to_gql(err, &lowered))?;
        let mut warnings = lowered.warnings.clone();
        warnings.extend(graph_result.stats.warnings.iter().cloned());
        warnings.sort();
        warnings.dedup();
        let plan = if options.include_plan {
            graph_result
                .plan
                .as_ref()
                .map(|plan| build_gql_pipeline_execution_explain(&lowered, plan, options))
        } else {
            None
        };
        let columns = graph_result.columns.clone();
        let stats = graph_result.stats.clone();
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
        Ok(GqlExecutionResult {
            kind: GqlStatementKind::Query,
            columns,
            rows: projected,
            next_cursor: graph_result.next_cursor,
            stats: GqlExecutionStats {
                rows_returned,
                rows_matched: stats.intermediate_rows.max(stats.rows_after_filter),
                rows_after_filter: stats.rows_after_filter,
                intermediate_bindings: stats.intermediate_rows,
                db_hits: stats.db_hits,
                elapsed_us,
                warnings,
            },
            mutation_stats: None,
            schema_stats: None,
            index_stats: None,
            plan,
        })
    }

    fn explain_gql_pipeline_target(
        &self,
        lowered: &GqlLoweredPlan,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionExplain, EngineError> {
        let GqlNativeTarget::GraphPipeline { query } = &lowered.native_target else {
            return Err(EngineError::InvalidOperation(
                "GQL pipeline explain received a non-pipeline target".to_string(),
            ));
        };
        let explain = self
            .explain_graph_pipeline(query)
            .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?;
        Ok(build_gql_pipeline_execution_explain(
            lowered,
            &explain,
            options,
        ))
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

    pub fn query_graph_pipeline(
        &self,
        query: &GraphPipelineQuery,
    ) -> Result<GraphPipelineResult, EngineError> {
        if graph_pipeline_legacy_fast_path_eligible(query) {
            return self.query_graph_pipeline_one_stage(query);
        }

        let normalized = normalize_graph_pipeline_query(query)?;
        let decoded_cursor = query
            .page
            .cursor
            .as_deref()
            .map(|cursor| graph_pipeline_decode_logical_cursor(cursor, query.options.max_cursor_bytes))
            .transpose()?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let cursor_state =
            graph_pipeline_cursor_state_from_decoded(
                decoded_cursor,
                &query.page,
                query.at_epoch,
                query.options.max_skip,
            )?;
        let outcome = published
            .view
            .query_graph_pipeline_normalized(&normalized, cursor_state)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    fn query_graph_pipeline_one_stage(
        &self,
        query: &GraphPipelineQuery,
    ) -> Result<GraphPipelineResult, EngineError> {
        let mut graph_row_query = graph_pipeline_one_stage_graph_row_query(query)?;
        graph_row_query.page.cursor = graph_pipeline_decode_request_cursor(
            query.page.cursor.as_deref(),
            query.options.max_cursor_bytes,
        )?;
        let decoded_cursor =
            graph_row_decode_request_cursor(&graph_row_query.page, &graph_row_query.options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let cursor_state = graph_row_cursor_state_from_decoded(
            decoded_cursor,
            &graph_row_query.page,
            graph_row_query.at_epoch,
        )?;
        graph_pipeline_validate_cursor_state(&cursor_state, &query.options)?;
        let normalized = normalize_graph_row_query(&graph_row_query)?;
        let outcome = published
            .view
            .query_graph_rows_outcome(&normalized, cursor_state)?;
        let mut result = outcome.value;
        graph_pipeline_enforce_result_caps(query, &result)?;
        result.next_cursor = graph_pipeline_encode_cursor(
            result.next_cursor.take(),
            query.options.max_cursor_bytes,
        )?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(graph_pipeline_result_from_graph_row_result(
            query,
            result,
        ))
    }

    pub fn explain_graph_pipeline(
        &self,
        query: &GraphPipelineQuery,
    ) -> Result<GraphPipelineExplain, EngineError> {
        if !graph_pipeline_legacy_fast_path_eligible(query) {
            let mut normalized = normalize_graph_pipeline_query(query)?;
            normalized.options.include_plan = true;
            let decoded_cursor = query
                .page
                .cursor
                .as_deref()
                .map(|cursor| {
                    graph_pipeline_decode_logical_cursor(cursor, query.options.max_cursor_bytes)
                })
                .transpose()?;
            let (_guard, published) = self.runtime.published_snapshot()?;
            let cursor_state = graph_pipeline_cursor_state_from_decoded(
                decoded_cursor,
                &query.page,
                query.at_epoch,
                query.options.max_skip,
            )?;
            if normalized.options.profile {
                let outcome = published
                    .view
                    .query_graph_pipeline_normalized(&normalized, cursor_state)?;
                return outcome.value.plan.ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "graph pipeline explain did not produce a plan".to_string(),
                    )
                });
            }
            return published
                .view
                .explain_graph_pipeline_normalized(&normalized, cursor_state);
        }

        let mut graph_row_query = graph_pipeline_one_stage_graph_row_query(query)?;
        graph_row_query.page.cursor = graph_pipeline_decode_request_cursor(
            query.page.cursor.as_deref(),
            query.options.max_cursor_bytes,
        )?;
        let decoded_cursor =
            graph_row_decode_request_cursor(&graph_row_query.page, &graph_row_query.options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let cursor_state = graph_row_cursor_state_from_decoded(
            decoded_cursor,
            &graph_row_query.page,
            graph_row_query.at_epoch,
        )?;
        graph_pipeline_validate_cursor_state(&cursor_state, &query.options)?;
        let normalized = normalize_graph_row_query(&graph_row_query)?;
        let graph_row_explain = published
            .view
            .explain_graph_rows_normalized(&normalized, cursor_state)?;
        Ok(graph_pipeline_explain_from_graph_row_explain(
            query,
            graph_row_explain,
            None,
        ))
    }

}

impl DatabaseEngine {
    fn execute_gql_index(
        &self,
        index: GqlIndexStatement,
        options: &GqlExecutionOptions,
        started_at: Instant,
    ) -> Result<GqlExecutionResult, EngineError> {
        if options.cursor.is_some() {
            return Err(EngineError::InvalidCursor {
                message: "GQL index statements do not accept cursors".into(),
            });
        }
        if options.mode == GqlExecutionMode::ReadOnly && index_statement_is_mutating(&index) {
            return Err(EngineError::InvalidOperation(
                "GQL index management is not allowed in ReadOnly mode".to_string(),
            ));
        }
        let bound = bind_index_statement(index)?;
        let plan = if options.include_plan {
            Some(build_gql_index_execution_explain(&bound, options))
        } else {
            None
        };
        match bound {
            GqlIndexSemanticPlan::Create(create) => {
                self.execute_bound_gql_index_create(create, options, started_at, plan)
            }
            GqlIndexSemanticPlan::Drop(drop) => {
                self.execute_bound_gql_index_drop(drop, options, started_at, plan)
            }
            GqlIndexSemanticPlan::Show { scope, .. } => {
                self.execute_bound_gql_index_show(scope, options, started_at, plan)
            }
        }
    }

    fn explain_gql_index(
        &self,
        index: GqlIndexStatement,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionExplain, EngineError> {
        if options.cursor.is_some() {
            return Err(EngineError::InvalidCursor {
                message: "GQL index statements do not accept cursors".into(),
            });
        }
        if options.mode == GqlExecutionMode::ReadOnly && index_statement_is_mutating(&index) {
            return Err(EngineError::InvalidOperation(
                "GQL index management is not allowed in ReadOnly mode".to_string(),
            ));
        }
        let bound = bind_index_statement(index)?;
        Ok(build_gql_index_execution_explain(&bound, options))
    }

    fn execute_bound_gql_index_create(
        &self,
        create: GqlBoundPropertyIndexStatement,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let row = match create.target_kind {
            GqlPropertyIndexTargetKind::Node => {
                let info = self.ensure_node_property_index(
                    &create.label,
                    &create.prop_key,
                    create.kind.clone(),
                )?;
                create_node_property_index_row(&info)
            }
            GqlPropertyIndexTargetKind::Edge => {
                let info = self.ensure_edge_property_index(
                    &create.label,
                    &create.prop_key,
                    create.kind.clone(),
                )?;
                create_edge_property_index_row(&info)
            }
        };
        Ok(gql_index_execution_result(
            create_property_index_operation_name(),
            create_property_index_columns(),
            vec![row],
            GqlIndexStatsCounts {
                indexes_ensured: 1,
                indexes_dropped: 0,
                indexes_returned: 0,
            },
            options,
            started_at,
            plan,
        ))
    }

    fn execute_bound_gql_index_drop(
        &self,
        drop: GqlBoundPropertyIndexStatement,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let dropped = match drop.target_kind {
            GqlPropertyIndexTargetKind::Node => {
                self.drop_node_property_index(&drop.label, &drop.prop_key, drop.kind.clone())?
            }
            GqlPropertyIndexTargetKind::Edge => {
                self.drop_edge_property_index(&drop.label, &drop.prop_key, drop.kind.clone())?
            }
        };
        let row = drop_property_index_row(&drop, dropped);
        Ok(gql_index_execution_result(
            drop_property_index_operation_name(),
            drop_property_index_columns(),
            vec![row],
            GqlIndexStatsCounts {
                indexes_ensured: 0,
                indexes_dropped: if dropped { 1 } else { 0 },
                indexes_returned: 0,
            },
            options,
            started_at,
            plan,
        ))
    }

    fn execute_bound_gql_index_show(
        &self,
        scope: GqlShowPropertyIndexScope,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let operation = show_property_index_operation_name(scope);
        let catalog_rows = self.show_property_index_catalog_rows(scope)?;
        if catalog_rows.len() > options.max_rows {
            return Err(EngineError::InvalidOperation(format!(
                "GQL index SHOW result has {} rows, exceeding max_rows={}; index SHOW does not support cursors",
                catalog_rows.len(),
                options.max_rows
            )));
        }
        let rows = catalog_rows
            .iter()
            .map(show_property_index_row)
            .collect::<Vec<_>>();
        let indexes_returned = rows.len() as u64;
        Ok(gql_index_execution_result(
            operation,
            show_property_index_columns(),
            rows,
            GqlIndexStatsCounts {
                indexes_ensured: 0,
                indexes_dropped: 0,
                indexes_returned,
            },
            options,
            started_at,
            plan,
        ))
    }

    fn show_property_index_catalog_rows(
        &self,
        scope: GqlShowPropertyIndexScope,
    ) -> Result<Vec<GqlPropertyIndexCatalogRow>, EngineError> {
        let mut rows = {
            let (_guard, published) = self.runtime.published_snapshot()?;
            let mut rows = Vec::new();
            for entry in &published.view.secondary_index_entries {
                let Some(row) =
                    gql_property_index_catalog_row(entry, published.label_catalog.as_ref())?
                else {
                    continue;
                };
                if show_property_index_scope_matches(scope, row.target_kind) {
                    rows.push(row);
                }
            }
            rows
        };
        rows.sort_unstable_by(|left, right| {
            gql_index_target_kind_rank(left.target_kind)
                .cmp(&gql_index_target_kind_rank(right.target_kind))
                .then_with(|| left.label.cmp(&right.label))
                .then_with(|| left.prop_key.cmp(&right.prop_key))
                .then_with(|| gql_index_kind_rank(&left.kind).cmp(&gql_index_kind_rank(&right.kind)))
                .then_with(|| left.index_id.cmp(&right.index_id))
        });
        Ok(rows)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GqlPropertyIndexCatalogRow {
    target_kind: GqlPropertyIndexTargetKind,
    label: String,
    prop_key: String,
    kind: SecondaryIndexKind,
    state: SecondaryIndexState,
    index_id: u64,
    last_error: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct GqlIndexStatsCounts {
    indexes_ensured: u64,
    indexes_dropped: u64,
    indexes_returned: u64,
}

fn gql_property_index_catalog_row(
    entry: &SecondaryIndexManifestEntry,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<Option<GqlPropertyIndexCatalogRow>, EngineError> {
    let (target_kind, label, prop_key) = match &entry.target {
        SecondaryIndexTarget::NodeProperty { label_id, prop_key } => (
            GqlPropertyIndexTargetKind::Node,
            catalog
                .node_label(*label_id)
                .map(str::to_string)
                .ok_or_else(|| {
                    EngineError::ManifestError(format!(
                        "node property index {} references missing node label label_id {}",
                        entry.index_id, label_id
                    ))
                })?,
            prop_key.clone(),
        ),
        SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => (
            GqlPropertyIndexTargetKind::Edge,
            catalog
                .edge_label(*label_id)
                .map(str::to_string)
                .ok_or_else(|| {
                    EngineError::ManifestError(format!(
                        "edge property index {} references missing edge-label label_id {}",
                        entry.index_id, label_id
                    ))
                })?,
            prop_key.clone(),
        ),
    };
    Ok(Some(GqlPropertyIndexCatalogRow {
        target_kind,
        label,
        prop_key,
        kind: entry.kind.clone(),
        state: entry.state,
        index_id: entry.index_id,
        last_error: entry.last_error.clone(),
    }))
}

fn show_property_index_scope_matches(
    scope: GqlShowPropertyIndexScope,
    target_kind: GqlPropertyIndexTargetKind,
) -> bool {
    matches!(
        (scope, target_kind),
        (GqlShowPropertyIndexScope::All, _)
            | (GqlShowPropertyIndexScope::Node, GqlPropertyIndexTargetKind::Node)
            | (GqlShowPropertyIndexScope::Edge, GqlPropertyIndexTargetKind::Edge)
    )
}

fn create_property_index_columns() -> Vec<String> {
    gql_index_columns([
        "operation",
        "target_kind",
        "label",
        "prop_key",
        "kind",
        "action",
        "state",
        "index_id",
        "last_error",
    ])
}

fn drop_property_index_columns() -> Vec<String> {
    gql_index_columns([
        "operation",
        "target_kind",
        "label",
        "prop_key",
        "kind",
        "action",
    ])
}

fn show_property_index_columns() -> Vec<String> {
    gql_index_columns([
        "target_kind",
        "label",
        "prop_key",
        "kind",
        "state",
        "index_id",
        "last_error",
    ])
}

fn gql_index_columns<const N: usize>(columns: [&str; N]) -> Vec<String> {
    columns.into_iter().map(str::to_string).collect()
}

fn create_node_property_index_row(info: &NodePropertyIndexInfo) -> GqlRow {
    create_property_index_row(
        GqlPropertyIndexTargetKind::Node,
        &info.label,
        &info.prop_key,
        &info.kind,
        info.state,
        info.index_id,
        info.last_error.as_ref(),
    )
}

fn create_edge_property_index_row(info: &EdgePropertyIndexInfo) -> GqlRow {
    create_property_index_row(
        GqlPropertyIndexTargetKind::Edge,
        &info.label,
        &info.prop_key,
        &info.kind,
        info.state,
        info.index_id,
        info.last_error.as_ref(),
    )
}

fn create_property_index_row(
    target_kind: GqlPropertyIndexTargetKind,
    label: &str,
    prop_key: &str,
    kind: &SecondaryIndexKind,
    state: SecondaryIndexState,
    index_id: u64,
    last_error: Option<&String>,
) -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String(create_property_index_operation_name().to_string()),
            GqlValue::String(gql_index_target_kind_name(target_kind).to_string()),
            GqlValue::String(label.to_string()),
            GqlValue::String(prop_key.to_string()),
            GqlValue::String(gql_index_kind_name(kind).to_string()),
            GqlValue::String("ensured".to_string()),
            GqlValue::String(gql_index_state_name(state).to_string()),
            GqlValue::UInt(index_id),
            gql_index_last_error_value(last_error),
        ],
    }
}

fn drop_property_index_row(bound: &GqlBoundPropertyIndexStatement, dropped: bool) -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String(drop_property_index_operation_name().to_string()),
            GqlValue::String(gql_index_target_kind_name(bound.target_kind).to_string()),
            GqlValue::String(bound.label.clone()),
            GqlValue::String(bound.prop_key.clone()),
            GqlValue::String(gql_index_kind_name(&bound.kind).to_string()),
            GqlValue::String(if dropped { "dropped" } else { "not_found" }.to_string()),
        ],
    }
}

fn show_property_index_row(row: &GqlPropertyIndexCatalogRow) -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String(gql_index_target_kind_name(row.target_kind).to_string()),
            GqlValue::String(row.label.clone()),
            GqlValue::String(row.prop_key.clone()),
            GqlValue::String(gql_index_kind_name(&row.kind).to_string()),
            GqlValue::String(gql_index_state_name(row.state).to_string()),
            GqlValue::UInt(row.index_id),
            gql_index_last_error_value(row.last_error.as_ref()),
        ],
    }
}

fn gql_index_last_error_value(last_error: Option<&String>) -> GqlValue {
    last_error
        .cloned()
        .map(GqlValue::String)
        .unwrap_or(GqlValue::Null)
}

fn gql_index_execution_result(
    operation: &str,
    columns: Vec<String>,
    rows: Vec<GqlRow>,
    counts: GqlIndexStatsCounts,
    options: &GqlExecutionOptions,
    started_at: Instant,
    plan: Option<GqlExecutionExplain>,
) -> GqlExecutionResult {
    let rows_returned = rows.len();
    let elapsed_us = index_elapsed(options, started_at);
    GqlExecutionResult {
        kind: GqlStatementKind::Index,
        columns,
        rows,
        next_cursor: None,
        stats: GqlExecutionStats {
            rows_returned,
            rows_matched: 0,
            rows_after_filter: 0,
            intermediate_bindings: 0,
            db_hits: 0,
            elapsed_us,
            warnings: Vec::new(),
        },
        mutation_stats: None,
        schema_stats: None,
        index_stats: Some(gql_index_stats(operation, counts, elapsed_us)),
        plan,
    }
}

fn gql_index_stats(
    operation: &str,
    counts: GqlIndexStatsCounts,
    elapsed_us: Option<u64>,
) -> GqlIndexStats {
    GqlIndexStats {
        operation: operation.to_string(),
        indexes_ensured: counts.indexes_ensured,
        indexes_dropped: counts.indexes_dropped,
        indexes_returned: counts.indexes_returned,
        elapsed_us,
        warnings: Vec::new(),
    }
}

fn index_elapsed(options: &GqlExecutionOptions, started_at: Instant) -> Option<u64> {
    if options.profile {
        started_at.elapsed().as_micros().try_into().ok()
    } else {
        None
    }
}

fn build_gql_index_execution_explain(
    plan: &GqlIndexSemanticPlan,
    options: &GqlExecutionOptions,
) -> GqlExecutionExplain {
    GqlExecutionExplain {
        kind: GqlStatementKind::Index,
        columns: gql_index_explain_columns(plan),
        read: None,
        mutation: None,
        schema: None,
        index: Some(gql_index_explain(plan)),
        caps: gql_execution_cap_summary(options),
        warnings: Vec::new(),
        notes: vec![
            "Index explain is side-effect-free and does not create labels, write manifests, enqueue builds, drop declarations, inspect sidecars, or scan graph records".to_string(),
        ],
    }
}

fn gql_index_explain(plan: &GqlIndexSemanticPlan) -> GqlIndexExplain {
    match plan {
        GqlIndexSemanticPlan::Create(create) => GqlIndexExplain {
            operation: create_property_index_operation_name().to_string(),
            targets: vec![gql_index_explain_property_target(create, "ensure")],
            uses_core_write_queue: true,
            publishes_manifest: true,
            creates_labels: true,
            schedules_background_build: true,
            drops_index_data_async: false,
            side_effect_free: false,
        },
        GqlIndexSemanticPlan::Drop(drop) => GqlIndexExplain {
            operation: drop_property_index_operation_name().to_string(),
            targets: vec![gql_index_explain_property_target(drop, "drop")],
            uses_core_write_queue: true,
            publishes_manifest: true,
            creates_labels: false,
            schedules_background_build: false,
            drops_index_data_async: true,
            side_effect_free: false,
        },
        GqlIndexSemanticPlan::Show { scope, .. } => GqlIndexExplain {
            operation: show_property_index_operation_name(*scope).to_string(),
            targets: vec![gql_index_explain_show_target(*scope)],
            uses_core_write_queue: false,
            publishes_manifest: false,
            creates_labels: false,
            schedules_background_build: false,
            drops_index_data_async: false,
            side_effect_free: true,
        },
    }
}

fn gql_index_explain_columns(plan: &GqlIndexSemanticPlan) -> Vec<String> {
    match plan {
        GqlIndexSemanticPlan::Create(_) => create_property_index_columns(),
        GqlIndexSemanticPlan::Drop(_) => drop_property_index_columns(),
        GqlIndexSemanticPlan::Show { .. } => show_property_index_columns(),
    }
}

fn gql_index_explain_property_target(
    bound: &GqlBoundPropertyIndexStatement,
    action: &str,
) -> GqlIndexExplainTarget {
    GqlIndexExplainTarget {
        target_kind: gql_index_target_kind_name(bound.target_kind).to_string(),
        label: Some(bound.label.clone()),
        prop_key: Some(bound.prop_key.clone()),
        kind: Some(gql_index_kind_name(&bound.kind).to_string()),
        action: Some(action.to_string()),
    }
}

fn gql_index_explain_show_target(scope: GqlShowPropertyIndexScope) -> GqlIndexExplainTarget {
    GqlIndexExplainTarget {
        target_kind: match scope {
            GqlShowPropertyIndexScope::All => "property_index_catalog",
            GqlShowPropertyIndexScope::Node => "node",
            GqlShowPropertyIndexScope::Edge => "edge",
        }
        .to_string(),
        label: None,
        prop_key: None,
        kind: None,
        action: Some("show".to_string()),
    }
}

fn create_property_index_operation_name() -> &'static str {
    "create_property_index"
}

fn drop_property_index_operation_name() -> &'static str {
    "drop_property_index"
}

fn show_property_index_operation_name(scope: GqlShowPropertyIndexScope) -> &'static str {
    match scope {
        GqlShowPropertyIndexScope::All => "show_property_indexes",
        GqlShowPropertyIndexScope::Node => "show_node_property_indexes",
        GqlShowPropertyIndexScope::Edge => "show_edge_property_indexes",
    }
}

fn gql_index_kind_name(kind: &SecondaryIndexKind) -> &'static str {
    match kind {
        SecondaryIndexKind::Equality => "equality",
        SecondaryIndexKind::Range => "range",
    }
}

fn gql_index_state_name(state: SecondaryIndexState) -> &'static str {
    match state {
        SecondaryIndexState::Building => "building",
        SecondaryIndexState::Ready => "ready",
        SecondaryIndexState::Failed => "failed",
    }
}

fn gql_index_target_kind_rank(kind: GqlPropertyIndexTargetKind) -> u8 {
    match kind {
        GqlPropertyIndexTargetKind::Node => 0,
        GqlPropertyIndexTargetKind::Edge => 1,
    }
}

fn gql_index_kind_rank(kind: &SecondaryIndexKind) -> u8 {
    match kind {
        SecondaryIndexKind::Equality => 0,
        SecondaryIndexKind::Range => 1,
    }
}

impl DatabaseEngine {
    fn execute_gql_schema(
        &self,
        schema: GqlSchemaStatement,
        params: &GqlParams,
        options: &GqlExecutionOptions,
        started_at: Instant,
    ) -> Result<GqlExecutionResult, EngineError> {
        if options.cursor.is_some() {
            return Err(EngineError::InvalidCursor {
                message: "GQL schema statements do not accept cursors".into(),
            });
        }
        if options.mode == GqlExecutionMode::ReadOnly && schema_statement_is_mutating(&schema) {
            return Err(EngineError::InvalidOperation(
                "GQL schema publication is not allowed in ReadOnly mode".to_string(),
            ));
        }
        validate_referenced_gql_schema_ast_params(&schema, params, options)?;
        let bound = bind_schema_statement(schema, params)?;
        let plan = if options.include_plan {
            Some(build_gql_schema_execution_explain(&bound, options))
        } else {
            None
        };
        match bound {
            GqlSchemaSemanticPlan::Alter(alter) => {
                self.execute_bound_gql_schema_alter(alter, options, started_at, plan)
            }
            GqlSchemaSemanticPlan::DropCurrentGraphType { .. } => {
                self.execute_bound_gql_schema_drop_current(options, started_at, plan)
            }
            GqlSchemaSemanticPlan::Check(check) => {
                self.execute_bound_gql_schema_check(check, options, started_at, plan)
            }
            GqlSchemaSemanticPlan::Show { kind, .. } => {
                self.execute_bound_gql_schema_show(kind, options, started_at, plan)
            }
        }
    }

    fn explain_gql_schema(
        &self,
        schema: GqlSchemaStatement,
        params: &GqlParams,
        options: &GqlExecutionOptions,
    ) -> Result<GqlExecutionExplain, EngineError> {
        if options.cursor.is_some() {
            return Err(EngineError::InvalidCursor {
                message: "GQL schema statements do not accept cursors".into(),
            });
        }
        if options.mode == GqlExecutionMode::ReadOnly && schema_statement_is_mutating(&schema) {
            return Err(EngineError::InvalidOperation(
                "GQL schema publication is not allowed in ReadOnly mode".to_string(),
            ));
        }
        validate_referenced_gql_schema_ast_params(&schema, params, options)?;
        let bound = bind_schema_statement(schema, params)?;
        Ok(build_gql_schema_execution_explain(&bound, options))
    }

    fn execute_bound_gql_schema_alter(
        &self,
        alter: GqlBoundAlterGraphTypeStatement,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let operation = alter_operation_name(alter.mode);
        match alter.mode {
            GqlGraphTypeAlterMode::Add => {
                let result = self.alter_graph_schema(alter.operations, alter.options)?;
                Ok(gql_schema_execution_result(
                    operation,
                    alter_add_set_columns(),
                    rows_for_gql_schema_publish(&result, false),
                    gql_schema_stats_from_publish(operation, &result, schema_elapsed(options, started_at)),
                    options,
                    started_at,
                    plan,
                ))
            }
            GqlGraphTypeAlterMode::Set => {
                let schema = alter.schema.expect("SET plan always carries a schema");
                let set_empty = schema.node_schemas.is_empty() && schema.edge_schemas.is_empty();
                let result = self.set_graph_schema(schema, alter.options)?;
                let rows = if set_empty {
                    vec![alter_set_empty_row()]
                } else {
                    rows_for_gql_schema_publish(&result, false)
                };
                Ok(gql_schema_execution_result(
                    operation,
                    alter_add_set_columns(),
                    rows,
                    gql_schema_stats_from_publish(operation, &result, schema_elapsed(options, started_at)),
                    options,
                    started_at,
                    plan,
                ))
            }
            GqlGraphTypeAlterMode::Drop => {
                let result = self.alter_graph_schema(alter.operations, alter.options)?;
                Ok(gql_schema_execution_result(
                    operation,
                    alter_drop_columns(),
                    rows_for_gql_schema_selected_drop(&result),
                    gql_schema_stats_from_publish(operation, &result, schema_elapsed(options, started_at)),
                    options,
                    started_at,
                    plan,
                ))
            }
        }
    }

    fn execute_bound_gql_schema_drop_current(
        &self,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let result = self.drop_graph_schema()?;
        let operation = "drop_current_graph_type";
        Ok(gql_schema_execution_result(
            operation,
            drop_current_columns(),
            vec![drop_current_row(&result)],
            gql_schema_stats_from_publish(operation, &result, schema_elapsed(options, started_at)),
            options,
            started_at,
            plan,
        ))
    }

    fn execute_bound_gql_schema_check(
        &self,
        check: GqlBoundCheckGraphTypeStatement,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let operation = check_operation_name(check.mode);
        let check_empty =
            check.schema.node_schemas.is_empty() && check.schema.edge_schemas.is_empty();
        let report = match check.mode {
            GqlGraphTypeCheckMode::Add => self.check_graph_schema_add(check.schema, check.options)?,
            GqlGraphTypeCheckMode::Set => self.check_graph_schema_set(check.schema, check.options)?,
        };
        let rows = if check.mode == GqlGraphTypeCheckMode::Set && check_empty {
            vec![check_set_empty_row()]
        } else {
            rows_for_gql_schema_check(&report)
        };
        Ok(gql_schema_execution_result(
            operation,
            check_columns(),
            rows,
            gql_schema_stats_from_check(operation, &report, schema_elapsed(options, started_at)),
            options,
            started_at,
            plan,
        ))
    }

    fn execute_bound_gql_schema_show(
        &self,
        kind: GqlShowSchemaKind,
        options: &GqlExecutionOptions,
        started_at: Instant,
        plan: Option<GqlExecutionExplain>,
    ) -> Result<GqlExecutionResult, EngineError> {
        let operation = show_operation_name(&kind);
        let rows = match kind {
            GqlShowSchemaKind::CurrentGraphType => {
                self.show_current_graph_type_rows()?
            }
            GqlShowSchemaKind::NodeSchemas => show_node_schema_rows(self.list_node_schemas()?),
            GqlShowSchemaKind::EdgeSchemas => show_edge_schema_rows(self.list_edge_schemas()?),
            GqlShowSchemaKind::NodeSchema { label } => self
                .get_node_schema(&label.name)?
                .map(|info| show_node_schema_rows(vec![info]))
                .unwrap_or_default(),
            GqlShowSchemaKind::EdgeSchema { label } => self
                .get_edge_schema(&label.name)?
                .map(|info| show_edge_schema_rows(vec![info]))
                .unwrap_or_default(),
        };
        if rows.len() > options.max_rows {
            return Err(EngineError::InvalidOperation(format!(
                "GQL schema SHOW result has {} rows, exceeding max_rows={}; schema SHOW does not support cursors",
                rows.len(),
                options.max_rows
            )));
        }
        Ok(gql_schema_execution_result(
            operation,
            show_columns(),
            rows,
            gql_schema_stats_for_show(operation, schema_elapsed(options, started_at)),
            options,
            started_at,
            plan,
        ))
    }

    fn show_current_graph_type_rows(&self) -> Result<Vec<GqlRow>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut node_infos = published
            .schema_catalog
            .node_schemas
            .iter()
            .map(|entry| {
                node_schema_info_from_entry_with_catalog(entry, published.label_catalog.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        node_infos.sort_unstable_by(|left, right| left.label.cmp(&right.label));

        let mut edge_infos = published
            .schema_catalog
            .edge_schemas
            .iter()
            .map(|entry| {
                edge_schema_info_from_entry_with_catalog(entry, published.label_catalog.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        edge_infos.sort_unstable_by(|left, right| left.label.cmp(&right.label));

        let mut rows = show_node_schema_rows(node_infos);
        rows.extend(show_edge_schema_rows(edge_infos));
        Ok(rows)
    }
}

fn gql_schema_execution_result(
    _operation: &str,
    columns: Vec<String>,
    rows: Vec<GqlRow>,
    schema_stats: GqlSchemaStats,
    options: &GqlExecutionOptions,
    started_at: Instant,
    plan: Option<GqlExecutionExplain>,
) -> GqlExecutionResult {
    let rows_returned = rows.len();
    let elapsed_us = schema_elapsed(options, started_at);
    GqlExecutionResult {
        kind: GqlStatementKind::Schema,
        columns,
        rows,
        next_cursor: None,
        stats: GqlExecutionStats {
            rows_returned,
            rows_matched: 0,
            rows_after_filter: 0,
            intermediate_bindings: 0,
            db_hits: 0,
            elapsed_us,
            warnings: Vec::new(),
        },
        mutation_stats: None,
        schema_stats: Some(schema_stats),
        index_stats: None,
        plan,
    }
}

fn alter_add_set_columns() -> Vec<String> {
    gql_schema_columns([
        "operation",
        "target_kind",
        "label",
        "action",
        "checked_records",
        "violation_count",
        "truncated",
        "scan_limit_hit",
    ])
}

fn alter_drop_columns() -> Vec<String> {
    gql_schema_columns(["operation", "target_kind", "label", "action"])
}

fn drop_current_columns() -> Vec<String> {
    gql_schema_columns([
        "operation",
        "target_kind",
        "label",
        "action",
        "node_schemas_dropped",
        "edge_schemas_dropped",
    ])
}

fn check_columns() -> Vec<String> {
    gql_schema_columns([
        "operation",
        "target_kind",
        "label",
        "checked_records",
        "violation_count",
        "truncated",
        "scan_limit_hit",
        "violations",
    ])
}

fn show_columns() -> Vec<String> {
    gql_schema_columns(["target_kind", "label", "schema"])
}

fn gql_schema_columns<const N: usize>(columns: [&str; N]) -> Vec<String> {
    columns.into_iter().map(str::to_string).collect()
}

fn rows_for_gql_schema_publish(
    result: &GraphSchemaPublishResult,
    _include_catalog_rows: bool,
) -> Vec<GqlRow> {
    let operation = match result.operation {
        GraphSchemaOperationKind::Add => "alter_graph_type_add",
        GraphSchemaOperationKind::Set => "alter_graph_type_set",
        other => panic!("unexpected publish operation for ALTER ADD/SET rows: {other:?}"),
    };
    result
        .validation
        .entries
        .iter()
        .map(|entry| alter_publish_row(operation, entry))
        .collect()
}

fn alter_publish_row(operation: &str, entry: &GraphSchemaValidationReportEntry) -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String(operation.to_string()),
            GqlValue::String(gql_schema_target_kind_name(entry.target_kind).to_string()),
            GqlValue::String(entry.label.clone()),
            GqlValue::String("published".to_string()),
            GqlValue::UInt(entry.report.checked_records),
            GqlValue::UInt(entry.report.violation_count),
            GqlValue::Bool(entry.report.truncated),
            GqlValue::Bool(entry.report.scan_limit_hit),
        ],
    }
}

fn alter_set_empty_row() -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String("alter_graph_type_set".to_string()),
            GqlValue::String("graph".to_string()),
            GqlValue::Null,
            GqlValue::String("published_empty_graph_type".to_string()),
            GqlValue::UInt(0),
            GqlValue::UInt(0),
            GqlValue::Bool(false),
            GqlValue::Bool(false),
        ],
    }
}

fn rows_for_gql_schema_selected_drop(result: &GraphSchemaPublishResult) -> Vec<GqlRow> {
    result
        .drop_targets
        .iter()
        .map(|target| GqlRow {
            values: vec![
                GqlValue::String("alter_graph_type_drop".to_string()),
                GqlValue::String(gql_schema_target_kind_name(target.target_kind).to_string()),
                GqlValue::String(target.label.clone()),
                GqlValue::String(drop_action_name(target.action).to_string()),
            ],
        })
        .collect()
}

fn drop_current_row(result: &GraphSchemaPublishResult) -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String("drop_current_graph_type".to_string()),
            GqlValue::String("graph".to_string()),
            GqlValue::Null,
            GqlValue::String("dropped".to_string()),
            GqlValue::UInt(result.node_schemas_dropped as u64),
            GqlValue::UInt(result.edge_schemas_dropped as u64),
        ],
    }
}

fn rows_for_gql_schema_check(report: &GraphSchemaCheckReport) -> Vec<GqlRow> {
    let operation = match report.operation {
        GraphSchemaOperationKind::CheckAdd => "check_graph_type_add",
        GraphSchemaOperationKind::CheckSet => "check_graph_type_set",
        other => panic!("unexpected check operation for CHECK rows: {other:?}"),
    };
    report
        .entries
        .iter()
        .map(|entry| check_row(operation, entry))
        .collect()
}

fn check_row(operation: &str, entry: &GraphSchemaValidationReportEntry) -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String(operation.to_string()),
            GqlValue::String(gql_schema_target_kind_name(entry.target_kind).to_string()),
            GqlValue::String(entry.label.clone()),
            GqlValue::UInt(entry.report.checked_records),
            GqlValue::UInt(entry.report.violation_count),
            GqlValue::Bool(entry.report.truncated),
            GqlValue::Bool(entry.report.scan_limit_hit),
            GqlValue::List(
                entry
                    .report
                    .violations
                    .iter()
                    .map(gql_value_from_schema_violation)
                    .collect(),
            ),
        ],
    }
}

fn check_set_empty_row() -> GqlRow {
    GqlRow {
        values: vec![
            GqlValue::String("check_graph_type_set".to_string()),
            GqlValue::String("graph".to_string()),
            GqlValue::Null,
            GqlValue::UInt(0),
            GqlValue::UInt(0),
            GqlValue::Bool(false),
            GqlValue::Bool(false),
            GqlValue::List(Vec::new()),
        ],
    }
}

fn show_node_schema_rows(infos: Vec<NodeSchemaInfo>) -> Vec<GqlRow> {
    infos
        .into_iter()
        .map(|info| GqlRow {
            values: vec![
                GqlValue::String("node".to_string()),
                GqlValue::String(info.label),
                gql_value_from_node_schema(&info.schema),
            ],
        })
        .collect()
}

fn show_edge_schema_rows(infos: Vec<EdgeSchemaInfo>) -> Vec<GqlRow> {
    infos
        .into_iter()
        .map(|info| GqlRow {
            values: vec![
                GqlValue::String("edge".to_string()),
                GqlValue::String(info.label),
                gql_value_from_edge_schema(&info.schema),
            ],
        })
        .collect()
}

fn gql_schema_stats_from_publish(
    operation: &str,
    result: &GraphSchemaPublishResult,
    elapsed_us: Option<u64>,
) -> GqlSchemaStats {
    gql_schema_stats_from_report(
        operation,
        &result.validation,
        result.targets_published as u64,
        result.targets_dropped as u64,
        elapsed_us,
    )
}

fn gql_schema_stats_from_check(
    operation: &str,
    report: &GraphSchemaCheckReport,
    elapsed_us: Option<u64>,
) -> GqlSchemaStats {
    gql_schema_stats_from_report(operation, report, 0, 0, elapsed_us)
}

fn gql_schema_stats_from_report(
    operation: &str,
    report: &GraphSchemaCheckReport,
    targets_published: u64,
    targets_dropped: u64,
    elapsed_us: Option<u64>,
) -> GqlSchemaStats {
    GqlSchemaStats {
        operation: operation.to_string(),
        targets_checked: report.entries.len() as u64,
        targets_published,
        targets_dropped,
        checked_records: report.checked_records,
        violation_count: report.violation_count,
        truncated: report.truncated,
        scan_limit_hit: report.scan_limit_hit,
        elapsed_us,
        warnings: Vec::new(),
    }
}

fn gql_schema_stats_for_show(operation: &str, elapsed_us: Option<u64>) -> GqlSchemaStats {
    GqlSchemaStats {
        operation: operation.to_string(),
        targets_checked: 0,
        targets_published: 0,
        targets_dropped: 0,
        checked_records: 0,
        violation_count: 0,
        truncated: false,
        scan_limit_hit: false,
        elapsed_us,
        warnings: Vec::new(),
    }
}

fn schema_elapsed(options: &GqlExecutionOptions, started_at: Instant) -> Option<u64> {
    if options.profile {
        started_at.elapsed().as_micros().try_into().ok()
    } else {
        None
    }
}

fn build_gql_schema_execution_explain(
    plan: &GqlSchemaSemanticPlan,
    options: &GqlExecutionOptions,
) -> GqlExecutionExplain {
    let schema = gql_schema_explain(plan);
    let columns = gql_schema_explain_columns(plan);
    let mut notes = vec![
        "Schema explain is side-effect-free and does not publish schemas, create labels, write manifests, drop schemas, or scan graph data".to_string(),
    ];
    if schema.uses_core_write_queue {
        notes.push(
            "Schema execution routes catalog publication/drop through the core serialized write queue"
                .to_string(),
        );
    } else {
        notes.push(
            "Schema execution uses side-effect-free CHECK or catalog SHOW APIs without the core write queue"
                .to_string(),
        );
    }
    GqlExecutionExplain {
        kind: GqlStatementKind::Schema,
        columns,
        read: None,
        mutation: None,
        schema: Some(schema),
        index: None,
        caps: gql_execution_cap_summary(options),
        warnings: Vec::new(),
        notes,
    }
}

fn gql_schema_explain(plan: &GqlSchemaSemanticPlan) -> GqlSchemaExplain {
    match plan {
        GqlSchemaSemanticPlan::Alter(alter) => gql_schema_explain_for_alter(alter),
        GqlSchemaSemanticPlan::DropCurrentGraphType { .. } => GqlSchemaExplain {
            operation: "drop_current_graph_type".to_string(),
            targets: vec![schema_explain_target("graph", None, Some("drop"))],
            replaces_entire_catalog: true,
            publishes_manifest: true,
            validates_existing_data: false,
            uses_core_write_queue: true,
            side_effect_free: false,
            options: empty_schema_explain_options(),
        },
        GqlSchemaSemanticPlan::Check(check) => gql_schema_explain_for_check(check),
        GqlSchemaSemanticPlan::Show { kind, .. } => GqlSchemaExplain {
            operation: show_operation_name(kind).to_string(),
            targets: show_schema_explain_targets(kind),
            replaces_entire_catalog: false,
            publishes_manifest: false,
            validates_existing_data: false,
            uses_core_write_queue: false,
            side_effect_free: true,
            options: empty_schema_explain_options(),
        },
    }
}

fn gql_schema_explain_for_alter(alter: &GqlBoundAlterGraphTypeStatement) -> GqlSchemaExplain {
    let targets = match alter.mode {
        GqlGraphTypeAlterMode::Add | GqlGraphTypeAlterMode::Set => alter
            .schema
            .as_ref()
            .map(schema_publish_explain_targets)
            .unwrap_or_default(),
        GqlGraphTypeAlterMode::Drop => alter_drop_explain_targets(&alter.operations),
    };
    GqlSchemaExplain {
        operation: alter_operation_name(alter.mode).to_string(),
        targets: if alter.mode == GqlGraphTypeAlterMode::Set && targets.is_empty() {
            vec![schema_explain_target(
                "graph",
                None,
                Some("publish_empty_graph_type"),
            )]
        } else {
            targets
        },
        replaces_entire_catalog: alter.mode == GqlGraphTypeAlterMode::Set,
        publishes_manifest: true,
        validates_existing_data: alter.mode != GqlGraphTypeAlterMode::Drop
            && alter
                .schema
                .as_ref()
                .is_some_and(|schema| !schema.node_schemas.is_empty() || !schema.edge_schemas.is_empty()),
        uses_core_write_queue: true,
        side_effect_free: false,
        options: if alter.mode == GqlGraphTypeAlterMode::Drop {
            empty_schema_explain_options()
        } else {
            GqlSchemaExplainOptions {
                max_violations: Some(alter.options.max_violations),
                chunk_size: Some(alter.options.chunk_size),
                scan_limit: alter.options.scan_limit,
            }
        },
    }
}

fn gql_schema_explain_for_check(check: &GqlBoundCheckGraphTypeStatement) -> GqlSchemaExplain {
    let targets = schema_publish_explain_targets(&check.schema);
    GqlSchemaExplain {
        operation: check_operation_name(check.mode).to_string(),
        targets: if check.mode == GqlGraphTypeCheckMode::Set && targets.is_empty() {
            vec![schema_explain_target(
                "graph",
                None,
                Some("check_empty_graph_type"),
            )]
        } else {
            targets
        },
        replaces_entire_catalog: check.mode == GqlGraphTypeCheckMode::Set,
        publishes_manifest: false,
        validates_existing_data: !check.schema.node_schemas.is_empty()
            || !check.schema.edge_schemas.is_empty(),
        uses_core_write_queue: false,
        side_effect_free: true,
        options: GqlSchemaExplainOptions {
            max_violations: Some(check.options.max_violations),
            chunk_size: Some(check.options.chunk_size),
            scan_limit: check.options.scan_limit,
        },
    }
}

fn gql_schema_explain_columns(plan: &GqlSchemaSemanticPlan) -> Vec<String> {
    match plan {
        GqlSchemaSemanticPlan::Alter(alter) => match alter.mode {
            GqlGraphTypeAlterMode::Add | GqlGraphTypeAlterMode::Set => alter_add_set_columns(),
            GqlGraphTypeAlterMode::Drop => alter_drop_columns(),
        },
        GqlSchemaSemanticPlan::DropCurrentGraphType { .. } => drop_current_columns(),
        GqlSchemaSemanticPlan::Check(_) => check_columns(),
        GqlSchemaSemanticPlan::Show { .. } => show_columns(),
    }
}

fn schema_publish_explain_targets(schema: &GraphSchema) -> Vec<GqlSchemaExplainTarget> {
    schema
        .node_schemas
        .iter()
        .map(|info| schema_explain_target("node", Some(info.label.clone()), Some("publish")))
        .chain(
            schema
                .edge_schemas
                .iter()
                .map(|info| schema_explain_target("edge", Some(info.label.clone()), Some("publish"))),
        )
        .collect()
}

fn alter_drop_explain_targets(
    operations: &[GraphSchemaOperation],
) -> Vec<GqlSchemaExplainTarget> {
    operations
        .iter()
        .map(|operation| match operation {
            GraphSchemaOperation::DropNode { label } => {
                schema_explain_target("node", Some(label.clone()), Some("drop"))
            }
            GraphSchemaOperation::DropEdge { label } => {
                schema_explain_target("edge", Some(label.clone()), Some("drop"))
            }
            GraphSchemaOperation::SetNode { .. } | GraphSchemaOperation::SetEdge { .. } => {
                unreachable!("ALTER DROP explain only receives drop operations")
            }
        })
        .collect()
}

fn show_schema_explain_targets(kind: &GqlShowSchemaKind) -> Vec<GqlSchemaExplainTarget> {
    match kind {
        GqlShowSchemaKind::CurrentGraphType => {
            vec![schema_explain_target("graph", None, Some("show"))]
        }
        GqlShowSchemaKind::NodeSchemas => {
            vec![schema_explain_target("node", None, Some("show"))]
        }
        GqlShowSchemaKind::EdgeSchemas => {
            vec![schema_explain_target("edge", None, Some("show"))]
        }
        GqlShowSchemaKind::NodeSchema { label } => {
            vec![schema_explain_target("node", Some(label.name.clone()), Some("show"))]
        }
        GqlShowSchemaKind::EdgeSchema { label } => {
            vec![schema_explain_target("edge", Some(label.name.clone()), Some("show"))]
        }
    }
}

fn schema_explain_target(
    target_kind: &str,
    label: Option<String>,
    action: Option<&str>,
) -> GqlSchemaExplainTarget {
    GqlSchemaExplainTarget {
        target_kind: target_kind.to_string(),
        label,
        action: action.map(str::to_string),
    }
}

fn empty_schema_explain_options() -> GqlSchemaExplainOptions {
    GqlSchemaExplainOptions {
        max_violations: None,
        chunk_size: None,
        scan_limit: None,
    }
}

fn alter_operation_name(mode: GqlGraphTypeAlterMode) -> &'static str {
    match mode {
        GqlGraphTypeAlterMode::Add => "alter_graph_type_add",
        GqlGraphTypeAlterMode::Set => "alter_graph_type_set",
        GqlGraphTypeAlterMode::Drop => "alter_graph_type_drop",
    }
}

fn check_operation_name(mode: GqlGraphTypeCheckMode) -> &'static str {
    match mode {
        GqlGraphTypeCheckMode::Add => "check_graph_type_add",
        GqlGraphTypeCheckMode::Set => "check_graph_type_set",
    }
}

fn show_operation_name(kind: &GqlShowSchemaKind) -> &'static str {
    match kind {
        GqlShowSchemaKind::CurrentGraphType => "show_current_graph_type",
        GqlShowSchemaKind::NodeSchemas => "show_node_schemas",
        GqlShowSchemaKind::EdgeSchemas => "show_edge_schemas",
        GqlShowSchemaKind::NodeSchema { .. } => "show_node_schema",
        GqlShowSchemaKind::EdgeSchema { .. } => "show_edge_schema",
    }
}

fn drop_action_name(action: GraphSchemaDropAction) -> &'static str {
    match action {
        GraphSchemaDropAction::Dropped => "dropped",
        GraphSchemaDropAction::NotFound => "not_found",
    }
}

fn graph_pipeline_legacy_fast_path_eligible(query: &GraphPipelineQuery) -> bool {
    let [GraphPipelineStage::Match(match_stage), GraphPipelineStage::Project(project_stage)] =
        query.stages.as_slice()
    else {
        return false;
    };
    !match_stage.optional
        && project_stage.kind == GraphProjectKind::Return
        && !project_stage.distinct
        && project_stage.where_.is_none()
        && project_stage.skip.is_none()
        && project_stage.limit.is_none()
        && !graph_project_stage_contains_aggregate(project_stage)
}

fn graph_project_stage_contains_aggregate(stage: &GraphProjectStage) -> bool {
    let item_contains_aggregate = match &stage.items {
        GraphProjectionItems::Star => false,
        GraphProjectionItems::Items(items) => items
            .iter()
            .any(|item| graph_expr_contains_aggregate(&item.expr)),
    };
    item_contains_aggregate || stage
        .order_by
        .iter()
        .any(|item| graph_expr_contains_aggregate(&item.expr))
}

fn graph_pipeline_one_stage_graph_row_query(
    query: &GraphPipelineQuery,
) -> Result<GraphRowQuery, EngineError> {
    if query.page.skip > query.options.max_skip {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline page skip {} exceeds max_skip {}",
            query.page.skip, query.options.max_skip
        )));
    }

    let [GraphPipelineStage::Match(match_stage), GraphPipelineStage::Project(project_stage)] =
        query.stages.as_slice()
    else {
        return Err(graph_pipeline_cp34_1_unsupported(
            "expected exactly Match followed by terminal Project",
        ));
    };

    if match_stage.optional {
        return Err(graph_pipeline_cp34_1_unsupported(
            "top-level optional Match stages are deferred",
        ));
    }
    if project_stage.kind != GraphProjectKind::Return {
        return Err(graph_pipeline_cp34_1_unsupported(
            "terminal Project must have kind Return",
        ));
    }
    if project_stage.distinct {
        return Err(graph_pipeline_cp34_1_unsupported(
            "Project DISTINCT is deferred",
        ));
    }
    if project_stage.where_.is_some() {
        return Err(graph_pipeline_cp34_1_unsupported(
            "Project WHERE filters are deferred",
        ));
    }
    if project_stage.skip.is_some() || project_stage.limit.is_some() {
        return Err(graph_pipeline_cp34_1_unsupported(
            "Project-local SKIP/LIMIT are deferred; use GraphPipelineQuery.page for CP34.1",
        ));
    }
    validate_pipeline_node_aliases(&match_stage.nodes)?;
    validate_pipeline_piece_aliases(&match_stage.pieces)?;

    let return_items = match &project_stage.items {
        GraphProjectionItems::Star => None,
        GraphProjectionItems::Items(items) => {
            if items.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "graph pipeline Project items must not be empty".to_string(),
                ));
            }
            for item in items {
                if let Some(alias) = item.alias.as_ref() {
                    validate_graph_pipeline_user_alias(alias, "projection alias")?;
                }
            }
            Some(
                items
                    .iter()
                    .map(|item| GraphReturnItem {
                        expr: item.expr.clone(),
                        alias: item.alias.clone(),
                        projection: item.projection.clone(),
                    })
                    .collect(),
            )
        }
    };

    let graph_row_query = GraphRowQuery {
        nodes: match_stage.nodes.clone(),
        pieces: match_stage.pieces.clone(),
        where_: match_stage.where_.clone(),
        return_items,
        order_by: project_stage.order_by.clone(),
        page: query.page.clone(),
        at_epoch: query.at_epoch,
        params: query.params.clone(),
        output: query.output.clone(),
        options: graph_query_options_from_pipeline(&query.options),
    };
    graph_pipeline_validate_referenced_params(&graph_row_query, &query.options)?;
    Ok(graph_row_query)
}

fn graph_pipeline_cp34_1_unsupported(detail: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph pipeline shape is not supported in CP34.1: {detail}"
    ))
}

fn graph_query_options_from_pipeline(options: &GraphPipelineOptions) -> GraphQueryOptions {
    GraphQueryOptions {
        allow_full_scan: options.allow_full_scan,
        max_intermediate_bindings: options
            .max_intermediate_bindings
            .min(options.max_pipeline_rows),
        max_frontier: options.max_frontier,
        max_path_hops: options.max_path_hops,
        max_paths_per_start: options.max_paths_per_start,
        max_page_limit: options.max_rows,
        max_order_materialization: options.max_order_materialization,
        max_cursor_bytes: options.max_cursor_bytes,
        max_query_bytes: options.max_query_bytes,
        include_plan: options.include_plan,
        profile: options.profile,
    }
}

const GRAPH_PIPELINE_CURSOR_PREFIX: &str = "ogr34p1_";
const GRAPH_PIPELINE_CURSOR_MAGIC: &[u8; 8] = b"OGR34PC1";
const GRAPH_PIPELINE_CURSOR_VERSION: u8 = 1;

fn graph_pipeline_decode_request_cursor(
    cursor: Option<&str>,
    max_cursor_bytes: usize,
) -> Result<Option<String>, EngineError> {
    cursor
        .map(|cursor| graph_pipeline_decode_cursor(cursor, max_cursor_bytes))
        .transpose()
}

fn graph_pipeline_decode_cursor(
    cursor: &str,
    max_cursor_bytes: usize,
) -> Result<String, EngineError> {
    let Some(encoded) = cursor.strip_prefix(GRAPH_PIPELINE_CURSOR_PREFIX) else {
        return Err(invalid_graph_pipeline_cursor(
            "invalid graph pipeline cursor prefix",
        ));
    };
    let transport_limit = graph_pipeline_encoded_cursor_transport_limit(max_cursor_bytes);
    if cursor.len() > transport_limit {
        return Err(invalid_graph_pipeline_cursor(format!(
            "encoded graph pipeline cursor is too large to decode within max_cursor_bytes {}",
            max_cursor_bytes
        )));
    }
    let bytes = base64url_no_pad_decode(encoded)?;
    if bytes.len() > max_cursor_bytes {
        return Err(invalid_graph_pipeline_cursor(format!(
            "decoded graph pipeline cursor is {} bytes, exceeding max_cursor_bytes {}",
            bytes.len(),
            max_cursor_bytes
        )));
    }
    if bytes.len() < GRAPH_PIPELINE_CURSOR_MAGIC.len() + 1 + 4 + 8 {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor payload is too short",
        ));
    }
    let checksum_offset = bytes
        .len()
        .checked_sub(8)
        .ok_or_else(|| invalid_graph_pipeline_cursor("graph pipeline cursor is missing checksum"))?;
    let payload = &bytes[..checksum_offset];
    let checksum = u64::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .map_err(|_| invalid_graph_pipeline_cursor("graph pipeline cursor checksum is malformed"))?,
    );
    if crate::types::fnv1a(payload) != checksum {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor checksum mismatch",
        ));
    }
    let mut reader = CursorPayloadReader::new(payload);
    if reader.take(GRAPH_PIPELINE_CURSOR_MAGIC.len())? != GRAPH_PIPELINE_CURSOR_MAGIC {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor magic mismatch",
        ));
    }
    let version = reader.read_u8()?;
    if version != GRAPH_PIPELINE_CURSOR_VERSION {
        return Err(invalid_graph_pipeline_cursor(format!(
            "unsupported graph pipeline cursor version {version}"
        )));
    }
    let inner = reader.read_bytes()?;
    if !reader.is_finished() {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor payload has trailing bytes",
        ));
    }
    let inner = std::str::from_utf8(inner)
        .map_err(|_| invalid_graph_pipeline_cursor("inner graph row cursor is not UTF-8"))?
        .to_string();
    if !inner.starts_with(GRAPH_ROW_CURSOR_PREFIX) {
        return Err(invalid_graph_pipeline_cursor(
            "inner cursor is not a graph row cursor",
        ));
    }
    Ok(inner)
}

fn graph_pipeline_encoded_cursor_transport_limit(max_decoded_bytes: usize) -> usize {
    let tail = match max_decoded_bytes % 3 {
        0 => 0,
        1 => 2,
        _ => 3,
    };
    let encoded = (max_decoded_bytes / 3)
        .checked_mul(4)
        .and_then(|value| value.checked_add(tail))
        .unwrap_or(usize::MAX);
    GRAPH_PIPELINE_CURSOR_PREFIX.len().saturating_add(encoded)
}

fn graph_pipeline_validate_referenced_params(
    query: &GraphRowQuery,
    options: &GraphPipelineOptions,
) -> Result<(), EngineError> {
    let referenced_params = collect_graph_row_referenced_params(query)?;
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for (name, value) in &referenced_params {
        graph_pipeline_validate_param_value(
            name,
            value,
            options,
            &mut total_items,
            &mut total_bytes,
        )?;
    }
    Ok(())
}

fn graph_pipeline_validate_param_value(
    name: &str,
    value: &GraphParamValue,
    options: &GraphPipelineOptions,
    total_items: &mut usize,
    total_bytes: &mut usize,
) -> Result<(), EngineError> {
    let mut stack = vec![(value, 0usize)];
    while let Some((value, container_depth)) = stack.pop() {
        match value {
            GraphParamValue::Null
            | GraphParamValue::Bool(_)
            | GraphParamValue::Int(_)
            | GraphParamValue::UInt(_)
            | GraphParamValue::Float(_) => {}
            GraphParamValue::String(value) => graph_pipeline_add_param_bytes(
                name,
                value.len(),
                "string",
                total_bytes,
                options,
            )?,
            GraphParamValue::Bytes(value) => graph_pipeline_add_param_bytes(
                name,
                value.len(),
                "bytes",
                total_bytes,
                options,
            )?,
            GraphParamValue::List(values) => {
                let depth = container_depth.saturating_add(1);
                graph_pipeline_check_param_depth(name, depth, options)?;
                graph_pipeline_add_param_items(name, values.len(), "list", total_items, options)?;
                for item in values.iter().rev() {
                    stack.push((item, depth));
                }
            }
            GraphParamValue::Map(values) => {
                let depth = container_depth.saturating_add(1);
                graph_pipeline_check_param_depth(name, depth, options)?;
                graph_pipeline_add_param_items(name, values.len(), "map", total_items, options)?;
                for (key, value) in values.iter().rev() {
                    graph_pipeline_add_param_bytes(
                        name,
                        key.len(),
                        "map key",
                        total_bytes,
                        options,
                    )?;
                    stack.push((value, depth));
                }
            }
        }
    }
    Ok(())
}

fn graph_pipeline_check_param_depth(
    name: &str,
    depth: usize,
    options: &GraphPipelineOptions,
) -> Result<(), EngineError> {
    if depth > options.max_ast_depth {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline parameter '{name}' nested list/map depth exceeds max_ast_depth {}",
            options.max_ast_depth
        )));
    }
    Ok(())
}

fn graph_pipeline_add_param_items(
    name: &str,
    count: usize,
    container_kind: &str,
    total_items: &mut usize,
    options: &GraphPipelineOptions,
) -> Result<(), EngineError> {
    if count > options.max_literal_items {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline parameter '{name}' {container_kind} contains {count} items, exceeding max_literal_items {}",
            options.max_literal_items
        )));
    }
    *total_items = total_items
        .checked_add(count)
        .filter(|total| *total <= options.max_literal_items)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "referenced graph pipeline parameters contain more than max_literal_items={} total list/map items",
                options.max_literal_items
            ))
        })?;
    Ok(())
}

fn graph_pipeline_add_param_bytes(
    name: &str,
    bytes: usize,
    value_kind: &str,
    total_bytes: &mut usize,
    options: &GraphPipelineOptions,
) -> Result<(), EngineError> {
    if bytes > options.max_param_bytes {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline parameter '{name}' {value_kind} is {bytes} bytes, exceeding max_param_bytes {}",
            options.max_param_bytes
        )));
    }
    *total_bytes = total_bytes
        .checked_add(bytes)
        .filter(|total| *total <= options.max_param_bytes)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "referenced graph pipeline parameters contain more than max_param_bytes={} total string/bytes/map-key bytes",
                options.max_param_bytes
            ))
        })?;
    Ok(())
}

fn graph_pipeline_encode_cursor(
    cursor: Option<String>,
    max_cursor_bytes: usize,
) -> Result<Option<String>, EngineError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor_bytes = cursor.as_bytes();
    let cursor_len = u32::try_from(cursor_bytes.len()).map_err(|_| {
        invalid_graph_pipeline_cursor("inner graph row cursor is too large to encode")
    })?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(GRAPH_PIPELINE_CURSOR_MAGIC);
    push_u8(&mut bytes, GRAPH_PIPELINE_CURSOR_VERSION);
    push_u32(&mut bytes, cursor_len);
    bytes.extend_from_slice(cursor_bytes);
    let checksum = crate::types::fnv1a(&bytes);
    push_u64(&mut bytes, checksum);
    if bytes.len() > max_cursor_bytes {
        return Err(invalid_graph_pipeline_cursor(format!(
            "emitted graph pipeline cursor payload is {} bytes, exceeding max_cursor_bytes {}",
            bytes.len(),
            max_cursor_bytes
        )));
    }
    Ok(Some(format!(
        "{GRAPH_PIPELINE_CURSOR_PREFIX}{}",
        base64url_no_pad_encode(&bytes)
    )))
}

fn invalid_graph_pipeline_cursor(message: impl Into<String>) -> EngineError {
    EngineError::InvalidCursor {
        message: message.into(),
    }
}

fn graph_pipeline_validate_cursor_state(
    cursor_state: &GraphRowCursorState,
    options: &GraphPipelineOptions,
) -> Result<(), EngineError> {
    if cursor_state.original_skip > options.max_skip as u64 {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline cursor original skip {} exceeds max_skip {}",
            cursor_state.original_skip, options.max_skip
        )));
    }
    Ok(())
}

fn graph_pipeline_enforce_result_caps(
    query: &GraphPipelineQuery,
    result: &GraphRowResult,
) -> Result<(), EngineError> {
    if result.stats.rows_after_filter > query.options.max_pipeline_rows {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline exceeded max_pipeline_rows {}",
            query.options.max_pipeline_rows
        )));
    }
    Ok(())
}

fn graph_pipeline_result_from_graph_row_result(
    query: &GraphPipelineQuery,
    mut result: GraphRowResult,
) -> GraphPipelineResult {
    let stats = graph_pipeline_stats_from_graph_row_stats(&result.stats);
    let plan = result
        .plan
        .take()
        .map(|explain| graph_pipeline_explain_from_graph_row_explain(query, explain, Some(stats.clone())));
    GraphPipelineResult {
        columns: result.columns,
        rows: result.rows,
        next_cursor: result.next_cursor,
        stats,
        plan,
    }
}

fn graph_pipeline_stats_from_graph_row_stats(stats: &GraphRowStats) -> GraphPipelineStats {
    GraphPipelineStats {
        rows_returned: stats.rows_returned,
        // CP34.1 starts the one supported pipeline shape from the implicit initial row.
        rows_entered_pipeline: 1,
        rows_after_filter: stats.rows_after_filter,
        intermediate_rows: stats.intermediate_bindings_peak,
        pipeline_rows_materialized: stats.rows_seen_for_page,
        groups: 0,
        collect_items: 0,
        union_branches: 0,
        union_dedup_keys: 0,
        subquery_invocations: 0,
        subquery_cache_hits: 0,
        shortest_path_pairs: 0,
        shortest_path_cache_hits: 0,
        db_hits: stats.db_hits,
        elapsed_us: stats.elapsed_us,
        effective_at_epoch: stats.effective_at_epoch,
        warnings: stats.warnings.clone(),
    }
}

fn graph_pipeline_validation_stats(
    explain: &GraphRowExplain,
    warnings: Vec<String>,
) -> GraphPipelineStats {
    GraphPipelineStats {
        rows_returned: 0,
        // Direct explain has no runtime row counts, but the CP34.1 pipeline shell still starts
        // from the same implicit initial row as execution.
        rows_entered_pipeline: 1,
        rows_after_filter: 0,
        intermediate_rows: 0,
        pipeline_rows_materialized: 0,
        groups: 0,
        collect_items: 0,
        union_branches: 0,
        union_dedup_keys: 0,
        subquery_invocations: 0,
        subquery_cache_hits: 0,
        shortest_path_pairs: 0,
        shortest_path_cache_hits: 0,
        db_hits: 0,
        elapsed_us: None,
        effective_at_epoch: explain.effective_at_epoch.unwrap_or_default(),
        warnings,
    }
}

fn graph_pipeline_explain_from_graph_row_explain(
    query: &GraphPipelineQuery,
    graph_row: GraphRowExplain,
    stats: Option<GraphPipelineStats>,
) -> GraphPipelineExplain {
    let columns = graph_row.columns.clone();
    let warnings = graph_row.warnings.clone();
    let notes = graph_row.notes.clone();
    let stats = stats.unwrap_or_else(|| graph_pipeline_validation_stats(&graph_row, warnings.clone()));
    let match_stage = GraphPipelineStageExplain {
        index: 0,
        kind: "Match".to_string(),
        detail: "graph-row-backed match stage".to_string(),
        columns: Vec::new(),
        graph_row: Some(Box::new(graph_row.clone())),
        warnings: graph_row.warnings.clone(),
        notes: graph_row.notes.clone(),
    };
    let project_stage = GraphPipelineStageExplain {
        index: 1,
        kind: "Project(Return)".to_string(),
        detail: "terminal graph-row projection stage".to_string(),
        columns: columns.clone(),
        graph_row: None,
        warnings: Vec::new(),
        notes: Vec::new(),
    };
    GraphPipelineExplain {
        columns,
        effective_at_epoch: graph_row.effective_at_epoch,
        fingerprint: format!("pipeline:{}", graph_row.fingerprint),
        stages: vec![match_stage, project_stage],
        row_ops: graph_row.row_ops,
        order: graph_row.order,
        cursor: graph_row.cursor,
        projection: graph_row.projection,
        caps: graph_pipeline_cap_explain(&query.options),
        summaries: graph_row.summaries,
        stats,
        warnings,
        notes,
    }
}

fn graph_pipeline_cap_explain(options: &GraphPipelineOptions) -> GraphPipelineCapExplain {
    GraphPipelineCapExplain {
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
                        | GqlMutationClausePlan::Merge(_)
                        | GqlMutationClausePlan::Set(_)
                        | GqlMutationClausePlan::Remove(_)
                        | GqlMutationClausePlan::Delete { .. }
                )
            })
}

#[derive(Clone)]
struct GqlCreateExecutionRow {
    read_nodes: BTreeMap<String, Option<u64>>,
    read_edges: BTreeMap<String, Option<u64>>,
    read_paths: BTreeMap<String, Option<GqlPathIdentity>>,
    read_scalars: BTreeMap<String, GraphValue>,
    expr_values: Vec<Option<GraphValue>>,
    created_nodes: BTreeMap<String, usize>,
    created_edges: BTreeMap<String, usize>,
    created_node_writes: BTreeSet<usize>,
    created_edge_writes: BTreeSet<usize>,
    touched_created_nodes: BTreeSet<usize>,
    touched_created_edges: BTreeSet<usize>,
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
            params,
            input.rows,
            &txn,
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
            schema_stats: None,
            index_stats: None,
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
            let read_result = execute_gql_mutation_read_prefix(
                snapshot,
                &read_prefix.lowered,
                options,
            )?;
            for followup in read_result.followups {
                self.runtime.enqueue_secondary_index_read_followup(followup);
            }
            if read_result.rows.len() > options.max_mutation_rows {
                return Err(gql_mutation_cap_error(
                    "max_mutation_rows",
                    read_result.rows.len(),
                    options.max_mutation_rows,
                ));
            }
            if read_result.next_cursor.is_some() {
                let (cap_name, cap_value) =
                    if options.max_intermediate_bindings <= options.max_mutation_rows {
                        ("max_intermediate_bindings", options.max_intermediate_bindings)
                    } else {
                        ("max_mutation_rows", options.max_mutation_rows)
                    };
                return Err(gql_mutation_cap_error(
                    cap_name,
                    read_result.rows.len().saturating_add(1),
                    cap_value,
                ));
            }
            let db_hits = read_result.db_hits;
            let rows = read_result
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
                read_scalars: BTreeMap::new(),
                expr_values: vec![None; plan.operation_exprs.len()],
                created_nodes: BTreeMap::new(),
                created_edges: BTreeMap::new(),
                created_node_writes: BTreeSet::new(),
                created_edge_writes: BTreeSet::new(),
                touched_created_nodes: BTreeSet::new(),
                touched_created_edges: BTreeSet::new(),
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

struct GqlMutationReadPrefixRuntimeResult {
    rows: Vec<GraphRow>,
    next_cursor: Option<String>,
    db_hits: usize,
    followups: Vec<SecondaryIndexReadFollowup>,
}

fn execute_gql_mutation_read_prefix(
    snapshot: &ReadView,
    lowered: &GqlLoweredPlan,
    options: &GqlExecutionOptions,
) -> Result<GqlMutationReadPrefixRuntimeResult, EngineError> {
    match &lowered.native_target {
        GqlNativeTarget::GraphRows { .. } => {
            let outcome = execute_gql_graph_row_target(snapshot, lowered)?;
            let graph_result = outcome.value;
            let db_hits = if options.profile {
                gql_profile_graph_row_db_hits(&graph_result.stats)
            } else {
                0
            };
            Ok(GqlMutationReadPrefixRuntimeResult {
                rows: graph_result.rows,
                next_cursor: graph_result.next_cursor,
                db_hits,
                followups: outcome.followups,
            })
        }
        GqlNativeTarget::GraphPipeline { .. } => {
            let outcome = execute_gql_graph_pipeline_target_on_view(snapshot, lowered)?;
            let graph_result = outcome.value;
            let db_hits = if options.profile {
                gql_profile_graph_pipeline_db_hits(&graph_result.stats)
            } else {
                0
            };
            Ok(GqlMutationReadPrefixRuntimeResult {
                rows: graph_result.rows,
                next_cursor: graph_result.next_cursor,
                db_hits,
                followups: outcome.followups,
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

fn gql_profile_graph_pipeline_db_hits(stats: &GraphPipelineStats) -> usize {
    stats
        .db_hits
        .max(stats.intermediate_rows)
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
        read_scalars: BTreeMap::new(),
        expr_values: vec![None; plan.operation_exprs.len()],
        created_nodes: BTreeMap::new(),
        created_edges: BTreeMap::new(),
        created_node_writes: BTreeSet::new(),
        created_edge_writes: BTreeSet::new(),
        touched_created_nodes: BTreeSet::new(),
        touched_created_edges: BTreeSet::new(),
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
                    GqlAliasKind::Scalar => {
                        return Err(EngineError::InvalidOperation(
                            "scalar aliases are not mutation targets".to_string(),
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
            GqlMutationInternalColumn::ScalarValue { alias, .. } => {
                let value = values.get(value_index).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "mutation read prefix returned fewer scalar columns than planned"
                            .to_string(),
                    )
                })?;
                row.read_scalars.insert(alias.clone(), value.clone());
                value_index += 1;
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
            if expr.late || supplied_by_read_prefix.contains(&expr.id) {
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

#[allow(clippy::too_many_arguments)]
fn materialize_gql_create(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    mut rows: Vec<GqlCreateExecutionRow>,
    txn: &WriteTxn,
    edge_uniqueness: bool,
    max_mutation_ops: usize,
    default_valid_from: i64,
    snapshot: &ReadView,
) -> Result<GqlCreateMaterialization, EngineError> {
    let (mut existing_node_ids, mut existing_edge_ids) =
        collect_gql_existing_update_targets(plan, &rows);
    collect_gql_late_expr_read_prefix_targets(
        plan,
        &rows,
        &mut existing_node_ids,
        &mut existing_edge_ids,
    );
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
    let mut merge_overlay = TxnMergeOverlay::default();
    let mut merge_node_locals: BTreeMap<TxnMergeLocalNodeRef, usize> = BTreeMap::new();
    let mut merge_edge_locals: BTreeMap<TxnMergeLocalEdgeRef, usize> = BTreeMap::new();
    let mut merge_db_hits = 0usize;
    let mut op_budget = GqlMaterializationOpBudget::new(max_mutation_ops);

    for clause in &plan.clauses {
        match clause {
            GqlMutationClausePlan::Create(patterns) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
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
            }
            GqlMutationClausePlan::Merge(merge) => {
                materialize_gql_merge_clause(
                    plan,
                    params,
                    merge,
                    &mut rows,
                    txn,
                    snapshot,
                    edge_uniqueness,
                    default_valid_from,
                    &mut nodes,
                    &mut edges,
                    &mut existing_nodes,
                    &mut existing_edges,
                    &mut edge_precheck_triples,
                    &mut merge_overlay,
                    &mut merge_node_locals,
                    &mut merge_edge_locals,
                    &mut merge_db_hits,
                    &mut target_applications,
                    &mut skipped_null_targets,
                    &mut first_existing_node_update_order,
                    &mut first_existing_edge_update_order,
                    &mut seen_existing_node_updates,
                    &mut seen_existing_edge_updates,
                    &mut op_budget,
                )?;
            }
            GqlMutationClausePlan::Set(items) => {
                for row in rows.iter_mut() {
                    apply_gql_set_items(
                        plan,
                        params,
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
            }
            GqlMutationClausePlan::Remove(items) => {
                for row in rows.iter_mut() {
                    apply_gql_remove_items(
                        plan,
                        params,
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
            }
            GqlMutationClausePlan::Delete { .. } => {
                for row in rows.iter_mut() {
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
        .saturating_add(existing_edge_deletes.len())
        .saturating_add(merge_db_hits);
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
fn materialize_gql_merge_clause(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    merge: &GqlMergePlan,
    rows: &mut [GqlCreateExecutionRow],
    txn: &WriteTxn,
    snapshot: &ReadView,
    edge_uniqueness: bool,
    default_valid_from: i64,
    nodes: &mut Vec<GqlCreatedNodeExecution>,
    edges: &mut Vec<GqlCreatedEdgeExecution>,
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
    edge_precheck_triples: &mut BTreeSet<(u64, u64, String)>,
    merge_overlay: &mut TxnMergeOverlay,
    merge_node_locals: &mut BTreeMap<TxnMergeLocalNodeRef, usize>,
    merge_edge_locals: &mut BTreeMap<TxnMergeLocalEdgeRef, usize>,
    merge_db_hits: &mut usize,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    first_existing_node_update_order: &mut Vec<u64>,
    first_existing_edge_update_order: &mut Vec<u64>,
    seen_existing_node_updates: &mut BTreeSet<u64>,
    seen_existing_edge_updates: &mut BTreeSet<u64>,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<(), EngineError> {
    match &merge.pattern {
        GqlMergePatternPlan::Node { alias, label, key } => materialize_gql_node_merge(
            plan,
            params,
            alias,
            label,
            key,
            &merge.on_create,
            &merge.on_match,
            rows,
            txn,
            snapshot,
            nodes,
            edges,
            existing_nodes,
            existing_edges,
            merge_overlay,
            merge_node_locals,
            merge_db_hits,
            target_applications,
            skipped_null_targets,
            first_existing_node_update_order,
            first_existing_edge_update_order,
            seen_existing_node_updates,
            seen_existing_edge_updates,
            op_budget,
        ),
        GqlMergePatternPlan::Relationship {
            alias,
            from_alias,
            to_alias,
            label,
        } => materialize_gql_relationship_merge(
            plan,
            params,
            alias,
            from_alias,
            to_alias,
            label,
            &merge.on_create,
            &merge.on_match,
            rows,
            txn,
            snapshot,
            edge_uniqueness,
            default_valid_from,
            nodes,
            edges,
            existing_nodes,
            existing_edges,
            edge_precheck_triples,
            merge_overlay,
            merge_edge_locals,
            merge_db_hits,
            target_applications,
            skipped_null_targets,
            first_existing_node_update_order,
            first_existing_edge_update_order,
            seen_existing_node_updates,
            seen_existing_edge_updates,
            op_budget,
        ),
    }
}

#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
fn materialize_gql_node_merge(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    alias: &str,
    label: &str,
    key_ref: &GqlMutationExprRef,
    on_create: &[GqlSetItemPlan],
    on_match: &[GqlSetItemPlan],
    rows: &mut [GqlCreateExecutionRow],
    txn: &WriteTxn,
    snapshot: &ReadView,
    nodes: &mut Vec<GqlCreatedNodeExecution>,
    edges: &mut Vec<GqlCreatedEdgeExecution>,
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
    merge_overlay: &mut TxnMergeOverlay,
    merge_node_locals: &mut BTreeMap<TxnMergeLocalNodeRef, usize>,
    merge_db_hits: &mut usize,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    first_existing_node_update_order: &mut Vec<u64>,
    first_existing_edge_update_order: &mut Vec<u64>,
    seen_existing_node_updates: &mut BTreeSet<u64>,
    seen_existing_edge_updates: &mut BTreeSet<u64>,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<(), EngineError> {
    let mut keys = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let key = gql_merge_string_key(gql_create_expr_value(row, key_ref.id)?)?;
        keys.push((label.to_string(), key));
    }
    let batch = txn.plan_keyed_node_merge_batch(merge_overlay, &keys)?;
    let missing_existing_ids = batch
        .existing_ids
        .iter()
        .filter(|id| !existing_nodes.contains_key(id))
        .count();
    *merge_db_hits = (*merge_db_hits)
        .saturating_add(batch.snapshot_lookup_count)
        .saturating_add(missing_existing_ids);
    ensure_gql_existing_node_targets(snapshot, existing_nodes, &batch.existing_ids)?;
    op_budget.reserve(
        batch
            .rows
            .iter()
            .filter(|outcome| matches!(outcome, TxnKeyedNodeMergeRowOutcome::Create(_)))
            .count(),
    )?;

    for (row_index, (row, outcome)) in rows.iter_mut().zip(batch.rows).enumerate() {
        match outcome {
            TxnKeyedNodeMergeRowOutcome::Existing(id) => {
                row.created_nodes.remove(alias);
                row.read_nodes.insert(alias.to_string(), Some(id));
                apply_gql_set_items(
                    plan,
                    params,
                    on_match,
                    row,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?;
            }
            TxnKeyedNodeMergeRowOutcome::MatchedLocal(local) => {
                let node_index = *merge_node_locals.get(&local).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "GQL node MERGE local overlay target was not materialized".to_string(),
                    )
                })?;
                row.read_nodes.remove(alias);
                row.created_nodes.insert(alias.to_string(), node_index);
                apply_gql_set_items(
                    plan,
                    params,
                    on_match,
                    row,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?;
            }
            TxnKeyedNodeMergeRowOutcome::Create(local) => {
                let node_index = nodes.len();
                let (_, key) = &keys[row_index];
                let local_ref = TxnLocalRef::Alias(format!("__gql_merge_node_{row_index}_{alias}"));
                nodes.push(GqlCreatedNodeExecution {
                    local: local_ref,
                    labels: vec![label.to_string()],
                    key: key.clone(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                });
                merge_node_locals.insert(local, node_index);
                row.read_nodes.remove(alias);
                row.created_nodes.insert(alias.to_string(), node_index);
                row.created_node_writes.insert(node_index);
                row.produced_write = true;
                apply_gql_set_items(
                    plan,
                    params,
                    on_create,
                    row,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?;
            }
        }
    }
    Ok(())
}
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
fn materialize_gql_relationship_merge(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    alias: &str,
    from_alias: &str,
    to_alias: &str,
    label: &str,
    on_create: &[GqlSetItemPlan],
    on_match: &[GqlSetItemPlan],
    rows: &mut [GqlCreateExecutionRow],
    txn: &WriteTxn,
    snapshot: &ReadView,
    _edge_uniqueness: bool,
    default_valid_from: i64,
    nodes: &mut Vec<GqlCreatedNodeExecution>,
    edges: &mut Vec<GqlCreatedEdgeExecution>,
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
    edge_precheck_triples: &mut BTreeSet<(u64, u64, String)>,
    merge_overlay: &mut TxnMergeOverlay,
    merge_edge_locals: &mut BTreeMap<TxnMergeLocalEdgeRef, usize>,
    merge_db_hits: &mut usize,
    target_applications: &mut BTreeMap<GqlMutationTargetKey, usize>,
    skipped_null_targets: &mut usize,
    first_existing_node_update_order: &mut Vec<u64>,
    first_existing_edge_update_order: &mut Vec<u64>,
    seen_existing_node_updates: &mut BTreeSet<u64>,
    seen_existing_edge_updates: &mut BTreeSet<u64>,
    op_budget: &mut GqlMaterializationOpBudget,
) -> Result<(), EngineError> {
    let mut inputs = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let Some(from) = gql_create_node_ref_for_alias(row, from_alias, nodes)? else {
            inputs.push(None);
            continue;
        };
        let Some(to) = gql_create_node_ref_for_alias(row, to_alias, nodes)? else {
            inputs.push(None);
            continue;
        };
        inputs.push(Some(TxnUniqueEdgeMergeInput {
            from,
            to,
            label: label.to_string(),
        }));
    }

    let batch = txn.plan_unique_edge_merge_batch(merge_overlay, &inputs)?;
    let missing_existing_ids = batch
        .existing_ids
        .iter()
        .filter(|id| !existing_edges.contains_key(id))
        .count();
    *merge_db_hits = (*merge_db_hits)
        .saturating_add(batch.snapshot_lookup_count)
        .saturating_add(missing_existing_ids);
    ensure_gql_existing_edge_targets(snapshot, existing_edges, &batch.existing_ids)?;
    edge_precheck_triples.extend(batch.missing_committed_triples.iter().cloned());
    op_budget.reserve(
        batch
            .rows
            .iter()
            .filter(|outcome| matches!(outcome, TxnUniqueEdgeMergeRowOutcome::Create { .. }))
            .count(),
    )?;

    for (row_index, (row, outcome)) in rows.iter_mut().zip(batch.rows).enumerate() {
        match outcome {
            TxnUniqueEdgeMergeRowOutcome::SkippedNull => {
                *skipped_null_targets += 1;
                row.created_edges.remove(alias);
                row.read_edges.insert(alias.to_string(), None);
            }
            TxnUniqueEdgeMergeRowOutcome::Existing(id) => {
                row.created_edges.remove(alias);
                row.read_edges.insert(alias.to_string(), Some(id));
                apply_gql_set_items(
                    plan,
                    params,
                    on_match,
                    row,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?;
            }
            TxnUniqueEdgeMergeRowOutcome::MatchedLocal(local) => {
                let edge_index = *merge_edge_locals.get(&local).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "GQL relationship MERGE local overlay target was not materialized"
                            .to_string(),
                    )
                })?;
                row.read_edges.remove(alias);
                row.created_edges.insert(alias.to_string(), edge_index);
                apply_gql_set_items(
                    plan,
                    params,
                    on_match,
                    row,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?;
            }
            TxnUniqueEdgeMergeRowOutcome::Create {
                local,
                from,
                to,
                label,
            } => {
                let local_ref = TxnLocalRef::Alias(format!("__gql_merge_edge_{row_index}_{alias}"));
                let edge_index = edges.len();
                edges.push(GqlCreatedEdgeExecution {
                    alias: Some(alias.to_string()),
                    local: Some(local_ref),
                    from,
                    to,
                    label,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: Some(default_valid_from),
                    valid_to: Some(i64::MAX),
                });
                merge_edge_locals.insert(local, edge_index);
                row.read_edges.remove(alias);
                row.created_edges.insert(alias.to_string(), edge_index);
                row.created_edge_writes.insert(edge_index);
                row.produced_write = true;
                apply_gql_set_items(
                    plan,
                    params,
                    on_create,
                    row,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                    target_applications,
                    skipped_null_targets,
                    first_existing_node_update_order,
                    first_existing_edge_update_order,
                    seen_existing_node_updates,
                    seen_existing_edge_updates,
                )?;
            }
        }
    }
    Ok(())
}

fn ensure_gql_existing_node_targets(
    snapshot: &ReadView,
    existing_nodes: &mut BTreeMap<u64, GqlExistingNodeExecution>,
    node_ids: &BTreeSet<u64>,
) -> Result<(), EngineError> {
    let missing = node_ids
        .iter()
        .filter(|id| !existing_nodes.contains_key(id))
        .copied()
        .collect::<BTreeSet<_>>();
    let hydrated = hydrate_gql_existing_node_targets(snapshot, &missing)?;
    existing_nodes.extend(hydrated);
    Ok(())
}

fn ensure_gql_existing_edge_targets(
    snapshot: &ReadView,
    existing_edges: &mut BTreeMap<u64, GqlExistingEdgeExecution>,
    edge_ids: &BTreeSet<u64>,
) -> Result<(), EngineError> {
    let missing = edge_ids
        .iter()
        .filter(|id| !existing_edges.contains_key(id))
        .copied()
        .collect::<BTreeSet<_>>();
    let hydrated = hydrate_gql_existing_edge_targets(snapshot, &missing)?;
    existing_edges.extend(hydrated);
    Ok(())
}

fn gql_merge_string_key(value: &GraphValue) -> Result<String, EngineError> {
    match value {
        GraphValue::String(value) if !value.is_empty() => Ok(value.clone()),
        _ => Err(gql_create_invalid_value(
            "GQL MERGE node key must be a non-empty string",
        )),
    }
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
    seen_edge_triples: &mut BTreeSet<(TxnMergeEndpointKey, TxnMergeEndpointKey, String)>,
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
        let node_index = nodes.len();
        row.created_nodes.insert(node.alias.clone(), node_index);
        row.created_node_writes.insert(node_index);
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
        let edge_index = edges.len();
        row.created_edge_writes.insert(edge_index);
        if let (Some(alias), Some(_)) = (&created.alias, &created.local) {
            row.created_edges.insert(alias.clone(), edge_index);
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
                GqlMutationClausePlan::Create(_)
                | GqlMutationClausePlan::Merge(_)
                | GqlMutationClausePlan::Delete { .. } => {}
            }
        }
    }
    (nodes, edges)
}

fn collect_gql_late_expr_read_prefix_targets(
    plan: &GqlMutationPlan,
    rows: &[GqlCreateExecutionRow],
    nodes: &mut BTreeSet<u64>,
    edges: &mut BTreeSet<u64>,
) {
    let mut node_aliases = BTreeSet::new();
    let mut edge_aliases = BTreeSet::new();
    for expr in plan.operation_exprs.iter().filter(|expr| expr.late) {
        collect_gql_read_prefix_element_aliases_in_expr(
            plan,
            &expr.source,
            &mut node_aliases,
            &mut edge_aliases,
        );
    }
    for row in rows {
        for alias in &node_aliases {
            if let Some(Some(id)) = row.read_nodes.get(alias) {
                nodes.insert(*id);
            }
        }
        for alias in &edge_aliases {
            if let Some(Some(id)) = row.read_edges.get(alias) {
                edges.insert(*id);
            }
        }
    }
}

fn collect_gql_read_prefix_element_aliases_in_expr(
    plan: &GqlMutationPlan,
    expr: &Expr,
    node_aliases: &mut BTreeSet<String>,
    edge_aliases: &mut BTreeSet<String>,
) {
    match &expr.kind {
        ExprKind::Variable(name) => {
            if let Some(binding) = plan.semantic.aliases.get(name) {
                if binding.origin == GqlAliasOrigin::ReadPrefix
                    && matches!(binding.kind, GqlAliasKind::Node | GqlAliasKind::Edge)
                {
                    match binding.kind {
                        GqlAliasKind::Node => {
                            node_aliases.insert(name.clone());
                        }
                        GqlAliasKind::Edge => {
                            edge_aliases.insert(name.clone());
                        }
                        GqlAliasKind::Path | GqlAliasKind::Scalar => {}
                    }
                }
            }
        }
        ExprKind::PropertyAccess { object, .. }
        | ExprKind::Unary { expr: object, .. }
        | ExprKind::IsNull { expr: object, .. } => {
            collect_gql_read_prefix_element_aliases_in_expr(
                plan,
                object,
                node_aliases,
                edge_aliases,
            );
        }
        ExprKind::Binary { left, right, .. } => {
            collect_gql_read_prefix_element_aliases_in_expr(
                plan,
                left,
                node_aliases,
                edge_aliases,
            );
            collect_gql_read_prefix_element_aliases_in_expr(
                plan,
                right,
                node_aliases,
                edge_aliases,
            );
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    arg,
                    node_aliases,
                    edge_aliases,
                );
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    arg,
                    node_aliases,
                    edge_aliases,
                );
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    operand,
                    node_aliases,
                    edge_aliases,
                );
            }
            for branch in branches {
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    &branch.when,
                    node_aliases,
                    edge_aliases,
                );
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    &branch.then,
                    node_aliases,
                    edge_aliases,
                );
            }
            if let Some(else_expr) = else_expr.as_ref() {
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    else_expr,
                    node_aliases,
                    edge_aliases,
                );
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_gql_read_prefix_element_aliases_in_expr(
                    plan,
                    &entry.value,
                    node_aliases,
                    edge_aliases,
                );
            }
        }
        ExprKind::ExistsSubquery(_)
        | ExprKind::Literal(_)
        | ExprKind::Parameter(_) => {}
    }
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
        GqlAliasKind::Path | GqlAliasKind::Scalar => {}
    }
}

fn gql_alias_kind_name(kind: GqlAliasKind) -> &'static str {
    match kind {
        GqlAliasKind::Node => "node",
        GqlAliasKind::Edge => "edge",
        GqlAliasKind::Path => "path",
        GqlAliasKind::Scalar => "scalar",
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
        GqlAliasKind::Path | GqlAliasKind::Scalar => Err(EngineError::InvalidOperation(
            format!("{} aliases are not mutation targets", gql_alias_kind_name(kind)),
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
    params: &GqlParams,
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
                let value = gql_mutation_set_expr_value(
                    plan,
                    params,
                    row,
                    value,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )?;
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
                    target.clone(),
                    property,
                    &value,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )? {
                    mark_gql_touched_created_target(row, &target);
                    row.produced_write = true;
                }
            }
            GqlSetItemPlan::MapMerge { alias, kind, value } => {
                let value = gql_mutation_set_expr_value(
                    plan,
                    params,
                    row,
                    value,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )?;
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
                    target.clone(),
                    &value,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )? {
                    mark_gql_touched_created_target(row, &target);
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
                if apply_gql_add_node_label(target.clone(), label, nodes, existing_nodes)? {
                    mark_gql_touched_created_target(row, &target);
                    row.produced_write = true;
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_gql_remove_items(
    _plan: &GqlMutationPlan,
    _params: &GqlParams,
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
                    target.clone(),
                    property,
                    nodes,
                    edges,
                    existing_nodes,
                    existing_edges,
                )? {
                    mark_gql_touched_created_target(row, &target);
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
                if apply_gql_remove_node_label(target.clone(), label, nodes, existing_nodes)? {
                    mark_gql_touched_created_target(row, &target);
                    row.produced_write = true;
                }
            }
        }
    }
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
        GqlAliasKind::Scalar => {
            return Err(EngineError::InvalidOperation(
                "scalar aliases are not mutation targets".to_string(),
            ));
        }
    };
    *target_applications.entry(target.clone()).or_default() += 1;
    Ok(Some(target))
}

fn mark_gql_touched_created_target(
    row: &mut GqlCreateExecutionRow,
    target: &GqlMutationTargetKey,
) {
    match target {
        GqlMutationTargetKey::CreatedNode(index) => {
            row.touched_created_nodes.insert(*index);
        }
        GqlMutationTargetKey::CreatedEdge(index) => {
            row.touched_created_edges.insert(*index);
        }
        GqlMutationTargetKey::ExistingNode(_) | GqlMutationTargetKey::ExistingEdge(_) => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn gql_mutation_set_expr_value(
    plan: &GqlMutationPlan,
    params: &GqlParams,
    row: &GqlCreateExecutionRow,
    expr_ref: &GqlMutationExprRef,
    nodes: &[GqlCreatedNodeExecution],
    edges: &[GqlCreatedEdgeExecution],
    existing_nodes: &BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &BTreeMap<u64, GqlExistingEdgeExecution>,
) -> Result<GraphValue, EngineError> {
    let expr_plan = plan.operation_exprs.get(expr_ref.id).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "GQL mutation expression ref #{} is missing from the execution plan",
            expr_ref.id
        ))
    })?;
    if !expr_plan.late {
        return gql_create_expr_value(row, expr_ref.id).cloned();
    }
    let hydrated = GqlMutationHydratedRecords::default();
    let context = GqlMutationReturnEvalContext {
        plan,
        row,
        nodes,
        edges,
        existing_nodes,
        existing_edges,
        commit: None,
        hydrated: &hydrated,
        include_vectors: false,
        path_id_only: false,
    };
    let value = gql_mutation_return_expr_value(&expr_plan.source, params, &context)?;
    let graph_value = gql_value_to_graph_eval_scalar(value)?.ok_or_else(|| {
            gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "GQL MERGE action expression must produce a scalar, list, map, or null value"
                    .to_string(),
                expr_plan.source.span.clone(),
            )
        })?;
    graph_eval_value_to_graph_value(graph_value)
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
            GqlAliasKind::Path | GqlAliasKind::Scalar => true,
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
    row.created_node_writes
        .iter()
        .any(|&index| nodes.get(index).is_some() && !created_node_deletes.contains(&index))
        || row
            .created_edge_writes
            .iter()
            .any(|&index| edges.get(index).is_some() && !created_edge_deletes.contains(&index))
        || row
            .touched_created_nodes
            .iter()
            .any(|&index| nodes.get(index).is_some() && !created_node_deletes.contains(&index))
        || row
            .touched_created_edges
            .iter()
            .any(|&index| edges.get(index).is_some() && !created_edge_deletes.contains(&index))
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
    distinct: bool,
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
    selected_rows: Vec<usize>,
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
        distinct: return_plan.distinct,
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
    let candidate_count =
        gql_mutation_return_count_after_row_ops(materialized.rows.len(), &static_plan);
    if !static_plan.distinct && candidate_count > options.max_rows {
        return Err(gql_mutation_cap_error(
            "max_rows",
            candidate_count,
            options.max_rows,
        ));
    }
    validate_gql_mutation_order_exprs_materialized(plan, &static_plan.order_by, materialized)?;
    if static_plan.distinct {
        validate_gql_mutation_return_distinct_exprs_static(plan, &static_plan.exprs)?;
        validate_gql_mutation_return_distinct_exprs_materialized(
            plan,
            &static_plan.exprs,
            materialized,
        )?;
    }

    let ordered_precommit = build_gql_mutation_ordered_rows_precommit(
        plan,
        &static_plan,
        params,
        materialized,
        snapshot,
        options,
    )?;
    let candidate_rows =
        selected_gql_mutation_return_rows(&ordered_precommit.rows, static_plan.skip, static_plan.limit);
    let mut candidate_hydration_needs = GqlMutationReturnHydrationNeeds::default();
    let mut read_set = ordered_precommit.read_set;
    for item in &static_plan.exprs {
        collect_gql_mutation_return_expr_ids(
            plan,
            materialized,
            &item.expr,
            &candidate_rows,
            GqlMutationReturnUse::Output,
            None,
            &mut candidate_hydration_needs,
            &mut read_set,
        );
    }
    let hydrated = hydrate_gql_mutation_return_records(snapshot, &candidate_hydration_needs)?;
    validate_gql_mutation_return_output_values_precommit(
        plan,
        &static_plan,
        params,
        materialized,
        &candidate_rows,
        &hydrated,
        options,
    )?;
    let selected_rows = if static_plan.distinct {
        select_distinct_gql_mutation_return_rows_precommit(
            plan,
            &static_plan,
            params,
            materialized,
            &candidate_rows,
            &hydrated,
            options,
        )?
    } else {
        candidate_rows
    };
    if selected_rows.len() > options.max_rows {
        return Err(gql_mutation_cap_error(
            "max_rows",
            selected_rows.len(),
            options.max_rows,
        ));
    }
    let output_hydration_needs = if static_plan.distinct {
        let mut needs = GqlMutationReturnHydrationNeeds::default();
        let mut selected_read_set = TxnReturnReadSet::default();
        for item in &static_plan.exprs {
            collect_gql_mutation_return_expr_ids(
                plan,
                materialized,
                &item.expr,
                &selected_rows,
                GqlMutationReturnUse::Output,
                None,
                &mut needs,
                &mut selected_read_set,
            );
        }
        read_set.node_ids.extend(selected_read_set.node_ids);
        read_set.edge_ids.extend(selected_read_set.edge_ids);
        needs
    } else {
        candidate_hydration_needs
    };
    Ok(Some(GqlMutationReturnExecutionPlan {
        static_plan,
        selected_rows,
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
                nodes: &materialized.nodes,
                edges: &materialized.edges,
                existing_nodes: &materialized.existing_nodes,
                existing_edges: &materialized.existing_edges,
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

fn gql_mutation_return_needs_committed_view(
    return_execution: &GqlMutationReturnExecutionPlan,
) -> bool {
    !return_execution.selected_rows.is_empty()
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
            nodes: &materialized.nodes,
            edges: &materialized.edges,
            existing_nodes: &materialized.existing_nodes,
            existing_edges: &materialized.existing_edges,
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

fn select_distinct_gql_mutation_return_rows_precommit(
    plan: &GqlMutationPlan,
    static_plan: &GqlMutationReturnStaticPlan,
    params: &GqlParams,
    materialized: &GqlCreateMaterialization,
    candidate_rows: &[usize],
    hydrated: &GqlMutationHydratedRecords,
    options: &GqlExecutionOptions,
) -> Result<Vec<usize>, EngineError> {
    let mut seen = BTreeSet::new();
    let mut selected = Vec::with_capacity(candidate_rows.len());
    for &row_index in candidate_rows {
        let row = materialized.rows.get(row_index).ok_or_else(|| {
            EngineError::InvalidOperation(
                "GQL mutation RETURN selected row index is out of bounds".to_string(),
            )
        })?;
        let context = GqlMutationReturnEvalContext {
            plan,
            row,
            nodes: &materialized.nodes,
            edges: &materialized.edges,
            existing_nodes: &materialized.existing_nodes,
            existing_edges: &materialized.existing_edges,
            commit: None,
            hydrated,
            include_vectors: options.include_vectors,
            path_id_only: false,
        };
        let key = static_plan
            .exprs
            .iter()
            .map(|item| gql_mutation_return_distinct_key_for_expr(&item.expr, params, &context))
            .collect::<Result<Vec<_>, _>>()?;
        if !seen.contains(&key) && seen.len() >= options.max_groups {
            return Err(gql_mutation_cap_error(
                "max_groups",
                seen.len().saturating_add(1),
                options.max_groups,
            ));
        }
        if seen.insert(key) {
            selected.push(row_index);
        }
    }
    Ok(selected)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GqlMutationReturnEntityDistinctKey {
    Existing(u64),
    Created(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GqlMutationReturnDistinctKey {
    Scalar(GraphCanonicalKey),
    Node(GqlMutationReturnEntityDistinctKey),
    Edge(GqlMutationReturnEntityDistinctKey),
    Path {
        nodes: Vec<GqlMutationReturnEntityDistinctKey>,
        edges: Vec<GqlMutationReturnEntityDistinctKey>,
    },
    List(Vec<GqlMutationReturnDistinctKey>),
    Map(Vec<(String, GqlMutationReturnDistinctKey)>),
}

fn gql_mutation_return_distinct_key_for_expr(
    expr: &Expr,
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<GqlMutationReturnDistinctKey, EngineError> {
    match &expr.kind {
        ExprKind::Literal(literal) => {
            gql_value_to_mutation_return_distinct_key(&gql_literal_to_value(literal), &expr.span)
        }
        ExprKind::Parameter(name) => {
            let value = params
                .get(name)
                .map(gql_param_to_value)
                .ok_or_else(|| EngineError::GqlParameter {
                    name: name.clone(),
                    expected: "GqlParamValue".to_string(),
                    message: format!("missing parameter '${name}'"),
                    span: expr.span.clone(),
                })?;
            gql_value_to_mutation_return_distinct_key(&value, &expr.span)
        }
        ExprKind::Variable(alias) => {
            gql_mutation_alias_distinct_key(alias, context, &expr.span)
        }
        ExprKind::List(items) => Ok(GqlMutationReturnDistinctKey::List(
            items
                .iter()
                .map(|item| gql_mutation_return_distinct_key_for_expr(item, params, context))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ExprKind::Map(map) => Ok(GqlMutationReturnDistinctKey::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        entry.key.name.clone(),
                        gql_mutation_return_distinct_key_for_expr(&entry.value, params, context)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?
                .into_iter()
                .collect(),
        )),
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Map(map) = &object.kind {
                if let Some(entry) = map
                    .entries
                    .iter()
                    .find(|entry| entry.key.name == property.name)
                {
                    return gql_mutation_return_distinct_key_for_expr(
                        &entry.value,
                        params,
                        context,
                    );
                }
                return Ok(GqlMutationReturnDistinctKey::Scalar(GraphCanonicalKey::Null));
            }
            let value = gql_mutation_return_expr_value(expr, params, context)?;
            gql_value_to_mutation_return_distinct_key(&value, &expr.span)
        }
        ExprKind::FunctionCall { name, args } => {
            if let Some(key) =
                gql_mutation_graph_function_distinct_key(&name.name, args, context, &expr.span)?
            {
                return Ok(key);
            }
            let value = gql_mutation_return_expr_value(expr, params, context)?;
            gql_value_to_mutation_return_distinct_key(&value, &expr.span)
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => gql_mutation_case_distinct_key(
            operand.as_deref(),
            branches,
            else_expr.as_deref(),
            params,
            context,
            &expr.span,
        ),
        ExprKind::Unary { .. } | ExprKind::Binary { .. } | ExprKind::IsNull { .. } => {
            let value = gql_mutation_return_expr_value(expr, params, context)?;
            gql_value_to_mutation_return_distinct_key(&value, &expr.span)
        }
        ExprKind::AggregateCall { name_span, .. } => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "aggregate functions are not supported in mutation RETURN".to_string(),
            name_span.clone(),
        )),
        ExprKind::ExistsSubquery(_) => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "GQL mutation RETURN does not support subquery expressions".to_string(),
            expr.span.clone(),
        )),
    }
}

fn gql_mutation_alias_distinct_key(
    alias: &str,
    context: &GqlMutationReturnEvalContext<'_>,
    span: &SourceSpan,
) -> Result<GqlMutationReturnDistinctKey, EngineError> {
    let Some(binding) = context.plan.semantic.aliases.get(alias) else {
        return Ok(GqlMutationReturnDistinctKey::Scalar(GraphCanonicalKey::Null));
    };
    match binding.kind {
        GqlAliasKind::Node => {
            if let Some(&index) = context.row.created_nodes.get(alias) {
                return Ok(GqlMutationReturnDistinctKey::Node(
                    GqlMutationReturnEntityDistinctKey::Created(index),
                ));
            }
            Ok(context
                .node_id(alias)
                .map(|id| {
                    GqlMutationReturnDistinctKey::Node(
                        GqlMutationReturnEntityDistinctKey::Existing(id),
                    )
                })
                .unwrap_or(GqlMutationReturnDistinctKey::Scalar(
                    GraphCanonicalKey::Null,
                )))
        }
        GqlAliasKind::Edge => {
            if let Some(&index) = context.row.created_edges.get(alias) {
                return Ok(GqlMutationReturnDistinctKey::Edge(
                    GqlMutationReturnEntityDistinctKey::Created(index),
                ));
            }
            Ok(context
                .edge_id(alias)
                .map(|id| {
                    GqlMutationReturnDistinctKey::Edge(
                        GqlMutationReturnEntityDistinctKey::Existing(id),
                    )
                })
                .unwrap_or(GqlMutationReturnDistinctKey::Scalar(
                    GraphCanonicalKey::Null,
                )))
        }
        GqlAliasKind::Path => Ok(context
            .path(alias)
            .map(gql_path_identity_distinct_key)
            .unwrap_or(GqlMutationReturnDistinctKey::Scalar(
                GraphCanonicalKey::Null,
            ))),
        GqlAliasKind::Scalar => {
            let value = row_scalar_value(alias, context)?;
            gql_value_to_mutation_return_distinct_key(&value, span)
        }
    }
}

fn gql_path_identity_distinct_key(path: &GqlPathIdentity) -> GqlMutationReturnDistinctKey {
    GqlMutationReturnDistinctKey::Path {
        nodes: path
            .node_ids
            .iter()
            .copied()
            .map(GqlMutationReturnEntityDistinctKey::Existing)
            .collect(),
        edges: path
            .edge_ids
            .iter()
            .copied()
            .map(GqlMutationReturnEntityDistinctKey::Existing)
            .collect(),
    }
}

fn gql_mutation_graph_function_distinct_key(
    function: &str,
    args: &[Expr],
    context: &GqlMutationReturnEvalContext<'_>,
    _span: &SourceSpan,
) -> Result<Option<GqlMutationReturnDistinctKey>, EngineError> {
    let Some(Expr {
        kind: ExprKind::Variable(alias),
        ..
    }) = args.first()
    else {
        return Ok(None);
    };
    let lower = function.to_ascii_lowercase();
    let Some(binding) = context.plan.semantic.aliases.get(alias) else {
        return Ok(Some(GqlMutationReturnDistinctKey::Scalar(
            GraphCanonicalKey::Null,
        )));
    };
    if binding.kind != GqlAliasKind::Path {
        return Ok(None);
    }
    let Some(path) = context.path(alias) else {
        return Ok(Some(GqlMutationReturnDistinctKey::Scalar(
            GraphCanonicalKey::Null,
        )));
    };
    let key = match lower.as_str() {
        "start_node" => path.node_ids.first().copied().map(|id| {
            GqlMutationReturnDistinctKey::Node(GqlMutationReturnEntityDistinctKey::Existing(id))
        }),
        "end_node" => path.node_ids.last().copied().map(|id| {
            GqlMutationReturnDistinctKey::Node(GqlMutationReturnEntityDistinctKey::Existing(id))
        }),
        "nodes" => Some(GqlMutationReturnDistinctKey::List(
            path.node_ids
                .iter()
                .copied()
                .map(|id| {
                    GqlMutationReturnDistinctKey::Node(
                        GqlMutationReturnEntityDistinctKey::Existing(id),
                    )
                })
                .collect(),
        )),
        "relationships" => Some(GqlMutationReturnDistinctKey::List(
            path.edge_ids
                .iter()
                .copied()
                .map(|id| {
                    GqlMutationReturnDistinctKey::Edge(
                        GqlMutationReturnEntityDistinctKey::Existing(id),
                    )
                })
                .collect(),
        )),
        _ => return Ok(None),
    };
    Ok(Some(key.unwrap_or({
        GqlMutationReturnDistinctKey::Scalar(GraphCanonicalKey::Null)
    })))
}

fn gql_mutation_case_distinct_key(
    operand: Option<&Expr>,
    branches: &[crate::gql::ast::CaseBranch],
    else_expr: Option<&Expr>,
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
    span: &SourceSpan,
) -> Result<GqlMutationReturnDistinctKey, EngineError> {
    if let Some(operand) = operand {
        let operand_value = gql_mutation_return_expr_value(operand, params, context)?;
        for branch in branches {
            let when_value = gql_mutation_return_expr_value(&branch.when, params, context)?;
            if let Some(value) =
                gql_mutation_try_eval_shared_binary(BinaryOp::Eq, &operand_value, &when_value, span)?
            {
                match value {
                    GqlValue::Bool(true) => {
                        return gql_mutation_return_distinct_key_for_expr(
                            &branch.then,
                            params,
                            context,
                        );
                    }
                    GqlValue::Bool(false) | GqlValue::Null => {}
                    _ => unreachable!("equality returns bool or null"),
                }
            } else if matches!(
                gql_mutation_compare_values(BinaryOp::Eq, operand_value.clone(), when_value),
                GqlValue::Bool(true)
            ) {
                return gql_mutation_return_distinct_key_for_expr(
                    &branch.then,
                    params,
                    context,
                );
            }
        }
    } else {
        for branch in branches {
            if let Some(true) = gql_mutation_bool_or_null(&branch.when, params, context)? {
                return gql_mutation_return_distinct_key_for_expr(
                    &branch.then,
                    params,
                    context,
                );
            }
        }
    }
    else_expr
        .map(|expr| gql_mutation_return_distinct_key_for_expr(expr, params, context))
        .unwrap_or_else(|| Ok(GqlMutationReturnDistinctKey::Scalar(GraphCanonicalKey::Null)))
}

fn gql_value_to_mutation_return_distinct_key(
    value: &GqlValue,
    span: &SourceSpan,
) -> Result<GqlMutationReturnDistinctKey, EngineError> {
    Ok(match value {
        GqlValue::Null
        | GqlValue::Bool(_)
        | GqlValue::Int(_)
        | GqlValue::UInt(_)
        | GqlValue::Float(_)
        | GqlValue::String(_)
        | GqlValue::Bytes(_) => {
            let graph_value = gql_value_ref_to_graph_eval_scalar(value)?.ok_or_else(|| {
                gql_distinct_key_error("GQL mutation RETURN DISTINCT scalar key is invalid", span)
            })?;
            GqlMutationReturnDistinctKey::Scalar(graph_canonical_key_for_value(&graph_value)?)
        }
        GqlValue::List(values) => GqlMutationReturnDistinctKey::List(
            values
                .iter()
                .map(|value| gql_value_to_mutation_return_distinct_key(value, span))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GqlValue::Map(values) => GqlMutationReturnDistinctKey::Map(
            values
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        gql_value_to_mutation_return_distinct_key(value, span)?,
                    ))
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
        ),
        GqlValue::Node(node) => GqlMutationReturnDistinctKey::Node(
            GqlMutationReturnEntityDistinctKey::Existing(node.id.ok_or_else(|| {
                gql_distinct_key_error(
                    "GQL mutation RETURN DISTINCT requires a precommit node identity",
                    span,
                )
            })?),
        ),
        GqlValue::Edge(edge) => GqlMutationReturnDistinctKey::Edge(
            GqlMutationReturnEntityDistinctKey::Existing(edge.id.ok_or_else(|| {
                gql_distinct_key_error(
                    "GQL mutation RETURN DISTINCT requires a precommit edge identity",
                    span,
                )
            })?),
        ),
        GqlValue::Path(path) => GqlMutationReturnDistinctKey::Path {
            nodes: path
                .node_ids
                .iter()
                .copied()
                .map(GqlMutationReturnEntityDistinctKey::Existing)
                .collect(),
            edges: path
                .edge_ids
                .iter()
                .copied()
                .map(GqlMutationReturnEntityDistinctKey::Existing)
                .collect(),
        },
    })
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
    let selected = return_execution.selected_rows.clone();
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
                nodes: &materialized.nodes,
                edges: &materialized.edges,
                existing_nodes: &materialized.existing_nodes,
                existing_edges: &materialized.existing_edges,
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
        .collect::<Result<Vec<_>, EngineError>>()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GqlMutationReturnUse {
    Output,
    Order,
}

struct GqlMutationReturnEvalContext<'a> {
    plan: &'a GqlMutationPlan,
    row: &'a GqlCreateExecutionRow,
    nodes: &'a [GqlCreatedNodeExecution],
    edges: &'a [GqlCreatedEdgeExecution],
    existing_nodes: &'a BTreeMap<u64, GqlExistingNodeExecution>,
    existing_edges: &'a BTreeMap<u64, GqlExistingEdgeExecution>,
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
        ExprKind::AggregateCall { name_span, .. } => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "aggregate functions are not supported in mutation RETURN".to_string(),
                name_span.clone(),
            ));
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => ExprKind::Case {
            operand: operand
                .as_ref()
                .map(|operand| {
                    resolve_mutation_return_aliases_in_expr(operand, return_aliases, plan)
                        .map(Box::new)
                })
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(crate::gql::ast::CaseBranch {
                        when: resolve_mutation_return_aliases_in_expr(
                            &branch.when,
                            return_aliases,
                            plan,
                        )?,
                        then: resolve_mutation_return_aliases_in_expr(
                            &branch.then,
                            return_aliases,
                            plan,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| {
                    resolve_mutation_return_aliases_in_expr(else_expr, return_aliases, plan)
                        .map(Box::new)
                })
                .transpose()?,
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
        ExprKind::ExistsSubquery(_) => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "GQL mutation RETURN does not support subquery expressions".to_string(),
                expr.span.clone(),
            ))
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
        validate_gql_mutation_return_commit_dependent_metadata_static(plan, &expr.expr, true)?;
    }
    Ok(())
}

fn validate_gql_mutation_return_distinct_exprs_static(
    plan: &GqlMutationPlan,
    exprs: &[GqlReturnExpr],
) -> Result<(), EngineError> {
    for expr in exprs {
        validate_gql_mutation_return_commit_dependent_metadata_static(
            plan, &expr.expr, false,
        )?;
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
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_gql_mutation_return_expr_static(plan, operand)?;
            }
            for branch in branches {
                validate_gql_mutation_return_expr_static(plan, &branch.when)?;
                validate_gql_mutation_return_expr_static(plan, &branch.then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_gql_mutation_return_expr_static(plan, else_expr)?;
            }
        }
        ExprKind::FunctionCall { name, args } => {
            validate_gql_mutation_return_function_static(plan, &name.name, args, &expr.span)?
        }
        ExprKind::AggregateCall { name_span, .. } => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "aggregate functions are not supported in mutation RETURN".to_string(),
                name_span.clone(),
            ));
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
        ExprKind::ExistsSubquery(_) => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "GQL mutation RETURN does not support subquery expressions".to_string(),
                expr.span.clone(),
            ));
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
    let lower = function.to_ascii_lowercase();
    if gql_scalar_function_name(&lower).is_some() {
        validate_gql_scalar_function_arity(&lower, function, args.len(), span)?;
        for arg in args {
            validate_gql_mutation_return_expr_static(plan, arg)?;
        }
        return Ok(());
    }
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
    let valid = match lower.as_str() {
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
            format!("function '{}' received an unsupported alias kind", lower),
            span.clone(),
        ))
    }
}

fn validate_gql_mutation_return_commit_dependent_metadata_static(
    plan: &GqlMutationPlan,
    expr: &Expr,
    allow_direct_output: bool,
) -> Result<(), EngineError> {
    if gql_mutation_return_expr_is_commit_dependent_created_metadata(plan, expr) {
        if allow_direct_output {
            return Ok(());
        }
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "commit-assigned created alias metadata cannot be used inside rich mutation RETURN expressions".to_string(),
            expr.span.clone(),
        ));
    }

    match &expr.kind {
        ExprKind::PropertyAccess { object, .. } => {
            validate_gql_mutation_return_commit_dependent_metadata_static(plan, object, false)
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            validate_gql_mutation_return_commit_dependent_metadata_static(plan, expr, false)
        }
        ExprKind::Binary { left, right, .. } => {
            validate_gql_mutation_return_commit_dependent_metadata_static(plan, left, false)?;
            validate_gql_mutation_return_commit_dependent_metadata_static(plan, right, false)
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_gql_mutation_return_commit_dependent_metadata_static(plan, operand, false)?;
            }
            for branch in branches {
                validate_gql_mutation_return_commit_dependent_metadata_static(
                    plan,
                    &branch.when,
                    false,
                )?;
                validate_gql_mutation_return_commit_dependent_metadata_static(
                    plan,
                    &branch.then,
                    false,
                )?;
            }
            if let Some(else_expr) = else_expr {
                validate_gql_mutation_return_commit_dependent_metadata_static(
                    plan, else_expr, false,
                )?;
            }
            Ok(())
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                validate_gql_mutation_return_commit_dependent_metadata_static(plan, arg, false)?;
            }
            Ok(())
        }
        ExprKind::AggregateCall { name_span, .. } => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "aggregate functions are not supported in mutation RETURN".to_string(),
            name_span.clone(),
        )),
        ExprKind::List(items) => {
            for item in items {
                validate_gql_mutation_return_commit_dependent_metadata_static(plan, item, false)?;
            }
            Ok(())
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                validate_gql_mutation_return_commit_dependent_metadata_static(
                    plan,
                    &entry.value,
                    false,
                )?;
            }
            Ok(())
        }
        ExprKind::ExistsSubquery(_) => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "GQL mutation RETURN does not support subquery expressions".to_string(),
            expr.span.clone(),
        )),
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => Ok(()),
    }
}

fn gql_mutation_return_expr_is_commit_dependent_created_metadata(
    plan: &GqlMutationPlan,
    expr: &Expr,
) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args } if name.name.eq_ignore_ascii_case("id") => {
            matches!(
                args.as_slice(),
                [Expr {
                    kind: ExprKind::Variable(alias),
                    ..
                }] if gql_mutation_created_alias_kind(plan, alias)
                    .is_some_and(|kind| matches!(kind, GqlAliasKind::Node | GqlAliasKind::Edge))
            )
        }
        ExprKind::PropertyAccess { object, property } => {
            let ExprKind::Variable(alias) = &object.kind else {
                return false;
            };
            match gql_mutation_created_alias_kind(plan, alias) {
                Some(GqlAliasKind::Node) => matches!(
                    property.name.as_str(),
                    "id" | "created_at" | "updated_at"
                ),
                Some(GqlAliasKind::Edge) => matches!(
                    property.name.as_str(),
                    "id" | "from" | "to" | "created_at" | "updated_at"
                ),
                Some(GqlAliasKind::Path | GqlAliasKind::Scalar) | None => false,
            }
        }
        _ => false,
    }
}

fn gql_mutation_created_alias_kind(plan: &GqlMutationPlan, alias: &str) -> Option<GqlAliasKind> {
    plan.semantic
        .aliases
        .get(alias)
        .filter(|binding| matches!(binding.origin, GqlAliasOrigin::Created | GqlAliasOrigin::Merged))
        .map(|binding| binding.kind)
}

fn validate_gql_scalar_function_arity(
    lower: &str,
    display: &str,
    arg_count: usize,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    let valid = match lower {
        "coalesce" => arg_count >= 1,
        "substring" => matches!(arg_count, 2 | 3),
        "to_string" | "to_integer" | "to_float" | "abs" | "floor" | "ceil" | "round"
        | "lower" | "upper" | "trim" | "size" | "head" | "last" => arg_count == 1,
        _ => false,
    };
    if valid {
        return Ok(());
    }
    let expected = match lower {
        "coalesce" => "at least one argument",
        "substring" => "two or three arguments",
        _ => "exactly one argument",
    };
    Err(gql_semantic_error(
        GqlSemanticErrorCode::InvalidReturnExpression,
        format!("function '{display}' expects {expected}"),
        span.clone(),
    ))
}

fn gql_scalar_function_name(lower: &str) -> Option<GraphFunction> {
    match lower {
        "coalesce" => Some(GraphFunction::Coalesce),
        "to_string" => Some(GraphFunction::ToString),
        "to_integer" => Some(GraphFunction::ToInteger),
        "to_float" => Some(GraphFunction::ToFloat),
        "abs" => Some(GraphFunction::Abs),
        "floor" => Some(GraphFunction::Floor),
        "ceil" => Some(GraphFunction::Ceil),
        "round" => Some(GraphFunction::Round),
        "lower" => Some(GraphFunction::Lower),
        "upper" => Some(GraphFunction::Upper),
        "trim" => Some(GraphFunction::Trim),
        "substring" => Some(GraphFunction::Substring),
        "size" => Some(GraphFunction::Size),
        "head" => Some(GraphFunction::Head),
        "last" => Some(GraphFunction::Last),
        _ => None,
    }
}

fn validate_gql_mutation_order_exprs_static(
    plan: &GqlMutationPlan,
    order_by: &[GqlMutationResolvedOrderItem],
) -> Result<(), EngineError> {
    for item in order_by {
        validate_gql_mutation_order_expr_static(plan, &item.expr, &item.span)?;
        validate_gql_mutation_return_commit_dependent_metadata_static(plan, &item.expr, false)
            .map_err(|err| match err {
                EngineError::GqlSemantic { .. }
                    if gql_mutation_return_expr_is_commit_dependent_created_metadata(
                        plan, &item.expr,
                    ) =>
                {
                    gql_order_key_error(&item.span)
                }
                other => other,
            })?;
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
            Some(GqlAliasKind::Scalar) => Ok(()),
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
        ExprKind::AggregateCall { name_span, .. } => Err(gql_order_key_error(name_span)),
        ExprKind::ExistsSubquery(_) => Err(gql_order_key_error(span)),
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
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_gql_mutation_order_expr_static(plan, operand, span)?;
            }
            for branch in branches {
                validate_gql_mutation_order_expr_static(plan, &branch.when, span)?;
                validate_gql_mutation_order_expr_static(plan, &branch.then, span)?;
            }
            if let Some(else_expr) = else_expr {
                validate_gql_mutation_order_expr_static(plan, else_expr, span)?;
            }
            Ok(())
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

fn validate_gql_mutation_return_distinct_exprs_materialized(
    plan: &GqlMutationPlan,
    exprs: &[GqlReturnExpr],
    materialized: &GqlCreateMaterialization,
) -> Result<(), EngineError> {
    for expr in exprs {
        validate_gql_mutation_return_distinct_expr_materialized(
            plan,
            materialized,
            &expr.expr,
        )?;
    }
    Ok(())
}

fn validate_gql_mutation_return_distinct_expr_materialized(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    expr: &Expr,
) -> Result<(), EngineError> {
    match &expr.kind {
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                validate_gql_mutation_return_distinct_alias_property_materialized(
                    plan,
                    materialized,
                    alias,
                    &property.name,
                    &expr.span,
                )?;
            } else {
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    object,
                )?;
            }
            Ok(())
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    arg,
                )?;
            }
            Ok(())
        }
        ExprKind::AggregateCall { name_span, .. } => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "aggregate functions are not supported in mutation RETURN".to_string(),
            name_span.clone(),
        )),
        ExprKind::ExistsSubquery(_) => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "GQL mutation RETURN does not support subquery expressions".to_string(),
            expr.span.clone(),
        )),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            validate_gql_mutation_return_distinct_expr_materialized(plan, materialized, expr)
        }
        ExprKind::Binary { left, right, .. } => {
            validate_gql_mutation_return_distinct_expr_materialized(plan, materialized, left)?;
            validate_gql_mutation_return_distinct_expr_materialized(plan, materialized, right)
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    operand,
                )?;
            }
            for branch in branches {
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    &branch.when,
                )?;
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    &branch.then,
                )?;
            }
            if let Some(else_expr) = else_expr {
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    else_expr,
                )?;
            }
            Ok(())
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                validate_gql_mutation_return_distinct_expr_materialized(
                    plan,
                    materialized,
                    &entry.value,
                )?;
            }
            Ok(())
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => Ok(()),
    }
}

fn validate_gql_mutation_return_distinct_alias_property_materialized(
    plan: &GqlMutationPlan,
    materialized: &GqlCreateMaterialization,
    alias: &str,
    property: &str,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    let Some(binding) = plan.semantic.aliases.get(alias) else {
        return Ok(());
    };
    if property == "updated_at"
        && matches!(binding.origin, GqlAliasOrigin::ReadPrefix | GqlAliasOrigin::Merged)
        && gql_mutation_order_alias_has_changed_target(materialized, alias, binding.kind)
    {
        return Err(gql_distinct_key_error(
            "GQL mutation RETURN DISTINCT cannot use same-mutation updated_at metadata",
            span,
        ));
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
                        matches!(binding.origin, GqlAliasOrigin::Created | GqlAliasOrigin::Merged)
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
        ExprKind::AggregateCall { name_span, .. } => Err(gql_order_key_error(name_span)),
        ExprKind::ExistsSubquery(_) => Err(gql_order_key_error(span)),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            validate_gql_mutation_order_expr_materialized(plan, materialized, expr, span)
        }
        ExprKind::Binary { left, right, .. } => {
            validate_gql_mutation_order_expr_materialized(plan, materialized, left, span)?;
            validate_gql_mutation_order_expr_materialized(plan, materialized, right, span)
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_gql_mutation_order_expr_materialized(plan, materialized, operand, span)?;
            }
            for branch in branches {
                validate_gql_mutation_order_expr_materialized(
                    plan,
                    materialized,
                    &branch.when,
                    span,
                )?;
                validate_gql_mutation_order_expr_materialized(
                    plan,
                    materialized,
                    &branch.then,
                    span,
                )?;
            }
            if let Some(else_expr) = else_expr {
                validate_gql_mutation_order_expr_materialized(
                    plan,
                    materialized,
                    else_expr,
                    span,
                )?;
            }
            Ok(())
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
    if matches!(binding.origin, GqlAliasOrigin::Created | GqlAliasOrigin::Merged) {
        let volatile = match binding.kind {
            GqlAliasKind::Node => matches!(property, "id" | "created_at" | "updated_at"),
            GqlAliasKind::Edge => {
                matches!(property, "id" | "from" | "to" | "created_at" | "updated_at")
            }
            GqlAliasKind::Path => false,
            GqlAliasKind::Scalar => false,
        };
        if volatile {
            return Err(gql_order_key_error(span));
        }
    }
    if property == "updated_at"
        && matches!(binding.origin, GqlAliasOrigin::ReadPrefix | GqlAliasOrigin::Merged)
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
        GqlAliasKind::Path | GqlAliasKind::Scalar => false,
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
        read_scalars: BTreeMap::new(),
        expr_values: Vec::new(),
        created_nodes: BTreeMap::new(),
        created_edges: BTreeMap::new(),
        created_node_writes: BTreeSet::new(),
        created_edge_writes: BTreeSet::new(),
        touched_created_nodes: BTreeSet::new(),
        touched_created_edges: BTreeSet::new(),
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
        nodes: &empty_materialization.nodes,
        edges: &empty_materialization.edges,
        existing_nodes: &empty_materialization.existing_nodes,
        existing_edges: &empty_materialization.existing_edges,
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
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| gql_mutation_expr_depends_on_alias(operand, plan))
                || branches.iter().any(|branch| {
                    gql_mutation_expr_depends_on_alias(&branch.when, plan)
                        || gql_mutation_expr_depends_on_alias(&branch.then, plan)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|else_expr| gql_mutation_expr_depends_on_alias(else_expr, plan))
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => args
            .iter()
            .any(|arg| gql_mutation_expr_depends_on_alias(arg, plan)),
        ExprKind::AggregateCall { arg, .. } => arg
            .as_ref()
            .is_some_and(|arg| gql_mutation_expr_depends_on_alias(arg, plan)),
        ExprKind::Map(map) => map
            .entries
            .iter()
            .any(|entry| gql_mutation_expr_depends_on_alias(&entry.value, plan)),
        ExprKind::ExistsSubquery(_) => true,
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
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
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
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_gql_mutation_return_expr_ids(
                    plan, materialized, operand, row_indices, use_, commit, ids, read_set,
                );
            }
            for branch in branches {
                collect_gql_mutation_return_expr_ids(
                    plan,
                    materialized,
                    &branch.when,
                    row_indices,
                    use_,
                    commit,
                    ids,
                    read_set,
                );
                collect_gql_mutation_return_expr_ids(
                    plan,
                    materialized,
                    &branch.then,
                    row_indices,
                    use_,
                    commit,
                    ids,
                    read_set,
                );
            }
            if let Some(else_expr) = else_expr {
                collect_gql_mutation_return_expr_ids(
                    plan,
                    materialized,
                    else_expr,
                    row_indices,
                    use_,
                    commit,
                    ids,
                    read_set,
                );
            }
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
        ExprKind::ExistsSubquery(_) => {}
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
                let local_created = row.created_nodes.get(alias).copied();
                if commit.is_none()
                    && matches!(binding.origin, GqlAliasOrigin::Created | GqlAliasOrigin::Merged)
                    && local_created.is_some()
                {
                    if use_ == GqlMutationReturnUse::Output {
                        if let Some(index) = local_created {
                            ids.created_node_indices.insert(index);
                        }
                    }
                    continue;
                }
                if let Some(id) =
                    gql_mutation_node_id_for_alias(alias, row, &materialized.nodes, commit)
                {
                    if matches!(
                        binding.origin,
                        GqlAliasOrigin::ReadPrefix | GqlAliasOrigin::Merged
                    )
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
                let local_created = row.created_edges.get(alias).copied();
                if commit.is_none()
                    && matches!(binding.origin, GqlAliasOrigin::Created | GqlAliasOrigin::Merged)
                    && local_created.is_some()
                {
                    if use_ == GqlMutationReturnUse::Output {
                        if let Some(index) = local_created {
                            ids.created_edge_indices.insert(index);
                        }
                    }
                    continue;
                }
                if let Some(id) =
                    gql_mutation_edge_id_for_alias(alias, row, &materialized.edges, commit)
                {
                    if matches!(
                        binding.origin,
                        GqlAliasOrigin::ReadPrefix | GqlAliasOrigin::Merged
                    )
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
            GqlAliasKind::Scalar => {}
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
        ExprKind::Unary { op, expr } => {
            let value = gql_mutation_return_expr_value(expr, params, context)?;
            let graph_value = gql_value_to_graph_eval_scalar(value)?.ok_or_else(|| {
                gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "unary scalar expression requires scalar or null input".to_string(),
                    expr.span.clone(),
                )
            })?;
            let graph_op = match op {
                UnaryOp::Not => GraphUnaryOp::Not,
                UnaryOp::Neg => GraphUnaryOp::Neg,
            };
            graph_eval_to_gql_scalar(eval_graph_unary_value(graph_op, &graph_value)?, &expr.span)
        }
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
            let lower = name.name.to_ascii_lowercase();
            if let Some(function) = gql_scalar_function_name(&lower) {
                return gql_mutation_scalar_function_value(
                    function,
                    &name.name,
                    args,
                    params,
                    context,
                    &expr.span,
                );
            }
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
        ExprKind::AggregateCall { name_span, .. } => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "aggregate functions are not supported in mutation RETURN".to_string(),
            name_span.clone(),
        )),
        ExprKind::ExistsSubquery(_) => Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "GQL mutation RETURN does not support subquery expressions".to_string(),
            expr.span.clone(),
        )),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => gql_mutation_case_value(
            operand.as_deref(),
            branches,
            else_expr.as_deref(),
            params,
            context,
            &expr.span,
        ),
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
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::StartsWith
        | BinaryOp::EndsWith
        | BinaryOp::Contains
        | BinaryOp::In => {
            let left_value = gql_mutation_return_expr_value(left, params, context)?;
            let right_value = gql_mutation_return_expr_value(right, params, context)?;
            if matches!(
                op,
                BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::StartsWith
                    | BinaryOp::EndsWith
                    | BinaryOp::Contains
            ) {
                return gql_mutation_eval_shared_binary(op, left_value, right_value, &left.span);
            }
            if let Some(value) =
                gql_mutation_try_eval_shared_binary(op, &left_value, &right_value, &left.span)?
            {
                return Ok(value);
            }
            Ok(gql_mutation_compare_values(op, left_value, right_value))
        }
    }
}

fn gql_mutation_eval_shared_binary(
    op: BinaryOp,
    left: GqlValue,
    right: GqlValue,
    span: &SourceSpan,
) -> Result<GqlValue, EngineError> {
    let left = gql_value_to_graph_eval_scalar(left)?.ok_or_else(|| {
        gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "scalar operator requires scalar, list, map, or null operands".to_string(),
            span.clone(),
        )
    })?;
    let right = gql_value_to_graph_eval_scalar(right)?.ok_or_else(|| {
        gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "scalar operator requires scalar, list, map, or null operands".to_string(),
            span.clone(),
        )
    })?;
    graph_eval_to_gql_scalar(
        eval_graph_binary_values(gql_binary_op_to_graph_op(op), &left, &right)?,
        span,
    )
}

fn gql_mutation_try_eval_shared_binary(
    op: BinaryOp,
    left: &GqlValue,
    right: &GqlValue,
    span: &SourceSpan,
) -> Result<Option<GqlValue>, EngineError> {
    let Some(left) = gql_value_ref_to_graph_eval_scalar(left)? else {
        return Ok(None);
    };
    let Some(right) = gql_value_ref_to_graph_eval_scalar(right)? else {
        return Ok(None);
    };
    graph_eval_to_gql_scalar(
        eval_graph_binary_values(gql_binary_op_to_graph_op(op), &left, &right)?,
        span,
    )
    .map(Some)
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

fn gql_mutation_scalar_function_value(
    function: GraphFunction,
    display: &str,
    args: &[Expr],
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
    span: &SourceSpan,
) -> Result<GqlValue, EngineError> {
    validate_gql_scalar_function_arity(
        &display.to_ascii_lowercase(),
        display,
        args.len(),
        span,
    )?;
    if function == GraphFunction::Coalesce {
        for arg in args {
            let value = gql_mutation_return_expr_value(arg, params, context)?;
            let graph_value = gql_value_to_graph_eval_scalar(value)?.ok_or_else(|| {
                gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!("function '{display}' expects scalar, list, map, or null input"),
                    arg.span.clone(),
                )
            })?;
            if !graph_value.is_null() {
                let checked = eval_graph_scalar_function_values(
                    GraphFunction::Coalesce,
                    std::slice::from_ref(&graph_value),
                )?;
                return graph_eval_to_gql_scalar(checked, &arg.span);
            }
        }
        return Ok(GqlValue::Null);
    }
    let values = args
        .iter()
        .map(|arg| {
            let value = gql_mutation_return_expr_value(arg, params, context)?;
            gql_value_to_graph_eval_scalar(value)?.ok_or_else(|| {
                gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!("function '{display}' expects scalar, list, map, or null input"),
                    arg.span.clone(),
                )
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    graph_eval_to_gql_scalar(eval_graph_scalar_function_values(function, &values)?, span)
}

fn gql_mutation_case_value(
    operand: Option<&Expr>,
    branches: &[crate::gql::ast::CaseBranch],
    else_expr: Option<&Expr>,
    params: &GqlParams,
    context: &GqlMutationReturnEvalContext<'_>,
    span: &SourceSpan,
) -> Result<GqlValue, EngineError> {
    if let Some(operand) = operand {
        let operand_value = gql_mutation_return_expr_value(operand, params, context)?;
        for branch in branches {
            let when_value = gql_mutation_return_expr_value(&branch.when, params, context)?;
            if let Some(value) = gql_mutation_try_eval_shared_binary(
                BinaryOp::Eq,
                &operand_value,
                &when_value,
                span,
            )? {
                match value {
                    GqlValue::Bool(true) => {
                        return gql_mutation_return_expr_value(&branch.then, params, context);
                    }
                    GqlValue::Bool(false) | GqlValue::Null => {}
                    _ => unreachable!("equality returns bool or null"),
                }
            } else if matches!(
                gql_mutation_compare_values(BinaryOp::Eq, operand_value.clone(), when_value),
                GqlValue::Bool(true)
            ) {
                    return gql_mutation_return_expr_value(&branch.then, params, context);
            }
        }
    } else {
        for branch in branches {
            if let Some(true) = gql_mutation_bool_or_null(&branch.when, params, context)? { return gql_mutation_return_expr_value(&branch.then, params, context) }
        }
    }
    else_expr
        .map(|expr| gql_mutation_return_expr_value(expr, params, context))
        .unwrap_or(Ok(GqlValue::Null))
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

fn gql_value_to_graph_eval_scalar(value: GqlValue) -> Result<Option<GraphEvalValue>, EngineError> {
    gql_value_ref_to_graph_eval_scalar(&value)
}

fn gql_value_ref_to_graph_eval_scalar(value: &GqlValue) -> Result<Option<GraphEvalValue>, EngineError> {
    Ok(match value {
        GqlValue::Null => Some(GraphEvalValue::Null),
        GqlValue::Bool(value) => Some(GraphEvalValue::Bool(*value)),
        GqlValue::Int(value) => Some(GraphEvalValue::Int(*value)),
        GqlValue::UInt(value) => Some(GraphEvalValue::UInt(*value)),
        GqlValue::Float(value) => Some(GraphEvalValue::Float(*value)),
        GqlValue::String(value) => Some(GraphEvalValue::String(value.clone())),
        GqlValue::Bytes(value) => Some(GraphEvalValue::Bytes(value.clone())),
        GqlValue::List(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let Some(value) = gql_value_ref_to_graph_eval_scalar(value)? else {
                    return Ok(None);
                };
                out.push(value);
            }
            Some(GraphEvalValue::List(out))
        }
        GqlValue::Map(values) => {
            let mut out = BTreeMap::new();
            for (key, value) in values {
                let Some(value) = gql_value_ref_to_graph_eval_scalar(value)? else {
                    return Ok(None);
                };
                out.insert(key.clone(), value);
            }
            Some(GraphEvalValue::Map(out))
        }
        GqlValue::Node(_) | GqlValue::Edge(_) | GqlValue::Path(_) => None,
    })
}

fn graph_eval_to_gql_scalar(
    value: GraphEvalValue,
    span: &SourceSpan,
) -> Result<GqlValue, EngineError> {
    Ok(match value {
        GraphEvalValue::Null => GqlValue::Null,
        GraphEvalValue::Bool(value) => GqlValue::Bool(value),
        GraphEvalValue::Int(value) => GqlValue::Int(value),
        GraphEvalValue::UInt(value) => GqlValue::UInt(value),
        GraphEvalValue::Float(value) => GqlValue::Float(value),
        GraphEvalValue::String(value) => GqlValue::String(value),
        GraphEvalValue::Bytes(value) => GqlValue::Bytes(value),
        GraphEvalValue::List(values) => GqlValue::List(
            values
                .into_iter()
                .map(|value| graph_eval_to_gql_scalar(value, span))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphEvalValue::Map(values) => GqlValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, graph_eval_to_gql_scalar(value, span)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphEvalValue::Node(_) | GraphEvalValue::Edge(_) | GraphEvalValue::Path(_) => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "scalar expression produced a graph element value".to_string(),
                span.clone(),
            ));
        }
    })
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
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::StartsWith
        | BinaryOp::EndsWith
        | BinaryOp::Contains => unreachable!(),
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
                        &context.nodes[index],
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
                        &context.edges[index],
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
        GqlAliasKind::Scalar => row_scalar_value(alias, context)?,
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
                        &context.nodes[index],
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
                        &context.edges[index],
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
        GqlAliasKind::Scalar => Ok(GqlValue::Null),
    }
}

fn row_scalar_value(
    alias: &str,
    context: &GqlMutationReturnEvalContext<'_>,
) -> Result<GqlValue, EngineError> {
    context
        .row
        .read_scalars
        .get(alias)
        .cloned()
        .map(graph_value_to_gql_value)
        .transpose()
        .map(|value| value.unwrap_or(GqlValue::Null))
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
                    context.nodes[index]
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
            .map(|&index| GqlValue::String(context.edges[index].label.clone()))
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
        gql_mutation_node_id_for_alias(alias, self.row, self.nodes, self.commit)
    }

    fn edge_id(&self, alias: &str) -> Option<u64> {
        gql_mutation_edge_id_for_alias(alias, self.row, self.edges, self.commit)
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
            if let Some(node) = self.existing_nodes.get(&id) {
                return Ok(gql_node_from_existing_execution(id, node, self.include_vectors));
            }
        }
        if let Some(node) = self.hydrated.nodes.get(&id) {
            return Ok(gql_node_from_record(&node.record, &node.labels, self.include_vectors));
        }
        if let Some(node) = self.existing_nodes.get(&id) {
            return Ok(gql_node_from_existing_execution(id, node, self.include_vectors));
        }
        if let Some(node) = self.nodes.iter().find(|node| {
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
            if let Some(edge) = self.existing_edges.get(&id) {
                return Ok(gql_edge_from_existing_execution(id, edge));
            }
        }
        if let Some(edge) = self.hydrated.edges.get(&id) {
            return Ok(gql_edge_from_record(&edge.record, &edge.label));
        }
        if let Some(edge) = self.existing_edges.get(&id) {
            return Ok(gql_edge_from_existing_execution(id, edge));
        }
        if let Some(edge) = self.edges.iter().find(|edge| {
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
    nodes: &[GqlCreatedNodeExecution],
    commit: Option<&TxnCommitResult>,
) -> Option<u64> {
    if let Some(&node_index) = row.created_nodes.get(alias) {
        let node = &nodes[node_index];
        return commit.and_then(|commit| commit.local_node_ids.get(&node.local).copied());
    }
    row.read_nodes.get(alias).copied().flatten()
}

fn gql_mutation_edge_id_for_alias(
    alias: &str,
    row: &GqlCreateExecutionRow,
    edges: &[GqlCreatedEdgeExecution],
    commit: Option<&TxnCommitResult>,
) -> Option<u64> {
    if let Some(&edge_index) = row.created_edges.get(alias) {
        let edge = &edges[edge_index];
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

fn gql_create_endpoint_key(target: &TxnNodeRef) -> TxnMergeEndpointKey {
    match target {
        TxnNodeRef::Id(id) => TxnMergeEndpointKey::Id(*id),
        TxnNodeRef::Local(local) => TxnMergeEndpointKey::Local(local.clone()),
        TxnNodeRef::Key { label, key } => TxnMergeEndpointKey::Key(label.clone(), key.clone()),
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
        schema: None,
        index: None,
        caps: gql_execution_cap_summary(options),
        notes: Vec::new(),
    }
}

fn validate_gql_mutation_plan_for_execution(plan: &GqlMutationPlan) -> Result<(), EngineError> {
    if let Some(read_prefix) = plan.read_prefix.as_ref() {
        match &read_prefix.lowered.native_target {
            GqlNativeTarget::GraphRows { .. } => {
                normalize_gql_graph_row_target(&read_prefix.lowered)?;
            }
            GqlNativeTarget::GraphPipeline { query } => {
                normalize_graph_pipeline_query(query)
                    .map_err(|err| graph_pipeline_execution_error_to_gql(err, &read_prefix.lowered))?;
            }
        }
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
                    GqlMutationInternalColumn::ScalarValue { .. }
                    | GqlMutationInternalColumn::ExprValue { .. } => None,
                })
                .collect(),
            expression_columns: read_prefix
                .internal_columns
                .iter()
                .filter(|column| {
                    matches!(
                        column,
                        GqlMutationInternalColumn::ScalarValue { .. }
                            | GqlMutationInternalColumn::ExprValue { .. }
                    )
                })
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
        "CREATE, MERGE, SET, REMOVE, DELETE, and DETACH DELETE execution are supported through one WriteTxn; MERGE uses batch transaction snapshot lookups plus statement-local overlays, SET/REMOVE use crate-private by-ID record replacement adapters, and DETACH DELETE reuses transaction cascade planning".to_string(),
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
            uses_transaction_snapshot: gql_mutation_uses_transaction_snapshot(plan),
            uses_write_txn: true,
            replacement_adapters: mutation_uses_replacement_adapters(plan),
            atomic_commit: true,
        }),
        schema: None,
        index: None,
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
        max_pipeline_rows: options.max_pipeline_rows,
        max_groups: options.max_groups,
        max_collect_items: options.max_collect_items,
        max_union_branches: options.max_union_branches,
        max_subquery_invocations: options.max_subquery_invocations,
        max_subquery_depth: options.max_subquery_depth,
        max_shortest_path_pairs: options.max_shortest_path_pairs,
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

fn gql_mutation_uses_transaction_snapshot(plan: &GqlMutationPlan) -> bool {
    plan.read_prefix.is_some()
        || plan
            .clauses
            .iter()
            .any(|clause| matches!(clause, GqlMutationClausePlan::Merge(_)))
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
            GqlMutationClausePlan::Merge(merge) => vec![merge_operation_explain(merge)],
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

fn merge_operation_explain(merge: &GqlMergePlan) -> GqlMutationOperationExplain {
    match &merge.pattern {
        GqlMergePatternPlan::Node { alias, label, key } => GqlMutationOperationExplain {
            op: "MERGE NODE".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row with statement-local key overlay".to_string(),
            detail: format!(
                "label={label:?}; key expr #{}; ON CREATE items={}; ON MATCH items={}; staged through WriteTxn during execution",
                key.id,
                merge.on_create.len(),
                merge.on_match.len()
            ),
        },
        GqlMergePatternPlan::Relationship {
            alias,
            from_alias,
            to_alias,
            label,
        } => GqlMutationOperationExplain {
            op: "MERGE EDGE".to_string(),
            target_alias: Some(alias.clone()),
            row_multiplicity: "per mutation input row with statement-local triple overlay".to_string(),
            detail: format!(
                "{from_alias} -[:{label}]-> {to_alias}; requires edge_uniqueness=true; ON CREATE items={}; ON MATCH items={}; staged through WriteTxn during execution",
                merge.on_create.len(),
                merge.on_match.len()
            ),
        },
    }
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
        match clause {
            GqlMutationClausePlan::Create(patterns) => {
                for pattern in patterns {
                    for node in &pattern.nodes {
                        if node.created {
                            labels.extend(node.labels.iter().cloned());
                        }
                    }
                }
            }
            GqlMutationClausePlan::Merge(merge) => {
                if let GqlMergePatternPlan::Node { label, .. } = &merge.pattern {
                    labels.insert(label.clone());
                }
            }
            GqlMutationClausePlan::Set(_)
            | GqlMutationClausePlan::Remove(_)
            | GqlMutationClausePlan::Delete { .. } => {}
        }
    }
    labels.into_iter().collect()
}

fn mutation_create_edge_labels(plan: &GqlMutationPlan) -> Vec<String> {
    let mut labels = BTreeSet::new();
    for clause in &plan.clauses {
        match clause {
            GqlMutationClausePlan::Create(patterns) => {
                for pattern in patterns {
                    labels.extend(pattern.edges.iter().map(|edge| edge.label.clone()));
                }
            }
            GqlMutationClausePlan::Merge(merge) => {
                if let GqlMergePatternPlan::Relationship { label, .. } = &merge.pattern {
                    labels.insert(label.clone());
                }
            }
            GqlMutationClausePlan::Set(_)
            | GqlMutationClausePlan::Remove(_)
            | GqlMutationClausePlan::Delete { .. } => {}
        }
    }
    labels.into_iter().collect()
}

fn mutation_uses_replacement_adapters(plan: &GqlMutationPlan) -> bool {
    plan.clauses
        .iter()
        .any(|clause| {
            matches!(
                clause,
                GqlMutationClausePlan::Set(_)
                    | GqlMutationClausePlan::Remove(_)
                    | GqlMutationClausePlan::Merge(_)
            )
        })
}

fn mutation_internal_column_summary(column: &GqlMutationInternalColumn) -> String {
    match column {
        GqlMutationInternalColumn::TargetId { alias, kind } => {
            format!("target id: {alias} ({kind:?})")
        }
        GqlMutationInternalColumn::TargetPath { alias } => {
            format!("target path identity: {alias}")
        }
        GqlMutationInternalColumn::ScalarValue { alias, expr } => {
            format!("scalar value: {alias} = {expr:?}")
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
    let GqlNativeTarget::GraphRows { query } = &mut lowered.native_target else {
        return Err(EngineError::InvalidOperation(
            "GQL graph-row target configuration received a non-graph-row target".to_string(),
        ));
    };
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

fn execute_gql_graph_pipeline_target_on_view(
    view: &ReadView,
    lowered: &GqlLoweredPlan,
) -> Result<QueryExecutionOutcome<GraphPipelineResult>, EngineError> {
    let GqlNativeTarget::GraphPipeline { query } = &lowered.native_target else {
        return Err(EngineError::InvalidOperation(
            "GQL graph-pipeline normalization received a non-graph-pipeline target".to_string(),
        ));
    };
    let normalized = normalize_graph_pipeline_query(query)
        .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?;
    let cursor_state = graph_pipeline_cursor_state_from_decoded(
        None,
        &query.page,
        query.at_epoch,
        query.options.max_skip,
    )
    .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?;
    view.query_graph_pipeline_normalized(&normalized, cursor_state)
        .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))
}

fn normalize_gql_graph_row_target(
    lowered: &GqlLoweredPlan,
) -> Result<NormalizedGraphRowQuery, EngineError> {
    let GqlNativeTarget::GraphRows { query } = &lowered.native_target else {
        return Err(EngineError::InvalidOperation(
            "GQL graph-row normalization received a non-graph-row target".to_string(),
        ));
    };
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
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| gql_expr_depends_on_alias(operand, plan))
                || branches.iter().any(|branch| {
                    gql_expr_depends_on_alias(&branch.when, plan)
                        || gql_expr_depends_on_alias(&branch.then, plan)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|else_expr| gql_expr_depends_on_alias(else_expr, plan))
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            args.iter().any(|arg| gql_expr_depends_on_alias(arg, plan))
        }
        ExprKind::AggregateCall { arg, .. } => arg
            .as_ref()
            .is_some_and(|arg| gql_expr_depends_on_alias(arg, plan)),
        ExprKind::ExistsSubquery(_) => true,
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
        ExprKind::AggregateCall {
            function,
            distinct,
            arg,
            name_span,
        } => ExprKind::AggregateCall {
            function: *function,
            distinct: *distinct,
            arg: arg
                .as_ref()
                .map(|arg| resolve_return_aliases_in_expr(arg, return_aliases, plan).map(Box::new))
                .transpose()?,
            name_span: name_span.clone(),
        },
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => ExprKind::Case {
            operand: operand
                .as_ref()
                .map(|operand| {
                    resolve_return_aliases_in_expr(operand, return_aliases, plan).map(Box::new)
                })
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(crate::gql::ast::CaseBranch {
                        when: resolve_return_aliases_in_expr(&branch.when, return_aliases, plan)?,
                        then: resolve_return_aliases_in_expr(&branch.then, return_aliases, plan)?,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| {
                    resolve_return_aliases_in_expr(else_expr, return_aliases, plan).map(Box::new)
                })
                .transpose()?,
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
        ExprKind::ExistsSubquery(_) => return Ok(expr.clone()),
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

fn gql_distinct_key_error(message: &str, span: &SourceSpan) -> EngineError {
    EngineError::GqlSemantic {
        code: GqlSemanticErrorCode::InvalidReturnExpression,
        message: message.to_string(),
        span: span.clone(),
    }
}

fn graph_pipeline_execution_error_to_gql(
    err: EngineError,
    lowered: &GqlLoweredPlan,
) -> EngineError {
    match err {
        EngineError::InvalidOperation(message) if graph_row_full_scan_error_message(&message) => {
            gql_semantic_error(
                GqlSemanticErrorCode::FullScanNotAllowed,
                message,
                lowered.semantic.query.pipeline.span.clone(),
            )
        }
        EngineError::InvalidOperation(message)
            if message.contains("ORDER BY")
                || message.contains("order contexts")
                || message.contains("orderable") =>
        {
            gql_order_key_error(&lowered.semantic.query.return_clause.span)
        }
        other => other,
    }
}

fn build_gql_pipeline_execution_explain(
    lowered: &GqlLoweredPlan,
    pipeline: &GraphPipelineExplain,
    options: &GqlExecutionOptions,
) -> GqlExecutionExplain {
    let read = build_gql_pipeline_read_explain(lowered, pipeline, options);
    let mut notes = lowered.notes.clone();
    notes.extend(pipeline.notes.iter().cloned());
    for stage in &pipeline.stages {
        notes.extend(stage.notes.iter().cloned());
    }
    notes.sort();
    notes.dedup();

    GqlExecutionExplain {
        kind: GqlStatementKind::Query,
        columns: read.columns.clone(),
        warnings: read.warnings.clone(),
        read: Some(read),
        mutation: None,
        schema: None,
        index: None,
        caps: gql_execution_cap_summary(options),
        notes,
    }
}

fn build_gql_pipeline_read_explain(
    lowered: &GqlLoweredPlan,
    pipeline: &GraphPipelineExplain,
    options: &GqlExecutionOptions,
) -> GqlExplain {
    let mut warnings = lowered.warnings.clone();
    warnings.extend(pipeline.warnings.iter().cloned());
    warnings.sort();
    warnings.dedup();

    let mut projection = Vec::new();
    for stage in &pipeline.stages {
        projection.push(format!(
            "graph pipeline stage {}: {}: {}",
            stage.index, stage.kind, stage.detail
        ));
        projection.extend(
            stage
                .notes
                .iter()
                .map(|note| format!("graph pipeline stage note: {note}")),
        );
        if let Some(graph_row) = stage.graph_row.as_ref() {
            projection.extend(graph_row.plan.iter().map(|node| {
                format!(
                    "nested graph row plan: stage {}: {}: {}",
                    stage.index, node.kind, node.detail
                )
            }));
            projection.extend(graph_row.row_ops.iter().map(|op| {
                format!(
                    "nested graph row row op: stage {}: {}: {}",
                    stage.index, op.kind, op.detail
                )
            }));
        }
    }
    projection.extend(
        pipeline
            .row_ops
            .iter()
            .map(|op| format!("graph pipeline row op: {}: {}", op.kind, op.detail)),
    );
    projection.push(format!(
        "graph pipeline cursor: supplied={}, codec_implemented={}, message={}",
        pipeline.cursor.supplied,
        pipeline.cursor.codec_implemented,
        pipeline.cursor.message.as_deref().unwrap_or("none")
    ));

    GqlExplain {
        columns: pipeline.columns.clone(),
        target: GqlLoweringTarget::GraphPipelineQuery,
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
        row_ops: gql_pipeline_row_ops(pipeline),
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
    }
}

fn gql_pipeline_row_ops(pipeline: &GraphPipelineExplain) -> Vec<GqlRowOperation> {
    let mut ops = Vec::new();
    if pipeline
        .row_ops
        .iter()
        .any(|op| op.kind.contains("Filter"))
    {
        ops.push(GqlRowOperation::ResidualFilter);
    }
    if pipeline.row_ops.iter().any(|op| op.kind == "Sort") {
        ops.push(GqlRowOperation::Sort);
    }
    if pipeline.row_ops.iter().any(|op| op.kind == "Skip") {
        ops.push(GqlRowOperation::Skip);
    }
    if pipeline.row_ops.iter().any(|op| op.kind == "Limit") {
        ops.push(GqlRowOperation::Limit);
    }
    ops.push(GqlRowOperation::Projection);
    ops
}

fn build_gql_explain(
    view: &ReadView,
    lowered: &GqlLoweredPlan,
    returns: &[GqlReturnExpr],
    order_by: &[GqlResolvedOrderItem],
    options: &GqlExecutionOptions,
) -> Result<GqlExplain, EngineError> {
    if let GqlNativeTarget::GraphPipeline { query } = &lowered.native_target {
        let mut normalized = normalize_graph_pipeline_query(query)
            .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?;
        normalized.options.include_plan = true;
        let cursor_state = graph_pipeline_cursor_state_from_decoded(
            None,
            &query.page,
            query.at_epoch,
            query.options.max_skip,
        )
        .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?;
        let pipeline_explain = if normalized.options.profile {
            view.query_graph_pipeline_normalized(&normalized, cursor_state)
                .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?
                .value
                .plan
                .ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "graph pipeline explain did not produce a plan".to_string(),
                    )
                })?
        } else {
            view.explain_graph_pipeline_normalized(&normalized, cursor_state)
                .map_err(|err| graph_pipeline_execution_error_to_gql(err, lowered))?
        };
        return Ok(build_gql_pipeline_read_explain(
            lowered,
            &pipeline_explain,
            options,
        ));
    }

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
        GqlNativeTargetKind::GraphPipeline => GqlLoweringTarget::GraphPipelineQuery,
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
                    _ => {}
                }
            }
        }
        GraphExpr::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                gql_append_graph_expr_output_summaries(summaries, arg);
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
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                gql_append_graph_expr_output_summaries(summaries, operand);
            }
            for branch in branches {
                gql_append_graph_expr_output_summaries(summaries, &branch.when);
                gql_append_graph_expr_output_summaries(summaries, &branch.then);
            }
            if let Some(else_expr) = else_expr {
                gql_append_graph_expr_output_summaries(summaries, else_expr);
            }
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
        | GraphExpr::ExistsSubquery(_)
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
        ExprKind::Unary { op, expr } => match op {
            UnaryOp::Not => format!("NOT {}", gql_expr_summary(expr)),
            UnaryOp::Neg => format!("-{}", gql_expr_summary(expr)),
        },
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
        ExprKind::AggregateCall {
            function,
            distinct,
            arg,
            ..
        } => {
            let function = match function {
                crate::gql::ast::AggregateFunction::Count => "count",
                crate::gql::ast::AggregateFunction::Sum => "sum",
                crate::gql::ast::AggregateFunction::Avg => "avg",
                crate::gql::ast::AggregateFunction::Min => "min",
                crate::gql::ast::AggregateFunction::Max => "max",
                crate::gql::ast::AggregateFunction::Collect => "collect",
            };
            let arg = match arg {
                Some(arg) if *distinct => format!("DISTINCT {}", gql_expr_summary(arg)),
                Some(arg) => gql_expr_summary(arg),
                None => "*".to_string(),
            };
            format!("{function}({arg})")
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            let mut parts = Vec::new();
            if let Some(operand) = operand {
                parts.push(gql_expr_summary(operand));
            }
            for branch in branches {
                parts.push(format!(
                    "WHEN {} THEN {}",
                    gql_expr_summary(&branch.when),
                    gql_expr_summary(&branch.then)
                ));
            }
            if let Some(else_expr) = else_expr {
                parts.push(format!("ELSE {}", gql_expr_summary(else_expr)));
            }
            format!("CASE {} END", parts.join(" "))
        }
        ExprKind::List(_) => "list".to_string(),
        ExprKind::Map(_) => "map".to_string(),
        ExprKind::ExistsSubquery(_) => "EXISTS subquery".to_string(),
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
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Eq => "=",
        BinaryOp::Neq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::In => "IN",
        BinaryOp::StartsWith => "STARTS WITH",
        BinaryOp::EndsWith => "ENDS WITH",
        BinaryOp::Contains => "CONTAINS",
    }
}
