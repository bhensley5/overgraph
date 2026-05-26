use crate::gql::eval::{
    build_runtime_projection, eval_expr_against_context, return_exprs, GqlEvalContext,
    GqlReturnExpr,
};
use crate::gql::ast::{BinaryOp, Expr, ExprKind, Literal, OrderDirection, UnaryOp};
use crate::gql::lower::{
    gql_expr_to_graph_expr, gql_order_direction_to_graph, lower_semantic_plan, GqlLoweredPlan,
    GqlNativeTarget, GqlNativeTargetKind,
};
use crate::gql::parser::{parse_query, GqlParseOptions};
use crate::gql::params::validate_referenced_gql_params;
use crate::gql::result::graph_value_to_gql_value;
use crate::gql::semantic::{
    bind_query, gql_semantic_error, GqlAliasKind, GqlReturnPlan, GqlSemanticPlan,
};
use std::time::Instant;

impl DatabaseEngine {
    pub fn execute_gql(
        &self,
        query: &str,
        params: &GqlParams,
        options: &GqlQueryOptions,
    ) -> Result<GqlResult, EngineError> {
        let started_at = Instant::now();
        let parse_options = GqlParseOptions {
            max_query_bytes: options.max_query_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        };
        let ast = parse_query(query, &parse_options)?;
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
                Some(build_gql_limit_zero_explain(
                    &lowered,
                    &return_exprs,
                    &resolved_order_by,
                    options,
                ))
            } else {
                None
            };
            let elapsed_us = if options.profile {
                started_at.elapsed().as_micros().try_into().ok()
            } else {
                None
            };
            return Ok(GqlResult {
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
                    truncated: false,
                    warnings,
                },
                plan,
            });
        }

        let (_guard, published) = self.runtime.published_snapshot()?;
        let plan = if options.include_plan {
            Some(build_gql_explain(
                &published.view,
                &lowered,
                &return_exprs,
                &resolved_order_by,
                options,
            )?)
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
        Ok(GqlResult {
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
                truncated: truncated_by_row_cap,
                warnings,
            },
            plan,
        })
    }

    pub fn explain_gql(
        &self,
        query: &str,
        params: &GqlParams,
        options: &GqlQueryOptions,
    ) -> Result<GqlExplain, EngineError> {
        let parse_options = GqlParseOptions {
            max_query_bytes: options.max_query_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        };
        let ast = parse_query(query, &parse_options)?;
        let semantic = bind_query(ast, params)?;
        validate_referenced_gql_params(&semantic, params, options)?;
        let mut lowered = lower_semantic_plan(semantic, params, options)?;
        let return_exprs = return_exprs(&lowered.semantic);
        let resolved_order_by = resolve_order_by_return_aliases(&lowered)?;
        validate_gql_row_independent_order_keys(&resolved_order_by, &lowered, params)?;
        let row_counts = evaluate_gql_row_counts(&lowered, params, options)?;
        configure_gql_graph_row_target(&mut lowered, &resolved_order_by, &row_counts, options)?;
        if row_counts.limit == Some(0) {
            return Ok(build_gql_limit_zero_explain(
                &lowered,
                &return_exprs,
                &resolved_order_by,
                options,
            ));
        }

        let (_guard, published) = self.runtime.published_snapshot()?;
        build_gql_explain(
            &published.view,
            &lowered,
            &return_exprs,
            &resolved_order_by,
            options,
        )
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
    options: &GqlQueryOptions,
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
    options: &GqlQueryOptions,
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
    options: &GqlQueryOptions,
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
    options: &GqlQueryOptions,
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
