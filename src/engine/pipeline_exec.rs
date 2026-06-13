#[derive(Clone, Debug)]
struct GraphPipelineCursorPayload {
    effective_at_epoch: i64,
    original_skip: u64,
    rows_emitted_after_skip: u64,
    query_fingerprint: u128,
    order_fingerprint: u128,
    output_fingerprint: u128,
    params_fingerprint: u128,
    last_sort_key: Vec<crate::graph_row::GraphSortAtom>,
    last_logical_row_key: Vec<crate::graph_row::GraphSortAtom>,
}

#[derive(Clone, Debug)]
struct GraphPipelineCursorState {
    decoded: Option<GraphPipelineCursorPayload>,
    effective_at_epoch: i64,
    original_skip: u64,
    rows_emitted_after_skip: u64,
}

impl GraphPipelineCursorState {
    fn is_cursor_page(&self) -> bool {
        self.decoded.is_some()
    }
}

#[derive(Debug)]
struct GraphRowStageExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    followups: Vec<SecondaryIndexReadFollowup>,
    explain: Option<GraphRowExplain>,
    warnings: Vec<String>,
    rows_after_filter: usize,
    intermediate_peak: usize,
}

#[derive(Debug)]
struct PipelineExistsExecution {
    exists: bool,
    followups: Vec<SecondaryIndexReadFollowup>,
    stats: GraphPipelineStats,
}

#[derive(Debug)]
struct PipelineProjectStageExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    followups: Vec<SecondaryIndexReadFollowup>,
    groups: usize,
    collect_items: usize,
    aggregate_distinct_keys: usize,
    subquery_invocations: usize,
    subquery_cache_hits: usize,
    nested_stats: GraphPipelineStats,
}

#[derive(Debug)]
struct PipelineOptionalCandidateFilterExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    followups: Vec<SecondaryIndexReadFollowup>,
    subquery_invocations: usize,
    subquery_cache_hits: usize,
    nested_stats: GraphPipelineStats,
    input_rows: usize,
    candidate_rows: usize,
    passed_rows: usize,
    preserved_miss_rows: usize,
    synthesized_miss_rows: usize,
}

#[derive(Debug)]
struct PipelineShortestPathStageExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    pair_count: usize,
    cache_hits: usize,
    no_path_count: usize,
    emitted_path_count: usize,
}

#[derive(Debug)]
struct PipelineCallStageExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    followups: Vec<SecondaryIndexReadFollowup>,
    subquery_invocations: usize,
    subquery_cache_hits: usize,
    nested_stats: GraphPipelineStats,
}

struct PipelineExistsProbePlan<'a> {
    match_stage: Option<&'a NormalizedPipelineMatchStage>,
    always_false: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PipelineSubqueryRowMode {
    Exists,
    Call,
}

#[derive(Clone, Debug)]
struct PipelineCallRepresentative {
    index: usize,
    outer_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ShortestPathPairKey {
    from_id: u64,
    to_id: u64,
    direction: u8,
    edge_label_filter: Vec<String>,
    min_hops: u8,
    max_hops: u8,
    weight_field: Option<String>,
    max_cost_bits: Option<u64>,
    max_paths: Option<usize>,
}

#[derive(Clone, Debug)]
enum ResolvedShortestPathEndpoint {
    Alias {
        slot: crate::graph_row::GraphBindingSlotRef,
    },
    Static(Option<u64>),
}

#[derive(Debug)]
struct PipelineStagesExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    row_projections: Option<Vec<Arc<[GraphReturnProjection]>>>,
    followups: Vec<SecondaryIndexReadFollowup>,
    stage_explains: Vec<GraphPipelineStageExplain>,
    warnings: Vec<String>,
    stats: GraphPipelineStats,
}

#[derive(Debug)]
struct PipelineUnionStageExecution {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    row_projections: Vec<Arc<[GraphReturnProjection]>>,
    followups: Vec<SecondaryIndexReadFollowup>,
    stage_explains: Vec<GraphPipelineStageExplain>,
    warnings: Vec<String>,
    stats: GraphPipelineStats,
}

#[derive(Clone, Debug)]
struct PipelineUnionBranchExplainSummary {
    branch_index: usize,
    stages: Vec<GraphPipelineStageExplain>,
    row_ops: Vec<GraphRowOperationExplain>,
    warnings: Vec<String>,
}

#[derive(Debug)]
struct PipelineSubqueryEvalStats {
    invocations: usize,
    cache_hits: usize,
    followups: Vec<SecondaryIndexReadFollowup>,
    nested_stats: GraphPipelineStats,
}

#[derive(Debug)]
struct SubqueryInvocationBudget {
    max: usize,
    used: usize,
}

impl SubqueryInvocationBudget {
    fn new(max: usize) -> Self {
        Self { max, used: 0 }
    }

    fn reserve(&mut self, operator: &str) -> Result<(), EngineError> {
        if self.used >= self.max {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline {operator} exceeded max_subquery_invocations {}",
                self.max
            )));
        }
        self.used = self.used.saturating_add(1);
        Ok(())
    }
}

impl Default for PipelineSubqueryEvalStats {
    fn default() -> Self {
        Self {
            invocations: 0,
            cache_hits: 0,
            followups: Vec::new(),
            nested_stats: empty_graph_pipeline_stats(0),
        }
    }
}

#[derive(Clone, Debug)]
struct PipelineFinalRow {
    row: crate::graph_row::GraphBindingRow,
    sort_key: Vec<crate::graph_row::GraphSortAtom>,
    logical_key: Vec<crate::graph_row::GraphSortAtom>,
    projections: Option<Arc<[GraphReturnProjection]>>,
}

impl GraphPipelineStats {
    fn merge_from(&mut self, other: &GraphPipelineStats) {
        // rows_after_filter belongs to the execution context that owns this stats value.
        // Nested pipeline stats contribute counters and peaks, but must not redefine the
        // parent pipeline's final row count.
        self.intermediate_rows = self.intermediate_rows.max(other.intermediate_rows);
        self.pipeline_rows_materialized = self
            .pipeline_rows_materialized
            .max(other.pipeline_rows_materialized);
        self.groups = self.groups.saturating_add(other.groups);
        self.collect_items = self.collect_items.saturating_add(other.collect_items);
        self.union_branches = self.union_branches.saturating_add(other.union_branches);
        self.union_dedup_keys = self
            .union_dedup_keys
            .saturating_add(other.union_dedup_keys);
        self.subquery_invocations = self
            .subquery_invocations
            .saturating_add(other.subquery_invocations);
        self.subquery_cache_hits = self
            .subquery_cache_hits
            .saturating_add(other.subquery_cache_hits);
        self.shortest_path_pairs = self
            .shortest_path_pairs
            .saturating_add(other.shortest_path_pairs);
        self.shortest_path_cache_hits = self
            .shortest_path_cache_hits
            .saturating_add(other.shortest_path_cache_hits);
        self.db_hits = self.db_hits.saturating_add(other.db_hits);
        self.warnings.extend(other.warnings.iter().cloned());
    }
}

fn empty_graph_pipeline_stats(effective_at_epoch: i64) -> GraphPipelineStats {
    GraphPipelineStats {
        rows_returned: 0,
        rows_entered_pipeline: 0,
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
        effective_at_epoch,
        warnings: Vec::new(),
    }
}

impl ReadView {
    fn explain_graph_pipeline_normalized(
        &self,
        pipeline: &NormalizedGraphPipeline,
        cursor_state: GraphPipelineCursorState,
    ) -> Result<GraphPipelineExplain, EngineError> {
        let fingerprints = graph_pipeline_cursor_fingerprints(
            pipeline,
            cursor_state.effective_at_epoch,
            cursor_state.original_skip,
        );
        if let Some(cursor) = cursor_state.decoded.as_ref() {
            graph_pipeline_validate_cursor_fingerprints(cursor, &fingerprints)?;
            graph_pipeline_validate_cursor_shape(pipeline, cursor)?;
        }

        let mut warnings = Vec::new();
        let mut stage_explains = Vec::with_capacity(pipeline.stages.len());
        for (stage_index, stage) in pipeline.stages.iter().enumerate() {
            match stage {
                NormalizedGraphPipelineStage::Match(stage) => {
                    let graph_row_cursor_state = GraphRowCursorState {
                        decoded: None,
                        effective_at_epoch: cursor_state.effective_at_epoch,
                        original_skip: 0,
                        rows_emitted_after_skip: 0,
                    };
                    let graph_row = self.explain_graph_rows_normalized(
                        &stage.query,
                        graph_row_cursor_state,
                    )?;
                    warnings.extend(graph_row.warnings.clone());
                    stage_explains.push(GraphPipelineStageExplain {
                        index: stage_index,
                        kind: if stage.optional {
                            "OptionalMatch".to_string()
                        } else {
                            "Match".to_string()
                        },
                        detail: pipeline_match_stage_detail(stage, None),
                        columns: pipeline_schema_columns(&stage.output_schema),
                        warnings: graph_row.warnings.clone(),
                        notes: pipeline_match_stage_notes(stage, None),
                        graph_row: Some(Box::new(graph_row)),
                    });
                }
                NormalizedGraphPipelineStage::ShortestPath(stage) => {
                    stage_explains.push(GraphPipelineStageExplain {
                        index: stage_index,
                        kind: if stage.optional {
                            "OptionalShortestPath".to_string()
                        } else {
                            "ShortestPath".to_string()
                        },
                        detail: pipeline_shortest_path_stage_detail(
                            stage,
                            &pipeline.options,
                            None,
                        ),
                        columns: pipeline_schema_columns(&stage.output_schema),
                        graph_row: None,
                        warnings: Vec::new(),
                        notes: vec![
                            "shortest-path stage uses native graph algorithms".to_string(),
                        ],
                    });
                }
                NormalizedGraphPipelineStage::Project(stage) => {
                    stage_explains.push(GraphPipelineStageExplain {
                        index: stage_index,
                        kind: match stage.kind {
                            GraphProjectKind::With => "Project(With)".to_string(),
                            GraphProjectKind::Return => "Project(Return)".to_string(),
                        },
                        detail: pipeline_project_stage_detail(stage, None, None, None, None),
                        columns: stage.columns.clone(),
                        graph_row: None,
                        warnings: Vec::new(),
                        notes: pipeline_project_stage_notes(stage),
                    });
                }
                NormalizedGraphPipelineStage::Call(stage) => {
                    let nested = self.explain_graph_pipeline_normalized(
                        &stage.query,
                        GraphPipelineCursorState {
                            decoded: None,
                            effective_at_epoch: cursor_state.effective_at_epoch,
                            original_skip: 0,
                            rows_emitted_after_skip: 0,
                        },
                    )?;
                    warnings.extend(nested.warnings.clone());
                    stage_explains.push(GraphPipelineStageExplain {
                        index: stage_index,
                        kind: "Call".to_string(),
                        detail: pipeline_call_stage_detail(stage, None, None, None),
                        columns: pipeline_schema_columns(&stage.output_schema),
                        graph_row: None,
                        warnings: nested.warnings.clone(),
                        notes: pipeline_call_stage_notes(stage, Some(&nested)),
                    });
                }
                NormalizedGraphPipelineStage::Union(stage) => {
                    let mut branch_summaries = Vec::with_capacity(stage.branches.len());
                    let mut stage_warnings = Vec::new();
                    for (branch_index, branch) in stage.branches.iter().enumerate() {
                        let branch_explain = self.explain_graph_pipeline_normalized(
                            &branch.pipeline,
                            GraphPipelineCursorState {
                                decoded: None,
                                effective_at_epoch: cursor_state.effective_at_epoch,
                                original_skip: 0,
                                rows_emitted_after_skip: 0,
                            },
                        )?;
                        stage_warnings.extend(branch_explain.warnings.clone());
                        branch_summaries.push(PipelineUnionBranchExplainSummary {
                            branch_index,
                            stages: branch_explain.stages,
                            row_ops: branch_explain.row_ops,
                            warnings: branch_explain.warnings,
                        });
                    }
                    warnings.extend(stage_warnings.clone());
                    stage_explains.push(GraphPipelineStageExplain {
                        index: stage_index,
                        kind: if stage.all {
                            "UnionAll".to_string()
                        } else {
                            "Union".to_string()
                        },
                        detail: pipeline_union_stage_detail(stage, None, None),
                        columns: stage.columns.clone(),
                        graph_row: None,
                        warnings: stage_warnings,
                        notes: pipeline_union_stage_notes(stage, &branch_summaries),
                    });
                }
            }
        }
        let stats = GraphPipelineStats {
            rows_returned: 0,
            rows_entered_pipeline: 1,
            rows_after_filter: 0,
            intermediate_rows: 0,
            pipeline_rows_materialized: 0,
            groups: 0,
            collect_items: 0,
            union_branches: pipeline_union_branch_count(pipeline),
            union_dedup_keys: 0,
            subquery_invocations: 0,
            subquery_cache_hits: 0,
            shortest_path_pairs: 0,
            shortest_path_cache_hits: 0,
            db_hits: 0,
            elapsed_us: None,
            effective_at_epoch: cursor_state.effective_at_epoch,
            warnings: warnings.clone(),
        };
        Ok(graph_pipeline_explain_from_normalized(
            pipeline,
            stage_explains,
            stats,
            fingerprints,
            warnings,
        ))
    }

    fn query_graph_pipeline_normalized(
        &self,
        pipeline: &NormalizedGraphPipeline,
        cursor_state: GraphPipelineCursorState,
    ) -> Result<QueryExecutionOutcome<GraphPipelineResult>, EngineError> {
        let started_at = std::time::Instant::now();
        let mut followups = Vec::new();
        let mut stage_explains = Vec::new();
        let mut warnings = Vec::new();
        let fingerprints = graph_pipeline_cursor_fingerprints(
            pipeline,
            cursor_state.effective_at_epoch,
            cursor_state.original_skip,
        );
        if let Some(cursor) = cursor_state.decoded.as_ref() {
            graph_pipeline_validate_cursor_fingerprints(cursor, &fingerprints)?;
            graph_pipeline_validate_cursor_shape(pipeline, cursor)?;
        }
        let mut execution = self.execute_graph_pipeline_stages(
            pipeline,
            cursor_state.effective_at_epoch,
            pipeline.options.include_plan,
        )?;
        let row_projections = execution.row_projections.take();
        followups.append(&mut execution.followups);
        stage_explains.append(&mut execution.stage_explains);
        warnings.append(&mut execution.warnings);
        let mut stats = execution.stats;
        let rows = execution.rows;

        let mut final_rows = self.pipeline_prepare_final_rows(
            rows,
            pipeline,
            cursor_state.decoded.as_ref(),
            &fingerprints,
            row_projections,
        )?;
        stats.pipeline_rows_materialized = stats.pipeline_rows_materialized.max(final_rows.len());
        let page_start = if cursor_state.is_cursor_page() {
            0
        } else {
            cursor_state.original_skip as usize
        };
        if page_start > pipeline.options.max_skip {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline page skip {page_start} exceeds max_skip {}",
                pipeline.options.max_skip
            )));
        }
        let total_after_cursor = final_rows.len();
        let skipped = page_start.min(final_rows.len());
        final_rows.drain(0..skipped);
        let limit = pipeline.page.limit.min(pipeline.options.max_rows);
        let has_more = final_rows.len() > limit;
        if has_more {
            final_rows.truncate(limit);
        }

        let graph_rows = if final_rows.iter().any(|row| row.projections.is_some()) {
            self.pipeline_project_output_rows_with_row_projections(
                &final_rows,
                &pipeline.terminal_return_items,
                &pipeline.output,
            )?
        } else {
            let mut output_rows = final_rows
                .iter()
                .map(|candidate| candidate.row.clone())
                .collect::<Vec<_>>();
            self.hydrate_graph_rows_for_needs(
                &mut output_rows,
                &pipeline.terminal_schema,
                &pipeline.terminal_output_needs,
            )?;
            self.pipeline_project_output_rows(
                &output_rows,
                &pipeline.terminal_return_items,
                &pipeline.output,
            )?
        };
        let next_cursor = if has_more {
            let last = final_rows.last().ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph pipeline cannot emit a cursor without a final row".to_string(),
                )
            })?;
            let payload = GraphPipelineCursorPayload {
                effective_at_epoch: cursor_state.effective_at_epoch,
                original_skip: cursor_state.original_skip,
                rows_emitted_after_skip: cursor_state
                    .rows_emitted_after_skip
                    .saturating_add(graph_rows.len() as u64),
                query_fingerprint: fingerprints.query,
                order_fingerprint: fingerprints.order,
                output_fingerprint: fingerprints.output,
                params_fingerprint: fingerprints.params,
                last_sort_key: last.sort_key.clone(),
                last_logical_row_key: last.logical_key.clone(),
            };
            Some(graph_pipeline_encode_logical_cursor(
                &payload,
                pipeline.options.max_cursor_bytes,
            )?)
        } else {
            None
        };

        stats.rows_returned = graph_rows.len();
        stats.rows_after_filter = total_after_cursor;
        stats.elapsed_us = if pipeline.options.profile {
            started_at.elapsed().as_micros().try_into().ok()
        } else {
            None
        };
        stats.warnings = warnings.clone();
        let plan = pipeline.options.include_plan.then(|| {
            graph_pipeline_explain_from_normalized(
                pipeline,
                stage_explains,
                stats.clone(),
                fingerprints,
                warnings.clone(),
            )
        });
        Ok(QueryExecutionOutcome {
            value: GraphPipelineResult {
                columns: pipeline.columns.clone(),
                rows: graph_rows,
                next_cursor,
                stats,
                plan,
            },
            followups,
        })
    }

    fn execute_graph_pipeline_stages(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        include_plan: bool,
    ) -> Result<PipelineStagesExecution, EngineError> {
        let rows = vec![pipeline.initial_schema.empty_row()];
        self.execute_graph_pipeline_stages_with_rows(
            pipeline,
            effective_at_epoch,
            include_plan,
            rows,
        )
    }

    fn execute_graph_pipeline_stages_with_rows(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        include_plan: bool,
        initial_rows: Vec<crate::graph_row::GraphBindingRow>,
    ) -> Result<PipelineStagesExecution, EngineError> {
        let mut subquery_budget =
            SubqueryInvocationBudget::new(pipeline.options.max_subquery_invocations);
        self.execute_graph_pipeline_stages_with_rows_budget(
            pipeline,
            effective_at_epoch,
            include_plan,
            initial_rows,
            &mut subquery_budget,
        )
    }

    fn execute_graph_pipeline_stages_with_rows_budget(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        include_plan: bool,
        initial_rows: Vec<crate::graph_row::GraphBindingRow>,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineStagesExecution, EngineError> {
        let mut followups = Vec::new();
        let mut stage_explains = Vec::new();
        let mut warnings = Vec::new();
        let initial_row_count = initial_rows.len();
        let mut stats = GraphPipelineStats {
            rows_returned: 0,
            rows_entered_pipeline: initial_row_count,
            rows_after_filter: initial_row_count,
            intermediate_rows: initial_row_count,
            pipeline_rows_materialized: initial_row_count,
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
            effective_at_epoch,
            warnings: Vec::new(),
        };

        let mut current_schema = pipeline.initial_schema.clone();
        let mut rows = initial_rows;
        let mut row_projections = None;
        for (stage_index, stage) in pipeline.stages.iter().enumerate() {
            match stage {
                NormalizedGraphPipelineStage::Match(stage) => {
                    row_projections = None;
                    let bridged_initial_rows = pipeline_bridge_rows(
                        &rows,
                        &current_schema,
                        &stage.query.binding_schema,
                        &stage.input_mappings,
                    )?;
                    let optional_left_rows = if current_schema.slots().is_empty() {
                        vec![stage.query.binding_schema.empty_row()]
                    } else {
                        bridged_initial_rows.clone()
                    };
                    let initial_rows = if current_schema.slots().is_empty() {
                        None
                    } else {
                        Some(bridged_initial_rows)
                    };
                    let execution = self.execute_graph_row_stage(
                        &stage.query,
                        initial_rows,
                        effective_at_epoch,
                        include_plan,
                        stage.optional,
                    )?;
                    followups.extend(execution.followups);
                    stats.intermediate_rows =
                        stats.intermediate_rows.max(execution.intermediate_peak);
                    stats.rows_after_filter = execution.rows_after_filter;
                    stats.db_hits = stats.db_hits.saturating_add(if pipeline.options.profile {
                        execution.rows_after_filter
                    } else {
                        0
                    });
                    let stage_warnings = execution.warnings.clone();
                    warnings.extend(stage_warnings.clone());
                    let mut graph_rows = execution.rows;
                    let optional_filter_execution = if stage.optional_candidate_filter.is_some() {
                        let mut filter_execution = self.apply_pipeline_optional_candidate_filter(
                            stage,
                            &optional_left_rows,
                            graph_rows,
                            effective_at_epoch,
                            subquery_budget,
                        )?;
                        followups.append(&mut filter_execution.followups);
                        warnings.extend(filter_execution.nested_stats.warnings.iter().cloned());
                        stats.merge_from(&filter_execution.nested_stats);
                        stats.subquery_invocations = stats
                            .subquery_invocations
                            .saturating_add(filter_execution.subquery_invocations);
                        stats.subquery_cache_hits = stats
                            .subquery_cache_hits
                            .saturating_add(filter_execution.subquery_cache_hits);
                        graph_rows = std::mem::take(&mut filter_execution.rows);
                        Some(filter_execution)
                    } else {
                        None
                    };
                    rows = pipeline_attach_cursor_keys(
                        graph_rows,
                        &stage.query.binding_schema,
                        &stage.output_schema,
                        &stage.output_mappings,
                        stage.cursor_slot,
                    )?;
                    current_schema = stage.output_schema.clone();
                    pipeline_enforce_intermediate_rows(
                        rows.len(),
                        &pipeline.options,
                        "max_pipeline_rows",
                    )?;
                    if include_plan {
                        stage_explains.push(GraphPipelineStageExplain {
                            index: stage_index,
                            kind: if stage.optional {
                                "OptionalMatch".to_string()
                            } else {
                                "Match".to_string()
                            },
                            detail: pipeline_match_stage_detail(stage, Some(rows.len())),
                            columns: pipeline_schema_columns(&current_schema),
                            graph_row: execution.explain.map(Box::new),
                            warnings: stage_warnings,
                            notes: pipeline_match_stage_notes(
                                stage,
                                optional_filter_execution.as_ref(),
                            ),
                        });
                    }
                }
                NormalizedGraphPipelineStage::ShortestPath(stage) => {
                    row_projections = None;
                    let before = rows.len();
                    let execution = self.execute_pipeline_shortest_path_stage(
                        stage,
                        rows,
                        effective_at_epoch,
                        &pipeline.options,
                    )?;
                    let stage_detail = if include_plan {
                        Some(pipeline_shortest_path_stage_detail(
                            stage,
                            &pipeline.options,
                            Some(&execution),
                        ))
                    } else {
                        None
                    };
                    rows = execution.rows;
                    current_schema = stage.output_schema.clone();
                    stats.shortest_path_pairs = stats
                        .shortest_path_pairs
                        .saturating_add(execution.pair_count);
                    stats.shortest_path_cache_hits = stats
                        .shortest_path_cache_hits
                        .saturating_add(execution.cache_hits);
                    stats.rows_after_filter = rows.len();
                    stats.intermediate_rows = stats.intermediate_rows.max(rows.len());
                    stats.pipeline_rows_materialized =
                        stats.pipeline_rows_materialized.max(rows.len());
                    pipeline_enforce_intermediate_rows(
                        rows.len(),
                        &pipeline.options,
                        "max_pipeline_rows",
                    )?;
                    if include_plan {
                        stage_explains.push(GraphPipelineStageExplain {
                            index: stage_index,
                            kind: if stage.optional {
                                "OptionalShortestPath".to_string()
                            } else {
                                "ShortestPath".to_string()
                            },
                            detail: stage_detail.expect("shortest-path detail prepared"),
                            columns: pipeline_schema_columns(&current_schema),
                            graph_row: None,
                            warnings: Vec::new(),
                            notes: vec![format!(
                                "native shortest-path stage consumed {before} row(s)"
                            )],
                        });
                    }
                }
                NormalizedGraphPipelineStage::Project(stage) => {
                    row_projections = None;
                    let before = rows.len();
                    let execution = self.execute_pipeline_project_stage(
                        stage,
                        rows,
                        effective_at_epoch,
                        &pipeline.options,
                        subquery_budget,
                    )?;
                    let aggregate_distinct_keys = execution.aggregate_distinct_keys;
                    rows = execution.rows;
                    stats.groups = stats.groups.saturating_add(execution.groups);
                    stats.collect_items =
                        stats.collect_items.saturating_add(execution.collect_items);
                    stats.subquery_invocations = stats
                        .subquery_invocations
                        .saturating_add(execution.subquery_invocations);
                    stats.subquery_cache_hits = stats
                        .subquery_cache_hits
                        .saturating_add(execution.subquery_cache_hits);
                    followups.extend(execution.followups);
                    warnings.extend(execution.nested_stats.warnings.iter().cloned());
                    stats.merge_from(&execution.nested_stats);
                    if stats.subquery_invocations > pipeline.options.max_subquery_invocations {
                        return Err(EngineError::InvalidOperation(format!(
                            "graph pipeline exceeded max_subquery_invocations {}",
                            pipeline.options.max_subquery_invocations
                        )));
                    }
                    current_schema = stage.output_schema.clone();
                    stats.rows_after_filter = rows.len();
                    stats.intermediate_rows = stats.intermediate_rows.max(rows.len());
                    stats.pipeline_rows_materialized =
                        stats.pipeline_rows_materialized.max(rows.len());
                    // Project(Return) is capped during final cursor/page emission by max_rows.
                    // max_pipeline_rows is the hard cap for intermediate pipeline stages.
                    if stage.kind != GraphProjectKind::Return {
                        pipeline_enforce_intermediate_rows(
                            rows.len(),
                            &pipeline.options,
                            "max_pipeline_rows",
                        )?;
                    }
                    if include_plan {
                        stage_explains.push(GraphPipelineStageExplain {
                            index: stage_index,
                            kind: match stage.kind {
                                GraphProjectKind::With => "Project(With)".to_string(),
                                GraphProjectKind::Return => "Project(Return)".to_string(),
                            },
                            detail: pipeline_project_stage_detail(
                                stage,
                                Some(before),
                                Some(rows.len()),
                                Some(aggregate_distinct_keys),
                                Some((
                                    execution.subquery_invocations,
                                    execution.subquery_cache_hits,
                                )),
                            ),
                            columns: stage.columns.clone(),
                            graph_row: None,
                            warnings: Vec::new(),
                            notes: pipeline_project_stage_notes(stage),
                        });
                    }
                }
                NormalizedGraphPipelineStage::Call(stage) => {
                    row_projections = None;
                    let before = rows.len();
                    let execution = self.execute_pipeline_call_stage(
                        stage,
                        rows,
                        effective_at_epoch,
                        &pipeline.options,
                        subquery_budget,
                    )?;
                    rows = execution.rows;
                    stats.subquery_invocations = stats
                        .subquery_invocations
                        .saturating_add(execution.subquery_invocations);
                    stats.subquery_cache_hits = stats
                        .subquery_cache_hits
                        .saturating_add(execution.subquery_cache_hits);
                    followups.extend(execution.followups);
                    warnings.extend(execution.nested_stats.warnings.iter().cloned());
                    stats.merge_from(&execution.nested_stats);
                    if stats.subquery_invocations > pipeline.options.max_subquery_invocations {
                        return Err(EngineError::InvalidOperation(format!(
                            "graph pipeline exceeded max_subquery_invocations {}",
                            pipeline.options.max_subquery_invocations
                        )));
                    }
                    current_schema = stage.output_schema.clone();
                    stats.rows_after_filter = rows.len();
                    stats.intermediate_rows = stats.intermediate_rows.max(rows.len());
                    stats.pipeline_rows_materialized =
                        stats.pipeline_rows_materialized.max(rows.len());
                    pipeline_enforce_intermediate_rows(
                        rows.len(),
                        &pipeline.options,
                        "max_pipeline_rows",
                    )?;
                    if include_plan {
                        stage_explains.push(GraphPipelineStageExplain {
                            index: stage_index,
                            kind: "Call".to_string(),
                            detail: pipeline_call_stage_detail(
                                stage,
                                Some(before),
                                Some(rows.len()),
                                Some((
                                    execution.subquery_invocations,
                                    execution.subquery_cache_hits,
                                )),
                            ),
                            columns: pipeline_schema_columns(&current_schema),
                            graph_row: None,
                            warnings: Vec::new(),
                            notes: pipeline_call_stage_notes(stage, None),
                        });
                    }
                }
                NormalizedGraphPipelineStage::Union(stage) => {
                    let execution = self.execute_pipeline_union_stage(
                        stage,
                        effective_at_epoch,
                        include_plan,
                        stage_index,
                        rows,
                        subquery_budget,
                    )?;
                    followups.extend(execution.followups);
                    warnings.extend(execution.warnings.clone());
                    stats.merge_from(&execution.stats);
                    rows = execution.rows;
                    row_projections = Some(execution.row_projections);
                    current_schema = stage.output_schema.clone();
                    stats.rows_after_filter = rows.len();
                    stats.intermediate_rows = stats.intermediate_rows.max(rows.len());
                    stats.pipeline_rows_materialized =
                        stats.pipeline_rows_materialized.max(rows.len());
                    pipeline_enforce_intermediate_rows(
                        rows.len(),
                        &pipeline.options,
                        "max_pipeline_rows",
                    )?;
                    if include_plan {
                        stage_explains.extend(execution.stage_explains);
                    }
                }
            }
        }
        stats.warnings = warnings.clone();
        Ok(PipelineStagesExecution {
            rows,
            row_projections,
            followups,
            stage_explains,
            warnings,
            stats,
        })
    }

    fn execute_graph_row_stage(
        &self,
        query: &NormalizedGraphRowQuery,
        initial_rows: Option<Vec<crate::graph_row::GraphBindingRow>>,
        effective_at_epoch: i64,
        include_plan: bool,
        optional_stage: bool,
    ) -> Result<GraphRowStageExecution, EngineError> {
        #[cfg(test)]
        self.query_execution_counters
            .graph_row_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let runtime = self.normalize_graph_row_runtime_plan(query)?;
        let physical_plan = self.plan_graph_row_physical(query, &runtime, include_plan)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let cursor_state = GraphRowCursorState {
            decoded: None,
            effective_at_epoch,
            original_skip: 0,
            rows_emitted_after_skip: 0,
        };
        let mut explain_trace = if include_plan {
            let mut trace = GraphRowExplainTrace::default();
            self.populate_graph_row_explain_trace_from_runtime(
                query,
                &cursor_state,
                &runtime,
                &physical_plan,
                &mut trace,
            )?;
            Some(trace)
        } else {
            None
        };
        let mut followups = Vec::new();
        let initial_row_count = initial_rows.as_ref().map_or(0, Vec::len);
        let mut optional_seed_misses = Vec::new();
        let initial_rows = match initial_rows {
            Some(rows) => {
                let (valid_rows, invalid_rows) = self
                    .graph_row_partition_initial_bound_node_constraint_rows(
                        query,
                        &runtime,
                        rows,
                        policy_cutoffs.as_ref(),
                    )?;
                if optional_stage {
                    optional_seed_misses = invalid_rows
                        .into_iter()
                        .map(|row| self.graph_row_null_extend_initial_optional_miss_row(query, row))
                        .collect::<Result<Vec<_>, EngineError>>()?;
                }
                Some(valid_rows)
            }
            None => None,
        };
        let mut intermediate_peak = initial_row_count.max(optional_seed_misses.len());
        let mut frontier_peak = 0usize;
        let mut paths_enumerated = 0usize;
        let mut rows = self.graph_row_execute_runtime_plan(
            query,
            &runtime,
            &physical_plan,
            initial_rows,
            GraphRowRuntimeGoal::AllRows,
            effective_at_epoch,
            policy_cutoffs.as_ref(),
            &mut followups,
            &mut frontier_peak,
            &mut intermediate_peak,
            &mut paths_enumerated,
            explain_trace.as_mut(),
        )?;
        if !optional_seed_misses.is_empty() {
            rows.extend(optional_seed_misses);
            graph_row_record_cap_peak(
                &mut intermediate_peak,
                rows.len(),
                "max_intermediate_bindings",
                query.options.max_intermediate_bindings,
            )?;
        }
        let residual_needs = query.projection_needs.residual.clone();
        if rows.len() > query.options.max_order_materialization
            && graph_row_entity_needs_require_selected_field_reads(&residual_needs)
        {
            return Err(graph_row_cap_error(
                "max_order_materialization",
                query.options.max_order_materialization,
            ));
        }
        self.hydrate_graph_rows_for_needs(&mut rows, &query.binding_schema, &residual_needs)?;
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(where_expr) = query.bound_where.as_ref() {
                let context = crate::graph_row::BoundGraphEvalContext { row: &row };
                if !crate::graph_row::eval_bound_graph_predicate(where_expr, &context)? {
                    continue;
                }
            }
            filtered.push(row);
        }
        let rows_after_filter = filtered.len();
        let explain = if include_plan {
            Some(build_graph_row_explain(
                query,
                Some(effective_at_epoch),
                &cursor_state,
                explain_trace,
                Some(GraphRowExplainRuntimeStats {
                    rows_returned: rows_after_filter,
                    rows_after_filter,
                    rows_seen_for_page: rows_after_filter,
                    intermediate_bindings_peak: intermediate_peak,
                    frontier_peak,
                    paths_enumerated,
                    next_cursor: false,
                }),
            ))
        } else {
            None
        };
        Ok(GraphRowStageExecution {
            rows: filtered,
            followups,
            explain,
            warnings: graph_row_runtime_warnings(&runtime.warnings),
            rows_after_filter,
            intermediate_peak,
        })
    }

    fn execute_graph_row_stage_exists(
        &self,
        query: &NormalizedGraphRowQuery,
        initial_rows: Option<Vec<crate::graph_row::GraphBindingRow>>,
        effective_at_epoch: i64,
    ) -> Result<GraphRowStageExecution, EngineError> {
        #[cfg(test)]
        self.query_execution_counters
            .graph_row_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let runtime = self.normalize_graph_row_runtime_plan(query)?;
        let physical_plan = self.plan_graph_row_physical(query, &runtime, false)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let mut followups = Vec::new();
        let initial_row_count = initial_rows.as_ref().map_or(0, Vec::len);
        let initial_rows = match initial_rows {
            Some(rows) => {
                let (valid_rows, _invalid_rows) = self
                    .graph_row_partition_initial_bound_node_constraint_rows(
                        query,
                        &runtime,
                        rows,
                        policy_cutoffs.as_ref(),
                    )?;
                Some(valid_rows)
            }
            None => None,
        };
        let mut intermediate_peak = initial_row_count;
        let mut frontier_peak = 0usize;
        let mut paths_enumerated = 0usize;
        let rows = self.graph_row_execute_runtime_plan(
            query,
            &runtime,
            &physical_plan,
            initial_rows,
            GraphRowRuntimeGoal::ExistsOne,
            effective_at_epoch,
            policy_cutoffs.as_ref(),
            &mut followups,
            &mut frontier_peak,
            &mut intermediate_peak,
            &mut paths_enumerated,
            None,
        )?;
        let exists = !rows.is_empty();
        let row_count = usize::from(exists);
        Ok(GraphRowStageExecution {
            rows: if exists {
                rows.into_iter().take(1).collect()
            } else {
                Vec::new()
            },
            followups,
            explain: None,
            warnings: graph_row_runtime_warnings(&runtime.warnings),
            rows_after_filter: row_count,
            intermediate_peak: intermediate_peak.max(row_count),
        })
    }

    fn execute_pipeline_project_stage(
        &self,
        stage: &NormalizedPipelineProjectStage,
        mut rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        options: &GraphPipelineOptions,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineProjectStageExecution, EngineError> {
        self.hydrate_graph_rows_for_needs(&mut rows, &stage.input_schema, &stage.input_needs)?;
        let mut groups = 0;
        let mut collect_items = 0;
        let mut aggregate_distinct_keys = 0;
        let mut projected = if let Some(aggregate) = stage.aggregate.as_ref() {
            let outcome =
                execute_pipeline_aggregate_stage(stage, aggregate, rows, options)?;
            groups = outcome.groups;
            collect_items = outcome.collect_items;
            aggregate_distinct_keys = outcome.aggregate_distinct_keys;
            outcome.rows
        } else {
            execute_pipeline_scalar_project_stage(stage, &rows, options)?
        };

        let distinct_keys = if stage.distinct {
            pipeline_apply_distinct(stage, &mut projected, options)?
        } else {
            0
        };
        groups = groups.saturating_add(distinct_keys);

        if !stage.order_by.is_empty() {
            if projected.len() > options.max_order_materialization {
                return Err(graph_row_cap_error(
                    "max_order_materialization",
                    options.max_order_materialization,
                ));
            }
            if !pipeline_projection_needs_is_empty(&stage.order_needs) {
                self.hydrate_graph_rows_for_needs(
                    &mut projected,
                    &stage.output_schema,
                    &stage.order_needs,
                )?;
            }
            sort_pipeline_rows(&mut projected, &stage.output_schema, &stage.order_by)?;
        }

        if stage.skip > options.max_skip {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline projection SKIP {} exceeds max_skip {}",
                stage.skip, options.max_skip
            )));
        }
        if stage.skip > 0 {
            let skip = stage.skip.min(projected.len());
            projected.drain(0..skip);
        }
        if let Some(limit) = stage.limit {
            projected.truncate(limit);
        }

        if !pipeline_projection_needs_is_empty(&stage.filter_needs) {
            self.hydrate_graph_rows_for_needs(
                &mut projected,
                &stage.output_schema,
                &stage.filter_needs,
            )?;
        }
        let exists_stats = if stage.exists_predicates.is_empty() {
            PipelineSubqueryEvalStats::default()
        } else {
            self.evaluate_pipeline_exists_predicates(
                stage,
                &mut projected,
                effective_at_epoch,
                subquery_budget,
            )?
        };
        if let Some(where_expr) = stage.where_expr.as_ref() {
            let mut filtered = Vec::with_capacity(projected.len());
            for row in projected {
                let context = crate::graph_row::BoundGraphEvalContext { row: &row };
                if crate::graph_row::eval_bound_graph_predicate(where_expr, &context)? {
                    filtered.push(row);
                }
            }
            projected = filtered;
        }

        Ok(PipelineProjectStageExecution {
            rows: projected,
            followups: exists_stats.followups,
            groups,
            collect_items,
            aggregate_distinct_keys,
            subquery_invocations: exists_stats.invocations,
            subquery_cache_hits: exists_stats.cache_hits,
            nested_stats: exists_stats.nested_stats,
        })
    }

    fn apply_pipeline_optional_candidate_filter(
        &self,
        stage: &NormalizedPipelineMatchStage,
        left_rows: &[crate::graph_row::GraphBindingRow],
        rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineOptionalCandidateFilterExecution, EngineError> {
        let filter = stage.optional_candidate_filter.as_ref().ok_or_else(|| {
            EngineError::InvalidOperation(
                "optional candidate filter execution requires a normalized filter".to_string(),
            )
        })?;
        let input_rows = left_rows.len();
        let raw_rows = rows.len();
        let mut miss_rows_by_key: BTreeMap<
            Vec<crate::graph_row::GraphCanonicalKey>,
            Vec<crate::graph_row::GraphBindingRow>,
        > = BTreeMap::new();
        let mut candidate_rows = Vec::new();
        for row in rows {
            let key = pipeline_optional_input_key(stage, &row)?;
            if pipeline_optional_row_is_miss(stage, &row)? {
                miss_rows_by_key.entry(key).or_default().push(row);
            } else {
                candidate_rows.push(row);
            }
        }

        let mut eval_rows = candidate_rows
            .iter()
            .map(|row| {
                pipeline_copy_row_to_schema(
                    &stage.query.binding_schema,
                    &filter.eval_schema,
                    row,
                )
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        if !pipeline_projection_needs_is_empty(&filter.filter_needs) {
            self.hydrate_graph_rows_for_needs(
                &mut eval_rows,
                &filter.eval_schema,
                &filter.filter_needs,
            )?;
        }
        let exists_stats = if filter.exists_predicates.is_empty() {
            PipelineSubqueryEvalStats {
                nested_stats: empty_graph_pipeline_stats(effective_at_epoch),
                ..PipelineSubqueryEvalStats::default()
            }
        } else {
            self.evaluate_pipeline_exists_predicates_for_schema(
                &filter.eval_schema,
                &filter.exists_predicates,
                &mut eval_rows,
                effective_at_epoch,
                subquery_budget,
            )?
        };

        let mut hits_by_key: BTreeMap<
            Vec<crate::graph_row::GraphCanonicalKey>,
            Vec<crate::graph_row::GraphBindingRow>,
        > = BTreeMap::new();
        let mut passed_rows = 0usize;
        for (row, eval_row) in candidate_rows.into_iter().zip(eval_rows) {
            let context = crate::graph_row::BoundGraphEvalContext { row: &eval_row };
            if crate::graph_row::eval_bound_graph_predicate(&filter.where_expr, &context)? {
                passed_rows = passed_rows.saturating_add(1);
                let key = pipeline_optional_input_key(stage, &row)?;
                hits_by_key.entry(key).or_default().push(row);
            }
        }

        let mut output = Vec::new();
        let mut preserved_miss_rows = 0usize;
        let mut synthesized_miss_rows = 0usize;
        for left in left_rows {
            let key = pipeline_optional_input_key(stage, left)?;
            if let Some(hits) = hits_by_key.get(&key).filter(|hits| !hits.is_empty()) {
                output.extend(hits.iter().cloned());
            } else if let Some(misses) = miss_rows_by_key.get(&key).filter(|misses| !misses.is_empty()) {
                preserved_miss_rows = preserved_miss_rows.saturating_add(misses.len());
                output.extend(misses.iter().cloned());
            } else {
                synthesized_miss_rows = synthesized_miss_rows.saturating_add(1);
                output.push(pipeline_optional_null_extend_row(stage, left)?);
            }
        }

        Ok(PipelineOptionalCandidateFilterExecution {
            rows: output,
            followups: exists_stats.followups,
            subquery_invocations: exists_stats.invocations,
            subquery_cache_hits: exists_stats.cache_hits,
            nested_stats: exists_stats.nested_stats,
            input_rows,
            candidate_rows: raw_rows,
            passed_rows,
            preserved_miss_rows,
            synthesized_miss_rows,
        })
    }

    fn execute_pipeline_shortest_path_stage(
        &self,
        stage: &NormalizedPipelineShortestPathStage,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        options: &GraphPipelineOptions,
    ) -> Result<PipelineShortestPathStageExecution, EngineError> {
        let effective_max_paths = match stage.mode {
            GraphShortestPathMode::One => None,
            GraphShortestPathMode::All => {
                Some(stage.max_paths.unwrap_or(options.max_paths_per_start))
            }
        };
        let from_endpoint = self.prepare_shortest_path_endpoint(&stage.from)?;
        let to_endpoint = self.prepare_shortest_path_endpoint(&stage.to)?;
        let max_cost_bits = stage.max_cost.map(f64::to_bits);
        let mut row_keys = Vec::with_capacity(rows.len());
        let mut distinct_pairs = BTreeSet::new();
        let mut cache_hits = 0usize;
        for row in &rows {
            let from_id = resolve_shortest_path_endpoint(&from_endpoint, row)?;
            let to_id = resolve_shortest_path_endpoint(&to_endpoint, row)?;
            let Some((from_id, to_id)) = from_id.zip(to_id) else {
                row_keys.push(None);
                continue;
            };
            let key = ShortestPathPairKey {
                from_id,
                to_id,
                direction: shortest_path_direction_code(stage.direction),
                edge_label_filter: stage.edge_label_filter.clone(),
                min_hops: stage.min_hops,
                max_hops: stage.max_hops,
                weight_field: stage.weight_field.clone(),
                max_cost_bits,
                max_paths: effective_max_paths,
            };
            if !distinct_pairs.insert(key.clone()) {
                cache_hits = cache_hits.saturating_add(1);
            }
            row_keys.push(Some(key));
        }

        if distinct_pairs.len() > options.max_shortest_path_pairs {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline shortest-path stage resolved {} distinct endpoint pair(s), exceeding max_shortest_path_pairs {}",
                distinct_pairs.len(),
                options.max_shortest_path_pairs
            )));
        }

        let mut cache: BTreeMap<ShortestPathPairKey, Vec<ShortestPath>> = BTreeMap::new();
        for key in distinct_pairs {
            let edge_label_filter = if key.edge_label_filter.is_empty() {
                None
            } else {
                Some(key.edge_label_filter.clone())
            };
            let paths = match stage.mode {
                GraphShortestPathMode::One => {
                    let options = ShortestPathOptions {
                        direction: stage.direction,
                        edge_label_filter,
                        weight_field: key.weight_field.clone(),
                        at_epoch: Some(effective_at_epoch),
                        max_depth: Some(key.max_hops as u32),
                        max_cost: key.max_cost_bits.map(f64::from_bits),
                    };
                    self.shortest_path(key.from_id, key.to_id, &options)?
                        .into_iter()
                        .filter(|path| path.edges.len() >= key.min_hops as usize)
                        .collect::<Vec<_>>()
                }
                GraphShortestPathMode::All => {
                    let options = AllShortestPathsOptions {
                        direction: stage.direction,
                        edge_label_filter,
                        weight_field: key.weight_field.clone(),
                        at_epoch: Some(effective_at_epoch),
                        max_depth: Some(key.max_hops as u32),
                        max_cost: key.max_cost_bits.map(f64::from_bits),
                        max_paths: key.max_paths,
                    };
                    self.all_shortest_paths(key.from_id, key.to_id, &options)?
                        .into_iter()
                        .filter(|path| path.edges.len() >= key.min_hops as usize)
                        .collect::<Vec<_>>()
                }
            };
            cache.insert(key, paths);
        }

        let pair_count = cache.len();
        let mut output_rows = Vec::new();
        let mut no_path_count = 0usize;
        let mut emitted_path_count = 0usize;
        for (row, key) in rows.into_iter().zip(row_keys) {
            let Some(key) = key else {
                no_path_count = no_path_count.saturating_add(1);
                if stage.optional {
                    pipeline_enforce_intermediate_rows(
                        output_rows.len().saturating_add(1),
                        options,
                        "max_pipeline_rows",
                    )?;
                    output_rows.push(shortest_path_null_output_row(stage, &row)?);
                }
                continue;
            };
            let paths = cache.get(&key).expect("shortest-path pair key cached");
            if paths.is_empty() {
                no_path_count = no_path_count.saturating_add(1);
                if stage.optional {
                    pipeline_enforce_intermediate_rows(
                        output_rows.len().saturating_add(1),
                        options,
                        "max_pipeline_rows",
                    )?;
                    output_rows.push(shortest_path_null_output_row(stage, &row)?);
                }
                continue;
            }
            for path in paths {
                pipeline_enforce_intermediate_rows(
                    output_rows.len().saturating_add(1),
                    options,
                    "max_pipeline_rows",
                )?;
                output_rows.push(shortest_path_output_row(stage, &row, path.clone())?);
                emitted_path_count = emitted_path_count.saturating_add(1);
                if stage.mode == GraphShortestPathMode::One {
                    break;
                }
            }
        }

        Ok(PipelineShortestPathStageExecution {
            rows: output_rows,
            pair_count,
            cache_hits,
            no_path_count,
            emitted_path_count,
        })
    }

    fn prepare_shortest_path_endpoint(
        &self,
        endpoint: &NormalizedShortestPathEndpoint,
    ) -> Result<ResolvedShortestPathEndpoint, EngineError> {
        match endpoint {
            NormalizedShortestPathEndpoint::Alias { slot, .. } => {
                Ok(ResolvedShortestPathEndpoint::Alias { slot: *slot })
            }
            NormalizedShortestPathEndpoint::NodeId(id) => {
                Ok(ResolvedShortestPathEndpoint::Static(Some(*id)))
            }
            NormalizedShortestPathEndpoint::NodeKey { label, key } => {
                let Some(label_id) = self.label_catalog.resolve_node_label_for_read(label)? else {
                    return Ok(ResolvedShortestPathEndpoint::Static(None));
                };
                let resolved = self
                    .sources()
                    .find_node_ids_by_label_keys(&[(label_id, key.as_str())])?
                    .pop()
                    .flatten();
                let Some(node_id) = resolved else {
                    return Ok(ResolvedShortestPathEndpoint::Static(None));
                };
                if self.policy_excluded_node_ids(&[node_id])?.contains(&node_id) {
                    return Ok(ResolvedShortestPathEndpoint::Static(None));
                }
                Ok(ResolvedShortestPathEndpoint::Static(Some(node_id)))
            }
        }
    }

    fn execute_pipeline_union_stage(
        &self,
        stage: &NormalizedPipelineUnionStage,
        effective_at_epoch: i64,
        include_plan: bool,
        stage_index: usize,
        initial_rows: Vec<crate::graph_row::GraphBindingRow>,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineUnionStageExecution, EngineError> {
        let mut followups = Vec::new();
        let mut warnings = Vec::new();
        let initial_row_count = initial_rows.len();
        let mut stats = GraphPipelineStats {
            rows_returned: 0,
            rows_entered_pipeline: initial_row_count,
            rows_after_filter: 0,
            intermediate_rows: initial_row_count,
            pipeline_rows_materialized: initial_row_count,
            groups: 0,
            collect_items: 0,
            union_branches: stage.branches.len(),
            union_dedup_keys: 0,
            subquery_invocations: 0,
            subquery_cache_hits: 0,
            shortest_path_pairs: 0,
            shortest_path_cache_hits: 0,
            db_hits: 0,
            elapsed_us: None,
            effective_at_epoch,
            warnings: Vec::new(),
        };
        let mut rows = Vec::new();
        let mut row_projections = Vec::new();
        let mut ordinal = 0u64;
        let mut branch_summaries = Vec::new();
        let branch_count = stage.branches.len();
        let mut reusable_initial_rows = Some(initial_rows);
        for (branch_index, branch) in stage.branches.iter().enumerate() {
            let branch_initial_rows = if branch_index + 1 == branch_count {
                reusable_initial_rows
                    .take()
                    .expect("initial union rows available for final branch")
            } else {
                reusable_initial_rows
                    .as_ref()
                    .expect("initial union rows available for branch")
                    .clone()
            };
            let mut branch_execution = self.execute_graph_pipeline_stages_with_rows_budget(
                &branch.pipeline,
                effective_at_epoch,
                include_plan,
                branch_initial_rows,
                subquery_budget,
            )?;
            let branch_warnings = branch_execution.warnings.clone();
            if include_plan {
                branch_summaries.push(PipelineUnionBranchExplainSummary {
                    branch_index,
                    stages: std::mem::take(&mut branch_execution.stage_explains),
                    row_ops: pipeline_row_ops(&branch.pipeline),
                    warnings: branch_warnings.clone(),
                });
            }
            followups.append(&mut branch_execution.followups);
            warnings.extend(branch_warnings);
            stats.merge_from(&branch_execution.stats);
            let branch_fingerprints =
                graph_pipeline_cursor_fingerprints(&branch.pipeline, effective_at_epoch, 0);
            let branch_rows = self.pipeline_prepare_final_rows(
                branch_execution.rows,
                &branch.pipeline,
                None,
                &branch_fingerprints,
                branch_execution.row_projections,
            )?;
            let branch_projections = pipeline_union_branch_return_projections(branch);
            stats.pipeline_rows_materialized =
                stats.pipeline_rows_materialized.max(branch_rows.len());
            for branch_row in branch_rows {
                let projections = branch_row
                    .projections
                    .unwrap_or_else(|| Arc::clone(&branch_projections));
                let output = pipeline_union_output_row(
                    stage,
                    branch,
                    branch_row.row,
                    ordinal,
                )?;
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "GraphUnionStage internal ordinal overflowed".to_string(),
                    )
                })?;
                rows.push(output);
                row_projections.push(projections);
            }
        }

        let dedup_keys = if stage.all {
            0
        } else {
            pipeline_apply_union_distinct(stage, &mut rows, &mut row_projections)?
        };
        stats.union_dedup_keys = stats.union_dedup_keys.saturating_add(dedup_keys);
        stats.rows_after_filter = rows.len();
        stats.intermediate_rows = stats.intermediate_rows.max(rows.len());
        stats.pipeline_rows_materialized = stats.pipeline_rows_materialized.max(rows.len());
        stats.warnings = warnings.clone();
        let stage_explains = if include_plan { {
                vec![GraphPipelineStageExplain {
                    index: stage_index,
                    kind: if stage.all {
                        "UnionAll".to_string()
                    } else {
                        "Union".to_string()
                    },
                    detail: pipeline_union_stage_detail(stage, Some(rows.len()), Some(dedup_keys)),
                    columns: stage.columns.clone(),
                    graph_row: None,
                    warnings: warnings.clone(),
                    notes: pipeline_union_stage_notes(stage, &branch_summaries),
                }]
            } } else { Default::default() };
        Ok(PipelineUnionStageExecution {
            rows,
            row_projections,
            followups,
            stage_explains,
            warnings,
            stats,
        })
    }

    fn execute_pipeline_call_stage(
        &self,
        stage: &NormalizedPipelineCallStage,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        options: &GraphPipelineOptions,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineCallStageExecution, EngineError> {
        let mut row_keys = Vec::with_capacity(rows.len());
        let mut representatives: BTreeMap<
            Vec<crate::graph_row::GraphCanonicalKey>,
            PipelineCallRepresentative,
        > = BTreeMap::new();
        let mut representative_count = 0usize;
        let mut cache_hits = 0usize;
        for (index, row) in rows.iter().enumerate() {
            let key = crate::graph_row::graph_canonical_key_for_row_slots(
                row,
                &stage.import_slots,
            )?;
            match representatives.entry(key.clone()) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    cache_hits = cache_hits.saturating_add(1);
                    entry.get_mut().outer_count = entry.get().outer_count.saturating_add(1);
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    if representative_count >= options.max_subquery_invocations {
                        return Err(EngineError::InvalidOperation(format!(
                            "graph pipeline CALL exceeded max_subquery_invocations {}",
                            options.max_subquery_invocations
                        )));
                    }
                    subquery_budget.reserve("CALL")?;
                    entry.insert(PipelineCallRepresentative {
                        index,
                        outer_count: 1,
                    });
                    representative_count = representative_count.saturating_add(1);
                }
            }
            row_keys.push(key);
        }

        let mut followups = Vec::new();
        let mut nested_stats = empty_graph_pipeline_stats(effective_at_epoch);
        let mut cache: BTreeMap<
            Vec<crate::graph_row::GraphCanonicalKey>,
            Vec<crate::graph_row::GraphBindingRow>,
        > = BTreeMap::new();
        let mut cached_subquery_rows = 0usize;
        let mut projected_join_rows = 0usize;
        for (key, representative) in representatives.iter() {
            let initial_rows = pipeline_bridge_rows(
                std::slice::from_ref(&rows[representative.index]),
                &stage.input_schema,
                &stage.query.initial_schema,
                &stage.import_mappings,
            )?;
            let (subquery_rows, mut subquery_followups, subquery_stats) =
                self.execute_pipeline_subquery_rows(
                    &stage.query,
                    effective_at_epoch,
                    initial_rows,
                    subquery_budget,
                    PipelineSubqueryRowMode::Call,
                )?;
            followups.append(&mut subquery_followups);
            nested_stats.merge_from(&subquery_stats);
            cached_subquery_rows = cached_subquery_rows
                .checked_add(subquery_rows.len())
                .ok_or_else(|| graph_row_cap_error("max_pipeline_rows", options.max_pipeline_rows))?;
            pipeline_enforce_intermediate_rows(
                cached_subquery_rows,
                options,
                "max_pipeline_rows",
            )?;
            let contribution = subquery_rows
                .len()
                .checked_mul(representative.outer_count)
                .ok_or_else(|| graph_row_cap_error("max_pipeline_rows", options.max_pipeline_rows))?;
            projected_join_rows = projected_join_rows
                .checked_add(contribution)
                .ok_or_else(|| graph_row_cap_error("max_pipeline_rows", options.max_pipeline_rows))?;
            pipeline_enforce_intermediate_rows(
                projected_join_rows,
                options,
                "max_pipeline_rows",
            )?;
            cache.insert(key.clone(), subquery_rows);
        }

        let mut output_rows = Vec::new();
        for (row, key) in rows.iter().zip(row_keys.iter()) {
            let Some(subquery_rows) = cache.get(key) else {
                return Err(EngineError::InvalidOperation(
                    "graph pipeline CALL cache is missing a correlation key".to_string(),
                ));
            };
            for subquery_row in subquery_rows {
                pipeline_enforce_intermediate_rows(
                    output_rows.len().saturating_add(1),
                    options,
                    "max_pipeline_rows",
                )?;
                output_rows.push(pipeline_call_output_row(stage, row, subquery_row)?);
            }
        }

        Ok(PipelineCallStageExecution {
            rows: output_rows,
            followups,
            subquery_invocations: cache.len(),
            subquery_cache_hits: cache_hits,
            nested_stats,
        })
    }

    fn evaluate_pipeline_exists_predicates(
        &self,
        stage: &NormalizedPipelineProjectStage,
        rows: &mut [crate::graph_row::GraphBindingRow],
        effective_at_epoch: i64,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineSubqueryEvalStats, EngineError> {
        self.evaluate_pipeline_exists_predicates_for_schema(
            &stage.output_schema,
            &stage.exists_predicates,
            rows,
            effective_at_epoch,
            subquery_budget,
        )
    }

    fn evaluate_pipeline_exists_predicates_for_schema(
        &self,
        schema: &crate::graph_row::GraphBindingSchema,
        predicates: &[NormalizedPipelineExistsPredicate],
        rows: &mut [crate::graph_row::GraphBindingRow],
        effective_at_epoch: i64,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineSubqueryEvalStats, EngineError> {
        let mut stats = PipelineSubqueryEvalStats {
            nested_stats: empty_graph_pipeline_stats(effective_at_epoch),
            ..PipelineSubqueryEvalStats::default()
        };
        for predicate in predicates {
            let mut row_keys = Vec::with_capacity(rows.len());
            let mut representatives: BTreeMap<
                Vec<crate::graph_row::GraphCanonicalKey>,
                usize,
            > = BTreeMap::new();
            for (index, row) in rows.iter().enumerate() {
                let key = crate::graph_row::graph_canonical_key_for_row_slots(
                    row,
                    &predicate.import_slots,
                )?;
                if representatives.contains_key(&key) {
                    stats.cache_hits = stats.cache_hits.saturating_add(1);
                } else {
                    subquery_budget.reserve("EXISTS")?;
                    representatives.insert(key.clone(), index);
                }
                row_keys.push(key);
            }

            let mut cache: BTreeMap<Vec<crate::graph_row::GraphCanonicalKey>, bool> =
                BTreeMap::new();
            for (key, index) in representatives.iter() {
                let initial_rows = pipeline_bridge_rows(
                    std::slice::from_ref(&rows[*index]),
                    schema,
                    &predicate.query.initial_schema,
                    &predicate.import_mappings,
                )?;
                let mut execution = self.execute_pipeline_subquery_exists(
                    &predicate.query,
                    effective_at_epoch,
                    initial_rows,
                    subquery_budget,
                )?;
                stats.followups.append(&mut execution.followups);
                stats.nested_stats.merge_from(&execution.stats);
                cache.insert(key.clone(), execution.exists);
            }
            stats.invocations = stats.invocations.saturating_add(cache.len());

            for (row, key) in rows.iter_mut().zip(row_keys.iter()) {
                let exists = *cache.get(key).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "graph pipeline EXISTS cache is missing a correlation key".to_string(),
                    )
                })?;
                bind_pipeline_value_to_slot(
                    schema,
                    row,
                    predicate.output_slot,
                    crate::graph_row::GraphEvalValue::Bool(exists),
                )?;
            }
        }
        Ok(stats)
    }

    fn execute_pipeline_subquery_exists(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        initial_rows: Vec<crate::graph_row::GraphBindingRow>,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineExistsExecution, EngineError> {
        if matches!(
            pipeline.stages.as_slice(),
            [NormalizedGraphPipelineStage::Union(_)]
        ) {
            return self.execute_pipeline_subquery_union_exists(
                pipeline,
                effective_at_epoch,
                initial_rows,
                subquery_budget,
            );
        }
        if let Some(execution) =
            self.execute_pipeline_subquery_exists_probe(pipeline, effective_at_epoch, &initial_rows)?
        {
            return Ok(execution);
        }

        let (rows, followups, stats) = self.execute_pipeline_subquery_rows(
            pipeline,
            effective_at_epoch,
            initial_rows,
            subquery_budget,
            PipelineSubqueryRowMode::Exists,
        )?;
        Ok(PipelineExistsExecution {
            exists: !rows.is_empty(),
            followups,
            stats,
        })
    }

    fn execute_pipeline_subquery_union_exists(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        initial_rows: Vec<crate::graph_row::GraphBindingRow>,
        subquery_budget: &mut SubqueryInvocationBudget,
    ) -> Result<PipelineExistsExecution, EngineError> {
        let [NormalizedGraphPipelineStage::Union(stage)] = pipeline.stages.as_slice() else {
            return Err(EngineError::InvalidOperation(
                "graph pipeline EXISTS union probe requires a terminal union stage".to_string(),
            ));
        };
        let mut stats = empty_graph_pipeline_stats(effective_at_epoch);
        stats.rows_entered_pipeline = initial_rows.len();
        stats.intermediate_rows = initial_rows.len();
        stats.pipeline_rows_materialized = initial_rows.len();
        stats.union_branches = stage.branches.len();
        if initial_rows.is_empty() {
            return Ok(PipelineExistsExecution {
                exists: false,
                followups: Vec::new(),
                stats,
            });
        }

        let mut followups = Vec::new();
        let branch_count = stage.branches.len();
        let mut reusable_initial_rows = Some(initial_rows);
        for (branch_index, branch) in stage.branches.iter().enumerate() {
            let branch_initial_rows = if branch_index + 1 == branch_count {
                reusable_initial_rows
                    .take()
                    .expect("initial union rows available for final EXISTS branch")
            } else {
                reusable_initial_rows
                    .as_ref()
                    .expect("initial union rows available for EXISTS branch")
                    .clone()
            };
            let mut branch_execution = self.execute_pipeline_subquery_exists(
                &branch.pipeline,
                effective_at_epoch,
                branch_initial_rows,
                subquery_budget,
            )?;
            followups.append(&mut branch_execution.followups);
            stats.merge_from(&branch_execution.stats);
            if branch_execution.exists {
                stats.rows_returned = 1;
                stats.rows_after_filter = 1;
                stats.pipeline_rows_materialized = stats.pipeline_rows_materialized.max(1);
                return Ok(PipelineExistsExecution {
                    exists: true,
                    followups,
                    stats,
                });
            }
        }

        stats.rows_returned = 0;
        stats.rows_after_filter = 0;
        Ok(PipelineExistsExecution {
            exists: false,
            followups,
            stats,
        })
    }

    fn execute_pipeline_subquery_exists_probe(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        initial_rows: &[crate::graph_row::GraphBindingRow],
    ) -> Result<Option<PipelineExistsExecution>, EngineError> {
        let Some(plan) = pipeline_exists_probe_plan(pipeline) else {
            return Ok(None);
        };
        if plan.always_false || initial_rows.is_empty() {
            let mut stats = empty_graph_pipeline_stats(effective_at_epoch);
            stats.rows_entered_pipeline = initial_rows.len();
            return Ok(Some(PipelineExistsExecution {
                exists: false,
                followups: Vec::new(),
                stats,
            }));
        }

        let Some(match_stage) = plan.match_stage else {
            let mut stats = empty_graph_pipeline_stats(effective_at_epoch);
            stats.rows_entered_pipeline = initial_rows.len();
            stats.rows_returned = 1;
            stats.rows_after_filter = 1;
            stats.intermediate_rows = 1;
            stats.pipeline_rows_materialized = 1;
            return Ok(Some(PipelineExistsExecution {
                exists: true,
                followups: Vec::new(),
                stats,
            }));
        };

        let bridged_rows = pipeline_bridge_rows(
            initial_rows,
            &pipeline.initial_schema,
            &match_stage.query.binding_schema,
            &match_stage.input_mappings,
        )?;
        let bridged_rows = if pipeline.initial_schema.slots().is_empty() {
            None
        } else {
            Some(bridged_rows)
        };
        let execution = self.execute_graph_row_stage_exists(
            &match_stage.query,
            bridged_rows,
            effective_at_epoch,
        )?;
        let exists = !execution.rows.is_empty();
        let mut stats = empty_graph_pipeline_stats(effective_at_epoch);
        stats.rows_entered_pipeline = initial_rows.len();
        stats.rows_returned = usize::from(exists);
        stats.rows_after_filter = execution.rows_after_filter;
        stats.intermediate_rows = execution.intermediate_peak;
        stats.pipeline_rows_materialized = usize::from(exists);
        stats.db_hits = if pipeline.options.profile {
            execution.rows_after_filter
        } else {
            0
        };
        stats.warnings = execution.warnings.clone();
        Ok(Some(PipelineExistsExecution {
            exists,
            followups: execution.followups,
            stats,
        }))
    }

    fn execute_pipeline_subquery_rows(
        &self,
        pipeline: &NormalizedGraphPipeline,
        effective_at_epoch: i64,
        initial_rows: Vec<crate::graph_row::GraphBindingRow>,
        subquery_budget: &mut SubqueryInvocationBudget,
        mode: PipelineSubqueryRowMode,
    ) -> Result<
        (
            Vec<crate::graph_row::GraphBindingRow>,
            Vec<SecondaryIndexReadFollowup>,
            GraphPipelineStats,
        ),
        EngineError,
    > {
        let mut execution = self.execute_graph_pipeline_stages_with_rows_budget(
            pipeline,
            effective_at_epoch,
            false,
            initial_rows,
            subquery_budget,
        )?;
        let fingerprints = graph_pipeline_cursor_fingerprints(pipeline, effective_at_epoch, 0);
        let mut final_rows = self.pipeline_prepare_final_rows(
            execution.rows,
            pipeline,
            None,
            &fingerprints,
            execution.row_projections.take(),
        )?;
        let total_after_cursor = final_rows.len();
        match mode {
            PipelineSubqueryRowMode::Exists => {
                let limit = pipeline.page.limit.min(pipeline.options.max_rows);
                if final_rows.len() > limit {
                    final_rows.truncate(limit);
                }
            }
            PipelineSubqueryRowMode::Call => {
                if final_rows.len() > pipeline.options.max_pipeline_rows {
                    return Err(graph_row_cap_error(
                        "max_pipeline_rows",
                        pipeline.options.max_pipeline_rows,
                    ));
                }
            }
        }
        let rows = final_rows
            .into_iter()
            .map(|row| row.row)
            .collect::<Vec<_>>();
        let mut stats = execution.stats;
        stats.rows_returned = rows.len();
        stats.rows_after_filter = total_after_cursor;
        stats.pipeline_rows_materialized = stats.pipeline_rows_materialized.max(total_after_cursor);
        Ok((rows, execution.followups, stats))
    }

    fn pipeline_prepare_final_rows(
        &self,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        pipeline: &NormalizedGraphPipeline,
        cursor: Option<&GraphPipelineCursorPayload>,
        fingerprints: &GraphPipelineFingerprints,
        row_projections: Option<Vec<Arc<[GraphReturnProjection]>>>,
    ) -> Result<Vec<PipelineFinalRow>, EngineError> {
        if rows.len() > pipeline.options.max_order_materialization {
            return Err(graph_row_cap_error(
                "max_order_materialization",
                pipeline.options.max_order_materialization,
            ));
        }
        if let Some(projections) = row_projections.as_ref() {
            if projections.len() != rows.len() {
                return Err(EngineError::InvalidOperation(format!(
                    "graph pipeline row projection sidecar length {} does not match row length {}",
                    projections.len(),
                    rows.len()
                )));
            }
        }
        let mut rows = rows;
        let order_needs = pipeline_order_needs(&pipeline.terminal_schema, &pipeline.terminal_order_by)?;
        if !pipeline_projection_needs_is_empty(&order_needs) {
            self.hydrate_graph_rows_for_needs(&mut rows, &pipeline.terminal_schema, &order_needs)?;
        }
        let directions = graph_row_order_directions(&pipeline.terminal_order_by);
        let mut final_rows = match row_projections {
            Some(projections) => rows
                .into_iter()
                .zip(projections)
                .enumerate()
                .map(|(ordinal, (row, projections))| {
                    let sort_key = pipeline_explicit_sort_key(&pipeline.terminal_order_by, &row)?;
                    let logical_key = pipeline_final_logical_key(pipeline, ordinal, &row)?;
                    Ok(PipelineFinalRow {
                        row,
                        sort_key,
                        logical_key,
                        projections: Some(projections),
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            None => rows
                .into_iter()
                .enumerate()
                .map(|(ordinal, row)| {
                    let sort_key = pipeline_explicit_sort_key(&pipeline.terminal_order_by, &row)?;
                    let logical_key = pipeline_final_logical_key(pipeline, ordinal, &row)?;
                    Ok(PipelineFinalRow {
                        row,
                        sort_key,
                        logical_key,
                        projections: None,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
        };
        final_rows.sort_by(|left, right| {
            compare_graph_final_keys_by_directions(
                &left.sort_key,
                &left.logical_key,
                &right.sort_key,
                &right.logical_key,
                &directions,
            )
        });
        if let Some(cursor) = cursor {
            graph_pipeline_validate_cursor_fingerprints(cursor, fingerprints)?;
            graph_pipeline_validate_cursor_shape(pipeline, cursor)?;
            final_rows.retain(|row| {
                compare_graph_final_keys_by_directions(
                    &row.sort_key,
                    &row.logical_key,
                    &cursor.last_sort_key,
                    &cursor.last_logical_row_key,
                    &directions,
                )
                .is_gt()
            });
        }
        Ok(final_rows)
    }

    fn pipeline_project_output_rows(
        &self,
        rows: &[crate::graph_row::GraphBindingRow],
        return_items: &[crate::graph_row::BoundGraphReturnItem],
        output: &GraphOutputOptions,
    ) -> Result<Vec<GraphRow>, EngineError> {
        let mut eval_rows = Vec::with_capacity(rows.len());
        let mut hydration_needs = PipelineNestedGraphHydrationNeeds::default();
        for row in rows {
            let context = crate::graph_row::BoundGraphEvalContext { row };
            let mut values = Vec::with_capacity(return_items.len());
            for item in return_items {
                let value = crate::graph_row::eval_bound_graph_expr(&item.expr, &context)?;
                pipeline_collect_output_value_hydration_needs(
                    &value,
                    &item.projection,
                    output,
                    &mut hydration_needs,
                )?;
                values.push(value);
            }
            eval_rows.push(values);
        }

        let (nodes_by_id, edges_by_id) =
            self.pipeline_fetch_nested_graph_values(&hydration_needs)?;
        eval_rows
            .into_iter()
            .map(|values| {
                let values = values
                    .into_iter()
                    .zip(return_items)
                    .map(|(value, item)| {
                        let value =
                            pipeline_hydrate_output_value(value, &nodes_by_id, &edges_by_id)?;
                        crate::graph_row::graph_eval_to_output_value(
                            &value,
                            &item.projection,
                            output,
                        )
                    })
                    .collect::<Result<Vec<_>, EngineError>>()?;
                Ok(GraphRow { values })
            })
            .collect()
    }

    fn pipeline_project_output_rows_with_row_projections(
        &self,
        rows: &[PipelineFinalRow],
        return_items: &[crate::graph_row::BoundGraphReturnItem],
        output: &GraphOutputOptions,
    ) -> Result<Vec<GraphRow>, EngineError> {
        let mut eval_rows = Vec::with_capacity(rows.len());
        let mut hydration_needs = PipelineNestedGraphHydrationNeeds::default();
        for row in rows {
            if let Some(projections) = row.projections.as_ref() {
                if projections.len() != return_items.len() {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph pipeline row projection sidecar has {} projection(s), expected {}",
                        projections.len(),
                        return_items.len()
                    )));
                }
            }
            let context = crate::graph_row::BoundGraphEvalContext { row: &row.row };
            let mut values = Vec::with_capacity(return_items.len());
            for (index, item) in return_items.iter().enumerate() {
                let projection = row_projection_for_item(row, item, index)?;
                let value = crate::graph_row::eval_bound_graph_expr(&item.expr, &context)?;
                pipeline_collect_output_value_hydration_needs(
                    &value,
                    projection,
                    output,
                    &mut hydration_needs,
                )?;
                values.push(value);
            }
            eval_rows.push((values, row.projections.clone()));
        }

        let (nodes_by_id, edges_by_id) =
            self.pipeline_fetch_nested_graph_values(&hydration_needs)?;
        eval_rows
            .into_iter()
            .map(|(values, projections)| {
                let values = values
                    .into_iter()
                    .zip(return_items)
                    .enumerate()
                    .map(|(index, (value, item))| {
                        let value =
                            pipeline_hydrate_output_value(value, &nodes_by_id, &edges_by_id)?;
                        let projection = projections
                            .as_deref()
                            .and_then(|items| items.get(index))
                            .unwrap_or(&item.projection);
                        crate::graph_row::graph_eval_to_output_value(
                            &value,
                            projection,
                            output,
                        )
                    })
                    .collect::<Result<Vec<_>, EngineError>>()?;
                Ok(GraphRow { values })
            })
            .collect()
    }

    fn pipeline_fetch_nested_graph_values(
        &self,
        needs: &PipelineNestedGraphHydrationNeeds,
    ) -> Result<(NodeIdMap<GraphNodeValue>, NodeIdMap<GraphEdgeValue>), EngineError> {
        let mut nodes_by_id = NodeIdMap::default();
        for (node_needs, ids) in pipeline_group_node_hydration_needs(&needs.node_needs_by_id) {
            if !ids.is_empty() {
                let selected = self.sources().find_node_projected_fields(&ids, &node_needs)?;
                if nodes_by_id.capacity() == 0 {
                    nodes_by_id = NodeIdMap::with_capacity_and_hasher(
                        needs.node_needs_by_id.len(),
                        Default::default(),
                    );
                }
                for (node_id, fields) in ids.into_iter().zip(selected) {
                    if let Some(fields) = fields {
                        nodes_by_id.insert(
                            node_id,
                            graph_node_value_from_selected(
                                node_id,
                                &fields,
                                &self.label_catalog,
                            )?,
                        );
                    }
                }
            }
        }

        let mut edges_by_id = NodeIdMap::default();
        for (edge_needs, ids) in pipeline_group_edge_hydration_needs(&needs.edge_needs_by_id) {
            if !ids.is_empty() {
                let selected = self.sources().find_edge_projected_fields(&ids, &edge_needs)?;
                if edges_by_id.capacity() == 0 {
                    edges_by_id = NodeIdMap::with_capacity_and_hasher(
                        needs.edge_needs_by_id.len(),
                        Default::default(),
                    );
                }
                for (edge_id, fields) in ids.into_iter().zip(selected) {
                    if let Some(fields) = fields {
                        edges_by_id.insert(
                            edge_id,
                            graph_edge_value_from_selected(
                                edge_id,
                                &fields,
                                &self.label_catalog,
                            )?,
                        );
                    }
                }
            }
        }

        Ok((nodes_by_id, edges_by_id))
    }
}

#[derive(Debug)]
struct PipelineAggregateOutcome {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    groups: usize,
    collect_items: usize,
    aggregate_distinct_keys: usize,
}

struct PipelineAggregateGroup {
    values: Vec<crate::graph_row::GraphEvalValue>,
    states: Vec<PipelineAggregateState>,
}

struct PipelineAggregateState {
    function: GraphAggregateFunction,
    distinct_seen: Option<BTreeSet<Vec<crate::graph_row::GraphCanonicalKey>>>,
    inner: PipelineAggregateInner,
}

enum PipelineAggregateInner {
    Count(u64),
    Sum(PipelineSumState),
    Avg { sum: f64, count: u64 },
    MinMax(Option<crate::graph_row::GraphEvalValue>),
    Collect(Vec<crate::graph_row::GraphEvalValue>),
}

enum PipelineSumState {
    Empty,
    Int(i64),
    Float(f64),
}

#[derive(Default)]
struct PipelineNestedGraphHydrationNeeds {
    node_needs_by_id: BTreeMap<u64, NodeSelectedFieldNeeds>,
    edge_needs_by_id: BTreeMap<u64, EdgeSelectedFieldNeeds>,
}

impl PipelineNestedGraphHydrationNeeds {
    fn add_node(
        &mut self,
        node_id: u64,
        needs: &NodeSelectedFieldNeeds,
    ) -> Result<(), EngineError> {
        let mut merged = self.node_needs_by_id.get(&node_id).cloned().unwrap_or_default();
        merged.merge_from(needs, ProjectionNeedClass::Output)?;
        self.node_needs_by_id.insert(node_id, merged);
        Ok(())
    }

    fn add_edge(
        &mut self,
        edge_id: u64,
        needs: &EdgeSelectedFieldNeeds,
    ) -> Result<(), EngineError> {
        let mut merged = self.edge_needs_by_id.get(&edge_id).cloned().unwrap_or_default();
        merged.merge_from(needs, ProjectionNeedClass::Output)?;
        self.edge_needs_by_id.insert(edge_id, merged);
        Ok(())
    }
}

fn pipeline_group_node_hydration_needs(
    needs_by_id: &BTreeMap<u64, NodeSelectedFieldNeeds>,
) -> Vec<(NodeSelectedFieldNeeds, Vec<u64>)> {
    let mut groups: Vec<(NodeSelectedFieldNeeds, Vec<u64>)> = Vec::new();
    for (id, needs) in needs_by_id {
        if let Some((_, ids)) = groups
            .iter_mut()
            .find(|(group_needs, _)| group_needs == needs)
        {
            ids.push(*id);
        } else {
            groups.push((needs.clone(), vec![*id]));
        }
    }
    groups
}

fn pipeline_group_edge_hydration_needs(
    needs_by_id: &BTreeMap<u64, EdgeSelectedFieldNeeds>,
) -> Vec<(EdgeSelectedFieldNeeds, Vec<u64>)> {
    let mut groups: Vec<(EdgeSelectedFieldNeeds, Vec<u64>)> = Vec::new();
    for (id, needs) in needs_by_id {
        if let Some((_, ids)) = groups
            .iter_mut()
            .find(|(group_needs, _)| group_needs == needs)
        {
            ids.push(*id);
        } else {
            groups.push((needs.clone(), vec![*id]));
        }
    }
    groups
}

fn row_projection_for_item<'a>(
    row: &'a PipelineFinalRow,
    item: &'a crate::graph_row::BoundGraphReturnItem,
    index: usize,
) -> Result<&'a GraphReturnProjection, EngineError> {
    match row.projections.as_ref() {
        Some(projections) => projections.get(index).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph pipeline row projection sidecar is missing projection {index}"
            ))
        }),
        None => Ok(&item.projection),
    }
}

fn pipeline_collect_output_value_hydration_needs(
    value: &crate::graph_row::GraphEvalValue,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut PipelineNestedGraphHydrationNeeds,
) -> Result<(), EngineError> {
    match value {
        crate::graph_row::GraphEvalValue::Node(node) => {
            if node.element.is_none() {
                if let Some(node_needs) = pipeline_node_output_hydration_needs(projection, output) {
                    needs.add_node(node.id, &node_needs)?;
                }
            }
        }
        crate::graph_row::GraphEvalValue::Edge(edge) => {
            if edge.element.is_none() {
                if let Some(edge_needs) = pipeline_edge_output_hydration_needs(projection, output) {
                    needs.add_edge(edge.id, &edge_needs)?;
                }
            }
        }
        crate::graph_row::GraphEvalValue::Path(path) => {
            if let Some(path_needs) = pipeline_path_output_hydration_needs(projection, output) {
                if let Some(node_needs) = graph_row_path_node_hydration_needs(&path_needs)? {
                    for node in &path.nodes {
                        if node.element.is_none() {
                            needs.add_node(node.id, &node_needs)?;
                        }
                    }
                }
                if let Some(edge_needs) = graph_row_path_edge_hydration_needs(&path_needs)? {
                    for edge in &path.edges {
                        if edge.element.is_none() {
                            needs.add_edge(edge.id, &edge_needs)?;
                        }
                    }
                }
            }
        }
        crate::graph_row::GraphEvalValue::List(values) => {
            for value in values {
                pipeline_collect_output_value_hydration_needs(value, projection, output, needs)?;
            }
        }
        crate::graph_row::GraphEvalValue::Map(values) => {
            for value in values.values() {
                pipeline_collect_output_value_hydration_needs(value, projection, output, needs)?;
            }
        }
        crate::graph_row::GraphEvalValue::Null
        | crate::graph_row::GraphEvalValue::Bool(_)
        | crate::graph_row::GraphEvalValue::Int(_)
        | crate::graph_row::GraphEvalValue::UInt(_)
        | crate::graph_row::GraphEvalValue::Float(_)
        | crate::graph_row::GraphEvalValue::String(_)
        | crate::graph_row::GraphEvalValue::Bytes(_) => {}
    }
    Ok(())
}

fn pipeline_node_output_hydration_needs(
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Option<NodeSelectedFieldNeeds> {
    match projection {
        GraphReturnProjection::IdOnly => None,
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids | GraphOutputMode::Projected => None,
            GraphOutputMode::Elements => crate::graph_row::node_source_needs_from_element(
                GraphElementProjection::Full,
                output.include_vectors,
            ),
        },
        GraphReturnProjection::Element(element) => {
            crate::graph_row::node_source_needs_from_element(
                element.clone(),
                output.include_vectors,
            )
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Node(selected)) => {
            crate::graph_row::node_source_needs_from_selected(selected)
        }
        GraphReturnProjection::Selected(_) => None,
    }
}

fn pipeline_edge_output_hydration_needs(
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Option<EdgeSelectedFieldNeeds> {
    match projection {
        GraphReturnProjection::IdOnly => None,
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids | GraphOutputMode::Projected => None,
            GraphOutputMode::Elements => {
                crate::graph_row::edge_source_needs_from_element(GraphElementProjection::Full)
            }
        },
        GraphReturnProjection::Element(element) => {
            crate::graph_row::edge_source_needs_from_element(element.clone())
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Edge(selected)) => {
            crate::graph_row::edge_source_needs_from_selected(selected)
        }
        GraphReturnProjection::Selected(_) => None,
    }
}

fn pipeline_path_output_hydration_needs(
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Option<PathSelectedFieldNeeds> {
    match projection {
        GraphReturnProjection::IdOnly => None,
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids | GraphOutputMode::Projected => None,
            GraphOutputMode::Elements => crate::graph_row::path_source_needs_from_element(
                GraphElementProjection::Full,
                output.include_vectors,
            ),
        },
        GraphReturnProjection::Element(element) => {
            crate::graph_row::path_source_needs_from_element(
                element.clone(),
                output.include_vectors,
            )
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Path(selected)) => {
            crate::graph_row::path_source_needs_from_selected(selected)
        }
        GraphReturnProjection::Selected(_) => None,
    }
}

fn pipeline_hydrate_output_value(
    value: crate::graph_row::GraphEvalValue,
    nodes_by_id: &NodeIdMap<GraphNodeValue>,
    edges_by_id: &NodeIdMap<GraphEdgeValue>,
) -> Result<crate::graph_row::GraphEvalValue, EngineError> {
    Ok(match value {
        crate::graph_row::GraphEvalValue::Node(node) => {
            if let Some(element) = nodes_by_id.get(&node.id) {
                crate::graph_row::GraphEvalValue::Node(
                    crate::graph_row::GraphBoundNode::with_element(node.id, element.clone()),
                )
            } else {
                crate::graph_row::GraphEvalValue::Node(node)
            }
        }
        crate::graph_row::GraphEvalValue::Edge(edge) => {
            if let Some(element) = edges_by_id.get(&edge.id) {
                crate::graph_row::GraphEvalValue::Edge(
                    crate::graph_row::GraphBoundEdge::with_element(edge.id, element.clone()),
                )
            } else {
                crate::graph_row::GraphEvalValue::Edge(edge)
            }
        }
        crate::graph_row::GraphEvalValue::Path(path) => {
            let graph_path = path.path.clone();
            let nodes = path
                .nodes
                .into_iter()
                .map(|node| {
                    if let Some(element) = nodes_by_id.get(&node.id) {
                        crate::graph_row::GraphBoundNode::with_element(node.id, element.clone())
                    } else {
                        node
                    }
                })
                .collect::<Vec<_>>();
            let edges = path
                .edges
                .into_iter()
                .map(|edge| {
                    if let Some(element) = edges_by_id.get(&edge.id) {
                        crate::graph_row::GraphBoundEdge::with_element(edge.id, element.clone())
                    } else {
                        edge
                    }
                })
                .collect::<Vec<_>>();
            crate::graph_row::GraphEvalValue::Path(crate::graph_row::GraphBoundPath::with_values(
                graph_path,
                nodes,
                edges,
            )?)
        }
        crate::graph_row::GraphEvalValue::List(values) => crate::graph_row::GraphEvalValue::List(
            values
                .into_iter()
                .map(|value| pipeline_hydrate_output_value(value, nodes_by_id, edges_by_id))
                .collect::<Result<Vec<_>, EngineError>>()?,
        ),
        crate::graph_row::GraphEvalValue::Map(values) => crate::graph_row::GraphEvalValue::Map(
            values
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        key,
                        pipeline_hydrate_output_value(value, nodes_by_id, edges_by_id)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        crate::graph_row::GraphEvalValue::Null
        | crate::graph_row::GraphEvalValue::Bool(_)
        | crate::graph_row::GraphEvalValue::Int(_)
        | crate::graph_row::GraphEvalValue::UInt(_)
        | crate::graph_row::GraphEvalValue::Float(_)
        | crate::graph_row::GraphEvalValue::String(_)
        | crate::graph_row::GraphEvalValue::Bytes(_) => value,
    })
}

fn execute_pipeline_scalar_project_stage(
    stage: &NormalizedPipelineProjectStage,
    rows: &[crate::graph_row::GraphBindingRow],
    options: &GraphPipelineOptions,
) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
    let mut projected = Vec::with_capacity(rows.len());
    for row in rows {
        let mut output = stage.output_schema.empty_row();
        for mapping in &stage.internal_mappings {
            let value = row.value_for_slot(mapping.source)?;
            bind_pipeline_value_to_slot(&stage.output_schema, &mut output, mapping.target, value)?;
        }
        for item in &stage.items {
            let value = match (item.source_slot, item.expr.as_ref()) {
                (Some(source), None) => row.value_for_slot(source)?,
                (None, Some(expr)) => {
                    let context = crate::graph_row::BoundGraphEvalContext { row };
                    crate::graph_row::eval_bound_graph_expr(expr, &context)?
                }
                _ => {
                    return Err(EngineError::InvalidOperation(
                        "graph pipeline projection item has invalid source/expression state"
                            .to_string(),
                    ));
                }
            };
            bind_pipeline_value_to_slot(&stage.output_schema, &mut output, item.output_slot, value)?;
        }
        projected.push(output);
        // Terminal projections cannot expand row count; final result emission applies max_rows.
        if stage.kind != GraphProjectKind::Return && projected.len() > options.max_pipeline_rows {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline exceeded max_pipeline_rows {}",
                options.max_pipeline_rows
            )));
        }
    }
    Ok(projected)
}

fn execute_pipeline_aggregate_stage(
    stage: &NormalizedPipelineProjectStage,
    aggregate: &NormalizedPipelineAggregate,
    rows: Vec<crate::graph_row::GraphBindingRow>,
    options: &GraphPipelineOptions,
) -> Result<PipelineAggregateOutcome, EngineError> {
    let mut groups: BTreeMap<Vec<crate::graph_row::GraphCanonicalKey>, PipelineAggregateGroup> =
        BTreeMap::new();
    for row in &rows {
        let context = crate::graph_row::BoundGraphEvalContext { row };
        let mut values = Vec::with_capacity(aggregate.group_keys.len());
        for group in &aggregate.group_keys {
            values.push(crate::graph_row::eval_bound_graph_expr(&group.expr, &context)?);
        }
        let key = values
            .iter()
            .map(crate::graph_row::graph_canonical_key_for_value)
            .collect::<Result<Vec<_>, _>>()?;
        if !groups.contains_key(&key) && groups.len() >= options.max_groups {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline exceeded max_groups {}",
                options.max_groups
            )));
        }
        let group = groups.entry(key).or_insert_with(|| PipelineAggregateGroup {
            values,
            states: aggregate
                .calls
                .iter()
                .map(PipelineAggregateState::new)
                .collect(),
        });
        for (call, state) in aggregate.calls.iter().zip(group.states.iter_mut()) {
            state.accumulate(call, row, options)?;
        }
    }
    if rows.is_empty() && aggregate.group_keys.is_empty() {
        groups.insert(
            Vec::new(),
            PipelineAggregateGroup {
                values: Vec::new(),
                states: aggregate
                    .calls
                    .iter()
                    .map(PipelineAggregateState::new)
                    .collect(),
            },
        );
    }

    let mut collect_items = 0usize;
    let mut aggregate_distinct_keys = 0usize;
    let group_count = groups.len();
    let mut output_rows = Vec::with_capacity(group_count);
    for (key, group) in groups {
        let mut eval_row = aggregate.eval_schema.empty_row();
        for (value, group_key) in group.values.into_iter().zip(&aggregate.group_keys) {
            bind_pipeline_value_to_slot(
                &aggregate.eval_schema,
                &mut eval_row,
                group_key.eval_slot,
                value,
            )?;
        }
        for (state, call) in group.states.into_iter().zip(&aggregate.calls) {
            aggregate_distinct_keys =
                aggregate_distinct_keys.saturating_add(state.distinct_key_count());
            let value = state.final_value()?;
            if let crate::graph_row::GraphEvalValue::List(items) = &value {
                if call.function == GraphAggregateFunction::Collect {
                    collect_items = collect_items.saturating_add(items.len());
                }
            }
            bind_pipeline_value_to_slot(&aggregate.eval_schema, &mut eval_row, call.eval_slot, value)?;
        }

        let eval_context = crate::graph_row::BoundGraphEvalContext { row: &eval_row };
        let mut output = stage.output_schema.empty_row();
        if let Some(slot) = aggregate.internal_cursor_slot {
            output.bind_scalar(
                slot,
                crate::graph_row::GraphEvalValue::Bytes(
                    crate::graph_row::encode_graph_canonical_keys(&key)?,
                ),
            )?;
        }
        for order in &aggregate.order_outputs {
            let value = crate::graph_row::eval_bound_graph_expr(&order.expr, &eval_context)?;
            bind_pipeline_value_to_slot(&stage.output_schema, &mut output, order.output_slot, value)?;
        }
        for item in &stage.items {
            let Some(expr) = item.aggregate_expr.as_ref() else {
                return Err(EngineError::InvalidOperation(
                    "graph pipeline aggregate projection item is missing aggregate expression"
                        .to_string(),
                ));
            };
            let value = crate::graph_row::eval_bound_graph_expr(expr, &eval_context)?;
            bind_pipeline_value_to_slot(&stage.output_schema, &mut output, item.output_slot, value)?;
        }
        output_rows.push(output);
        // Terminal aggregate output is bounded by max_groups and final max_rows pagination.
        if stage.kind != GraphProjectKind::Return && output_rows.len() > options.max_pipeline_rows {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline exceeded max_pipeline_rows {}",
                options.max_pipeline_rows
            )));
        }
    }
    Ok(PipelineAggregateOutcome {
        rows: output_rows,
        groups: group_count,
        collect_items,
        aggregate_distinct_keys,
    })
}

impl PipelineAggregateState {
    fn new(call: &NormalizedPipelineAggregateCall) -> Self {
        let inner = match call.function {
            GraphAggregateFunction::Count => PipelineAggregateInner::Count(0),
            GraphAggregateFunction::Sum => PipelineAggregateInner::Sum(PipelineSumState::Empty),
            GraphAggregateFunction::Avg => PipelineAggregateInner::Avg { sum: 0.0, count: 0 },
            GraphAggregateFunction::Min | GraphAggregateFunction::Max => {
                PipelineAggregateInner::MinMax(None)
            }
            GraphAggregateFunction::Collect => PipelineAggregateInner::Collect(Vec::new()),
        };
        Self {
            function: call.function,
            distinct_seen: call.distinct.then(BTreeSet::new),
            inner,
        }
    }

    fn accumulate(
        &mut self,
        call: &NormalizedPipelineAggregateCall,
        row: &crate::graph_row::GraphBindingRow,
        options: &GraphPipelineOptions,
    ) -> Result<(), EngineError> {
        let value = match call.arg.as_ref() {
            Some(arg) => {
                let context = crate::graph_row::BoundGraphEvalContext { row };
                crate::graph_row::eval_bound_graph_expr(arg, &context)?
            }
            None => crate::graph_row::GraphEvalValue::Null,
        };
        if call.arg.is_some() && value.is_null() {
            return Ok(());
        }
        if let Some(seen) = self.distinct_seen.as_mut() {
            let key = vec![crate::graph_row::graph_canonical_key_for_value(&value)?];
            if !seen.contains(&key) && seen.len() >= options.max_groups {
                return Err(EngineError::InvalidOperation(format!(
                    "graph pipeline aggregate DISTINCT exceeded max_groups {}",
                    options.max_groups
                )));
            }
            if !seen.insert(key) {
                return Ok(());
            }
        }
        match &mut self.inner {
            PipelineAggregateInner::Count(count) => {
                *count = count.checked_add(1).ok_or_else(|| {
                    EngineError::InvalidOperation("graph pipeline count overflow".to_string())
                })?;
            }
            PipelineAggregateInner::Sum(sum) => sum.accumulate(&value)?,
            PipelineAggregateInner::Avg { sum, count } => {
                let value = aggregate_numeric_as_f64(&value, "avg")?;
                *sum = checked_finite_aggregate_float(*sum + value, "avg result")?;
                *count = count.checked_add(1).ok_or_else(|| {
                    EngineError::InvalidOperation("graph pipeline avg count overflow".to_string())
                })?;
            }
            PipelineAggregateInner::MinMax(current) => {
                validate_min_max_value(&value)?;
                if let Some(existing) = current.as_ref() {
                    let ordering = partial_cmp_aggregate_values(&value, existing)?;
                    let replace = match self.function {
                        GraphAggregateFunction::Min => ordering.is_lt(),
                        GraphAggregateFunction::Max => ordering.is_gt(),
                        _ => false,
                    };
                    if replace {
                        *current = Some(value);
                    }
                } else {
                    *current = Some(value);
                }
            }
            PipelineAggregateInner::Collect(items) => {
                if items.len() >= options.max_collect_items {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph pipeline collect exceeded max_collect_items {}",
                        options.max_collect_items
                    )));
                }
                items.push(value);
            }
        }
        Ok(())
    }

    fn final_value(self) -> Result<crate::graph_row::GraphEvalValue, EngineError> {
        Ok(match self.inner {
            PipelineAggregateInner::Count(count) => crate::graph_row::GraphEvalValue::UInt(count),
            PipelineAggregateInner::Sum(sum) => sum.final_value()?,
            PipelineAggregateInner::Avg { sum, count } => {
                if count == 0 {
                    crate::graph_row::GraphEvalValue::Null
                } else {
                    let count = crate::property_value_semantics::exact_u64_to_f64(
                        count,
                        "graph pipeline avg count",
                    )?;
                    crate::graph_row::GraphEvalValue::Float(checked_finite_aggregate_float(
                        sum / count,
                        "avg result",
                    )?)
                }
            }
            PipelineAggregateInner::MinMax(value) => {
                value.unwrap_or(crate::graph_row::GraphEvalValue::Null)
            }
            PipelineAggregateInner::Collect(items) => crate::graph_row::GraphEvalValue::List(items),
        })
    }

    fn distinct_key_count(&self) -> usize {
        self.distinct_seen.as_ref().map_or(0, BTreeSet::len)
    }
}

impl PipelineSumState {
    fn accumulate(&mut self, value: &crate::graph_row::GraphEvalValue) -> Result<(), EngineError> {
        match self {
            PipelineSumState::Empty => match value {
                crate::graph_row::GraphEvalValue::Int(value) => *self = PipelineSumState::Int(*value),
                crate::graph_row::GraphEvalValue::UInt(value) => {
                    let value = i64::try_from(*value).map_err(|_| {
                        EngineError::InvalidOperation(
                            "graph pipeline sum unsigned value does not fit signed output"
                                .to_string(),
                        )
                    })?;
                    *self = PipelineSumState::Int(value);
                }
                crate::graph_row::GraphEvalValue::Float(value) => {
                    *self = PipelineSumState::Float(checked_finite_aggregate_float(
                        *value,
                        "sum input",
                    )?)
                }
                _ => return Err(aggregate_numeric_error("sum")),
            },
            PipelineSumState::Int(current) => match value {
                crate::graph_row::GraphEvalValue::Int(value) => {
                    *current = current.checked_add(*value).ok_or_else(|| {
                        EngineError::InvalidOperation("graph pipeline sum overflow".to_string())
                    })?;
                }
                crate::graph_row::GraphEvalValue::UInt(value) => {
                    let value = i64::try_from(*value).map_err(|_| {
                        EngineError::InvalidOperation(
                            "graph pipeline sum unsigned value does not fit signed output"
                                .to_string(),
                        )
                    })?;
                    *current = current.checked_add(value).ok_or_else(|| {
                        EngineError::InvalidOperation("graph pipeline sum overflow".to_string())
                    })?;
                }
                crate::graph_row::GraphEvalValue::Float(value) => {
                    *self = PipelineSumState::Float(checked_finite_aggregate_float(
                        crate::property_value_semantics::exact_i64_to_f64(
                            *current,
                            "graph pipeline sum integer input",
                        )?
                            + checked_finite_aggregate_float(*value, "sum input")?,
                        "sum result",
                    )?);
                }
                _ => return Err(aggregate_numeric_error("sum")),
            },
            PipelineSumState::Float(current) => {
                let value = aggregate_numeric_as_f64(value, "sum")?;
                *current = checked_finite_aggregate_float(*current + value, "sum result")?;
            }
        }
        Ok(())
    }

    fn final_value(self) -> Result<crate::graph_row::GraphEvalValue, EngineError> {
        Ok(match self {
            PipelineSumState::Empty => crate::graph_row::GraphEvalValue::Null,
            PipelineSumState::Int(value) => crate::graph_row::GraphEvalValue::Int(value),
            PipelineSumState::Float(value) => crate::graph_row::GraphEvalValue::Float(
                checked_finite_aggregate_float(value, "sum result")?,
            ),
        })
    }
}

fn aggregate_numeric_as_f64(
    value: &crate::graph_row::GraphEvalValue,
    function: &str,
) -> Result<f64, EngineError> {
    match value {
        crate::graph_row::GraphEvalValue::Int(value) => {
            crate::property_value_semantics::exact_i64_to_f64(
                *value,
                "graph pipeline aggregate integer input",
            )
        }
        crate::graph_row::GraphEvalValue::UInt(value) => {
            if function == "sum" {
                let value = i64::try_from(*value).map_err(|_| {
                    EngineError::InvalidOperation(
                        "graph pipeline sum unsigned value does not fit signed output"
                            .to_string(),
                    )
                })?;
                crate::property_value_semantics::exact_i64_to_f64(
                    value,
                    "graph pipeline sum unsigned input",
                )
            } else {
                crate::property_value_semantics::exact_u64_to_f64(
                    *value,
                    "graph pipeline aggregate unsigned input",
                )
            }
        }
        crate::graph_row::GraphEvalValue::Float(value) => {
            checked_finite_aggregate_float(*value, "aggregate float input")
        }
        _ => Err(aggregate_numeric_error(function)),
    }
}

fn checked_finite_aggregate_float(value: f64, context: &str) -> Result<f64, EngineError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(EngineError::InvalidOperation(format!(
            "graph pipeline aggregate {context} must be finite"
        )))
    }
}

fn aggregate_numeric_error(function: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph pipeline {function} accepts numeric inputs only"
    ))
}

fn validate_min_max_value(value: &crate::graph_row::GraphEvalValue) -> Result<(), EngineError> {
    match value {
        crate::graph_row::GraphEvalValue::Bool(_)
        | crate::graph_row::GraphEvalValue::Int(_)
        | crate::graph_row::GraphEvalValue::UInt(_)
        | crate::graph_row::GraphEvalValue::String(_) => Ok(()),
        crate::graph_row::GraphEvalValue::Float(value) => {
            checked_finite_aggregate_float(*value, "min/max input").map(|_| ())
        }
        _ => Err(EngineError::InvalidOperation(
            "graph pipeline min/max support numeric, string, and bool inputs only".to_string(),
        )),
    }
}

fn partial_cmp_aggregate_values(
    left: &crate::graph_row::GraphEvalValue,
    right: &crate::graph_row::GraphEvalValue,
) -> Result<std::cmp::Ordering, EngineError> {
    let left_numeric = aggregate_numeric_key(left)?;
    let right_numeric = aggregate_numeric_key(right)?;
    match (left_numeric, right_numeric) {
        (Some(left), Some(right)) => {
            return Ok(crate::property_value_semantics::compare_numeric_keys(left, right));
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(EngineError::InvalidOperation(
                "graph pipeline min/max cannot mix incompatible numeric and non-numeric domains"
                    .to_string(),
            ));
        }
        _ => {}
    }
    match (left, right) {
        (
            crate::graph_row::GraphEvalValue::Bool(left),
            crate::graph_row::GraphEvalValue::Bool(right),
        ) => Ok(left.cmp(right)),
        (
            crate::graph_row::GraphEvalValue::String(left),
            crate::graph_row::GraphEvalValue::String(right),
        ) => Ok(left.cmp(right)),
        _ => Err(EngineError::InvalidOperation(
            "graph pipeline min/max cannot mix incompatible value domains".to_string(),
        )),
    }
}

fn aggregate_numeric_key(
    value: &crate::graph_row::GraphEvalValue,
) -> Result<Option<crate::property_value_semantics::NumericScalarKey>, EngineError> {
    Ok(match value {
        crate::graph_row::GraphEvalValue::Int(value) => {
            Some(crate::property_value_semantics::numeric_key_from_i64(*value))
        }
        crate::graph_row::GraphEvalValue::UInt(value) => {
            Some(crate::property_value_semantics::numeric_key_from_u64(*value))
        }
        crate::graph_row::GraphEvalValue::Float(value) => Some(
            crate::property_value_semantics::numeric_key_from_f64(*value).ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph pipeline min/max float input must be finite".to_string(),
                )
            })?,
        ),
        _ => None,
    })
}

fn pipeline_apply_distinct(
    stage: &NormalizedPipelineProjectStage,
    rows: &mut Vec<crate::graph_row::GraphBindingRow>,
    options: &GraphPipelineOptions,
) -> Result<usize, EngineError> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let key =
            crate::graph_row::graph_canonical_key_for_row_slots(&row, &stage.distinct_slots)?;
        if !seen.contains(&key) && seen.len() >= options.max_groups {
            return Err(EngineError::InvalidOperation(format!(
                "graph pipeline DISTINCT exceeded max_groups {}",
                options.max_groups
            )));
        }
        if seen.insert(key) {
            unique.push(row);
        }
    }
    let key_count = seen.len();
    *rows = unique;
    Ok(key_count)
}

fn pipeline_union_branch_return_projections(
    branch: &NormalizedPipelineUnionBranch,
) -> Arc<[GraphReturnProjection]> {
    branch
        .pipeline
        .terminal_return_items
        .iter()
        .map(|item| item.projection.clone())
        .collect::<Vec<_>>()
        .into()
}

fn pipeline_apply_union_distinct(
    stage: &NormalizedPipelineUnionStage,
    rows: &mut Vec<crate::graph_row::GraphBindingRow>,
    row_projections: &mut Vec<Arc<[GraphReturnProjection]>>,
) -> Result<usize, EngineError> {
    if row_projections.len() != rows.len() {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage projection sidecar length {} does not match row length {}",
            row_projections.len(),
            rows.len()
        )));
    }
    let mut seen = BTreeSet::new();
    let mut unique = Vec::with_capacity(rows.len());
    let mut unique_projections = Vec::with_capacity(row_projections.len());
    for (row, projections) in rows.drain(..).zip(row_projections.drain(..)) {
        let key =
            crate::graph_row::graph_canonical_key_for_row_slots(&row, &stage.distinct_slots)?;
        if !seen.contains(&key) && seen.len() >= stage.branches[0].pipeline.options.max_groups {
            return Err(EngineError::InvalidOperation(format!(
                "GraphUnionStage DISTINCT exceeded max_groups {}",
                stage.branches[0].pipeline.options.max_groups
            )));
        }
        if seen.insert(key) {
            unique.push(row);
            unique_projections.push(projections);
        }
    }
    let key_count = seen.len();
    *rows = unique;
    *row_projections = unique_projections;
    Ok(key_count)
}

fn pipeline_union_output_row(
    stage: &NormalizedPipelineUnionStage,
    branch: &NormalizedPipelineUnionBranch,
    row: crate::graph_row::GraphBindingRow,
    ordinal: u64,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = stage.output_schema.empty_row();
    output.bind_scalar(
        stage.ordinal_slot,
        crate::graph_row::GraphEvalValue::UInt(ordinal),
    )?;
    for mapping in &branch.output_mappings {
        let value = row.value_for_slot(mapping.source)?;
        bind_pipeline_value_to_slot(&stage.output_schema, &mut output, mapping.target, value)?;
    }
    Ok(output)
}

fn pipeline_final_logical_key(
    pipeline: &NormalizedGraphPipeline,
    ordinal: usize,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<Vec<crate::graph_row::GraphSortAtom>, EngineError> {
    if pipeline_uses_pipeline_order_cursor(pipeline) {
        let ordinal: u64 = ordinal.try_into().map_err(|_| {
            EngineError::InvalidOperation(
                "graph pipeline output ordinal does not fit cursor key".to_string(),
            )
        })?;
        return Ok(vec![crate::graph_row::GraphSortAtom::Bytes(
            ordinal.to_be_bytes().to_vec(),
        )]);
    }
    row.logical_sort_key(&pipeline.terminal_schema)
}

fn pipeline_uses_pipeline_order_cursor(pipeline: &NormalizedGraphPipeline) -> bool {
    pipeline.preserve_pipeline_order && pipeline.terminal_order_by.is_empty()
}

fn pipeline_bridge_rows(
    rows: &[crate::graph_row::GraphBindingRow],
    input_schema: &crate::graph_row::GraphBindingSchema,
    output_schema: &crate::graph_row::GraphBindingSchema,
    mappings: &[PipelineSlotMapping],
) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
    rows.iter()
        .map(|row| {
            let mut output = output_schema.empty_row();
            for mapping in mappings {
                let value = row.value_for_slot(mapping.source)?;
                let source_info = input_schema.slot(mapping.source).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph pipeline source slot {:?}:{} is missing",
                        mapping.source.kind, mapping.source.index
                    ))
                })?;
                let target_info = output_schema.slot(mapping.target).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph pipeline target slot {:?}:{} is missing",
                        mapping.target.kind, mapping.target.index
                    ))
                })?;
                if source_info.kind != target_info.kind {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph pipeline slot kind mismatch for alias '{}'",
                        source_info.name
                    )));
                }
                bind_pipeline_value_to_slot(output_schema, &mut output, mapping.target, value)?;
            }
            Ok(output)
        })
        .collect()
}

fn pipeline_copy_row_to_schema(
    input_schema: &crate::graph_row::GraphBindingSchema,
    output_schema: &crate::graph_row::GraphBindingSchema,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = output_schema.empty_row();
    let slots = input_schema
        .slots()
        .iter()
        .map(|slot| crate::graph_row::GraphBindingSlotRef {
            kind: slot.kind,
            index: slot.index,
        })
        .collect::<Vec<_>>();
    output.copy_slots_from(row, &slots)?;
    Ok(output)
}

fn pipeline_optional_input_key(
    stage: &NormalizedPipelineMatchStage,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<Vec<crate::graph_row::GraphCanonicalKey>, EngineError> {
    crate::graph_row::graph_canonical_key_for_row_slots(row, &stage.input_slots)
}

fn pipeline_optional_row_is_miss(
    stage: &NormalizedPipelineMatchStage,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<bool, EngineError> {
    if stage.optional_slots.is_empty() {
        return Ok(false);
    }
    stage
        .optional_slots
        .iter()
        .map(|slot| row.slot_is_null(*slot))
        .try_fold(true, |all_null, is_null| Ok(all_null && is_null?))
}

fn pipeline_optional_null_extend_row(
    stage: &NormalizedPipelineMatchStage,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = row.clone();
    for slot in &stage.optional_slots {
        if !output.slot_is_null(*slot)? {
            output.set_null(&stage.query.binding_schema, *slot)?;
        }
    }
    Ok(output)
}

fn pipeline_attach_cursor_keys(
    rows: Vec<crate::graph_row::GraphBindingRow>,
    source_schema: &crate::graph_row::GraphBindingSchema,
    output_schema: &crate::graph_row::GraphBindingSchema,
    mappings: &[PipelineSlotMapping],
    cursor_slot: crate::graph_row::GraphBindingSlotRef,
) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
    rows.into_iter()
        .map(|row| {
            let cursor_key = pipeline_cursor_key_bytes(&row, source_schema)?;
            let mut output = output_schema.empty_row();
            for mapping in mappings {
                if mapping.target == cursor_slot {
                    continue;
                }
                let value = row.value_for_slot(mapping.source)?;
                bind_pipeline_value_to_slot(output_schema, &mut output, mapping.target, value)?;
            }
            output.bind_scalar(
                cursor_slot,
                crate::graph_row::GraphEvalValue::Bytes(cursor_key),
            )?;
            Ok(output)
        })
        .collect()
}

fn pipeline_cursor_key_bytes(
    row: &crate::graph_row::GraphBindingRow,
    schema: &crate::graph_row::GraphBindingSchema,
) -> Result<Vec<u8>, EngineError> {
    let key = row.logical_sort_key(schema)?;
    let mut bytes = Vec::new();
    encode_graph_sort_atoms(&mut bytes, &key)?;
    Ok(bytes)
}

fn shortest_path_output_row(
    stage: &NormalizedPipelineShortestPathStage,
    input: &crate::graph_row::GraphBindingRow,
    path: ShortestPath,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = shortest_path_copy_input_row(stage, input)?;
    output.bind_path(
        stage.output_path_slot,
        crate::graph_row::GraphBoundPath::id_only(GraphPath {
            nodes: path.nodes,
            edges: path.edges,
        })?,
    )?;
    Ok(output)
}

fn shortest_path_null_output_row(
    stage: &NormalizedPipelineShortestPathStage,
    input: &crate::graph_row::GraphBindingRow,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = shortest_path_copy_input_row(stage, input)?;
    output.set_null(&stage.output_schema, stage.output_path_slot)?;
    Ok(output)
}

fn resolve_shortest_path_endpoint(
    endpoint: &ResolvedShortestPathEndpoint,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<Option<u64>, EngineError> {
    match endpoint {
        ResolvedShortestPathEndpoint::Alias { slot } => row.node_id_for_slot_if_bound(*slot),
        ResolvedShortestPathEndpoint::Static(id) => Ok(*id),
    }
}

fn shortest_path_copy_input_row(
    stage: &NormalizedPipelineShortestPathStage,
    input: &crate::graph_row::GraphBindingRow,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = stage.output_schema.empty_row();
    for mapping in &stage.input_mappings {
        let value = input.value_for_slot(mapping.source)?;
        bind_pipeline_value_to_slot(&stage.output_schema, &mut output, mapping.target, value)?;
    }
    Ok(output)
}

fn pipeline_call_output_row(
    stage: &NormalizedPipelineCallStage,
    outer: &crate::graph_row::GraphBindingRow,
    subquery: &crate::graph_row::GraphBindingRow,
) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
    let mut output = stage.output_schema.empty_row();
    for mapping in &stage.input_mappings {
        let value = outer.value_for_slot(mapping.source)?;
        bind_pipeline_value_to_slot(&stage.output_schema, &mut output, mapping.target, value)?;
    }
    for mapping in &stage.output_mappings {
        let value = subquery.value_for_slot(mapping.source)?;
        bind_pipeline_value_to_slot(&stage.output_schema, &mut output, mapping.target, value)?;
    }
    Ok(output)
}

fn bind_pipeline_value_to_slot(
    schema: &crate::graph_row::GraphBindingSchema,
    row: &mut crate::graph_row::GraphBindingRow,
    slot: crate::graph_row::GraphBindingSlotRef,
    value: crate::graph_row::GraphEvalValue,
) -> Result<(), EngineError> {
    if value.is_null() {
        return row.set_null(schema, slot);
    }
    match (slot.kind, value) {
        (crate::graph_row::GraphBindingSlotKind::Node, crate::graph_row::GraphEvalValue::Node(value)) => row.bind_node(slot, value),
        (crate::graph_row::GraphBindingSlotKind::Edge, crate::graph_row::GraphEvalValue::Edge(value)) => row.bind_edge(slot, value),
        (crate::graph_row::GraphBindingSlotKind::Path, crate::graph_row::GraphEvalValue::Path(value)) => row.bind_path(slot, value),
        (crate::graph_row::GraphBindingSlotKind::Scalar, value) => row.bind_scalar(slot, value),
        (kind, _) => Err(EngineError::InvalidOperation(format!(
            "graph pipeline cannot bind value to {kind:?} slot"
        ))),
    }
}

fn sort_pipeline_rows(
    rows: &mut [crate::graph_row::GraphBindingRow],
    schema: &crate::graph_row::GraphBindingSchema,
    order_by: &[crate::graph_row::BoundGraphOrderItem],
) -> Result<(), EngineError> {
    let directions = graph_row_order_directions(order_by);
    let mut keyed = rows
        .iter()
        .cloned()
        .map(|row| {
            let sort_key = pipeline_explicit_sort_key(order_by, &row)?;
            let logical_key = row.logical_sort_key(schema)?;
            Ok((row, sort_key, logical_key))
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    keyed.sort_by(|left, right| {
        compare_graph_final_keys_by_directions(
            &left.1,
            &left.2,
            &right.1,
            &right.2,
            &directions,
        )
    });
    for (target, (row, _, _)) in rows.iter_mut().zip(keyed) {
        *target = row;
    }
    Ok(())
}

fn pipeline_explicit_sort_key(
    order_by: &[crate::graph_row::BoundGraphOrderItem],
    row: &crate::graph_row::GraphBindingRow,
) -> Result<Vec<crate::graph_row::GraphSortAtom>, EngineError> {
    if order_by.is_empty() {
        return Ok(Vec::new());
    }
    let context = crate::graph_row::BoundGraphEvalContext { row };
    order_by
        .iter()
        .map(|item| {
            let value = crate::graph_row::eval_bound_graph_expr(&item.expr, &context)?;
            crate::graph_row::graph_sort_atom_for_value(&value)
        })
        .collect()
}

fn pipeline_order_needs(
    _schema: &crate::graph_row::GraphBindingSchema,
    _order_by: &[crate::graph_row::BoundGraphOrderItem],
) -> Result<EntityProjectionNeeds, EngineError> {
    Ok(EntityProjectionNeeds::default())
}

fn pipeline_projection_needs_is_empty(needs: &EntityProjectionNeeds) -> bool {
    needs.nodes.is_empty()
        && needs.edges.is_empty()
        && needs.paths.is_empty()
        && needs.hidden_edges.is_empty()
        && needs.hidden_paths.is_empty()
}

fn pipeline_enforce_intermediate_rows(
    len: usize,
    options: &GraphPipelineOptions,
    cap: &str,
) -> Result<(), EngineError> {
    let limit = match cap {
        "max_pipeline_rows" => options.max_pipeline_rows,
        _ => options.max_intermediate_bindings,
    };
    if len > limit {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline exceeded {cap} {limit}"
        )));
    }
    Ok(())
}

fn pipeline_schema_columns(schema: &crate::graph_row::GraphBindingSchema) -> Vec<String> {
    schema
        .slots()
        .iter()
        .filter(|slot| pipeline_slot_is_user_visible(slot))
        .filter_map(|slot| slot.user_alias.clone())
        .collect()
}

fn pipeline_union_branch_count(pipeline: &NormalizedGraphPipeline) -> usize {
    pipeline
        .stages
        .iter()
        .filter_map(|stage| match stage {
            NormalizedGraphPipelineStage::Union(stage) => Some(stage.branches.len()),
            _ => None,
        })
        .sum()
}

fn pipeline_match_stage_detail(
    stage: &NormalizedPipelineMatchStage,
    output_rows: Option<usize>,
) -> String {
    let seeded = pipeline_match_seeded_node_aliases(stage);
    let carried = pipeline_match_carried_aliases(stage, &seeded);
    format!(
        "graph-row-backed match stage; seeded_node_aliases={}; carried_aliases={}; rows={}; optional_candidate_filter={}; optional_candidate_exists_predicates={}",
        seeded.join(","),
        carried.join(","),
        output_rows
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage.optional_candidate_filter.is_some(),
        stage
            .optional_candidate_filter
            .as_ref()
            .map(|filter| filter.exists_predicates.len())
            .unwrap_or(0)
    )
}

fn pipeline_match_stage_notes(
    stage: &NormalizedPipelineMatchStage,
    optional_filter_execution: Option<&PipelineOptionalCandidateFilterExecution>,
) -> Vec<String> {
    let mut notes = vec![
        "match executed through the Phase 32 graph-row runtime".to_string(),
    ];
    let Some(filter) = stage.optional_candidate_filter.as_ref() else {
        return notes;
    };
    let (input_rows, candidate_rows, passed_rows, preserved_miss_rows, synthesized_miss_rows) =
        optional_filter_execution
            .map(|execution| {
                (
                    execution.input_rows.to_string(),
                    execution.candidate_rows.to_string(),
                    execution.passed_rows.to_string(),
                    execution.preserved_miss_rows.to_string(),
                    execution.synthesized_miss_rows.to_string(),
                )
            })
            .unwrap_or_else(|| {
                (
                    "n/a".to_string(),
                    "n/a".to_string(),
                    "n/a".to_string(),
                    "n/a".to_string(),
                    "n/a".to_string(),
                )
            });
    let (subquery_invocations, subquery_cache_hits) = optional_filter_execution
        .map(|execution| {
            (
                execution.subquery_invocations.to_string(),
                execution.subquery_cache_hits.to_string(),
            )
        })
        .unwrap_or_else(|| ("n/a".to_string(), "n/a".to_string()));
    notes.push(format!(
        "optional candidate filter: input_rows={input_rows}; candidate_rows={candidate_rows}; passed_rows={passed_rows}; preserved_miss_rows={preserved_miss_rows}; synthesized_miss_rows={synthesized_miss_rows}; subquery_invocations={subquery_invocations}; subquery_cache_hits={subquery_cache_hits}; left_outer=true"
    ));
    for predicate in &filter.exists_predicates {
        notes.push(format!(
            "optional EXISTS subquery {}: mode={}, imports={}, internal_limit={}, physical_exists_probe={}, invocation_cap={}, cache=canonical-correlation-key",
            predicate.output_alias,
            pipeline_subquery_correlation_mode(&predicate.import_aliases),
            pipeline_alias_list(&predicate.import_aliases),
            predicate.internal_limit,
            pipeline_exists_probe_plan(&predicate.query).is_some(),
            predicate.query.options.max_subquery_invocations
        ));
        notes.push(format!(
            "optional EXISTS subquery {} nested stages: {}",
            predicate.output_alias,
            pipeline_stage_kind_summary(&predicate.query)
        ));
    }
    notes
}

fn pipeline_shortest_path_stage_detail(
    stage: &NormalizedPipelineShortestPathStage,
    options: &GraphPipelineOptions,
    execution: Option<&PipelineShortestPathStageExecution>,
) -> String {
    let algorithm = if stage.weight_field.is_some() {
        "bidirectional_dijkstra"
    } else {
        "bidirectional_bfs"
    };
    let endpoint_mode = format!(
        "{}->{}",
        shortest_path_endpoint_detail(&stage.from),
        shortest_path_endpoint_detail(&stage.to)
    );
    let max_cost = stage
        .max_cost
        .map(|cost| cost.to_string())
        .unwrap_or_else(|| "none".to_string());
    let max_paths = pipeline_shortest_path_effective_max_paths(stage, options)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string());
    let (pair_count, cache_hits, no_path_count, emitted_path_count) = execution
        .map(|execution| {
            (
                execution.pair_count.to_string(),
                execution.cache_hits.to_string(),
                execution.no_path_count.to_string(),
                execution.emitted_path_count.to_string(),
            )
        })
        .unwrap_or_else(|| {
            (
                "n/a".to_string(),
                "n/a".to_string(),
                "n/a".to_string(),
                "n/a".to_string(),
            )
        });
    format!(
        "algorithm={algorithm}; mode={:?}; endpoint_mode={endpoint_mode}; direction={:?}; labels={}; min_hops={}; max_depth={}; max_cost={max_cost}; max_paths={max_paths}; output_path_alias={}; distinct_pair_count={pair_count}; cache_hits={cache_hits}; no_path_count={no_path_count}; emitted_path_count={emitted_path_count}",
        stage.mode,
        stage.direction,
        if stage.edge_label_filter.is_empty() {
            "*".to_string()
        } else {
            stage.edge_label_filter.join("|")
        },
        stage.min_hops,
        stage.max_hops,
        stage.output_path_alias,
    )
}

fn pipeline_shortest_path_effective_max_paths(
    stage: &NormalizedPipelineShortestPathStage,
    options: &GraphPipelineOptions,
) -> Option<usize> {
    match stage.mode {
        GraphShortestPathMode::One => None,
        GraphShortestPathMode::All => Some(stage.max_paths.unwrap_or(options.max_paths_per_start)),
    }
}

fn shortest_path_endpoint_detail(endpoint: &NormalizedShortestPathEndpoint) -> String {
    match endpoint {
        NormalizedShortestPathEndpoint::Alias { alias, .. } => format!("alias({alias})"),
        NormalizedShortestPathEndpoint::NodeId(_) => "node_id".to_string(),
        NormalizedShortestPathEndpoint::NodeKey { .. } => "node_key".to_string(),
    }
}

fn shortest_path_direction_code(direction: Direction) -> u8 {
    match direction {
        Direction::Outgoing => 1,
        Direction::Incoming => 2,
        Direction::Both => 3,
    }
}

fn pipeline_match_seeded_node_aliases(stage: &NormalizedPipelineMatchStage) -> Vec<String> {
    let referenced = pipeline_match_referenced_node_aliases(&stage.query);
    stage
        .input_mappings
        .iter()
        .filter_map(|mapping| stage.query.binding_schema.slot(mapping.target))
        .filter(|slot| slot.kind == crate::graph_row::GraphBindingSlotKind::Node)
        .filter(|slot| {
            slot.user_alias
                .as_ref()
                .is_some_and(|alias| referenced.contains(alias))
        })
        .filter_map(|slot| slot.user_alias.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn pipeline_match_referenced_node_aliases(query: &NormalizedGraphRowQuery) -> BTreeSet<String> {
    let mut aliases = BTreeSet::new();
    for piece in &query.pieces {
        pipeline_collect_match_piece_node_aliases(piece, &mut aliases);
    }
    for node in &query.nodes {
        if graph_node_pattern_has_structural_anchor(node) {
            aliases.insert(node.alias.clone());
        }
    }
    aliases
}

fn pipeline_collect_match_piece_node_aliases(
    piece: &GraphPatternPiece,
    aliases: &mut BTreeSet<String>,
) {
    match piece {
        GraphPatternPiece::Edge(edge) => {
            aliases.insert(edge.from_alias.clone());
            aliases.insert(edge.to_alias.clone());
        }
        GraphPatternPiece::Optional(group) => {
            for child in &group.pieces {
                pipeline_collect_match_piece_node_aliases(child, aliases);
            }
        }
        GraphPatternPiece::VariableLength(path) => {
            aliases.insert(path.from_alias.clone());
            aliases.insert(path.to_alias.clone());
        }
    }
}

fn pipeline_match_carried_aliases(
    stage: &NormalizedPipelineMatchStage,
    seeded_node_aliases: &[String],
) -> Vec<String> {
    let seeded = seeded_node_aliases
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    stage
        .input_mappings
        .iter()
        .filter_map(|mapping| stage.query.binding_schema.slot(mapping.target))
        .filter_map(|slot| slot.user_alias.clone())
        .filter(|alias| !seeded.contains(alias))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn graph_row_runtime_warnings(warnings: &[QueryPlanWarning]) -> Vec<String> {
    warnings
        .iter()
        .map(|warning| gql_query_plan_warning_message(*warning).to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn pipeline_project_stage_detail(
    stage: &NormalizedPipelineProjectStage,
    input_rows: Option<usize>,
    output_rows: Option<usize>,
    aggregate_distinct_keys: Option<usize>,
    subquery_stats: Option<(usize, usize)>,
) -> String {
    let (group_keys, aggregate_calls, aggregate_order_outputs) = stage
        .aggregate
        .as_ref()
        .map(|aggregate| {
            (
                aggregate.group_keys.len(),
                aggregate.calls.len(),
                aggregate.order_outputs.len(),
            )
        })
        .unwrap_or((0, 0, 0));
    let (subquery_invocations, subquery_cache_hits) = subquery_stats
        .map(|(invocations, cache_hits)| (invocations.to_string(), cache_hits.to_string()))
        .unwrap_or_else(|| ("n/a".to_string(), "n/a".to_string()));
    format!(
        "columns={}; input_rows={}; output_rows={}; distinct={}; aggregate={}; group_keys={}; aggregate_calls={}; aggregate_order_outputs={}; aggregate_distinct_keys={}; filter={}; exists_predicates={}; subquery_invocations={}; subquery_cache_hits={}; order_items={}; skip={}; limit={}; preserved={}; created_scalars={}; dropped={}",
        stage.columns.join(","),
        input_rows
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        output_rows
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage.distinct,
        stage.aggregate.is_some(),
        group_keys,
        aggregate_calls,
        aggregate_order_outputs,
        aggregate_distinct_keys
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage.where_expr.is_some(),
        stage.exists_predicates.len(),
        subquery_invocations,
        subquery_cache_hits,
        stage.order_by.len(),
        stage.skip,
        stage
            .limit
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        pipeline_project_preserved_aliases(stage).join(","),
        pipeline_project_created_scalar_aliases(stage).join(","),
        pipeline_project_dropped_aliases(stage).join(",")
    )
}

fn pipeline_project_stage_notes(stage: &NormalizedPipelineProjectStage) -> Vec<String> {
    let mut notes = Vec::new();
    let preserved = pipeline_project_preserved_aliases(stage);
    if !preserved.is_empty() {
        notes.push(format!("preserved aliases: {}", preserved.join(", ")));
    }
    let created = pipeline_project_created_scalar_aliases(stage);
    if !created.is_empty() {
        notes.push(format!("created scalar aliases: {}", created.join(", ")));
    }
    let scalar_exprs = pipeline_project_scalar_expression_summaries(stage);
    if !scalar_exprs.is_empty() {
        notes.push(format!("scalar expressions: {}", scalar_exprs.join(", ")));
    }
    if stage.distinct {
        notes.push(format!(
            "DISTINCT uses {} visible output slot(s) and preserves first occurrence order before projection-local row ops",
            stage.distinct_slots.len()
        ));
    }
    if let Some(aggregate) = stage.aggregate.as_ref() {
        if aggregate.group_keys.is_empty() {
            notes.push("aggregate grouping: global group".to_string());
        } else {
            notes.push(format!(
                "aggregate group keys: {}",
                aggregate
                    .group_keys
                    .iter()
                    .map(|key| key.summary.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        notes.push(format!(
            "aggregate calls: {}",
            aggregate
                .calls
                .iter()
                .map(|call| call.summary.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        if aggregate.calls.iter().any(|call| call.distinct) {
            notes.push(
                "aggregate DISTINCT uses per-group canonical key sets capped by max_groups"
                    .to_string(),
            );
        }
        if !aggregate.order_outputs.is_empty() {
            notes.push(format!(
                "aggregate ORDER BY materializes {} hidden output slot(s)",
                aggregate.order_outputs.len()
            ));
        }
    }
    let dropped = pipeline_project_dropped_aliases(stage);
    if !dropped.is_empty() {
        notes.push(format!("dropped aliases: {}", dropped.join(", ")));
    }
    if stage.where_expr.is_some() {
        notes.push("projection filter evaluated after output aliases are bound".to_string());
    }
    if !stage.exists_predicates.is_empty() {
        for predicate in &stage.exists_predicates {
            notes.push(format!(
                "EXISTS subquery {}: mode={}, imports={}, internal_limit={}, physical_exists_probe={}, invocation_cap={}, cache=canonical-correlation-key",
                predicate.output_alias,
                pipeline_subquery_correlation_mode(&predicate.import_aliases),
                pipeline_alias_list(&predicate.import_aliases),
                predicate.internal_limit,
                pipeline_exists_probe_plan(&predicate.query).is_some(),
                predicate.query.options.max_subquery_invocations
            ));
            notes.push(format!(
                "EXISTS subquery {} nested stages: {}",
                predicate.output_alias,
                pipeline_stage_kind_summary(&predicate.query)
            ));
        }
    }
    if !stage.order_by.is_empty() || stage.skip > 0 || stage.limit.is_some() {
        notes.push(format!(
            "row ops: order_items={}, skip={}, limit={}",
            stage.order_by.len(),
            stage.skip,
            stage
                .limit
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string())
        ));
    }
    if !stage.internal_mappings.is_empty() {
        notes.push("internal cursor key preserved for deterministic paging".to_string());
    }
    notes
}

fn pipeline_exists_probe_plan(
    pipeline: &NormalizedGraphPipeline,
) -> Option<PipelineExistsProbePlan<'_>> {
    let match_count = pipeline
        .stages
        .iter()
        .filter(|stage| matches!(stage, NormalizedGraphPipelineStage::Match(_)))
        .count();
    if match_count > 1 {
        return None;
    }
    let mut match_stage = None;
    for stage in &pipeline.stages {
        match stage {
            NormalizedGraphPipelineStage::Match(stage) => {
                if stage.optional
                    || !graph_row_query_allows_physical_exists_probe(&stage.query)
                {
                    return None;
                }
                match_stage = Some(stage);
            }
            NormalizedGraphPipelineStage::Project(stage) => {
                if match_stage.is_none() && match_count == 1 {
                    return None;
                }
                if !pipeline_project_allows_physical_exists_probe(stage) {
                    return None;
                }
                if stage.limit == Some(0) {
                    return Some(PipelineExistsProbePlan {
                        match_stage,
                        always_false: true,
                    });
                }
            }
            NormalizedGraphPipelineStage::ShortestPath(_)
            | NormalizedGraphPipelineStage::Call(_)
            | NormalizedGraphPipelineStage::Union(_) => return None,
        }
    }
    Some(PipelineExistsProbePlan {
        match_stage,
        always_false: false,
    })
}

fn graph_row_query_allows_physical_exists_probe(query: &NormalizedGraphRowQuery) -> bool {
    query.bound_where.is_none()
        && query.bound_order_by.is_empty()
        && !query
            .pieces
            .iter()
            .any(graph_pattern_piece_blocks_physical_exists_probe)
}

fn graph_pattern_piece_blocks_physical_exists_probe(piece: &GraphPatternPiece) -> bool {
    match piece {
        GraphPatternPiece::Edge(_) | GraphPatternPiece::VariableLength(_) => false,
        GraphPatternPiece::Optional(_) => true,
    }
}

fn pipeline_project_allows_physical_exists_probe(stage: &NormalizedPipelineProjectStage) -> bool {
    !stage.distinct
        && stage.aggregate.is_none()
        && stage.where_expr.is_none()
        && stage.exists_predicates.is_empty()
        && stage.order_by.is_empty()
        && stage.skip == 0
        && stage
            .items
            .iter()
            .all(pipeline_project_item_allows_physical_exists_probe)
}

fn pipeline_project_item_allows_physical_exists_probe(
    item: &NormalizedPipelineProjectItem,
) -> bool {
    item.aggregate_expr.is_none()
        && match item.expr.as_ref() {
            None => true,
            Some(expr) => bound_graph_expr_allows_physical_exists_probe(expr),
        }
}

fn bound_graph_expr_allows_physical_exists_probe(
    expr: &crate::graph_row::BoundGraphExpr,
) -> bool {
    match expr {
        crate::graph_row::BoundGraphExpr::Null
        | crate::graph_row::BoundGraphExpr::Bool(_)
        | crate::graph_row::BoundGraphExpr::Int(_)
        | crate::graph_row::BoundGraphExpr::UInt(_)
        | crate::graph_row::BoundGraphExpr::Float(_)
        | crate::graph_row::BoundGraphExpr::String(_)
        | crate::graph_row::BoundGraphExpr::Bytes(_) => true,
        crate::graph_row::BoundGraphExpr::List(items) => items
            .iter()
            .all(bound_graph_expr_allows_physical_exists_probe),
        crate::graph_row::BoundGraphExpr::Map(items) => items
            .values()
            .all(bound_graph_expr_allows_physical_exists_probe),
        crate::graph_row::BoundGraphExpr::Binding(_)
        | crate::graph_row::BoundGraphExpr::Property { .. }
        | crate::graph_row::BoundGraphExpr::NodeField { .. }
        | crate::graph_row::BoundGraphExpr::EdgeField { .. }
        | crate::graph_row::BoundGraphExpr::PathField { .. } => true,
        crate::graph_row::BoundGraphExpr::Function { name, args } => {
            *name == GraphFunction::Id
                && args
                    .iter()
                    .all(bound_graph_expr_allows_physical_exists_probe)
        }
        crate::graph_row::BoundGraphExpr::IsNull(expr)
        | crate::graph_row::BoundGraphExpr::IsNotNull(expr) => {
            bound_graph_expr_allows_physical_exists_probe(expr)
        }
        crate::graph_row::BoundGraphExpr::Unary { .. }
        | crate::graph_row::BoundGraphExpr::Binary { .. }
        | crate::graph_row::BoundGraphExpr::Case { .. } => false,
    }
}

fn pipeline_call_stage_detail(
    stage: &NormalizedPipelineCallStage,
    input_rows: Option<usize>,
    output_rows: Option<usize>,
    invocation_stats: Option<(usize, usize)>,
) -> String {
    let (invocations, cache_hits) = invocation_stats
        .map(|(invocations, cache_hits)| (invocations.to_string(), cache_hits.to_string()))
        .unwrap_or_else(|| ("n/a".to_string(), "n/a".to_string()));
    format!(
        "mode=inner_apply; correlation={}; imports={}; columns={}; input_rows={}; output_rows={}; invocations={}; cache_hits={}; invocation_cap={}; nested_stages={}",
        pipeline_subquery_correlation_mode(&stage.import_aliases),
        pipeline_alias_list(&stage.import_aliases),
        stage.columns.join(","),
        input_rows
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        output_rows
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        invocations,
        cache_hits,
        stage.query.options.max_subquery_invocations,
        pipeline_stage_kind_summary(&stage.query),
    )
}

fn pipeline_call_stage_notes(
    stage: &NormalizedPipelineCallStage,
    nested: Option<&GraphPipelineExplain>,
) -> Vec<String> {
    let mut notes = vec![
        "CALL uses native inner-apply subquery execution".to_string(),
        "correlation cache key uses shared canonical graph key semantics".to_string(),
        format!(
            "import aliases: {}",
            pipeline_alias_list(&stage.import_aliases)
        ),
        format!("output columns: {}", pipeline_alias_list(&stage.columns)),
    ];
    if let Some(nested) = nested {
        notes.push(format!(
            "nested explain stages: {}",
            nested
                .stages
                .iter()
                .map(|stage| format!("{}: {}", stage.kind, stage.detail))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    notes
}

fn pipeline_subquery_correlation_mode(import_aliases: &[String]) -> &'static str {
    if import_aliases.is_empty() {
        "uncorrelated"
    } else {
        "correlated"
    }
}

fn pipeline_alias_list(aliases: &[String]) -> String {
    if aliases.is_empty() {
        "none".to_string()
    } else {
        aliases.join(",")
    }
}

fn pipeline_stage_kind_summary(pipeline: &NormalizedGraphPipeline) -> String {
    pipeline
        .stages
        .iter()
        .map(|stage| match stage {
            NormalizedGraphPipelineStage::Match(_) => "Match",
            NormalizedGraphPipelineStage::ShortestPath(_) => "ShortestPath",
            NormalizedGraphPipelineStage::Project(stage) => match stage.kind {
                GraphProjectKind::With => "Project(With)",
                GraphProjectKind::Return => "Project(Return)",
            },
            NormalizedGraphPipelineStage::Call(_) => "Call",
            NormalizedGraphPipelineStage::Union(stage) => {
                if stage.all {
                    "UnionAll"
                } else {
                    "Union"
                }
            }
        })
        .collect::<Vec<_>>()
        .join(">")
}

fn pipeline_union_stage_detail(
    stage: &NormalizedPipelineUnionStage,
    output_rows: Option<usize>,
    dedup_keys: Option<usize>,
) -> String {
    format!(
        "branches={}; all={}; columns={}; output_rows={}; dedupe_keys={}; dedupe_cap={}",
        stage.branches.len(),
        stage.all,
        stage.columns.join(","),
        output_rows
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        dedup_keys
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage.branches[0].pipeline.options.max_groups,
    )
}

fn pipeline_union_stage_notes(
    stage: &NormalizedPipelineUnionStage,
    branch_summaries: &[PipelineUnionBranchExplainSummary],
) -> Vec<String> {
    let mut notes = vec![format!(
        "branch columns: {}",
        stage.columns.join(", ")
    )];
    if stage.all {
        notes.push("UNION ALL appends branch rows in source branch order".to_string());
    } else {
        notes.push(format!(
            "UNION dedupes {} visible output slot(s) with canonical row keys and preserves first occurrence order",
            stage.distinct_slots.len()
        ));
    }
    notes.push(
        "internal ordinal key preserves union order for final cursor paging".to_string(),
    );
    notes.extend(pipeline_union_branch_summary_notes(branch_summaries));
    notes
}

fn pipeline_union_branch_summary_notes(
    branch_summaries: &[PipelineUnionBranchExplainSummary],
) -> Vec<String> {
    let mut notes = Vec::new();
    for branch in branch_summaries {
        let branch_number = branch.branch_index + 1;
        let stages = branch
            .stages
            .iter()
            .map(|stage| format!("{}: {}", stage.kind, stage.detail))
            .collect::<Vec<_>>();
        if !stages.is_empty() {
            notes.push(format!(
                "branch {branch_number} stages: {}",
                stages.join(" | ")
            ));
        }
        for row_op in &branch.row_ops {
            notes.push(format!(
                "branch {branch_number} row op: {}: {}",
                row_op.kind, row_op.detail
            ));
        }
        for warning in &branch.warnings {
            notes.push(format!("branch {branch_number} warning: {warning}"));
        }
    }
    notes
}

fn pipeline_project_scalar_expression_summaries(
    stage: &NormalizedPipelineProjectStage,
) -> Vec<String> {
    stage
        .items
        .iter()
        .filter_map(|item| {
            item.expr_summary
                .as_ref()
                .map(|expr| format!("{} := {expr}", item.output_name))
        })
        .collect()
}

fn pipeline_project_preserved_aliases(stage: &NormalizedPipelineProjectStage) -> Vec<String> {
    stage
        .items
        .iter()
        .filter(|item| item.source_slot.is_some())
        .map(|item| item.output_name.clone())
        .collect()
}

fn pipeline_project_created_scalar_aliases(stage: &NormalizedPipelineProjectStage) -> Vec<String> {
    stage
        .items
        .iter()
        .filter(|item| item.expr.is_some() || item.aggregate_expr.is_some())
        .map(|item| item.output_name.clone())
        .collect()
}

fn pipeline_project_dropped_aliases(stage: &NormalizedPipelineProjectStage) -> Vec<String> {
    let output = stage
        .output_schema
        .slots()
        .iter()
        .filter_map(|slot| slot.user_alias.as_ref())
        .cloned()
        .collect::<BTreeSet<_>>();
    stage
        .input_schema
        .slots()
        .iter()
        .filter_map(|slot| slot.user_alias.as_ref())
        .filter(|alias| !output.contains(*alias))
        .cloned()
        .collect()
}

fn graph_pipeline_explain_from_normalized(
    pipeline: &NormalizedGraphPipeline,
    stages: Vec<GraphPipelineStageExplain>,
    stats: GraphPipelineStats,
    fingerprints: GraphPipelineFingerprints,
    warnings: Vec<String>,
) -> GraphPipelineExplain {
    GraphPipelineExplain {
        columns: pipeline.columns.clone(),
        effective_at_epoch: Some(stats.effective_at_epoch),
        fingerprint: format!("{:032x}", fingerprints.query),
        stages,
        row_ops: pipeline_row_ops(pipeline),
        order: GraphOrderExplain {
            explicit: !pipeline.terminal_order_by.is_empty(),
            items: pipeline.terminal_order_by.len(),
            stable_logical_row_key: true,
        },
        cursor: GraphCursorExplain {
            supplied: pipeline.page.cursor.is_some(),
            codec_implemented: true,
            message: Some("logical graph pipeline cursor".to_string()),
        },
        projection: GraphProjectionExplain {
            columns: pipeline.columns.clone(),
            output_mode: pipeline.output.mode.clone(),
            include_vectors: pipeline.output.include_vectors,
            compact_rows: pipeline.output.compact_rows,
        },
        caps: graph_pipeline_cap_explain(&pipeline.options),
        summaries: GraphExecutionSummaries {
            validation_only: false,
            rows_planned: stats.intermediate_rows,
            warnings: warnings.clone(),
        },
        stats,
        warnings,
        notes: vec![
            "pipeline match stages are graph-row-backed; projection stages use native scalar slots"
                .to_string(),
        ],
    }
}

fn graph_pipeline_cursor_fingerprints(
    pipeline: &NormalizedGraphPipeline,
    effective_at_epoch: i64,
    original_skip: u64,
) -> GraphPipelineFingerprints {
    let mut query_writer = GraphRowFingerprintWriter::new("pipeline_query_cursor");
    query_writer.u16(1);
    query_writer.i64(effective_at_epoch);
    query_writer.u64(original_skip);
    query_writer.raw_bytes(&pipeline.fingerprint_shape.query_shape.to_be_bytes());
    GraphPipelineFingerprints {
        query: query_writer.finish(),
        order: pipeline.fingerprint_shape.order,
        output: pipeline.fingerprint_shape.output,
        params: pipeline.fingerprint_shape.params,
    }
}

fn pipeline_row_ops(pipeline: &NormalizedGraphPipeline) -> Vec<GraphRowOperationExplain> {
    let mut ops = Vec::new();
    for stage in &pipeline.stages {
        match stage {
            NormalizedGraphPipelineStage::ShortestPath(shortest) => {
                ops.push(GraphRowOperationExplain {
                    kind: "ShortestPath".to_string(),
                    detail: format!(
                        "{:?} {} min_hops={} max_hops={} algorithm={}",
                        shortest.mode,
                        shortest.output_path_alias,
                        shortest.min_hops,
                        shortest.max_hops,
                        if shortest.weight_field.is_some() {
                            "bidirectional_dijkstra"
                        } else {
                            "bidirectional_bfs"
                        }
                    ),
                });
            }
            NormalizedGraphPipelineStage::Project(project) => {
                if let Some(aggregate) = project.aggregate.as_ref() {
                    ops.push(GraphRowOperationExplain {
                        kind: "Aggregate".to_string(),
                        detail: format!(
                            "{:?} group_keys={} aggregate_calls={}",
                            project.kind,
                            aggregate.group_keys.len(),
                            aggregate.calls.len()
                        ),
                    });
                }
                if project.distinct {
                    ops.push(GraphRowOperationExplain {
                        kind: "Distinct".to_string(),
                        detail: format!(
                            "{:?} DISTINCT visible_slots={}",
                            project.kind,
                            project.distinct_slots.len()
                        ),
                    });
                }
                if project.where_expr.is_some() {
                    ops.push(GraphRowOperationExplain {
                        kind: "ProjectFilter".to_string(),
                        detail: format!("{:?} WHERE", project.kind),
                    });
                }
                if !project.order_by.is_empty() {
                    ops.push(GraphRowOperationExplain {
                        kind: "Sort".to_string(),
                        detail: format!(
                            "{:?} ORDER BY {} item(s)",
                            project.kind,
                            project.order_by.len()
                        ),
                    });
                }
                if project.skip > 0 {
                    ops.push(GraphRowOperationExplain {
                        kind: "Skip".to_string(),
                        detail: format!("{:?} SKIP {}", project.kind, project.skip),
                    });
                }
                if let Some(limit) = project.limit {
                    ops.push(GraphRowOperationExplain {
                        kind: "Limit".to_string(),
                        detail: format!("{:?} LIMIT {}", project.kind, limit),
                    });
                }
            }
            NormalizedGraphPipelineStage::Call(call) => {
                ops.push(GraphRowOperationExplain {
                    kind: "CallSubquery".to_string(),
                    detail: format!(
                        "imports={} nested_stages={}",
                        pipeline_alias_list(&call.import_aliases),
                        pipeline_stage_kind_summary(&call.query)
                    ),
                });
            }
            NormalizedGraphPipelineStage::Match(_) | NormalizedGraphPipelineStage::Union(_) => {}
        }
    }
    ops
}

const GRAPH_PIPELINE_LOGICAL_CURSOR_MAGIC: &[u8; 8] = b"OGR34PL1";
const GRAPH_PIPELINE_LOGICAL_CURSOR_VERSION: u8 = 1;

fn graph_pipeline_cursor_state_from_decoded(
    decoded_cursor: Option<GraphPipelineCursorPayload>,
    page: &GraphPageRequest,
    at_epoch: Option<i64>,
    max_skip: usize,
) -> Result<GraphPipelineCursorState, EngineError> {
    let effective_at_epoch = match (decoded_cursor.as_ref(), at_epoch) {
        (None, Some(epoch)) => epoch,
        (None, None) => now_millis(),
        (Some(cursor), None) => cursor.effective_at_epoch,
        (Some(cursor), Some(epoch)) if epoch == cursor.effective_at_epoch => epoch,
        (Some(cursor), Some(epoch)) => {
            return Err(invalid_graph_pipeline_cursor(format!(
                "explicit at_epoch {epoch} does not match cursor epoch {}",
                cursor.effective_at_epoch
            )));
        }
    };
    let original_skip = match decoded_cursor.as_ref() {
        Some(cursor) => {
            let current_skip = page.skip as u64;
            if current_skip != 0 && current_skip != cursor.original_skip {
                return Err(invalid_graph_pipeline_cursor(format!(
                    "cursor page skip {current_skip} does not match original skip {}",
                    cursor.original_skip
                )));
            }
            cursor.original_skip
        }
        None => page.skip as u64,
    };
    if original_skip > max_skip as u64 {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline cursor original skip {original_skip} exceeds max_skip {max_skip}"
        )));
    }
    Ok(GraphPipelineCursorState {
        decoded: decoded_cursor.clone(),
        effective_at_epoch,
        original_skip,
        rows_emitted_after_skip: decoded_cursor.map_or(0, |cursor| cursor.rows_emitted_after_skip),
    })
}

fn graph_pipeline_encode_logical_cursor(
    cursor: &GraphPipelineCursorPayload,
    max_cursor_bytes: usize,
) -> Result<String, EngineError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(GRAPH_PIPELINE_LOGICAL_CURSOR_MAGIC);
    push_u8(&mut bytes, GRAPH_PIPELINE_LOGICAL_CURSOR_VERSION);
    push_i64(&mut bytes, cursor.effective_at_epoch);
    push_u64(&mut bytes, cursor.original_skip);
    push_u64(&mut bytes, cursor.rows_emitted_after_skip);
    push_u128(&mut bytes, cursor.query_fingerprint);
    push_u128(&mut bytes, cursor.order_fingerprint);
    push_u128(&mut bytes, cursor.output_fingerprint);
    push_u128(&mut bytes, cursor.params_fingerprint);
    encode_graph_sort_atoms(&mut bytes, &cursor.last_sort_key)?;
    encode_graph_sort_atoms(&mut bytes, &cursor.last_logical_row_key)?;
    let checksum = crate::types::fnv1a(&bytes);
    push_u64(&mut bytes, checksum);
    if bytes.len() > max_cursor_bytes {
        return Err(invalid_graph_pipeline_cursor(format!(
            "emitted graph pipeline cursor payload is {} bytes, exceeding max_cursor_bytes {}",
            bytes.len(),
            max_cursor_bytes
        )));
    }
    Ok(format!(
        "{GRAPH_PIPELINE_CURSOR_PREFIX}{}",
        base64url_no_pad_encode(&bytes)
    ))
}

fn graph_pipeline_decode_logical_cursor(
    cursor: &str,
    max_cursor_bytes: usize,
) -> Result<GraphPipelineCursorPayload, EngineError> {
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
    if bytes.len() < GRAPH_PIPELINE_LOGICAL_CURSOR_MAGIC.len() + 1 + 8 {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor payload is too short",
        ));
    }
    let checksum_offset = bytes
        .len()
        .checked_sub(8)
        .ok_or_else(|| invalid_graph_pipeline_cursor("graph pipeline cursor is missing checksum"))?;
    let checksum = u64::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .map_err(|_| invalid_graph_pipeline_cursor("graph pipeline cursor checksum is malformed"))?,
    );
    if crate::types::fnv1a(&bytes[..checksum_offset]) != checksum {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor checksum mismatch",
        ));
    }
    let mut reader = CursorPayloadReader::new(&bytes[..checksum_offset]);
    if reader.take(GRAPH_PIPELINE_LOGICAL_CURSOR_MAGIC.len())?
        != GRAPH_PIPELINE_LOGICAL_CURSOR_MAGIC
    {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor magic mismatch",
        ));
    }
    let version = reader.read_u8()?;
    if version != GRAPH_PIPELINE_LOGICAL_CURSOR_VERSION {
        return Err(invalid_graph_pipeline_cursor(format!(
            "unsupported graph pipeline cursor version {version}"
        )));
    }
    let payload = GraphPipelineCursorPayload {
        effective_at_epoch: reader.read_i64()?,
        original_skip: reader.read_u64()?,
        rows_emitted_after_skip: reader.read_u64()?,
        query_fingerprint: reader.read_u128()?,
        order_fingerprint: reader.read_u128()?,
        output_fingerprint: reader.read_u128()?,
        params_fingerprint: reader.read_u128()?,
        last_sort_key: decode_graph_sort_atoms(&mut reader)?,
        last_logical_row_key: decode_graph_sort_atoms(&mut reader)?,
    };
    if !reader.is_finished() {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor payload has trailing bytes",
        ));
    }
    Ok(payload)
}

fn graph_pipeline_validate_cursor_fingerprints(
    cursor: &GraphPipelineCursorPayload,
    fingerprints: &GraphPipelineFingerprints,
) -> Result<(), EngineError> {
    if cursor.query_fingerprint != fingerprints.query {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor query fingerprint mismatch",
        ));
    }
    if cursor.order_fingerprint != fingerprints.order {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor order fingerprint mismatch",
        ));
    }
    if cursor.output_fingerprint != fingerprints.output {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor output fingerprint mismatch",
        ));
    }
    if cursor.params_fingerprint != fingerprints.params {
        return Err(invalid_graph_pipeline_cursor(
            "graph pipeline cursor params fingerprint mismatch",
        ));
    }
    Ok(())
}

fn graph_pipeline_validate_cursor_shape(
    pipeline: &NormalizedGraphPipeline,
    cursor: &GraphPipelineCursorPayload,
) -> Result<(), EngineError> {
    if cursor.last_sort_key.len() != pipeline.terminal_order_by.len() {
        return Err(invalid_graph_pipeline_cursor(format!(
            "graph pipeline cursor sort key has {} atom(s), expected {}",
            cursor.last_sort_key.len(),
            pipeline.terminal_order_by.len()
        )));
    }
    if pipeline_uses_pipeline_order_cursor(pipeline) {
        if cursor.last_logical_row_key.len() != 1 {
            return Err(invalid_graph_pipeline_cursor(format!(
                "graph pipeline cursor logical row key has {} atom(s), expected 1 pipeline-order atom",
                cursor.last_logical_row_key.len()
            )));
        }
        if !matches!(
            cursor.last_logical_row_key.first(),
            Some(crate::graph_row::GraphSortAtom::Bytes(value)) if value.len() == 8
        ) {
            return Err(invalid_graph_pipeline_cursor(
                "graph pipeline cursor logical row key does not contain a pipeline-order atom",
            ));
        }
        return Ok(());
    }
    if cursor.last_logical_row_key.len() != pipeline.terminal_schema.slots().len() {
        return Err(invalid_graph_pipeline_cursor(format!(
            "graph pipeline cursor logical row key has {} atom(s), expected {}",
            cursor.last_logical_row_key.len(),
            pipeline.terminal_schema.slots().len()
        )));
    }
    for (slot, atom) in pipeline
        .terminal_schema
        .slots()
        .iter()
        .zip(cursor.last_logical_row_key.iter())
    {
        if pipeline_internal_cursor_slot_info(slot)
            && !matches!(atom, crate::graph_row::GraphSortAtom::Bytes(_))
        {
            return Err(invalid_graph_pipeline_cursor(format!(
                "graph pipeline cursor internal cursor key atom does not match slot '{}'",
                slot.name
            )));
        }
        if !graph_pipeline_cursor_atom_matches_slot(pipeline, atom, slot) {
            return Err(invalid_graph_pipeline_cursor(format!(
                "graph pipeline cursor logical row key atom does not match slot '{}'",
                slot.name
            )));
        }
        if let crate::graph_row::GraphSortAtom::Path {
            hop_count,
            nodes,
            edges,
        } = atom
        {
            graph_row_validate_cursor_path_atom(*hop_count, nodes, edges)?;
        }
    }
    for (index, (item, atom)) in pipeline
        .terminal_order_by
        .iter()
        .zip(cursor.last_sort_key.iter())
        .enumerate()
    {
        let expectation = if graph_pipeline_order_expr_is_any_value_slot(pipeline, &item.expr) {
            GraphRowCursorAtomExpectation::AnyOrderable
        } else {
            graph_row_cursor_order_atom_expectation(&item.expr, &pipeline.terminal_schema)?
        };
        if !graph_row_cursor_atom_matches_expectation(atom, expectation) {
            return Err(invalid_graph_pipeline_cursor(format!(
                "graph pipeline cursor order key atom {} does not match order expression result kind",
                index + 1
            )));
        }
        if let crate::graph_row::GraphSortAtom::Path {
            hop_count,
            nodes,
            edges,
        } = atom
        {
            graph_row_validate_cursor_path_atom(*hop_count, nodes, edges)?;
        }
    }
    Ok(())
}

fn graph_pipeline_cursor_atom_matches_slot(
    pipeline: &NormalizedGraphPipeline,
    atom: &crate::graph_row::GraphSortAtom,
    slot: &crate::graph_row::GraphBindingSlot,
) -> bool {
    if graph_row_cursor_atom_matches_slot(atom, slot) {
        return true;
    }
    if slot.kind != crate::graph_row::GraphBindingSlotKind::Scalar {
        return false;
    }
    let slot_ref = crate::graph_row::GraphBindingSlotRef {
        kind: slot.kind,
        index: slot.index,
    };
    pipeline.terminal_any_value_slots.contains(&slot_ref)
        && matches!(
            atom,
            crate::graph_row::GraphSortAtom::Node(_)
                | crate::graph_row::GraphSortAtom::Edge(_)
                | crate::graph_row::GraphSortAtom::Path { .. }
        )
}

fn graph_pipeline_order_expr_is_any_value_slot(
    pipeline: &NormalizedGraphPipeline,
    expr: &crate::graph_row::BoundGraphExpr,
) -> bool {
    let crate::graph_row::BoundGraphExpr::Binding(slot) = expr else {
        return false;
    };
    pipeline.terminal_any_value_slots.contains(slot)
}
