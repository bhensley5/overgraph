#[derive(Clone, Debug)]
struct NormalizedGraphPipeline {
    initial_schema: crate::graph_row::GraphBindingSchema,
    stages: Vec<NormalizedGraphPipelineStage>,
    columns: Vec<String>,
    terminal_schema: crate::graph_row::GraphBindingSchema,
    terminal_any_value_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    terminal_return_items: Vec<crate::graph_row::BoundGraphReturnItem>,
    terminal_output_needs: EntityProjectionNeeds,
    terminal_order_by: Vec<crate::graph_row::BoundGraphOrderItem>,
    page: GraphPageRequest,
    output: GraphOutputOptions,
    options: GraphPipelineOptions,
    fingerprint_shape: GraphPipelineFingerprintShape,
    preserve_pipeline_order: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraphPipelineFingerprints {
    query: u128,
    order: u128,
    output: u128,
    params: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraphPipelineFingerprintShape {
    query_shape: u128,
    order: u128,
    output: u128,
    params: u128,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
enum NormalizedGraphPipelineStage {
    Match(NormalizedPipelineMatchStage),
    ShortestPath(NormalizedPipelineShortestPathStage),
    Project(NormalizedPipelineProjectStage),
    Call(NormalizedPipelineCallStage),
    Union(NormalizedPipelineUnionStage),
}

#[derive(Clone, Debug)]
struct NormalizedPipelineMatchStage {
    optional: bool,
    query: NormalizedGraphRowQuery,
    output_schema: crate::graph_row::GraphBindingSchema,
    output_mappings: Vec<PipelineSlotMapping>,
    cursor_slot: crate::graph_row::GraphBindingSlotRef,
    input_mappings: Vec<PipelineSlotMapping>,
    input_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    optional_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    optional_candidate_filter: Option<NormalizedPipelineOptionalCandidateFilter>,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineProjectStage {
    kind: GraphProjectKind,
    distinct: bool,
    input_schema: crate::graph_row::GraphBindingSchema,
    output_schema: crate::graph_row::GraphBindingSchema,
    items: Vec<NormalizedPipelineProjectItem>,
    internal_mappings: Vec<PipelineSlotMapping>,
    distinct_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    aggregate: Option<NormalizedPipelineAggregate>,
    input_needs: EntityProjectionNeeds,
    filter_needs: EntityProjectionNeeds,
    order_needs: EntityProjectionNeeds,
    where_expr: Option<crate::graph_row::BoundGraphExpr>,
    exists_predicates: Vec<NormalizedPipelineExistsPredicate>,
    order_by: Vec<crate::graph_row::BoundGraphOrderItem>,
    skip: usize,
    limit: Option<usize>,
    columns: Vec<String>,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineExistsPredicate {
    output_slot: crate::graph_row::GraphBindingSlotRef,
    output_alias: String,
    import_aliases: Vec<String>,
    import_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    import_mappings: Vec<PipelineSlotMapping>,
    query: NormalizedGraphPipeline,
    internal_limit: bool,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineOptionalCandidateFilter {
    eval_schema: crate::graph_row::GraphBindingSchema,
    filter_needs: EntityProjectionNeeds,
    where_expr: crate::graph_row::BoundGraphExpr,
    exists_predicates: Vec<NormalizedPipelineExistsPredicate>,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineProjectItem {
    output_name: String,
    output_slot: crate::graph_row::GraphBindingSlotRef,
    source_slot: Option<crate::graph_row::GraphBindingSlotRef>,
    expr: Option<crate::graph_row::BoundGraphExpr>,
    aggregate_expr: Option<crate::graph_row::BoundGraphExpr>,
    expr_summary: Option<String>,
    projection: GraphReturnProjection,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineShortestPathStage {
    optional: bool,
    mode: GraphShortestPathMode,
    output_schema: crate::graph_row::GraphBindingSchema,
    input_mappings: Vec<PipelineSlotMapping>,
    output_path_slot: crate::graph_row::GraphBindingSlotRef,
    output_path_alias: String,
    from: NormalizedShortestPathEndpoint,
    to: NormalizedShortestPathEndpoint,
    direction: Direction,
    edge_label_filter: Vec<String>,
    min_hops: u8,
    max_hops: u8,
    weight_field: Option<String>,
    max_cost: Option<f64>,
    max_paths: Option<usize>,
}

#[derive(Clone, Debug)]
enum NormalizedShortestPathEndpoint {
    Alias {
        alias: String,
        slot: crate::graph_row::GraphBindingSlotRef,
    },
    NodeId(u64),
    NodeKey { label: String, key: String },
}

#[derive(Clone, Debug)]
struct NormalizedPipelineAggregate {
    eval_schema: crate::graph_row::GraphBindingSchema,
    group_keys: Vec<NormalizedPipelineGroupKey>,
    calls: Vec<NormalizedPipelineAggregateCall>,
    order_outputs: Vec<NormalizedPipelineAggregateOrderOutput>,
    internal_cursor_slot: Option<crate::graph_row::GraphBindingSlotRef>,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineGroupKey {
    expr: crate::graph_row::BoundGraphExpr,
    eval_slot: crate::graph_row::GraphBindingSlotRef,
    summary: String,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineAggregateCall {
    function: GraphAggregateFunction,
    distinct: bool,
    arg: Option<crate::graph_row::BoundGraphExpr>,
    eval_slot: crate::graph_row::GraphBindingSlotRef,
    summary: String,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineAggregateOrderOutput {
    expr: crate::graph_row::BoundGraphExpr,
    output_slot: crate::graph_row::GraphBindingSlotRef,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineUnionStage {
    all: bool,
    branches: Vec<NormalizedPipelineUnionBranch>,
    output_schema: crate::graph_row::GraphBindingSchema,
    ordinal_slot: crate::graph_row::GraphBindingSlotRef,
    cursor_any_value_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    distinct_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    columns: Vec<String>,
    projections: Vec<GraphReturnProjection>,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineCallStage {
    input_schema: crate::graph_row::GraphBindingSchema,
    output_schema: crate::graph_row::GraphBindingSchema,
    input_mappings: Vec<PipelineSlotMapping>,
    import_aliases: Vec<String>,
    import_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    import_mappings: Vec<PipelineSlotMapping>,
    query: NormalizedGraphPipeline,
    output_mappings: Vec<PipelineSlotMapping>,
    columns: Vec<String>,
}

#[derive(Clone, Debug)]
struct NormalizedPipelineUnionBranch {
    pipeline: NormalizedGraphPipeline,
    output_mappings: Vec<PipelineSlotMapping>,
}

#[derive(Clone, Debug)]
struct PipelineSlotMapping {
    source: crate::graph_row::GraphBindingSlotRef,
    target: crate::graph_row::GraphBindingSlotRef,
}

const PIPELINE_CURSOR_KEY_SLOT: &str = "__og_pipeline_cursor_key";
const PIPELINE_UNION_ORDER_SLOT: &str = "__og_union_order";

fn normalize_graph_pipeline_query(
    query: &GraphPipelineQuery,
) -> Result<NormalizedGraphPipeline, EngineError> {
    normalize_graph_pipeline_query_with_initial_schema(
        query,
        crate::graph_row::GraphBindingSchema::new(),
        0,
    )
}

fn normalize_graph_pipeline_query_with_initial_schema(
    query: &GraphPipelineQuery,
    initial_schema: crate::graph_row::GraphBindingSchema,
    subquery_depth: usize,
) -> Result<NormalizedGraphPipeline, EngineError> {
    validate_graph_pipeline_request(query)?;
    if subquery_depth > query.options.max_subquery_depth {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline subquery depth {subquery_depth} exceeds max_subquery_depth {}",
            query.options.max_subquery_depth
        )));
    }
    let referenced_params = collect_graph_pipeline_referenced_params(query)?;
    validate_graph_pipeline_referenced_params(&referenced_params, &query.options)?;

    let mut current_schema = initial_schema.clone();
    let mut stages = Vec::with_capacity(query.stages.len());
    let mut terminal_seen = false;
    let mut terminal_schema = None;
    let mut terminal_return_items = None;
    let mut terminal_output_needs = None;
    let mut terminal_order_by = None;
    let mut columns = Vec::new();
    let mut current_any_value_slots = Vec::new();

    for (index, stage) in query.stages.iter().enumerate() {
        if terminal_seen {
            return Err(EngineError::InvalidOperation(
                "graph pipeline stages cannot appear after terminal Project(Return)".to_string(),
            ));
        }
        match stage {
            GraphPipelineStage::Match(match_stage) => {
                let normalized = normalize_pipeline_match_stage(
                    match_stage,
                    &current_schema,
                    query,
                    subquery_depth,
                )?;
                let bridged_any_value_slots = pipeline_remap_any_value_slots(
                    &current_any_value_slots,
                    &normalized.input_mappings,
                );
                current_any_value_slots = pipeline_remap_any_value_slots(
                    &bridged_any_value_slots,
                    &normalized.output_mappings,
                );
                current_schema = normalized.output_schema.clone();
                stages.push(NormalizedGraphPipelineStage::Match(normalized));
            }
            GraphPipelineStage::ShortestPath(shortest_stage) => {
                let normalized = normalize_pipeline_shortest_path_stage(
                    shortest_stage,
                    &current_schema,
                    query,
                )?;
                current_any_value_slots = pipeline_remap_any_value_slots(
                    &current_any_value_slots,
                    &normalized.input_mappings,
                );
                current_schema = normalized.output_schema.clone();
                stages.push(NormalizedGraphPipelineStage::ShortestPath(normalized));
            }
            GraphPipelineStage::Project(project_stage) => {
                let is_terminal = project_stage.kind == GraphProjectKind::Return;
                if is_terminal && index + 1 != query.stages.len() {
                    return Err(EngineError::InvalidOperation(
                        "terminal Project(Return) must be the final graph pipeline stage"
                            .to_string(),
                    ));
                }
                let normalized = normalize_pipeline_project_stage(
                    project_stage,
                    &current_schema,
                    query,
                    subquery_depth,
                )?;
                if is_terminal {
                    columns = normalized.columns.clone();
                    terminal_schema = Some(normalized.output_schema.clone());
                    let terminal_graph_items = pipeline_terminal_graph_return_items(&normalized.items);
                    terminal_output_needs = Some(
                        crate::graph_row::collect_graph_row_projection_needs(
                            &normalized.output_schema,
                            &[],
                            &[],
                            None,
                            &[],
                            &terminal_graph_items,
                            &query.output,
                        )?
                        .output,
                    );
                    terminal_return_items = Some(pipeline_terminal_return_items(
                        &normalized.output_schema,
                        &normalized.items,
                    )?);
                    terminal_order_by = Some(normalized.order_by.clone());
                    terminal_seen = true;
                }
                current_any_value_slots = pipeline_project_any_value_slots(
                    &current_any_value_slots,
                    &normalized,
                );
                current_schema = normalized.output_schema.clone();
                stages.push(NormalizedGraphPipelineStage::Project(normalized));
            }
            GraphPipelineStage::Union(union_stage) => {
                if index + 1 != query.stages.len() {
                    return Err(EngineError::InvalidOperation(
                        "GraphUnionStage must be the final graph pipeline stage".to_string(),
                    ));
                }
                let normalized = normalize_pipeline_union_stage(
                    union_stage,
                    &current_schema,
                    query,
                    subquery_depth,
                )?;
                columns = normalized.columns.clone();
                terminal_schema = Some(normalized.output_schema.clone());
                terminal_output_needs = Some(
                    crate::graph_row::collect_graph_row_projection_needs(
                        &normalized.output_schema,
                        &[],
                        &[],
                        None,
                        &[],
                        &pipeline_union_terminal_graph_return_items(&normalized),
                        &query.output,
                    )?
                    .output,
                );
                terminal_return_items =
                    Some(pipeline_union_terminal_return_items(&normalized)?);
                terminal_order_by = Some(Vec::new());
                terminal_seen = true;
                current_any_value_slots = normalized.cursor_any_value_slots.clone();
                current_schema = normalized.output_schema.clone();
                stages.push(NormalizedGraphPipelineStage::Union(normalized));
            }
            GraphPipelineStage::Call(call_stage) => {
                let normalized = normalize_pipeline_call_stage(
                    call_stage,
                    &current_schema,
                    query,
                    subquery_depth,
                )?;
                current_any_value_slots = pipeline_call_any_value_slots(
                    &current_any_value_slots,
                    &normalized,
                );
                current_schema = normalized.output_schema.clone();
                stages.push(NormalizedGraphPipelineStage::Call(normalized));
            }
        }
    }

    if !terminal_seen {
        return Err(EngineError::InvalidOperation(
            "graph pipeline requires a terminal Project(Return) stage".to_string(),
        ));
    }

    let terminal_schema = terminal_schema.expect("terminal schema recorded");
    let terminal_return_items = terminal_return_items.expect("terminal return items recorded");
    let terminal_output_needs = terminal_output_needs.expect("terminal output needs recorded");
    let terminal_order_by = terminal_order_by.expect("terminal order recorded");
    let fingerprint_shape = graph_pipeline_fingerprint_shape(
        query,
        &columns,
        &referenced_params,
        &terminal_order_by,
    );
    let preserve_pipeline_order = stages
        .iter()
        .any(|stage| matches!(stage, NormalizedGraphPipelineStage::Call(_)));

    Ok(NormalizedGraphPipeline {
        initial_schema,
        stages,
        columns,
        terminal_schema,
        terminal_any_value_slots: current_any_value_slots,
        terminal_return_items,
        terminal_output_needs,
        terminal_order_by,
        page: query.page.clone(),
        output: query.output.clone(),
        options: query.options.clone(),
        fingerprint_shape,
        preserve_pipeline_order,
    })
}

fn validate_graph_pipeline_request(query: &GraphPipelineQuery) -> Result<(), EngineError> {
    if query.stages.is_empty() {
        return Err(EngineError::InvalidOperation(
            "graph pipeline requires at least one stage".to_string(),
        ));
    }
    if query.page.limit == 0 {
        return Err(EngineError::InvalidOperation(
            "graph pipeline page limit must be greater than zero".to_string(),
        ));
    }
    if query.page.limit > query.options.max_rows {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline page limit {} exceeds max_rows {}",
            query.page.limit, query.options.max_rows
        )));
    }
    if query.page.skip > query.options.max_skip {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline page skip {} exceeds max_skip {}",
            query.page.skip, query.options.max_skip
        )));
    }
    if query.options.max_rows == 0
        || query.options.max_pipeline_rows == 0
        || query.options.max_groups == 0
        || query.options.max_shortest_path_pairs == 0
        || query.options.max_paths_per_start == 0
        || query.options.max_intermediate_bindings == 0
    {
        return Err(EngineError::InvalidOperation(
            "graph pipeline row, group, and path caps must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn normalize_pipeline_union_stage(
    stage: &GraphUnionStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
    parent: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<NormalizedPipelineUnionStage, EngineError> {
    if stage.branches.len() < 2 {
        return Err(EngineError::InvalidOperation(
            "GraphUnionStage requires at least two branches".to_string(),
        ));
    }
    if stage.branches.len() > parent.options.max_union_branches {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage has {} branch(es), exceeding max_union_branches {}",
            stage.branches.len(),
            parent.options.max_union_branches
        )));
    }

    let mut branches = Vec::with_capacity(stage.branches.len());
    for (index, branch) in stage.branches.iter().enumerate() {
        validate_pipeline_union_branch_request(index, branch, parent)?;
        let mut branch_query = branch.clone();
        branch_query.params = parent.params.clone();
        branch_query.at_epoch = None;
        branch_query.page = GraphPageRequest {
            skip: 0,
            limit: parent.options.max_rows.max(1),
            cursor: None,
        };
        branch_query.output = parent.output.clone();
        branch_query.options = parent.options.clone();
        branches.push(normalize_graph_pipeline_query_with_initial_schema(
            &branch_query,
            input_schema.clone(),
            subquery_depth,
        )?);
    }

    let first = branches.first().expect("branch count checked");
    let columns = first.columns.clone();
    let column_metadata = pipeline_union_column_metadata(&branches, &columns)?;
    let projections = column_metadata
        .iter()
        .map(|metadata| metadata.projection.clone())
        .collect::<Vec<_>>();
    let mut output_schema = crate::graph_row::GraphBindingSchema::new();
    let ordinal_slot = output_schema.add_internal_scalar(
        PIPELINE_UNION_ORDER_SLOT.to_string(),
        false,
    )?;
    let mut cursor_any_value_slots = Vec::new();
    for (column, metadata) in columns.iter().zip(column_metadata.iter()) {
        let slot = add_pipeline_output_slot(
            &mut output_schema,
            column,
            metadata.kind,
            metadata.nullable,
        )?;
        if metadata.kind == crate::graph_row::GraphBindingSlotKind::Scalar
            && (metadata.mixed_kinds || metadata.any_value)
        {
            cursor_any_value_slots.push(slot);
        }
    }
    let distinct_slots = pipeline_visible_slots(&output_schema);

    let normalized_branches = branches
        .into_iter()
        .enumerate()
        .map(|(index, pipeline)| {
            validate_pipeline_union_branch_columns(index, &columns, &output_schema, &pipeline)?;
            let output_mappings =
                pipeline_union_branch_output_mappings(&pipeline.terminal_schema, &output_schema, &columns)?;
            Ok(NormalizedPipelineUnionBranch {
                pipeline,
                output_mappings,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;

    Ok(NormalizedPipelineUnionStage {
        all: stage.all,
        branches: normalized_branches,
        output_schema,
        ordinal_slot,
        cursor_any_value_slots,
        distinct_slots,
        columns,
        projections,
    })
}

fn validate_pipeline_union_branch_request(
    index: usize,
    branch: &GraphPipelineQuery,
    parent: &GraphPipelineQuery,
) -> Result<(), EngineError> {
    if branch.page.cursor.is_some() {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage branch {} cannot supply a raw cursor",
            index + 1
        )));
    }
    if branch.page.skip != 0 {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage branch {} cannot use public page skip",
            index + 1
        )));
    }
    if branch.at_epoch.is_some() {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage branch {} cannot override the parent at_epoch",
            index + 1
        )));
    }
    if !branch.params.is_empty() && branch.params != parent.params {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage branch {} params must match the parent pipeline params",
            index + 1
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PipelineUnionColumnMetadata {
    kind: crate::graph_row::GraphBindingSlotKind,
    nullable: bool,
    projection: GraphReturnProjection,
    mixed_kinds: bool,
    any_value: bool,
}

fn pipeline_union_column_metadata(
    branches: &[NormalizedGraphPipeline],
    columns: &[String],
) -> Result<Vec<PipelineUnionColumnMetadata>, EngineError> {
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let first = branches.first().expect("branch count checked");
            let first_source = first.terminal_schema.slot_for_alias(column).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GraphUnionStage first branch terminal schema is missing column '{column}'"
                ))
            })?;
            let first_info = first.terminal_schema.slot(first_source).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GraphUnionStage first branch column '{column}' has no slot metadata"
                ))
            })?;
            if first_info.kind == crate::graph_row::GraphBindingSlotKind::HiddenOccurrence {
                return Err(EngineError::InvalidOperation(format!(
                    "GraphUnionStage branch 1 column '{column}' cannot expose a hidden occurrence slot"
                )));
            }
            let mut nullable = first_info.nullable;
            let mut kinds = vec![first_info.kind];
            let mut any_value = first.terminal_any_value_slots.contains(&first_source);
            let first_projection =
                first.terminal_return_items.get(column_index).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "GraphUnionStage first branch is missing return projection for column '{column}'"
                    ))
                })?;
            let mut projections = vec![first_projection.projection.clone()];
            for (index, branch) in branches.iter().enumerate().skip(1) {
                if branch.columns.len() != columns.len() || branch.columns != columns {
                    continue;
                }
                let source = branch.terminal_schema.slot_for_alias(column).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "GraphUnionStage branch {} terminal schema is missing column '{column}'",
                        index + 1
                    ))
                })?;
                let source_info = branch.terminal_schema.slot(source).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "GraphUnionStage branch {} column '{column}' has no slot metadata",
                        index + 1
                    ))
                })?;
                if source_info.kind == crate::graph_row::GraphBindingSlotKind::HiddenOccurrence {
                    return Err(EngineError::InvalidOperation(format!(
                        "GraphUnionStage branch {} column '{column}' cannot expose a hidden occurrence slot",
                        index + 1
                    )));
                }
                nullable |= source_info.nullable;
                kinds.push(source_info.kind);
                any_value |= branch.terminal_any_value_slots.contains(&source);
                let projection = branch.terminal_return_items.get(column_index).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "GraphUnionStage branch {} is missing return projection for column '{column}'",
                        index + 1
                    ))
                })?;
                projections.push(projection.projection.clone());
            }
            Ok(PipelineUnionColumnMetadata {
                kind: pipeline_union_output_kind(&kinds),
                nullable,
                projection: pipeline_union_output_projection(&projections),
                mixed_kinds: pipeline_union_kinds_are_mixed(&kinds),
                any_value,
            })
        })
        .collect()
}

fn pipeline_union_kinds_are_mixed(kinds: &[crate::graph_row::GraphBindingSlotKind]) -> bool {
    let first = kinds.first().copied().expect("at least one branch kind");
    kinds.iter().any(|kind| *kind != first)
}

fn pipeline_union_output_kind(
    kinds: &[crate::graph_row::GraphBindingSlotKind],
) -> crate::graph_row::GraphBindingSlotKind {
    let first = kinds.first().copied().expect("at least one branch kind");
    if kinds.iter().all(|kind| *kind == first) {
        first
    } else {
        crate::graph_row::GraphBindingSlotKind::Scalar
    }
}

fn pipeline_union_output_projection(projections: &[GraphReturnProjection]) -> GraphReturnProjection {
    let first = projections
        .first()
        .cloned()
        .expect("at least one branch projection");
    if projections.iter().all(|projection| projection == &first) {
        return first;
    }
    // Actual union output uses branch-aware row projection sidecars. This fallback is
    // intentionally non-escalating so metadata never widens Selected(...) to Full.
    GraphReturnProjection::Auto
}

fn validate_pipeline_union_branch_columns(
    index: usize,
    columns: &[String],
    output_schema: &crate::graph_row::GraphBindingSchema,
    branch: &NormalizedGraphPipeline,
) -> Result<(), EngineError> {
    if branch.columns.len() != columns.len() {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage branch {} returns {} column(s), expected {}",
            index + 1,
            branch.columns.len(),
            columns.len()
        )));
    }
    if branch.columns != columns {
        return Err(EngineError::InvalidOperation(format!(
            "GraphUnionStage branch {} columns {:?} do not match {:?}",
            index + 1,
            branch.columns,
            columns
        )));
    }
    for column in columns {
        let source = branch.terminal_schema.slot_for_alias(column).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphUnionStage branch {} terminal schema is missing column '{column}'",
                index + 1
            ))
        })?;
        let target = output_schema.slot_for_alias(column).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphUnionStage output schema is missing column '{column}'"
            ))
        })?;
        let source_info = branch.terminal_schema.slot(source).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphUnionStage branch {} column '{column}' has no slot metadata",
                index + 1
            ))
        })?;
        let target_info = output_schema.slot(target).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphUnionStage output column '{column}' has no slot metadata"
            ))
        })?;
        if source_info.kind == crate::graph_row::GraphBindingSlotKind::HiddenOccurrence {
            return Err(EngineError::InvalidOperation(format!(
                "GraphUnionStage branch {} column '{column}' cannot expose a hidden occurrence slot",
                index + 1
            )));
        }
        if target_info.kind != crate::graph_row::GraphBindingSlotKind::Scalar
            && source_info.kind != target_info.kind
        {
            return Err(EngineError::InvalidOperation(format!(
                "GraphUnionStage branch {} column '{column}' has kind {:?}, expected {:?}",
                index + 1,
                source_info.kind,
                target_info.kind
            )));
        }
    }
    Ok(())
}

fn pipeline_union_branch_output_mappings(
    branch_schema: &crate::graph_row::GraphBindingSchema,
    output_schema: &crate::graph_row::GraphBindingSchema,
    columns: &[String],
) -> Result<Vec<PipelineSlotMapping>, EngineError> {
    columns
        .iter()
        .map(|column| {
            let source = branch_schema.slot_for_alias(column).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GraphUnionStage branch schema is missing column '{column}'"
                ))
            })?;
            let target = output_schema.slot_for_alias(column).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GraphUnionStage output schema is missing column '{column}'"
                ))
            })?;
            Ok(PipelineSlotMapping { source, target })
        })
        .collect()
}

fn normalize_pipeline_match_stage(
    stage: &GraphPipelineMatchStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
    query: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<NormalizedPipelineMatchStage, EngineError> {
    if !stage.optional && stage.optional_candidate_where.is_some() {
        return Err(EngineError::InvalidOperation(
            "GraphPipelineMatchStage optional_candidate_where requires optional=true".to_string(),
        ));
    }
    let mut nodes = stage.nodes.clone();
    let mut pieces = pipeline_match_stage_pieces(stage, input_schema)?;
    let where_ = if stage.optional {
        None
    } else {
        stage
            .where_
            .as_ref()
            .map(|expr| resolve_graph_expr_params(expr, &query.params))
            .transpose()?
    };
    if stage.optional {
        let optional_where = stage
            .where_
            .as_ref()
            .map(|expr| resolve_graph_expr_params(expr, &query.params))
            .transpose()?;
        pieces = vec![GraphPatternPiece::Optional(GraphOptionalGroup {
            pieces,
            where_: optional_where,
        })];
    }
    pipeline_add_input_node_patterns(input_schema, &mut nodes);
    validate_pipeline_node_aliases(&nodes)?;
    validate_pipeline_piece_aliases(&pieces)?;
    let mut options = graph_query_options_from_pipeline(&query.options);
    options.max_page_limit = options
        .max_page_limit
        .max(query.options.max_intermediate_bindings.max(1));
    let stage_page_limit = query.options.max_intermediate_bindings.saturating_sub(1).max(1);
    let graph_row_query = GraphRowQuery {
        nodes,
        pieces,
        where_,
        return_items: None,
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: stage_page_limit,
            cursor: None,
        },
        at_epoch: query.at_epoch,
        params: query.params.clone(),
        output: query.output.clone(),
        options,
    };
    let normalized = normalize_graph_row_query_with_pipeline_input(
        &graph_row_query,
        &BTreeMap::new(),
        None,
        &[],
        input_schema,
    )?;
    let mut output_schema = normalized.binding_schema.clone();
    let cursor_slot = ensure_pipeline_cursor_key_slot(&mut output_schema)?;
    let input_mappings = pipeline_slot_mappings(input_schema, &normalized.binding_schema)?;
    let output_mappings = pipeline_slot_mappings(&normalized.binding_schema, &output_schema)?;
    let input_slots = input_mappings
        .iter()
        .map(|mapping| mapping.target)
        .collect::<Vec<_>>();
    let optional_slots = pipeline_optional_match_introduced_slots(
        &normalized.binding_schema,
        &input_slots,
    );
    let optional_candidate_filter = stage
        .optional_candidate_where
        .as_ref()
        .map(|expr| {
            normalize_pipeline_optional_candidate_filter(
                expr,
                &normalized.binding_schema,
                query,
                subquery_depth,
            )
        })
        .transpose()?;
    Ok(NormalizedPipelineMatchStage {
        optional: stage.optional,
        query: normalized,
        output_schema,
        output_mappings,
        cursor_slot,
        input_mappings,
        input_slots,
        optional_slots,
        optional_candidate_filter,
    })
}

fn pipeline_optional_match_introduced_slots(
    schema: &crate::graph_row::GraphBindingSchema,
    input_slots: &[crate::graph_row::GraphBindingSlotRef],
) -> Vec<crate::graph_row::GraphBindingSlotRef> {
    schema
        .slots()
        .iter()
        .filter_map(|slot| {
            let slot_ref = crate::graph_row::GraphBindingSlotRef {
                kind: slot.kind,
                index: slot.index,
            };
            if slot.nullable && !input_slots.contains(&slot_ref) {
                Some(slot_ref)
            } else {
                None
            }
        })
        .collect()
}

fn normalize_pipeline_optional_candidate_filter(
    expr: &GraphExpr,
    base_schema: &crate::graph_row::GraphBindingSchema,
    query: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<NormalizedPipelineOptionalCandidateFilter, EngineError> {
    let mut eval_schema = base_schema.clone();
    let mut where_expr = Some(resolve_graph_expr_params(expr, &query.params)?);
    let exists_predicates = normalize_pipeline_exists_predicates(
        &mut where_expr,
        &mut eval_schema,
        query,
        subquery_depth,
    )?;
    let where_expr = where_expr.ok_or_else(|| {
        EngineError::InvalidOperation(
            "optional candidate filter normalization lost the predicate".to_string(),
        )
    })?;
    let filter_needs = crate::graph_row::collect_graph_expr_projection_needs(
        &eval_schema,
        &where_expr,
        ProjectionNeedClass::Residual,
    )?;
    let where_expr = crate::graph_row::bind_graph_expr(&eval_schema, &where_expr)?;
    Ok(NormalizedPipelineOptionalCandidateFilter {
        eval_schema,
        filter_needs,
        where_expr,
        exists_predicates,
    })
}

fn pipeline_match_stage_pieces(
    stage: &GraphPipelineMatchStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
) -> Result<Vec<GraphPatternPiece>, EngineError> {
    if !stage.pieces.is_empty() {
        return Ok(stage.pieces.clone());
    }
    let new_nodes = stage
        .nodes
        .iter()
        .filter(|node| input_schema.slot_for_alias(&node.alias).is_none())
        .collect::<Vec<_>>();
    if new_nodes.len() > 1 {
        return Err(EngineError::InvalidOperation(
            "graph pipeline node-only Match stages may introduce at most one unconnected node alias"
                .to_string(),
        ));
    }
    Ok(Vec::new())
}

fn normalize_pipeline_shortest_path_stage(
    stage: &GraphShortestPathStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
    query: &GraphPipelineQuery,
) -> Result<NormalizedPipelineShortestPathStage, EngineError> {
    validate_graph_pipeline_user_alias(&stage.output_path_alias, "shortest-path alias")?;
    if stage.min_hops > stage.max_hops {
        return Err(EngineError::InvalidOperation(format!(
            "GraphShortestPathStage min_hops {} exceeds max_hops {}",
            stage.min_hops, stage.max_hops
        )));
    }
    if stage.max_hops > query.options.max_path_hops {
        return Err(EngineError::InvalidOperation(format!(
            "GraphShortestPathStage max_hops {} exceeds max_path_hops {}",
            stage.max_hops, query.options.max_path_hops
        )));
    }
    if stage.mode == GraphShortestPathMode::All
        && stage.max_paths.unwrap_or(query.options.max_paths_per_start) == 0
    {
        return Err(EngineError::InvalidOperation(
            "GraphShortestPathStage max_paths must be greater than zero".to_string(),
        ));
    }
    let mut output_schema = input_schema.clone();
    let output_path_slot = output_schema.add_path_alias(stage.output_path_alias.clone(), stage.optional)?;
    let input_mappings = pipeline_slot_mappings(input_schema, &output_schema)?;
    let from = normalize_shortest_path_endpoint(&stage.from, input_schema, &query.params)?;
    let to = normalize_shortest_path_endpoint(&stage.to, input_schema, &query.params)?;
    let mut edge_label_filter = stage.edge_label_filter.clone();
    edge_label_filter.sort();
    edge_label_filter.dedup();
    Ok(NormalizedPipelineShortestPathStage {
        optional: stage.optional,
        mode: stage.mode,
        output_schema,
        input_mappings,
        output_path_slot,
        output_path_alias: stage.output_path_alias.clone(),
        from,
        to,
        direction: stage.direction,
        edge_label_filter,
        min_hops: stage.min_hops,
        max_hops: stage.max_hops,
        weight_field: stage.weight_field.clone(),
        max_cost: stage.max_cost,
        max_paths: stage.max_paths,
    })
}

fn normalize_shortest_path_endpoint(
    endpoint: &GraphShortestPathEndpoint,
    input_schema: &crate::graph_row::GraphBindingSchema,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<NormalizedShortestPathEndpoint, EngineError> {
    match endpoint {
        GraphShortestPathEndpoint::Alias(alias) => {
            validate_graph_pipeline_user_alias(alias, "shortest-path endpoint alias")?;
            let slot = input_schema.slot_for_alias(alias).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GraphShortestPathStage endpoint alias '{alias}' is not available in pipeline scope"
                ))
            })?;
            let slot_info = input_schema.slot(slot).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "GraphShortestPathStage endpoint alias '{alias}' slot is missing"
                ))
            })?;
            if slot_info.kind != crate::graph_row::GraphBindingSlotKind::Node {
                return Err(EngineError::InvalidOperation(format!(
                    "GraphShortestPathStage endpoint alias '{alias}' must be a node alias"
                )));
            }
            Ok(NormalizedShortestPathEndpoint::Alias {
                alias: alias.clone(),
                slot,
            })
        }
        GraphShortestPathEndpoint::NodeId(id) => Ok(NormalizedShortestPathEndpoint::NodeId(*id)),
        GraphShortestPathEndpoint::NodeKey { label, key } => {
            if label.is_empty() || key.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "GraphShortestPathStage NodeKey endpoints require non-empty label and key"
                        .to_string(),
                ));
            }
            Ok(NormalizedShortestPathEndpoint::NodeKey {
                label: label.clone(),
                key: key.clone(),
            })
        }
        GraphShortestPathEndpoint::Expr(expr) => {
            let resolved = resolve_graph_expr_params(expr, params)?;
            match resolved {
                GraphExpr::UInt(id) => Ok(NormalizedShortestPathEndpoint::NodeId(id)),
                GraphExpr::Int(id) if id >= 0 => Ok(NormalizedShortestPathEndpoint::NodeId(id as u64)),
                _ => Err(EngineError::InvalidOperation(
                    "GraphShortestPathStage expression endpoints must resolve to constant node IDs"
                        .to_string(),
                )),
            }
        }
    }
}

fn normalize_pipeline_call_stage(
    stage: &GraphSubqueryStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
    parent: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<NormalizedPipelineCallStage, EngineError> {
    let next_depth = subquery_depth.saturating_add(1);
    if next_depth > parent.options.max_subquery_depth {
        return Err(EngineError::InvalidOperation(format!(
            "GraphSubqueryStage depth {next_depth} exceeds max_subquery_depth {}",
            parent.options.max_subquery_depth
        )));
    }
    let (subquery_input_schema, import_slots, import_mappings) =
        pipeline_import_schema_and_mappings(input_schema, &stage.import_aliases)?;
    let mut subquery = (*stage.query).clone();
    subquery.params = parent.params.clone();
    subquery.at_epoch = None;
    subquery.page = GraphPageRequest {
        skip: 0,
        limit: parent.options.max_pipeline_rows.max(1),
        cursor: None,
    };
    subquery.output = parent.output.clone();
    subquery.options = parent.options.clone();
    subquery.options.max_rows = parent.options.max_pipeline_rows.max(1);
    let query = normalize_graph_pipeline_query_with_initial_schema(
        &subquery,
        subquery_input_schema,
        next_depth,
    )?;

    let mut output_schema = input_schema.clone();
    let input_mappings = pipeline_slot_mappings(input_schema, &output_schema)?;
    let mut output_mappings = Vec::with_capacity(query.columns.len());
    for column in &query.columns {
        if output_schema.slot_for_alias(column).is_some() {
            return Err(EngineError::InvalidOperation(format!(
                "GraphSubqueryStage output '{column}' collides with an incoming alias"
            )));
        }
        let source = query.terminal_schema.slot_for_alias(column).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphSubqueryStage terminal schema is missing output column '{column}'"
            ))
        })?;
        let source_info = query.terminal_schema.slot(source).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphSubqueryStage output column '{column}' has no slot metadata"
            ))
        })?;
        if source_info.kind == crate::graph_row::GraphBindingSlotKind::HiddenOccurrence {
            return Err(EngineError::InvalidOperation(format!(
                "GraphSubqueryStage output column '{column}' cannot expose a hidden occurrence slot"
            )));
        }
        let target = add_pipeline_output_slot(
            &mut output_schema,
            column,
            source_info.kind,
            source_info.nullable,
        )?;
        output_mappings.push(PipelineSlotMapping { source, target });
    }
    Ok(NormalizedPipelineCallStage {
        input_schema: input_schema.clone(),
        output_schema,
        input_mappings,
        import_aliases: stage.import_aliases.clone(),
        import_slots,
        import_mappings,
        output_mappings,
        columns: query.columns.clone(),
        query,
    })
}

fn normalize_pipeline_exists_predicates(
    expr: &mut Option<GraphExpr>,
    schema: &mut crate::graph_row::GraphBindingSchema,
    parent: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<Vec<NormalizedPipelineExistsPredicate>, EngineError> {
    let Some(expr) = expr.as_mut() else {
        return Ok(Vec::new());
    };
    let mut predicates = Vec::new();
    rewrite_pipeline_exists_expr(expr, schema, parent, subquery_depth, &mut predicates)?;
    Ok(predicates)
}

fn rewrite_pipeline_exists_expr(
    expr: &mut GraphExpr,
    schema: &mut crate::graph_row::GraphBindingSchema,
    parent: &GraphPipelineQuery,
    subquery_depth: usize,
    predicates: &mut Vec<NormalizedPipelineExistsPredicate>,
) -> Result<(), EngineError> {
    match expr {
        GraphExpr::ExistsSubquery(stage) => {
            let next_depth = subquery_depth.saturating_add(1);
            if next_depth > parent.options.max_subquery_depth {
                return Err(EngineError::InvalidOperation(format!(
                    "EXISTS subquery depth {next_depth} exceeds max_subquery_depth {}",
                    parent.options.max_subquery_depth
                )));
            }
            let (subquery_input_schema, import_slots, import_mappings) =
                pipeline_import_schema_and_mappings(schema, &stage.import_aliases)?;
            let mut subquery = (*stage.query).clone();
            subquery.params = parent.params.clone();
            subquery.at_epoch = None;
            subquery.page = GraphPageRequest {
                skip: 0,
                limit: 1,
                cursor: None,
            };
            subquery.output = parent.output.clone();
            subquery.options = parent.options.clone();
            let query = normalize_graph_pipeline_query_with_initial_schema(
                &subquery,
                subquery_input_schema,
                next_depth,
            )?;
            let mut output_index = predicates.len();
            let output_alias = loop {
                let candidate = format!("__og_exists_{output_index}");
                if schema.slot_for_alias(&candidate).is_none() {
                    break candidate;
                }
                output_index = output_index.saturating_add(1);
            };
            let output_slot = schema.add_scalar_alias(output_alias.clone(), false)?;
            let internal_limit = exists_query_has_internal_limit(&stage.query);
            predicates.push(NormalizedPipelineExistsPredicate {
                output_slot,
                output_alias: output_alias.clone(),
                import_aliases: stage.import_aliases.clone(),
                import_slots,
                import_mappings,
                query,
                internal_limit,
            });
            *expr = GraphExpr::Binding(output_alias);
        }
        GraphExpr::List(items) => {
            for item in items {
                rewrite_pipeline_exists_expr(item, schema, parent, subquery_depth, predicates)?;
            }
        }
        GraphExpr::Map(items) => {
            for item in items.values_mut() {
                rewrite_pipeline_exists_expr(item, schema, parent, subquery_depth, predicates)?;
            }
        }
        GraphExpr::Function { args, .. } => {
            for arg in args {
                rewrite_pipeline_exists_expr(arg, schema, parent, subquery_depth, predicates)?;
            }
        }
        GraphExpr::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_mut() {
                rewrite_pipeline_exists_expr(arg, schema, parent, subquery_depth, predicates)?;
            }
        }
        GraphExpr::Unary { expr, .. } | GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            rewrite_pipeline_exists_expr(expr, schema, parent, subquery_depth, predicates)?;
        }
        GraphExpr::Binary { left, right, .. } => {
            rewrite_pipeline_exists_expr(left, schema, parent, subquery_depth, predicates)?;
            rewrite_pipeline_exists_expr(right, schema, parent, subquery_depth, predicates)?;
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_mut() {
                rewrite_pipeline_exists_expr(operand, schema, parent, subquery_depth, predicates)?;
            }
            for branch in branches {
                rewrite_pipeline_exists_expr(
                    &mut branch.when,
                    schema,
                    parent,
                    subquery_depth,
                    predicates,
                )?;
                rewrite_pipeline_exists_expr(
                    &mut branch.then,
                    schema,
                    parent,
                    subquery_depth,
                    predicates,
                )?;
            }
            if let Some(else_expr) = else_expr.as_mut() {
                rewrite_pipeline_exists_expr(
                    else_expr,
                    schema,
                    parent,
                    subquery_depth,
                    predicates,
                )?;
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
        | GraphExpr::Property { .. }
        | GraphExpr::NodeField { .. }
        | GraphExpr::EdgeField { .. }
        | GraphExpr::PathField { .. } => {}
    }
    Ok(())
}

fn exists_query_has_internal_limit(query: &GraphPipelineQuery) -> bool {
    query.stages.iter().rev().any(|stage| {
        match stage {
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                limit: Some(GraphExpr::UInt(1)),
                ..
            }) => true,
            GraphPipelineStage::Union(union) => union
                .branches
                .iter()
                .all(exists_query_has_internal_limit),
            _ => false,
        }
    })
}

fn pipeline_import_schema_and_mappings(
    input_schema: &crate::graph_row::GraphBindingSchema,
    import_aliases: &[String],
) -> Result<
    (
        crate::graph_row::GraphBindingSchema,
        Vec<crate::graph_row::GraphBindingSlotRef>,
        Vec<PipelineSlotMapping>,
    ),
    EngineError,
> {
    let mut seen = BTreeSet::new();
    let mut import_schema = crate::graph_row::GraphBindingSchema::new();
    let mut import_slots = Vec::with_capacity(import_aliases.len());
    let mut mappings = Vec::with_capacity(import_aliases.len());
    for alias in import_aliases {
        validate_graph_pipeline_user_alias(alias, "subquery import alias")?;
        if !seen.insert(alias.clone()) {
            return Err(EngineError::InvalidOperation(format!(
                "GraphSubqueryStage imports alias '{alias}' more than once"
            )));
        }
        let source = input_schema.slot_for_alias(alias).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphSubqueryStage import alias '{alias}' is not available in pipeline scope"
            ))
        })?;
        let source_info = input_schema.slot(source).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "GraphSubqueryStage import alias '{alias}' slot is missing"
            ))
        })?;
        if source_info.kind == crate::graph_row::GraphBindingSlotKind::HiddenOccurrence {
            return Err(EngineError::InvalidOperation(format!(
                "GraphSubqueryStage import alias '{alias}' cannot reference a hidden occurrence"
            )));
        }
        let target = add_pipeline_output_slot(
            &mut import_schema,
            alias,
            source_info.kind,
            source_info.nullable,
        )?;
        import_slots.push(source);
        mappings.push(PipelineSlotMapping { source, target });
    }
    Ok((import_schema, import_slots, mappings))
}

fn validate_pipeline_node_aliases(nodes: &[GraphNodePattern]) -> Result<(), EngineError> {
    for node in nodes {
        if graph_pipeline_alias_is_generated_anonymous_node(&node.alias) {
            continue;
        }
        validate_graph_pipeline_user_alias(&node.alias, "node alias")?;
    }
    Ok(())
}

fn validate_pipeline_piece_aliases(pieces: &[GraphPatternPiece]) -> Result<(), EngineError> {
    for piece in pieces {
        match piece {
            GraphPatternPiece::Edge(edge) => {
                if let Some(alias) = edge.alias.as_ref() {
                    validate_graph_pipeline_user_alias(alias, "edge alias")?;
                }
            }
            GraphPatternPiece::Optional(group) => validate_pipeline_piece_aliases(&group.pieces)?,
            GraphPatternPiece::VariableLength(path) => {
                if let Some(alias) = path.path_alias.as_ref() {
                    validate_graph_pipeline_user_alias(alias, "path alias")?;
                }
                if let Some(alias) = path.edge_alias.as_ref() {
                    validate_graph_pipeline_user_alias(alias, "edge alias")?;
                }
            }
        }
    }
    Ok(())
}

fn validate_graph_pipeline_user_alias(alias: &str, context: &str) -> Result<(), EngineError> {
    if alias.starts_with("__gql_") || alias.starts_with("__og_") || alias == PIPELINE_CURSOR_KEY_SLOT {
        return Err(EngineError::InvalidOperation(format!(
            "graph pipeline {context} '{alias}' uses a reserved internal alias prefix"
        )));
    }
    Ok(())
}

fn graph_pipeline_alias_is_generated_anonymous_node(alias: &str) -> bool {
    alias
        .strip_prefix("__gql_anon_node_")
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()))
}

fn pipeline_add_input_node_patterns(
    input_schema: &crate::graph_row::GraphBindingSchema,
    nodes: &mut Vec<GraphNodePattern>,
) {
    let mut seen = nodes
        .iter()
        .map(|node| node.alias.clone())
        .collect::<BTreeSet<_>>();
    for slot in input_schema.slots() {
        if slot.kind != crate::graph_row::GraphBindingSlotKind::Node {
            continue;
        }
        let Some(alias) = slot.user_alias.as_ref() else {
            continue;
        };
        if seen.insert(alias.clone()) {
            nodes.push(GraphNodePattern {
                alias: alias.clone(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            });
        }
    }
}

fn pipeline_slot_mappings(
    input: &crate::graph_row::GraphBindingSchema,
    output: &crate::graph_row::GraphBindingSchema,
) -> Result<Vec<PipelineSlotMapping>, EngineError> {
    let mut mappings = Vec::new();
    for slot in input.slots() {
        let source = if let Some(alias) = slot.user_alias.as_ref() {
            input.slot_for_alias(alias).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph pipeline input schema is missing alias '{alias}'"
                ))
            })?
        } else if pipeline_internal_cursor_slot_info(slot) {
            crate::graph_row::GraphBindingSlotRef {
                kind: slot.kind,
                index: slot.index,
            }
        } else {
            continue;
        };
        let target = if let Some(alias) = slot.user_alias.as_ref() {
            output.slot_for_alias(alias).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph pipeline stage schema dropped incoming alias '{alias}'"
                ))
            })?
        } else {
            pipeline_internal_cursor_slot(output).ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph pipeline stage schema dropped internal cursor key".to_string(),
                )
            })?
        };
        mappings.push(PipelineSlotMapping { source, target });
    }
    Ok(mappings)
}

fn pipeline_remap_any_value_slots(
    any_value_slots: &[crate::graph_row::GraphBindingSlotRef],
    mappings: &[PipelineSlotMapping],
) -> Vec<crate::graph_row::GraphBindingSlotRef> {
    let mut remapped = Vec::new();
    for mapping in mappings {
        if any_value_slots.contains(&mapping.source) && !remapped.contains(&mapping.target) {
            remapped.push(mapping.target);
        }
    }
    remapped
}

fn pipeline_project_any_value_slots(
    input_any_value_slots: &[crate::graph_row::GraphBindingSlotRef],
    stage: &NormalizedPipelineProjectStage,
) -> Vec<crate::graph_row::GraphBindingSlotRef> {
    let mut output_any_value_slots =
        pipeline_remap_any_value_slots(input_any_value_slots, &stage.internal_mappings);
    for item in &stage.items {
        if item
            .source_slot
            .is_some_and(|source| input_any_value_slots.contains(&source))
            && !output_any_value_slots.contains(&item.output_slot)
        {
            output_any_value_slots.push(item.output_slot);
        }
    }
    output_any_value_slots
}

fn pipeline_call_any_value_slots(
    input_any_value_slots: &[crate::graph_row::GraphBindingSlotRef],
    stage: &NormalizedPipelineCallStage,
) -> Vec<crate::graph_row::GraphBindingSlotRef> {
    let mut output_any_value_slots =
        pipeline_remap_any_value_slots(input_any_value_slots, &stage.input_mappings);
    for mapping in &stage.output_mappings {
        if stage.query.terminal_any_value_slots.contains(&mapping.source)
            && !output_any_value_slots.contains(&mapping.target)
        {
            output_any_value_slots.push(mapping.target);
        }
    }
    output_any_value_slots
}

fn normalize_pipeline_project_stage(
    stage: &GraphProjectStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
    query: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<NormalizedPipelineProjectStage, EngineError> {
    let params = &query.params;
    let mut output_schema = crate::graph_row::GraphBindingSchema::new();
    let mut items = Vec::new();
    let mut internal_mappings = Vec::new();
    if pipeline_internal_cursor_slot(input_schema).is_some() {
        ensure_pipeline_cursor_key_slot(&mut output_schema)?;
        internal_mappings = pipeline_internal_cursor_mappings(input_schema, &output_schema)?;
    }
    let project_items = pipeline_project_items(stage, input_schema)?
        .into_iter()
        .map(|mut item| {
            item.expr = resolve_graph_expr_params(&item.expr, params)?;
            Ok(item)
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let resolved_order = stage
        .order_by
        .iter()
        .map(|item| {
            Ok(GraphOrderItem {
                expr: resolve_graph_expr_params(&item.expr, params)?,
                direction: item.direction,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let contains_aggregate = project_items
        .iter()
        .any(|item| graph_expr_contains_aggregate(&item.expr))
        || resolved_order
            .iter()
            .any(|item| graph_expr_contains_aggregate(&item.expr));
    if contains_aggregate {
        if matches!(stage.items, GraphProjectionItems::Star) {
            return Err(EngineError::InvalidOperation(
                "graph pipeline * projections cannot be mixed with aggregate calls".to_string(),
            ));
        }
        return normalize_pipeline_aggregate_project_stage(
            stage,
            input_schema,
            output_schema,
            internal_mappings,
            project_items,
            resolved_order,
            params,
            query,
            subquery_depth,
        );
    }
    let mut input_needs = EntityProjectionNeeds::default();

    for item in project_items {
        let output_name = pipeline_project_output_name(&item)?;
        let source_slot = match &item.expr {
            GraphExpr::Binding(alias) => input_schema.slot_for_alias(alias),
            _ => None,
        };
        let (output_slot, bound_expr, expr_summary) = match source_slot {
            Some(source) => {
                let source_info = input_schema.slot(source).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph pipeline source slot {:?}:{} is missing",
                        source.kind, source.index
                    ))
                })?;
                let output_slot = add_pipeline_output_slot(
                    &mut output_schema,
                    &output_name,
                    source.kind,
                    source_info.nullable,
                )?;
                (output_slot, None, None)
            }
            None => {
                let needs = crate::graph_row::collect_graph_expr_projection_needs(
                    input_schema,
                    &item.expr,
                    ProjectionNeedClass::Residual,
                )?;
                input_needs.merge_from(&needs, ProjectionNeedClass::Residual)?;
                let output_slot = output_schema.add_scalar_alias(output_name.clone(), true)?;
                let bound = crate::graph_row::bind_graph_expr(input_schema, &item.expr)?;
                (output_slot, Some(bound), Some(format!("{:?}", item.expr)))
            }
        };
        items.push(NormalizedPipelineProjectItem {
            output_name,
            output_slot,
            source_slot,
            expr: bound_expr,
            aggregate_expr: None,
            expr_summary,
            projection: item.projection,
        });
    }

    let mut where_expr = stage
        .where_
        .as_ref()
        .map(|expr| resolve_graph_expr_params(expr, params))
        .transpose()?;
    let exists_predicates = normalize_pipeline_exists_predicates(
        &mut where_expr,
        &mut output_schema,
        query,
        subquery_depth,
    )?;
    let mut filter_needs = EntityProjectionNeeds::default();
    if let Some(expr) = where_expr.as_ref() {
        filter_needs = crate::graph_row::collect_graph_expr_projection_needs(
            &output_schema,
            expr,
            ProjectionNeedClass::Residual,
        )?;
    }
    let where_expr = where_expr
        .as_ref()
        .map(|expr| crate::graph_row::bind_graph_expr(&output_schema, expr))
        .transpose()?;

    let mut order_needs = EntityProjectionNeeds::default();
    for item in &resolved_order {
        let needs = crate::graph_row::collect_graph_expr_projection_needs(
            &output_schema,
            &item.expr,
            ProjectionNeedClass::Order,
        )?;
        order_needs.merge_from(&needs, ProjectionNeedClass::Order)?;
    }
    let order_by = crate::graph_row::bind_graph_order_items(&output_schema, &resolved_order)?;
    let skip = stage
        .skip
        .as_ref()
        .map(|expr| pipeline_count_expr(expr, params, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = stage
        .limit
        .as_ref()
        .map(|expr| pipeline_count_expr(expr, params, "LIMIT"))
        .transpose()?;

    let columns = items
        .iter()
        .map(|item| item.output_name.clone())
        .collect::<Vec<_>>();
    let distinct_slots = pipeline_visible_slots(&output_schema);
    Ok(NormalizedPipelineProjectStage {
        kind: stage.kind,
        distinct: stage.distinct,
        input_schema: input_schema.clone(),
        output_schema,
        items,
        internal_mappings,
        distinct_slots,
        aggregate: None,
        input_needs,
        filter_needs,
        order_needs,
        where_expr,
        exists_predicates,
        order_by,
        skip,
        limit,
        columns,
    })
}

#[allow(clippy::too_many_arguments)]
fn normalize_pipeline_aggregate_project_stage(
    stage: &GraphProjectStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
    mut output_schema: crate::graph_row::GraphBindingSchema,
    internal_mappings: Vec<PipelineSlotMapping>,
    project_items: Vec<GraphProjectItem>,
    resolved_order: Vec<GraphOrderItem>,
    params: &BTreeMap<String, GraphParamValue>,
    query: &GraphPipelineQuery,
    subquery_depth: usize,
) -> Result<NormalizedPipelineProjectStage, EngineError> {
    let mut analysis = PipelineAggregateAnalysis::default();
    for item in &project_items {
        aggregate_collect_group_exprs(&item.expr, false, &mut analysis.group_exprs);
    }
    let output_names = project_items
        .iter()
        .map(pipeline_project_output_name)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let project_item_output_names = project_items
        .iter()
        .map(|item| Ok((item.expr.clone(), pipeline_project_output_name(item)?)))
        .collect::<Result<Vec<_>, EngineError>>()?;
    for item in &resolved_order {
        if graph_expr_contains_aggregate(&item.expr) {
            aggregate_collect_group_exprs(&item.expr, false, &mut analysis.group_exprs);
        } else if !matches!(&item.expr, GraphExpr::Binding(alias) if output_names.contains(alias)) {
            aggregate_push_unique_expr(&mut analysis.group_exprs, item.expr.clone());
        }
    }

    let mut eval_schema = crate::graph_row::GraphBindingSchema::new();
    let internal_cursor_slot = if pipeline_internal_cursor_slot(input_schema).is_some() {
        Some(ensure_pipeline_cursor_key_slot(&mut output_schema)?)
    } else {
        None
    };

    let mut input_needs = EntityProjectionNeeds::default();
    let mut group_keys = Vec::with_capacity(analysis.group_exprs.len());
    for (index, expr) in analysis.group_exprs.iter().enumerate() {
        let needs = crate::graph_row::collect_graph_expr_projection_needs(
            input_schema,
            expr,
            ProjectionNeedClass::Residual,
        )?;
        input_needs.merge_from(&needs, ProjectionNeedClass::Residual)?;
        let eval_slot = eval_schema.add_scalar_alias(format!("__gql_group_{index}"), true)?;
        group_keys.push(NormalizedPipelineGroupKey {
            expr: crate::graph_row::bind_graph_expr(input_schema, expr)?,
            eval_slot,
            summary: format!("{expr:?}"),
        });
    }

    let mut rewritten_item_exprs = Vec::with_capacity(project_items.len());
    for item in project_items {
        let output_name = pipeline_project_output_name(&item)?;
        let rewritten = aggregate_rewrite_expr(&item.expr, &mut analysis)?;
        let source_slot = match &item.expr {
            GraphExpr::Binding(alias) if !graph_expr_contains_aggregate(&item.expr) => {
                input_schema.slot_for_alias(alias)
            }
            _ => None,
        };
        let output_slot = match source_slot {
            Some(source) => {
                let source_info = input_schema.slot(source).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph pipeline source slot {:?}:{} is missing",
                        source.kind, source.index
                    ))
                })?;
                add_pipeline_output_slot(
                    &mut output_schema,
                    &output_name,
                    source.kind,
                    source_info.nullable,
                )?
            }
            None => output_schema.add_scalar_alias(output_name.clone(), true)?,
        };
        rewritten_item_exprs.push((
            output_name,
            output_slot,
            rewritten,
            Some(format!("{:?}", item.expr)),
            item.projection,
        ));
    }

    let rewritten_order = resolved_order
        .iter()
        .map(|item| {
            let expr = if let Some(output_name) = project_item_output_names
                .iter()
                .find(|(expr, _)| expr == &item.expr)
                .map(|(_, output_name)| output_name.clone())
            {
                GraphExpr::Binding(output_name)
            } else if matches!(&item.expr, GraphExpr::Binding(alias) if output_names.contains(alias)) {
                item.expr.clone()
            } else if graph_expr_contains_aggregate(&item.expr)
                || !matches!(&item.expr, GraphExpr::Binding(alias) if output_names.contains(alias))
            {
                aggregate_rewrite_expr(&item.expr, &mut analysis)?
            } else {
                item.expr.clone()
            };
            Ok(GraphOrderItem {
                expr,
                direction: item.direction,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;

    let mut calls = Vec::with_capacity(analysis.aggregate_calls.len());
    for (index, call) in analysis.aggregate_calls.iter().enumerate() {
        let arg = call
            .arg
            .as_ref()
            .map(|arg| {
                let needs = crate::graph_row::collect_graph_expr_projection_needs(
                    input_schema,
                    arg,
                    ProjectionNeedClass::Residual,
                )?;
                input_needs.merge_from(&needs, ProjectionNeedClass::Residual)?;
                crate::graph_row::bind_graph_expr(input_schema, arg)
            })
            .transpose()?;
        let eval_slot = eval_schema.add_scalar_alias(format!("__gql_agg_{index}"), true)?;
        calls.push(NormalizedPipelineAggregateCall {
            function: call.function,
            distinct: call.distinct,
            arg,
            eval_slot,
            summary: aggregate_call_summary(call),
        });
    }

    let rewritten_items = rewritten_item_exprs
        .into_iter()
        .map(
            |(output_name, output_slot, rewritten, expr_summary, projection)| {
                Ok(NormalizedPipelineProjectItem {
                    output_name,
                    output_slot,
                    source_slot: None,
                    expr: None,
                    aggregate_expr: Some(crate::graph_row::bind_graph_expr(
                        &eval_schema,
                        &rewritten,
                    )?),
                    expr_summary,
                    projection,
                })
            },
        )
        .collect::<Result<Vec<_>, EngineError>>()?;

    let mut where_expr = stage
        .where_
        .as_ref()
        .map(|expr| resolve_graph_expr_params(expr, params))
        .transpose()?;
    let exists_predicates = normalize_pipeline_exists_predicates(
        &mut where_expr,
        &mut output_schema,
        query,
        subquery_depth,
    )?;
    let mut filter_needs = EntityProjectionNeeds::default();
    if let Some(expr) = where_expr.as_ref() {
        filter_needs = crate::graph_row::collect_graph_expr_projection_needs(
            &output_schema,
            expr,
            ProjectionNeedClass::Residual,
        )?;
    }
    let where_expr = where_expr
        .as_ref()
        .map(|expr| crate::graph_row::bind_graph_expr(&output_schema, expr))
        .transpose()?;

    let mut final_order = Vec::with_capacity(rewritten_order.len());
    let mut order_outputs = Vec::new();
    for (index, item) in rewritten_order.iter().enumerate() {
        if crate::graph_row::bind_graph_expr(&output_schema, &item.expr).is_ok() {
            final_order.push(item.clone());
            continue;
        }
        let bound = crate::graph_row::bind_graph_expr(&eval_schema, &item.expr)?;
        let alias = format!("__gql_order_{index}");
        let output_slot = output_schema.add_scalar_alias(alias.clone(), true)?;
        order_outputs.push(NormalizedPipelineAggregateOrderOutput {
            expr: bound,
            output_slot,
        });
        final_order.push(GraphOrderItem {
            expr: GraphExpr::Binding(alias),
            direction: item.direction,
        });
    }
    let mut order_needs = EntityProjectionNeeds::default();
    for item in &final_order {
        let needs = crate::graph_row::collect_graph_expr_projection_needs(
            &output_schema,
            &item.expr,
            ProjectionNeedClass::Order,
        )?;
        order_needs.merge_from(&needs, ProjectionNeedClass::Order)?;
    }
    let order_by = crate::graph_row::bind_graph_order_items(&output_schema, &final_order)?;
    let skip = stage
        .skip
        .as_ref()
        .map(|expr| pipeline_count_expr(expr, params, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = stage
        .limit
        .as_ref()
        .map(|expr| pipeline_count_expr(expr, params, "LIMIT"))
        .transpose()?;
    let columns = rewritten_items
        .iter()
        .map(|item| item.output_name.clone())
        .collect::<Vec<_>>();
    let distinct_slots = pipeline_visible_slots(&output_schema);
    Ok(NormalizedPipelineProjectStage {
        kind: stage.kind,
        distinct: stage.distinct,
        input_schema: input_schema.clone(),
        output_schema,
        items: rewritten_items,
        internal_mappings,
        distinct_slots,
        aggregate: Some(NormalizedPipelineAggregate {
            eval_schema,
            group_keys,
            calls,
            order_outputs,
            internal_cursor_slot,
        }),
        input_needs,
        filter_needs,
        order_needs,
        where_expr,
        exists_predicates,
        order_by,
        skip,
        limit,
        columns,
    })
}

#[derive(Default)]
struct PipelineAggregateAnalysis {
    group_exprs: Vec<GraphExpr>,
    aggregate_calls: Vec<PipelineAggregateCallExpr>,
}

#[derive(Clone)]
struct PipelineAggregateCallExpr {
    function: GraphAggregateFunction,
    distinct: bool,
    arg: Option<GraphExpr>,
}

fn graph_expr_contains_aggregate(expr: &GraphExpr) -> bool {
    match expr {
        GraphExpr::AggregateCall { .. } => true,
        GraphExpr::ExistsSubquery(stage) => stage
            .query
            .stages
            .iter()
            .any(graph_pipeline_stage_contains_aggregate),
        GraphExpr::List(items) => items.iter().any(graph_expr_contains_aggregate),
        GraphExpr::Map(items) => items.values().any(graph_expr_contains_aggregate),
        GraphExpr::Function { args, .. } => args.iter().any(graph_expr_contains_aggregate),
        GraphExpr::Unary { expr, .. } | GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            graph_expr_contains_aggregate(expr)
        }
        GraphExpr::Binary { left, right, .. } => {
            graph_expr_contains_aggregate(left) || graph_expr_contains_aggregate(right)
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| graph_expr_contains_aggregate(expr))
                || branches.iter().any(|branch| {
                    graph_expr_contains_aggregate(&branch.when)
                        || graph_expr_contains_aggregate(&branch.then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| graph_expr_contains_aggregate(expr))
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

fn graph_pipeline_stage_contains_aggregate(stage: &GraphPipelineStage) -> bool {
    match stage {
        GraphPipelineStage::Match(stage) => {
            stage
                .where_
                .as_ref()
                .is_some_and(graph_expr_contains_aggregate)
                || stage
                    .optional_candidate_where
                    .as_ref()
                    .is_some_and(graph_expr_contains_aggregate)
        }
        GraphPipelineStage::Project(stage) => {
            (match &stage.items {
                GraphProjectionItems::Star => false,
                GraphProjectionItems::Items(items) => items
                    .iter()
                    .any(|item| graph_expr_contains_aggregate(&item.expr)),
            }) || stage
                .where_
                .as_ref()
                .is_some_and(graph_expr_contains_aggregate)
                || stage
                    .order_by
                    .iter()
                    .any(|item| graph_expr_contains_aggregate(&item.expr))
                || stage
                    .skip
                    .as_ref()
                    .is_some_and(graph_expr_contains_aggregate)
                || stage
                    .limit
                    .as_ref()
                    .is_some_and(graph_expr_contains_aggregate)
        }
        GraphPipelineStage::Call(stage) => stage
            .query
            .stages
            .iter()
            .any(graph_pipeline_stage_contains_aggregate),
        GraphPipelineStage::Union(stage) => stage.branches.iter().any(|branch| {
            branch
                .stages
                .iter()
                .any(graph_pipeline_stage_contains_aggregate)
        }),
        GraphPipelineStage::ShortestPath(_) => false,
    }
}

fn aggregate_collect_group_exprs(
    expr: &GraphExpr,
    inside_aggregate: bool,
    group_exprs: &mut Vec<GraphExpr>,
) {
    if inside_aggregate {
        return;
    }
    if !graph_expr_contains_aggregate(expr) {
        if !graph_expr_is_literal_only(expr) {
            aggregate_push_unique_expr(group_exprs, expr.clone());
        }
        return;
    }
    match expr {
        GraphExpr::AggregateCall { .. } | GraphExpr::ExistsSubquery(_) => {}
        GraphExpr::List(items) => {
            for item in items {
                aggregate_collect_group_exprs(item, false, group_exprs);
            }
        }
        GraphExpr::Map(items) => {
            for item in items.values() {
                aggregate_collect_group_exprs(item, false, group_exprs);
            }
        }
        GraphExpr::Function { args, .. } => {
            for arg in args {
                aggregate_collect_group_exprs(arg, false, group_exprs);
            }
        }
        GraphExpr::Unary { expr, .. } | GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            aggregate_collect_group_exprs(expr, false, group_exprs);
        }
        GraphExpr::Binary { left, right, .. } => {
            aggregate_collect_group_exprs(left, false, group_exprs);
            aggregate_collect_group_exprs(right, false, group_exprs);
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                aggregate_collect_group_exprs(operand, false, group_exprs);
            }
            for branch in branches {
                aggregate_collect_group_exprs(&branch.when, false, group_exprs);
                aggregate_collect_group_exprs(&branch.then, false, group_exprs);
            }
            if let Some(else_expr) = else_expr {
                aggregate_collect_group_exprs(else_expr, false, group_exprs);
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
        | GraphExpr::Property { .. }
        | GraphExpr::NodeField { .. }
        | GraphExpr::EdgeField { .. }
        | GraphExpr::PathField { .. } => {}
    }
}

fn aggregate_push_unique_expr(group_exprs: &mut Vec<GraphExpr>, expr: GraphExpr) {
    if !group_exprs.iter().any(|existing| existing == &expr) {
        group_exprs.push(expr);
    }
}

fn graph_expr_is_literal_only(expr: &GraphExpr) -> bool {
    match expr {
        GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_) => true,
        GraphExpr::List(items) => items.iter().all(graph_expr_is_literal_only),
        GraphExpr::Map(items) => items.values().all(graph_expr_is_literal_only),
        _ => false,
    }
}

fn aggregate_rewrite_expr(
    expr: &GraphExpr,
    analysis: &mut PipelineAggregateAnalysis,
) -> Result<GraphExpr, EngineError> {
    if let Some(index) = analysis.group_exprs.iter().position(|group| group == expr) {
        return Ok(GraphExpr::Binding(format!("__gql_group_{index}")));
    }
    Ok(match expr {
        GraphExpr::AggregateCall {
            function,
            distinct,
            arg,
        } => {
            if *distinct && arg.is_none() {
                return Err(EngineError::InvalidOperation(
                    "graph pipeline aggregate DISTINCT requires an argument".to_string(),
                ));
            }
            if *function != GraphAggregateFunction::Count && arg.is_none() {
                return Err(EngineError::InvalidOperation(format!(
                    "graph pipeline {} aggregate requires an argument",
                    graph_aggregate_function_name(*function)
                )));
            }
            let index = analysis.aggregate_calls.len();
            analysis.aggregate_calls.push(PipelineAggregateCallExpr {
                function: *function,
                distinct: *distinct,
                arg: arg.as_ref().map(|arg| (**arg).clone()),
            });
            GraphExpr::Binding(format!("__gql_agg_{index}"))
        }
        GraphExpr::ExistsSubquery(_) => expr.clone(),
        GraphExpr::List(items) => GraphExpr::List(
            items
                .iter()
                .map(|item| aggregate_rewrite_expr(item, analysis))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphExpr::Map(items) => GraphExpr::Map(
            items
                .iter()
                .map(|(key, value)| Ok((key.clone(), aggregate_rewrite_expr(value, analysis)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphExpr::Function { name, args } => GraphExpr::Function {
            name: *name,
            args: args
                .iter()
                .map(|arg| aggregate_rewrite_expr(arg, analysis))
                .collect::<Result<Vec<_>, _>>()?,
        },
        GraphExpr::Unary { op, expr } => GraphExpr::Unary {
            op: *op,
            expr: Box::new(aggregate_rewrite_expr(expr, analysis)?),
        },
        GraphExpr::Binary { left, op, right } => GraphExpr::Binary {
            left: Box::new(aggregate_rewrite_expr(left, analysis)?),
            op: *op,
            right: Box::new(aggregate_rewrite_expr(right, analysis)?),
        },
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => GraphExpr::Case {
            operand: operand
                .as_ref()
                .map(|operand| aggregate_rewrite_expr(operand, analysis).map(Box::new))
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(GraphCaseBranch {
                        when: aggregate_rewrite_expr(&branch.when, analysis)?,
                        then: aggregate_rewrite_expr(&branch.then, analysis)?,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| aggregate_rewrite_expr(else_expr, analysis).map(Box::new))
                .transpose()?,
        },
        GraphExpr::IsNull(expr) => GraphExpr::IsNull(Box::new(aggregate_rewrite_expr(expr, analysis)?)),
        GraphExpr::IsNotNull(expr) => {
            GraphExpr::IsNotNull(Box::new(aggregate_rewrite_expr(expr, analysis)?))
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
        | GraphExpr::PathField { .. } => expr.clone(),
    })
}

fn aggregate_call_summary(call: &PipelineAggregateCallExpr) -> String {
    let name = graph_aggregate_function_name(call.function);
    let distinct = if call.distinct { "DISTINCT " } else { "" };
    let arg = call
        .arg
        .as_ref()
        .map(|arg| format!("{arg:?}"))
        .unwrap_or_else(|| "*".to_string());
    format!("{name}({distinct}{arg})")
}

fn graph_aggregate_function_name(function: GraphAggregateFunction) -> &'static str {
    match function {
        GraphAggregateFunction::Count => "count",
        GraphAggregateFunction::Sum => "sum",
        GraphAggregateFunction::Avg => "avg",
        GraphAggregateFunction::Min => "min",
        GraphAggregateFunction::Max => "max",
        GraphAggregateFunction::Collect => "collect",
    }
}

fn pipeline_visible_slots(
    schema: &crate::graph_row::GraphBindingSchema,
) -> Vec<crate::graph_row::GraphBindingSlotRef> {
    schema
        .slots()
        .iter()
        .filter(|slot| pipeline_slot_is_user_visible(slot))
        .map(|slot| crate::graph_row::GraphBindingSlotRef {
            kind: slot.kind,
            index: slot.index,
        })
        .collect()
}

fn pipeline_slot_is_user_visible(slot: &crate::graph_row::GraphBindingSlot) -> bool {
    slot.user_alias
        .as_ref()
        .is_some_and(|alias| !alias.starts_with("__gql_") && !alias.starts_with("__og_"))
}

fn ensure_pipeline_cursor_key_slot(
    schema: &mut crate::graph_row::GraphBindingSchema,
) -> Result<crate::graph_row::GraphBindingSlotRef, EngineError> {
    if let Some(slot) = pipeline_internal_cursor_slot(schema) {
        return Ok(slot);
    }
    schema.add_internal_scalar(PIPELINE_CURSOR_KEY_SLOT.to_string(), false)
}

fn pipeline_internal_cursor_mappings(
    input: &crate::graph_row::GraphBindingSchema,
    output: &crate::graph_row::GraphBindingSchema,
) -> Result<Vec<PipelineSlotMapping>, EngineError> {
    let Some(source) = pipeline_internal_cursor_slot(input) else {
        return Ok(Vec::new());
    };
    let target = pipeline_internal_cursor_slot(output).ok_or_else(|| {
        EngineError::InvalidOperation(
            "graph pipeline stage schema dropped internal cursor key".to_string(),
        )
    })?;
    Ok(vec![PipelineSlotMapping { source, target }])
}

fn pipeline_internal_cursor_slot(
    schema: &crate::graph_row::GraphBindingSchema,
) -> Option<crate::graph_row::GraphBindingSlotRef> {
    schema.slots().iter().find_map(|slot| {
        if pipeline_internal_cursor_slot_info(slot) {
            Some(crate::graph_row::GraphBindingSlotRef {
                kind: slot.kind,
                index: slot.index,
            })
        } else {
            None
        }
    })
}

fn pipeline_internal_cursor_slot_info(slot: &crate::graph_row::GraphBindingSlot) -> bool {
    slot.kind == crate::graph_row::GraphBindingSlotKind::Scalar
        && slot.user_alias.is_none()
        && slot.name == PIPELINE_CURSOR_KEY_SLOT
}

fn pipeline_project_items(
    stage: &GraphProjectStage,
    input_schema: &crate::graph_row::GraphBindingSchema,
) -> Result<Vec<GraphProjectItem>, EngineError> {
    match &stage.items {
        GraphProjectionItems::Star => {
            let mut items = Vec::new();
            for slot in input_schema.slots() {
                if slot.kind == crate::graph_row::GraphBindingSlotKind::HiddenOccurrence {
                    continue;
                }
                if !pipeline_slot_is_user_visible(slot) {
                    continue;
                }
                let Some(alias) = slot.user_alias.as_ref() else {
                    continue;
                };
                items.push(GraphProjectItem {
                    expr: GraphExpr::Binding(alias.clone()),
                    alias: Some(alias.clone()),
                    projection: GraphReturnProjection::Auto,
                });
            }
            if items.is_empty() {
                if pipeline_project_stage_allows_empty_star_filter(stage) {
                    return Ok(items);
                }
                return Err(EngineError::InvalidOperation(
                    "graph pipeline WITH * requires at least one visible alias".to_string(),
                ));
            }
            Ok(items)
        }
        GraphProjectionItems::Items(items) => {
            if items.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "graph pipeline Project items must not be empty".to_string(),
                ));
            }
            Ok(items.clone())
        }
    }
}

fn pipeline_project_stage_allows_empty_star_filter(stage: &GraphProjectStage) -> bool {
    stage.kind == GraphProjectKind::With
        && !stage.distinct
        && stage.where_.is_some()
        && stage.order_by.is_empty()
        && stage.skip.is_none()
        && stage.limit.is_none()
}

fn pipeline_project_output_name(item: &GraphProjectItem) -> Result<String, EngineError> {
    if let Some(alias) = item.alias.as_ref() {
        validate_graph_pipeline_user_alias(alias, "projection alias")?;
        return Ok(alias.clone());
    }
    let output_name = match &item.expr {
        GraphExpr::Binding(alias) => Ok(alias.clone()),
        GraphExpr::Property { alias, key } => Ok(format!("{alias}.{key}")),
        GraphExpr::NodeField { alias, field } => Ok(format!("{alias}.{}", graph_node_field_name(*field))),
        GraphExpr::EdgeField { alias, field } => Ok(format!("{alias}.{}", graph_edge_field_name(*field))),
        GraphExpr::PathField { alias, field } => Ok(format!("{alias}.{}", graph_path_field_name(*field))),
        _ => Err(EngineError::InvalidOperation(
            "graph pipeline complex Project expressions require an alias".to_string(),
        )),
    }?;
    validate_graph_pipeline_user_alias(&output_name, "projection alias")?;
    Ok(output_name)
}

fn add_pipeline_output_slot(
    schema: &mut crate::graph_row::GraphBindingSchema,
    alias: &str,
    kind: crate::graph_row::GraphBindingSlotKind,
    nullable: bool,
) -> Result<crate::graph_row::GraphBindingSlotRef, EngineError> {
    match kind {
        crate::graph_row::GraphBindingSlotKind::Node => schema.add_node_alias(alias.to_string(), nullable),
        crate::graph_row::GraphBindingSlotKind::Edge => schema.add_edge_alias(alias.to_string(), nullable),
        crate::graph_row::GraphBindingSlotKind::Path => schema.add_path_alias(alias.to_string(), nullable),
        crate::graph_row::GraphBindingSlotKind::Scalar => {
            schema.add_scalar_alias(alias.to_string(), nullable)
        }
        crate::graph_row::GraphBindingSlotKind::HiddenOccurrence => Err(
            EngineError::InvalidOperation(
                "graph pipeline projection cannot expose hidden occurrence slots".to_string(),
            ),
        ),
    }
}

fn pipeline_count_expr(
    expr: &GraphExpr,
    params: &BTreeMap<String, GraphParamValue>,
    context: &str,
) -> Result<usize, EngineError> {
    let resolved = resolve_graph_expr_params(expr, params)?;
    match resolved {
        GraphExpr::Int(value) if value >= 0 => usize::try_from(value).map_err(|_| {
            EngineError::InvalidOperation(format!(
                "graph pipeline {context} value does not fit usize"
            ))
        }),
        GraphExpr::UInt(value) => usize::try_from(value).map_err(|_| {
            EngineError::InvalidOperation(format!(
                "graph pipeline {context} value does not fit usize"
            ))
        }),
        GraphExpr::Int(_) => Err(EngineError::InvalidOperation(format!(
            "graph pipeline {context} value must be non-negative"
        ))),
        _ => Err(EngineError::InvalidOperation(format!(
            "graph pipeline {context} must be a non-negative integer literal or parameter"
        ))),
    }
}

fn pipeline_terminal_return_items(
    schema: &crate::graph_row::GraphBindingSchema,
    items: &[NormalizedPipelineProjectItem],
) -> Result<Vec<crate::graph_row::BoundGraphReturnItem>, EngineError> {
    let return_items = items
        .iter()
        .map(|item| GraphReturnItem {
            expr: GraphExpr::Binding(item.output_name.clone()),
            alias: Some(item.output_name.clone()),
            projection: item.projection.clone(),
        })
        .collect::<Vec<_>>();
    crate::graph_row::bind_graph_return_items(schema, &return_items)
}

fn pipeline_union_terminal_return_items(
    stage: &NormalizedPipelineUnionStage,
) -> Result<Vec<crate::graph_row::BoundGraphReturnItem>, EngineError> {
    let return_items = stage
        .columns
        .iter()
        .zip(stage.projections.iter())
        .map(|(column, projection)| GraphReturnItem {
            expr: GraphExpr::Binding(column.clone()),
            alias: Some(column.clone()),
            projection: projection.clone(),
        })
        .collect::<Vec<_>>();
    crate::graph_row::bind_graph_return_items(&stage.output_schema, &return_items)
}

fn collect_graph_pipeline_referenced_params(
    query: &GraphPipelineQuery,
) -> Result<Vec<(String, GraphParamValue)>, EngineError> {
    let mut names = BTreeSet::new();
    for stage in &query.stages {
        collect_graph_pipeline_stage_param_names(stage, &mut names);
    }
    names
        .into_iter()
        .map(|name| {
            let value = query.params.get(&name).cloned().ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph pipeline expression references missing param '{name}'"
                ))
            })?;
            Ok((name, value))
        })
        .collect()
}

fn pipeline_terminal_graph_return_items(
    items: &[NormalizedPipelineProjectItem],
) -> Vec<GraphReturnItem> {
    items
        .iter()
        .map(|item| GraphReturnItem {
            expr: GraphExpr::Binding(item.output_name.clone()),
            alias: Some(item.output_name.clone()),
            projection: item.projection.clone(),
        })
        .collect()
}

fn pipeline_union_terminal_graph_return_items(
    stage: &NormalizedPipelineUnionStage,
) -> Vec<GraphReturnItem> {
    stage
        .columns
        .iter()
        .zip(stage.projections.iter())
        .map(|(column, projection)| GraphReturnItem {
            expr: GraphExpr::Binding(column.clone()),
            alias: Some(column.clone()),
            projection: projection.clone(),
        })
        .collect()
}

fn collect_graph_pipeline_stage_param_names(stage: &GraphPipelineStage, names: &mut BTreeSet<String>) {
    match stage {
        GraphPipelineStage::Match(stage) => {
            collect_graph_piece_param_names(&stage.pieces, names);
            if let Some(expr) = stage.where_.as_ref() {
                collect_graph_expr_param_names(expr, names);
            }
            if let Some(expr) = stage.optional_candidate_where.as_ref() {
                collect_graph_expr_param_names(expr, names);
            }
        }
        GraphPipelineStage::Project(stage) => {
            match &stage.items {
                GraphProjectionItems::Star => {}
                GraphProjectionItems::Items(items) => {
                    for item in items {
                        collect_graph_expr_param_names(&item.expr, names);
                    }
                }
            }
            if let Some(expr) = stage.where_.as_ref() {
                collect_graph_expr_param_names(expr, names);
            }
            for item in &stage.order_by {
                collect_graph_expr_param_names(&item.expr, names);
            }
            if let Some(expr) = stage.skip.as_ref() {
                collect_graph_expr_param_names(expr, names);
            }
            if let Some(expr) = stage.limit.as_ref() {
                collect_graph_expr_param_names(expr, names);
            }
        }
        GraphPipelineStage::Union(union) => {
            for branch in &union.branches {
                for stage in &branch.stages {
                    collect_graph_pipeline_stage_param_names(stage, names);
                }
            }
        }
        GraphPipelineStage::Call(call) => {
            for stage in &call.query.stages {
                collect_graph_pipeline_stage_param_names(stage, names);
            }
        }
        GraphPipelineStage::ShortestPath(stage) => {
            if let GraphShortestPathEndpoint::Expr(expr) = &stage.from {
                collect_graph_expr_param_names(expr, names);
            }
            if let GraphShortestPathEndpoint::Expr(expr) = &stage.to {
                collect_graph_expr_param_names(expr, names);
            }
        }
    }
}

fn validate_graph_pipeline_referenced_params(
    referenced_params: &[(String, GraphParamValue)],
    options: &GraphPipelineOptions,
) -> Result<(), EngineError> {
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for (name, value) in referenced_params {
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

fn graph_pipeline_fingerprint_shape(
    query: &GraphPipelineQuery,
    columns: &[String],
    referenced_params: &[(String, GraphParamValue)],
    terminal_order_by: &[crate::graph_row::BoundGraphOrderItem],
) -> GraphPipelineFingerprintShape {
    let mut query_writer = GraphRowFingerprintWriter::new("pipeline_query");
    query_writer.u16(1);
    graph_pipeline_fingerprint_stages(&mut query_writer, &query.stages);

    let mut order_writer = GraphRowFingerprintWriter::new("pipeline_order");
    order_writer.len(terminal_order_by.len());
    if let Some(GraphPipelineStage::Project(project)) = query.stages.last() {
        graph_row_fingerprint_order_items(&mut order_writer, &project.order_by);
    }

    let mut output_writer = GraphRowFingerprintWriter::new("pipeline_output");
    graph_row_fingerprint_string_vec(&mut output_writer, columns);
    graph_row_fingerprint_output_options(&mut output_writer, &query.output);

    let mut params_writer = GraphRowFingerprintWriter::new("pipeline_params");
    params_writer.len(referenced_params.len());
    for (name, value) in referenced_params {
        params_writer.str(name);
        graph_row_fingerprint_param_value(&mut params_writer, value);
    }

    GraphPipelineFingerprintShape {
        query_shape: query_writer.finish(),
        order: order_writer.finish(),
        output: output_writer.finish(),
        params: params_writer.finish(),
    }
}

fn graph_pipeline_fingerprint_stages(
    writer: &mut GraphRowFingerprintWriter,
    stages: &[GraphPipelineStage],
) {
    writer.len(stages.len());
    for stage in stages {
        match stage {
            GraphPipelineStage::Match(stage) => {
                writer.tag(1);
                writer.bool(stage.optional);
                graph_row_fingerprint_node_patterns(writer, &stage.nodes);
                graph_row_fingerprint_pattern_pieces(writer, &stage.pieces);
                graph_row_fingerprint_option_expr(writer, stage.where_.as_ref());
                graph_row_fingerprint_option_expr(
                    writer,
                    stage.optional_candidate_where.as_ref(),
                );
            }
            GraphPipelineStage::Project(stage) => {
                writer.tag(2);
                writer.tag(match stage.kind {
                    GraphProjectKind::With => 1,
                    GraphProjectKind::Return => 2,
                });
                writer.bool(stage.distinct);
                graph_pipeline_fingerprint_projection_items(writer, &stage.items);
                graph_row_fingerprint_option_expr(writer, stage.where_.as_ref());
                graph_row_fingerprint_order_items(writer, &stage.order_by);
                graph_row_fingerprint_option_expr(writer, stage.skip.as_ref());
                graph_row_fingerprint_option_expr(writer, stage.limit.as_ref());
            }
            GraphPipelineStage::Union(stage) => {
                writer.tag(3);
                writer.bool(stage.all);
                writer.len(stage.branches.len());
                for branch in &stage.branches {
                    graph_pipeline_fingerprint_stages(writer, &branch.stages);
                    graph_row_fingerprint_string_vec(
                        writer,
                        &graph_pipeline_declared_branch_columns(branch),
                    );
                }
            }
            GraphPipelineStage::ShortestPath(stage) => {
                writer.tag(4);
                writer.bool(stage.optional);
                writer.str(&stage.output_path_alias);
                writer.tag(match stage.mode {
                    GraphShortestPathMode::One => 1,
                    GraphShortestPathMode::All => 2,
                });
                graph_pipeline_fingerprint_shortest_endpoint(writer, &stage.from);
                graph_pipeline_fingerprint_shortest_endpoint(writer, &stage.to);
                writer.tag(graph_pipeline_direction_tag(stage.direction));
                graph_row_fingerprint_string_vec(writer, &stage.edge_label_filter);
                writer.u64(stage.min_hops as u64);
                writer.u64(stage.max_hops as u64);
                if let Some(weight_field) = stage.weight_field.as_ref() {
                    writer.bool(true);
                    writer.str(weight_field);
                } else {
                    writer.bool(false);
                }
                if let Some(max_cost) = stage.max_cost {
                    writer.bool(true);
                    writer.u64(max_cost.to_bits());
                } else {
                    writer.bool(false);
                }
                if let Some(max_paths) = stage.max_paths {
                    writer.bool(true);
                    writer.u64(max_paths as u64);
                } else {
                    writer.bool(false);
                }
            }
            GraphPipelineStage::Call(stage) => {
                writer.tag(5);
                graph_row_fingerprint_string_vec(writer, &stage.import_aliases);
                graph_pipeline_fingerprint_stages(writer, &stage.query.stages);
                graph_row_fingerprint_string_vec(
                    writer,
                    &graph_pipeline_declared_branch_columns(&stage.query),
                );
            }
        }
    }
}

fn graph_pipeline_fingerprint_shortest_endpoint(
    writer: &mut GraphRowFingerprintWriter,
    endpoint: &GraphShortestPathEndpoint,
) {
    match endpoint {
        GraphShortestPathEndpoint::Alias(alias) => {
            writer.tag(1);
            writer.str(alias);
        }
        GraphShortestPathEndpoint::NodeId(id) => {
            writer.tag(2);
            writer.u64(*id);
        }
        GraphShortestPathEndpoint::NodeKey { label, key } => {
            writer.tag(3);
            writer.str(label);
            writer.str(key);
        }
        GraphShortestPathEndpoint::Expr(expr) => {
            writer.tag(4);
            graph_row_fingerprint_expr(writer, expr);
        }
    }
}

fn graph_pipeline_direction_tag(direction: Direction) -> u8 {
    match direction {
        Direction::Outgoing => 1,
        Direction::Incoming => 2,
        Direction::Both => 3,
    }
}

fn graph_pipeline_declared_branch_columns(query: &GraphPipelineQuery) -> Vec<String> {
    query
        .stages
        .iter()
        .rev()
        .find_map(|stage| match stage {
            GraphPipelineStage::Project(project) if project.kind == GraphProjectKind::Return => {
                match &project.items {
                    GraphProjectionItems::Star => Some(vec!["*".to_string()]),
                    GraphProjectionItems::Items(items) => Some(
                        items
                            .iter()
                            .map(|item| {
                                item.alias.clone().unwrap_or_else(|| match &item.expr {
                                    GraphExpr::Binding(alias) => alias.clone(),
                                    GraphExpr::Property { alias, key } => {
                                        format!("{alias}.{key}")
                                    }
                                    GraphExpr::NodeField { alias, field } => {
                                        format!("{alias}.{}", graph_node_field_name(*field))
                                    }
                                    GraphExpr::EdgeField { alias, field } => {
                                        format!("{alias}.{}", graph_edge_field_name(*field))
                                    }
                                    GraphExpr::PathField { alias, field } => {
                                        format!("{alias}.{}", graph_path_field_name(*field))
                                    }
                                    _ => "<expr>".to_string(),
                                })
                            })
                            .collect(),
                    ),
                }
            }
            _ => None,
        })
        .unwrap_or_default()
}

fn graph_pipeline_fingerprint_projection_items(
    writer: &mut GraphRowFingerprintWriter,
    items: &GraphProjectionItems,
) {
    match items {
        GraphProjectionItems::Star => writer.tag(1),
        GraphProjectionItems::Items(items) => {
            writer.tag(2);
            writer.len(items.len());
            for item in items {
                graph_row_fingerprint_expr(writer, &item.expr);
                match item.alias.as_ref() {
                    Some(alias) => {
                        writer.tag(1);
                        writer.str(alias);
                    }
                    None => writer.tag(0),
                }
                graph_row_fingerprint_return_projection(writer, &item.projection);
            }
        }
    }
}
