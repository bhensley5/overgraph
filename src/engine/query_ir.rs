#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeQueryCandidateSourceKind {
    ExplicitIds,
    KeyLookup,
    NodeLabelIndex,
    PropertyEqualityIndex,
    PropertyRangeIndex,
    CompoundEqualityIndex,
    CompoundRangeIndex,
    TimestampIndex,
    FallbackNodeLabelScan,
    FallbackFullNodeScan,
}

#[derive(Clone, Debug, PartialEq)]
enum NormalizedNodeFilter {
    AlwaysTrue,
    AlwaysFalse,
    IdRange {
        lower: Option<u64>,
        upper: Option<u64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    KeyEquals(String),
    KeyIn {
        values: Vec<String>,
    },
    PropertyEquals {
        key: String,
        value: PropValue,
    },
    PropertyIn {
        key: String,
        values: Vec<PropValue>,
        value_keys: Vec<Vec<u8>>,
    },
    PropertyRange {
        key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    PropertyExists {
        key: String,
    },
    PropertyMissing {
        key: String,
    },
    UpdatedAtRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    WeightRange {
        lower: Option<f32>,
        upper: Option<f32>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    CreatedAtRange {
        lower: Option<i64>,
        upper: Option<i64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    And(Vec<NormalizedNodeFilter>),
    Or(Vec<NormalizedNodeFilter>),
    Not(Box<NormalizedNodeFilter>),
}

#[derive(Clone, Debug)]
struct NormalizedNodeQuery {
    single_label_id: Option<u32>,
    label_filter: ResolvedNodeLabelFilter,
    ids: Vec<u64>,
    keys: Vec<String>,
    filter: NormalizedNodeFilter,
    allow_full_scan: bool,
    page: PageRequest,
    warnings: Vec<QueryPlanWarning>,
}

#[derive(Clone, Debug, PartialEq)]
enum NormalizedEdgeFilter {
    AlwaysTrue,
    AlwaysFalse,
    IdRange {
        lower: Option<u64>,
        upper: Option<u64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    PropertyEquals {
        key: String,
        value: PropValue,
    },
    PropertyIn {
        key: String,
        values: Vec<PropValue>,
        value_keys: Vec<Vec<u8>>,
    },
    PropertyRange {
        key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    PropertyExists {
        key: String,
    },
    PropertyMissing {
        key: String,
    },
    WeightRange {
        lower: Option<f32>,
        upper: Option<f32>,
    },
    UpdatedAtRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    CreatedAtRange {
        lower: Option<i64>,
        upper: Option<i64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    ValidAt {
        epoch_ms: i64,
    },
    ValidFromRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    ValidToRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    And(Vec<NormalizedEdgeFilter>),
    Or(Vec<NormalizedEdgeFilter>),
    Not(Box<NormalizedEdgeFilter>),
}

#[derive(Clone, Debug)]
struct NormalizedEdgeQuery {
    label_id: Option<u32>,
    ids: Vec<u64>,
    from_ids: Vec<u64>,
    to_ids: Vec<u64>,
    endpoint_ids: Vec<u64>,
    filter: NormalizedEdgeFilter,
    allow_full_scan: bool,
    page: PageRequest,
    warnings: Vec<QueryPlanWarning>,
}

#[derive(Clone, Debug)]
pub(crate) struct NormalizedGraphRowQuery {
    pub(crate) binding_schema: crate::graph_row::GraphBindingSchema,
    pub(crate) initial_bound_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    pub(crate) nodes: Vec<GraphNodePattern>,
    pub(crate) pieces: Vec<GraphPatternPiece>,
    pub(crate) fixed_paths: Vec<GraphFixedPathBinding>,
    pub(crate) edge_id_constraints: BTreeMap<String, Vec<u64>>,
    pub(crate) fingerprint_where: Option<GraphExpr>,
    pub(crate) fingerprint_order_by: Vec<GraphOrderItem>,
    pub(crate) fingerprint_return_items: Option<Vec<GraphReturnItem>>,
    pub(crate) return_items: Vec<GraphReturnItem>,
    pub(crate) bound_return_items: Vec<crate::graph_row::BoundGraphReturnItem>,
    pub(crate) columns: Vec<String>,
    pub(crate) order_by: Vec<GraphOrderItem>,
    pub(crate) bound_order_by: Vec<crate::graph_row::BoundGraphOrderItem>,
    pub(crate) bound_where: Option<crate::graph_row::BoundGraphExpr>,
    pub(crate) page: GraphPageRequest,
    pub(crate) logical_limit: Option<usize>,
    pub(crate) at_epoch: Option<i64>,
    pub(crate) output: GraphOutputOptions,
    pub(crate) options: GraphQueryOptions,
    pub(crate) projection_needs: crate::row_projection::ProjectionNeeds,
    pub(crate) referenced_params: Vec<(String, GraphParamValue)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GraphFixedPathBinding {
    pub(crate) scope: Vec<usize>,
    pub(crate) alias: String,
    pub(crate) node_aliases: Vec<String>,
    pub(crate) edge_piece_indices: Vec<usize>,
    pub(crate) after_piece_index: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphAliasScope {
    Required,
    Optional,
}

#[derive(Default)]
struct GraphRowAliasState {
    node_aliases: HashSet<String>,
    edge_aliases: HashSet<String>,
    path_aliases: HashSet<String>,
    scalar_aliases: HashSet<String>,
    external_node_aliases: HashSet<String>,
    node_first_scope: HashMap<String, GraphAliasScope>,
    edge_first_scope: HashMap<String, GraphAliasScope>,
    path_first_scope: HashMap<String, GraphAliasScope>,
    node_order: Vec<String>,
    required_edge_order: Vec<String>,
    optional_alias_order: Vec<String>,
    path_order: Vec<String>,
    scalar_order: Vec<String>,
}

#[derive(Clone, Default)]
struct GraphRowVisibleAliases {
    node_aliases: HashSet<String>,
    edge_aliases: HashSet<String>,
    path_aliases: HashSet<String>,
}

#[derive(Default)]
struct GraphRowAnchorState {
    bound_nodes: HashSet<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphExprKind {
    Scalar,
    List,
    NodeList,
    EdgeList,
    Map,
    Node,
    Edge,
    Path,
}

fn prop_value_canonical_bytes(value: &PropValue) -> Vec<u8> {
    semantic_equality_key_bytes(value)
}

pub(crate) fn normalize_graph_row_query(
    query: &GraphRowQuery,
) -> Result<NormalizedGraphRowQuery, EngineError> {
    normalize_graph_row_query_with_internal_limits(query, &BTreeMap::new(), None, &[])
}

pub(crate) fn normalize_graph_row_query_with_gql_fixed_paths(
    query: &GraphRowQuery,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    logical_limit: Option<usize>,
    fixed_paths: &[GraphFixedPathBinding],
) -> Result<NormalizedGraphRowQuery, EngineError> {
    normalize_graph_row_query_with_internal_limits(
        query,
        edge_id_constraints,
        logical_limit,
        fixed_paths,
    )
}

pub(crate) fn normalize_graph_row_query_with_pipeline_input(
    query: &GraphRowQuery,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    logical_limit: Option<usize>,
    fixed_paths: &[GraphFixedPathBinding],
    input_schema: &crate::graph_row::GraphBindingSchema,
) -> Result<NormalizedGraphRowQuery, EngineError> {
    normalize_graph_row_query_with_internal_limits_and_input(
        query,
        edge_id_constraints,
        logical_limit,
        fixed_paths,
        Some(input_schema),
    )
}

fn normalize_graph_row_query_with_internal_limits(
    query: &GraphRowQuery,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    logical_limit: Option<usize>,
    fixed_paths: &[GraphFixedPathBinding],
) -> Result<NormalizedGraphRowQuery, EngineError> {
    normalize_graph_row_query_with_internal_limits_and_input(
        query,
        edge_id_constraints,
        logical_limit,
        fixed_paths,
        None,
    )
}

fn normalize_graph_row_query_with_internal_limits_and_input(
    query: &GraphRowQuery,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    logical_limit: Option<usize>,
    fixed_paths: &[GraphFixedPathBinding],
    input_schema: Option<&crate::graph_row::GraphBindingSchema>,
) -> Result<NormalizedGraphRowQuery, EngineError> {
    validate_graph_row_page(&query.page, &query.options)?;
    let referenced_params = collect_graph_row_referenced_params(query)?;

    let mut aliases = GraphRowAliasState::default();
    if let Some(input_schema) = input_schema {
        seed_graph_row_aliases_from_input_schema(input_schema, &mut aliases)?;
    }
    collect_graph_row_node_aliases(query, &mut aliases)?;
    for piece in &query.pieces {
        collect_graph_row_piece_aliases(piece, GraphAliasScope::Required, &mut aliases)?;
    }
    validate_and_collect_graph_row_fixed_paths(fixed_paths, &query.pieces, &mut aliases)?;
    validate_graph_row_vlp_options(&query.pieces, query.options.max_path_hops)?;
    for alias in &aliases.node_order {
        aliases
            .node_first_scope
            .entry(alias.clone())
            .or_insert(GraphAliasScope::Required);
    }

    validate_graph_row_anchors(query, edge_id_constraints, &aliases.external_node_aliases)?;
    validate_graph_row_optional_filters(&query.pieces, fixed_paths, &aliases, &query.params)?;
    if let Some(expr) = query.where_.as_ref() {
        validate_graph_expr_aliases(expr, &aliases, &query.params)?;
    }
    for order in &query.order_by {
        validate_graph_order_expr(order, &aliases, &query.params)?;
    }
    validate_graph_row_required_connectivity(query, &aliases.external_node_aliases)?;

    let mut return_items = match query.return_items.as_ref() {
        Some(items) => {
            if items.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "graph row return_items must not be empty".to_string(),
                ));
            }
            items.clone()
        }
        None => expand_graph_row_return_star(&aliases)?,
    };
    for item in &mut return_items {
        item.expr = resolve_graph_expr_params(&item.expr, &query.params)?;
    }
    let resolved_pieces = resolve_graph_row_piece_params(&query.pieces, &query.params)?;
    let resolved_where = query
        .where_
        .as_ref()
        .map(|expr| resolve_graph_expr_params(expr, &query.params))
        .transpose()?;
    let resolved_order_by = query
        .order_by
        .iter()
        .map(|item| {
            Ok(GraphOrderItem {
                expr: resolve_graph_expr_params(&item.expr, &query.params)?,
                direction: item.direction,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;

    if matches!(query.output.mode, GraphOutputMode::Projected)
        && return_items
            .iter()
            .any(|item| matches!(item.projection, GraphReturnProjection::Auto))
    {
        return Err(EngineError::InvalidOperation(
            "graph row projected output mode requires selected return projections".to_string(),
        ));
    }

    let mut columns = Vec::with_capacity(return_items.len());
    for item in &return_items {
        let expr_kind = graph_expr_kind(&item.expr, &aliases, &query.params)?;
        validate_graph_return_projection(&item.projection, &query.output, expr_kind)?;
        columns.push(graph_return_column_name(item)?);
    }
    let binding_schema =
        build_graph_row_binding_schema(&aliases, &query.pieces, input_schema)?;
    let initial_bound_slots =
        graph_row_initial_bound_slots(input_schema, &binding_schema);
    let bound_return_items = crate::graph_row::bind_graph_return_items(&binding_schema, &return_items)?;
    let bound_order_by = crate::graph_row::bind_graph_order_items(&binding_schema, &resolved_order_by)?;
    let bound_where = resolved_where
        .as_ref()
        .map(|expr| crate::graph_row::bind_graph_expr(&binding_schema, expr))
        .transpose()?;
    let node_filters = query
        .nodes
        .iter()
        .map(|node| (node.alias.clone(), node.filter.clone()))
        .collect::<Vec<_>>();
    let projection_needs = crate::graph_row::collect_graph_row_projection_needs(
        &binding_schema,
        &node_filters,
        &resolved_pieces,
        resolved_where.as_ref(),
        &resolved_order_by,
        &return_items,
        &query.output,
    )?;

    Ok(NormalizedGraphRowQuery {
        binding_schema,
        initial_bound_slots,
        nodes: query.nodes.clone(),
        pieces: resolved_pieces,
        fixed_paths: fixed_paths.to_vec(),
        edge_id_constraints: edge_id_constraints.clone(),
        fingerprint_where: query.where_.clone(),
        fingerprint_order_by: query.order_by.clone(),
        fingerprint_return_items: query.return_items.clone(),
        return_items,
        bound_return_items,
        columns,
        order_by: resolved_order_by,
        bound_order_by,
        bound_where,
        page: query.page.clone(),
        logical_limit,
        at_epoch: query.at_epoch,
        output: query.output.clone(),
        options: query.options.clone(),
        projection_needs,
        referenced_params,
    })
}

fn graph_row_initial_bound_slots(
    input_schema: Option<&crate::graph_row::GraphBindingSchema>,
    binding_schema: &crate::graph_row::GraphBindingSchema,
) -> Vec<crate::graph_row::GraphBindingSlotRef> {
    let Some(input_schema) = input_schema else {
        return Vec::new();
    };
    let mut slots = input_schema
        .slots()
        .iter()
        .filter_map(|slot| {
            if let Some(alias) = slot.user_alias.as_ref() {
                binding_schema.slot_for_alias(alias)
            } else {
                graph_row_internal_scalar_slot(binding_schema, slot)
            }
        })
        .collect::<Vec<_>>();
    slots.sort_unstable();
    slots.dedup();
    slots
}

fn graph_row_internal_scalar_slot(
    schema: &crate::graph_row::GraphBindingSchema,
    source: &crate::graph_row::GraphBindingSlot,
) -> Option<crate::graph_row::GraphBindingSlotRef> {
    if source.kind != crate::graph_row::GraphBindingSlotKind::Scalar || source.user_alias.is_some() {
        return None;
    }
    schema.slots().iter().find_map(|slot| {
        if slot.kind == crate::graph_row::GraphBindingSlotKind::Scalar
            && slot.user_alias.is_none()
            && slot.name == source.name
        {
            Some(crate::graph_row::GraphBindingSlotRef {
                kind: slot.kind,
                index: slot.index,
            })
        } else {
            None
        }
    })
}

fn collect_graph_row_referenced_params(
    query: &GraphRowQuery,
) -> Result<Vec<(String, GraphParamValue)>, EngineError> {
    let mut names = BTreeSet::new();
    if let Some(expr) = query.where_.as_ref() {
        collect_graph_expr_param_names(expr, &mut names);
    }
    for item in &query.order_by {
        collect_graph_expr_param_names(&item.expr, &mut names);
    }
    if let Some(items) = query.return_items.as_ref() {
        for item in items {
            collect_graph_expr_param_names(&item.expr, &mut names);
        }
    }
    collect_graph_piece_param_names(&query.pieces, &mut names);

    names
        .into_iter()
        .map(|name| {
            let value = query.params.get(&name).cloned().ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row expression references missing param '{name}'"
                ))
            })?;
            Ok((name, value))
        })
        .collect()
}

fn collect_graph_piece_param_names(pieces: &[GraphPatternPiece], names: &mut BTreeSet<String>) {
    for piece in pieces {
        match piece {
            GraphPatternPiece::Optional(group) => {
                if let Some(expr) = group.where_.as_ref() {
                    collect_graph_expr_param_names(expr, names);
                }
                collect_graph_piece_param_names(&group.pieces, names);
            }
            GraphPatternPiece::Edge(_) | GraphPatternPiece::VariableLength(_) => {}
        }
    }
}

fn collect_graph_expr_param_names(expr: &GraphExpr, names: &mut BTreeSet<String>) {
    match expr {
        GraphExpr::Param(name) => {
            names.insert(name.clone());
        }
        GraphExpr::List(items) => {
            for item in items {
                collect_graph_expr_param_names(item, names);
            }
        }
        GraphExpr::Map(items) => {
            for item in items.values() {
                collect_graph_expr_param_names(item, names);
            }
        }
        GraphExpr::Function { args, .. } => {
            for arg in args {
                collect_graph_expr_param_names(arg, names);
            }
        }
        GraphExpr::AggregateCall { arg, .. } => {
            if let Some(arg) = arg {
                collect_graph_expr_param_names(arg, names);
            }
        }
        GraphExpr::ExistsSubquery(stage) => {
            for stage in &stage.query.stages {
                collect_graph_pipeline_stage_param_names(stage, names);
            }
        }
        GraphExpr::Unary { expr, .. } | GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            collect_graph_expr_param_names(expr, names);
        }
        GraphExpr::Binary { left, right, .. } => {
            collect_graph_expr_param_names(left, names);
            collect_graph_expr_param_names(right, names);
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_graph_expr_param_names(operand, names);
            }
            for branch in branches {
                collect_graph_expr_param_names(&branch.when, names);
                collect_graph_expr_param_names(&branch.then, names);
            }
            if let Some(else_expr) = else_expr {
                collect_graph_expr_param_names(else_expr, names);
            }
        }
        GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_)
        | GraphExpr::Binding(_)
        | GraphExpr::Property { .. }
        | GraphExpr::NodeField { .. }
        | GraphExpr::EdgeField { .. }
        | GraphExpr::PathField { .. } => {}
    }
}

fn validate_graph_row_required_connectivity(
    query: &GraphRowQuery,
    external_bound_nodes: &HashSet<String>,
) -> Result<(), EngineError> {
    let required_edges = query
        .pieces
        .iter()
        .filter_map(|piece| match piece {
            GraphPatternPiece::Edge(edge) => Some(edge),
            GraphPatternPiece::Optional(_) | GraphPatternPiece::VariableLength(_) => None,
        })
        .collect::<Vec<_>>();

    if required_edges.len() != query.pieces.len() {
        return Ok(());
    }

    if required_edges.is_empty() {
        return Ok(());
    }

    let required_nodes = query
        .nodes
        .iter()
        .map(|node| node.alias.as_str())
        .collect::<HashSet<_>>();
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &required_edges {
        if required_nodes.contains(edge.from_alias.as_str())
            && required_nodes.contains(edge.to_alias.as_str())
        {
            adjacency
                .entry(edge.from_alias.as_str())
                .or_default()
                .push(edge.to_alias.as_str());
            adjacency
                .entry(edge.to_alias.as_str())
                .or_default()
                .push(edge.from_alias.as_str());
        }
    }

    let start = required_edges[0].from_alias.as_str();
    let mut visited = HashSet::new();
    let mut stack = vec![start];
    while let Some(alias) = stack.pop() {
        if !visited.insert(alias) {
            continue;
        }
        if let Some(next) = adjacency.get(alias) {
            stack.extend(next.iter().copied());
        }
    }

    for node in &query.nodes {
        if external_bound_nodes.contains(&node.alias) {
            continue;
        }
        if !visited.contains(node.alias.as_str()) {
            return Err(EngineError::InvalidOperation(
                "graph row required fixed patterns must be connected".to_string(),
            ));
        }
    }

    Ok(())
}

fn resolve_graph_row_piece_params(
    pieces: &[GraphPatternPiece],
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<Vec<GraphPatternPiece>, EngineError> {
    pieces
        .iter()
        .map(|piece| resolve_graph_row_piece_param_refs(piece, params))
        .collect()
}

fn resolve_graph_row_piece_param_refs(
    piece: &GraphPatternPiece,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<GraphPatternPiece, EngineError> {
    Ok(match piece {
        GraphPatternPiece::Edge(edge) => GraphPatternPiece::Edge(edge.clone()),
        GraphPatternPiece::VariableLength(path) => GraphPatternPiece::VariableLength(path.clone()),
        GraphPatternPiece::Optional(group) => GraphPatternPiece::Optional(GraphOptionalGroup {
            pieces: resolve_graph_row_piece_params(&group.pieces, params)?,
            where_: group
                .where_
                .as_ref()
                .map(|expr| resolve_graph_expr_params(expr, params))
                .transpose()?,
        }),
    })
}

fn resolve_graph_expr_params(
    expr: &GraphExpr,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<GraphExpr, EngineError> {
    Ok(match expr {
        GraphExpr::Param(name) => graph_param_value_to_expr(params.get(name).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row expression references missing param '{name}'"
            ))
        })?)?,
        GraphExpr::List(items) => GraphExpr::List(
            items
                .iter()
                .map(|item| resolve_graph_expr_params(item, params))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphExpr::Map(items) => GraphExpr::Map(
            items
                .iter()
                .map(|(key, value)| Ok((key.clone(), resolve_graph_expr_params(value, params)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphExpr::Function { name, args } => GraphExpr::Function {
            name: *name,
            args: args
                .iter()
                .map(|arg| resolve_graph_expr_params(arg, params))
                .collect::<Result<Vec<_>, _>>()?,
        },
        GraphExpr::AggregateCall {
            function,
            distinct,
            arg,
        } => GraphExpr::AggregateCall {
            function: *function,
            distinct: *distinct,
            arg: arg
                .as_ref()
                .map(|arg| resolve_graph_expr_params(arg, params).map(Box::new))
                .transpose()?,
        },
        GraphExpr::ExistsSubquery(stage) => {
            let mut query = (*stage.query).clone();
            query.params = params.clone();
            GraphExpr::ExistsSubquery(GraphSubqueryStage {
                query: Box::new(query),
                import_aliases: stage.import_aliases.clone(),
            })
        }
        GraphExpr::Unary { op, expr } => GraphExpr::Unary {
            op: *op,
            expr: Box::new(resolve_graph_expr_params(expr, params)?),
        },
        GraphExpr::Binary { left, op, right } => GraphExpr::Binary {
            left: Box::new(resolve_graph_expr_params(left, params)?),
            op: *op,
            right: Box::new(resolve_graph_expr_params(right, params)?),
        },
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => GraphExpr::Case {
            operand: operand
                .as_ref()
                .map(|operand| resolve_graph_expr_params(operand, params).map(Box::new))
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(GraphCaseBranch {
                        when: resolve_graph_expr_params(&branch.when, params)?,
                        then: resolve_graph_expr_params(&branch.then, params)?,
                    })
                })
                .collect::<Result<Vec<_>, EngineError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| resolve_graph_expr_params(else_expr, params).map(Box::new))
                .transpose()?,
        },
        GraphExpr::IsNull(inner) => {
            GraphExpr::IsNull(Box::new(resolve_graph_expr_params(inner, params)?))
        }
        GraphExpr::IsNotNull(inner) => {
            GraphExpr::IsNotNull(Box::new(resolve_graph_expr_params(inner, params)?))
        }
        GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_)
        | GraphExpr::Binding(_)
        | GraphExpr::Property { .. }
        | GraphExpr::NodeField { .. }
        | GraphExpr::EdgeField { .. }
        | GraphExpr::PathField { .. } => expr.clone(),
    })
}

fn graph_param_value_to_expr(value: &GraphParamValue) -> Result<GraphExpr, EngineError> {
    Ok(match value {
        GraphParamValue::Null => GraphExpr::Null,
        GraphParamValue::Bool(value) => GraphExpr::Bool(*value),
        GraphParamValue::Int(value) => GraphExpr::Int(*value),
        GraphParamValue::UInt(value) => GraphExpr::UInt(*value),
        GraphParamValue::Float(value) => GraphExpr::Float(*value),
        GraphParamValue::String(value) => GraphExpr::String(value.clone()),
        GraphParamValue::Bytes(value) => GraphExpr::Bytes(value.clone()),
        GraphParamValue::List(values) => GraphExpr::List(
            values
                .iter()
                .map(graph_param_value_to_expr)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphParamValue::Map(values) => GraphExpr::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), graph_param_value_to_expr(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
    })
}

pub(crate) fn validate_graph_row_page(
    page: &GraphPageRequest,
    options: &GraphQueryOptions,
) -> Result<(), EngineError> {
    if page.limit == 0 {
        return Err(EngineError::InvalidOperation(
            "graph row page limit must be > 0".to_string(),
        ));
    }
    if page.limit > options.max_page_limit {
        return Err(EngineError::InvalidOperation(format!(
            "graph row page limit {} exceeds max_page_limit {}",
            page.limit, options.max_page_limit
        )));
    }
    if let Some(cursor) = page.cursor.as_ref() {
        let cursor_bytes = cursor.len();
        let transport_limit = graph_row_encoded_cursor_transport_limit(options.max_cursor_bytes);
        if cursor_bytes > transport_limit {
            return Err(EngineError::InvalidCursor {
                message: format!(
                    "encoded graph row cursor is too large to decode within max_cursor_bytes {}",
                    options.max_cursor_bytes
                ),
            });
        }
    }
    Ok(())
}

fn validate_graph_row_anchors(
    query: &GraphRowQuery,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    external_bound_nodes: &HashSet<String>,
) -> Result<(), EngineError> {
    if query.pieces.is_empty() {
        return validate_graph_row_no_piece_anchors(query, external_bound_nodes);
    }

    if query.options.allow_full_scan {
        return Ok(());
    }

    let node_by_alias: HashMap<&str, &GraphNodePattern> = query
        .nodes
        .iter()
        .map(|node| (node.alias.as_str(), node))
        .collect();
    let mut anchors = GraphRowAnchorState {
        bound_nodes: external_bound_nodes.clone(),
    };
    for node in &query.nodes {
        if graph_node_pattern_has_structural_anchor(node) {
            anchors.bound_nodes.insert(node.alias.clone());
        }
    }

    for piece in &query.pieces {
        validate_graph_row_piece_anchor(piece, &node_by_alias, edge_id_constraints, &mut anchors)?;
    }
    Ok(())
}

fn validate_graph_row_no_piece_anchors(
    query: &GraphRowQuery,
    external_bound_nodes: &HashSet<String>,
) -> Result<(), EngineError> {
    let unbound_nodes = query
        .nodes
        .iter()
        .filter(|node| !external_bound_nodes.contains(&node.alias))
        .collect::<Vec<_>>();
    if unbound_nodes.len() > 1 {
        return Err(EngineError::InvalidOperation(
            "graph row queries with multiple unconnected node aliases are out of scope"
                .to_string(),
        ));
    }
    if query.options.allow_full_scan || unbound_nodes.is_empty() {
        return Ok(());
    }
    let Some(node) = unbound_nodes.first() else {
        return Ok(());
    };
    if graph_node_pattern_has_structural_anchor(node) {
        Ok(())
    } else {
        Err(EngineError::InvalidOperation(
            "graph row query requires an anchor or allow_full_scan=true".to_string(),
        ))
    }
}

fn validate_graph_row_piece_anchor(
    piece: &GraphPatternPiece,
    node_by_alias: &HashMap<&str, &GraphNodePattern>,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    anchors: &mut GraphRowAnchorState,
) -> Result<(), EngineError> {
    match piece {
        GraphPatternPiece::Edge(edge) => {
            if !graph_edge_has_structural_anchor(edge, node_by_alias, edge_id_constraints, anchors) {
                return Err(EngineError::InvalidOperation(
                    "graph row required edge pattern requires an anchor or allow_full_scan=true"
                        .to_string(),
                ));
            }
            anchors.bound_nodes.insert(edge.from_alias.clone());
            anchors.bound_nodes.insert(edge.to_alias.clone());
        }
        GraphPatternPiece::VariableLength(path) => {
            if !graph_vlp_has_structural_anchor(path, node_by_alias, edge_id_constraints, anchors) {
                return Err(EngineError::InvalidOperation(
                    "graph row variable-length pattern requires an anchor or allow_full_scan=true"
                        .to_string(),
                ));
            }
            anchors.bound_nodes.insert(path.from_alias.clone());
            anchors.bound_nodes.insert(path.to_alias.clone());
        }
        GraphPatternPiece::Optional(group) => {
            let mut group_anchors = GraphRowAnchorState {
                bound_nodes: anchors.bound_nodes.clone(),
            };
            let had_correlation = group
                .pieces
                .iter()
                .any(|piece| graph_piece_references_bound_node(piece, &anchors.bound_nodes));
            let had_internal_anchor = group.pieces.iter().any(|piece| {
                graph_piece_has_internal_structural_anchor(
                    piece,
                    node_by_alias,
                    edge_id_constraints,
                    &anchors.bound_nodes,
                )
            });
            if !had_correlation && !had_internal_anchor {
                return Err(EngineError::InvalidOperation(
                    "graph row optional group requires correlation, an internal anchor, or allow_full_scan=true"
                        .to_string(),
                ));
            }
            for child in &group.pieces {
                validate_graph_row_piece_anchor(
                    child,
                    node_by_alias,
                    edge_id_constraints,
                    &mut group_anchors,
                )?;
            }
            anchors.bound_nodes.extend(group_anchors.bound_nodes);
        }
    }
    Ok(())
}

fn graph_node_pattern_has_structural_anchor(node: &GraphNodePattern) -> bool {
    !node.ids.is_empty()
        || !node.keys.is_empty()
        || node.filter.is_some()
        || node
            .label_filter
            .as_ref()
            .is_some_and(|filter| !filter.labels.is_empty())
}

fn graph_edge_has_structural_anchor(
    edge: &GraphEdgePattern,
    node_by_alias: &HashMap<&str, &GraphNodePattern>,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    anchors: &GraphRowAnchorState,
) -> bool {
    !edge.label_filter.is_empty()
        || edge.filter.is_some()
        || edge
            .alias
            .as_ref()
            .is_some_and(|alias| edge_id_constraints.get(alias).is_some_and(|ids| !ids.is_empty()))
        || anchors.bound_nodes.contains(&edge.from_alias)
        || anchors.bound_nodes.contains(&edge.to_alias)
        || node_by_alias
            .get(edge.from_alias.as_str())
            .is_some_and(|node| graph_node_pattern_has_structural_anchor(node))
        || node_by_alias
            .get(edge.to_alias.as_str())
            .is_some_and(|node| graph_node_pattern_has_structural_anchor(node))
}

fn graph_vlp_has_structural_anchor(
    path: &GraphVariableLengthPattern,
    node_by_alias: &HashMap<&str, &GraphNodePattern>,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    anchors: &GraphRowAnchorState,
) -> bool {
    anchors.bound_nodes.contains(&path.from_alias)
        || anchors.bound_nodes.contains(&path.to_alias)
        || path
            .edge_alias
            .as_ref()
            .is_some_and(|alias| edge_id_constraints.get(alias).is_some_and(|ids| !ids.is_empty()))
        || (path.min_hops == 1
            && path.max_hops == 1
            && (!path.label_filter.is_empty() || path.filter.is_some()))
        || node_by_alias
            .get(path.from_alias.as_str())
            .is_some_and(|node| graph_node_pattern_has_structural_anchor(node))
        || node_by_alias
            .get(path.to_alias.as_str())
            .is_some_and(|node| graph_node_pattern_has_structural_anchor(node))
}

fn graph_piece_references_bound_node(
    piece: &GraphPatternPiece,
    bound_nodes: &HashSet<String>,
) -> bool {
    match piece {
        GraphPatternPiece::Edge(edge) => {
            bound_nodes.contains(&edge.from_alias) || bound_nodes.contains(&edge.to_alias)
        }
        GraphPatternPiece::VariableLength(path) => {
            bound_nodes.contains(&path.from_alias) || bound_nodes.contains(&path.to_alias)
        }
        GraphPatternPiece::Optional(group) => group
            .pieces
            .iter()
            .any(|child| graph_piece_references_bound_node(child, bound_nodes)),
    }
}

fn graph_piece_has_internal_structural_anchor(
    piece: &GraphPatternPiece,
    node_by_alias: &HashMap<&str, &GraphNodePattern>,
    edge_id_constraints: &BTreeMap<String, Vec<u64>>,
    outer_bound_nodes: &HashSet<String>,
) -> bool {
    match piece {
        GraphPatternPiece::Edge(edge) => {
            !edge.label_filter.is_empty()
                || edge.alias.as_ref().is_some_and(|alias| {
                    edge_id_constraints.get(alias).is_some_and(|ids| !ids.is_empty())
                })
                || (!outer_bound_nodes.contains(&edge.from_alias)
                    && node_by_alias
                        .get(edge.from_alias.as_str())
                        .is_some_and(|node| graph_node_pattern_has_structural_anchor(node)))
                || (!outer_bound_nodes.contains(&edge.to_alias)
                    && node_by_alias
                        .get(edge.to_alias.as_str())
                        .is_some_and(|node| graph_node_pattern_has_structural_anchor(node)))
        }
        GraphPatternPiece::VariableLength(path) => {
            path.edge_alias.as_ref().is_some_and(|alias| {
                edge_id_constraints.get(alias).is_some_and(|ids| !ids.is_empty())
            })
                || (path.min_hops == 1
                && path.max_hops == 1
                && (!path.label_filter.is_empty() || path.filter.is_some()))
                || (!outer_bound_nodes.contains(&path.from_alias)
                && node_by_alias
                    .get(path.from_alias.as_str())
                    .is_some_and(|node| graph_node_pattern_has_structural_anchor(node)))
                || (!outer_bound_nodes.contains(&path.to_alias)
                    && node_by_alias
                        .get(path.to_alias.as_str())
                        .is_some_and(|node| graph_node_pattern_has_structural_anchor(node)))
        }
        GraphPatternPiece::Optional(group) => group.pieces.iter().any(|child| {
            graph_piece_has_internal_structural_anchor(
                child,
                node_by_alias,
                edge_id_constraints,
                outer_bound_nodes,
            )
        }),
    }
}

fn collect_graph_row_node_aliases(
    query: &GraphRowQuery,
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    let mut query_seen = HashSet::new();
    for node in &query.nodes {
        validate_graph_alias("node", &node.alias)?;
        if !query_seen.insert(node.alias.clone()) {
            return Err(EngineError::InvalidOperation(format!(
                "graph row node alias '{}' is introduced more than once",
                node.alias
            )));
        }
        if aliases.edge_aliases.contains(&node.alias)
            || aliases.path_aliases.contains(&node.alias)
            || aliases.scalar_aliases.contains(&node.alias)
        {
            return Err(EngineError::InvalidOperation(format!(
                "graph row node alias '{}' collides with an existing non-node alias",
                node.alias
            )));
        }
        if aliases.external_node_aliases.contains(&node.alias) {
            continue;
        }
        if !aliases.node_aliases.insert(node.alias.clone()) {
            return Err(EngineError::InvalidOperation(format!(
                "graph row node alias '{}' is introduced more than once",
                node.alias
            )));
        }
        aliases.node_order.push(node.alias.clone());
    }
    Ok(())
}

fn seed_graph_row_aliases_from_input_schema(
    input_schema: &crate::graph_row::GraphBindingSchema,
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    for slot in input_schema.slots() {
        let Some(alias) = slot.user_alias.as_ref() else {
            continue;
        };
        match slot.kind {
            crate::graph_row::GraphBindingSlotKind::Node => {
                validate_graph_alias("node", alias)?;
                aliases.node_aliases.insert(alias.clone());
                aliases.external_node_aliases.insert(alias.clone());
                aliases.node_first_scope.insert(
                    alias.clone(),
                    if slot.nullable {
                        GraphAliasScope::Optional
                    } else {
                        GraphAliasScope::Required
                    },
                );
                aliases.node_order.push(alias.clone());
            }
            crate::graph_row::GraphBindingSlotKind::Edge => {
                validate_graph_alias("edge", alias)?;
                aliases.edge_aliases.insert(alias.clone());
                aliases.edge_first_scope.insert(
                    alias.clone(),
                    if slot.nullable {
                        GraphAliasScope::Optional
                    } else {
                        GraphAliasScope::Required
                    },
                );
                if slot.nullable {
                    aliases.optional_alias_order.push(alias.clone());
                } else {
                    aliases.required_edge_order.push(alias.clone());
                }
            }
            crate::graph_row::GraphBindingSlotKind::Path => {
                validate_graph_alias("path", alias)?;
                aliases.path_aliases.insert(alias.clone());
                aliases.path_first_scope.insert(
                    alias.clone(),
                    if slot.nullable {
                        GraphAliasScope::Optional
                    } else {
                        GraphAliasScope::Required
                    },
                );
                aliases.path_order.push(alias.clone());
            }
            crate::graph_row::GraphBindingSlotKind::Scalar => {
                validate_graph_alias("scalar", alias)?;
                aliases.scalar_aliases.insert(alias.clone());
                aliases.scalar_order.push(alias.clone());
            }
            crate::graph_row::GraphBindingSlotKind::HiddenOccurrence => {}
        }
    }
    Ok(())
}

fn collect_graph_row_piece_aliases(
    piece: &GraphPatternPiece,
    scope: GraphAliasScope,
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    match piece {
        GraphPatternPiece::Edge(edge) => {
            collect_graph_row_node_reference(&edge.from_alias, scope, aliases)?;
            collect_graph_row_node_reference(&edge.to_alias, scope, aliases)?;
            if let Some(alias) = edge.alias.as_ref() {
                collect_graph_row_edge_alias(alias, scope, aliases)?;
            }
        }
        GraphPatternPiece::Optional(group) => {
            for child in &group.pieces {
                collect_graph_row_piece_aliases(child, GraphAliasScope::Optional, aliases)?;
            }
        }
        GraphPatternPiece::VariableLength(path) => {
            collect_graph_row_node_reference(&path.from_alias, scope, aliases)?;
            collect_graph_row_node_reference(&path.to_alias, scope, aliases)?;
            if path.min_hops > path.max_hops {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row variable-length pattern has min_hops {} greater than max_hops {}",
                    path.min_hops, path.max_hops
                )));
            }
            if let Some(alias) = path.edge_alias.as_ref() {
                if path.min_hops != 1 || path.max_hops != 1 {
                    return Err(EngineError::InvalidOperation(
                        "graph row variable-length edge_alias is only supported for 1..1 patterns"
                            .to_string(),
                    ));
                }
                collect_graph_row_edge_alias(alias, scope, aliases)?;
            }
            if let Some(alias) = path.path_alias.as_ref() {
                collect_graph_row_path_alias(alias, scope, aliases)?;
            }
        }
    }
    Ok(())
}

fn validate_graph_row_vlp_options(
    pieces: &[GraphPatternPiece],
    max_path_hops: u8,
) -> Result<(), EngineError> {
    for piece in pieces {
        match piece {
            GraphPatternPiece::Edge(_) => {}
            GraphPatternPiece::Optional(group) => {
                validate_graph_row_vlp_options(&group.pieces, max_path_hops)?;
            }
            GraphPatternPiece::VariableLength(path) => {
                if path.max_hops > max_path_hops {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph row variable-length max_hops {} exceeds max_path_hops {}",
                        path.max_hops, max_path_hops
                    )));
                }
            }
        }
    }
    Ok(())
}

fn collect_graph_row_node_reference(
    alias: &str,
    scope: GraphAliasScope,
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    if !aliases.node_aliases.contains(alias) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row pattern references unknown node alias '{alias}'"
        )));
    }
    if !aliases.node_first_scope.contains_key(alias) {
        aliases.node_first_scope.insert(alias.to_string(), scope);
        if scope == GraphAliasScope::Optional {
            aliases.optional_alias_order.push(alias.to_string());
        }
    }
    Ok(())
}

fn collect_graph_row_edge_alias(
    alias: &str,
    scope: GraphAliasScope,
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    validate_graph_alias("edge", alias)?;
    if aliases.node_aliases.contains(alias) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row edge alias '{alias}' collides with a node alias"
        )));
    }
    if aliases.scalar_aliases.contains(alias) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row edge alias '{alias}' collides with a scalar alias"
        )));
    }
    if aliases.path_aliases.contains(alias) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row edge alias '{alias}' collides with a path alias"
        )));
    }
    if !aliases.edge_aliases.insert(alias.to_string()) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row edge alias '{alias}' is introduced more than once"
        )));
    }
    aliases.edge_first_scope.insert(alias.to_string(), scope);
    match scope {
        GraphAliasScope::Required => aliases.required_edge_order.push(alias.to_string()),
        GraphAliasScope::Optional => {
            aliases.optional_alias_order.push(alias.to_string());
        }
    }
    Ok(())
}

fn collect_graph_row_path_alias(
    alias: &str,
    scope: GraphAliasScope,
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    validate_graph_alias("path", alias)?;
    if aliases.node_aliases.contains(alias)
        || aliases.edge_aliases.contains(alias)
        || aliases.scalar_aliases.contains(alias)
    {
        return Err(EngineError::InvalidOperation(format!(
            "graph row path alias '{alias}' collides with a node, edge, or scalar alias"
        )));
    }
    if !aliases.path_aliases.insert(alias.to_string()) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row path alias '{alias}' is introduced more than once"
        )));
    }
    aliases.path_first_scope.insert(alias.to_string(), scope);
    aliases.path_order.push(alias.to_string());
    Ok(())
}

fn validate_and_collect_graph_row_fixed_paths(
    fixed_paths: &[GraphFixedPathBinding],
    root_pieces: &[GraphPatternPiece],
    aliases: &mut GraphRowAliasState,
) -> Result<(), EngineError> {
    for fixed_path in fixed_paths {
        let (scope_pieces, scope) = graph_row_fixed_path_scope_pieces(root_pieces, &fixed_path.scope)?;
        if fixed_path.edge_piece_indices.is_empty() {
            return Err(EngineError::InvalidOperation(format!(
                "graph row fixed path alias '{}' requires at least one edge",
                fixed_path.alias
            )));
        }
        if fixed_path.node_aliases.len() != fixed_path.edge_piece_indices.len() + 1 {
            return Err(EngineError::InvalidOperation(format!(
                "graph row fixed path alias '{}' must have exactly one more node alias than edge reference",
                fixed_path.alias
            )));
        }
        if fixed_path.after_piece_index >= scope_pieces.len() {
            return Err(EngineError::InvalidOperation(format!(
                "graph row fixed path alias '{}' references out-of-range after_piece_index {}",
                fixed_path.alias, fixed_path.after_piece_index
            )));
        }

        for alias in &fixed_path.node_aliases {
            collect_graph_row_node_reference(alias, scope, aliases)?;
        }

        for (path_index, edge_piece_index) in fixed_path.edge_piece_indices.iter().copied().enumerate() {
            if edge_piece_index > fixed_path.after_piece_index {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row fixed path alias '{}' has edge piece {} after composition point {}",
                    fixed_path.alias, edge_piece_index, fixed_path.after_piece_index
                )));
            }
            let Some(GraphPatternPiece::Edge(edge)) = scope_pieces.get(edge_piece_index) else {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row fixed path alias '{}' references non-fixed edge piece {}",
                    fixed_path.alias, edge_piece_index
                )));
            };
            let expected_from = &fixed_path.node_aliases[path_index];
            let expected_to = &fixed_path.node_aliases[path_index + 1];
            if &edge.from_alias != expected_from || &edge.to_alias != expected_to {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row fixed path alias '{}' edge piece {} does not match source-order node aliases",
                    fixed_path.alias, edge_piece_index
                )));
            }
        }

        collect_graph_row_path_alias(&fixed_path.alias, scope, aliases)?;
    }
    Ok(())
}

fn graph_row_fixed_path_scope_pieces<'a>(
    root_pieces: &'a [GraphPatternPiece],
    scope: &[usize],
) -> Result<(&'a [GraphPatternPiece], GraphAliasScope), EngineError> {
    let mut pieces = root_pieces;
    for piece_index in scope {
        match pieces.get(*piece_index) {
            Some(GraphPatternPiece::Optional(group)) => {
                pieces = &group.pieces;
            }
            Some(_) => {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row fixed path scope references non-optional piece {piece_index}"
                )));
            }
            None => {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row fixed path scope references out-of-range piece {piece_index}"
                )));
            }
        }
    }
    let scope = if scope.is_empty() {
        GraphAliasScope::Required
    } else {
        GraphAliasScope::Optional
    };
    Ok((pieces, scope))
}

fn validate_graph_alias(kind: &str, alias: &str) -> Result<(), EngineError> {
    if alias.is_empty() {
        return Err(EngineError::InvalidOperation(format!(
            "graph row {kind} alias must be non-empty"
        )));
    }
    Ok(())
}

fn validate_graph_row_optional_filters(
    pieces: &[GraphPatternPiece],
    fixed_paths: &[GraphFixedPathBinding],
    aliases: &GraphRowAliasState,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<(), EngineError> {
    let referenced_nodes = graph_row_piece_node_references(pieces);
    let mut visible = GraphRowVisibleAliases {
        node_aliases: aliases
            .node_aliases
            .difference(&referenced_nodes)
            .cloned()
            .collect(),
        edge_aliases: HashSet::new(),
        path_aliases: HashSet::new(),
    };
    validate_graph_row_optional_filters_scoped(pieces, fixed_paths, &[], &mut visible, params)
}

fn validate_graph_row_optional_filters_scoped(
    pieces: &[GraphPatternPiece],
    fixed_paths: &[GraphFixedPathBinding],
    scope: &[usize],
    visible: &mut GraphRowVisibleAliases,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<(), EngineError> {
    for (piece_index, piece) in pieces.iter().enumerate() {
        match piece {
            GraphPatternPiece::Edge(edge) => {
                visible.node_aliases.insert(edge.from_alias.clone());
                visible.node_aliases.insert(edge.to_alias.clone());
                if let Some(alias) = edge.alias.as_ref() {
                    visible.edge_aliases.insert(alias.clone());
                }
            }
            GraphPatternPiece::VariableLength(path) => {
                visible.node_aliases.insert(path.from_alias.clone());
                visible.node_aliases.insert(path.to_alias.clone());
                if let Some(alias) = path.edge_alias.as_ref() {
                    visible.edge_aliases.insert(alias.clone());
                }
                if let Some(alias) = path.path_alias.as_ref() {
                    visible.path_aliases.insert(alias.clone());
                }
            }
            GraphPatternPiece::Optional(group) => {
                let mut group_visible = visible.clone();
                let mut group_scope = scope.to_vec();
                group_scope.push(piece_index);
                validate_graph_row_optional_filters_scoped(
                    &group.pieces,
                    fixed_paths,
                    &group_scope,
                    &mut group_visible,
                    params,
                )?;
                if let Some(expr) = group.where_.as_ref() {
                    validate_graph_expr_aliases(expr, &group_visible.to_alias_state(), params)?;
                }
                visible.node_aliases.extend(group_visible.node_aliases);
                visible.edge_aliases.extend(group_visible.edge_aliases);
                visible.path_aliases.extend(group_visible.path_aliases);
            }
        }
        for fixed_path in fixed_paths {
            if fixed_path.scope.as_slice() == scope && fixed_path.after_piece_index == piece_index {
                visible.path_aliases.insert(fixed_path.alias.clone());
            }
        }
    }
    Ok(())
}

fn graph_row_piece_node_references(pieces: &[GraphPatternPiece]) -> HashSet<String> {
    let mut references = HashSet::new();
    collect_graph_row_piece_node_references(pieces, &mut references);
    references
}

fn collect_graph_row_piece_node_references(
    pieces: &[GraphPatternPiece],
    references: &mut HashSet<String>,
) {
    for piece in pieces {
        match piece {
            GraphPatternPiece::Edge(edge) => {
                references.insert(edge.from_alias.clone());
                references.insert(edge.to_alias.clone());
            }
            GraphPatternPiece::VariableLength(path) => {
                references.insert(path.from_alias.clone());
                references.insert(path.to_alias.clone());
            }
            GraphPatternPiece::Optional(group) => {
                collect_graph_row_piece_node_references(&group.pieces, references);
            }
        }
    }
}

impl GraphRowVisibleAliases {
    fn to_alias_state(&self) -> GraphRowAliasState {
        GraphRowAliasState {
            node_aliases: self.node_aliases.clone(),
            edge_aliases: self.edge_aliases.clone(),
            path_aliases: self.path_aliases.clone(),
            scalar_aliases: HashSet::new(),
            external_node_aliases: HashSet::new(),
            node_first_scope: HashMap::new(),
            edge_first_scope: HashMap::new(),
            path_first_scope: HashMap::new(),
            node_order: Vec::new(),
            required_edge_order: Vec::new(),
            optional_alias_order: Vec::new(),
            path_order: Vec::new(),
            scalar_order: Vec::new(),
        }
    }
}

fn validate_graph_order_expr(
    order: &GraphOrderItem,
    aliases: &GraphRowAliasState,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<(), EngineError> {
    let kind = graph_expr_kind(&order.expr, aliases, params)?;
    if graph_expr_kind_is_list_or_map(kind) {
        return Err(EngineError::InvalidOperation(
            "graph row order expression must not be a list or map value".to_string(),
        ));
    }
    Ok(())
}

fn validate_graph_expr_aliases(
    expr: &GraphExpr,
    aliases: &GraphRowAliasState,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<(), EngineError> {
    graph_expr_kind(expr, aliases, params).map(|_| ())
}

fn graph_expr_kind(
    expr: &GraphExpr,
    aliases: &GraphRowAliasState,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<GraphExprKind, EngineError> {
    match expr {
        GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_) => Ok(GraphExprKind::Scalar),
        GraphExpr::List(items) => graph_list_expr_kind(
            items
                .iter()
                .map(|item| graph_expr_kind(item, aliases, params))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphExpr::Map(map) => {
            for item in map.values() {
                graph_expr_kind(item, aliases, params)?;
            }
            Ok(GraphExprKind::Map)
        }
        GraphExpr::Param(name) => {
            graph_param_expr_kind(params.get(name).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row expression references missing param '{name}'"
                ))
            })?)
        }
        GraphExpr::Binding(alias) => graph_binding_alias_kind(alias, aliases),
        GraphExpr::Property { alias, .. } => {
            if aliases.node_aliases.contains(alias) || aliases.edge_aliases.contains(alias) {
                Ok(GraphExprKind::Scalar)
            } else if aliases.path_aliases.contains(alias) {
                Err(EngineError::InvalidOperation(format!(
                    "graph row property expression cannot reference path alias '{alias}'"
                )))
            } else if aliases.scalar_aliases.contains(alias) {
                Err(EngineError::InvalidOperation(format!(
                    "graph row property expression requires a node or edge alias, got scalar alias '{alias}'"
                )))
            } else {
                Err(unknown_graph_alias(alias))
            }
        }
        GraphExpr::NodeField { alias, field } => {
            if aliases.node_aliases.contains(alias) {
                Ok(match field {
                    GraphNodeField::Labels => GraphExprKind::List,
                    GraphNodeField::Id
                    | GraphNodeField::Key
                    | GraphNodeField::Weight
                    | GraphNodeField::CreatedAt
                    | GraphNodeField::UpdatedAt => GraphExprKind::Scalar,
                })
            } else if aliases.edge_aliases.contains(alias)
                || aliases.path_aliases.contains(alias)
                || aliases.scalar_aliases.contains(alias)
            {
                Err(EngineError::InvalidOperation(format!(
                    "graph row node field references non-node alias '{alias}'"
                )))
            } else {
                Err(unknown_graph_alias(alias))
            }
        }
        GraphExpr::EdgeField { alias, .. } => {
            if aliases.edge_aliases.contains(alias) {
                Ok(GraphExprKind::Scalar)
            } else if aliases.node_aliases.contains(alias)
                || aliases.path_aliases.contains(alias)
                || aliases.scalar_aliases.contains(alias)
            {
                Err(EngineError::InvalidOperation(format!(
                    "graph row edge field references non-edge alias '{alias}'"
                )))
            } else {
                Err(unknown_graph_alias(alias))
            }
        }
        GraphExpr::PathField { alias, field } => {
            if aliases.path_aliases.contains(alias) {
                Ok(match field {
                    GraphPathField::NodeIds | GraphPathField::EdgeIds => GraphExprKind::List,
                    GraphPathField::Length => GraphExprKind::Scalar,
                })
            } else if aliases.node_aliases.contains(alias)
                || aliases.edge_aliases.contains(alias)
                || aliases.scalar_aliases.contains(alias)
            {
                Err(EngineError::InvalidOperation(format!(
                    "graph row path field references non-path alias '{alias}'"
                )))
            } else {
                Err(unknown_graph_alias(alias))
            }
        }
        GraphExpr::Function { name, args } => graph_function_expr_kind(*name, args, aliases, params),
        GraphExpr::AggregateCall { .. } => Err(EngineError::InvalidOperation(
            "aggregate expressions require graph pipeline projection execution".to_string(),
        )),
        GraphExpr::ExistsSubquery(_) => Err(EngineError::InvalidOperation(
            "EXISTS subqueries require graph pipeline predicate execution".to_string(),
        )),
        GraphExpr::Unary { op, expr } => {
            let kind = graph_expr_kind(expr, aliases, params)?;
            graph_require_scalar_operand(graph_unary_operator_name(*op), kind)?;
            Ok(GraphExprKind::Scalar)
        }
        GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            graph_expr_kind(expr, aliases, params)?;
            Ok(GraphExprKind::Scalar)
        }
        GraphExpr::Binary { left, op, right } => {
            let left_kind = graph_expr_kind(left, aliases, params)?;
            let right_kind = graph_expr_kind(right, aliases, params)?;
            validate_graph_binary_operand_kinds(*op, left_kind, right_kind)?;
            Ok(GraphExprKind::Scalar)
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                graph_expr_kind(operand, aliases, params)?;
            }
            let mut result_kind = None;
            for branch in branches {
                let when_kind = graph_expr_kind(&branch.when, aliases, params)?;
                if operand.is_none() {
                    graph_require_scalar_operand("CASE WHEN", when_kind)?;
                }
                result_kind = Some(graph_merge_case_result_kind(
                    result_kind,
                    graph_expr_kind(&branch.then, aliases, params)?,
                ));
            }
            if let Some(else_expr) = else_expr {
                result_kind = Some(graph_merge_case_result_kind(
                    result_kind,
                    graph_expr_kind(else_expr, aliases, params)?,
                ));
            } else {
                result_kind = Some(graph_merge_case_result_kind(
                    result_kind,
                    GraphExprKind::Scalar,
                ));
            }
            Ok(result_kind.unwrap_or(GraphExprKind::Scalar))
        }
    }
}

fn graph_param_expr_kind(value: &GraphParamValue) -> Result<GraphExprKind, EngineError> {
    Ok(match value {
        GraphParamValue::List(_) => GraphExprKind::List,
        GraphParamValue::Map(_) => GraphExprKind::Map,
        GraphParamValue::Null
        | GraphParamValue::Bool(_)
        | GraphParamValue::Int(_)
        | GraphParamValue::UInt(_)
        | GraphParamValue::Float(_)
        | GraphParamValue::String(_)
        | GraphParamValue::Bytes(_) => GraphExprKind::Scalar,
    })
}

fn graph_function_expr_kind(
    name: GraphFunction,
    args: &[GraphExpr],
    aliases: &GraphRowAliasState,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<GraphExprKind, EngineError> {
    if is_scalar_graph_function(name) {
        validate_graph_scalar_function_arity(name, args.len())?;
        for arg in args {
            let arg_kind = graph_expr_kind(arg, aliases, params)?;
            let invalid_arg = match name {
                GraphFunction::Size => matches!(
                    arg_kind,
                    GraphExprKind::Node | GraphExprKind::Edge | GraphExprKind::Path
                ),
                _ => matches!(
                    arg_kind,
                    GraphExprKind::Node
                        | GraphExprKind::Edge
                        | GraphExprKind::Path
                        | GraphExprKind::NodeList
                        | GraphExprKind::EdgeList
                ),
            };
            if invalid_arg {
                return Err(graph_function_kind_error(
                    name,
                    "scalar, list, map, or null input",
                    arg_kind,
                ));
            }
        }
        return Ok(GraphExprKind::Scalar);
    }
    if args.len() != 1 {
        return Err(EngineError::InvalidOperation(format!(
            "graph row function {} expects exactly one argument",
            graph_function_name(name)
        )));
    }
    let arg_kind = graph_expr_kind(&args[0], aliases, params)?;
    match name {
        GraphFunction::Id if matches!(arg_kind, GraphExprKind::Node | GraphExprKind::Edge) => {
            Ok(GraphExprKind::Scalar)
        }
        GraphFunction::Labels if arg_kind == GraphExprKind::Node => Ok(GraphExprKind::List),
        GraphFunction::Type if arg_kind == GraphExprKind::Edge => Ok(GraphExprKind::Scalar),
        GraphFunction::Length if arg_kind == GraphExprKind::Path => Ok(GraphExprKind::Scalar),
        GraphFunction::StartNode | GraphFunction::EndNode if arg_kind == GraphExprKind::Path => {
            Ok(GraphExprKind::Node)
        }
        GraphFunction::Nodes if arg_kind == GraphExprKind::Path => Ok(GraphExprKind::NodeList),
        GraphFunction::Relationships if arg_kind == GraphExprKind::Path => {
            Ok(GraphExprKind::EdgeList)
        }
        GraphFunction::Id => Err(graph_function_kind_error(name, "a node or edge", arg_kind)),
        GraphFunction::Labels => Err(graph_function_kind_error(name, "a node", arg_kind)),
        GraphFunction::Type => Err(graph_function_kind_error(name, "an edge", arg_kind)),
        GraphFunction::Length
        | GraphFunction::StartNode
        | GraphFunction::EndNode
        | GraphFunction::Nodes
        | GraphFunction::Relationships => {
            Err(graph_function_kind_error(name, "a path", arg_kind))
        }
        _ => Err(EngineError::InvalidOperation(format!(
            "graph row function {} is not supported in graph-row single-block expressions",
            graph_function_name(name)
        ))),
    }
}

fn validate_graph_scalar_function_arity(
    name: GraphFunction,
    arg_count: usize,
) -> Result<(), EngineError> {
    let valid = match name {
        GraphFunction::Coalesce => arg_count >= 1,
        GraphFunction::Substring => matches!(arg_count, 2 | 3),
        GraphFunction::ToString
        | GraphFunction::ToInteger
        | GraphFunction::ToFloat
        | GraphFunction::Abs
        | GraphFunction::Floor
        | GraphFunction::Ceil
        | GraphFunction::Round
        | GraphFunction::Lower
        | GraphFunction::Upper
        | GraphFunction::Trim
        | GraphFunction::Size
        | GraphFunction::Head
        | GraphFunction::Last => arg_count == 1,
        _ => false,
    };
    if valid {
        return Ok(());
    }
    let expected = match name {
        GraphFunction::Coalesce => "at least one argument",
        GraphFunction::Substring => "two or three arguments",
        _ => "exactly one argument",
    };
    Err(EngineError::InvalidOperation(format!(
        "graph row function {} expects {expected}",
        graph_function_name(name)
    )))
}

fn is_scalar_graph_function(name: GraphFunction) -> bool {
    matches!(
        name,
        GraphFunction::Coalesce
            | GraphFunction::ToString
            | GraphFunction::ToInteger
            | GraphFunction::ToFloat
            | GraphFunction::Abs
            | GraphFunction::Floor
            | GraphFunction::Ceil
            | GraphFunction::Round
            | GraphFunction::Lower
            | GraphFunction::Upper
            | GraphFunction::Trim
            | GraphFunction::Substring
            | GraphFunction::Size
            | GraphFunction::Head
            | GraphFunction::Last
    )
}

fn graph_list_expr_kind(item_kinds: Vec<GraphExprKind>) -> Result<GraphExprKind, EngineError> {
    if item_kinds.is_empty() {
        return Ok(GraphExprKind::List);
    }
    if item_kinds
        .iter()
        .all(|kind| *kind == GraphExprKind::Node)
    {
        return Ok(GraphExprKind::NodeList);
    }
    if item_kinds
        .iter()
        .all(|kind| *kind == GraphExprKind::Edge)
    {
        return Ok(GraphExprKind::EdgeList);
    }
    Ok(GraphExprKind::List)
}

fn graph_expr_kind_is_list_or_map(kind: GraphExprKind) -> bool {
    matches!(
        kind,
        GraphExprKind::List | GraphExprKind::NodeList | GraphExprKind::EdgeList | GraphExprKind::Map
    )
}

fn validate_graph_binary_operand_kinds(
    op: GraphBinaryOp,
    left: GraphExprKind,
    right: GraphExprKind,
) -> Result<(), EngineError> {
    match op {
        GraphBinaryOp::And
        | GraphBinaryOp::Or
        | GraphBinaryOp::Lt
        | GraphBinaryOp::Le
        | GraphBinaryOp::Gt
        | GraphBinaryOp::Ge
        | GraphBinaryOp::Add
        | GraphBinaryOp::Sub
        | GraphBinaryOp::Mul
        | GraphBinaryOp::Div
        | GraphBinaryOp::StartsWith
        | GraphBinaryOp::EndsWith
        | GraphBinaryOp::Contains => {
            let operator = graph_binary_operator_name(op);
            graph_require_scalar_operand(operator, left)?;
            graph_require_scalar_operand(operator, right)
        }
        GraphBinaryOp::Eq | GraphBinaryOp::Neq | GraphBinaryOp::In => Ok(()),
    }
}

fn graph_require_scalar_operand(operator: &str, actual: GraphExprKind) -> Result<(), EngineError> {
    if actual == GraphExprKind::Scalar {
        return Ok(());
    }
    Err(EngineError::InvalidOperation(format!(
        "graph row operator {operator} expects scalar operands, got {}",
        graph_expr_kind_name(actual)
    )))
}

fn graph_merge_case_result_kind(
    current: Option<GraphExprKind>,
    next: GraphExprKind,
) -> GraphExprKind {
    let Some(current) = current else {
        return next;
    };
    if current == next {
        return current;
    }
    if graph_expr_kind_is_list_or_map(current) {
        return current;
    }
    if graph_expr_kind_is_list_or_map(next) {
        return next;
    }
    if current != GraphExprKind::Scalar {
        return current;
    }
    next
}

fn graph_unary_operator_name(op: GraphUnaryOp) -> &'static str {
    match op {
        GraphUnaryOp::Not => "NOT",
        GraphUnaryOp::Neg => "-",
    }
}

fn graph_binary_operator_name(op: GraphBinaryOp) -> &'static str {
    match op {
        GraphBinaryOp::Or => "OR",
        GraphBinaryOp::And => "AND",
        GraphBinaryOp::Eq => "=",
        GraphBinaryOp::Neq => "<>",
        GraphBinaryOp::Lt => "<",
        GraphBinaryOp::Le => "<=",
        GraphBinaryOp::Gt => ">",
        GraphBinaryOp::Ge => ">=",
        GraphBinaryOp::In => "IN",
        GraphBinaryOp::Add => "+",
        GraphBinaryOp::Sub => "-",
        GraphBinaryOp::Mul => "*",
        GraphBinaryOp::Div => "/",
        GraphBinaryOp::StartsWith => "STARTS WITH",
        GraphBinaryOp::EndsWith => "ENDS WITH",
        GraphBinaryOp::Contains => "CONTAINS",
    }
}

fn graph_function_kind_error(
    name: GraphFunction,
    expected: &str,
    actual: GraphExprKind,
) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row function {} expects {}, got {}",
        graph_function_name(name),
        expected,
        graph_expr_kind_name(actual)
    ))
}

fn graph_function_name(name: GraphFunction) -> &'static str {
    match name {
        GraphFunction::Id => "id",
        GraphFunction::Labels => "labels",
        GraphFunction::Type => "type",
        GraphFunction::Length => "length",
        GraphFunction::StartNode => "start_node",
        GraphFunction::EndNode => "end_node",
        GraphFunction::Nodes => "nodes",
        GraphFunction::Relationships => "relationships",
        GraphFunction::Coalesce => "coalesce",
        GraphFunction::ToString => "to_string",
        GraphFunction::ToInteger => "to_integer",
        GraphFunction::ToFloat => "to_float",
        GraphFunction::Abs => "abs",
        GraphFunction::Floor => "floor",
        GraphFunction::Ceil => "ceil",
        GraphFunction::Round => "round",
        GraphFunction::Lower => "lower",
        GraphFunction::Upper => "upper",
        GraphFunction::Trim => "trim",
        GraphFunction::Substring => "substring",
        GraphFunction::Size => "size",
        GraphFunction::Head => "head",
        GraphFunction::Last => "last",
    }
}

fn graph_expr_kind_name(kind: GraphExprKind) -> &'static str {
    match kind {
        GraphExprKind::Scalar => "a scalar",
        GraphExprKind::List => "a list",
        GraphExprKind::NodeList => "a node list",
        GraphExprKind::EdgeList => "an edge list",
        GraphExprKind::Map => "a map",
        GraphExprKind::Node => "a node",
        GraphExprKind::Edge => "an edge",
        GraphExprKind::Path => "a path",
    }
}

fn graph_binding_alias_kind(
    alias: &str,
    aliases: &GraphRowAliasState,
) -> Result<GraphExprKind, EngineError> {
    if aliases.node_aliases.contains(alias) {
        Ok(GraphExprKind::Node)
    } else if aliases.edge_aliases.contains(alias) {
        Ok(GraphExprKind::Edge)
    } else if aliases.path_aliases.contains(alias) {
        Ok(GraphExprKind::Path)
    } else if aliases.scalar_aliases.contains(alias) {
        Ok(GraphExprKind::Scalar)
    } else {
        Err(unknown_graph_alias(alias))
    }
}

fn unknown_graph_alias(alias: &str) -> EngineError {
    EngineError::InvalidOperation(format!("graph row expression references unknown alias '{alias}'"))
}

fn expand_graph_row_return_star(
    aliases: &GraphRowAliasState,
) -> Result<Vec<GraphReturnItem>, EngineError> {
    let mut items = Vec::new();
    for alias in &aliases.node_order {
        if aliases.node_first_scope.get(alias) != Some(&GraphAliasScope::Optional) {
            items.push(graph_return_binding(alias));
        }
    }
    for alias in &aliases.required_edge_order {
        items.push(graph_return_binding(alias));
    }
    for alias in &aliases.path_order {
        items.push(graph_return_binding(alias));
    }
    for alias in &aliases.optional_alias_order {
        items.push(graph_return_binding(alias));
    }
    for alias in &aliases.scalar_order {
        items.push(graph_return_binding(alias));
    }
    if items.is_empty() {
        return Err(EngineError::InvalidOperation(
            "graph row RETURN * requires at least one user-visible alias".to_string(),
        ));
    }
    Ok(items)
}

fn build_graph_row_binding_schema(
    aliases: &GraphRowAliasState,
    pieces: &[GraphPatternPiece],
    input_schema: Option<&crate::graph_row::GraphBindingSchema>,
) -> Result<crate::graph_row::GraphBindingSchema, EngineError> {
    let mut schema = crate::graph_row::GraphBindingSchema::new();
    if let Some(input_schema) = input_schema {
        for slot in input_schema.slots() {
            match (slot.user_alias.as_ref(), slot.kind) {
                (Some(alias), crate::graph_row::GraphBindingSlotKind::Node) => {
                    schema.add_node_alias(alias.clone(), slot.nullable)?;
                }
                (Some(alias), crate::graph_row::GraphBindingSlotKind::Edge) => {
                    schema.add_edge_alias(alias.clone(), slot.nullable)?;
                }
                (Some(alias), crate::graph_row::GraphBindingSlotKind::Path) => {
                    schema.add_path_alias(alias.clone(), slot.nullable)?;
                }
                (Some(alias), crate::graph_row::GraphBindingSlotKind::Scalar) => {
                    schema.add_scalar_alias(alias.clone(), slot.nullable)?;
                }
                (None, crate::graph_row::GraphBindingSlotKind::Scalar) => {
                    schema.add_internal_scalar(slot.name.clone(), slot.nullable)?;
                }
                (_, crate::graph_row::GraphBindingSlotKind::HiddenOccurrence) | (None, _) => {}
            }
        }
    }
    for alias in &aliases.node_order {
        if schema.slot_for_alias(alias).is_some() {
            continue;
        }
        let nullable = aliases.node_first_scope.get(alias) == Some(&GraphAliasScope::Optional);
        schema.add_node_alias(alias.clone(), nullable)?;
    }
    for alias in &aliases.required_edge_order {
        if schema.slot_for_alias(alias).is_some() {
            continue;
        }
        schema.add_edge_alias(alias.clone(), false)?;
    }
    for alias in &aliases.optional_alias_order {
        if schema.slot_for_alias(alias).is_some() {
            continue;
        }
        if aliases.edge_aliases.contains(alias) {
            schema.add_edge_alias(alias.clone(), true)?;
        }
    }
    for alias in &aliases.path_order {
        if schema.slot_for_alias(alias).is_some() {
            continue;
        }
        let nullable = aliases.path_first_scope.get(alias) == Some(&GraphAliasScope::Optional);
        schema.add_path_alias(alias.clone(), nullable)?;
    }
    for alias in &aliases.scalar_order {
        if schema.slot_for_alias(alias).is_none() {
            schema.add_scalar_alias(alias.clone(), true)?;
        }
    }
    add_hidden_occurrence_slots(pieces, &mut schema, &mut 0, false)?;
    Ok(schema)
}

fn add_hidden_occurrence_slots(
    pieces: &[GraphPatternPiece],
    schema: &mut crate::graph_row::GraphBindingSchema,
    next_id: &mut usize,
    in_optional: bool,
) -> Result<(), EngineError> {
    for piece in pieces {
        match piece {
            GraphPatternPiece::Edge(edge) => {
                if edge.alias.is_none() {
                    let label = format!("__hidden_edge_occurrence_{next_id}");
                    *next_id += 1;
                    schema.add_hidden_occurrence_with_nullability(label, in_optional)?;
                }
            }
            GraphPatternPiece::VariableLength(path) => {
                if path.edge_alias.is_none() && path.path_alias.is_none() {
                    let label = format!("__hidden_path_occurrence_{next_id}");
                    *next_id += 1;
                    schema.add_hidden_occurrence_with_nullability(label, in_optional)?;
                }
            }
            GraphPatternPiece::Optional(group) => {
                add_hidden_occurrence_slots(&group.pieces, schema, next_id, true)?;
            }
        }
    }
    Ok(())
}

fn graph_return_binding(alias: &str) -> GraphReturnItem {
    GraphReturnItem {
        expr: GraphExpr::Binding(alias.to_string()),
        alias: Some(alias.to_string()),
        projection: GraphReturnProjection::Auto,
    }
}

fn validate_graph_return_projection(
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    expr_kind: GraphExprKind,
) -> Result<(), EngineError> {
    if !output.include_vectors && graph_projection_requests_vectors(projection) {
        return Err(EngineError::InvalidOperation(
            "graph row selected vector projection requires include_vectors=true".to_string(),
        ));
    }
    match projection {
        GraphReturnProjection::Auto | GraphReturnProjection::IdOnly => Ok(()),
        GraphReturnProjection::Element(_) => {
            if matches!(
                expr_kind,
                GraphExprKind::Node
                    | GraphExprKind::Edge
                    | GraphExprKind::Path
                    | GraphExprKind::NodeList
                    | GraphExprKind::EdgeList
            ) {
                Ok(())
            } else {
                Err(graph_projection_kind_error(
                    "element projection",
                    "a node, edge, or path",
                    expr_kind,
                ))
            }
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Node(_)) => {
            if matches!(expr_kind, GraphExprKind::Node | GraphExprKind::NodeList) {
                Ok(())
            } else {
                Err(graph_projection_kind_error(
                    "selected node projection",
                    "a node or node list",
                    expr_kind,
                ))
            }
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Edge(_)) => {
            if matches!(expr_kind, GraphExprKind::Edge | GraphExprKind::EdgeList) {
                Ok(())
            } else {
                Err(graph_projection_kind_error(
                    "selected edge projection",
                    "an edge or edge list",
                    expr_kind,
                ))
            }
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Path(_)) => {
            if expr_kind == GraphExprKind::Path {
                Ok(())
            } else {
                Err(graph_projection_kind_error(
                    "selected path projection",
                    "a path",
                    expr_kind,
                ))
            }
        }
    }
}

fn graph_projection_kind_error(
    projection: &str,
    expected: &str,
    actual: GraphExprKind,
) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row {projection} expects {expected}, got {}",
        graph_expr_kind_name(actual)
    ))
}

fn graph_projection_requests_vectors(projection: &GraphReturnProjection) -> bool {
    match projection {
        GraphReturnProjection::Auto
        | GraphReturnProjection::IdOnly
        | GraphReturnProjection::Element(_) => false,
        GraphReturnProjection::Selected(GraphSelectedProjection::Node(node)) => {
            node.vectors != GraphVectorSelection::None
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Edge(_)) => false,
        GraphReturnProjection::Selected(GraphSelectedProjection::Path(path)) => {
            path.nodes
                .as_ref()
                .is_some_and(|node| node.vectors != GraphVectorSelection::None)
        }
    }
}

fn graph_return_column_name(item: &GraphReturnItem) -> Result<String, EngineError> {
    if let Some(alias) = item.alias.as_ref() {
        validate_graph_alias("return", alias)?;
        return Ok(alias.clone());
    }
    match &item.expr {
        GraphExpr::Binding(alias) => Ok(alias.clone()),
        GraphExpr::Property { alias, key } => Ok(format!("{alias}.{key}")),
        GraphExpr::NodeField { alias, field } => Ok(format!("{alias}.{}", graph_node_field_name(*field))),
        GraphExpr::EdgeField { alias, field } => Ok(format!("{alias}.{}", graph_edge_field_name(*field))),
        GraphExpr::PathField { alias, field } => Ok(format!("{alias}.{}", graph_path_field_name(*field))),
        _ => Err(EngineError::InvalidOperation(
            "graph row complex return expressions require an alias".to_string(),
        )),
    }
}

fn graph_node_field_name(field: GraphNodeField) -> &'static str {
    match field {
        GraphNodeField::Id => "id",
        GraphNodeField::Labels => "labels",
        GraphNodeField::Key => "key",
        GraphNodeField::Weight => "weight",
        GraphNodeField::CreatedAt => "created_at",
        GraphNodeField::UpdatedAt => "updated_at",
    }
}

fn graph_edge_field_name(field: GraphEdgeField) -> &'static str {
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

fn graph_path_field_name(field: GraphPathField) -> &'static str {
    match field {
        GraphPathField::NodeIds => "node_ids",
        GraphPathField::EdgeIds => "edge_ids",
        GraphPathField::Length => "length",
    }
}

fn prop_values_equal_for_filter(left: &PropValue, right: &PropValue) -> bool {
    semantic_property_eq(left, right)
}

fn push_len_prefixed_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    target.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    target.extend_from_slice(bytes);
}

fn push_range_bound_structural_key(target: &mut Vec<u8>, bound: Option<&PropertyRangeBound>) {
    match bound {
        Some(bound) => {
            target.push(1);
            push_len_prefixed_bytes(target, &semantic_range_bound_key_bytes(bound));
        }
        None => target.push(0),
    }
}

impl NormalizedNodeFilter {
    fn is_always_true(&self) -> bool {
        matches!(self, Self::AlwaysTrue)
    }

    fn is_always_false(&self) -> bool {
        matches!(self, Self::AlwaysFalse)
    }

    fn structural_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::AlwaysTrue => key.push(0),
            Self::AlwaysFalse => key.push(1),
            Self::IdRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            } => {
                key.push(11);
                key.push(lower.is_some() as u8);
                key.extend_from_slice(&lower.unwrap_or(0).to_be_bytes());
                key.push(upper.is_some() as u8);
                key.extend_from_slice(&upper.unwrap_or(0).to_be_bytes());
                key.push(*lower_inclusive as u8);
                key.push(*upper_inclusive as u8);
            }
            Self::KeyEquals(value) => {
                key.push(12);
                push_len_prefixed_bytes(&mut key, value.as_bytes());
            }
            Self::KeyIn { values } => {
                key.push(13);
                for value in values {
                    push_len_prefixed_bytes(&mut key, value.as_bytes());
                }
            }
            Self::PropertyEquals { key: prop_key, value } => {
                key.push(2);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                push_len_prefixed_bytes(&mut key, &prop_value_canonical_bytes(value));
            }
            Self::PropertyIn {
                key: prop_key,
                value_keys,
                ..
            } => {
                key.push(3);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                for value_key in value_keys {
                    push_len_prefixed_bytes(&mut key, value_key);
                }
            }
            Self::PropertyRange {
                key: prop_key,
                lower,
                upper,
            } => {
                key.push(4);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                push_range_bound_structural_key(&mut key, lower.as_ref());
                push_range_bound_structural_key(&mut key, upper.as_ref());
            }
            Self::PropertyExists { key: prop_key } => {
                key.push(5);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
            }
            Self::PropertyMissing { key: prop_key } => {
                key.push(6);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
            }
            Self::UpdatedAtRange { lower_ms, upper_ms } => {
                key.push(7);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::WeightRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            } => {
                key.push(14);
                key.push(lower.is_some() as u8);
                key.extend_from_slice(&lower.map(f32::to_bits).unwrap_or(0).to_be_bytes());
                key.push(upper.is_some() as u8);
                key.extend_from_slice(&upper.map(f32::to_bits).unwrap_or(0).to_be_bytes());
                key.push(*lower_inclusive as u8);
                key.push(*upper_inclusive as u8);
            }
            Self::CreatedAtRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            } => {
                key.push(15);
                key.push(lower.is_some() as u8);
                key.extend_from_slice(&lower.unwrap_or(0).to_be_bytes());
                key.push(upper.is_some() as u8);
                key.extend_from_slice(&upper.unwrap_or(0).to_be_bytes());
                key.push(*lower_inclusive as u8);
                key.push(*upper_inclusive as u8);
            }
            Self::And(children) => {
                key.push(8);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Or(children) => {
                key.push(9);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Not(child) => {
                key.push(10);
                push_len_prefixed_bytes(&mut key, &child.structural_key());
            }
        }
        key
    }
}

impl NormalizedEdgeFilter {
    fn is_always_true(&self) -> bool {
        matches!(self, Self::AlwaysTrue)
    }

    fn is_always_false(&self) -> bool {
        matches!(self, Self::AlwaysFalse)
    }

    fn has_metadata_anchor(&self) -> bool {
        match self {
            Self::IdRange { .. }
            | Self::WeightRange { .. }
            | Self::UpdatedAtRange { .. }
            | Self::CreatedAtRange { .. }
            | Self::ValidAt { .. }
            | Self::ValidFromRange { .. }
            | Self::ValidToRange { .. } => true,
            Self::And(children) => children.iter().any(Self::has_metadata_anchor),
            Self::Or(children) => {
                !children.is_empty() && children.iter().all(Self::has_metadata_anchor)
            }
            Self::Not(_) => false,
            Self::AlwaysTrue
            | Self::AlwaysFalse
            | Self::PropertyEquals { .. }
            | Self::PropertyIn { .. }
            | Self::PropertyRange { .. }
            | Self::PropertyExists { .. }
            | Self::PropertyMissing { .. } => false,
        }
    }

    fn structural_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::AlwaysTrue => key.push(0),
            Self::AlwaysFalse => key.push(1),
            Self::IdRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            } => {
                key.push(15);
                key.push(lower.is_some() as u8);
                key.extend_from_slice(&lower.unwrap_or(0).to_be_bytes());
                key.push(upper.is_some() as u8);
                key.extend_from_slice(&upper.unwrap_or(0).to_be_bytes());
                key.push(*lower_inclusive as u8);
                key.push(*upper_inclusive as u8);
            }
            Self::PropertyEquals { key: prop_key, value } => {
                key.push(2);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                push_len_prefixed_bytes(&mut key, &prop_value_canonical_bytes(value));
            }
            Self::PropertyIn {
                key: prop_key,
                value_keys,
                ..
            } => {
                key.push(3);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                for value_key in value_keys {
                    push_len_prefixed_bytes(&mut key, value_key);
                }
            }
            Self::PropertyRange {
                key: prop_key,
                lower,
                upper,
            } => {
                key.push(4);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                push_range_bound_structural_key(&mut key, lower.as_ref());
                push_range_bound_structural_key(&mut key, upper.as_ref());
            }
            Self::PropertyExists { key: prop_key } => {
                key.push(5);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
            }
            Self::PropertyMissing { key: prop_key } => {
                key.push(6);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
            }
            Self::WeightRange { lower, upper } => {
                key.push(7);
                key.extend_from_slice(&lower.map(f32::to_bits).unwrap_or(0).to_be_bytes());
                key.extend_from_slice(&upper.map(f32::to_bits).unwrap_or(0).to_be_bytes());
                key.push(lower.is_some() as u8);
                key.push(upper.is_some() as u8);
            }
            Self::UpdatedAtRange { lower_ms, upper_ms } => {
                key.push(8);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::CreatedAtRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            } => {
                key.push(16);
                key.push(lower.is_some() as u8);
                key.extend_from_slice(&lower.unwrap_or(0).to_be_bytes());
                key.push(upper.is_some() as u8);
                key.extend_from_slice(&upper.unwrap_or(0).to_be_bytes());
                key.push(*lower_inclusive as u8);
                key.push(*upper_inclusive as u8);
            }
            Self::ValidAt { epoch_ms } => {
                key.push(9);
                key.extend_from_slice(&epoch_ms.to_be_bytes());
            }
            Self::ValidFromRange { lower_ms, upper_ms } => {
                key.push(10);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::ValidToRange { lower_ms, upper_ms } => {
                key.push(11);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::And(children) => {
                key.push(12);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Or(children) => {
                key.push(13);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Not(child) => {
                key.push(14);
                push_len_prefixed_bytes(&mut key, &child.structural_key());
            }
        }
        key
    }
}

fn require_non_empty_filter_key(key: &str, context: &str) -> Result<(), EngineError> {
    if key.is_empty() {
        Err(EngineError::InvalidOperation(format!(
            "{context} property key must be non-empty"
        )))
    } else {
        Ok(())
    }
}

fn filter_children_sorted_dedup(mut children: Vec<NormalizedNodeFilter>) -> Vec<NormalizedNodeFilter> {
    children.sort_by_key(NormalizedNodeFilter::structural_key);
    children.dedup_by(|left, right| left.structural_key() == right.structural_key());
    children
}

fn insert_range_constraint(
    ranges_by_key: &mut HashMap<String, ValidatedNumericRange>,
    key: &str,
    range: ValidatedNumericRange,
) -> bool {
    if range.is_empty {
        return false;
    }
    match ranges_by_key.entry(key.to_string()) {
        Entry::Occupied(mut entry) => {
            let intersected = intersect_validated_numeric_ranges(entry.get(), &range);
            if intersected.is_empty {
                false
            } else {
                entry.insert(intersected);
                true
            }
        }
        Entry::Vacant(entry) => {
            entry.insert(range);
            true
        }
    }
}

fn normalize_normalized_and_filter(
    mut flattened: Vec<NormalizedNodeFilter>,
) -> Result<NormalizedNodeFilter, EngineError> {
    let mut eq_by_key: HashMap<String, PropValue> = HashMap::new();
    let mut ranges_by_key: HashMap<String, ValidatedNumericRange> = HashMap::new();
    let mut exists_keys = HashSet::new();
    let mut missing_keys = HashSet::new();
    for child in &flattened {
        match child {
            NormalizedNodeFilter::PropertyEquals { key, value } => {
                if let Some(existing) = eq_by_key.get(key) {
                    if !prop_values_equal_for_filter(existing, value) {
                        return Ok(NormalizedNodeFilter::AlwaysFalse);
                    }
                } else {
                    eq_by_key.insert(key.clone(), value.clone());
                }
                exists_keys.insert(key.clone());
            }
            NormalizedNodeFilter::PropertyRange { key, lower, upper } => {
                let range = validate_numeric_range_bounds(lower.as_ref(), upper.as_ref(), None)?;
                if !insert_range_constraint(&mut ranges_by_key, key, range) {
                    return Ok(NormalizedNodeFilter::AlwaysFalse);
                }
                exists_keys.insert(key.clone());
            }
            NormalizedNodeFilter::PropertyExists { key } => {
                exists_keys.insert(key.clone());
            }
            NormalizedNodeFilter::PropertyMissing { key } => {
                missing_keys.insert(key.clone());
            }
            _ => {}
        }
    }

    if exists_keys.iter().any(|key| missing_keys.contains(key)) {
        return Ok(NormalizedNodeFilter::AlwaysFalse);
    }

    for (key, value) in &eq_by_key {
        if let Some(range) = ranges_by_key.get(key) {
            if !prop_value_within_validated_range(value, range) {
                return Ok(NormalizedNodeFilter::AlwaysFalse);
            }
        }
    }

    flattened.retain(|child| match child {
        NormalizedNodeFilter::PropertyExists { key } => !eq_by_key.contains_key(key),
        _ => true,
    });

    let flattened = filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedNodeFilter::AlwaysTrue,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedNodeFilter::And(flattened),
    })
}

fn normalize_and_filter(
    children: &[NodeFilterExpr],
) -> Result<NormalizedNodeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "and filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_node_filter_expr(child)? {
            NormalizedNodeFilter::AlwaysFalse => return Ok(NormalizedNodeFilter::AlwaysFalse),
            NormalizedNodeFilter::AlwaysTrue => {}
            NormalizedNodeFilter::And(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    normalize_normalized_and_filter(flattened)
}

fn normalize_or_filter(children: &[NodeFilterExpr]) -> Result<NormalizedNodeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "or filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_node_filter_expr(child)? {
            NormalizedNodeFilter::AlwaysTrue => return Ok(NormalizedNodeFilter::AlwaysTrue),
            NormalizedNodeFilter::AlwaysFalse => {}
            NormalizedNodeFilter::Or(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    let flattened = filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedNodeFilter::AlwaysFalse,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedNodeFilter::Or(flattened),
    })
}

type FlexibleU64Range = Option<(Option<u64>, Option<u64>, bool, bool)>;
type FlexibleI64Range = Option<(Option<i64>, Option<i64>, bool, bool)>;

fn normalize_u64_flexible_range(
    lower: Option<u64>,
    upper: Option<u64>,
    lower_inclusive: bool,
    upper_inclusive: bool,
    context: &str,
) -> Result<FlexibleU64Range, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(format!(
            "{context} range filters require at least one bound"
        )));
    }
    if let (Some(lower), Some(upper)) = (lower, upper) {
        if lower > upper || (lower == upper && (!lower_inclusive || !upper_inclusive)) {
            return Ok(None);
        }
    }
    Ok(Some((lower, upper, lower_inclusive, upper_inclusive)))
}

fn normalize_i64_flexible_range(
    lower: Option<i64>,
    upper: Option<i64>,
    lower_inclusive: bool,
    upper_inclusive: bool,
    context: &str,
) -> Result<FlexibleI64Range, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(format!(
            "{context} range filters require at least one bound"
        )));
    }
    if let (Some(lower), Some(upper)) = (lower, upper) {
        if lower > upper || (lower == upper && (!lower_inclusive || !upper_inclusive)) {
            return Ok(None);
        }
    }
    Ok(Some((lower, upper, lower_inclusive, upper_inclusive)))
}

fn normalize_node_weight_range(
    lower: Option<f32>,
    upper: Option<f32>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> Result<NormalizedNodeFilter, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(
            "node weight range filters require at least one bound".into(),
        ));
    }
    if lower.is_some_and(f32::is_nan) || upper.is_some_and(f32::is_nan) {
        return Err(EngineError::InvalidOperation(
            "node weight range bounds must not be NaN".into(),
        ));
    }
    if let (Some(lower), Some(upper)) = (lower, upper) {
        if lower > upper || (lower == upper && (!lower_inclusive || !upper_inclusive)) {
            return Ok(NormalizedNodeFilter::AlwaysFalse);
        }
    }
    Ok(NormalizedNodeFilter::WeightRange {
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
    })
}

fn normalize_node_filter_expr(expr: &NodeFilterExpr) -> Result<NormalizedNodeFilter, EngineError> {
    match expr {
        NodeFilterExpr::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            let Some((lower, upper, lower_inclusive, upper_inclusive)) = normalize_u64_flexible_range(
                *lower,
                *upper,
                *lower_inclusive,
                *upper_inclusive,
                "node id",
            )?
            else {
                return Ok(NormalizedNodeFilter::AlwaysFalse);
            };
            Ok(NormalizedNodeFilter::IdRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            })
        }
        NodeFilterExpr::KeyEquals(value) => {
            if value.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "node key equals filters require a non-empty key".into(),
                ));
            }
            Ok(NormalizedNodeFilter::KeyEquals(value.clone()))
        }
        NodeFilterExpr::KeyIn(values) => {
            if values.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "node key in filters must contain at least one value".into(),
                ));
            }
            if values.iter().any(String::is_empty) {
                return Err(EngineError::InvalidOperation(
                    "node key in filters require non-empty keys".into(),
                ));
            }
            let mut values = values.clone();
            values.sort();
            values.dedup();
            if values.len() == 1 {
                return Ok(NormalizedNodeFilter::KeyEquals(values.pop().unwrap()));
            }
            Ok(NormalizedNodeFilter::KeyIn { values })
        }
        NodeFilterExpr::PropertyEquals { key, value } => {
            require_non_empty_filter_key(key, "property equals filter")?;
            Ok(NormalizedNodeFilter::PropertyEquals {
                key: key.clone(),
                value: value.clone(),
            })
        }
        NodeFilterExpr::PropertyIn { key, values } => {
            require_non_empty_filter_key(key, "property in filter")?;
            if values.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "property in filters must contain at least one value".into(),
                ));
            }
            let mut keyed_values: Vec<(Vec<u8>, PropValue)> = values
                .iter()
                .map(|value| (prop_value_canonical_bytes(value), value.clone()))
                .collect();
            keyed_values.sort_by(|left, right| left.0.cmp(&right.0));
            keyed_values.dedup_by(|left, right| left.0 == right.0);
            if keyed_values.len() == 1 {
                let (_, value) = keyed_values.into_iter().next().unwrap();
                return Ok(NormalizedNodeFilter::PropertyEquals {
                    key: key.clone(),
                    value,
                });
            }
            let (value_keys, deduped_values): (Vec<Vec<u8>>, Vec<PropValue>) =
                keyed_values.into_iter().unzip();
            Ok(NormalizedNodeFilter::PropertyIn {
                key: key.clone(),
                values: deduped_values,
                value_keys,
            })
        }
        NodeFilterExpr::PropertyRange { key, lower, upper } => {
            require_non_empty_filter_key(key, "property range filter")?;
            if validate_numeric_range_bounds(lower.as_ref(), upper.as_ref(), None)?.is_empty {
                return Ok(NormalizedNodeFilter::AlwaysFalse);
            }
            Ok(NormalizedNodeFilter::PropertyRange {
                key: key.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
            })
        }
        NodeFilterExpr::PropertyExists { key } => {
            require_non_empty_filter_key(key, "property exists filter")?;
            Ok(NormalizedNodeFilter::PropertyExists { key: key.clone() })
        }
        NodeFilterExpr::PropertyMissing { key } => {
            require_non_empty_filter_key(key, "property missing filter")?;
            Ok(NormalizedNodeFilter::PropertyMissing { key: key.clone() })
        }
        NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            if lower_ms.is_none() && upper_ms.is_none() {
                return Err(EngineError::InvalidOperation(
                    "updated-at range filters require at least one bound".into(),
                ));
            }
            let lower_ms = lower_ms.unwrap_or(i64::MIN);
            let upper_ms = upper_ms.unwrap_or(i64::MAX);
            if lower_ms > upper_ms {
                return Ok(NormalizedNodeFilter::AlwaysFalse);
            }
            Ok(NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms })
        }
        NodeFilterExpr::WeightRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => normalize_node_weight_range(
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        ),
        NodeFilterExpr::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            let Some((lower, upper, lower_inclusive, upper_inclusive)) = normalize_i64_flexible_range(
                *lower,
                *upper,
                *lower_inclusive,
                *upper_inclusive,
                "node created-at",
            )?
            else {
                return Ok(NormalizedNodeFilter::AlwaysFalse);
            };
            Ok(NormalizedNodeFilter::CreatedAtRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            })
        }
        NodeFilterExpr::And(children) => normalize_and_filter(children),
        NodeFilterExpr::Or(children) => normalize_or_filter(children),
        NodeFilterExpr::Not(child) => match normalize_node_filter_expr(child)? {
            NormalizedNodeFilter::AlwaysTrue => Ok(NormalizedNodeFilter::AlwaysFalse),
            NormalizedNodeFilter::AlwaysFalse => Ok(NormalizedNodeFilter::AlwaysTrue),
            NormalizedNodeFilter::PropertyExists { key } => {
                Ok(NormalizedNodeFilter::PropertyMissing { key })
            }
            NormalizedNodeFilter::PropertyMissing { key } => {
                Ok(NormalizedNodeFilter::PropertyExists { key })
            }
            NormalizedNodeFilter::Not(grandchild) => Ok(*grandchild),
            normalized => Ok(NormalizedNodeFilter::Not(Box::new(normalized))),
        },
    }
}

fn normalize_optional_node_filter(
    filter: Option<&NodeFilterExpr>,
) -> Result<NormalizedNodeFilter, EngineError> {
    filter.map(normalize_node_filter_expr).unwrap_or(Ok(NormalizedNodeFilter::AlwaysTrue))
}

fn edge_filter_children_sorted_dedup(mut children: Vec<NormalizedEdgeFilter>) -> Vec<NormalizedEdgeFilter> {
    children.sort_by_key(NormalizedEdgeFilter::structural_key);
    children.dedup_by(|left, right| left.structural_key() == right.structural_key());
    children
}

fn normalize_normalized_edge_and_filter(
    mut flattened: Vec<NormalizedEdgeFilter>,
) -> Result<NormalizedEdgeFilter, EngineError> {
    let mut eq_by_key: HashMap<String, PropValue> = HashMap::new();
    let mut ranges_by_key: HashMap<String, ValidatedNumericRange> = HashMap::new();
    let mut exists_keys = HashSet::new();
    let mut missing_keys = HashSet::new();
    for child in &flattened {
        match child {
            NormalizedEdgeFilter::PropertyEquals { key, value } => {
                if let Some(existing) = eq_by_key.get(key) {
                    if !prop_values_equal_for_filter(existing, value) {
                        return Ok(NormalizedEdgeFilter::AlwaysFalse);
                    }
                } else {
                    eq_by_key.insert(key.clone(), value.clone());
                }
                exists_keys.insert(key.clone());
            }
            NormalizedEdgeFilter::PropertyRange { key, lower, upper } => {
                let range = validate_numeric_range_bounds(lower.as_ref(), upper.as_ref(), None)?;
                if !insert_range_constraint(&mut ranges_by_key, key, range) {
                    return Ok(NormalizedEdgeFilter::AlwaysFalse);
                }
                exists_keys.insert(key.clone());
            }
            NormalizedEdgeFilter::PropertyIn { key, .. }
            | NormalizedEdgeFilter::PropertyExists { key } => {
                exists_keys.insert(key.clone());
            }
            NormalizedEdgeFilter::PropertyMissing { key } => {
                missing_keys.insert(key.clone());
            }
            _ => {}
        }
    }

    if exists_keys.iter().any(|key| missing_keys.contains(key)) {
        return Ok(NormalizedEdgeFilter::AlwaysFalse);
    }

    for (key, value) in &eq_by_key {
        if let Some(range) = ranges_by_key.get(key) {
            if !prop_value_within_validated_range(value, range) {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            }
        }
    }

    flattened.retain(|child| match child {
        NormalizedEdgeFilter::PropertyExists { key } => !eq_by_key.contains_key(key),
        _ => true,
    });

    let flattened = edge_filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedEdgeFilter::AlwaysTrue,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedEdgeFilter::And(flattened),
    })
}

fn normalize_edge_and_filter(
    children: &[EdgeFilterExpr],
) -> Result<NormalizedEdgeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "edge and filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_edge_filter_expr(child)? {
            NormalizedEdgeFilter::AlwaysFalse => return Ok(NormalizedEdgeFilter::AlwaysFalse),
            NormalizedEdgeFilter::AlwaysTrue => {}
            NormalizedEdgeFilter::And(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    normalize_normalized_edge_and_filter(flattened)
}

fn normalize_edge_or_filter(children: &[EdgeFilterExpr]) -> Result<NormalizedEdgeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "edge or filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_edge_filter_expr(child)? {
            NormalizedEdgeFilter::AlwaysTrue => return Ok(NormalizedEdgeFilter::AlwaysTrue),
            NormalizedEdgeFilter::AlwaysFalse => {}
            NormalizedEdgeFilter::Or(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    let flattened = edge_filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedEdgeFilter::AlwaysFalse,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedEdgeFilter::Or(flattened),
    })
}

fn normalize_i64_range(
    lower: Option<i64>,
    upper: Option<i64>,
    context: &str,
) -> Result<Option<(i64, i64)>, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(format!(
            "{context} range filters require at least one bound"
        )));
    }
    let lower = lower.unwrap_or(i64::MIN);
    let upper = upper.unwrap_or(i64::MAX);
    if lower > upper {
        Ok(None)
    } else {
        Ok(Some((lower, upper)))
    }
}

fn normalize_weight_range(
    lower: Option<f32>,
    upper: Option<f32>,
) -> Result<NormalizedEdgeFilter, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(
            "edge weight range filters require at least one bound".into(),
        ));
    }
    if lower.is_some_and(f32::is_nan) || upper.is_some_and(f32::is_nan) {
        return Err(EngineError::InvalidOperation(
            "edge weight range bounds must not be NaN".into(),
        ));
    }
    if let (Some(lower), Some(upper)) = (lower, upper) {
        if lower > upper {
            return Ok(NormalizedEdgeFilter::AlwaysFalse);
        }
    }
    Ok(NormalizedEdgeFilter::WeightRange { lower, upper })
}

fn normalize_edge_filter_expr(expr: &EdgeFilterExpr) -> Result<NormalizedEdgeFilter, EngineError> {
    match expr {
        EdgeFilterExpr::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            let Some((lower, upper, lower_inclusive, upper_inclusive)) = normalize_u64_flexible_range(
                *lower,
                *upper,
                *lower_inclusive,
                *upper_inclusive,
                "edge id",
            )?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::IdRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            })
        }
        EdgeFilterExpr::PropertyEquals { key, value } => {
            require_non_empty_filter_key(key, "edge property equals filter")?;
            Ok(NormalizedEdgeFilter::PropertyEquals {
                key: key.clone(),
                value: value.clone(),
            })
        }
        EdgeFilterExpr::PropertyIn { key, values } => {
            require_non_empty_filter_key(key, "edge property in filter")?;
            if values.is_empty() {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            }
            let mut keyed_values: Vec<(Vec<u8>, PropValue)> = values
                .iter()
                .map(|value| (prop_value_canonical_bytes(value), value.clone()))
                .collect();
            keyed_values.sort_by(|left, right| left.0.cmp(&right.0));
            keyed_values.dedup_by(|left, right| left.0 == right.0);
            if keyed_values.len() == 1 {
                let (_, value) = keyed_values.into_iter().next().unwrap();
                return Ok(NormalizedEdgeFilter::PropertyEquals {
                    key: key.clone(),
                    value,
                });
            }
            let (value_keys, deduped_values): (Vec<Vec<u8>>, Vec<PropValue>) =
                keyed_values.into_iter().unzip();
            Ok(NormalizedEdgeFilter::PropertyIn {
                key: key.clone(),
                values: deduped_values,
                value_keys,
            })
        }
        EdgeFilterExpr::PropertyRange { key, lower, upper } => {
            require_non_empty_filter_key(key, "edge property range filter")?;
            if validate_numeric_range_bounds(lower.as_ref(), upper.as_ref(), None)?.is_empty {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            }
            Ok(NormalizedEdgeFilter::PropertyRange {
                key: key.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
            })
        }
        EdgeFilterExpr::PropertyExists { key } => {
            require_non_empty_filter_key(key, "edge property exists filter")?;
            Ok(NormalizedEdgeFilter::PropertyExists { key: key.clone() })
        }
        EdgeFilterExpr::PropertyMissing { key } => {
            require_non_empty_filter_key(key, "edge property missing filter")?;
            Ok(NormalizedEdgeFilter::PropertyMissing { key: key.clone() })
        }
        EdgeFilterExpr::WeightRange { lower, upper } => normalize_weight_range(*lower, *upper),
        EdgeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            let Some((lower_ms, upper_ms)) =
                normalize_i64_range(*lower_ms, *upper_ms, "edge updated-at")?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms })
        }
        EdgeFilterExpr::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            let Some((lower, upper, lower_inclusive, upper_inclusive)) = normalize_i64_flexible_range(
                *lower,
                *upper,
                *lower_inclusive,
                *upper_inclusive,
                "edge created-at",
            )?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::CreatedAtRange {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            })
        }
        EdgeFilterExpr::ValidAt { epoch_ms } => {
            Ok(NormalizedEdgeFilter::ValidAt { epoch_ms: *epoch_ms })
        }
        EdgeFilterExpr::ValidFromRange { lower_ms, upper_ms } => {
            let Some((lower_ms, upper_ms)) =
                normalize_i64_range(*lower_ms, *upper_ms, "edge valid-from")?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms })
        }
        EdgeFilterExpr::ValidToRange { lower_ms, upper_ms } => {
            let Some((lower_ms, upper_ms)) =
                normalize_i64_range(*lower_ms, *upper_ms, "edge valid-to")?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms })
        }
        EdgeFilterExpr::And(children) => normalize_edge_and_filter(children),
        EdgeFilterExpr::Or(children) => normalize_edge_or_filter(children),
        EdgeFilterExpr::Not(child) => match normalize_edge_filter_expr(child)? {
            NormalizedEdgeFilter::AlwaysTrue => Ok(NormalizedEdgeFilter::AlwaysFalse),
            NormalizedEdgeFilter::AlwaysFalse => Ok(NormalizedEdgeFilter::AlwaysTrue),
            NormalizedEdgeFilter::PropertyExists { key } => {
                Ok(NormalizedEdgeFilter::PropertyMissing { key })
            }
            NormalizedEdgeFilter::PropertyMissing { key } => {
                Ok(NormalizedEdgeFilter::PropertyExists { key })
            }
            NormalizedEdgeFilter::Not(grandchild) => Ok(*grandchild),
            normalized => Ok(NormalizedEdgeFilter::Not(Box::new(normalized))),
        },
    }
}

fn normalize_optional_edge_filter(
    filter: Option<&EdgeFilterExpr>,
) -> Result<NormalizedEdgeFilter, EngineError> {
    filter.map(normalize_edge_filter_expr).unwrap_or(Ok(NormalizedEdgeFilter::AlwaysTrue))
}

fn sorted_dedup_u64(mut values: Vec<u64>) -> Vec<u64> {
    values.sort_unstable();
    values.dedup();
    values
}

fn push_query_warning(warnings: &mut Vec<QueryPlanWarning>, warning: QueryPlanWarning) {
    if !warnings.contains(&warning) {
        warnings.push(warning);
    }
}

fn unknown_node_label_count(filter: &ResolvedNodeLabelFilter) -> usize {
    match filter {
        ResolvedNodeLabelFilter::Unconstrained => 0,
        ResolvedNodeLabelFilter::Empty {
            unknown_label_count,
            ..
        }
        | ResolvedNodeLabelFilter::LabelSet {
            unknown_label_count,
            ..
        } => *unknown_label_count,
    }
}

fn single_resolved_label_id(filter: &ResolvedNodeLabelFilter) -> Option<u32> {
    match filter {
        ResolvedNodeLabelFilter::LabelSet { label_ids, .. } if label_ids.len() == 1 => {
            Some(label_ids.single_label_id())
        }
        _ => None,
    }
}

impl ReadView {
    fn resolve_node_query_label_filter(
        &self,
        label_filter: Option<&NodeLabelFilter>,
    ) -> Result<(ResolvedNodeLabelFilter, Option<u32>, Vec<QueryPlanWarning>), EngineError> {
        let resolved = self
            .label_catalog
            .resolve_node_label_filter_request(label_filter)?;
        let mut warnings = Vec::new();
        if unknown_node_label_count(&resolved) > 0 {
            push_query_warning(&mut warnings, QueryPlanWarning::UnknownNodeLabel);
        }
        let single_label_id = single_resolved_label_id(&resolved);
        Ok((resolved, single_label_id, warnings))
    }

    fn normalize_edge_query_with_anchor_requirement(
        &self,
        query: &EdgeQuery,
        require_anchor: bool,
    ) -> Result<NormalizedEdgeQuery, EngineError> {
        let mut warnings = Vec::new();
        let mut label_id = None;
        let mut filter = normalize_optional_edge_filter(query.filter.as_ref())?;
        if let Some(label) = query.label.as_deref() {
            match self.label_catalog.resolve_edge_label_for_read(label)? {
                Some(resolved) => label_id = Some(resolved),
                None => {
                    filter = NormalizedEdgeFilter::AlwaysFalse;
                    push_query_warning(&mut warnings, QueryPlanWarning::UnknownEdgeLabel);
                }
            }
        }
        let ids = sorted_dedup_u64(query.ids.clone());
        let from_ids = sorted_dedup_u64(query.from_ids.clone());
        let to_ids = sorted_dedup_u64(query.to_ids.clone());
        let endpoint_ids = sorted_dedup_u64(query.endpoint_ids.clone());

        if require_anchor
            && label_id.is_none()
            && ids.is_empty()
            && from_ids.is_empty()
            && to_ids.is_empty()
            && endpoint_ids.is_empty()
            && !filter.has_metadata_anchor()
            && !query.allow_full_scan
            && !filter.is_always_false()
        {
            return Err(EngineError::InvalidOperation(
                "edge query requires label, ids, from_ids, to_ids, endpoint_ids, metadata filter, or allow_full_scan".into(),
            ));
        }

        Ok(NormalizedEdgeQuery {
            label_id,
            ids,
            from_ids,
            to_ids,
            endpoint_ids,
            filter,
            allow_full_scan: query.allow_full_scan,
            page: query.page.clone(),
            warnings,
        })
    }

    fn normalize_edge_query(&self, query: &EdgeQuery) -> Result<NormalizedEdgeQuery, EngineError> {
        self.normalize_edge_query_with_anchor_requirement(query, true)
    }

    fn normalize_node_query_with_anchor_requirement(
        &self,
        query: &NodeQuery,
        require_anchor: bool,
    ) -> Result<NormalizedNodeQuery, EngineError> {
        let (label_filter, single_label_id, warnings) = self.resolve_node_query_label_filter(
            query.label_filter.as_ref(),
        )?;
        let mut filter = normalize_optional_node_filter(query.filter.as_ref())?;
        if label_filter.is_empty_constraint() {
            filter = NormalizedNodeFilter::AlwaysFalse;
        }
        let mut ids = query.ids.clone();
        ids.sort_unstable();
        ids.dedup();

        let mut keys = query.keys.clone();
        keys.sort();
        keys.dedup();

        if !keys.is_empty() {
            match label_filter {
                ResolvedNodeLabelFilter::LabelSet { label_ids, .. } if label_ids.len() == 1 => {}
                ResolvedNodeLabelFilter::Empty { .. } => {}
                ResolvedNodeLabelFilter::Unconstrained => {
                    return Err(EngineError::InvalidOperation(
                        "node query keys require exactly one resolved label".into(),
                    ));
                }
                ResolvedNodeLabelFilter::LabelSet { .. } => {
                    return Err(EngineError::InvalidOperation(
                        "node query keys require exactly one resolved label".into(),
                    ));
                }
            }
        }

        let has_label_anchor = matches!(
            label_filter,
            ResolvedNodeLabelFilter::LabelSet { .. } | ResolvedNodeLabelFilter::Empty { .. }
        );
        if require_anchor
            && ids.is_empty()
            && keys.is_empty()
            && !has_label_anchor
            && !query.allow_full_scan
            && !filter.is_always_false()
        {
            return Err(EngineError::InvalidOperation(
                "node query requires label_filter, ids, keys, or allow_full_scan".into(),
            ));
        }

        Ok(NormalizedNodeQuery {
            single_label_id,
            label_filter,
            ids,
            keys,
            filter,
            allow_full_scan: query.allow_full_scan,
            page: query.page.clone(),
            warnings,
        })
    }

    fn normalize_node_query(&self, query: &NodeQuery) -> Result<NormalizedNodeQuery, EngineError> {
        self.normalize_node_query_with_anchor_requirement(query, true)
    }

}
