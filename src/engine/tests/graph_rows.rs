// Graph-row DTO, normalizer, binding, and evaluator tests.

use crate::graph_row::{
    bind_graph_expr, compare_graph_sort_atoms, eval_bound_graph_expr, eval_graph_expr,
    eval_graph_predicate, graph_sort_atom_for_value, project_bound_graph_row_values,
    project_graph_row_values, BoundGraphEvalContext, BoundGraphExpr, GraphBindingSchema,
    GraphBindingSlotKind, GraphBoundEdge, GraphBoundNode, GraphBoundPath, GraphEvalContext,
    GraphEvalValue, GraphHiddenOccurrence, GraphSortAtom,
};
use crate::row_projection::{PathSelectedFieldNeeds, PropertySelection as RowPropertySelection};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BTreeMap;

fn graph_node(alias: &str) -> GraphNodePattern {
    GraphNodePattern {
        alias: alias.to_string(),
        label_filter: None,
        ids: Vec::new(),
        keys: Vec::new(),
        filter: None,
    }
}

fn graph_node_with_label(alias: &str, label: &str) -> GraphNodePattern {
    GraphNodePattern {
        alias: alias.to_string(),
        label_filter: Some(NodeLabelFilter {
            labels: vec![label.to_string()],
            mode: LabelMatchMode::All,
        }),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: None,
    }
}

fn graph_edge(alias: Option<&str>, from: &str, to: &str) -> GraphPatternPiece {
    GraphPatternPiece::Edge(GraphEdgePattern {
        alias: alias.map(str::to_string),
        from_alias: from.to_string(),
        to_alias: to.to_string(),
        direction: Direction::Outgoing,
        label_filter: Vec::new(),
        filter: None,
    })
}

fn graph_vlp(
    path_alias: Option<&str>,
    edge_alias: Option<&str>,
    from: &str,
    to: &str,
    min_hops: u8,
    max_hops: u8,
) -> GraphPatternPiece {
    GraphPatternPiece::VariableLength(GraphVariableLengthPattern {
        path_alias: path_alias.map(str::to_string),
        edge_alias: edge_alias.map(str::to_string),
        from_alias: from.to_string(),
        to_alias: to.to_string(),
        direction: Direction::Outgoing,
        label_filter: Vec::new(),
        filter: None,
        min_hops,
        max_hops,
    })
}

fn graph_optional(pieces: Vec<GraphPatternPiece>, where_: Option<GraphExpr>) -> GraphPatternPiece {
    GraphPatternPiece::Optional(GraphOptionalGroup { pieces, where_ })
}

fn graph_query(nodes: &[&str], pieces: Vec<GraphPatternPiece>) -> GraphRowQuery {
    GraphRowQuery {
        nodes: nodes.iter().map(|alias| graph_node(alias)).collect(),
        pieces,
        where_: None,
        return_items: None,
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        at_epoch: None,
        params: std::collections::BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: true,
            ..GraphQueryOptions::default()
        },
    }
}

fn graph_row_test_engine() -> (TempDir, DatabaseEngine) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    (dir, engine)
}

fn execute_graph_row_with_anchored_edge_override(
    view: &ReadView,
    normalized: &NormalizedGraphRowQuery,
    runtime: &GraphRowRuntimePlan,
    physical: &GraphRowPhysicalPlan,
    source_override: &GraphRowAnchoredEdgeSourceOverride<'_>,
) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
    let mut caches = GraphRowExecutionCaches::new(normalized.options.max_intermediate_bindings);
    let mut attempt = caches.begin_attempt();
    let mut followups = GraphRowUniqueFollowups::new(Vec::new());
    let result = view.graph_row_execute_runtime_plan_with_caches(
        normalized,
        runtime,
        physical,
        Some(source_override),
        None,
        GraphRowRuntimeGoal::AllRows,
        normalized.at_epoch.unwrap_or(i64::MAX / 2),
        view.query_policy_cutoffs().as_ref(),
        &mut followups,
        &mut 0,
        &mut 0,
        &mut 0,
        None,
        &mut caches,
        &mut attempt,
    );
    if result.is_ok() {
        caches.commit_attempt(&mut attempt);
    } else {
        caches.rollback_attempt(&mut attempt);
    }
    result
}

fn assert_graph_row_single_read_followup_enqueued(engine: &DatabaseEngine, action: impl FnOnce()) {
    let (followup_ready_rx, followup_release_tx) = engine.set_runtime_publish_pause();
    action();
    followup_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 1);
    followup_release_tx.send(()).unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while engine.pending_secondary_index_followup_count_for_test() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "secondary-index read followup did not drain"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn graph_row_props(entries: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn insert_graph_row_node(
    engine: &DatabaseEngine,
    label: &str,
    key: &str,
    entries: &[(&str, PropValue)],
) -> u64 {
    engine
        .upsert_node(
            label,
            key,
            UpsertNodeOptions {
                props: graph_row_props(entries),
                ..Default::default()
            },
        )
        .unwrap()
}

fn insert_graph_row_node_with_labels(
    engine: &DatabaseEngine,
    labels: &[&str],
    key: &str,
    entries: &[(&str, PropValue)],
) -> u64 {
    engine
        .upsert_node(
            labels,
            key,
            UpsertNodeOptions {
                props: graph_row_props(entries),
                ..Default::default()
            },
        )
        .unwrap()
}

fn insert_graph_row_edge(
    engine: &DatabaseEngine,
    from: u64,
    to: u64,
    label: &str,
    entries: &[(&str, PropValue)],
) -> u64 {
    engine
        .upsert_edge(
            from,
            to,
            label,
            UpsertEdgeOptions {
                props: graph_row_props(entries),
                ..Default::default()
            },
        )
        .unwrap()
}

fn insert_graph_row_weighted_edge(
    engine: &DatabaseEngine,
    from: u64,
    to: u64,
    label: &str,
    entries: &[(&str, PropValue)],
    weight: f32,
) -> u64 {
    engine
        .upsert_edge(
            from,
            to,
            label,
            UpsertEdgeOptions {
                props: graph_row_props(entries),
                weight,
                ..Default::default()
            },
        )
        .unwrap()
}

fn set_graph_row_edge_updated_at(engine: &DatabaseEngine, edge_id: u64, updated_at: i64) {
    let edge = internal_edge_record(engine, edge_id).unwrap().unwrap();
    write_internal_wal_op(
        engine,
        &WalOp::UpsertEdge(EdgeRecord {
            updated_at,
            ..edge
        }),
    )
    .unwrap();
}

fn set_graph_row_edge_validity(
    engine: &DatabaseEngine,
    edge_id: u64,
    valid_from: i64,
    valid_to: i64,
) {
    let edge = internal_edge_record(engine, edge_id).unwrap().unwrap();
    write_internal_wal_op(
        engine,
        &WalOp::UpsertEdge(EdgeRecord {
            valid_from,
            valid_to,
            updated_at: edge.updated_at.saturating_add(1),
            ..edge
        }),
    )
    .unwrap();
}

fn graph_edge_with_label(alias: Option<&str>, from: &str, to: &str, label: &str) -> GraphPatternPiece {
    GraphPatternPiece::Edge(GraphEdgePattern {
        alias: alias.map(str::to_string),
        from_alias: from.to_string(),
        to_alias: to.to_string(),
        direction: Direction::Outgoing,
        label_filter: vec![label.to_string()],
        filter: None,
    })
}

fn graph_return_binding(alias: &str, projection: GraphReturnProjection) -> GraphReturnItem {
    GraphReturnItem {
        expr: GraphExpr::Binding(alias.to_string()),
        alias: Some(alias.to_string()),
        projection,
    }
}

fn graph_return_expr(expr: GraphExpr, alias: &str) -> GraphReturnItem {
    GraphReturnItem {
        expr,
        alias: Some(alias.to_string()),
        projection: GraphReturnProjection::Auto,
    }
}

fn graph_prop(alias: &str, key: &str) -> GraphExpr {
    GraphExpr::Property {
        alias: alias.to_string(),
        key: key.to_string(),
    }
}

fn first_epoch_from_cursor(cursor: &str) -> i64 {
    graph_row_decode_cursor(cursor, GraphQueryOptions::default().max_cursor_bytes)
        .unwrap()
        .effective_at_epoch
}

fn decoded_cursor_payload_len(cursor: &str) -> usize {
    let encoded = cursor.strip_prefix(GRAPH_ROW_CURSOR_PREFIX).unwrap();
    base64url_no_pad_decode(encoded).unwrap().len()
}

fn graph_pipeline_from_row_query(query: &GraphRowQuery) -> GraphPipelineQuery {
    let items = match query.return_items.clone() {
        Some(items) => GraphProjectionItems::Items(
            items
                .into_iter()
                .map(|item| GraphProjectItem {
                    expr: item.expr,
                    alias: item.alias,
                    projection: item.projection,
                })
                .collect(),
        ),
        None => GraphProjectionItems::Star,
    };
    GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: query.nodes.clone(),
                pieces: query.pieces.clone(),
                optional_candidate_where: None,
                where_: query.where_.clone(),
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items,
                distinct: false,
                where_: None,
                order_by: query.order_by.clone(),
                skip: None,
                limit: None,
            }),
        ],
        params: query.params.clone(),
        at_epoch: query.at_epoch,
        page: query.page.clone(),
        output: query.output.clone(),
        options: GraphPipelineOptions {
            allow_full_scan: query.options.allow_full_scan,
            max_rows: query.options.max_page_limit,
            max_intermediate_bindings: query.options.max_intermediate_bindings,
            max_frontier: query.options.max_frontier,
            max_path_hops: query.options.max_path_hops,
            max_paths_per_start: query.options.max_paths_per_start,
            max_order_materialization: query.options.max_order_materialization,
            max_cursor_bytes: query.options.max_cursor_bytes,
            max_query_bytes: query.options.max_query_bytes,
            include_plan: query.options.include_plan,
            profile: query.options.profile,
            ..GraphPipelineOptions::default()
        },
    }
}

fn assert_graph_pipeline_invalid(
    engine: &DatabaseEngine,
    query: &GraphPipelineQuery,
    expected: &str,
) {
    let err = engine.query_graph_pipeline(query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains(expected),
        "expected error containing {expected:?}, got {message:?}"
    );
}

#[test]
fn graph_pipeline_stats_merge_preserves_owner_row_count() {
    let mut owner = empty_graph_pipeline_stats(7);
    owner.rows_after_filter = 5;
    owner.intermediate_rows = 3;
    let mut nested = empty_graph_pipeline_stats(7);
    nested.rows_after_filter = 99;
    nested.intermediate_rows = 11;
    nested.pipeline_rows_materialized = 13;
    nested.groups = 2;
    nested.subquery_invocations = 1;

    owner.merge_from(&nested);

    assert_eq!(owner.rows_after_filter, 5);
    assert_eq!(owner.intermediate_rows, 11);
    assert_eq!(owner.pipeline_rows_materialized, 13);
    assert_eq!(owner.groups, 2);
    assert_eq!(owner.subquery_invocations, 1);
}

#[test]
fn graph_pipeline_options_default_matches_spec() {
    let options = GraphPipelineOptions::default();
    assert!(!options.allow_full_scan);
    assert_eq!(options.max_rows, 10_000);
    assert_eq!(options.max_pipeline_rows, 65_536);
    assert_eq!(options.max_groups, 65_536);
    assert_eq!(options.max_collect_items, 65_536);
    assert_eq!(options.max_union_branches, 16);
    assert_eq!(options.max_subquery_invocations, 4_096);
    assert_eq!(options.max_subquery_depth, 2);
    assert_eq!(options.max_shortest_path_pairs, 4_096);
    assert_eq!(options.max_intermediate_bindings, 65_536);
    assert_eq!(options.max_frontier, 65_536);
    assert_eq!(options.max_path_hops, 16);
    assert_eq!(options.max_paths_per_start, 4_096);
    assert_eq!(options.max_order_materialization, 65_536);
    assert_eq!(options.max_skip, 100_000);
    assert_eq!(options.max_cursor_bytes, 16 * 1024);
    assert_eq!(options.max_query_bytes, 1_048_576);
    assert_eq!(options.max_param_bytes, 1_048_576);
    assert_eq!(options.max_ast_depth, 256);
    assert_eq!(options.max_literal_items, 10_000);
    assert!(!options.include_plan);
    assert!(!options.profile);
}

#[test]
fn graph_row_profile_timing_stats_cover_fast_and_general_paths() {
    let (_dir, engine) = graph_row_test_engine();
    let ada = insert_graph_row_node(&engine, "ProfilePerson", "ada", &[]);
    let ben = insert_graph_row_node(&engine, "ProfilePerson", "ben", &[]);
    let knows = insert_graph_row_edge(&engine, ada, ben, "PROFILE_KNOWS", &[]);

    let mut fast = graph_query(&["n"], Vec::new());
    fast.nodes[0].ids = vec![ada];
    fast.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    fast.page.limit = 1;

    let plain_fast = engine.query_graph_rows(&fast).unwrap();
    assert_eq!(plain_fast.stats.elapsed_us, None);
    assert_eq!(plain_fast.stats.planning_ns, None);
    assert_eq!(plain_fast.stats.execution_ns, None);

    fast.options.profile = true;
    let profiled_fast = engine.query_graph_rows(&fast).unwrap();
    assert!(profiled_fast.stats.elapsed_us.is_some());
    assert!(profiled_fast.stats.planning_ns.is_some());
    assert!(profiled_fast.stats.execution_ns.is_some());

    let mut general = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "PROFILE_KNOWS")],
    );
    general.nodes[0].ids = vec![ada];
    general.return_items = Some(vec![
        graph_return_binding("a", GraphReturnProjection::IdOnly),
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("b", GraphReturnProjection::IdOnly),
    ]);
    general.page.limit = 1;

    let plain_general = engine.query_graph_rows(&general).unwrap();
    assert_eq!(plain_general.stats.elapsed_us, None);
    assert_eq!(plain_general.stats.planning_ns, None);
    assert_eq!(plain_general.stats.execution_ns, None);
    assert_eq!(plain_general.rows[0].values[1], GraphValue::EdgeId(knows));

    general.options.profile = true;
    let profiled_general = engine.query_graph_rows(&general).unwrap();
    assert!(profiled_general.stats.elapsed_us.is_some());
    assert!(profiled_general.stats.planning_ns.is_some());
    assert!(profiled_general.stats.execution_ns.is_some());
}

#[test]
fn graph_pipeline_one_stage_matches_graph_row_result_and_cursor() {
    let (_dir, engine) = graph_row_test_engine();
    insert_graph_row_node(
        &engine,
        "PipelinePerson",
        "ada",
        &[("name", PropValue::String("Ada".to_string()))],
    );
    insert_graph_row_node(
        &engine,
        "PipelinePerson",
        "ben",
        &[("name", PropValue::String("Ben".to_string()))],
    );
    let epoch = now_millis();
    let mut graph_query = GraphRowQuery {
        nodes: vec![graph_node_with_label("n", "PipelinePerson")],
        pieces: Vec::new(),
        where_: None,
        return_items: Some(vec![graph_return_expr(graph_prop("n", "name"), "name")]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 1,
            cursor: None,
        },
        at_epoch: Some(epoch),
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            include_plan: true,
            ..GraphQueryOptions::default()
        },
    };
    let mut pipeline_query = graph_pipeline_from_row_query(&graph_query);

    let graph_first = engine.query_graph_rows(&graph_query).unwrap();
    let pipeline_first = engine.query_graph_pipeline(&pipeline_query).unwrap();
    assert_eq!(pipeline_first.columns, graph_first.columns);
    assert_eq!(pipeline_first.rows, graph_first.rows);
    assert!(graph_first.next_cursor.is_some());
    assert!(pipeline_first.next_cursor.is_some());
    assert_ne!(pipeline_first.next_cursor, graph_first.next_cursor);
    assert!(pipeline_first
        .next_cursor
        .as_ref()
        .is_some_and(|cursor| cursor.starts_with(GRAPH_PIPELINE_CURSOR_PREFIX)));
    assert_eq!(pipeline_first.stats.rows_returned, graph_first.stats.rows_returned);
    assert_eq!(pipeline_first.stats.rows_after_filter, graph_first.stats.rows_after_filter);
    assert!(pipeline_first.plan.is_some());

    let raw_graph_cursor = graph_first.next_cursor.clone().unwrap();
    let pipeline_cursor = pipeline_first.next_cursor.clone().unwrap();
    pipeline_query.page.cursor = Some(raw_graph_cursor.clone());
    assert_graph_pipeline_invalid(
        &engine,
        &pipeline_query,
        "invalid graph pipeline cursor prefix",
    );
    graph_query.page.cursor = Some(pipeline_cursor.clone());
    let graph_cursor_err = engine.query_graph_rows(&graph_query).unwrap_err();
    assert!(
        graph_cursor_err
            .to_string()
            .contains("invalid graph row cursor prefix"),
        "unexpected graph-row cursor error: {graph_cursor_err:?}"
    );

    graph_query.page.cursor = Some(raw_graph_cursor);
    pipeline_query.page.cursor = Some(pipeline_cursor);
    let graph_second = engine.query_graph_rows(&graph_query).unwrap();
    let pipeline_second = engine.query_graph_pipeline(&pipeline_query).unwrap();
    assert_eq!(pipeline_second.columns, graph_second.columns);
    assert_eq!(pipeline_second.rows, graph_second.rows);
    assert_eq!(pipeline_second.next_cursor, None);
    assert_eq!(graph_second.next_cursor, None);
}

#[test]
fn graph_pipeline_multistage_caps_and_cursor_namespaces_are_enforced() {
    let (_dir, engine) = graph_row_test_engine();
    for key in ["a", "b", "c"] {
        insert_graph_row_node(
            &engine,
            "PipelineWithCaps",
            key,
            &[("name", PropValue::String(key.to_string()))],
        );
    }
    let mut query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineWithCaps")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: graph_prop("n", "name"),
                    alias: Some("name".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("name".to_string()),
                    alias: Some("name".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: vec![GraphOrderItem {
                    expr: GraphExpr::Binding("name".to_string()),
                    direction: GraphOrderDirection::Asc,
                }],
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 1,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };

    let first = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(first.clone()),
        vec![vec![GraphValue::String("a".to_string())]]
    );
    assert!(first.next_cursor.is_some());

    let pipeline_cursor = first.next_cursor.clone();
    query.page.cursor = pipeline_cursor.clone();
    let second = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(second),
        vec![vec![GraphValue::String("b".to_string())]]
    );

    let graph_query = GraphRowQuery {
        nodes: vec![graph_node_with_label("n", "PipelineWithCaps")],
        pieces: Vec::new(),
        where_: None,
        return_items: Some(vec![graph_return_expr(graph_prop("n", "name"), "name")]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 1,
            cursor: None,
        },
        at_epoch: query.at_epoch,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            ..GraphQueryOptions::default()
        },
    };
    let raw_graph_cursor = engine
        .query_graph_rows(&graph_query)
        .unwrap()
        .next_cursor
        .unwrap();
    query.page.cursor = Some(raw_graph_cursor);
    assert_graph_pipeline_invalid(&engine, &query, "invalid graph pipeline cursor prefix");

    query.page.cursor = pipeline_cursor;
    let mut tiny_cursor_cap = query.clone();
    tiny_cursor_cap.options.max_cursor_bytes = 4;
    assert_graph_pipeline_invalid(&engine, &tiny_cursor_cap, "max_cursor_bytes 4");

    let mut order_cap = query.clone();
    order_cap.page.cursor = None;
    order_cap.options.max_order_materialization = 1;
    assert_graph_pipeline_invalid(&engine, &order_cap, "max_order_materialization");

    let mut row_cap = query.clone();
    row_cap.page.cursor = None;
    row_cap.options.max_pipeline_rows = 1;
    assert_graph_pipeline_invalid(&engine, &row_cap, "max_intermediate_bindings");

    let mut max_rows = query.clone();
    max_rows.page.cursor = None;
    max_rows.page.limit = 2;
    max_rows.options.max_rows = 1;
    assert_graph_pipeline_invalid(&engine, &max_rows, "max_rows");

    let mut max_skip = query;
    max_skip.page.cursor = None;
    max_skip.page.skip = 2;
    max_skip.options.max_skip = 1;
    assert_graph_pipeline_invalid(&engine, &max_skip, "max_skip");
}

#[test]
fn graph_pipeline_legacy_match_preserves_prefilter_intermediate_cap_and_precedence() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for source_index in 0..2 {
        let source = insert_graph_row_node(
            &engine,
            "PipelineLegacyCapSource",
            &format!("pipeline-legacy-cap-source-{source_index}"),
            &[],
        );
        sources.push(source);
        for target_index in 0..2 {
            let target = insert_graph_row_node(
                &engine,
                "PipelineLegacyCapTarget",
                &format!("pipeline-legacy-cap-target-{source_index}-{target_index}"),
                &[],
            );
            insert_graph_row_edge(
                &engine,
                source,
                target,
                "PIPELINE_LEGACY_CAP_EDGE",
                &[("keep", PropValue::Bool(source_index == 0 && target_index == 0))],
            );
        }
    }

    let mut query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node("source"), graph_node("target")],
                pieces: vec![graph_edge_with_label(
                    Some("edge"),
                    "source",
                    "target",
                    "PIPELINE_LEGACY_CAP_EDGE",
                )],
                optional_candidate_where: None,
                where_: Some(GraphExpr::Binary {
                    left: Box::new(graph_prop("edge", "keep")),
                    op: GraphBinaryOp::Eq,
                    right: Box::new(GraphExpr::Bool(true)),
                }),
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("target".to_string()),
                    alias: Some("target".to_string()),
                    projection: GraphReturnProjection::IdOnly,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: true,
            max_intermediate_bindings: 3,
            max_pipeline_rows: 10,
            ..GraphPipelineOptions::default()
        },
    };
    if let GraphPipelineStage::Match(stage) = &mut query.stages[0] {
        stage.nodes[0].ids = sources;
    }
    assert_graph_pipeline_invalid(&engine, &query, "max_intermediate_bindings");

    query.options.max_intermediate_bindings = 10;
    query.options.max_pipeline_rows = 3;
    assert_graph_pipeline_invalid(&engine, &query, "max_intermediate_bindings");
    query.page.limit = 2;
    assert_graph_pipeline_invalid(&engine, &query, "max_intermediate_bindings");

    query.options.max_intermediate_bindings = 100;
    query.options.max_pipeline_rows = 100;
    query.options.include_plan = true;
    let explained = engine.query_graph_pipeline(&query).unwrap();
    let plan = format!("{:?}", explained.plan.unwrap());
    assert!(plan.contains("source=RuntimeOnce{reason=Stage}"), "{plan}");
    assert!(plan.contains("eligibility=stage_sink"), "{plan}");
}

#[test]
fn graph_pipeline_terminal_projection_uses_final_row_cap() {
    let (_dir, engine) = graph_row_test_engine();
    for key in ["a", "b", "c"] {
        insert_graph_row_node(
            &engine,
            "PipelineTerminalCap",
            key,
            &[("name", PropValue::String(key.to_string()))],
        );
    }

    let query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineTerminalCap")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: graph_prop("n", "name"),
                    alias: Some("name".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: vec![GraphOrderItem {
                    expr: graph_prop("n", "name"),
                    direction: GraphOrderDirection::Asc,
                }],
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 1,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            max_pipeline_rows: 3,
            max_rows: 1,
            ..GraphPipelineOptions::default()
        },
    };

    let result = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(result.clone()),
        vec![vec![GraphValue::String("a".to_string())]]
    );
    assert_eq!(result.stats.rows_after_filter, 3);
    assert_eq!(result.stats.rows_returned, 1);
    assert!(result.next_cursor.is_some());

    let mut low_pipeline_cap = query;
    low_pipeline_cap.options.max_pipeline_rows = 2;
    assert_graph_pipeline_invalid(&engine, &low_pipeline_cap, "max_intermediate_bindings");
}

#[test]
fn graph_pipeline_terminal_aggregate_uses_group_and_final_row_caps() {
    let (_dir, engine) = graph_row_test_engine();
    for key in ["a", "b", "c"] {
        insert_graph_row_node(
            &engine,
            "PipelineTerminalAggCap",
            key,
            &[("group", PropValue::String(key.to_string()))],
        );
    }

    let query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineTerminalAggCap")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![
                    GraphProjectItem {
                        expr: graph_prop("n", "group"),
                        alias: Some("group".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Count,
                            distinct: false,
                            arg: None,
                        },
                        alias: Some("count".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                ]),
                distinct: false,
                where_: None,
                order_by: vec![GraphOrderItem {
                    expr: GraphExpr::Binding("group".to_string()),
                    direction: GraphOrderDirection::Asc,
                }],
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 1,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            max_pipeline_rows: 3,
            max_groups: 3,
            max_rows: 1,
            ..GraphPipelineOptions::default()
        },
    };

    let result = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(result.clone()),
        vec![vec![GraphValue::String("a".to_string()), GraphValue::UInt(1)]]
    );
    assert_eq!(result.stats.groups, 3);
    assert_eq!(result.stats.rows_after_filter, 3);
    assert_eq!(result.stats.rows_returned, 1);
    assert!(result.next_cursor.is_some());

    let mut low_group_cap = query;
    low_group_cap.options.max_groups = 2;
    assert_graph_pipeline_invalid(&engine, &low_group_cap, "max_groups");
}

#[test]
fn graph_pipeline_executes_distinct_and_aggregate_project_stages() {
    let (_dir, engine) = graph_row_test_engine();
    for (key, group, score) in [
        ("a", "x", PropValue::Int(1)),
        ("b", "x", PropValue::Int(2)),
        ("c", "y", PropValue::Int(3)),
    ] {
        insert_graph_row_node(
            &engine,
            "PipelineAgg",
            key,
            &[
                ("group", PropValue::String(group.to_string())),
                ("score", score),
            ],
        );
    }

    let distinct = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineAgg")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: graph_prop("n", "group"),
                    alias: Some("group".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: true,
                where_: None,
                order_by: vec![GraphOrderItem {
                    expr: GraphExpr::Binding("group".to_string()),
                    direction: GraphOrderDirection::Asc,
                }],
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            include_plan: true,
            ..GraphPipelineOptions::default()
        },
    };
    let distinct_result = engine.query_graph_pipeline(&distinct).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(distinct_result.clone()),
        vec![
            vec![GraphValue::String("x".to_string())],
            vec![GraphValue::String("y".to_string())],
        ]
    );
    assert!(distinct_result
        .plan
        .unwrap()
        .row_ops
        .iter()
        .any(|op| op.kind == "Distinct"));

    let aggregate = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineAgg")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![
                    GraphProjectItem {
                        expr: graph_prop("n", "group"),
                        alias: Some("group".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Count,
                            distinct: false,
                            arg: None,
                        },
                        alias: Some("count".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Sum,
                            distinct: false,
                            arg: Some(Box::new(graph_prop("n", "score"))),
                        },
                        alias: Some("sum".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Count,
                            distinct: true,
                            arg: Some(Box::new(graph_prop("n", "score"))),
                        },
                        alias: Some("distinct_scores".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                ]),
                distinct: false,
                where_: None,
                order_by: vec![GraphOrderItem {
                    expr: GraphExpr::Binding("group".to_string()),
                    direction: GraphOrderDirection::Asc,
                }],
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            include_plan: true,
            ..GraphPipelineOptions::default()
        },
    };
    let aggregate_result = engine.query_graph_pipeline(&aggregate).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(aggregate_result.clone()),
        vec![
            vec![
                GraphValue::String("x".to_string()),
                GraphValue::UInt(2),
                GraphValue::Int(3),
                GraphValue::UInt(2),
            ],
            vec![
                GraphValue::String("y".to_string()),
                GraphValue::UInt(1),
                GraphValue::Int(3),
                GraphValue::UInt(1),
            ],
        ]
    );
    assert_eq!(aggregate_result.stats.groups, 2);
    let aggregate_plan = aggregate_result.plan.unwrap();
    assert!(aggregate_plan
        .row_ops
        .iter()
        .any(|op| op.kind == "Aggregate"));
    assert!(aggregate_plan.stages.iter().any(|stage| {
        stage.detail.contains("aggregate_distinct_keys=3")
            && stage
                .notes
                .iter()
                .any(|note| note.contains("aggregate DISTINCT"))
    }));

    let count_distinct_star = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Project(GraphProjectStage {
            kind: GraphProjectKind::Return,
            items: GraphProjectionItems::Items(vec![GraphProjectItem {
                expr: GraphExpr::AggregateCall {
                    function: GraphAggregateFunction::Count,
                    distinct: true,
                    arg: None,
                },
                alias: Some("bad".to_string()),
                projection: GraphReturnProjection::Auto,
            }]),
            distinct: false,
            where_: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        })],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    assert_graph_pipeline_invalid(
        &engine,
        &count_distinct_star,
        "DISTINCT requires an argument",
    );

    let sum_star = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Project(GraphProjectStage {
            kind: GraphProjectKind::Return,
            items: GraphProjectionItems::Items(vec![GraphProjectItem {
                expr: GraphExpr::AggregateCall {
                    function: GraphAggregateFunction::Sum,
                    distinct: false,
                    arg: None,
                },
                alias: Some("bad".to_string()),
                projection: GraphReturnProjection::Auto,
            }]),
            distinct: false,
            where_: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        })],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    assert_graph_pipeline_invalid(&engine, &sum_star, "sum aggregate requires an argument");

    let zero_groups = GraphPipelineQuery {
        options: GraphPipelineOptions {
            max_groups: 0,
            ..GraphPipelineOptions::default()
        },
        ..sum_star
    };
    assert_graph_pipeline_invalid(&engine, &zero_groups, "greater than zero");

    let reserved_project_alias = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Project(GraphProjectStage {
            kind: GraphProjectKind::Return,
            items: GraphProjectionItems::Items(vec![GraphProjectItem {
                expr: GraphExpr::Int(1),
                alias: Some("__gql_bad".to_string()),
                projection: GraphReturnProjection::Auto,
            }]),
            distinct: false,
            where_: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        })],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    assert_graph_pipeline_invalid(&engine, &reserved_project_alias, "reserved internal");

    let reserved_match_alias = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("__gql_bad", "PipelineAgg")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Star,
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    assert_graph_pipeline_invalid(&engine, &reserved_match_alias, "reserved internal");
}

#[test]
fn graph_pipeline_aggregate_collect_hydrates_nested_graph_values_at_output() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(
        &engine,
        "PipelineCollectElement",
        "a",
        &[("name", PropValue::String("a".to_string()))],
    );
    let b = insert_graph_row_node(
        &engine,
        "PipelineCollectElement",
        "b",
        &[("name", PropValue::String("b".to_string()))],
    );
    let edge = insert_graph_row_edge(
        &engine,
        a,
        b,
        "PIPELINE_COLLECT_ELEMENT",
        &[("rank", PropValue::Int(1))],
    );

    let mut start = graph_node_with_label("a", "PipelineCollectElement");
    start.ids = vec![a];
    let mut end = graph_node_with_label("b", "PipelineCollectElement");
    end.ids = vec![b];
    let mut path = graph_vlp(Some("p"), Some("r"), "a", "b", 1, 1);
    if let GraphPatternPiece::VariableLength(path) = &mut path {
        path.label_filter = vec!["PIPELINE_COLLECT_ELEMENT".to_string()];
    }
    let query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![start, end],
                pieces: vec![path],
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Collect,
                            distinct: false,
                            arg: Some(Box::new(GraphExpr::Binding("a".to_string()))),
                        },
                        alias: Some("nodes".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Collect,
                            distinct: false,
                            arg: Some(Box::new(GraphExpr::Binding("r".to_string()))),
                        },
                        alias: Some("edges".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::AggregateCall {
                            function: GraphAggregateFunction::Collect,
                            distinct: false,
                            arg: Some(Box::new(GraphExpr::Binding("p".to_string()))),
                        },
                        alias: Some("paths".to_string()),
                        projection: GraphReturnProjection::Auto,
                    },
                ]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            include_vectors: false,
            compact_rows: false,
        },
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };

    let result = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0].values;

    let GraphValue::List(nodes) = &row[0] else {
        panic!("expected collected nodes");
    };
    let GraphValue::Node(node) = &nodes[0] else {
        panic!("expected collected node element");
    };
    assert_eq!(node.id, Some(a));
    assert_eq!(node.key.as_deref(), Some("a"));
    assert_eq!(
        node.props.as_ref().unwrap().get("name"),
        Some(&GraphValue::String("a".to_string()))
    );

    let GraphValue::List(edges) = &row[1] else {
        panic!("expected collected edges");
    };
    let GraphValue::Edge(collected_edge) = &edges[0] else {
        panic!("expected collected edge element");
    };
    assert_eq!(collected_edge.id, Some(edge));
    assert_eq!(
        collected_edge.label.as_deref(),
        Some("PIPELINE_COLLECT_ELEMENT")
    );
    assert_eq!(
        collected_edge.props.as_ref().unwrap().get("rank"),
        Some(&GraphValue::Int(1))
    );

    let GraphValue::List(paths) = &row[2] else {
        panic!("expected collected paths");
    };
    let GraphValue::Path(path) = &paths[0] else {
        panic!("expected collected path element");
    };
    assert_eq!(path.node_ids, vec![a, b]);
    assert_eq!(path.edge_ids, vec![edge]);
    assert_eq!(path.nodes.as_ref().unwrap()[0].key.as_deref(), Some("a"));
    assert_eq!(
        path.edges.as_ref().unwrap()[0].label.as_deref(),
        Some("PIPELINE_COLLECT_ELEMENT")
    );
}

#[test]
fn graph_pipeline_seeded_bound_node_alias_verifies_later_match_constraints() {
    let (_dir, engine) = graph_row_test_engine();
    let active = insert_graph_row_node_with_labels(
        &engine,
        &["PipelineSeedSource", "PipelineSeedRequired"],
        "active",
        &[("status", PropValue::String("active".to_string()))],
    );
    let inactive = insert_graph_row_node_with_labels(
        &engine,
        &["PipelineSeedSource"],
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
    );
    let active_target = insert_graph_row_node(&engine, "PipelineSeedTarget", "active-target", &[]);
    let inactive_target =
        insert_graph_row_node(&engine, "PipelineSeedTarget", "inactive-target", &[]);
    insert_graph_row_edge(
        &engine,
        active,
        active_target,
        "PIPELINE_SEED_REQUIRED_REL",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        inactive,
        inactive_target,
        "PIPELINE_SEED_REQUIRED_REL",
        &[],
    );

    let query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineSeedSource")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("n".to_string()),
                    alias: Some("n".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![GraphNodePattern {
                    alias: "n".to_string(),
                    label_filter: Some(NodeLabelFilter {
                        labels: vec!["PipelineSeedRequired".to_string()],
                        mode: LabelMatchMode::All,
                    }),
                    ids: Vec::new(),
                    keys: Vec::new(),
                    filter: Some(NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    }),
                }, graph_node_with_label("m", "PipelineSeedTarget")],
                pieces: vec![graph_edge_with_label(
                    Some("r"),
                    "n",
                    "m",
                    "PIPELINE_SEED_REQUIRED_REL",
                )],
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("n".to_string()),
                    alias: Some("n".to_string()),
                    projection: GraphReturnProjection::IdOnly,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };

    assert_eq!(
        graph_pipeline_value_rows(engine.query_graph_pipeline(&query).unwrap()),
        vec![vec![GraphValue::NodeId(active)]]
    );

    let mut optional_query = query;
    if let GraphPipelineStage::Match(stage) = &mut optional_query.stages[2] {
        stage.optional = true;
    }
    if let GraphPipelineStage::Project(stage) = &mut optional_query.stages[3] {
        stage.items = GraphProjectionItems::Items(vec![
            GraphProjectItem {
                expr: GraphExpr::Binding("n".to_string()),
                alias: Some("n".to_string()),
                projection: GraphReturnProjection::IdOnly,
            },
            GraphProjectItem {
                expr: GraphExpr::Binding("m".to_string()),
                alias: Some("m".to_string()),
                projection: GraphReturnProjection::IdOnly,
            },
        ]);
        stage.order_by = vec![GraphOrderItem {
            expr: GraphExpr::NodeField {
                alias: "n".to_string(),
                field: GraphNodeField::Id,
            },
            direction: GraphOrderDirection::Asc,
        }];
    }
    assert_eq!(
        graph_pipeline_value_rows(engine.query_graph_pipeline(&optional_query).unwrap()),
        vec![
            vec![GraphValue::NodeId(active), GraphValue::NodeId(active_target)],
            vec![GraphValue::NodeId(inactive), GraphValue::Null]
        ]
    );
}

#[test]
fn graph_pipeline_cursor_preserves_scalar_only_duplicate_rows() {
    let (_dir, engine) = graph_row_test_engine();
    for key in ["a", "b", "c"] {
        insert_graph_row_node(
            &engine,
            "PipelineCursorDup",
            key,
            &[("name", PropValue::String("same".to_string()))],
        );
    }
    let mut query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineCursorDup")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: graph_prop("n", "name"),
                    alias: Some("name".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Int(1),
                    alias: Some("one".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: vec![GraphOrderItem {
                    expr: GraphExpr::Binding("one".to_string()),
                    direction: GraphOrderDirection::Asc,
                }],
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 1,
            limit: 1,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            max_skip: 1,
            ..GraphPipelineOptions::default()
        },
    };

    let first = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(graph_pipeline_value_rows(first.clone()), vec![vec![GraphValue::Int(1)]]);
    let cursor = first.next_cursor.expect("duplicate scalar page should continue");

    query.page.skip = 0;
    query.page.cursor = Some(cursor.clone());
    let second = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(
        graph_pipeline_value_rows(second.clone()),
        vec![vec![GraphValue::Int(1)]]
    );
    assert!(second.next_cursor.is_none());

    let mut lowered_skip_cap = query.clone();
    lowered_skip_cap.options.max_skip = 0;
    assert_graph_pipeline_invalid(
        &engine,
        &lowered_skip_cap,
        "original skip 1 exceeds max_skip 0",
    );

    let mut wrong_sort_shape = query.clone();
    wrong_sort_shape.page.cursor = Some(tampered_pipeline_cursor_sort_key(cursor.clone()));
    assert_graph_pipeline_invalid(&engine, &wrong_sort_shape, "cursor sort key has");

    let mut wrong_logical_shape = query.clone();
    wrong_logical_shape.page.cursor = Some(tampered_pipeline_cursor_logical_key(cursor.clone()));
    assert_graph_pipeline_invalid(&engine, &wrong_logical_shape, "cursor logical row key has");

    let mut wrong_internal_key_shape = query;
    wrong_internal_key_shape.page.cursor = Some(tampered_pipeline_cursor_internal_key_atom(cursor));
    engine.reset_query_execution_counters_for_test();
    assert_graph_pipeline_invalid(
        &engine,
        &wrong_internal_key_shape,
        "internal cursor key atom",
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_query_calls, 0);
}

#[test]
fn graph_pipeline_enforces_pipeline_rows_and_cursor_skip_caps() {
    let (_dir, engine) = graph_row_test_engine();
    for key in ["a", "b", "c", "d"] {
        insert_graph_row_node(
            &engine,
            "PipelineCaps",
            key,
            &[("name", PropValue::String(key.to_string()))],
        );
    }
    let graph_query = GraphRowQuery {
        nodes: vec![graph_node_with_label("n", "PipelineCaps")],
        pieces: Vec::new(),
        where_: None,
        return_items: Some(vec![graph_return_expr(graph_prop("n", "name"), "name")]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 2,
            cursor: None,
        },
        at_epoch: Some(now_millis()),
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            ..GraphQueryOptions::default()
        },
    };
    let mut capped = graph_pipeline_from_row_query(&graph_query);
    capped.options.max_pipeline_rows = 1;
    assert_graph_pipeline_invalid(&engine, &capped, "max_intermediate_bindings");

    let mut first_page = graph_pipeline_from_row_query(&graph_query);
    first_page.page.skip = 2;
    first_page.page.limit = 1;
    first_page.options.max_skip = 2;
    let first = engine.query_graph_pipeline(&first_page).unwrap();
    assert!(first.next_cursor.is_some());

    let mut resume = first_page;
    resume.page.skip = 0;
    resume.page.cursor = first.next_cursor;
    resume.options.max_skip = 1;
    assert_graph_pipeline_invalid(&engine, &resume, "original skip 2 exceeds max_skip 1");

    let mut oversized_cursor = graph_pipeline_from_row_query(&graph_query);
    oversized_cursor.options.max_cursor_bytes = 4;
    oversized_cursor.page.cursor = Some(format!(
        "{GRAPH_PIPELINE_CURSOR_PREFIX}{}",
        "A".repeat(32)
    ));
    let err = engine.query_graph_pipeline(&oversized_cursor).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
    assert!(
        err.to_string()
            .contains("too large to decode within max_cursor_bytes 4"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_pipeline_validates_referenced_param_byte_caps() {
    let (_dir, engine) = graph_row_test_engine();
    let graph_query = GraphRowQuery {
        nodes: vec![graph_node_with_label("n", "PipelineParamCaps")],
        pieces: Vec::new(),
        where_: None,
        return_items: Some(vec![graph_return_binding(
            "n",
            GraphReturnProjection::IdOnly,
        )]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        at_epoch: Some(now_millis()),
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            ..GraphQueryOptions::default()
        },
    };
    let mut query = graph_pipeline_from_row_query(&graph_query);
    if let GraphPipelineStage::Project(project) = &mut query.stages[1] {
        project.items = GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Param("needle".to_string()),
            alias: Some("needle".to_string()),
            projection: GraphReturnProjection::Auto,
        }]);
    }
    query.options.max_param_bytes = 4;
    query
        .params
        .insert("needle".to_string(), GraphParamValue::String("too-long".to_string()));
    query.params.insert(
        "unused".to_string(),
        GraphParamValue::String("also-too-long-but-unreferenced".to_string()),
    );
    assert_graph_pipeline_invalid(&engine, &query, "exceeding max_param_bytes 4");

    query
        .params
        .insert("needle".to_string(), GraphParamValue::String("ok".to_string()));
    let result = engine.query_graph_pipeline(&query).unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn graph_pipeline_explain_reports_stage_shell_and_caps() {
    let (_dir, engine) = graph_row_test_engine();
    insert_graph_row_node(
        &engine,
        "PipelineExplain",
        "ada",
        &[("name", PropValue::String("Ada".to_string()))],
    );
    let mut graph_query = GraphRowQuery {
        nodes: vec![graph_node_with_label("n", "PipelineExplain")],
        pieces: Vec::new(),
        where_: None,
        return_items: Some(vec![graph_return_expr(graph_prop("n", "name"), "name")]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        at_epoch: Some(now_millis()),
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            ..GraphQueryOptions::default()
        },
    };
    graph_query.options.include_plan = true;
    let mut pipeline_query = graph_pipeline_from_row_query(&graph_query);
    pipeline_query.options.max_pipeline_rows = 123;
    pipeline_query.options.max_groups = 45;
    pipeline_query.options.max_collect_items = 67;
    pipeline_query.options.max_union_branches = 3;
    pipeline_query.options.max_subquery_invocations = 89;
    pipeline_query.options.max_subquery_depth = 1;
    pipeline_query.options.max_shortest_path_pairs = 21;

    let explain = engine.explain_graph_pipeline(&pipeline_query).unwrap();
    assert_eq!(explain.columns, vec!["name"]);
    assert_eq!(explain.stages.len(), 2);
    assert_eq!(explain.stages[0].kind, "Match");
    assert!(explain.stages[0].graph_row.is_some());
    assert_eq!(explain.stages[1].kind, "Project(Return)");
    assert_eq!(explain.stages[1].columns, vec!["name"]);
    assert_eq!(explain.caps.max_pipeline_rows, 123);
    assert_eq!(explain.caps.max_groups, 45);
    assert_eq!(explain.caps.max_collect_items, 67);
    assert_eq!(explain.caps.max_union_branches, 3);
    assert_eq!(explain.caps.max_subquery_invocations, 89);
    assert_eq!(explain.caps.max_subquery_depth, 1);
    assert_eq!(explain.caps.max_shortest_path_pairs, 21);
    assert_eq!(explain.stats.rows_entered_pipeline, 1);
    assert!(!explain
        .notes
        .iter()
        .any(|note| note.contains("CP34.1 supports only")));

    let native_pipeline = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineExplain")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: graph_prop("n", "name"),
                    alias: Some("name".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("name".to_string()),
                    alias: Some("name".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: graph_query.at_epoch,
        page: graph_query.page.clone(),
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            include_plan: true,
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    engine.reset_query_execution_counters_for_test();
    let native_explain = engine.explain_graph_pipeline(&native_pipeline).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_query_calls, 0);
    assert_eq!(
        native_explain
            .stages
            .iter()
            .map(|stage| stage.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["Match", "Project(With)", "Project(Return)"]
    );
    assert!(native_explain.stages[0].graph_row.is_some());
    assert!(native_explain.stages[0]
        .detail
        .contains("seeded_node_aliases="));
    assert!(!native_explain.stages[0].detail.contains("seeded_aliases="));
    assert!(native_explain.stages[1]
        .notes
        .iter()
        .any(|note| note.contains("created scalar aliases: name")));
    assert!(native_explain.stages[1]
        .notes
        .iter()
        .any(|note| note.contains("scalar expressions: name :=")));

    let carried_pipeline = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineExplain")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("n".to_string()),
                    alias: Some("n".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("m", "PipelineExplain")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("m".to_string()),
                    alias: Some("m".to_string()),
                    projection: GraphReturnProjection::IdOnly,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: graph_query.at_epoch,
        page: graph_query.page.clone(),
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            include_plan: true,
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    let carried_explain = engine.explain_graph_pipeline(&carried_pipeline).unwrap();
    assert!(carried_explain.stages[2]
        .detail
        .contains("seeded_node_aliases=; carried_aliases=n"));

    let seeded_pipeline = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineExplain")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("n".to_string()),
                    alias: Some("n".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![
                    graph_node("n"),
                    graph_node_with_label("m", "PipelineExplain"),
                ],
                pieces: vec![graph_edge_with_label(
                    Some("r"),
                    "n",
                    "m",
                    "PIPELINE_EXPLAIN_REL",
                )],
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("m".to_string()),
                    alias: Some("m".to_string()),
                    projection: GraphReturnProjection::IdOnly,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: graph_query.at_epoch,
        page: graph_query.page.clone(),
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            include_plan: true,
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    let seeded_explain = engine.explain_graph_pipeline(&seeded_pipeline).unwrap();
    assert!(seeded_explain.stages[2]
        .detail
        .contains("seeded_node_aliases=n; carried_aliases="));
}

#[test]
fn graph_pipeline_shortest_path_stage_executes_and_reports_stats() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "PipelineShortest", "a", &[]);
    let b = insert_graph_row_node(&engine, "PipelineShortest", "b", &[]);
    let c = insert_graph_row_node(&engine, "PipelineShortest", "c", &[]);
    let ab = engine
        .upsert_edge(a, b, "PIPELINE_SHORTEST", UpsertEdgeOptions::default())
        .unwrap();
    let bc = engine
        .upsert_edge(b, c, "PIPELINE_SHORTEST", UpsertEdgeOptions::default())
        .unwrap();

    let query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::ShortestPath(GraphShortestPathStage {
                optional: false,
                output_path_alias: "p".to_string(),
                mode: GraphShortestPathMode::One,
                from: GraphShortestPathEndpoint::NodeId(a),
                to: GraphShortestPathEndpoint::NodeId(c),
                direction: Direction::Outgoing,
                edge_label_filter: vec!["PIPELINE_SHORTEST".to_string()],
                min_hops: 1,
                max_hops: 4,
                weight_field: None,
                max_cost: None,
                max_paths: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("p".to_string()),
                    alias: Some("p".to_string()),
                    projection: GraphReturnProjection::Element(GraphElementProjection::Full),
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            include_plan: true,
            ..GraphPipelineOptions::default()
        },
    };

    let result = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(result.stats.shortest_path_pairs, 1);
    assert_eq!(result.stats.shortest_path_cache_hits, 0);
    let rows = graph_pipeline_value_rows(result.clone());
    let GraphValue::Path(path) = &rows[0][0] else {
        panic!("expected path output");
    };
    assert_eq!(path.node_ids, vec![a, b, c]);
    assert_eq!(path.edge_ids, vec![ab, bc]);
    let plan = result.plan.expect("include_plan should attach explain");
    assert!(plan.stages.iter().any(|stage| {
        stage.kind == "ShortestPath"
            && stage.detail.contains("algorithm=bidirectional_bfs")
            && stage.detail.contains("distinct_pair_count=1")
            && stage.detail.contains("emitted_path_count=1")
    }));
}

#[test]
fn graph_pipeline_shortest_path_node_key_endpoints_use_cached_id_resolution() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "PipelineShortestKey", "a", &[]);
    let b = insert_graph_row_node(&engine, "PipelineShortestKey", "b", &[]);
    let c = insert_graph_row_node(&engine, "PipelineShortestKey", "c", &[]);
    insert_graph_row_node(&engine, "PipelineShortestKeyDup", "dup-1", &[]);
    insert_graph_row_node(&engine, "PipelineShortestKeyDup", "dup-2", &[]);
    let ab = engine
        .upsert_edge(a, b, "PIPELINE_SHORTEST_KEY", UpsertEdgeOptions::default())
        .unwrap();
    let bc = engine
        .upsert_edge(b, c, "PIPELINE_SHORTEST_KEY", UpsertEdgeOptions::default())
        .unwrap();

    let query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("d", "PipelineShortestKeyDup")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::ShortestPath(GraphShortestPathStage {
                optional: false,
                output_path_alias: "p".to_string(),
                mode: GraphShortestPathMode::One,
                from: GraphShortestPathEndpoint::NodeKey {
                    label: "PipelineShortestKey".to_string(),
                    key: "a".to_string(),
                },
                to: GraphShortestPathEndpoint::NodeKey {
                    label: "PipelineShortestKey".to_string(),
                    key: "c".to_string(),
                },
                direction: Direction::Outgoing,
                edge_label_filter: vec!["PIPELINE_SHORTEST_KEY".to_string()],
                min_hops: 1,
                max_hops: 4,
                weight_field: None,
                max_cost: None,
                max_paths: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("p".to_string()),
                    alias: Some("p".to_string()),
                    projection: GraphReturnProjection::IdOnly,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            include_plan: true,
            allow_full_scan: true,
            ..GraphPipelineOptions::default()
        },
    };

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_pipeline(&query).unwrap();
    assert_eq!(result.stats.shortest_path_pairs, 1);
    assert_eq!(result.stats.shortest_path_cache_hits, 1);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);

    let rows = graph_pipeline_value_rows(result.clone());
    assert_eq!(rows.len(), 2);
    for row in rows {
        let GraphValue::Path(path) = &row[0] else {
            panic!("expected path output");
        };
        assert_eq!(path.node_ids, vec![a, b, c]);
        assert_eq!(path.edge_ids, vec![ab, bc]);
    }
}

#[test]
fn graph_pipeline_union_executes_all_and_distinct_with_stats() {
    let (_dir, engine) = graph_row_test_engine();
    for (key, side, name) in [
        ("a", "left", "a"),
        ("b", "left", "b"),
        ("b2", "right", "b"),
        ("c", "right", "c"),
    ] {
        insert_graph_row_node(
            &engine,
            "PipelineUnion",
            key,
            &[
                ("side", PropValue::String(side.to_string())),
                ("name", PropValue::String(name.to_string())),
            ],
        );
    }

    fn branch(side: &str, desc: bool) -> GraphPipelineQuery {
        let direction = if desc {
            GraphOrderDirection::Desc
        } else {
            GraphOrderDirection::Asc
        };
        GraphPipelineQuery {
            stages: vec![
                GraphPipelineStage::Match(GraphPipelineMatchStage {
                    optional: false,
                    nodes: vec![graph_node_with_label("n", "PipelineUnion")],
                    pieces: Vec::new(),
                    optional_candidate_where: None,
                    where_: Some(GraphExpr::Binary {
                        left: Box::new(graph_prop("n", "side")),
                        op: GraphBinaryOp::Eq,
                        right: Box::new(GraphExpr::String(side.to_string())),
                    }),
                }),
                GraphPipelineStage::Project(GraphProjectStage {
                    kind: GraphProjectKind::Return,
                    items: GraphProjectionItems::Items(vec![GraphProjectItem {
                        expr: graph_prop("n", "name"),
                        alias: Some("name".to_string()),
                        projection: GraphReturnProjection::Auto,
                    }]),
                    distinct: false,
                    where_: None,
                    order_by: vec![GraphOrderItem {
                        expr: GraphExpr::Binding("name".to_string()),
                        direction,
                    }],
                    skip: None,
                    limit: None,
                }),
            ],
            params: BTreeMap::new(),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: 10,
                cursor: None,
            },
            output: GraphOutputOptions::default(),
            options: GraphPipelineOptions {
                allow_full_scan: true,
                include_plan: true,
                ..GraphPipelineOptions::default()
            },
        }
    }

    let union_all = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![branch("left", true), branch("right", false)],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: true,
            include_plan: true,
            ..GraphPipelineOptions::default()
        },
    };
    let all = engine.query_graph_pipeline(&union_all).unwrap();
    assert_eq!(
        all.rows
            .iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            GraphValue::String("b".to_string()),
            GraphValue::String("a".to_string()),
            GraphValue::String("b".to_string()),
            GraphValue::String("c".to_string()),
        ]
    );
    assert_eq!(all.stats.union_branches, 2);
    assert_eq!(all.stats.union_dedup_keys, 0);
    let plan = all.plan.as_ref().unwrap();
    assert_eq!(plan.stages[0].kind, "UnionAll");
    assert!(plan.stages[0].detail.contains("branches=2"));
    assert!(plan.stages[0]
        .notes
        .iter()
        .any(|note| note.contains("branch 1 stages: Match")));
    assert!(plan.stages[0]
        .notes
        .iter()
        .any(|note| note.contains("branch 2 row op: Sort")));

    let mut dedupe = union_all.clone();
    if let GraphPipelineStage::Union(stage) = &mut dedupe.stages[0] {
        stage.all = false;
    }
    let distinct = engine.query_graph_pipeline(&dedupe).unwrap();
    assert_eq!(
        distinct
            .rows
            .iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            GraphValue::String("b".to_string()),
            GraphValue::String("a".to_string()),
            GraphValue::String("c".to_string()),
        ]
    );
    assert_eq!(distinct.stats.union_branches, 2);
    assert_eq!(distinct.stats.union_dedup_keys, 3);

    let source = insert_graph_row_node(&engine, "PipelineUnionEpochNode", "source", &[]);
    let past = insert_graph_row_node(&engine, "PipelineUnionEpochNode", "past", &[]);
    let future = insert_graph_row_node(&engine, "PipelineUnionEpochNode", "future", &[]);
    engine
        .upsert_edge(
            source,
            past,
            "PipelineUnionEpochEdge",
            UpsertEdgeOptions {
                props: graph_row_props(&[
                    ("side", PropValue::String("past".to_string())),
                    ("name", PropValue::String("past".to_string())),
                ]),
                valid_from: Some(100),
                valid_to: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            future,
            "PipelineUnionEpochEdge",
            UpsertEdgeOptions {
                props: graph_row_props(&[
                    ("side", PropValue::String("future".to_string())),
                    ("name", PropValue::String("future".to_string())),
                ]),
                valid_from: Some(300),
                valid_to: None,
                ..Default::default()
            },
        )
        .unwrap();
    fn epoch_branch(side: &str) -> GraphPipelineQuery {
        GraphPipelineQuery {
            stages: vec![
                GraphPipelineStage::Match(GraphPipelineMatchStage {
                    optional: false,
                    nodes: vec![graph_node("source"), graph_node("target")],
                    pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                        alias: Some("r".to_string()),
                        from_alias: "source".to_string(),
                        to_alias: "target".to_string(),
                        direction: Direction::Outgoing,
                        label_filter: vec!["PipelineUnionEpochEdge".to_string()],
                        filter: None,
                    })],
                    optional_candidate_where: None,
                    where_: Some(GraphExpr::Binary {
                        left: Box::new(graph_prop("r", "side")),
                        op: GraphBinaryOp::Eq,
                        right: Box::new(GraphExpr::String(side.to_string())),
                    }),
                }),
                GraphPipelineStage::Project(GraphProjectStage {
                    kind: GraphProjectKind::Return,
                    items: GraphProjectionItems::Items(vec![GraphProjectItem {
                        expr: graph_prop("r", "name"),
                        alias: Some("name".to_string()),
                        projection: GraphReturnProjection::Auto,
                    }]),
                    distinct: false,
                    where_: None,
                    order_by: Vec::new(),
                    skip: None,
                    limit: None,
                }),
            ],
            params: BTreeMap::new(),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: 10,
                cursor: None,
            },
            output: GraphOutputOptions::default(),
            options: GraphPipelineOptions {
                allow_full_scan: true,
                ..GraphPipelineOptions::default()
            },
        }
    }
    let epoch_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![epoch_branch("past"), epoch_branch("future")],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: Some(150),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let snapshot = engine.query_graph_pipeline(&epoch_union).unwrap();
    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![GraphValue::String("past".to_string())]
    );

    let nullable_source = insert_graph_row_node(&engine, "PipelineUnionNullable", "source", &[]);
    let nullable_missing =
        insert_graph_row_node(&engine, "PipelineUnionNullable", "missing", &[]);
    let nullable_target = insert_graph_row_node(&engine, "PipelineUnionNullable", "target", &[]);
    insert_graph_row_edge(
        &engine,
        nullable_source,
        nullable_target,
        "PipelineUnionNullableEdge",
        &[],
    );
    fn nullable_branch(source_id: u64, optional: bool) -> GraphPipelineQuery {
        let mut source = graph_node_with_label("source", "PipelineUnionNullable");
        source.ids = vec![source_id];
        let edge = graph_edge_with_label(
            Some("r"),
            "source",
            "item",
            "PipelineUnionNullableEdge",
        );
        let pieces = if optional {
            vec![graph_optional(vec![edge], None)]
        } else {
            vec![edge]
        };
        GraphPipelineQuery {
            stages: vec![
                GraphPipelineStage::Match(GraphPipelineMatchStage {
                    optional: false,
                    nodes: vec![
                        source,
                        graph_node_with_label("item", "PipelineUnionNullable"),
                    ],
                    pieces,
                    optional_candidate_where: None,
                    where_: None,
                }),
                GraphPipelineStage::Project(GraphProjectStage {
                    kind: GraphProjectKind::Return,
                    items: GraphProjectionItems::Items(vec![GraphProjectItem {
                        expr: GraphExpr::Binding("item".to_string()),
                        alias: Some("item".to_string()),
                        projection: GraphReturnProjection::IdOnly,
                    }]),
                    distinct: false,
                    where_: None,
                    order_by: Vec::new(),
                    skip: None,
                    limit: None,
                }),
            ],
            params: BTreeMap::new(),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: 10,
                cursor: None,
            },
            output: GraphOutputOptions::default(),
            options: GraphPipelineOptions::default(),
        }
    }
    let nullable_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![
                nullable_branch(nullable_source, false),
                nullable_branch(nullable_missing, true),
            ],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let nullable = engine.query_graph_pipeline(&nullable_union).unwrap();
    assert_eq!(
        nullable
            .rows
            .iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![GraphValue::NodeId(nullable_target), GraphValue::Null]
    );

    let mixed_node = insert_graph_row_node(
        &engine,
        "PipelineUnionMixed",
        "node",
        &[("name", PropValue::String("node".to_string()))],
    );
    let mixed_node_two = insert_graph_row_node(
        &engine,
        "PipelineUnionMixed",
        "node-two",
        &[("name", PropValue::String("node-two".to_string()))],
    );
    let mixed_scalar_branch = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Project(GraphProjectStage {
            kind: GraphProjectKind::Return,
            items: GraphProjectionItems::Items(vec![GraphProjectItem {
                expr: GraphExpr::String("literal".to_string()),
                alias: Some("value".to_string()),
                projection: GraphReturnProjection::Auto,
            }]),
            distinct: false,
            where_: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let mixed_node_branch = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![GraphNodePattern {
                    alias: "n".to_string(),
                    label_filter: Some(NodeLabelFilter {
                        labels: vec!["PipelineUnionMixed".to_string()],
                        mode: LabelMatchMode::All,
                    }),
                    ids: vec![mixed_node, mixed_node_two],
                    keys: Vec::new(),
                    filter: None,
                }],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("n".to_string()),
                    alias: Some("value".to_string()),
                    projection: GraphReturnProjection::Element(GraphElementProjection::Full),
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let mixed_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![mixed_scalar_branch.clone(), mixed_node_branch],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let mixed = engine.query_graph_pipeline(&mixed_union).unwrap();
    assert_eq!(mixed.rows[0].values[0], GraphValue::String("literal".to_string()));
    match &mixed.rows[1].values[0] {
        GraphValue::Node(node) => assert_eq!(node.id, Some(mixed_node)),
        other => panic!("expected mixed union node output, got {other:?}"),
    }
    let mut paged_mixed_all = mixed_union.clone();
    paged_mixed_all.page.limit = 2;
    paged_mixed_all.options.max_rows = 2;
    let paged_all_first = engine.query_graph_pipeline(&paged_mixed_all).unwrap();
    assert_eq!(paged_all_first.rows.len(), 2);
    assert!(paged_all_first.next_cursor.is_some());
    let paged_all_second = engine
        .query_graph_pipeline(&GraphPipelineQuery {
            page: GraphPageRequest {
                cursor: paged_all_first.next_cursor.clone(),
                ..paged_mixed_all.page.clone()
            },
            ..paged_mixed_all.clone()
        })
        .unwrap();
    assert_eq!(paged_all_second.rows.len(), 1);
    match &paged_all_second.rows[0].values[0] {
        GraphValue::Node(node) => assert_eq!(node.id, Some(mixed_node_two)),
        other => panic!("expected second mixed cursor page node output, got {other:?}"),
    }
    let mut paged_mixed_dedupe = paged_mixed_all.clone();
    if let GraphPipelineStage::Union(stage) = &mut paged_mixed_dedupe.stages[0] {
        stage.all = false;
    }
    let paged_dedupe_first = engine.query_graph_pipeline(&paged_mixed_dedupe).unwrap();
    assert_eq!(paged_dedupe_first.rows.len(), 2);
    assert!(paged_dedupe_first.next_cursor.is_some());
    let paged_dedupe_second = engine
        .query_graph_pipeline(&GraphPipelineQuery {
            page: GraphPageRequest {
                cursor: paged_dedupe_first.next_cursor.clone(),
                ..paged_mixed_dedupe.page.clone()
            },
            ..paged_mixed_dedupe.clone()
        })
        .unwrap();
    assert_eq!(paged_dedupe_second.rows.len(), 1);
    match &paged_dedupe_second.rows[0].values[0] {
        GraphValue::Node(node) => assert_eq!(node.id, Some(mixed_node_two)),
        other => panic!("expected second mixed dedupe cursor page node output, got {other:?}"),
    }

    let selected_node_id = insert_graph_row_node(
        &engine,
        "PipelineUnionProjection",
        "selected",
        &[
            ("visible", PropValue::String("yes".to_string())),
            ("hidden", PropValue::String("no".to_string())),
        ],
    );
    let full_node_id = insert_graph_row_node(
        &engine,
        "PipelineUnionProjection",
        "full",
        &[
            ("visible", PropValue::String("full".to_string())),
            ("hidden", PropValue::String("full-hidden".to_string())),
        ],
    );
    let compact_node_id =
        insert_graph_row_node(&engine, "PipelineUnionProjection", "compact", &[]);
    let selected_projection = GraphReturnProjection::Selected(GraphSelectedProjection::Node(
        GraphSelectedNodeProjection {
            id: true,
            labels: false,
            key: false,
            props: GraphPropertySelection::Keys(vec!["visible".to_string()]),
            weight: false,
            created_at: false,
            updated_at: false,
            vectors: GraphVectorSelection::None,
        },
    ));
    fn projection_branch(node_id: u64, projection: GraphReturnProjection) -> GraphPipelineQuery {
        GraphPipelineQuery {
            stages: vec![
                GraphPipelineStage::Match(GraphPipelineMatchStage {
                    optional: false,
                    nodes: vec![GraphNodePattern {
                        alias: "n".to_string(),
                        label_filter: Some(NodeLabelFilter {
                            labels: vec!["PipelineUnionProjection".to_string()],
                            mode: LabelMatchMode::All,
                        }),
                        ids: vec![node_id],
                        keys: Vec::new(),
                        filter: None,
                    }],
                    pieces: Vec::new(),
                    optional_candidate_where: None,
                    where_: None,
                }),
                GraphPipelineStage::Project(GraphProjectStage {
                    kind: GraphProjectKind::Return,
                    items: GraphProjectionItems::Items(vec![GraphProjectItem {
                        expr: GraphExpr::Binding("n".to_string()),
                        alias: Some("value".to_string()),
                        projection,
                    }]),
                    distinct: false,
                    where_: None,
                    order_by: Vec::new(),
                    skip: None,
                    limit: None,
                }),
            ],
            params: BTreeMap::new(),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: 10,
                cursor: None,
            },
            output: GraphOutputOptions::default(),
            options: GraphPipelineOptions::default(),
        }
    }
    let selected_scalar_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![
                projection_branch(selected_node_id, selected_projection.clone()),
                mixed_scalar_branch,
            ],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let selected_scalar = engine.query_graph_pipeline(&selected_scalar_union).unwrap();
    match &selected_scalar.rows[0].values[0] {
        GraphValue::Node(node) => {
            assert_eq!(node.id, Some(selected_node_id));
            assert!(node.labels.is_none());
            assert!(node.key.is_none());
            assert_eq!(
                node.props.as_ref().and_then(|props| props.get("visible")),
                Some(&GraphValue::String("yes".to_string()))
            );
            assert!(!node
                .props
                .as_ref()
                .is_some_and(|props| props.contains_key("hidden")));
        }
        other => panic!("expected selected node output, got {other:?}"),
    }
    assert_eq!(
        selected_scalar.rows[1].values[0],
        GraphValue::String("literal".to_string())
    );

    let selected_full_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![
                projection_branch(selected_node_id, selected_projection.clone()),
                projection_branch(
                    full_node_id,
                    GraphReturnProjection::Element(GraphElementProjection::Full),
                ),
            ],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    engine.reset_query_execution_counters_for_test();
    let selected_full = engine.query_graph_pipeline(&selected_full_union).unwrap();
    let selected_full_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(selected_full_counters.node_selected_field_batches, 2);
    assert_eq!(selected_full_counters.node_selected_field_ids, 2);
    match &selected_full.rows[0].values[0] {
        GraphValue::Node(node) => {
            assert_eq!(node.id, Some(selected_node_id));
            assert!(node.labels.is_none());
            assert!(node.key.is_none());
            assert!(!node
                .props
                .as_ref()
                .is_some_and(|props| props.contains_key("hidden")));
        }
        other => panic!("expected selected node output, got {other:?}"),
    }
    match &selected_full.rows[1].values[0] {
        GraphValue::Node(node) => {
            assert_eq!(node.id, Some(full_node_id));
            assert!(node.labels.is_some());
            assert!(node.key.is_some());
            assert!(node
                .props
                .as_ref()
                .is_some_and(|props| props.contains_key("hidden")));
        }
        other => panic!("expected full node output, got {other:?}"),
    }

    let selected_compact_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![
                projection_branch(selected_node_id, selected_projection),
                projection_branch(
                    compact_node_id,
                    GraphReturnProjection::Element(GraphElementProjection::Compact),
                ),
            ],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions::default(),
    };
    let selected_compact = engine.query_graph_pipeline(&selected_compact_union).unwrap();
    match &selected_compact.rows[1].values[0] {
        GraphValue::Node(node) => {
            assert_eq!(node.id, Some(compact_node_id));
            assert!(node.labels.is_some());
            assert!(node.key.is_some());
            assert!(node.props.is_none());
        }
        other => panic!("expected compact node output, got {other:?}"),
    }

    fn full_scan_branch() -> GraphPipelineQuery {
        GraphPipelineQuery {
            stages: vec![
                GraphPipelineStage::Match(GraphPipelineMatchStage {
                    optional: false,
                    nodes: vec![graph_node("n")],
                    pieces: Vec::new(),
                    optional_candidate_where: None,
                    where_: None,
                }),
                GraphPipelineStage::Project(GraphProjectStage {
                    kind: GraphProjectKind::Return,
                    items: GraphProjectionItems::Items(vec![GraphProjectItem {
                        expr: GraphExpr::Binding("n".to_string()),
                        alias: Some("id".to_string()),
                        projection: GraphReturnProjection::IdOnly,
                    }]),
                    distinct: false,
                    where_: None,
                    order_by: Vec::new(),
                    skip: None,
                    limit: Some(GraphExpr::UInt(1)),
                }),
            ],
            params: BTreeMap::new(),
            at_epoch: None,
            page: GraphPageRequest {
                skip: 0,
                limit: 10,
                cursor: None,
            },
            output: GraphOutputOptions::default(),
            options: GraphPipelineOptions::default(),
        }
    }
    let full_scan_union = GraphPipelineQuery {
        stages: vec![GraphPipelineStage::Union(GraphUnionStage {
            branches: vec![full_scan_branch(), full_scan_branch()],
            all: true,
        })],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: true,
            ..GraphPipelineOptions::default()
        },
    };
    let full_scan_explain = engine.explain_graph_pipeline(&full_scan_union).unwrap();
    assert!(full_scan_explain
        .warnings
        .iter()
        .any(|warning| warning.contains("FullScanExplicitlyAllowed")));
    assert!(full_scan_explain.stages[0]
        .warnings
        .iter()
        .any(|warning| warning.contains("FullScanExplicitlyAllowed")));
    assert!(full_scan_explain.stages[0]
        .notes
        .iter()
        .any(|note| note.contains("branch 1 warning: FullScanExplicitlyAllowed")));
}

#[test]
fn graph_pipeline_rejects_cp34_1_deferred_shapes() {
    let (_dir, engine) = graph_row_test_engine();
    let base_graph = GraphRowQuery {
        nodes: vec![graph_node_with_label("n", "PipelineReject")],
        pieces: Vec::new(),
        where_: None,
        return_items: Some(vec![graph_return_binding(
            "n",
            GraphReturnProjection::IdOnly,
        )]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        at_epoch: Some(now_millis()),
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: true,
            ..GraphQueryOptions::default()
        },
    };
    let base = graph_pipeline_from_row_query(&base_graph);

    let mut only_match = base.clone();
    only_match.stages.truncate(1);
    assert_graph_pipeline_invalid(&engine, &only_match, "terminal Project(Return)");

    let mut only_project = base.clone();
    only_project.stages.remove(0);
    assert_graph_pipeline_invalid(&engine, &only_project, "unknown binding");

    let mut union = base.clone();
    union.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches: vec![base.clone()],
        all: false,
    })];
    assert_graph_pipeline_invalid(&engine, &union, "at least two");

    let mut union_branch_base = base.clone();
    union_branch_base.at_epoch = None;

    let mut column_count_mismatch = base.clone();
    let mut two_columns = union_branch_base.clone();
    if let GraphPipelineStage::Project(project) = &mut two_columns.stages[1] {
        project.items = GraphProjectionItems::Items(vec![
            GraphProjectItem {
                expr: GraphExpr::Binding("n".to_string()),
                alias: Some("n".to_string()),
                projection: GraphReturnProjection::IdOnly,
            },
            GraphProjectItem {
                expr: GraphExpr::UInt(1),
                alias: Some("extra".to_string()),
                projection: GraphReturnProjection::Auto,
            },
        ]);
    }
    column_count_mismatch.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches: vec![union_branch_base.clone(), two_columns],
        all: false,
    })];
    assert_graph_pipeline_invalid(&engine, &column_count_mismatch, "returns 2 column");

    let mut column_name_mismatch = base.clone();
    let mut renamed = union_branch_base.clone();
    if let GraphPipelineStage::Project(project) = &mut renamed.stages[1] {
        project.items = GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("other".to_string()),
            projection: GraphReturnProjection::IdOnly,
        }]);
    }
    column_name_mismatch.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches: vec![union_branch_base.clone(), renamed],
        all: false,
    })];
    assert_graph_pipeline_invalid(&engine, &column_name_mismatch, "columns");

    let mut branch_cap = base.clone();
    branch_cap.options.max_union_branches = 1;
    branch_cap.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches: vec![base.clone(), base.clone()],
        all: true,
    })];
    assert_graph_pipeline_invalid(&engine, &branch_cap, "max_union_branches");

    let mut branch_cursor = base.clone();
    branch_cursor.at_epoch = None;
    branch_cursor.page.cursor = Some("raw-branch-cursor".to_string());
    let mut cursor_union = base.clone();
    cursor_union.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches: vec![union_branch_base.clone(), branch_cursor],
        all: true,
    })];
    assert_graph_pipeline_invalid(&engine, &cursor_union, "raw cursor");

    let mut branch_skip = union_branch_base.clone();
    branch_skip.page.skip = 1;
    let mut skip_union = base.clone();
    skip_union.stages = vec![GraphPipelineStage::Union(GraphUnionStage {
        branches: vec![union_branch_base.clone(), branch_skip],
        all: true,
    })];
    assert_graph_pipeline_invalid(&engine, &skip_union, "public page skip");

    let mut reserved_alias = union_branch_base.clone();
    if let GraphPipelineStage::Project(project) = &mut reserved_alias.stages[1] {
        project.items = GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::UInt(1),
            alias: Some("__og_union_order".to_string()),
            projection: GraphReturnProjection::Auto,
        }]);
    }
    assert_graph_pipeline_invalid(&engine, &reserved_alias, "reserved internal alias");

    let mut call_collision = base.clone();
    call_collision.stages = vec![
        base.stages[0].clone(),
        GraphPipelineStage::Call(GraphSubqueryStage {
            query: Box::new(base.clone()),
            import_aliases: vec!["n".to_string()],
        }),
        base.stages[1].clone(),
    ];
    assert_graph_pipeline_invalid(&engine, &call_collision, "collides");

    let mut shortest_path = base.clone();
    let shortest_path_match = shortest_path.stages[0].clone();
    let shortest_path_return = shortest_path.stages[1].clone();
    shortest_path.stages = vec![
        shortest_path_match,
        GraphPipelineStage::ShortestPath(GraphShortestPathStage {
            optional: false,
            output_path_alias: "p".to_string(),
            mode: GraphShortestPathMode::One,
            from: GraphShortestPathEndpoint::Alias("a".to_string()),
            to: GraphShortestPathEndpoint::Alias("b".to_string()),
            direction: Direction::Outgoing,
            edge_label_filter: Vec::new(),
            min_hops: 1,
            max_hops: 2,
            weight_field: None,
            max_cost: None,
            max_paths: None,
        }),
        shortest_path_return,
    ];
    assert_graph_pipeline_invalid(&engine, &shortest_path, "endpoint alias");

    let mut extra_stage = base.clone();
    extra_stage.stages.push(extra_stage.stages[1].clone());
    assert_graph_pipeline_invalid(&engine, &extra_stage, "must be the final");

    let mut with_project = base.clone();
    if let GraphPipelineStage::Project(stage) = &mut with_project.stages[1] {
        stage.kind = GraphProjectKind::With;
    }
    assert_graph_pipeline_invalid(&engine, &with_project, "terminal Project(Return)");

    let alias_kind_conflict = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineReject")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: graph_prop("n", "name"),
                    alias: Some("n".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("n", "PipelineReject")],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("n".to_string()),
                    alias: Some("n".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(now_millis()),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: true,
            ..GraphPipelineOptions::default()
        },
    };
    assert_graph_pipeline_invalid(
        &engine,
        &alias_kind_conflict,
        "collides with an existing non-node alias",
    );
}

fn tampered_cursor_checksum(cursor: &str) -> String {
    let encoded = cursor.strip_prefix(GRAPH_ROW_CURSOR_PREFIX).unwrap();
    let mut bytes = base64url_no_pad_decode(encoded).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 0x01;
    format!("{GRAPH_ROW_CURSOR_PREFIX}{}", base64url_no_pad_encode(&bytes))
}

fn tampered_cursor_version(cursor: &str, offset: usize, value: u8) -> String {
    let encoded = cursor.strip_prefix(GRAPH_ROW_CURSOR_PREFIX).unwrap();
    let mut bytes = base64url_no_pad_decode(encoded).unwrap();
    bytes[offset] = value;
    let checksum_offset = bytes.len() - 8;
    let checksum = crate::types::fnv1a(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    format!("{GRAPH_ROW_CURSOR_PREFIX}{}", base64url_no_pad_encode(&bytes))
}

fn tampered_cursor_sort_atom_len(cursor: &str, len: u32) -> String {
    let encoded = cursor.strip_prefix(GRAPH_ROW_CURSOR_PREFIX).unwrap();
    let mut bytes = base64url_no_pad_decode(encoded).unwrap();
    let sort_key_len_offset = GRAPH_ROW_CURSOR_MAGIC.len() + 1 + 2 + 2 + 8 + 8 + 8 + (16 * 4);
    bytes[sort_key_len_offset..sort_key_len_offset + 4].copy_from_slice(&len.to_be_bytes());
    let checksum_offset = bytes.len() - 8;
    let checksum = crate::types::fnv1a(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    format!("{GRAPH_ROW_CURSOR_PREFIX}{}", base64url_no_pad_encode(&bytes))
}

fn tampered_cursor_sort_key_atom(
    cursor: &str,
    atom: GraphSortAtom,
) -> String {
    let mut payload = graph_row_decode_cursor(cursor, GraphQueryOptions::default().max_cursor_bytes)
        .unwrap();
    payload.last_sort_key = vec![atom];
    graph_row_encode_cursor(&payload, GraphQueryOptions::default().max_cursor_bytes).unwrap()
}

fn tampered_cursor_logical_key_atom(
    cursor: &str,
    index: usize,
    atom: GraphSortAtom,
) -> String {
    let mut payload = graph_row_decode_cursor(cursor, GraphQueryOptions::default().max_cursor_bytes)
        .unwrap();
    payload.last_logical_row_key[index] = atom;
    graph_row_encode_cursor(&payload, GraphQueryOptions::default().max_cursor_bytes).unwrap()
}

fn tampered_pipeline_cursor_sort_key(cursor: String) -> String {
    let mut payload =
        graph_pipeline_decode_logical_cursor(&cursor, GraphPipelineOptions::default().max_cursor_bytes)
            .unwrap();
    payload.last_sort_key.push(GraphSortAtom::Null);
    graph_pipeline_encode_logical_cursor(&payload, GraphPipelineOptions::default().max_cursor_bytes)
        .unwrap()
}

fn tampered_pipeline_cursor_logical_key(cursor: String) -> String {
    let mut payload =
        graph_pipeline_decode_logical_cursor(&cursor, GraphPipelineOptions::default().max_cursor_bytes)
            .unwrap();
    payload.last_logical_row_key.pop();
    graph_pipeline_encode_logical_cursor(&payload, GraphPipelineOptions::default().max_cursor_bytes)
        .unwrap()
}

fn tampered_pipeline_cursor_internal_key_atom(cursor: String) -> String {
    let mut payload =
        graph_pipeline_decode_logical_cursor(&cursor, GraphPipelineOptions::default().max_cursor_bytes)
            .unwrap();
    let atom = payload
        .last_logical_row_key
        .iter_mut()
        .find(|atom| matches!(atom, GraphSortAtom::Bytes(_)))
        .expect("pipeline cursor logical key should include internal bytes atom");
    *atom = GraphSortAtom::String(b"not-bytes".to_vec());
    graph_pipeline_encode_logical_cursor(&payload, GraphPipelineOptions::default().max_cursor_bytes)
        .unwrap()
}

fn graph_row_value_rows(result: GraphRowResult) -> Vec<Vec<GraphValue>> {
    result.rows.into_iter().map(|row| row.values).collect()
}

fn graph_pipeline_value_rows(result: GraphPipelineResult) -> Vec<Vec<GraphValue>> {
    result.rows.into_iter().map(|row| row.values).collect()
}

fn graph_row_single_u64_column(result: GraphRowResult) -> Vec<u64> {
    result
        .rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [GraphValue::NodeId(id)] | [GraphValue::EdgeId(id)] | [GraphValue::UInt(id)] => *id,
            other => panic!("expected one ID-like graph value, got {other:?}"),
        })
        .collect()
}

fn graph_row_single_path_column(result: GraphRowResult) -> Vec<GraphPathValue> {
    result
        .rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [GraphValue::Path(path)] => path.clone(),
            other => panic!("expected one path graph value, got {other:?}"),
        })
        .collect()
}

fn graph_row_path_ids(result: GraphRowResult) -> Vec<(Vec<u64>, Vec<u64>)> {
    graph_row_single_path_column(result)
        .into_iter()
        .map(|path| (path.node_ids, path.edge_ids))
        .collect()
}

fn graph_row_explain_text(explain: &GraphRowExplain) -> String {
    let mut text = String::new();
    for node in &explain.plan {
        text.push_str(&node.kind);
        text.push(' ');
        text.push_str(&node.detail);
        text.push('\n');
    }
    for op in &explain.row_ops {
        text.push_str(&op.kind);
        text.push(' ');
        text.push_str(&op.detail);
        text.push('\n');
    }
    for warning in &explain.warnings {
        text.push_str(warning);
        text.push('\n');
    }
    for note in &explain.notes {
        text.push_str(note);
        text.push('\n');
    }
    text
}

fn assert_graph_row_explain_contains(explain: &GraphRowExplain, expected: &str) {
    let text = graph_row_explain_text(explain);
    assert!(
        text.contains(expected),
        "expected graph-row explain to contain {expected:?}, got:\n{text}"
    );
}

fn assert_graph_row_explain_not_contains(explain: &GraphRowExplain, unexpected: &str) {
    let text = graph_row_explain_text(explain);
    assert!(
        !text.contains(unexpected),
        "expected graph-row explain not to contain {unexpected:?}, got:\n{text}"
    );
}

fn selected_node(
    props: GraphPropertySelection,
    vectors: GraphVectorSelection,
) -> GraphSelectedNodeProjection {
    GraphSelectedNodeProjection {
        id: true,
        labels: true,
        key: true,
        props,
        weight: true,
        created_at: true,
        updated_at: true,
        vectors,
    }
}

fn selected_edge(props: GraphPropertySelection) -> GraphSelectedEdgeProjection {
    GraphSelectedEdgeProjection {
        id: true,
        from: true,
        to: true,
        label: true,
        props,
        weight: true,
        created_at: true,
        updated_at: true,
        valid_from: true,
        valid_to: true,
    }
}

fn synthetic_node(id: u64) -> GraphBoundNode {
    let mut props = BTreeMap::new();
    props.insert("name".to_string(), GraphValue::String(format!("node-{id}")));
    props.insert("rank".to_string(), GraphValue::UInt(id));
    GraphBoundNode::with_element(
        id,
        GraphNodeValue {
            id: Some(id),
            labels: Some(vec!["Person".to_string(), "Account".to_string()]),
            key: Some(format!("node-key-{id}")),
            props: Some(props),
            weight: Some(1.5),
            created_at: Some(100 + id as i64),
            updated_at: Some(200 + id as i64),
            dense_vector: Some(vec![id as f32, 0.5]),
            sparse_vector: Some(vec![(id as u32, 1.0)]),
        },
    )
}

fn synthetic_edge(id: u64, from: u64, to: u64) -> GraphBoundEdge {
    let mut props = BTreeMap::new();
    props.insert("since".to_string(), GraphValue::Int(2024));
    props.insert("rank".to_string(), GraphValue::UInt(id));
    GraphBoundEdge::with_element(
        id,
        GraphEdgeValue {
            id: Some(id),
            from: Some(from),
            to: Some(to),
            label: Some("KNOWS".to_string()),
            props: Some(props),
            weight: Some(2.5),
            created_at: Some(300 + id as i64),
            updated_at: Some(400 + id as i64),
            valid_from: Some(10),
            valid_to: Some(20),
        },
    )
}

fn synthetic_path(node_ids: &[u64], edge_ids: &[u64]) -> GraphBoundPath {
    let nodes = node_ids
        .iter()
        .copied()
        .map(synthetic_node)
        .collect::<Vec<_>>();
    let edges = edge_ids
        .iter()
        .copied()
        .enumerate()
        .map(|(index, edge_id)| synthetic_edge(edge_id, node_ids[index], node_ids[index + 1]))
        .collect::<Vec<_>>();
    GraphBoundPath::with_values(
        GraphPath {
            nodes: node_ids.to_vec(),
            edges: edge_ids.to_vec(),
        },
        nodes,
        edges,
    )
    .unwrap()
}

fn eval_with_row(
    schema: &GraphBindingSchema,
    row: &crate::graph_row::GraphBindingRow,
    expr: GraphExpr,
) -> Result<GraphEvalValue, EngineError> {
    eval_graph_expr(
        &expr,
        &GraphEvalContext {
            schema,
            row,
            params: &BTreeMap::new(),
        },
    )
}

fn assert_graph_row_invalid(query: &GraphRowQuery, expected: &str) {
    let err = normalize_graph_row_query(query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains(expected),
        "expected error to contain {expected:?}, got {message:?}"
    );
}

fn expr_contains_param(expr: &GraphExpr) -> bool {
    match expr {
        GraphExpr::Param(_) => true,
        GraphExpr::List(items) => items.iter().any(expr_contains_param),
        GraphExpr::Map(items) => items.values().any(expr_contains_param),
        GraphExpr::Function { args, .. } => args.iter().any(expr_contains_param),
        GraphExpr::AggregateCall { arg, .. } => {
            arg.as_deref().is_some_and(expr_contains_param)
        }
        GraphExpr::ExistsSubquery(stage) => stage
            .query
            .stages
            .iter()
            .any(graph_pipeline_stage_contains_param_for_test),
        GraphExpr::Unary { expr, .. } | GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            expr_contains_param(expr)
        }
        GraphExpr::Binary { left, right, .. } => {
            expr_contains_param(left) || expr_contains_param(right)
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_contains_param)
                || branches
                    .iter()
                    .any(|branch| expr_contains_param(&branch.when) || expr_contains_param(&branch.then))
                || else_expr.as_deref().is_some_and(expr_contains_param)
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
        | GraphExpr::PathField { .. } => false,
    }
}

fn graph_pipeline_stage_contains_param_for_test(stage: &GraphPipelineStage) -> bool {
    match stage {
        GraphPipelineStage::Match(stage) => stage
            .where_
            .as_ref()
            .is_some_and(expr_contains_param),
        GraphPipelineStage::Project(stage) => {
            let items = match &stage.items {
                GraphProjectionItems::Star => false,
                GraphProjectionItems::Items(items) => {
                    items.iter().any(|item| expr_contains_param(&item.expr))
                }
            };
            items
                || stage.where_.as_ref().is_some_and(expr_contains_param)
                || stage.order_by.iter().any(|item| expr_contains_param(&item.expr))
                || stage.skip.as_ref().is_some_and(expr_contains_param)
                || stage.limit.as_ref().is_some_and(expr_contains_param)
        }
        GraphPipelineStage::Call(stage) => stage
            .query
            .stages
            .iter()
            .any(graph_pipeline_stage_contains_param_for_test),
        GraphPipelineStage::Union(stage) => stage.branches.iter().any(|branch| {
            branch
                .stages
                .iter()
                .any(graph_pipeline_stage_contains_param_for_test)
        }),
        GraphPipelineStage::ShortestPath(stage) => {
            matches!(&stage.from, GraphShortestPathEndpoint::Expr(expr) if expr_contains_param(expr))
                || matches!(&stage.to, GraphShortestPathEndpoint::Expr(expr) if expr_contains_param(expr))
        }
    }
}

#[test]
fn graph_row_binding_schema_slot_lookup_covers_all_slot_kinds() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", false).unwrap();
    let edge = schema.add_edge_alias("r", true).unwrap();
    let path = schema.add_path_alias("p", false).unwrap();
    let scalar = schema.add_scalar_alias("score", true).unwrap();
    let hidden = schema.add_hidden_occurrence("__hidden_r0").unwrap();

    assert_eq!(schema.slot_for_alias("n"), Some(node));
    assert_eq!(schema.slot_for_alias("r"), Some(edge));
    assert_eq!(schema.slot_for_alias("p"), Some(path));
    assert_eq!(schema.slot_for_alias("score"), Some(scalar));
    assert_eq!(schema.slot_for_alias("__hidden_r0"), None);
    assert_eq!(schema.slot(node).unwrap().name, "n");
    assert_eq!(schema.slot(edge).unwrap().name, "r");
    assert_eq!(schema.slot(path).unwrap().name, "p");
    assert_eq!(schema.slot(scalar).unwrap().name, "score");
    assert_eq!(schema.slot(hidden).unwrap().name, "__hidden_r0");
    assert_eq!(
        schema
            .slots()
            .iter()
            .map(|slot| slot.kind)
            .collect::<Vec<_>>(),
        vec![
            GraphBindingSlotKind::Node,
            GraphBindingSlotKind::Edge,
            GraphBindingSlotKind::Path,
            GraphBindingSlotKind::Scalar,
            GraphBindingSlotKind::HiddenOccurrence,
        ]
    );
    assert!(schema.slots()[1].nullable);
    assert_eq!(schema.slots()[0].name, "n");
    assert_eq!(schema.slots()[0].user_alias.as_deref(), Some("n"));
    assert_eq!(schema.slots()[4].name, "__hidden_r0");
    assert_eq!(schema.slots()[4].user_alias, None);

    let mut row = schema.empty_row();
    row.bind_node(node, synthetic_node(1)).unwrap();
    row.bind_edge(edge, synthetic_edge(2, 1, 3)).unwrap();
    row.bind_path(path, synthetic_path(&[1, 3], &[2])).unwrap();
    row.bind_scalar(scalar, GraphEvalValue::UInt(99)).unwrap();
    row.bind_hidden(hidden, GraphHiddenOccurrence::Edge(2)).unwrap();

    assert_eq!(
        row.value_for_alias(&schema, "score").unwrap(),
        GraphEvalValue::UInt(99)
    );

    let mut null_schema = GraphBindingSchema::new();
    let nullable_edge = null_schema.add_edge_alias("r", true).unwrap();
    let mut null_row = null_schema.empty_row();
    null_row.set_null(&null_schema, nullable_edge).unwrap();
    assert_eq!(
        null_row.value_for_alias(&null_schema, "r").unwrap(),
        GraphEvalValue::Null
    );
}

#[test]
fn graph_row_bindings_reject_conflicting_rebinds_and_null_required_slots() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", false).unwrap();
    let edge = schema.add_edge_alias("r", false).unwrap();
    let path = schema.add_path_alias("p", false).unwrap();
    let scalar = schema.add_scalar_alias("score", false).unwrap();
    let hidden = schema.add_hidden_occurrence("__hidden_r0").unwrap();
    let nullable = schema.add_node_alias("opt", true).unwrap();
    let nullable_bound = schema.add_node_alias("nullable_bound", true).unwrap();
    let mut row = schema.empty_row();

    row.bind_node(node, synthetic_node(1)).unwrap();
    row.bind_node(node, synthetic_node(1)).unwrap();
    assert!(row.bind_node(node, synthetic_node(2)).unwrap_err().to_string().contains("conflicting node"));

    row.bind_edge(edge, synthetic_edge(10, 1, 2)).unwrap();
    row.bind_edge(edge, synthetic_edge(10, 1, 2)).unwrap();
    assert!(row
        .bind_edge(edge, synthetic_edge(11, 1, 2))
        .unwrap_err()
        .to_string()
        .contains("conflicting edge"));

    row.bind_path(path, synthetic_path(&[1, 2], &[10])).unwrap();
    row.bind_path(path, synthetic_path(&[1, 2], &[10])).unwrap();
    assert!(row
        .bind_path(path, synthetic_path(&[1, 3], &[10]))
        .unwrap_err()
        .to_string()
        .contains("conflicting path"));

    row.bind_scalar(scalar, GraphEvalValue::UInt(1)).unwrap();
    row.bind_scalar(scalar, GraphEvalValue::UInt(1)).unwrap();
    assert!(row
        .bind_scalar(scalar, GraphEvalValue::UInt(2))
        .unwrap_err()
        .to_string()
        .contains("conflicting scalar"));

    row.bind_hidden(hidden, GraphHiddenOccurrence::Edge(10))
        .unwrap();
    row.bind_hidden(hidden, GraphHiddenOccurrence::Edge(10))
        .unwrap();
    assert!(row
        .bind_hidden(hidden, GraphHiddenOccurrence::Edge(11))
        .unwrap_err()
        .to_string()
        .contains("conflicting hidden occurrence"));

    assert!(row
        .set_null(&schema, node)
        .unwrap_err()
        .to_string()
        .contains("not nullable"));
    row.set_null(&schema, nullable).unwrap();
    assert!(row
        .bind_node(nullable, synthetic_node(3))
        .unwrap_err()
        .to_string()
        .contains("null node binding cannot be rebound"));
    row.bind_node(nullable_bound, synthetic_node(4)).unwrap();
    assert!(row
        .set_null(&schema, nullable_bound)
        .unwrap_err()
        .to_string()
        .contains("already bound"));
}

#[test]
fn graph_row_identity_rebinds_merge_loaded_payloads() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", false).unwrap();
    let edge = schema.add_edge_alias("r", false).unwrap();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();

    row.bind_node(node, GraphBoundNode::id_only(1)).unwrap();
    row.bind_node(node, synthetic_node(1)).unwrap();
    assert_eq!(
        row.value_for_alias(&schema, "n").unwrap(),
        GraphEvalValue::Node(synthetic_node(1))
    );
    assert!(row
        .bind_node(node, synthetic_node(2))
        .unwrap_err()
        .to_string()
        .contains("conflicting node"));

    row.bind_edge(edge, GraphBoundEdge::id_only(10)).unwrap();
    row.bind_edge(edge, synthetic_edge(10, 1, 2)).unwrap();
    assert_eq!(
        row.value_for_alias(&schema, "r").unwrap(),
        GraphEvalValue::Edge(synthetic_edge(10, 1, 2))
    );
    assert!(row
        .bind_edge(edge, synthetic_edge(11, 1, 2))
        .unwrap_err()
        .to_string()
        .contains("conflicting edge"));

    row.bind_path(
        path,
        GraphBoundPath::id_only(GraphPath {
            nodes: vec![1, 2],
            edges: vec![10],
        })
        .unwrap(),
    )
    .unwrap();
    row.bind_path(path, synthetic_path(&[1, 2], &[10])).unwrap();
    assert_eq!(
        row.value_for_alias(&schema, "p").unwrap(),
        GraphEvalValue::Path(synthetic_path(&[1, 2], &[10]))
    );
    assert!(row
        .bind_path(path, synthetic_path(&[1, 3], &[10]))
        .unwrap_err()
        .to_string()
        .contains("conflicting path"));
}

#[test]
fn graph_row_bound_element_ids_are_validated_and_normalized() {
    let mut schema = GraphBindingSchema::new();
    let node_slot = schema.add_node_alias("n", false).unwrap();
    let edge_slot = schema.add_edge_alias("r", false).unwrap();

    let mut mismatched_node = synthetic_node(1);
    mismatched_node.element.as_mut().unwrap().id = Some(2);
    let mut row = schema.empty_row();
    assert!(row
        .bind_node(node_slot, mismatched_node)
        .unwrap_err()
        .to_string()
        .contains("node element id 2 does not match binding id 1"));

    let mut missing_node_id = synthetic_node(1);
    missing_node_id.element.as_mut().unwrap().id = None;
    row.bind_node(node_slot, missing_node_id).unwrap();
    let GraphEvalValue::Node(node) = row.value_for_alias(&schema, "n").unwrap() else {
        panic!("expected node binding");
    };
    assert_eq!(node.element.as_ref().unwrap().id, Some(1));

    let mut mismatched_edge = synthetic_edge(10, 1, 2);
    mismatched_edge.element.as_mut().unwrap().id = Some(11);
    assert!(row
        .bind_edge(edge_slot, mismatched_edge)
        .unwrap_err()
        .to_string()
        .contains("edge element id 11 does not match binding id 10"));

    let mut missing_edge_id = synthetic_edge(10, 1, 2);
    missing_edge_id.element.as_mut().unwrap().id = None;
    row.bind_edge(edge_slot, missing_edge_id).unwrap();
    let GraphEvalValue::Edge(edge) = row.value_for_alias(&schema, "r").unwrap() else {
        panic!("expected edge binding");
    };
    assert_eq!(edge.element.as_ref().unwrap().id, Some(10));

    let mut bad_path_node = synthetic_node(1);
    bad_path_node.element.as_mut().unwrap().id = Some(99);
    assert!(GraphBoundPath::with_values(
        GraphPath {
            nodes: vec![1],
            edges: vec![],
        },
        vec![bad_path_node],
        Vec::new(),
    )
    .unwrap_err()
    .to_string()
    .contains("node element id 99 does not match binding id 1"));
}

#[test]
fn graph_row_null_and_three_valued_boolean_semantics_are_gql_shaped() {
    let schema = GraphBindingSchema::new();
    let row = schema.empty_row();
    let context = GraphEvalContext {
        schema: &schema,
        row: &row,
        params: &BTreeMap::new(),
    };

    assert!(eval_graph_predicate(&GraphExpr::Bool(true), &context).unwrap());
    assert!(!eval_graph_predicate(&GraphExpr::Bool(false), &context).unwrap());
    assert!(!eval_graph_predicate(&GraphExpr::Null, &context).unwrap());

    let values = [
        (GraphExpr::Bool(true), Some(true)),
        (GraphExpr::Bool(false), Some(false)),
        (GraphExpr::Null, None),
    ];
    for (left_expr, left) in &values {
        for (right_expr, right) in &values {
            let and_value = eval_graph_expr(
                &GraphExpr::Binary {
                    left: Box::new(left_expr.clone()),
                    op: GraphBinaryOp::And,
                    right: Box::new(right_expr.clone()),
                },
                &context,
            )
            .unwrap();
            let or_value = eval_graph_expr(
                &GraphExpr::Binary {
                    left: Box::new(left_expr.clone()),
                    op: GraphBinaryOp::Or,
                    right: Box::new(right_expr.clone()),
                },
                &context,
            )
            .unwrap();
            let expected_and = match (*left, *right) {
                (Some(false), _) | (_, Some(false)) => GraphEvalValue::Bool(false),
                (Some(true), Some(true)) => GraphEvalValue::Bool(true),
                _ => GraphEvalValue::Null,
            };
            let expected_or = match (*left, *right) {
                (Some(true), _) | (_, Some(true)) => GraphEvalValue::Bool(true),
                (Some(false), Some(false)) => GraphEvalValue::Bool(false),
                _ => GraphEvalValue::Null,
            };
            assert_eq!(and_value, expected_and, "AND {left:?} {right:?}");
            assert_eq!(or_value, expected_or, "OR {left:?} {right:?}");
        }
    }

    assert_eq!(
        eval_graph_expr(
            &GraphExpr::Unary {
                op: GraphUnaryOp::Not,
                expr: Box::new(GraphExpr::Bool(true)),
            },
            &context,
        )
        .unwrap(),
        GraphEvalValue::Bool(false)
    );
    assert_eq!(
        eval_graph_expr(
            &GraphExpr::Unary {
                op: GraphUnaryOp::Not,
                expr: Box::new(GraphExpr::Null),
            },
            &context,
        )
        .unwrap(),
        GraphEvalValue::Null
    );
    assert_eq!(
        eval_graph_expr(&GraphExpr::IsNull(Box::new(GraphExpr::Null)), &context).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        eval_graph_expr(
            &GraphExpr::IsNotNull(Box::new(GraphExpr::String("x".to_string()))),
            &context,
        )
        .unwrap(),
        GraphEvalValue::Bool(true)
    );
}

#[test]
fn graph_row_numeric_scalar_and_in_semantics_reuse_phase31b_rules() {
    let schema = GraphBindingSchema::new();
    let row = schema.empty_row();
    let context = GraphEvalContext {
        schema: &schema,
        row: &row,
        params: &BTreeMap::new(),
    };
    let cmp = |left: GraphExpr, op: GraphBinaryOp, right: GraphExpr| {
        eval_graph_expr(
            &GraphExpr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            &context,
        )
    };

    assert_eq!(
        cmp(GraphExpr::Int(1), GraphBinaryOp::Eq, GraphExpr::UInt(1)).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(GraphExpr::UInt(1), GraphBinaryOp::Eq, GraphExpr::Float(1.0)).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(GraphExpr::Int(-1), GraphBinaryOp::Lt, GraphExpr::UInt(0)).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(GraphExpr::UInt(u64::MAX), GraphBinaryOp::Lt, GraphExpr::Float(18_446_744_073_709_551_616.0)).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(GraphExpr::Float(-0.0), GraphBinaryOp::Eq, GraphExpr::Float(0.0)).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert!(cmp(
        GraphExpr::Float(f64::NAN),
        GraphBinaryOp::Eq,
        GraphExpr::Float(f64::NAN),
    )
    .unwrap_err()
    .to_string()
    .contains("non-finite"));

    assert_eq!(
        cmp(
            GraphExpr::String("a".to_string()),
            GraphBinaryOp::Lt,
            GraphExpr::String("b".to_string())
        )
        .unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(GraphExpr::Bool(false), GraphBinaryOp::Lt, GraphExpr::Bool(true)).unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(
            GraphExpr::Bytes(vec![1, 2]),
            GraphBinaryOp::Lt,
            GraphExpr::Bytes(vec![1, 3])
        )
        .unwrap(),
        GraphEvalValue::Bool(true)
    );

    assert_eq!(
        cmp(
            GraphExpr::UInt(1),
            GraphBinaryOp::In,
            GraphExpr::List(vec![GraphExpr::Int(1)])
        )
        .unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert_eq!(
        cmp(
            GraphExpr::UInt(2),
            GraphBinaryOp::In,
            GraphExpr::List(vec![GraphExpr::Null, GraphExpr::Int(1)])
        )
        .unwrap(),
        GraphEvalValue::Null
    );
}

#[test]
fn graph_row_property_field_and_function_evaluation_uses_synthetic_bindings() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", false).unwrap();
    let edge = schema.add_edge_alias("r", false).unwrap();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_node(node, synthetic_node(1)).unwrap();
    row.bind_edge(edge, synthetic_edge(10, 1, 2)).unwrap();
    row.bind_path(path, synthetic_path(&[1, 2, 3], &[10, 11])).unwrap();

    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Property {
                alias: "n".to_string(),
                key: "name".to_string(),
            },
        )
        .unwrap(),
        GraphEvalValue::String("node-1".to_string())
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Property {
                alias: "r".to_string(),
                key: "since".to_string(),
            },
        )
        .unwrap(),
        GraphEvalValue::Int(2024)
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::Id,
                args: vec![GraphExpr::Binding("n".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::UInt(1)
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::Labels,
                args: vec![GraphExpr::Binding("n".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::List(vec![
            GraphEvalValue::String("Person".to_string()),
            GraphEvalValue::String("Account".to_string()),
        ])
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::Type,
                args: vec![GraphExpr::Binding("r".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::String("KNOWS".to_string())
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::Length,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::UInt(2)
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::StartNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::Node(synthetic_node(1))
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::EndNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::Node(synthetic_node(3))
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::Nodes,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::List(vec![
            GraphEvalValue::Node(synthetic_node(1)),
            GraphEvalValue::Node(synthetic_node(2)),
            GraphEvalValue::Node(synthetic_node(3)),
        ])
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Function {
                name: GraphFunction::Relationships,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
        )
        .unwrap(),
        GraphEvalValue::List(vec![
            GraphEvalValue::Edge(synthetic_edge(10, 1, 2)),
            GraphEvalValue::Edge(synthetic_edge(11, 2, 3)),
        ])
    );
}

#[test]
fn graph_row_path_derived_endpoint_functions_compose_with_loaded_path_payloads() {
    let mut schema = GraphBindingSchema::new();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_path(path, synthetic_path(&[1, 2, 3], &[10, 11]))
        .unwrap();

    let labels_start = GraphExpr::Function {
        name: GraphFunction::Labels,
        args: vec![GraphExpr::Function {
            name: GraphFunction::StartNode,
            args: vec![GraphExpr::Binding("p".to_string())],
        }],
    };
    let labels_end = GraphExpr::Function {
        name: GraphFunction::Labels,
        args: vec![GraphExpr::Function {
            name: GraphFunction::EndNode,
            args: vec![GraphExpr::Binding("p".to_string())],
        }],
    };
    let expected_labels = GraphEvalValue::List(vec![
        GraphEvalValue::String("Person".to_string()),
        GraphEvalValue::String("Account".to_string()),
    ]);

    assert_eq!(
        eval_with_row(&schema, &row, labels_start.clone()).unwrap(),
        expected_labels
    );
    assert_eq!(
        eval_with_row(&schema, &row, labels_end.clone()).unwrap(),
        expected_labels
    );

    let bound_context = BoundGraphEvalContext { row: &row };
    assert_eq!(
        eval_bound_graph_expr(
            &bind_graph_expr(&schema, &labels_start).unwrap(),
            &bound_context
        )
        .unwrap(),
        expected_labels
    );
    assert_eq!(
        eval_bound_graph_expr(&bind_graph_expr(&schema, &labels_end).unwrap(), &bound_context)
            .unwrap(),
        expected_labels
    );

    let direct_start = bind_graph_expr(
        &schema,
        &GraphExpr::Function {
            name: GraphFunction::StartNode,
            args: vec![GraphExpr::Binding("p".to_string())],
        },
    )
    .unwrap();
    assert_eq!(
        eval_bound_graph_expr(&direct_start, &bound_context).unwrap(),
        GraphEvalValue::Node(synthetic_node(1))
    );

    let mut id_only_row = schema.empty_row();
    id_only_row
        .bind_path(
            path,
            GraphBoundPath::id_only(GraphPath {
                nodes: vec![1, 2],
                edges: vec![10],
            })
            .unwrap(),
        )
        .unwrap();
    let err = eval_bound_graph_expr(
        &bind_graph_expr(&schema, &labels_start).unwrap(),
        &BoundGraphEvalContext { row: &id_only_row },
    )
    .unwrap_err();
    assert!(err.to_string().contains("missing loaded field 'labels'"));
}

#[test]
fn graph_row_unloaded_fields_error_but_loaded_absent_properties_are_null() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", false).unwrap();
    let edge = schema.add_edge_alias("r", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_node(node, GraphBoundNode::id_only(1)).unwrap();
    row.bind_edge(edge, GraphBoundEdge::id_only(10)).unwrap();

    assert!(eval_with_row(
        &schema,
        &row,
        GraphExpr::Property {
            alias: "n".to_string(),
            key: "name".to_string(),
        },
    )
    .unwrap_err()
    .to_string()
    .contains("missing loaded field 'props'"));
    assert!(eval_with_row(
        &schema,
        &row,
        GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Labels,
        },
    )
    .unwrap_err()
    .to_string()
    .contains("missing loaded field 'labels'"));
    assert!(eval_with_row(
        &schema,
        &row,
        GraphExpr::EdgeField {
            alias: "r".to_string(),
            field: GraphEdgeField::Label,
        },
    )
    .unwrap_err()
    .to_string()
    .contains("missing loaded field 'label'"));

    let full_node = project_graph_row_values(
        &schema,
        &row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::Full),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: false,
        },
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert!(full_node.to_string().contains("missing loaded field 'element'"));

    let selected_node = project_graph_row_values(
        &schema,
        &row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                GraphSelectedNodeProjection {
                    id: false,
                    labels: false,
                    key: true,
                    props: GraphPropertySelection::None,
                    weight: false,
                    created_at: false,
                    updated_at: false,
                    vectors: GraphVectorSelection::None,
                },
            )),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Projected,
            compact_rows: false,
            include_vectors: false,
        },
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert!(selected_node
        .to_string()
        .contains("missing loaded field 'key'"));

    let mut partial_row = schema.empty_row();
    partial_row
        .bind_node(
            node,
            GraphBoundNode::with_element(
                1,
                GraphNodeValue {
                    id: Some(1),
                    labels: Some(vec!["Person".to_string()]),
                    key: None,
                    props: Some(BTreeMap::new()),
                    weight: Some(1.0),
                    created_at: Some(10),
                    updated_at: Some(20),
                    dense_vector: None,
                    sparse_vector: None,
                },
            ),
        )
        .unwrap();
    partial_row
        .bind_edge(
            edge,
            GraphBoundEdge::with_element(
                10,
                GraphEdgeValue {
                    id: Some(10),
                    from: Some(1),
                    to: Some(2),
                    label: Some("KNOWS".to_string()),
                    props: Some(BTreeMap::new()),
                    weight: Some(1.0),
                    created_at: Some(10),
                    updated_at: Some(20),
                    valid_from: Some(30),
                    valid_to: None,
                },
            ),
        )
        .unwrap();
    let partial_node = project_graph_row_values(
        &schema,
        &partial_row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::Full),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: false,
        },
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert!(partial_node
        .to_string()
        .contains("missing loaded field 'key'"));

    let partial_edge = project_graph_row_values(
        &schema,
        &partial_row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("r".to_string()),
            alias: Some("r".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::Full),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: false,
        },
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert!(partial_edge
        .to_string()
        .contains("missing loaded field 'valid_to'"));

    let mut loaded_row = schema.empty_row();
    loaded_row.bind_node(node, synthetic_node(1)).unwrap();
    assert_eq!(
        eval_with_row(
            &schema,
            &loaded_row,
            GraphExpr::Property {
                alias: "n".to_string(),
                key: "missing".to_string(),
            },
        )
        .unwrap(),
        GraphEvalValue::Null
    );
}

#[test]
fn graph_row_null_alias_property_path_equality_and_order_rejections() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", true).unwrap();
    let path = schema.add_path_alias("p", false).unwrap();
    let other_path = schema.add_path_alias("q", false).unwrap();
    let mut row = schema.empty_row();
    row.set_null(&schema, node).unwrap();
    row.bind_path(path, synthetic_path(&[1, 2], &[10])).unwrap();
    row.bind_path(other_path, synthetic_path(&[1, 2], &[10])).unwrap();

    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Property {
                alias: "n".to_string(),
                key: "name".to_string(),
            },
        )
        .unwrap(),
        GraphEvalValue::Null
    );
    assert_eq!(
        eval_with_row(
            &schema,
            &row,
            GraphExpr::Binary {
                left: Box::new(GraphExpr::Binding("p".to_string())),
                op: GraphBinaryOp::Eq,
                right: Box::new(GraphExpr::Binding("q".to_string())),
            },
        )
        .unwrap(),
        GraphEvalValue::Bool(true)
    );
    assert!(eval_with_row(
        &schema,
        &row,
        GraphExpr::Property {
            alias: "p".to_string(),
            key: "bad".to_string(),
        },
    )
    .unwrap_err()
    .to_string()
    .contains("path alias"));
    assert!(eval_with_row(
        &schema,
        &row,
        GraphExpr::Binary {
            left: Box::new(GraphExpr::List(vec![GraphExpr::Int(1)])),
            op: GraphBinaryOp::Lt,
            right: Box::new(GraphExpr::List(vec![GraphExpr::Int(2)])),
        },
    )
    .unwrap_err()
    .to_string()
    .contains("not orderable"));
    assert!(eval_with_row(
        &schema,
        &row,
        GraphExpr::Binary {
            left: Box::new(GraphExpr::Map(BTreeMap::new())),
            op: GraphBinaryOp::Lt,
            right: Box::new(GraphExpr::Map(BTreeMap::new())),
        },
    )
    .unwrap_err()
    .to_string()
    .contains("not orderable"));
}

#[test]
fn graph_row_bound_paths_reject_invalid_shapes() {
    assert!(GraphBoundPath::id_only(GraphPath {
        nodes: Vec::new(),
        edges: Vec::new(),
    })
    .unwrap_err()
    .to_string()
    .contains("at least one node id"));

    assert!(GraphBoundPath::id_only(GraphPath {
        nodes: vec![1],
        edges: vec![10],
    })
    .unwrap_err()
    .to_string()
    .contains("one more node id"));

    assert!(GraphBoundPath::with_values(
        GraphPath {
            nodes: vec![1, 2],
            edges: Vec::new(),
        },
        vec![synthetic_node(1), synthetic_node(2)],
        Vec::new(),
    )
    .unwrap_err()
    .to_string()
    .contains("one more node id"));
}

#[test]
fn graph_row_sort_atoms_cover_numeric_signed_zero_and_path_keys() {
    let zero = graph_sort_atom_for_value(&GraphEvalValue::Float(-0.0)).unwrap();
    let uint_zero = graph_sort_atom_for_value(&GraphEvalValue::UInt(0)).unwrap();
    assert_eq!(compare_graph_sort_atoms(&zero, &uint_zero), CmpOrdering::Equal);
    assert_eq!(
        compare_graph_sort_atoms(
            &graph_sort_atom_for_value(&GraphEvalValue::Null).unwrap(),
            &graph_sort_atom_for_value(&GraphEvalValue::Bool(true)).unwrap(),
        ),
        CmpOrdering::Greater
    );
    assert_eq!(
        graph_sort_atom_for_value(&GraphEvalValue::Path(synthetic_path(&[1, 2, 3], &[9, 10])))
            .unwrap(),
        GraphSortAtom::Path {
            hop_count: 2,
            nodes: vec![1, 2, 3],
            edges: vec![9, 10],
        }
    );
}

#[test]
fn graph_row_path_functions_preserve_elements_and_collect_output_needs() {
    let return_items = vec![
        GraphReturnItem {
            expr: GraphExpr::Function {
                name: GraphFunction::StartNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            alias: Some("start".to_string()),
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Relationships,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            alias: Some("rels".to_string()),
            projection: GraphReturnProjection::Auto,
        },
    ];

    let mut query = graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)]);
    query.output = GraphOutputOptions {
        mode: GraphOutputMode::Elements,
        compact_rows: false,
        include_vectors: false,
    };
    query.return_items = Some(return_items.clone());
    let normalized = normalize_graph_row_query(&query).unwrap();
    let path_needs = normalized.projection_needs.output.paths.get("p").unwrap();
    assert_eq!(
        path_needs.start_node,
        Some(crate::row_projection::NodeSelectedFieldNeeds {
            key: true,
            created_at: true,
            props: RowPropertySelection::All,
            vectors: crate::row_projection::VectorSelection::None,
        })
    );
    assert_eq!(path_needs.nodes, None);
    assert_eq!(
        path_needs.edges,
        Some(crate::row_projection::EdgeSelectedFieldNeeds {
            created_at: true,
            props: RowPropertySelection::All,
        })
    );

    let mut schema = GraphBindingSchema::new();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_path(path, synthetic_path(&[1, 2, 3], &[10, 11])).unwrap();
    let values = project_graph_row_values(
        &schema,
        &row,
        &return_items,
        &query.output,
        &BTreeMap::new(),
    )
    .unwrap();

    let GraphValue::Node(start) = &values[0] else {
        panic!("expected start node element");
    };
    assert_eq!(start.id, Some(1));
    assert!(start.props.as_ref().unwrap().contains_key("name"));
    assert_eq!(start.dense_vector, None);

    let GraphValue::List(rels) = &values[1] else {
        panic!("expected relationship list");
    };
    assert_eq!(rels.len(), 2);
    let GraphValue::Edge(first_rel) = &rels[0] else {
        panic!("expected edge element");
    };
    assert_eq!(first_rel.id, Some(10));
    assert_eq!(first_rel.from, Some(1));
    assert!(first_rel.props.as_ref().unwrap().contains_key("since"));
}

#[test]
fn graph_row_path_list_functions_support_selected_output() {
    let return_items = vec![
        GraphReturnItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Nodes,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            alias: Some("nodes".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                selected_node(
                    GraphPropertySelection::Keys(vec!["name".to_string()]),
                    GraphVectorSelection::None,
                ),
            )),
        },
        GraphReturnItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Relationships,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            alias: Some("relationships".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Edge(
                selected_edge(GraphPropertySelection::Keys(vec!["since".to_string()])),
            )),
        },
    ];
    let mut query = graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)]);
    query.output = GraphOutputOptions {
        mode: GraphOutputMode::Projected,
        compact_rows: false,
        include_vectors: false,
    };
    query.return_items = Some(return_items.clone());
    normalize_graph_row_query(&query).unwrap();

    let mut element_query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    element_query.return_items = Some(vec![
        GraphReturnItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Nodes,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            alias: Some("nodes".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::Compact),
        },
        GraphReturnItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Relationships,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            alias: Some("relationships".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::Compact),
        },
    ]);
    normalize_graph_row_query(&element_query).unwrap();

    let mut schema = GraphBindingSchema::new();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_path(path, synthetic_path(&[1, 2, 3], &[10, 11]))
        .unwrap();
    let values =
        project_graph_row_values(&schema, &row, &return_items, &query.output, &BTreeMap::new())
            .unwrap();

    let GraphValue::List(nodes) = &values[0] else {
        panic!("expected selected node list");
    };
    assert_eq!(nodes.len(), 3);
    let GraphValue::Node(first_node) = &nodes[0] else {
        panic!("expected selected node");
    };
    assert_eq!(first_node.id, Some(1));
    assert_eq!(first_node.props.as_ref().unwrap().len(), 1);
    assert!(first_node.props.as_ref().unwrap().contains_key("name"));

    let GraphValue::List(edges) = &values[1] else {
        panic!("expected selected edge list");
    };
    assert_eq!(edges.len(), 2);
    let GraphValue::Edge(first_edge) = &edges[0] else {
        panic!("expected selected edge");
    };
    assert_eq!(first_edge.id, Some(10));
    assert_eq!(first_edge.props.as_ref().unwrap().len(), 1);
    assert!(first_edge.props.as_ref().unwrap().contains_key("since"));
}

#[test]
fn graph_row_rich_path_function_outputs_preserve_hydrated_elements() {
    let return_items = vec![
        GraphReturnItem {
            expr: GraphExpr::Case {
                operand: None,
                branches: vec![GraphCaseBranch {
                    when: GraphExpr::Bool(true),
                    then: GraphExpr::Function {
                        name: GraphFunction::Nodes,
                        args: vec![GraphExpr::Binding("p".to_string())],
                    },
                }],
                else_expr: Some(Box::new(GraphExpr::List(Vec::new()))),
            },
            alias: Some("nodes".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                selected_node(
                    GraphPropertySelection::Keys(vec!["name".to_string()]),
                    GraphVectorSelection::None,
                ),
            )),
        },
        GraphReturnItem {
            expr: GraphExpr::Case {
                operand: None,
                branches: vec![GraphCaseBranch {
                    when: GraphExpr::Bool(true),
                    then: GraphExpr::Function {
                        name: GraphFunction::Relationships,
                        args: vec![GraphExpr::Binding("p".to_string())],
                    },
                }],
                else_expr: Some(Box::new(GraphExpr::List(Vec::new()))),
            },
            alias: Some("relationships".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Edge(
                selected_edge(GraphPropertySelection::Keys(vec!["since".to_string()])),
            )),
        },
    ];
    let mut query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    query.output = GraphOutputOptions {
        mode: GraphOutputMode::Projected,
        compact_rows: false,
        include_vectors: false,
    };
    query.return_items = Some(return_items.clone());
    let normalized = normalize_graph_row_query(&query).unwrap();
    let path_needs = normalized.projection_needs.output.paths.get("p").unwrap();
    assert!(path_needs.nodes.is_some());
    assert!(path_needs.edges.is_some());

    let mut schema = GraphBindingSchema::new();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_path(path, synthetic_path(&[1, 2, 3], &[10, 11]))
        .unwrap();
    let values =
        project_graph_row_values(&schema, &row, &return_items, &query.output, &BTreeMap::new())
            .unwrap();

    let GraphValue::List(nodes) = &values[0] else {
        panic!("expected selected node list");
    };
    let GraphValue::Node(first_node) = &nodes[0] else {
        panic!("expected selected node");
    };
    assert_eq!(first_node.id, Some(1));
    assert_eq!(first_node.props.as_ref().unwrap().len(), 1);
    assert!(first_node.props.as_ref().unwrap().contains_key("name"));

    let GraphValue::List(edges) = &values[1] else {
        panic!("expected selected edge list");
    };
    let GraphValue::Edge(first_edge) = &edges[0] else {
        panic!("expected selected edge");
    };
    assert_eq!(first_edge.id, Some(10));
    assert_eq!(first_edge.props.as_ref().unwrap().len(), 1);
    assert!(first_edge.props.as_ref().unwrap().contains_key("since"));
}

#[test]
fn graph_row_synthetic_output_conversion_covers_modes_paths_vectors_and_nulls() {
    let mut schema = GraphBindingSchema::new();
    let node = schema.add_node_alias("n", false).unwrap();
    let edge = schema.add_edge_alias("r", false).unwrap();
    let path = schema.add_path_alias("p", false).unwrap();
    let optional = schema.add_node_alias("opt", true).unwrap();
    let mut row = schema.empty_row();
    row.bind_node(node, synthetic_node(1)).unwrap();
    row.bind_edge(edge, synthetic_edge(10, 1, 2)).unwrap();
    row.bind_path(path, synthetic_path(&[1, 2, 3], &[10, 11])).unwrap();
    row.set_null(&schema, optional).unwrap();

    let return_items = vec![
        GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("r".to_string()),
            alias: Some("r".to_string()),
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p".to_string()),
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("opt".to_string()),
            alias: Some("opt".to_string()),
            projection: GraphReturnProjection::Auto,
        },
    ];
    let id_values = project_graph_row_values(
        &schema,
        &row,
        &return_items,
        &GraphOutputOptions::default(),
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(id_values[0], GraphValue::NodeId(1));
    assert_eq!(id_values[1], GraphValue::EdgeId(10));
    assert_eq!(
        id_values[2],
        GraphValue::Path(GraphPathValue {
            node_ids: vec![1, 2, 3],
            edge_ids: vec![10, 11],
            nodes: None,
            edges: None,
        })
    );
    assert_eq!(id_values[3], GraphValue::Null);

    let element_values = project_graph_row_values(
        &schema,
        &row,
        &return_items,
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: false,
        },
        &BTreeMap::new(),
    )
    .unwrap();
    let GraphValue::Node(node_value) = &element_values[0] else {
        panic!("expected node value");
    };
    assert_eq!(node_value.id, Some(1));
    assert_eq!(node_value.dense_vector, None);
    let GraphValue::Path(path_value) = &element_values[2] else {
        panic!("expected path value");
    };
    assert_eq!(path_value.node_ids, vec![1, 2, 3]);
    assert_eq!(path_value.nodes.as_ref().unwrap().len(), 3);

    let id_only_path = project_graph_row_values(
        &schema,
        &row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::IdOnly),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: true,
        },
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(
        id_only_path[0],
        GraphValue::Path(GraphPathValue {
            node_ids: vec![1, 2, 3],
            edge_ids: vec![10, 11],
            nodes: None,
            edges: None,
        })
    );

    let vector_values = project_graph_row_values(
        &schema,
        &row,
        &return_items[0..1],
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: true,
        },
        &BTreeMap::new(),
    )
    .unwrap();
    let GraphValue::Node(vector_node) = &vector_values[0] else {
        panic!("expected node value");
    };
    assert_eq!(vector_node.dense_vector, Some(vec![1.0, 0.5]));

    let selected_items = vec![
        GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                selected_node(
                    GraphPropertySelection::Keys(vec!["name".to_string()]),
                    GraphVectorSelection::Dense,
                ),
            )),
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Path(
                GraphSelectedPathProjection {
                    node_ids: true,
                    edge_ids: true,
                    nodes: Some(selected_node(GraphPropertySelection::None, GraphVectorSelection::None)),
                    edges: Some(selected_edge(GraphPropertySelection::Keys(vec![
                        "since".to_string(),
                    ]))),
                },
            )),
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("opt".to_string()),
            alias: Some("opt".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                selected_node(GraphPropertySelection::None, GraphVectorSelection::None),
            )),
        },
    ];
    let selected_values = project_graph_row_values(
        &schema,
        &row,
        &selected_items,
        &GraphOutputOptions {
            mode: GraphOutputMode::Projected,
            compact_rows: false,
            include_vectors: true,
        },
        &BTreeMap::new(),
    )
    .unwrap();
    let GraphValue::Node(selected_node_value) = &selected_values[0] else {
        panic!("expected selected node");
    };
    assert_eq!(selected_node_value.dense_vector, Some(vec![1.0, 0.5]));
    assert_eq!(selected_node_value.props.as_ref().unwrap().len(), 1);
    assert!(selected_node_value
        .props
        .as_ref()
        .unwrap()
        .contains_key("name"));
    let GraphValue::Path(selected_path_value) = &selected_values[1] else {
        panic!("expected selected path");
    };
    assert_eq!(selected_path_value.edges.as_ref().unwrap().len(), 2);
    assert_eq!(selected_values[2], GraphValue::Null);
}

#[test]
fn graph_row_selected_projection_preserves_nested_nulls_and_optional_vectors() {
    let mut schema = GraphBindingSchema::new();
    let optional = schema.add_node_alias("opt", true).unwrap();
    let node = schema.add_node_alias("n", false).unwrap();
    let mut row = schema.empty_row();
    row.set_null(&schema, optional).unwrap();
    let mut no_vector_node = synthetic_node(5);
    no_vector_node.element.as_mut().unwrap().dense_vector = None;
    no_vector_node.element.as_mut().unwrap().sparse_vector = None;
    row.bind_node(node, no_vector_node).unwrap();

    let selected_projection = GraphReturnProjection::Selected(GraphSelectedProjection::Node(
        selected_node(GraphPropertySelection::None, GraphVectorSelection::Both),
    ));
    let values = project_graph_row_values(
        &schema,
        &row,
        &[
            GraphReturnItem {
                expr: GraphExpr::List(vec![GraphExpr::Binding("opt".to_string())]),
                alias: Some("opt_list".to_string()),
                projection: selected_projection.clone(),
            },
            GraphReturnItem {
                expr: GraphExpr::Map(BTreeMap::from([(
                    "value".to_string(),
                    GraphExpr::Binding("opt".to_string()),
                )])),
                alias: Some("opt_map".to_string()),
                projection: selected_projection.clone(),
            },
            GraphReturnItem {
                expr: GraphExpr::Binding("n".to_string()),
                alias: Some("n".to_string()),
                projection: selected_projection,
            },
        ],
        &GraphOutputOptions {
            mode: GraphOutputMode::Projected,
            compact_rows: false,
            include_vectors: true,
        },
        &BTreeMap::new(),
    )
    .unwrap();

    assert_eq!(values[0], GraphValue::List(vec![GraphValue::Null]));
    assert_eq!(
        values[1],
        GraphValue::Map(BTreeMap::from([("value".to_string(), GraphValue::Null)]))
    );
    let GraphValue::Node(node_value) = &values[2] else {
        panic!("expected selected node");
    };
    assert_eq!(node_value.dense_vector, None);
    assert_eq!(node_value.sparse_vector, None);

    let full_with_missing_vectors = project_graph_row_values(
        &schema,
        &row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::Full),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Elements,
            compact_rows: false,
            include_vectors: true,
        },
        &BTreeMap::new(),
    )
    .unwrap();
    let GraphValue::Node(full_node) = &full_with_missing_vectors[0] else {
        panic!("expected full node");
    };
    assert_eq!(full_node.dense_vector, None);
    assert_eq!(full_node.sparse_vector, None);
}

#[test]
fn graph_row_selected_path_projection_respects_id_field_flags() {
    let mut schema = GraphBindingSchema::new();
    let path = schema.add_path_alias("p", false).unwrap();
    let mut row = schema.empty_row();
    row.bind_path(path, synthetic_path(&[1, 2], &[10])).unwrap();

    let values = project_graph_row_values(
        &schema,
        &row,
        &[GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Path(
                GraphSelectedPathProjection {
                    node_ids: false,
                    edge_ids: false,
                    nodes: Some(selected_node(
                        GraphPropertySelection::None,
                        GraphVectorSelection::None,
                    )),
                    edges: Some(selected_edge(GraphPropertySelection::None)),
                },
            )),
        }],
        &GraphOutputOptions {
            mode: GraphOutputMode::Projected,
            compact_rows: false,
            include_vectors: false,
        },
        &BTreeMap::new(),
    )
    .unwrap();

    let GraphValue::Path(path_value) = &values[0] else {
        panic!("expected selected path");
    };
    assert!(path_value.node_ids.is_empty());
    assert!(path_value.edge_ids.is_empty());
    assert_eq!(path_value.nodes.as_ref().unwrap().len(), 2);
    assert_eq!(path_value.edges.as_ref().unwrap().len(), 1);
    assert_eq!(path_value.nodes.as_ref().unwrap()[0].id, Some(1));
    assert_eq!(path_value.edges.as_ref().unwrap()[0].id, Some(10));
}

#[test]
fn graph_row_path_output_converts_zero_one_and_multi_hop_shapes() {
    let mut schema = GraphBindingSchema::new();
    let path = schema.add_path_alias("p", false).unwrap();
    for (node_ids, edge_ids) in [
        (vec![1], vec![]),
        (vec![1, 2], vec![10]),
        (vec![1, 2, 3], vec![10, 11]),
    ] {
        let mut row = schema.empty_row();
        row.bind_path(path, synthetic_path(&node_ids, &edge_ids)).unwrap();
        let values = project_graph_row_values(
            &schema,
            &row,
            &[GraphReturnItem {
                expr: GraphExpr::Binding("p".to_string()),
                alias: Some("p".to_string()),
                projection: GraphReturnProjection::IdOnly,
            }],
            &GraphOutputOptions::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            values[0],
            GraphValue::Path(GraphPathValue {
                node_ids,
                edge_ids,
                nodes: None,
                edges: None,
            })
        );
    }
}

#[test]
fn graph_row_return_names_and_complex_alias_requirements_are_normalized() {
    let mut query = graph_query(&["n", "m"], vec![graph_vlp(Some("p"), None, "n", "m", 1, 2)]);
    query.return_items = Some(vec![
        GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: None,
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::Property {
                alias: "n".to_string(),
                key: "name".to_string(),
            },
            alias: None,
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::PathField {
                alias: "p".to_string(),
                field: GraphPathField::Length,
            },
            alias: None,
            projection: GraphReturnProjection::Auto,
        },
    ]);
    let normalized = normalize_graph_row_query(&query).unwrap();
    assert_eq!(normalized.columns, vec!["n", "n.name", "p.length"]);

    query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binary {
            left: Box::new(GraphExpr::Int(1)),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::UInt(1)),
        },
        alias: None,
        projection: GraphReturnProjection::Auto,
    }]);
    assert_graph_row_invalid(&query, "complex return expressions require an alias");
}

#[test]
fn graph_row_projection_needs_group_verifier_residual_order_output_and_paths() {
    let mut query = graph_query(&["n", "m"], vec![graph_edge(Some("r"), "n", "m")]);
    query.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "tenant".to_string(),
        value: PropValue::String("a".to_string()),
    });
    if let GraphPatternPiece::Edge(edge) = &mut query.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyExists {
            key: "since".to_string(),
        });
    }
    query.where_ = Some(GraphExpr::Property {
        alias: "n".to_string(),
        key: "status".to_string(),
    });
    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Property {
            alias: "r".to_string(),
            key: "rank".to_string(),
        },
        direction: GraphOrderDirection::Asc,
    }];
    query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binding("n".to_string()),
        alias: Some("n".to_string()),
        projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
            selected_node(
                GraphPropertySelection::Keys(vec!["name".to_string()]),
                GraphVectorSelection::None,
            ),
        )),
    }]);
    let normalized = normalize_graph_row_query(&query).unwrap();
    assert_eq!(
        normalized.projection_needs.verifier.nodes["n"].props,
        RowPropertySelection::Keys(vec!["tenant".to_string()])
    );
    assert_eq!(
        normalized.projection_needs.verifier.edges["r"].props,
        RowPropertySelection::Keys(vec!["since".to_string()])
    );
    assert_eq!(
        normalized.projection_needs.residual.nodes["n"].props,
        RowPropertySelection::Keys(vec!["status".to_string()])
    );
    assert_eq!(
        normalized.projection_needs.order.edges["r"].props,
        RowPropertySelection::Keys(vec!["rank".to_string()])
    );
    assert_eq!(
        normalized.projection_needs.output.nodes["n"].props,
        RowPropertySelection::Keys(vec!["name".to_string()])
    );
    assert!(normalized.projection_needs.output.nodes["n"].key);

    let mut path_query =
        graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)]);
    path_query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binding("p".to_string()),
        alias: Some("p".to_string()),
        projection: GraphReturnProjection::Selected(GraphSelectedProjection::Path(
            GraphSelectedPathProjection {
                node_ids: true,
                edge_ids: true,
                nodes: Some(selected_node(GraphPropertySelection::None, GraphVectorSelection::None)),
                edges: None,
            },
        )),
    }]);
    let path_needs = normalize_graph_row_query(&path_query)
        .unwrap()
        .projection_needs
        .output
        .paths
        .get("p")
        .cloned()
        .unwrap();
    assert_eq!(
        path_needs,
        PathSelectedFieldNeeds {
            node_ids: true,
            edge_ids: false,
            nodes: Some(crate::row_projection::NodeSelectedFieldNeeds {
                key: true,
                created_at: true,
                props: RowPropertySelection::None,
                vectors: crate::row_projection::VectorSelection::None,
            }),
            edges: None,
            ..PathSelectedFieldNeeds::default()
        }
    );

    let mut nested_path_query =
        graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)]);
    nested_path_query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Labels,
            args: vec![GraphExpr::Function {
                name: GraphFunction::StartNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            }],
        },
        alias: Some("start_labels".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    let nested_path_needs = normalize_graph_row_query(&nested_path_query)
        .unwrap()
        .projection_needs
        .output
        .paths
        .get("p")
        .cloned()
        .unwrap();
    assert!(nested_path_needs.node_ids);
    assert_eq!(
        nested_path_needs.start_node,
        Some(crate::row_projection::NodeSelectedFieldNeeds::default())
    );
    assert_eq!(nested_path_needs.nodes, None);
}

#[test]
fn graph_row_projection_needs_recurse_into_output_lists_and_maps() {
    let mut list_query = graph_query(&["n"], Vec::new());
    list_query.output = GraphOutputOptions {
        mode: GraphOutputMode::Elements,
        compact_rows: false,
        include_vectors: false,
    };
    list_query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::List(vec![GraphExpr::Binding("n".to_string())]),
        alias: Some("nodes".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    let list_needs = normalize_graph_row_query(&list_query)
        .unwrap()
        .projection_needs
        .output;
    assert_eq!(
        list_needs.nodes["n"].props,
        RowPropertySelection::All
    );
    assert!(list_needs.nodes["n"].key);

    let mut map_query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    map_query.output = GraphOutputOptions {
        mode: GraphOutputMode::Elements,
        compact_rows: false,
        include_vectors: false,
    };
    map_query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Map(BTreeMap::from([(
            "start".to_string(),
            GraphExpr::Function {
                name: GraphFunction::StartNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
        )])),
        alias: Some("m".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    let map_path_needs = normalize_graph_row_query(&map_query)
        .unwrap()
        .projection_needs
        .output
        .paths
        .get("p")
        .cloned()
        .unwrap();
    assert_eq!(
        map_path_needs.start_node,
        Some(crate::row_projection::NodeSelectedFieldNeeds {
            key: true,
            created_at: true,
            props: RowPropertySelection::All,
            vectors: crate::row_projection::VectorSelection::None,
        })
    );
    assert_eq!(map_path_needs.nodes, None);

    let mut node_list_query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    node_list_query.output = GraphOutputOptions {
        mode: GraphOutputMode::Elements,
        compact_rows: false,
        include_vectors: false,
    };
    node_list_query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Nodes,
            args: vec![GraphExpr::Binding("p".to_string())],
        },
        alias: Some("nodes".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    let node_list_path_needs = normalize_graph_row_query(&node_list_query)
        .unwrap()
        .projection_needs
        .output
        .paths
        .get("p")
        .cloned()
        .unwrap();
    assert_eq!(
        node_list_path_needs.nodes,
        Some(crate::row_projection::NodeSelectedFieldNeeds {
            key: true,
            created_at: true,
            props: RowPropertySelection::All,
            vectors: crate::row_projection::VectorSelection::None,
        })
    );
    assert_eq!(node_list_path_needs.start_node, None);
}

#[test]
fn graph_row_projection_needs_skip_id_only_output_reads() {
    let mut query = graph_query(
        &["n", "m"],
        vec![
            graph_edge(Some("r"), "n", "m"),
            graph_vlp(Some("p"), None, "m", "n", 1, 2),
        ],
    );
    query.return_items = Some(vec![
        GraphReturnItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::IdOnly),
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("r".to_string()),
            alias: Some("r".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::IdOnly),
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p".to_string()),
            projection: GraphReturnProjection::Element(GraphElementProjection::IdOnly),
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("m".to_string()),
            alias: Some("m".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                GraphSelectedNodeProjection {
                    id: true,
                    labels: false,
                    key: false,
                    props: GraphPropertySelection::None,
                    weight: false,
                    created_at: false,
                    updated_at: false,
                    vectors: GraphVectorSelection::None,
                },
            )),
        },
        GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p_ids".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Path(
                GraphSelectedPathProjection {
                    node_ids: true,
                    edge_ids: true,
                    nodes: None,
                    edges: None,
                },
            )),
        },
    ]);
    let output_needs = normalize_graph_row_query(&query)
        .unwrap()
        .projection_needs
        .output;

    assert!(!output_needs.nodes.contains_key("n"));
    assert!(!output_needs.nodes.contains_key("m"));
    assert!(!output_needs.edges.contains_key("r"));
    assert!(!output_needs.paths.contains_key("p"));
}

#[test]
fn graph_row_projection_needs_cover_hidden_filters_and_optional_where() {
    let mut query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge(None, "a", "b"),
            graph_vlp(Some("p"), None, "b", "c", 1, 2),
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![graph_edge(Some("oe"), "c", "d")],
                where_: Some(GraphExpr::Property {
                    alias: "d".to_string(),
                    key: "optional_status".to_string(),
                }),
            }),
        ],
    );
    if let GraphPatternPiece::Edge(edge) = &mut query.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyExists {
            key: "hidden_since".to_string(),
        });
    }
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[1] {
        path.filter = Some(EdgeFilterExpr::PropertyExists {
            key: "path_since".to_string(),
        });
    }

    let needs = normalize_graph_row_query(&query).unwrap().projection_needs;
    assert_eq!(
        needs.verifier.hidden_edges[&0].props,
        RowPropertySelection::Keys(vec!["hidden_since".to_string()])
    );
    assert!(!needs
        .verifier
        .edges
        .contains_key("__hidden_edge_occurrence_0"));
    assert_eq!(
        needs.verifier.paths["p"].edges.as_ref().unwrap().props,
        RowPropertySelection::Keys(vec!["path_since".to_string()])
    );
    assert_eq!(
        needs.residual.nodes["d"].props,
        RowPropertySelection::Keys(vec!["optional_status".to_string()])
    );

    let mut hidden_path_query =
        graph_query(&["a", "b"], vec![graph_vlp(None, None, "a", "b", 1, 2)]);
    if let GraphPatternPiece::VariableLength(path) = &mut hidden_path_query.pieces[0] {
        path.filter = Some(EdgeFilterExpr::PropertyExists {
            key: "anonymous_path_since".to_string(),
        });
    }

    let hidden_path_needs = normalize_graph_row_query(&hidden_path_query)
        .unwrap()
        .projection_needs
        .verifier;
    assert_eq!(
        hidden_path_needs.hidden_paths[&0]
            .edges
            .as_ref()
            .unwrap()
            .props,
        RowPropertySelection::Keys(vec!["anonymous_path_since".to_string()])
    );
    assert!(!hidden_path_needs
        .paths
        .contains_key("__hidden_path_occurrence_0"));
}

#[test]
fn graph_row_normalization_resolves_params_in_return_order_and_filters() {
    let mut query = graph_query(
        &["n", "m", "d"],
        vec![GraphPatternPiece::Optional(GraphOptionalGroup {
            pieces: vec![graph_edge(Some("oe"), "m", "d")],
            where_: Some(GraphExpr::Binary {
                left: Box::new(GraphExpr::Property {
                    alias: "d".to_string(),
                    key: "optional_status".to_string(),
                }),
                op: GraphBinaryOp::Eq,
                right: Box::new(GraphExpr::Param("status".to_string())),
            }),
        })],
    );
    query.params.insert(
        "answer".to_string(),
        GraphParamValue::List(vec![GraphParamValue::UInt(42)]),
    );
    query
        .params
        .insert("sort".to_string(), GraphParamValue::Int(7));
    query
        .params
        .insert("status".to_string(), GraphParamValue::String("ok".to_string()));
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::Property {
            alias: "n".to_string(),
            key: "status".to_string(),
        }),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::Param("status".to_string())),
    });
    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Param("sort".to_string()),
        direction: GraphOrderDirection::Asc,
    }];
    query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Param("answer".to_string()),
        alias: Some("answer".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);

    let normalized = normalize_graph_row_query(&query).unwrap();
    assert!(!expr_contains_param(&normalized.return_items[0].expr));
    assert!(!expr_contains_param(&normalized.order_by[0].expr));
    assert_eq!(
        normalized.return_items[0].expr,
        GraphExpr::List(vec![GraphExpr::UInt(42)])
    );
    assert_eq!(normalized.order_by[0].expr, GraphExpr::Int(7));
    assert_eq!(
        normalized.projection_needs.residual.nodes["n"].props,
        RowPropertySelection::Keys(vec!["status".to_string()])
    );
    assert_eq!(
        normalized.projection_needs.residual.nodes["d"].props,
        RowPropertySelection::Keys(vec!["optional_status".to_string()])
    );
}

#[test]
fn graph_row_normalization_binds_expressions_to_slots_for_hot_path_eval() {
    let mut query = graph_query(&["n"], Vec::new());
    query.params.insert("rank".to_string(), GraphParamValue::UInt(1));
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::Property {
            alias: "n".to_string(),
            key: "rank".to_string(),
        }),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::Param("rank".to_string())),
    });
    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Id,
        },
        direction: GraphOrderDirection::Asc,
    }];
    query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binding("n".to_string()),
        alias: Some("n".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);

    let normalized = normalize_graph_row_query(&query).unwrap();
    let node_slot = normalized.binding_schema.slot_for_alias("n").unwrap();
    assert_eq!(
        normalized.bound_return_items[0].expr,
        BoundGraphExpr::Binding(node_slot)
    );
    assert_eq!(
        normalized.bound_order_by[0].expr,
        BoundGraphExpr::NodeField {
            slot: node_slot,
            field: GraphNodeField::Id,
        }
    );
    let BoundGraphExpr::Binary { left, right, .. } =
        normalized.bound_where.as_ref().expect("bound where")
    else {
        panic!("expected bound binary where");
    };
    assert_eq!(
        left.as_ref(),
        &BoundGraphExpr::Property {
            slot: node_slot,
            key: "rank".to_string(),
        }
    );
    assert_eq!(right.as_ref(), &BoundGraphExpr::UInt(1));

    let mut row = normalized.binding_schema.empty_row();
    row.bind_node(node_slot, synthetic_node(1)).unwrap();
    let bound_context = BoundGraphEvalContext { row: &row };
    assert_eq!(
        eval_bound_graph_expr(normalized.bound_where.as_ref().unwrap(), &bound_context).unwrap(),
        GraphEvalValue::Bool(true)
    );
    let values = project_bound_graph_row_values(
        &row,
        &normalized.bound_return_items,
        &normalized.output,
    )
    .unwrap();
    assert_eq!(values, vec![GraphValue::NodeId(1)]);
}

#[test]
fn graph_output_and_query_options_defaults_match_spec() {
    let output = GraphOutputOptions::default();
    assert_eq!(output.mode, GraphOutputMode::Ids);
    assert!(!output.compact_rows);
    assert!(!output.include_vectors);

    let options = GraphQueryOptions::default();
    assert!(!options.allow_full_scan);
    assert_eq!(options.max_intermediate_bindings, 65_536);
    assert_eq!(options.max_frontier, 65_536);
    assert_eq!(options.max_path_hops, 16);
    assert_eq!(options.max_paths_per_start, 4_096);
    assert_eq!(options.max_page_limit, 10_000);
    assert_eq!(options.max_order_materialization, 65_536);
    assert_eq!(options.max_cursor_bytes, 16 * 1024);
    assert_eq!(options.max_query_bytes, 1_048_576);
    assert!(!options.include_plan);
    assert!(!options.profile);
}

#[test]
fn graph_row_omitted_return_items_expand_in_semantic_alias_order() {
    let query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge(Some("e"), "a", "b"),
            graph_vlp(Some("p"), None, "b", "c", 1, 2),
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![graph_edge(Some("oe"), "c", "d")],
                where_: None,
            }),
        ],
    );

    let normalized = normalize_graph_row_query(&query).unwrap();

    assert_eq!(
        normalized.columns,
        vec!["a", "b", "c", "e", "p", "d", "oe"]
    );
    assert_eq!(normalized.return_items.len(), 7);
    assert!(normalized.return_items.iter().all(|item| matches!(
        item.projection,
        GraphReturnProjection::Auto
    )));
}

#[test]
fn graph_row_duplicate_node_alias_is_rejected() {
    let query = graph_query(&["a", "a"], vec![graph_edge(Some("e"), "a", "a")]);

    assert_graph_row_invalid(&query, "node alias 'a' is introduced more than once");
}

#[test]
fn graph_row_duplicate_edge_alias_is_rejected_across_nested_pieces() {
    let query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge(Some("e"), "a", "b"),
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![GraphPatternPiece::Optional(GraphOptionalGroup {
                    pieces: vec![graph_edge(Some("e"), "c", "d")],
                    where_: None,
                })],
                where_: None,
            }),
        ],
    );

    assert_graph_row_invalid(&query, "edge alias 'e' is introduced more than once");
}

#[test]
fn graph_row_path_alias_collision_with_node_or_edge_is_rejected() {
    let node_collision = graph_query(&["a", "b"], vec![graph_vlp(Some("a"), None, "a", "b", 1, 2)]);
    assert_graph_row_invalid(&node_collision, "path alias 'a' collides");

    let edge_collision = graph_query(
        &["a", "b"],
        vec![
            graph_edge(Some("e"), "a", "b"),
            graph_vlp(Some("e"), None, "a", "b", 1, 2),
        ],
    );
    assert_graph_row_invalid(&edge_collision, "path alias 'e' collides");
}

#[test]
fn graph_row_unknown_aliases_are_rejected_in_all_public_surfaces() {
    let edge_piece = graph_query(&["a"], vec![graph_edge(Some("e"), "a", "missing")]);
    assert_graph_row_invalid(&edge_piece, "unknown node alias 'missing'");

    let vlp_piece = graph_query(&["a"], vec![graph_vlp(Some("p"), None, "missing", "a", 1, 2)]);
    assert_graph_row_invalid(&vlp_piece, "unknown node alias 'missing'");

    let mut return_expr = graph_query(&["a"], Vec::new());
    return_expr.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binding("missing".to_string()),
        alias: Some("missing".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    assert_graph_row_invalid(&return_expr, "unknown alias 'missing'");

    let mut order_expr = graph_query(&["a"], Vec::new());
    order_expr.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Binding("missing".to_string()),
        direction: GraphOrderDirection::Asc,
    }];
    assert_graph_row_invalid(&order_expr, "unknown alias 'missing'");

    let mut filter_expr = graph_query(&["a"], Vec::new());
    filter_expr.where_ = Some(GraphExpr::Binding("missing".to_string()));
    assert_graph_row_invalid(&filter_expr, "unknown alias 'missing'");
}

#[test]
fn graph_row_variable_length_edge_alias_requires_one_hop() {
    let query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), Some("e"), "a", "b", 1, 2)],
    );

    assert_graph_row_invalid(&query, "edge_alias is only supported for 1..1");
}

#[test]
fn graph_row_variable_length_hop_bounds_are_validated() {
    let min_gt_max = graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 3, 2)]);
    assert_graph_row_invalid(&min_gt_max, "min_hops 3 greater than max_hops 2");

    let mut over_cap = graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 3)]);
    over_cap.options.max_path_hops = 2;
    assert_graph_row_invalid(&over_cap, "max_hops 3 exceeds max_path_hops 2");
}

#[test]
fn graph_row_vlp_zero_hop_binds_endpoints_filters_and_requires_anchor() {
    let (_dir, engine) = graph_row_test_engine();
    let keep = insert_graph_row_node(
        &engine,
        "ZeroHop",
        "zero-hop-keep",
        &[("status", PropValue::String("keep".to_string()))],
    );
    let drop = insert_graph_row_node(
        &engine,
        "ZeroHop",
        "zero-hop-drop",
        &[("status", PropValue::String("drop".to_string()))],
    );

    let mut equal = graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 0, 0)]);
    equal.nodes[0].ids = vec![keep];
    equal.nodes[1].ids = vec![keep];
    equal.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&equal).unwrap()),
        vec![(vec![keep], vec![])]
    );

    let mut unequal = equal.clone();
    unequal.nodes[1].ids = vec![drop];
    assert!(engine.query_graph_rows(&unequal).unwrap().rows.is_empty());

    let mut bind_other =
        graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 0, 0)]);
    bind_other.nodes[0].ids = vec![keep];
    bind_other.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    bind_other.return_items = Some(vec![
        graph_return_binding("b", GraphReturnProjection::IdOnly),
        graph_return_binding("p", GraphReturnProjection::IdOnly),
    ]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&bind_other).unwrap()),
        vec![vec![
            GraphValue::NodeId(keep),
            GraphValue::Path(GraphPathValue {
                node_ids: vec![keep],
                edge_ids: vec![],
                nodes: None,
                edges: None,
            }),
        ]]
    );

    let mut filtered_out = bind_other;
    filtered_out.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("drop".to_string()),
    });
    assert!(engine.query_graph_rows(&filtered_out).unwrap().rows.is_empty());

    let mut unanchored =
        graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 0, 0)]);
    unanchored.options.allow_full_scan = false;
    let err = engine.query_graph_rows(&unanchored).unwrap_err();
    assert!(
        err.to_string()
            .contains("requires an anchor or allow_full_scan=true"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_row_vlp_one_hop_matches_fixed_edge_and_binds_edge_and_path_aliases() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "OneHop", "one-hop-a", &[]);
    let b = insert_graph_row_node(&engine, "OneHop", "one-hop-b", &[]);
    let first = insert_graph_row_edge(&engine, a, b, "ONE_HOP", &[("rank", PropValue::Int(1))]);
    let second = insert_graph_row_edge(&engine, a, b, "ONE_HOP", &[("rank", PropValue::Int(2))]);

    let mut fixed = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "ONE_HOP")],
    );
    fixed.nodes[0].ids = vec![a];
    fixed.nodes[1].ids = vec![b];
    fixed.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);

    let mut vlp = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), Some("r"), "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut vlp.pieces[0] {
        path.label_filter = vec!["ONE_HOP".to_string()];
    }
    vlp.nodes[0].ids = vec![a];
    vlp.nodes[1].ids = vec![b];
    vlp.return_items = Some(vec![
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("p", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&fixed).unwrap()),
        vec![first, second]
    );
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&vlp).unwrap()),
        vec![
            vec![
                GraphValue::EdgeId(first),
                GraphValue::Path(GraphPathValue {
                    node_ids: vec![a, b],
                    edge_ids: vec![first],
                    nodes: None,
                    edges: None,
                }),
            ],
            vec![
                GraphValue::EdgeId(second),
                GraphValue::Path(GraphPathValue {
                    node_ids: vec![a, b],
                    edge_ids: vec![second],
                    nodes: None,
                    edges: None,
                }),
            ],
        ]
    );
}

#[test]
fn graph_row_one_hop_and_multi_hop_filtered_delegate_unfiltered_remains_raw() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "OneHopDelegated", "a", &[]);
    let b = insert_graph_row_node(&engine, "OneHopDelegated", "b", &[]);
    let c = insert_graph_row_node(&engine, "OneHopDelegated", "c", &[]);
    let hot = insert_graph_row_edge(
        &engine,
        a,
        b,
        "ONE_HOP_DELEGATED",
        &[("status", PropValue::String("hot".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        a,
        c,
        "ONE_HOP_DELEGATED",
        &[("status", PropValue::String("cold".to_string()))],
    );

    let mut filtered = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("path"), Some("edge"), "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut filtered.pieces[0] {
        path.label_filter = vec!["ONE_HOP_DELEGATED".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    filtered.nodes[0].ids = vec![a];
    filtered.options.include_plan = true;
    filtered.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&filtered).unwrap()),
        vec![hot]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    let filtered_explain = engine.query_graph_rows(&filtered).unwrap().plan.unwrap();
    assert_graph_row_explain_contains(&filtered_explain, "GraphRowSourceRead");
    assert_graph_row_explain_contains(&filtered_explain, "DelegatedEdgeQuery");
    assert_graph_row_explain_contains(&filtered_explain, "VariableLengthPathRuntime");

    let seed = insert_graph_row_node(&engine, "OneHopDelegated", "seed", &[]);
    insert_graph_row_edge(&engine, seed, a, "ONE_HOP_PREFIX", &[]);
    let mut unfiltered = graph_query(
        &["seed", "a", "b"],
        vec![
            graph_edge_with_label(Some("prefix"), "seed", "a", "ONE_HOP_PREFIX"),
            graph_vlp(Some("path"), Some("edge"), "a", "b", 1, 1),
        ],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut unfiltered.pieces[1] {
        path.label_filter = vec!["ONE_HOP_DELEGATED".to_string()];
    }
    unfiltered.nodes[0].ids = vec![seed];
    unfiltered.options.include_plan = true;
    unfiltered.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);
    engine.reset_query_execution_counters_for_test();
    let unfiltered_result = engine.query_graph_rows(&unfiltered).unwrap();
    assert_eq!(unfiltered_result.rows.len(), 2);
    let unfiltered_explain = unfiltered_result.plan.unwrap();
    assert_graph_row_explain_contains(&unfiltered_explain, "VariableLengthPathRuntime");
    assert!(!unfiltered_explain
        .row_ops
        .iter()
        .any(|node| node.kind == "GraphRowSourceRead"));
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        0
    );

    let mut multi_hop = filtered;
    if let GraphPatternPiece::VariableLength(path) = &mut multi_hop.pieces[0] {
        path.max_hops = 2;
        path.edge_alias = None;
    }
    multi_hop.return_items = Some(vec![graph_return_binding(
        "path",
        GraphReturnProjection::IdOnly,
    )]);
    engine.reset_query_execution_counters_for_test();
    let multi_hop_result = engine.query_graph_rows(&multi_hop).unwrap();
    assert_eq!(multi_hop_result.rows.len(), 1);
    let multi_hop_explain = multi_hop_result.plan.unwrap();
    assert_graph_row_explain_contains(
        &multi_hop_explain,
        "step_source=DelegatedEdgeQuery{queries=2; verified_step_edges=1;",
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        2
    );

    let mut unfiltered_multi_hop = unfiltered;
    if let GraphPatternPiece::VariableLength(path) = &mut unfiltered_multi_hop.pieces[1] {
        path.max_hops = 2;
        path.edge_alias = None;
    }
    unfiltered_multi_hop.return_items = Some(vec![graph_return_binding(
        "path",
        GraphReturnProjection::IdOnly,
    )]);
    engine.reset_query_execution_counters_for_test();
    let unfiltered_multi_hop_result = engine.query_graph_rows(&unfiltered_multi_hop).unwrap();
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        0
    );
    assert!(!format!("{:?}", unfiltered_multi_hop_result.plan.unwrap())
        .contains("step_source=DelegatedEdgeQuery"));

    let mut capped = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("path"), Some("edge"), "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut capped.pieces[0] {
        path.label_filter = vec!["ONE_HOP_DELEGATED".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: vec![
                PropValue::String("hot".to_string()),
                PropValue::String("cold".to_string()),
            ],
        });
    }
    capped.nodes[0].ids = vec![a];
    capped.options.max_frontier = 1;
    let message = engine.query_graph_rows(&capped).unwrap_err().to_string();
    assert!(message.contains("max_frontier"), "{message}");
    assert!(message.contains("path=path"), "{message}");
}

#[test]
fn graph_row_vlp_orders_paths_and_enforces_relationship_simple_traversal() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "PathOrder", "path-order-a", &[]);
    let b = insert_graph_row_node(&engine, "PathOrder", "path-order-b", &[]);
    let c = insert_graph_row_node(&engine, "PathOrder", "path-order-c", &[]);
    let open = [("status", PropValue::String("open".to_string()))];
    let ab = insert_graph_row_edge(&engine, a, b, "PATH_ORDER", &open);
    let bc = insert_graph_row_edge(&engine, b, c, "PATH_ORDER", &open);
    let ac = insert_graph_row_edge(&engine, a, c, "PATH_ORDER", &open);
    let ca = insert_graph_row_edge(&engine, c, a, "PATH_ORDER", &open);

    let mut query = graph_query(
        &["a", "z"],
        vec![graph_vlp(Some("p"), None, "a", "z", 0, 3)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["PATH_ORDER".to_string()];
    }
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Binding("p".to_string()),
        direction: GraphOrderDirection::Asc,
    }];

    let expected = vec![
        (vec![a], vec![]),
        (vec![a, b], vec![ab]),
        (vec![a, c], vec![ac]),
        (vec![a, b, c], vec![ab, bc]),
        (vec![a, c, a], vec![ac, ca]),
        (vec![a, b, c, a], vec![ab, bc, ca]),
        (vec![a, c, a, b], vec![ac, ca, ab]),
    ];
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&query).unwrap()),
        expected
    );

    let mut delegated = query;
    if let GraphPatternPiece::VariableLength(path) = &mut delegated.pieces[0] {
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("open".to_string()),
        });
    }
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&delegated).unwrap()),
        expected
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        3,
        "one delegated query executes for each expandable depth"
    );
}

#[test]
fn graph_row_vlp_direction_incoming_both_self_loop_and_parallel_edges_are_logical() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "DirectionPath", "direction-a", &[]);
    let b = insert_graph_row_node(&engine, "DirectionPath", "direction-b", &[]);
    let incoming_edge = insert_graph_row_edge(&engine, b, a, "INCOMING_PATH", &[]);

    let mut incoming = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut incoming.pieces[0] {
        path.direction = Direction::Incoming;
        path.label_filter = vec!["INCOMING_PATH".to_string()];
    }
    incoming.nodes[0].ids = vec![a];
    incoming.nodes[1].ids = vec![b];
    incoming.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&incoming).unwrap()),
        vec![(vec![a, b], vec![incoming_edge])]
    );

    let loop_node = insert_graph_row_node(&engine, "DirectionPath", "direction-loop", &[]);
    let loop_edge = insert_graph_row_edge(&engine, loop_node, loop_node, "BOTH_PATH", &[]);
    let p1 = insert_graph_row_edge(&engine, a, b, "BOTH_PATH", &[]);
    let p2 = insert_graph_row_edge(&engine, a, b, "BOTH_PATH", &[]);

    let mut both_loop = graph_query(
        &["n"],
        vec![graph_vlp(Some("p"), None, "n", "n", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut both_loop.pieces[0] {
        path.direction = Direction::Both;
        path.label_filter = vec!["BOTH_PATH".to_string()];
    }
    both_loop.nodes[0].ids = vec![loop_node];
    both_loop.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&both_loop).unwrap()),
        vec![(vec![loop_node, loop_node], vec![loop_edge])]
    );

    let mut parallel = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut parallel.pieces[0] {
        path.direction = Direction::Both;
        path.label_filter = vec!["BOTH_PATH".to_string()];
    }
    parallel.nodes[0].ids = vec![a];
    parallel.nodes[1].ids = vec![b];
    parallel.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&parallel).unwrap()),
        vec![(vec![a, b], vec![p1]), (vec![a, b], vec![p2])]
    );
}

#[test]
fn graph_row_vlp_filters_temporal_tombstone_prune_and_endpoint_predicates() {
    let (_dir, engine) = graph_row_test_engine();
    let start = insert_graph_row_node(&engine, "VlpFilterStart", "vlp-filter-start", &[]);
    let keep_mid = insert_graph_row_node(&engine, "VlpFilterMid", "vlp-filter-mid-keep", &[]);
    let keep_end = insert_graph_row_node(
        &engine,
        "VlpFilterEnd",
        "vlp-filter-end-keep",
        &[("status", PropValue::String("keep".to_string()))],
    );
    let other_end = insert_graph_row_node(
        &engine,
        "VlpFilterEnd",
        "vlp-filter-end-drop",
        &[("status", PropValue::String("drop".to_string()))],
    );
    let deleted_end = insert_graph_row_node(&engine, "VlpFilterEnd", "vlp-filter-end-deleted", &[]);
    let pruned_end = engine
        .upsert_node(
            "VlpFilterEnd",
            "vlp-filter-end-pruned",
            UpsertNodeOptions {
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    let first = engine
        .upsert_edge(
            start,
            keep_mid,
            "VLP_FILTER",
            UpsertEdgeOptions {
                props: graph_row_props(&[("status", PropValue::String("open".to_string()))]),
                valid_from: Some(0),
                valid_to: Some(i64::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    let second = engine
        .upsert_edge(
            keep_mid,
            keep_end,
            "VLP_FILTER",
            UpsertEdgeOptions {
                props: graph_row_props(&[("status", PropValue::String("open".to_string()))]),
                valid_from: Some(100),
                valid_to: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
    insert_graph_row_edge(
        &engine,
        start,
        other_end,
        "VLP_FILTER",
        &[("status", PropValue::String("closed".to_string()))],
    );
    insert_graph_row_edge(&engine, start, keep_end, "VLP_OTHER_LABEL", &[]);
    let deleted_edge = insert_graph_row_edge(&engine, start, deleted_end, "VLP_FILTER", &[]);
    let pruned_edge = insert_graph_row_edge(&engine, start, pruned_end, "VLP_FILTER", &[]);
    engine.delete_node(deleted_end).unwrap();
    engine.delete_edge(deleted_edge).unwrap();
    engine
        .set_prune_policy(
            "vlp-low-weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("VlpFilterEnd".to_string()),
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["VLP_FILTER".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("open".to_string()),
        });
    }
    query.nodes[0].ids = vec![start];
    query.nodes[1] = graph_node_with_label("b", "VlpFilterEnd");
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    query.at_epoch = Some(150);
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    engine.reset_query_planning_probe_counters_for_test();
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&query).unwrap()),
        vec![(vec![start, keep_mid, keep_end], vec![first, second])]
    );
    assert_eq!(
        engine
            .query_planning_probe_snapshot_for_test()
            .edge_validity_range_estimates,
        0,
        "system VLP ValidAt must remain invisible to planning estimates"
    );

    let mut user_valid_at = query.clone();
    if let GraphPatternPiece::VariableLength(path) = &mut user_valid_at.pieces[0] {
        path.filter = Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("open".to_string()),
            },
            EdgeFilterExpr::ValidAt { epoch_ms: 150 },
        ]));
    }
    engine.reset_query_planning_probe_counters_for_test();
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&user_valid_at).unwrap()),
        vec![(vec![start, keep_mid, keep_end], vec![first, second])]
    );
    assert!(
        engine
            .query_planning_probe_snapshot_for_test()
            .edge_validity_range_estimates
            >= 2,
        "user VLP ValidAt must remain visible to planning estimates"
    );

    query.at_epoch = Some(250);
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());

    engine.remove_prune_policy("vlp-low-weight").unwrap();
    let mut pruned_query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut pruned_query.pieces[0] {
        path.label_filter = vec!["VLP_FILTER".to_string()];
    }
    pruned_query.nodes[0].ids = vec![start];
    pruned_query.nodes[1].ids = vec![pruned_end];
    pruned_query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&pruned_query).unwrap()),
        vec![(vec![start, pruned_end], vec![pruned_edge])]
    );
}

#[test]
fn graph_row_vlp_caps_report_path_context_before_growth() {
    let (_dir, engine) = graph_row_test_engine();
    let start = insert_graph_row_node(&engine, "VlpCap", "vlp-cap-start", &[]);
    let a = insert_graph_row_node(&engine, "VlpCap", "vlp-cap-a", &[]);
    let b = insert_graph_row_node(&engine, "VlpCap", "vlp-cap-b", &[]);
    insert_graph_row_edge(&engine, start, a, "VLP_CAP", &[]);
    insert_graph_row_edge(&engine, start, b, "VLP_CAP", &[]);

    let mut frontier = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut frontier.pieces[0] {
        path.label_filter = vec!["VLP_CAP".to_string()];
    }
    frontier.nodes[0].ids = vec![start];
    frontier.options.max_frontier = 1;
    let err = engine.query_graph_rows(&frontier).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_frontier"));
    assert!(message.contains("configured cap 1"));
    assert!(message.contains("path=p"));

    let mut zero_frontier = frontier.clone();
    zero_frontier.options.max_frontier = 0;
    let zero_frontier_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&zero_frontier).unwrap_err()
    });
    assert!(zero_frontier_error
        .to_string()
        .contains("max_frontier exceeded configured cap 0"));

    let mut paths = frontier.clone();
    paths.options.max_frontier = 10;
    paths.options.max_paths_per_start = 1;
    engine.reset_query_execution_counters_for_test();
    let err = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&paths).unwrap_err()
    });
    let message = err.to_string();
    assert!(message.contains("max_paths_per_start"));
    assert!(message.contains("configured cap 1"));
    assert!(message.contains("path=p"));
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_cap_retries,
        0
    );

    let mut zero_paths = paths.clone();
    zero_paths.options.max_paths_per_start = 0;
    let zero_paths_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&zero_paths).unwrap_err()
    });
    assert!(zero_paths_error
        .to_string()
        .contains("max_paths_per_start exceeded configured cap 0"));

    let mut intermediate = paths;
    intermediate.options.max_paths_per_start = 10;
    intermediate.options.max_intermediate_bindings = 1;
    let err = engine.query_graph_rows(&intermediate).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_intermediate_bindings"));
    assert!(message.contains("configured cap 1"));
    assert!(message.contains("path=p"));
}

#[test]
fn graph_row_vlp_reverse_anchor_counts_paths_per_logical_start() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-a", &[]);
    let b = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-b", &[]);
    let pass_mid_a = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-pass-a", &[]);
    let pass_mid_b = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-pass-b", &[]);
    let fail_mid_a = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-fail-a", &[]);
    let fail_mid_b = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-fail-b", &[]);
    let target = insert_graph_row_node(&engine, "ReverseCap", "reverse-cap-target", &[]);

    let pass_a_first = insert_graph_row_edge(&engine, a, pass_mid_a, "VLP_REVERSE_PASS", &[]);
    let pass_a_second = insert_graph_row_edge(&engine, pass_mid_a, target, "VLP_REVERSE_PASS", &[]);
    let pass_b_first = insert_graph_row_edge(&engine, b, pass_mid_b, "VLP_REVERSE_PASS", &[]);
    let pass_b_second = insert_graph_row_edge(&engine, pass_mid_b, target, "VLP_REVERSE_PASS", &[]);

    insert_graph_row_edge(&engine, a, fail_mid_a, "VLP_REVERSE_FAIL", &[]);
    insert_graph_row_edge(&engine, fail_mid_a, target, "VLP_REVERSE_FAIL", &[]);
    insert_graph_row_edge(&engine, a, fail_mid_b, "VLP_REVERSE_FAIL", &[]);
    insert_graph_row_edge(&engine, fail_mid_b, target, "VLP_REVERSE_FAIL", &[]);

    let mut pass = graph_query(
        &["a", "z"],
        vec![graph_vlp(Some("p"), None, "a", "z", 2, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut pass.pieces[0] {
        path.label_filter = vec!["VLP_REVERSE_PASS".to_string()];
    }
    pass.nodes[1].ids = vec![target];
    pass.options.max_paths_per_start = 1;
    pass.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    pass.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Binding("p".to_string()),
        direction: GraphOrderDirection::Asc,
    }];
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&pass).unwrap()),
        vec![
            (vec![a, pass_mid_a, target], vec![pass_a_first, pass_a_second]),
            (vec![b, pass_mid_b, target], vec![pass_b_first, pass_b_second]),
        ]
    );

    let mut fail = pass;
    if let GraphPatternPiece::VariableLength(path) = &mut fail.pieces[0] {
        path.label_filter = vec!["VLP_REVERSE_FAIL".to_string()];
    }
    let err = engine.query_graph_rows(&fail).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_paths_per_start"));
    assert!(message.contains("configured cap 1"));
    assert!(message.contains("path=p"));
}

#[test]
fn graph_row_vlp_groups_duplicate_bound_searches_without_collapsing_rows() {
    let (_dir, engine) = graph_row_test_engine();
    let root = insert_graph_row_node(&engine, "VlpGroup", "vlp-group-root", &[]);
    let start = insert_graph_row_node(&engine, "VlpGroup", "vlp-group-start", &[]);
    let target = insert_graph_row_node(&engine, "VlpGroup", "vlp-group-target", &[]);
    insert_graph_row_edge(&engine, root, start, "VLP_GROUP_LEFT", &[]);
    insert_graph_row_edge(&engine, root, start, "VLP_GROUP_LEFT", &[]);
    let path_edge = insert_graph_row_edge(
        &engine,
        start,
        target,
        "VLP_GROUP_PATH",
        &[("status", PropValue::String("hot".to_string()))],
    );

    let mut query = graph_query(
        &["root", "a", "z"],
        vec![
            graph_edge_with_label(None, "root", "a", "VLP_GROUP_LEFT"),
            graph_vlp(Some("p"), None, "a", "z", 1, 2),
        ],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[1] {
        path.label_filter = vec!["VLP_GROUP_PATH".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    query.nodes[0].ids = vec![root];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(result.stats.paths_enumerated, 1);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "distinct_search_groups=1");
    assert_graph_row_explain_contains(explain, "search_cache_hits=1");
    assert_graph_row_explain_contains(
        explain,
        "step_source=DelegatedEdgeQuery{queries=2; verified_step_edges=1;",
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 1);
    assert_eq!(
        graph_row_path_ids(result),
        vec![
            (vec![start, target], vec![path_edge]),
            (vec![start, target], vec![path_edge]),
        ]
    );
}

#[test]
fn graph_row_runtime_cache_reuses_one_and_multi_hop_vlp_across_direct_core_calls() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "CacheVlp", "cache-vlp-a", &[]);
    let b = insert_graph_row_node(&engine, "CacheVlp", "cache-vlp-b", &[]);
    let c = insert_graph_row_node(&engine, "CacheVlp", "cache-vlp-c", &[]);
    insert_graph_row_edge(
        &engine,
        a,
        b,
        "CACHE_VLP",
        &[("status", PropValue::String("hot".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        b,
        c,
        "CACHE_VLP",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let view = engine.published_state().view.clone();

    for (min_hops, max_hops) in [(1, 1), (1, 2)] {
        let mut query = graph_query(
            &["a", "z"],
            vec![graph_vlp(Some("p"), None, "a", "z", min_hops, max_hops)],
        );
        query.nodes[0].ids = vec![a];
        if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
            path.label_filter = vec!["CACHE_VLP".to_string()];
            path.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
        let normalized = normalize_graph_row_query(&query).unwrap();
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let physical = view
            .plan_graph_row_physical(&normalized, &runtime, false, true)
            .unwrap();
        let policy_cutoffs = view.query_policy_cutoffs();
        let mut caches =
            GraphRowExecutionCaches::new(normalized.options.max_intermediate_bindings);
        let mut followups = GraphRowUniqueFollowups::new(Vec::new());

        engine.reset_query_execution_counters_for_test();
        let mut first_attempt = caches.begin_attempt();
        let mut cold_paths = 0;
        let cold = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut 0,
                &mut 0,
                &mut cold_paths,
                None,
                &mut caches,
                &mut first_attempt,
            )
            .unwrap();
        caches.commit_attempt(&mut first_attempt);

        engine.reset_query_execution_counters_for_test();
        let mut second_attempt = caches.begin_attempt();
        let mut warm_paths = 0;
        let warm = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut 0,
                &mut 0,
                &mut warm_paths,
                None,
                &mut caches,
                &mut second_attempt,
            )
            .unwrap();
        caches.commit_attempt(&mut second_attempt);

        assert_eq!(warm, cold);
        assert!(cold_paths > 0);
        assert_eq!(warm_paths, 0);
        let warm_counters = engine.query_execution_counter_snapshot_for_test();
        assert!(warm_counters.graph_row_vlp_cross_chunk_cache_hits > 0);
        assert_eq!(warm_counters.graph_row_delegated_edge_queries, 0);
        assert_eq!(warm_counters.graph_row_delegated_verified_candidates, 0);

        let mut no_admit_caches = GraphRowExecutionCaches::new(0);
        let mut no_admit_followups = GraphRowUniqueFollowups::new(Vec::new());
        let mut no_admit_path_counts = Vec::new();
        for _ in 0..2 {
            let mut no_admit_attempt = no_admit_caches.begin_attempt();
            let mut paths = 0;
            let rows = view
                .graph_row_execute_runtime_plan_with_caches(
                    &normalized,
                    &runtime,
                    &physical,
                    None,
                    None,
                    GraphRowRuntimeGoal::AllRows,
                    i64::MAX / 2,
                    policy_cutoffs.as_ref(),
                    &mut no_admit_followups,
                    &mut 0,
                    &mut 0,
                    &mut paths,
                    None,
                    &mut no_admit_caches,
                    &mut no_admit_attempt,
                )
                .unwrap();
            assert_eq!(rows, cold);
            assert!(no_admit_attempt.is_empty());
            no_admit_path_counts.push(paths);
        }
        assert_eq!(no_admit_path_counts, vec![cold_paths, cold_paths]);
        assert!(no_admit_caches.no_admit >= 2);
    }
}

#[test]
fn graph_row_runtime_cache_nested_piece_paths_are_collision_free() {
    let (_dir, engine) = graph_row_test_engine();
    let query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_optional(vec![graph_vlp(None, None, "a", "b", 1, 2)], None),
            graph_optional(vec![graph_vlp(None, None, "c", "d", 1, 2)], None),
        ],
    );
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let mut optional_paths = Vec::new();
    let mut vlp_paths = Vec::new();
    for step in &runtime.steps {
        let GraphRowRuntimeStep::Optional(group) = step else {
            continue;
        };
        optional_paths.push(group.piece_path.to_vec());
        for nested in &group.runtime.steps {
            if let GraphRowRuntimeStep::VariableLength(path) = nested {
                vlp_paths.push(path.piece_path.to_vec());
            }
        }
    }
    assert_eq!(optional_paths, vec![vec![0], vec![1]]);
    assert_eq!(vlp_paths, vec![vec![0, 0], vec![1, 0]]);
}

#[test]
fn graph_row_vlp_delegated_direction_reverse_and_both_mapping_is_exact() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "VlpDirection", "a", &[]);
    let b = insert_graph_row_node(&engine, "VlpDirection", "b", &[]);
    let c = insert_graph_row_node(&engine, "VlpDirection", "c", &[]);
    let outgoing = insert_graph_row_edge(
        &engine,
        a,
        b,
        "VLP_DIRECTION_FILTERED",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let incoming = insert_graph_row_edge(
        &engine,
        c,
        a,
        "VLP_DIRECTION_FILTERED",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let self_loop = insert_graph_row_edge(
        &engine,
        a,
        a,
        "VLP_DIRECTION_FILTERED",
        &[("status", PropValue::String("hot".to_string()))],
    );

    let make_query = |direction: Direction, bind_from: Option<u64>, bind_to: Option<u64>| {
        let mut query = graph_query(
            &["a", "b"],
            vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
        );
        if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
            path.direction = direction;
            path.label_filter = vec!["VLP_DIRECTION_FILTERED".to_string()];
            path.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
        if let Some(id) = bind_from {
            query.nodes[0].ids = vec![id];
        }
        if let Some(id) = bind_to {
            query.nodes[1].ids = vec![id];
        }
        query.return_items = Some(vec![graph_return_binding(
            "p",
            GraphReturnProjection::IdOnly,
        )]);
        query
    };

    let outgoing_rows = graph_row_path_ids(
        engine
            .query_graph_rows(&make_query(Direction::Outgoing, Some(a), None))
            .unwrap(),
    );
    assert!(outgoing_rows.contains(&(vec![a, b], vec![outgoing])));
    assert!(outgoing_rows.contains(&(vec![a, a], vec![self_loop])));
    assert!(!outgoing_rows.iter().any(|(_, edges)| edges == &vec![incoming]));

    let incoming_rows = graph_row_path_ids(
        engine
            .query_graph_rows(&make_query(Direction::Incoming, Some(a), None))
            .unwrap(),
    );
    assert!(incoming_rows.contains(&(vec![a, c], vec![incoming])));
    assert!(incoming_rows.contains(&(vec![a, a], vec![self_loop])));
    assert!(!incoming_rows.iter().any(|(_, edges)| edges == &vec![outgoing]));

    let both_rows = graph_row_path_ids(
        engine
            .query_graph_rows(&make_query(Direction::Both, Some(a), None))
            .unwrap(),
    );
    assert!(both_rows.contains(&(vec![a, b], vec![outgoing])));
    assert!(both_rows.contains(&(vec![a, c], vec![incoming])));
    assert_eq!(
        both_rows
            .iter()
            .filter(|(_, edges)| edges == &vec![self_loop])
            .count(),
        1
    );

    let reverse_outgoing = graph_row_path_ids(
        engine
            .query_graph_rows(&make_query(Direction::Outgoing, None, Some(b)))
            .unwrap(),
    );
    assert!(reverse_outgoing.contains(&(vec![a, b], vec![outgoing])));
    let reverse_incoming = graph_row_path_ids(
        engine
            .query_graph_rows(&make_query(Direction::Incoming, None, Some(c)))
            .unwrap(),
    );
    assert!(reverse_incoming.contains(&(vec![a, c], vec![incoming])));
    let reverse_both = graph_row_path_ids(
        engine
            .query_graph_rows(&make_query(Direction::Both, None, Some(b)))
            .unwrap(),
    );
    assert!(reverse_both.contains(&(vec![a, b], vec![outgoing])));
}

#[test]
fn graph_row_vlp_delegated_verified_cap_and_empty_boundaries_are_truthful() {
    let (_dir, engine) = graph_row_test_engine();
    let start = insert_graph_row_node(&engine, "VlpDelegatedCap", "start", &[]);
    let hot_target = insert_graph_row_node(&engine, "VlpDelegatedCap", "hot", &[]);
    let cold_target = insert_graph_row_node(&engine, "VlpDelegatedCap", "cold", &[]);
    let hot = insert_graph_row_edge(
        &engine,
        start,
        hot_target,
        "VLP_DELEGATED_CAP",
        &[("status", PropValue::String("hot".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        start,
        cold_target,
        "VLP_DELEGATED_CAP",
        &[("status", PropValue::String("cold".to_string()))],
    );
    let mut query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["VLP_DELEGATED_CAP".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    query.nodes[0].ids = vec![start];
    query.options.max_frontier = 1;
    query.return_items = Some(vec![graph_return_binding(
        "p",
        GraphReturnProjection::IdOnly,
    )]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&query).unwrap()),
        vec![(vec![start, hot_target], vec![hot])]
    );

    insert_graph_row_edge(
        &engine,
        start,
        cold_target,
        "VLP_DELEGATED_CAP",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let message = engine.query_graph_rows(&query).unwrap_err().to_string();
    assert!(message.contains("max_frontier"), "{message}");
    assert!(message.contains("path=p"), "{message}");

    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.filter = Some(EdgeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: Vec::new(),
        });
    }
    engine.reset_query_execution_counters_for_test();
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        0
    );

    query.nodes[0].ids = vec![u64::MAX];
    engine.reset_query_execution_counters_for_test();
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        0
    );
}

#[test]
fn graph_row_vlp_delegated_multi_label_trace_is_aggregated_and_deterministic() {
    let (_dir, engine) = graph_row_test_engine();
    let start = insert_graph_row_node(&engine, "VlpMultiLabel", "start", &[]);
    let left = insert_graph_row_node(&engine, "VlpMultiLabel", "left", &[]);
    let right = insert_graph_row_node(&engine, "VlpMultiLabel", "right", &[]);
    let left_edge = insert_graph_row_edge(
        &engine,
        start,
        left,
        "VLP_MULTI_LABEL_A",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let right_edge = insert_graph_row_edge(
        &engine,
        start,
        right,
        "VLP_MULTI_LABEL_B",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let mut query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec![
            "VLP_MULTI_LABEL_B".to_string(),
            "VLP_MULTI_LABEL_A".to_string(),
        ];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    query.nodes[0].ids = vec![start];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "p",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let first = engine.query_graph_rows(&query).unwrap();
    let first_detail = first
        .plan
        .as_ref()
        .unwrap()
        .plan
        .iter()
        .find(|node| node.kind == "VariableLengthPathRuntime")
        .unwrap()
        .detail
        .clone();
    assert_eq!(
        graph_row_path_ids(first),
        vec![
            (vec![start, left], vec![left_edge]),
            (vec![start, right], vec![right_edge]),
        ]
    );
    assert!(first_detail.contains(
        "step_source=DelegatedEdgeQuery{queries=4; verified_step_edges=2; planned_modes="
    ));
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 4);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 2);
    assert_eq!(counters.edge_verifier_metadata_lookup_calls, 2);

    let second = engine.query_graph_rows(&query).unwrap();
    let second_detail = &second
        .plan
        .as_ref()
        .unwrap()
        .plan
        .iter()
        .find(|node| node.kind == "VariableLengthPathRuntime")
        .unwrap()
        .detail;
    assert_eq!(&first_detail, second_detail);
}

#[test]
fn graph_row_vlp_path_output_hydrates_after_page_and_dedupes_elements() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(
        &engine,
        "HydratePath",
        "hydrate-a",
        &[("name", PropValue::String("a".to_string()))],
    );
    let b = insert_graph_row_node(
        &engine,
        "HydratePath",
        "hydrate-b",
        &[("name", PropValue::String("b".to_string()))],
    );
    let ab = insert_graph_row_edge(
        &engine,
        a,
        b,
        "HYDRATE_PATH",
        &[("kind", PropValue::String("ab".to_string()))],
    );
    let ba = insert_graph_row_edge(
        &engine,
        b,
        a,
        "HYDRATE_PATH",
        &[("kind", PropValue::String("ba".to_string()))],
    );

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 2, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["HYDRATE_PATH".to_string()];
    }
    query.nodes[0].ids = vec![a];
    query.nodes[1].ids = vec![a];
    query.return_items = Some(vec![
        GraphReturnItem {
            expr: GraphExpr::Binding("p".to_string()),
            alias: Some("p".to_string()),
            projection: GraphReturnProjection::Selected(GraphSelectedProjection::Path(
                GraphSelectedPathProjection {
                    node_ids: true,
                    edge_ids: true,
                    nodes: Some(selected_node(
                        GraphPropertySelection::Keys(vec!["name".to_string()]),
                        GraphVectorSelection::None,
                    )),
                    edges: Some(selected_edge(GraphPropertySelection::Keys(vec![
                        "kind".to_string(),
                    ]))),
                },
            )),
        },
    ]);
    query.output.mode = GraphOutputMode::Projected;
    query.output.include_vectors = false;

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 2);
    assert_eq!(counters.edge_selected_field_batches, 1);
    assert_eq!(counters.edge_selected_field_ids, 2);

    let row = &result.rows[0].values;
    let GraphValue::Path(path) = &row[0] else {
        panic!("expected path output");
    };
    assert_eq!(path.node_ids, vec![a, b, a]);
    assert_eq!(path.edge_ids, vec![ab, ba]);
    let nodes = path.nodes.as_ref().unwrap();
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].id, Some(a));
    assert_eq!(nodes[0].dense_vector, None);
    assert_eq!(
        nodes[0]
            .props
            .as_ref()
            .unwrap()
            .get("name"),
        Some(&GraphValue::String("a".to_string()))
    );
    let edges = path.edges.as_ref().unwrap();
    assert_eq!(edges[0].id, Some(ab));
    assert_eq!(
        edges[0]
            .props
            .as_ref()
            .unwrap()
            .get("kind"),
        Some(&GraphValue::String("ab".to_string()))
    );

    let mut function_query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 2, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut function_query.pieces[0] {
        path.label_filter = vec!["HYDRATE_PATH".to_string()];
    }
    function_query.nodes[0].ids = vec![a];
    function_query.nodes[1].ids = vec![a];
    function_query.output.mode = GraphOutputMode::Elements;
    function_query.return_items = Some(vec![
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::Length,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            "len",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::StartNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            "start",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::EndNode,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            "end",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::Nodes,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            "nodes",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::Relationships,
                args: vec![GraphExpr::Binding("p".to_string())],
            },
            "relationships",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::Size,
                args: vec![GraphExpr::Function {
                    name: GraphFunction::Nodes,
                    args: vec![GraphExpr::Binding("p".to_string())],
                }],
            },
            "node_count",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::Size,
                args: vec![GraphExpr::Function {
                    name: GraphFunction::Relationships,
                    args: vec![GraphExpr::Binding("p".to_string())],
                }],
            },
            "edge_count",
        ),
        graph_return_expr(
            GraphExpr::Function {
                name: GraphFunction::Size,
                args: vec![GraphExpr::List(vec![GraphExpr::Binding("a".to_string())])],
            },
            "literal_node_list_count",
        ),
    ]);
    let function_values = &engine.query_graph_rows(&function_query).unwrap().rows[0].values;
    assert_eq!(function_values[0], GraphValue::UInt(2));
    assert!(matches!(function_values[1], GraphValue::Node(_)));
    assert!(matches!(function_values[2], GraphValue::Node(_)));
    assert!(matches!(function_values[3], GraphValue::List(_)));
    assert!(matches!(function_values[4], GraphValue::List(_)));
    assert_eq!(function_values[5], GraphValue::UInt(3));
    assert_eq!(function_values[6], GraphValue::UInt(2));
    assert_eq!(function_values[7], GraphValue::UInt(1));
}

#[test]
fn graph_row_vlp_cursor_pagination_matches_unpaged_order() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "CursorPath", "cursor-a", &[]);
    let b = insert_graph_row_node(&engine, "CursorPath", "cursor-b", &[]);
    let c = insert_graph_row_node(&engine, "CursorPath", "cursor-c", &[]);
    let hot = [("status", PropValue::String("hot".to_string()))];
    let ab = insert_graph_row_edge(&engine, a, b, "CURSOR_PATH", &hot);
    let ac = insert_graph_row_edge(&engine, a, c, "CURSOR_PATH", &hot);
    let bc = insert_graph_row_edge(&engine, b, c, "CURSOR_PATH", &hot);

    let mut query = graph_query(
        &["a", "z"],
        vec![graph_vlp(Some("p"), None, "a", "z", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["CURSOR_PATH".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    query.nodes[0].ids = vec![a];
    query.page.limit = 10;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    engine.reset_query_execution_counters_for_test();
    let unpaged_result = engine.query_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        unpaged_result.plan.as_ref().unwrap(),
        "step_source=DelegatedEdgeQuery",
    );
    assert!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries
            > 0
    );
    let unpaged = graph_row_path_ids(unpaged_result);
    assert_eq!(
        unpaged,
        vec![
            (vec![a, b], vec![ab]),
            (vec![a, c], vec![ac]),
            (vec![a, b, c], vec![ab, bc]),
        ]
    );

    query.page.limit = 1;
    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        query.page.cursor = cursor.take();
        engine.reset_query_execution_counters_for_test();
        let page = engine.query_graph_rows(&query).unwrap();
        assert!(
            engine
                .query_execution_counter_snapshot_for_test()
                .graph_row_delegated_edge_queries
                > 0
        );
        assert_graph_row_explain_contains(
            page.plan.as_ref().unwrap(),
            "step_source=DelegatedEdgeQuery",
        );
        let next_cursor = page.next_cursor.clone();
        paged.extend(graph_row_path_ids(page));
        match next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(paged, unpaged);
}

#[test]
fn graph_row_vlp_optional_and_null_dependency_semantics_match_fixed_optional() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "OptionalPath", "optional-path-a", &[]);
    let b = insert_graph_row_node(&engine, "OptionalPath", "optional-path-b", &[]);
    let c = insert_graph_row_node(&engine, "OptionalPath", "optional-path-c", &[]);
    let d = insert_graph_row_node(&engine, "OptionalPath", "optional-path-d", &[]);
    let hot = [("status", PropValue::String("hot".to_string()))];
    let _ab = insert_graph_row_edge(&engine, a, b, "OPTIONAL_PATH_HIT", &hot);
    let bc = insert_graph_row_edge(&engine, b, c, "OPTIONAL_PATH_HIT", &hot);
    insert_graph_row_edge(&engine, a, b, "OPTIONAL_REQUIRED", &[]);

    let mut hit = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "OPTIONAL_REQUIRED"),
            graph_optional(vec![graph_vlp(Some("p"), None, "b", "c", 1, 2)], None),
        ],
    );
    if let GraphPatternPiece::Optional(group) = &mut hit.pieces[1] {
        if let GraphPatternPiece::VariableLength(path) = &mut group.pieces[0] {
            path.label_filter = vec!["OPTIONAL_PATH_HIT".to_string()];
            path.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
    }
    hit.nodes[0].ids = vec![a];
    hit.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&hit).unwrap()),
        vec![(vec![b, c], vec![bc])]
    );
    assert!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries
            > 0
    );

    let mut miss = hit.clone();
    miss.nodes[2].ids = vec![d];
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&miss).unwrap()),
        vec![vec![GraphValue::Null]]
    );

    let mut null_dependency = graph_query(
        &["a", "b", "c"],
        vec![
            graph_optional(
                vec![graph_edge_with_label(
                    Some("r"),
                    "a",
                    "b",
                    "OPTIONAL_PATH_MISSING",
                )],
                None,
            ),
            graph_optional(vec![graph_vlp(Some("p"), None, "b", "c", 1, 1)], None),
        ],
    );
    if let GraphPatternPiece::Optional(group) = &mut null_dependency.pieces[1] {
        if let GraphPatternPiece::VariableLength(path) = &mut group.pieces[0] {
            path.label_filter = vec!["OPTIONAL_PATH_HIT".to_string()];
            path.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
    }
    null_dependency.nodes[0].ids = vec![a];
    null_dependency.return_items = Some(vec![
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("p", GraphReturnProjection::IdOnly),
    ]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&null_dependency).unwrap()),
        vec![vec![GraphValue::Null, GraphValue::Null]]
    );

    let mut required_after_null = null_dependency.clone();
    required_after_null.pieces.push(graph_vlp(Some("q"), None, "b", "c", 1, 1));
    if let GraphPatternPiece::VariableLength(path) = &mut required_after_null.pieces[2] {
        path.label_filter = vec!["OPTIONAL_PATH_HIT".to_string()];
    }
    assert!(engine
        .query_graph_rows(&required_after_null)
        .unwrap()
        .rows
        .is_empty());

}

#[test]
fn graph_row_vlp_explain_reports_bounds_caps_source_verification_and_runtime_stats() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "ExplainPath", "explain-path-a", &[]);
    let b = insert_graph_row_node(&engine, "ExplainPath", "explain-path-b", &[]);
    insert_graph_row_edge(&engine, a, b, "EXPLAIN_PATH", &[]);

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.direction = Direction::Both;
        path.label_filter = vec!["EXPLAIN_PATH".to_string()];
    }
    query.nodes[0].ids = vec![a];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);

    let result = engine.query_graph_rows(&query).unwrap();
    let explain = result.plan.unwrap();
    assert_graph_row_explain_contains(&explain, "VariableLengthPath");
    assert_graph_row_explain_contains(&explain, "min_hops=1");
    assert_graph_row_explain_contains(&explain, "max_hops=2");
    assert_graph_row_explain_contains(&explain, "direction=Both");
    assert_graph_row_explain_contains(&explain, "relationship_simple=true");
    assert_graph_row_explain_contains(&explain, "max_frontier");
    assert_graph_row_explain_contains(&explain, "source_verification=latest_visible_edges");
    assert_graph_row_explain_contains(&explain, "VariableLengthPathRuntime");
    assert!(result.stats.paths_enumerated > 0);
}

#[test]
fn graph_row_page_limit_and_cursor_caps_are_validated() {
    let mut zero = graph_query(&["a"], Vec::new());
    zero.page.limit = 0;
    assert_graph_row_invalid(&zero, "page limit must be > 0");

    let mut over_limit = graph_query(&["a"], Vec::new());
    over_limit.page.limit = 11;
    over_limit.options.max_page_limit = 10;
    assert_graph_row_invalid(&over_limit, "exceeds max_page_limit 10");

    let mut cursor = graph_query(&["a"], Vec::new());
    cursor.options.max_cursor_bytes = 4;
    cursor.page.cursor = Some(format!("{GRAPH_ROW_CURSOR_PREFIX}{}", "A".repeat(16)));
    let err = normalize_graph_row_query(&cursor).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
    assert!(
        err.to_string()
            .contains("too large to decode within max_cursor_bytes 4"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_row_emitted_cursor_respects_max_cursor_bytes() {
    let (_dir, engine) = graph_row_test_engine();
    insert_graph_row_node(&engine, "GRAPH_ROW_CURSOR_EMIT_CAP", "emit-cap-1", &[]);
    insert_graph_row_node(&engine, "GRAPH_ROW_CURSOR_EMIT_CAP", "emit-cap-2", &[]);

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_CURSOR_EMIT_CAP");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.page.limit = 1;
    query.options.max_cursor_bytes = 16;

    engine.reset_query_execution_counters_for_test();
    let err = engine.query_graph_rows(&query).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
    assert!(
        err.to_string().contains("emitted graph row cursor payload")
            && err.to_string().contains("max_cursor_bytes 16"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_row_anchor_rules_reject_obvious_full_scans() {
    let mut no_piece = graph_query(&["a"], Vec::new());
    no_piece.options.allow_full_scan = false;
    assert_graph_row_invalid(&no_piece, "requires an anchor or allow_full_scan=true");

    let mut anchored_node = graph_query(&["a"], Vec::new());
    anchored_node.nodes[0] = graph_node_with_label("a", "Person");
    anchored_node.options.allow_full_scan = false;
    normalize_graph_row_query(&anchored_node).unwrap();

    let mut cartesian = graph_query(&["a", "b"], Vec::new());
    cartesian.options.allow_full_scan = true;
    assert_graph_row_invalid(&cartesian, "multiple unconnected node aliases");

    let mut unanchored_edge = graph_query(&["a", "b"], vec![graph_edge(Some("e"), "a", "b")]);
    unanchored_edge.options.allow_full_scan = false;
    assert_graph_row_invalid(&unanchored_edge, "required edge pattern requires an anchor");

    let mut anchored_edge = graph_query(&["a", "b"], vec![graph_edge(Some("e"), "a", "b")]);
    anchored_edge.nodes[0] = graph_node_with_label("a", "Person");
    anchored_edge.options.allow_full_scan = false;
    normalize_graph_row_query(&anchored_edge).unwrap();

    let mut uncorrelated_optional = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Optional(GraphOptionalGroup {
            pieces: vec![graph_edge(Some("e"), "a", "b")],
            where_: None,
        })],
    );
    uncorrelated_optional.options.allow_full_scan = false;
    assert_graph_row_invalid(&uncorrelated_optional, "optional group requires correlation");
}

#[test]
fn graph_row_filter_only_unindexed_anchors_fail_clearly_without_full_scan() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(
        &engine,
        "Person",
        "filter-anchor-source",
        &[("status", PropValue::String("active".to_string()))],
    );
    let target = insert_graph_row_node(&engine, "Person", "filter-anchor-target", &[]);
    insert_graph_row_edge(
        &engine,
        source,
        target,
        "FILTER_ONLY_EDGE",
        &[("status", PropValue::String("active".to_string()))],
    );

    let mut node_query = graph_query(&["n"], Vec::new());
    node_query.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("active".to_string()),
    });
    node_query.options.allow_full_scan = false;
    node_query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    let node_err = engine.query_graph_rows(&node_query).unwrap_err();
    assert!(
        node_err
            .to_string()
            .contains("node query requires label_filter, ids, keys, or allow_full_scan"),
        "unexpected node error: {node_err}"
    );

    let mut edge_query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: Vec::new(),
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }),
        })],
    );
    edge_query.options.allow_full_scan = false;
    edge_query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    let edge_err = engine.query_graph_rows(&edge_query).unwrap_err();
    assert!(
        edge_err
            .to_string()
            .contains("graph row required edge pattern requires an anchor or allow_full_scan=true"),
        "unexpected edge error: {edge_err}"
    );
}

#[test]
fn graph_row_required_fixed_patterns_must_be_connected() {
    let disconnected_node = graph_query(
        &["a", "b", "c"],
        vec![graph_edge(Some("r"), "a", "b")],
    );
    assert_graph_row_invalid(
        &disconnected_node,
        "required fixed patterns must be connected",
    );

    let disconnected_edges = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge(Some("r"), "a", "b"),
            graph_edge(Some("s"), "c", "d"),
        ],
    );
    assert_graph_row_invalid(
        &disconnected_edges,
        "required fixed patterns must be connected",
    );

    let connected = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge(Some("r"), "a", "b"),
            graph_edge(Some("s"), "b", "c"),
        ],
    );
    normalize_graph_row_query(&connected).unwrap();
}

#[test]
fn graph_row_optional_filters_cannot_reference_later_edge_or_path_aliases() {
    let query = graph_query(
        &["a", "b", "c"],
        vec![
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![graph_edge(Some("oe"), "a", "b")],
                where_: Some(GraphExpr::Binding("later".to_string())),
            }),
            graph_edge(Some("later"), "b", "c"),
        ],
    );

    assert_graph_row_invalid(&query, "unknown alias 'later'");

    let later_node = graph_query(
        &["a", "b", "c", "d"],
        vec![
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![graph_edge(Some("oe"), "a", "b")],
                where_: Some(GraphExpr::Property {
                    alias: "d".to_string(),
                    key: "status".to_string(),
                }),
            }),
            graph_edge(Some("later_edge"), "c", "d"),
        ],
    );
    assert_graph_row_invalid(&later_node, "unknown alias 'd'");
}

#[test]
fn graph_row_selected_vector_projection_requires_include_vectors() {
    let selected_node = GraphReturnProjection::Selected(GraphSelectedProjection::Node(
        GraphSelectedNodeProjection {
            id: true,
            labels: false,
            key: false,
            props: GraphPropertySelection::None,
            weight: false,
            created_at: false,
            updated_at: false,
            vectors: GraphVectorSelection::Dense,
        },
    ));
    let mut query = graph_query(&["a"], Vec::new());
    query.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binding("a".to_string()),
        alias: Some("a".to_string()),
        projection: selected_node,
    }]);

    assert_graph_row_invalid(&query, "selected vector projection requires include_vectors=true");

    query.output.include_vectors = true;
    normalize_graph_row_query(&query).unwrap();
}

#[test]
fn graph_row_functions_validate_arity_and_argument_kind() {
    let mut wrong_arity = graph_query(&["a"], Vec::new());
    wrong_arity.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Labels,
            args: Vec::new(),
        },
        alias: Some("labels".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    assert_graph_row_invalid(&wrong_arity, "function labels expects exactly one argument");

    let mut wrong_kind = graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)]);
    wrong_kind.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Length,
            args: vec![GraphExpr::Binding("a".to_string())],
        },
        alias: Some("length".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    assert_graph_row_invalid(&wrong_kind, "function length expects a path, got a node");

    let mut valid_path_function =
        graph_query(&["a", "b"], vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)]);
    valid_path_function.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Length,
            args: vec![GraphExpr::Binding("p".to_string())],
        },
        alias: Some("length".to_string()),
        projection: GraphReturnProjection::Auto,
    }]);
    normalize_graph_row_query(&valid_path_function).unwrap();
}

#[test]
fn graph_row_return_projection_rejects_obvious_kind_mismatches() {
    let selected_node = GraphReturnProjection::Selected(GraphSelectedProjection::Node(
        GraphSelectedNodeProjection {
            id: true,
            labels: false,
            key: false,
            props: GraphPropertySelection::None,
            weight: false,
            created_at: false,
            updated_at: false,
            vectors: GraphVectorSelection::None,
        },
    ));
    let mut edge_as_node = graph_query(&["a", "b"], vec![graph_edge(Some("e"), "a", "b")]);
    edge_as_node.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Binding("e".to_string()),
        alias: Some("e".to_string()),
        projection: selected_node,
    }]);
    assert_graph_row_invalid(&edge_as_node, "selected node projection expects a node");

    let mut scalar_as_element = graph_query(&["a"], Vec::new());
    scalar_as_element.return_items = Some(vec![GraphReturnItem {
        expr: GraphExpr::Property {
            alias: "a".to_string(),
            key: "name".to_string(),
        },
        alias: Some("name".to_string()),
        projection: GraphReturnProjection::Element(GraphElementProjection::Full),
    }]);
    assert_graph_row_invalid(
        &scalar_as_element,
        "element projection expects a node, edge, or path",
    );
}

#[test]
fn graph_row_order_over_obvious_list_or_map_is_rejected() {
    let mut query = graph_query(&["a"], Vec::new());
    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::List(vec![GraphExpr::Int(1)]),
        direction: GraphOrderDirection::Asc,
    }];

    assert_graph_row_invalid(&query, "order expression must not be a list or map value");

    let mut computed = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    computed.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Nodes,
            args: vec![GraphExpr::Binding("p".to_string())],
        },
        direction: GraphOrderDirection::Asc,
    }];
    assert_graph_row_invalid(
        &computed,
        "order expression must not be a list or map value",
    );

    computed.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Function {
            name: GraphFunction::Relationships,
            args: vec![GraphExpr::Binding("p".to_string())],
        },
        direction: GraphOrderDirection::Asc,
    }];
    assert_graph_row_invalid(
        &computed,
        "order expression must not be a list or map value",
    );

    let mut labels = graph_query(&["a"], Vec::new());
    labels.order_by = vec![GraphOrderItem {
        expr: GraphExpr::NodeField {
            alias: "a".to_string(),
            field: GraphNodeField::Labels,
        },
        direction: GraphOrderDirection::Asc,
    }];
    assert_graph_row_invalid(&labels, "order expression must not be a list or map value");

    let mut case_list = graph_query(&["a"], Vec::new());
    case_list.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Case {
            operand: None,
            branches: vec![GraphCaseBranch {
                when: GraphExpr::Bool(true),
                then: GraphExpr::List(vec![GraphExpr::Int(1)]),
            }],
            else_expr: Some(Box::new(GraphExpr::Int(2))),
        },
        direction: GraphOrderDirection::Asc,
    }];
    assert_graph_row_invalid(&case_list, "order expression must not be a list or map value");
}

#[test]
fn graph_row_scalar_operators_reject_obvious_graph_element_operands() {
    let mut neg_node = graph_query(&["a"], Vec::new());
    neg_node.return_items = Some(vec![graph_return_expr(
        GraphExpr::Unary {
            op: GraphUnaryOp::Neg,
            expr: Box::new(GraphExpr::Binding("a".to_string())),
        },
        "bad",
    )]);
    assert_graph_row_invalid(
        &neg_node,
        "operator - expects scalar operands, got a node",
    );

    let mut string_predicate_node = graph_query(&["a"], Vec::new());
    string_predicate_node.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::Binding("a".to_string())),
        op: GraphBinaryOp::StartsWith,
        right: Box::new(GraphExpr::String("a".to_string())),
    });
    assert_graph_row_invalid(
        &string_predicate_node,
        "operator STARTS WITH expects scalar operands, got a node",
    );

    let mut coalesce_case_node = graph_query(&["a"], Vec::new());
    coalesce_case_node.return_items = Some(vec![graph_return_expr(
        GraphExpr::Function {
            name: GraphFunction::Coalesce,
            args: vec![
                GraphExpr::Case {
                    operand: None,
                    branches: vec![GraphCaseBranch {
                        when: GraphExpr::Bool(true),
                        then: GraphExpr::Binding("a".to_string()),
                    }],
                    else_expr: Some(Box::new(GraphExpr::Null)),
                },
                GraphExpr::String("fallback".to_string()),
            ],
        },
        "bad",
    )]);
    assert_graph_row_invalid(
        &coalesce_case_node,
        "function coalesce expects scalar, list, map, or null input, got a node",
    );
}

#[test]
fn graph_row_executes_node_only_query_over_visible_nodes() {
    let (_dir, engine) = graph_row_test_engine();
    let alice = insert_graph_row_node(&engine, "Person", "node-only-alice", &[]);
    let bob = insert_graph_row_node(&engine, "Person", "node-only-bob", &[]);
    insert_graph_row_node(&engine, "Company", "node-only-acme", &[]);

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "Person");
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);

    query.options.include_plan = true;
    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![alice, bob]);
    assert_graph_row_explain_contains(
        result.plan.as_ref().unwrap(),
        "node-only default-order fast path candidate source",
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 0);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 0);
    assert_eq!(counters.edge_verifier_metadata_lookup_calls, 0);
}

#[test]
fn graph_row_executes_one_edge_fixed_pattern_in_id_mode() {
    let (_dir, engine) = graph_row_test_engine();
    let alice = insert_graph_row_node(&engine, "Person", "one-edge-alice", &[]);
    let bob = insert_graph_row_node(&engine, "Person", "one-edge-bob", &[]);
    let edge = insert_graph_row_edge(&engine, alice, bob, "KNOWS", &[]);

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "KNOWS")],
    );
    query.return_items = Some(vec![
        graph_return_binding("a", GraphReturnProjection::IdOnly),
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("b", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::NodeId(alice),
            GraphValue::EdgeId(edge),
            GraphValue::NodeId(bob),
        ]]
    );
}

#[test]
fn graph_row_optional_hit_binds_introduced_node_and_edge_aliases() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-hit-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-hit-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-hit-c", &[]);
    let required = insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_REQUIRED", &[]);
    let optional = insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_OPTIONAL_HIT", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_HIT",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::EdgeId(required),
            GraphValue::EdgeId(optional),
            GraphValue::NodeId(c),
        ]]
    );
}

#[test]
fn graph_row_optional_miss_emits_one_null_extended_row_and_preserves_outer_aliases() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-miss-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-miss-b", &[]);
    let required = insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_MISS_REQUIRED", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_MISS_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_MISSING",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("a", GraphReturnProjection::IdOnly),
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("b", GraphReturnProjection::IdOnly),
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::NodeId(a),
            GraphValue::EdgeId(required),
            GraphValue::NodeId(b),
            GraphValue::Null,
            GraphValue::Null,
        ]]
    );
}

#[test]
fn graph_row_optional_multiple_hits_preserve_bag_multiplication() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-multi-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-multi-b", &[]);
    let c1 = insert_graph_row_node(&engine, "Company", "optional-multi-c1", &[]);
    let c2 = insert_graph_row_node(&engine, "Company", "optional-multi-c2", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_MULTI_REQUIRED", &[]);
    let s1 = insert_graph_row_edge(&engine, b, c1, "GRAPH_ROW_OPTIONAL_MULTI", &[]);
    let s2 = insert_graph_row_edge(&engine, b, c2, "GRAPH_ROW_OPTIONAL_MULTI", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_MULTI_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_MULTI",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![
            vec![GraphValue::EdgeId(s1), GraphValue::NodeId(c1)],
            vec![GraphValue::EdgeId(s2), GraphValue::NodeId(c2)],
        ]
    );
}

#[test]
fn graph_row_optional_nested_outer_miss_nulls_outer_and_nested_aliases() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-nested-outer-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-nested-outer-b", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_NESTED_OUTER_REQUIRED", &[]);

    let mut query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge_with_label(
                Some("r"),
                "a",
                "b",
                "GRAPH_ROW_OPTIONAL_NESTED_OUTER_REQUIRED",
            ),
            graph_optional(
                vec![
                    graph_edge_with_label(
                        Some("s"),
                        "b",
                        "c",
                        "GRAPH_ROW_OPTIONAL_NESTED_OUTER_MISSING",
                    ),
                    graph_optional(
                        vec![graph_edge_with_label(
                            Some("t"),
                            "c",
                            "d",
                            "GRAPH_ROW_OPTIONAL_NESTED_INNER",
                        )],
                        None,
                    ),
                ],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
        graph_return_binding("t", GraphReturnProjection::IdOnly),
        graph_return_binding("d", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::Null,
            GraphValue::Null,
            GraphValue::Null,
            GraphValue::Null,
        ]]
    );
}

#[test]
fn graph_row_optional_nested_inner_miss_nulls_only_inner_aliases() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-nested-inner-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-nested-inner-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-nested-inner-c", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_NESTED_INNER_REQUIRED", &[]);
    let s = insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_OPTIONAL_NESTED_OUTER_HIT", &[]);

    let mut query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge_with_label(
                Some("r"),
                "a",
                "b",
                "GRAPH_ROW_OPTIONAL_NESTED_INNER_REQUIRED",
            ),
            graph_optional(
                vec![
                    graph_edge_with_label(
                        Some("s"),
                        "b",
                        "c",
                        "GRAPH_ROW_OPTIONAL_NESTED_OUTER_HIT",
                    ),
                    graph_optional(
                        vec![graph_edge_with_label(
                            Some("t"),
                            "c",
                            "d",
                            "GRAPH_ROW_OPTIONAL_NESTED_INNER_MISSING",
                        )],
                        None,
                    ),
                ],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
        graph_return_binding("t", GraphReturnProjection::IdOnly),
        graph_return_binding("d", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::EdgeId(s),
            GraphValue::NodeId(c),
            GraphValue::Null,
            GraphValue::Null,
        ]]
    );
}

#[test]
fn graph_row_optional_chained_groups_handle_null_and_hit_dependencies() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-chain-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-chain-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-chain-c", &[]);
    let d = insert_graph_row_node(&engine, "Topic", "optional-chain-d", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_CHAIN_REQUIRED", &[]);
    let s = insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_OPTIONAL_CHAIN_FIRST", &[]);
    let t = insert_graph_row_edge(&engine, c, d, "GRAPH_ROW_OPTIONAL_CHAIN_SECOND", &[]);

    let mut query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_CHAIN_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_CHAIN_FIRST",
                )],
                None,
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("t"),
                    "c",
                    "d",
                    "GRAPH_ROW_OPTIONAL_CHAIN_SECOND",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
        graph_return_binding("t", GraphReturnProjection::IdOnly),
        graph_return_binding("d", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::EdgeId(s),
            GraphValue::NodeId(c),
            GraphValue::EdgeId(t),
            GraphValue::NodeId(d),
        ]]
    );

    engine.delete_edge(t).unwrap();
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::EdgeId(s),
            GraphValue::NodeId(c),
            GraphValue::Null,
            GraphValue::Null,
        ]]
    );

    engine.delete_edge(s).unwrap();
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::Null,
            GraphValue::Null,
            GraphValue::Null,
            GraphValue::Null,
        ]]
    );
}

#[test]
fn graph_row_optional_filters_turn_all_rejected_candidates_into_misses() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-filter-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-filter-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-filter-c", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_FILTER_REQUIRED", &[]);
    insert_graph_row_edge(
        &engine,
        b,
        c,
        "GRAPH_ROW_OPTIONAL_FILTER_EDGE",
        &[("status", PropValue::String("inactive".to_string()))],
    );

    let mut optional_edge = match graph_edge_with_label(
        Some("s"),
        "b",
        "c",
        "GRAPH_ROW_OPTIONAL_FILTER_EDGE",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    optional_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("active".to_string()),
    });
    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_FILTER_REQUIRED"),
            graph_optional(vec![GraphPatternPiece::Edge(optional_edge)], None),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::Null]]
    );

    let mut where_edge = match graph_edge_with_label(
        Some("s"),
        "b",
        "c",
        "GRAPH_ROW_OPTIONAL_FILTER_EDGE",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    where_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("inactive".to_string()),
    });
    let mut where_query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_FILTER_REQUIRED"),
            graph_optional(
                vec![GraphPatternPiece::Edge(where_edge)],
                Some(GraphExpr::Binary {
                    left: Box::new(graph_prop("s", "status")),
                    op: GraphBinaryOp::Eq,
                    right: Box::new(GraphExpr::String("active".to_string())),
                }),
            ),
        ],
    );
    where_query.nodes[0].ids = vec![a];
    where_query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&where_query).unwrap()),
        vec![vec![GraphValue::Null]]
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );

}

#[test]
fn graph_row_optional_top_level_where_runs_after_optional_and_can_reject_null_rows() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-where-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-where-b", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_WHERE_REQUIRED", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_WHERE_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_WHERE_MISSING",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.where_ = Some(GraphExpr::IsNotNull(Box::new(GraphExpr::Binding(
        "c".to_string(),
    ))));
    query.return_items = Some(vec![graph_return_binding("a", GraphReturnProjection::IdOnly)]);

    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
}

#[test]
fn graph_row_optional_later_required_piece_drops_null_optional_aliases_and_expands_hits() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-required-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-required-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-required-c", &[]);
    let d = insert_graph_row_node(&engine, "Topic", "optional-required-d", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_REQUIRED_ROOT", &[]);
    let s = insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_OPTIONAL_REQUIRED_OPT", &[]);
    let t = insert_graph_row_edge(&engine, c, d, "GRAPH_ROW_OPTIONAL_REQUIRED_LATER", &[]);

    let mut query = graph_query(
        &["a", "b", "c", "d"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_REQUIRED_ROOT"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_REQUIRED_OPT",
                )],
                None,
            ),
            graph_edge_with_label(Some("t"), "c", "d", "GRAPH_ROW_OPTIONAL_REQUIRED_LATER"),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![
        graph_return_binding("s", GraphReturnProjection::IdOnly),
        graph_return_binding("t", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::EdgeId(s), GraphValue::EdgeId(t)]]
    );

    engine.delete_edge(s).unwrap();
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
}

#[test]
fn graph_row_optional_null_projects_in_id_element_and_selected_modes() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-project-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-project-b", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_PROJECT_REQUIRED", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_PROJECT_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_PROJECT_MISSING",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.output.mode = GraphOutputMode::Projected;
    query.return_items = Some(vec![
        graph_return_binding("c", GraphReturnProjection::IdOnly),
        graph_return_binding(
            "c",
            GraphReturnProjection::Element(GraphElementProjection::Full),
        ),
        graph_return_binding(
            "s",
            GraphReturnProjection::Selected(GraphSelectedProjection::Edge(selected_edge(
                GraphPropertySelection::All,
            ))),
        ),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::Null, GraphValue::Null, GraphValue::Null]]
    );
}

#[test]
fn graph_row_optional_null_ordering_and_cursor_pagination_are_stable() {
    let (_dir, engine) = graph_row_test_engine();
    let a1 = insert_graph_row_node(&engine, "Person", "optional-order-a1", &[]);
    let a2 = insert_graph_row_node(&engine, "Person", "optional-order-a2", &[]);
    let b1 = insert_graph_row_node(&engine, "Person", "optional-order-b1", &[]);
    let b2 = insert_graph_row_node(&engine, "Person", "optional-order-b2", &[]);
    let c = insert_graph_row_node(
        &engine,
        "Company",
        "optional-order-c",
        &[("rank", PropValue::Int(1))],
    );
    insert_graph_row_edge(&engine, a1, b1, "GRAPH_ROW_OPTIONAL_ORDER_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a2, b2, "GRAPH_ROW_OPTIONAL_ORDER_REQUIRED", &[]);
    insert_graph_row_edge(
        &engine,
        b1,
        c,
        "GRAPH_ROW_OPTIONAL_ORDER_HIT",
        &[("status", PropValue::String("hot".to_string()))],
    );

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_ORDER_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_ORDER_HIT",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a2, a1];
    query.return_items = Some(vec![
        graph_return_binding("b", GraphReturnProjection::IdOnly),
        graph_return_binding("c", GraphReturnProjection::IdOnly),
    ]);
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("c", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    query.page.limit = 1;
    if let GraphPatternPiece::Optional(group) = &mut query.pieces[1] {
        if let GraphPatternPiece::Edge(edge) = &mut group.pieces[0] {
            edge.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
    }

    engine.reset_query_execution_counters_for_test();
    let first = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(first.clone()),
        vec![vec![GraphValue::NodeId(b1), GraphValue::NodeId(c)]]
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );
    let cursor = first.next_cursor.unwrap();
    query.page.cursor = Some(cursor);
    engine.reset_query_execution_counters_for_test();
    let second = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(second),
        vec![vec![GraphValue::NodeId(b2), GraphValue::Null]]
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );
}

#[test]
fn graph_row_optional_uncorrelated_group_runs_as_reusable_apply() {
    let (_dir, engine) = graph_row_test_engine();
    let a1 = insert_graph_row_node(&engine, "Person", "optional-uncorr-a1", &[]);
    let a2 = insert_graph_row_node(&engine, "Person", "optional-uncorr-a2", &[]);
    let b1 = insert_graph_row_node(&engine, "Person", "optional-uncorr-b1", &[]);
    let b2 = insert_graph_row_node(&engine, "Person", "optional-uncorr-b2", &[]);
    let x = insert_graph_row_node(&engine, "Company", "optional-uncorr-x", &[]);
    let y = insert_graph_row_node(&engine, "Topic", "optional-uncorr-y", &[]);
    insert_graph_row_edge(&engine, a1, b1, "GRAPH_ROW_OPTIONAL_UNCORR_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a2, b2, "GRAPH_ROW_OPTIONAL_UNCORR_REQUIRED", &[]);
    let independent = insert_graph_row_edge(
        &engine,
        x,
        y,
        "GRAPH_ROW_OPTIONAL_UNCORR_INDEPENDENT",
        &[("status", PropValue::String("hot".to_string()))],
    );

    let mut query = graph_query(
        &["a", "b", "x", "y"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_UNCORR_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "x",
                    "y",
                    "GRAPH_ROW_OPTIONAL_UNCORR_INDEPENDENT",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a1, a2];
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    query.options.include_plan = true;
    if let GraphPatternPiece::Optional(group) = &mut query.pieces[1] {
        if let GraphPatternPiece::Edge(edge) = &mut group.pieces[0] {
            edge.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
    }

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(result.clone()),
        vec![vec![GraphValue::EdgeId(independent)], vec![GraphValue::EdgeId(independent)]]
    );
    let explain = result.plan.unwrap();
    assert_graph_row_explain_contains(&explain, "correlated=false");
    assert_graph_row_explain_contains(&explain, "full_scan_per_left_row=false");
    assert_graph_row_explain_contains(&explain, "reusable_subplan_rows=1");
    assert_graph_row_explain_contains(&explain, "hit_rows=2");
    assert_graph_row_explain_contains(&explain, "miss_rows=0");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );

    let mut miss_query = graph_query(
        &["a", "b", "x", "y"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_UNCORR_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "x",
                    "y",
                    "GRAPH_ROW_OPTIONAL_UNCORR_INDEPENDENT",
                )],
                Some(GraphExpr::Bool(false)),
            ),
        ],
    );
    miss_query.nodes[0].ids = vec![a1, a2];
    miss_query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    miss_query.options.include_plan = true;
    if let GraphPatternPiece::Optional(group) = &mut miss_query.pieces[1] {
        if let GraphPatternPiece::Edge(edge) = &mut group.pieces[0] {
            edge.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
    }

    engine.reset_query_execution_counters_for_test();
    let miss_result = engine.query_graph_rows(&miss_query).unwrap();
    assert_eq!(
        graph_row_value_rows(miss_result.clone()),
        vec![vec![GraphValue::Null], vec![GraphValue::Null]]
    );
    let miss_explain = miss_result.plan.unwrap();
    assert_graph_row_explain_contains(&miss_explain, "correlated=false");
    assert_graph_row_explain_contains(&miss_explain, "hit_rows=0");
    assert_graph_row_explain_contains(&miss_explain, "miss_rows=2");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );
}

#[test]
fn graph_row_runtime_cache_reuses_uncorrelated_optional_across_direct_core_calls() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "CacheOptional", "cache-optional-a", &[]);
    let b = insert_graph_row_node(&engine, "CacheOptional", "cache-optional-b", &[]);
    let x = insert_graph_row_node(&engine, "CacheOptionalX", "cache-optional-x", &[]);
    let y = insert_graph_row_node(&engine, "CacheOptionalY", "cache-optional-y", &[]);
    insert_graph_row_edge(&engine, a, b, "CACHE_OPTIONAL_REQUIRED", &[]);
    insert_graph_row_edge(&engine, x, y, "CACHE_OPTIONAL_INDEPENDENT", &[]);

    let mut query = graph_query(
        &["a", "b", "x", "y"],
        vec![
            graph_edge_with_label(None, "a", "b", "CACHE_OPTIONAL_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    None,
                    "x",
                    "y",
                    "CACHE_OPTIONAL_INDEPENDENT",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let policy_cutoffs = view.query_policy_cutoffs();
    let mut caches = GraphRowExecutionCaches::new(normalized.options.max_intermediate_bindings);
    let mut followups = GraphRowUniqueFollowups::new(Vec::new());

    let mut wrapper_followups = Vec::new();
    let mut wrapper_frontier = 0;
    let mut wrapper_intermediate = 0;
    let mut wrapper_paths = 0;
    let mut wrapper_trace = GraphRowExplainTrace::default();
    let wrapper_rows = view
        .graph_row_execute_runtime_plan(
            &normalized,
            &runtime,
            &physical,
            None,
            GraphRowRuntimeGoal::AllRows,
            i64::MAX / 2,
            policy_cutoffs.as_ref(),
            &mut wrapper_followups,
            &mut wrapper_frontier,
            &mut wrapper_intermediate,
            &mut wrapper_paths,
            Some(&mut wrapper_trace),
        )
        .unwrap();

    engine.reset_query_execution_counters_for_test();
    let mut first_attempt = caches.begin_attempt();
    let (cold, cold_frontier, cold_intermediate, cold_paths, cold_trace) = {
        let mut frontier_peak = 0;
        let mut intermediate_peak = 0;
        let mut paths_enumerated = 0;
        let mut trace = GraphRowExplainTrace::default();
        let rows = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut frontier_peak,
                &mut intermediate_peak,
                &mut paths_enumerated,
                Some(&mut trace),
                &mut caches,
                &mut first_attempt,
            )
            .unwrap();
        (rows, frontier_peak, intermediate_peak, paths_enumerated, trace)
    };
    caches.commit_attempt(&mut first_attempt);
    let mut second_attempt = caches.begin_attempt();
    let warm = {
        let mut frontier_peak = 0;
        let mut intermediate_peak = 0;
        let mut paths_enumerated = 0;
        view.graph_row_execute_runtime_plan_with_caches(
            &normalized,
            &runtime,
            &physical,
            None,
            None,
            GraphRowRuntimeGoal::AllRows,
            i64::MAX / 2,
            policy_cutoffs.as_ref(),
            &mut followups,
            &mut frontier_peak,
            &mut intermediate_peak,
            &mut paths_enumerated,
            None,
            &mut caches,
            &mut second_attempt,
        )
        .unwrap()
    };
    caches.commit_attempt(&mut second_attempt);

    assert_eq!(warm, cold);
    assert_eq!(wrapper_rows, cold);
    assert_eq!(wrapper_frontier, cold_frontier);
    assert_eq!(wrapper_intermediate, cold_intermediate);
    assert_eq!(wrapper_paths, cold_paths);
    assert_eq!(wrapper_trace.plan, cold_trace.plan);
    assert_eq!(wrapper_trace.row_ops, cold_trace.row_ops);
    assert_eq!(wrapper_trace.notes, cold_trace.notes);
    assert_eq!(
        wrapper_followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>(),
        followups
            .followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>()
    );
    assert_eq!(caches.optional_physical_plan_count(), 1);
    assert_eq!(caches.uncorrelated_optional_results.len(), 1);
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_optional_group_cache_hits,
        1
    );
    let mut capped_query = query.clone();
    capped_query.options.max_intermediate_bindings = 0;
    assert_eq!(
        normalize_graph_row_query(&capped_query)
            .unwrap_err()
            .to_string(),
        "invalid operation: graph row max_intermediate_bindings must be >= 1"
    );
}

#[test]
fn graph_row_chunk_pipeline_runtime_once_stage_preserves_rows_order_and_complete_trace() {
    let (_dir, engine) = graph_row_test_engine();
    let a1 = insert_graph_row_node(&engine, "ChunkPipelineA", "chunk-pipeline-a1", &[]);
    let a2 = insert_graph_row_node(&engine, "ChunkPipelineA", "chunk-pipeline-a2", &[]);
    let b1 = insert_graph_row_node(&engine, "ChunkPipelineB", "chunk-pipeline-b1", &[]);
    let b2 = insert_graph_row_node(&engine, "ChunkPipelineB", "chunk-pipeline-b2", &[]);
    let c = insert_graph_row_node(&engine, "ChunkPipelineC", "chunk-pipeline-c", &[]);
    insert_graph_row_edge(&engine, a1, b1, "CHUNK_PIPELINE_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a2, b2, "CHUNK_PIPELINE_REQUIRED", &[]);
    insert_graph_row_edge(&engine, b1, c, "CHUNK_PIPELINE_OPTIONAL", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("required"), "a", "b", "CHUNK_PIPELINE_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "b",
                    "c",
                    "CHUNK_PIPELINE_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a2, a1];
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let policy_cutoffs = view.query_policy_cutoffs();
    let epoch = i64::MAX / 2;

    let mut oracle_followups = Vec::new();
    let mut oracle_frontier = 0;
    let mut oracle_intermediate = 0;
    let mut oracle_paths = 0;
    let oracle = view
        .graph_row_execute_runtime_plan(
            &normalized,
            &runtime,
            &physical,
            None,
            GraphRowRuntimeGoal::AllRows,
            epoch,
            policy_cutoffs.as_ref(),
            &mut oracle_followups,
            &mut oracle_frontier,
            &mut oracle_intermediate,
            &mut oracle_paths,
            None,
        )
        .unwrap();

    let cursor_state = GraphRowCursorState {
        decoded: None,
        effective_at_epoch: epoch,
        original_skip: 0,
        rows_emitted_after_skip: 0,
    };
    let mut trace = GraphRowExplainTrace::default();
    engine.reset_query_execution_counters_for_test();
    let routed = view
        .graph_row_execute_chunked(
            &normalized,
            &runtime,
            &physical,
            Some(GraphRowRuntimeOnceExecution {
                reason: GraphRowRuntimeOnceReason::Stage,
                initial_rows: None,
                goal: GraphRowRuntimeGoal::AllRows,
            }),
            &cursor_state,
            0,
            true,
            epoch,
            policy_cutoffs.as_ref(),
            Some(&mut trace),
        )
        .unwrap();
    let GraphRowProductionOutput::CollectAll(routed) = routed else {
        panic!("stage must use collect-all")
    };
    assert_eq!(routed.rows, oracle);
    assert_eq!(
        routed
            .followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>(),
        oracle_followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>()
    );
    assert_eq!(routed.frontier_peak, oracle_frontier);
    assert_eq!(routed.intermediate_peak, oracle_intermediate);
    assert_eq!(routed.paths_enumerated, oracle_paths);
    let runtime_detail = trace
        .plan
        .iter()
        .find(|node| node.kind == "ChunkedRowProductionRuntime")
        .unwrap()
        .detail
        .as_str();
    assert_eq!(
        runtime_detail,
        "source=RuntimeOnce{reason=Stage}; source_pulls=0; successful_leaves=1; \
         scheduled_sizes=[]; leaf_size_min=0; leaf_size_max=0; source_rows=0; \
         produced_rows=2; early_exit_eligible=false; eligibility=stage_sink; \
         early_exit=false; cursor_seek=none; heap_capacity=0; cap_retries=0; \
         result_cache_units=0/65536; cache_no_admit=0; optional_cache_hits=0; \
         vlp_cross_chunk_cache_hits=0"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunks_executed,
        1
    );
}

#[test]
fn graph_row_chunk_pipeline_runtime_once_some_rows_and_exists_are_single_call() {
    let (_dir, engine) = graph_row_test_engine();
    let node = insert_graph_row_node(&engine, "ChunkPipelineSeed", "chunk-pipeline-seed", &[]);
    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "ChunkPipelineSeed");
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, false)
        .unwrap();
    let policy_cutoffs = view.query_policy_cutoffs();
    let epoch = i64::MAX / 2;
    let cursor_state = GraphRowCursorState {
        decoded: None,
        effective_at_epoch: epoch,
        original_skip: 0,
        rows_emitted_after_skip: 0,
    };
    let slot = runtime.nodes[0].slot;
    let mut seed = normalized.binding_schema.empty_row();
    seed.bind_node(slot, crate::graph_row::GraphBoundNode::id_only(node))
        .unwrap();

    let mut stage_trace = GraphRowExplainTrace::default();
    let stage = view
        .graph_row_execute_chunked(
            &normalized,
            &runtime,
            &physical,
            Some(GraphRowRuntimeOnceExecution {
                reason: GraphRowRuntimeOnceReason::Stage,
                initial_rows: Some(vec![seed.clone()]),
                goal: GraphRowRuntimeGoal::AllRows,
            }),
            &cursor_state,
            0,
            true,
            epoch,
            policy_cutoffs.as_ref(),
            Some(&mut stage_trace),
        )
        .unwrap();
    let GraphRowProductionOutput::CollectAll(stage) = stage else {
        panic!("stage must use collect-all")
    };
    assert_eq!(stage.rows, vec![seed.clone()]);
    let stage_detail = &stage_trace
        .plan
        .iter()
        .find(|node| node.kind == "ChunkedRowProductionRuntime")
        .unwrap()
        .detail;
    assert!(stage_detail.contains(
        "source=RuntimeOnce{reason=Stage}; source_pulls=0; successful_leaves=1; \
         scheduled_sizes=[]; leaf_size_min=1; leaf_size_max=1; source_rows=1; produced_rows=1"
    ));
    assert!(stage_detail.ends_with(
        "early_exit=false; cursor_seek=none; heap_capacity=0; cap_retries=0; \
         result_cache_units=0/65536; cache_no_admit=0; optional_cache_hits=0; \
         vlp_cross_chunk_cache_hits=0"
    ));

    let mut exists_trace = GraphRowExplainTrace::default();
    engine.reset_query_execution_counters_for_test();
    let exists = view
        .graph_row_execute_chunked(
            &normalized,
            &runtime,
            &physical,
            Some(GraphRowRuntimeOnceExecution {
                reason: GraphRowRuntimeOnceReason::Exists,
                initial_rows: Some(vec![seed]),
                goal: GraphRowRuntimeGoal::ExistsOne,
            }),
            &cursor_state,
            0,
            true,
            epoch,
            policy_cutoffs.as_ref(),
            Some(&mut exists_trace),
        )
        .unwrap();
    let GraphRowProductionOutput::CollectAll(exists) = exists else {
        panic!("EXISTS must use collect-all")
    };
    assert_eq!(exists.rows.len(), 1);
    assert_eq!(
        exists_trace
            .plan
            .iter()
            .find(|node| node.kind == "ChunkedRowProductionRuntime")
            .unwrap()
            .detail,
        "source=RuntimeOnce{reason=Exists}; source_pulls=0; successful_leaves=1; \
         scheduled_sizes=[]; leaf_size_min=1; leaf_size_max=1; source_rows=1; \
         produced_rows=1; early_exit_eligible=false; eligibility=runtime_once; \
         early_exit=false; cursor_seek=none; heap_capacity=0; cap_retries=0; \
         result_cache_units=0/65536; cache_no_admit=0; optional_cache_hits=0; \
         vlp_cross_chunk_cache_hits=0"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunks_executed,
        1
    );

    let scratch_allocs = graph_row_test_scratch_trace_allocs(|| {
        let result = view
            .graph_row_execute_chunked(
                &normalized,
                &runtime,
                &physical,
                Some(GraphRowRuntimeOnceExecution {
                    reason: GraphRowRuntimeOnceReason::Exists,
                    initial_rows: None,
                    goal: GraphRowRuntimeGoal::ExistsOne,
                }),
                &cursor_state,
                0,
                true,
                epoch,
                policy_cutoffs.as_ref(),
                None,
            )
            .unwrap();
        let GraphRowProductionOutput::CollectAll(result) = result else {
            panic!("EXISTS must use collect-all")
        };
        assert_eq!(result.rows.len(), 1);
    });
    assert_eq!(scratch_allocs, 0);
}

#[test]
fn graph_row_chunk_pipeline_production_stage_matches_compatibility_oracle() {
    let (_dir, engine) = graph_row_test_engine();
    let start = insert_graph_row_node(&engine, "ChunkPipelineProd", "prod-start", &[]);
    let middle = insert_graph_row_node(&engine, "ChunkPipelineProd", "prod-middle", &[]);
    let end = insert_graph_row_node(&engine, "ChunkPipelineProd", "prod-end", &[]);
    insert_graph_row_edge(&engine, start, middle, "CHUNK_PIPELINE_PROD_REQUIRED", &[]);
    insert_graph_row_edge(&engine, middle, end, "CHUNK_PIPELINE_PROD_VLP", &[]);

    let view = engine.published_state().view.clone();
    let policy_cutoffs = view.query_policy_cutoffs();
    let epoch = i64::MAX / 2;
    let cases = vec![
        (
            "none/no-step",
            {
                let mut query = graph_query(&["a"], Vec::new());
                query.nodes[0] = graph_node_with_label("a", "ChunkPipelineProd");
                query
            },
            None,
        ),
        (
            "some/no-step",
            graph_query(&["a"], Vec::new()),
            Some(start),
        ),
        (
            "some/required",
            graph_query(
                &["a", "b"],
                vec![graph_edge_with_label(
                    Some("required"),
                    "a",
                    "b",
                    "CHUNK_PIPELINE_PROD_REQUIRED",
                )],
            ),
            Some(start),
        ),
        (
            "some/optional",
            graph_query(
                &["a", "b"],
                vec![graph_optional(
                    vec![graph_edge_with_label(
                        Some("optional"),
                        "a",
                        "b",
                        "CHUNK_PIPELINE_PROD_REQUIRED",
                    )],
                    None,
                )],
            ),
            Some(start),
        ),
        (
            "some/VLP",
            {
                let mut query = graph_query(
                    &["a", "b"],
                    vec![graph_vlp(Some("path"), None, "a", "b", 1, 1)],
                );
                if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
                    path.label_filter = vec!["CHUNK_PIPELINE_PROD_REQUIRED".to_string()];
                }
                query
            },
            Some(start),
        ),
    ];

    for (name, query, seed_id) in cases {
        let normalized = normalize_graph_row_query(&query).unwrap();
        let initial_rows = seed_id.map(|id| {
            let slot = normalized.binding_schema.slot_for_alias("a").unwrap();
            let mut row = normalized.binding_schema.empty_row();
            row.bind_node(slot, crate::graph_row::GraphBoundNode::id_only(id))
                .unwrap();
            vec![row]
        });
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let physical = view
            .plan_graph_row_physical(&normalized, &runtime, true, false)
            .unwrap();
        let mut oracle_followups = Vec::new();
        let mut oracle_frontier = 0;
        let mut oracle_intermediate = 0;
        let mut oracle_paths = 0;
        let oracle_rows = view
            .graph_row_execute_runtime_plan(
                &normalized,
                &runtime,
                &physical,
                initial_rows.clone(),
                GraphRowRuntimeGoal::AllRows,
                epoch,
                policy_cutoffs.as_ref(),
                &mut oracle_followups,
                &mut oracle_frontier,
                &mut oracle_intermediate,
                &mut oracle_paths,
                None,
            )
            .unwrap();

        engine.reset_query_execution_counters_for_test();
        let routed = view
            .execute_graph_row_stage(&normalized, initial_rows.clone(), epoch, true, false)
            .unwrap();
        assert_eq!(routed.rows, oracle_rows, "row/vector mismatch for {name}");
        assert_eq!(
            routed
                .followups
                .iter()
                .map(SecondaryIndexReadFollowup::dedup_key)
                .collect::<Vec<_>>(),
            oracle_followups
                .iter()
                .map(SecondaryIndexReadFollowup::dedup_key)
                .collect::<Vec<_>>(),
            "followup mismatch for {name}"
        );
        assert_eq!(
            routed.intermediate_peak,
            oracle_intermediate.max(initial_rows.as_ref().map_or(0, Vec::len)),
            "intermediate peak mismatch for {name}"
        );
        assert_eq!(routed.rows_after_filter, routed.rows.len(), "{name}");
        assert_eq!(
            routed.warnings,
            graph_row_runtime_warnings(&runtime.warnings),
            "warning mismatch for {name}"
        );
        let explain = graph_row_explain_text(routed.explain.as_ref().unwrap());
        assert!(
            explain.contains("source=RuntimeOnce{reason=Stage}"),
            "{name}: {explain}"
        );
        assert!(explain.contains("eligibility=stage_sink"), "{name}: {explain}");
        assert!(
            explain.contains(
                "early_exit=false; cursor_seek=none; heap_capacity=0; cap_retries=0; \
                 result_cache_units=0/65536; cache_no_admit=0; optional_cache_hits=0; \
                 vlp_cross_chunk_cache_hits=0"
            ),
            "{name}: {explain}"
        );
        assert_eq!(
            engine
                .query_execution_counter_snapshot_for_test()
                .graph_row_chunks_executed,
            1,
            "RuntimeOnce must execute the production stage once for {name}"
        );
    }

    let valid = insert_graph_row_node_with_labels(
        &engine,
        &["ChunkPipelineSeed", "ChunkPipelineSeedRequired"],
        "seed-valid",
        &[],
    );
    let invalid = insert_graph_row_node(&engine, "ChunkPipelineSeed", "seed-invalid", &[]);
    let target = insert_graph_row_node(&engine, "ChunkPipelineSeedTarget", "seed-target", &[]);
    insert_graph_row_edge(&engine, valid, target, "CHUNK_PIPELINE_SEED_EDGE", &[]);
    let seed_match = GraphPipelineMatchStage {
        optional: true,
        nodes: vec![
            graph_node_with_label("n", "ChunkPipelineSeedRequired"),
            graph_node_with_label("m", "ChunkPipelineSeedTarget"),
        ],
        pieces: vec![graph_edge_with_label(
            Some("edge"),
            "n",
            "m",
            "CHUNK_PIPELINE_SEED_EDGE",
        )],
        optional_candidate_where: None,
        where_: None,
    };
    let seed_return = GraphProjectStage {
        kind: GraphProjectKind::Return,
        items: GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding("n".to_string()),
            alias: Some("n".to_string()),
            projection: GraphReturnProjection::IdOnly,
        }]),
        distinct: false,
        where_: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };
    let mut initial_schema = GraphBindingSchema::new();
    let initial_n_slot = initial_schema.add_node_alias("n", false).unwrap();
    let seed_pipeline = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(seed_match.clone()),
            GraphPipelineStage::Project(seed_return.clone()),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(epoch),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    let normalized_seed_pipeline = normalize_graph_pipeline_query_with_initial_schema(
        &seed_pipeline,
        initial_schema.clone(),
        0,
    )
    .unwrap();
    let NormalizedGraphPipelineStage::Match(seed_stage) =
        &normalized_seed_pipeline.stages[0]
    else {
        unreachable!()
    };
    let seed_view = engine.published_state().view.clone();
    let seed_rows = [invalid, valid]
        .into_iter()
        .map(|id| {
            let mut row = initial_schema.empty_row();
            row.bind_node(
                initial_n_slot,
                crate::graph_row::GraphBoundNode::id_only(id),
            )
                .unwrap();
            row
        })
        .collect::<Vec<_>>();
    let bridged_seed_rows = pipeline_bridge_rows(
        &seed_rows,
        &initial_schema,
        &seed_stage.query.binding_schema,
        &seed_stage.input_mappings,
    )
    .unwrap();
    let n_slot = seed_stage.query.binding_schema.slot_for_alias("n").unwrap();
    let m_slot = seed_stage.query.binding_schema.slot_for_alias("m").unwrap();
    let seed_execution = seed_view
        .execute_graph_row_stage(
            &seed_stage.query,
            Some(bridged_seed_rows),
            epoch,
            true,
            true,
        )
        .unwrap();
    assert_eq!(
        seed_execution
            .rows
            .iter()
            .map(|row| row.node_id_for_slot_if_bound(n_slot).unwrap())
            .collect::<Vec<_>>(),
        vec![Some(valid), Some(invalid)],
        "seed misses must remain appended after runtime rows"
    );
    assert_eq!(
        seed_execution
            .rows
            .iter()
            .map(|row| row.node_id_for_slot_if_bound(m_slot).unwrap())
            .collect::<Vec<_>>(),
        vec![Some(target), None],
        "seed-miss rows must retain their historical null extension"
    );
    assert!(seed_execution.followups.is_empty());

    let mut capped_seed_pipeline = seed_pipeline;
    capped_seed_pipeline.options.max_intermediate_bindings = 1;
    let normalized_capped_seed_pipeline = normalize_graph_pipeline_query_with_initial_schema(
        &capped_seed_pipeline,
        initial_schema.clone(),
        0,
    )
    .unwrap();
    let NormalizedGraphPipelineStage::Match(capped_seed_stage) =
        &normalized_capped_seed_pipeline.stages[0]
    else {
        unreachable!()
    };
    let capped_rows = [invalid, valid]
        .into_iter()
        .map(|id| {
            let mut row = initial_schema.empty_row();
            row.bind_node(
                initial_n_slot,
                crate::graph_row::GraphBoundNode::id_only(id),
            )
            .unwrap();
            row
        })
        .collect::<Vec<_>>();
    let bridged_capped_rows = pipeline_bridge_rows(
        &capped_rows,
        &initial_schema,
        &capped_seed_stage.query.binding_schema,
        &capped_seed_stage.input_mappings,
    )
    .unwrap();
    assert_eq!(
        seed_view.execute_graph_row_stage(
            &capped_seed_stage.query,
            Some(bridged_capped_rows),
            epoch,
            false,
            true,
        )
        .unwrap_err()
        .to_string(),
        "invalid operation: graph row max_intermediate_bindings exceeded configured cap 1"
    );
}

#[test]
fn graph_row_chunk_pipeline_correlated_optional_with_limit_preserves_vector_order() {
    let (_dir, engine) = graph_row_test_engine();
    let a1 = insert_graph_row_node(&engine, "ChunkPipelineOrderA", "order-a1", &[]);
    let a2 = insert_graph_row_node(&engine, "ChunkPipelineOrderA", "order-a2", &[]);
    let a3 = insert_graph_row_node(&engine, "ChunkPipelineOrderA", "order-a3", &[]);
    let b1 = insert_graph_row_node(&engine, "ChunkPipelineOrderB", "order-b1", &[]);
    let b2 = insert_graph_row_node(&engine, "ChunkPipelineOrderB", "order-b2", &[]);
    let c = insert_graph_row_node(&engine, "ChunkPipelineOrderC", "order-c", &[]);
    insert_graph_row_edge(&engine, a1, b1, "CHUNK_PIPELINE_ORDER_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a2, b2, "CHUNK_PIPELINE_ORDER_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a3, b1, "CHUNK_PIPELINE_ORDER_REQUIRED", &[]);
    insert_graph_row_edge(&engine, b1, c, "CHUNK_PIPELINE_ORDER_OPTIONAL", &[]);

    let order_match = GraphPipelineMatchStage {
        optional: false,
        nodes: vec![
            {
                let mut node = graph_node_with_label("a", "ChunkPipelineOrderA");
                node.ids = vec![a1, a2, a3];
                node
            },
            graph_node_with_label("b", "ChunkPipelineOrderB"),
            graph_node_with_label("c", "ChunkPipelineOrderC"),
        ],
        pieces: vec![
            graph_edge_with_label(
                Some("required"),
                "a",
                "b",
                "CHUNK_PIPELINE_ORDER_REQUIRED",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "b",
                    "c",
                    "CHUNK_PIPELINE_ORDER_OPTIONAL",
                )],
                None,
            ),
        ],
        optional_candidate_where: None,
        where_: None,
    };
    let with_limit = GraphProjectStage {
        kind: GraphProjectKind::With,
        items: GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding("a".to_string()),
            alias: Some("a".to_string()),
            projection: GraphReturnProjection::IdOnly,
        }]),
        distinct: false,
        where_: None,
        order_by: Vec::new(),
        skip: None,
        limit: Some(GraphExpr::UInt(2)),
    };
    let return_stage = GraphProjectStage {
        kind: GraphProjectKind::Return,
        items: GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding("a".to_string()),
            alias: Some("a".to_string()),
            projection: GraphReturnProjection::IdOnly,
        }]),
        distinct: false,
        where_: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };
    let pipeline_options = GraphPipelineOptions {
        allow_full_scan: false,
        include_plan: true,
        ..GraphPipelineOptions::default()
    };
    let pipeline = |stages| GraphPipelineQuery {
        stages,
        params: BTreeMap::new(),
        at_epoch: Some(i64::MAX / 2),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: pipeline_options.clone(),
    };

    let none_pipeline = pipeline(vec![
        GraphPipelineStage::Match(order_match.clone()),
        GraphPipelineStage::Project(with_limit.clone()),
        GraphPipelineStage::Project(return_stage.clone()),
    ]);
    let some_pipeline = pipeline(vec![
        GraphPipelineStage::Match(GraphPipelineMatchStage {
            optional: false,
            nodes: vec![{
                let mut node = graph_node_with_label("a", "ChunkPipelineOrderA");
                node.ids = vec![a1, a2, a3];
                node
            }],
            pieces: Vec::new(),
            optional_candidate_where: None,
            where_: None,
        }),
        GraphPipelineStage::Match(order_match),
        GraphPipelineStage::Project(with_limit),
        GraphPipelineStage::Project(return_stage),
    ]);

    let expected = vec![vec![GraphValue::NodeId(a1)], vec![GraphValue::NodeId(a3)]];
    for (name, query) in [("initial_rows=None", none_pipeline), ("initial_rows=Some", some_pipeline)] {
        engine.reset_query_execution_counters_for_test();
        let result = engine.query_graph_pipeline(&query).unwrap();
        assert_eq!(
            graph_pipeline_value_rows(result.clone()),
            expected,
            "{name} must retain the whole-call correlated-optional order before WITH LIMIT"
        );
        let plan = format!("{:?}", result.plan.unwrap());
        assert!(plan.contains("source=RuntimeOnce{reason=Stage}"), "{name}: {plan}");
        assert!(plan.contains("eligibility=stage_sink"), "{name}: {plan}");
        assert!(
            plan.contains("scheduled_sizes=[]"),
            "RuntimeOnce stage scheduling must remain unchunked for {name}: {plan}"
        );
        assert_eq!(
            engine
                .query_execution_counter_snapshot_for_test()
                .graph_row_chunk_cap_retries,
            0,
            "{name}"
        );
    }

    let mut oracle_query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "a",
                "b",
                "CHUNK_PIPELINE_ORDER_REQUIRED",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "b",
                    "c",
                    "CHUNK_PIPELINE_ORDER_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    oracle_query.nodes[0] = {
        let mut node = graph_node_with_label("a", "ChunkPipelineOrderA");
        node.ids = vec![a1, a2, a3];
        node
    };
    oracle_query.nodes[1] = graph_node_with_label("b", "ChunkPipelineOrderB");
    oracle_query.nodes[2] = graph_node_with_label("c", "ChunkPipelineOrderC");
    let normalized = normalize_graph_row_query(&oracle_query).unwrap();
    let a_slot = normalized.binding_schema.slot_for_alias("a").unwrap();
    let initial_rows = [a1, a2, a3]
        .into_iter()
        .map(|id| {
            let mut row = normalized.binding_schema.empty_row();
            row.bind_node(a_slot, crate::graph_row::GraphBoundNode::id_only(id))
                .unwrap();
            row
        })
        .collect::<Vec<_>>();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, false)
        .unwrap();
    let policy_cutoffs = view.query_policy_cutoffs();
    for (name, initial_rows) in [
        ("compatibility None", None),
        ("compatibility Some", Some(initial_rows)),
    ] {
        let mut followups = Vec::new();
        let mut frontier = 0;
        let mut intermediate = 0;
        let mut paths = 0;
        let oracle = view
            .graph_row_execute_runtime_plan(
                &normalized,
                &runtime,
                &physical,
                initial_rows,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut frontier,
                &mut intermediate,
                &mut paths,
                None,
            )
            .unwrap();
        assert_eq!(
            oracle
                .iter()
                .take(2)
                .map(|row| row.node_id_for_slot_if_bound(a_slot).unwrap())
                .collect::<Vec<_>>(),
            vec![Some(a1), Some(a3)],
            "{name}"
        );
    }
}

#[test]
fn graph_row_chunk_pipeline_production_exists_probe_preserves_goal_and_caps() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..8 {
        insert_graph_row_node(
            &engine,
            "ChunkPipelineExistsHit",
            &format!("exists-hit-{index}"),
            &[],
        );
    }
    let epoch = i64::MAX / 2;
    let return_node = |alias: &str| GraphProjectStage {
        kind: GraphProjectKind::Return,
        items: GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding(alias.to_string()),
            alias: Some(alias.to_string()),
            projection: GraphReturnProjection::IdOnly,
        }]),
        distinct: false,
        where_: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };
    let uncorrelated_pipeline = |label: &str| GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![graph_node_with_label("inner", label)],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(return_node("inner")),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(epoch),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    let view = engine.published_state().view.clone();
    let policy_cutoffs = view.query_policy_cutoffs();

    for (label, expected) in [
        ("ChunkPipelineExistsHit", true),
        ("ChunkPipelineExistsMissing", false),
    ] {
        let initial_schema = GraphBindingSchema::new();
        let pipeline = normalize_graph_pipeline_query_with_initial_schema(
            &uncorrelated_pipeline(label),
            initial_schema.clone(),
            0,
        )
        .unwrap();
        let NormalizedGraphPipelineStage::Match(stage) = &pipeline.stages[0] else {
            unreachable!()
        };
        let runtime = view.normalize_graph_row_runtime_plan(&stage.query).unwrap();
        let physical = view
            .plan_graph_row_physical(&stage.query, &runtime, false, false)
            .unwrap();
        let mut oracle_followups = Vec::new();
        let mut oracle_frontier = 0;
        let mut oracle_intermediate = 0;
        let mut oracle_paths = 0;
        let oracle = view
            .graph_row_execute_runtime_plan(
                &stage.query,
                &runtime,
                &physical,
                None,
                GraphRowRuntimeGoal::ExistsOne,
                epoch,
                policy_cutoffs.as_ref(),
                &mut oracle_followups,
                &mut oracle_frontier,
                &mut oracle_intermediate,
                &mut oracle_paths,
                None,
            )
            .unwrap();
        assert_eq!(!oracle.is_empty(), expected, "compatibility oracle {label}");
        if expected {
            let mut all_rows_followups = Vec::new();
            let mut all_rows_frontier = 0;
            let mut all_rows_intermediate = 0;
            let mut all_rows_paths = 0;
            let all_rows = view
                .graph_row_execute_runtime_plan(
                    &stage.query,
                    &runtime,
                    &physical,
                    None,
                    GraphRowRuntimeGoal::AllRows,
                    epoch,
                    policy_cutoffs.as_ref(),
                    &mut all_rows_followups,
                    &mut all_rows_frontier,
                    &mut all_rows_intermediate,
                    &mut all_rows_paths,
                    None,
                )
                .unwrap();
            assert_eq!(all_rows.len(), 8);
            assert!(
                all_rows_intermediate > oracle_intermediate,
                "the actual-work oracle must distinguish ExistsOne from AllRows"
            );
        }

        engine.reset_query_execution_counters_for_test();
        let execution = view
            .execute_pipeline_subquery_exists_probe(
                &pipeline,
                epoch,
                &[initial_schema.empty_row()],
            )
            .unwrap()
            .expect("simple MATCH/RETURN must use the physical EXISTS probe");
        assert_eq!(execution.exists, expected, "production probe {label}");
        assert_eq!(execution.stats.rows_entered_pipeline, 1, "{label}");
        assert_eq!(execution.stats.rows_returned, usize::from(expected), "{label}");
        assert_eq!(execution.stats.rows_after_filter, usize::from(expected), "{label}");
        assert_eq!(
            execution.stats.intermediate_rows, oracle_intermediate,
            "production actual work must match the ExistsOne oracle for {label}"
        );
        assert_eq!(
            execution.followups
                .iter()
                .map(SecondaryIndexReadFollowup::dedup_key)
                .collect::<Vec<_>>(),
            oracle_followups
                .iter()
                .map(SecondaryIndexReadFollowup::dedup_key)
                .collect::<Vec<_>>(),
            "{label}"
        );
        let counters = engine.query_execution_counter_snapshot_for_test();
        assert_eq!(counters.graph_row_query_calls, 1, "{label}");
        assert_eq!(counters.graph_row_chunks_executed, 1, "{label}");
        assert_eq!(
            counters.graph_row_successful_leaf_rows_peak,
            0,
            "RuntimeOnce(None) retains the implicit-empty-binding source-row accounting for {label}"
        );
        assert_eq!(counters.graph_row_result_cache_units_peak, 0, "{label}");
        assert_eq!(counters.graph_row_result_cache_no_admit, 0, "{label}");
        assert_eq!(counters.graph_row_chunk_early_exits, 0, "{label}");
    }

    let source = insert_graph_row_node(&engine, "ChunkPipelineExistsSource", "exists-source", &[]);
    let target1 = insert_graph_row_node(&engine, "ChunkPipelineExistsTarget", "exists-target-1", &[]);
    let target2 = insert_graph_row_node(&engine, "ChunkPipelineExistsTarget", "exists-target-2", &[]);
    insert_graph_row_edge(&engine, source, target1, "CHUNK_PIPELINE_EXISTS_EDGE", &[]);
    insert_graph_row_edge(&engine, source, target2, "CHUNK_PIPELINE_EXISTS_EDGE", &[]);
    let correlated_view = engine.published_state().view.clone();
    let correlated_policy_cutoffs = correlated_view.query_policy_cutoffs();
    let mut initial_schema = GraphBindingSchema::new();
    let source_slot = initial_schema.add_node_alias("source", false).unwrap();
    let correlated_query = GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![
                    graph_node_with_label("source", "ChunkPipelineExistsSource"),
                    graph_node_with_label("target", "ChunkPipelineExistsTarget"),
                ],
                pieces: vec![graph_edge_with_label(
                    Some("edge"),
                    "source",
                    "target",
                    "CHUNK_PIPELINE_EXISTS_EDGE",
                )],
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(return_node("target")),
        ],
        params: BTreeMap::new(),
        at_epoch: Some(epoch),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            ..GraphPipelineOptions::default()
        },
    };
    let normalized_correlated = normalize_graph_pipeline_query_with_initial_schema(
        &correlated_query,
        initial_schema.clone(),
        0,
    )
    .unwrap();
    let NormalizedGraphPipelineStage::Match(correlated_stage) =
        &normalized_correlated.stages[0]
    else {
        unreachable!()
    };
    let mut source_row = initial_schema.empty_row();
    source_row
        .bind_node(
            source_slot,
            crate::graph_row::GraphBoundNode::id_only(source),
        )
        .unwrap();
    let bridged = pipeline_bridge_rows(
        std::slice::from_ref(&source_row),
        &initial_schema,
        &correlated_stage.query.binding_schema,
        &correlated_stage.input_mappings,
    )
    .unwrap();
    let runtime = correlated_view
        .normalize_graph_row_runtime_plan(&correlated_stage.query)
        .unwrap();
    let physical = correlated_view
        .plan_graph_row_physical(&correlated_stage.query, &runtime, false, false)
        .unwrap();
    let mut oracle_followups = Vec::new();
    let mut oracle_frontier = 0;
    let mut oracle_intermediate = 0;
    let mut oracle_paths = 0;
    let oracle = correlated_view
        .graph_row_execute_runtime_plan(
            &correlated_stage.query,
            &runtime,
            &physical,
            Some(bridged),
            GraphRowRuntimeGoal::ExistsOne,
            epoch,
            correlated_policy_cutoffs.as_ref(),
            &mut oracle_followups,
            &mut oracle_frontier,
            &mut oracle_intermediate,
            &mut oracle_paths,
            None,
        )
        .unwrap();
    assert_eq!(oracle.len(), 1, "compatibility ExistsOne must stop at one row");
    let bridged_all_rows = pipeline_bridge_rows(
        std::slice::from_ref(&source_row),
        &initial_schema,
        &correlated_stage.query.binding_schema,
        &correlated_stage.input_mappings,
    )
    .unwrap();
    let mut all_rows_followups = Vec::new();
    let mut all_rows_frontier = 0;
    let mut all_rows_intermediate = 0;
    let mut all_rows_paths = 0;
    let all_rows = correlated_view
        .graph_row_execute_runtime_plan(
            &correlated_stage.query,
            &runtime,
            &physical,
            Some(bridged_all_rows),
            GraphRowRuntimeGoal::AllRows,
            epoch,
            correlated_policy_cutoffs.as_ref(),
            &mut all_rows_followups,
            &mut all_rows_frontier,
            &mut all_rows_intermediate,
            &mut all_rows_paths,
            None,
        )
        .unwrap();
    assert_eq!(all_rows.len(), 2);
    assert!(all_rows_intermediate > oracle_intermediate);

    engine.reset_query_execution_counters_for_test();
    let correlated = correlated_view
        .execute_pipeline_subquery_exists_probe(
            &normalized_correlated,
            epoch,
            std::slice::from_ref(&source_row),
        )
        .unwrap()
        .expect("correlated MATCH/RETURN must use the physical EXISTS probe");
    assert!(correlated.exists);
    assert_eq!(correlated.stats.rows_returned, 1);
    assert_eq!(correlated.stats.rows_after_filter, 1);
    assert_eq!(
        correlated.stats.intermediate_rows,
        oracle_intermediate.max(1),
        "production actual work must match the correlated ExistsOne oracle"
    );
    assert_eq!(
        correlated
            .followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>(),
        oracle_followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>()
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_query_calls, 1);
    assert_eq!(counters.graph_row_chunks_executed, 1);
    assert_eq!(counters.graph_row_successful_leaf_rows_peak, 1);
    assert_eq!(counters.graph_row_result_cache_units_peak, 0);

    let mut capped_query = correlated_query;
    capped_query.stages[0] = GraphPipelineStage::Match(GraphPipelineMatchStage {
        optional: false,
        nodes: vec![
            graph_node_with_label("source", "ChunkPipelineExistsSource"),
            graph_node_with_label("target", "ChunkPipelineExistsTarget"),
        ],
        pieces: vec![{
            let mut path = graph_vlp(Some("path"), None, "source", "target", 1, 1);
            if let GraphPatternPiece::VariableLength(path) = &mut path {
                path.label_filter = vec!["CHUNK_PIPELINE_EXISTS_EDGE".to_string()];
            }
            path
        }],
        optional_candidate_where: None,
        where_: None,
    });
    capped_query.options.max_frontier = 0;
    let capped = normalize_graph_pipeline_query_with_initial_schema(
        &capped_query,
        initial_schema.clone(),
        0,
    )
    .unwrap();
    let NormalizedGraphPipelineStage::Match(capped_stage) = &capped.stages[0] else {
        unreachable!()
    };
    let capped_bridged = pipeline_bridge_rows(
        std::slice::from_ref(&source_row),
        &initial_schema,
        &capped_stage.query.binding_schema,
        &capped_stage.input_mappings,
    )
    .unwrap();
    let capped_runtime = correlated_view
        .normalize_graph_row_runtime_plan(&capped_stage.query)
        .unwrap();
    let capped_physical = correlated_view
        .plan_graph_row_physical(&capped_stage.query, &capped_runtime, false, false)
        .unwrap();
    let mut capped_followups = Vec::new();
    let mut capped_frontier = 0;
    let mut capped_intermediate = 0;
    let mut capped_paths = 0;
    let compatibility_error = correlated_view
        .graph_row_execute_runtime_plan(
            &capped_stage.query,
            &capped_runtime,
            &capped_physical,
            Some(capped_bridged),
            GraphRowRuntimeGoal::ExistsOne,
            epoch,
            correlated_policy_cutoffs.as_ref(),
            &mut capped_followups,
            &mut capped_frontier,
            &mut capped_intermediate,
            &mut capped_paths,
            None,
        )
        .unwrap_err()
        .to_string();
    let production_error = correlated_view
        .execute_pipeline_subquery_exists_probe(&capped, epoch, &[source_row])
        .unwrap_err()
        .to_string();
    assert_eq!(
        production_error,
        compatibility_error,
        "production EXISTS cap behavior must match the pre-routing compatibility path"
    );
    assert_eq!(
        compatibility_error,
        "invalid operation: graph row max_frontier exceeded configured cap 0; path=path; piece_index=0"
    );
}

#[test]
fn graph_row_runtime_cache_executes_nested_same_local_paths_without_collision() {
    let (_dir, engine) = graph_row_test_engine();
    let x = insert_graph_row_node(&engine, "NestedCache", "nested-cache-x", &[]);
    let y = insert_graph_row_node(&engine, "NestedCache", "nested-cache-y", &[]);
    let u = insert_graph_row_node(&engine, "NestedCache", "nested-cache-u", &[]);
    let v = insert_graph_row_node(&engine, "NestedCache", "nested-cache-v", &[]);
    insert_graph_row_edge(&engine, x, y, "NESTED_CACHE_A", &[]);
    insert_graph_row_edge(&engine, u, v, "NESTED_CACHE_B", &[]);
    let mut query = graph_query(
        &["x", "y", "u", "v"],
        vec![
            graph_optional(
                vec![graph_optional(
                    vec![graph_edge_with_label(None, "x", "y", "NESTED_CACHE_A")],
                    None,
                )],
                None,
            ),
            graph_optional(
                vec![graph_optional(
                    vec![graph_edge_with_label(None, "u", "v", "NESTED_CACHE_B")],
                    None,
                )],
                None,
            ),
        ],
    );
    query.options.allow_full_scan = true;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let policy_cutoffs = view.query_policy_cutoffs();
    let mut caches = GraphRowExecutionCaches::new(64);
    let mut followups = GraphRowUniqueFollowups::new(Vec::new());
    let mut results = Vec::new();
    for _ in 0..2 {
        let mut attempt = caches.begin_attempt();
        let rows = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut 0,
                &mut 0,
                &mut 0,
                None,
                &mut caches,
                &mut attempt,
            )
            .unwrap();
        caches.commit_attempt(&mut attempt);
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(caches.uncorrelated_optional_results.len(), 4);
    let row = &results[1][0];
    for (alias, expected) in [("x", x), ("y", y), ("u", u), ("v", v)] {
        let slot = normalized.binding_schema.slot_for_alias(alias).unwrap();
        assert_eq!(row.node_id_for_slot_if_bound(slot).unwrap(), Some(expected));
    }
}

#[test]
fn graph_row_runtime_cache_correlated_negative_no_admit_and_rollback_are_directly_observable() {
    let (_dir, engine) = graph_row_test_engine();
    let a1 = insert_graph_row_node(&engine, "CacheCorr", "cache-corr-a1", &[]);
    let a2 = insert_graph_row_node(&engine, "CacheCorr", "cache-corr-a2", &[]);
    let b = insert_graph_row_node(&engine, "CacheCorr", "cache-corr-b", &[]);
    let c = insert_graph_row_node(&engine, "CacheCorr", "cache-corr-c", &[]);
    insert_graph_row_edge(&engine, a1, b, "CACHE_CORR_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a2, b, "CACHE_CORR_REQUIRED", &[]);
    insert_graph_row_edge(
        &engine,
        b,
        c,
        "CACHE_CORR_OPTIONAL",
        &[("status", PropValue::String("hot".to_string()))],
    );

    for negative in [false, true] {
        let mut query = graph_query(
            &["a", "b", "c"],
            vec![
                graph_edge_with_label(None, "a", "b", "CACHE_CORR_REQUIRED"),
                graph_optional(
                    vec![graph_edge_with_label(
                        None,
                        "b",
                        "c",
                        "CACHE_CORR_OPTIONAL",
                    )],
                    negative.then_some(GraphExpr::Bool(false)),
                ),
            ],
        );
        query.nodes[0].ids = vec![a1, a2];
        if let GraphPatternPiece::Optional(group) = &mut query.pieces[1] {
            if let GraphPatternPiece::Edge(edge) = &mut group.pieces[0] {
                edge.filter = Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                });
            }
        }
        let normalized = normalize_graph_row_query(&query).unwrap();
        let view = engine.published_state().view.clone();
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let physical = view
            .plan_graph_row_physical(&normalized, &runtime, false, true)
            .unwrap();
        let policy_cutoffs = view.query_policy_cutoffs();
        let mut followups = GraphRowUniqueFollowups::new(Vec::new());
        let mut caches = GraphRowExecutionCaches::new(64);

        let mut first_attempt = caches.begin_attempt();
        let mut first_paths = 0;
        let first = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut 0,
                &mut 0,
                &mut first_paths,
                None,
                &mut caches,
                &mut first_attempt,
            )
            .unwrap();
        assert_eq!(caches.correlated_optional_results.len(), 1);
        caches.rollback_attempt(&mut first_attempt);
        assert!(caches.correlated_optional_results.is_empty());
        assert_eq!(caches.used_units, 0);

        engine.reset_query_execution_counters_for_test();
        let mut committed = caches.begin_attempt();
        let recomputed = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut 0,
                &mut 0,
                &mut 0,
                None,
                &mut caches,
                &mut committed,
            )
            .unwrap();
        assert_eq!(recomputed, first);
        assert!(
            engine
                .query_execution_counter_snapshot_for_test()
                .graph_row_delegated_edge_queries
                > 0
        );
        caches.commit_attempt(&mut committed);

        engine.reset_query_execution_counters_for_test();
        let mut warm_attempt = caches.begin_attempt();
        let warm = view
            .graph_row_execute_runtime_plan_with_caches(
                &normalized,
                &runtime,
                &physical,
                None,
                None,
                GraphRowRuntimeGoal::AllRows,
                i64::MAX / 2,
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut 0,
                &mut 0,
                &mut 0,
                None,
                &mut caches,
                &mut warm_attempt,
            )
            .unwrap();
        assert_eq!(warm, first);
        let warm_counters = engine.query_execution_counter_snapshot_for_test();
        assert_eq!(warm_counters.graph_row_optional_group_cache_hits, 1);
        assert_eq!(warm_counters.graph_row_delegated_edge_queries, 0);

        let mut no_admit = GraphRowExecutionCaches::new(0);
        let mut no_admit_followups = GraphRowUniqueFollowups::new(Vec::new());
        let mut runs = Vec::new();
        for _ in 0..2 {
            let mut attempt = no_admit.begin_attempt();
            runs.push(
                view.graph_row_execute_runtime_plan_with_caches(
                    &normalized,
                    &runtime,
                    &physical,
                    None,
                    None,
                    GraphRowRuntimeGoal::AllRows,
                    i64::MAX / 2,
                    policy_cutoffs.as_ref(),
                    &mut no_admit_followups,
                    &mut 0,
                    &mut 0,
                    &mut 0,
                    None,
                    &mut no_admit,
                    &mut attempt,
                )
                .unwrap(),
            );
            assert!(attempt.is_empty());
        }
        assert_eq!(runs, vec![first.clone(), first]);
        assert!(no_admit.correlated_optional_results.is_empty());
        assert!(no_admit.no_admit >= 2);
    }
}

#[test]
fn graph_row_optional_uncorrelated_without_anchor_requires_full_scan_permission() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-anchor-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-anchor-b", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_ANCHOR_REQUIRED", &[]);

    let mut query = graph_query(
        &["a", "b", "x", "y"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_ANCHOR_REQUIRED"),
            graph_optional(vec![graph_edge(Some("s"), "x", "y")], None),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.options.allow_full_scan = false;

    let err = engine.query_graph_rows(&query).unwrap_err();
    assert!(err
        .to_string()
        .contains("optional group requires correlation, an internal anchor, or allow_full_scan=true"));
}

#[test]
fn graph_row_optional_correlated_batches_by_dependency_bindings() {
    let (_dir, engine) = graph_row_test_engine();
    let a1 = insert_graph_row_node(&engine, "Person", "optional-dependency-a1", &[]);
    let a2 = insert_graph_row_node(&engine, "Person", "optional-dependency-a2", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-dependency-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-dependency-c", &[]);
    insert_graph_row_edge(&engine, a1, b, "GRAPH_ROW_OPTIONAL_DEP_REQUIRED", &[]);
    insert_graph_row_edge(&engine, a2, b, "GRAPH_ROW_OPTIONAL_DEP_REQUIRED", &[]);
    let optional_edge = insert_graph_row_edge(
        &engine,
        b,
        c,
        "GRAPH_ROW_OPTIONAL_DEP_HIT",
        &[("status", PropValue::String("hot".to_string()))],
    );

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_DEP_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_DEP_HIT",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a1, a2];
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    query.options.include_plan = true;
    if let GraphPatternPiece::Optional(group) = &mut query.pieces[1] {
        if let GraphPatternPiece::Edge(edge) = &mut group.pieces[0] {
            edge.filter = Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            });
        }
    }

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(result.clone()),
        vec![
            vec![GraphValue::EdgeId(optional_edge)],
            vec![GraphValue::EdgeId(optional_edge)],
        ]
    );
    let explain = result.plan.unwrap();
    assert_graph_row_explain_contains(&explain, "correlated=true");
    assert_graph_row_explain_contains(&explain, "distinct_dependency_bindings=1");
    assert_graph_row_explain_contains(&explain, "batched_by_dependency_bindings=true");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );
}

#[test]
fn graph_row_optional_explain_reports_apply_aliases_filters_and_caps() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-explain-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-explain-b", &[]);
    let c = insert_graph_row_node(
        &engine,
        "Company",
        "optional-explain-c",
        &[("status", PropValue::String("active".to_string()))],
    );
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_EXPLAIN_REQUIRED", &[]);
    insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_OPTIONAL_EXPLAIN_HIT", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_EXPLAIN_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_EXPLAIN_HIT",
                )],
                Some(GraphExpr::Binary {
                    left: Box::new(graph_prop("c", "status")),
                    op: GraphBinaryOp::Eq,
                    right: Box::new(GraphExpr::String("active".to_string())),
                }),
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding("c", GraphReturnProjection::IdOnly)]);

    let result = engine.query_graph_rows(&query).unwrap();
    let explain = result.plan.unwrap();
    assert_graph_row_explain_contains(&explain, "OptionalApply");
    assert_graph_row_explain_contains(&explain, "introduced_slots=");
    assert_graph_row_explain_contains(&explain, "dependency_slots=");
    assert_graph_row_explain_contains(&explain, "left_outer=true");
    assert_graph_row_explain_contains(&explain, "where_present=true");
    assert_graph_row_explain_contains(&explain, "max_intermediate_bindings");
    assert_graph_row_explain_contains(&explain, "latest visible");
}

#[test]
fn graph_row_optional_source_correctness_handles_edge_and_endpoint_tombstones() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-source-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-source-b", &[]);
    let edge_deleted_target =
        insert_graph_row_node(&engine, "Company", "optional-source-edge-deleted", &[]);
    let node_deleted_target =
        insert_graph_row_node(&engine, "Company", "optional-source-node-deleted", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_SOURCE_REQUIRED", &[]);
    let edge_deleted = insert_graph_row_edge(
        &engine,
        b,
        edge_deleted_target,
        "GRAPH_ROW_OPTIONAL_SOURCE_EDGE_DELETED",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        b,
        node_deleted_target,
        "GRAPH_ROW_OPTIONAL_SOURCE_NODE_DELETED",
        &[],
    );
    engine.delete_edge(edge_deleted).unwrap();
    engine.delete_node(node_deleted_target).unwrap();

    for label in [
        "GRAPH_ROW_OPTIONAL_SOURCE_EDGE_DELETED",
        "GRAPH_ROW_OPTIONAL_SOURCE_NODE_DELETED",
    ] {
        let mut query = graph_query(
            &["a", "b", "c"],
            vec![
                graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_SOURCE_REQUIRED"),
                graph_optional(vec![graph_edge_with_label(Some("s"), "b", "c", label)], None),
            ],
        );
        query.nodes[0].ids = vec![a];
        query.return_items = Some(vec![
            graph_return_binding("s", GraphReturnProjection::IdOnly),
            graph_return_binding("c", GraphReturnProjection::IdOnly),
        ]);

        assert_eq!(
            graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
            vec![vec![GraphValue::Null, GraphValue::Null]]
        );
    }
}

#[test]
fn graph_row_optional_source_correctness_honors_temporal_edge_validity() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-temporal-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-temporal-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-temporal-c", &[]);
    engine
        .upsert_edge(
            a,
            b,
            "GRAPH_ROW_OPTIONAL_TEMPORAL_REQUIRED",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(i64::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    let valid_edge = engine
        .upsert_edge(
            b,
            c,
            "GRAPH_ROW_OPTIONAL_TEMPORAL",
            UpsertEdgeOptions {
                valid_from: Some(100),
                valid_to: Some(200),
                ..Default::default()
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_TEMPORAL_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_TEMPORAL",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    query.at_epoch = Some(150);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::EdgeId(valid_edge)]]
    );
    query.at_epoch = Some(250);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::Null]]
    );
}

#[test]
fn graph_row_optional_edge_property_filters_use_selected_fields_without_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-selected-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-selected-b", &[]);
    let keep = insert_graph_row_node(&engine, "Company", "optional-selected-keep", &[]);
    let drop = insert_graph_row_node(&engine, "Company", "optional-selected-drop", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_SELECTED_REQUIRED", &[]);
    let keep_edge = insert_graph_row_edge(
        &engine,
        b,
        keep,
        "GRAPH_ROW_OPTIONAL_SELECTED",
        &[("status", PropValue::String("active".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        b,
        drop,
        "GRAPH_ROW_OPTIONAL_SELECTED",
        &[("status", PropValue::String("inactive".to_string()))],
    );

    let mut optional_edge = match graph_edge_with_label(
        Some("s"),
        "b",
        "c",
        "GRAPH_ROW_OPTIONAL_SELECTED",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    optional_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("active".to_string()),
    });
    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_SELECTED_REQUIRED"),
            graph_optional(vec![GraphPatternPiece::Edge(optional_edge)], None),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_ids, 2);
}

#[test]
fn graph_row_optional_where_hydrates_only_group_local_needs_before_top_level_where() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-local-needs-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-local-needs-b", &[]);
    let keep = insert_graph_row_node(
        &engine,
        "Company",
        "optional-local-needs-keep",
        &[("name", PropValue::String("keep".to_string()))],
    );
    let drop = insert_graph_row_node(
        &engine,
        "Company",
        "optional-local-needs-drop",
        &[("name", PropValue::String("drop".to_string()))],
    );
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_LOCAL_NEEDS_REQUIRED", &[]);
    insert_graph_row_edge(
        &engine,
        b,
        keep,
        "GRAPH_ROW_OPTIONAL_LOCAL_NEEDS",
        &[("status", PropValue::String("active".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        b,
        drop,
        "GRAPH_ROW_OPTIONAL_LOCAL_NEEDS",
        &[("status", PropValue::String("inactive".to_string()))],
    );

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(
                Some("r"),
                "a",
                "b",
                "GRAPH_ROW_OPTIONAL_LOCAL_NEEDS_REQUIRED",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_LOCAL_NEEDS",
                )],
                Some(GraphExpr::Binary {
                    left: Box::new(graph_prop("s", "status")),
                    op: GraphBinaryOp::Eq,
                    right: Box::new(GraphExpr::String("active".to_string())),
                }),
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("c", "name")),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::String("keep".to_string())),
    });
    query.return_items = Some(vec![graph_return_binding("c", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::NodeId(keep)]]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn graph_row_optional_source_correctness_uses_active_memtable_shadow_over_segment() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-shadow-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-shadow-b", &[]);
    let c = insert_graph_row_node(&engine, "Company", "optional-shadow-c", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_SHADOW_REQUIRED", &[]);
    let optional_edge = insert_graph_row_edge(
        &engine,
        b,
        c,
        "GRAPH_ROW_OPTIONAL_SHADOW",
        &[("status", PropValue::String("old".to_string()))],
    );
    engine.flush().unwrap();
    let old_edge = internal_edge_record(&engine, optional_edge).unwrap().unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(EdgeRecord {
            props: graph_row_props(&[("status", PropValue::String("new".to_string()))]),
            ..old_edge
        }),
    )
    .unwrap();

    let mut old_filter = match graph_edge_with_label(
        Some("s"),
        "b",
        "c",
        "GRAPH_ROW_OPTIONAL_SHADOW",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    old_filter.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("old".to_string()),
    });
    let mut old_query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_SHADOW_REQUIRED"),
            graph_optional(vec![GraphPatternPiece::Edge(old_filter)], None),
        ],
    );
    old_query.nodes[0].ids = vec![a];
    old_query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&old_query).unwrap()),
        vec![vec![GraphValue::Null]]
    );

    let mut new_filter = match graph_edge_with_label(
        Some("s"),
        "b",
        "c",
        "GRAPH_ROW_OPTIONAL_SHADOW",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    new_filter.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("new".to_string()),
    });
    let mut new_query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_SHADOW_REQUIRED"),
            graph_optional(vec![GraphPatternPiece::Edge(new_filter)], None),
        ],
    );
    new_query.nodes[0].ids = vec![a];
    new_query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&new_query).unwrap()),
        vec![vec![GraphValue::EdgeId(optional_edge)]]
    );
}

#[test]
fn graph_row_optional_source_correctness_misses_prune_hidden_endpoint() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-prune-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-prune-b", &[]);
    let hidden = engine
        .upsert_node(
            "Company",
            "optional-prune-hidden",
            UpsertNodeOptions {
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_PRUNE_REQUIRED", &[]);
    insert_graph_row_edge(&engine, b, hidden, "GRAPH_ROW_OPTIONAL_PRUNE", &[]);
    engine
        .set_prune_policy(
            "graph-row-optional-prune",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("Company".to_string()),
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_PRUNE_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_PRUNE",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::Null]]
    );
}

#[test]
fn graph_row_optional_stale_edge_property_index_candidates_are_verified_away() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let index_id;
    let segment_id;
    let left_a;
    let red_one;
    let red_two;
    let blue;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        left_a = insert_graph_row_node(&engine, "Person", "optional-stale-left-a", &[]);
        let left_b = insert_graph_row_node(&engine, "Person", "optional-stale-left-b", &[]);
        insert_graph_row_edge(
            &engine,
            left_a,
            left_b,
            "GRAPH_ROW_OPTIONAL_STALE_REQUIRED",
            &[],
        );
        let nodes = (0..4)
            .map(|idx| {
                insert_graph_row_node(
                    &engine,
                    "Person",
                    &format!("optional-stale-candidate-{idx}"),
                    &[],
                )
            })
            .collect::<Vec<_>>();
        red_one = insert_graph_row_edge(
            &engine,
            nodes[0],
            nodes[1],
            "GRAPH_ROW_OPTIONAL_STALE_EDGE",
            &[("color", PropValue::String("red".to_string()))],
        );
        red_two = insert_graph_row_edge(
            &engine,
            nodes[0],
            nodes[2],
            "GRAPH_ROW_OPTIONAL_STALE_EDGE",
            &[("color", PropValue::String("red".to_string()))],
        );
        blue = insert_graph_row_edge(
            &engine,
            nodes[0],
            nodes[3],
            "GRAPH_ROW_OPTIONAL_STALE_EDGE",
            &[("color", PropValue::String("blue".to_string()))],
        );
        engine.flush().unwrap();
        let index = engine
            .ensure_edge_property_index("GRAPH_ROW_OPTIONAL_STALE_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);
        index_id = index.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::edge_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    replace_equality_sidecar_group_id_in_place(
        &sidecar_path,
        hash_prop_equality_key(&PropValue::String("red".to_string())),
        red_two,
        blue,
    );

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let mut optional_edge = match graph_edge_with_label(
        Some("s"),
        "x",
        "y",
        "GRAPH_ROW_OPTIONAL_STALE_EDGE",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    optional_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "color".to_string(),
        value: PropValue::String("red".to_string()),
    });
    let mut query = graph_query(
        &["a", "b", "x", "y"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_STALE_REQUIRED"),
            graph_optional(vec![GraphPatternPiece::Edge(optional_edge)], None),
        ],
    );
    query.nodes[0].ids = vec![left_a];
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);

    let result = reopened.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(result.clone()),
        vec![vec![GraphValue::EdgeId(red_one)]]
    );
    let explain = result.plan.unwrap();
    assert_graph_row_explain_contains(&explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(&explain, "stale index candidates");
}

#[test]
fn graph_row_optional_uncorrelated_full_scan_enforces_caps() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "Person", "optional-cap-a", &[]);
    let b = insert_graph_row_node(&engine, "Person", "optional-cap-b", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_OPTIONAL_CAP_REQUIRED", &[]);
    for index in 0..3 {
        let source = insert_graph_row_node(&engine, "Person", &format!("optional-cap-x-{index}"), &[]);
        let target = insert_graph_row_node(&engine, "Person", &format!("optional-cap-y-{index}"), &[]);
        insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_OPTIONAL_CAP_SCAN", &[]);
    }

    let mut query = graph_query(
        &["a", "b", "x", "y"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_CAP_REQUIRED"),
            graph_optional(vec![graph_edge(Some("s"), "x", "y")], None),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.options.allow_full_scan = true;
    query.options.max_frontier = 2;
    query.options.max_intermediate_bindings = 100;
    query.return_items = Some(vec![graph_return_binding("s", GraphReturnProjection::IdOnly)]);

    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_frontier"), "{message}");
    assert!(message.contains('2'), "{message}");
}

#[test]
fn graph_row_executes_one_edge_with_element_and_selected_projection() {
    let (_dir, engine) = graph_row_test_engine();
    let alice = insert_graph_row_node(&engine, "Person", "project-alice", &[]);
    let bob = insert_graph_row_node(
        &engine,
        "Person",
        "project-bob",
        &[("name", PropValue::String("Bob".to_string()))],
    );
    let edge = insert_graph_row_edge(
        &engine,
        alice,
        bob,
        "KNOWS",
        &[("since", PropValue::Int(2024))],
    );

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "KNOWS")],
    );
    query.output.mode = GraphOutputMode::Projected;
    query.return_items = Some(vec![
        graph_return_binding(
            "r",
            GraphReturnProjection::Element(GraphElementProjection::Full),
        ),
        graph_return_binding(
            "b",
            GraphReturnProjection::Selected(GraphSelectedProjection::Node(
                GraphSelectedNodeProjection {
                    id: true,
                    labels: true,
                    key: true,
                    props: GraphPropertySelection::Keys(vec!["name".to_string()]),
                    weight: false,
                    created_at: false,
                    updated_at: false,
                    vectors: GraphVectorSelection::None,
                },
            )),
        ),
    ]);

    let rows = graph_row_value_rows(engine.query_graph_rows(&query).unwrap());
    let [GraphValue::Edge(edge_value), GraphValue::Node(node_value)] = rows[0].as_slice() else {
        panic!("expected edge and node values, got {:?}", rows[0]);
    };
    assert_eq!(edge_value.id, Some(edge));
    assert_eq!(edge_value.from, Some(alice));
    assert_eq!(edge_value.to, Some(bob));
    assert_eq!(edge_value.label.as_deref(), Some("KNOWS"));
    assert_eq!(node_value.id, Some(bob));
    assert_eq!(node_value.labels.as_ref().unwrap(), &vec!["Person".to_string()]);
    assert_eq!(
        node_value.props.as_ref().unwrap().get("name"),
        Some(&GraphValue::String("Bob".to_string()))
    );
}

#[test]
fn graph_row_executes_branching_required_fixed_pattern() {
    let (_dir, engine) = graph_row_test_engine();
    let root = insert_graph_row_node(&engine, "Person", "branch-root", &[]);
    let left = insert_graph_row_node(&engine, "Person", "branch-left", &[]);
    let right = insert_graph_row_node(&engine, "Person", "branch-right", &[]);
    let left_edge = insert_graph_row_edge(&engine, root, left, "KNOWS", &[]);
    let right_edge = insert_graph_row_edge(&engine, root, right, "LIKES", &[]);

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "KNOWS"),
            graph_edge_with_label(Some("s"), "a", "c", "LIKES"),
        ],
    );
    query.nodes[0].ids = vec![root];
    query.return_items = Some(vec![
        graph_return_binding("r", GraphReturnProjection::IdOnly),
        graph_return_binding("s", GraphReturnProjection::IdOnly),
    ]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![
            GraphValue::EdgeId(left_edge),
            GraphValue::EdgeId(right_edge),
        ]]
    );
}

#[test]
fn graph_row_repeated_alias_equality_and_relaxed_distinctness_allow_self_loops() {
    let (_dir, engine) = graph_row_test_engine();
    let node = insert_graph_row_node(&engine, "Person", "self-loop-node", &[]);
    let loop_edge = insert_graph_row_edge(&engine, node, node, "LOOP", &[]);

    let mut repeated = graph_query(
        &["a"],
        vec![graph_edge_with_label(Some("r"), "a", "a", "LOOP")],
    );
    repeated.return_items = Some(vec![
        graph_return_binding("a", GraphReturnProjection::IdOnly),
        graph_return_binding("r", GraphReturnProjection::IdOnly),
    ]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&repeated).unwrap()),
        vec![vec![GraphValue::NodeId(node), GraphValue::EdgeId(loop_edge)]]
    );

    let mut relaxed = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "LOOP")],
    );
    relaxed.return_items = Some(vec![
        graph_return_binding("a", GraphReturnProjection::IdOnly),
        graph_return_binding("b", GraphReturnProjection::IdOnly),
        graph_return_binding("r", GraphReturnProjection::IdOnly),
    ]);
    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&relaxed).unwrap()),
        vec![vec![
            GraphValue::NodeId(node),
            GraphValue::NodeId(node),
            GraphValue::EdgeId(loop_edge),
        ]]
    );
}

#[test]
fn graph_row_parallel_edges_preserve_multiplicity() {
    let (_dir, engine) = graph_row_test_engine();
    let from = insert_graph_row_node(&engine, "Person", "parallel-from", &[]);
    let to = insert_graph_row_node(&engine, "Person", "parallel-to", &[]);
    let first = insert_graph_row_edge(&engine, from, to, "KNOWS", &[("rank", PropValue::Int(1))]);
    let second = insert_graph_row_edge(&engine, from, to, "LIKES", &[("rank", PropValue::Int(2))]);

    let mut query = graph_query(&["a", "b"], vec![graph_edge(Some("r"), "a", "b")]);
    query.nodes[0].ids = vec![from];
    query.nodes[1].ids = vec![to];
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    query.options.include_plan = true;

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![first, second]
    );
}

#[test]
fn graph_row_fixed_queries_respect_flush_reopen_and_source_precedence() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let old = insert_graph_row_node(
        &engine,
        "Person",
        "shadowed",
        &[("status", PropValue::String("old".to_string()))],
    );
    let other = insert_graph_row_node(&engine, "Person", "other", &[]);
    let segment_edge = insert_graph_row_edge(&engine, old, other, "KNOWS", &[]);
    engine.flush().unwrap();

    let shadow = engine
        .upsert_node(
            "Person",
            "shadowed",
            UpsertNodeOptions {
                props: graph_row_props(&[("status", PropValue::String("new".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(shadow, old);
    let active_edge = insert_graph_row_edge(&engine, other, old, "KNOWS", &[]);

    let mut edge_query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "KNOWS")],
    );
    edge_query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&edge_query).unwrap()),
        vec![segment_edge, active_edge]
    );

    let mut old_status = graph_query(&["n"], Vec::new());
    old_status.nodes[0].label_filter = Some(NodeLabelFilter {
        labels: vec!["Person".to_string()],
        mode: LabelMatchMode::All,
    });
    old_status.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("old".to_string()),
    });
    old_status.options.allow_full_scan = false;
    assert!(engine.query_graph_rows(&old_status).unwrap().rows.is_empty());

    engine.flush().unwrap();
    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let mut reopened_query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "KNOWS")],
    );
    reopened_query.return_items =
        Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_single_u64_column(reopened.query_graph_rows(&reopened_query).unwrap()),
        vec![segment_edge, active_edge]
    );
}

#[test]
fn graph_row_fixed_queries_hide_node_and_edge_tombstones() {
    let (_dir, engine) = graph_row_test_engine();
    let alive = insert_graph_row_node(&engine, "Person", "alive", &[]);
    let deleted = insert_graph_row_node(&engine, "Person", "deleted", &[]);
    let edge = insert_graph_row_edge(&engine, alive, deleted, "KNOWS", &[]);
    engine.delete_node(deleted).unwrap();

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "KNOWS")],
    );
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());

    let replacement = insert_graph_row_node(&engine, "Person", "replacement", &[]);
    let edge_to_delete = insert_graph_row_edge(&engine, alive, replacement, "KNOWS", &[]);
    assert_ne!(edge, edge_to_delete);
    engine.delete_edge(edge_to_delete).unwrap();
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
}

#[test]
fn graph_row_fixed_queries_apply_prune_policy_and_temporal_edge_validity() {
    let (_dir, engine) = graph_row_test_engine();
    let keep = engine
        .upsert_node(
            "Person",
            "prune-keep",
            UpsertNodeOptions {
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    let prune = engine
        .upsert_node(
            "Person",
            "prune-drop",
            UpsertNodeOptions {
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    insert_graph_row_edge(&engine, keep, prune, "KNOWS", &[]);
    engine
        .set_prune_policy(
            "low-weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("Person".to_string()),
            },
        )
        .unwrap();

    let mut pruned_query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "KNOWS")],
    );
    pruned_query.return_items =
        Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    assert!(engine.query_graph_rows(&pruned_query).unwrap().rows.is_empty());

    engine.remove_prune_policy("low-weight").unwrap();
    let valid_edge = engine
        .upsert_edge(
            keep,
            prune,
            "TEMP",
            UpsertEdgeOptions {
                valid_from: Some(100),
                valid_to: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
    let mut temporal = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(Some("r"), "a", "b", "TEMP")],
    );
    temporal.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    temporal.at_epoch = Some(150);
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&temporal).unwrap()),
        vec![valid_edge]
    );
    temporal.at_epoch = Some(250);
    assert!(engine.query_graph_rows(&temporal).unwrap().rows.is_empty());
}

#[test]
fn graph_row_fixed_queries_verify_node_and_edge_filters() {
    let (_dir, engine) = graph_row_test_engine();
    let hot = insert_graph_row_node(
        &engine,
        "Person",
        "filter-hot",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let cold = insert_graph_row_node(
        &engine,
        "Person",
        "filter-cold",
        &[("status", PropValue::String("cold".to_string()))],
    );
    let keep = insert_graph_row_edge(
        &engine,
        hot,
        cold,
        "LIKES",
        &[
            ("status", PropValue::String("hot".to_string())),
            ("rank", PropValue::Int(7)),
        ],
    );
    insert_graph_row_edge(
        &engine,
        cold,
        hot,
        "LIKES",
        &[("status", PropValue::String("cold".to_string()))],
    );

    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["LIKES".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::WeightRange {
                    lower: Some(0.0),
                    upper: Some(2.0),
                },
            ])),
        })],
    );
    query.nodes[0] = GraphNodePattern {
        alias: "a".to_string(),
        label_filter: Some(NodeLabelFilter {
            labels: vec!["Person".to_string()],
            mode: LabelMatchMode::All,
        }),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
    };
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    query.options.allow_full_scan = false;

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep]
    );
}

#[test]
fn graph_row_multi_label_filters_cover_all_and_any_targets() {
    let (_dir, engine) = graph_row_test_engine();
    let anchor = insert_graph_row_node(&engine, "Company", "multi-label-anchor", &[]);
    let both =
        insert_graph_row_node_with_labels(&engine, &["Person", "Employee"], "multi-label-both", &[]);
    let person = insert_graph_row_node(&engine, "Person", "multi-label-person", &[]);
    let employee = insert_graph_row_node(&engine, "Employee", "multi-label-employee", &[]);
    let both_edge = insert_graph_row_edge(&engine, anchor, both, "GRAPH_ROW_MULTI_LABEL", &[]);
    let person_edge =
        insert_graph_row_edge(&engine, anchor, person, "GRAPH_ROW_MULTI_LABEL", &[]);
    let employee_edge =
        insert_graph_row_edge(&engine, anchor, employee, "GRAPH_ROW_MULTI_LABEL", &[]);

    let mut all_query = graph_query(
        &["anchor", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "anchor",
            "target",
            "GRAPH_ROW_MULTI_LABEL",
        )],
    );
    all_query.nodes[0].ids = vec![anchor];
    all_query.nodes[1].label_filter = Some(NodeLabelFilter {
        labels: vec!["Person".to_string(), "Employee".to_string()],
        mode: LabelMatchMode::All,
    });
    all_query.return_items =
        Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&all_query).unwrap()),
        vec![both_edge]
    );

    let mut any_query = all_query;
    any_query.nodes[1].label_filter = Some(NodeLabelFilter {
        labels: vec!["Person".to_string(), "Employee".to_string()],
        mode: LabelMatchMode::Any,
    });

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&any_query).unwrap()),
        vec![both_edge, person_edge, employee_edge]
    );
}

#[test]
fn graph_row_label_only_targets_verify_metadata_without_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let anchor = insert_graph_row_node(&engine, "Company", "label-only-anchor", &[]);
    let both =
        insert_graph_row_node_with_labels(&engine, &["Person", "Employee"], "label-only-both", &[]);
    let person = insert_graph_row_node(&engine, "Person", "label-only-person", &[]);
    let employee = insert_graph_row_node(&engine, "Employee", "label-only-employee", &[]);
    let both_edge = insert_graph_row_edge(&engine, anchor, both, "GRAPH_ROW_LABEL_ONLY", &[]);
    let person_edge = insert_graph_row_edge(&engine, anchor, person, "GRAPH_ROW_LABEL_ONLY", &[]);
    let employee_edge =
        insert_graph_row_edge(&engine, anchor, employee, "GRAPH_ROW_LABEL_ONLY", &[]);

    let mut all_query = graph_query(
        &["anchor", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "anchor",
            "target",
            "GRAPH_ROW_LABEL_ONLY",
        )],
    );
    all_query.nodes[0].ids = vec![anchor];
    all_query.nodes[1].label_filter = Some(NodeLabelFilter {
        labels: vec!["Person".to_string(), "Employee".to_string()],
        mode: LabelMatchMode::All,
    });
    all_query.return_items =
        Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&all_query).unwrap()),
        vec![both_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);

    let mut any_query = all_query;
    any_query.nodes[1].label_filter = Some(NodeLabelFilter {
        labels: vec!["Person".to_string(), "Employee".to_string()],
        mode: LabelMatchMode::Any,
    });

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&any_query).unwrap()),
        vec![both_edge, person_edge, employee_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);
}

#[test]
fn graph_row_property_target_uses_selected_projection_for_predicate() {
    let (_dir, engine) = graph_row_test_engine();
    let anchor = insert_graph_row_node(&engine, "Company", "property-target-anchor", &[]);
    let active = insert_graph_row_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "property-target-active",
        &[("status", PropValue::String("active".to_string()))],
    );
    let inactive = insert_graph_row_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "property-target-inactive",
        &[("status", PropValue::String("inactive".to_string()))],
    );
    let active_edge = insert_graph_row_edge(&engine, anchor, active, "GRAPH_ROW_TARGET_PROP", &[]);
    insert_graph_row_edge(&engine, anchor, inactive, "GRAPH_ROW_TARGET_PROP", &[]);

    let mut query = graph_query(
        &["anchor", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "anchor",
            "target",
            "GRAPH_ROW_TARGET_PROP",
        )],
    );
    query.nodes[0].ids = vec![anchor];
    query.nodes[1].label_filter = Some(NodeLabelFilter {
        labels: vec!["Person".to_string(), "Employee".to_string()],
        mode: LabelMatchMode::All,
    });
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("active".to_string()),
    });
    query.return_items = Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![active_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert_eq!(counters.node_selected_field_ids, 2);
}

#[test]
fn graph_row_edge_updated_at_filter_uses_metadata_without_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Person", "updated-at-source", &[]);
    let old_target = insert_graph_row_node(&engine, "Company", "updated-at-old", &[]);
    let keep_target = insert_graph_row_node(&engine, "Company", "updated-at-keep", &[]);
    let old_edge = insert_graph_row_edge(
        &engine,
        source,
        old_target,
        "GRAPH_ROW_UPDATED_AT_EDGE",
        &[],
    );
    let keep_edge = insert_graph_row_edge(
        &engine,
        source,
        keep_target,
        "GRAPH_ROW_UPDATED_AT_EDGE",
        &[],
    );
    set_graph_row_edge_updated_at(&engine, old_edge, 1_000);
    set_graph_row_edge_updated_at(&engine, keep_edge, 2_000);
    let keep_record = internal_edge_record(&engine, keep_edge).unwrap().unwrap();
    assert_eq!(keep_record.updated_at, 2_000);
    assert_ne!(keep_record.created_at, keep_record.updated_at);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_UPDATED_AT_EDGE".to_string()],
            filter: Some(EdgeFilterExpr::UpdatedAtRange {
                lower_ms: Some(2_000),
                upper_ms: Some(2_000),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("target", "Company");
    query.return_items = Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "metadata_only");
    assert_graph_row_explain_contains(&explain, "EdgeVerification");

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_ids, 0);
}

#[test]
fn graph_row_edge_property_filter_projects_only_metadata_survivors() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Person", "edge-prop-source", &[]);
    let keep_target = insert_graph_row_node(&engine, "Company", "edge-prop-keep", &[]);
    let keep_edge = engine
        .upsert_edge(
            source,
            keep_target,
            "GRAPH_ROW_EDGE_PROP_FILTER",
            UpsertEdgeOptions {
                props: graph_row_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    for index in 0..10 {
        let target = insert_graph_row_node(
            &engine,
            "Company",
            &format!("edge-prop-inactive-{index}"),
            &[],
        );
        engine
            .upsert_edge(
                source,
                target,
                "GRAPH_ROW_EDGE_PROP_FILTER",
                UpsertEdgeOptions {
                    props: graph_row_props(&[(
                        "status",
                        PropValue::String("inactive".to_string()),
                    )]),
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let metadata_drop = insert_graph_row_node(&engine, "Company", "edge-prop-metadata-drop", &[]);
    engine
        .upsert_edge(
            source,
            metadata_drop,
            "GRAPH_ROW_EDGE_PROP_FILTER",
            UpsertEdgeOptions {
                props: graph_row_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EDGE_PROP_FILTER".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::WeightRange {
                    lower: None,
                    upper: Some(1.0),
                },
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
            ])),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("target", "Company");
    query.return_items = Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "metadata_only");
    assert_graph_row_explain_contains(&explain, "edge_property_projection");

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_ids, 11);
}

#[test]
fn graph_row_edge_property_filter_reuses_verification_across_duplicate_frontiers() {
    let (_dir, engine) = graph_row_test_engine();
    let root = insert_graph_row_node(&engine, "Person", "edge-prop-cache-root", &[]);
    let mid = insert_graph_row_node(&engine, "Company", "edge-prop-cache-mid", &[]);
    let leaf = insert_graph_row_node(&engine, "Article", "edge-prop-cache-leaf", &[]);
    for _ in 0..64 {
        insert_graph_row_edge(&engine, root, mid, "GRAPH_ROW_CACHE_FIRST", &[]);
    }
    let second_edge = insert_graph_row_edge(
        &engine,
        mid,
        leaf,
        "GRAPH_ROW_CACHE_SECOND",
        &[("status", PropValue::String("active".to_string()))],
    );

    let mut query = graph_query(
        &["root", "mid", "leaf"],
        vec![
            graph_edge_with_label(Some("first"), "root", "mid", "GRAPH_ROW_CACHE_FIRST"),
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("second".to_string()),
                from_alias: "mid".to_string(),
                to_alias: "leaf".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["GRAPH_ROW_CACHE_SECOND".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }),
            }),
        ],
    );
    query.nodes[0].ids = vec![root];
    query.nodes[1] = graph_node_with_label("mid", "Company");
    query.nodes[2] = graph_node_with_label("leaf", "Article");
    query.page.limit = 100;
    query.return_items = Some(vec![graph_return_binding(
        "second",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let rows = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(rows.len(), 64);
    assert!(rows.iter().all(|edge| *edge == second_edge));
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_ids, 1);
}

#[test]
fn graph_row_edge_metadata_filter_preserves_order_and_cursor_page_shape() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Person", "metadata-filter-source", &[]);
    let mut expected = Vec::new();
    for index in 0..10 {
        let target = insert_graph_row_node(
            &engine,
            "Company",
            &format!("metadata-filter-target-{index}"),
            &[],
        );
        let edge = engine
            .upsert_edge(
                source,
                target,
                "GRAPH_ROW_METADATA_FILTER",
                UpsertEdgeOptions {
                    weight: if index % 2 == 0 { 0.5 } else { 2.0 },
                    ..Default::default()
                },
            )
            .unwrap();
        if index % 2 == 0 {
            expected.push(edge);
        }
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_METADATA_FILTER".to_string()],
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: None,
                upper: Some(1.0),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("target", "Company");
    query.page.limit = 3;
    query.return_items = Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(graph_row_single_u64_column(result.clone()), expected[..3]);
    assert!(result.next_cursor.is_some());
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_selected_field_ids, 0);
}

#[test]
fn graph_row_execution_does_not_call_public_node_or_edge_queries() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "NodeLabel129", "no-public-source", &[]);
    let target = insert_graph_row_node(&engine, "NodeLabel129", "no-public-target", &[]);
    let edge = insert_graph_row_edge(
        &engine,
        source,
        target,
        "GRAPH_ROW_NO_PUBLIC_EDGE",
        &[("status", PropValue::String("hot".to_string()))],
    );
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_NO_PUBLIC_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_NO_PUBLIC_EDGE".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding("edge", GraphReturnProjection::IdOnly)]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_query_calls, 1);
    assert_eq!(counters.public_node_query_calls, 0);
    assert_eq!(counters.public_edge_query_calls, 0);
}

#[test]
fn graph_row_delegated_bucket_queries_preserve_disjunctive_mapping_and_order() {
    let (_dir, engine) = graph_row_test_engine();
    let outgoing = insert_graph_row_node(&engine, "BoundBuckets", "outgoing", &[]);
    let incoming = insert_graph_row_node(&engine, "BoundBuckets", "incoming", &[]);
    let both = insert_graph_row_node(&engine, "BoundBuckets", "both", &[]);
    let mut expected = Vec::new();
    for label in ["BOUND_BUCKET_A", "BOUND_BUCKET_B"] {
        let out_target = insert_graph_row_node(
            &engine,
            "BoundBuckets",
            &format!("{label}-out-target"),
            &[],
        );
        let in_source = insert_graph_row_node(
            &engine,
            "BoundBuckets",
            &format!("{label}-in-source"),
            &[],
        );
        let both_target = insert_graph_row_node(
            &engine,
            "BoundBuckets",
            &format!("{label}-both-target"),
            &[],
        );
        expected.push(insert_graph_row_edge(
            &engine,
            outgoing,
            out_target,
            label,
            &[("status", PropValue::String("keep".to_string()))],
        ));
        expected.push(insert_graph_row_edge(
            &engine,
            in_source,
            incoming,
            label,
            &[("status", PropValue::String("keep".to_string()))],
        ));
        expected.push(insert_graph_row_edge(
            &engine,
            both,
            both_target,
            label,
            &[("status", PropValue::String("keep".to_string()))],
        ));
    }
    expected.sort_unstable();

    let query = graph_query(
        &["from", "to"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "from".to_string(),
            to_alias: "to".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BOUND_BUCKET_B".to_string(), "BOUND_BUCKET_A".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("keep".to_string()),
            }),
        })],
    );
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let edge = &runtime.edges[0];
    let mut frontier_peak = 0;

    engine.reset_query_execution_counters_for_test();
    let read = view
        .graph_row_delegated_bound_bucket_read(
            &normalized,
            edge,
            &[outgoing, outgoing],
            &[incoming, incoming],
            &[both, both],
            i64::MAX / 2,
            None,
            &mut frontier_peak,
            true,
            None,
        )
        .unwrap();
    let GraphRowDelegatedEdgeSourceRead::Ready {
        metadata,
        followups,
        buckets,
        label_branches,
        drivers,
        planned_modes,
        ..
    } = read
    else {
        panic!("endpoint-anchored bucket reads must remain independently legal");
    };
    assert!(followups.is_empty());
    assert_eq!(buckets, vec!["outgoing", "incoming", "both"]);
    assert_eq!(label_branches, 2);
    assert_eq!(drivers.len(), 6);
    assert_eq!(planned_modes, vec!["eager"]);
    assert_eq!(
        metadata.iter().map(|meta| meta.id).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(frontier_peak, expected.len());
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 6);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 6);
}

#[test]
fn graph_row_delegated_bucket_mapping_integrates_direction_and_bound_side() {
    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        direction: Direction,
        bind_from: bool,
        expected_bucket: &'static str,
    }

    for case in [
        Case {
            name: "outgoing_from",
            direction: Direction::Outgoing,
            bind_from: true,
            expected_bucket: "outgoing",
        },
        Case {
            name: "outgoing_to",
            direction: Direction::Outgoing,
            bind_from: false,
            expected_bucket: "incoming",
        },
        Case {
            name: "incoming_from",
            direction: Direction::Incoming,
            bind_from: true,
            expected_bucket: "incoming",
        },
        Case {
            name: "incoming_to",
            direction: Direction::Incoming,
            bind_from: false,
            expected_bucket: "outgoing",
        },
        Case {
            name: "both_from",
            direction: Direction::Both,
            bind_from: true,
            expected_bucket: "both",
        },
        Case {
            name: "both_to",
            direction: Direction::Both,
            bind_from: false,
            expected_bucket: "both",
        },
    ] {
        let (_dir, engine) = graph_row_test_engine();
        let label = format!("BOUND_MAPPING_{}", case.name.to_ascii_uppercase());
        let logical_from =
            insert_graph_row_node(&engine, "BoundMapping", &format!("{}-from", case.name), &[]);
        let logical_to =
            insert_graph_row_node(&engine, "BoundMapping", &format!("{}-to", case.name), &[]);
        let decoy =
            insert_graph_row_node(&engine, "BoundMapping", &format!("{}-decoy", case.name), &[]);
        let bound = if case.bind_from {
            logical_from
        } else {
            logical_to
        };
        let props = [("status", PropValue::String("keep".to_string()))];
        let mut expected = Vec::new();

        match case.direction {
            Direction::Outgoing => expected.push(insert_graph_row_edge(
                &engine,
                logical_from,
                logical_to,
                &label,
                &props,
            )),
            Direction::Incoming => expected.push(insert_graph_row_edge(
                &engine,
                logical_to,
                logical_from,
                &label,
                &props,
            )),
            Direction::Both => {
                let other = insert_graph_row_node(
                    &engine,
                    "BoundMapping",
                    &format!("{}-other", case.name),
                    &[],
                );
                expected.push(insert_graph_row_edge(
                    &engine,
                    bound,
                    if case.bind_from { logical_to } else { logical_from },
                    &label,
                    &props,
                ));
                expected.push(insert_graph_row_edge(
                    &engine,
                    other,
                    bound,
                    &label,
                    &props,
                ));
                expected.push(insert_graph_row_edge(
                    &engine, bound, bound, &label, &props,
                ));
            }
        }

        if case.expected_bucket == "outgoing" {
            insert_graph_row_edge(&engine, decoy, bound, &label, &props);
        } else if case.expected_bucket == "incoming" {
            insert_graph_row_edge(&engine, bound, decoy, &label, &props);
        }

        let mut query = graph_query(
            &["from", "to"],
            vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("edge".to_string()),
                from_alias: "from".to_string(),
                to_alias: "to".to_string(),
                direction: case.direction,
                label_filter: vec![label],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("keep".to_string()),
                }),
            })],
        );
        query.nodes[usize::from(!case.bind_from)].ids = vec![bound];
        query.options.include_plan = true;
        query.return_items = Some(vec![graph_return_binding(
            "edge",
            GraphReturnProjection::IdOnly,
        )]);

        engine.reset_query_execution_counters_for_test();
        let result = engine.query_graph_rows(&query).unwrap();
        let mut actual = graph_row_single_u64_column(result.clone());
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected, "mapping case {}", case.name);
        assert_graph_row_explain_contains(
            result.plan.as_ref().unwrap(),
            &format!("buckets={}", case.expected_bucket),
        );
        let counters = engine.query_execution_counter_snapshot_for_test();
        assert_eq!(counters.graph_row_delegated_edge_queries, 1, "{}", case.name);
        assert_eq!(
            counters.graph_row_delegated_verified_candidates,
            expected.len(),
            "{}",
            case.name
        );
    }

    let (_dir, engine) = graph_row_test_engine();
    let anchor = insert_graph_row_node(&engine, "BoundMappingBoth", "anchor", &[]);
    let from = insert_graph_row_node(&engine, "BoundMappingBoth", "from", &[]);
    let to = insert_graph_row_node(&engine, "BoundMappingBoth", "to", &[]);
    let from_decoy = insert_graph_row_node(&engine, "BoundMappingBoth", "from-decoy", &[]);
    let to_decoy = insert_graph_row_node(&engine, "BoundMappingBoth", "to-decoy", &[]);
    let props = [("status", PropValue::String("keep".to_string()))];
    insert_graph_row_edge(&engine, anchor, from, "BOUND_MAPPING_PREFIX_FROM", &[]);
    insert_graph_row_edge(&engine, from, to, "BOUND_MAPPING_PREFIX_TO", &[]);
    let expected = insert_graph_row_edge(
        &engine,
        from,
        to,
        "BOUND_MAPPING_BOTH_BOUND",
        &props,
    );
    insert_graph_row_edge(
        &engine,
        from,
        from_decoy,
        "BOUND_MAPPING_BOTH_BOUND",
        &props,
    );
    insert_graph_row_edge(
        &engine,
        to_decoy,
        to,
        "BOUND_MAPPING_BOTH_BOUND",
        &props,
    );
    let mut query = graph_query(
        &["anchor", "from", "to"],
        vec![
            graph_edge_with_label(
                Some("prefix_from"),
                "anchor",
                "from",
                "BOUND_MAPPING_PREFIX_FROM",
            ),
            graph_edge_with_label(
                Some("prefix_to"),
                "from",
                "to",
                "BOUND_MAPPING_PREFIX_TO",
            ),
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                    alias: Some("edge".to_string()),
                    from_alias: "from".to_string(),
                    to_alias: "to".to_string(),
                    direction: Direction::Outgoing,
                    label_filter: vec!["BOUND_MAPPING_BOTH_BOUND".to_string()],
                    filter: Some(EdgeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("keep".to_string()),
                    }),
                })],
                where_: None,
            }),
        ],
    );
    query.nodes[0].ids = vec![anchor];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![expected]);
    assert_graph_row_explain_contains(result.plan.as_ref().unwrap(), "buckets=outgoing,incoming");
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 4);
}

#[test]
fn graph_row_bound_delegated_valid_at_is_planner_invisible_but_verifier_visible() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "BoundValidAt", "source", &[]);
    let target = insert_graph_row_node(&engine, "BoundValidAt", "target", &[]);
    let edge = insert_graph_row_edge(
        &engine,
        source,
        target,
        "BOUND_VALID_AT",
        &[("status", PropValue::String("keep".to_string()))],
    );
    set_graph_row_edge_validity(&engine, edge, 100, 200);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BOUND_VALID_AT".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("keep".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.at_epoch = Some(150);
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_planning_probe_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![edge]
    );
    assert_eq!(
        engine
            .query_planning_probe_snapshot_for_test()
            .edge_validity_range_estimates,
        0
    );

    query.at_epoch = Some(200);
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
}

#[test]
fn graph_row_delegated_endpoint_proof_skips_only_unconstrained_node_patterns() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "EndpointProof", "source", &[]);
    let keep_target = insert_graph_row_node(
        &engine,
        "EndpointProof",
        "keep",
        &[("status", PropValue::String("keep".to_string()))],
    );
    let drop_target = insert_graph_row_node(
        &engine,
        "EndpointProof",
        "drop",
        &[("status", PropValue::String("drop".to_string()))],
    );
    let keep_edge = insert_graph_row_edge(
        &engine,
        source,
        keep_target,
        "ENDPOINT_PROOF",
        &[("edge_status", PropValue::String("keep".to_string()))],
    );
    let drop_edge = insert_graph_row_edge(
        &engine,
        source,
        drop_target,
        "ENDPOINT_PROOF",
        &[("edge_status", PropValue::String("keep".to_string()))],
    );

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["ENDPOINT_PROOF".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "edge_status".to_string(),
                value: PropValue::String("keep".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge, drop_edge]
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_visibility_meta_reads,
        2
    );

    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_visibility_meta_reads,
        4,
        "the constrained target must retain its two-node verifier read"
    );

    query.nodes[1].filter = None;
    engine.delete_node(drop_target).unwrap();
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge],
        "the shared edge verifier must reject the deleted endpoint before proof reuse"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_visibility_meta_reads,
        2
    );
}

#[test]
fn graph_row_bound_mode_reuses_streamed_endpoint_and_selective_eager_edge_plans() {
    let fixture = build_edge_streaming_scale_test_fixture(false);
    let hub = fixture.hub_source_ids[0];
    let make_graph_query = |filter: EdgeFilterExpr| {
        let mut query = graph_query(
            &["source", "target"],
            vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("edge".to_string()),
                from_alias: "source".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["RELATES_TO".to_string()],
                filter: Some(filter),
            })],
        );
        query.nodes[0].ids = vec![hub];
        query.options.include_plan = true;
        query.page.limit = 100;
        query.return_items = Some(vec![graph_return_binding(
            "edge",
            GraphReturnProjection::IdOnly,
        )]);
        query
    };

    let broad_filter = EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    };
    let broad = make_graph_query(broad_filter.clone());
    let broad_oracle = fixture
        .engine
        .query_edge_ids(&EdgeQuery {
            label: Some("RELATES_TO".to_string()),
            from_ids: vec![hub],
            filter: Some(broad_filter),
            page: PageRequest {
                limit: Some(100),
                after: None,
            },
            ..Default::default()
        })
        .unwrap()
        .edge_ids;
    fixture.engine.reset_query_execution_counters_for_test();
    let broad_result = fixture.engine.query_graph_rows(&broad).unwrap();
    assert_eq!(graph_row_single_u64_column(broad_result.clone()), broad_oracle);
    let broad_explain = broad_result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(broad_explain, "source=DelegatedEdgeQuery");
    assert_graph_row_explain_contains(broad_explain, "EdgeEndpointAdjacency");
    assert_graph_row_explain_contains(broad_explain, "planned_modes=streamed");
    let counters = fixture.engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(
        counters.node_visibility_meta_reads, 2,
        "only the initial bound anchor and its constrained source pattern should reread nodes"
    );
    assert!(counters.streamed_edge_intersect_drivers > 0);
    assert!(counters.streamed_edge_cursor_seeks > 0);

    let selective_filter = EdgeFilterExpr::And(vec![
        EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        },
        EdgeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("t000".to_string()),
        },
    ]);
    let selective = make_graph_query(selective_filter.clone());
    let selective_oracle = fixture
        .engine
        .query_edge_ids(&EdgeQuery {
            label: Some("RELATES_TO".to_string()),
            from_ids: vec![hub],
            filter: Some(selective_filter),
            page: PageRequest {
                limit: Some(100),
                after: None,
            },
            ..Default::default()
        })
        .unwrap()
        .edge_ids;
    fixture.engine.reset_query_execution_counters_for_test();
    let selective_result = fixture.engine.query_graph_rows(&selective).unwrap();
    assert_eq!(
        graph_row_single_u64_column(selective_result.clone()),
        selective_oracle
    );
    let selective_explain = selective_result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(selective_explain, "source=DelegatedEdgeQuery");
    assert_graph_row_explain_contains(selective_explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(selective_explain, "planned_modes=eager");
    let counters = fixture.engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.node_visibility_meta_reads, 2);
    assert_eq!(counters.streamed_edge_intersect_drivers, 0);
    assert_eq!(counters.streamed_edge_cursor_seeks, 0);
}

#[test]
fn graph_row_unbound_edge_filters_use_planner_before_candidate_cap() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Person", "planner-source", &[]);
    let mut keep = None;
    for index in 0..8 {
        let target = insert_graph_row_node(&engine, "Person", &format!("planner-target-{index}"), &[]);
        let status = if index == 3 { "keep" } else { "drop" };
        let edge = insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_PLANNER_EDGE",
            &[("status", PropValue::String(status.to_string()))],
        );
        if index == 3 {
            keep = Some(edge);
        }
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_PLANNER_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_PLANNER_EDGE".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("keep".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.options.max_intermediate_bindings = 2;
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep.unwrap()]
    );
}

#[test]
fn graph_row_verified_metadata_single_chunk_has_no_orientation_reread_or_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "VerifiedMetadata", "source", &[]);
    let first = insert_graph_row_node(&engine, "VerifiedMetadata", "first", &[]);
    let second = insert_graph_row_node(&engine, "VerifiedMetadata", "second", &[]);
    let first_edge = insert_graph_row_edge(
        &engine,
        source,
        first,
        "GRAPH_ROW_VERIFIED_METADATA",
        &[],
    );
    let second_edge = insert_graph_row_edge(
        &engine,
        source,
        second,
        "GRAPH_ROW_VERIFIED_METADATA",
        &[],
    );

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_VERIFIED_METADATA".to_string()],
            filter: None,
        })],
    );
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![first_edge, second_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 2);
    assert_eq!(counters.edge_verifier_metadata_lookup_calls, 1);
    assert_eq!(counters.edge_verifier_metadata_lookup_ids, 2);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn graph_row_unbound_delegates_filtered_unfiltered_and_multi_label_branches() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedSource", "delegated-source", &[]);
    let first = insert_graph_row_node(&engine, "DelegatedTarget", "delegated-first", &[]);
    let second = insert_graph_row_node(&engine, "DelegatedTarget", "delegated-second", &[]);
    let third = insert_graph_row_node(&engine, "DelegatedTarget", "delegated-third", &[]);
    let first_edge = insert_graph_row_edge(
        &engine,
        source,
        first,
        "GRAPH_ROW_DELEGATED_A",
        &[("status", PropValue::String("keep".to_string()))],
    );
    let second_edge = insert_graph_row_edge(
        &engine,
        source,
        second,
        "GRAPH_ROW_DELEGATED_B",
        &[("status", PropValue::String("keep".to_string()))],
    );
    let third_edge = insert_graph_row_edge(
        &engine,
        source,
        third,
        "GRAPH_ROW_DELEGATED_A",
        &[("status", PropValue::String("drop".to_string()))],
    );

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            // One delegated read per normalized label branch.
            label_filter: vec![
                "GRAPH_ROW_DELEGATED_A".to_string(),
                "GRAPH_ROW_DELEGATED_B".to_string(),
            ],
            filter: None,
        })],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    engine.reset_query_planning_probe_counters_for_test();
    let unfiltered = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_single_u64_column(unfiltered.clone()),
        vec![first_edge, second_edge, third_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 3);
    let explain = unfiltered.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "materialized_source=EdgePullDelegatedMetadata");
    assert_graph_row_explain_contains(
        explain,
        "context=executed prepared graph-row edge anchor source",
    );
    assert_graph_row_explain_contains(explain, "prepared_source=NativeCursor");
    assert_graph_row_explain_contains(explain, "source_pulls=2; successful_leaves=2");
    assert_graph_row_explain_not_contains(explain, "EdgeValidityIndex");
    assert_eq!(
        engine
            .query_planning_probe_snapshot_for_test()
            .edge_validity_range_estimates,
        0,
        "system-only effective time must remain invisible to validity planning estimates"
    );

    if let GraphPatternPiece::Edge(edge) = &mut query.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        });
    }
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![first_edge, second_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 2);
}

#[test]
fn graph_row_unbound_valid_at_injection_does_not_legalize_anchorless_read() {
    let (_dir, engine) = graph_row_test_engine();
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge(Some("edge"), "source", "target")],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let without_explicit_epoch = engine.query_graph_rows(&query).unwrap_err().to_string();
    let without_epoch_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(without_epoch_counters.graph_row_edge_pull_executions, 0);
    assert_eq!(without_epoch_counters.graph_row_delegated_edge_queries, 0);
    assert_eq!(without_epoch_counters.graph_row_chunks_executed, 0);
    query.at_epoch = Some(123);
    engine.reset_query_execution_counters_for_test();
    let with_explicit_epoch = engine.query_graph_rows(&query).unwrap_err().to_string();
    let with_epoch_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(with_epoch_counters.graph_row_edge_pull_executions, 0);
    assert_eq!(with_epoch_counters.graph_row_delegated_edge_queries, 0);
    assert_eq!(with_epoch_counters.graph_row_chunks_executed, 0);

    assert_eq!(without_explicit_epoch, with_explicit_epoch);
    assert_eq!(
        with_explicit_epoch,
        "invalid operation: graph row required edge pattern requires an anchor or allow_full_scan=true"
    );
}

#[test]
fn graph_row_valid_at_split_keeps_legal_metadata_anchor_temporally_correct() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedGuardSkip", "guard-skip-source", &[]);
    let live_target =
        insert_graph_row_node(&engine, "DelegatedGuardSkip", "guard-skip-live", &[]);
    let expired_target =
        insert_graph_row_node(&engine, "DelegatedGuardSkip", "guard-skip-expired", &[]);
    let live = insert_graph_row_weighted_edge(
        &engine,
        source,
        live_target,
        "GRAPH_ROW_DELEGATED_GUARD_SKIP",
        &[],
        1.0,
    );
    set_graph_row_edge_validity(&engine, live, 0, i64::MAX);
    let expired = insert_graph_row_weighted_edge(
        &engine,
        source,
        expired_target,
        "GRAPH_ROW_DELEGATED_GUARD_SKIP",
        &[],
        1.0,
    );
    set_graph_row_edge_validity(&engine, expired, 0, 100);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            // WeightRange is independently legal to the edge planner, but it is
            // intentionally absent from DEC-42-018's syntactic injection guard.
            label_filter: Vec::new(),
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: Some(0.5),
                upper: Some(1.5),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    query.at_epoch = Some(50);
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![live, expired]
    );
    query.at_epoch = Some(150);
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![live],
        "the independently planned metadata anchor must still enforce the system epoch in verification"
    );
}

#[test]
fn graph_row_delegated_stale_candidates_do_not_consume_frontier_cap() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedStale", "stale-source", &[]);
    let mut edge_ids = Vec::new();
    for index in 0..8 {
        let target = insert_graph_row_node(
            &engine,
            "DelegatedStale",
            &format!("stale-target-{index}"),
            &[],
        );
        edge_ids.push(insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_DELEGATED_STALE",
            &[("color", PropValue::String("red".to_string()))],
        ));
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index(
            "GRAPH_ROW_DELEGATED_STALE",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("color")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    // Shadow seven segment records with a non-matching active record. The red
    // sidecar still returns eight raw IDs, but the delegated page returns only
    // the one edge verified against latest-visible state.
    for edge_id in edge_ids.iter().skip(1) {
        let old = internal_edge_record(&engine, *edge_id).unwrap().unwrap();
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(EdgeRecord {
                props: graph_row_props(&[("color", PropValue::String("blue".to_string()))]),
                updated_at: old.updated_at.saturating_add(1),
                ..old
            }),
        )
        .unwrap();
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_DELEGATED_STALE".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "color".to_string(),
                value: PropValue::String("red".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.options.max_frontier = 2;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![edge_ids[0]]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 1);

    query.nodes[0].ids = vec![source];
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![edge_ids[0]],
        "bound delegation must cap the verified result rather than stale raw postings"
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 1);
}

#[test]
fn graph_row_edge_pull_verified_candidates_above_cap_succeed_in_bounded_leaves() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedCap", "cap-source", &[]);
    for index in 0..3 {
        let target = insert_graph_row_node(
            &engine,
            "DelegatedCap",
            &format!("cap-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_DELEGATED_CAP", &[]);
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_DELEGATED_CAP",
        )],
    );
    query.options.allow_full_scan = false;
    query.options.max_frontier = 2;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(result.rows.len(), 3);
    assert!(result.stats.frontier_peak <= 2);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_edge_pull_executions, 1);
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 3);
    assert_eq!(counters.graph_row_chunks_executed, 2);
    assert_eq!(counters.graph_row_node_verification_resolutions, 0);
    assert_eq!(counters.graph_row_node_verification_set_builds, 0);
    assert_eq!(counters.graph_row_fixed_edge_bucket_builds, 0);
    assert_eq!(counters.graph_row_all_candidate_endpoints_proven, 4);
    assert_eq!(counters.graph_row_fixed_edge_workspace_growths, 1);

    query.nodes[0].ids = vec![source];
    if let GraphPatternPiece::Edge(edge) = &mut query.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::ValidAt {
            epoch_ms: i64::MAX / 2,
        });
    }
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_graph_rows(&query).unwrap_err().to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 2"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );
}

#[test]
fn graph_row_edge_pull_constrained_node_cache_reuses_positive_and_negative_results() {
    let (dir, engine) = graph_row_test_engine();
    let db_path = dir.path().join("db");
    let source = insert_graph_row_node(
        &engine,
        "EdgePullCacheSource",
        "cache-source",
        &[("kind", PropValue::String("good".to_string()))],
    );
    let target = insert_graph_row_node(
        &engine,
        "EdgePullCacheOther",
        "cache-target",
        &[("kind", PropValue::String("other".to_string()))],
    );
    insert_graph_row_edge(&engine, source, target, "EDGE_PULL_CACHE_A", &[]);
    insert_graph_row_edge(&engine, source, target, "EDGE_PULL_CACHE_B", &[]);
    let policy_hidden = engine
        .upsert_node(
            "EdgePullCacheOther",
            "cache-policy-hidden",
            UpsertNodeOptions {
                weight: 0.1,
                props: graph_row_props(&[(
                    "kind",
                    PropValue::String("wanted".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    insert_graph_row_edge(&engine, source, policy_hidden, "EDGE_PULL_CACHE_A", &[]);
    insert_graph_row_edge(&engine, source, policy_hidden, "EDGE_PULL_CACHE_B", &[]);
    engine
        .set_prune_policy(
            "edge-pull-cache-hidden",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("EdgePullCacheOther".to_string()),
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec![
                "EDGE_PULL_CACHE_A".to_string(),
                "EDGE_PULL_CACHE_B".to_string(),
            ],
            filter: None,
        })],
    );
    query.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("good".to_string()),
    });
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("wanted".to_string()),
    });
    query.options.allow_full_scan = false;
    query.options.max_frontier = 1;
    query.options.include_plan = true;
    query.page.limit = 1;
    query.at_epoch = Some(now_millis());

    let assert_cache = |engine: &DatabaseEngine| {
        let mut oracle_query = query.clone();
        oracle_query.options.max_frontier = 16;
        oracle_query.options.max_intermediate_bindings = 16;
        oracle_query.options.include_plan = false;
        let oracle = graph_row_chunk_runtime_once_result(engine, &oracle_query);
        engine.reset_query_execution_counters_for_test();
        let result = engine.query_graph_rows(&query).unwrap();
        graph_row_chunk_assert_page_parity(
            &result,
            &oracle,
            "constrained cache policy warm/reopen RuntimeOnce oracle",
        );
        assert!(result.rows.is_empty());
        let counters = engine.query_execution_counter_snapshot_for_test();
        assert_eq!(counters.graph_row_edge_pull_executions, 1);
        assert_eq!(counters.graph_row_node_verification_resolutions, 2);
        assert_eq!(counters.graph_row_node_verification_cache_hits, 2);
        assert_eq!(counters.graph_row_node_verification_cache_units_peak, 2);
        assert_eq!(counters.graph_row_node_verification_set_builds, 4);
        assert_eq!(counters.graph_row_all_candidate_endpoints_proven, 0);
        assert_eq!(counters.graph_row_fixed_edge_bucket_builds, 0);
    };

    engine.reset_query_execution_counters_for_test();
    let verification_error = with_graph_row_test_node_verification_failure(|| {
        engine.query_graph_rows(&query).unwrap_err()
    });
    assert_eq!(
        verification_error.to_string(),
        "invalid operation: injected graph-row node verification failure"
    );
    let failed = engine.query_execution_counter_snapshot_for_test();
    assert!(failed.graph_row_node_verification_resolutions > 0);
    assert_eq!(
        failed.graph_row_node_verification_cache_units_peak, 0,
        "a failed miss batch must admit no positive or negative cache entries"
    );

    assert_cache(&engine);
    engine.flush().unwrap();
    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_cache(&reopened);
}

#[test]
fn graph_row_valid_at_delegation_matches_open_and_bounded_window_oracle() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedValidAt", "valid-source", &[]);
    let targets = (0..4)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "DelegatedValidAt",
                &format!("valid-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let windows = [
        (0, i64::MAX),
        (100, i64::MAX),
        (0, 200),
        (100, 200),
    ];
    let edge_ids = targets
        .iter()
        .zip(windows)
        .map(|(target, (valid_from, valid_to))| {
            engine
                .upsert_edge(
                    source,
                    *target,
                    "GRAPH_ROW_DELEGATED_VALID_AT",
                    UpsertEdgeOptions {
                        valid_from: Some(valid_from),
                        valid_to: Some(valid_to),
                        ..Default::default()
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_DELEGATED_VALID_AT",
        )],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    let cases = [
        (99, vec![edge_ids[0], edge_ids[2]]),
        (100, edge_ids.clone()),
        (199, edge_ids.clone()),
        (200, vec![edge_ids[0], edge_ids[1]]),
    ];
    for (epoch, expected) in cases {
        query.at_epoch = Some(epoch);
        assert_eq!(
            graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
            expected,
            "delegated ValidAt must use valid_from <= epoch < valid_to at epoch {epoch}"
        );
    }
}

#[test]
fn graph_row_valid_at_user_filter_composes_with_injected_effective_epoch() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedValidCompose", "compose-source", &[]);
    let open_target =
        insert_graph_row_node(&engine, "DelegatedValidCompose", "compose-open", &[]);
    let bounded_target =
        insert_graph_row_node(&engine, "DelegatedValidCompose", "compose-bounded", &[]);
    let open = insert_graph_row_edge(
        &engine,
        source,
        open_target,
        "GRAPH_ROW_DELEGATED_VALID_COMPOSE",
        &[],
    );
    set_graph_row_edge_validity(&engine, open, 0, i64::MAX);
    let bounded = engine
        .upsert_edge(
            source,
            bounded_target,
            "GRAPH_ROW_DELEGATED_VALID_COMPOSE",
            UpsertEdgeOptions {
                valid_from: Some(100),
                valid_to: Some(200),
                ..Default::default()
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_DELEGATED_VALID_COMPOSE".to_string()],
            // The delegated helper must AND this user predicate with the
            // effective query epoch instead of replacing either condition.
            filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 150 }),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    query.at_epoch = Some(150);
    engine.reset_query_planning_probe_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![open, bounded]
    );
    assert!(
        engine
            .query_planning_probe_snapshot_for_test()
            .edge_validity_range_estimates
            >= 2,
        "user-supplied ValidAt must remain visible to valid_from and valid_to estimates"
    );
    query.at_epoch = Some(200);
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![open]
    );
}

#[test]
fn graph_row_orientation_delegated_metadata_pass_preserves_direction_and_visibility() {
    let (_dir, engine) = graph_row_test_engine();
    let first_source =
        insert_graph_row_node(&engine, "DelegatedOrientation", "orientation-source-a", &[]);
    let first_target =
        insert_graph_row_node(&engine, "DelegatedOrientation", "orientation-target-a", &[]);
    let first_edge = insert_graph_row_edge(
        &engine,
        first_source,
        first_target,
        "GRAPH_ROW_DELEGATED_ORIENTATION",
        &[],
    );
    set_graph_row_edge_validity(&engine, first_edge, 0, i64::MAX);
    engine.flush().unwrap();

    let second_source =
        insert_graph_row_node(&engine, "DelegatedOrientation", "orientation-source-b", &[]);
    let second_target =
        insert_graph_row_node(&engine, "DelegatedOrientation", "orientation-target-b", &[]);
    let second_edge = insert_graph_row_edge(
        &engine,
        second_source,
        second_target,
        "GRAPH_ROW_DELEGATED_ORIENTATION",
        &[],
    );
    set_graph_row_edge_validity(&engine, second_edge, 0, i64::MAX);
    let expired_target =
        insert_graph_row_node(&engine, "DelegatedOrientation", "orientation-expired", &[]);
    engine
        .upsert_edge(
            second_source,
            expired_target,
            "GRAPH_ROW_DELEGATED_ORIENTATION",
            UpsertEdgeOptions {
                valid_from: Some(10),
                valid_to: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    let deleted_target =
        insert_graph_row_node(&engine, "DelegatedOrientation", "orientation-deleted", &[]);
    let deleted_edge = insert_graph_row_edge(
        &engine,
        first_source,
        deleted_target,
        "GRAPH_ROW_DELEGATED_ORIENTATION",
        &[],
    );
    set_graph_row_edge_validity(&engine, deleted_edge, 0, i64::MAX);
    engine.delete_node(deleted_target).unwrap();

    let mut query = graph_query(
        &["left", "right"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "left".to_string(),
            to_alias: "right".to_string(),
            direction: Direction::Incoming,
            label_filter: vec!["GRAPH_ROW_DELEGATED_ORIENTATION".to_string()],
            filter: None,
        })],
    );
    query.at_epoch = Some(150);
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![
        graph_return_binding("left", GraphReturnProjection::IdOnly),
        graph_return_binding("right", GraphReturnProjection::IdOnly),
        graph_return_binding("edge", GraphReturnProjection::IdOnly),
    ]);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    let mut actual = result
        .rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [GraphValue::NodeId(left), GraphValue::NodeId(right), GraphValue::EdgeId(edge)] => {
                (*left, *right, *edge)
            }
            other => panic!("expected oriented node/node/edge row, got {other:?}"),
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = vec![
        (first_target, first_source, first_edge),
        (second_target, second_source, second_edge),
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);

    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 2);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_selected_field_ids, 0);
}

#[test]
fn graph_row_verified_metadata_terminal_matches_full_verifier_across_source_states() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedOrientationOracle", "oracle-source", &[]);
    let targets = (0..9)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "DelegatedOrientationOracle",
                &format!("oracle-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let label = "GRAPH_ROW_DELEGATED_ORIENTATION_ORACLE";
    let other_label = "GRAPH_ROW_DELEGATED_ORIENTATION_DRIFTED";

    let segment_keep = insert_graph_row_edge(&engine, source, targets[0], label, &[]);
    set_graph_row_edge_validity(&engine, segment_keep, 0, i64::MAX);
    let drifted = insert_graph_row_edge(&engine, source, targets[1], label, &[]);
    let tombstoned = insert_graph_row_edge(&engine, source, targets[2], label, &[]);
    let label_token_seed = insert_graph_row_edge(&engine, source, targets[3], other_label, &[]);
    engine.flush().unwrap();

    let other_label_id = engine.get_edge_label_id(other_label).unwrap().unwrap();
    let drifted_record = internal_edge_record(&engine, drifted).unwrap().unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(EdgeRecord {
            label_id: other_label_id,
            updated_at: drifted_record.updated_at.saturating_add(1),
            ..drifted_record
        }),
    )
    .unwrap();
    engine.delete_edge(tombstoned).unwrap();
    let immutable_keep = insert_graph_row_edge(&engine, source, targets[4], label, &[]);
    set_graph_row_edge_validity(&engine, immutable_keep, 0, i64::MAX);
    engine.freeze_memtable().unwrap();

    let active_keep = insert_graph_row_edge(&engine, source, targets[5], label, &[]);
    set_graph_row_edge_validity(&engine, active_keep, 0, i64::MAX);
    let expired = insert_graph_row_edge(&engine, source, targets[6], label, &[]);
    set_graph_row_edge_validity(&engine, expired, 0, 100);
    let deleted_endpoint = insert_graph_row_edge(&engine, source, targets[7], label, &[]);
    set_graph_row_edge_validity(&engine, deleted_endpoint, 0, i64::MAX);
    engine.delete_node(targets[7]).unwrap();

    let edge_query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 150 }),
        page: PageRequest::default(),
        ..Default::default()
    };
    let verified_ids = engine.query_edge_ids(&edge_query).unwrap().edge_ids;
    assert_eq!(verified_ids, vec![segment_keep, immutable_keep, active_keep]);
    for rejected in [drifted, tombstoned, label_token_seed, expired, deleted_endpoint] {
        assert!(!verified_ids.contains(&rejected));
    }

    let query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec![label.to_string()],
            filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 150 }),
        })],
    );
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let edge = &runtime.edges[0];
    let normalized_edge = view.normalize_edge_query(&edge_query).unwrap();
    let planned = view
        .plan_normalized_edge_query(&normalized_edge)
        .unwrap();
    let (page, followups) = view
        .query_edge_metadata_page_planned(&normalized_edge, planned, None)
        .unwrap();
    assert!(followups.is_empty());
    let retained = page.metadata;
    let full = view
        .graph_row_verify_edge_candidates(&verified_ids, edge, 150, None)
        .unwrap();
    assert_eq!(
        retained.iter().map(|meta| meta.id).collect::<Vec<_>>(),
        full.iter().map(|meta| meta.id).collect::<Vec<_>>()
    );
    assert_eq!(
        retained.iter()
            .map(|meta| (meta.id, meta.from, meta.to, meta.label_id))
            .collect::<Vec<_>>(),
        full.iter()
            .map(|meta| (meta.id, meta.from, meta.to, meta.label_id))
            .collect::<Vec<_>>()
    );
}

fn prepared_edge_metadata_pull_find_source(
    plan: &EdgePhysicalPlan,
    kind: EdgeQueryCandidateSourceKind,
) -> Option<PlannedEdgeCandidateSource> {
    match plan {
        EdgePhysicalPlan::Source(source) if source.kind == kind => Some(source.clone()),
        EdgePhysicalPlan::BufferedIdSort(input) => {
            prepared_edge_metadata_pull_find_source(input, kind)
        }
        EdgePhysicalPlan::Intersect { inputs, .. } | EdgePhysicalPlan::Union { inputs, .. } => {
            inputs
                .iter()
                .find_map(|input| prepared_edge_metadata_pull_find_source(input, kind))
        }
        EdgePhysicalPlan::Empty | EdgePhysicalPlan::Source(_) => None,
    }
}

#[test]
fn prepared_edge_metadata_pull_native_label_continues_once_across_flush() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    let source_node = insert_query_node(&engine, "Person", "prepared-source", &[], 1.0);
    let targets = (0..7)
        .map(|index| {
            insert_query_node(
                &engine,
                "Person",
                &format!("prepared-target-{index}"),
                &[],
                1.0,
            )
        })
        .collect::<Vec<_>>();
    let label = "PREPARED_EDGE_LABEL";
    let mut expected = Vec::new();
    for target in &targets[..3] {
        expected.push(
            engine
                .upsert_edge(source_node, *target, label, UpsertEdgeOptions::default())
                .unwrap(),
        );
    }
    engine.flush().unwrap();
    for target in &targets[3..6] {
        expected.push(
            engine
                .upsert_edge(source_node, *target, label, UpsertEdgeOptions::default())
                .unwrap(),
        );
    }

    let query = EdgeQuery {
        label: Some(label.to_string()),
        page: PageRequest {
            limit: Some(4),
            ..Default::default()
        },
        ..Default::default()
    };
    let view = engine.published_state().view.clone();
    let planning_query = view.normalize_edge_query(&query).unwrap();
    let execution_query = planning_query.clone();
    reset_prepared_edge_source_test_snapshot();
    let (mut prepared, preparation_followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(
            &planning_query,
            &execution_query,
            None,
        )
        .unwrap();
    assert!(preparation_followups.is_empty());
    assert_eq!(prepared.planning_limit(), 4);
    assert_eq!(prepared.prepared_mode(), PreparedEdgeSourceMode::NativeCursor);
    assert_eq!(prepared.fallback(), PreparedEdgeFallback::None);
    assert_eq!(prepared.retained_id_units(), 0);

    let zero_error = prepared.pull(0).unwrap_err().to_string();
    assert_eq!(
        zero_error,
        "invalid operation: prepared edge metadata pull limit must be in 1..=4, got 0"
    );
    let above_error = prepared.pull(5).unwrap_err().to_string();
    assert_eq!(
        above_error,
        "invalid operation: prepared edge metadata pull limit must be in 1..=4, got 5"
    );

    let first = prepared.pull(1).unwrap();
    assert_eq!(first.metadata.len(), 1);
    assert!(first.has_more);
    assert!(first.followups.is_empty());
    let mut actual = first.metadata.into_iter().map(|meta| meta.id).collect::<Vec<_>>();

    let post_prepare = engine
        .upsert_edge(
            source_node,
            targets[6],
            label,
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine.flush().unwrap();

    loop {
        let pull = prepared.pull(1).unwrap();
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert_eq!(actual, expected);
    assert!(!actual.contains(&post_prepare));

    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.branch_plans, 1);
    assert_eq!(snapshot.preparations, 1);
    assert_eq!(snapshot.native_cursor_builds, 2);
    assert_eq!(snapshot.streamed_cursor_builds, 0);
    assert_eq!(snapshot.eager_materializations, 0);
    assert_eq!(snapshot.buffered_sorts, 0);
    assert_eq!(snapshot.fallback_selections, 0);
    assert_eq!(snapshot.retained_id_units_peak, 0);
    assert!(snapshot.verification_endpoint_cache_units_peak
        <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP + 2 * QUERY_VERIFY_CHUNK);

    drop(prepared);
    drop(view);
    engine.close().unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let reopened_view = reopened.published_state().view.clone();
    let reopened_query = reopened_view.normalize_edge_query(&query).unwrap();
    let (mut reopened_prepared, followups) = reopened_view
        .plan_and_prepare_edge_metadata_pull_source_for_test(
            &reopened_query,
            &reopened_query,
            None,
        )
        .unwrap();
    assert!(followups.is_empty());
    let mut reopened_ids = Vec::new();
    loop {
        let pull = reopened_prepared.pull(1).unwrap();
        reopened_ids.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    let mut reopened_expected = expected;
    reopened_expected.push(post_prepare);
    assert_eq!(reopened_ids, reopened_expected);
}

#[test]
fn prepared_edge_metadata_pull_empty_and_full_scan_modes_match_stateless_ids() {
    let (_dir, engine) = query_test_engine();
    let source_node = insert_query_node(&engine, "Person", "prepared-full-source", &[], 1.0);
    let targets = (0..5)
        .map(|index| {
            insert_query_node(
                &engine,
                "Person",
                &format!("prepared-full-target-{index}"),
                &[],
                1.0,
            )
        })
        .collect::<Vec<_>>();
    let expected = targets
        .iter()
        .map(|target| {
            engine
                .upsert_edge(
                    source_node,
                    *target,
                    "PREPARED_FULL_SCAN",
                    UpsertEdgeOptions::default(),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    engine.flush().unwrap();
    let view = engine.published_state().view.clone();

    let full_query = EdgeQuery {
        allow_full_scan: true,
        page: PageRequest {
            limit: Some(3),
            ..Default::default()
        },
        ..Default::default()
    };
    let normalized = view.normalize_edge_query(&full_query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    let (mut prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&normalized, &normalized, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(prepared.prepared_mode(), PreparedEdgeSourceMode::NativeCursor);
    let mut actual = Vec::new();
    loop {
        let pull = prepared.pull(2).unwrap();
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert_eq!(actual, expected);

    let planned_full_scan = view.plan_normalized_edge_query(&normalized).unwrap();
    let fallback_source = match &planned_full_scan.driver {
        EdgePhysicalPlan::Source(source) => source.clone(),
        _ => panic!("expected full-scan source"),
    };
    let forced_fallback = PlannedEdgeQuery {
        driver: EdgePhysicalPlan::Source(fallback_source.clone()),
        execution_mode: EdgeDriverExecutionMode::Streamed,
        cap_context: planned_full_scan.cap_context,
        legal_universe_fallback: Some(fallback_source),
        warnings: Vec::new(),
        followups: Vec::new(),
    };
    reset_prepared_edge_source_test_snapshot();
    let (mut fallback_prepared, followups) = view
        .prepare_edge_metadata_pull_source(
            &normalized,
            &normalized,
            forced_fallback,
            None,
        )
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(fallback_prepared.prepared_mode(), PreparedEdgeSourceMode::NativeCursor);
    assert_eq!(
        fallback_prepared.fallback(),
        PreparedEdgeFallback::PreparedLegalUniverse
    );
    let first = fallback_prepared.pull(1).unwrap();
    assert_eq!(first.metadata[0].id, expected[0]);
    assert_eq!(prepared_edge_source_test_snapshot().fallback_selections, 1);

    let empty = normalized.clone();
    let empty_plan = PlannedEdgeQuery {
        driver: EdgePhysicalPlan::Empty,
        execution_mode: EdgeDriverExecutionMode::Eager,
        cap_context: EdgeQueryCapContext::default(),
        legal_universe_fallback: None,
        warnings: Vec::new(),
        followups: Vec::new(),
    };
    let (mut prepared_empty, followups) = view
        .prepare_edge_metadata_pull_source(&empty, &empty, empty_plan, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(prepared_empty.prepared_mode(), PreparedEdgeSourceMode::Empty);
    let pull = prepared_empty.pull(1).unwrap();
    assert!(pull.metadata.is_empty());
    assert!(!pull.has_more);
}

#[test]
fn prepared_edge_metadata_pull_rejects_unreachable_anchored_sources() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "prepared-negative-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "prepared-negative-to", &[], 1.0);
    let edge = engine
        .upsert_edge(from, to, "PREPARED_NEGATIVE", UpsertEdgeOptions::default())
        .unwrap();
    let view = engine.published_state().view.clone();
    let cases = [
        EdgeQuery {
            ids: vec![edge],
            page: PageRequest {
                limit: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
        EdgeQuery {
            from_ids: vec![from],
            page: PageRequest {
                limit: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
        EdgeQuery {
            to_ids: vec![to],
            page: PageRequest {
                limit: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
        EdgeQuery {
            endpoint_ids: vec![from],
            page: PageRequest {
                limit: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
        EdgeQuery {
            label: Some("PREPARED_NEGATIVE".to_string()),
            from_ids: vec![from],
            to_ids: vec![to],
            page: PageRequest {
                limit: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
    ];
    for query in cases {
        let normalized = view.normalize_edge_query(&query).unwrap();
        let planned = view.plan_normalized_edge_query(&normalized).unwrap();
        let error = view
            .prepare_edge_metadata_pull_source(&normalized, &normalized, planned, None)
            .err()
            .expect("anchored source must be rejected");
        assert_eq!(
            error.to_string(),
            "invalid operation: prepared graph-row edge source received unreachable anchored input source"
        );
    }
}

#[test]
fn prepared_edge_metadata_pull_streamed_union_builds_once_across_many_pulls() {
    let (_dir, engine) = query_test_engine();
    let label = "PREPARED_STREAMED_UNION";
    ensure_ready_edge_property_index(&engine, label, "status", SecondaryIndexKind::Equality);
    ensure_ready_edge_property_index(&engine, label, "region", SecondaryIndexKind::Equality);
    ensure_ready_edge_property_index(&engine, label, "score", SecondaryIndexKind::Range);
    let (_nodes, edge_ids) = seed_streamed_edge_union_fixture(&engine, label, 5_000);
    engine.flush().unwrap();
    let expected = edge_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, edge_id)| {
            (ordinal % 3 == 0 || ordinal.is_multiple_of(2)).then_some(*edge_id)
        })
        .collect::<Vec<_>>();
    let query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ])),
        page: PageRequest {
            limit: Some(12),
            after: None,
        },
        ..Default::default()
    };
    let view = engine.published_state().view.clone();
    let normalized = view.normalize_edge_query(&query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    let (mut prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&normalized, &normalized, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(prepared.prepared_mode(), PreparedEdgeSourceMode::StreamedUnion);
    assert_eq!(prepared.retained_id_units(), 0);
    let mut actual = Vec::new();
    let mut pulls = 0usize;
    loop {
        let pull = prepared.pull(7).unwrap();
        pulls += 1;
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert!(pulls >= 3);
    assert_eq!(actual, expected);
    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.branch_plans, 1);
    assert_eq!(snapshot.preparations, 1);
    assert_eq!(snapshot.streamed_cursor_builds, 2);
    assert_eq!(snapshot.buffered_sorts, 0);
    assert_eq!(snapshot.fallback_selections, 0);
    assert!(snapshot.verification_endpoint_cache_units_peak
        <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP + 2 * QUERY_VERIFY_CHUNK);

    let intersect_query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ])),
        page: PageRequest {
            limit: Some(12),
            after: None,
        },
        ..Default::default()
    };
    let intersect_expected = edge_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, edge_id)| {
            (ordinal % 3 == 0 && ordinal.is_multiple_of(2)).then_some(*edge_id)
        })
        .collect::<Vec<_>>();
    let intersect = view.normalize_edge_query(&intersect_query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    let (mut prepared_intersect, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&intersect, &intersect, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        prepared_intersect.prepared_mode(),
        PreparedEdgeSourceMode::StreamedIntersect
    );
    let mut actual = Vec::new();
    loop {
        let pull = prepared_intersect.pull(5).unwrap();
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert_eq!(actual, intersect_expected);
    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.branch_plans, 1);
    assert_eq!(snapshot.preparations, 1);
    assert_eq!(snapshot.streamed_cursor_builds, 2);

    let single_query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
        page: PageRequest {
            limit: Some(12),
            after: None,
        },
        ..Default::default()
    };
    let single_expected = edge_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, edge_id)| (ordinal % 3 == 0).then_some(*edge_id))
        .collect::<Vec<_>>();
    let single = view.normalize_edge_query(&single_query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    note_prepared_edge_branch_plan();
    let mut single_plan = view.plan_normalized_edge_query(&single).unwrap();
    single_plan.execution_mode = EdgeDriverExecutionMode::Streamed;
    single_plan.driver = EdgePhysicalPlan::Source(
        prepared_edge_metadata_pull_find_source(
            &single_plan.driver,
            EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
        )
        .expect("single query should contain the equality source"),
    );
    let (mut prepared_single, followups) = view
        .prepare_edge_metadata_pull_source(&single, &single, single_plan, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        prepared_single.prepared_mode(),
        PreparedEdgeSourceMode::StreamedSingle
    );
    let mut actual = Vec::new();
    loop {
        let pull = prepared_single.pull(3).unwrap();
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert_eq!(actual, single_expected);
    assert_eq!(prepared_edge_source_test_snapshot().streamed_cursor_builds, 1);

    let buffered_query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
                upper: Some(PropertyRangeBound::Excluded(PropValue::Int(2_500))),
            },
        ])),
        page: PageRequest {
            limit: Some(12),
            after: None,
        },
        ..Default::default()
    };
    let buffered_expected = edge_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, edge_id)| {
            (ordinal < 2_500 && ordinal % 3 == 0).then_some(*edge_id)
        })
        .collect::<Vec<_>>();
    let buffered = view.normalize_edge_query(&buffered_query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    note_prepared_edge_branch_plan();
    let mut buffered_plan = view.plan_normalized_edge_query(&buffered).unwrap();
    buffered_plan.execution_mode = EdgeDriverExecutionMode::Streamed;
    let equality_source = prepared_edge_metadata_pull_find_source(
        &buffered_plan.driver,
        EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
    )
    .expect("buffered query should contain the equality source");
    let range_source = prepared_edge_metadata_pull_find_source(
        &buffered_plan.driver,
        EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex,
    )
    .expect("buffered query should contain the range source");
    buffered_plan.driver = EdgePhysicalPlan::Intersect {
        inputs: vec![
            EdgePhysicalPlan::Source(equality_source),
            EdgePhysicalPlan::BufferedIdSort(Box::new(EdgePhysicalPlan::Source(range_source))),
        ],
        mode: EdgeSetOpExecutionMode::Streamed,
    };
    let (mut prepared_buffered, followups) = view
        .prepare_edge_metadata_pull_source(&buffered, &buffered, buffered_plan, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        prepared_buffered.prepared_mode(),
        PreparedEdgeSourceMode::StreamedIntersect
    );
    let mut actual = Vec::new();
    loop {
        let pull = prepared_buffered.pull(6).unwrap();
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert_eq!(actual, buffered_expected);
    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.buffered_sorts, 1);
    assert_eq!(snapshot.streamed_cursor_builds, 2);
    assert!(snapshot.retained_id_units_peak <= PREPARED_EDGE_RETAINED_ID_CAP);
}

#[test]
fn prepared_edge_metadata_pull_eager_union_materializes_once() {
    let (_dir, engine) = query_test_engine();
    let label = "PREPARED_EAGER_UNION";
    ensure_ready_edge_property_index(&engine, label, "status", SecondaryIndexKind::Equality);
    ensure_ready_edge_property_index(&engine, label, "region", SecondaryIndexKind::Equality);
    let (_nodes, edge_ids) = seed_streamed_edge_union_fixture(&engine, label, 3_000);
    engine.flush().unwrap();
    let expected = edge_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, edge_id)| {
            (ordinal % 3 == 0 || ordinal.is_multiple_of(2)).then_some(*edge_id)
        })
        .collect::<Vec<_>>();
    let query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ])),
        page: PageRequest {
            limit: Some(12),
            after: None,
        },
        ..Default::default()
    };
    let view = engine.published_state().view.clone();
    let normalized = view.normalize_edge_query(&query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    let (mut prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&normalized, &normalized, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(prepared.prepared_mode(), PreparedEdgeSourceMode::RetainedIds);
    assert_eq!(prepared.retained_id_units(), expected.len());
    let mut actual = Vec::new();
    loop {
        let pull = prepared.pull(7).unwrap();
        actual.extend(pull.metadata.into_iter().map(|meta| meta.id));
        if !pull.has_more {
            break;
        }
    }
    assert_eq!(actual, expected);
    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.branch_plans, 1);
    assert_eq!(snapshot.preparations, 1);
    assert_eq!(snapshot.eager_materializations, 3);
    assert_eq!(snapshot.streamed_cursor_builds, 0);
    assert_eq!(snapshot.fallback_selections, 0);
    assert!(snapshot.retained_id_units_peak <= PREPARED_EDGE_RETAINED_ID_CAP);
}

#[test]
fn prepared_edge_metadata_pull_endpoint_cache_is_persistent_and_bounded_for_200k_candidates() {
    const CANDIDATE_COUNT: usize = 200_000;
    const FIRST_DISTINCT_ENDPOINT_ID: u64 = 1_000_000;

    let (_dir, engine) = query_test_engine();
    let label = "PREPARED_ENDPOINT_CACHE_BOUND";
    let label_id = engine.ensure_edge_label(label).unwrap();
    let matching_from = insert_query_node(
        &engine,
        "Person",
        "prepared-endpoint-cache-match-from",
        &[],
        1.0,
    );
    let matching_to = insert_query_node(
        &engine,
        "Person",
        "prepared-endpoint-cache-match-to",
        &[],
        1.0,
    );
    let first_edge_id = engine.next_edge_id().unwrap();
    let matching_edge_id = first_edge_id + CANDIDATE_COUNT as u64 - 1;

    // Build the large fixture directly in the test memtable so this regression does
    // not perform 200,000 individual public WAL writes. Every candidate uses two
    // endpoint IDs that are distinct across the entire fixture. The final edge uses
    // the only live endpoint pair and is the only residual-filter match, forcing one
    // pull to scan the complete candidate universe through real ReadView semantics.
    engine
        .with_core_mut(|core| {
            let mut write_seq = core.engine_seq;
            for ordinal in 0..CANDIDATE_COUNT {
                let edge_id = first_edge_id + ordinal as u64;
                let is_match = ordinal + 1 == CANDIDATE_COUNT;
                let (from, to) = if is_match {
                    (matching_from, matching_to)
                } else {
                    let from = FIRST_DISTINCT_ENDPOINT_ID + ordinal as u64 * 2;
                    (from, from + 1)
                };
                let mut props = BTreeMap::new();
                if is_match {
                    props.insert("needle".to_string(), PropValue::Bool(true));
                }
                write_seq += 1;
                core.active_memtable().apply_op(
                    &WalOp::UpsertEdge(EdgeRecord {
                        id: edge_id,
                        from,
                        to,
                        label_id,
                        props,
                        created_at: 1,
                        updated_at: 1,
                        weight: 1.0,
                        valid_from: 0,
                        valid_to: i64::MAX,
                        last_write_seq: 0,
                    }),
                    write_seq,
                );
            }
            core.engine_seq = write_seq;
            core.next_edge_id = matching_edge_id + 1;
            core.update_next_edge_id_seen();
            Ok(())
        })
        .unwrap();

    let query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "needle".to_string(),
            value: PropValue::Bool(true),
        }),
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };
    let view = engine.published_state().view.clone();
    let normalized = view.normalize_edge_query(&query).unwrap();
    reset_prepared_edge_source_test_snapshot();
    let (mut prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&normalized, &normalized, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(prepared.prepared_mode(), PreparedEdgeSourceMode::NativeCursor);

    let pull = prepared.pull(1).unwrap();
    assert_eq!(
        pull.metadata.iter().map(|meta| meta.id).collect::<Vec<_>>(),
        vec![matching_edge_id]
    );
    assert!(!pull.has_more);
    assert!(pull.followups.is_empty());

    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.branch_plans, 1);
    assert_eq!(snapshot.preparations, 1);
    assert_eq!(snapshot.native_cursor_builds, 1);
    assert_eq!(
        snapshot.verification_endpoint_cache_units_peak,
        PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP + 2 * QUERY_VERIFY_CHUNK
    );
    assert_eq!(
        snapshot.verification_endpoint_cache_retained_peak,
        PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP
    );
    assert_eq!(
        snapshot.verification_endpoint_cache_transient_peak,
        2 * QUERY_VERIFY_CHUNK
    );
    assert_eq!(
        snapshot.verification_endpoint_visibility_resolutions,
        2 * CANDIDATE_COUNT
    );
}

#[test]
fn graph_row_delegated_rows_explain_and_cursor_are_deterministic_after_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let source = insert_graph_row_node(&engine, "DelegatedStable", "stable-source", &[]);
    let mut expected = Vec::new();
    for index in 0..4 {
        let target = insert_graph_row_node(
            &engine,
            "DelegatedStable",
            &format!("stable-target-{index}"),
            &[],
        );
        expected.push(insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_DELEGATED_STABLE",
            &[("status", PropValue::String("hot".to_string()))],
        ));
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_DELEGATED_STABLE",
        )],
    );
    query.at_epoch = Some(i64::MAX / 2);
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    if let GraphPatternPiece::Edge(edge) = &mut query.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    query.page.limit = 2;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let first = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        1
    );
    let repeat = engine.query_graph_rows(&query).unwrap();
    assert_eq!(first.rows, repeat.rows);
    assert_eq!(first.next_cursor, repeat.next_cursor);
    assert_eq!(first.plan, repeat.plan);
    assert_eq!(
        first.plan.as_ref().unwrap().fingerprint,
        repeat.plan.as_ref().unwrap().fingerprint
    );
    let cursor = first
        .next_cursor
        .clone()
        .unwrap_or_else(|| panic!("first page should continue; rows={:?}", first.rows));

    let mut second_query = query.clone();
    second_query.page.cursor = Some(cursor.clone());
    let second = engine.query_graph_rows(&second_query).unwrap();
    let mut paged = graph_row_single_u64_column(first.clone());
    paged.extend(graph_row_single_u64_column(second));
    assert_eq!(paged, expected);

    for page_limit in [1, 2, 7, 100] {
        let mut page_query = query.clone();
        page_query.page.limit = page_limit;
        page_query.page.cursor = None;
        let mut walked = Vec::new();
        loop {
            let page = engine.query_graph_rows(&page_query).unwrap();
            let next = page.next_cursor.clone();
            walked.extend(graph_row_single_u64_column(page));
            page_query.page.cursor = next;
            if page_query.page.cursor.is_none() {
                break;
            }
        }
        assert_eq!(walked, expected, "delegated cursor walk at limit {page_limit}");
    }

    engine.flush().unwrap();
    engine.close().unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    reopened.reset_query_execution_counters_for_test();
    let reopened_first = reopened.query_graph_rows(&query).unwrap();
    assert_eq!(reopened_first.rows, first.rows);
    assert_eq!(reopened_first.next_cursor, Some(cursor));
    assert_eq!(
        reopened_first.plan.as_ref().unwrap().fingerprint,
        first.plan.as_ref().unwrap().fingerprint
    );
    assert_eq!(
        reopened
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        0
    );
    let reopened_plan = reopened_first.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(
        reopened_plan,
        "source=AdjacencyPull{edge=alias:edge}",
    );
    assert_graph_row_explain_contains(reopened_plan, "choice=AdjacencyPullChunk");
}

#[test]
fn graph_row_edge_pull_cross_label_totals_succeed_with_bounded_frontiers() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedUnionCap", "union-cap-source", &[]);
    for (label, offset) in [
        ("GRAPH_ROW_DELEGATED_UNION_A", 0usize),
        ("GRAPH_ROW_DELEGATED_UNION_B", 2usize),
    ] {
        for index in 0..2 {
            let target = insert_graph_row_node(
                &engine,
                "DelegatedUnionCap",
                &format!("union-cap-target-{}", offset + index),
                &[],
            );
            insert_graph_row_edge(&engine, source, target, label, &[]);
        }
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec![
                "GRAPH_ROW_DELEGATED_UNION_A".to_string(),
                "GRAPH_ROW_DELEGATED_UNION_B".to_string(),
            ],
            filter: None,
        })],
    );
    query.options.allow_full_scan = false;
    query.options.max_frontier = 3;

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(result.rows.len(), 4);
    assert!(result.stats.frontier_peak <= 3);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_edge_pull_executions, 1);
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 4);
    assert_eq!(counters.graph_row_chunks_executed, 2);
}

#[test]
fn graph_row_edge_pull_endpoint_filter_keeps_candidate_frontier_inflight() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(
        &engine,
        "DelegatedConservativeCap",
        "conservative-source",
        &[],
    );
    for index in 0..3 {
        let target = insert_graph_row_node(
            &engine,
            "DelegatedConservativeCap",
            &format!("conservative-target-{index}"),
            &[(
                "status",
                PropValue::String(if index == 0 { "keep" } else { "drop" }.to_string()),
            )],
        );
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_DELEGATED_CONSERVATIVE_CAP",
            &[],
        );
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_DELEGATED_CONSERVATIVE_CAP",
        )],
    );
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    query.options.allow_full_scan = false;
    query.options.max_frontier = 2;

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.stats.frontier_peak, 2);
    assert_eq!(result.stats.intermediate_bindings_peak, 1);
}

#[test]
fn anchored_edge_source_override_delegated_and_raw_ids_bypass_complete_source_tree() {
    let (_dir, engine) = graph_row_test_engine();
    let source_a = insert_graph_row_node(&engine, "Override", "override-source-a", &[]);
    let target_a = insert_graph_row_node(&engine, "Override", "override-target-a", &[]);
    let source_b = insert_graph_row_node(&engine, "Override", "override-source-b", &[]);
    let target_b = insert_graph_row_node(&engine, "Override", "override-target-b", &[]);
    let edge_a = insert_graph_row_edge(&engine, source_a, target_a, "ANCHORED_OVERRIDE", &[]);
    let edge_b = insert_graph_row_edge(&engine, source_b, target_b, "ANCHORED_OVERRIDE", &[]);
    engine.flush().unwrap();
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "ANCHORED_OVERRIDE",
        )],
    );
    query.options.allow_full_scan = false;
    let mut normalized = normalize_graph_row_query(&query).unwrap();
    normalized
        .edge_id_constraints
        .insert("edge".to_string(), vec![edge_a, edge_b]);
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let edge_slot = runtime.edges[0].edge_slot.unwrap();

    let delegated_metadata = [EdgeMetadataForQuery::from(
        &internal_edge_record(&engine, edge_a).unwrap().unwrap(),
    )];
    let delegated_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 0,
        read: GraphRowAnchoredEdgeSourceRead::DelegatedMetadata(&delegated_metadata),
        consumed: std::cell::Cell::new(false),
    };
    engine.reset_query_execution_counters_for_test();
    let delegated_rows = execute_graph_row_with_anchored_edge_override(
        &view,
        &normalized,
        &runtime,
        &physical,
        &delegated_override,
    )
    .unwrap();
    assert!(delegated_override.consumed.get());
    assert_eq!(delegated_rows.len(), 1);
    assert_eq!(
        delegated_rows[0]
            .edge_id_for_slot_if_bound(edge_slot)
            .unwrap(),
        Some(edge_a)
    );
    let delegated_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(delegated_counters.graph_row_delegated_edge_queries, 0);
    assert_eq!(delegated_counters.edge_verifier_metadata_lookup_calls, 0);

    let raw_slice = [edge_b];
    let raw_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 0,
        read: GraphRowAnchoredEdgeSourceRead::RawIds(&raw_slice),
        consumed: std::cell::Cell::new(false),
    };
    engine.reset_query_execution_counters_for_test();
    let raw_rows = execute_graph_row_with_anchored_edge_override(
        &view,
        &normalized,
        &runtime,
        &physical,
        &raw_override,
    )
    .unwrap();
    assert!(raw_override.consumed.get());
    assert_eq!(raw_rows.len(), 1);
    assert_eq!(
        raw_rows[0].edge_id_for_slot_if_bound(edge_slot).unwrap(),
        Some(edge_b)
    );
    let raw_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(raw_counters.graph_row_delegated_edge_queries, 0);
    assert_eq!(
        graph_row_source_choice_label(GraphRowEdgeCandidateSourceChoice::EdgePullChunk),
        "EdgePullChunk"
    );
}

#[test]
fn anchored_edge_source_override_consumption_error_missing_and_retry_freshness_are_exact() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "OverrideState", "override-state-source", &[]);
    let target = insert_graph_row_node(&engine, "OverrideState", "override-state-target", &[]);
    let edge_id = insert_graph_row_edge(&engine, source, target, "ANCHORED_OVERRIDE_STATE", &[]);
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "ANCHORED_OVERRIDE_STATE",
        )],
    );
    query.options.allow_full_scan = false;
    let mut normalized = normalize_graph_row_query(&query).unwrap();
    normalized.options.max_frontier = 0;
    let view = engine.published_state().view.clone();
    let mut runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let ids = [edge_id];

    let error_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 0,
        read: GraphRowAnchoredEdgeSourceRead::RawIds(&ids),
        consumed: std::cell::Cell::new(false),
    };
    runtime.edges[0].filter = NormalizedEdgeFilter::AlwaysFalse;
    assert_eq!(
        execute_graph_row_with_anchored_edge_override(
            &view,
            &normalized,
            &runtime,
            &physical,
            &error_override,
        )
        .unwrap_err()
        .to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 0"
    );
    assert!(error_override.consumed.get());

    normalized.options.max_frontier = 16;
    runtime.edges[0].filter = NormalizedEdgeFilter::AlwaysTrue;
    let mismatched = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 99,
        read: GraphRowAnchoredEdgeSourceRead::RawIds(&ids),
        consumed: std::cell::Cell::new(false),
    };
    let ordinary_rows = execute_graph_row_with_anchored_edge_override(
        &view,
        &normalized,
        &runtime,
        &physical,
        &mismatched,
    )
    .unwrap();
    assert_eq!(ordinary_rows.len(), 1);
    assert!(!mismatched.consumed.get());
    #[cfg(debug_assertions)]
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        debug_assert!(
            mismatched.consumed.get(),
            "future EdgePull caller requires post-attempt consumption"
        );
    }))
    .is_err());

    for _ in 0..2 {
        let fresh = GraphRowAnchoredEdgeSourceOverride {
            edge_index: 0,
            read: GraphRowAnchoredEdgeSourceRead::RawIds(&ids),
            consumed: std::cell::Cell::new(false),
        };
        let rows = execute_graph_row_with_anchored_edge_override(
            &view,
            &normalized,
            &runtime,
            &physical,
            &fresh,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(fresh.consumed.get());
    }
}

#[test]
fn anchored_edge_source_override_later_edge_optional_and_vlp_collisions_are_contained() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "OverrideContain", "contain-source", &[]);
    let middle = insert_graph_row_node(&engine, "OverrideContain", "contain-middle", &[]);
    let target = insert_graph_row_node(&engine, "OverrideContain", "contain-target", &[]);
    let first = insert_graph_row_edge(&engine, source, middle, "ANCHORED_CONTAIN_FIRST", &[]);
    let second = insert_graph_row_edge(&engine, middle, target, "ANCHORED_CONTAIN_SECOND", &[]);

    let mut chain = graph_query(
        &["source", "middle", "target"],
        vec![
            graph_edge_with_label(Some("first"), "source", "middle", "ANCHORED_CONTAIN_FIRST"),
            graph_edge_with_label(
                Some("second"),
                "middle",
                "target",
                "ANCHORED_CONTAIN_SECOND",
            ),
        ],
    );
    chain.options.allow_full_scan = false;
    let normalized = normalize_graph_row_query(&chain).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let anchor_index = physical.segments[0].edge_order[0];
    assert_eq!(physical.segments[0].edge_order.len(), 2);
    let anchor_id = [first, second][anchor_index];
    let anchor_metadata = [EdgeMetadataForQuery::from(
        &internal_edge_record(&engine, anchor_id).unwrap().unwrap(),
    )];
    let source_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: anchor_index,
        read: GraphRowAnchoredEdgeSourceRead::DelegatedMetadata(&anchor_metadata),
        consumed: std::cell::Cell::new(false),
    };
    let rows = execute_graph_row_with_anchored_edge_override(
        &view,
        &normalized,
        &runtime,
        &physical,
        &source_override,
    )
    .unwrap();
    assert!(source_override.consumed.get());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .edge_id_for_slot_if_bound(runtime.edges[0].edge_slot.unwrap())
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        rows[0]
            .edge_id_for_slot_if_bound(runtime.edges[1].edge_slot.unwrap())
            .unwrap(),
        Some(second)
    );

    let mut optional = graph_query(
        &["source", "middle", "target"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "source",
                "middle",
                "ANCHORED_CONTAIN_FIRST",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "middle",
                    "target",
                    "ANCHORED_CONTAIN_SECOND",
                )],
                None,
            ),
        ],
    );
    optional.options.allow_full_scan = false;
    let optional_normalized = normalize_graph_row_query(&optional).unwrap();
    let optional_runtime = view
        .normalize_graph_row_runtime_plan(&optional_normalized)
        .unwrap();
    let optional_physical = view
        .plan_graph_row_physical(&optional_normalized, &optional_runtime, false, true)
        .unwrap();
    let required_metadata = [EdgeMetadataForQuery::from(
        &internal_edge_record(&engine, first).unwrap().unwrap(),
    )];
    let optional_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 0,
        read: GraphRowAnchoredEdgeSourceRead::DelegatedMetadata(&required_metadata),
        consumed: std::cell::Cell::new(false),
    };
    let optional_rows = execute_graph_row_with_anchored_edge_override(
        &view,
        &optional_normalized,
        &optional_runtime,
        &optional_physical,
        &optional_override,
    )
    .unwrap();
    assert!(optional_override.consumed.get());
    assert_eq!(optional_rows.len(), 1);
    let optional_slot = optional_normalized
        .binding_schema
        .slot_for_alias("optional")
        .unwrap();
    assert_eq!(
        optional_rows[0]
            .edge_id_for_slot_if_bound(optional_slot)
            .unwrap(),
        Some(second)
    );

    let mut vlp = graph_vlp(
        Some("path"),
        Some("vlp_edge"),
        "middle",
        "target",
        1,
        1,
    );
    let GraphPatternPiece::VariableLength(path) = &mut vlp else {
        unreachable!()
    };
    path.label_filter = vec!["ANCHORED_CONTAIN_SECOND".to_string()];
    let mut required_then_vlp = graph_query(
        &["source", "middle", "target"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "source",
                "middle",
                "ANCHORED_CONTAIN_FIRST",
            ),
            vlp,
        ],
    );
    required_then_vlp.options.allow_full_scan = false;
    let vlp_normalized = normalize_graph_row_query(&required_then_vlp).unwrap();
    let vlp_runtime = view
        .normalize_graph_row_runtime_plan(&vlp_normalized)
        .unwrap();
    let vlp_physical = view
        .plan_graph_row_physical(&vlp_normalized, &vlp_runtime, false, true)
        .unwrap();
    assert!(matches!(
        vlp_runtime.steps.as_slice(),
        [GraphRowRuntimeStep::RequiredSegment(0), GraphRowRuntimeStep::VariableLength(_)]
    ));
    let vlp_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 0,
        read: GraphRowAnchoredEdgeSourceRead::DelegatedMetadata(&required_metadata),
        consumed: std::cell::Cell::new(false),
    };
    let vlp_rows = execute_graph_row_with_anchored_edge_override(
        &view,
        &vlp_normalized,
        &vlp_runtime,
        &vlp_physical,
        &vlp_override,
    )
    .unwrap();
    assert!(vlp_override.consumed.get());
    assert_eq!(vlp_rows.len(), 1);
    let vlp_edge_slot = vlp_normalized
        .binding_schema
        .slot_for_alias("vlp_edge")
        .unwrap();
    assert_eq!(
        vlp_rows[0]
            .edge_id_for_slot_if_bound(vlp_edge_slot)
            .unwrap(),
        Some(second)
    );
}

#[test]
#[cfg(debug_assertions)]
fn anchored_edge_source_override_duplicate_consumption_panics() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "OverrideDup", "override-dup-source", &[]);
    let target = insert_graph_row_node(&engine, "OverrideDup", "override-dup-target", &[]);
    let edge_id = insert_graph_row_edge(&engine, source, target, "ANCHORED_OVERRIDE_DUP", &[]);
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "ANCHORED_OVERRIDE_DUP",
        )],
    );
    query.options.allow_full_scan = false;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let physical = view
        .plan_graph_row_physical(&normalized, &runtime, false, true)
        .unwrap();
    let ids = [edge_id];
    let source_override = GraphRowAnchoredEdgeSourceOverride {
        edge_index: 0,
        read: GraphRowAnchoredEdgeSourceRead::RawIds(&ids),
        consumed: std::cell::Cell::new(false),
    };
    execute_graph_row_with_anchored_edge_override(
        &view,
        &normalized,
        &runtime,
        &physical,
        &source_override,
    )
    .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = execute_graph_row_with_anchored_edge_override(
            &view,
            &normalized,
            &runtime,
            &physical,
            &source_override,
        );
    }))
    .is_err());
}

#[test]
fn graph_row_delegated_mixed_bound_unbound_union_composes_retained_metadata() {
    let (_dir, engine) = graph_row_test_engine();
    let bound = insert_graph_row_node(&engine, "DelegatedMixed", "mixed-bound", &[]);
    let keep_target = insert_graph_row_node(&engine, "DelegatedMixed", "mixed-keep", &[]);
    let drop_target = insert_graph_row_node(&engine, "DelegatedMixed", "mixed-drop", &[]);
    let other_source = insert_graph_row_node(&engine, "DelegatedMixed", "mixed-other", &[]);
    let other_target = insert_graph_row_node(&engine, "DelegatedMixed", "mixed-other-target", &[]);
    let keep = insert_graph_row_edge(
        &engine,
        bound,
        keep_target,
        "GRAPH_ROW_DELEGATED_MIXED_NEXT",
        &[("status", PropValue::String("keep".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        bound,
        drop_target,
        "GRAPH_ROW_DELEGATED_MIXED_NEXT",
        &[("status", PropValue::String("drop".to_string()))],
    );
    let other = insert_graph_row_edge(
        &engine,
        other_source,
        other_target,
        "GRAPH_ROW_DELEGATED_MIXED_NEXT",
        &[("status", PropValue::String("keep".to_string()))],
    );

    let mut query = graph_query(
        &["bound", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "bound".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_DELEGATED_MIXED_NEXT".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("keep".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let edge = &runtime.edges[0];
    let mut bound_row = normalized.binding_schema.empty_row();
    bound_row
        .bind_node(edge.from_slot, GraphBoundNode::id_only(bound))
        .unwrap();
    let unbound_row = normalized.binding_schema.empty_row();

    engine.reset_query_execution_counters_for_test();
    let mut followups = Vec::new();
    let mut frontier_peak = 0;
    let mut oriented_scratch = Vec::new();
    let candidates = view
        .graph_row_fixed_edge_candidates(
            &normalized,
            edge,
            None,
            None,
            &[bound_row, unbound_row],
            i64::MAX / 2,
            None,
            &mut followups,
            &mut frontier_peak,
            None,
            None,
            &mut oriented_scratch,
        )
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        candidates
            .edges
            .iter()
            .map(|candidate| candidate.meta.id)
            .collect::<Vec<_>>(),
        vec![keep, other]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 3);
    assert_eq!(counters.edge_verifier_metadata_lookup_calls, 2);
    assert!(counters.edge_selected_field_ids > 0);
}

#[test]
fn graph_row_mixed_unfiltered_bound_and_delegated_unbound_union_uses_full_verifier() {
    let (_dir, engine) = graph_row_test_engine();
    let bound = insert_graph_row_node(&engine, "DelegatedMixedRaw", "mixed-bound", &[]);
    let bound_target =
        insert_graph_row_node(&engine, "DelegatedMixedRaw", "mixed-bound-target", &[]);
    let other_source =
        insert_graph_row_node(&engine, "DelegatedMixedRaw", "mixed-other-source", &[]);
    let other_target =
        insert_graph_row_node(&engine, "DelegatedMixedRaw", "mixed-other-target", &[]);
    let bound_edge = insert_graph_row_edge(
        &engine,
        bound,
        bound_target,
        "GRAPH_ROW_DELEGATED_MIXED_RAW",
        &[],
    );
    let deleted_target =
        insert_graph_row_node(&engine, "DelegatedMixedRaw", "mixed-deleted-target", &[]);
    insert_graph_row_edge(
        &engine,
        bound,
        deleted_target,
        "GRAPH_ROW_DELEGATED_MIXED_RAW",
        &[],
    );
    engine.delete_node(deleted_target).unwrap();
    let other_edge = insert_graph_row_edge(
        &engine,
        other_source,
        other_target,
        "GRAPH_ROW_DELEGATED_MIXED_RAW",
        &[],
    );

    let query = graph_query(
        &["bound", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "bound",
            "target",
            "GRAPH_ROW_DELEGATED_MIXED_RAW",
        )],
    );
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_state().view.clone();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let edge = &runtime.edges[0];
    let mut bound_row = normalized.binding_schema.empty_row();
    bound_row
        .bind_node(edge.from_slot, GraphBoundNode::id_only(bound))
        .unwrap();
    let unbound_row = normalized.binding_schema.empty_row();

    engine.reset_query_execution_counters_for_test();
    let mut followups = Vec::new();
    let mut frontier_peak = 0;
    let mut oriented_scratch = Vec::new();
    let candidates = view
        .graph_row_fixed_edge_candidates(
            &normalized,
            edge,
            None,
            None,
            &[bound_row, unbound_row],
            i64::MAX / 2,
            None,
            &mut followups,
            &mut frontier_peak,
            None,
            None,
            &mut oriented_scratch,
        )
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        candidates
            .edges
            .iter()
            .map(|candidate| candidate.meta.id)
            .collect::<Vec<_>>(),
        vec![bound_edge, other_edge]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 2);
}

#[test]
fn graph_row_delegated_warning_truthfulness_stays_in_sorted_explain_detail() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "DelegatedWarnings", "warning-source", &[]);
    let target = insert_graph_row_node(&engine, "DelegatedWarnings", "warning-target", &[]);
    let expected = insert_graph_row_edge(
        &engine,
        source,
        target,
        "GRAPH_ROW_DELEGATED_WARNINGS",
        &[("status", PropValue::String("keep".to_string()))],
    );
    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_DELEGATED_WARNINGS".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("keep".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![expected]);
    assert!(result.stats.warnings.is_empty());
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "prepared_source=NativeCursor");
    assert_graph_row_explain_contains(explain, "warnings=[\"MissingReadyIndex\", \"EdgePropertyPostFilter\", \"VerifyOnlyFilter\"]");
    assert_graph_row_explain_contains(explain, "materialized_source=EdgePullDelegatedMetadata");
}

#[test]
fn edge_pull_routing_optional_and_vlp_first_matrix_stays_ineligible() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "RouteMatrix", "route-source", &[]);
    let target = insert_graph_row_node(&engine, "RouteMatrix", "route-target", &[]);
    let prefix = insert_graph_row_node(&engine, "RouteMatrix", "route-prefix", &[]);
    let optional_from = insert_graph_row_node(&engine, "RouteMatrix", "route-optional-from", &[]);
    let optional_to = insert_graph_row_node(&engine, "RouteMatrix", "route-optional-to", &[]);
    insert_graph_row_edge(&engine, source, target, "ROUTE_REQUIRED", &[]);
    insert_graph_row_edge(&engine, prefix, source, "ROUTE_VLP", &[]);
    insert_graph_row_edge(&engine, optional_from, optional_to, "ROUTE_OPTIONAL", &[]);
    let view = engine.published_state().view.clone();

    let mut optional_first = graph_query(
        &["optional_from", "optional_to", "source", "target"],
        vec![
            graph_optional(
                vec![graph_edge_with_label(
                    None,
                    "optional_from",
                    "optional_to",
                    "ROUTE_OPTIONAL",
                )],
                None,
            ),
            graph_edge_with_label(Some("edge"), "source", "target", "ROUTE_REQUIRED"),
        ],
    );
    optional_first.options.allow_full_scan = false;
    let optional_normalized = normalize_graph_row_query(&optional_first).unwrap();
    let optional_runtime = view
        .normalize_graph_row_runtime_plan(&optional_normalized)
        .unwrap();
    let optional_physical = view
        .plan_graph_row_physical(&optional_normalized, &optional_runtime, false, true)
        .unwrap();
    assert!(matches!(
        optional_runtime.steps.first(),
        Some(GraphRowRuntimeStep::Optional(_))
    ));
    assert!(matches!(
        optional_physical.initial_driver,
        GraphRowInitialDriver::Edge { .. }
    ));
    assert!(!graph_row_edge_pull_route_is_eligible(
        &optional_normalized,
        &optional_runtime,
        &optional_physical,
    ));

    for shared_anchor in [false, true] {
        let (vlp_from, vlp_to) = if shared_anchor {
            ("prefix", "source")
        } else {
            ("prefix", "vlp_target")
        };
        let mut vlp = graph_vlp(None, None, vlp_from, vlp_to, 1, 1);
        let GraphPatternPiece::VariableLength(path) = &mut vlp else {
            unreachable!()
        };
        path.label_filter = vec!["ROUTE_VLP".to_string()];
        let nodes = if shared_anchor {
            vec!["prefix", "source", "target"]
        } else {
            vec!["prefix", "vlp_target", "source", "target"]
        };
        let mut query = graph_query(
            &nodes,
            vec![
                vlp,
                graph_edge_with_label(Some("edge"), "source", "target", "ROUTE_REQUIRED"),
            ],
        );
        query.options.allow_full_scan = false;
        let normalized = normalize_graph_row_query(&query).unwrap();
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let physical = view
            .plan_graph_row_physical(&normalized, &runtime, false, true)
            .unwrap();
        assert!(matches!(
            runtime.steps.first(),
            Some(GraphRowRuntimeStep::VariableLength(_))
        ));
        assert!(matches!(
            physical.initial_driver,
            GraphRowInitialDriver::Edge { .. }
        ));
        assert!(!graph_row_edge_pull_route_is_eligible(
            &normalized,
            &runtime,
            &physical,
        ));
    }
}

#[test]
fn graph_row_fo033_native_residual_succeeds_with_inflight_caps() {
    let (dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    let mut targets = Vec::new();
    let selected = [1usize, 7, 13, 19];
    for index in 0..20 {
        let source = insert_graph_row_node(
            &engine,
            "Fo033Source",
            &format!("fo033-source-{index}"),
            &[],
        );
        sources.push(source);
        let target = insert_graph_row_node(
            &engine,
            "Fo033Target",
            &format!("fo033-target-{index}"),
            &[],
        );
        targets.push(target);
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "FO033_REQUIRED",
            &[
                (
                    "role",
                    PropValue::String(
                        if selected.contains(&index) {
                            "selected"
                        } else {
                            "other"
                        }
                        .to_string(),
                    ),
                ),
                ("score", PropValue::Int(index as i64)),
            ],
        );
        if matches!(index, 7 | 19) {
            let doc = insert_graph_row_node(
                &engine,
                "Fo033Document",
                &format!("fo033-document-{index}"),
                &[],
            );
            insert_graph_row_edge(&engine, target, doc, "FO033_OPTIONAL", &[]);
        }
    }

    engine.reset_query_execution_counters_for_test();
    let gql = engine
        .execute_gql(
            "MATCH (source)-[edge:FO033_REQUIRED]->(target) \
             WHERE edge.role = 'selected' \
             OPTIONAL MATCH (target)-[opt:FO033_OPTIONAL]->(doc) \
             RETURN id(target) AS target_id \
             ORDER BY edge.score DESC, id(target) LIMIT 3",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                max_intermediate_bindings: 8,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        gql.rows
            .iter()
            .map(|row| match row.values.as_slice() {
                [GqlValue::UInt(id)] => *id,
                other => panic!("expected one target id, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![targets[19], targets[13], targets[7]]
    );
    let gql_read = gql.plan.as_ref().unwrap().read.as_ref().unwrap();
    assert!(gql_read
        .pushed_down
        .iter()
        .any(|item| item.contains("edge.role")));
    assert!(gql_read.projection.iter().any(|item| {
        item.contains("GraphRowSourceRead") && item.contains("EdgePullDelegatedMetadata")
    }));

    let mut native = graph_query(
        &["source", "target", "doc"],
        vec![
            graph_edge_with_label(Some("edge"), "source", "target", "FO033_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("opt"),
                    "target",
                    "doc",
                    "FO033_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    native.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("edge", "role")),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::String("selected".to_string())),
    });
    native.order_by = vec![
        GraphOrderItem {
            expr: graph_prop("edge", "score"),
            direction: GraphOrderDirection::Desc,
        },
        GraphOrderItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Id,
                args: vec![GraphExpr::Binding("target".to_string())],
            },
            direction: GraphOrderDirection::Asc,
        },
    ];
    native.page.limit = 3;
    native.return_items = Some(vec![graph_return_binding(
        "target",
        GraphReturnProjection::IdOnly,
    )]);
    native.options.include_plan = true;
    native.options.max_intermediate_bindings = 8;
    let fo033_normalized = normalize_graph_row_query(&native).unwrap();
    let fo033_view = engine.published_state().view.clone();
    let fo033_runtime = fo033_view
        .normalize_graph_row_runtime_plan(&fo033_normalized)
        .unwrap();
    let fo033_physical = fo033_view
        .plan_graph_row_physical(&fo033_normalized, &fo033_runtime, false, true)
        .unwrap();
    assert!(graph_row_edge_pull_route_is_eligible(
        &fo033_normalized,
        &fo033_runtime,
        &fo033_physical,
    ));
    engine.reset_query_execution_counters_for_test();
    let edge_pull = engine.query_graph_rows(&native).unwrap();
    assert_eq!(
        graph_row_single_u64_column(edge_pull.clone()),
        vec![targets[19], targets[13], targets[7]]
    );
    let edge_pull_counters = engine.query_execution_counter_snapshot_for_test();
    let edge_pull_plan = format!("{:?}", edge_pull.plan.unwrap());
    assert!(
        edge_pull_plan.contains("source=EdgePull{edge=alias:edge}"),
        "{edge_pull_plan}"
    );
    assert!(
        edge_pull_plan.contains("eligibility=edge_id_order_not_logical_prefix"),
        "{edge_pull_plan}"
    );
    assert!(
        edge_pull_plan.contains(
            "source_pulls=3; successful_leaves=3; scheduled_sizes=8..8"
        ),
        "the cap-8 FO-033 closure must use bounded leaves: {edge_pull_plan}"
    );
    assert_eq!(
        edge_pull_counters.graph_row_edge_pull_executions, 1,
        "the unconstrained native FO-033 query must execute through EdgePull"
    );
    assert!(edge_pull_counters.graph_row_result_cache_units_peak > 0);
    assert_eq!(edge_pull_counters.graph_row_optional_group_cache_hits, 0);
    assert_eq!(edge_pull_counters.graph_row_vlp_cross_chunk_cache_hits, 0);
    assert_eq!(edge_pull_counters.graph_row_chunk_early_exits, 0);
    assert_eq!(edge_pull_counters.graph_row_cursor_anchor_seeks, 0);

    let mut single_edge_pull_query = native.clone();
    single_edge_pull_query.options.max_intermediate_bindings = 64;
    engine.reset_query_execution_counters_for_test();
    let single_edge_pull = engine.query_graph_rows(&single_edge_pull_query).unwrap();
    assert_eq!(
        graph_row_single_u64_column(single_edge_pull.clone()),
        vec![targets[19], targets[13], targets[7]]
    );
    let single_edge_pull_counters = engine.query_execution_counter_snapshot_for_test();
    let single_edge_pull_plan = format!("{:?}", single_edge_pull.plan.unwrap());
    assert!(
        single_edge_pull_plan.contains(
            "successful_leaves=1; scheduled_sizes=64..64; leaf_size_min=20; \
             leaf_size_max=20; source_rows=20; produced_rows=20"
        ),
        "{single_edge_pull_plan}"
    );
    assert!(
        single_edge_pull_plan.contains(
            "result_cache_units=0/64; cache_no_admit=0; optional_cache_hits=0; \
             vlp_cross_chunk_cache_hits=0"
        ),
        "a terminal single EdgePull leaf must use Bypass: {single_edge_pull_plan}"
    );
    assert_eq!(single_edge_pull_counters.graph_row_result_cache_units_peak, 0);
    assert_eq!(single_edge_pull_counters.graph_row_result_cache_no_admit, 0);

    native.nodes[0].ids = sources;
    native.options.max_intermediate_bindings = 64;
    engine.reset_query_execution_counters_for_test();
    let single_leaf = engine.query_graph_rows(&native).unwrap();
    let single_leaf_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(
        graph_row_single_u64_column(single_leaf.clone()),
        vec![targets[19], targets[13], targets[7]]
    );
    let single_leaf_plan = format!("{:?}", single_leaf.plan.unwrap());
    assert!(
        single_leaf_plan.contains("source=AnchorPull{alias=source}"),
        "{single_leaf_plan}"
    );
    assert!(
        single_leaf_plan.contains(
            "successful_leaves=1; scheduled_sizes=64..64; leaf_size_min=20; \
             leaf_size_max=20; source_rows=20; produced_rows=20"
        ),
        "{single_leaf_plan}"
    );
    assert!(
        single_leaf_plan.contains(
            "result_cache_units=0/64; cache_no_admit=0; optional_cache_hits=0; \
             vlp_cross_chunk_cache_hits=0"
        ),
        "a terminal single AnchorPull leaf must bypass dead execution-wide admission: \
         {single_leaf_plan}"
    );
    assert_eq!(single_leaf_counters.graph_row_result_cache_units_peak, 0);
    assert_eq!(single_leaf_counters.graph_row_result_cache_no_admit, 0);

    native.options.max_intermediate_bindings = 8;
    // Phase 43's approved in-flight-cap semantics intentionally remove the old
    // cross-row-total failure: every source slice stays within the cap.
    engine.reset_query_execution_counters_for_test();
    let native_result = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&native).unwrap()
    });
    let anchor_pull_counters = engine.query_execution_counter_snapshot_for_test();
    assert!(
        anchor_pull_counters.graph_row_result_cache_units_peak > 0,
        "AnchorPull must retain execution-wide result-cache admission"
    );
    native.at_epoch = Some(native_result.stats.effective_at_epoch);
    for size in [1, 2, 256, usize::MAX] {
        let native_result = with_graph_row_test_chunk_override(size, || {
            engine.query_graph_rows(&native).unwrap()
        });
        assert_eq!(
            graph_row_single_u64_column(native_result),
            vec![targets[19], targets[13], targets[7]],
            "forced size {size}"
        );
    }
    assert_eq!(
        gql.rows
            .iter()
            .map(|row| match row.values.as_slice() {
                [GqlValue::UInt(id)] => *id,
                other => panic!("expected one target id, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![targets[19], targets[13], targets[7]]
    );
    let mut equivalence = native.clone();
    equivalence.options.max_intermediate_bindings = 64;
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &equivalence,
        &[1, 2, 256, usize::MAX],
    );
    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_assert_forced_equivalence(
        &reopened,
        &equivalence,
        &[1, 2, 256, usize::MAX],
    );
    for size in [1, 2, 256, usize::MAX] {
        let result = with_graph_row_test_chunk_override(size, || {
            reopened.query_graph_rows(&native).unwrap()
        });
        assert_eq!(
            graph_row_single_u64_column(result),
            vec![targets[19], targets[13], targets[7]],
            "reopen forced size {size}"
        );
    }
}

#[test]
fn graph_row_max_frontier_caps_bound_endpoint_candidates() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Person", "frontier-cap-source", &[]);
    for index in 0..3 {
        let target = insert_graph_row_node(
            &engine,
            "Person",
            &format!("frontier-cap-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_FRONTIER_CAP", &[]);
    }

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("r"),
            "a",
            "b",
            "GRAPH_ROW_FRONTIER_CAP",
        )],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    query.options.max_frontier = 2;
    query.options.max_intermediate_bindings = 100;

    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("max_frontier") && message.contains('2'),
        "expected max_frontier cap error with value 2, got {message:?}"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_delegated_edge_queries,
        0,
        "unfiltered bound rule-4 traversal must not delegate"
    );
}

#[test]
fn graph_row_max_order_materialization_caps_rows_before_sorting() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..3 {
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_ORDER_CAP",
            &format!("order-cap-{index}"),
            &[],
        );
    }

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_ORDER_CAP");
    query.page.limit = 3;
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.options.max_order_materialization = 2;
    query.options.max_intermediate_bindings = 100;

    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("max_order_materialization") && message.contains('2'),
        "expected max_order_materialization cap error with value 2, got {message:?}"
    );
}

#[test]
fn graph_row_default_order_uses_bounded_page_materialization() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..3 {
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_ORDER_CAP_BOUNDED",
            &format!("order-cap-bounded-{index}"),
            &[],
        );
    }

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_ORDER_CAP_BOUNDED");
    query.page.limit = 1;
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.options.max_order_materialization = 2;
    query.options.max_intermediate_bindings = 1;

    let result = engine.query_graph_rows(&query).unwrap();

    assert_eq!(result.rows.len(), 1);
    assert!(result.next_cursor.is_some());
}

#[test]
fn graph_row_explicit_order_caps_filtered_rows_before_order_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let mut ids = Vec::new();
    for index in 0..3 {
        ids.push(insert_graph_row_node(
            &engine,
            "GRAPH_ROW_EXPLICIT_ORDER_CAP",
            &format!("explicit-order-cap-{index}"),
            &[("rank", PropValue::Int(index))],
        ));
    }

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_EXPLICIT_ORDER_CAP");
    query.page.limit = 1;
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    query.options.max_order_materialization = 2;
    query.options.max_intermediate_bindings = 100;

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![ids[0]]);
    assert!(result.next_cursor.is_some());
}

#[test]
fn graph_row_order_cap_rejects_before_unbounded_residual_field_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let mut ids = Vec::new();
    for index in 0..3 {
        ids.push(insert_graph_row_node(
            &engine,
            "GRAPH_ROW_RESIDUAL_ORDER_CAP",
            &format!("residual-order-cap-{index}"),
            &[
                ("status", PropValue::String("active".to_string())),
                ("rank", PropValue::Int(index)),
            ],
        ));
    }

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_RESIDUAL_ORDER_CAP");
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("n", "status")),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::String("active".to_string())),
    });
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.page.limit = 1;
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    query.options.max_order_materialization = 2;
    query.options.max_intermediate_bindings = 100;

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![ids[0]]);
    assert!(result.next_cursor.is_some());
    assert!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_selected_field_ids
            >= 3
    );
}

#[test]
fn graph_row_metadata_residual_reads_only_the_early_exit_selection_leaf() {
    let (_dir, engine) = graph_row_test_engine();
    let mut ids = Vec::new();
    for index in 0..3 {
        ids.push(insert_graph_row_node(
            &engine,
            "GRAPH_ROW_METADATA_RESIDUAL_ORDER_CAP",
            &format!("metadata-residual-order-cap-{index}"),
            &[],
        ));
    }

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_METADATA_RESIDUAL_ORDER_CAP");
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Weight,
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Float(0.0)),
    });
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.page.limit = 1;
    query.options.max_order_materialization = 2;
    query.options.max_intermediate_bindings = 100;

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![ids[0]]);
    assert!(result.next_cursor.is_some());
    // CP43.4's proof-bearing source reads exactly limit + continuation proof here;
    // the third anchor is suffix work and must not contribute selected-field reads.
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_selected_field_ids,
        2
    );
}

#[test]
fn graph_row_default_logical_order_is_stable() {
    let (_dir, engine) = graph_row_test_engine();
    let first = insert_graph_row_node(&engine, "GRAPH_ROW_STABLE", "stable-1", &[]);
    let second = insert_graph_row_node(&engine, "GRAPH_ROW_STABLE", "stable-2", &[]);
    let third = insert_graph_row_node(&engine, "GRAPH_ROW_STABLE", "stable-3", &[]);

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_STABLE");
    query.nodes[0].ids = vec![third, first, second];
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);

    let first_run = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    let second_run = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());

    assert_eq!(first_run, vec![first, second, third]);
    assert_eq!(second_run, first_run);
}

#[test]
fn graph_row_explicit_property_order_and_nulls_are_deterministic() {
    let (_dir, engine) = graph_row_test_engine();
    let rank_two = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_PROP_ORDER",
        "rank-2",
        &[("rank", PropValue::Int(2))],
    );
    let rank_null = insert_graph_row_node(&engine, "GRAPH_ROW_PROP_ORDER", "rank-null", &[]);
    let rank_one = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_PROP_ORDER",
        "rank-1",
        &[("rank", PropValue::Int(1))],
    );
    let rank_three = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_PROP_ORDER",
        "rank-3",
        &[("rank", PropValue::Int(3))],
    );

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_PROP_ORDER");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Asc,
    }];

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![rank_one, rank_two, rank_three, rank_null]
    );

    query.order_by[0].direction = GraphOrderDirection::Desc;
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![rank_three, rank_two, rank_one, rank_null]
    );
}

#[test]
fn graph_row_identity_bool_string_bytes_and_numeric_order_atoms() {
    let (_dir, engine) = graph_row_test_engine();
    let n1 = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_SCALAR_ORDER",
        "scalar-1",
        &[
            ("flag", PropValue::Bool(true)),
            ("name", PropValue::String("b".to_string())),
            ("raw", PropValue::Bytes(vec![2])),
            ("num", PropValue::UInt(2)),
        ],
    );
    let n2 = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_SCALAR_ORDER",
        "scalar-2",
        &[
            ("flag", PropValue::Bool(false)),
            ("name", PropValue::String("a".to_string())),
            ("raw", PropValue::Bytes(vec![1])),
            ("num", PropValue::Int(-1)),
        ],
    );
    let n3 = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_SCALAR_ORDER",
        "scalar-3",
        &[
            ("flag", PropValue::Bool(true)),
            ("name", PropValue::String("c".to_string())),
            ("raw", PropValue::Bytes(vec![3])),
            ("num", PropValue::Float(1.5)),
        ],
    );

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_SCALAR_ORDER");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);

    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Binding("n".to_string()),
        direction: GraphOrderDirection::Desc,
    }];
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![n3, n2, n1]
    );

    for (key, expected) in [
        ("flag", vec![n2, n1, n3]),
        ("name", vec![n2, n1, n3]),
        ("raw", vec![n2, n1, n3]),
        ("num", vec![n2, n3, n1]),
    ] {
        query.order_by = vec![GraphOrderItem {
            expr: graph_prop("n", key),
            direction: GraphOrderDirection::Asc,
        }];
        assert_eq!(
            graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
            expected,
            "unexpected order for {key}"
        );
    }
}

#[test]
fn graph_row_mixed_order_atom_classes_sort_by_total_atom_order() {
    let (_dir, engine) = graph_row_test_engine();
    let bool_id = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_MIXED_ATOM_ORDER",
        "mixed-bool",
        &[("mixed", PropValue::Bool(false))],
    );
    let number_id = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_MIXED_ATOM_ORDER",
        "mixed-number",
        &[("mixed", PropValue::Int(1))],
    );
    let string_id = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_MIXED_ATOM_ORDER",
        "mixed-string",
        &[("mixed", PropValue::String("a".to_string()))],
    );
    let bytes_id = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_MIXED_ATOM_ORDER",
        "mixed-bytes",
        &[("mixed", PropValue::Bytes(vec![1]))],
    );
    let null_id = insert_graph_row_node(&engine, "GRAPH_ROW_MIXED_ATOM_ORDER", "mixed-null", &[]);

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_MIXED_ATOM_ORDER");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "mixed"),
        direction: GraphOrderDirection::Asc,
    }];

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![bool_id, number_id, string_id, bytes_id, null_id]
    );
}

#[test]
fn graph_row_order_rejects_nonfinite_and_unorderable_values() {
    let (_dir, engine) = graph_row_test_engine();
    insert_graph_row_node(&engine, "GRAPH_ROW_BAD_ORDER", "bad-order", &[]);

    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_BAD_ORDER");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Float(f64::NAN),
        direction: GraphOrderDirection::Asc,
    }];
    let err = engine.query_graph_rows(&query).unwrap_err();
    assert!(err.to_string().contains("non-finite"));

    query.order_by[0].expr = GraphExpr::List(vec![GraphExpr::Int(1)]);
    let err = engine.query_graph_rows(&query).unwrap_err();
    assert!(err.to_string().contains("list or map"));

    query.order_by[0].expr = GraphExpr::Map(BTreeMap::from([("a".to_string(), GraphExpr::Int(1))]));
    let err = engine.query_graph_rows(&query).unwrap_err();
    assert!(err.to_string().contains("list or map"));
}

#[test]
fn graph_row_cursor_pages_concatenate_and_validate_replay_fields() {
    let (_dir, engine) = graph_row_test_engine();
    let mut ids = Vec::new();
    for index in 0..5 {
        ids.push(insert_graph_row_node(
            &engine,
            "GRAPH_ROW_CURSOR",
            &format!("cursor-{index}"),
            &[("rank", PropValue::Int(index))],
        ));
    }

    let mut oracle = graph_query(&["n"], Vec::new());
    oracle.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_CURSOR");
    oracle.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    oracle.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    oracle.page.limit = 10;
    let expected = graph_row_single_u64_column(engine.query_graph_rows(&oracle).unwrap());
    assert_eq!(expected, ids);

    let mut first_page = oracle.clone();
    first_page.page.skip = 1;
    first_page.page.limit = 2;
    first_page.options.include_plan = true;
    let page1 = engine.query_graph_rows(&first_page).unwrap();
    let page1_plan_fingerprint = page1
        .plan
        .as_ref()
        .expect("include_plan should attach graph-row explain")
        .fingerprint
        .clone();
    let cursor = page1.next_cursor.clone().expect("expected continuation");
    let decoded_cursor_len = decoded_cursor_payload_len(&cursor);
    assert!(
        cursor.len() > decoded_cursor_len,
        "encoded cursor should include prefix/base64 overhead"
    );
    assert_eq!(graph_row_single_u64_column(page1), vec![ids[1], ids[2]]);

    let mut page2 = oracle.clone();
    page2.page.cursor = Some(cursor.clone());
    page2.page.limit = 1;
    page2.options.include_plan = true;
    page2.options.max_cursor_bytes = decoded_cursor_len;
    let page2_result = engine.query_graph_rows(&page2).unwrap();
    assert_eq!(
        page2_result
            .plan
            .as_ref()
            .expect("include_plan should attach graph-row explain")
            .fingerprint
            .as_str(),
        page1_plan_fingerprint.as_str()
    );
    assert_eq!(graph_row_single_u64_column(page2_result), vec![ids[3]]);
    let cursor_explain = engine.explain_graph_rows(&page2).unwrap();
    assert_eq!(
        cursor_explain.effective_at_epoch,
        Some(first_epoch_from_cursor(&cursor))
    );
    assert_eq!(cursor_explain.fingerprint, page1_plan_fingerprint);

    let mut compact_replay = page2.clone();
    compact_replay.output.compact_rows = true;
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&compact_replay).unwrap()),
        vec![ids[3]]
    );

    let mut replay_skip = page2.clone();
    replay_skip.page.skip = 1;
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&replay_skip).unwrap()),
        vec![ids[3]]
    );
    replay_skip.options.include_plan = true;
    let replay_skip_plan = engine
        .query_graph_rows(&replay_skip)
        .unwrap()
        .plan
        .expect("include_plan should attach graph-row explain");
    assert_eq!(replay_skip_plan.fingerprint, page1_plan_fingerprint);

    let mut bad_skip = page2.clone();
    bad_skip.page.skip = 2;
    let err = engine.query_graph_rows(&bad_skip).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
}

#[test]
fn graph_row_cursor_fingerprint_epoch_and_payload_errors_are_invalid_cursor() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..3 {
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_CURSOR_MISMATCH",
            &format!("cursor-mismatch-{index}"),
            &[("rank", PropValue::Int(index))],
        );
    }
    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_CURSOR_MISMATCH");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("n", "rank")),
        op: GraphBinaryOp::Ge,
        right: Box::new(GraphExpr::Param("min".to_string())),
    });
    query.params.insert("min".to_string(), GraphParamValue::Int(0));
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    query.page.limit = 1;
    let first = engine.query_graph_rows(&query).unwrap();
    let cursor = first.next_cursor.expect("expected cursor");

    let mut changed_query = query.clone();
    changed_query.page.cursor = Some(cursor.clone());
    changed_query.nodes[0].label_filter = Some(NodeLabelFilter {
        labels: vec!["OTHER".to_string()],
        mode: LabelMatchMode::All,
    });
    assert!(matches!(
        engine.query_graph_rows(&changed_query).unwrap_err(),
        EngineError::InvalidCursor { .. }
    ));

    let mut changed_order = query.clone();
    changed_order.page.cursor = Some(cursor.clone());
    changed_order.order_by[0].direction = GraphOrderDirection::Desc;
    let err = engine.query_graph_rows(&changed_order).unwrap_err();
    assert!(err.to_string().contains("order fingerprint"));

    let mut changed_output = query.clone();
    changed_output.page.cursor = Some(cursor.clone());
    changed_output.return_items = Some(vec![graph_return_expr(graph_prop("n", "rank"), "rank")]);
    let err = engine.query_graph_rows(&changed_output).unwrap_err();
    assert!(err.to_string().contains("output fingerprint"));

    let mut changed_params = query.clone();
    changed_params.page.cursor = Some(cursor.clone());
    changed_params.params.insert("min".to_string(), GraphParamValue::Int(1));
    let err = engine.query_graph_rows(&changed_params).unwrap_err();
    assert!(err.to_string().contains("params fingerprint"));

    let mut explicit_epoch = query.clone();
    explicit_epoch.page.cursor = Some(cursor.clone());
    explicit_epoch.at_epoch = Some(first_epoch_from_cursor(&cursor).saturating_add(1));
    let err = engine.query_graph_rows(&explicit_epoch).unwrap_err();
    assert!(err.to_string().contains("at_epoch"));

    let mut wrong_sort_atom = query.clone();
    wrong_sort_atom.page.cursor = Some(tampered_cursor_sort_key_atom(
        &cursor,
        GraphSortAtom::Node(1),
    ));
    let err = engine.query_graph_rows(&wrong_sort_atom).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
    assert!(err.to_string().contains("order key atom"));

    let mut null_logical_key = query.clone();
    null_logical_key.page.cursor = Some(tampered_cursor_logical_key_atom(
        &cursor,
        0,
        GraphSortAtom::Null,
    ));
    let err = engine.query_graph_rows(&null_logical_key).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
    assert!(err.to_string().contains("logical row key atom"));

    let malformed_cursors = vec![
        "bad_prefix".to_string(),
        "ogr32c1_*".to_string(),
        tampered_cursor_checksum(&cursor),
        tampered_cursor_version(&cursor, 8, 2),
        tampered_cursor_version(&cursor, 9, 2),
        tampered_cursor_sort_atom_len(&cursor, u32::MAX),
    ];
    for bad in malformed_cursors {
        let mut malformed = query.clone();
        malformed.page.cursor = Some(bad);
        assert!(matches!(
            engine.query_graph_rows(&malformed).unwrap_err(),
            EngineError::InvalidCursor { .. }
        ));
    }

    let mut invalid_query_with_bad_cursor = query.clone();
    invalid_query_with_bad_cursor.page.cursor = Some("bad_prefix".to_string());
    invalid_query_with_bad_cursor.nodes.push(graph_node("n"));
    assert!(matches!(
        engine
            .query_graph_rows(&invalid_query_with_bad_cursor)
            .unwrap_err(),
        EngineError::InvalidCursor { .. }
    ));
}

#[test]
fn graph_row_cursor_pages_reflect_intervening_writes_by_final_row_key() {
    let (_dir, engine) = graph_row_test_engine();

    let first = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_AFTER",
        "after-first",
        &[("rank", PropValue::Int(10))],
    );
    let second = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_AFTER",
        "after-second",
        &[("rank", PropValue::Int(20))],
    );
    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_CURSOR_WRITES_AFTER");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    query.page.limit = 1;

    let first_page = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(first_page.clone()), vec![first]);
    let cursor = first_page.next_cursor.expect("expected continuation");
    let before_cursor = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_AFTER",
        "after-before-cursor",
        &[("rank", PropValue::Int(5))],
    );
    let after_cursor = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_AFTER",
        "after-after-cursor",
        &[("rank", PropValue::Int(30))],
    );

    let mut page2 = query.clone();
    page2.page.cursor = Some(cursor);
    page2.page.limit = 10;
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&page2).unwrap()),
        vec![second, after_cursor]
    );
    assert!(!graph_row_single_u64_column(engine.query_graph_rows(&page2).unwrap())
        .contains(&before_cursor));

    let tomb_first = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_TOMBSTONE",
        "tomb-first",
        &[("rank", PropValue::Int(1))],
    );
    let tomb_deleted = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_TOMBSTONE",
        "tomb-deleted",
        &[("rank", PropValue::Int(2))],
    );
    let tomb_survivor = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_TOMBSTONE",
        "tomb-survivor",
        &[("rank", PropValue::Int(3))],
    );
    let mut tomb_query = query.clone();
    tomb_query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_CURSOR_WRITES_TOMBSTONE");
    let tomb_page1 = engine.query_graph_rows(&tomb_query).unwrap();
    assert_eq!(graph_row_single_u64_column(tomb_page1.clone()), vec![tomb_first]);
    engine.delete_node(tomb_deleted).unwrap();
    tomb_query.page.cursor = tomb_page1.next_cursor;
    tomb_query.page.limit = 10;
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&tomb_query).unwrap()),
        vec![tomb_survivor]
    );

    let move_first = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_MOVE",
        "move-first",
        &[("rank", PropValue::Int(1))],
    );
    let move_second = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_MOVE",
        "move-second",
        &[("rank", PropValue::Int(2))],
    );
    let move_before = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CURSOR_WRITES_MOVE",
        "move-before",
        &[("rank", PropValue::Int(3))],
    );
    let mut move_query = query.clone();
    move_query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_CURSOR_WRITES_MOVE");
    let move_page1 = engine.query_graph_rows(&move_query).unwrap();
    assert_eq!(graph_row_single_u64_column(move_page1.clone()), vec![move_first]);
    assert_eq!(
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_CURSOR_WRITES_MOVE",
            "move-before",
            &[("rank", PropValue::Int(0))],
        ),
        move_before
    );
    move_query.page.cursor = move_page1.next_cursor;
    move_query.page.limit = 10;
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&move_query).unwrap()),
        vec![move_second]
    );
}

#[test]
fn graph_row_frontier_peak_tracks_candidate_pressure_separately() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Person", "frontier-stats-source", &[]);
    let keep_target = insert_graph_row_node(&engine, "Keep", "frontier-stats-keep", &[]);
    let keep_edge = insert_graph_row_edge(
        &engine,
        source,
        keep_target,
        "GRAPH_ROW_FRONTIER_STATS",
        &[],
    );
    for index in 0..3 {
        let target = insert_graph_row_node(
            &engine,
            "Drop",
            &format!("frontier-stats-drop-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_FRONTIER_STATS",
            &[],
        );
    }

    let mut query = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("r"),
            "a",
            "b",
            "GRAPH_ROW_FRONTIER_STATS",
        )],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("b", "Keep");
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);
    query.options.include_plan = true;

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![keep_edge]);
    assert_eq!(result.stats.intermediate_bindings_peak, 1);
    assert_eq!(result.stats.frontier_peak, 4);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "cap pressure");
    assert_graph_row_explain_contains(explain, "frontier_peak=4");
    assert_graph_row_explain_contains(explain, "max_frontier");
}

#[test]
fn graph_row_stale_edge_property_index_candidates_are_verified_away() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let index_id;
    let segment_id;
    let red_one;
    let red_two;
    let blue;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let nodes = (0..4)
            .map(|idx| {
                insert_graph_row_node(&engine, "Person", &format!("graph-row-stale-{idx}"), &[])
            })
            .collect::<Vec<_>>();
        red_one = insert_graph_row_edge(
            &engine,
            nodes[0],
            nodes[1],
            "GRAPH_ROW_STALE_EDGE",
            &[("color", PropValue::String("red".to_string()))],
        );
        red_two = insert_graph_row_edge(
            &engine,
            nodes[0],
            nodes[2],
            "GRAPH_ROW_STALE_EDGE",
            &[("color", PropValue::String("red".to_string()))],
        );
        blue = insert_graph_row_edge(
            &engine,
            nodes[0],
            nodes[3],
            "GRAPH_ROW_STALE_EDGE",
            &[("color", PropValue::String("blue".to_string()))],
        );
        engine.flush().unwrap();
        let index = engine
            .ensure_edge_property_index("GRAPH_ROW_STALE_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);
        index_id = index.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::edge_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    replace_equality_sidecar_group_id_in_place(
        &sidecar_path,
        hash_prop_equality_key(&PropValue::String("red".to_string())),
        red_two,
        blue,
    );

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_STALE_EDGE".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "color".to_string(),
                value: PropValue::String("red".to_string()),
            }),
        })],
    );
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);

    let explain = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(&explain, "stale index candidates");
    assert_graph_row_explain_contains(&explain, "candidate_universe_not_equivalent");

    assert_eq!(
        graph_row_single_u64_column(reopened.query_graph_rows(&query).unwrap()),
        vec![red_one]
    );
}

#[test]
fn graph_row_stale_node_property_index_candidates_are_verified_away() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let index_id;
    let segment_id;
    let red_one;
    let red_two;
    let blue;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        red_one = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_STALE_NODE",
            "stale-node-red-one",
            &[("color", PropValue::String("red".to_string()))],
        );
        red_two = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_STALE_NODE",
            "stale-node-red-two",
            &[("color", PropValue::String("red".to_string()))],
        );
        blue = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_STALE_NODE",
            "stale-node-blue",
            &[("color", PropValue::String("blue".to_string()))],
        );
        engine.flush().unwrap();
        let index = engine
            .ensure_node_property_index("GRAPH_ROW_STALE_NODE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);
        index_id = index.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    replace_equality_sidecar_group_id_in_place(
        &sidecar_path,
        hash_prop_equality_key(&PropValue::String("red".to_string())),
        red_two,
        blue,
    );

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_STALE_NODE");
    query.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "color".to_string(),
        value: PropValue::String("red".to_string()),
    });
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);
    query.options.allow_full_scan = false;

    let explain = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "PropertyEqualityIndex");
    assert_graph_row_explain_contains(&explain, "stale index candidates");

    assert_eq!(
        graph_row_single_u64_column(reopened.query_graph_rows(&query).unwrap()),
        vec![red_one]
    );
}

#[test]
fn graph_row_partial_unknown_edge_labels_surface_warning() {
    let (_dir, engine) = graph_row_test_engine();
    let from = insert_graph_row_node(&engine, "Person", "warn-from", &[]);
    let to = insert_graph_row_node(&engine, "Person", "warn-to", &[]);
    let edge = insert_graph_row_edge(&engine, from, to, "GRAPH_ROW_WARN_EDGE", &[]);
    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec![
                "GRAPH_ROW_WARN_EDGE".to_string(),
                "GRAPH_ROW_MISSING_EDGE".to_string(),
            ],
            filter: None,
        })],
    );
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![edge]);
    assert!(result
        .stats
        .warnings
        .iter()
        .any(|warning| warning.contains("UnknownEdgeLabel")));
}

#[test]
fn graph_row_page_limit_hydrates_only_final_output_rows() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..5 {
        insert_graph_row_node(
            &engine,
            "Person",
            &format!("page-{index}"),
            &[("name", PropValue::String(format!("name-{index}")))],
        );
    }
    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "Person");
    query.page.limit = 1;
    query.output.mode = GraphOutputMode::Projected;
    query.return_items = Some(vec![graph_return_binding(
        "n",
        GraphReturnProjection::Selected(GraphSelectedProjection::Node(
            GraphSelectedNodeProjection {
                id: true,
                labels: false,
                key: false,
                props: GraphPropertySelection::Keys(vec!["name".to_string()]),
                weight: false,
                created_at: false,
                updated_at: false,
                vectors: GraphVectorSelection::None,
            },
        )),
    )]);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(result.rows.len(), 1);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
}

#[test]
fn graph_row_execution_deferred_features_return_structured_errors() {
    let (_dir, engine) = graph_row_test_engine();

    let mut cursor = graph_query(&["a"], Vec::new());
    cursor.page.cursor = Some("deferred".to_string());
    let err = engine.query_graph_rows(&cursor).unwrap_err();
    assert!(matches!(err, EngineError::InvalidCursor { .. }));
}

#[test]
fn graph_row_explain_reports_current_fixed_execution_shape() {
    let mut query = graph_query(&["a", "b"], vec![graph_edge(Some("e"), "a", "b")]);
    let temp = TempDir::new().unwrap();
    let engine = DatabaseEngine::open(temp.path(), &DbOptions::default()).unwrap();

    let explain = engine.explain_graph_rows(&query).unwrap();

    assert_eq!(explain.columns, vec!["a", "b", "e"]);
    assert_eq!(explain.fingerprint.len(), 32);
    assert!(!explain.summaries.validation_only);
    assert_eq!(explain.plan[0].kind, "GraphRowPhysicalPlan");
    assert!(!explain
        .plan
        .iter()
        .any(|node| node.kind.contains("Pattern") || node.detail.contains("GraphPatternQuery")));
    assert_graph_row_explain_contains(&explain, "EdgeCandidateSource");
    assert_graph_row_explain_contains(&explain, "FallbackFullEdgeScan");
    assert_graph_row_explain_contains(&explain, "EdgeVerification");
    assert_graph_row_explain_contains(&explain, "EndpointNodeVerification");
    assert_graph_row_explain_contains(&explain, "ProjectionNeeds");
    assert_graph_row_explain_contains(&explain, "FinalHydrationProjection");
    assert_graph_row_explain_contains(&explain, "ResidualFilter");
    assert_graph_row_explain_contains(&explain, "Order");
    assert_graph_row_explain_contains(&explain, "CursorSeek");
    assert_graph_row_explain_contains(&explain, "SkipLimit");
    assert_graph_row_explain_contains(&explain, "max_frontier");
    assert_graph_row_explain_contains(&explain, "source correctness");
    assert_graph_row_explain_contains(&explain, "GraphRowPlanAlternative");
    assert_graph_row_explain_contains(&explain, "fanout-aware physical source choice");
    assert!(explain.effective_at_epoch.is_some());

    query.options.include_plan = true;
    let result = engine.query_graph_rows(&query).unwrap();
    let execution_plan = result.plan.unwrap();
    assert_eq!(
        execution_plan.effective_at_epoch,
        Some(result.stats.effective_at_epoch)
    );
    assert_graph_row_explain_contains(&execution_plan, "cap pressure");
    assert_graph_row_explain_contains(&execution_plan, "rows_returned=0");
}

#[test]
fn graph_row_explain_reports_node_candidate_source_used() {
    let (_dir, engine) = graph_row_test_engine();
    insert_graph_row_node(&engine, "GRAPH_ROW_EXPLAIN_NODE", "node-plan-a", &[]);
    let mut query = graph_query(&["n"], Vec::new());
    query.nodes[0] = graph_node_with_label("n", "GRAPH_ROW_EXPLAIN_NODE");
    query.return_items = Some(vec![graph_return_binding("n", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();

    assert_eq!(explain.plan[0].kind, "GraphRowPhysicalPlan");
    assert_graph_row_explain_contains(&explain, "NodeCandidateSource");
    assert_graph_row_explain_contains(&explain, "alias=n");
    assert_graph_row_explain_contains(&explain, "NodeLabelIndex");
    assert_graph_row_explain_contains(&explain, "node-only default-order fast path candidate source");
    assert_graph_row_explain_contains(&explain, "NodeVerification");
}

#[test]
fn graph_row_explain_reports_adjacency_expansion_and_endpoint_verification() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "ExplainSource", "adj-source", &[]);
    let keep = insert_graph_row_node(
        &engine,
        "ExplainTarget",
        "adj-keep",
        &[("state", PropValue::String("ok".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        source,
        keep,
        "GRAPH_ROW_EXPLAIN_ADJ",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_ADJ".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("target", "ExplainTarget");
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "state".to_string(),
        value: PropValue::String("ok".to_string()),
    });
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();

    assert_graph_row_explain_contains(&explain, "NodeCandidateSource");
    assert_graph_row_explain_contains(&explain, "source=EndpointAdjacency");
    assert_graph_row_explain_contains(&explain, "direction=Outgoing");
    assert_graph_row_explain_contains(&explain, "filter_verification=edge_property_projection");
    assert_graph_row_explain_contains(&explain, "EndpointNodeVerification");
    assert_graph_row_explain_contains(&explain, "selected verifier fields");
    assert_graph_row_explain_contains(&explain, "need_class=verifier");
    assert_graph_row_explain_contains(&explain, "node_aliases=[\"target\"]");
}

#[test]
fn graph_row_explain_reports_unbound_edge_candidate_source_and_fallback_notes() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "ExplainUnbound", "unbound-source", &[]);
    let target = insert_graph_row_node(&engine, "ExplainUnbound", "unbound-target", &[]);
    insert_graph_row_edge(
        &engine,
        source,
        target,
        "GRAPH_ROW_EXPLAIN_UNBOUND",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("r".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_UNBOUND".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding("r", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();

    assert_graph_row_explain_contains(&explain, "unbound required edge candidate source");
    assert_graph_row_explain_contains(&explain, "EdgeLabelIndex");
    assert_graph_row_explain_contains(&explain, "MissingReadyIndex");
    assert_graph_row_explain_contains(&explain, "EdgePropertyPostFilter");
    assert_graph_row_explain_contains(&explain, "VerifyOnlyFilter");
    assert_graph_row_explain_contains(&explain, "stale index candidates");
}

#[test]
fn graph_row_explain_reports_optional_and_vlp_runtime_plan_nodes() {
    let (_dir, engine) = graph_row_test_engine();
    let optional = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Optional(GraphOptionalGroup {
            pieces: vec![graph_edge(Some("r"), "a", "b")],
            where_: None,
        })],
    );
    let optional_explain = engine.explain_graph_rows(&optional).unwrap();
    assert_graph_row_explain_contains(&optional_explain, "OptionalApply");
    assert_graph_row_explain_contains(&optional_explain, "left_outer=true");
    assert_graph_row_explain_contains(&optional_explain, "barrier=true");

    let vlp = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    let vlp_explain = engine.explain_graph_rows(&vlp).unwrap();
    assert_graph_row_explain_contains(&vlp_explain, "VariableLengthPath");
    assert_graph_row_explain_contains(&vlp_explain, "relationship_simple=true");
    assert_graph_row_explain_contains(&vlp_explain, "source_verification=latest_visible_edges");
}

#[test]
fn graph_row_explain_reports_edge_property_post_filters_and_warnings() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "ExplainPostFilterNode", "post-a", &[]);
    let b = insert_graph_row_node(&engine, "ExplainPostFilterNode", "post-b", &[]);
    insert_graph_row_edge(
        &engine,
        a,
        b,
        "GRAPH_ROW_EXPLAIN_POST_FILTER",
        &[
            ("rel", PropValue::String("friend".to_string())),
            ("score", PropValue::Int(5)),
        ],
    );
    engine.flush().unwrap();

    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("e".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_POST_FILTER".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "rel".to_string(),
                    value: PropValue::String("friend".to_string()),
                },
                EdgeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(3))),
                    upper: None,
                },
            ])),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding("e", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();

    assert_graph_row_explain_contains(&explain, "EdgeCandidateSource");
    assert_graph_row_explain_contains(&explain, "EdgeLabelIndex");
    assert_graph_row_explain_contains(&explain, "EdgePropertyPostFilter");
    assert_graph_row_explain_contains(&explain, "VerifyOnlyFilter");
    assert_graph_row_explain_contains(&explain, "edge_property_projection");
    assert_graph_row_explain_contains(&explain, "stale index candidates/hash collisions");
}

#[test]
fn graph_row_explain_reports_both_direction_self_loop_and_repeated_node_projection() {
    let (_dir, engine) = graph_row_test_engine();
    let keep = insert_graph_row_node(
        &engine,
        "ExplainLoopNode",
        "loop-keep",
        &[("state", PropValue::String("keep".to_string()))],
    );
    let drop = insert_graph_row_node(
        &engine,
        "ExplainLoopNode",
        "loop-drop",
        &[("state", PropValue::String("drop".to_string()))],
    );
    let keep_edge = insert_graph_row_edge(
        &engine,
        keep,
        keep,
        "GRAPH_ROW_EXPLAIN_LOOP",
        &[("kind", PropValue::String("loop".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        drop,
        drop,
        "GRAPH_ROW_EXPLAIN_LOOP",
        &[("kind", PropValue::String("loop".to_string()))],
    );
    // Extra same-label edges between unrelated nodes keep the edge-label
    // anchor strictly more expensive than the node anchor, so the plan
    // drives from the node and reports the Both-direction adjacency step.
    let outside_a = insert_graph_row_node(&engine, "ExplainLoopOutside", "loop-outside-a", &[]);
    let outside_b = insert_graph_row_node(&engine, "ExplainLoopOutside", "loop-outside-b", &[]);
    for _ in 0..5 {
        insert_graph_row_edge(
            &engine,
            outside_a,
            outside_b,
            "GRAPH_ROW_EXPLAIN_LOOP",
            &[("kind", PropValue::String("loop".to_string()))],
        );
    }

    let mut query = graph_query(
        &["same"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("loop".to_string()),
            from_alias: "same".to_string(),
            to_alias: "same".to_string(),
            direction: Direction::Both,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_LOOP".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "kind".to_string(),
                value: PropValue::String("loop".to_string()),
            }),
        })],
    );
    query.nodes[0] = graph_node_with_label("same", "ExplainLoopNode");
    query.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "state".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    // Pin the intended node-driven explain shape explicitly. The physical
    // edge-bound repair legitimately makes the tiny edge-label source cheaper
    // than a fallback node-label scan, while this test specifically exercises
    // the Both-direction self-loop adjacency explanation.
    query.nodes[0].ids = vec![keep];
    query.return_items = Some(vec![graph_return_binding(
        "loop",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "direction=Both");
    assert_graph_row_explain_contains(&explain, "EndpointNodeVerification");
    assert_graph_row_explain_contains(&explain, "selected verifier fields");
    assert_graph_row_explain_contains(&explain, "node_aliases=[\"same\"]");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
}

#[test]
fn graph_row_unflushed_write_keeps_estimate_driven_edge_order() {
    // Planner review P5: one unflushed edge write downgrades every adjacency
    // fanout to GlobalFallback coverage, and the old pair-state comparator
    // then ignored estimates entirely and expanded edges in query order —
    // hub-first here. Estimates must stay decisive under fallback coverage.
    let (_dir, engine) = graph_row_test_engine();
    let anchor = insert_graph_row_node(&engine, "P5Anchor", "anchor", &[]);
    for index in 0..50 {
        let hub_target =
            insert_graph_row_node(&engine, "P5Hub", &format!("hub-{index}"), &[]);
        insert_graph_row_edge(&engine, anchor, hub_target, "P5_HUB_REL", &[]);
    }
    let tiny_target = insert_graph_row_node(&engine, "P5Tiny", "tiny", &[]);
    insert_graph_row_edge(&engine, anchor, tiny_target, "P5_TINY_REL", &[]);
    engine.flush().unwrap();

    // One unrelated unflushed edge forces fallback fanout coverage.
    let noise_a = insert_graph_row_node(&engine, "P5Noise", "noise-a", &[]);
    let noise_b = insert_graph_row_node(&engine, "P5Noise", "noise-b", &[]);
    insert_graph_row_edge(&engine, noise_a, noise_b, "P5_NOISE_REL", &[]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while engine.published_read_view_for_test().memtable.edge_count() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "published view never reflected the unflushed edge"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    // Hub edge first in query order; the tiny edge must still expand first.
    let mut query = graph_query(
        &["a", "h", "t"],
        vec![
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("hub".to_string()),
                from_alias: "a".to_string(),
                to_alias: "h".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["P5_HUB_REL".to_string()],
                filter: None,
            }),
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("tiny".to_string()),
                from_alias: "a".to_string(),
                to_alias: "t".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["P5_TINY_REL".to_string()],
                filter: None,
            }),
        ],
    );
    query.nodes[0] = graph_node_with_label("a", "P5Anchor");
    query.nodes[1] = graph_node_with_label("h", "P5Hub");
    query.nodes[2] = graph_node_with_label("t", "P5Tiny");

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:tiny\", \"alias:hub\"]",
    );
    assert_eq!(engine.query_graph_rows(&query).unwrap().rows.len(), 10);
}

#[test]
fn graph_row_explain_reports_endpoint_key_verification_without_public_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "ExplainKeySource", "key-source", &[]);
    let keep = insert_graph_row_node(&engine, "ExplainKeyTarget", "key-keep", &[]);
    let drop = insert_graph_row_node(&engine, "ExplainKeyTarget", "key-drop", &[]);
    let keep_edge = insert_graph_row_edge(
        &engine,
        source,
        keep,
        "GRAPH_ROW_EXPLAIN_KEY",
        &[("status", PropValue::String("hot".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        source,
        drop,
        "GRAPH_ROW_EXPLAIN_KEY",
        &[("status", PropValue::String("hot".to_string()))],
    );

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_KEY".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("target", "ExplainKeyTarget");
    query.nodes[1].keys = vec![NodeKeyQuery {
        label: "ExplainKeyTarget".to_string(),
        key: "key-keep".to_string(),
    }];
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "source=EdgeCandidateSource");
    assert_graph_row_explain_contains(&explain, "key constraints are normalized to candidate IDs");
    assert_graph_row_explain_contains(&explain, "without public hydration");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
}

#[test]
fn graph_row_explain_reports_endpoint_source_precedence_tombstone_prune_and_temporal_checks() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "ExplainPrecedenceSource", "precedence-source", &[]);
    let keep = insert_graph_row_node(
        &engine,
        "ExplainPrecedenceTarget",
        "precedence-keep",
        &[("state", PropValue::String("drop".to_string()))],
    );
    let deleted = insert_graph_row_node(
        &engine,
        "ExplainPrecedenceTarget",
        "precedence-deleted",
        &[("state", PropValue::String("keep".to_string()))],
    );
    let hidden = engine
        .upsert_node(
            "ExplainPrecedenceTarget",
            "precedence-hidden",
            UpsertNodeOptions {
                props: graph_row_props(&[("state", PropValue::String("keep".to_string()))]),
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    let keep_edge = insert_graph_row_edge(
        &engine,
        source,
        keep,
        "GRAPH_ROW_EXPLAIN_PRECEDENCE",
        &[("status", PropValue::String("hot".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        source,
        deleted,
        "GRAPH_ROW_EXPLAIN_PRECEDENCE",
        &[("status", PropValue::String("hot".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        source,
        hidden,
        "GRAPH_ROW_EXPLAIN_PRECEDENCE",
        &[("status", PropValue::String("hot".to_string()))],
    );
    engine.flush().unwrap();
    let updated_keep = insert_graph_row_node(
        &engine,
        "ExplainPrecedenceTarget",
        "precedence-keep",
        &[("state", PropValue::String("keep".to_string()))],
    );
    assert_eq!(updated_keep, keep);
    engine.delete_node(deleted).unwrap();
    engine
        .set_prune_policy(
            "graph-row-explain-precedence-prune",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("ExplainPrecedenceTarget".to_string()),
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_PRECEDENCE".to_string()],
            filter: Some(EdgeFilterExpr::ValidAt {
                epoch_ms: i64::MAX / 2,
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1] = graph_node_with_label("target", "ExplainPrecedenceTarget");
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "state".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "source correctness");
    assert_graph_row_explain_contains(&explain, "active memtable wins");
    assert_graph_row_explain_contains(&explain, "newer shadows older records");
    assert_graph_row_explain_contains(&explain, "tombstones hide older records");
    assert_graph_row_explain_contains(&explain, "prune policies apply at read time");
    assert_graph_row_explain_contains(&explain, "temporal validity at effective_at_epoch");
    assert_graph_row_explain_contains(&explain, "metadata_only");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
}

#[test]
fn graph_row_planner_explain_reports_mutable_and_temporal_prune_confidence_downgrades() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CONFIDENCE_SOURCE",
        "confidence-source",
        &[],
    );
    let target = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_CONFIDENCE_TARGET",
        "confidence-target",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        source,
        target,
        "GRAPH_ROW_CONFIDENCE_REL",
        &[],
    );
    engine
        .set_prune_policy(
            "graph-row-confidence-prune",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("GRAPH_ROW_CONFIDENCE_TARGET".to_string()),
            },
        )
        .unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_CONFIDENCE_REL",
        )],
    );
    query.nodes[0].ids = vec![source];
    query.at_epoch = Some(i64::MAX / 2);
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &explain,
        "fanout confidence downgraded because active/immutable memtables are not represented by immutable adjacency rollups",
    );
    assert_graph_row_explain_contains(
        &explain,
        "temporal/prune active state downgrades fanout confidence; final visibility verification remains authoritative",
    );
}

#[test]
fn graph_row_explain_reports_remaining_edge_property_projection_after_metadata_survivors() {
    let (_dir, engine) = graph_row_test_engine();
    let left = insert_graph_row_node(&engine, "ExplainBranchLeft", "branch-left", &[]);
    let mid = insert_graph_row_node(&engine, "ExplainBranchMid", "branch-mid", &[]);
    let keep = insert_graph_row_node(&engine, "ExplainBranchLeaf", "branch-keep", &[]);
    let drop = insert_graph_row_node(&engine, "ExplainBranchLeaf", "branch-drop", &[]);
    insert_graph_row_edge(
        &engine,
        left,
        mid,
        "GRAPH_ROW_EXPLAIN_BRANCH_ANCHOR",
        &[],
    );
    let keep_edge = insert_graph_row_edge(
        &engine,
        mid,
        keep,
        "GRAPH_ROW_EXPLAIN_BRANCH_REMAINING",
        &[("role", PropValue::String("keep".to_string()))],
    );
    insert_graph_row_edge(
        &engine,
        mid,
        drop,
        "GRAPH_ROW_EXPLAIN_BRANCH_REMAINING",
        &[("role", PropValue::String("drop".to_string()))],
    );

    let mut query = graph_query(
        &["left", "mid", "leaf"],
        vec![
            graph_edge_with_label(
                Some("anchor"),
                "left",
                "mid",
                "GRAPH_ROW_EXPLAIN_BRANCH_ANCHOR",
            ),
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("remaining".to_string()),
                from_alias: "mid".to_string(),
                to_alias: "leaf".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["GRAPH_ROW_EXPLAIN_BRANCH_REMAINING".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "role".to_string(),
                    value: PropValue::String("keep".to_string()),
                }),
            }),
        ],
    );
    query.nodes[0].ids = vec![left];
    query.return_items = Some(vec![graph_return_binding(
        "remaining",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "edge=alias:remaining");
    assert_graph_row_explain_contains(&explain, "source=EndpointAdjacency");
    assert_graph_row_explain_contains(&explain, "edge_property_projection");
    assert_graph_row_explain_contains(&explain, "ProjectionNeeds");
    assert_graph_row_explain_contains(&explain, "need_class=verifier");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep_edge]
    );
}

#[test]
fn graph_row_explain_reports_exists_missing_boolean_fallback_sources() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "ExplainExistsNode", "exists-a", &[]);
    let b = insert_graph_row_node(&engine, "ExplainExistsNode", "exists-b", &[]);
    let c = insert_graph_row_node(&engine, "ExplainExistsNode", "exists-c", &[]);
    insert_graph_row_edge(
        &engine,
        a,
        b,
        "GRAPH_ROW_EXPLAIN_EXISTS",
        &[("flag", PropValue::String("yes".to_string()))],
    );
    insert_graph_row_edge(&engine, a, c, "GRAPH_ROW_EXPLAIN_EXISTS", &[]);
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_EXPLAIN_EXISTS", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("flag").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    for filter in [
        EdgeFilterExpr::PropertyExists {
            key: "flag".to_string(),
        },
        EdgeFilterExpr::PropertyMissing {
            key: "flag".to_string(),
        },
        EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "flag".to_string(),
                value: PropValue::String("yes".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "missing_indexed_key".to_string(),
                value: PropValue::String("archived".to_string()),
            },
        ]),
        EdgeFilterExpr::Not(Box::new(EdgeFilterExpr::PropertyEquals {
            key: "flag".to_string(),
            value: PropValue::String("yes".to_string()),
        })),
    ] {
        let mut query = graph_query(
            &["a", "b"],
            vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("e".to_string()),
                from_alias: "a".to_string(),
                to_alias: "b".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["GRAPH_ROW_EXPLAIN_EXISTS".to_string()],
                filter: Some(filter),
            })],
        );
        query.options.allow_full_scan = false;
        query.return_items = Some(vec![graph_return_binding("e", GraphReturnProjection::IdOnly)]);

        let explain = engine.explain_graph_rows(&query).unwrap();
        assert_graph_row_explain_contains(&explain, "EdgeLabelIndex");
        assert_graph_row_explain_contains(&explain, "VerifyOnlyFilter");
        assert_graph_row_explain_contains(&explain, "edge_property_projection");
        assert_graph_row_explain_not_contains(&explain, "source=EdgePropertyEqualityIndex");
    }
}

#[test]
fn graph_row_compound_node_anchor_wins_when_selective() {
    let (_dir, engine) = graph_row_test_engine();
    let target = insert_graph_row_node(&engine, "CompoundGraphTarget", "node-anchor-target", &[]);
    let keep = insert_graph_row_node(
        &engine,
        "CompoundGraphPerson",
        "node-anchor-keep",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
    );
    let other_status = insert_graph_row_node(
        &engine,
        "CompoundGraphPerson",
        "node-anchor-other-status",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
        ],
    );
    let other_tenant = insert_graph_row_node(
        &engine,
        "CompoundGraphPerson",
        "node-anchor-other-tenant",
        &[
            ("tenant", PropValue::String("globex".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
    );
    insert_graph_row_edge(&engine, keep, target, "GRAPH_ROW_COMPOUND_NODE", &[]);
    insert_graph_row_edge(
        &engine,
        other_status,
        target,
        "GRAPH_ROW_COMPOUND_NODE",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        other_tenant,
        target,
        "GRAPH_ROW_COMPOUND_NODE",
        &[],
    );
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index(
            "CompoundGraphPerson",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_COMPOUND_NODE",
        )],
    );
    query.nodes[0] = graph_node_with_label("source", "CompoundGraphPerson");
    query.nodes[0].filter = Some(NodeFilterExpr::And(vec![
        NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("acme".to_string()),
        },
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
    ]));
    query.nodes[1] = graph_node_with_label("target", "CompoundGraphTarget");
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "source",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=source");
    assert_graph_row_explain_contains(&explain, "CompoundEqualityIndex");
    assert_graph_row_explain_contains(&explain, "final_verification: true");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep]
    );
}

#[test]
fn graph_row_compound_multi_label_any_anchor_unions_per_label() {
    let (_dir, engine) = graph_row_test_engine();
    let target = insert_graph_row_node(&engine, "AnyAnchorTarget", "any-anchor-target", &[]);
    let tuple_props = [
        ("tenant", PropValue::String("acme".to_string())),
        ("status", PropValue::String("active".to_string())),
    ];
    let a_match = insert_graph_row_node(&engine, "AnyAnchorA", "any-anchor-a", &tuple_props);
    let b_match = insert_graph_row_node(&engine, "AnyAnchorB", "any-anchor-b", &tuple_props);
    let a_other = insert_graph_row_node(
        &engine,
        "AnyAnchorA",
        "any-anchor-a-other",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
        ],
    );
    for source in [a_match, b_match, a_other] {
        insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_ANY_ANCHOR", &[]);
    }
    // Extra unconnected targets keep the target anchor from being trivially
    // cheapest; memtable-only data keeps compound estimates exact.
    for index in 0..8 {
        insert_graph_row_node(
            &engine,
            "AnyAnchorTarget",
            &format!("any-anchor-target-extra-{index}"),
            &[],
        );
    }
    let spec = || {
        SecondaryIndexSpec::equality(vec![
            SecondaryIndexField::property("tenant"),
            SecondaryIndexField::property("status"),
        ])
    };
    let index_a = engine
        .ensure_node_property_index("AnyAnchorA", spec())
        .unwrap();
    wait_for_property_index_state(&engine, index_a.index_id, SecondaryIndexState::Ready);
    let index_b = engine
        .ensure_node_property_index("AnyAnchorB", spec())
        .unwrap();
    wait_for_property_index_state(&engine, index_b.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_ANY_ANCHOR",
        )],
    );
    query.nodes[0] = GraphNodePattern {
        alias: "source".to_string(),
        label_filter: Some(NodeLabelFilter {
            labels: vec!["AnyAnchorA".to_string(), "AnyAnchorB".to_string()],
            mode: LabelMatchMode::Any,
        }),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ])),
    };
    query.nodes[1] = graph_node_with_label("target", "AnyAnchorTarget");
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "source",
        GraphReturnProjection::IdOnly,
    )]);

    // No dropped rows: both Any labels contribute matches through the
    // compound union anchor (or a correct fallback).
    let mut rows = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    rows.sort_unstable();
    let mut expected = vec![a_match, b_match];
    expected.sort_unstable();
    assert_eq!(rows, expected);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "CompoundEqualityIndex");
}

#[test]
fn graph_row_compound_edge_anchor_wins_when_selective() {
    let (_dir, engine) = graph_row_test_engine();
    let mut expected = None;
    for index in 0..80 {
        let from = insert_graph_row_node(
            &engine,
            "CompoundGraphEdgeFrom",
            &format!("edge-anchor-from-{index}"),
            &[],
        );
        let to = insert_graph_row_node(
            &engine,
            "CompoundGraphEdgeTo",
            &format!("edge-anchor-to-{index}"),
            &[],
        );
        let status = if index == 37 { "hot" } else { "cold" };
        let edge = insert_graph_row_weighted_edge(
            &engine,
            from,
            to,
            "GRAPH_ROW_COMPOUND_EDGE_ANCHOR",
            &[("status", PropValue::String(status.to_string()))],
            1.0,
        );
        if index == 37 {
            expected = Some(edge);
        }
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index(
            "GRAPH_ROW_COMPOUND_EDGE_ANCHOR",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_COMPOUND_EDGE_ANCHOR".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::WeightRange {
                    lower: Some(0.0),
                    upper: Some(2.0),
                },
            ])),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=EdgeAnchor(edge=alias:edge");
    assert_graph_row_explain_contains(&explain, "source=EdgeCandidateSource");
    assert_graph_row_explain_contains(&explain, "CompoundRangeIndex");
    assert_graph_row_explain_contains(&explain, "final_verification: true");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![expected.unwrap()]
    );
}

#[test]
fn graph_row_compound_node_anchor_missing_sidecar_falls_back_to_legal_source() {
    let (_dir, engine) = graph_row_test_engine();
    let target = insert_graph_row_node(&engine, "NodeAnchorMissingTarget", "missing-target", &[]);
    let keep = insert_graph_row_node(
        &engine,
        "NodeAnchorMissingPerson",
        "missing-keep",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
    );
    let other_status = insert_graph_row_node(
        &engine,
        "NodeAnchorMissingPerson",
        "missing-other-status",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
        ],
    );
    insert_graph_row_edge(&engine, keep, target, "GRAPH_ROW_COMPOUND_NODE_MISSING", &[]);
    insert_graph_row_edge(
        &engine,
        other_status,
        target,
        "GRAPH_ROW_COMPOUND_NODE_MISSING",
        &[],
    );
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index(
            "NodeAnchorMissingPerson",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_COMPOUND_NODE_MISSING",
        )],
    );
    query.nodes[0] = graph_node_with_label("source", "NodeAnchorMissingPerson");
    query.nodes[0].filter = Some(NodeFilterExpr::And(vec![
        NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("acme".to_string()),
        },
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
    ]));
    query.nodes[1] = graph_node_with_label("target", "NodeAnchorMissingTarget");
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "source",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=source");
    assert_graph_row_explain_contains(&explain, "CompoundEqualityIndex");
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep]
    );

    // Remove the segment compound sidecar while the declaration is still
    // published Ready: the planned compound node anchor must fall back to a
    // legal non-compound source and keep the row set identical.
    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::node_compound_eq_sidecar_path(
        &crate::segment_writer::segment_dir(engine.path(), segment_id),
        index.index_id,
    );
    std::fs::remove_file(&sidecar_path).unwrap();

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep]
    );
}

fn seed_compound_edge_fallback_graph(
    engine: &DatabaseEngine,
    label: &str,
) -> (u64, u64, EdgePropertyIndexInfo, GraphRowQuery) {
    let mut expected = None;
    let mut expected_from = None;
    let common_from = insert_graph_row_node(
        engine,
        &format!("{label}From"),
        "edge-fallback-from",
        &[],
    );
    for index in 0..80 {
        let to = insert_graph_row_node(
            engine,
            &format!("{label}To"),
            &format!("edge-fallback-to-{index}"),
            &[],
        );
        let status = if index == 37 { "hot" } else { "cold" };
        let edge = insert_graph_row_weighted_edge(
            engine,
            common_from,
            to,
            label,
            &[("status", PropValue::String(status.to_string()))],
            1.0,
        );
        if index == 37 {
            expected = Some(edge);
            expected_from = Some(common_from);
        }
    }
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index(
            label,
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(engine, info.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec![label.to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::WeightRange {
                    lower: Some(0.0),
                    upper: Some(2.0),
                },
            ])),
        })],
    );
    query.options.allow_full_scan = false;
    // Tight frontier cap: the selective compound source fits comfortably, but
    // raw label-scan candidate materialization (80 edges) does not. Fallback
    // must therefore stream verified matches instead of reporting the sidecar
    // failure as a max_frontier violation.
    query.options.max_frontier = 16;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);
    (expected.unwrap(), expected_from.unwrap(), info, query)
}

#[test]
fn graph_row_delegated_compound_missing_sidecar_falls_back_and_preserves_followup() {
    let (_dir, engine) = graph_row_test_engine();
    let (expected, _, info, query) =
        seed_compound_edge_fallback_graph(&engine, "GraphRowCompoundEdgeMissing");

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "source=EdgeCandidateSource");
    assert_graph_row_explain_contains(&explain, "CompoundRangeIndex");
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![expected]
    );

    // Remove the segment compound sidecar while the declaration is still
    // published Ready: the planned compound edge source must fall back to a
    // legal non-compound source instead of failing as too broad.
    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::edge_compound_range_sidecar_path(
        &crate::segment_writer::segment_dir(engine.path(), segment_id),
        info.index_id,
    );
    std::fs::remove_file(&sidecar_path).unwrap();

    let mut actual = None;
    assert_graph_row_single_read_followup_enqueued(&engine, || {
        actual = Some(graph_row_single_u64_column(
            engine.query_graph_rows(&query).unwrap(),
        ));
    });
    assert_eq!(actual.unwrap(), vec![expected]);
}

#[test]
fn graph_row_vlp_delegated_compound_followups_reach_engine_reconciliation() {
    let (_dir, engine) = graph_row_test_engine();
    let label = "GraphRowVlpCompoundMissing";
    let (_expected, expected_from, info, fixed_query) =
        seed_compound_edge_fallback_graph(&engine, label);
    let GraphPatternPiece::Edge(fixed_edge) = &fixed_query.pieces[0] else {
        unreachable!();
    };
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_vlp(Some("path"), None, "source", "target", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec![label.to_string()];
        path.filter = fixed_edge.filter.clone();
    }
    query.nodes[0].ids = vec![expected_from];
    query.options.allow_full_scan = false;
    query.options.max_frontier = 16;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "path",
        GraphReturnProjection::IdOnly,
    )]);

    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::edge_compound_range_sidecar_path(
        &crate::segment_writer::segment_dir(engine.path(), segment_id),
        info.index_id,
    );
    std::fs::remove_file(&sidecar_path).unwrap();

    engine.reset_query_execution_counters_for_test();
    let mut actual = None;
    assert_graph_row_single_read_followup_enqueued(&engine, || {
        let result = engine.query_graph_rows(&query).unwrap();
        let counters = engine.query_execution_counter_snapshot_for_test();
        actual = Some((result, counters));
    });
    let (actual, counters) = actual.unwrap();
    assert_eq!(actual.rows.len(), 1);
    assert_graph_row_explain_contains(actual.plan.as_ref().unwrap(), "planned_modes=");
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 1);
}

#[test]
fn graph_row_delegated_compound_corrupt_sidecar_falls_back_and_preserves_followup() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let (expected, _, info, query) =
        seed_compound_edge_fallback_graph(&engine, "GraphRowCompoundEdgeCorrupt");

    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::edge_compound_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        info.index_id,
    );
    engine.close().unwrap();
    corrupt_compound_sidecar_payload_only_in_place(&sidecar_path);

    // Payload-only corruption passes lightweight reopen validation, so the
    // planner still selects the compound source; execution must fall back.
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_edge_property_index_state(&reopened, info.index_id, SecondaryIndexState::Ready);
    let explain = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "CompoundRangeIndex");
    let mut actual = None;
    assert_graph_row_single_read_followup_enqueued(&reopened, || {
        actual = Some(graph_row_single_u64_column(
            reopened.query_graph_rows(&query).unwrap(),
        ));
    });
    assert_eq!(actual.unwrap(), vec![expected]);
    reopened.close().unwrap();
}

#[test]
fn graph_row_delegated_compound_too_broad_streams_verified_fallback_once() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "CompoundGraphBroadFrom", "broad-from", &[]);
    let b = insert_graph_row_node(&engine, "CompoundGraphBroadTo", "broad-to", &[]);

    // More matching tuples than the planner's per-source candidate cap: the
    // compound prefix scan and the raw label scan both materialize TooBroad
    // with no sidecar-failure followup, while only one edge actually passes
    // final verification.
    let total = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 100;
    let mut inputs = Vec::with_capacity(total);
    for index in 0..total {
        let mut props = BTreeMap::new();
        props.insert(
            "status".to_string(),
            PropValue::String("hot".to_string()),
        );
        if index == 1234 {
            props.insert("marker".to_string(), PropValue::Int(1));
        }
        inputs.push(EdgeInput {
            from: a,
            to: b,
            label: "GRAPH_ROW_COMPOUND_BROAD".to_string(),
            props,
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        });
    }
    let edge_ids = engine.batch_upsert_edges(inputs).unwrap();
    let expected = edge_ids[1234];

    let info = engine
        .ensure_edge_property_index(
            "GRAPH_ROW_COMPOUND_BROAD",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_COMPOUND_BROAD".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::PropertyEquals {
                    key: "marker".to_string(),
                    value: PropValue::Int(1),
                },
            ])),
        })],
    );
    query.options.allow_full_scan = false;
    query.options.max_frontier = 16;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![expected]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 1);
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 0);
}

#[test]
fn graph_row_endpoint_adjacency_beats_broad_compound_edge_source() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "CompoundGraphEndpoint", "endpoint-source", &[]);
    let target = insert_graph_row_node(&engine, "CompoundGraphEndpoint", "endpoint-target", &[]);
    let keep = insert_graph_row_edge(
        &engine,
        source,
        target,
        "GRAPH_ROW_COMPOUND_ADJACENCY",
        &[("status", PropValue::String("hot".to_string()))],
    );
    for index in 0..96 {
        let from = insert_graph_row_node(
            &engine,
            "CompoundGraphEndpointOther",
            &format!("endpoint-other-from-{index}"),
            &[],
        );
        let to = insert_graph_row_node(
            &engine,
            "CompoundGraphEndpointOther",
            &format!("endpoint-other-to-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            from,
            to,
            "GRAPH_ROW_COMPOUND_ADJACENCY",
            &[("status", PropValue::String("hot".to_string()))],
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index(
            "GRAPH_ROW_COMPOUND_ADJACENCY",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_COMPOUND_ADJACENCY".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::WeightRange {
                    lower: Some(0.0),
                    upper: Some(2.0),
                },
            ])),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=source");
    assert_graph_row_explain_contains(&explain, "source=EndpointAdjacency");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![keep]
    );
}

#[test]
fn graph_row_optional_compound_miss_preserves_null_row() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "CompoundGraphOptional", "optional-a", &[]);
    let b = insert_graph_row_node(&engine, "CompoundGraphOptional", "optional-b", &[]);
    let c = insert_graph_row_node(&engine, "CompoundGraphOptional", "optional-c", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_COMPOUND_REQUIRED", &[]);
    insert_graph_row_weighted_edge(
        &engine,
        b,
        c,
        "GRAPH_ROW_COMPOUND_OPTIONAL",
        &[("status", PropValue::String("present".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index(
            "GRAPH_ROW_COMPOUND_OPTIONAL",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut optional_edge = match graph_edge_with_label(
        Some("optional"),
        "b",
        "c",
        "GRAPH_ROW_COMPOUND_OPTIONAL",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    optional_edge.filter = Some(EdgeFilterExpr::And(vec![
        EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("missing".to_string()),
        },
        EdgeFilterExpr::WeightRange {
            lower: Some(0.0),
            upper: Some(2.0),
        },
    ]));
    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("required"), "a", "b", "GRAPH_ROW_COMPOUND_REQUIRED"),
            graph_optional(vec![GraphPatternPiece::Edge(optional_edge)], None),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding(
        "optional",
        GraphReturnProjection::IdOnly,
    )]);

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&query).unwrap()),
        vec![vec![GraphValue::Null]]
    );
}

#[test]
fn graph_row_edge_bound_is_snapshot_safe_across_mutation_and_epoch_sealing() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "GraphRowBoundNode", "bound-a", &[]);
    let b = insert_graph_row_node(&engine, "GraphRowBoundNode", "bound-b", &[]);
    let c = insert_graph_row_node(&engine, "GraphRowBoundNode", "bound-c", &[]);
    let d = insert_graph_row_node(&engine, "GraphRowBoundNode", "bound-d", &[]);
    let segment_edge = insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_BOUND_SEGMENT", &[]);
    engine.flush().unwrap();

    let mut pinned = vec![engine.published_read_view_for_test()];
    let active_edge = insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_BOUND_ACTIVE", &[]);
    let original_active = internal_edge_record(&engine, active_edge).unwrap().unwrap();
    pinned.push(engine.published_read_view_for_test());

    engine.delete_edge(active_edge).unwrap();
    pinned.push(engine.published_read_view_for_test());

    write_internal_wal_op(&engine, &WalOp::UpsertEdge(original_active.clone())).unwrap();
    pinned.push(engine.published_read_view_for_test());

    let rewrite_label_id = engine.ensure_edge_label("GRAPH_ROW_BOUND_REWRITTEN").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(EdgeRecord {
            from: c,
            to: d,
            label_id: rewrite_label_id,
            updated_at: original_active.updated_at.saturating_add(1),
            ..original_active
        }),
    )
    .unwrap();
    pinned.push(engine.published_read_view_for_test());

    engine.freeze_memtable().unwrap();
    pinned.push(engine.published_read_view_for_test());
    let active_after_seal = insert_graph_row_edge(
        &engine,
        d,
        a,
        "GRAPH_ROW_BOUND_POST_SEAL_ACTIVE",
        &[],
    );
    pinned.push(engine.published_read_view_for_test());

    let all_edge_ids = [segment_edge, active_edge, active_after_seal];
    for (index, view) in pinned.iter().enumerate() {
        let visible = view
            .get_edges(&all_edge_ids)
            .unwrap()
            .into_iter()
            .flatten()
            .count() as u64;
        assert!(
            view.graph_row_physical_edge_bound() >= visible,
            "pinned view {index} bound {} must cover {visible} visible edges",
            view.graph_row_physical_edge_bound()
        );
    }

    let current = pinned.last().unwrap();
    let segment_term = current
        .segments
        .iter()
        .fold(0u64, |total, segment| total.saturating_add(segment.edge_count()));
    let active_term = current.memtable.retained_edge_slot_count() as u64;
    let immutable_term = current.immutable_epochs.iter().fold(0u64, |total, epoch| {
        total.saturating_add(epoch.memtable.retained_edge_slot_count() as u64)
    });
    assert!(segment_term > 0);
    assert!(active_term > 0);
    assert!(immutable_term > 0);
    assert_eq!(
        current.graph_row_physical_edge_bound(),
        segment_term
            .saturating_add(active_term)
            .saturating_add(immutable_term)
    );
}

#[test]
fn graph_row_edge_bound_rollups_tail_fallback_multilabel_and_both_are_sound() {
    let (dir, engine) = graph_row_test_engine();
    let nodes = (0..5)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowBoundRollupNode",
                &format!("bound-rollup-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    insert_graph_row_edge(&engine, nodes[0], nodes[1], "GRAPH_ROW_BOUND_A", &[]);
    insert_graph_row_edge(&engine, nodes[0], nodes[2], "GRAPH_ROW_BOUND_A", &[]);
    insert_graph_row_edge(&engine, nodes[1], nodes[3], "GRAPH_ROW_BOUND_B", &[]);
    engine.flush().unwrap();

    let flushed_view = engine.published_read_view_for_test();
    let flushed_bound = flushed_view.graph_row_physical_edge_bound_context();
    let flushed_physical = flushed_bound.physical_edge_bound;
    let absent = flushed_view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Outgoing,
        Some(&[u32::MAX]),
        None,
        flushed_bound,
    );
    assert_eq!(absent.visible_total_edges_bound, Some(flushed_physical));

    insert_graph_row_edge(&engine, nodes[2], nodes[4], "GRAPH_ROW_BOUND_A", &[]);
    let view = engine.published_read_view_for_test();
    let edge_bound = view.graph_row_physical_edge_bound_context();
    let physical = edge_bound.physical_edge_bound;
    assert_eq!(physical, 4);
    let retained = view.memtable.retained_edge_slot_count() as u64;
    assert_eq!(retained, 1);
    let label_a = view
        .label_catalog
        .resolve_edge_label_for_read("GRAPH_ROW_BOUND_A")
        .unwrap()
        .unwrap();
    let label_b = view
        .label_catalog
        .resolve_edge_label_for_read("GRAPH_ROW_BOUND_B")
        .unwrap()
        .unwrap();

    let outgoing_a = view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Outgoing,
        Some(&[label_a]),
        None,
        edge_bound,
    );
    let incoming_a = view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Incoming,
        Some(&[label_a]),
        None,
        edge_bound,
    );
    assert_eq!(outgoing_a.visible_total_edges_bound, Some(3));
    assert_eq!(incoming_a.visible_total_edges_bound, Some(3));
    assert_eq!(
        outgoing_a
            .clone()
            .combine_sum(incoming_a)
            .visible_total_edges_bound,
        Some(6),
        "Direction::Both sums the two per-direction bounds"
    );

    let multi = view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Outgoing,
        Some(&[label_a, label_b]),
        None,
        edge_bound,
    );
    assert_eq!(multi.visible_total_edges_bound, Some(physical));

    let missing = view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Outgoing,
        Some(&[u32::MAX]),
        None,
        edge_bound,
    );
    assert_eq!(missing.visible_total_edges_bound, Some(physical));
    assert_eq!(missing.coverage, GraphRowFanoutCoverage::GlobalFallback);

    let empty = view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Outgoing,
        Some(&[]),
        None,
        edge_bound,
    );
    assert_eq!(empty.visible_total_edges_bound, Some(0));

    engine.flush().unwrap();
    let corrupt_segment_id = engine.segments_for_test()[0].segment_id;
    engine.close().unwrap();
    corrupt_planner_stats_for_segment(&dir.path().join("db"), corrupt_segment_id);
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    let corrupt_view = reopened.published_read_view_for_test();
    let corrupt_bound = corrupt_view.graph_row_physical_edge_bound_context();
    let corrupt_physical = corrupt_bound.physical_edge_bound;
    let corrupt = corrupt_view.graph_row_single_direction_fanout_estimate(
        crate::planner_stats::PlannerStatsDirection::Outgoing,
        Some(&[label_a]),
        None,
        corrupt_bound,
    );
    assert_eq!(
        corrupt.visible_total_edges_bound,
        Some(corrupt_physical),
        "uncovered/corrupt planner stats retain the physical bound"
    );
}

#[test]
fn graph_row_known_id_posting_cap_memo_proof_and_declines_are_sound() {
    let (_dir, engine) = graph_row_test_engine();
    let sources = (0..4)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowKnownPostingSource",
                &format!("known-posting-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..24)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowKnownPostingTarget",
                &format!("known-posting-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for target in &targets[..20] {
        insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_KNOWN_POSTING_REL",
            &[],
        );
    }
    for index in 1..sources.len() {
        insert_graph_row_edge(
            &engine,
            sources[index],
            targets[19 + index],
            "GRAPH_ROW_KNOWN_POSTING_REL",
            &[],
        );
    }
    insert_graph_row_edge(
        &engine,
        sources[0],
        sources[0],
        "GRAPH_ROW_KNOWN_POSTING_REL",
        &[],
    );
    engine.flush().unwrap();
    insert_graph_row_edge(
        &engine,
        sources[0],
        targets[0],
        "GRAPH_ROW_KNOWN_POSTING_OTHER_LABEL",
        &[],
    );

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_KNOWN_POSTING_REL",
        )],
    );
    query.nodes[0].ids = sources.clone();
    query.options.allow_full_scan = false;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let edge = &runtime.edges[0];
    let edge_bound = view.graph_row_physical_edge_bound_context();
    let source_ids = view
        .graph_row_known_node_ids_for_fanout(&runtime.nodes[0])
        .unwrap();
    assert_eq!(source_ids, sources);

    reset_graph_row_planning_probe();
    let mut memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let estimate = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&source_ids),
        &normalized,
        edge,
        edge_bound,
        &mut memo,
    );
    assert_eq!(
        estimate.known_id_posting_bound,
        Some(25),
        "the known-ID endpoint posting upper bound may include the active other-label posting"
    );
    assert_eq!(estimate.hub_risk, GraphRowHubRisk::High);
    assert!(estimate.cost_fanout() > 25);
    let counters = snapshot_graph_row_planning_probe();
    assert_eq!(counters.known_id_posting_consults, 1);
    assert_eq!(counters.known_id_posting_misses, 1);

    reset_graph_row_planning_probe();
    let legacy_estimate = with_graph_row_test_legacy_estimator(|| {
        let mut legacy_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
        view.graph_row_edge_fanout_estimate(
            0,
            true,
            Direction::Outgoing,
            edge.label_filter_ids.as_deref(),
            Some(&source_ids),
            &normalized,
            edge,
            edge_bound,
            &mut legacy_memo,
        )
    });
    assert_eq!(legacy_estimate.known_id_posting_bound, None);
    let legacy_counters = snapshot_graph_row_planning_probe();
    assert_eq!(legacy_counters.known_id_posting_consults, 0);
    assert_eq!(legacy_counters.known_id_posting_misses, 0);
    assert!(!graph_row_test_legacy_estimator_is_enabled());

    reset_graph_row_planning_probe();
    let mut restored_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let restored = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&source_ids),
        &normalized,
        edge,
        edge_bound,
        &mut restored_memo,
    );
    assert_eq!(restored.known_id_posting_bound, Some(25));
    assert_eq!(snapshot_graph_row_planning_probe().known_id_posting_consults, 1);

    let repeated = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&source_ids),
        &normalized,
        edge,
        edge_bound,
        &mut memo,
    );
    assert_eq!(repeated.known_id_posting_bound, Some(25));
    let opposite = view.graph_row_edge_fanout_estimate(
        0,
        false,
        Direction::Incoming,
        edge.label_filter_ids.as_deref(),
        Some(&source_ids),
        &normalized,
        edge,
        edge_bound,
        &mut memo,
    );
    assert_eq!(opposite.known_id_posting_bound, Some(1));
    let counters = snapshot_graph_row_planning_probe();
    assert_eq!(counters.known_id_posting_consults, 3);
    assert_eq!(counters.known_id_posting_misses, 2);

    reset_graph_row_planning_probe();
    let at_cap_ids = (1..=1_024).collect::<Vec<_>>();
    let mut at_cap_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let at_cap = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&at_cap_ids),
        &normalized,
        edge,
        edge_bound,
        &mut at_cap_memo,
    );
    assert!(at_cap.known_id_posting_bound.is_some());
    let counters = snapshot_graph_row_planning_probe();
    assert_eq!(counters.known_id_posting_consults, 1);
    assert_eq!(counters.known_id_posting_misses, 1);

    assert_eq!(
        view.graph_row_known_id_posting_bound(
            &source_ids,
            Direction::Both,
            edge.label_filter_ids.as_deref(),
        ),
        Some(26),
        "Direction::Both sums outgoing and incoming physical postings, including a self-loop twice"
    );

    let node_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| {
                alternative.kind == "NodeAnchor"
                    && alternative.detail.contains("alias=source")
            })
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    let distinct = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    let duplicate_frontier = view
        .plan_graph_row_physical(&normalized, &runtime, true, false)
        .unwrap();
    let distinct_cost = node_cost(&distinct);
    let duplicate_cost = node_cost(&duplicate_frontier);
    assert_eq!(
        distinct_cost.estimated_work,
        distinct_cost.anchor_cost.estimated_work + 25,
        "the distinct first hop clamps independently to the posting upper bound and physical bound"
    );
    assert_eq!(
        duplicate_cost.estimated_work,
        duplicate_cost.anchor_cost.estimated_work + 4 * 25,
        "a duplicate-bearing frontier receives only the per-row cap, never a global clamp or M/ids average"
    );

    reset_graph_row_planning_probe();
    let mut above_cap_ids = source_ids.clone();
    above_cap_ids.extend((0..1_021).map(|offset| u64::MAX - offset));
    above_cap_ids.sort_unstable();
    above_cap_ids.dedup();
    assert_eq!(above_cap_ids.len(), 1_025);
    let mut decline_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    for _ in 0..2 {
        let declined = view.graph_row_edge_fanout_estimate(
            0,
            true,
            Direction::Outgoing,
            edge.label_filter_ids.as_deref(),
            Some(&above_cap_ids),
            &normalized,
            edge,
            edge_bound,
            &mut decline_memo,
        );
        assert_eq!(declined.known_id_posting_bound, None);
        assert_eq!(declined.avg_upper_fanout, estimate.avg_upper_fanout);
        assert_eq!(declined.p99_fanout, estimate.p99_fanout);
        assert_eq!(declined.max_fanout, estimate.max_fanout);
        assert_eq!(declined.visible_total_edges_bound, estimate.visible_total_edges_bound);
        assert_eq!(declined.hub_risk, estimate.hub_risk);
        assert_eq!(declined.confidence, estimate.confidence);
        assert_eq!(declined.coverage, estimate.coverage);
    }
    let counters = snapshot_graph_row_planning_probe();
    assert_eq!(counters.known_id_posting_consults, 2);
    assert_eq!(counters.known_id_posting_misses, 1);

    let mut temporal = normalized.clone();
    temporal.at_epoch = Some(0);
    let mut temporal_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let temporal_accepted = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&source_ids),
        &temporal,
        edge,
        edge_bound,
        &mut temporal_memo,
    );
    let mut temporal_decline_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let temporal_declined = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&above_cap_ids),
        &temporal,
        edge,
        edge_bound,
        &mut temporal_decline_memo,
    );
    assert_eq!(temporal_accepted.confidence, temporal_declined.confidence);
    assert_eq!(temporal_accepted.coverage, temporal_declined.coverage);
    assert_eq!(temporal_accepted.hub_risk, temporal_declined.hub_risk);
    assert_eq!(temporal_accepted.known_id_posting_bound, Some(25));
    assert_eq!(temporal_declined.known_id_posting_bound, None);

    engine
        .set_prune_policy(
            "graph-row-known-posting-prune",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("GraphRowKnownPostingTarget".to_string()),
            },
        )
        .unwrap();
    let prune_view = engine.published_read_view_for_test();
    let prune_edge_bound = prune_view.graph_row_physical_edge_bound_context();
    let mut prune_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let prune_accepted = prune_view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&source_ids),
        &normalized,
        edge,
        prune_edge_bound,
        &mut prune_memo,
    );
    let mut prune_decline_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let prune_declined = prune_view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        Some(&above_cap_ids),
        &normalized,
        edge,
        prune_edge_bound,
        &mut prune_decline_memo,
    );
    assert_eq!(prune_accepted.confidence, prune_declined.confidence);
    assert_eq!(prune_accepted.coverage, prune_declined.coverage);
    assert_eq!(prune_accepted.hub_risk, prune_declined.hub_risk);
    assert_eq!(prune_accepted.known_id_posting_bound, Some(25));
    assert_eq!(prune_declined.known_id_posting_bound, None);
    assert_ne!(
        prune_accepted.confidence, estimate.confidence,
        "an active prune policy must retain its confidence downgrade when the posting cap is accepted"
    );

    reset_graph_row_planning_probe();
    let mut no_probe_memo = GraphRowKnownIdPostingMemo::new(runtime.edges.len());
    let id_free = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        edge.label_filter_ids.as_deref(),
        None,
        &normalized,
        edge,
        edge_bound,
        &mut no_probe_memo,
    );
    assert_eq!(id_free.known_id_posting_bound, None);
    let empty_labels = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Outgoing,
        Some(&[]),
        Some(&source_ids),
        &normalized,
        edge,
        edge_bound,
        &mut no_probe_memo,
    );
    assert_eq!(empty_labels.cost_fanout(), 0);
    assert_eq!(empty_labels.known_id_posting_bound, None);
    let counters = snapshot_graph_row_planning_probe();
    assert_eq!(counters.known_id_posting_consults, 0);
    assert_eq!(counters.known_id_posting_misses, 0);

    engine.delete_node(sources[0]).unwrap();
    let deleted_view = engine.published_read_view_for_test();
    assert_eq!(
        deleted_view.graph_row_known_id_posting_bound(
            &source_ids,
            Direction::Outgoing,
            edge.label_filter_ids.as_deref(),
        ),
        Some(24),
        "the deleted source contributes no active-memtable term while older segment postings remain"
    );
}

#[test]
fn graph_row_known_id_posting_multi_anchor_plan_reuses_orientation_memo() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "GraphRowKnownMemo", "known-memo-a", &[]);
    let b = insert_graph_row_node(&engine, "GraphRowKnownMemo", "known-memo-b", &[]);
    let c = insert_graph_row_node(&engine, "GraphRowKnownMemo", "known-memo-c", &[]);
    insert_graph_row_edge(&engine, a, b, "GRAPH_ROW_KNOWN_MEMO_FIRST", &[]);
    insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_KNOWN_MEMO_SECOND", &[]);
    engine.flush().unwrap();

    let mut query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("first"), "a", "b", "GRAPH_ROW_KNOWN_MEMO_FIRST"),
            graph_edge_with_label(Some("second"), "b", "c", "GRAPH_ROW_KNOWN_MEMO_SECOND"),
        ],
    );
    query.nodes[0].ids = vec![a];
    query.nodes[1].ids = vec![b];
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();

    reset_graph_row_planning_probe();
    let plan = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    assert_eq!(plan.edge_order.len(), 2);
    let counters = snapshot_graph_row_planning_probe();
    assert_eq!(counters.known_id_posting_consults, 7);
    assert_eq!(
        counters.known_id_posting_misses, 3,
        "the second anchor and both edge-anchor simulations must reuse prior edge/orientation computations"
    );
}

#[test]
fn graph_row_edge_bound_unknown_stats_first_node_hop_clamps_but_edge_anchor_does_not() {
    let (_dir, engine) = graph_row_test_engine();
    let sources = (0..8)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowUnknownBoundSource",
                &format!("unknown-bound-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..12)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowUnknownBoundTarget",
                &format!("unknown-bound-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for target in &targets {
        insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_UNKNOWN_BOUND_REL",
            &[],
        );
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_UNKNOWN_BOUND_REL",
        )],
    );
    query.nodes[0].ids = sources;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let clamped = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    let unclamped = view
        .plan_graph_row_physical(&normalized, &runtime, true, false)
        .unwrap();
    let node_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source"))
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    let edge_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| alternative.kind == "EdgeAnchor")
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    let physical = view.graph_row_physical_edge_bound();
    assert_eq!(
        node_cost(&clamped)
            .estimated_work
            .saturating_sub(node_cost(&clamped).anchor_cost.estimated_work),
        physical
    );
    assert!(node_cost(&unclamped).estimated_work > node_cost(&clamped).estimated_work);
    assert_eq!(
        edge_cost(&clamped).estimated_work,
        edge_cost(&unclamped).estimated_work,
        "EdgeAnchor simulations never receive the distinct-frontier proof"
    );
}

#[test]
fn graph_row_distinct_proof_top_level_parity_and_incoming_stage_negatives() {
    let (_dir, engine) = graph_row_test_engine();
    let sources = (0..12)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowDistinctSource",
                &format!("distinct-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..20)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowDistinctTarget",
                &format!("distinct-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for target in &targets {
        insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_DISTINCT_REL",
            &[],
        );
    }
    for source in &sources[1..] {
        insert_graph_row_edge(
            &engine,
            *source,
            targets[0],
            "GRAPH_ROW_DISTINCT_REL",
            &[],
        );
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_DISTINCT_REL",
        )],
    );
    query.nodes[0].ids = sources.clone();
    query.options.include_plan = true;
    query.page.limit = 100;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let top_level = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    let incoming = view
        .plan_graph_row_physical(&normalized, &runtime, true, false)
        .unwrap();
    let node_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source"))
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    let edge_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| alternative.kind == "EdgeAnchor")
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    let physical = view.graph_row_physical_edge_bound();
    assert_eq!(physical, 31);
    assert_eq!(
        node_cost(&top_level)
            .estimated_work
            .saturating_sub(node_cost(&top_level).anchor_cost.estimated_work),
        physical
    );
    assert!(node_cost(&incoming).estimated_work > node_cost(&top_level).estimated_work);
    assert_eq!(edge_cost(&incoming).estimated_work, edge_cost(&top_level).estimated_work);

    let expected_cost = format!("cost_work={}", node_cost(&top_level).estimated_work);
    let static_explain = graph_row_explain_text(&engine.explain_graph_rows(&query).unwrap());
    assert!(static_explain.contains(&expected_cost), "{static_explain}");
    let executed = engine.query_graph_rows(&query).unwrap();
    let executed_explain = graph_row_explain_text(executed.plan.as_ref().unwrap());
    assert!(executed_explain.contains(&expected_cost), "{executed_explain}");

    let source_slot = normalized.binding_schema.slot_for_alias("source").unwrap();
    let repeated_rows = [sources[0], sources[0], sources[1]]
        .into_iter()
        .map(|source_id| {
            let mut row = normalized.binding_schema.empty_row();
            row.bind_node(source_slot, crate::graph_row::GraphBoundNode::id_only(source_id))
                .unwrap();
            row
        })
        .collect::<Vec<_>>();
    let stage = view
        .execute_graph_row_stage(
            &normalized,
            Some(repeated_rows.clone()),
            i64::MAX / 2,
            true,
            false,
        )
        .unwrap();
    let incoming_cost = format!("cost_work={}", node_cost(&incoming).estimated_work);
    let stage_explain = graph_row_explain_text(stage.explain.as_ref().unwrap());
    assert!(stage_explain.contains(&incoming_cost), "{stage_explain}");
    assert!(!stage.rows.is_empty());

    let exists = view
        .execute_graph_row_stage_exists(
            &normalized,
            Some(repeated_rows),
            i64::MAX / 2,
        )
        .unwrap();
    assert_eq!(exists.rows.len(), 1);
}

#[test]
fn graph_row_distinct_proof_later_segment_and_both_bound_first_edge_consume_it() {
    let (_dir, engine) = graph_row_test_engine();
    let anchors = (0..6)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowProofAnchor",
                &format!("proof-anchor-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..18)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowProofTarget",
                &format!("proof-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for anchor in &anchors {
        insert_graph_row_edge(
            &engine,
            *anchor,
            *anchor,
            "GRAPH_ROW_PROOF_SELF",
            &[],
        );
    }
    for target in &targets {
        insert_graph_row_edge(
            &engine,
            anchors[0],
            *target,
            "GRAPH_ROW_PROOF_HIGH",
            &[],
        );
    }
    let barrier_left = insert_graph_row_node(&engine, "GraphRowProofBarrier", "proof-left", &[]);
    let barrier_right = insert_graph_row_node(&engine, "GraphRowProofBarrier", "proof-right", &[]);
    insert_graph_row_edge(
        &engine,
        barrier_left,
        barrier_right,
        "GRAPH_ROW_PROOF_BARRIER",
        &[],
    );
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();

    let mut both_bound_query = graph_query(
        &["source", "target"],
        vec![
            graph_edge_with_label(
                Some("self_edge"),
                "source",
                "source",
                "GRAPH_ROW_PROOF_SELF",
            ),
            graph_edge_with_label(
                Some("high_edge"),
                "source",
                "target",
                "GRAPH_ROW_PROOF_HIGH",
            ),
        ],
    );
    both_bound_query.nodes[0].ids = anchors.clone();
    let normalized = normalize_graph_row_query(&both_bound_query).unwrap();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let eligible = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    let ineligible = view
        .plan_graph_row_physical(&normalized, &runtime, true, false)
        .unwrap();
    let source_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source"))
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    assert_eq!(
        source_cost(&eligible).estimated_work,
        source_cost(&ineligible).estimated_work,
        "the both-endpoints-bound first edge consumes the proof before the high-fanout hop"
    );

    let mut later_query = graph_query(
        &["a", "b", "x", "c", "d"],
        vec![
            graph_edge_with_label(
                Some("barrier_edge"),
                "a",
                "b",
                "GRAPH_ROW_PROOF_BARRIER",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional_edge"),
                    "b",
                    "x",
                    "GRAPH_ROW_PROOF_OPTIONAL",
                )],
                None,
            ),
            graph_edge_with_label(
                Some("later_edge"),
                "c",
                "d",
                "GRAPH_ROW_PROOF_HIGH",
            ),
        ],
    );
    later_query.nodes[0].ids = vec![barrier_left];
    later_query.nodes[3].ids = anchors;
    let later_normalized = normalize_graph_row_query(&later_query).unwrap();
    let later_runtime = view
        .normalize_graph_row_runtime_plan(&later_normalized)
        .unwrap();
    let later_eligible = view
        .plan_graph_row_physical(&later_normalized, &later_runtime, true, true)
        .unwrap();
    let later_ineligible = view
        .plan_graph_row_physical(&later_normalized, &later_runtime, true, false)
        .unwrap();
    let later_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| {
                alternative.kind == "NodeAnchor"
                    && alternative.detail.contains("segment=1")
                    && alternative.detail.contains("alias=c")
            })
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    assert_eq!(
        later_cost(&later_eligible).estimated_work,
        later_cost(&later_ineligible).estimated_work,
        "later required segments never receive the distinct-frontier proof"
    );
}

#[test]
fn graph_row_distinct_proof_optional_static_and_execution_keep_incoming_context() {
    let (_dir, engine) = graph_row_test_engine();
    let sources = (0..6)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowProofOptionalSource",
                &format!("proof-optional-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..16)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowProofOptionalTarget",
                &format!("proof-optional-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let outers = (0..sources.len())
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowProofOptionalOuter",
                &format!("proof-optional-outer-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for (outer, source) in outers.iter().zip(&sources) {
        insert_graph_row_edge(
            &engine,
            *outer,
            *source,
            "GRAPH_ROW_PROOF_OPTIONAL_OUTER",
            &[],
        );
    }
    for target in &targets {
        insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_PROOF_OPTIONAL_HIGH",
            &[],
        );
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["outer", "source", "target"],
        vec![
            graph_edge_with_label(
                Some("outer_edge"),
                "outer",
                "source",
                "GRAPH_ROW_PROOF_OPTIONAL_OUTER",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("edge"),
                    "source",
                    "target",
                    "GRAPH_ROW_PROOF_OPTIONAL_HIGH",
                )],
                None,
            ),
        ],
    );
    query.nodes[0].ids = outers;
    query.nodes[1].ids = sources;
    query.options.include_plan = true;
    query.page.limit = 100;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let group = runtime
        .steps
        .iter()
        .find_map(|step| match step {
            GraphRowRuntimeStep::Optional(group) => Some(group),
            _ => None,
        })
        .unwrap();
    assert!(!group.dependency_slots.is_empty());
    let eligible = view
        .plan_graph_row_physical(&normalized, &group.runtime, true, true)
        .unwrap();
    let incoming = view
        .plan_graph_row_physical(&normalized, &group.runtime, true, false)
        .unwrap();
    let source_cost = |plan: &GraphRowPhysicalPlan| {
        plan.alternatives
            .iter()
            .find(|alternative| alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source"))
            .and_then(|alternative| alternative.cost.as_ref())
            .cloned()
            .unwrap()
    };
    assert!(source_cost(&incoming).estimated_work > source_cost(&eligible).estimated_work);
    let incoming_cost = format!("cost_work={}", source_cost(&incoming).estimated_work);

    let static_explain = graph_row_explain_text(&engine.explain_graph_rows(&query).unwrap());
    assert!(static_explain.contains(&incoming_cost), "{static_explain}");
    let executed = engine.query_graph_rows(&query).unwrap();
    assert!(!executed.rows.is_empty());
}

#[test]
fn graph_row_hydration_split_charges_candidates_but_feeds_output_rows_downstream() {
    let (_dir, engine) = graph_row_test_engine();
    let decoy_sources = (0..33)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHydrationDecoySource",
                &format!("hydration-decoy-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let decoy_targets = (0..66)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHydrationDecoyTarget",
                &format!("hydration-decoy-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for (index, source) in decoy_sources.iter().enumerate() {
        for target in &decoy_targets[index * 2..index * 2 + 2] {
            insert_graph_row_edge(
                &engine,
                *source,
                *target,
                "GRAPH_ROW_HYDRATION_FIRST",
                &[("status", PropValue::String("hot".to_string()))],
            );
        }
    }
    let sources = (0..2)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHydrationSource",
                &format!("hydration-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let middles = (0..4)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHydrationMiddle",
                &format!("hydration-middle-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let ends = (0..4)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHydrationEnd",
                &format!("hydration-end-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for (index, middle) in middles.iter().enumerate() {
        insert_graph_row_edge(
            &engine,
            sources[index / 2],
            *middle,
            "GRAPH_ROW_HYDRATION_FIRST",
            &[("status", PropValue::String("hot".to_string()))],
        );
        insert_graph_row_edge(
            &engine,
            *middle,
            ends[index],
            "GRAPH_ROW_HYDRATION_SECOND",
            &[],
        );
    }
    engine.flush().unwrap();

    let mut first_edge = match graph_edge_with_label(
        Some("first"),
        "source",
        "middle",
        "GRAPH_ROW_HYDRATION_FIRST",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    first_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let mut query = graph_query(
        &["source", "middle", "end"],
        vec![
            GraphPatternPiece::Edge(first_edge),
            graph_edge_with_label(
                Some("second"),
                "middle",
                "end",
                "GRAPH_ROW_HYDRATION_SECOND",
            ),
        ],
    );
    query.nodes[0].ids = sources;
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let plan = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    let cost = plan
        .alternatives
        .iter()
        .find(|alternative| alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source"))
        .and_then(|alternative| alternative.cost.as_ref())
        .unwrap();
    assert_eq!(cost.anchor_cost.estimated_work, 3);
    assert_eq!(
        cost.estimated_work, 15,
        "3 anchor work + 4 candidate/4 output hydration work + 4 downstream rows"
    );
    assert_eq!(
        cost.simulated_frontier, 4,
        "first-hop output rows, not its 8 units of hydration work, feed hop two"
    );
    assert_eq!(plan.edge_order, vec![0, 1]);
    assert_eq!(engine.query_graph_rows(&query).unwrap().rows.len(), 4);

    let legacy = with_graph_row_test_legacy_estimator(|| {
        view.plan_graph_row_physical(&normalized, &runtime, true, true)
            .unwrap()
    });
    let legacy_cost = legacy
        .alternatives
        .iter()
        .find(|alternative| {
            alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source")
        })
        .and_then(|alternative| alternative.cost.as_ref())
        .unwrap();
    assert_eq!(legacy_cost.estimated_work, 19);
    assert_eq!(legacy_cost.simulated_frontier, 8);
    assert!(!graph_row_test_legacy_estimator_is_enabled());

    let restored = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    let restored_cost = restored
        .alternatives
        .iter()
        .find(|alternative| {
            alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source")
        })
        .and_then(|alternative| alternative.cost.as_ref())
        .unwrap();
    assert_eq!(restored_cost.estimated_work, 15);
    assert_eq!(restored_cost.simulated_frontier, 4);
}

#[test]
fn graph_row_explicit_edge_ids_suppress_adjacency_and_execute_explicit_source() {
    let (_dir, engine) = graph_row_test_engine();
    let sources = (0..8)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowExplicitEdgeSource",
                &format!("explicit-edge-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..24)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowExplicitEdgeTarget",
                &format!("explicit-edge-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let mut edge_ids = Vec::new();
    for target in &targets {
        edge_ids.push(insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_EXPLICIT_EDGE_REL",
            &[],
        ));
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_EXPLICIT_EDGE_REL",
        )],
    );
    query.nodes[0].ids = sources;
    query.options.allow_full_scan = false;
    let mut normalized = normalize_graph_row_query(&query).unwrap();
    normalized
        .edge_id_constraints
        .insert("edge".to_string(), edge_ids.clone());
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    assert_eq!(runtime.edges[0].candidate_edge_ids, edge_ids);
    reset_graph_row_planning_probe();
    let plan = view
        .plan_graph_row_physical(&normalized, &runtime, true, true)
        .unwrap();
    assert_eq!(
        snapshot_graph_row_planning_probe().endpoint_adjacency_offers,
        0,
        "current pricing must suppress the impossible endpoint-adjacency choice"
    );
    assert!(matches!(
        plan.initial_driver,
        GraphRowInitialDriver::Edge { edge_index: 0, .. }
    ));
    assert_eq!(
        plan.edge_source_choices[0],
        Some(GraphRowEdgeCandidateSourceChoice::ExplicitIds)
    );
    assert!(plan
        .alternatives
        .iter()
        .any(|alternative| alternative.chosen && alternative.kind == "EdgeAnchor"));

    reset_graph_row_planning_probe();
    let legacy_plan = with_graph_row_test_legacy_estimator(|| {
        view.plan_graph_row_physical(&normalized, &runtime, true, true)
            .unwrap()
    });
    assert!(
        snapshot_graph_row_planning_probe().endpoint_adjacency_offers > 0,
        "legacy pricing must offer the formerly impossible endpoint-adjacency choice even when it does not win"
    );
    assert_eq!(
        legacy_plan.edge_source_choices[0],
        Some(GraphRowEdgeCandidateSourceChoice::ExplicitIds)
    );
    assert!(!graph_row_test_legacy_estimator_is_enabled());

    let policy_cutoffs = view.query_policy_cutoffs();
    let mut followups = Vec::new();
    let mut frontier = 0;
    let mut intermediate = 0;
    let mut paths = 0;
    let rows = view
        .graph_row_execute_runtime_plan(
            &normalized,
            &runtime,
            &plan,
            None,
            GraphRowRuntimeGoal::AllRows,
            i64::MAX / 2,
            policy_cutoffs.as_ref(),
            &mut followups,
            &mut frontier,
            &mut intermediate,
            &mut paths,
            None,
        )
        .unwrap();
    assert_eq!(rows.len(), 24);
    let edge_slot = runtime.edges[0].edge_slot.unwrap();
    let mut actual_edge_ids = rows
        .iter()
        .map(|row| row.edge_id_for_slot_if_bound(edge_slot).unwrap().unwrap())
        .collect::<Vec<_>>();
    actual_edge_ids.sort_unstable();
    assert_eq!(actual_edge_ids, edge_ids);
}

#[test]
fn graph_row_hub_poisoned_plan_clamps_warm_and_reopened_with_deterministic_caps() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        },
    )
    .unwrap();
    let sources = (0..20)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHubPlanSource",
                &format!("hub-plan-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..100)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowHubPlanTarget",
                &format!("hub-plan-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for target in &targets[..80] {
        insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_HUB_PLAN_REL",
            &[],
        );
    }
    engine.flush().unwrap();
    for target in &targets[80..] {
        insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_HUB_PLAN_REL",
            &[],
        );
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_HUB_PLAN_REL",
        )],
    );
    query.nodes[0] = graph_node_with_label("source", "GraphRowHubPlanSource");
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.page.limit = 10;

    let physical_costs = |engine: &DatabaseEngine| {
        let normalized = normalize_graph_row_query(&query).unwrap();
        let view = engine.published_read_view_for_test();
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let plan = view
            .plan_graph_row_physical(&normalized, &runtime, true, true)
            .unwrap();
        assert!(matches!(
            plan.initial_driver,
            GraphRowInitialDriver::Node { ref alias, .. } if alias == "source"
        ));
        let node = plan
            .alternatives
            .iter()
            .find(|alternative| alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source"))
            .and_then(|alternative| alternative.cost.as_ref())
            .unwrap();
        let edge = plan
            .alternatives
            .iter()
            .find(|alternative| alternative.kind == "EdgeAnchor")
            .and_then(|alternative| alternative.cost.as_ref())
            .unwrap();
        assert!(node.estimated_work < edge.estimated_work);

        let legacy_plan = with_graph_row_test_legacy_estimator(|| {
            view.plan_graph_row_physical(&normalized, &runtime, true, true)
                .unwrap()
        });
        let legacy_node = legacy_plan
            .alternatives
            .iter()
            .find(|alternative| {
                alternative.kind == "NodeAnchor" && alternative.detail.contains("alias=source")
            })
            .and_then(|alternative| alternative.cost.as_ref())
            .unwrap();
        assert!(
            legacy_node.estimated_work > node.estimated_work,
            "legacy pricing must omit the distinct-frontier physical-edge clamp"
        );
        assert!(!graph_row_test_legacy_estimator_is_enabled());
        (node.estimated_work, edge.estimated_work)
    };

    let warm_costs = physical_costs(&engine);
    let warm = engine.query_graph_rows(&query).unwrap();
    let mut pinned = query.clone();
    pinned.at_epoch = Some(warm.stats.effective_at_epoch);
    let pinned_first = engine.query_graph_rows(&pinned).unwrap();
    let repeated = engine.query_graph_rows(&pinned).unwrap();
    assert_eq!(pinned_first.rows, repeated.rows);
    assert_eq!(pinned_first.next_cursor, repeated.next_cursor);
    assert!(pinned_first.next_cursor.is_some());
    let mut continuation = pinned.clone();
    continuation.page.cursor = pinned_first.next_cursor.clone();
    assert_eq!(engine.query_graph_rows(&continuation).unwrap().rows.len(), 10);

    let mut ordered = pinned.clone();
    ordered.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Binding("target".to_string()),
        direction: GraphOrderDirection::Asc,
    }];
    ordered.page.cursor = None;
    let warm_ordered = engine.query_graph_rows(&ordered).unwrap();

    engine.flush().unwrap();
    engine.close().unwrap();
    let reopened = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        },
    )
    .unwrap();
    assert_eq!(physical_costs(&reopened), warm_costs);
    let reopened_page = reopened.query_graph_rows(&pinned).unwrap();
    assert_eq!(reopened_page.rows, pinned_first.rows);
    assert_eq!(reopened_page.next_cursor, pinned_first.next_cursor);
    assert_eq!(reopened.query_graph_rows(&ordered).unwrap().rows, warm_ordered.rows);
}

#[test]
fn graph_row_legacy_estimator_restores_zero_unknown_for_single_and_both_no_rollup() {
    let dir = TempDir::new().unwrap();
    let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    let query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: Vec::new(),
            filter: None,
        })],
    );
    let normalized = normalize_graph_row_query(&query).unwrap();
    let view = engine.published_read_view_for_test();
    let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
    let edge = &runtime.edges[0];
    let edge_bound = view.graph_row_physical_edge_bound_context();

    let current_single = view.graph_row_single_direction_fanout_estimate(
        PlannerStatsDirection::Outgoing,
        None,
        None,
        edge_bound,
    );
    assert_eq!(current_single.cost_fanout(), GRAPH_ROW_FANOUT_UNKNOWN_WORK);
    assert_eq!(current_single.visible_total_edges_bound, Some(0));
    let legacy_single = with_graph_row_test_legacy_estimator(|| {
        view.graph_row_single_direction_fanout_estimate(
            PlannerStatsDirection::Outgoing,
            None,
            None,
            edge_bound,
        )
    });
    assert_eq!(legacy_single, GraphRowFanoutEstimate::unknown());
    assert_eq!(legacy_single.cost_fanout(), 0);

    let mut current_memo = GraphRowKnownIdPostingMemo::new(1);
    let current_both = view.graph_row_edge_fanout_estimate(
        0,
        true,
        Direction::Both,
        None,
        None,
        &normalized,
        edge,
        edge_bound,
        &mut current_memo,
    );
    assert_eq!(current_both.cost_fanout(), GRAPH_ROW_FANOUT_UNKNOWN_WORK);
    assert_eq!(current_both.visible_total_edges_bound, Some(0));
    let legacy_both = with_graph_row_test_legacy_estimator(|| {
        let mut legacy_memo = GraphRowKnownIdPostingMemo::new(1);
        view.graph_row_edge_fanout_estimate(
            0,
            true,
            Direction::Both,
            None,
            None,
            &normalized,
            edge,
            edge_bound,
            &mut legacy_memo,
        )
    });
    assert_eq!(legacy_both.cost_fanout(), 0);
    assert_eq!(legacy_both.visible_total_edges_bound, None);
    assert_eq!(legacy_both.coverage, GraphRowFanoutCoverage::Missing);
    assert!(!graph_row_test_legacy_estimator_is_enabled());
}

#[derive(Clone)]
enum GraphRowLegacyCorpusWorkload {
    Graph(GraphRowQuery),
    GraphExplainExecute(GraphRowQuery),
    Pipeline(GraphPipelineQuery),
    Gql(String),
    ExplicitEdgeIds {
        query: GraphRowQuery,
        edge_alias: String,
        edge_ids: Vec<u64>,
    },
}

#[derive(Clone)]
struct GraphRowLegacyCorpusCase {
    name: &'static str,
    family: &'static str,
    ordered: bool,
    allow_unordered_plan_dependent_rows: bool,
    workload: GraphRowLegacyCorpusWorkload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphRowLegacyCorpusObservation {
    signatures: Vec<GraphRowTestPlanSignature>,
    rows: Option<String>,
    next_cursor: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
struct GraphRowLegacyFlipAllowlistEntry {
    case_name: &'static str,
    legacy_signatures: Vec<GraphRowTestPlanSignature>,
    current_signatures: Vec<GraphRowTestPlanSignature>,
    causing_decision: &'static str,
    soundness_rationale: &'static str,
    audit_classification: &'static str,
}

fn graph_row_legacy_single_edge_signature(
    initial_driver: &str,
    source_choice: &str,
) -> Vec<GraphRowTestPlanSignature> {
    vec![GraphRowTestPlanSignature {
        initial_driver: initial_driver.to_string(),
        initial_edge_production: initial_driver
            .starts_with("Edge(")
            .then(|| "EdgePull".to_string()),
        segments: vec![GraphRowTestSegmentSignature {
            segment_index: 0,
            initial_driver: initial_driver.to_string(),
            edge_order: vec![0],
        }],
        edge_order: vec![0],
        edge_source_choices: vec![Some(source_choice.to_string())],
    }]
}

fn graph_row_legacy_corpus_observe(
    engine: &DatabaseEngine,
    case: &GraphRowLegacyCorpusCase,
    legacy: bool,
) -> GraphRowLegacyCorpusObservation {
    let execute = || match &case.workload {
        GraphRowLegacyCorpusWorkload::Graph(query) => engine
            .query_graph_rows(query)
            .map(|result| (format!("{:?}", result.rows), result.next_cursor)),
        GraphRowLegacyCorpusWorkload::GraphExplainExecute(query) => {
            engine.explain_graph_rows(query)?;
            engine
                .query_graph_rows(query)
                .map(|result| (format!("{:?}", result.rows), result.next_cursor))
        }
        GraphRowLegacyCorpusWorkload::Pipeline(query) => engine
            .query_graph_pipeline(query)
            .map(|result| (format!("{:?}", result.rows), result.next_cursor)),
        GraphRowLegacyCorpusWorkload::Gql(source) => engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    allow_full_scan: true,
                    ..GqlExecutionOptions::default()
                },
            )
            .map(|result| (format!("{:?}", result.rows), result.next_cursor)),
        GraphRowLegacyCorpusWorkload::ExplicitEdgeIds {
            query,
            edge_alias,
            edge_ids,
        } => {
            let mut normalized = normalize_graph_row_query(query)?;
            normalized
                .edge_id_constraints
                .insert(edge_alias.clone(), edge_ids.clone());
            let view = engine.published_read_view_for_test();
            let runtime = view.normalize_graph_row_runtime_plan(&normalized)?;
            let plan = view.plan_graph_row_physical(&normalized, &runtime, true, true)?;
            let policy_cutoffs = view.query_policy_cutoffs();
            let mut followups = Vec::new();
            let mut frontier = 0;
            let mut intermediate = 0;
            let mut paths = 0;
            let rows = view.graph_row_execute_runtime_plan(
                &normalized,
                &runtime,
                &plan,
                None,
                GraphRowRuntimeGoal::AllRows,
                normalized.at_epoch.unwrap_or(i64::MAX / 2),
                policy_cutoffs.as_ref(),
                &mut followups,
                &mut frontier,
                &mut intermediate,
                &mut paths,
                None,
            )?;
            Ok((format!("{rows:?}"), None))
        }
    };
    let (result, signatures) = with_graph_row_test_plan_signature_capture(|| {
        if legacy {
            with_graph_row_test_legacy_estimator(execute)
        } else {
            execute()
        }
    });
    match result {
        Ok((rows, next_cursor)) => GraphRowLegacyCorpusObservation {
            signatures,
            rows: Some(rows),
            next_cursor,
            error: None,
        },
        Err(error) => GraphRowLegacyCorpusObservation {
            signatures,
            rows: None,
            next_cursor: None,
            error: Some(error.to_string()),
        },
    }
}

fn graph_row_legacy_corpus_walk(
    engine: &DatabaseEngine,
    query: &GraphRowQuery,
    legacy: bool,
) -> (Vec<String>, Vec<String>) {
    let walk = || {
        let mut query = query.clone();
        let mut rows = Vec::new();
        let mut cursors = Vec::new();
        loop {
            let page = engine.query_graph_rows(&query).unwrap();
            rows.extend(page.rows.iter().map(|row| format!("{row:?}")));
            let Some(cursor) = page.next_cursor else {
                break;
            };
            assert!(cursors.len() < 256, "cursor walk did not terminate");
            cursors.push(cursor.clone());
            query.page.cursor = Some(cursor);
        }
        (rows, cursors)
    };
    if legacy {
        with_graph_row_test_legacy_estimator(walk)
    } else {
        walk()
    }
}

#[test]
fn graph_row_legacy_estimator_ab_corpus_is_allowlisted_and_behaviorally_sound() {
    const REQUIRED_MANIFEST: [(&str, &str); 27] = [
        ("node_only_no_fixed_edge", "top-level node-only fast path"),
        ("anchored_low_fanout_single_hop", "anchored Node/EndpointAdjacency"),
        ("unanchored_indexed_single_hop_edge_driver", "Edge-driven by legality"),
        ("warm_hub_unordered_limit", "hub-poisoned unordered LIMIT"),
        ("reopened_hub_ordered_limit", "hub-poisoned ordered/reopen"),
        ("explicit_source_ids_both", "known-ID Both orientation"),
        ("explicit_edge_ids", "explicit-edge-ID suppression"),
        ("hydrating_two_hop_required", "hydration rows/work split"),
        ("branched_required_path", "branched required path"),
        ("later_required_after_optional_barrier", "later required segment"),
        ("both_endpoints_bound_first", "proof-consuming bound edge"),
        ("uncorrelated_optional", "optional reusable apply"),
        ("correlated_optional_repeated_inputs", "optional repeated dependencies"),
        ("one_hop_vlp", "one-hop VLP"),
        ("multi_hop_vlp", "multi-hop VLP"),
        ("native_pipeline_incoming_unordered_limit", "native seeded MATCH stage"),
        ("gql_lowered_seeded_match_stage", "GQL lowered seeded MATCH"),
        ("ordered_fixed_edge_cursor_walk", "ordered multi-page fixed edge"),
        ("top_level_explain_execute_parity", "top-level explain/execute entry points"),
        ("optional_static_runtime_parity", "optional static/runtime entry points"),
        ("pipeline_exists_incoming", "RuntimeOnce Exists entry point"),
        ("one_stage_pipeline_compatibility", "one-stage RuntimeOnce Stage compatibility"),
        ("absent_label_empty_source", "EmptyResult source"),
        ("reverse_anchor_orientation", "reverse endpoint orientation"),
        ("bound_edge_candidate_source", "edge candidate source versus adjacency"),
        ("gql_direct_fixed_chain", "direct GQL GraphRow lowering"),
        ("edge_property_fallback_source", "unavailable property index falls back to edge label source"),
    ];
    const EXCLUDED_FAMILIES: [(&str, &str); 7] = [
        ("binding/evaluator", "synthetic bindings never call physical planning"),
        ("projection/output", "needs collection and conversion have no ReadView plan"),
        ("normalizer-only", "stops after normalize_graph_row_query"),
        ("validation/error-only", "fails before runtime-plan construction"),
        ("pipeline value-only", "stats/options/project-only cases contain no MATCH"),
        ("GQL parser/semantic/lowering unit", "constructs IR but no ReadView physical plan"),
        ("mutation/schema/index DDL", "contains no graph-row read planning"),
    ];
    assert!(EXCLUDED_FAMILIES.iter().all(|(family, reason)| {
        !family.is_empty() && reason.contains(|character: char| character.is_ascii_alphabetic())
    }));
    const FIXTURE_FAMILY_CLASSIFICATION: [(&str, &str, &str); 18] = [
        ("top-level node/fixed/planner/explain", "case", "top_level_explain_execute_parity"),
        ("anchored and unanchored source legality", "case", "anchored_low_fanout_single_hop"),
        ("hub-poisoned warm/reopen/order/limit", "case", "reopened_hub_ordered_limit"),
        ("physical-bound and known-ID estimator proofs", "case", "explicit_source_ids_both"),
        ("explicit edge source-policy interaction", "case", "explicit_edge_ids"),
        ("property-index candidate source", "case", "bound_edge_candidate_source"),
        ("property-sidecar unavailable/corrupt fallback", "case", "edge_property_fallback_source"),
        ("optional static/runtime and correlation", "case", "optional_static_runtime_parity"),
        ("VLP fixed/delegated/optional", "case", "multi_hop_vlp"),
        ("native pipeline MATCH and one-stage compatibility", "case", "native_pipeline_incoming_unordered_limit"),
        ("pipeline EXISTS/subquery", "case", "pipeline_exists_incoming"),
        ("cursor/order/chunk production", "case", "ordered_fixed_edge_cursor_walk"),
        ("direct GQL fixed/optional/VLP lowering", "case", "gql_direct_fixed_chain"),
        ("GQL WITH/later MATCH pipeline", "case", "gql_lowered_seeded_match_stage"),
        ("binding/evaluator and projection/output", "exclude", "binding/evaluator"),
        ("normalizer and validation/error paths", "exclude", "normalizer-only"),
        ("pipeline/GQL value and parser-only units", "exclude", "pipeline value-only"),
        ("mutation/schema/index DDL", "exclude", "mutation/schema/index DDL"),
    ];
    for (family, disposition, target) in FIXTURE_FAMILY_CLASSIFICATION {
        assert!(!family.is_empty());
        match disposition {
            "case" => assert!(
                REQUIRED_MANIFEST.iter().any(|(name, _)| *name == target),
                "planner-bearing fixture family {family} must map to a corpus case"
            ),
            "exclude" => assert!(
                EXCLUDED_FAMILIES.iter().any(|(name, _)| *name == target),
                "non-planning fixture family {family} must map to an exclusion"
            ),
            _ => panic!("unknown fixture-family disposition {disposition}"),
        }
    }

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        },
    )
    .unwrap();
    let sources = (0..20)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowLegacyCorpusSource",
                &format!("legacy-source-{index}"),
                &[("rank", PropValue::Int(index))],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..100)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "GraphRowLegacyCorpusTarget",
                &format!("legacy-target-{index}"),
                &[("rank", PropValue::Int(index))],
            )
        })
        .collect::<Vec<_>>();
    let mut hub_edge_ids = Vec::new();
    for target in &targets[..80] {
        hub_edge_ids.push(insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_LEGACY_HUB",
            &[("status", PropValue::String("hot".to_string()))],
        ));
    }
    insert_graph_row_edge(
        &engine,
        sources[1],
        targets[0],
        "GRAPH_ROW_LEGACY_LOW",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        sources[2],
        targets[0],
        "GRAPH_ROW_LEGACY_LOW",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        targets[0],
        targets[1],
        "GRAPH_ROW_LEGACY_CHAIN",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        targets[1],
        targets[2],
        "GRAPH_ROW_LEGACY_CHAIN",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        targets[0],
        targets[3],
        "GRAPH_ROW_LEGACY_OPTIONAL",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        sources[3],
        targets[4],
        "GRAPH_ROW_LEGACY_LATER",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        sources[4],
        sources[4],
        "GRAPH_ROW_LEGACY_SELF",
        &[],
    );
    engine.flush().unwrap();
    for target in &targets[80..] {
        hub_edge_ids.push(insert_graph_row_edge(
            &engine,
            sources[0],
            *target,
            "GRAPH_ROW_LEGACY_HUB",
            &[("status", PropValue::String("hot".to_string()))],
        ));
    }

    let index = engine
        .ensure_edge_property_index(
            "GRAPH_ROW_LEGACY_HUB",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let make_hub_query = |ordered: bool| {
        let mut query = graph_query(
            &["source", "target"],
            vec![graph_edge_with_label(
                Some("edge"),
                "source",
                "target",
                "GRAPH_ROW_LEGACY_HUB",
            )],
        );
        query.nodes[0] = graph_node_with_label("source", "GraphRowLegacyCorpusSource");
        query.options.allow_full_scan = false;
        query.page.limit = 3;
        if ordered {
            query.order_by = vec![GraphOrderItem {
                expr: GraphExpr::Binding("target".to_string()),
                direction: GraphOrderDirection::Asc,
            }];
        }
        query
    };

    let mut node_only = graph_query(&["source"], Vec::new());
    node_only.nodes[0].ids = vec![sources[0], sources[1]];
    node_only.page.limit = 10;

    let mut anchored = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_LEGACY_LOW",
        )],
    );
    anchored.nodes[0].ids = vec![sources[1]];
    anchored.options.allow_full_scan = false;

    let mut unanchored = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_LEGACY_LOW",
        )],
    );
    unanchored.options.allow_full_scan = false;

    let mut explicit_ids_both = make_hub_query(false);
    explicit_ids_both.nodes[0] = graph_node("source");
    explicit_ids_both.nodes[0].ids = vec![sources[0]];
    if let GraphPatternPiece::Edge(edge) = &mut explicit_ids_both.pieces[0] {
        edge.direction = Direction::Both;
    }

    let mut explicit_edge_query = make_hub_query(true);
    explicit_edge_query.nodes[0] = graph_node("source");
    explicit_edge_query.nodes[0].ids = sources.clone();

    let mut hydrating_edge = match graph_edge_with_label(
        Some("first"),
        "source",
        "middle",
        "GRAPH_ROW_LEGACY_HUB",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    hydrating_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let mut hydrating = graph_query(
        &["source", "middle", "end"],
        vec![
            GraphPatternPiece::Edge(hydrating_edge),
            graph_edge_with_label(
                Some("second"),
                "middle",
                "end",
                "GRAPH_ROW_LEGACY_CHAIN",
            ),
        ],
    );
    hydrating.nodes[0].ids = vec![sources[0]];

    let mut branched = graph_query(
        &["source", "left", "right"],
        vec![
            graph_edge_with_label(
                Some("left_edge"),
                "source",
                "left",
                "GRAPH_ROW_LEGACY_HUB",
            ),
            graph_edge_with_label(
                Some("right_edge"),
                "source",
                "right",
                "GRAPH_ROW_LEGACY_LOW",
            ),
        ],
    );
    branched.nodes[0].ids = vec![sources[0], sources[1]];

    let mut later = graph_query(
        &["source", "middle", "optional", "later_source", "later_target"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "source",
                "middle",
                "GRAPH_ROW_LEGACY_LOW",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional_edge"),
                    "middle",
                    "optional",
                    "GRAPH_ROW_LEGACY_OPTIONAL",
                )],
                None,
            ),
            graph_edge_with_label(
                Some("later_edge"),
                "later_source",
                "later_target",
                "GRAPH_ROW_LEGACY_LATER",
            ),
        ],
    );
    later.nodes[0].ids = vec![sources[1]];
    later.nodes[3].ids = vec![sources[3]];

    let mut both_bound = graph_query(
        &["source", "target", "end"],
        vec![
            graph_edge_with_label(
                Some("self_edge"),
                "source",
                "source",
                "GRAPH_ROW_LEGACY_SELF",
            ),
            graph_edge_with_label(
                Some("follow"),
                "source",
                "target",
                "GRAPH_ROW_LEGACY_HUB",
            ),
            graph_edge_with_label(
                Some("end"),
                "target",
                "end",
                "GRAPH_ROW_LEGACY_CHAIN",
            ),
        ],
    );
    both_bound.nodes[0].ids = vec![sources[4]];

    let mut uncorrelated_optional = graph_query(
        &["source", "target", "optional_source", "optional_target"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "source",
                "target",
                "GRAPH_ROW_LEGACY_LOW",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional_edge"),
                    "optional_source",
                    "optional_target",
                    "GRAPH_ROW_LEGACY_LATER",
                )],
                None,
            ),
        ],
    );
    uncorrelated_optional.nodes[0].ids = vec![sources[1]];
    uncorrelated_optional.nodes[2].ids = vec![sources[3]];

    let mut correlated_optional = graph_query(
        &["source", "middle", "optional"],
        vec![
            graph_edge_with_label(
                Some("outer"),
                "source",
                "middle",
                "GRAPH_ROW_LEGACY_LOW",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional_edge"),
                    "middle",
                    "optional",
                    "GRAPH_ROW_LEGACY_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    correlated_optional.nodes[0].ids = vec![sources[1], sources[2]];

    let mut one_hop_vlp = graph_query(
        &["source", "target"],
        vec![graph_vlp(Some("path"), Some("edge"), "source", "target", 1, 1)],
    );
    one_hop_vlp.nodes[0].ids = vec![targets[0]];
    let mut multi_hop_vlp = graph_query(
        &["source", "target"],
        vec![graph_vlp(Some("path"), Some("edge"), "source", "target", 1, 2)],
    );
    multi_hop_vlp.nodes[0].ids = vec![targets[0]];

    let first_match = GraphPipelineStage::Match(GraphPipelineMatchStage {
        optional: false,
        nodes: vec![graph_node_with_label("source", "GraphRowLegacyCorpusSource")],
        pieces: Vec::new(),
        optional_candidate_where: None,
        where_: None,
    });
    let with_limit = GraphPipelineStage::Project(GraphProjectStage {
        kind: GraphProjectKind::With,
        items: GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding("source".to_string()),
            alias: Some("source".to_string()),
            projection: GraphReturnProjection::Auto,
        }]),
        distinct: false,
        where_: None,
        order_by: Vec::new(),
        skip: None,
        limit: Some(GraphExpr::UInt(3)),
    });
    let second_match = GraphPipelineStage::Match(GraphPipelineMatchStage {
        optional: false,
        nodes: vec![graph_node("source"), graph_node("target")],
        pieces: vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_LEGACY_HUB",
        )],
        optional_candidate_where: None,
        where_: None,
    });
    let return_stage = GraphPipelineStage::Project(GraphProjectStage {
        kind: GraphProjectKind::Return,
        items: GraphProjectionItems::Items(vec![GraphProjectItem {
            expr: GraphExpr::Binding("target".to_string()),
            alias: Some("target".to_string()),
            projection: GraphReturnProjection::IdOnly,
        }]),
        distinct: false,
        where_: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });
    let pipeline = GraphPipelineQuery {
        stages: vec![first_match, with_limit, second_match, return_stage],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: true,
            ..GraphPipelineOptions::default()
        },
    };

    let mut absent = anchored.clone();
    if let GraphPatternPiece::Edge(edge) = &mut absent.pieces[0] {
        edge.label_filter = vec!["GRAPH_ROW_LEGACY_ABSENT".to_string()];
    }
    let mut reverse = anchored.clone();
    reverse.nodes[0].ids.clear();
    reverse.nodes[1].ids = vec![targets[0]];
    let mut candidate_source = make_hub_query(false);
    candidate_source.nodes[0] = graph_node("source");
    candidate_source.nodes[0].ids = vec![sources[0]];
    if let GraphPatternPiece::Edge(edge) = &mut candidate_source.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    let mut candidate_fallback = candidate_source.clone();
    candidate_fallback.nodes[0] = graph_node("source");
    if let GraphPatternPiece::Edge(edge) = &mut candidate_fallback.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "unindexed_status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    candidate_fallback.options.allow_full_scan = true;

    let ordered_cursor = make_hub_query(true);
    let hub_unordered = make_hub_query(false);
    let hub_ordered = make_hub_query(true);
    let one_stage_pipeline = graph_pipeline_from_row_query(&anchored);
    let gql_seeded = "MATCH (source:GraphRowLegacyCorpusSource) WITH source LIMIT 3 MATCH (source)-[:GRAPH_ROW_LEGACY_HUB]->(target) RETURN id(target) AS target".to_string();
    let gql_exists = "MATCH (source:GraphRowLegacyCorpusSource) WHERE EXISTS { MATCH (source)-[:GRAPH_ROW_LEGACY_HUB]->(target) RETURN target } RETURN id(source) AS source ORDER BY source".to_string();
    let gql_direct = format!(
        "MATCH (source)-[:GRAPH_ROW_LEGACY_LOW]->(middle)-[:GRAPH_ROW_LEGACY_CHAIN]->(target) WHERE id(source) = {} RETURN id(source), id(middle), id(target) ORDER BY id(target)",
        sources[1]
    );

    let mut cases = vec![
        GraphRowLegacyCorpusCase { name: "node_only_no_fixed_edge", family: REQUIRED_MANIFEST[0].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(node_only) },
        GraphRowLegacyCorpusCase { name: "anchored_low_fanout_single_hop", family: REQUIRED_MANIFEST[1].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(anchored.clone()) },
        GraphRowLegacyCorpusCase { name: "unanchored_indexed_single_hop_edge_driver", family: REQUIRED_MANIFEST[2].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(unanchored) },
        GraphRowLegacyCorpusCase { name: "warm_hub_unordered_limit", family: REQUIRED_MANIFEST[3].1, ordered: false, allow_unordered_plan_dependent_rows: true, workload: GraphRowLegacyCorpusWorkload::Graph(hub_unordered) },
        GraphRowLegacyCorpusCase { name: "reopened_hub_ordered_limit", family: REQUIRED_MANIFEST[4].1, ordered: true, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(hub_ordered.clone()) },
        GraphRowLegacyCorpusCase { name: "explicit_source_ids_both", family: REQUIRED_MANIFEST[5].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(explicit_ids_both) },
        GraphRowLegacyCorpusCase { name: "explicit_edge_ids", family: REQUIRED_MANIFEST[6].1, ordered: true, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::ExplicitEdgeIds { query: explicit_edge_query, edge_alias: "edge".to_string(), edge_ids: hub_edge_ids.clone() } },
        GraphRowLegacyCorpusCase { name: "hydrating_two_hop_required", family: REQUIRED_MANIFEST[7].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(hydrating) },
        GraphRowLegacyCorpusCase { name: "branched_required_path", family: REQUIRED_MANIFEST[8].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(branched) },
        GraphRowLegacyCorpusCase { name: "later_required_after_optional_barrier", family: REQUIRED_MANIFEST[9].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(later) },
        GraphRowLegacyCorpusCase { name: "both_endpoints_bound_first", family: REQUIRED_MANIFEST[10].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(both_bound) },
        GraphRowLegacyCorpusCase { name: "uncorrelated_optional", family: REQUIRED_MANIFEST[11].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(uncorrelated_optional) },
        GraphRowLegacyCorpusCase { name: "correlated_optional_repeated_inputs", family: REQUIRED_MANIFEST[12].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(correlated_optional.clone()) },
        GraphRowLegacyCorpusCase { name: "one_hop_vlp", family: REQUIRED_MANIFEST[13].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(one_hop_vlp) },
        GraphRowLegacyCorpusCase { name: "multi_hop_vlp", family: REQUIRED_MANIFEST[14].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(multi_hop_vlp) },
        GraphRowLegacyCorpusCase { name: "native_pipeline_incoming_unordered_limit", family: REQUIRED_MANIFEST[15].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Pipeline(pipeline) },
        GraphRowLegacyCorpusCase { name: "gql_lowered_seeded_match_stage", family: REQUIRED_MANIFEST[16].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Gql(gql_seeded) },
        GraphRowLegacyCorpusCase { name: "ordered_fixed_edge_cursor_walk", family: REQUIRED_MANIFEST[17].1, ordered: true, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(ordered_cursor.clone()) },
        GraphRowLegacyCorpusCase { name: "top_level_explain_execute_parity", family: REQUIRED_MANIFEST[18].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::GraphExplainExecute(anchored.clone()) },
        GraphRowLegacyCorpusCase { name: "optional_static_runtime_parity", family: REQUIRED_MANIFEST[19].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::GraphExplainExecute(correlated_optional) },
        GraphRowLegacyCorpusCase { name: "pipeline_exists_incoming", family: REQUIRED_MANIFEST[20].1, ordered: true, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Gql(gql_exists) },
        GraphRowLegacyCorpusCase { name: "one_stage_pipeline_compatibility", family: REQUIRED_MANIFEST[21].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Pipeline(one_stage_pipeline) },
        GraphRowLegacyCorpusCase { name: "absent_label_empty_source", family: REQUIRED_MANIFEST[22].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(absent) },
        GraphRowLegacyCorpusCase { name: "reverse_anchor_orientation", family: REQUIRED_MANIFEST[23].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(reverse) },
        GraphRowLegacyCorpusCase { name: "bound_edge_candidate_source", family: REQUIRED_MANIFEST[24].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(candidate_source) },
        GraphRowLegacyCorpusCase { name: "gql_direct_fixed_chain", family: REQUIRED_MANIFEST[25].1, ordered: true, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Gql(gql_direct) },
        GraphRowLegacyCorpusCase { name: "edge_property_fallback_source", family: REQUIRED_MANIFEST[26].1, ordered: false, allow_unordered_plan_dependent_rows: false, workload: GraphRowLegacyCorpusWorkload::Graph(candidate_fallback) },
    ];
    assert_eq!(
        cases.iter().map(|case| (case.name, case.family)).collect::<Vec<_>>(),
        REQUIRED_MANIFEST
    );

    let pinned_epoch = engine
        .query_graph_rows(&anchored)
        .unwrap()
        .stats
        .effective_at_epoch;
    for case in &mut cases {
        match &mut case.workload {
            GraphRowLegacyCorpusWorkload::Graph(query) => query.at_epoch = Some(pinned_epoch),
            GraphRowLegacyCorpusWorkload::GraphExplainExecute(query) => {
                query.at_epoch = Some(pinned_epoch)
            }
            GraphRowLegacyCorpusWorkload::Pipeline(query) => query.at_epoch = Some(pinned_epoch),
            GraphRowLegacyCorpusWorkload::Gql(_) => {}
            GraphRowLegacyCorpusWorkload::ExplicitEdgeIds { query, .. } => {
                query.at_epoch = Some(pinned_epoch)
            }
        }
    }
    let mut ordered_cursor = ordered_cursor;
    ordered_cursor.at_epoch = Some(pinned_epoch);
    let mut hub_ordered = hub_ordered;
    hub_ordered.at_epoch = Some(pinned_epoch);

    let legacy_hub_signature = graph_row_legacy_single_edge_signature(
        "Edge(0:alias:edge)",
        "EdgeCandidateSource",
    );
    let current_hub_signature = graph_row_legacy_single_edge_signature(
        "Node(0:source)",
        "EndpointAdjacency",
    );
    let flip_allowlist = [
        GraphRowLegacyFlipAllowlistEntry {
            case_name: "warm_hub_unordered_limit",
            legacy_signatures: legacy_hub_signature.clone(),
            current_signatures: current_hub_signature.clone(),
            causing_decision: "DEC-43b-004/005",
            soundness_rationale: "snapshot-safe aggregate clamp is proof-gated to the distinct top-level first NodeAnchor frontier",
            audit_classification: "old-success/current-success; deterministic unordered plan-dependent LIMIT rows",
        },
        GraphRowLegacyFlipAllowlistEntry {
            case_name: "reopened_hub_ordered_limit",
            legacy_signatures: legacy_hub_signature.clone(),
            current_signatures: current_hub_signature.clone(),
            causing_decision: "DEC-43b-004/005",
            soundness_rationale: "snapshot-safe aggregate clamp is proof-gated to the distinct top-level first NodeAnchor frontier",
            audit_classification: "old-success/current-success; ordered rows/cursors identical; repeat and reopen deterministic",
        },
        GraphRowLegacyFlipAllowlistEntry {
            case_name: "ordered_fixed_edge_cursor_walk",
            legacy_signatures: legacy_hub_signature,
            current_signatures: current_hub_signature,
            causing_decision: "DEC-43b-004/005",
            soundness_rationale: "snapshot-safe aggregate clamp is proof-gated to the distinct top-level first NodeAnchor frontier",
            audit_classification: "old-success/current-success; every ordered row/cursor identical through exhaustion",
        },
    ];
    assert!(flip_allowlist.iter().all(|entry| {
        entry.causing_decision == "DEC-43b-004/005"
            && entry.soundness_rationale.contains("proof-gated")
            && entry.audit_classification.contains("old-success/current-success")
    }));
    let assert_allowlisted_flip =
        |case_name: &str,
         legacy_signatures: &[GraphRowTestPlanSignature],
         current_signatures: &[GraphRowTestPlanSignature]| {
            let entry = flip_allowlist
                .iter()
                .find(|entry| entry.case_name == case_name)
                .unwrap_or_else(|| panic!("unallowlisted signature flip for {case_name}"));
            assert_eq!(legacy_signatures, entry.legacy_signatures);
            assert_eq!(current_signatures, entry.current_signatures);
        };

    let reopened_case = cases
        .iter()
        .find(|case| case.name == "reopened_hub_ordered_limit")
        .unwrap();
    let warm_reopen_observation = graph_row_legacy_corpus_observe(&engine, reopened_case, false);
    assert_eq!(
        graph_row_legacy_corpus_observe(&engine, reopened_case, false),
        warm_reopen_observation,
        "warm current reference for reopen must be deterministic"
    );

    let mut flips = Vec::new();
    for case in &cases {
        if case.name == "reopened_hub_ordered_limit" {
            continue;
        }
        let legacy = graph_row_legacy_corpus_observe(&engine, case, true);
        let legacy_repeat = graph_row_legacy_corpus_observe(&engine, case, true);
        assert_eq!(legacy_repeat, legacy, "{} legacy mode was not deterministic", case.name);
        assert!(!graph_row_test_legacy_estimator_is_enabled());

        let current = graph_row_legacy_corpus_observe(&engine, case, false);
        let current_repeat = graph_row_legacy_corpus_observe(&engine, case, false);
        assert_eq!(current_repeat, current, "{} current mode was not deterministic", case.name);

        if legacy.error.is_none() {
            assert!(current.error.is_none(), "{} succeeded under legacy pricing but newly failed under current pricing: {:?}", case.name, current.error);
        }
        let flipped = legacy.signatures != current.signatures;
        if flipped {
            assert_allowlisted_flip(case.name, &legacy.signatures, &current.signatures);
            flips.push(case.name);
        }
        if !flipped || case.ordered || !case.allow_unordered_plan_dependent_rows {
            assert_eq!(current.rows, legacy.rows, "{} changed rows without an accepted unordered plan-dependent flip", case.name);
            assert_eq!(current.next_cursor, legacy.next_cursor, "{} changed cursor bytes outside the accepted boundary", case.name);
        }
        if case.name == "top_level_explain_execute_parity" {
            for (mode, observation) in [("legacy", &legacy), ("current", &current)] {
                assert_eq!(observation.signatures.len(), 2, "{mode} top-level explain/execute must each plan once");
                assert_eq!(observation.signatures[0], observation.signatures[1]);
            }
        }
        if case.name == "optional_static_runtime_parity" {
            for (mode, observation) in [("legacy", &legacy), ("current", &current)] {
                assert_eq!(observation.signatures.len(), 4, "{mode} optional explain/execute must capture top-level and nested plans");
                assert_eq!(observation.signatures[1], observation.signatures[2], "{mode} static/runtime top-level plans diverged");
                assert_eq!(observation.signatures[0], observation.signatures[3], "{mode} static/runtime optional plans diverged");
            }
        }
    }

    let legacy_walk = graph_row_legacy_corpus_walk(&engine, &ordered_cursor, true);
    let legacy_walk_repeat = graph_row_legacy_corpus_walk(&engine, &ordered_cursor, true);
    let current_walk = graph_row_legacy_corpus_walk(&engine, &ordered_cursor, false);
    let current_walk_repeat = graph_row_legacy_corpus_walk(&engine, &ordered_cursor, false);
    assert_eq!(legacy_walk_repeat, legacy_walk);
    assert_eq!(current_walk_repeat, current_walk);
    assert_eq!(current_walk, legacy_walk, "ordered cursor walks must remain bit-identical");

    engine.flush().unwrap();
    engine.close().unwrap();
    let reopened = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        },
    )
    .unwrap();
    let reopened_legacy = graph_row_legacy_corpus_observe(&reopened, reopened_case, true);
    assert_eq!(
        graph_row_legacy_corpus_observe(&reopened, reopened_case, true),
        reopened_legacy,
        "reopened legacy mode was not deterministic"
    );
    let reopened_current = graph_row_legacy_corpus_observe(&reopened, reopened_case, false);
    assert_eq!(
        graph_row_legacy_corpus_observe(&reopened, reopened_case, false),
        reopened_current,
        "reopened current mode was not deterministic"
    );
    assert!(reopened_legacy.error.is_none());
    assert!(reopened_current.error.is_none());
    assert_ne!(reopened_legacy.signatures, reopened_current.signatures);
    assert_allowlisted_flip(
        reopened_case.name,
        &reopened_legacy.signatures,
        &reopened_current.signatures,
    );
    flips.push(reopened_case.name);
    assert_eq!(reopened_current.rows, reopened_legacy.rows);
    assert_eq!(reopened_current.next_cursor, reopened_legacy.next_cursor);
    assert_eq!(reopened_current.rows, warm_reopen_observation.rows);
    assert_eq!(reopened_current.next_cursor, warm_reopen_observation.next_cursor);
    let reopened_legacy_walk = graph_row_legacy_corpus_walk(&reopened, &hub_ordered, true);
    assert_eq!(
        graph_row_legacy_corpus_walk(&reopened, &hub_ordered, true),
        reopened_legacy_walk
    );
    let reopened_current_walk = graph_row_legacy_corpus_walk(&reopened, &hub_ordered, false);
    assert_eq!(reopened_current_walk, reopened_legacy_walk);
    assert_eq!(
        reopened_current_walk,
        current_walk,
        "current ordered rows and cursor bytes must survive full flush/reopen"
    );
    flips.sort_unstable();
    let mut expected_flips = flip_allowlist
        .iter()
        .map(|entry| entry.case_name)
        .collect::<Vec<_>>();
    expected_flips.sort_unstable();
    assert_eq!(flips, expected_flips, "every and only exact allowlisted flips must occur");
    assert!(!graph_row_test_legacy_estimator_is_enabled());
}

#[test]
fn graph_row_explain_reports_edge_property_sidecar_fallbacks_and_followups() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let index_id;
    let segment_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_graph_row_node(&engine, "ExplainSidecarNode", "sidecar-a", &[]);
        let b = insert_graph_row_node(&engine, "ExplainSidecarNode", "sidecar-b", &[]);
        insert_graph_row_edge(
            &engine,
            a,
            b,
            "GRAPH_ROW_EXPLAIN_SIDECAR",
            &[("status", PropValue::String("hot".to_string()))],
        );
        engine.flush().unwrap();
        let index = engine
            .ensure_edge_property_index("GRAPH_ROW_EXPLAIN_SIDECAR", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);
        index_id = index.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::edge_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_planner_stats_for_segment(&db_path, segment_id);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_edge_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    corrupt_sidecar_header_in_place(&sidecar_path);

    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("e".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_SIDECAR".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding("e", GraphReturnProjection::IdOnly)]);

    let unavailable = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&unavailable, "EdgeLabelIndex");
    assert_graph_row_explain_contains(&unavailable, "MissingReadyIndex");
    assert_graph_row_explain_contains(&unavailable, "secondary-index read followup");
    assert_graph_row_explain_not_contains(&unavailable, "source=EdgePropertyEqualityIndex");

    reopened.shutdown_secondary_index_worker();
    reopened
        .with_runtime_manifest_write(|manifest| {
            let entry = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == index_id)
                .unwrap();
            entry.state = SecondaryIndexState::Failed;
            entry.last_error = Some("forced graph-row explain fallback".to_string());
            Ok(())
        })
        .unwrap();
    reopened.rebuild_secondary_index_catalog().unwrap();
    let failed = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&failed, "EdgeLabelIndex");
    assert_graph_row_explain_contains(&failed, "MissingReadyIndex");
    assert_graph_row_explain_not_contains(&failed, "source=EdgePropertyEqualityIndex");
}

#[test]
fn graph_row_explain_reports_node_candidate_sidecar_followup_without_old_edge_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let index_id;
    let segment_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let source = insert_graph_row_node(
            &engine,
            "ExplainNodeSidecar",
            "node-sidecar-source",
            &[("status", PropValue::String("active".to_string()))],
        );
        let target = insert_graph_row_node(&engine, "ExplainNodeSidecarTarget", "node-sidecar-target", &[]);
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_EXPLAIN_NODE_SIDECAR",
            &[],
        );
        engine.flush().unwrap();
        let index = engine
            .ensure_node_property_index("ExplainNodeSidecar", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);
        index_id = index.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_planner_stats_for_segment(&db_path, segment_id);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    corrupt_sidecar_header_in_place(&sidecar_path);

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_EXPLAIN_NODE_SIDECAR",
        )],
    );
    query.nodes[0] = graph_node_with_label("source", "ExplainNodeSidecar");
    query.nodes[0].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("active".to_string()),
    });
    query.nodes[1] = graph_node_with_label("target", "ExplainNodeSidecarTarget");

    let explain = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "NodeCandidateSource");
    assert_graph_row_explain_contains(&explain, "MissingReadyIndex");
    assert_graph_row_explain_contains(&explain, "node candidate planning recorded");
    assert_graph_row_explain_contains(&explain, "source=EdgeCandidateSource");
    assert_graph_row_explain_not_contains(&explain, "PatternEdgeAnchor");
}

#[test]
fn graph_row_planner_node_anchor_label_cardinality_choice() {
    let (_dir, engine) = graph_row_test_engine();
    let rare = insert_graph_row_node(&engine, "GRAPH_ROW_PLANNER_RARE", "planner-rare", &[]);
    let mut common = Vec::new();
    for index in 0..8 {
        let node = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_PLANNER_COMMON",
            &format!("planner-common-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, node, rare, "GRAPH_ROW_PLANNER_REL", &[]);
        common.push(node);
    }

    let mut query = graph_query(
        &["common", "rare"],
        vec![graph_edge_with_label(
            Some("rel"),
            "common",
            "rare",
            "GRAPH_ROW_PLANNER_REL",
        )],
    );
    query.nodes[0] = graph_node_with_label("common", "GRAPH_ROW_PLANNER_COMMON");
    query.nodes[1] = graph_node_with_label("rare", "GRAPH_ROW_PLANNER_RARE");
    query.return_items = Some(vec![graph_return_binding(
        "common",
        GraphReturnProjection::IdOnly,
    )]);
    query.page.limit = 20;

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=rare");
    assert_graph_row_explain_contains(&explain, "kind=NodeAnchor; segment=0; alias=rare");
    assert_graph_row_explain_contains(&explain, "decision=rejected_by=");
    assert_graph_row_explain_contains(&explain, "direction=Incoming");

    let mut actual = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    actual.sort_unstable();
    common.sort_unstable();
    assert_eq!(actual, common);
}

#[test]
fn graph_row_planner_reverse_direction_from_selective_anchor() {
    let (_dir, engine) = graph_row_test_engine();
    let rare = insert_graph_row_node(&engine, "GRAPH_ROW_REVERSE_RARE", "reverse-rare", &[]);
    let mut common = Vec::new();
    for index in 0..12 {
        let node = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_REVERSE_COMMON",
            &format!("reverse-common-{index}"),
            &[],
        );
        common.push(node);
        insert_graph_row_edge(&engine, node, rare, "GRAPH_ROW_REVERSE_REL", &[]);
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["common", "rare"],
        vec![graph_edge_with_label(
            Some("rel"),
            "common",
            "rare",
            "GRAPH_ROW_REVERSE_REL",
        )],
    );
    query.nodes[0] = graph_node_with_label("common", "GRAPH_ROW_REVERSE_COMMON");
    query.nodes[1] = graph_node_with_label("rare", "GRAPH_ROW_REVERSE_RARE");
    query.return_items = Some(vec![graph_return_binding(
        "common",
        GraphReturnProjection::IdOnly,
    )]);
    query.page.limit = 20;

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=rare");
    assert_graph_row_explain_contains(&explain, "direction=Incoming");

    let mut actual = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    actual.sort_unstable();
    common.sort_unstable();
    assert_eq!(actual, common);
}

#[test]
fn graph_row_planner_incomplete_fanout_keeps_query_order_before_alias_tie_break() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_INCOMPLETE_STATS", "tie-source", &[]);
    let first_target = insert_graph_row_node(&engine, "GRAPH_ROW_INCOMPLETE_STATS", "tie-first", &[]);
    let second_target =
        insert_graph_row_node(&engine, "GRAPH_ROW_INCOMPLETE_STATS", "tie-second", &[]);
    insert_graph_row_edge(&engine, source, first_target, "GRAPH_ROW_INCOMPLETE_FIRST", &[]);
    insert_graph_row_edge(&engine, source, second_target, "GRAPH_ROW_INCOMPLETE_SECOND", &[]);

    let mut query = graph_query(
        &["source", "first", "second"],
        vec![
            graph_edge_with_label(
                Some("z_first_edge"),
                "source",
                "first",
                "GRAPH_ROW_INCOMPLETE_FIRST",
            ),
            graph_edge_with_label(
                Some("a_second_edge"),
                "source",
                "second",
                "GRAPH_ROW_INCOMPLETE_SECOND",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "source",
        GraphReturnProjection::IdOnly,
    )]);
    query.page.limit = 20;

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "missing fanout stats");
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:z_first_edge\", \"alias:a_second_edge\"]",
    );
}

#[test]
fn graph_row_planner_missing_fanout_stats_preserves_deterministic_order() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_MISSING_STATS", "missing-source", &[]);
    let first_target = insert_graph_row_node(&engine, "GRAPH_ROW_MISSING_STATS", "missing-first", &[]);
    let second_target = insert_graph_row_node(&engine, "GRAPH_ROW_MISSING_STATS", "missing-second", &[]);
    insert_graph_row_edge(&engine, source, first_target, "GRAPH_ROW_MISSING_STATS_FIRST", &[]);
    insert_graph_row_edge(&engine, source, second_target, "GRAPH_ROW_MISSING_STATS_SECOND", &[]);

    let mut query = graph_query(
        &["source", "first", "second"],
        vec![
            graph_edge_with_label(
                Some("first_edge"),
                "source",
                "first",
                "GRAPH_ROW_MISSING_STATS_FIRST",
            ),
            graph_edge_with_label(
                Some("second_edge"),
                "source",
                "second",
                "GRAPH_ROW_MISSING_STATS_SECOND",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "source",
        GraphReturnProjection::IdOnly,
    )]);
    query.page.limit = 20;

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "missing fanout stats");
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:first_edge\", \"alias:second_edge\"]",
    );
}

#[test]
fn graph_row_planner_fanout_cost_can_choose_larger_lower_expansion_anchor() {
    let (_dir, engine) = graph_row_test_engine();
    let small = insert_graph_row_node(&engine, "GRAPH_ROW_FANOUT_SMALL", "fanout-small", &[]);
    let bridge_hit = insert_graph_row_node(&engine, "GRAPH_ROW_FANOUT_BRIDGE", "fanout-bridge-hit", &[]);
    insert_graph_row_edge(&engine, small, bridge_hit, "GRAPH_ROW_FANOUT_HIGH", &[]);
    for index in 0..39 {
        let bridge = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_FANOUT_BRIDGE",
            &format!("fanout-bridge-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, small, bridge, "GRAPH_ROW_FANOUT_HIGH", &[]);
    }
    let mut larger = Vec::new();
    for index in 0..5 {
        let node = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_FANOUT_LARGER",
            &format!("fanout-larger-{index}"),
            &[],
        );
        larger.push(node);
        insert_graph_row_edge(&engine, node, bridge_hit, "GRAPH_ROW_FANOUT_LOW", &[]);
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["small", "larger", "bridge"],
        vec![
            graph_edge_with_label(
                Some("high_edge"),
                "small",
                "bridge",
                "GRAPH_ROW_FANOUT_HIGH",
            ),
            graph_edge_with_label(
                Some("low_edge"),
                "larger",
                "bridge",
                "GRAPH_ROW_FANOUT_LOW",
            ),
        ],
    );
    query.nodes[0].ids = vec![small];
    query.nodes[1].ids = larger.clone();
    query.return_items = Some(vec![graph_return_binding(
        "larger",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=larger");
    assert_graph_row_explain_contains(&explain, "kind=NodeAnchor; segment=0; alias=larger");
    let mut actual = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    actual.sort_unstable();
    larger.sort_unstable();
    assert_eq!(actual, larger);
}

#[test]
fn graph_row_planner_required_segments_do_not_cross_optional_or_vlp_barriers() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_BARRIER", "barrier-source", &[]);
    const HIGH_FANOUT_COUNT: usize = 34;
    for index in 0..HIGH_FANOUT_COUNT {
        let high = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BARRIER_HIGH",
            &format!("barrier-high-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, high, "GRAPH_ROW_BARRIER_HIGH_REL", &[]);
    }
    let bridge = insert_graph_row_node(&engine, "GRAPH_ROW_BARRIER_BRIDGE", "barrier-bridge", &[]);
    let low = insert_graph_row_node(&engine, "GRAPH_ROW_BARRIER_LOW", "barrier-low", &[]);
    insert_graph_row_edge(&engine, bridge, low, "GRAPH_ROW_BARRIER_LOW_REL", &[]);
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "high", "opt", "bridge", "low"],
        vec![
            graph_edge_with_label(
                Some("high_edge"),
                "source",
                "high",
                "GRAPH_ROW_BARRIER_HIGH_REL",
            ),
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![graph_edge_with_label(
                    Some("optional_edge"),
                    "high",
                    "opt",
                    "GRAPH_ROW_BARRIER_OPTIONAL_REL",
                )],
                where_: None,
            }),
            graph_vlp(Some("path"), None, "high", "bridge", 1, 2),
            graph_edge_with_label(
                Some("low_edge"),
                "bridge",
                "low",
                "GRAPH_ROW_BARRIER_LOW_REL",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:high_edge\", \"alias:low_edge\"]",
    );
    assert_graph_row_explain_contains(&explain, "RequiredSegmentBarrier");
    assert_graph_row_explain_contains(
        &explain,
        "barriers_before=Optional@piece1|VariableLength@piece2",
    );
    assert_graph_row_explain_contains(&explain, "physical_edge_order=[\"alias:high_edge\"]");
    assert_graph_row_explain_contains(&explain, "physical_edge_order=[\"alias:low_edge\"]");
    assert_graph_row_explain_contains(&explain, "segment-local fanout planning never reorders");
}

#[test]
fn graph_row_fanout_delays_high_hub_expansion() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_HUB", "hub-source", &[]);
    for index in 0..24 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_HUB_TARGET",
            &format!("hub-high-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_HUB_HIGH", &[]);
    }
    let selective = insert_graph_row_node(&engine, "GRAPH_ROW_HUB_TARGET", "hub-selective", &[]);
    insert_graph_row_edge(&engine, source, selective, "GRAPH_ROW_HUB_LOW", &[]);
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "high", "low"],
        vec![
            graph_edge_with_label(Some("high_edge"), "source", "high", "GRAPH_ROW_HUB_HIGH"),
            graph_edge_with_label(Some("low_edge"), "source", "low", "GRAPH_ROW_HUB_LOW"),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "high_edge",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:low_edge\", \"alias:high_edge\"]",
    );
    assert_graph_row_explain_contains(&explain, "hub_risk_rank");
}

#[test]
fn graph_row_planner_target_filter_selectivity_reduces_fanout_cost() {
    let (_dir, engine) = graph_row_test_engine();
    let rare = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_TARGET_SELECTIVE_RARE",
        "target-selective-rare",
        &[("status", PropValue::String("hit".to_string()))],
    );
    let mut sources = Vec::new();
    for index in 0..16 {
        let source = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_TARGET_SELECTIVE_SOURCE",
            &format!("target-selective-source-{index}"),
            &[],
        );
        sources.push(source);
        insert_graph_row_edge(
            &engine,
            source,
            rare,
            "GRAPH_ROW_TARGET_SELECTIVE_REL",
            &[],
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index("GRAPH_ROW_TARGET_SELECTIVE_RARE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_TARGET_SELECTIVE_REL",
        )],
    );
    query.nodes[0] = graph_node_with_label("source", "GRAPH_ROW_TARGET_SELECTIVE_SOURCE");
    query.nodes[1] = graph_node_with_label("target", "GRAPH_ROW_TARGET_SELECTIVE_RARE");
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hit".to_string()),
    });
    query.return_items = Some(vec![graph_return_binding(
        "source",
        GraphReturnProjection::IdOnly,
    )]);
    query.page.limit = 20;

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "initial_driver=NodeAnchor(alias=target");
    assert_graph_row_explain_contains(&explain, "direction=Incoming");
    let mut actual = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    actual.sort_unstable();
    sources.sort_unstable();
    assert_eq!(actual, sources);
}

#[test]
fn graph_row_planner_absent_edge_label_uses_empty_result_source() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_ABSENT_LABEL", "absent-source", &[]);
    let target = insert_graph_row_node(&engine, "GRAPH_ROW_ABSENT_LABEL", "absent-target", &[]);
    insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_PRESENT_LABEL", &[]);
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("missing"),
            "source",
            "target",
            "GRAPH_ROW_NEVER_CREATED_LABEL",
        )],
    );
    query.return_items = Some(vec![graph_return_binding(
        "missing",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "source=EmptyResult");
    assert!(engine.query_graph_rows(&query).unwrap().rows.is_empty());
}

#[test]
fn graph_row_planner_both_bound_constraint_runs_before_unbound_expansion() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_BOTH_BOUND", "both-source", &[]);
    let bound_target = insert_graph_row_node(&engine, "GRAPH_ROW_BOTH_BOUND", "both-target", &[]);
    insert_graph_row_edge(
        &engine,
        source,
        bound_target,
        "GRAPH_ROW_BOTH_BOUND_CONSTRAINT",
        &[],
    );
    let mut wide_edges = Vec::new();
    for index in 0..10 {
        let other = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BOTH_BOUND_OTHER",
            &format!("both-other-{index}"),
            &[],
        );
        wide_edges.push(insert_graph_row_edge(
            &engine,
            source,
            other,
            "GRAPH_ROW_BOTH_BOUND_WIDE",
            &[],
        ));
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "bound", "other"],
        vec![
            graph_edge_with_label(
                Some("wide_edge"),
                "source",
                "other",
                "GRAPH_ROW_BOTH_BOUND_WIDE",
            ),
            graph_edge_with_label(
                Some("bound_edge"),
                "source",
                "bound",
                "GRAPH_ROW_BOTH_BOUND_CONSTRAINT",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.nodes[1].ids = vec![bound_target];
    query.return_items = Some(vec![graph_return_binding(
        "wide_edge",
        GraphReturnProjection::IdOnly,
    )]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:bound_edge\", \"alias:wide_edge\"]",
    );
    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        wide_edges
    );
}

#[test]
fn graph_row_planner_edge_property_equality_materializes_edge_source() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_EDGE_EQ", "edge-eq-source", &[]);
    let mut hit = None;
    for index in 0..18 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_EDGE_EQ_TARGET",
            &format!("edge-eq-target-{index}"),
            &[],
        );
        let value = if index == 7 { "hit" } else { "miss" };
        let edge = insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_EDGE_EQ_REL",
            &[("bucket", PropValue::String(value.to_string()))],
        );
        if index == 7 {
            hit = Some(edge);
        }
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_EDGE_EQ_REL", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("bucket").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EDGE_EQ_REL".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "bucket".to_string(),
                value: PropValue::String("hit".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![hit.unwrap()]);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "GraphRowSourceRead");
    assert_graph_row_explain_contains(explain, "choice=EdgePullChunk");
    assert_graph_row_explain_contains(explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(explain, "GraphRowPreparedEdgeSource");
    assert_graph_row_explain_contains(
        explain,
        "materialized_source=EdgePullDelegatedMetadata{",
    );
    assert_graph_row_explain_contains(explain, "verified_candidates=");
}

#[test]
fn graph_row_explain_standalone_reports_edge_source_choice() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_EXPLAIN_BOUND_EDGE", "bound-source", &[]);
    let hit_target = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_EXPLAIN_BOUND_EDGE_TARGET",
        "bound-hit",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        source,
        hit_target,
        "GRAPH_ROW_EXPLAIN_BOUND_EDGE_REL",
        &[("bucket", PropValue::String("hit".to_string()))],
    );
    for index in 0..17 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_EXPLAIN_BOUND_EDGE_TARGET",
            &format!("bound-miss-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_EXPLAIN_BOUND_EDGE_REL",
            &[("bucket", PropValue::String("miss".to_string()))],
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_EXPLAIN_BOUND_EDGE_REL", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("bucket").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_BOUND_EDGE_REL".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "bucket".to_string(),
                value: PropValue::String("hit".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(&explain, "source=EdgeCandidateSource");
    assert_graph_row_explain_not_contains(&explain, "source=EndpointAdjacency; direction=Outgoing");
}

#[test]
fn graph_row_planner_edge_property_range_materializes_edge_source() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_EDGE_RANGE", "edge-range-source", &[]);
    let mut hit = None;
    for index in 0..18 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_EDGE_RANGE_TARGET",
            &format!("edge-range-target-{index}"),
            &[],
        );
        let value = index as i64;
        let edge = insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_EDGE_RANGE_REL",
            &[("score", PropValue::Int(value))],
        );
        if index == 7 {
            hit = Some(edge);
        }
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_EDGE_RANGE_REL", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EDGE_RANGE_REL".to_string()],
            filter: Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(7))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(7))),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![hit.unwrap()]);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "choice=EdgePullChunk");
    assert_graph_row_explain_contains(explain, "EdgePropertyRangeIndex");
}

#[test]
fn graph_row_adjacency_pull_first_cursor_and_plan_flips_preserve_pages() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..1_000 {
        let source = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_ADJ_SOURCE",
            &format!("adj-source-{index:04}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_ADJ_TARGET",
            &format!("adj-target-{index:04}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_ADJ_REL",
            &[],
        );
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_ADJ_REL",
        )],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.options.max_frontier = 1_024;
    query.options.max_intermediate_bindings = 1_024;
    query.page.limit = 10;

    let (active_first, active_signatures) =
        with_graph_row_test_plan_signature_capture(|| engine.query_graph_rows(&query).unwrap());
    assert!(active_signatures.iter().any(|signature| {
        signature.initial_edge_production.as_deref() == Some("EdgePull")
    }));
    let active_cursor = active_first.next_cursor.clone().unwrap();
    let mut active_second_query = query.clone();
    active_second_query.at_epoch = Some(active_first.stats.effective_at_epoch);
    active_second_query.page.cursor = Some(active_cursor.clone());
    let active_second = engine.query_graph_rows(&active_second_query).unwrap();
    assert_graph_row_explain_contains(
        active_first.plan.as_ref().unwrap(),
        "source=EdgePull{edge=alias:edge}",
    );
    assert_graph_row_explain_contains(
        active_first.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );

    query.at_epoch = Some(active_first.stats.effective_at_epoch);
    engine.flush().unwrap();
    let (flushed_first, flushed_signatures) =
        with_graph_row_test_plan_signature_capture(|| engine.query_graph_rows(&query).unwrap());
    assert!(flushed_signatures.iter().any(|signature| {
        signature.initial_edge_production.as_deref() == Some("AdjacencyPull")
    }));
    assert_eq!(flushed_first.rows, active_first.rows);
    assert_eq!(flushed_first.next_cursor, active_first.next_cursor);
    let flushed_plan = flushed_first.plan.as_ref().unwrap();
    for fragment in [
        "kind=AdjacencyPull",
        "source=AdjacencyPull{edge=alias:edge}",
        "choice=AdjacencyPullChunk",
        "source_order=logical_from_group_asc",
        "proof_boundary=completed_owner_group",
        "early_exit=true",
    ] {
        assert_graph_row_explain_contains(flushed_plan, fragment);
    }

    for (limit, skip) in [(1, 0), (10, 0), (10, 3), (64, 0)] {
        let mut shaped = query.clone();
        shaped.page.limit = limit;
        shaped.page.skip = skip;
        let exhaustive = with_graph_row_test_early_exit_disabled(|| {
            engine.query_graph_rows(&shaped).unwrap()
        });
        let actual = engine.query_graph_rows(&shaped).unwrap();
        graph_row_chunk_early_exit_assert_identity(&actual, &exhaustive);
        assert_graph_row_explain_contains(
            actual.plan.as_ref().unwrap(),
            "source=AdjacencyPull{edge=alias:edge}",
        );
    }

    let mut walk_query = query.clone();
    walk_query.page.limit = 7;
    let mut walked = Vec::new();
    loop {
        let exhaustive = with_graph_row_test_early_exit_disabled(|| {
            engine.query_graph_rows(&walk_query).unwrap()
        });
        let actual = engine.query_graph_rows(&walk_query).unwrap();
        graph_row_chunk_early_exit_assert_identity(&actual, &exhaustive);
        walked.extend(actual.rows.clone());
        match actual.next_cursor.clone() {
            Some(cursor) => walk_query.page.cursor = Some(cursor),
            None => {
                assert_graph_row_explain_contains(
                    actual.plan.as_ref().unwrap(),
                    "early_exit=false",
                );
                break;
            }
        }
    }
    assert_eq!(walked.len(), 1_000);

    engine.reset_query_execution_counters_for_test();
    let (retried, attempt_sizes) = with_graph_row_test_forced_adjacency_retry(2, || {
        engine.query_graph_rows(&query).unwrap()
    });
    assert_eq!(retried.rows, flushed_first.rows);
    assert_eq!(retried.next_cursor, flushed_first.next_cursor);
    assert!(attempt_sizes.len() >= 3, "{attempt_sizes:?}");
    assert!(attempt_sizes[0] > 1, "{attempt_sizes:?}");
    assert_eq!(attempt_sizes[1], attempt_sizes[0] / 2, "{attempt_sizes:?}");
    assert_eq!(
        attempt_sizes[2],
        attempt_sizes[0] - attempt_sizes[1],
        "{attempt_sizes:?}"
    );
    let retried_plan = format!("{:?}", retried.plan.as_ref().unwrap());
    assert!(retried_plan.contains("cap_retries=1"), "{retried_plan}");
    assert!(
        retried_plan.contains(&format!("successful_leaves={}", attempt_sizes.len() - 1)),
        "{retried_plan}"
    );
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(&retried_plan, "source_rows"),
        attempt_sizes[1..].iter().sum::<usize>(),
        "failed original attempts must not duplicate committed source counters"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        1
    );

    engine.reset_query_execution_counters_for_test();
    let sink_error = with_graph_row_test_sink_failure(|| engine.query_graph_rows(&query));
    assert!(sink_error.is_err());
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        0,
        "a failed sink cannot commit an adjacency stop boundary"
    );

    let mut hydrated_query = query.clone();
    hydrated_query.nodes[1] = graph_node_with_label("target", "GRAPH_ROW_ADJ_TARGET");
    engine.reset_query_execution_counters_for_test();
    let hydration_error = with_graph_row_test_node_verification_failure(|| {
        engine.query_graph_rows(&hydrated_query)
    });
    assert!(hydration_error.is_err());
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        0,
        "failed node hydration cannot commit an adjacency stop boundary"
    );

    let gql = "MATCH (source)-[edge:GRAPH_ROW_ADJ_REL]->(target) \
               RETURN id(source), id(edge), id(target) LIMIT 5";
    let gql_first = engine
        .execute_gql(
            gql,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                include_plan: true,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_first.rows.len(), 2);
    let gql_first_plan = format!("{:?}", gql_first.plan.as_ref().unwrap());
    assert!(
        gql_first_plan.contains("source=AdjacencyPull{edge=alias:edge}"),
        "{gql_first_plan}"
    );
    assert!(gql_first_plan.contains("logical_limit=Some(5)"), "{gql_first_plan}");
    let gql_second = engine
        .execute_gql(
            gql,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: gql_first.next_cursor,
                max_rows: 2,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_second.rows.len(), 2);
    let gql_third = engine
        .execute_gql(
            gql,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: gql_second.next_cursor,
                max_rows: 2,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_third.rows.len(), 1);
    assert!(gql_third.next_cursor.is_none());

    let mut adjacency_cursor_query = query.clone();
    adjacency_cursor_query.at_epoch = Some(active_first.stats.effective_at_epoch);
    adjacency_cursor_query.page.cursor = Some(active_cursor);
    let flushed_second = engine.query_graph_rows(&adjacency_cursor_query).unwrap();
    assert_eq!(flushed_second.rows, active_second.rows);
    assert_eq!(flushed_second.next_cursor, active_second.next_cursor);
    let second_plan = flushed_second.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(second_plan, "source=AdjacencyPull{edge=alias:edge}");
    assert_graph_row_explain_contains(second_plan, "cursor_seek=anchor_ge:");
    assert_graph_row_explain_contains(second_plan, "cursor_seek_units=");

    let fallback = with_graph_row_test_adjacency_prepare_failure(|| {
        engine.query_graph_rows(&adjacency_cursor_query).unwrap()
    });
    assert_eq!(fallback.rows, flushed_second.rows);
    assert_eq!(fallback.next_cursor, flushed_second.next_cursor);
    let fallback_plan = fallback.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(fallback_plan, "source=EdgePull{edge=alias:edge}");
    assert_graph_row_explain_contains(fallback_plan, "fallback=adjacency_prepare_error");
    assert_graph_row_explain_contains(fallback_plan, "choice=EdgePullChunk");
    assert_graph_row_explain_not_contains(fallback_plan, "source_order=logical_from_group_asc");

    let extra_source = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_ADJ_SOURCE",
        "adj-source-extra",
        &[],
    );
    let extra_target = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_ADJ_TARGET",
        "adj-target-extra",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        extra_source,
        extra_target,
        "GRAPH_ROW_ADJ_REL",
        &[],
    );
    let edge_pull_second = engine.query_graph_rows(&adjacency_cursor_query).unwrap();
    assert_eq!(edge_pull_second.rows, flushed_second.rows);
    assert_eq!(edge_pull_second.next_cursor, flushed_second.next_cursor);
    assert_graph_row_explain_contains(
        edge_pull_second.plan.as_ref().unwrap(),
        "source=EdgePull{edge=alias:edge}",
    );
}

#[test]
fn graph_row_adjacency_pull_pinned_cursor_walk_crosses_every_physical_state_and_source() {
    let (dir, engine) = graph_row_test_engine();
    let mut edge_ids = Vec::new();
    for index in 0..128u64 {
        let source = insert_graph_row_node(
            &engine,
            "AdjFlipSource",
            &format!("adj-flip-source-{index:03}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "AdjFlipTarget",
            &format!("adj-flip-target-{index:03}"),
            &[],
        );
        edge_ids.push(insert_graph_row_edge(
            &engine,
            source,
            target,
            "ADJ_FLIP_EDGE",
            &[],
        ));
    }

    let mut query = graph_row_adjacency_selected_edge_query(
        &["ADJ_FLIP_EDGE"],
        Direction::Outgoing,
        5,
    );
    let warm = engine.query_graph_rows(&query).unwrap();
    query.at_epoch = Some(warm.stats.effective_at_epoch);
    let active = engine.query_graph_rows(&query).unwrap();
    let active_oracle = graph_row_chunk_runtime_once_result(&engine, &query);
    graph_row_chunk_assert_page_parity(&active, &active_oracle, "active/RuntimeOnce");
    assert_graph_row_explain_contains(active.plan.as_ref().unwrap(), "source=EdgePull{");
    assert_graph_row_explain_contains(
        active.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );
    let fingerprint = active.plan.as_ref().unwrap().fingerprint.clone();
    let effective_epoch = active.stats.effective_at_epoch;
    let mut walked = graph_row_adjacency_selected_edges(&active)
        .iter()
        .map(|edge| edge.id.unwrap())
        .collect::<Vec<_>>();
    query.page.cursor = active.next_cursor.clone();

    engine.freeze_memtable().unwrap();
    let (_, immutable_adjacency, immutable_edge, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    assert_eq!(
        immutable_adjacency.plan.as_ref().unwrap().fingerprint,
        fingerprint
    );
    assert_eq!(immutable_adjacency.stats.effective_at_epoch, effective_epoch);
    assert_eq!(immutable_adjacency.next_cursor, immutable_edge.next_cursor);
    walked.extend(
        graph_row_adjacency_selected_edges(&immutable_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap()),
    );
    query.page.cursor = immutable_adjacency.next_cursor.clone();

    engine.flush().unwrap();
    let (_, flushed_adjacency, flushed_edge, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    assert_eq!(flushed_edge.plan.as_ref().unwrap().fingerprint, fingerprint);
    assert_eq!(flushed_edge.stats.effective_at_epoch, effective_epoch);
    assert_eq!(flushed_edge.next_cursor, flushed_adjacency.next_cursor);
    walked.extend(
        graph_row_adjacency_selected_edges(&flushed_edge)
            .iter()
            .map(|edge| edge.id.unwrap()),
    );
    query.page.cursor = flushed_edge.next_cursor.clone();

    let late_source = insert_graph_row_node(&engine, "AdjFlipLate", "adj-flip-late-source", &[]);
    let late_target = insert_graph_row_node(&engine, "AdjFlipLate", "adj-flip-late-target", &[]);
    insert_graph_row_edge(
        &engine,
        late_source,
        late_target,
        "ADJ_FLIP_EDGE",
        &[],
    );
    engine.flush().unwrap();
    let (_, layered_adjacency, layered_edge, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    assert_eq!(
        layered_adjacency.plan.as_ref().unwrap().fingerprint,
        fingerprint
    );
    assert_eq!(layered_adjacency.stats.effective_at_epoch, effective_epoch);
    assert_eq!(layered_adjacency.next_cursor, layered_edge.next_cursor);
    walked.extend(
        graph_row_adjacency_selected_edges(&layered_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap()),
    );
    query.page.cursor = layered_adjacency.next_cursor.clone();

    engine.compact().unwrap();
    let (_, compacted_adjacency, compacted_edge, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    assert_eq!(compacted_edge.plan.as_ref().unwrap().fingerprint, fingerprint);
    assert_eq!(compacted_edge.stats.effective_at_epoch, effective_epoch);
    assert_eq!(compacted_edge.next_cursor, compacted_adjacency.next_cursor);
    walked.extend(
        graph_row_adjacency_selected_edges(&compacted_edge)
            .iter()
            .map(|edge| edge.id.unwrap()),
    );
    query.page.cursor = compacted_edge.next_cursor.clone();

    drop(engine);
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    let (_, reopened_adjacency, reopened_edge, _) =
        graph_row_adjacency_forced_differential(&reopened, &query);
    assert_eq!(
        reopened_adjacency.plan.as_ref().unwrap().fingerprint,
        fingerprint
    );
    assert_eq!(reopened_adjacency.stats.effective_at_epoch, effective_epoch);
    assert_eq!(reopened_adjacency.next_cursor, reopened_edge.next_cursor);
    walked.extend(
        graph_row_adjacency_selected_edges(&reopened_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap()),
    );

    assert_eq!(walked, edge_ids[..walked.len()]);
}

#[test]
fn graph_row_adjacency_pull_partial_owner_cannot_reuse_stale_boundary() {
    let (_dir, engine) = graph_row_test_engine();
    let mut giant_owner = None;
    let mut giant_owner_edges = Vec::new();
    for owner_index in 0..800 {
        let source = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_ADJ_BOUNDARY_SOURCE",
            &format!("adj-boundary-source-{owner_index:04}"),
            &[],
        );
        let fanout = if owner_index == 1 { 20 } else { 1 };
        if owner_index == 1 {
            giant_owner = Some(source);
        }
        for target_index in 0..fanout {
            let target = insert_graph_row_node(
                &engine,
                "GRAPH_ROW_ADJ_BOUNDARY_TARGET",
                &format!("adj-boundary-target-{owner_index:04}-{target_index:02}"),
                &[],
            );
            let edge_id = insert_graph_row_edge(
                &engine,
                source,
                target,
                "GRAPH_ROW_ADJ_BOUNDARY_REL",
                &[],
            );
            if owner_index == 1 {
                giant_owner_edges.push(edge_id);
            }
        }
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "GRAPH_ROW_ADJ_BOUNDARY_REL",
        )],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.options.max_frontier = 1_024;
    query.options.max_intermediate_bindings = 1_024;
    query.page.limit = 3;

    let initial = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&query).unwrap()
    });
    query.at_epoch = Some(initial.stats.effective_at_epoch);
    let exhaustive = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&query).unwrap()
    });
    let (actual, pull_trace) = with_graph_row_test_adjacency_pull_trace(|| {
        engine.query_graph_rows(&query).unwrap()
    });
    graph_row_chunk_early_exit_assert_identity(&actual, &exhaustive);
    assert_eq!(pull_trace.len(), 3, "{pull_trace:?}");
    assert_eq!(
        pull_trace[0],
        GraphRowTestAdjacencyPullTrace {
            oriented_metadata: 2,
            physical_scan_units: 4,
            raw_postings_scanned: 2,
            pending_completed_through_owner: None,
            pending_last_completed_owner: Some(1),
            has_more: true,
            partial_owner_pull: true,
        }
    );
    assert_eq!(
        pull_trace[1],
        GraphRowTestAdjacencyPullTrace {
            oriented_metadata: 8,
            physical_scan_units: 8,
            raw_postings_scanned: 8,
            pending_completed_through_owner: None,
            pending_last_completed_owner: None,
            has_more: true,
            partial_owner_pull: true,
        }
    );
    assert_eq!(
        pull_trace[2].pending_completed_through_owner,
        giant_owner
    );
    assert_eq!(pull_trace[2].pending_last_completed_owner, giant_owner);
    assert!(pull_trace[2].has_more);
    assert!(!pull_trace[2].partial_owner_pull);
    let plan = format!("{:?}", actual.plan.as_ref().unwrap());
    assert!(plan.contains("source=AdjacencyPull{edge=alias:edge}"), "{plan}");
    assert!(plan.contains("early_exit=true"), "{plan}");
    assert!(
        graph_row_chunk_cursor_seek_runtime_metric(&plan, "source_pulls") > 1,
        "the partial giant owner must force a later original pull: {plan}"
    );
    assert!(
        graph_row_chunk_cursor_seek_runtime_metric(&plan, "partial_owner_pulls") > 0,
        "{plan}"
    );
    assert!(
        plan.contains(&format!(
            "last_completed_owner={}",
            giant_owner.expect("giant owner")
        )),
        "the later pull must commit its own giant-owner boundary: {plan}"
    );

    for edge_id in [
        giant_owner_edges[0],
        giant_owner_edges[giant_owner_edges.len() / 2],
        *giant_owner_edges.last().unwrap(),
    ] {
        let (retried, attempt_sizes) = with_graph_row_test_forced_adjacency_retry_edge(
            edge_id,
            || engine.query_graph_rows(&query).unwrap(),
        );
        graph_row_chunk_early_exit_assert_identity(&retried, &actual);
        assert!(attempt_sizes.first().is_some_and(|size| *size > 1));
        assert!(attempt_sizes.contains(&1), "{attempt_sizes:?}");
        let retried_plan = format!("{:?}", retried.plan.as_ref().unwrap());
        assert!(
            graph_row_chunk_cursor_seek_runtime_metric(&retried_plan, "cap_retries") > 0,
            "{retried_plan}"
        );
        assert_eq!(
            graph_row_chunk_cursor_seek_runtime_metric(&retried_plan, "source_rows"),
            graph_row_chunk_cursor_seek_runtime_metric(&plan, "source_rows"),
            "retry failures must not duplicate committed original-pull counters"
        );
    }

    for limit in [1, 2, 10, 21] {
        let mut first_query = query.clone();
        first_query.page.limit = limit;
        let first = engine.query_graph_rows(&first_query).unwrap();
        let mut cursor_query = first_query;
        cursor_query.page.cursor = first.next_cursor;
        let oracle = with_graph_row_test_cursor_seek_disabled(|| {
            engine.query_graph_rows(&cursor_query).unwrap()
        });
        let cursor_page = engine.query_graph_rows(&cursor_query).unwrap();
        graph_row_chunk_early_exit_assert_identity(&cursor_page, &oracle);
        let cursor_plan = format!("{:?}", cursor_page.plan.as_ref().unwrap());
        assert!(
            cursor_plan.contains("source=AdjacencyPull{edge=alias:edge}"),
            "limit={limit}: {cursor_plan}"
        );
        assert!(
            cursor_plan.contains("cursor_seek=anchor_ge:"),
            "limit={limit}: {cursor_plan}"
        );
    }
}

fn graph_row_adjacency_forced_differential(
    engine: &DatabaseEngine,
    query: &GraphRowQuery,
) -> (GraphRowResult, GraphRowResult, GraphRowResult, GraphRowResult) {
    let first = engine.query_graph_rows(query).unwrap();
    let mut pinned = query.clone();
    pinned.at_epoch = Some(first.stats.effective_at_epoch);
    pinned.options.include_plan = true;

    let automatic = engine.query_graph_rows(&pinned).unwrap();
    let adjacency = with_graph_row_test_forced_edge_production(
        GraphRowEdgeAnchorProduction::AdjacencyPull,
        || engine.query_graph_rows(&pinned).unwrap(),
    );
    let edge = with_graph_row_test_forced_edge_production(
        GraphRowEdgeAnchorProduction::EdgePull,
        || engine.query_graph_rows(&pinned).unwrap(),
    );
    let runtime_once = graph_row_chunk_runtime_once_result(engine, &pinned);

    graph_row_chunk_assert_page_parity(&automatic, &runtime_once, "automatic/RuntimeOnce");
    graph_row_chunk_assert_page_parity(&adjacency, &runtime_once, "AdjacencyPull/RuntimeOnce");
    graph_row_chunk_assert_page_parity(&edge, &runtime_once, "EdgePull/RuntimeOnce");
    assert_graph_row_explain_contains(
        adjacency.plan.as_ref().unwrap(),
        "source=AdjacencyPull{",
    );
    assert_graph_row_explain_contains(edge.plan.as_ref().unwrap(), "source=EdgePull{");

    (automatic, adjacency, edge, runtime_once)
}

fn graph_row_adjacency_selected_edges(result: &GraphRowResult) -> Vec<GraphEdgeValue> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [GraphValue::Edge(edge)] => edge.clone(),
            values => panic!("expected one selected edge value, got {values:?}"),
        })
        .collect()
}

fn graph_row_adjacency_selected_edge_query(
    labels: &[&str],
    direction: Direction,
    limit: usize,
) -> GraphRowQuery {
    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction,
            label_filter: labels.iter().map(|label| (*label).to_string()).collect(),
            filter: None,
        })],
    );
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::Selected(GraphSelectedProjection::Edge(selected_edge(
            GraphPropertySelection::All,
        ))),
    )]);
    query.output.mode = GraphOutputMode::Projected;
    query.page.limit = limit;
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query
}

#[test]
fn graph_row_adjacency_pull_forced_sources_prove_same_id_layers_compaction_and_reopen() {
    let (dir, engine) = graph_row_test_engine();
    let smaller = insert_graph_row_node(&engine, "AdjPublicNode", "same-id-smaller", &[]);
    let target_a = insert_graph_row_node(&engine, "AdjPublicNode", "same-id-target-a", &[]);
    let original = insert_graph_row_node(&engine, "AdjPublicNode", "same-id-original", &[]);
    let target_b = insert_graph_row_node(&engine, "AdjPublicNode", "same-id-target-b", &[]);
    let larger = insert_graph_row_node(&engine, "AdjPublicNode", "same-id-larger", &[]);
    engine.flush().unwrap();

    let old_label = engine.ensure_edge_label("ADJ_PUBLIC_OLD").unwrap();
    let new_label = engine.ensure_edge_label("ADJ_PUBLIC_NEW").unwrap();
    for index in 0..64u64 {
        let source = insert_graph_row_node(
            &engine,
            "AdjPublicFiller",
            &format!("same-id-filler-source-{index:03}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "AdjPublicFiller",
            &format!("same-id-filler-target-{index:03}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "ADJ_PUBLIC_NEW", &[]);
    }
    let fixed_id = 9_440_005;
    let mut old = prepared_adjacency_test_edge(fixed_id, original, target_a, old_label);
    old.props.insert("version".to_string(), PropValue::Int(1));
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(old)).unwrap();
    engine.flush().unwrap();

    let query = graph_row_adjacency_selected_edge_query(
        &["ADJ_PUBLIC_OLD", "ADJ_PUBLIC_NEW"],
        Direction::Outgoing,
        7,
    );
    let old_only = graph_row_adjacency_selected_edge_query(
        &["ADJ_PUBLIC_OLD"],
        Direction::Outgoing,
        128,
    );
    let new_only = graph_row_adjacency_selected_edge_query(
        &["ADJ_PUBLIC_NEW"],
        Direction::Outgoing,
        128,
    );
    let (_, segment_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    assert!(graph_row_adjacency_selected_edges(&segment_adjacency)
        .iter()
        .any(|edge| edge.id == Some(fixed_id) && edge.from == Some(original)));
    let (_, segment_old, _, _) = graph_row_adjacency_forced_differential(&engine, &old_only);
    let (_, segment_new, _, _) = graph_row_adjacency_forced_differential(&engine, &new_only);
    assert_eq!(
        graph_row_adjacency_selected_edges(&segment_old)
            .iter()
            .filter(|edge| edge.id == Some(fixed_id))
            .count(),
        1
    );
    assert!(!graph_row_adjacency_selected_edges(&segment_new)
        .iter()
        .any(|edge| edge.id == Some(fixed_id)));

    let mut moved_smaller =
        prepared_adjacency_test_edge(fixed_id, smaller, target_b, new_label);
    moved_smaller.updated_at = 2;
    moved_smaller.weight = 2.0;
    moved_smaller
        .props
        .insert("version".to_string(), PropValue::Int(2));
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(moved_smaller)).unwrap();
    let active_warm = engine.query_graph_rows(&query).unwrap();
    let mut active_query = query.clone();
    active_query.at_epoch = Some(active_warm.stats.effective_at_epoch);
    let active = engine.query_graph_rows(&active_query).unwrap();
    assert_graph_row_explain_contains(active.plan.as_ref().unwrap(), "source=EdgePull{");
    assert_graph_row_explain_contains(active.plan.as_ref().unwrap(), "mutable_active_memtable");
    assert!(graph_row_adjacency_selected_edges(&active)
        .iter()
        .any(|edge| edge.id == Some(fixed_id) && edge.from == Some(smaller)));
    let forced_active = with_graph_row_test_forced_edge_production(
        GraphRowEdgeAnchorProduction::AdjacencyPull,
        || engine.query_graph_rows(&active_query).unwrap(),
    );
    assert_eq!(forced_active.rows, active.rows);
    assert_eq!(forced_active.next_cursor, active.next_cursor);
    assert_graph_row_explain_contains(forced_active.plan.as_ref().unwrap(), "source=EdgePull{");
    assert_graph_row_explain_contains(
        forced_active.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );

    engine.freeze_memtable().unwrap();
    let (_, immutable_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    let (_, immutable_old, _, _) =
        graph_row_adjacency_forced_differential(&engine, &old_only);
    let (_, immutable_new, _, _) =
        graph_row_adjacency_forced_differential(&engine, &new_only);
    assert!(!graph_row_adjacency_selected_edges(&immutable_old)
        .iter()
        .any(|edge| edge.id == Some(fixed_id)));
    assert_eq!(
        graph_row_adjacency_selected_edges(&immutable_new)
            .iter()
            .filter(|edge| edge.id == Some(fixed_id))
            .count(),
        1
    );
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(
            &graph_row_explain_text(immutable_old.plan.as_ref().unwrap()),
            "shadowed_or_stale_postings",
        ),
        1,
        "the old-label posting must be visited and rejected as stale"
    );
    let immutable_edges = graph_row_adjacency_selected_edges(&immutable_adjacency);
    assert!(immutable_edges.iter().any(|edge| {
        edge.id == Some(fixed_id)
            && edge.from == Some(smaller)
            && edge.to == Some(target_b)
            && edge.label.as_deref() == Some("ADJ_PUBLIC_NEW")
            && edge.weight == Some(2.0)
    }));
    assert!(graph_row_explain_text(immutable_adjacency.plan.as_ref().unwrap())
        .contains("shadowed_or_stale_postings="));

    let mut moved_larger = prepared_adjacency_test_edge(fixed_id, larger, target_a, new_label);
    moved_larger.updated_at = 3;
    moved_larger.weight = 3.0;
    moved_larger
        .props
        .insert("version".to_string(), PropValue::Int(3));
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(moved_larger)).unwrap();
    let active_larger = engine.query_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        active_larger.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );
    engine.freeze_memtable().unwrap();
    let (_, layered_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    let layered_edges = graph_row_adjacency_selected_edges(&layered_adjacency);
    assert!(layered_edges.iter().any(|edge| {
        edge.id == Some(fixed_id)
            && edge.from == Some(larger)
            && edge.to == Some(target_a)
            && edge.props.as_ref().is_some_and(|props| {
                props.get("version") == Some(&GraphValue::Int(3))
            })
    }));
    assert!(!layered_edges
        .iter()
        .any(|edge| edge.id == Some(fixed_id) && edge.from == Some(smaller)));

    let mut metadata_only = prepared_adjacency_test_edge(fixed_id, larger, target_a, new_label);
    metadata_only.updated_at = 4;
    metadata_only.weight = 4.0;
    metadata_only
        .props
        .insert("version".to_string(), PropValue::Int(4));
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(metadata_only)).unwrap();
    let metadata_active = engine.query_graph_rows(&new_only).unwrap();
    assert_graph_row_explain_contains(
        metadata_active.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );
    engine.freeze_memtable().unwrap();
    let (_, metadata_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &new_only);
    let metadata_edges = graph_row_adjacency_selected_edges(&metadata_adjacency);
    let metadata_edge = metadata_edges
        .iter()
        .find(|edge| edge.id == Some(fixed_id))
        .expect("metadata-only update must remain visible");
    assert_eq!(metadata_edge.from, Some(larger));
    assert_eq!(metadata_edge.to, Some(target_a));
    assert_eq!(metadata_edge.label.as_deref(), Some("ADJ_PUBLIC_NEW"));
    assert_eq!(metadata_edge.weight, Some(4.0));
    assert!(metadata_edge.props.as_ref().is_some_and(|props| {
        props.get("version") == Some(&GraphValue::Int(4))
    }));

    write_internal_wal_op(
        &engine,
        &WalOp::DeleteEdge {
            id: fixed_id,
            deleted_at: 5,
        },
    )
    .unwrap();
    let tombstone_active = engine.query_graph_rows(&query).unwrap();
    assert!(!graph_row_adjacency_selected_edges(&tombstone_active)
        .iter()
        .any(|edge| edge.id == Some(fixed_id)));
    assert_graph_row_explain_contains(
        tombstone_active.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );
    engine.freeze_memtable().unwrap();
    let (_, tombstone_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &query);
    assert!(!graph_row_adjacency_selected_edges(&tombstone_adjacency)
        .iter()
        .any(|edge| edge.id == Some(fixed_id)));

    let mut resurrected =
        prepared_adjacency_test_edge(fixed_id, original, target_b, old_label);
    resurrected.updated_at = 6;
    resurrected.weight = 6.0;
    resurrected
        .props
        .insert("version".to_string(), PropValue::Int(6));
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(resurrected)).unwrap();
    let resurrection_active = engine.query_graph_rows(&query).unwrap();
    let pinned_epoch = resurrection_active.stats.effective_at_epoch;
    assert_graph_row_explain_contains(
        resurrection_active.plan.as_ref().unwrap(),
        "mutable_active_memtable",
    );
    engine.freeze_memtable().unwrap();

    let mut pinned = query.clone();
    pinned.at_epoch = Some(pinned_epoch);
    let (_, final_immutable, _, _) =
        graph_row_adjacency_forced_differential(&engine, &pinned);
    let final_rows = final_immutable.rows.clone();
    let final_cursor = final_immutable.next_cursor.clone();
    let final_edges = graph_row_adjacency_selected_edges(&final_immutable);
    assert!(final_edges.iter().any(|edge| {
        edge.id == Some(fixed_id)
            && edge.from == Some(original)
            && edge.to == Some(target_b)
            && edge.label.as_deref() == Some("ADJ_PUBLIC_OLD")
            && edge.weight == Some(6.0)
    }));

    engine.flush().unwrap();
    let (_, flushed, _, _) = graph_row_adjacency_forced_differential(&engine, &pinned);
    assert_eq!(flushed.rows, final_rows);
    assert_eq!(flushed.next_cursor, final_cursor);
    engine.compact().unwrap();
    let (_, compacted, _, _) = graph_row_adjacency_forced_differential(&engine, &pinned);
    assert_eq!(compacted.rows, final_rows);
    assert_eq!(compacted.next_cursor, final_cursor);

    drop(engine);
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    let (_, reopened_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&reopened, &pinned);
    assert_eq!(reopened_adjacency.rows, final_rows);
    assert_eq!(reopened_adjacency.next_cursor, final_cursor);
}

#[test]
fn graph_row_adjacency_pull_public_orientation_filter_endpoint_and_policy_matrix() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(
        &engine,
        "AdjMatrixSource",
        "matrix-a",
        &[("kind", PropValue::String("keep".to_string()))],
    );
    let b = insert_graph_row_node(
        &engine,
        "AdjMatrixTarget",
        "matrix-b",
        &[("kind", PropValue::String("keep".to_string()))],
    );
    let c = engine
        .upsert_node(
            "AdjMatrixTarget",
            "matrix-c",
            UpsertNodeOptions {
                props: graph_row_props(&[(
                    "kind",
                    PropValue::String("drop".to_string()),
                )]),
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    let first = engine
        .upsert_edge(
            a,
            b,
            "ADJ_MATRIX_ONE",
            UpsertEdgeOptions {
                props: graph_row_props(&[("score", PropValue::Int(7))]),
                valid_from: Some(10),
                valid_to: Some(20),
                weight: 1.0,
            },
        )
        .unwrap();
    let second = engine
        .upsert_edge(
            b,
            a,
            "ADJ_MATRIX_TWO",
            UpsertEdgeOptions {
                props: graph_row_props(&[("score", PropValue::Int(9))]),
                valid_from: Some(10),
                valid_to: Some(20),
                weight: 1.0,
            },
        )
        .unwrap();
    let loop_edge = engine
        .upsert_edge(
            a,
            a,
            "ADJ_MATRIX_TWO",
            UpsertEdgeOptions {
                props: graph_row_props(&[("score", PropValue::Int(11))]),
                valid_from: Some(10),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    let rejected = engine
        .upsert_edge(
            a,
            c,
            "ADJ_MATRIX_ONE",
            UpsertEdgeOptions {
                props: graph_row_props(&[("score", PropValue::Int(1))]),
                valid_from: Some(10),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    let missing_edge_id = 9_440_006;
    let matrix_one_label = engine.ensure_edge_label("ADJ_MATRIX_ONE").unwrap();
    let mut missing_endpoint =
        prepared_adjacency_test_edge(missing_edge_id, a, 9_999_999, matrix_one_label);
    missing_endpoint.valid_from = 10;
    missing_endpoint.valid_to = 20;
    missing_endpoint
        .props
        .insert("score".to_string(), PropValue::Int(7));
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(missing_endpoint)).unwrap();
    engine.flush().unwrap();

    let mut both = graph_row_adjacency_selected_edge_query(
        &["ADJ_MATRIX_TWO", "ADJ_MATRIX_ONE", "ADJ_MATRIX_TWO"],
        Direction::Both,
        20,
    );
    both.at_epoch = Some(15);
    let (_, both_adjacency, _, _) = graph_row_adjacency_forced_differential(&engine, &both);
    let both_ids = graph_row_adjacency_selected_edges(&both_adjacency)
        .iter()
        .map(|edge| edge.id.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        both_ids,
        vec![loop_edge, first, second, rejected, first, second, rejected]
    );
    assert!(!both_ids.contains(&missing_edge_id));

    for (direction, expected) in [
        (
            Direction::Outgoing,
            vec![loop_edge, first, rejected, second],
        ),
        (
            Direction::Incoming,
            vec![loop_edge, second, first, rejected],
        ),
    ] {
        let mut directed = both.clone();
        let GraphPatternPiece::Edge(edge) = &mut directed.pieces[0] else {
            unreachable!()
        };
        edge.direction = direction;
        let (_, adjacency, _, _) =
            graph_row_adjacency_forced_differential(&engine, &directed);
        assert_eq!(
            graph_row_adjacency_selected_edges(&adjacency)
                .iter()
                .map(|edge| edge.id.unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }

    let mut equality = both.clone();
    let GraphPatternPiece::Edge(edge) = &mut equality.pieces[0] else {
        unreachable!()
    };
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "score".to_string(),
        value: PropValue::Int(7),
    });
    let (_, equality_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &equality);
    assert_eq!(
        graph_row_adjacency_selected_edges(&equality_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![first, first]
    );

    let mut range = both.clone();
    let GraphPatternPiece::Edge(edge) = &mut range.pieces[0] else {
        unreachable!()
    };
    edge.filter = Some(EdgeFilterExpr::PropertyRange {
        key: "score".to_string(),
        lower: Some(PropertyRangeBound::Included(PropValue::Int(8))),
        upper: Some(PropertyRangeBound::Included(PropValue::Int(10))),
    });
    let (_, range_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &range);
    assert_eq!(
        graph_row_adjacency_selected_edges(&range_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![second, second]
    );

    let mut residual = both.clone();
    residual.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("edge", "score")),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Int(7)),
    });
    let (_, residual_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &residual);
    assert_eq!(
        graph_row_adjacency_selected_edges(&residual_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![loop_edge, second, second]
    );

    for (epoch_ms, expected) in [
        (9, Vec::new()),
        (
            10,
            vec![loop_edge, first, second, rejected, first, second, rejected],
        ),
        (20, Vec::new()),
    ] {
        let mut temporal = both.clone();
        let GraphPatternPiece::Edge(edge) = &mut temporal.pieces[0] else {
            unreachable!()
        };
        edge.filter = Some(EdgeFilterExpr::ValidAt { epoch_ms });
        let temporal_adjacency = engine.query_graph_rows(&temporal).unwrap();
        let runtime_once = graph_row_chunk_runtime_once_result(&engine, &temporal);
        graph_row_chunk_assert_page_parity(
            &temporal_adjacency,
            &runtime_once,
            "temporal boundary/RuntimeOnce",
        );
        assert_eq!(
            graph_row_adjacency_selected_edges(&temporal_adjacency)
                .iter()
                .map(|edge| edge.id.unwrap())
                .collect::<Vec<_>>(),
            expected,
            "valid_at={epoch_ms}"
        );
    }

    let mut endpoint = both.clone();
    endpoint.nodes[1] = graph_node_with_label("target", "AdjMatrixTarget");
    endpoint.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    let (_, endpoint_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &endpoint);
    assert_eq!(
        graph_row_adjacency_selected_edges(&endpoint_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![first, second]
    );

    engine
        .set_prune_policy(
            "adj-matrix-node-policy",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("AdjMatrixTarget".to_string()),
            },
        )
        .unwrap();
    let (_, policy_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &both);
    assert_eq!(
        graph_row_adjacency_selected_edges(&policy_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![loop_edge, first, second, first, second]
    );
    engine.remove_prune_policy("adj-matrix-node-policy").unwrap();

    engine.delete_node(c).unwrap();
    engine.flush().unwrap();
    let (_, deleted_endpoint, _, _) =
        graph_row_adjacency_forced_differential(&engine, &both);
    assert_eq!(
        graph_row_adjacency_selected_edges(&deleted_endpoint)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![loop_edge, first, second, first, second]
    );
    assert!(!graph_row_adjacency_selected_edges(&deleted_endpoint)
        .iter()
        .any(|edge| edge.id == Some(missing_edge_id)));

    let mut successful_zero = equality;
    let GraphPatternPiece::Edge(edge) = &mut successful_zero.pieces[0] else {
        unreachable!()
    };
    edge.direction = Direction::Outgoing;
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "score".to_string(),
        value: PropValue::Int(9),
    });
    successful_zero.page.limit = 1;
    let (_, zero_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&engine, &successful_zero);
    let zero_plan = zero_adjacency.plan.as_ref().unwrap();
    assert_eq!(
        graph_row_adjacency_selected_edges(&zero_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        vec![second]
    );
    let zero_text = graph_row_explain_text(zero_plan);
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(&zero_text, "completed_owner_groups"),
        2
    );
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(&zero_text, "last_completed_owner"),
        b as usize
    );
    assert_graph_row_explain_contains(zero_plan, "proof_boundary=completed_owner_group");
}

#[test]
fn graph_row_adjacency_pull_low_id_hub_and_sparse_label_are_truthful() {
    let (_dir, hub_engine) = graph_row_test_engine();
    let hub = insert_graph_row_node(&hub_engine, "AdjHub", "hub-owner", &[]);
    let mut hub_edge_ids = Vec::new();
    for index in 0..4_100u64 {
        let target = insert_graph_row_node(
            &hub_engine,
            "AdjHubTarget",
            &format!("hub-target-{index:04}"),
            &[],
        );
        hub_edge_ids.push(insert_graph_row_edge(
            &hub_engine,
            hub,
            target,
            "ADJ_PUBLIC_HUB",
            &[],
        ));
    }
    let mut tail_edge_ids = Vec::new();
    let mut tail_owner_ids = Vec::new();
    for index in 0..20u64 {
        let source = insert_graph_row_node(
            &hub_engine,
            "AdjHubTail",
            &format!("hub-tail-source-{index:02}"),
            &[],
        );
        let target = insert_graph_row_node(
            &hub_engine,
            "AdjHubTail",
            &format!("hub-tail-target-{index:02}"),
            &[],
        );
        tail_owner_ids.push(source);
        tail_edge_ids.push(insert_graph_row_edge(
            &hub_engine,
            source,
            target,
            "ADJ_PUBLIC_HUB",
            &[],
        ));
    }
    hub_engine.flush().unwrap();

    let mut hub_query = graph_row_adjacency_selected_edge_query(
        &["ADJ_PUBLIC_HUB"],
        Direction::Outgoing,
        10,
    );
    let (automatic, forced_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&hub_engine, &hub_query);
    assert_graph_row_explain_contains(automatic.plan.as_ref().unwrap(), "source=EdgePull{");
    assert_graph_row_explain_contains(
        automatic.plan.as_ref().unwrap(),
        "reason=owner_bound_exceeds_pull_ceiling",
    );
    assert_graph_row_explain_contains(
        automatic.plan.as_ref().unwrap(),
        "H=4100; K=11; F=4096",
    );
    let forced_text = graph_row_explain_text(forced_adjacency.plan.as_ref().unwrap());
    assert!(
        graph_row_chunk_cursor_seek_runtime_metric(&forced_text, "raw_adjacency_postings_scanned")
            >= 4_100,
        "the first page must finish the whole low-ID owner: {forced_text}"
    );
    assert!(forced_text.contains("early_exit=true"), "{forced_text}");

    let (_, first_trace) = with_graph_row_test_adjacency_pull_trace(|| {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || hub_engine.query_graph_rows(&hub_query).unwrap(),
        )
    });
    assert!(first_trace.len() > 1, "{first_trace:?}");
    assert!(first_trace.iter().all(|pull| {
        pull.oriented_metadata <= 4_096
            && pull.physical_scan_units <= 4_096
            && pull.raw_postings_scanned <= 4_096
    }));
    let completed_index = first_trace
        .iter()
        .position(|pull| pull.pending_completed_through_owner == Some(hub))
        .expect("the hub boundary must eventually complete");
    assert!(first_trace[..completed_index]
        .iter()
        .all(|pull| pull.pending_completed_through_owner.is_none()));

    hub_query.page.limit = 4_095;
    let first_large = with_graph_row_test_forced_edge_production(
        GraphRowEdgeAnchorProduction::AdjacencyPull,
        || hub_engine.query_graph_rows(&hub_query).unwrap(),
    );
    assert_eq!(first_large.rows.len(), 4_095);
    let cursor = first_large.next_cursor.clone().unwrap();
    hub_query.at_epoch = Some(first_large.stats.effective_at_epoch);
    hub_query.page.cursor = Some(cursor);
    hub_query.page.limit = 10;
    let (same_owner_page, same_owner_trace) = with_graph_row_test_adjacency_pull_trace(|| {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || hub_engine.query_graph_rows(&hub_query).unwrap(),
        )
    });
    let (same_owner_repeat, repeat_trace) = with_graph_row_test_adjacency_pull_trace(|| {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || hub_engine.query_graph_rows(&hub_query).unwrap(),
        )
    });
    assert_eq!(same_owner_page.rows, same_owner_repeat.rows);
    assert_eq!(same_owner_page.next_cursor, same_owner_repeat.next_cursor);
    assert_eq!(same_owner_page.rows.len(), 10);
    let page_ids = graph_row_adjacency_selected_edges(&same_owner_page)
        .iter()
        .map(|edge| edge.id.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&page_ids[..5], &hub_edge_ids[4_095..]);
    assert_eq!(&page_ids[5..], &tail_edge_ids[..5]);
    let same_owner_text = graph_row_explain_text(same_owner_page.plan.as_ref().unwrap());
    assert!(same_owner_text.contains("cursor_seek=anchor_ge:"), "{same_owner_text}");
    assert!(
        graph_row_chunk_cursor_seek_runtime_metric(
            &same_owner_text,
            "raw_adjacency_postings_scanned"
        ) >= 4_100,
        "inclusive same-owner replay must report repeated hub work: {same_owner_text}"
    );
    assert!(same_owner_trace.iter().all(|pull| {
        pull.oriented_metadata <= 4_096
            && pull.physical_scan_units <= 4_096
            && pull.raw_postings_scanned <= 4_096
    }));
    assert_eq!(repeat_trace, same_owner_trace);
    assert!(same_owner_trace
        .iter()
        .filter(|pull| pull.partial_owner_pull)
        .all(|pull| pull.pending_completed_through_owner.is_none()));
    let stopping_pull = same_owner_trace.last().expect("crossing page pull trace");
    assert_eq!(
        stopping_pull.pending_completed_through_owner,
        Some(tail_owner_ids[5])
    );
    assert_eq!(
        stopping_pull.pending_last_completed_owner,
        Some(tail_owner_ids[5])
    );
    assert!(stopping_pull.has_more);
    assert!(!stopping_pull.partial_owner_pull);
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(&same_owner_text, "completed_owner_groups"),
        7
    );
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(&same_owner_text, "last_completed_owner"),
        tail_owner_ids[5] as usize
    );
    assert!(same_owner_text.contains("early_exit=true"), "{same_owner_text}");
    let runtime_page = graph_row_chunk_runtime_once_result(&hub_engine, &hub_query);
    graph_row_chunk_assert_page_parity(
        &same_owner_page,
        &runtime_page,
        "same-owner hub cursor RuntimeOnce",
    );
    let trace_peaks = |trace: &[GraphRowTestAdjacencyPullTrace]| {
        (
            trace
                .iter()
                .map(|pull| pull.oriented_metadata)
                .max()
                .unwrap_or(0),
            trace
                .iter()
                .map(|pull| pull.raw_postings_scanned)
                .max()
                .unwrap_or(0),
            trace
                .iter()
                .map(|pull| pull.physical_scan_units)
                .max()
                .unwrap_or(0),
        )
    };
    let first_peaks = trace_peaks(&first_trace);
    let cursor_peaks = trace_peaks(&same_owner_trace);
    eprintln!(
        "Phase 44b forced hub capability: automatic_source=EdgePull; H=4100; first_selection_capacity=11; first_raw_total={}; first_output_raw_physical_peaks={first_peaks:?}; first_early_exit=true; cursor_seek=inclusive_owner; cursor_selection_capacity=11; cursor_raw_total={}; cursor_output_raw_physical_peaks={cursor_peaks:?}; large_first_selection_capacity=4096; completed_owner_groups=7",
        graph_row_chunk_cursor_seek_runtime_metric(
            &forced_text,
            "raw_adjacency_postings_scanned"
        ),
        graph_row_chunk_cursor_seek_runtime_metric(
            &same_owner_text,
            "raw_adjacency_postings_scanned"
        ),
    );

    let (_sparse_dir, sparse_engine) = graph_row_test_engine();
    let mut sparse_ids = Vec::new();
    for index in 0..1_000u64 {
        let source = insert_graph_row_node(
            &sparse_engine,
            "AdjSparse",
            &format!("sparse-source-{index:04}"),
            &[],
        );
        let target = insert_graph_row_node(
            &sparse_engine,
            "AdjSparse",
            &format!("sparse-target-{index:04}"),
            &[],
        );
        insert_graph_row_edge(
            &sparse_engine,
            source,
            target,
            "ADJ_SPARSE_DENSE",
            &[],
        );
        if index % 100 == 99 {
            sparse_ids.push(insert_graph_row_edge(
                &sparse_engine,
                source,
                target,
                "ADJ_SPARSE_RARE",
                &[],
            ));
        }
    }
    sparse_engine.flush().unwrap();
    let mut sparse_query = graph_row_adjacency_selected_edge_query(
        &["ADJ_SPARSE_RARE"],
        Direction::Outgoing,
        4,
    );
    let (sparse_auto, sparse_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&sparse_engine, &sparse_query);
    let sparse_auto_text = graph_row_explain_text(sparse_auto.plan.as_ref().unwrap());
    let sparse_first_cost = sparse_auto_text
        .lines()
        .find(|line| line.contains("kind=AdjacencyPull;") && line.contains("; P="))
        .unwrap_or_else(|| panic!("sparse first-page adjacency cost line: {sparse_auto_text}"));
    for fact in [
        "P=1010; M=10; O=10; A=10; C=10; R=Some(10); H=1; K=5; F=4096; L=1; S=0",
        "owners_needed=5; index_entries=1010; raw_postings_needed=5; candidate_edges=5; owner_merge_work=5",
        "dense=false; prefix_heuristic=true; fanout_complete=true; partial_or_stale=false; coverage=advisory",
        "edge_pull_work=54; ordered_work=1020; four_x_pass=false",
        "decision=rejected; reason=four_x_margin_failed",
    ] {
        assert!(sparse_first_cost.contains(fact), "missing {fact}: {sparse_first_cost}");
    }
    assert!(sparse_auto_text.contains("source=EdgePull{"), "{sparse_auto_text}");
    let expected_first = sparse_ids[..4].to_vec();
    assert_eq!(
        graph_row_adjacency_selected_edges(&sparse_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        expected_first
    );
    sparse_query.at_epoch = Some(sparse_adjacency.stats.effective_at_epoch);
    sparse_query.page.cursor = sparse_adjacency.next_cursor.clone();
    let (sparse_cursor_auto, sparse_cursor_adjacency, _, _) =
        graph_row_adjacency_forced_differential(&sparse_engine, &sparse_query);
    assert_eq!(
        graph_row_adjacency_selected_edges(&sparse_cursor_adjacency)
            .iter()
            .map(|edge| edge.id.unwrap())
            .collect::<Vec<_>>(),
        sparse_ids[4..8]
    );
    assert_graph_row_explain_contains(
        sparse_cursor_adjacency.plan.as_ref().unwrap(),
        "cursor_seek=anchor_ge:",
    );
    let sparse_cursor_text = graph_row_explain_text(sparse_cursor_auto.plan.as_ref().unwrap());
    let sparse_cursor_cost = sparse_cursor_text
        .lines()
        .find(|line| line.contains("kind=AdjacencyPull;") && line.contains("; P="))
        .unwrap_or_else(|| panic!("sparse cursor-page adjacency cost line: {sparse_cursor_text}"));
    for fact in [
        "P=1010; M=10; O=10; A=10; C=10; R=Some(10); H=1; K=5; F=4096; L=1",
        "owners_needed=5; index_entries=1010; raw_postings_needed=5; candidate_edges=5; owner_merge_work=5",
        "coverage=advisory",
        "edge_pull_work=54",
        "four_x_pass=false",
        "decision=rejected; reason=four_x_margin_failed",
    ] {
        assert!(sparse_cursor_cost.contains(fact), "missing {fact}: {sparse_cursor_cost}");
    }
    let cursor_seek_work = graph_row_chunk_cursor_seek_runtime_metric(sparse_cursor_cost, "S");
    assert!(cursor_seek_work > 0, "{sparse_cursor_cost}");
    assert_eq!(
        graph_row_chunk_cursor_seek_runtime_metric(sparse_cursor_cost, "ordered_work"),
        1_020 + cursor_seek_work
    );
    assert!(sparse_cursor_text.contains("source=EdgePull{"), "{sparse_cursor_text}");
}

#[test]
fn graph_row_adjacency_pull_invalid_header_rejects_only_adjacency_alternative() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..1_000u64 {
        let source = insert_graph_row_node(
            &engine,
            "AdjInvalidHeader",
            &format!("invalid-header-source-{index:04}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "AdjInvalidHeader",
            &format!("invalid-header-target-{index:04}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "ADJ_INVALID_HEADER",
            &[],
        );
    }
    engine.flush().unwrap();

    let mut query = graph_row_adjacency_selected_edge_query(
        &["ADJ_INVALID_HEADER"],
        Direction::Outgoing,
        10,
    );
    let warm = engine.query_graph_rows(&query).unwrap();
    query.at_epoch = Some(warm.stats.effective_at_epoch);
    let runtime_once = graph_row_chunk_runtime_once_result(&engine, &query);

    let automatic = with_graph_row_test_invalid_adjacency_header(|| {
        engine.query_graph_rows(&query).unwrap()
    });
    let forced_adjacency = with_graph_row_test_invalid_adjacency_header(|| {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || engine.query_graph_rows(&query).unwrap(),
        )
    });
    let forced_edge = with_graph_row_test_invalid_adjacency_header(|| {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::EdgePull,
            || engine.query_graph_rows(&query).unwrap(),
        )
    });
    for (name, result) in [
        ("automatic", &automatic),
        ("forced AdjacencyPull", &forced_adjacency),
        ("forced EdgePull", &forced_edge),
    ] {
        graph_row_chunk_assert_page_parity(result, &runtime_once, name);
        let plan = result.plan.as_ref().unwrap();
        assert_graph_row_explain_contains(plan, "kind=AdjacencyPull");
        assert_graph_row_explain_contains(plan, "reason=invalid_adjacency_header");
        assert_graph_row_explain_contains(plan, "source=EdgePull{edge=alias:edge}");
        assert_graph_row_explain_contains(plan, "choice=EdgePullChunk");
        for forbidden in [
            "source=AdjacencyPull{",
            "choice=AdjacencyPullChunk",
            "source_order=logical_from_group_asc",
            "proof_boundary=completed_owner_group",
            "adjacency_index_entries_scanned=",
            "raw_adjacency_postings_scanned=",
            "shadowed_or_stale_postings=",
        ] {
            assert_graph_row_explain_not_contains(plan, forbidden);
        }
    }
}

#[test]
fn graph_row_adjacency_pull_public_corruption_is_lazy_terminal_and_never_falls_back() {
    let (_dir, engine) = graph_row_test_engine();
    let mut owners = Vec::new();
    for index in 0..1_000u64 {
        let source = insert_graph_row_node(
            &engine,
            "AdjPublicError",
            &format!("error-source-{index:04}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "AdjPublicError",
            &format!("error-target-{index:04}"),
            &[],
        );
        owners.push(source);
        insert_graph_row_edge(
            &engine,
            source,
            target,
            "ADJ_PUBLIC_ERROR",
            &[],
        );
    }
    engine.flush().unwrap();

    let mut query = graph_row_adjacency_selected_edge_query(
        &["ADJ_PUBLIC_ERROR"],
        Direction::Outgoing,
        1,
    );
    engine.reset_query_execution_counters_for_test();
    let required = with_graph_row_test_adjacency_posting_failure(owners[0], || {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || engine.query_graph_rows(&query),
        )
    })
    .unwrap_err();
    assert!(matches!(required, EngineError::CorruptRecord(_)));
    let required_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(required_counters.graph_row_edge_pull_executions, 0);
    assert_eq!(required_counters.graph_row_chunk_early_exits, 0);

    let skipped = with_graph_row_test_adjacency_posting_failure(owners[900], || {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || engine.query_graph_rows(&query).unwrap(),
        )
    });
    assert_eq!(skipped.rows.len(), 1);
    assert_graph_row_explain_contains(skipped.plan.as_ref().unwrap(), "early_exit=true");

    query.page.limit = 900;
    let prefix = with_graph_row_test_forced_edge_production(
        GraphRowEdgeAnchorProduction::AdjacencyPull,
        || engine.query_graph_rows(&query).unwrap(),
    );
    assert_eq!(prefix.rows.len(), 900);
    query.at_epoch = Some(prefix.stats.effective_at_epoch);
    query.page.cursor = prefix.next_cursor;
    query.page.limit = 1;
    engine.reset_query_execution_counters_for_test();
    let later = with_graph_row_test_adjacency_posting_failure(owners[900], || {
        with_graph_row_test_forced_edge_production(
            GraphRowEdgeAnchorProduction::AdjacencyPull,
            || engine.query_graph_rows(&query),
        )
    })
    .unwrap_err();
    assert!(matches!(later, EngineError::CorruptRecord(_)));
    let later_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(later_counters.graph_row_edge_pull_executions, 0);
    assert_eq!(later_counters.graph_row_chunk_early_exits, 0);
}

#[test]
fn graph_row_planner_bound_by_prior_edge_explains_selective_edge_source() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_PRIOR_BOUND", "prior-source", &[]);
    let mid = insert_graph_row_node(&engine, "GRAPH_ROW_PRIOR_BOUND_MID", "prior-mid", &[]);
    insert_graph_row_edge(
        &engine,
        source,
        mid,
        "GRAPH_ROW_PRIOR_BOUND_FIRST",
        &[("bucket", PropValue::String("hit".to_string()))],
    );
    let hit_target = insert_graph_row_node(
        &engine,
        "GRAPH_ROW_PRIOR_BOUND_TARGET",
        "prior-target-hit",
        &[],
    );
    let _hit_edge = insert_graph_row_edge(
        &engine,
        mid,
        hit_target,
        "GRAPH_ROW_PRIOR_BOUND_SECOND",
        &[("bucket", PropValue::String("hit".to_string()))],
    );
    for index in 0..90 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_PRIOR_BOUND_TARGET",
            &format!("prior-target-miss-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            mid,
            target,
            "GRAPH_ROW_PRIOR_BOUND_SECOND",
            &[("bucket", PropValue::String("miss".to_string()))],
        );
    }
    engine.flush().unwrap();
    let first_index = engine
        .ensure_edge_property_index("GRAPH_ROW_PRIOR_BOUND_FIRST", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("bucket").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(
        &engine,
        first_index.index_id,
        SecondaryIndexState::Ready,
    );
    let second_index = engine
        .ensure_edge_property_index("GRAPH_ROW_PRIOR_BOUND_SECOND", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("bucket").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(
        &engine,
        second_index.index_id,
        SecondaryIndexState::Ready,
    );

    let mut query = graph_query(
        &["source", "mid", "target"],
        vec![
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("a_first_edge".to_string()),
                from_alias: "source".to_string(),
                to_alias: "mid".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["GRAPH_ROW_PRIOR_BOUND_FIRST".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "bucket".to_string(),
                    value: PropValue::String("hit".to_string()),
                }),
            }),
            GraphPatternPiece::Optional(GraphOptionalGroup {
                pieces: vec![graph_edge_with_label(
                    Some("optional_edge"),
                    "mid",
                    "target",
                    "GRAPH_ROW_PRIOR_BOUND_OPTIONAL",
                )],
                where_: None,
            }),
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("z_second_edge".to_string()),
                from_alias: "mid".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["GRAPH_ROW_PRIOR_BOUND_SECOND".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "bucket".to_string(),
                    value: PropValue::String("hit".to_string()),
                }),
            }),
        ],
    );
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "z_second_edge",
        GraphReturnProjection::IdOnly,
    )]);

    let static_explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &static_explain,
        "physical_edge_order=[\"alias:a_first_edge\", \"alias:z_second_edge\"]",
    );
    assert_graph_row_explain_contains(
        &static_explain,
        "edge=alias:z_second_edge; context=bound endpoint selective edge candidate source",
    );
    assert_graph_row_explain_not_contains(
        &static_explain,
        "edge=alias:z_second_edge; source=EndpointAdjacency",
    );
}

#[test]
fn graph_row_planner_edge_property_in_anchor_preserves_signed_zero() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_EDGE_IN_ZERO", "edge-in-source", &[]);
    let positive_target =
        insert_graph_row_node(&engine, "GRAPH_ROW_EDGE_IN_ZERO", "edge-in-positive", &[]);
    let negative_target =
        insert_graph_row_node(&engine, "GRAPH_ROW_EDGE_IN_ZERO", "edge-in-negative", &[]);
    let positive = insert_graph_row_edge(
        &engine,
        source,
        positive_target,
        "GRAPH_ROW_EDGE_IN_ZERO_REL",
        &[("z", PropValue::Float(0.0))],
    );
    let negative = insert_graph_row_edge(
        &engine,
        source,
        negative_target,
        "GRAPH_ROW_EDGE_IN_ZERO_REL",
        &[("z", PropValue::Float(-0.0))],
    );
    for index in 0..40 {
        let miss_target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_EDGE_IN_ZERO",
            &format!("edge-in-miss-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            source,
            miss_target,
            "GRAPH_ROW_EDGE_IN_ZERO_REL",
            &[("z", PropValue::Float(index as f64 + 1.0))],
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_EDGE_IN_ZERO_REL", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("z").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EDGE_IN_ZERO_REL".to_string()],
            filter: Some(EdgeFilterExpr::PropertyIn {
                key: "z".to_string(),
                values: vec![
                    PropValue::Float(-0.0),
                    PropValue::Float(0.0),
                    PropValue::Float(-0.0),
                ],
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_single_u64_column(result.clone()),
        vec![positive, negative]
    );
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "choice=EdgePullChunk");
    assert_graph_row_explain_contains(explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(explain, "semantic numeric equality/range equivalence");
}

#[test]
fn graph_row_planner_broad_edge_property_source_stays_with_adjacency() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_BROAD_EDGE", "broad-source", &[]);
    let mut expected = Vec::new();
    for index in 0..3 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BROAD_EDGE_TARGET",
            &format!("broad-local-{index}"),
            &[],
        );
        expected.push(insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_BROAD_EDGE_REL",
            &[("bucket", PropValue::String("red".to_string()))],
        ));
    }
    for index in 0..80 {
        let other_source = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BROAD_EDGE",
            &format!("broad-other-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BROAD_EDGE_TARGET",
            &format!("broad-other-target-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            other_source,
            target,
            "GRAPH_ROW_BROAD_EDGE_REL",
            &[("bucket", PropValue::String("red".to_string()))],
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_edge_property_index("GRAPH_ROW_BROAD_EDGE_REL", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("bucket").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("rel".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_BROAD_EDGE_REL".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "bucket".to_string(),
                value: PropValue::String("red".to_string()),
            }),
        })],
    );
    query.nodes[0].ids = vec![source];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    let result = engine.query_graph_rows(&query).unwrap();
    let mut actual = graph_row_single_u64_column(result.clone());
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "GraphRowSourceRead");
    assert_graph_row_explain_contains(explain, "choice=DelegatedEdgeQuery");
    assert_graph_row_explain_contains(explain, "planned_driver=EndpointAdjacency");
    assert_graph_row_explain_contains(explain, "EdgeEndpointAdjacency");
    assert_graph_row_explain_contains(explain, "fallback_source=none");
}

#[test]
fn graph_row_planner_high_fanout_small_limit_returns_deterministic_top_rows() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_LIMIT_PLAN", "limit-plan-source", &[]);
    let low_target = insert_graph_row_node(&engine, "GRAPH_ROW_LIMIT_PLAN", "limit-plan-low", &[]);
    insert_graph_row_edge(&engine, source, low_target, "GRAPH_ROW_LIMIT_PLAN_LOW", &[]);
    let mut high_edges = Vec::new();
    for index in 0..9 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_LIMIT_PLAN",
            &format!("limit-plan-high-{index}"),
            &[],
        );
        high_edges.push(insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_LIMIT_PLAN_HIGH",
            &[],
        ));
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "high", "low"],
        vec![
            graph_edge_with_label(
                Some("high_edge"),
                "source",
                "high",
                "GRAPH_ROW_LIMIT_PLAN_HIGH",
            ),
            graph_edge_with_label(
                Some("low_edge"),
                "source",
                "low",
                "GRAPH_ROW_LIMIT_PLAN_LOW",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "high_edge",
        GraphReturnProjection::IdOnly,
    )]);

    let full = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    let mut limited = query.clone();
    limited.page.limit = 3;
    limited.options.include_plan = true;
    let page = engine.query_graph_rows(&limited).unwrap();
    assert_eq!(graph_row_single_u64_column(page.clone()), full[..3].to_vec());
    assert_graph_row_explain_contains(
        page.plan.as_ref().unwrap(),
        "physical_edge_order=[\"alias:low_edge\", \"alias:high_edge\"]",
    );
    assert_eq!(full, high_edges);
}

#[test]
fn graph_row_edge_pull_empty_result_does_not_prepare_later_sources() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_SKIP_SOURCE", "skip-source", &[]);
    let first = insert_graph_row_node(&engine, "GRAPH_ROW_SKIP_SOURCE", "skip-first", &[]);
    let second = insert_graph_row_node(&engine, "GRAPH_ROW_SKIP_SOURCE", "skip-second", &[]);
    insert_graph_row_edge(&engine, source, first, "GRAPH_ROW_SKIP_PRESENT", &[]);
    insert_graph_row_edge(&engine, source, second, "GRAPH_ROW_SKIP_SECOND", &[]);
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "first", "second"],
        vec![
            graph_edge_with_label(
                Some("empty_edge"),
                "source",
                "first",
                "GRAPH_ROW_SKIP_MISSING",
            ),
            graph_edge_with_label(
                Some("skipped_edge"),
                "source",
                "second",
                "GRAPH_ROW_SKIP_SECOND",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "skipped_edge",
        GraphReturnProjection::IdOnly,
    )]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert!(result.rows.is_empty());
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "edge=alias:empty_edge");
    assert_graph_row_explain_contains(explain, "choice=EmptyResult");
    assert_graph_row_explain_not_contains(explain, "choice=SkippedEmptyFrontier");
    assert_graph_row_explain_contains(
        explain,
        "ChunkedRowProductionRuntime source=EdgePull{edge=alias:empty_edge}; source_pulls=0; successful_leaves=0; scheduled_sizes=[]; leaf_size_min=0; leaf_size_max=0; source_rows=0; produced_rows=0",
    );
}

#[test]
fn graph_row_planner_broad_label_only_edge_anchor_streams_under_cap() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..3 {
        let source = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BROAD_LABEL",
            &format!("broad-label-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_BROAD_LABEL",
            &format!("broad-label-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "GRAPH_ROW_BROAD_LABEL_REL", &[]);
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("rel"),
            "source",
            "target",
            "GRAPH_ROW_BROAD_LABEL_REL",
        )],
    );
    query.options.max_frontier = 2;
    query.return_items = Some(vec![graph_return_binding(
        "rel",
        GraphReturnProjection::IdOnly,
    )]);

    query.options.include_plan = true;
    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), vec![1, 2, 3]);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "choice=EdgePullChunk");
    assert_graph_row_explain_contains(explain, "source_rows=3");
    assert_graph_row_explain_contains(explain, "leaf_size_max=2");
}

#[test]
fn graph_row_planner_fanout_stats_preserve_results_after_reopen() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("db");
    let expected;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let source = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_REOPEN_PLAN",
            "reopen-plan-source",
            &[],
        );
        let low_target = insert_graph_row_node(&engine, "GRAPH_ROW_REOPEN_PLAN", "reopen-low", &[]);
        insert_graph_row_edge(&engine, source, low_target, "GRAPH_ROW_REOPEN_LOW", &[]);
        let mut high_edges = Vec::new();
        for index in 0..7 {
            let target = insert_graph_row_node(
                &engine,
                "GRAPH_ROW_REOPEN_PLAN",
                &format!("reopen-high-{index}"),
                &[],
            );
            high_edges.push(insert_graph_row_edge(
                &engine,
                source,
                target,
                "GRAPH_ROW_REOPEN_HIGH",
                &[],
            ));
        }
        engine.flush().unwrap();

        let mut query = graph_query(
            &["source", "high", "low"],
            vec![
                graph_edge_with_label(
                    Some("high_edge"),
                    "source",
                    "high",
                    "GRAPH_ROW_REOPEN_HIGH",
                ),
                graph_edge_with_label(
                    Some("low_edge"),
                    "source",
                    "low",
                    "GRAPH_ROW_REOPEN_LOW",
                ),
            ],
        );
        query.nodes[0].ids = vec![source];
        query.return_items = Some(vec![graph_return_binding(
            "high_edge",
            GraphReturnProjection::IdOnly,
        )]);
        expected = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
        assert_eq!(expected, high_edges);
        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let source = reopened
        .query_node_ids(&NodeQuery {
            label_filter: Some(node_label_filter(&["GRAPH_ROW_REOPEN_PLAN"], LabelMatchMode::All)),
            keys: vec!["reopen-plan-source".to_string()],
            allow_full_scan: true,
            ..NodeQuery::default()
        })
        .unwrap()
        .items[0];
    let mut query = graph_query(
        &["source", "high", "low"],
        vec![
            graph_edge_with_label(
                Some("high_edge"),
                "source",
                "high",
                "GRAPH_ROW_REOPEN_HIGH",
            ),
            graph_edge_with_label(
                Some("low_edge"),
                "source",
                "low",
                "GRAPH_ROW_REOPEN_LOW",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "high_edge",
        GraphReturnProjection::IdOnly,
    )]);
    let explain = reopened.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(
        &explain,
        "physical_edge_order=[\"alias:low_edge\", \"alias:high_edge\"]",
    );
    assert_eq!(
        graph_row_single_u64_column(reopened.query_graph_rows(&query).unwrap()),
        expected
    );
}

#[test]
fn graph_row_planner_physical_reorder_preserves_cursor_pages() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_CURSOR_PLAN", "cursor-plan-source", &[]);
    let low_target = insert_graph_row_node(&engine, "GRAPH_ROW_CURSOR_PLAN", "cursor-plan-low", &[]);
    insert_graph_row_edge(&engine, source, low_target, "GRAPH_ROW_CURSOR_PLAN_LOW", &[]);
    let mut high_edges = Vec::new();
    for index in 0..11 {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_CURSOR_PLAN",
            &format!("cursor-plan-high-{index}"),
            &[],
        );
        high_edges.push(insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_CURSOR_PLAN_HIGH",
            &[],
        ));
    }
    engine.flush().unwrap();

    let mut query = graph_query(
        &["source", "high", "low"],
        vec![
            graph_edge_with_label(
                Some("high_edge"),
                "source",
                "high",
                "GRAPH_ROW_CURSOR_PLAN_HIGH",
            ),
            graph_edge_with_label(
                Some("low_edge"),
                "source",
                "low",
                "GRAPH_ROW_CURSOR_PLAN_LOW",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "high_edge",
        GraphReturnProjection::IdOnly,
    )]);
    query.page.limit = 100;
    let full = graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap());
    assert_eq!(full, high_edges);

    let mut paged = query.clone();
    paged.page.limit = 4;
    paged.options.include_plan = true;
    let mut concatenated = Vec::new();
    loop {
        let page = engine.query_graph_rows(&paged).unwrap();
        if let Some(plan) = page.plan.as_ref() {
            assert_graph_row_explain_contains(
                plan,
                "physical_edge_order=[\"alias:low_edge\", \"alias:high_edge\"]",
            );
        }
        concatenated.extend(graph_row_single_u64_column(page.clone()));
        if let Some(cursor) = page.next_cursor {
            paged.page.cursor = Some(cursor);
        } else {
            break;
        }
    }
    assert_eq!(concatenated, full);
}

#[test]
fn graph_row_planner_physical_reorder_preserves_explicit_order_by_oracle() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "GRAPH_ROW_ORDER_PLAN", "order-plan-source", &[]);
    let low_target = insert_graph_row_node(&engine, "GRAPH_ROW_ORDER_PLAN", "order-plan-low", &[]);
    insert_graph_row_edge(&engine, source, low_target, "GRAPH_ROW_ORDER_PLAN_LOW", &[]);
    let mut oracle = Vec::new();
    for rank in [7_i64, 2, 9, 1, 5, 0, 4, 6, 3, 8, 10] {
        let target = insert_graph_row_node(
            &engine,
            "GRAPH_ROW_ORDER_PLAN",
            &format!("order-plan-high-{rank}"),
            &[("rank", PropValue::Int(rank))],
        );
        let edge = insert_graph_row_edge(
            &engine,
            source,
            target,
            "GRAPH_ROW_ORDER_PLAN_HIGH",
            &[],
        );
        oracle.push((rank, edge));
    }
    engine.flush().unwrap();
    oracle.sort_by_key(|(rank, edge)| (*rank, *edge));
    let expected = oracle
        .into_iter()
        .map(|(_rank, edge)| edge)
        .collect::<Vec<_>>();

    let mut query = graph_query(
        &["source", "high", "low"],
        vec![
            graph_edge_with_label(
                Some("high_edge"),
                "source",
                "high",
                "GRAPH_ROW_ORDER_PLAN_HIGH",
            ),
            graph_edge_with_label(
                Some("low_edge"),
                "source",
                "low",
                "GRAPH_ROW_ORDER_PLAN_LOW",
            ),
        ],
    );
    query.nodes[0].ids = vec![source];
    query.return_items = Some(vec![graph_return_binding(
        "high_edge",
        GraphReturnProjection::IdOnly,
    )]);
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("high", "rank"),
        direction: GraphOrderDirection::Asc,
    }];
    query.page.limit = 100;
    query.options.include_plan = true;

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_single_u64_column(result.clone()), expected);
    assert_graph_row_explain_contains(
        result.plan.as_ref().unwrap(),
        "physical_edge_order=[\"alias:low_edge\", \"alias:high_edge\"]",
    );
}

#[test]
fn graph_row_explain_reports_mixed_sources_dedupe_newest_props_and_numeric_verification() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "ExplainMixedNode", "mixed-source", &[]);
    let target_a = insert_graph_row_node(&engine, "ExplainMixedNode", "mixed-target-a", &[]);
    let target_b = insert_graph_row_node(&engine, "ExplainMixedNode", "mixed-target-b", &[]);
    let edge_a = insert_graph_row_edge(
        &engine,
        source,
        target_a,
        "GRAPH_ROW_EXPLAIN_MIXED",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let edge_b = insert_graph_row_edge(
        &engine,
        source,
        target_b,
        "GRAPH_ROW_EXPLAIN_MIXED",
        &[("status", PropValue::String("hot".to_string()))],
    );
    engine.flush().unwrap();
    let eq_index = engine
        .ensure_edge_property_index("GRAPH_ROW_EXPLAIN_MIXED", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, eq_index.index_id, SecondaryIndexState::Ready);
    set_query_edge_props(
        &engine,
        edge_a,
        graph_row_props(&[("status", PropValue::String("hot".to_string()))]),
    );
    set_query_edge_props(
        &engine,
        edge_b,
        graph_row_props(&[("status", PropValue::String("cold".to_string()))]),
    );

    let mut query = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("e".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_MIXED".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.return_items = Some(vec![graph_return_binding("e", GraphReturnProjection::IdOnly)]);

    let explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(&explain, "newer shadows older records");
    assert_graph_row_explain_contains(&explain, "stale index candidates/hash collisions");
    assert_graph_row_explain_contains(&explain, "semantic numeric equality/range equivalence");

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![edge_a]
    );
}

#[test]
fn graph_row_explain_reports_range_numeric_equivalence_and_order_cursor_row_ops() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "ExplainRangeNode", "range-a", &[]);
    let b = insert_graph_row_node(&engine, "ExplainRangeNode", "range-b", &[]);
    let c = insert_graph_row_node(&engine, "ExplainRangeNode", "range-c", &[]);
    let int_edge = insert_graph_row_edge(
        &engine,
        a,
        b,
        "GRAPH_ROW_EXPLAIN_RANGE",
        &[("metric", PropValue::Int(5))],
    );
    let uint_edge = insert_graph_row_edge(
        &engine,
        a,
        c,
        "GRAPH_ROW_EXPLAIN_RANGE",
        &[("metric", PropValue::UInt(5))],
    );
    engine.flush().unwrap();
    let range = engine
        .ensure_edge_property_index("GRAPH_ROW_EXPLAIN_RANGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("metric").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_edge_property_index_state(&engine, range.index_id, SecondaryIndexState::Ready);

    let mut query = graph_query(
        &["a", "b"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("e".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["GRAPH_ROW_EXPLAIN_RANGE".to_string()],
            filter: Some(EdgeFilterExpr::PropertyRange {
                key: "metric".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(5))),
            }),
        })],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.page.limit = 1;
    query.return_items = Some(vec![graph_return_binding("e", GraphReturnProjection::IdOnly)]);

    let first_page = engine.query_graph_rows(&query).unwrap();
    let first_edge = graph_row_single_u64_column(first_page.clone());
    assert_eq!(first_edge, vec![int_edge]);
    assert!(first_page.next_cursor.is_some());
    let first_explain = first_page.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(first_explain, "EdgePropertyRangeIndex");
    assert_graph_row_explain_contains(first_explain, "semantic numeric equality/range equivalence");
    assert_graph_row_explain_contains(first_explain, "Order");
    assert_graph_row_explain_contains(first_explain, "SkipLimit");
    assert_graph_row_explain_contains(first_explain, "cap pressure");
    assert_graph_row_explain_contains(first_explain, "next_cursor=true");

    query.page.cursor = first_page.next_cursor;
    let second_explain = engine.explain_graph_rows(&query).unwrap();
    assert_graph_row_explain_contains(&second_explain, "effective_at_epoch source: cursor payload");
    assert_graph_row_explain_contains(&second_explain, "cursor_supplied=true");

    let second_page = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_single_u64_column(second_page),
        vec![uint_edge]
    );
}

#[test]
fn graph_row_query_executes_valid_fixed_request_without_matches() {
    let query = graph_query(&["a", "b"], vec![graph_edge(Some("e"), "a", "b")]);
    let temp = TempDir::new().unwrap();
    let engine = DatabaseEngine::open(temp.path(), &DbOptions::default()).unwrap();

    let result = engine.query_graph_rows(&query).unwrap();

    assert!(result.rows.is_empty());
    assert_eq!(result.next_cursor, None);
}

#[test]
fn graph_row_query_still_validates_before_execution() {
    let mut query = graph_query(&["a"], Vec::new());
    query.page.limit = 0;
    let temp = TempDir::new().unwrap();
    let engine = DatabaseEngine::open(temp.path(), &DbOptions::default()).unwrap();

    let err = engine.query_graph_rows(&query).unwrap_err();

    assert!(
        err.to_string().contains("page limit must be > 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_row_chunk_eq1_forced_sizes_preserve_rows_cursors_and_pages() {
    let (dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    let mut brute_rows = Vec::new();
    for index in 0..12 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkEqSource",
            &format!("chunk-eq-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "ChunkEqTarget",
            &format!("chunk-eq-target-{index}"),
            &[],
        );
        let edge = insert_graph_row_edge(
            &engine,
            source,
            target,
            "CHUNK_EQ_EDGE",
            &[
                ("rank", PropValue::Int(index)),
                ("keep", PropValue::Bool(index % 2 == 0)),
            ],
        );
        sources.push(source);
        brute_rows.push(GraphRow {
            values: vec![
                GraphValue::NodeId(source),
                GraphValue::EdgeId(edge),
                GraphValue::NodeId(target),
            ],
        });
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CHUNK_EQ_EDGE",
        )],
    );
    query.nodes[0].ids = sources;
    query.page.limit = 5;
    query.order_by = vec![GraphOrderItem {
        expr: graph_prop("edge", "rank"),
        direction: GraphOrderDirection::Desc,
    }];
    query.return_items = Some(vec![
        graph_return_binding("source", GraphReturnProjection::IdOnly),
        graph_return_binding("edge", GraphReturnProjection::IdOnly),
        graph_return_binding("target", GraphReturnProjection::IdOnly),
    ]);

    brute_rows.reverse();
    let first = engine.query_graph_rows(&query).unwrap();
    assert_eq!(first.rows, brute_rows[..5]);
    graph_row_chunk_assert_forced_equivalence(&engine, &query, &[1, 3, 256, usize::MAX]);

    let mut unordered = query.clone();
    unordered.order_by.clear();
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &unordered,
        &[1, 3, 256, usize::MAX],
    );

    let mut residual = unordered.clone();
    residual.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("edge", "rank")),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Int(3)),
    });
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &residual,
        &[1, 3, 256, usize::MAX],
    );

    let mut hydration = query.clone();
    hydration.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("edge", "keep")),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::Bool(true)),
    });
    engine.reset_query_execution_counters_for_test();
    let hydrated = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&hydration).unwrap()
    });
    assert_eq!(hydrated.stats.rows_after_filter, 6);
    let hydration_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(hydration_counters.edge_record_hydration_reads, 0);
    assert_eq!(hydration_counters.edge_selected_field_batches, 18);
    assert_eq!(hydration_counters.edge_selected_field_ids, 18);

    let mut delegated = query.clone();
    if let GraphPatternPiece::Edge(edge) = &mut delegated.pieces[0] {
        edge.filter = Some(EdgeFilterExpr::PropertyRange {
            key: "rank".to_string(),
            lower: Some(PropertyRangeBound::Excluded(PropValue::Int(3))),
            upper: None,
        });
    }
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &delegated,
        &[1, 3, 256, usize::MAX],
    );

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_assert_forced_equivalence(
        &reopened,
        &query,
        &[1, 3, 256, usize::MAX],
    );
    let mut explained = query;
    explained.options.include_plan = true;
    explained.at_epoch = Some(now_millis());
    let first_plan = with_graph_row_test_chunk_override(1, || {
        reopened.query_graph_rows(&explained).unwrap().plan.unwrap()
    });
    let second_plan = with_graph_row_test_chunk_override(1, || {
        reopened.query_graph_rows(&explained).unwrap().plan.unwrap()
    });
    assert_eq!(first_plan, second_plan);
}

fn graph_row_chunk_assert_forced_equivalence(
    engine: &DatabaseEngine,
    query: &GraphRowQuery,
    sizes: &[usize],
) {
    let first = with_graph_row_test_chunk_override(usize::MAX, || {
        engine.query_graph_rows(query).unwrap()
    });
    let mut pinned = query.clone();
    pinned.at_epoch = Some(first.stats.effective_at_epoch);
    pinned.options.include_plan = true;
    let oracle = with_graph_row_test_chunk_override(usize::MAX, || {
        engine.query_graph_rows(&pinned).unwrap()
    });
    let runtime_once = graph_row_chunk_runtime_once_result(engine, &pinned);
    graph_row_chunk_assert_page_parity(&runtime_once, &oracle, "RuntimeOnce oracle");
    for &size in sizes {
        let actual = with_graph_row_test_chunk_override(size, || {
            engine.query_graph_rows(&pinned).unwrap()
        });
        graph_row_chunk_assert_page_parity(&actual, &oracle, &format!("forced size {size}"));
    }

    for page_limit in [1, 2, 7, 100] {
        let mut paged = pinned.clone();
        paged.page.limit = page_limit;
        paged.page.cursor = None;
        for &size in sizes {
            graph_row_chunk_assert_walk_parity(engine, &paged, size);
        }
    }
}

fn graph_row_chunk_runtime_once_result(
    engine: &DatabaseEngine,
    query: &GraphRowQuery,
) -> GraphRowResult {
    let normalized = normalize_graph_row_query(query).unwrap();
    let cursor_state = graph_row_prepare_cursor_state(
        &normalized.page,
        normalized.at_epoch,
        &normalized.options,
    )
    .unwrap();
    engine
        .published_state()
        .view
        .query_graph_rows_outcome_with_source_policy(
            &normalized,
            cursor_state,
            Some(GraphRowRuntimeOnceReason::NonNodeInitialDriver),
        )
        .unwrap()
        .value
}

fn graph_row_chunk_assert_page_parity(
    actual: &GraphRowResult,
    oracle: &GraphRowResult,
    context: &str,
) {
    assert_eq!(actual.columns, oracle.columns, "{context}: columns");
    assert_eq!(actual.rows, oracle.rows, "{context}: rows/order/values");
    assert_eq!(actual.next_cursor, oracle.next_cursor, "{context}: cursor bytes");
    assert_eq!(
        actual.stats.rows_returned, oracle.stats.rows_returned,
        "{context}: rows_returned"
    );
    let edge_pull = actual.plan.as_ref().is_some_and(|plan| {
        plan.plan.iter().any(|node| {
            node.kind == "ChunkedRowProductionRuntime"
                && node.detail.contains("source=EdgePull{")
        })
    });
    if edge_pull {
        assert_eq!(
            actual.stats.rows_after_filter, oracle.stats.rows_after_filter,
            "{context}: rows_after_filter"
        );
        assert_eq!(
            actual.stats.rows_seen_for_page, oracle.stats.rows_seen_for_page,
            "{context}: rows_seen_for_page"
        );
    }
    assert_eq!(
        actual.stats.effective_at_epoch, oracle.stats.effective_at_epoch,
        "{context}: effective epoch"
    );
    assert_eq!(actual.stats.warnings, oracle.stats.warnings, "{context}: warnings");
    if let (Some(actual), Some(oracle)) = (&actual.plan, &oracle.plan) {
        assert_eq!(actual.fingerprint, oracle.fingerprint, "{context}: fingerprint");
    }
}

fn graph_row_chunk_assert_walk_parity(
    engine: &DatabaseEngine,
    query: &GraphRowQuery,
    forced_size: usize,
) {
    let mut actual_query = query.clone();
    let mut oracle_query = query.clone();
    let mut page_index = 0usize;
    loop {
        let oracle = graph_row_chunk_runtime_once_result(engine, &oracle_query);
        let actual = with_graph_row_test_chunk_override(forced_size, || {
            engine.query_graph_rows(&actual_query).unwrap()
        });
        graph_row_chunk_assert_page_parity(
            &actual,
            &oracle,
            &format!("forced size {forced_size}, page {page_index}"),
        );
        match oracle.next_cursor {
            Some(cursor) => {
                actual_query.page.cursor = Some(cursor.clone());
                oracle_query.page.cursor = Some(cursor);
            }
            None => break,
        }
        page_index = page_index.saturating_add(1);
        assert!(page_index < 10_000, "cursor walk must terminate");
    }
}

#[test]
fn graph_row_edge_pull_native_shapes_keep_node_as_logical_slot_zero() {
    let required = graph_edge_with_label(Some("edge"), "source", "middle", "EQ44_SLOT");
    let optional = graph_optional(
        vec![graph_edge_with_label(
            Some("optional"),
            "middle",
            "target",
            "EQ44_SLOT_OPTIONAL",
        )],
        None,
    );
    let vlp = graph_vlp(Some("path"), None, "middle", "target", 1, 3);
    for query in [
        graph_query(&["source", "middle"], vec![required.clone()]),
        graph_query(
            &["source", "middle", "target"],
            vec![required.clone(), optional],
        ),
        graph_query(&["source", "middle", "target"], vec![required, vlp]),
    ] {
        let normalized = normalize_graph_row_query(&query).unwrap();
        assert_eq!(
            normalized.binding_schema.slots()[0].kind,
            GraphBindingSlotKind::Node
        );
    }
}

#[test]
fn graph_row_edge_pull_eq44_1_2_3_9_10_12_equivalence_and_reopen() {
    let (dir, engine) = graph_row_test_engine();
    let sources = (0..6)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44Source",
                &format!("eq44-source-{index}"),
                &[("keep", PropValue::Bool(index % 2 == 0))],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..6)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44Target",
                &format!("eq44-target-{index}"),
                &[("keep", PropValue::Bool(index % 2 == 0))],
            )
        })
        .collect::<Vec<_>>();
    for index in (0..6).rev() {
        insert_graph_row_edge(
            &engine,
            sources[index],
            targets[5 - index],
            "EQ44_PRIMARY",
            &[("rank", PropValue::Int(index as i64))],
        );
    }
    insert_graph_row_edge(
        &engine,
        sources[0],
        sources[0],
        "EQ44_PRIMARY",
        &[("rank", PropValue::Int(6))],
    );

    let mut filtered = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["EQ44_PRIMARY".to_string()],
            filter: Some(EdgeFilterExpr::PropertyRange {
                key: "rank".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(2))),
                upper: None,
            }),
        })],
    );
    filtered.options.allow_full_scan = false;
    filtered.page.limit = 4;
    filtered.return_items = Some(vec![
        graph_return_binding("source", GraphReturnProjection::IdOnly),
        graph_return_binding("edge", GraphReturnProjection::IdOnly),
        graph_return_binding("target", GraphReturnProjection::IdOnly),
    ]);
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &filtered,
        &[1, 3, 256, usize::MAX],
    );

    let mut label_only = filtered.clone();
    let GraphPatternPiece::Edge(label_edge) = &mut label_only.pieces[0] else {
        unreachable!()
    };
    label_edge.filter = None;
    graph_row_chunk_assert_forced_equivalence(&engine, &label_only, &[1, 7, usize::MAX]);

    let mut unlabeled = label_only.clone();
    let GraphPatternPiece::Edge(unlabeled_edge) = &mut unlabeled.pieces[0] else {
        unreachable!()
    };
    unlabeled_edge.label_filter.clear();
    unlabeled.options.allow_full_scan = true;
    graph_row_chunk_assert_forced_equivalence(&engine, &unlabeled, &[1, 7, usize::MAX]);

    let mut both = label_only.clone();
    let GraphPatternPiece::Edge(both_edge) = &mut both.pieces[0] else {
        unreachable!()
    };
    both_edge.direction = Direction::Both;
    graph_row_chunk_assert_forced_equivalence(&engine, &both, &[1, 2, 7, 256]);

    let mut residual_order = label_only.clone();
    residual_order.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("target", "keep")),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::Bool(true)),
    });
    residual_order.order_by = vec![
        GraphOrderItem {
            expr: graph_prop("edge", "rank"),
            direction: GraphOrderDirection::Desc,
        },
        GraphOrderItem {
            expr: GraphExpr::Function {
                name: GraphFunction::Id,
                args: vec![GraphExpr::Binding("source".to_string())],
            },
            direction: GraphOrderDirection::Asc,
        },
    ];
    residual_order.page.skip = 1;
    residual_order.page.limit = 2;
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &residual_order,
        &[1, 7, 256, usize::MAX],
    );

    let mut endpoint_filtered = label_only.clone();
    endpoint_filtered.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "keep".to_string(),
        value: PropValue::Bool(true),
    });
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &endpoint_filtered,
        &[1, 3, usize::MAX],
    );

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_assert_forced_equivalence(
        &reopened,
        &filtered,
        &[1, 3, 256, usize::MAX],
    );
    graph_row_chunk_assert_forced_equivalence(&reopened, &both, &[1, 2, 7, 256]);
    graph_row_chunk_assert_forced_equivalence(
        &reopened,
        &residual_order,
        &[1, 7, 256, usize::MAX],
    );
    graph_row_chunk_assert_forced_equivalence(
        &reopened,
        &endpoint_filtered,
        &[1, 3, usize::MAX],
    );
}

#[test]
fn graph_row_edge_pull_eq44_4_6_7_8_11_suffix_and_branch_equivalence() {
    let (dir, engine) = graph_row_test_engine();
    let sources = (0..5)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44BranchSource",
                &format!("eq44-branch-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let middles = (0..5)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44BranchMiddle",
                &format!("eq44-branch-middle-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let targets = (0..5)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44BranchTarget",
                &format!("eq44-branch-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    for index in 0..5 {
        insert_graph_row_edge(
            &engine,
            sources[index],
            middles[index],
            "EQ44_BRANCH_A",
            &[],
        );
        insert_graph_row_edge(
            &engine,
            middles[index],
            targets[index],
            "EQ44_SECOND_HOP",
            &[("status", PropValue::String("hot".to_string()))],
        );
        if index < 3 {
            let doc = insert_graph_row_node(
                &engine,
                "Eq44BranchDoc",
                &format!("eq44-branch-doc-{index}"),
                &[],
            );
            insert_graph_row_edge(
                &engine,
                middles[index],
                doc,
                "EQ44_OPTIONAL",
                &[],
            );
        }
        if index < 2 {
            insert_graph_row_edge(
                &engine,
                sources[index],
                targets[4 - index],
                "EQ44_BRANCH_B",
                &[],
            );
        }
    }
    let empty_seed = insert_graph_row_edge(
        &engine,
        sources[0],
        targets[0],
        "EQ44_BRANCH_EMPTY",
        &[],
    );
    engine.delete_edge(empty_seed).unwrap();

    let mut multi_label = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec![
                "EQ44_BRANCH_EMPTY".to_string(),
                "EQ44_BRANCH_B".to_string(),
                "EQ44_BRANCH_A".to_string(),
            ],
            filter: None,
        })],
    );
    multi_label.options.allow_full_scan = false;
    multi_label.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);
    graph_row_chunk_assert_forced_equivalence(&engine, &multi_label, &[1, 3, usize::MAX]);
    multi_label.options.include_plan = true;
    reset_prepared_edge_source_test_snapshot();
    engine.reset_query_execution_counters_for_test();
    let multi_label_one = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&multi_label).unwrap()
    });
    let multi_label_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(multi_label_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(multi_label_counters.graph_row_delegated_edge_queries, 8);
    assert_eq!(multi_label_counters.graph_row_delegated_verified_candidates, 7);
    assert_eq!(multi_label_counters.graph_row_chunks_executed, 7);
    let prepared_snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(prepared_snapshot.branch_plans, 3);
    assert_eq!(prepared_snapshot.preparations, 3);
    let multi_label_plan = format!("{:?}", multi_label_one.plan.unwrap());
    assert!(multi_label_plan.contains("source_pulls=8"), "{multi_label_plan}");

    let repeated_source = insert_graph_row_node(
        &engine,
        "Eq44BranchSource",
        "eq44-branch-source-repeated",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        repeated_source,
        middles[0],
        "EQ44_BRANCH_A",
        &[],
    );

    let mut two_hop = graph_query(
        &["source", "middle", "target"],
        vec![
            graph_edge_with_label(Some("seed"), "source", "middle", "EQ44_BRANCH_A"),
            graph_edge_with_label(
                Some("fanout"),
                "middle",
                "target",
                "EQ44_SECOND_HOP",
            ),
        ],
    );
    two_hop.options.allow_full_scan = false;
    graph_row_chunk_assert_forced_equivalence(&engine, &two_hop, &[1, 3, 256]);

    let mut optional = graph_query(
        &["source", "middle", "doc"],
        vec![
            graph_edge_with_label(Some("seed"), "source", "middle", "EQ44_BRANCH_A"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "middle",
                    "doc",
                    "EQ44_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    optional.options.allow_full_scan = false;
    graph_row_chunk_assert_forced_equivalence(&engine, &optional, &[1, 3, 256]);
    let optional_oracle = graph_row_chunk_runtime_once_result(&engine, &optional);
    assert_eq!(optional_oracle.rows.len(), 6);
    let doc_column = optional_oracle
        .columns
        .iter()
        .position(|column| column == "doc")
        .unwrap();
    assert_eq!(
        optional_oracle
            .rows
            .iter()
            .filter(|row| row.values[doc_column] == GraphValue::Null)
            .count(),
        2
    );
    engine.reset_query_execution_counters_for_test();
    with_graph_row_test_chunk_override(1, || engine.query_graph_rows(&optional).unwrap());
    let optional_cache = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(optional_cache.graph_row_edge_pull_executions, 1);
    assert!(optional_cache.graph_row_optional_group_cache_hits > 0);
    assert!(optional_cache.graph_row_result_cache_units_peak > 0);
    assert_eq!(optional_cache.graph_row_node_verification_cache_hits, 0);

    let mut later_empty_optional = graph_query(
        &["source", "middle", "doc"],
        vec![
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("seed".to_string()),
                from_alias: "source".to_string(),
                to_alias: "middle".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec![
                    "EQ44_BRANCH_EMPTY".to_string(),
                    "EQ44_BRANCH_B".to_string(),
                    "EQ44_BRANCH_A".to_string(),
                ],
                filter: None,
            }),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "middle",
                    "doc",
                    "EQ44_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    later_empty_optional.options.allow_full_scan = false;
    later_empty_optional.options.include_plan = true;
    engine.reset_query_execution_counters_for_test();
    let later_empty_result = with_graph_row_test_chunk_override(usize::MAX, || {
        engine.query_graph_rows(&later_empty_optional).unwrap()
    });
    let later_empty_plan = format!("{:?}", later_empty_result.plan.unwrap());
    let later_empty_cache = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(later_empty_cache.graph_row_edge_pull_executions, 1);
    assert!(later_empty_cache.graph_row_result_cache_units_peak > 0);
    assert!(later_empty_plan.contains("source_pulls=3"), "{later_empty_plan}");

    let uncorrelated_source =
        insert_graph_row_node(&engine, "Eq44Uncorrelated", "eq44-uncorrelated-source", &[]);
    let uncorrelated_target =
        insert_graph_row_node(&engine, "Eq44Uncorrelated", "eq44-uncorrelated-target", &[]);
    insert_graph_row_edge(
        &engine,
        uncorrelated_source,
        uncorrelated_target,
        "EQ44_UNCORRELATED_OPTIONAL",
        &[],
    );
    let mut uncorrelated_optional = graph_query(
        &["source", "middle", "optional_source", "optional_target"],
        vec![
            graph_edge_with_label(Some("seed"), "source", "middle", "EQ44_BRANCH_A"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "optional_source",
                    "optional_target",
                    "EQ44_UNCORRELATED_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    uncorrelated_optional.options.allow_full_scan = true;
    graph_row_chunk_assert_forced_equivalence(
        &engine,
        &uncorrelated_optional,
        &[1, 3, 256],
    );
    assert_eq!(
        graph_row_chunk_runtime_once_result(&engine, &uncorrelated_optional)
            .rows
            .len(),
        6
    );

    let mut vlp = graph_vlp(Some("path"), None, "middle", "target", 1, 3);
    let GraphPatternPiece::VariableLength(vlp_pattern) = &mut vlp else {
        unreachable!()
    };
    vlp_pattern.label_filter = vec!["EQ44_SECOND_HOP".to_string()];
    let mut with_vlp = graph_query(
        &["source", "middle", "target"],
        vec![
            graph_edge_with_label(Some("seed"), "source", "middle", "EQ44_BRANCH_A"),
            vlp,
        ],
    );
    with_vlp.options.allow_full_scan = false;
    graph_row_chunk_assert_forced_equivalence(&engine, &with_vlp, &[1, 2, 7]);
    engine.reset_query_execution_counters_for_test();
    with_graph_row_test_chunk_override(1, || engine.query_graph_rows(&with_vlp).unwrap());
    let vlp_cache = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(vlp_cache.graph_row_edge_pull_executions, 1);
    assert!(vlp_cache.graph_row_vlp_cross_chunk_cache_hits > 0);
    assert!(vlp_cache.graph_row_result_cache_units_peak > 0);
    assert_eq!(vlp_cache.graph_row_node_verification_cache_hits, 0);

    let mut one_hop_vlp = with_vlp.clone();
    let GraphPatternPiece::VariableLength(one_hop) = &mut one_hop_vlp.pieces[1] else {
        unreachable!()
    };
    one_hop.max_hops = 1;
    one_hop.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    graph_row_chunk_assert_forced_equivalence(&engine, &one_hop_vlp, &[1, 2, 7]);
    assert_eq!(
        graph_row_chunk_runtime_once_result(&engine, &one_hop_vlp)
            .rows
            .len(),
        6
    );

    let mut reverse_vlp = one_hop_vlp.clone();
    let GraphPatternPiece::VariableLength(reverse) = &mut reverse_vlp.pieces[1] else {
        unreachable!()
    };
    reverse.from_alias = "target".to_string();
    reverse.to_alias = "middle".to_string();
    reverse.direction = Direction::Incoming;
    graph_row_chunk_assert_forced_equivalence(&engine, &reverse_vlp, &[1, 2, 7]);
    assert_eq!(
        graph_row_chunk_runtime_once_result(&engine, &reverse_vlp)
            .rows
            .len(),
        6
    );

    let mut path_cap = with_vlp.clone();
    path_cap.options.max_paths_per_start = 0;
    engine.reset_query_execution_counters_for_test();
    let path_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&path_cap).unwrap_err()
    });
    assert!(path_error
        .to_string()
        .contains("max_paths_per_start exceeded configured cap 0"));
    let path_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(path_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(path_counters.graph_row_chunk_cap_retries, 0);

    let lead_source = insert_graph_row_node(&engine, "Eq44Lead", "eq44-lead-source", &[]);
    let lead_target = insert_graph_row_node(&engine, "Eq44Lead", "eq44-lead-target", &[]);
    let lead_doc = insert_graph_row_node(&engine, "Eq44Lead", "eq44-lead-doc", &[]);
    insert_graph_row_edge(
        &engine,
        lead_source,
        lead_target,
        "EQ44_LEADING_EDGE",
        &[],
    );
    insert_graph_row_edge(
        &engine,
        lead_target,
        lead_doc,
        "EQ44_LEADING_OPTIONAL",
        &[],
    );
    for index in 0..5 {
        insert_graph_row_edge(
            &engine,
            sources[index],
            targets[index],
            "EQ44_LATER_REQUIRED",
            &[],
        );
    }
    let mut barrier = graph_query(
        &["lead_source", "lead_target", "lead_doc", "later_source", "later_target"],
        vec![
            graph_edge_with_label(
                Some("lead"),
                "lead_source",
                "lead_target",
                "EQ44_LEADING_EDGE",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "lead_target",
                    "lead_doc",
                    "EQ44_LEADING_OPTIONAL",
                )],
                None,
            ),
            graph_edge_with_label(
                Some("later"),
                "later_source",
                "later_target",
                "EQ44_LATER_REQUIRED",
            ),
        ],
    );
    barrier.options.allow_full_scan = false;
    graph_row_chunk_assert_forced_equivalence(&engine, &barrier, &[1, 3, 7]);

    let mut leading_vlp = graph_vlp(
        Some("lead_path"),
        None,
        "lead_target",
        "lead_doc",
        1,
        1,
    );
    let GraphPatternPiece::VariableLength(leading_vlp_pattern) = &mut leading_vlp else {
        unreachable!()
    };
    leading_vlp_pattern.label_filter = vec!["EQ44_LEADING_OPTIONAL".to_string()];
    let mut vlp_barrier = graph_query(
        &["lead_source", "lead_target", "lead_doc", "later_source", "later_target"],
        vec![
            graph_edge_with_label(
                Some("lead"),
                "lead_source",
                "lead_target",
                "EQ44_LEADING_EDGE",
            ),
            leading_vlp,
            graph_edge_with_label(
                Some("later"),
                "later_source",
                "later_target",
                "EQ44_LATER_REQUIRED",
            ),
        ],
    );
    vlp_barrier.options.allow_full_scan = false;
    graph_row_chunk_assert_forced_equivalence(&engine, &vlp_barrier, &[1, 3, 7]);

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    for query in [
        &optional,
        &uncorrelated_optional,
        &one_hop_vlp,
        &reverse_vlp,
        &barrier,
        &vlp_barrier,
    ] {
        graph_row_chunk_assert_forced_equivalence(&reopened, query, &[1, 3, 7]);
    }
}

#[test]
fn graph_row_edge_pull_eq44_5_raw_ids_preserve_verification_and_counters() {
    let (dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Eq44Raw", "eq44-raw-source", &[]);
    let targets = (0..4)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44Raw",
                &format!("eq44-raw-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let edge_ids = targets
        .iter()
        .map(|target| insert_graph_row_edge(&engine, source, *target, "EQ44_RAW", &[]))
        .collect::<Vec<_>>();
    engine.delete_edge(edge_ids[1]).unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EQ44_RAW",
        )],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding(
        "edge",
        GraphReturnProjection::IdOnly,
    )]);
    let mut normalized = normalize_graph_row_query(&query).unwrap();
    normalized.edge_id_constraints.insert(
        "edge".to_string(),
        vec![edge_ids[3], u64::MAX, edge_ids[0], edge_ids[1], edge_ids[3]],
    );
    normalized.at_epoch = Some(now_millis());

    let assert_view = |engine: &DatabaseEngine| {
        let view = engine.published_state().view.clone();
        let run = |reason| {
            let cursor_state = graph_row_prepare_cursor_state(
                &normalized.page,
                normalized.at_epoch,
                &normalized.options,
            )
            .unwrap();
            view.query_graph_rows_outcome_with_source_policy(&normalized, cursor_state, reason)
                .unwrap()
                .value
        };
        let oracle = run(Some(GraphRowRuntimeOnceReason::NonNodeInitialDriver));
        assert_eq!(graph_row_single_u64_column(oracle.clone()), vec![edge_ids[0], edge_ids[3]]);
        for size in [1, 2, 7, usize::MAX] {
            engine.reset_query_execution_counters_for_test();
            let actual = with_graph_row_test_chunk_override(size, || run(None));
            graph_row_chunk_assert_page_parity(&actual, &oracle, &format!("raw size {size}"));
            let counters = engine.query_execution_counter_snapshot_for_test();
            assert_eq!(counters.graph_row_edge_pull_executions, 1);
            assert_eq!(counters.graph_row_delegated_edge_queries, 0);
            assert_eq!(counters.graph_row_delegated_verified_candidates, 0);
            let plan = format!("{:?}", actual.plan.unwrap());
            assert!(plan.contains("materialized_source=EdgePullExplicitIds"), "{plan}");
            if size == 1 {
                assert_eq!(counters.graph_row_chunks_executed, 4);
                assert!(plan.contains("source_pulls=4; successful_leaves=4"), "{plan}");
                assert!(plan.contains("source_rows=4"), "{plan}");
            }
        }
        for page_limit in [1, 2, 7, 100] {
            for size in [1, 2, 7, usize::MAX] {
                let mut actual_query = normalized.clone();
                let mut oracle_query = normalized.clone();
                actual_query.page.limit = page_limit;
                oracle_query.page.limit = page_limit;
                loop {
                    let run_page = |query: &NormalizedGraphRowQuery, reason| {
                        let cursor_state = graph_row_prepare_cursor_state(
                            &query.page,
                            query.at_epoch,
                            &query.options,
                        )
                        .unwrap();
                        view.query_graph_rows_outcome_with_source_policy(
                            query,
                            cursor_state,
                            reason,
                        )
                        .unwrap()
                        .value
                    };
                    let oracle = run_page(
                        &oracle_query,
                        Some(GraphRowRuntimeOnceReason::NonNodeInitialDriver),
                    );
                    let actual = with_graph_row_test_chunk_override(size, || {
                        run_page(&actual_query, None)
                    });
                    graph_row_chunk_assert_page_parity(
                        &actual,
                        &oracle,
                        &format!("raw page_limit {page_limit}, size {size}"),
                    );
                    match oracle.next_cursor {
                        Some(cursor) => {
                            actual_query.page.cursor = Some(cursor.clone());
                            oracle_query.page.cursor = Some(cursor);
                        }
                        None => break,
                    }
                }
            }
        }
    };
    assert_view(&engine);

    let mut raw_before_empty_query = query.clone();
    let GraphPatternPiece::Edge(raw_before_empty_edge) = &mut raw_before_empty_query.pieces[0]
    else {
        unreachable!()
    };
    raw_before_empty_edge.filter = Some(EdgeFilterExpr::PropertyIn {
        key: "missing".to_string(),
        values: Vec::new(),
    });
    raw_before_empty_query.options.max_frontier = 0;
    let mut raw_before_empty = normalize_graph_row_query(&raw_before_empty_query).unwrap();
    raw_before_empty
        .edge_id_constraints
        .insert("edge".to_string(), vec![edge_ids[0]]);
    let cursor_state = graph_row_prepare_cursor_state(
        &raw_before_empty.page,
        raw_before_empty.at_epoch,
        &raw_before_empty.options,
    )
    .unwrap();
    engine.reset_query_execution_counters_for_test();
    let raw_before_empty_error = match engine
        .published_state()
        .view
        .query_graph_rows_outcome(&raw_before_empty, cursor_state)
    {
        Ok(_) => panic!("RawIds must take precedence over the static EmptyResult source"),
        Err(error) => error,
    };
    assert_eq!(
        raw_before_empty_error.to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 0"
    );
    let precedence_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(precedence_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(precedence_counters.graph_row_delegated_edge_queries, 0);

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    assert_view(&reopened);
}

#[test]
fn graph_row_edge_pull_empty_result_frontier_zero_and_no_explain_allocations() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Eq44Empty", "eq44-empty-source", &[]);
    let target = insert_graph_row_node(&engine, "Eq44Empty", "eq44-empty-target", &[]);
    insert_graph_row_edge(&engine, source, target, "EQ44_EMPTY", &[]);

    let mut empty = graph_query(
        &["source", "target"],
        vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["EQ44_EMPTY".to_string()],
            filter: Some(EdgeFilterExpr::PropertyIn {
                key: "missing".to_string(),
                values: Vec::new(),
            }),
        })],
    );
    empty.options.allow_full_scan = false;
    empty.options.include_plan = true;
    engine.reset_query_execution_counters_for_test();
    let empty_result = engine.query_graph_rows(&empty).unwrap();
    assert!(empty_result.rows.is_empty());
    let empty_plan = format!("{:?}", empty_result.plan.unwrap());
    assert!(empty_plan.contains("source=EdgePull{edge=alias:edge}"), "{empty_plan}");
    assert!(empty_plan.contains("materialized_source=EmptyResult"), "{empty_plan}");
    assert!(empty_plan.contains("source_pulls=0; successful_leaves=0"), "{empty_plan}");
    let empty_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(empty_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(empty_counters.graph_row_chunks_executed, 0);
    assert_eq!(empty_counters.graph_row_delegated_edge_queries, 0);

    let mut frontier_zero = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EQ44_EMPTY",
        )],
    );
    frontier_zero.options.allow_full_scan = false;
    frontier_zero.options.max_frontier = 0;
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_graph_rows(&frontier_zero).unwrap_err().to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 0"
    );
    let zero_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(zero_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(zero_counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(zero_counters.graph_row_delegated_verified_candidates, 1);
    assert_eq!(zero_counters.graph_row_chunks_executed, 0);

    let mut no_explain = frontier_zero;
    no_explain.options.max_frontier = GraphQueryOptions::default().max_frontier;
    no_explain.options.include_plan = false;
    let scratch_allocs = graph_row_test_scratch_trace_allocs(|| {
        engine.query_graph_rows(&no_explain).unwrap();
    });
    assert_eq!(scratch_allocs, 0);
}

#[test]
fn graph_row_edge_pull_default_caps_stream_200k_raw_candidates() {
    const CANDIDATE_COUNT: usize = 200_000;

    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Eq44LargeRaw", "eq44-large-raw-source", &[]);
    let target = insert_graph_row_node(&engine, "Eq44LargeRaw", "eq44-large-raw-target", &[]);
    let edge = insert_graph_row_edge(&engine, source, target, "EQ44_LARGE_RAW", &[]);
    engine.delete_edge(edge).unwrap();

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EQ44_LARGE_RAW",
        )],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    let mut normalized = normalize_graph_row_query(&query).unwrap();
    normalized.edge_id_constraints.insert(
        "edge".to_string(),
        (1_000_000..1_000_000 + CANDIDATE_COUNT as u64).collect(),
    );
    let cursor_state = graph_row_prepare_cursor_state(
        &normalized.page,
        normalized.at_epoch,
        &normalized.options,
    )
    .unwrap();

    engine.reset_query_execution_counters_for_test();
    let result = engine
        .published_state()
        .view
        .query_graph_rows_outcome(&normalized, cursor_state)
        .unwrap()
        .value;
    assert!(result.rows.is_empty());
    let plan = format!("{:?}", result.plan.unwrap());
    assert!(
        plan.contains("source_pulls=49; successful_leaves=49"),
        "{plan}"
    );
    assert!(
        plan.contains("source_rows=200000; produced_rows=0"),
        "{plan}"
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_edge_pull_executions, 1);
    assert_eq!(counters.graph_row_chunks_executed, 49);
    assert_eq!(counters.graph_row_delegated_edge_queries, 0);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 0);
    assert_eq!(counters.graph_row_chunk_cap_retries, 0);
}

#[test]
fn graph_row_edge_pull_pull_and_sink_errors_preserve_exact_accounting() {
    let (_dir, engine) = graph_row_test_engine();
    let source = insert_graph_row_node(&engine, "Eq44Error", "eq44-error-source", &[]);
    let target = insert_graph_row_node(&engine, "Eq44Error", "eq44-error-target", &[]);
    insert_graph_row_edge(&engine, source, target, "EQ44_ERROR", &[]);

    let mut edge_pull = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EQ44_ERROR",
        )],
    );
    edge_pull.options.allow_full_scan = false;

    engine.reset_query_execution_counters_for_test();
    let pull_error = with_graph_row_test_prepared_pull_failure(|| {
        engine.query_graph_rows(&edge_pull).unwrap_err()
    });
    assert_eq!(
        pull_error.to_string(),
        "invalid operation: injected graph-row prepared edge pull failure"
    );
    let pull = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(pull.graph_row_edge_pull_executions, 1);
    assert_eq!(pull.graph_row_delegated_edge_queries, 1);
    assert_eq!(pull.graph_row_delegated_verified_candidates, 0);
    assert_eq!(pull.graph_row_chunks_executed, 0);
    assert_eq!(pull.graph_row_chunk_cap_retries, 0);

    engine.reset_query_execution_counters_for_test();
    let edge_sink_error = with_graph_row_test_sink_failure(|| {
        engine.query_graph_rows(&edge_pull).unwrap_err()
    });
    assert_eq!(
        edge_sink_error.to_string(),
        "invalid operation: injected graph-row production sink failure"
    );
    let edge_sink = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(edge_sink.graph_row_edge_pull_executions, 1);
    assert_eq!(edge_sink.graph_row_delegated_edge_queries, 1);
    assert_eq!(edge_sink.graph_row_delegated_verified_candidates, 1);
    assert_eq!(edge_sink.graph_row_chunks_executed, 1);
    assert_eq!(edge_sink.graph_row_chunk_cap_retries, 0);

    let mut anchor_pull = edge_pull;
    anchor_pull.nodes[0].ids = vec![source];
    engine.reset_query_execution_counters_for_test();
    let anchor_sink_error = with_graph_row_test_sink_failure(|| {
        engine.query_graph_rows(&anchor_pull).unwrap_err()
    });
    assert_eq!(
        anchor_sink_error.to_string(),
        "invalid operation: injected graph-row production sink failure"
    );
    let anchor_sink = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(anchor_sink.graph_row_edge_pull_executions, 0);
    assert_eq!(anchor_sink.graph_row_delegated_edge_queries, 0);
    assert_eq!(anchor_sink.graph_row_chunks_executed, 1);
}

#[test]
fn graph_row_edge_pull_retry_splits_deterministically_and_propagates_one_entry_failure() {
    fn execute_forced_branch_driver(
        engine: &DatabaseEngine,
        query: &GraphRowQuery,
    ) -> Result<(GraphRowProductionOutput, GraphRowExplainTrace), EngineError> {
        let normalized = normalize_graph_row_query(query).unwrap();
        let view = engine.published_state().view.clone();
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let mut physical = view
            .plan_graph_row_physical(&normalized, &runtime, false, true)
            .unwrap();
        physical.initial_driver = GraphRowInitialDriver::Edge {
            edge_index: 1,
            edge_name: "alias:branch".to_string(),
        };
        physical.edge_order = vec![1, 0];
        physical.segments[0].initial_driver = physical.initial_driver.clone();
        physical.segments[0].edge_order = physical.edge_order.clone();
        let cursor_state = graph_row_prepare_cursor_state(
            &normalized.page,
            normalized.at_epoch,
            &normalized.options,
        )
        .unwrap();
        let effective_at_epoch = cursor_state.effective_at_epoch;
        let policy_cutoffs = view.query_policy_cutoffs();
        let mut trace = GraphRowExplainTrace::default();
        let output = with_graph_row_test_chunk_override(2, || {
            view.graph_row_execute_chunked(
                &normalized,
                &runtime,
                &physical,
                None,
                &cursor_state,
                0,
                true,
                effective_at_epoch,
                policy_cutoffs.as_ref(),
                Some(&mut trace),
            )
        })?;
        Ok((output, trace))
    }

    let (_dir, engine) = graph_row_test_engine();
    let sources = (0..6)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44RetrySource",
                &format!("eq44-retry-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let middles = (0..2)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44RetryMiddle",
                &format!("eq44-retry-middle-{index}"),
                &[("kind", PropValue::String("good".to_string()))],
            )
        })
        .collect::<Vec<_>>();
    for (index, source) in sources.iter().enumerate() {
        insert_graph_row_edge(
            &engine,
            *source,
            middles[index / 3],
            "EQ44_RETRY_SEED",
            &[],
        );
    }
    let targets = (0..2)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "Eq44RetryTarget",
                &format!("eq44-retry-target-{index}"),
                &[("kind", PropValue::String("good".to_string()))],
            )
        })
        .collect::<Vec<_>>();
    for index in 0..2 {
        insert_graph_row_edge(
            &engine,
            middles[index],
            targets[index],
            "EQ44_RETRY_BRANCH",
            &[],
        );
    }
    let mut query = graph_query(
        &["source", "middle", "target"],
        vec![
            graph_edge_with_label(
                Some("seed"),
                "source",
                "middle",
                "EQ44_RETRY_SEED",
            ),
            graph_edge_with_label(
                Some("branch"),
                "middle",
                "target",
                "EQ44_RETRY_BRANCH",
            ),
        ],
    );
    query.options.allow_full_scan = false;
    query.options.include_plan = true;
    query.options.max_frontier = 4;
    query.options.max_intermediate_bindings = 4;
    query.page.limit = 10;
    query.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("good".to_string()),
    });
    query.nodes[2].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("good".to_string()),
    });
    engine.reset_query_execution_counters_for_test();
    let (result, trace) = execute_forced_branch_driver(&engine, &query).unwrap();
    let GraphRowProductionOutput::CollectAll(result) = result else {
        panic!("forced EdgePull retry test must use collect-all")
    };
    assert_eq!(result.rows.len(), 6);
    let plan = trace
        .plan
        .iter()
        .find(|node| node.kind == "ChunkedRowProductionRuntime")
        .unwrap()
        .detail
        .clone();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_edge_pull_executions, 1);
    assert_eq!(counters.graph_row_chunk_cap_retries, 1, "{plan}");
    assert_eq!(counters.graph_row_chunks_executed, 2);
    assert_eq!(counters.graph_row_delegated_edge_queries, 1);
    assert_eq!(counters.graph_row_delegated_verified_candidates, 2);
    assert_eq!(counters.graph_row_node_verification_resolutions, 4);
    assert_eq!(counters.graph_row_node_verification_cache_hits, 4);
    assert_eq!(counters.graph_row_node_verification_cache_units_peak, 4);
    assert_eq!(
        counters.graph_row_fixed_edge_workspace_growths, 2,
        "the retry halves must reuse capacities grown by the failed parent attempt"
    );
    assert!(plan.contains("source_pulls=1"), "{plan}");
    assert!(plan.contains("source_rows=2"), "{plan}");
    assert!(plan.contains("cap_retries=1"), "{plan}");

    let mut both_query = query.clone();
    let GraphPatternPiece::Edge(both_branch) = &mut both_query.pieces[1] else {
        unreachable!()
    };
    both_branch.direction = Direction::Both;
    engine.reset_query_execution_counters_for_test();
    let (both_result, _) = execute_forced_branch_driver(&engine, &both_query).unwrap();
    let GraphRowProductionOutput::CollectAll(both_result) = both_result else {
        panic!("forced Direction::Both EdgePull retry must use collect-all")
    };
    assert_eq!(both_result.rows.len(), 6);
    let both_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(both_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(both_counters.graph_row_chunk_cap_retries, 1);
    assert_eq!(both_counters.graph_row_chunks_executed, 2);
    assert!(both_counters.graph_row_fixed_edge_workspace_growths <= 2);

    for branch in 0..2 {
        let source = insert_graph_row_node(
            &engine,
            "Eq44RetrySource",
            &format!("eq44-retry-terminal-source-{branch}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            source,
            middles[0],
            "EQ44_RETRY_SEED",
            &[],
        );
    }
    engine.reset_query_execution_counters_for_test();
    let error = match execute_forced_branch_driver(&engine, &query) {
        Ok(_) => panic!("one-entry EdgePull retry must propagate its cap error"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 4"
    );
    let terminal = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(terminal.graph_row_edge_pull_executions, 1);
    assert_eq!(terminal.graph_row_chunk_cap_retries, 1);
    assert_eq!(terminal.graph_row_chunks_executed, 0);
    assert_eq!(terminal.graph_row_delegated_edge_queries, 1);
    assert_eq!(terminal.graph_row_delegated_verified_candidates, 2);
    assert!(
        terminal.graph_row_fixed_edge_workspace_growths > 0,
        "terminal failed attempts must report scratch growth before cleanup"
    );

    let mut intermediate_query = query;
    intermediate_query.options.max_frontier = 100;
    intermediate_query.options.max_intermediate_bindings = 4;
    engine.reset_query_execution_counters_for_test();
    let intermediate_error = match execute_forced_branch_driver(&engine, &intermediate_query) {
        Ok(_) => panic!("one-entry EdgePull retry must preserve its intermediate cap error"),
        Err(error) => error,
    };
    assert_eq!(
        intermediate_error.to_string(),
        "invalid operation: graph row max_intermediate_bindings exceeded configured cap 4"
    );
    let intermediate = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(intermediate.graph_row_edge_pull_executions, 1);
    assert_eq!(intermediate.graph_row_chunk_cap_retries, 1);
    assert_eq!(intermediate.graph_row_chunks_executed, 0);
    assert_eq!(intermediate.graph_row_delegated_edge_queries, 1);
    assert_eq!(intermediate.graph_row_delegated_verified_candidates, 2);
    assert!(
        intermediate.graph_row_fixed_edge_workspace_growths > 0,
        "intermediate-cap failures must report scratch growth before cleanup"
    );
}

#[test]
fn graph_row_chunk_eq2_eq6_chains_and_required_cartesian_are_equivalent() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    let mut middles = Vec::new();
    let mut targets = Vec::new();
    let mut branches = Vec::new();
    for index in 0..8 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkChainSource",
            &format!("chunk-chain-source-{index}"),
            &[],
        );
        let middle = insert_graph_row_node(
            &engine,
            "ChunkChainMiddle",
            &format!("chunk-chain-middle-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "ChunkChainTarget",
            &format!("chunk-chain-target-{index}"),
            &[],
        );
        let branch = insert_graph_row_node(
            &engine,
            "ChunkChainBranch",
            &format!("chunk-chain-branch-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, middle, "CHUNK_CHAIN_A", &[]);
        insert_graph_row_edge(&engine, middle, target, "CHUNK_CHAIN_B", &[]);
        insert_graph_row_edge(&engine, source, branch, "CHUNK_CHAIN_C", &[]);
        let second_branch = insert_graph_row_node(
            &engine,
            "ChunkChainBranch",
            &format!("chunk-chain-second-branch-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, second_branch, "CHUNK_CHAIN_C", &[]);
        sources.push(source);
        middles.push(middle);
        targets.push(target);
        branches.push(branch);
        branches.push(second_branch);
    }
    let mut query = graph_query(
        &["source", "middle", "target", "branch"],
        vec![
            graph_edge_with_label(Some("a"), "source", "middle", "CHUNK_CHAIN_A"),
            graph_edge_with_label(Some("b"), "middle", "target", "CHUNK_CHAIN_B"),
            graph_edge_with_label(Some("c"), "source", "branch", "CHUNK_CHAIN_C"),
        ],
    );
    query.nodes[0].ids = sources;
    query.page.limit = 2;
    graph_row_chunk_assert_forced_equivalence(&engine, &query, &[2, 7, usize::MAX]);

    let mut mixed_bound = query.clone();
    mixed_bound.nodes[1].ids = middles;
    graph_row_chunk_assert_forced_equivalence(&engine, &mixed_bound, &[2, 7, usize::MAX]);

    let mut reverse_anchor = query.clone();
    reverse_anchor.nodes[0].ids.clear();
    reverse_anchor.nodes[2].ids = targets;
    graph_row_chunk_assert_forced_equivalence(&engine, &reverse_anchor, &[2, 7, usize::MAX]);

    let mut branch_bound = query;
    branch_bound.nodes[3].ids = branches;
    graph_row_chunk_assert_forced_equivalence(&engine, &branch_bound, &[1, 3, usize::MAX]);
}

#[test]
fn graph_row_chunk_eq3_eq4_optional_shapes_are_equivalent() {
    let (dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    let mut docs = Vec::new();
    for index in 0..8 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkOptionalSource",
            &format!("chunk-optional-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "ChunkOptionalTarget",
            &format!("chunk-optional-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "CHUNK_OPTIONAL_REQUIRED", &[]);
        if index % 2 == 0 {
            let doc = insert_graph_row_node(
                &engine,
                "ChunkOptionalDoc",
                &format!("chunk-optional-doc-{index}"),
                &[],
            );
            insert_graph_row_edge(&engine, target, doc, "CHUNK_OPTIONAL_EDGE", &[]);
            let tail = insert_graph_row_node(
                &engine,
                "ChunkOptionalTail",
                &format!("chunk-optional-tail-{index}"),
                &[],
            );
            insert_graph_row_edge(&engine, doc, tail, "CHUNK_OPTIONAL_TAIL", &[]);
            docs.push(doc);
        }
        sources.push(source);
    }
    let shared_target =
        insert_graph_row_node(&engine, "ChunkOptionalTarget", "chunk-optional-shared", &[]);
    let shared_doc =
        insert_graph_row_node(&engine, "ChunkOptionalDoc", "chunk-optional-shared-doc", &[]);
    insert_graph_row_edge(&engine, shared_target, shared_doc, "CHUNK_OPTIONAL_EDGE", &[]);
    for &source in &sources {
        insert_graph_row_edge(
            &engine,
            source,
            shared_target,
            "CHUNK_OPTIONAL_REQUIRED",
            &[],
        );
    }
    let mut correlated = graph_query(
        &["source", "target", "doc"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "source",
                "target",
                "CHUNK_OPTIONAL_REQUIRED",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("optional"),
                    "target",
                    "doc",
                    "CHUNK_OPTIONAL_EDGE",
                )],
                None,
            ),
        ],
    );
    correlated.nodes[0].ids = sources;
    correlated.page.limit = 2;
    graph_row_chunk_assert_forced_equivalence(&engine, &correlated, &[1, 3, 256]);
    correlated.at_epoch = Some(
        engine
            .query_graph_rows(&correlated)
            .unwrap()
            .stats
            .effective_at_epoch,
    );
    engine.reset_query_execution_counters_for_test();
    let admitted = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&correlated).unwrap()
    });
    let admitted_counters = engine.query_execution_counter_snapshot_for_test();
    assert!(admitted_counters.graph_row_optional_group_cache_hits > 0);
    assert!(admitted_counters.graph_row_result_cache_units_peak > 0);
    assert!(
        admitted_counters.graph_row_result_cache_units_peak
            <= correlated.options.max_intermediate_bindings
    );

    let mut no_admit = correlated.clone();
    no_admit.options.max_intermediate_bindings = 2;
    engine.reset_query_execution_counters_for_test();
    let exhausted = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&no_admit).unwrap()
    });
    assert_eq!(exhausted.rows, admitted.rows);
    assert_eq!(exhausted.next_cursor, admitted.next_cursor);
    let exhausted_counters = engine.query_execution_counter_snapshot_for_test();
    assert!(exhausted_counters.graph_row_result_cache_no_admit > 0);
    assert!(exhausted_counters.graph_row_result_cache_units_peak <= 2);

    let mut optional_where = correlated.clone();
    if let GraphPatternPiece::Optional(group) = &mut optional_where.pieces[1] {
        group.where_ = Some(GraphExpr::Binary {
            left: Box::new(GraphExpr::Function {
                name: GraphFunction::Id,
                args: vec![GraphExpr::Binding("doc".to_string())],
            }),
            op: GraphBinaryOp::Gt,
            right: Box::new(GraphExpr::Int(0)),
        });
    }
    graph_row_chunk_assert_forced_equivalence(&engine, &optional_where, &[1, 3, 256]);

    let mut chained_null = correlated.clone();
    chained_null.nodes.push(graph_node("tail"));
    chained_null.pieces.push(graph_optional(
        vec![graph_edge_with_label(
            Some("tail_edge"),
            "doc",
            "tail",
            "CHUNK_OPTIONAL_TAIL",
        )],
        None,
    ));
    graph_row_chunk_assert_forced_equivalence(&engine, &chained_null, &[1, 3, 256]);

    let global_from = insert_graph_row_node(&engine, "ChunkOptionalGlobal", "global-from", &[]);
    let global_to = insert_graph_row_node(&engine, "ChunkOptionalGlobal", "global-to", &[]);
    insert_graph_row_edge(&engine, global_from, global_to, "CHUNK_OPTIONAL_GLOBAL", &[]);
    let mut uncorrelated = correlated.clone();
    uncorrelated.nodes.push(graph_node("global_from"));
    uncorrelated.nodes.push(graph_node("global_to"));
    uncorrelated.pieces.push(graph_optional(
        vec![graph_edge_with_label(
            Some("global_edge"),
            "global_from",
            "global_to",
            "CHUNK_OPTIONAL_GLOBAL",
        )],
        None,
    ));
    graph_row_chunk_assert_forced_equivalence(&engine, &uncorrelated, &[1, 7, usize::MAX]);

    let mut nested = correlated.clone();
    nested.nodes.push(graph_node("tail"));
    nested.pieces[1] = graph_optional(
        vec![
            graph_edge_with_label(
                Some("optional"),
                "target",
                "doc",
                "CHUNK_OPTIONAL_EDGE",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("nested_tail"),
                    "doc",
                    "tail",
                    "CHUNK_OPTIONAL_TAIL",
                )],
                None,
            ),
        ],
        None,
    );
    graph_row_chunk_assert_forced_equivalence(&engine, &nested, &[1, 7, usize::MAX]);

    assert!(!docs.is_empty());
    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_assert_forced_equivalence(&reopened, &optional_where, &[1, 3, 256]);
    graph_row_chunk_assert_forced_equivalence(&reopened, &nested, &[1, 7, usize::MAX]);
}

#[test]
fn graph_row_chunk_eq5_vlp_shapes_are_equivalent() {
    let (dir, engine) = graph_row_test_engine();
    let mut starts = Vec::new();
    let mut middles = Vec::new();
    let mut ends = Vec::new();
    for index in 0..6 {
        let start = insert_graph_row_node(
            &engine,
            "ChunkVlp",
            &format!("chunk-vlp-start-{index}"),
            &[],
        );
        let middle = insert_graph_row_node(
            &engine,
            "ChunkVlp",
            &format!("chunk-vlp-middle-{index}"),
            &[],
        );
        let end = insert_graph_row_node(
            &engine,
            "ChunkVlp",
            &format!("chunk-vlp-end-{index}"),
            &[],
        );
        insert_graph_row_edge(
            &engine,
            start,
            middle,
            "CHUNK_VLP_EDGE",
            &[("status", PropValue::String("hot".to_string()))],
        );
        insert_graph_row_edge(
            &engine,
            middle,
            end,
            "CHUNK_VLP_EDGE",
            &[("status", PropValue::String("hot".to_string()))],
        );
        starts.push(start);
        middles.push(middle);
        ends.push(end);
    }
    let mut query = graph_query(
        &["start", "end"],
        vec![graph_vlp(Some("path"), None, "start", "end", 1, 3)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["CHUNK_VLP_EDGE".to_string()];
    }
    query.nodes[0].ids = starts;
    query.page.limit = 2;
    query.return_items = Some(vec![
        graph_return_binding("start", GraphReturnProjection::IdOnly),
        graph_return_binding("end", GraphReturnProjection::IdOnly),
    ]);
    let mut brute = Vec::new();
    for ((&start, &middle), &end) in query.nodes[0]
        .ids
        .iter()
        .zip(&middles)
        .zip(&ends)
    {
        brute.push(GraphRow {
            values: vec![GraphValue::NodeId(start), GraphValue::NodeId(middle)],
        });
        brute.push(GraphRow {
            values: vec![GraphValue::NodeId(start), GraphValue::NodeId(end)],
        });
    }
    let mut brute_query = query.clone();
    brute_query.page.limit = 100;
    assert_eq!(engine.query_graph_rows(&brute_query).unwrap().rows, brute);
    graph_row_chunk_assert_forced_equivalence(&engine, &query, &[1, 2, 7, 256]);

    let mut one_hop = query.clone();
    if let GraphPatternPiece::VariableLength(path) = &mut one_hop.pieces[0] {
        path.max_hops = 1;
    }
    graph_row_chunk_assert_forced_equivalence(&engine, &one_hop, &[1, 2, 7, 256]);

    let mut filtered = query.clone();
    if let GraphPatternPiece::VariableLength(path) = &mut filtered.pieces[0] {
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        });
    }
    graph_row_chunk_assert_forced_equivalence(&engine, &filtered, &[1, 2, 7, 256]);

    let mut reverse = graph_query(
        &["end", "start"],
        vec![graph_vlp(Some("reverse_path"), None, "end", "start", 1, 3)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut reverse.pieces[0] {
        path.direction = Direction::Incoming;
        path.label_filter = vec!["CHUNK_VLP_EDGE".to_string()];
    }
    reverse.nodes[0].ids = ends;
    reverse.page.limit = 2;
    graph_row_chunk_assert_forced_equivalence(&engine, &reverse, &[1, 2, 7, 256]);

    let loop_node = insert_graph_row_node(&engine, "ChunkVlp", "chunk-vlp-loop", &[]);
    insert_graph_row_edge(&engine, loop_node, loop_node, "CHUNK_VLP_EDGE", &[]);
    let mut both = graph_query(
        &["start", "end"],
        vec![graph_vlp(Some("both_path"), None, "start", "end", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut both.pieces[0] {
        path.direction = Direction::Both;
        path.label_filter = vec!["CHUNK_VLP_EDGE".to_string()];
    }
    both.nodes[0].ids = vec![loop_node];
    both.page.limit = 1;
    graph_row_chunk_assert_forced_equivalence(&engine, &both, &[1, 2, 7, 256]);

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_assert_forced_equivalence(&reopened, &query, &[1, 2, 7, 256]);
    graph_row_chunk_assert_forced_equivalence(&reopened, &both, &[1, 2, 7, 256]);
}

#[test]
fn graph_row_chunk_eq8_no_step_residual_and_order_are_equivalent() {
    let (dir, engine) = graph_row_test_engine();
    let mut ids = Vec::new();
    for index in 0..9 {
        ids.push(insert_graph_row_node(
            &engine,
            "ChunkNoStep",
            &format!("chunk-no-step-{index}"),
            &[("rank", PropValue::Int(index))],
        ));
    }
    let mut residual = graph_query(&["n"], Vec::new());
    residual.nodes[0].ids = ids;
    residual.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("n", "rank")),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Int(2)),
    });
    residual.page.limit = 2;
    graph_row_chunk_assert_forced_equivalence(&engine, &residual, &[1, 7, usize::MAX]);

    let mut ordered = residual.clone();
    ordered.order_by = vec![GraphOrderItem {
        expr: graph_prop("n", "rank"),
        direction: GraphOrderDirection::Desc,
    }];
    graph_row_chunk_assert_forced_equivalence(&engine, &ordered, &[1, 7, usize::MAX]);

    let mut element = ordered.clone();
    element.return_items = Some(vec![graph_return_binding(
        "n",
        GraphReturnProjection::Element(GraphElementProjection::Full),
    )]);
    graph_row_chunk_assert_forced_equivalence(&engine, &element, &[1, 7, usize::MAX]);
    element.output.include_vectors = true;
    graph_row_chunk_assert_forced_equivalence(&engine, &element, &[1, 7, usize::MAX]);

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_assert_forced_equivalence(&reopened, &residual, &[1, 7, usize::MAX]);
    graph_row_chunk_assert_forced_equivalence(&reopened, &ordered, &[1, 7, usize::MAX]);
}

#[test]
fn graph_row_chunk_retry_splits_left_first_and_bounds_components() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    let mut expected = Vec::new();
    for source_index in 0..2 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkRetrySource",
            &format!("chunk-retry-source-{source_index}"),
            &[],
        );
        sources.push(source);
        for target_index in 0..3 {
            let target = insert_graph_row_node(
                &engine,
                "ChunkRetryTarget",
                &format!("chunk-retry-target-{source_index}-{target_index}"),
                &[],
            );
            expected.push(target);
            insert_graph_row_edge(&engine, source, target, "CHUNK_RETRY_EDGE", &[]);
        }
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CHUNK_RETRY_EDGE",
        )],
    );
    query.nodes[0].ids = sources;
    query.page.limit = 10;
    query.options.max_intermediate_bindings = 4;
    query.options.include_plan = true;
    query.options.profile = true;
    query.return_items = Some(vec![graph_return_binding(
        "target",
        GraphReturnProjection::IdOnly,
    )]);

    engine.reset_query_execution_counters_for_test();
    let result = with_graph_row_test_chunk_override(2, || {
        engine.query_graph_rows(&query).unwrap()
    });
    assert_eq!(graph_row_single_u64_column(result.clone()), expected);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunk_cap_retries, 1);
    assert_eq!(counters.graph_row_chunks_executed, 2);
    assert_eq!(counters.graph_row_retry_input_rows_peak, 2);
    assert_eq!(counters.graph_row_successful_leaf_rows_peak, 1);
    assert_eq!(counters.graph_row_page_heap_rows_peak, 6);
    assert!(result.stats.intermediate_bindings_peak <= 4);
    assert!(result.stats.frontier_peak <= 4);
    assert_eq!(result.stats.rows_after_filter, 6);
    assert_eq!(result.stats.rows_seen_for_page, 6);
    assert_eq!(result.stats.rows_returned, 6);
    assert!(result.stats.execution_ns.is_some());
    let explain = format!("{:?}", result.plan.unwrap());
    assert!(explain.contains("cap_retries=1"), "{explain}");
    assert!(explain.contains("successful_leaves=2"), "{explain}");
    assert!(explain.contains("leaf_size_min=1; leaf_size_max=1"), "{explain}");
    assert!(explain.contains("first_leaf_sample=true"), "{explain}");
    assert!(explain.contains("early_exit=false"), "{explain}");
    assert!(explain.contains("cursor_seek=none"), "{explain}");
}

#[test]
fn graph_row_chunk_retry_one_id_floor_preserves_intermediate_and_frontier_errors() {
    let (_dir, engine) = graph_row_test_engine();
    let broad = insert_graph_row_node(&engine, "ChunkFloor", "chunk-floor-broad", &[]);
    let narrow = insert_graph_row_node(&engine, "ChunkFloor", "chunk-floor-narrow", &[]);
    for index in 0..3 {
        let target = insert_graph_row_node(
            &engine,
            "ChunkFloorTarget",
            &format!("chunk-floor-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, broad, target, "CHUNK_FLOOR_EDGE", &[]);
    }
    let narrow_target =
        insert_graph_row_node(&engine, "ChunkFloorTarget", "chunk-floor-narrow-target", &[]);
    insert_graph_row_edge(&engine, narrow, narrow_target, "CHUNK_FLOOR_EDGE", &[]);

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CHUNK_FLOOR_EDGE",
        )],
    );
    query.nodes[0].ids = vec![broad, narrow];
    query.page.limit = 10;
    query.options.max_intermediate_bindings = 2;
    engine.reset_query_execution_counters_for_test();
    let intermediate = with_graph_row_test_chunk_override(2, || {
        engine.query_graph_rows(&query).unwrap_err()
    });
    assert_eq!(
        intermediate.to_string(),
        "invalid operation: graph row max_intermediate_bindings exceeded configured cap 2"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_cap_retries,
        1
    );

    query.options.max_intermediate_bindings = 100;
    query.options.max_frontier = 2;
    engine.reset_query_execution_counters_for_test();
    let frontier = with_graph_row_test_chunk_override(2, || {
        engine.query_graph_rows(&query).unwrap_err()
    });
    assert_eq!(
        frontier.to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 2"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_cap_retries,
        1
    );
}

#[test]
fn graph_row_chunk_anchor_count_above_cap_succeeds_with_bounded_peaks() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for index in 0..10 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkAnchorScale",
            &format!("chunk-anchor-scale-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "ChunkAnchorScaleTarget",
            &format!("chunk-anchor-scale-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "CHUNK_ANCHOR_SCALE_EDGE", &[]);
        sources.push(source);
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CHUNK_ANCHOR_SCALE_EDGE",
        )],
    );
    query.nodes[0].ids = sources;
    query.page.limit = 100;
    query.options.max_intermediate_bindings = 2;
    engine.reset_query_execution_counters_for_test();
    let result = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&query).unwrap()
    });
    assert_eq!(result.rows.len(), 10);
    assert!(result.stats.intermediate_bindings_peak <= 2);
    assert!(result.stats.frontier_peak <= 2);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunks_executed, 10);
    assert_eq!(counters.graph_row_retry_input_rows_peak, 1);
    assert_eq!(counters.graph_row_successful_leaf_rows_peak, 1);
    assert_eq!(counters.graph_row_page_heap_rows_peak, 10);
}

#[test]
fn graph_row_chunk_retry_rolls_back_result_cache_before_committing_leaves() {
    let (_dir, engine) = graph_row_test_engine();
    let shared_target =
        insert_graph_row_node(&engine, "ChunkRollbackTarget", "chunk-rollback-target", &[]);
    let doc = insert_graph_row_node(&engine, "ChunkRollbackDoc", "chunk-rollback-doc", &[]);
    insert_graph_row_edge(
        &engine,
        shared_target,
        doc,
        "CHUNK_ROLLBACK_OPTIONAL",
        &[("status", PropValue::String("hot".to_string()))],
    );
    let mut sources = Vec::new();
    for source_index in 0..2 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkRollbackSource",
            &format!("chunk-rollback-source-{source_index}"),
            &[],
        );
        sources.push(source);
        insert_graph_row_edge(
            &engine,
            source,
            shared_target,
            "CHUNK_ROLLBACK_REQUIRED",
            &[],
        );
        for branch_index in 0..2 {
            let branch = insert_graph_row_node(
                &engine,
                "ChunkRollbackBranch",
                &format!("chunk-rollback-branch-{source_index}-{branch_index}"),
                &[],
            );
            insert_graph_row_edge(
                &engine,
                source,
                branch,
                "CHUNK_ROLLBACK_LATE",
                &[],
            );
        }
    }

    let mut optional = match graph_edge_with_label(
        Some("optional"),
        "target",
        "doc",
        "CHUNK_ROLLBACK_OPTIONAL",
    ) {
        GraphPatternPiece::Edge(edge) => edge,
        _ => unreachable!(),
    };
    optional.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let mut query = graph_query(
        &["source", "target", "doc", "branch"],
        vec![
            graph_edge_with_label(
                Some("required"),
                "source",
                "target",
                "CHUNK_ROLLBACK_REQUIRED",
            ),
            graph_optional(vec![GraphPatternPiece::Edge(optional)], None),
            graph_edge_with_label(
                Some("late"),
                "source",
                "branch",
                "CHUNK_ROLLBACK_LATE",
            ),
        ],
    );
    query.nodes[0].ids = sources;
    query.page.limit = 10;
    query.options.max_intermediate_bindings = 3;
    engine.reset_query_execution_counters_for_test();
    let result = with_graph_row_test_chunk_override(2, || {
        engine.query_graph_rows(&query).unwrap()
    });
    assert_eq!(result.rows.len(), 4);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunk_cap_retries, 1);
    // The failed parent admission is rolled back, the left leaf recomputes and commits,
    // and the right leaf then reuses that committed dependency-key entry.
    assert_eq!(counters.graph_row_delegated_edge_queries, 2);
    assert_eq!(counters.graph_row_optional_group_cache_hits, 1);
    assert!(counters.graph_row_result_cache_units_peak <= 3);
}

#[test]
fn graph_row_chunk_typed_frontier_retry_and_runtime_once_no_retry() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for source_index in 0..2 {
        let source = insert_graph_row_node(
            &engine,
            "ChunkFrontierSource",
            &format!("chunk-frontier-source-{source_index}"),
            &[],
        );
        sources.push(source);
        for target_index in 0..3 {
            let target = insert_graph_row_node(
                &engine,
                "ChunkFrontierTarget",
                &format!("chunk-frontier-target-{source_index}-{target_index}"),
                &[],
            );
            insert_graph_row_edge(&engine, source, target, "CHUNK_FRONTIER_EDGE", &[]);
        }
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CHUNK_FRONTIER_EDGE",
        )],
    );
    query.nodes[0].ids = sources;
    query.page.limit = 10;
    query.options.max_intermediate_bindings = 100;
    query.options.max_frontier = 4;
    engine.reset_query_execution_counters_for_test();
    let result = with_graph_row_test_chunk_override(2, || {
        engine.query_graph_rows(&query).unwrap()
    });
    assert_eq!(result.rows.len(), 6);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunk_cap_retries, 1);
    assert_eq!(counters.graph_row_chunks_executed, 2);

    let mut edge_pull = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CHUNK_FRONTIER_EDGE",
        )],
    );
    edge_pull.options.max_intermediate_bindings = 100;
    edge_pull.options.max_frontier = 4;
    engine.reset_query_execution_counters_for_test();
    let edge_pull_result = engine.query_graph_rows(&edge_pull).unwrap();
    assert_eq!(edge_pull_result.rows.len(), 6);
    assert!(edge_pull_result.stats.frontier_peak <= 4);
    let edge_pull_counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(edge_pull_counters.graph_row_edge_pull_executions, 1);
    assert_eq!(edge_pull_counters.graph_row_chunk_cap_retries, 0);
}

#[test]
fn graph_row_chunk_native_zero_intermediate_cap_is_rejected() {
    let (_dir, engine) = graph_row_test_engine();
    let mut query = graph_query(&["n"], Vec::new());
    query.options.max_intermediate_bindings = 0;
    assert_eq!(
        engine.query_graph_rows(&query).unwrap_err().to_string(),
        "invalid operation: graph row max_intermediate_bindings must be >= 1"
    );
}

#[test]
fn graph_row_chunk_proof_classification_and_fast_path_matrix() {
    let (_dir, engine) = graph_row_test_engine();
    let first = insert_graph_row_node(&engine, "ChunkProof", "chunk-proof-first", &[]);
    let second = insert_graph_row_node(&engine, "ChunkProof", "chunk-proof-second", &[]);
    insert_graph_row_edge(&engine, first, second, "CHUNK_PROOF_EDGE", &[]);

    let mut eligible = graph_query(&["n"], Vec::new());
    eligible.nodes[0] = graph_node_with_label("n", "ChunkProof");
    eligible.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Weight,
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Float(0.0)),
    });
    eligible.options.include_plan = true;
    let eligible_result = engine.query_graph_rows(&eligible).unwrap();
    let eligible_explain = eligible_result.plan.unwrap();
    let eligible_plan = format!("{eligible_explain:?}");
    assert!(eligible_plan.contains("source=AnchorPull{alias=n}"), "{eligible_plan}");
    assert!(eligible_plan.contains("eligibility=eligible"), "{eligible_plan}");
    assert!(eligible_plan.contains("early_exit=false; cursor_seek=none"), "{eligible_plan}");
    assert_eq!(
        eligible_explain
            .row_ops
            .iter()
            .find(|row_op| row_op.kind == "Order")
            .unwrap()
            .detail,
        "explicit_order=false; order_items=0; stable logical row key is always the deterministic tie-breaker; top_k=incremental"
    );
    assert!(eligible_explain
        .row_ops
        .iter()
        .find(|row_op| row_op.kind == "CursorSeek")
        .unwrap()
        .detail
        .ends_with("; anchor_seek=none"));

    let mut no_explain = eligible.clone();
    no_explain.options.include_plan = false;
    let scratch_allocs = graph_row_test_scratch_trace_allocs(|| {
        engine.query_graph_rows(&no_explain).unwrap();
    });
    assert_eq!(scratch_allocs, 0);

    let mut ordered = eligible.clone();
    ordered.order_by = vec![GraphOrderItem {
        expr: GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Id,
        },
        direction: GraphOrderDirection::Desc,
    }];
    let ordered_plan = format!(
        "{:?}",
        engine.query_graph_rows(&ordered).unwrap().plan.unwrap()
    );
    assert!(ordered_plan.contains("eligibility=ordered_query"), "{ordered_plan}");

    let mut later_anchor = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "CHUNK_PROOF_EDGE",
        )],
    );
    later_anchor.nodes[1].ids = vec![second];
    later_anchor.options.include_plan = true;
    let later_plan = format!(
        "{:?}",
        engine
            .query_graph_rows(&later_anchor)
            .unwrap()
            .plan
            .unwrap()
    );
    assert!(
        later_plan.contains("eligibility=anchor_not_first_logical_slot"),
        "{later_plan}"
    );

    let mut edge_pull = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "CHUNK_PROOF_EDGE",
        )],
    );
    edge_pull.options.include_plan = true;
    let edge_pull_plan = format!(
        "{:?}",
        engine
            .query_graph_rows(&edge_pull)
            .unwrap()
            .plan
            .unwrap()
    );
    assert!(
        edge_pull_plan.contains("source=EdgePull{edge=alias:edge}"),
        "{edge_pull_plan}"
    );
    assert!(
        edge_pull_plan.contains("eligibility=edge_id_order_not_logical_prefix"),
        "{edge_pull_plan}"
    );
    assert!(edge_pull_plan.contains("source_pulls=1"), "{edge_pull_plan}");
    assert!(edge_pull_plan.contains("successful_leaves=1"), "{edge_pull_plan}");
    assert!(edge_pull_plan.contains("scheduled_sizes=4096..4096"), "{edge_pull_plan}");
    assert!(
        edge_pull_plan.contains("leaf_size_min=1; leaf_size_max=1; source_rows=1"),
        "{edge_pull_plan}"
    );

    let mut fast = graph_query(&["n"], Vec::new());
    fast.nodes[0] = graph_node_with_label("n", "ChunkProof");
    engine.reset_query_execution_counters_for_test();
    engine.query_graph_rows(&fast).unwrap();
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunks_executed,
        0
    );
}

fn graph_row_chunk_early_exit_assert_identity(
    actual: &GraphRowResult,
    exhaustive: &GraphRowResult,
) {
    assert_eq!(actual.columns, exhaustive.columns);
    assert_eq!(actual.rows, exhaustive.rows);
    assert_eq!(actual.next_cursor, exhaustive.next_cursor);
    assert_eq!(actual.stats.rows_returned, exhaustive.stats.rows_returned);
    assert_eq!(actual.stats.effective_at_epoch, exhaustive.stats.effective_at_epoch);
    assert_eq!(actual.stats.warnings, exhaustive.stats.warnings);
    assert_eq!(
        actual.plan.as_ref().map(|plan| &plan.fingerprint),
        exhaustive.plan.as_ref().map(|plan| &plan.fingerprint)
    );
}

#[test]
fn graph_row_chunk_early_exit_complete_leaf_natural_exhaustion_and_gates() {
    let (dir, engine) = graph_row_test_engine();
    let targets = (0..8)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "EarlyTarget",
                &format!("early-target-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let sources = (0..5)
        .map(|index| {
            insert_graph_row_node(
                &engine,
                "EarlySource",
                &format!("early-source-{index}"),
                &[],
            )
        })
        .collect::<Vec<_>>();
    // The first anchor deliberately emits later-slot atoms in descending ID order.
    for target in [targets[7], targets[6], targets[5]] {
        insert_graph_row_edge(&engine, sources[0], target, "EARLY_EDGE", &[]);
    }
    for (source, target) in sources[1..].iter().zip(targets.iter()) {
        insert_graph_row_edge(&engine, *source, *target, "EARLY_EDGE", &[]);
    }

    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EARLY_EDGE",
        )],
    );
    query.nodes[0].ids = sources.clone();
    query.page.limit = 2;
    query.options.include_plan = true;
    query.return_items = Some(vec![
        graph_return_binding("source", GraphReturnProjection::IdOnly),
        graph_return_binding("target", GraphReturnProjection::IdOnly),
    ]);

    let exhaustive = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&query).unwrap()
    });
    query.at_epoch = Some(exhaustive.stats.effective_at_epoch);
    engine.reset_query_execution_counters_for_test();
    let actual = engine.query_graph_rows(&query).unwrap();
    graph_row_chunk_early_exit_assert_identity(&actual, &exhaustive);
    let plan = format!("{:?}", actual.plan.as_ref().unwrap());
    assert!(plan.contains("eligibility=eligible"), "{plan}");
    assert!(plan.contains("scheduled_sizes=3..3"), "{plan}");
    assert!(plan.contains("source_rows=3; produced_rows=5"), "{plan}");
    assert!(plan.contains("early_exit=true; cursor_seek=none"), "{plan}");
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunk_early_exits, 1);
    assert_eq!(counters.graph_row_chunks_executed, 1);

    let mut exhausted = query.clone();
    exhausted.nodes[0].ids.truncate(3);
    engine.reset_query_execution_counters_for_test();
    let exhausted_result = engine.query_graph_rows(&exhausted).unwrap();
    let exhausted_plan = format!("{:?}", exhausted_result.plan.unwrap());
    assert!(exhausted_plan.contains("scheduled_sizes=3..3"), "{exhausted_plan}");
    assert!(exhausted_plan.contains("early_exit=false"), "{exhausted_plan}");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        0
    );

    let mut no_continuation = query.clone();
    no_continuation.nodes[0].ids = sources[1..3].to_vec();
    let no_continuation_result = engine.query_graph_rows(&no_continuation).unwrap();
    assert_eq!(no_continuation_result.rows.len(), 2);
    assert!(no_continuation_result.next_cursor.is_none());
    let no_continuation_plan = format!("{:?}", no_continuation_result.plan.unwrap());
    assert!(
        no_continuation_plan.contains("source_rows=2; produced_rows=2"),
        "{no_continuation_plan}"
    );
    assert!(
        no_continuation_plan.contains("early_exit=false"),
        "{no_continuation_plan}"
    );

    let mut ordered = query.clone();
    ordered.order_by = vec![GraphOrderItem {
        expr: GraphExpr::NodeField {
            alias: "target".to_string(),
            field: GraphNodeField::Id,
        },
        direction: GraphOrderDirection::Desc,
    }];
    engine.reset_query_execution_counters_for_test();
    let ordered_plan = format!("{:?}", engine.query_graph_rows(&ordered).unwrap().plan.unwrap());
    assert!(ordered_plan.contains("eligibility=ordered_query"), "{ordered_plan}");
    assert!(ordered_plan.contains("early_exit=false"), "{ordered_plan}");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        0
    );

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    let reopened_exhaustive = with_graph_row_test_early_exit_disabled(|| {
        reopened.query_graph_rows(&query).unwrap()
    });
    let reopened_actual = reopened.query_graph_rows(&query).unwrap();
    graph_row_chunk_early_exit_assert_identity(&reopened_actual, &reopened_exhaustive);
    graph_row_chunk_early_exit_assert_identity(&reopened_actual, &actual);
}

#[test]
fn graph_row_chunk_early_exit_proof_gate_runtime_once_stage_and_fast_path_matrix() {
    let (_dir, engine) = graph_row_test_engine();
    let first = insert_graph_row_node(&engine, "EarlyGate", "early-gate-first", &[]);
    let second = insert_graph_row_node(&engine, "EarlyGate", "early-gate-second", &[]);
    insert_graph_row_edge(&engine, first, second, "EARLY_GATE_EDGE", &[]);

    let mut eligible = graph_query(&["n"], Vec::new());
    eligible.nodes[0] = graph_node_with_label("n", "EarlyGate");
    eligible.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Weight,
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Float(0.0)),
    });
    eligible.page.limit = 1;
    eligible.options.include_plan = true;
    let eligible_plan = format!("{:?}", engine.query_graph_rows(&eligible).unwrap().plan.unwrap());
    assert!(eligible_plan.contains("eligibility=eligible"), "{eligible_plan}");

    let mut ordered = eligible.clone();
    ordered.order_by = vec![GraphOrderItem {
        expr: GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Id,
        },
        direction: GraphOrderDirection::Desc,
    }];
    let ordered_plan = format!("{:?}", engine.query_graph_rows(&ordered).unwrap().plan.unwrap());
    assert!(ordered_plan.contains("eligibility=ordered_query"), "{ordered_plan}");
    assert!(ordered_plan.contains("early_exit=false"), "{ordered_plan}");

    let mut later_anchor = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "EARLY_GATE_EDGE",
        )],
    );
    later_anchor.nodes[1].ids = vec![second];
    later_anchor.options.include_plan = true;
    let later_plan = format!(
        "{:?}",
        engine.query_graph_rows(&later_anchor).unwrap().plan.unwrap()
    );
    assert!(
        later_plan.contains("eligibility=anchor_not_first_logical_slot"),
        "{later_plan}"
    );
    assert!(later_plan.contains("early_exit=false"), "{later_plan}");

    let mut edge_pull = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "EARLY_GATE_EDGE",
        )],
    );
    edge_pull.options.include_plan = true;
    let edge_pull_plan = format!(
        "{:?}",
        engine.query_graph_rows(&edge_pull).unwrap().plan.unwrap()
    );
    assert!(edge_pull_plan.contains("eligibility=edge_id_order_not_logical_prefix"));
    assert!(edge_pull_plan.contains("early_exit=false"));
    assert!(edge_pull_plan.contains(
        "result_cache_units=0/65536; cache_no_admit=0; optional_cache_hits=0; \
         vlp_cross_chunk_cache_hits=0"
    ));

    let mut empty_edge_pull = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "EARLY_GATE_UNKNOWN_EDGE_LABEL",
        )],
    );
    empty_edge_pull.options.include_plan = true;
    let empty_plan = format!(
        "{:?}",
        engine
            .query_graph_rows(&empty_edge_pull)
            .unwrap()
            .plan
            .unwrap()
    );
    assert!(
        empty_plan.contains("source=EdgePull{edge=alias:edge}"),
        "{empty_plan}"
    );
    assert!(empty_plan.contains("source=EmptyResult"), "{empty_plan}");
    assert!(empty_plan.contains("eligibility=edge_id_order_not_logical_prefix"));
    assert!(empty_plan.contains("early_exit=false"));

    let normalized = normalize_graph_row_query(&eligible).unwrap();
    let cursor_state = graph_row_prepare_cursor_state(
        &normalized.page,
        normalized.at_epoch,
        &normalized.options,
    )
    .unwrap();
    let stage = engine
        .published_state()
        .view
        .query_graph_rows_outcome_with_source_policy(
            &normalized,
            cursor_state,
            Some(GraphRowRuntimeOnceReason::Stage),
        )
        .unwrap()
        .value;
    let stage_plan = format!("{:?}", stage.plan.unwrap());
    assert!(stage_plan.contains("source=RuntimeOnce{reason=Stage}"), "{stage_plan}");
    assert!(stage_plan.contains("eligibility=stage_sink"), "{stage_plan}");
    assert!(stage_plan.contains("early_exit=false"), "{stage_plan}");

    let mut fast = graph_query(&["n"], Vec::new());
    fast.nodes[0] = graph_node_with_label("n", "EarlyGate");
    engine.reset_query_execution_counters_for_test();
    engine.query_graph_rows(&fast).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunks_executed, 0);
    assert_eq!(counters.graph_row_chunk_early_exits, 0);
}

#[test]
fn graph_row_chunk_early_exit_residual_where_doubles_after_post_filter_misses() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for index in 0..9 {
        let source = insert_graph_row_node(
            &engine,
            "EarlyResidualSource",
            &format!("early-residual-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "EarlyResidualTarget",
            &format!("early-residual-target-{index}"),
            &[("keep", PropValue::Bool((2..=3).contains(&index)))],
        );
        insert_graph_row_edge(&engine, source, target, "EARLY_RESIDUAL_EDGE", &[]);
        sources.push(source);
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EARLY_RESIDUAL_EDGE",
        )],
    );
    query.nodes[0].ids = sources;
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(graph_prop("target", "keep")),
        op: GraphBinaryOp::Eq,
        right: Box::new(GraphExpr::Bool(true)),
    });
    query.page.limit = 1;
    query.options.include_plan = true;

    let exhaustive = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&query).unwrap()
    });
    query.at_epoch = Some(exhaustive.stats.effective_at_epoch);
    engine.reset_query_execution_counters_for_test();
    let actual = engine.query_graph_rows(&query).unwrap();
    graph_row_chunk_early_exit_assert_identity(&actual, &exhaustive);
    let plan = format!("{:?}", actual.plan.as_ref().unwrap());
    assert!(plan.contains("source_pulls=2"), "{plan}");
    assert!(plan.contains("scheduled_sizes=2..4"), "{plan}");
    assert!(plan.contains("source_rows=6; produced_rows=6"), "{plan}");
    assert!(plan.contains("early_exit=true"), "{plan}");
    assert_eq!(actual.stats.rows_after_filter, 2);
    assert_eq!(actual.stats.rows_seen_for_page, 2);
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        1
    );

    let mut skipped = query.clone();
    skipped.page.skip = 1;
    let skipped_exhaustive = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&skipped).unwrap()
    });
    let skipped_actual = engine.query_graph_rows(&skipped).unwrap();
    graph_row_chunk_early_exit_assert_identity(&skipped_actual, &skipped_exhaustive);

    let mut enabled_walk = query.clone();
    let mut exhaustive_walk = query;
    loop {
        let exhaustive_page = with_graph_row_test_early_exit_disabled(|| {
            engine.query_graph_rows(&exhaustive_walk).unwrap()
        });
        let enabled_page = engine.query_graph_rows(&enabled_walk).unwrap();
        graph_row_chunk_early_exit_assert_identity(&enabled_page, &exhaustive_page);
        match exhaustive_page.next_cursor {
            Some(cursor) => {
                enabled_walk.page.cursor = Some(cursor.clone());
                exhaustive_walk.page.cursor = Some(cursor);
            }
            None => break,
        }
    }
}

#[test]
fn graph_row_chunk_early_exit_skips_later_cap_and_retry_sibling_work() {
    let (_dir, engine) = graph_row_test_engine();
    let left = insert_graph_row_node(&engine, "EarlyRetry", "early-retry-left", &[]);
    let right = insert_graph_row_node(&engine, "EarlyRetry", "early-retry-right", &[]);
    for index in 0..2 {
        let target = insert_graph_row_node(
            &engine,
            "EarlyRetryTarget",
            &format!("early-retry-left-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, left, target, "EARLY_RETRY_EDGE", &[]);
    }
    for index in 0..4 {
        let target = insert_graph_row_node(
            &engine,
            "EarlyRetryTarget",
            &format!("early-retry-right-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, right, target, "EARLY_RETRY_EDGE", &[]);
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EARLY_RETRY_EDGE",
        )],
    );
    query.nodes[0].ids = vec![left, right];
    query.page.limit = 1;
    query.options.max_intermediate_bindings = 3;
    query.options.include_plan = true;

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_graph_rows(&query).unwrap();
    let plan = format!("{:?}", result.plan.unwrap());
    assert!(plan.contains("source_rows=2; produced_rows=2"), "{plan}");
    assert!(plan.contains("successful_leaves=1"), "{plan}");
    assert!(plan.contains("cap_retries=1"), "{plan}");
    assert!(plan.contains("early_exit=true"), "{plan}");
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_chunk_early_exits, 1);
    assert_eq!(counters.graph_row_chunk_cap_retries, 1);
    assert_eq!(counters.graph_row_chunks_executed, 1);
    assert_eq!(counters.graph_row_unique_followups_peak, 0);
    assert_eq!(result.stats.rows_after_filter, 2);
    assert_eq!(result.stats.rows_seen_for_page, 2);
    assert_eq!(result.stats.intermediate_bindings_peak, 2);
    assert_eq!(result.stats.frontier_peak, 2);
    assert_eq!(result.stats.paths_enumerated, 0);
    assert!(result.stats.warnings.is_empty());

    let error = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&query).unwrap_err()
    });
    assert_eq!(
        error.to_string(),
        "invalid operation: graph row max_intermediate_bindings exceeded configured cap 3"
    );

    let mut filling_leaf_error = query;
    filling_leaf_error.options.max_intermediate_bindings = 16;
    filling_leaf_error.options.max_frontier = 1;
    engine.reset_query_execution_counters_for_test();
    let error = engine.query_graph_rows(&filling_leaf_error).unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 1"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        0
    );
}

#[test]
fn graph_row_chunk_early_exit_skips_later_non_retryable_error_until_work_is_required() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for index in 0..3 {
        let source = insert_graph_row_node(
            &engine,
            "EarlyLazySource",
            &format!("early-lazy-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "EarlyLazyTarget",
            &format!("early-lazy-target-{index}"),
            &[("divisor", PropValue::Int(if index == 2 { 0 } else { 1 }))],
        );
        insert_graph_row_edge(&engine, source, target, "EARLY_LAZY_EDGE", &[]);
        sources.push(source);
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "EARLY_LAZY_EDGE",
        )],
    );
    query.nodes[0].ids = sources.clone();
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::Binary {
            left: Box::new(GraphExpr::Int(10)),
            op: GraphBinaryOp::Div,
            right: Box::new(graph_prop("target", "divisor")),
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Int(0)),
    });
    query.page.limit = 1;
    query.options.include_plan = true;

    engine.reset_query_execution_counters_for_test();
    let result = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&query).unwrap()
    });
    assert_eq!(result.rows.len(), 1);
    let plan = format!("{:?}", result.plan.unwrap());
    assert!(plan.contains("source_rows=2; produced_rows=2"), "{plan}");
    assert!(plan.contains("early_exit=true"), "{plan}");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_chunk_early_exits,
        1
    );

    let disabled_error = with_graph_row_test_chunk_override(1, || {
        with_graph_row_test_early_exit_disabled(|| engine.query_graph_rows(&query).unwrap_err())
    });
    let expected = "invalid operation: graph row division by zero";
    assert_eq!(disabled_error.to_string(), expected);

    let mut raised_target = query.clone();
    raised_target.page.limit = 10;
    let raised_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&raised_target).unwrap_err()
    });
    assert_eq!(raised_error.to_string(), expected);

    let mut filling_leaf_error = query;
    filling_leaf_error.nodes[0].ids = vec![sources[2]];
    let filling_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&filling_leaf_error).unwrap_err()
    });
    assert_eq!(filling_error.to_string(), expected);
}

#[test]
fn graph_row_chunk_early_exit_optional_vlp_and_no_step_match_disabled_oracle() {
    let (_dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for index in 0..6 {
        let source = insert_graph_row_node(
            &engine,
            "EarlyOracleSource",
            &format!("early-oracle-source-{index}"),
            &[],
        );
        let target = insert_graph_row_node(
            &engine,
            "EarlyOracleTarget",
            &format!("early-oracle-target-{index}"),
            &[],
        );
        let doc = insert_graph_row_node(
            &engine,
            "EarlyOracleDoc",
            &format!("early-oracle-doc-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "EARLY_ORACLE_REQUIRED", &[]);
        insert_graph_row_edge(&engine, target, doc, "EARLY_ORACLE_PATH", &[]);
        if index % 2 == 0 {
            insert_graph_row_edge(&engine, target, doc, "EARLY_ORACLE_OPTIONAL", &[]);
        }
        sources.push(source);
    }

    let mut optional = graph_query(
        &["source", "target", "doc"],
        vec![
            graph_edge_with_label(
                Some("edge"),
                "source",
                "target",
                "EARLY_ORACLE_REQUIRED",
            ),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("opt"),
                    "target",
                    "doc",
                    "EARLY_ORACLE_OPTIONAL",
                )],
                None,
            ),
        ],
    );
    optional.nodes[0].ids = sources.clone();
    optional.page.limit = 1;
    optional.options.include_plan = true;

    let mut vlp = graph_query(
        &["source", "target", "doc"],
        vec![
            graph_edge_with_label(
                Some("edge"),
                "source",
                "target",
                "EARLY_ORACLE_REQUIRED",
            ),
            graph_vlp(Some("path"), None, "target", "doc", 1, 1),
        ],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut vlp.pieces[1] {
        path.label_filter = vec!["EARLY_ORACLE_PATH".to_string()];
    }
    vlp.nodes[0].ids = sources.clone();
    vlp.page.limit = 1;
    vlp.options.include_plan = true;

    let mut no_step = graph_query(&["source"], Vec::new());
    no_step.nodes[0].ids = sources;
    no_step.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::NodeField {
            alias: "source".to_string(),
            field: GraphNodeField::Weight,
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Float(0.0)),
    });
    no_step.page.limit = 1;
    no_step.options.include_plan = true;

    for (name, query) in [("optional", &optional), ("vlp", &vlp), ("no-step", &no_step)] {
        let exhaustive = with_graph_row_test_early_exit_disabled(|| {
            engine.query_graph_rows(query).unwrap()
        });
        let mut pinned = query.clone();
        pinned.at_epoch = Some(exhaustive.stats.effective_at_epoch);
        let exhaustive = with_graph_row_test_early_exit_disabled(|| {
            engine.query_graph_rows(&pinned).unwrap()
        });
        let actual = engine.query_graph_rows(&pinned).unwrap();
        graph_row_chunk_early_exit_assert_identity(&actual, &exhaustive);
        let actual_plan = format!("{:?}", actual.plan.unwrap());
        let exhaustive_plan = format!("{:?}", exhaustive.plan.unwrap());
        assert!(actual_plan.contains("early_exit=true"), "{name}: {actual_plan}");
        assert!(
            exhaustive_plan.contains("early_exit=false"),
            "{name}: {exhaustive_plan}"
        );

        let mut cursor_query = pinned.clone();
        cursor_query.page.cursor = actual.next_cursor.clone();
        while cursor_query.page.cursor.is_some() {
            let cursor_oracle = with_graph_row_test_cursor_seek_disabled(|| {
                with_graph_row_test_early_exit_disabled(|| {
                    engine.query_graph_rows(&cursor_query).unwrap()
                })
            });
            let cursor_actual = engine.query_graph_rows(&cursor_query).unwrap();
            graph_row_chunk_early_exit_assert_identity(&cursor_actual, &cursor_oracle);
            let cursor_plan = format!("{:?}", cursor_actual.plan.as_ref().unwrap());
            assert!(
                cursor_plan.contains("cursor_seek=anchor_ge:"),
                "{name}: {cursor_plan}"
            );
            cursor_query.page.cursor = cursor_actual.next_cursor;
        }
    }

    let initial = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&vlp).unwrap()
    });
    let mut enabled_walk = vlp.clone();
    enabled_walk.at_epoch = Some(initial.stats.effective_at_epoch);
    let mut exhaustive_walk = enabled_walk.clone();
    loop {
        let exhaustive_page = with_graph_row_test_early_exit_disabled(|| {
            engine.query_graph_rows(&exhaustive_walk).unwrap()
        });
        let enabled_page = engine.query_graph_rows(&enabled_walk).unwrap();
        graph_row_chunk_early_exit_assert_identity(&enabled_page, &exhaustive_page);
        match exhaustive_page.next_cursor {
            Some(cursor) => {
                enabled_walk.page.cursor = Some(cursor.clone());
                exhaustive_walk.page.cursor = Some(cursor);
            }
            None => break,
        }
    }
}

fn graph_row_chunk_cursor_seek_assert_page_identity(
    actual: &GraphRowResult,
    oracle: &GraphRowResult,
) {
    assert_eq!(actual.columns, oracle.columns);
    assert_eq!(actual.rows, oracle.rows);
    assert_eq!(actual.next_cursor, oracle.next_cursor);
    assert_eq!(actual.stats.rows_returned, oracle.stats.rows_returned);
    assert_eq!(actual.stats.effective_at_epoch, oracle.stats.effective_at_epoch);
    assert_eq!(actual.stats.warnings, oracle.stats.warnings);
    assert_eq!(
        actual.plan.as_ref().map(|plan| &plan.fingerprint),
        oracle.plan.as_ref().map(|plan| &plan.fingerprint)
    );
}

fn graph_row_chunk_cursor_seek_runtime_metric(plan: &str, name: &str) -> usize {
    let value = plan
        .split_once(&format!("{name}="))
        .unwrap_or_else(|| panic!("missing {name} in {plan}"))
        .1;
    value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("invalid {name} in {plan}"))
}

fn graph_row_chunk_cursor_seek_assert_walk_matrix(
    engine: &DatabaseEngine,
    base: &GraphRowQuery,
    expected_rows: &[GraphRow],
) {
    for limit in [1, 7, 100] {
        for chunk_size in [1, 3, 256, usize::MAX] {
            let mut query = base.clone();
            query.page.limit = limit;
            let mut walked = Vec::new();
            let mut page_index = 0usize;
            loop {
                let oracle = with_graph_row_test_chunk_override(chunk_size, || {
                    with_graph_row_test_cursor_seek_disabled(|| {
                        engine.query_graph_rows(&query).unwrap()
                    })
                });
                engine.reset_query_execution_counters_for_test();
                let actual = with_graph_row_test_chunk_override(chunk_size, || {
                    engine.query_graph_rows(&query).unwrap()
                });
                graph_row_chunk_cursor_seek_assert_page_identity(&actual, &oracle);
                assert_eq!(actual.rows.len(), oracle.rows.len());
                walked.extend(actual.rows.clone());
                let plan = format!("{:?}", actual.plan.as_ref().unwrap());
                let seeks = engine
                    .query_execution_counter_snapshot_for_test()
                    .graph_row_cursor_anchor_seeks;
                if page_index == 0 {
                    assert!(plan.contains("cursor_seek=none"), "{plan}");
                    assert_eq!(seeks, 0);
                } else {
                    assert!(plan.contains("cursor_seek=anchor_ge:"), "{plan}");
                    assert!(plan.contains("anchor_seek=applied"), "{plan}");
                    assert_eq!(seeks, 1);

                    let exhaustive_oracle = with_graph_row_test_chunk_override(chunk_size, || {
                        with_graph_row_test_cursor_seek_disabled(|| {
                            with_graph_row_test_early_exit_disabled(|| {
                                engine.query_graph_rows(&query).unwrap()
                            })
                        })
                    });
                    let exhaustive_actual = with_graph_row_test_chunk_override(chunk_size, || {
                        with_graph_row_test_early_exit_disabled(|| {
                            engine.query_graph_rows(&query).unwrap()
                        })
                    });
                    graph_row_chunk_cursor_seek_assert_page_identity(
                        &exhaustive_actual,
                        &exhaustive_oracle,
                    );
                    assert_eq!(
                        exhaustive_actual.stats.rows_seen_for_page,
                        exhaustive_oracle.stats.rows_seen_for_page
                    );
                }
                page_index = page_index.saturating_add(1);
                match actual.next_cursor {
                    Some(cursor) => query.page.cursor = Some(cursor),
                    None => break,
                }
            }
            assert_eq!(walked, expected_rows, "limit={limit}, chunk={chunk_size}");
        }
    }
}

#[test]
fn graph_row_chunk_cursor_seek_inclusive_mid_anchor_walks_and_reopen() {
    let (dir, engine) = graph_row_test_engine();
    let mut sources = Vec::new();
    for source_index in 0..40 {
        let source = insert_graph_row_node(
            &engine,
            "CursorSeekSource",
            &format!("cursor-seek-source-{source_index}"),
            &[],
        );
        sources.push(source);
        for target_index in 0..3 {
            let target = insert_graph_row_node(
                &engine,
                "CursorSeekTarget",
                &format!("cursor-seek-target-{source_index}-{target_index}"),
                &[],
            );
            insert_graph_row_edge(&engine, source, target, "CURSOR_SEEK_EDGE", &[]);
        }
    }
    let mut base = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CURSOR_SEEK_EDGE",
        )],
    );
    base.nodes[0].ids = sources;
    base.return_items = Some(vec![
        graph_return_binding("source", GraphReturnProjection::IdOnly),
        graph_return_binding("target", GraphReturnProjection::IdOnly),
    ]);
    base.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::NodeField {
            alias: "source".to_string(),
            field: GraphNodeField::Weight,
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Float(0.0)),
    });
    base.options.include_plan = true;
    let mut full_query = base.clone();
    full_query.page.limit = 10_000;
    let full = with_graph_row_test_early_exit_disabled(|| {
        engine.query_graph_rows(&full_query).unwrap()
    });
    base.at_epoch = Some(full.stats.effective_at_epoch);
    let expected_rows = full.rows;
    assert_eq!(expected_rows.len(), 120);
    graph_row_chunk_cursor_seek_assert_walk_matrix(&engine, &base, &expected_rows);

    let mut boundary_query = base.clone();
    boundary_query.page.limit = 1;
    let boundary_first = engine.query_graph_rows(&boundary_query).unwrap();
    boundary_query.page.cursor = boundary_first.next_cursor;
    let boundary_second = engine.query_graph_rows(&boundary_query).unwrap();
    assert_eq!(boundary_first.rows[0].values[0], boundary_second.rows[0].values[0]);
    assert_ne!(boundary_first.rows[0].values[1], boundary_second.rows[0].values[1]);

    let mut deep_query = base.clone();
    deep_query.page.limit = 90;
    let deep_first = engine.query_graph_rows(&deep_query).unwrap();
    deep_query.page.limit = 1;
    deep_query.page.cursor = deep_first.next_cursor;
    let deep_oracle = with_graph_row_test_chunk_override(1, || {
        with_graph_row_test_cursor_seek_disabled(|| engine.query_graph_rows(&deep_query).unwrap())
    });
    engine.reset_query_execution_counters_for_test();
    let deep_actual = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&deep_query).unwrap()
    });
    graph_row_chunk_cursor_seek_assert_page_identity(&deep_actual, &deep_oracle);
    assert_eq!(deep_actual.stats.rows_seen_for_page, deep_oracle.stats.rows_seen_for_page);
    assert!(deep_actual.stats.rows_after_filter < deep_oracle.stats.rows_after_filter);
    assert!(deep_actual.stats.intermediate_bindings_peak <= deep_oracle.stats.intermediate_bindings_peak);
    assert!(deep_actual.stats.frontier_peak <= deep_oracle.stats.frontier_peak);
    assert_eq!(deep_actual.stats.paths_enumerated, 0);
    assert!(deep_actual.stats.warnings.is_empty());
    let actual_plan = format!("{:?}", deep_actual.plan.as_ref().unwrap());
    let oracle_plan = format!("{:?}", deep_oracle.plan.as_ref().unwrap());
    for metric in ["source_pulls", "successful_leaves", "source_rows", "produced_rows"] {
        assert!(
            graph_row_chunk_cursor_seek_runtime_metric(&actual_plan, metric)
                < graph_row_chunk_cursor_seek_runtime_metric(&oracle_plan, metric),
            "metric={metric}; actual={actual_plan}; oracle={oracle_plan}"
        );
    }
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_cursor_anchor_seeks, 1);
    assert_eq!(counters.graph_row_unique_followups_peak, 0);
    assert!(actual_plan.contains("early_exit=true; cursor_seek=anchor_ge:"));

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    graph_row_chunk_cursor_seek_assert_walk_matrix(&reopened, &base, &expected_rows);
}

#[test]
fn graph_row_chunk_cursor_seek_proof_source_validation_and_fast_path_gates() {
    let (_dir, engine) = graph_row_test_engine();
    let first = insert_graph_row_node(&engine, "CursorSeekGate", "cursor-seek-gate-a", &[]);
    let second = insert_graph_row_node(&engine, "CursorSeekGate", "cursor-seek-gate-b", &[]);
    let third = insert_graph_row_node(&engine, "CursorSeekGate", "cursor-seek-gate-c", &[]);
    insert_graph_row_edge(&engine, first, second, "CURSOR_SEEK_GATE_EDGE", &[]);
    insert_graph_row_edge(&engine, third, second, "CURSOR_SEEK_GATE_EDGE", &[]);

    let mut eligible = graph_query(&["n"], Vec::new());
    eligible.nodes[0] = graph_node_with_label("n", "CursorSeekGate");
    eligible.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Weight,
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Float(0.0)),
    });
    eligible.page.limit = 1;
    eligible.options.include_plan = true;
    engine.reset_query_execution_counters_for_test();
    let first_page = engine.query_graph_rows(&eligible).unwrap();
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_cursor_anchor_seeks,
        0
    );

    let mut later_anchor = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "CURSOR_SEEK_GATE_EDGE",
        )],
    );
    later_anchor.nodes[1].ids = vec![second];
    later_anchor.page.limit = 1;
    later_anchor.options.include_plan = true;
    let later_first = engine.query_graph_rows(&later_anchor).unwrap();
    later_anchor.at_epoch = Some(later_first.stats.effective_at_epoch);
    later_anchor.page.cursor = later_first.next_cursor;
    engine.reset_query_execution_counters_for_test();
    let later_plan = format!(
        "{:?}",
        engine.query_graph_rows(&later_anchor).unwrap().plan.unwrap()
    );
    assert!(later_plan.contains("eligibility=anchor_not_first_logical_slot"));
    assert!(later_plan.contains("cursor_seek=none"));
    assert!(later_plan.contains("anchor_seek=ineligible"));
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_cursor_anchor_seeks,
        0
    );
    eligible.at_epoch = Some(first_page.stats.effective_at_epoch);
    eligible.page.cursor = first_page.next_cursor.clone();
    engine.reset_query_execution_counters_for_test();
    let applied = engine.query_graph_rows(&eligible).unwrap();
    let applied_plan = format!("{:?}", applied.plan.unwrap());
    assert!(applied_plan.contains("cursor_seek=anchor_ge:"), "{applied_plan}");
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_cursor_anchor_seeks,
        1
    );

    let normalized = normalize_graph_row_query(&eligible).unwrap();
    let cursor_state = graph_row_prepare_cursor_state(
        &normalized.page,
        normalized.at_epoch,
        &normalized.options,
    )
    .unwrap();
    engine.reset_query_execution_counters_for_test();
    let stage = engine
        .published_state()
        .view
        .query_graph_rows_outcome_with_source_policy(
            &normalized,
            cursor_state,
            Some(GraphRowRuntimeOnceReason::Stage),
        )
        .unwrap()
        .value;
    let stage_plan = format!("{:?}", stage.plan.unwrap());
    assert!(stage_plan.contains("source=RuntimeOnce{reason=Stage}"), "{stage_plan}");
    assert!(stage_plan.contains("cursor_seek=none"), "{stage_plan}");
    assert!(stage_plan.contains("anchor_seek=ineligible"), "{stage_plan}");
    assert_eq!(engine.query_execution_counter_snapshot_for_test().graph_row_cursor_anchor_seeks, 0);

    let invalid = tampered_cursor_logical_key_atom(
        eligible.page.cursor.as_ref().unwrap(),
        0,
        GraphSortAtom::Edge(first),
    );
    let mut invalid_query = eligible.clone();
    invalid_query.page.cursor = Some(invalid);
    engine.reset_query_execution_counters_for_test();
    assert!(engine
        .query_graph_rows(&invalid_query)
        .unwrap_err()
        .to_string()
        .contains("cursor logical row key atom does not match slot 'n'"));
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .graph_row_cursor_anchor_seeks,
        0
    );

    let mut ordered = eligible.clone();
    ordered.order_by = vec![GraphOrderItem {
        expr: GraphExpr::NodeField {
            alias: "n".to_string(),
            field: GraphNodeField::Id,
        },
        direction: GraphOrderDirection::Asc,
    }];
    ordered.page.cursor = None;
    let ordered_first = engine.query_graph_rows(&ordered).unwrap();
    ordered.at_epoch = Some(ordered_first.stats.effective_at_epoch);
    ordered.page.cursor = ordered_first.next_cursor;
    engine.reset_query_execution_counters_for_test();
    let ordered_plan = format!("{:?}", engine.query_graph_rows(&ordered).unwrap().plan.unwrap());
    assert!(ordered_plan.contains("cursor_seek=none"), "{ordered_plan}");
    assert!(ordered_plan.contains("anchor_seek=ineligible"), "{ordered_plan}");
    assert_eq!(engine.query_execution_counter_snapshot_for_test().graph_row_cursor_anchor_seeks, 0);

    let mut empty_edge_pull = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "CURSOR_SEEK_UNKNOWN_EDGE",
        )],
    );
    empty_edge_pull.options.include_plan = true;
    engine.reset_query_execution_counters_for_test();
    let empty_plan = format!("{:?}", engine.query_graph_rows(&empty_edge_pull).unwrap().plan.unwrap());
    assert!(empty_plan.contains("source=EdgePull{edge=alias:edge}"));
    assert!(empty_plan.contains("cursor_seek=none"));
    assert_eq!(engine.query_execution_counter_snapshot_for_test().graph_row_cursor_anchor_seeks, 0);

    let mut edge_pull = graph_query(
        &["a", "b"],
        vec![graph_edge_with_label(
            Some("edge"),
            "a",
            "b",
            "CURSOR_SEEK_GATE_EDGE",
        )],
    );
    edge_pull.page.limit = 1;
    edge_pull.options.include_plan = true;
    let runtime_first = engine.query_graph_rows(&edge_pull).unwrap();
    edge_pull.at_epoch = Some(runtime_first.stats.effective_at_epoch);
    edge_pull.page.cursor = runtime_first.next_cursor;
    engine.reset_query_execution_counters_for_test();
    let runtime_plan = format!("{:?}", engine.query_graph_rows(&edge_pull).unwrap().plan.unwrap());
    assert!(runtime_plan.contains("source=EdgePull{edge=alias:edge}"));
    assert!(runtime_plan.contains("cursor_seek=none"));
    assert_eq!(engine.query_execution_counter_snapshot_for_test().graph_row_cursor_anchor_seeks, 0);

    let mut fast = graph_query(&["n"], Vec::new());
    fast.nodes[0] = graph_node_with_label("n", "CursorSeekGate");
    fast.page.limit = 1;
    let fast_first = engine.query_graph_rows(&fast).unwrap();
    fast.at_epoch = Some(fast_first.stats.effective_at_epoch);
    fast.page.cursor = fast_first.next_cursor;
    engine.reset_query_execution_counters_for_test();
    engine.query_graph_rows(&fast).unwrap();
    assert_eq!(engine.query_execution_counter_snapshot_for_test().graph_row_cursor_anchor_seeks, 0);
}

#[test]
fn graph_row_chunk_cursor_seek_skips_prefix_error_but_observes_anchor_error_and_cap() {
    let (_dir, engine) = graph_row_test_engine();
    let future_epoch = now_millis().saturating_add(60_000);
    let mut sources = Vec::new();
    let mut source_keys = Vec::new();
    for index in 0..6 {
        let key = format!("cursor-seek-lazy-source-{index}");
        let source = insert_graph_row_node(
            &engine,
            "CursorSeekLazySource",
            &key,
            &[("divisor", PropValue::Int(1))],
        );
        let target = insert_graph_row_node(
            &engine,
            "CursorSeekLazyTarget",
            &format!("cursor-seek-lazy-target-{index}"),
            &[],
        );
        insert_graph_row_edge(&engine, source, target, "CURSOR_SEEK_LAZY_EDGE", &[]);
        if index == 3 {
            for extra in 0..4 {
                let extra_target = insert_graph_row_node(
                    &engine,
                    "CursorSeekLazyTarget",
                    &format!("cursor-seek-lazy-hub-target-{extra}"),
                    &[],
                );
                insert_graph_row_edge(
                    &engine,
                    source,
                    extra_target,
                    "CURSOR_SEEK_LAZY_EDGE",
                    &[],
                );
            }
        }
        sources.push(source);
        source_keys.push(key);
    }
    let mut query = graph_query(
        &["source", "target"],
        vec![graph_edge_with_label(
            Some("edge"),
            "source",
            "target",
            "CURSOR_SEEK_LAZY_EDGE",
        )],
    );
    query.nodes[0].ids = sources.clone();
    query.where_ = Some(GraphExpr::Binary {
        left: Box::new(GraphExpr::Binary {
            left: Box::new(GraphExpr::Int(10)),
            op: GraphBinaryOp::Div,
            right: Box::new(graph_prop("source", "divisor")),
        }),
        op: GraphBinaryOp::Gt,
        right: Box::new(GraphExpr::Int(0)),
    });
    query.page.limit = 1;
    query.at_epoch = Some(future_epoch);
    query.options.include_plan = true;

    let first = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&query).unwrap()
    });
    let base_cursor = first.next_cursor.unwrap();
    engine
        .upsert_node(
            "CursorSeekLazySource",
            &source_keys[0],
            UpsertNodeOptions {
                props: graph_row_props(&[("divisor", PropValue::Int(0))]),
                ..Default::default()
            },
        )
        .unwrap();

    let deep_cursor = tampered_cursor_logical_key_atom(
        &base_cursor,
        0,
        GraphSortAtom::Node(sources[3]),
    );
    let mut deep = query.clone();
    deep.page.cursor = Some(deep_cursor);
    let result = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&deep).unwrap()
    });
    let plan = format!("{:?}", result.plan.unwrap());
    assert!(plan.contains(&format!("cursor_seek=anchor_ge:{}", sources[3])), "{plan}");
    let prefix_error = with_graph_row_test_chunk_override(1, || {
        with_graph_row_test_cursor_seek_disabled(|| engine.query_graph_rows(&deep).unwrap_err())
    });
    assert_eq!(
        prefix_error.to_string(),
        "invalid operation: graph row division by zero"
    );

    let anchor_cursor = tampered_cursor_logical_key_atom(
        &base_cursor,
        0,
        GraphSortAtom::Node(sources[0]),
    );
    let mut at_anchor = query.clone();
    at_anchor.page.cursor = Some(anchor_cursor);
    let anchor_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&at_anchor).unwrap_err()
    });
    assert_eq!(
        anchor_error.to_string(),
        "invalid operation: graph row division by zero"
    );

    let mut capped = deep;
    capped.where_ = None;
    capped.options.max_frontier = 2;
    // Rebuild a matching cursor because WHERE/options are part of the fingerprint.
    capped.page.cursor = None;
    let capped_first = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&capped).unwrap()
    });
    capped.page.cursor = Some(tampered_cursor_logical_key_atom(
        capped_first.next_cursor.as_ref().unwrap(),
        0,
        GraphSortAtom::Node(sources[3]),
    ));
    let cap_error = with_graph_row_test_chunk_override(1, || {
        engine.query_graph_rows(&capped).unwrap_err()
    });
    assert_eq!(
        cap_error.to_string(),
        "invalid operation: graph row max_frontier exceeded configured cap 2"
    );
}
