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
    assert_graph_pipeline_invalid(&engine, &capped, "max_pipeline_rows");

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
fn graph_row_vlp_orders_paths_and_enforces_relationship_simple_traversal() {
    let (_dir, engine) = graph_row_test_engine();
    let a = insert_graph_row_node(&engine, "PathOrder", "path-order-a", &[]);
    let b = insert_graph_row_node(&engine, "PathOrder", "path-order-b", &[]);
    let c = insert_graph_row_node(&engine, "PathOrder", "path-order-c", &[]);
    let ab = insert_graph_row_edge(&engine, a, b, "PATH_ORDER", &[]);
    let bc = insert_graph_row_edge(&engine, b, c, "PATH_ORDER", &[]);
    let ac = insert_graph_row_edge(&engine, a, c, "PATH_ORDER", &[]);
    let ca = insert_graph_row_edge(&engine, c, a, "PATH_ORDER", &[]);

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

    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&query).unwrap()),
        vec![
            (vec![a], vec![]),
            (vec![a, b], vec![ab]),
            (vec![a, c], vec![ac]),
            (vec![a, b, c], vec![ab, bc]),
            (vec![a, c, a], vec![ac, ca]),
            (vec![a, b, c, a], vec![ab, bc, ca]),
            (vec![a, c, a, b], vec![ac, ca, ab]),
        ]
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

    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&query).unwrap()),
        vec![(vec![start, keep_mid, keep_end], vec![first, second])]
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

    let mut paths = frontier.clone();
    paths.options.max_frontier = 10;
    paths.options.max_paths_per_start = 1;
    let err = engine.query_graph_rows(&paths).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_paths_per_start"));
    assert!(message.contains("configured cap 1"));
    assert!(message.contains("path=p"));

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
    let path_edge = insert_graph_row_edge(&engine, start, target, "VLP_GROUP_PATH", &[]);

    let mut query = graph_query(
        &["root", "a", "z"],
        vec![
            graph_edge_with_label(None, "root", "a", "VLP_GROUP_LEFT"),
            graph_vlp(Some("p"), None, "a", "z", 1, 2),
        ],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[1] {
        path.label_filter = vec!["VLP_GROUP_PATH".to_string()];
    }
    query.nodes[0].ids = vec![root];
    query.options.include_plan = true;
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);

    let result = engine.query_graph_rows(&query).unwrap();
    assert_eq!(result.stats.paths_enumerated, 1);
    let explain = result.plan.as_ref().unwrap();
    assert_graph_row_explain_contains(explain, "distinct_search_groups=1");
    assert_graph_row_explain_contains(explain, "search_cache_hits=1");
    assert_eq!(
        graph_row_path_ids(result),
        vec![
            (vec![start, target], vec![path_edge]),
            (vec![start, target], vec![path_edge]),
        ]
    );
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
    let ab = insert_graph_row_edge(&engine, a, b, "CURSOR_PATH", &[]);
    let ac = insert_graph_row_edge(&engine, a, c, "CURSOR_PATH", &[]);
    let bc = insert_graph_row_edge(&engine, b, c, "CURSOR_PATH", &[]);

    let mut query = graph_query(
        &["a", "z"],
        vec![graph_vlp(Some("p"), None, "a", "z", 1, 2)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut query.pieces[0] {
        path.label_filter = vec!["CURSOR_PATH".to_string()];
    }
    query.nodes[0].ids = vec![a];
    query.page.limit = 10;
    query.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    let unpaged = graph_row_path_ids(engine.query_graph_rows(&query).unwrap());
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
        let page = engine.query_graph_rows(&query).unwrap();
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
    let _ab = insert_graph_row_edge(&engine, a, b, "OPTIONAL_PATH_HIT", &[]);
    let bc = insert_graph_row_edge(&engine, b, c, "OPTIONAL_PATH_HIT", &[]);
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
        }
    }
    hit.nodes[0].ids = vec![a];
    hit.return_items = Some(vec![graph_return_binding("p", GraphReturnProjection::IdOnly)]);
    assert_eq!(
        graph_row_path_ids(engine.query_graph_rows(&hit).unwrap()),
        vec![(vec![b, c], vec![bc])]
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

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![alice, bob]
    );
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

    let mut where_query = graph_query(
        &["a", "b", "c"],
        vec![
            graph_edge_with_label(Some("r"), "a", "b", "GRAPH_ROW_OPTIONAL_FILTER_REQUIRED"),
            graph_optional(
                vec![graph_edge_with_label(
                    Some("s"),
                    "b",
                    "c",
                    "GRAPH_ROW_OPTIONAL_FILTER_EDGE",
                )],
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

    assert_eq!(
        graph_row_value_rows(engine.query_graph_rows(&where_query).unwrap()),
        vec![vec![GraphValue::Null]]
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
    insert_graph_row_edge(&engine, b1, c, "GRAPH_ROW_OPTIONAL_ORDER_HIT", &[]);

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

    let first = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(first.clone()),
        vec![vec![GraphValue::NodeId(b1), GraphValue::NodeId(c)]]
    );
    let cursor = first.next_cursor.unwrap();
    query.page.cursor = Some(cursor);
    let second = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_value_rows(second),
        vec![vec![GraphValue::NodeId(b2), GraphValue::Null]]
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
    let independent = insert_graph_row_edge(&engine, x, y, "GRAPH_ROW_OPTIONAL_UNCORR_INDEPENDENT", &[]);

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

    let miss_result = engine.query_graph_rows(&miss_query).unwrap();
    assert_eq!(
        graph_row_value_rows(miss_result.clone()),
        vec![vec![GraphValue::Null], vec![GraphValue::Null]]
    );
    let miss_explain = miss_result.plan.unwrap();
    assert_graph_row_explain_contains(&miss_explain, "correlated=false");
    assert_graph_row_explain_contains(&miss_explain, "hit_rows=0");
    assert_graph_row_explain_contains(&miss_explain, "miss_rows=2");
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
    let optional_edge = insert_graph_row_edge(&engine, b, c, "GRAPH_ROW_OPTIONAL_DEP_HIT", &[]);

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
    for index in 0..3 {
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_EXPLICIT_ORDER_CAP",
            &format!("explicit-order-cap-{index}"),
            &[("rank", PropValue::Int(index))],
        );
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

    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("max_order_materialization") && message.contains('2'),
        "expected max_order_materialization cap error with value 2, got {message:?}"
    );
}

#[test]
fn graph_row_order_cap_rejects_before_unbounded_residual_field_hydration() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..3 {
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_RESIDUAL_ORDER_CAP",
            &format!("residual-order-cap-{index}"),
            &[
                ("status", PropValue::String("active".to_string())),
                ("rank", PropValue::Int(index)),
            ],
        );
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
    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("max_order_materialization") && message.contains('2'),
        "expected max_order_materialization cap error with value 2, got {message:?}"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_selected_field_ids,
        0
    );
}

#[test]
fn graph_row_order_cap_rejects_metadata_residual_before_selected_field_reads() {
    let (_dir, engine) = graph_row_test_engine();
    for index in 0..3 {
        insert_graph_row_node(
            &engine,
            "GRAPH_ROW_METADATA_RESIDUAL_ORDER_CAP",
            &format!("metadata-residual-order-cap-{index}"),
            &[],
        );
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
    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("max_order_materialization") && message.contains('2'),
        "expected max_order_materialization cap error with value 2, got {message:?}"
    );
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .node_selected_field_ids,
        0
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
) -> (u64, EdgePropertyIndexInfo, GraphRowQuery) {
    let mut expected = None;
    for index in 0..80 {
        let from = insert_graph_row_node(
            engine,
            &format!("{label}From"),
            &format!("edge-fallback-from-{index}"),
            &[],
        );
        let to = insert_graph_row_node(
            engine,
            &format!("{label}To"),
            &format!("edge-fallback-to-{index}"),
            &[],
        );
        let status = if index == 37 { "hot" } else { "cold" };
        let edge = insert_graph_row_weighted_edge(
            engine,
            from,
            to,
            label,
            &[("status", PropValue::String(status.to_string()))],
            1.0,
        );
        if index == 37 {
            expected = Some(edge);
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
    (expected.unwrap(), info, query)
}

#[test]
fn graph_row_compound_edge_anchor_missing_sidecar_falls_back_to_legal_source() {
    let (_dir, engine) = graph_row_test_engine();
    let (expected, info, query) =
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

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![expected]
    );
}

#[test]
fn graph_row_compound_edge_anchor_corrupt_sidecar_falls_back_to_legal_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let (expected, info, query) =
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
    assert_eq!(
        graph_row_single_u64_column(reopened.query_graph_rows(&query).unwrap()),
        vec![expected]
    );
    reopened.close().unwrap();
}

#[test]
fn graph_row_too_broad_compound_edge_source_streams_verified_fallback() {
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

    assert_eq!(
        graph_row_single_u64_column(engine.query_graph_rows(&query).unwrap()),
        vec![expected]
    );
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
    assert_graph_row_explain_contains(explain, "choice=EdgeCandidateSource");
    assert_graph_row_explain_contains(explain, "EdgePropertyEqualityIndex");
    assert_graph_row_explain_contains(explain, "materialized_source=");
    assert_graph_row_explain_contains(explain, "subset_intersection_source_materialized=");
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
    assert_graph_row_explain_contains(explain, "choice=EdgeCandidateSource");
    assert_graph_row_explain_contains(explain, "EdgePropertyRangeIndex");
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
    assert_graph_row_explain_contains(explain, "choice=EdgeCandidateSource");
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
    assert_graph_row_explain_contains(explain, "choice=EndpointAdjacency");
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
fn graph_row_explain_records_skipped_source_after_empty_frontier() {
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
    assert_graph_row_explain_contains(explain, "edge=alias:skipped_edge");
    assert_graph_row_explain_contains(explain, "choice=SkippedEmptyFrontier");
    assert_graph_row_explain_contains(explain, "planned_driver=EndpointAdjacency");
    assert_graph_row_explain_contains(explain, "skipped_due_to_empty_frontier=true");
    assert_graph_row_explain_contains(explain, "materialized_source=none");
}

#[test]
fn graph_row_planner_broad_label_only_edge_anchor_errors_with_cap_source() {
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

    let err = engine.query_graph_rows(&query).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_frontier"), "{message}");
    assert!(message.contains("source=EdgeCandidateSource"), "{message}");
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
