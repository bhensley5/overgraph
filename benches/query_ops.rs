use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeFilterExpr, EdgeInput, EdgeQuery,
    GqlExecutionOptions, GqlParamValue, GqlParams, GqlStatementKind, GraphBinaryOp,
    GraphEdgePattern, GraphExpr, GraphNodeField, GraphNodePattern, GraphOrderDirection,
    GraphOrderItem, GraphOutputOptions, GraphPageRequest, GraphParamValue, GraphPatternPiece,
    GraphQueryOptions, GraphReturnItem, GraphReturnProjection, LabelMatchMode, NodeFilterExpr,
    NodeInput, NodeLabelFilter, NodeQuery, PageRequest, PropValue, PropertyRangeBound,
    SecondaryIndexKind, SecondaryIndexState,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const QUERY_SEGMENT_COUNT: usize = 1;
const QUERY_NODES_PER_SEGMENT: usize = 5_000;
const QUERY_MEMTABLE_TAIL_COUNT: usize = 5_000;
const QUERY_LIMIT: usize = 100;
const QUERY_LARGE_UNIVERSE_COUNT: usize = 25_000;
const QUERY_SMALL_LABEL_COUNT: usize = 128;
const QUERY_LARGE_IN_VALUE_COUNT: usize = 512;

macro_rules! filter_and {
    [] => {
        None
    };
    [$single:expr $(,)?] => {
        Some($single)
    };
    [$($filter:expr),+ $(,)?] => {
        Some(NodeFilterExpr::And(vec![$($filter),+]))
    };
}

fn temp_db_with_edge_uniqueness(edge_uniqueness: bool) -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        edge_uniqueness,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    seed_bench_label_tokens(&engine);
    (dir, engine)
}

fn temp_db() -> (tempfile::TempDir, DatabaseEngine) {
    temp_db_with_edge_uniqueness(true)
}

fn seed_bench_label_tokens(engine: &DatabaseEngine) {
    for label_token_id in 1..=256 {
        assert_eq!(
            engine
                .ensure_node_label(&bench_node_label(label_token_id))
                .unwrap(),
            label_token_id
        );
        assert_eq!(
            engine
                .ensure_edge_label(&format!("BenchEdge{label_token_id}"))
                .unwrap(),
            label_token_id
        );
    }
}

fn bench_node_label(label_token_id: u32) -> String {
    format!("BenchNode{label_token_id}")
}

fn query_props(i: usize) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String(
            if i.is_multiple_of(10) {
                "active"
            } else {
                "inactive"
            }
            .to_string(),
        ),
    );
    props.insert(
        "tier".to_string(),
        PropValue::String(
            if i.is_multiple_of(20) {
                "gold"
            } else {
                "standard"
            }
            .to_string(),
        ),
    );
    props.insert("score".to_string(), PropValue::Int((i % 100) as i64));
    props.insert(
        "region".to_string(),
        PropValue::String(format!("r{:02}", i % 17)),
    );
    props.insert(
        "tenant".to_string(),
        PropValue::String(format!("t{:02}", i % 100)),
    );
    props
}

fn query_nodes(prefix: &str, start: usize, count: usize) -> Vec<NodeInput> {
    (start..start + count)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(1)],
            key: format!("{prefix}-{i}"),
            props: query_props(i),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

fn query_nodes_with_label_id(
    label_id: u32,
    prefix: &str,
    start: usize,
    count: usize,
) -> Vec<NodeInput> {
    (start..start + count)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(label_id)],
            key: format!("{prefix}-{i}"),
            props: query_props(i),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

fn wait_for_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if engine
            .list_node_property_indexes()
            .unwrap()
            .into_iter()
            .any(|info| info.index_id == index_id && info.state == expected_state)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for property index {} to reach {:?}",
            index_id,
            expected_state
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_edge_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if engine
            .list_edge_property_indexes()
            .unwrap()
            .into_iter()
            .any(|info| info.index_id == index_id && info.state == expected_state)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for edge property index {} to reach {:?}",
            index_id,
            expected_state
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn ensure_query_indexes(engine: &mut DatabaseEngine) {
    let label = bench_node_label(1);
    let status = engine
        .ensure_node_property_index(&label, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);

    let tier = engine
        .ensure_node_property_index(&label, "tier", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(engine, tier.index_id, SecondaryIndexState::Ready);

    let tenant = engine
        .ensure_node_property_index(&label, "tenant", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(engine, tenant.index_id, SecondaryIndexState::Ready);

    let score = engine
        .ensure_node_property_index(&label, "score", SecondaryIndexKind::Range)
        .unwrap();
    wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
}

fn build_indexed_query_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, mut engine) = temp_db();
    ensure_query_indexes(&mut engine);
    load_query_mixed_sources(&engine, "indexed");
    (dir, engine)
}

fn load_query_mixed_sources(engine: &DatabaseEngine, prefix: &str) {
    for segment in 0..QUERY_SEGMENT_COUNT {
        let start = segment * QUERY_NODES_PER_SEGMENT;
        let nodes = query_nodes(prefix, start, QUERY_NODES_PER_SEGMENT);
        engine.batch_upsert_nodes(nodes.clone()).unwrap();
        engine.flush().unwrap();
    }

    let tail_start = QUERY_SEGMENT_COUNT * QUERY_NODES_PER_SEGMENT;
    let tail_nodes = query_nodes(prefix, tail_start, QUERY_MEMTABLE_TAIL_COUNT);
    engine.batch_upsert_nodes(tail_nodes.clone()).unwrap();
}

fn build_fallback_query_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    load_query_mixed_sources(&engine, "fallback");
    (dir, engine)
}

fn build_small_label_universe_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let filler = query_nodes_with_label_id(2, "large-universe", 0, QUERY_LARGE_UNIVERSE_COUNT);
    engine.batch_upsert_nodes(filler.clone()).unwrap();
    engine.flush().unwrap();

    let segment_small = query_nodes_with_label_id(1, "small-label", 0, QUERY_SMALL_LABEL_COUNT / 2);
    engine.batch_upsert_nodes(segment_small.clone()).unwrap();
    engine.flush().unwrap();

    let memtable_small = query_nodes_with_label_id(
        1,
        "small-label",
        QUERY_SMALL_LABEL_COUNT / 2,
        QUERY_SMALL_LABEL_COUNT - QUERY_SMALL_LABEL_COUNT / 2,
    );
    engine.batch_upsert_nodes(memtable_small.clone()).unwrap();
    (dir, engine)
}

fn two_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tier".to_string(),
                value: PropValue::String("gold".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn status_active_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn equality_and_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(50))),
                upper: None,
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn broad_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("inactive".to_string()),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn broad_equality_and_selective_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("t07".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn broad_equality_and_selective_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(99))),
                upper: None,
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn range_stats_selective_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(7))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(7))),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn range_stats_broad_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(99))),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn now_millis_for_bench() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn timestamp_stats_recent_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(now_millis_for_bench().saturating_sub(60_000)),
            upper_ms: None,
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn timestamp_stats_broad_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(0),
            upper_ms: Some(i64::MAX),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn label_scan_fallback_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn label_scoped_verify_only_boolean_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "tier".to_string(),
                    value: PropValue::String("gold".to_string()),
                },
            ]),
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyMissing {
                key: "tenant".to_string(),
            })),
            NodeFilterExpr::PropertyExists {
                key: "region".to_string(),
            },
            NodeFilterExpr::PropertyMissing {
                key: "deletedAt".to_string(),
            },
        ])),
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn tenant_eq_filter(value: &str) -> NodeFilterExpr {
    NodeFilterExpr::PropertyEquals {
        key: "tenant".to_string(),
        value: PropValue::String(value.to_string()),
    }
}

fn score_at_least_filter(value: i64) -> NodeFilterExpr {
    NodeFilterExpr::PropertyRange {
        key: "score".to_string(),
        lower: Some(PropertyRangeBound::Included(PropValue::Int(value))),
        upper: None,
    }
}

fn boolean_or_union_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::Or(vec![
            tenant_eq_filter("t07"),
            tenant_eq_filter("t11"),
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_in_union_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "tenant".to_string(),
            values: vec![
                PropValue::String("t03".to_string()),
                PropValue::String("t07".to_string()),
                PropValue::String("t11".to_string()),
                PropValue::String("t19".to_string()),
            ],
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_and_or_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                tenant_eq_filter("t91"),
                tenant_eq_filter("t95"),
                tenant_eq_filter("t99"),
            ]),
            score_at_least_filter(95),
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_verify_only_label_fallback_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::Or(vec![
            tenant_eq_filter("t07"),
            NodeFilterExpr::PropertyMissing {
                key: "deletedAt".to_string(),
            },
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_large_in_verify_only_query(limit: Option<usize>) -> NodeQuery {
    let mut values: Vec<PropValue> = (0..QUERY_LARGE_IN_VALUE_COUNT)
        .map(|index| PropValue::String(format!("missing-region-{index:03}")))
        .collect();
    values.push(PropValue::String("r03".to_string()));

    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "region".to_string(),
            values,
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn label_only_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn label_with_large_explicit_ids_query(ids: Vec<u64>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        ids,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn label_with_large_keys_query(keys: Vec<String>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        keys,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn full_scan_query() -> NodeQuery {
    NodeQuery {
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        allow_full_scan: true,
        ..Default::default()
    }
}

fn full_scan_limit_one_query() -> NodeQuery {
    NodeQuery {
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        allow_full_scan: true,
        ..Default::default()
    }
}

fn explicit_ids_query(ids: &[u64]) -> NodeQuery {
    NodeQuery {
        ids: ids.to_vec(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn explicit_ids_and_selective_property_query(ids: &[u64]) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        ids: ids.to_vec(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("t07".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn bench_node_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_node_planner");
    group.sample_size(20);

    group.bench_function("query_node_ids_intersected_two_equality_predicates", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = two_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_nodes_intersected_equality_and_range_hydrated", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_scan_fallback", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = label_scan_fallback_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_label_scoped_verify_only_boolean_filter",
        |b| {
            let (_dir, engine) = build_fallback_query_engine();
            let query = label_scoped_verify_only_boolean_query();
            b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_node_ids_boolean_or_union", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_or_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_boolean_in_union", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_in_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_boolean_and_or_range", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_and_or_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_boolean_verify_only_label_fallback", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = boolean_verify_only_label_fallback_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_boolean_large_in_verify_only_label_fallback",
        |b| {
            let (_dir, engine) = build_fallback_query_engine();
            let query = boolean_large_in_verify_only_query(Some(QUERY_LIMIT));
            b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_nodes_boolean_hydrated_final_page", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let all_ids = engine
            .query_node_ids(&NodeQuery {
                page: PageRequest {
                    limit: None,
                    after: None,
                },
                ..boolean_or_union_query(None)
            })
            .unwrap()
            .items;
        let after = all_ids
            .get(all_ids.len().saturating_sub(2))
            .copied()
            .unwrap_or_default();
        let query = NodeQuery {
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: Some(after),
            },
            ..boolean_or_union_query(None)
        };
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_only", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = label_only_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_vs_large_explicit_ids", |b| {
        let (_dir, engine) = build_small_label_universe_engine();
        let ids = (1..=(QUERY_LARGE_UNIVERSE_COUNT + QUERY_SMALL_LABEL_COUNT) as u64).collect();
        let query = label_with_large_explicit_ids_query(ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_vs_large_keys", |b| {
        let (_dir, engine) = build_small_label_universe_engine();
        let mut keys: Vec<String> = (0..QUERY_LARGE_UNIVERSE_COUNT)
            .map(|i| format!("missing-key-{i}"))
            .collect();
        keys.extend((0..QUERY_SMALL_LABEL_COUNT).map(|i| format!("small-label-{i}")));
        let query = label_with_large_keys_query(keys);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_explicit_full_scan_opt_in", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = full_scan_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_explicit_ids_verify", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let nodes = query_nodes("explicit", 0, 512);
        let ids = engine.batch_upsert_nodes(nodes.clone()).unwrap();
        let query = explicit_ids_query(&ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_vs_hydrated_payload_ids", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_vs_hydrated_payload_nodes", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_intersected_predicates", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_broad_equality", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_broad_equality_selective_equality", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_and_selective_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_broad_equality_selective_range", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_and_selective_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_range_stats_selective", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = range_stats_selective_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_range_stats_broad_fallback", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = range_stats_broad_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_timestamp_stats_recent_window", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = timestamp_stats_recent_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_timestamp_stats_broad_window", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = timestamp_stats_broad_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_stale_heavy_equality_visible_small", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_and_selective_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_heavy_hitter_equality_skipped", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_in_heavy_hitter_vs_rare_values", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_in_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_or_union_stats_costed", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_or_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_range_no_candidate_probe", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = range_stats_selective_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_large_explicit_ids_selective_index", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let label = bench_node_label(1);
        let ids = engine.nodes_by_labels(&label).unwrap();
        let query = explicit_ids_and_selective_property_query(&ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_full_scan_limit_one", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = full_scan_limit_one_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_nodes_hydrated_final_page", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let all_ids = engine
            .query_node_ids(&NodeQuery {
                page: PageRequest {
                    limit: None,
                    after: None,
                },
                ..broad_equality_and_selective_equality_query(None)
            })
            .unwrap()
            .items;
        let after = all_ids
            .get(all_ids.len().saturating_sub(2))
            .copied()
            .unwrap_or_default();
        let query = NodeQuery {
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: Some(after),
            },
            ..broad_equality_and_selective_equality_query(None)
        };
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.finish();
}

fn edge_query_props(i: usize) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "role".to_string(),
        PropValue::String(
            if i.is_multiple_of(10) {
                "lead"
            } else {
                "member"
            }
            .to_string(),
        ),
    );
    props.insert("score".to_string(), PropValue::Int((i % 100) as i64));
    props
}

fn build_edge_query_engine() -> (tempfile::TempDir, DatabaseEngine, u64, Vec<u64>, i64) {
    let (dir, engine) = temp_db();
    let edge_count = QUERY_NODES_PER_SEGMENT + QUERY_MEMTABLE_TAIL_COUNT;
    let valid_epoch = 1_700_000_000_100i64;
    let mut nodes = Vec::with_capacity(edge_count + 1);
    nodes.push(NodeInput {
        labels: vec![bench_node_label(1)],
        key: "edge-query-source".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    });
    nodes.extend((0..edge_count).map(|i| NodeInput {
        labels: vec![bench_node_label(2)],
        key: format!("edge-query-target-{i}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let node_ids = engine.batch_upsert_nodes(nodes.clone()).unwrap();
    let source_id = node_ids[0];
    let target_ids = &node_ids[1..];
    let make_edges = |start: usize, count: usize| -> Vec<EdgeInput> {
        (start..start + count)
            .map(|i| EdgeInput {
                from: source_id,
                to: target_ids[i],
                label: "BenchEdge10".to_string(),
                props: edge_query_props(i),
                weight: if i.is_multiple_of(2) { 2.0 } else { 0.5 },
                valid_from: Some(1_700_000_000_000),
                valid_to: Some(1_700_000_010_000),
            })
            .collect()
    };

    let mut edge_ids = engine
        .batch_upsert_edges(make_edges(0, QUERY_NODES_PER_SEGMENT))
        .unwrap();
    engine.flush().unwrap();
    edge_ids.extend(
        engine
            .batch_upsert_edges(make_edges(
                QUERY_NODES_PER_SEGMENT,
                QUERY_MEMTABLE_TAIL_COUNT,
            ))
            .unwrap(),
    );
    (dir, engine, source_id, edge_ids, valid_epoch)
}

fn build_edge_query_indexed_engine() -> (tempfile::TempDir, DatabaseEngine, u64, Vec<u64>, i64) {
    let (dir, engine, source_id, edge_ids, valid_epoch) = build_edge_query_engine();
    let label = "BenchEdge10".to_string();
    let role = engine
        .ensure_edge_property_index(&label, "role", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, role.index_id, SecondaryIndexState::Ready);
    let score = engine
        .ensure_edge_property_index(&label, "score", SecondaryIndexKind::Range)
        .unwrap();
    wait_for_edge_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    (dir, engine, source_id, edge_ids, valid_epoch)
}

fn edge_query_with_filter(source_id: u64, filter: Option<EdgeFilterExpr>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge10".to_string()),
        from_ids: vec![source_id],
        filter,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn bench_edge_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_edge_planner");
    group.sample_size(20);

    group.bench_function("query_edge_ids_explicit_ids", |b| {
        let (_dir, engine, _source_id, edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = EdgeQuery {
            ids: edge_ids.iter().take(512).copied().collect(),
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            }),
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: None,
            },
            ..Default::default()
        };
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_label_only", |b| {
        let (_dir, engine, _source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = EdgeQuery {
            label: Some("BenchEdge10".to_string()),
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: None,
            },
            ..Default::default()
        };
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_from_endpoint_label", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(source_id, None);
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_endpoint_list_label", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = EdgeQuery {
            label: Some("BenchEdge10".to_string()),
            endpoint_ids: vec![source_id],
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: None,
            },
            ..Default::default()
        };
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_weight_range", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_updated_at_range", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::UpdatedAtRange {
                lower_ms: Some(0),
                upper_ms: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_valid_at_endpoint", |b| {
        let (_dir, engine, source_id, _edge_ids, valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::ValidAt {
                epoch_ms: valid_epoch,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_property_verifier_bounded", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_property_indexed_equality", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_property_indexed_range", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edges_metadata_final_page", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edges(black_box(&query)).unwrap()));
    });

    group.finish();

    let mut property_group = c.benchmark_group("edge_property_index_queries");
    property_group.sample_size(20);

    property_group.bench_function("equality_fallback_scan", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.bench_function("equality_declared", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.bench_function("range_fallback_scan", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.bench_function("range_declared", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.finish();
}

fn build_pattern_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, mut engine) = temp_db();
    ensure_query_indexes(&mut engine);
    let account_inputs = query_nodes("acct", 0, 1_000);
    let account_ids = engine.batch_upsert_nodes(account_inputs.clone()).unwrap();

    let companies: Vec<NodeInput> = (0..200)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(2)],
            key: format!("company-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let company_ids = engine.batch_upsert_nodes(companies.clone()).unwrap();

    let edges: Vec<EdgeInput> = account_ids
        .iter()
        .enumerate()
        .map(|(i, &from)| EdgeInput {
            from,
            to: company_ids[i % company_ids.len()],
            label: "BenchEdge10".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(edges.clone()).unwrap();
    engine.flush().unwrap();
    (dir, engine, company_ids[0])
}

const GRAPH_ROW_BENCH_EDGES: usize = 1_000;

fn build_graph_row_optional_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, engine) = temp_db();
    let source = engine
        .batch_upsert_nodes(vec![NodeInput {
            labels: vec![bench_node_label(1)],
            key: "graph-row-source".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let targets: Vec<NodeInput> = (0..GRAPH_ROW_BENCH_EDGES)
        .map(|index| NodeInput {
            labels: vec![bench_node_label(2)],
            key: format!("graph-row-target-{index:04}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let target_ids = engine.batch_upsert_nodes(targets.clone()).unwrap();
    let work_edges: Vec<EdgeInput> = target_ids
        .iter()
        .enumerate()
        .map(|(index, &target)| {
            let mut props = BTreeMap::new();
            props.insert(
                "role".to_string(),
                PropValue::String(if index % 10 == 0 { "lead" } else { "member" }.to_string()),
            );
            props.insert("score".to_string(), PropValue::Int((index % 100) as i64));
            EdgeInput {
                from: source,
                to: target,
                label: "BenchEdge10".to_string(),
                props,
                weight: if index % 2 == 0 { 2.0 } else { 0.5 },
                valid_from: None,
                valid_to: None,
            }
        })
        .collect();
    engine.batch_upsert_edges(work_edges).unwrap();
    let docs: Vec<NodeInput> = (0..GRAPH_ROW_BENCH_EDGES)
        .step_by(8)
        .map(|index| NodeInput {
            labels: vec![bench_node_label(3)],
            key: format!("graph-row-doc-{index:04}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let doc_ids = engine.batch_upsert_nodes(docs.clone()).unwrap();
    let mention_edges: Vec<EdgeInput> = doc_ids
        .iter()
        .enumerate()
        .map(|(doc_index, &doc_id)| EdgeInput {
            from: target_ids[doc_index * 8],
            to: doc_id,
            label: "BenchEdge20".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(mention_edges).unwrap();
    engine.flush().unwrap();
    (dir, engine, source)
}

fn graph_row_return_binding(alias: &str) -> GraphReturnItem {
    GraphReturnItem {
        expr: GraphExpr::Binding(alias.to_string()),
        alias: Some(alias.to_string()),
        projection: GraphReturnProjection::IdOnly,
    }
}

fn graph_row_fixed_query(source_id: u64, limit: usize) -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "source".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![bench_node_label(1)],
                    mode: LabelMatchMode::All,
                }),
                ids: vec![source_id],
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "target".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![bench_node_label(2)],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BenchEdge10".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        })],
        where_: Some(GraphExpr::Binary {
            left: Box::new(GraphExpr::Property {
                alias: "edge".to_string(),
                key: "role".to_string(),
            }),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::Param("role".to_string())),
        }),
        return_items: Some(vec![
            graph_row_return_binding("source"),
            graph_row_return_binding("edge"),
            graph_row_return_binding("target"),
        ]),
        order_by: graph_row_order_by_score_then_target(),
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::from([(
            "role".to_string(),
            GraphParamValue::String("lead".to_string()),
        )]),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions::default(),
    }
}

fn graph_row_optional_query(source_id: u64, limit: usize) -> overgraph::GraphRowQuery {
    let mut query = graph_row_fixed_query(source_id, limit);
    query.nodes.push(GraphNodePattern {
        alias: "doc".to_string(),
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(3)],
            mode: LabelMatchMode::All,
        }),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: None,
    });
    query
        .pieces
        .push(GraphPatternPiece::Optional(overgraph::GraphOptionalGroup {
            pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("ref".to_string()),
                from_alias: "target".to_string(),
                to_alias: "doc".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["BenchEdge20".to_string()],
                filter: None,
            })],
            where_: None,
        }));
    query.return_items = Some(vec![
        graph_row_return_binding("source"),
        graph_row_return_binding("edge"),
        graph_row_return_binding("target"),
        graph_row_return_binding("ref"),
        graph_row_return_binding("doc"),
    ]);
    query
}

fn graph_row_order_by_score_then_target() -> Vec<GraphOrderItem> {
    vec![
        GraphOrderItem {
            expr: GraphExpr::Property {
                alias: "edge".to_string(),
                key: "score".to_string(),
            },
            direction: GraphOrderDirection::Desc,
        },
        GraphOrderItem {
            expr: GraphExpr::NodeField {
                alias: "target".to_string(),
                field: GraphNodeField::Id,
            },
            direction: GraphOrderDirection::Asc,
        },
    ]
}

fn assert_graph_row_result_count(
    result: overgraph::GraphRowResult,
    expected: usize,
) -> overgraph::GraphRowResult {
    assert_eq!(result.rows.len(), expected);
    assert_eq!(result.stats.rows_returned, expected);
    result
}

fn temp_gql_mutation_bench_db() -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    (dir, engine)
}

fn gql_bench_node(label: &str, key: &str, props: BTreeMap<String, PropValue>) -> NodeInput {
    NodeInput {
        labels: vec![label.to_string()],
        key: key.to_string(),
        props,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn gql_bench_props(values: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn setup_gql_set_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    engine
        .batch_upsert_nodes(vec![gql_bench_node(
            "GqlBenchSet",
            "n",
            gql_bench_props(&[("status", PropValue::String("old".to_string()))]),
        )])
        .unwrap();
    (dir, engine)
}

fn setup_gql_detach_delete_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    let node_ids = engine
        .batch_upsert_nodes(vec![
            gql_bench_node("GqlBenchDetach", "hub", BTreeMap::new()),
            gql_bench_node("GqlBenchDetach", "left", BTreeMap::new()),
            gql_bench_node("GqlBenchDetach", "right", BTreeMap::new()),
        ])
        .unwrap();
    engine
        .batch_upsert_edges(vec![
            EdgeInput {
                from: node_ids[0],
                to: node_ids[1],
                label: "GQL_BENCH_DETACH".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: node_ids[2],
                to: node_ids[0],
                label: "GQL_BENCH_DETACH".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            },
        ])
        .unwrap();
    (dir, engine)
}

fn setup_gql_mutation_return_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    let nodes: Vec<NodeInput> = (0..3)
        .map(|i| {
            gql_bench_node(
                "GqlBenchReturn",
                &format!("n{i}"),
                gql_bench_props(&[
                    ("rank", PropValue::Int(i as i64)),
                    ("status", PropValue::String("old".to_string())),
                ]),
            )
        })
        .collect();
    engine.batch_upsert_nodes(nodes).unwrap();
    (dir, engine)
}

fn setup_gql_merge_match_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    engine
        .batch_upsert_nodes(vec![gql_bench_node(
            "GqlBenchMerge",
            "n",
            gql_bench_props(&[("status", PropValue::String("old".to_string()))]),
        )])
        .unwrap();
    (dir, engine)
}

fn assert_gql_mutation_result(
    result: overgraph::GqlExecutionResult,
    expected_rows: usize,
) -> overgraph::GqlExecutionResult {
    assert_eq!(result.kind, GqlStatementKind::Mutation);
    assert_eq!(result.rows.len(), expected_rows);
    assert_eq!(result.stats.rows_returned, expected_rows);
    assert!(result.next_cursor.is_none());
    assert!(result.mutation_stats.is_some());
    result
}

const GQL_SCHEMA_ALTER_ADD_BENCH: &str = "ALTER CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128 }";
const GQL_SCHEMA_CHECK_ADD_BENCH: &str = "CHECK CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128, max_violations: 4 }";

fn setup_gql_schema_existing_data_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let ids = engine
        .batch_upsert_nodes(vec![
            NodeInput {
                labels: vec!["SchemaPerson".to_string()],
                key: "person-0".to_string(),
                props: BTreeMap::from([(
                    "name".to_string(),
                    PropValue::String("name-0".to_string()),
                )]),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["SchemaCompany".to_string()],
                key: "company-0".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
        ])
        .unwrap();
    engine
        .batch_upsert_edges(vec![EdgeInput {
            from: ids[0],
            to: ids[1],
            label: "SCHEMA_WORKS_AT".to_string(),
            props: BTreeMap::from([("role".to_string(), PropValue::String("role-0".to_string()))]),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        }])
        .unwrap();
    (dir, engine)
}

fn assert_gql_schema_targets_published(
    result: overgraph::GqlExecutionResult,
    expected_targets: u64,
) -> overgraph::GqlExecutionResult {
    assert_eq!(result.kind, GqlStatementKind::Schema);
    let stats = result.schema_stats.as_ref().unwrap();
    assert_eq!(stats.targets_published, expected_targets);
    result
}

fn assert_gql_schema_violations(
    result: overgraph::GqlExecutionResult,
    expected_violations: u64,
) -> overgraph::GqlExecutionResult {
    assert_eq!(result.kind, GqlStatementKind::Schema);
    let stats = result.schema_stats.as_ref().unwrap();
    assert_eq!(stats.violation_count, expected_violations);
    result
}

fn bench_gql_queries(c: &mut Criterion) {
    let mut graph_group = c.benchmark_group("graph_row_query");
    graph_group.sample_size(20);

    graph_group.bench_function("graph_row_fixed_connected_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let query = graph_row_fixed_query(source_id, QUERY_LIMIT);
        b.iter(|| {
            let result = engine.query_graph_rows(black_box(&query)).unwrap();
            black_box(assert_graph_row_result_count(result, QUERY_LIMIT))
        });
    });

    graph_group.bench_function("graph_row_optional_edge_traversal_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let query = graph_row_optional_query(source_id, QUERY_LIMIT);
        b.iter(|| {
            let result = engine.query_graph_rows(black_box(&query)).unwrap();
            black_box(assert_graph_row_result_count(result, QUERY_LIMIT))
        });
    });

    graph_group.finish();

    let mut group = c.benchmark_group("execute_gql");
    group.sample_size(20);

    group.bench_function("native_query_node_ids_indexed_property_baseline", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = status_active_query(Some(GqlExecutionOptions::default().max_rows));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("gql_explain_parse_lower_plan_ordered", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant ORDER BY n.score LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .explain_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_return_id_indexed_property", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN id(n)";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_return_property_no_hydration", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_return_node_element_without_vectors", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_order_by_limit", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant ORDER BY n.score LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_full_scan_opt_in", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions {
            allow_full_scan: true,
            ..GqlExecutionOptions::default()
        };
        let query = "MATCH (n) WHERE n.region = 'r03' RETURN id(n) LIMIT 100";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_direct_edge_property_indexed_row_ops", |b| {
        let (_dir, engine, _source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let params = GqlParams::from([(
            "role".to_string(),
            GqlParamValue::String("lead".to_string()),
        )]);
        let options = GqlExecutionOptions::default();
        let query = "MATCH ()-[r:BenchEdge10]->() WHERE r.role = $role RETURN id(r), r.score ORDER BY r.score DESC LIMIT 100";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_include_plan_profile", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions {
            include_plan: true,
            profile: true,
            ..GqlExecutionOptions::default()
        };
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant ORDER BY n.score LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_union_all_two_indexed_branches", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN id(n) AS id \
                     UNION ALL \
                     MATCH (n:BenchNode1) WHERE n.tier = 'gold' RETURN id(n) AS id";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), 1_500);
            black_box(result)
        });
    });

    group.bench_function("gql_union_dedupe_overlapping_indexed_branches", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN id(n) AS id \
                     UNION \
                     MATCH (n:BenchNode1) WHERE n.tier = 'gold' RETURN id(n) AS id";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), 1_000);
            black_box(result)
        });
    });

    group.bench_function("gql_fixed_one_hop_pattern", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (p:BenchNode1)-[r:BenchEdge10]->(c:BenchNode2) RETURN id(p), id(r), id(c)";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_shortest_path_bounded_endpoint_smoke", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (a:BenchNode1) WHERE a.key = 'acct-0' \
                     WITH a \
                     MATCH (b:BenchNode2) WHERE b.key = 'company-0' \
                     WITH a, b \
                     MATCH p = shortestPath((a)-[:BenchEdge10*1..1]->(b)) \
                     RETURN length(p)";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), 1);
            black_box(result)
        });
    });

    group.bench_function("gql_fixed_branching_pattern", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (p:BenchNode1)-[:BenchEdge10]->(c:BenchNode2)<-[:BenchEdge10]-(peer:BenchNode1) RETURN id(p), id(c), id(peer) LIMIT 100";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_graph_row_fixed_connected_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let params = GqlParams::from([
            (
                "role".to_string(),
                GqlParamValue::String("lead".to_string()),
            ),
            ("source".to_string(), GqlParamValue::UInt(source_id)),
        ]);
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (source:BenchNode1)-[edge:BenchEdge10 {role: $role}]->(target:BenchNode2) \
                     WHERE id(source) = $source \
                     RETURN id(source) AS source, id(edge) AS edge, id(target) AS target \
                     ORDER BY edge.score DESC, id(target) LIMIT 100";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), QUERY_LIMIT);
            black_box(result)
        });
    });

    group.bench_function("gql_graph_row_optional_edge_traversal_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let params = GqlParams::from([
            (
                "role".to_string(),
                GqlParamValue::String("lead".to_string()),
            ),
            ("source".to_string(), GqlParamValue::UInt(source_id)),
        ]);
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (source:BenchNode1)-[edge:BenchEdge10 {role: $role}]->(target:BenchNode2) \
                     WHERE id(source) = $source \
                     OPTIONAL MATCH (target)-[ref:BenchEdge20]->(doc:BenchNode3) \
                     RETURN id(source) AS source, id(edge) AS edge, id(target) AS target, \
                            id(ref) AS ref, id(doc) AS doc \
                     ORDER BY edge.score DESC, id(target) LIMIT 100";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), QUERY_LIMIT);
            black_box(result)
        });
    });

    group.bench_function("gql_schema_alter_add_existing_data", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        b.iter_batched(
            setup_gql_schema_existing_data_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(
                        black_box(GQL_SCHEMA_ALTER_ADD_BENCH),
                        black_box(&params),
                        black_box(&options),
                    )
                    .unwrap();
                black_box(assert_gql_schema_targets_published(result, 2))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_schema_check_add_existing_data", |b| {
        let (_dir, engine) = setup_gql_schema_existing_data_db();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        b.iter(|| {
            let result = engine
                .execute_gql(
                    black_box(GQL_SCHEMA_CHECK_ADD_BENCH),
                    black_box(&params),
                    black_box(&options),
                )
                .unwrap();
            black_box(assert_gql_schema_violations(result, 0))
        });
    });

    group.bench_function("gql_mutation_create_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "CREATE (n:GqlBenchCreate {key: 'n', status: 'new'})";
        b.iter_batched(
            temp_gql_mutation_bench_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_created, 1);
                assert_eq!(stats.mutation_ops, 1);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_match_set_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:GqlBenchSet) WHERE n.key = 'n' SET n.status = 'new'";
        b.iter_batched(
            setup_gql_set_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_updated, 1);
                assert_eq!(stats.properties_set, 1);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_merge_create_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MERGE (n:GqlBenchMerge {key: 'n'}) ON CREATE SET n.status = 'created'";
        b.iter_batched(
            temp_gql_mutation_bench_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_created, 1);
                assert_eq!(stats.nodes_updated, 0);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_merge_match_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MERGE (n:GqlBenchMerge {key: 'n'}) ON MATCH SET n.status = 'matched'";
        b.iter_batched(
            setup_gql_merge_match_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_created, 0);
                assert_eq!(stats.nodes_updated, 1);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_detach_delete_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:GqlBenchDetach) WHERE n.key = 'hub' DETACH DELETE n";
        b.iter_batched(
            setup_gql_detach_delete_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_deleted, 1);
                assert_eq!(stats.edges_deleted, 2);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_return_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:GqlBenchReturn) SET n.touched = true RETURN n.key AS key ORDER BY n.rank LIMIT 2";
        b.iter_batched(
            setup_gql_mutation_return_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 2);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.mutation_rows, 3);
                assert_eq!(stats.nodes_updated, 3);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_node_queries,
    bench_edge_queries,
    bench_gql_queries
);
criterion_main!(benches);
