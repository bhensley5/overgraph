use criterion::{black_box, criterion_group, criterion_main, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeInput, EdgePattern, GraphPatternQuery,
    NodeFilterExpr, NodeInput, NodePattern, NodeQuery, PageRequest, PatternOrder, PropValue,
    PropertyRangeBound, SecondaryIndexKind, SecondaryIndexRangeDomain, SecondaryIndexState,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const QUERY_SEGMENT_COUNT: usize = 1;
const QUERY_NODES_PER_SEGMENT: usize = 5_000;
const QUERY_MEMTABLE_TAIL_COUNT: usize = 5_000;
const QUERY_LIMIT: usize = 100;
const QUERY_LARGE_UNIVERSE_COUNT: usize = 25_000;
const QUERY_SMALL_TYPE_COUNT: usize = 128;
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
    (dir, engine)
}

fn temp_db() -> (tempfile::TempDir, DatabaseEngine) {
    temp_db_with_edge_uniqueness(true)
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
            type_id: 1,
            key: format!("{prefix}-{i}"),
            props: query_props(i),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

fn query_nodes_with_type(type_id: u32, prefix: &str, start: usize, count: usize) -> Vec<NodeInput> {
    (start..start + count)
        .map(|i| NodeInput {
            type_id,
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

fn ensure_query_indexes(engine: &mut DatabaseEngine) {
    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);

    let tier = engine
        .ensure_node_property_index(1, "tier", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(engine, tier.index_id, SecondaryIndexState::Ready);

    let tenant = engine
        .ensure_node_property_index(1, "tenant", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(engine, tenant.index_id, SecondaryIndexState::Ready);

    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        engine.batch_upsert_nodes(&nodes).unwrap();
        engine.flush().unwrap();
    }

    let tail_start = QUERY_SEGMENT_COUNT * QUERY_NODES_PER_SEGMENT;
    let tail_nodes = query_nodes(prefix, tail_start, QUERY_MEMTABLE_TAIL_COUNT);
    engine.batch_upsert_nodes(&tail_nodes).unwrap();
}

fn build_fallback_query_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    load_query_mixed_sources(&engine, "fallback");
    (dir, engine)
}

fn build_small_type_universe_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let filler = query_nodes_with_type(2, "large-universe", 0, QUERY_LARGE_UNIVERSE_COUNT);
    engine.batch_upsert_nodes(&filler).unwrap();
    engine.flush().unwrap();

    let segment_small = query_nodes_with_type(1, "small-type", 0, QUERY_SMALL_TYPE_COUNT / 2);
    engine.batch_upsert_nodes(&segment_small).unwrap();
    engine.flush().unwrap();

    let memtable_small = query_nodes_with_type(
        1,
        "small-type",
        QUERY_SMALL_TYPE_COUNT / 2,
        QUERY_SMALL_TYPE_COUNT - QUERY_SMALL_TYPE_COUNT / 2,
    );
    engine.batch_upsert_nodes(&memtable_small).unwrap();
    (dir, engine)
}

fn two_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
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

fn equality_and_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(0),
            upper_ms: Some(i64::MAX),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn type_scan_fallback_query() -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
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

fn type_scoped_verify_only_boolean_query() -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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
        type_id: Some(1),
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

fn boolean_verify_only_type_fallback_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
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
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "region".to_string(),
            values,
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn type_only_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn type_with_large_explicit_ids_query(ids: Vec<u64>) -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
        ids,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn type_with_large_keys_query(keys: Vec<String>) -> NodeQuery {
    NodeQuery {
        type_id: Some(1),
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
        type_id: Some(1),
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

    group.bench_function("query_node_ids_type_scan_fallback", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = type_scan_fallback_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_type_scoped_verify_only_boolean_filter",
        |b| {
            let (_dir, engine) = build_fallback_query_engine();
            let query = type_scoped_verify_only_boolean_query();
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

    group.bench_function("query_node_ids_boolean_verify_only_type_fallback", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = boolean_verify_only_type_fallback_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_boolean_large_in_verify_only_type_fallback",
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

    group.bench_function("query_node_ids_type_only", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = type_only_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_type_vs_large_explicit_ids", |b| {
        let (_dir, engine) = build_small_type_universe_engine();
        let ids = (1..=(QUERY_LARGE_UNIVERSE_COUNT + QUERY_SMALL_TYPE_COUNT) as u64).collect();
        let query = type_with_large_explicit_ids_query(ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_type_vs_large_keys", |b| {
        let (_dir, engine) = build_small_type_universe_engine();
        let mut keys: Vec<String> = (0..QUERY_LARGE_UNIVERSE_COUNT)
            .map(|i| format!("missing-key-{i}"))
            .collect();
        keys.extend((0..QUERY_SMALL_TYPE_COUNT).map(|i| format!("small-type-{i}")));
        let query = type_with_large_keys_query(keys);
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
        let ids = engine.batch_upsert_nodes(&nodes).unwrap();
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
        let ids = engine.nodes_by_type(1).unwrap();
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

fn build_pattern_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, mut engine) = temp_db();
    ensure_query_indexes(&mut engine);
    let account_inputs = query_nodes("acct", 0, 1_000);
    let account_ids = engine.batch_upsert_nodes(&account_inputs).unwrap();

    let companies: Vec<NodeInput> = (0..200)
        .map(|i| NodeInput {
            type_id: 2,
            key: format!("company-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let company_ids = engine.batch_upsert_nodes(&companies).unwrap();

    let edges: Vec<EdgeInput> = account_ids
        .iter()
        .enumerate()
        .map(|(i, &from)| EdgeInput {
            from,
            to: company_ids[i % company_ids.len()],
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();
    (dir, engine, company_ids[0])
}

fn linear_pattern_query() -> GraphPatternQuery {
    GraphPatternQuery {
        nodes: vec![
            NodePattern {
                alias: "person".to_string(),
                type_id: Some(1),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: filter_and![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }],
            },
            NodePattern {
                alias: "company".to_string(),
                type_id: Some(2),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        edges: vec![EdgePattern {
            alias: Some("works_at".to_string()),
            from_alias: "person".to_string(),
            to_alias: "company".to_string(),
            direction: Direction::Outgoing,
            type_filter: Some(vec![10]),
            property_predicates: Vec::new(),
        }],
        at_epoch: None,
        limit: QUERY_LIMIT,
        order: PatternOrder::AnchorThenAliasesAsc,
    }
}

fn boolean_pattern_anchor_query() -> GraphPatternQuery {
    let mut query = linear_pattern_query();
    query.nodes[0].filter = Some(NodeFilterExpr::Or(vec![
        tenant_eq_filter("t07"),
        tenant_eq_filter("t11"),
    ]));
    query
}

fn branching_pattern_query(company_id: u64) -> GraphPatternQuery {
    let mut query = linear_pattern_query();
    query.nodes.push(NodePattern {
        alias: "peer".to_string(),
        type_id: Some(1),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tier".to_string(),
            value: PropValue::String("gold".to_string()),
        }],
    });
    query.edges.push(EdgePattern {
        alias: None,
        from_alias: "peer".to_string(),
        to_alias: "company".to_string(),
        direction: Direction::Outgoing,
        type_filter: Some(vec![10]),
        property_predicates: Vec::new(),
    });
    query.nodes[1].ids = vec![company_id];
    query
}

fn build_high_fanout_pattern_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, engine) = temp_db();
    let source = engine
        .batch_upsert_nodes(&[NodeInput {
            type_id: 1,
            key: "fanout-source".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let targets: Vec<NodeInput> = (0..5_000)
        .map(|i| NodeInput {
            type_id: 2,
            key: format!("fanout-target-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let target_ids = engine.batch_upsert_nodes(&targets).unwrap();
    let edges: Vec<EdgeInput> = target_ids
        .iter()
        .map(|&target| EdgeInput {
            from: source,
            to: target,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();
    (dir, engine, source)
}

fn build_fanout_anchor_choice_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let hub = engine
        .batch_upsert_nodes(&[NodeInput {
            type_id: 1,
            key: "small-hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let mid_inputs: Vec<_> = (0..500)
        .map(|index| NodeInput {
            type_id: 3,
            key: format!("mid-{index:03}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mids = engine.batch_upsert_nodes(&mid_inputs).unwrap();
    let anchor_inputs: Vec<_> = (0..32)
        .map(|index| NodeInput {
            type_id: 2,
            key: format!("anchor-{index:02}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let anchors = engine.batch_upsert_nodes(&anchor_inputs).unwrap();
    let mut edges = Vec::new();
    for &mid in &mids {
        edges.push(EdgeInput {
            from: hub,
            to: mid,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        });
    }
    for (mid, anchor) in mids.iter().take(anchors.len()).zip(anchors.iter()) {
        edges.push(EdgeInput {
            from: *mid,
            to: *anchor,
            type_id: 20,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        });
    }
    engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();
    (dir, engine)
}

fn fanout_anchor_choice_query() -> GraphPatternQuery {
    GraphPatternQuery {
        nodes: vec![
            NodePattern {
                alias: "small_hub".to_string(),
                type_id: Some(1),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
            NodePattern {
                alias: "larger_anchor".to_string(),
                type_id: Some(2),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
            NodePattern {
                alias: "middle".to_string(),
                type_id: Some(3),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        edges: vec![
            EdgePattern {
                alias: Some("hub_to_middle".to_string()),
                from_alias: "small_hub".to_string(),
                to_alias: "middle".to_string(),
                direction: Direction::Outgoing,
                type_filter: Some(vec![10]),
                property_predicates: Vec::new(),
            },
            EdgePattern {
                alias: Some("middle_to_anchor".to_string()),
                from_alias: "middle".to_string(),
                to_alias: "larger_anchor".to_string(),
                direction: Direction::Outgoing,
                type_filter: Some(vec![20]),
                property_predicates: Vec::new(),
            },
        ],
        at_epoch: None,
        limit: QUERY_LIMIT,
        order: PatternOrder::AnchorThenAliasesAsc,
    }
}

fn build_high_hub_delay_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, engine) = temp_db();
    let root = engine
        .batch_upsert_nodes(&[NodeInput {
            type_id: 1,
            key: "root".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let low = engine
        .batch_upsert_nodes(&[NodeInput {
            type_id: 3,
            key: "low".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let target_inputs: Vec<_> = (0..512)
        .map(|index| NodeInput {
            type_id: 2,
            key: format!("hub-target-{index:03}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let targets = engine.batch_upsert_nodes(&target_inputs).unwrap();
    let mut edges = vec![EdgeInput {
        from: root,
        to: low,
        type_id: 20,
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }];
    for target in targets {
        edges.push(EdgeInput {
            from: root,
            to: target,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        });
    }
    engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();
    (dir, engine, root)
}

fn high_hub_delay_query(root: u64) -> GraphPatternQuery {
    GraphPatternQuery {
        nodes: vec![
            NodePattern {
                alias: "root".to_string(),
                type_id: Some(1),
                ids: vec![root],
                keys: Vec::new(),
                filter: None,
            },
            NodePattern {
                alias: "hub_target".to_string(),
                type_id: Some(2),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
            NodePattern {
                alias: "low_target".to_string(),
                type_id: Some(3),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        edges: vec![
            EdgePattern {
                alias: Some("aaa_hub".to_string()),
                from_alias: "root".to_string(),
                to_alias: "hub_target".to_string(),
                direction: Direction::Outgoing,
                type_filter: Some(vec![10]),
                property_predicates: Vec::new(),
            },
            EdgePattern {
                alias: Some("zzz_low".to_string()),
                from_alias: "root".to_string(),
                to_alias: "low_target".to_string(),
                direction: Direction::Outgoing,
                type_filter: Some(vec![20]),
                property_predicates: Vec::new(),
            },
        ],
        at_epoch: None,
        limit: QUERY_LIMIT,
        order: PatternOrder::AnchorThenAliasesAsc,
    }
}

fn build_parallel_edge_pattern_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, engine) = temp_db_with_edge_uniqueness(false);
    let source = engine
        .batch_upsert_nodes(&[NodeInput {
            type_id: 1,
            key: "parallel-source".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let target = engine
        .batch_upsert_nodes(&[NodeInput {
            type_id: 2,
            key: "parallel-target".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let edges: Vec<EdgeInput> = (0..512)
        .map(|_| EdgeInput {
            from: source,
            to: target,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();
    (dir, engine, source)
}

fn unnamed_edge_constraint_query(source_id: u64) -> GraphPatternQuery {
    GraphPatternQuery {
        nodes: vec![
            NodePattern {
                alias: "source".to_string(),
                type_id: Some(1),
                ids: vec![source_id],
                keys: Vec::new(),
                filter: None,
            },
            NodePattern {
                alias: "target".to_string(),
                type_id: Some(2),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        edges: vec![EdgePattern {
            alias: None,
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            type_filter: Some(vec![10]),
            property_predicates: Vec::new(),
        }],
        at_epoch: None,
        limit: QUERY_LIMIT,
        order: PatternOrder::AnchorThenAliasesAsc,
    }
}

fn bench_pattern_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_pattern_planner");
    group.sample_size(20);

    group.bench_function("query_pattern_selective_anchor_linear", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let query = linear_pattern_query();
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.bench_function("query_pattern_boolean_or_union_anchor", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let query = boolean_pattern_anchor_query();
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.bench_function("query_pattern_branching_small", |b| {
        let (_dir, engine, company_id) = build_pattern_engine();
        let query = branching_pattern_query(company_id);
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.bench_function("query_pattern_high_fanout_unnamed_edge", |b| {
        let (_dir, engine, source_id) = build_high_fanout_pattern_engine();
        let query = unnamed_edge_constraint_query(source_id);
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.bench_function("query_pattern_parallel_unnamed_edge_constraint", |b| {
        let (_dir, engine, source_id) = build_parallel_edge_pattern_engine();
        let query = unnamed_edge_constraint_query(source_id);
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.bench_function("query_pattern_fanout_chooses_lower_expansion_anchor", |b| {
        let (_dir, engine) = build_fanout_anchor_choice_engine();
        let query = fanout_anchor_choice_query();
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.bench_function("query_pattern_high_hub_edge_delayed", |b| {
        let (_dir, engine, root) = build_high_hub_delay_engine();
        let query = high_hub_delay_query(root);
        b.iter(|| black_box(engine.query_pattern(black_box(&query)).unwrap()));
    });

    group.finish();
}

criterion_group!(benches, bench_node_queries, bench_pattern_queries);
criterion_main!(benches);
