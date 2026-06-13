// Planner tests: validation, scan-backed node queries, and explain.

// --- validation and oracle helpers ---

fn query_test_props(entries: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn segment_component_path(
    seg_dir: &std::path::Path,
    kind: crate::segment_components::SegmentComponentKind,
) -> std::path::PathBuf {
    let manifest_bytes = std::fs::read(
        seg_dir.join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME),
    )
    .unwrap();
    let manifest = crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
    let record = manifest
        .components
        .iter()
        .find(|record| record.kind == kind)
        .unwrap();
    match &record.handle {
        crate::segment_components::ComponentHandleV1::ExternalFile { relative_path, .. } => {
            seg_dir.join(relative_path)
        }
        crate::segment_components::ComponentHandleV1::PackedRange { .. } => {
            panic!("test component unexpectedly used a packed handle")
        }
    }
}

fn ready_node_property_equality_entry(
    index_id: u64,
    label_id: u32,
    prop_key: &str,
) -> SecondaryIndexManifestEntry {
    SecondaryIndexManifestEntry {
        index_id,
        target: SecondaryIndexTarget::NodeProperty {
            label_id,
            prop_key: prop_key.to_string(),
        },
        kind: SecondaryIndexKind::Equality,
        state: SecondaryIndexState::Ready,
        last_error: None,
    }
}

fn publish_planner_stats_for_test(
    seg_dir: &std::path::Path,
    stats: crate::planner_stats::SegmentPlannerStatsV1,
    ready_indexes: &[SecondaryIndexManifestEntry],
) {
    let payload = crate::planner_stats::planner_stats_sidecar_payload(stats)
        .unwrap()
        .expect("planner stats payload should fit test cap");
    crate::segment_writer::publish_planner_stats_component_payload(
        seg_dir,
        ready_indexes,
        &payload,
    )
    .unwrap();
}

fn corrupt_planner_stats_for_segment(db_path: &std::path::Path, segment_id: u64) {
    let seg_dir = crate::segment_writer::segment_dir(db_path, segment_id);
    let stats_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::PlannerStats,
    );
    std::fs::write(stats_path, b"corrupt planner stats").unwrap();
}

fn test_equality_read_followup(index_id: u64) -> SecondaryIndexReadFollowup {
    SecondaryIndexReadFollowup::EqualitySidecarFailure {
        index_id,
        error: None,
    }
}

fn assert_single_read_followup_enqueued(engine: &DatabaseEngine, action: impl FnOnce()) {
    let (followup_ready_rx, followup_release_tx) = engine.set_runtime_publish_pause();
    action();
    followup_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 1);
    followup_release_tx.send(()).unwrap();
    wait_for_pending_secondary_index_followup_count(engine, 0);
}

fn write_test_bytes_at(path: &std::path::Path, offset: u64, bytes: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn insert_query_node(
    engine: &DatabaseEngine,
    label: &str,
    key: &str,
    entries: &[(&str, PropValue)],
    weight: f32,
) -> u64 {
    engine
        .upsert_node(
            label,
            key,
            UpsertNodeOptions {
                props: query_test_props(entries),
                weight,
                ..Default::default()
            },
        )
        .unwrap()
}

fn insert_query_node_with_labels(
    engine: &DatabaseEngine,
    labels: &[&str],
    key: &str,
    entries: &[(&str, PropValue)],
    weight: f32,
) -> u64 {
    engine
        .upsert_node(
            labels,
            key,
            UpsertNodeOptions {
                props: query_test_props(entries),
                weight,
                ..Default::default()
            },
        )
        .unwrap()
}

fn node_label_filter(labels: &[&str], mode: LabelMatchMode) -> NodeLabelFilter {
    NodeLabelFilter {
        labels: labels.iter().map(|label| (*label).to_string()).collect(),
        mode,
    }
}

fn query_label_filter(labels: &[&str], mode: LabelMatchMode) -> NodeQuery {
    NodeQuery {
        label_filter: Some(node_label_filter(labels, mode)),
        ..Default::default()
    }
}

fn query_ids(
    label: Option<&str>,
    filter_exprs: Vec<NodeFilterExpr>,
    allow_full_scan: bool,
) -> NodeQuery {
    NodeQuery {
        label_filter: label.map(|label| node_label_filter(&[label], LabelMatchMode::All)),
        filter: filter_from_conjunction(filter_exprs),
        allow_full_scan,
        ..Default::default()
    }
}





fn filter_from_conjunction(filter_exprs: Vec<NodeFilterExpr>) -> Option<NodeFilterExpr> {
    match filter_exprs.len() {
        0 => None,
        1 => filter_exprs.into_iter().next(),
        _ => Some(NodeFilterExpr::And(filter_exprs)),
    }
}

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









fn seed_query_test_catalog(engine: &DatabaseEngine) {
    for label in ["Person", "Company", "Article", "Topic", "City"] {
        engine.ensure_node_label(label).unwrap();
    }
    for label in [
        "RELATES_TO",
        "LIKES",
        "FRIENDS_WITH",
        "COLLABORATES_WITH",
        "RELATED_TO",
        "KNOWS",
        "BLOCKS",
    ] {
        engine.ensure_edge_label(label).unwrap();
    }
}

fn query_test_engine() -> (TempDir, DatabaseEngine) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    seed_query_test_catalog(&engine);
    (dir, engine)
}

#[test]
fn edge_query_normalizes_anchors_and_enforces_full_scan_opt_in() {
    let (_dir, engine) = query_test_engine();
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();

    let err = published
        .view
        .normalize_edge_query(&EdgeQuery::default())
        .unwrap_err();
    assert!(
        err.to_string().contains("edge query requires label"),
        "unexpected error: {err}"
    );

    let filter_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let err = published
        .view
        .normalize_edge_query(&filter_only)
        .unwrap_err();
    assert!(
        err.to_string().contains("allow_full_scan"),
        "unexpected error: {err}"
    );

    let metadata_filter_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(0.5),
            upper: None,
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&metadata_filter_only)
        .unwrap();
    assert!(matches!(
        normalized.filter,
        NormalizedEdgeFilter::WeightRange {
            lower: Some(0.5),
            upper: None,
        }
    ));

    let always_false_filter_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: Vec::new(),
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&always_false_filter_only)
        .unwrap();
    assert!(matches!(normalized.filter, NormalizedEdgeFilter::AlwaysFalse));

    let anchored = EdgeQuery {
        label: Some("FRIENDS_WITH".to_string()),
        ids: vec![9, 3, 9, 1],
        from_ids: vec![4, 4, 2],
        to_ids: vec![8, 6, 8],
        endpoint_ids: vec![5, 1, 5],
        ..Default::default()
    };
    let normalized = published.view.normalize_edge_query(&anchored).unwrap();
    let expected_label_id = engine.get_edge_label_id("FRIENDS_WITH").unwrap().unwrap();
    assert_eq!(normalized.label_id, Some(expected_label_id));
    assert_eq!(normalized.ids, vec![1, 3, 9]);
    assert_eq!(normalized.from_ids, vec![2, 4]);
    assert_eq!(normalized.to_ids, vec![6, 8]);
    assert_eq!(normalized.endpoint_ids, vec![1, 5]);
}

#[test]
fn edge_query_filter_validation_and_canonical_in_dedupe() {
    let (_dir, engine) = query_test_engine();
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();

    let duplicate_single = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::PropertyIn {
            key: "score".to_string(),
            values: vec![PropValue::Int(10), PropValue::Int(10)],
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&duplicate_single)
        .unwrap();
    assert!(matches!(
        normalized.filter,
        NormalizedEdgeFilter::PropertyEquals {
            key,
            value: PropValue::Int(10),
        } if key == "score"
    ));

    let signed_zero = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::PropertyIn {
            key: "z".to_string(),
            values: vec![PropValue::Float(-0.0), PropValue::Float(0.0)],
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&signed_zero)
        .unwrap();
    match normalized.filter {
        NormalizedEdgeFilter::PropertyEquals { value, .. } => {
            assert!(prop_values_equal_for_filter(&value, &PropValue::Int(0)));
        }
        other => panic!("expected signed-zero IN to normalize to one semantic value, got {other:?}"),
    }

    let invalid_weight = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(f32::NAN),
            upper: None,
        }),
        ..Default::default()
    };
    let err = published
        .view
        .normalize_edge_query(&invalid_weight)
        .unwrap_err();
    assert!(err.to_string().contains("must not be NaN"));

    let empty_updated_at = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::UpdatedAtRange {
            lower_ms: None,
            upper_ms: None,
        }),
        ..Default::default()
    };
    let err = published
        .view
        .normalize_edge_query(&empty_updated_at)
        .unwrap_err();
    assert!(err.to_string().contains("at least one bound"));

    let inverted_valid_from = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::ValidFromRange {
            lower_ms: Some(20),
            upper_ms: Some(10),
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&inverted_valid_from)
        .unwrap();
    assert!(matches!(normalized.filter, NormalizedEdgeFilter::AlwaysFalse));

    let mixed_property_range = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(1))),
            upper: Some(PropertyRangeBound::Included(PropValue::Float(2.0))),
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&mixed_property_range)
        .unwrap();
    assert!(matches!(
        normalized.filter,
        NormalizedEdgeFilter::PropertyRange { .. }
    ));

    let invalid_property_range = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::String("1".to_string()))),
            upper: None,
        }),
        ..Default::default()
    };
    let err = published
        .view
        .normalize_edge_query(&invalid_property_range)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("range bound must be a finite numeric scalar"));
}

#[test]
fn property_numeric_normalization_dedupes_and_detects_contradictions() {
    let (_dir, engine) = query_test_engine();
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();

    let in_query = NodeQuery {
        allow_full_scan: true,
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "score".to_string(),
            values: vec![
                PropValue::Int(1),
                PropValue::UInt(1),
                PropValue::Float(1.0),
            ],
        }),
        ..Default::default()
    };
    let normalized = published.view.normalize_node_query(&in_query).unwrap();
    match normalized.filter {
        NormalizedNodeFilter::PropertyEquals { value, .. } => {
            assert!(prop_values_equal_for_filter(&value, &PropValue::Float(1.0)));
        }
        other => panic!("expected semantic IN dedupe to one equality, got {other:?}"),
    }

    let compatible_eq = NodeQuery {
        allow_full_scan: true,
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Int(1),
            },
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Float(1.0),
            },
        ])),
        ..Default::default()
    };
    assert!(!published
        .view
        .normalize_node_query(&compatible_eq)
        .unwrap()
        .filter
        .is_always_false());

    let incompatible_eq = NodeQuery {
        allow_full_scan: true,
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Int(1),
            },
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Float(1.5),
            },
        ])),
        ..Default::default()
    };
    assert!(published
        .view
        .normalize_node_query(&incompatible_eq)
        .unwrap()
        .filter
        .is_always_false());

    let empty_mixed_range = NodeQuery {
        allow_full_scan: true,
        filter: Some(NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Excluded(PropValue::Int(2))),
            upper: Some(PropertyRangeBound::Included(PropValue::Float(2.0))),
        }),
        ..Default::default()
    };
    assert!(published
        .view
        .normalize_node_query(&empty_mixed_range)
        .unwrap()
        .filter
        .is_always_false());

    let eq_outside_range = NodeQuery {
        allow_full_scan: true,
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Float(1.5),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(2))),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    assert!(published
        .view
        .normalize_node_query(&eq_outside_range)
        .unwrap()
        .filter
        .is_always_false());
}

#[test]
fn edge_property_scan_verifier_uses_semantic_numeric_equality_and_range() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "semantic-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "semantic-edge-b", &[], 1.0);

    let mut equality_expected = Vec::new();
    for (idx, value) in [
        PropValue::Int(1),
        PropValue::UInt(1),
        PropValue::Float(1.0),
    ]
    .into_iter()
    .enumerate()
    {
        equality_expected.push(
            engine
                .upsert_edge(
                    a,
                    b,
                    "LIKES",
                    UpsertEdgeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
        assert_eq!(idx + 1, equality_expected.len());
    }

    let string_edge = engine
        .upsert_edge(
            a,
            b,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::String("1".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    equality_expected.sort_unstable();
    for rhs in [PropValue::Int(1), PropValue::UInt(1), PropValue::Float(1.0)] {
        let mut actual = engine
            .query_edge_ids(&EdgeQuery {
                label: Some("LIKES".to_string()),
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "score".to_string(),
                    value: rhs,
                }),
                allow_full_scan: true,
                ..Default::default()
            })
            .unwrap()
            .edge_ids;
        actual.sort_unstable();
        assert_eq!(actual, equality_expected);
    }

    let range_result = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("LIKES".to_string()),
            filter: Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Float(-0.0))),
                upper: Some(PropertyRangeBound::Included(PropValue::Float(1.0))),
            }),
            allow_full_scan: true,
            ..Default::default()
        })
        .unwrap()
        .edge_ids;
    assert_eq!(range_result, equality_expected);

    let raw_string_result = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("LIKES".to_string()),
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::String("1".to_string()),
            }),
            allow_full_scan: true,
            ..Default::default()
        })
        .unwrap()
        .edge_ids;
    assert_eq!(raw_string_result, vec![string_edge]);
}

#[test]
fn edge_query_metadata_and_hydrated_verifier_semantics() {
    let (_dir, engine) = query_test_engine();
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let edge_label_id = engine.get_edge_label_id("LIKES").unwrap().unwrap();
    let edge = EdgeRecord {
        id: 42,
        from: 7,
        to: 9,
        label_id: edge_label_id,
        props: query_test_props(&[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(5)),
        ]),
        created_at: 1,
        updated_at: 100,
        weight: 0.75,
        valid_from: 10,
        valid_to: 20,
        last_write_seq: 0,
    };

    let query = EdgeQuery {
        label: Some("LIKES".to_string()),
        ids: vec![42],
        from_ids: vec![7],
        endpoint_ids: vec![9],
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::WeightRange {
                lower: Some(0.5),
                upper: Some(1.0),
            },
            EdgeFilterExpr::UpdatedAtRange {
                lower_ms: Some(90),
                upper_ms: Some(110),
            },
            EdgeFilterExpr::ValidAt { epoch_ms: 10 },
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ])),
        ..Default::default()
    };
    let normalized = published.view.normalize_edge_query(&query).unwrap();
    let meta = EdgeMetadataForQuery::from(&edge);
    assert!(edge_filter_requires_hydration(&normalized.filter));
    assert!(edge_query_metadata_matches(&normalized, &meta));
    assert!(edge_query_matches(&normalized, &edge));

    let expired = EdgeQuery {
        label: Some("LIKES".to_string()),
        ids: vec![42],
        filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 20 }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&expired)
        .unwrap();
    assert!(!edge_query_metadata_matches(&normalized, &meta));
    assert!(!edge_query_matches(&normalized, &edge));

    let nan_weight = EdgeRecord {
        weight: f32::NAN,
        ..edge.clone()
    };
    let range_query = EdgeQuery {
        label: Some("LIKES".to_string()),
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(0.0),
            upper: Some(1.0),
        }),
        ..Default::default()
    };
    let normalized = published
        .view
        .normalize_edge_query(&range_query)
        .unwrap();
    let meta = EdgeMetadataForQuery::from(&nan_weight);
    assert!(!edge_query_metadata_matches(&normalized, &meta));
    assert!(!edge_query_matches(&normalized, &nan_weight));
}

#[test]
fn edge_query_executes_type_endpoint_metadata_and_explain_sources() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "c", &[], 1.0);

    let keep = engine
        .upsert_edge(
            a,
            b,
            "KNOWS",
            UpsertEdgeOptions {
                weight: -0.0,
                valid_from: Some(10),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            "KNOWS",
            UpsertEdgeOptions {
                weight: 2.0,
                valid_from: Some(10),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            b,
            c,
            "REPORTS_TO",
            UpsertEdgeOptions {
                weight: 0.0,
                valid_from: Some(10),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("KNOWS".to_string()),
        from_ids: vec![a],
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(0.0),
            upper: Some(0.0),
        }),
        ..Default::default()
    };

    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![keep]);
    assert_eq!(ids.next_cursor, None);

    let edges = engine.query_edges(&query).unwrap();
    assert_eq!(
        edges.edges.iter().map(|edge| edge.id).collect::<Vec<_>>(),
        vec![keep]
    );

    let plan = engine.explain_edge_query(&query).unwrap();
    assert_eq!(plan.kind, QueryPlanKind::EdgeQuery);
    assert!(matches!(
        &plan.root,
        QueryPlanNode::VerifyEdgeFilter { .. }
    ));
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeLabelIndex));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgeEndpointAdjacency
    ));
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeMetadataScan));
}

#[test]
fn edge_query_triple_index_returns_parallel_edges() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);

    let first = engine
        .upsert_edge(
            a,
            b,
            "FRIENDS_WITH",
            UpsertEdgeOptions {
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    let second = engine
        .upsert_edge(
            a,
            b,
            "FRIENDS_WITH",
            UpsertEdgeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    assert_ne!(first, second);

    let query = EdgeQuery {
        label: Some("FRIENDS_WITH".to_string()),
        from_ids: vec![a],
        to_ids: vec![b],
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![first, second]);

    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeTripleIndex));
}

#[test]
fn edge_query_reads_segment_and_active_memtable_sources() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "c", &[], 1.0);

    let flushed = engine
        .upsert_edge(a, b, "BLOCKS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    let active = engine
        .upsert_edge(a, c, "BLOCKS", UpsertEdgeOptions::default())
        .unwrap();

    let query = EdgeQuery {
        label: Some("BLOCKS".to_string()),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![flushed, active]);
}

#[test]
fn edge_query_id_range_and_created_at_range_anchor_metadata_scan_without_full_scan() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "meta-anchor-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "meta-anchor-b", &[], 1.0);

    let first = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let second = engine
        .upsert_edge(b, a, "LIKES", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    let third = engine
        .upsert_edge(a, b, "BLOCKS", UpsertEdgeOptions::default())
        .unwrap();

    let id_range_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::IdRange {
            lower: Some(second),
            upper: None,
            lower_inclusive: true,
            upper_inclusive: true,
        }),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&id_range_only).unwrap();
    assert_eq!(ids.edge_ids, vec![second, third]);
    let plan = engine.explain_edge_query(&id_range_only).unwrap();
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeMetadataScan));

    let exclusive_id_range_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::IdRange {
            lower: Some(first),
            upper: Some(third),
            lower_inclusive: false,
            upper_inclusive: false,
        }),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&exclusive_id_range_only).unwrap();
    assert_eq!(ids.edge_ids, vec![second]);

    let created_at_range_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::CreatedAtRange {
            lower: Some(1),
            upper: None,
            lower_inclusive: true,
            upper_inclusive: true,
        }),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&created_at_range_only).unwrap();
    assert_eq!(ids.edge_ids, vec![first, second, third]);
    let plan = engine.explain_edge_query(&created_at_range_only).unwrap();
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeMetadataScan));

    let empty_created_at_range_only = EdgeQuery {
        filter: Some(EdgeFilterExpr::CreatedAtRange {
            lower: None,
            upper: Some(0),
            lower_inclusive: true,
            upper_inclusive: true,
        }),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&empty_created_at_range_only).unwrap();
    assert!(ids.edge_ids.is_empty());

    let or_with_unindexed_anchor = EdgeQuery {
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::IdRange {
                lower: Some(third),
                upper: None,
                lower_inclusive: true,
                upper_inclusive: true,
            },
            EdgeFilterExpr::WeightRange {
                lower: Some(2.0),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&or_with_unindexed_anchor).unwrap();
    assert_eq!(ids.edge_ids, vec![third]);
}

#[test]
fn edge_query_metadata_sidecar_unavailable_falls_back_at_engine_level() {
    #[derive(Clone, Copy)]
    enum SidecarRewrite {
        Missing,
        Corrupt,
    }

    fn edge_metadata_kind(logical_name: &str) -> crate::segment_components::SegmentComponentKind {
        match logical_name {
            crate::edge_metadata::EDGE_WEIGHT_INDEX_LOGICAL_NAME => {
                crate::segment_components::SegmentComponentKind::EdgeWeightIndex
            }
            crate::edge_metadata::EDGE_UPDATED_AT_INDEX_LOGICAL_NAME => {
                crate::segment_components::SegmentComponentKind::EdgeUpdatedAtIndex
            }
            crate::edge_metadata::EDGE_VALID_FROM_INDEX_LOGICAL_NAME => {
                crate::segment_components::SegmentComponentKind::EdgeValidFromIndex
            }
            crate::edge_metadata::EDGE_VALID_TO_INDEX_LOGICAL_NAME => {
                crate::segment_components::SegmentComponentKind::EdgeValidToIndex
            }
            other => panic!("unexpected edge metadata logical name {other}"),
        }
    }

    fn rewrite_sidecar(seg_dir: &std::path::Path, logical_name: &str, mode: SidecarRewrite) {
        let kind = edge_metadata_kind(logical_name);
        match mode {
            SidecarRewrite::Missing => {
                let manifest_path = seg_dir
                    .join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME);
                let mut manifest = crate::segment_components::decode_manifest_envelope(
                    &std::fs::read(&manifest_path).unwrap(),
                )
                .unwrap();
                manifest.components.retain(|record| record.kind != kind);
                let data = crate::segment_components::encode_manifest_envelope(&manifest).unwrap();
                std::fs::write(manifest_path, data).unwrap();
            }
            SidecarRewrite::Corrupt => {
                let manifest_path = seg_dir
                    .join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME);
                let manifest = crate::segment_components::decode_manifest_envelope(
                    &std::fs::read(&manifest_path).unwrap(),
                )
                .unwrap();
                let record = manifest
                    .components
                    .iter()
                    .find(|record| record.kind == kind)
                    .unwrap();
                match &record.handle {
                    crate::segment_components::ComponentHandleV1::ExternalFile {
                        relative_path,
                        ..
                    } => write_test_bytes_at(
                        &seg_dir.join(relative_path),
                        0,
                        b"corrupt metadata sidecar",
                    ),
                    crate::segment_components::ComponentHandleV1::PackedRange { offset, .. } => {
                        let core_path =
                            seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME);
                        let core = std::fs::read(&core_path).unwrap();
                        let header =
                            crate::segment_components::decode_identity_header(&core).unwrap();
                        let start = header.payload_offset as usize + *offset as usize;
                        write_test_bytes_at(&core_path, start as u64, &u64::MAX.to_le_bytes());
                    }
                }
            }
        }
    }

    fn run_case(logical_name: &str, filter: EdgeFilterExpr, rewrite: SidecarRewrite) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
        let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
        let c = insert_query_node(&engine, "Person",  "c", &[], 1.0);
        let first = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_41",
                UpsertEdgeOptions {
                    weight: 1.0,
                    valid_from: Some(10),
                    valid_to: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_41",
                UpsertEdgeOptions {
                    weight: 2.0,
                    valid_from: Some(20),
                    valid_to: Some(200),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let query = EdgeQuery {
            label: Some("EDGE_LABEL_41".to_string()),
            filter: Some(filter),
            ..Default::default()
        };
        let baseline = engine.query_edge_ids(&query).unwrap().edge_ids;
        assert!(
            baseline.contains(&first),
            "baseline should include the selective edge for {logical_name}"
        );
        engine.close().unwrap();

        let seg_dir = crate::segment_writer::segment_dir(&db_path, 1);
        rewrite_sidecar(&seg_dir, logical_name, rewrite);

        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, baseline);
        let plan = reopened.explain_edge_query(&query).unwrap();
        assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeMetadataScan));
        reopened.close().unwrap();
    }

    run_case(
        crate::edge_metadata::EDGE_WEIGHT_INDEX_LOGICAL_NAME,
        EdgeFilterExpr::WeightRange {
            lower: Some(1.0),
            upper: Some(1.0),
        },
        SidecarRewrite::Missing,
    );
    run_case(
        crate::edge_metadata::EDGE_UPDATED_AT_INDEX_LOGICAL_NAME,
        EdgeFilterExpr::UpdatedAtRange {
            lower_ms: Some(i64::MIN),
            upper_ms: Some(i64::MAX),
        },
        SidecarRewrite::Corrupt,
    );
    run_case(
        crate::edge_metadata::EDGE_VALID_FROM_INDEX_LOGICAL_NAME,
        EdgeFilterExpr::ValidFromRange {
            lower_ms: Some(10),
            upper_ms: Some(10),
        },
        SidecarRewrite::Missing,
    );
    run_case(
        crate::edge_metadata::EDGE_VALID_TO_INDEX_LOGICAL_NAME,
        EdgeFilterExpr::ValidToRange {
            lower_ms: Some(100),
            upper_ms: Some(100),
        },
        SidecarRewrite::Corrupt,
    );
}

#[test]
fn edge_query_property_filter_uses_legal_universe_and_hydrates() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "c", &[], 1.0);

    let keep = engine
        .upsert_edge(
            a,
            b,
            "DEPENDS_ON",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            "DEPENDS_ON",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("DEPENDS_ON".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![keep]);

    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan.warnings.contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));

    let metadata_and_property_query = EdgeQuery {
        label: Some("DEPENDS_ON".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::WeightRange {
                lower: Some(0.5),
                upper: Some(1.5),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine
            .query_edge_ids(&metadata_and_property_query)
            .unwrap()
            .edge_ids,
        vec![keep]
    );
    let mixed_plan = engine
        .explain_edge_query(&metadata_and_property_query)
        .unwrap();
    assert!(mixed_plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(mixed_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
}

#[test]
fn edge_query_uses_ready_edge_property_equality_index() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "eq-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "eq-edge-b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "eq-edge-c", &[], 1.0);

    let keep = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_82",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_82",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_82", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_82".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(!plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(!plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));

    engine.reset_query_execution_counters_for_test();
    let ids = engine.query_edge_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(ids.edge_ids, vec![keep]);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
}

#[test]
fn edge_active_equality_index_uses_semantic_hashes() {
    let (_dir, engine) = query_test_engine();
    let info = engine
        .ensure_edge_property_index("EDGE_SEMANTIC_ACTIVE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let a = insert_query_node(&engine, "Person", "edge-sem-active-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "edge-sem-active-b", &[], 1.0);
    let mut expected_numeric = Vec::new();
    for value in [PropValue::Int(1), PropValue::UInt(1), PropValue::Float(1.0)] {
        expected_numeric.push(
            engine
                .upsert_edge(
                    a,
                    b,
                    "EDGE_SEMANTIC_ACTIVE",
                    UpsertEdgeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    expected_numeric.sort_unstable();

    let string_edge = engine
        .upsert_edge(
            a,
            b,
            "EDGE_SEMANTIC_ACTIVE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::String("1".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let array_edge = engine
        .upsert_edge(
            a,
            b,
            "EDGE_SEMANTIC_ACTIVE",
            UpsertEdgeOptions {
                props: query_test_props(&[(
                    "score",
                    PropValue::Array(vec![PropValue::Int(1)]),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    let mut map_int = BTreeMap::new();
    map_int.insert("x".to_string(), PropValue::Int(1));
    let map_edge = engine
        .upsert_edge(
            a,
            b,
            "EDGE_SEMANTIC_ACTIVE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Map(map_int))]),
                ..Default::default()
            },
        )
        .unwrap();
    let inf_edge = engine
        .upsert_edge(
            a,
            b,
            "EDGE_SEMANTIC_ACTIVE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Float(f64::INFINITY))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            b,
            "EDGE_SEMANTIC_ACTIVE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Float(f64::NAN))]),
                ..Default::default()
            },
        )
        .unwrap();

    let query_ids_for = |value: PropValue| {
        engine
            .query_edge_ids(&EdgeQuery {
                label: Some("EDGE_SEMANTIC_ACTIVE".to_string()),
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "score".to_string(),
                    value,
                }),
                ..Default::default()
            })
            .unwrap()
            .edge_ids
    };

    for rhs in [PropValue::Int(1), PropValue::UInt(1), PropValue::Float(1.0)] {
        assert_eq!(query_ids_for(rhs), expected_numeric);
    }
    assert_eq!(
        query_ids_for(PropValue::String("1".to_string())),
        vec![string_edge]
    );
    assert_eq!(
        query_ids_for(PropValue::Array(vec![PropValue::Int(1)])),
        vec![array_edge]
    );
    assert!(query_ids_for(PropValue::Array(vec![PropValue::Float(1.0)])).is_empty());
    let mut map_float = BTreeMap::new();
    map_float.insert("x".to_string(), PropValue::Float(1.0));
    assert_eq!(
        query_ids_for(PropValue::Map({
            let mut expected = BTreeMap::new();
            expected.insert("x".to_string(), PropValue::Int(1));
            expected
        })),
        vec![map_edge]
    );
    assert!(query_ids_for(PropValue::Map(map_float)).is_empty());
    assert_eq!(query_ids_for(PropValue::Float(f64::INFINITY)), vec![inf_edge]);
    assert!(query_ids_for(PropValue::Float(f64::NAN)).is_empty());

    let plan = engine
        .explain_edge_query(&EdgeQuery {
            label: Some("EDGE_SEMANTIC_ACTIVE".to_string()),
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Float(1.0),
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
}

#[test]
fn semantic_equality_sidecars_survive_flush_reopen_and_compaction_for_nodes_and_edges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let first_node;
    let second_node;
    let first_edge;
    let second_edge;
    let node_index;
    let edge_index;
    let a;
    let b;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        node_index = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap()
            .index_id;
        edge_index = engine
            .ensure_edge_property_index("EDGE_SEMANTIC_SEG", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap()
            .index_id;
        wait_for_property_index_state(&engine, node_index, SecondaryIndexState::Ready);
        wait_for_edge_property_index_state(&engine, edge_index, SecondaryIndexState::Ready);

        a = insert_query_node(&engine, "Person", "semantic-sidecar-a", &[], 1.0);
        b = insert_query_node(&engine, "Person", "semantic-sidecar-b", &[], 1.0);
        first_node = engine
            .upsert_node(
                "Person",
                "semantic-sidecar-int",
                UpsertNodeOptions {
                    props: query_test_props(&[("score", PropValue::Int(1))]),
                    ..Default::default()
                },
            )
            .unwrap();
        second_node = engine
            .upsert_node(
                "Person",
                "semantic-sidecar-uint",
                UpsertNodeOptions {
                    props: query_test_props(&[("score", PropValue::UInt(1))]),
                    ..Default::default()
                },
            )
            .unwrap();
        first_edge = engine
            .upsert_edge(
                a,
                b,
                "EDGE_SEMANTIC_SEG",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::Int(1))]),
                    ..Default::default()
                },
            )
            .unwrap();
        second_edge = engine
            .upsert_edge(
                a,
                b,
                "EDGE_SEMANTIC_SEG",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::UInt(1))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&reopened, node_index, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&reopened, edge_index, SecondaryIndexState::Ready);

    let mut reopened_nodes = reopened
        .find_nodes("Person", "score", &PropValue::Float(1.0))
        .unwrap();
    reopened_nodes.sort_unstable();
    assert_eq!(reopened_nodes, vec![first_node, second_node]);
    let edge_query = |value: PropValue| EdgeQuery {
        label: Some("EDGE_SEMANTIC_SEG".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "score".to_string(),
            value,
        }),
        ..Default::default()
    };
    assert_eq!(
        reopened
            .query_edge_ids(&edge_query(PropValue::Float(1.0)))
            .unwrap()
            .edge_ids,
        vec![first_edge, second_edge]
    );

    let third_node = reopened
        .upsert_node(
            "Person",
            "semantic-sidecar-float",
            UpsertNodeOptions {
                props: query_test_props(&[("score", PropValue::Float(1.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    let third_edge = reopened
        .upsert_edge(
            a,
            b,
            "EDGE_SEMANTIC_SEG",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Float(1.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    reopened.flush().unwrap();

    let stats = reopened.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);

    let mut compacted_nodes = reopened
        .find_nodes("Person", "score", &PropValue::UInt(1))
        .unwrap();
    compacted_nodes.sort_unstable();
    assert_eq!(compacted_nodes, vec![first_node, second_node, third_node]);
    assert_eq!(
        reopened
            .query_edge_ids(&edge_query(PropValue::UInt(1)))
            .unwrap()
            .edge_ids,
        vec![first_edge, second_edge, third_edge]
    );

    reopened.close().unwrap();
    let compacted_reopen = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&compacted_reopen, node_index, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&compacted_reopen, edge_index, SecondaryIndexState::Ready);
    let mut final_nodes = compacted_reopen
        .find_nodes("Person", "score", &PropValue::Int(1))
        .unwrap();
    final_nodes.sort_unstable();
    assert_eq!(final_nodes, vec![first_node, second_node, third_node]);
    assert_eq!(
        compacted_reopen
            .query_edge_ids(&edge_query(PropValue::Int(1)))
            .unwrap()
            .edge_ids,
        vec![first_edge, second_edge, third_edge]
    );
}

#[test]
fn edge_query_uses_ready_edge_property_range_index() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "range-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "range-edge-b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "range-edge-c", &[], 1.0);
    let d = insert_query_node(&engine, "Person",  "range-edge-d", &[], 1.0);

    engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_83",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(2))]),
                ..Default::default()
            },
        )
        .unwrap();
    let keep = engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_83",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(5))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            d,
            "EDGE_LABEL_83",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(9))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_83", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_83".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(4))),
            upper: Some(PropertyRangeBound::Excluded(PropValue::Int(9))),
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
    assert!(!plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(!plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_eq!(engine.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
}

#[test]
fn edge_property_range_index_query_paginates_by_edge_id_cursor() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "range-page-edge-a", &[], 1.0);
    let targets = (0..5)
        .map(|idx| insert_query_node(&engine, "Person",  &format!("range-page-edge-{idx}"), &[], 1.0))
        .collect::<Vec<_>>();

    engine
        .upsert_edge(
            a,
            targets[0],
            "SPECIAL_EDGE_831",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(10))]),
                ..Default::default()
            },
        )
        .unwrap();
    let first = engine
        .upsert_edge(
            a,
            targets[1],
            "SPECIAL_EDGE_831",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(80))]),
                ..Default::default()
            },
        )
        .unwrap();
    let second = engine
        .upsert_edge(
            a,
            targets[2],
            "SPECIAL_EDGE_831",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(90))]),
                ..Default::default()
            },
        )
        .unwrap();
    let third = engine
        .upsert_edge(
            a,
            targets[3],
            "SPECIAL_EDGE_831",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(100))]),
                ..Default::default()
            },
        )
        .unwrap();
    let deleted = engine
        .upsert_edge(
            a,
            targets[4],
            "SPECIAL_EDGE_831",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(110))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.delete_edge(deleted).unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("SPECIAL_EDGE_831", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut query = EdgeQuery {
        label: Some("SPECIAL_EDGE_831".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(80))),
            upper: None,
        }),
        page: PageRequest {
            limit: Some(2),
            after: None,
        },
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));

    let first_page = engine.query_edge_ids(&query).unwrap();
    assert_eq!(first_page.edge_ids, vec![first, second]);
    assert_eq!(first_page.next_cursor, Some(second));

    query.page.after = first_page.next_cursor;
    let second_page = engine.query_edge_ids(&query).unwrap();
    assert_eq!(second_page.edge_ids, vec![third]);
    assert_eq!(second_page.next_cursor, None);
}

#[test]
fn edge_property_index_does_not_make_filter_only_edge_query_legal() {
    let (_dir, engine) = query_test_engine();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_84", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let err = engine.query_edge_ids(&query).unwrap_err();
    assert!(
        err.to_string().contains("edge query requires label"),
        "unexpected error: {err}"
    );
}

#[test]
fn edge_query_missing_edge_property_index_remains_verifier_only() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "missing-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "missing-edge-b", &[], 1.0);
    let keep = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_85",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_85".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_eq!(engine.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
}

#[test]
fn edge_property_in_uses_index_union_and_preserves_signed_zero() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "edge-in-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "edge-in-b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "edge-in-c", &[], 1.0);
    let d = insert_query_node(&engine, "Person",  "edge-in-d", &[], 1.0);

    let positive_zero = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_86",
            UpsertEdgeOptions {
                props: query_test_props(&[("z", PropValue::Float(0.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    let negative_zero = engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_86",
            UpsertEdgeOptions {
                props: query_test_props(&[("z", PropValue::Float(-0.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            d,
            "EDGE_LABEL_86",
            UpsertEdgeOptions {
                props: query_test_props(&[("z", PropValue::Float(1.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    for idx in 0..10 {
        engine
            .upsert_edge(
                b,
                d,
                "EDGE_LABEL_86",
                UpsertEdgeOptions {
                    props: query_test_props(&[("z", PropValue::Float(idx as f64 + 2.0))]),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_86", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("z").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_86".to_string()),
        filter: Some(EdgeFilterExpr::PropertyIn {
            key: "z".to_string(),
            values: vec![
                PropValue::Float(-0.0),
                PropValue::Float(0.0),
                PropValue::Float(-0.0),
            ],
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(!plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_eq!(
        engine.query_edge_ids(&query).unwrap().edge_ids,
        vec![positive_zero, negative_zero]
    );
}

#[test]
fn edge_property_range_uses_ready_v2_numeric_range_sidecar() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "edge-domain-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "edge-domain-b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_87",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Float(5.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_87", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_87".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Float(4.0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Float(6.0))),
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
    assert!(!plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert!(!plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert_eq!(engine.query_edge_ids(&query).unwrap().edge_ids, vec![edge_id]);
}

#[test]
fn edge_property_or_with_verifier_branch_falls_back_whole_or() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "edge-or-index-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "edge-or-index-b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "edge-or-index-c", &[], 1.0);
    let indexed = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_88",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let exists_only = engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_88",
            UpsertEdgeOptions {
                props: query_test_props(&[("tag", PropValue::String("present".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_88", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_88".to_string()),
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            EdgeFilterExpr::PropertyExists {
                key: "tag".to_string(),
            },
        ])),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::BooleanBranchFallback));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_eq!(
        engine.query_edge_ids(&query).unwrap().edge_ids,
        vec![indexed, exists_only]
    );
}

#[test]
fn edge_property_indexed_not_filters_use_bounded_positive_universe_only() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "edge-not-source", &[], 1.0);
    let active_keep_node = insert_query_node(&engine, "Person",  "edge-not-active-keep", &[], 1.0);
    let active_drop_node = insert_query_node(&engine, "Person",  "edge-not-active-drop", &[], 1.0);
    let inactive_flagged_node =
        insert_query_node(&engine, "Person",  "edge-not-inactive-flagged", &[], 1.0);
    let inactive_plain_node =
        insert_query_node(&engine, "Person",  "edge-not-inactive-plain", &[], 1.0);

    let active_keep = engine
        .upsert_edge(
            source,
            active_keep_node,
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let active_drop = engine
        .upsert_edge(
            source,
            active_drop_node,
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("status", PropValue::String("active".to_string())),
                    ("flag", PropValue::String("drop".to_string())),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    let inactive_flagged = engine
        .upsert_edge(
            source,
            inactive_flagged_node,
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("status", PropValue::String("inactive".to_string())),
                    ("flag", PropValue::String("keep".to_string())),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            inactive_plain_node,
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_89", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let and_not_query = EdgeQuery {
        label: Some("EDGE_LABEL_89".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            EdgeFilterExpr::Not(Box::new(EdgeFilterExpr::PropertyEquals {
                key: "flag".to_string(),
                value: PropValue::String("drop".to_string()),
            })),
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_edge_ids(&and_not_query).unwrap().edge_ids,
        vec![active_keep]
    );
    let and_not_plan = engine.explain_edge_query(&and_not_query).unwrap();
    assert!(plan_contains_node(
        &and_not_plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(and_not_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));

    let or_not_query = EdgeQuery {
        label: Some("EDGE_LABEL_89".to_string()),
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            EdgeFilterExpr::Not(Box::new(EdgeFilterExpr::PropertyMissing {
                key: "flag".to_string(),
            })),
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_edge_ids(&or_not_query).unwrap().edge_ids,
        vec![active_keep, active_drop, inactive_flagged]
    );
    let or_not_plan = engine.explain_edge_query(&or_not_query).unwrap();
    assert!(!plan_contains_node(
        &or_not_plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(plan_contains_node(
        &or_not_plan.root,
        &QueryPlanNode::EdgeLabelIndex
    ));
    assert!(or_not_plan
        .warnings
        .contains(&QueryPlanWarning::BooleanBranchFallback));

    let endpoint_not_query = EdgeQuery {
        from_ids: vec![source],
        filter: Some(EdgeFilterExpr::Not(Box::new(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("inactive".to_string()),
        }))),
        ..Default::default()
    };
    assert_eq!(
        engine.query_edge_ids(&endpoint_not_query).unwrap().edge_ids,
        vec![active_keep, active_drop]
    );
    let endpoint_not_plan = engine.explain_edge_query(&endpoint_not_query).unwrap();
    assert!(!plan_contains_node(
        &endpoint_not_plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(plan_contains_node(
        &endpoint_not_plan.root,
        &QueryPlanNode::EdgeEndpointAdjacency
    ));
}

#[test]
fn edge_property_index_visibility_merges_active_frozen_and_segments() {
    let (_dir, engine) = query_test_engine();
    let nodes = (0..8)
        .map(|idx| insert_query_node(&engine, "Person",  &format!("edge-vis-{idx}"), &[], 1.0))
        .collect::<Vec<_>>();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_89", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let shadowed = engine
        .upsert_edge(
            nodes[0],
            nodes[1],
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let deleted = engine
        .upsert_edge(
            nodes[0],
            nodes[2],
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let segment_keep = engine
        .upsert_edge(
            nodes[0],
            nodes[3],
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    set_query_edge_props(
        &engine,
        shadowed,
        query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
    );
    engine.delete_edge(deleted).unwrap();
    let frozen_keep = engine
        .upsert_edge(
            nodes[0],
            nodes[4],
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.freeze_memtable().unwrap();
    let active_keep = engine
        .upsert_edge(
            nodes[0],
            nodes[5],
            "EDGE_LABEL_89",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_89".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_edge_ids(&query).unwrap().edge_ids,
        vec![segment_keep, frozen_keep, active_keep]
    );
}

#[test]
fn edge_property_equality_verifier_filters_stale_or_colliding_postings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let red_one;
    let red_two;
    let blue;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let nodes = (0..4)
            .map(|idx| insert_query_node(&engine, "Person",  &format!("edge-collision-{idx}"), &[], 1.0))
            .collect::<Vec<_>>();
        red_one = engine
            .upsert_edge(
                nodes[0],
                nodes[1],
                "EDGE_LABEL_90",
                UpsertEdgeOptions {
                    props: query_test_props(&[("color", PropValue::String("red".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        red_two = engine
            .upsert_edge(
                nodes[0],
                nodes[2],
                "EDGE_LABEL_90",
                UpsertEdgeOptions {
                    props: query_test_props(&[("color", PropValue::String("red".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        blue = engine
            .upsert_edge(
                nodes[0],
                nodes[3],
                "EDGE_LABEL_90",
                UpsertEdgeOptions {
                    props: query_test_props(&[("color", PropValue::String("blue".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_90", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
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
    let query = EdgeQuery {
        label: Some("EDGE_LABEL_90".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "color".to_string(),
            value: PropValue::String("red".to_string()),
        }),
        ..Default::default()
    };
    let plan = reopened.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![red_one]);
}

#[test]
fn edge_property_query_falls_back_and_marks_corrupt_equality_sidecar_failed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_query_node(&engine, "Person",  "edge-corrupt-a", &[], 1.0);
        let b = insert_query_node(&engine, "Person",  "edge-corrupt-b", &[], 1.0);
        keep = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_91",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_91", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::edge_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_sidecar_header_in_place(&sidecar_path);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = EdgeQuery {
        label: Some("EDGE_LABEL_91".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
    wait_for_edge_property_index_state(&reopened, index_id, SecondaryIndexState::Failed);
}

#[test]
fn edge_property_query_enqueues_planning_followup_for_corrupt_equality_sidecar() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_query_node(&engine, "Person",  "edge-plan-eq-a", &[], 1.0);
        let b = insert_query_node(&engine, "Person",  "edge-plan-eq-b", &[], 1.0);
        let c = insert_query_node(&engine, "Person",  "edge-plan-eq-c", &[], 1.0);
        keep = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_94",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_94",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_94", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
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
    let query = EdgeQuery {
        label: Some("EDGE_LABEL_94".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let plan = reopened.explain_edge_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgeLabelIndex
    ));

    let (followup_ready_rx, followup_release_tx) = reopened.set_runtime_publish_pause();
    assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
    followup_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(reopened.pending_secondary_index_followup_count_for_test(), 1);
    followup_release_tx.send(()).unwrap();
    wait_for_pending_secondary_index_followup_count(&reopened, 0);
}

#[test]
fn edge_property_query_enqueues_planning_followup_for_corrupt_range_sidecar() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_query_node(&engine, "Person",  "edge-corrupt-range-a", &[], 1.0);
        let b = insert_query_node(&engine, "Person",  "edge-corrupt-range-b", &[], 1.0);
        let c = insert_query_node(&engine, "Person",  "edge-corrupt-range-c", &[], 1.0);
        keep = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_95",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::Int(7))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_95",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::Int(20))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_95", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::edge_prop_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_planner_stats_for_segment(&db_path, segment_id);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_edge_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    corrupt_sidecar_header_in_place(&sidecar_path);
    let query = EdgeQuery {
        label: Some("EDGE_LABEL_95".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(10))),
        }),
        ..Default::default()
    };
    let plan = reopened.explain_edge_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgeLabelIndex
    ));

    let (followup_ready_rx, followup_release_tx) = reopened.set_runtime_publish_pause();
    assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
    followup_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(reopened.pending_secondary_index_followup_count_for_test(), 1);
    followup_release_tx.send(()).unwrap();
    wait_for_pending_secondary_index_followup_count(&reopened, 0);
}

#[test]
fn edge_property_query_falls_back_and_marks_corrupt_range_sidecar_failed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_query_node(&engine, "Person",  "edge-corrupt-range-failed-a", &[], 1.0);
        let b = insert_query_node(&engine, "Person",  "edge-corrupt-range-failed-b", &[], 1.0);
        let c = insert_query_node(&engine, "Person",  "edge-corrupt-range-failed-c", &[], 1.0);
        keep = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_96",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::Int(7))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_96",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::Int(20))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_96", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::edge_prop_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_sidecar_header_in_place(&sidecar_path);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = EdgeQuery {
        label: Some("EDGE_LABEL_96".to_string()),
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(10))),
        }),
        ..Default::default()
    };
    assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
    wait_for_edge_property_index_state(&reopened, index_id, SecondaryIndexState::Failed);

    let failed_plan = reopened.explain_edge_query(&query).unwrap();
    assert!(!plan_contains_node(
        &failed_plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
}

#[test]
fn edge_property_query_uses_sidecar_counts_when_planner_stats_are_missing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = insert_query_node(&engine, "Person",  "edge-no-stats-a", &[], 1.0);
        let b = insert_query_node(&engine, "Person",  "edge-no-stats-b", &[], 1.0);
        let c = insert_query_node(&engine, "Person",  "edge-no-stats-c", &[], 1.0);
        keep = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_92",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_92",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_92", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        engine.close().unwrap();
    }
    let stats_path = crate::segment_writer::segment_dir(&db_path, 1)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = EdgeQuery {
        label: Some("EDGE_LABEL_92".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    let plan = reopened.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
}

#[test]
fn edge_property_in_union_materializes_when_union_cap_allows_it() {
    let (_dir, engine) = query_test_engine();
    let matching_count = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 904;
    let nonmatching_count = 2_000usize;
    let total_edges = matching_count + nonmatching_count;
    let nodes = (0..=total_edges)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("edge-union-cap-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edge_inputs = node_ids[1..]
        .iter()
        .enumerate()
        .map(|(idx, to)| EdgeInput {
            from: hub,
            to: *to,
            label: "EDGE_LABEL_93".to_string(),
            props: query_test_props(&[(
                "bucket",
                PropValue::String(
                    if idx < matching_count {
                        if idx % 2 == 0 { "a" } else { "b" }
                    } else {
                        "c"
                    }
                    .to_string(),
                ),
            )]),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    let edge_ids = engine.batch_upsert_edges(edge_inputs).unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_93", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("bucket").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_93".to_string()),
        filter: Some(EdgeFilterExpr::PropertyIn {
            key: "bucket".to_string(),
            values: vec![
                PropValue::String("a".to_string()),
                PropValue::String("b".to_string()),
            ],
        }),
        ..Default::default()
    };
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_edge_query(&query).unwrap();
    let planned = published.view.plan_normalized_edge_query(&normalized).unwrap();
    match published
        .view
        .materialize_edge_physical_plan(&normalized, planned.cap_context, &planned.driver)
        .unwrap()
    {
        CandidateMaterializationResult::Ready { ids, .. } => {
            assert_eq!(ids, edge_ids[..matching_count]);
        }
        CandidateMaterializationResult::TooBroad { .. } => {
            panic!("edge property IN union should materialize under the union cap")
        }
    }
}

#[test]
fn edge_query_endpoint_visibility_does_not_hydrate_nodes() {
    let (_dir, engine) = query_test_engine();
    let seg_a = insert_query_node(&engine, "Person",  "endpoint-segment-a", &[], 1.0);
    let seg_b = insert_query_node(&engine, "Person",  "endpoint-segment-b", &[], 1.0);
    let segment_edge = engine
        .upsert_edge(
            seg_a,
            seg_b,
            "EDGE_LABEL_42",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let frozen_a = insert_query_node(&engine, "Person",  "endpoint-frozen-a", &[], 1.0);
    let frozen_b = insert_query_node(&engine, "Person",  "endpoint-frozen-b", &[], 1.0);
    let frozen_edge = engine
        .upsert_edge(
            frozen_a,
            frozen_b,
            "EDGE_LABEL_42",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    engine.freeze_memtable().unwrap();

    let active_a = insert_query_node(&engine, "Person",  "endpoint-active-a", &[], 1.0);
    let active_b = insert_query_node(&engine, "Person",  "endpoint-active-b", &[], 1.0);
    let active_edge = engine
        .upsert_edge(
            active_a,
            active_b,
            "EDGE_LABEL_42",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(100),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_42".to_string()),
        filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 50 }),
        ..Default::default()
    };

    engine.reset_query_execution_counters_for_test();
    let ids = engine.query_edge_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(ids.edge_ids, vec![segment_edge, frozen_edge, active_edge]);
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn edge_query_prune_endpoint_visibility_uses_metadata_only() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "prune-source", &[], 1.0);
    let hidden = insert_query_node(&engine, "Person",  "prune-hidden", &[], 0.1);
    let visible = insert_query_node(&engine, "Person",  "prune-visible", &[], 1.0);
    engine
        .upsert_edge(source, hidden, "EDGE_LABEL_43", UpsertEdgeOptions::default())
        .unwrap();
    let keep = engine
        .upsert_edge(source, visible, "EDGE_LABEL_43", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    engine
        .set_prune_policy(
            "hide-light-endpoints",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: None,
            },
        )
        .unwrap();

    engine.reset_query_execution_counters_for_test();
    let ids = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("EDGE_LABEL_43".to_string()),
            from_ids: vec![source],
            ..Default::default()
        })
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(ids.edge_ids, vec![keep]);
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn edge_query_property_hydrates_only_metadata_survivors() {
    let (_dir, engine) = query_test_engine();
    let nodes = (0..41)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("property-prefilter-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let mut expected = None;
    for (idx, to) in node_ids[1..].iter().enumerate() {
        let selective = idx == 3 || idx == 17 || idx == 29;
        let props = if idx == 17 {
            query_test_props(&[("status", PropValue::String("active".to_string()))])
        } else {
            query_test_props(&[("status", PropValue::String("inactive".to_string()))])
        };
        let edge_id = engine
            .upsert_edge(
                hub,
                *to,
                "EDGE_LABEL_44",
                UpsertEdgeOptions {
                    props,
                    weight: if selective { 9.0 } else { 1.0 },
                    ..Default::default()
                },
            )
            .unwrap();
        if idx == 17 {
            expected = Some(edge_id);
        }
    }
    engine.flush().unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_44".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::WeightRange {
                lower: Some(9.0),
                upper: Some(9.0),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ])),
        ..Default::default()
    };

    engine.reset_query_execution_counters_for_test();
    let ids = engine.query_edge_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(ids.edge_ids, vec![expected.unwrap()]);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
}

#[test]
fn edge_query_or_filter_uses_projected_properties_without_hydration() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "edge-or-source", &[], 1.0);
    let metadata_match = insert_query_node(&engine, "Person",  "edge-or-metadata", &[], 1.0);
    let property_match = insert_query_node(&engine, "Person",  "edge-or-property", &[], 1.0);
    let drop = insert_query_node(&engine, "Person",  "edge-or-drop", &[], 1.0);

    let property_edge = engine
        .upsert_edge(
            source,
            property_match,
            "EDGE_LABEL_45",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    let metadata_edge = engine
        .upsert_edge(
            source,
            metadata_match,
            "EDGE_LABEL_45",
            UpsertEdgeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop,
            "EDGE_LABEL_45",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_45".to_string()),
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::WeightRange {
                lower: None,
                upper: Some(1.0),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ])),
        ..Default::default()
    };

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_edge_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(result.edge_ids, vec![property_edge, metadata_edge]);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
}

#[test]
fn edge_query_edges_hydrates_only_final_property_filtered_page() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "edge-output-cache-source", &[], 1.0);
    let keep = insert_query_node(&engine, "Person",  "edge-output-cache-keep", &[], 1.0);
    let drop = insert_query_node(&engine, "Person",  "edge-output-cache-drop", &[], 1.0);
    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "EDGE_LABEL_46",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop,
            "EDGE_LABEL_46",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_46".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_edges(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        result.edges.iter().map(|edge| edge.id).collect::<Vec<_>>(),
        vec![keep_edge]
    );
    assert_eq!(counters.edge_record_hydration_reads, 1);
    assert_eq!(counters.edge_record_hydration_calls, 1);
}

#[test]
fn edge_query_excludes_edges_cascaded_by_deleted_endpoint() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let deleted = insert_query_node(&engine, "Person",  "deleted", &[], 1.0);
    let visible = insert_query_node(&engine, "Person",  "visible", &[], 1.0);

    engine
        .upsert_edge(a, deleted, "PUBLISHED_BY", UpsertEdgeOptions::default())
        .unwrap();
    let keep = engine
        .upsert_edge(a, visible, "PUBLISHED_BY", UpsertEdgeOptions::default())
        .unwrap();
    engine.delete_node(deleted).unwrap();

    let ids = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("PUBLISHED_BY".to_string()),
            from_ids: vec![a],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids.edge_ids, vec![keep]);
}

#[test]
fn edge_query_excludes_edges_with_prune_hidden_endpoint() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 0.9);
    let hidden = insert_query_node(&engine, "Person",  "hidden", &[], 0.2);
    let visible = insert_query_node(&engine, "Person",  "visible", &[], 0.8);

    engine
        .upsert_edge(a, hidden, "TAGGED_WITH", UpsertEdgeOptions::default())
        .unwrap();
    let keep = engine
        .upsert_edge(a, visible, "TAGGED_WITH", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .set_prune_policy(
            "low-weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: None,
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("TAGGED_WITH".to_string()),
        from_ids: vec![a],
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![keep]);

    let edges = engine.query_edges(&query).unwrap();
    assert_eq!(
        edges.edges.iter().map(|edge| edge.id).collect::<Vec<_>>(),
        vec![keep]
    );
}

#[test]
fn edge_query_valid_at_uses_half_open_validity_window() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "c", &[], 1.0);

    engine
        .upsert_edge(
            a,
            b,
            "ASSIGNED_TO",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    let live = engine
        .upsert_edge(
            a,
            c,
            "ASSIGNED_TO",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(101),
                ..Default::default()
            },
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("ASSIGNED_TO".to_string()),
        filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 100 }),
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![live]);

    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeMetadataScan));
}

#[test]
fn edge_query_paginates_edge_ids_by_cursor() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "c", &[], 1.0);

    let first = engine
        .upsert_edge(a, b, "REVIEWED_BY", UpsertEdgeOptions::default())
        .unwrap();
    let second = engine
        .upsert_edge(a, c, "REVIEWED_BY", UpsertEdgeOptions::default())
        .unwrap();

    let first_page = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("REVIEWED_BY".to_string()),
            page: PageRequest {
                limit: Some(1),
                after: None,
            },
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first_page.edge_ids, vec![first]);
    assert_eq!(first_page.next_cursor, Some(first));

    let second_page = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("REVIEWED_BY".to_string()),
            page: PageRequest {
                limit: Some(1),
                after: first_page.next_cursor,
            },
            ..Default::default()
        })
        .unwrap();
    assert_eq!(second_page.edge_ids, vec![second]);
    assert_eq!(second_page.next_cursor, None);
}

#[test]
fn edge_query_broad_label_source_uses_streaming_fallback_page() {
    let (_dir, engine) = query_test_engine();
    let edge_count = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 1;
    let nodes = (0..=edge_count)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("broad-edge-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edge_inputs = node_ids[1..]
        .iter()
        .map(|to| EdgeInput {
            from: hub,
            to: *to,
            label: "RATES".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    let edge_ids = engine.batch_upsert_edges(edge_inputs).unwrap();
    engine.flush().unwrap();

    let query = EdgeQuery {
        label: Some("RATES".to_string()),
        page: PageRequest {
            limit: Some(2),
            after: None,
        },
        ..Default::default()
    };

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_edge_query(&query).unwrap();
        let planned = published.view.plan_normalized_edge_query(&normalized).unwrap();
        assert!(planned
            .warnings
            .contains(&QueryPlanWarning::CandidateCapExceeded));
        match published
            .view
            .materialize_edge_physical_plan(&normalized, planned.cap_context, &planned.driver)
            .unwrap()
        {
            CandidateMaterializationResult::TooBroad { .. } => {}
            CandidateMaterializationResult::Ready { ids, .. } => {
                panic!("expected broad edge source to avoid materialization, got {}", ids.len())
            }
        }
    }

    let first_page = engine.query_edge_ids(&query).unwrap();
    assert_eq!(first_page.edge_ids, edge_ids[..2]);
    assert_eq!(first_page.next_cursor, Some(edge_ids[1]));

    let second_page = engine
        .query_edge_ids(&EdgeQuery {
            page: PageRequest {
                limit: Some(2),
                after: first_page.next_cursor,
            },
            ..query
        })
        .unwrap();
    assert_eq!(second_page.edge_ids, edge_ids[2..4]);
}

#[test]
fn edge_query_selective_metadata_source_is_capped_before_too_broad() {
    let (_dir, engine) = query_test_engine();
    let edge_count = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 1;
    let nodes = (0..=edge_count)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("metadata-range-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edge_inputs = node_ids[1..]
        .iter()
        .enumerate()
        .map(|(idx, to)| EdgeInput {
            from: hub,
            to: *to,
            label: "EDGE_LABEL_33".to_string(),
            props: BTreeMap::new(),
            weight: if idx == 7 { 9.0 } else { 1.0 },
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    let edge_ids = engine.batch_upsert_edges(edge_inputs).unwrap();
    engine.flush().unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_33".to_string()),
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(9.0),
            upper: Some(9.0),
        }),
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_edge_query(&query).unwrap();
    let planned = published.view.plan_normalized_edge_query(&normalized).unwrap();
    match published
        .view
        .materialize_edge_physical_plan(&normalized, planned.cap_context, &planned.driver)
        .unwrap()
    {
        CandidateMaterializationResult::Ready { ids, .. } => {
            assert_eq!(ids, vec![edge_ids[7]]);
        }
        CandidateMaterializationResult::TooBroad { .. } => {
            panic!("selective metadata sidecar source should be capped before TooBroad")
        }
    }
}

#[test]
fn edge_query_broad_endpoint_anchor_does_not_fall_back_to_full_scan() {
    let (_dir, engine) = query_test_engine();
    let edge_count = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 1;
    let nodes = (0..=edge_count)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("endpoint-fallback-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edge_inputs = node_ids[1..]
        .iter()
        .map(|to| EdgeInput {
            from: hub,
            to: *to,
            label: "EDGE_LABEL_34".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    let edge_ids = engine.batch_upsert_edges(edge_inputs).unwrap();
    engine.flush().unwrap();

    let query = EdgeQuery {
        from_ids: vec![hub],
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(0.0),
            upper: Some(2.0),
        }),
        page: PageRequest {
            limit: Some(2),
            after: None,
        },
        ..Default::default()
    };

    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::CandidateCapExceeded));
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::RangeCandidateCapExceeded));

    engine.reset_query_execution_counters_for_test();
    let page = engine.query_edge_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(page.edge_ids, edge_ids[..2]);
    assert_eq!(page.next_cursor, Some(edge_ids[1]));
    assert_eq!(counters.edge_full_scan_pages, 0);
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert!(
        counters.endpoint_adjacency_candidates <= 12,
        "endpoint scan should stop after the first verification chunk, got {} candidates",
        counters.endpoint_adjacency_candidates
    );
}

#[test]
fn edge_query_active_memtable_endpoint_scan_is_bounded() {
    let (_dir, engine) = query_test_engine();
    let edge_count = 512usize;
    let nodes = (0..=edge_count)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("active-endpoint-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edge_inputs = node_ids[1..]
        .iter()
        .map(|to| EdgeInput {
            from: hub,
            to: *to,
            label: "EDGE_LABEL_35".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    let edge_ids = engine.batch_upsert_edges(edge_inputs).unwrap();

    let query = EdgeQuery {
        from_ids: vec![hub],
        page: PageRequest {
            limit: Some(2),
            after: None,
        },
        ..Default::default()
    };

    crate::memtable::reset_endpoint_cursor_entries_visited_for_test();
    let plan = engine.explain_edge_query(&query).unwrap();
    assert_eq!(
        crate::memtable::endpoint_cursor_entries_visited_for_test(),
        0,
        "edge endpoint planning must use cheap memtable count bounds, not cursor through the hub"
    );
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgeEndpointAdjacency
    ));
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::FallbackFullEdgeScan
    ));

    engine.reset_query_execution_counters_for_test();
    let first_page = engine.query_edge_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(first_page.edge_ids, edge_ids[..2]);
    assert_eq!(first_page.next_cursor, Some(edge_ids[1]));
    assert_eq!(counters.edge_full_scan_pages, 0);
    assert!(
        counters.endpoint_adjacency_candidates <= 12,
        "active endpoint scan should stop after a bounded chunk, got {} candidates",
        counters.endpoint_adjacency_candidates
    );

    let second_page = engine
        .query_edge_ids(&EdgeQuery {
            page: PageRequest {
                limit: Some(2),
                after: first_page.next_cursor,
            },
            ..query
        })
        .unwrap();
    assert_eq!(second_page.edge_ids, edge_ids[2..4]);
}

#[test]
fn edge_query_active_memtable_label_scan_pages_without_materializing_driver() {
    let (_dir, engine) = query_test_engine();
    let edge_count = 512usize;
    let nodes = (0..=edge_count)
        .map(|idx| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("active-edge-label-node-{idx}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edge_ids = engine
        .batch_upsert_edges(
            node_ids[1..]
                .iter()
                .map(|to| EdgeInput {
                    from: hub,
                    to: *to,
                    label: "EDGE_LABEL_36".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_36".to_string()),
        page: PageRequest {
            limit: Some(2),
            after: None,
        },
        ..Default::default()
    };

    let first_page = engine.query_edge_ids(&query).unwrap();
    assert_eq!(first_page.edge_ids, edge_ids[..2]);
    assert_eq!(first_page.next_cursor, Some(edge_ids[1]));
    let second_page = engine
        .query_edge_ids(&EdgeQuery {
            page: PageRequest {
                limit: Some(2),
                after: first_page.next_cursor,
            },
            ..query
        })
        .unwrap();
    assert_eq!(second_page.edge_ids, edge_ids[2..4]);
}

#[test]
fn edge_query_endpoint_list_uses_batched_source_semantics() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person",  "endpoint-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "endpoint-b", &[], 1.0);
    let c = insert_query_node(&engine, "Person",  "endpoint-c", &[], 1.0);
    let d = insert_query_node(&engine, "Person",  "endpoint-d", &[], 1.0);
    let e = insert_query_node(&engine, "Person",  "endpoint-e", &[], 1.0);

    let first = engine.upsert_edge(a, d, "EDGE_LABEL_31", UpsertEdgeOptions::default()).unwrap();
    let second = engine.upsert_edge(b, d, "EDGE_LABEL_31", UpsertEdgeOptions::default()).unwrap();
    let third = engine.upsert_edge(c, e, "EDGE_LABEL_31", UpsertEdgeOptions::default()).unwrap();
    engine.upsert_edge(a, e, "EDGE_LABEL_32", UpsertEdgeOptions::default()).unwrap();
    engine.flush().unwrap();

    let query = EdgeQuery {
        label: Some("EDGE_LABEL_31".to_string()),
        endpoint_ids: vec![b, a, b, c],
        ..Default::default()
    };
    let ids = engine.query_edge_ids(&query).unwrap();
    assert_eq!(ids.edge_ids, vec![first, second, third]);

    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(matches!(
        &plan.root,
        QueryPlanNode::VerifyEdgeFilter { .. }
    ));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgeEndpointAdjacency
    ));
}

#[test]
fn edge_query_rejects_filter_only_without_full_scan_opt_in() {
    let (_dir, engine) = query_test_engine();
    let err = engine
        .query_edge_ids(&EdgeQuery {
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("allow_full_scan"));
}

#[test]
fn edge_query_metadata_filter_only_is_native_anchor() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "metadata-filter-source", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "metadata-filter-target-a", &[], 1.0);
    let c = insert_query_node(&engine, "Person", "metadata-filter-target-b", &[], 1.0);
    let light = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_98",
            UpsertEdgeOptions {
                weight: 0.25,
                ..Default::default()
            },
        )
        .unwrap();
    let heavy = engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_98",
            UpsertEdgeOptions {
                weight: 0.75,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let query = EdgeQuery {
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(0.5),
            upper: None,
        }),
        ..Default::default()
    };
    let result = engine.query_edge_ids(&query).unwrap();
    assert_eq!(result.edge_ids, vec![heavy]);
    assert!(!result.edge_ids.contains(&light));

    let explain = engine.explain_edge_query(&query).unwrap();
    assert!(plan_contains_node(&explain.root, &QueryPlanNode::EdgeWeightIndex));
    assert!(!plan_contains_node(
        &explain.root,
        &QueryPlanNode::FallbackFullEdgeScan
    ));
}












fn oracle_node_matches(query: &NodeQuery, node: &NodeView) -> bool {
    if let Some(filter) = query.label_filter.as_ref() {
        match filter.mode {
            LabelMatchMode::Any => {
                if !filter
                    .labels
                    .iter()
                    .any(|label| node.labels.iter().any(|node_label| node_label == label))
                {
                    return false;
                }
            }
            LabelMatchMode::All => {
                if !filter
                    .labels
                    .iter()
                    .all(|label| node.labels.iter().any(|node_label| node_label == label))
                {
                    return false;
                }
            }
        }
    }
    if !query.ids.is_empty() && !query.ids.contains(&node.id) {
        return false;
    }
    if !query.keys.is_empty() && !query.keys.contains(&node.key) {
        return false;
    }
    query
        .filter
        .as_ref()
        .is_none_or(|filter| oracle_filter_matches(filter, node))
}

fn oracle_filter_matches(filter: &NodeFilterExpr, node: &NodeView) -> bool {
    match filter {
        NodeFilterExpr::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            lower.is_none_or(|lower| {
                if *lower_inclusive {
                    node.id >= lower
                } else {
                    node.id > lower
                }
            }) && upper.is_none_or(|upper| {
                if *upper_inclusive {
                    node.id <= upper
                } else {
                    node.id < upper
                }
            })
        }
        NodeFilterExpr::KeyEquals(key) => node.key == *key,
        NodeFilterExpr::KeyIn(keys) => keys.iter().any(|key| key == &node.key),
        NodeFilterExpr::PropertyEquals { key, value } => {
            node.props.get(key).is_some_and(|candidate| candidate == value)
        }
        NodeFilterExpr::PropertyIn { key, values } => node
            .props
            .get(key)
            .is_some_and(|candidate| values.iter().any(|value| candidate == value)),
        NodeFilterExpr::PropertyRange { key, lower, upper } => {
            let Some(value) = node.props.get(key) else {
                return false;
            };
            let lower_matches = lower.as_ref().is_none_or(|bound| {
                let Some(ordering) = compare_range_values(value, bound.value()) else {
                    return false;
                };
                match bound {
                    PropertyRangeBound::Included(_) => ordering != std::cmp::Ordering::Less,
                    PropertyRangeBound::Excluded(_) => ordering == std::cmp::Ordering::Greater,
                }
            });
            let upper_matches = upper.as_ref().is_none_or(|bound| {
                let Some(ordering) = compare_range_values(value, bound.value()) else {
                    return false;
                };
                match bound {
                    PropertyRangeBound::Included(_) => ordering != std::cmp::Ordering::Greater,
                    PropertyRangeBound::Excluded(_) => ordering == std::cmp::Ordering::Less,
                }
            });
            lower_matches && upper_matches
        }
        NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            lower_ms.is_none_or(|lower| node.updated_at >= lower)
                && upper_ms.is_none_or(|upper| node.updated_at <= upper)
        }
        NodeFilterExpr::WeightRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            lower.is_none_or(|lower| {
                if *lower_inclusive {
                    node.weight >= lower
                } else {
                    node.weight > lower
                }
            }) && upper.is_none_or(|upper| {
                if *upper_inclusive {
                    node.weight <= upper
                } else {
                    node.weight < upper
                }
            })
        }
        NodeFilterExpr::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            lower.is_none_or(|lower| {
                if *lower_inclusive {
                    node.created_at >= lower
                } else {
                    node.created_at > lower
                }
            }) && upper.is_none_or(|upper| {
                if *upper_inclusive {
                    node.created_at <= upper
                } else {
                    node.created_at < upper
                }
            })
        }
        NodeFilterExpr::PropertyExists { key } => node.props.contains_key(key),
        NodeFilterExpr::PropertyMissing { key } => !node.props.contains_key(key),
        NodeFilterExpr::And(children) => {
            children.iter().all(|child| oracle_filter_matches(child, node))
        }
        NodeFilterExpr::Or(children) => {
            children.iter().any(|child| oracle_filter_matches(child, node))
        }
        NodeFilterExpr::Not(child) => !oracle_filter_matches(child, node),
    }
}

fn oracle_query_ids(
    engine: &DatabaseEngine,
    candidate_ids: &[u64],
    query: &NodeQuery,
) -> Vec<u64> {
    let mut ids = candidate_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    engine
        .get_nodes(&ids)
        .unwrap()
        .into_iter()
        .flatten()
        .filter(|node| oracle_node_matches(query, node))
        .map(|node| node.id)
        .collect()
}

fn set_query_node_updated_at(engine: &DatabaseEngine, node_id: u64, updated_at: i64) {
    let node = internal_node_record(engine, node_id).unwrap().unwrap();
    write_internal_wal_op(engine, &WalOp::UpsertNode(NodeRecord {
            created_at: updated_at,
            updated_at,
            ..node
        }))
        .unwrap();
}

fn set_query_edge_props(engine: &DatabaseEngine, edge_id: u64, props: BTreeMap<String, PropValue>) {
    let edge = internal_edge_record(engine, edge_id).unwrap().unwrap();
    write_internal_wal_op(engine, &WalOp::UpsertEdge(EdgeRecord { props, ..edge }))
        .unwrap();
}

fn replace_equality_sidecar_group_id_in_place(
    path: &std::path::Path,
    value_hash: u64,
    from_id: u64,
    to_id: u64,
) {
    use std::io::{Seek, SeekFrom, Write};

    const SECONDARY_EQ_ENTRY_SIZE: usize = 20;
    let data = std::fs::read(path).unwrap();
    let payload_offset = component_payload_offset_for_test(path) as usize;
    let payload = &data[payload_offset..];
    assert!(payload.len() >= 8, "equality sidecar payload missing count");
    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;

    for index in 0..count {
        let entry_off = 8 + index * SECONDARY_EQ_ENTRY_SIZE;
        let entry_value_hash =
            u64::from_le_bytes(payload[entry_off..entry_off + 8].try_into().unwrap());
        if entry_value_hash != value_hash {
            continue;
        }
        let group_offset =
            u64::from_le_bytes(payload[entry_off + 8..entry_off + 16].try_into().unwrap())
                as usize;
        let id_count =
            u32::from_le_bytes(payload[entry_off + 16..entry_off + 20].try_into().unwrap())
                as usize;
        for id_index in 0..id_count {
            let id_offset = group_offset + id_index * 8;
            let existing = u64::from_le_bytes(payload[id_offset..id_offset + 8].try_into().unwrap());
            if existing == from_id {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap();
                file.seek(SeekFrom::Start((payload_offset + id_offset) as u64))
                    .unwrap();
                file.write_all(&to_id.to_le_bytes()).unwrap();
                file.sync_all().unwrap();
                return;
            }
        }
        panic!("target equality sidecar group did not contain id {from_id}");
    }

    panic!("target equality sidecar group hash {value_hash} not found");
}

fn plan_contains_node(node: &QueryPlanNode, expected: &QueryPlanNode) -> bool {
    if node == expected {
        return true;
    }
    match node {
        QueryPlanNode::Intersect { inputs } | QueryPlanNode::Union { inputs } => {
            inputs.iter().any(|input| plan_contains_node(input, expected))
        }
        QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input } => plan_contains_node(input, expected),
        _ => false,
    }
}

fn explain_input_node(plan: &QueryPlan) -> &QueryPlanNode {
    match &plan.root {
        QueryPlanNode::VerifyNodeFilter { input } => input.as_ref(),
        other => panic!("expected VerifyNodeFilter root, got {other:?}"),
    }
}

fn explain_input_nodes(plan: &QueryPlan) -> Vec<QueryPlanNode> {
    match explain_input_node(plan) {
        QueryPlanNode::Intersect { inputs } => inputs.clone(),
        node => vec![node.clone()],
    }
}

fn assert_plan_input_nodes(plan: &QueryPlan, expected: Vec<QueryPlanNode>) {
    assert_eq!(explain_input_nodes(plan), expected);
}

fn assert_plan_includes_input_nodes(plan: &QueryPlan, expected: &[QueryPlanNode]) {
    let mut actual = explain_input_nodes(plan);
    for expected_node in expected {
        let position = actual
            .iter()
            .position(|node| node == expected_node)
            .unwrap_or_else(|| panic!("expected plan to include {expected_node:?}; got {actual:?}"));
        actual.remove(position);
    }
}

fn find_compound_details(
    node: &QueryPlanNode,
    range: bool,
) -> Option<&CompoundIndexPlanDetails> {
    match node {
        QueryPlanNode::CompoundEqualityIndex { details } if !range => Some(details),
        QueryPlanNode::CompoundRangeIndex { details } if range => Some(details),
        QueryPlanNode::Intersect { inputs } | QueryPlanNode::Union { inputs } => inputs
            .iter()
            .find_map(|input| find_compound_details(input, range)),
        QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input } => {
            find_compound_details(input, range)
        }
        _ => None,
    }
}

fn compound_equality_details(plan: &QueryPlan) -> &CompoundIndexPlanDetails {
    find_compound_details(&plan.root, false)
        .unwrap_or_else(|| panic!("expected compound equality plan, got {:?}", plan.root))
}

fn compound_range_details(plan: &QueryPlan) -> &CompoundIndexPlanDetails {
    find_compound_details(&plan.root, true)
        .unwrap_or_else(|| panic!("expected compound range plan, got {:?}", plan.root))
}

#[test]
fn test_compound_node_equality_plans_executes_and_explains() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(
        &engine,
        "Person",
        "keep",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );
    let other_status = insert_query_node(
        &engine,
        "Person",
        "other-status",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
        ],
        1.0,
    );
    let other_tenant = insert_query_node(
        &engine,
        "Person",
        "other-tenant",
        &[
            ("tenant", PropValue::String("globex".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );

    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );

    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);
    let plan = engine.explain_node_query(&query).unwrap();
    let details = compound_equality_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.target_kind, QueryPlanCompoundTargetKind::Node);
    assert_eq!(details.matched_prefix_len, 2);
    assert_eq!(details.in_expansions, 1);
    // Both predicates are consumed by the index prefix; nothing is residual.
    assert_eq!(details.residual_predicates, 0);
    assert!(details.final_verification);
    assert_eq!(details.range_field, None);
    assert_eq!(details.fallback_reason, None);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[keep, other_status, other_tenant], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_node_range_and_in_cap_planning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut expected = Vec::new();
    for score in 0..80 {
        let tenant = if score < 70 { "acme" } else { "globex" };
        let id = insert_query_node(
            &engine,
            "Person",
            &format!("person-{score}"),
            &[
                ("tenant", PropValue::String(tenant.to_string())),
                ("score", PropValue::Int(score)),
            ],
            1.0,
        );
        if (10..=20).contains(&score) {
            expected.push(id);
        }
    }

    let range_info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, range_info.index_id, SecondaryIndexState::Ready);
    let equality_info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, equality_info.index_id, SecondaryIndexState::Ready);

    let range_query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(20))),
            },
        ],
        false,
    );
    assert_eq!(engine.query_node_ids(&range_query).unwrap().items, expected);
    let plan = engine.explain_node_query(&range_query).unwrap();
    let details = compound_range_details(&plan);
    assert_eq!(details.index_id, range_info.index_id);
    assert_eq!(details.matched_prefix_len, 1);
    assert_eq!(
        details.range_field,
        Some(SecondaryIndexField::property("score"))
    );
    // The tenant equality and score range are both consumed by the scan.
    assert_eq!(details.residual_predicates, 0);
    assert!(details.final_verification);

    let in_64_values = (0..64).map(PropValue::Int).collect::<Vec<_>>();
    let in_64_query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyIn {
                key: "score".to_string(),
                values: in_64_values,
            },
        ],
        false,
    );
    let in_64_plan = engine.explain_node_query(&in_64_query).unwrap();
    let in_64_details = find_compound_details(&in_64_plan.root, false)
        .or_else(|| find_compound_details(&in_64_plan.root, true))
        .unwrap_or_else(|| panic!("expected compound IN=64 plan, got {:?}", in_64_plan.root));
    assert_eq!(in_64_details.in_expansions, 64);

    let in_65_values = (0..65).map(PropValue::Int).collect::<Vec<_>>();
    let in_65_query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyIn {
                key: "score".to_string(),
                values: in_65_values,
            },
        ],
        false,
    );
    let in_65_plan = engine.explain_node_query(&in_65_query).unwrap();
    assert!(find_compound_details(&in_65_plan.root, false).is_none());
    // DEC-37-014: exceeding the IN expansion cap is a broad-skip, not a
    // prefix-not-satisfied condition (the prefix IS constrained here).
    assert!(in_65_plan
        .warnings
        .contains(&QueryPlanWarning::IndexSkippedAsBroad));
    assert!(!in_65_plan
        .warnings
        .contains(&QueryPlanWarning::CompoundIndexPrefixNotSatisfied));

    engine.close().unwrap();
}

#[test]
fn test_single_predicate_query_uses_compound_prefix() {
    // CP37.5 review S2 / planner review P1 (user-ratified): one predicate
    // constraining the leading declaration field drives a compound prefix
    // scan instead of a fallback label scan.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let acme_a = insert_query_node(
        &engine,
        "Person",
        "acme-a",
        &[("tenant", PropValue::String("acme".to_string()))],
        1.0,
    );
    let acme_b = insert_query_node(
        &engine,
        "Person",
        "acme-b",
        &[("tenant", PropValue::String("acme".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "globex",
        &[("tenant", PropValue::String("globex".to_string()))],
        1.0,
    );
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("acme".to_string()),
        }],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    let details = compound_equality_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.matched_prefix_len, 1);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![acme_a, acme_b]
    );

    // Same plan and results once the postings live in a segment sidecar.
    engine.flush().unwrap();
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(compound_equality_details(&plan).index_id, info.index_id);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![acme_a, acme_b]
    );

    // A predicate on a non-leading declaration field still cannot use the
    // compound index: the left prefix is unconstrained.
    let non_prefix_query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    let non_prefix_plan = engine.explain_node_query(&non_prefix_query).unwrap();
    assert!(find_compound_details(&non_prefix_plan.root, false).is_none());
    assert!(non_prefix_plan
        .warnings
        .contains(&QueryPlanWarning::CompoundIndexPrefixNotSatisfied));
    assert!(engine.query_node_ids(&non_prefix_query).unwrap().items.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_edge_single_predicate_with_anchor_uses_compound_prefix() {
    // Edge twin of the single-predicate rule: endpoint anchors satisfy the
    // leading `from` field, so one property predicate is enough to drive a
    // compound prefix scan instead of adjacency expansion plus verify.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let from = insert_query_node(&engine, "Person", "anchor-from", &[], 1.0);
    let mut hot = None;
    for index in 0..30 {
        let to = insert_query_node(&engine, "Person", &format!("anchor-to-{index}"), &[], 1.0);
        let status = if index == 17 { "hot" } else { "cold" };
        let edge = engine
            .upsert_edge(
                from,
                to,
                "ANCHORED_REL",
                UpsertEdgeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String(status.to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        if index == 17 {
            hot = Some(edge);
        }
    }
    let info = engine
        .ensure_edge_property_index(
            "ANCHORED_REL",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_published_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("ANCHORED_REL".to_string()),
        from_ids: vec![from],
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    let details = find_compound_details(&plan.root, false)
        .unwrap_or_else(|| panic!("expected compound equality plan, got {:?}", plan.root));
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.matched_prefix_len, 2);
    assert_eq!(
        engine.query_edge_ids(&query).unwrap().edge_ids,
        vec![hot.unwrap()]
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_one_field_metadata_key_index_plans_and_executes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(
        &engine,
        "Person",
        "alpha",
        &[("tenant", PropValue::String("acme".to_string()))],
        1.0,
    );
    let other_key = insert_query_node(
        &engine,
        "Person",
        "beta",
        &[("tenant", PropValue::String("acme".to_string()))],
        1.0,
    );
    let other_label = insert_query_node(
        &engine,
        "Company",
        "alpha",
        &[("tenant", PropValue::String("acme".to_string()))],
        1.0,
    );

    // A one-field metadata declaration persists as NodeFieldIndex and remains
    // usable by the planner.
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::node_meta(
                NodeMetadataIndexField::Key,
            )]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::KeyEquals("alpha".to_string()),
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
        ],
        false,
    );
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);
    let plan = engine.explain_node_query(&query).unwrap();
    let details = compound_equality_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.matched_prefix_len, 1);
    assert!(details.compound);
    assert_eq!(
        details.fields,
        vec![SecondaryIndexField::node_meta(NodeMetadataIndexField::Key)]
    );
    // The key leaf is consumed by the scan; the tenant leaf stays residual.
    assert_eq!(details.residual_predicates, 1);
    assert_eq!(details.range_field, None);
    assert!(details.final_verification);
    assert_eq!(details.fallback_reason, None);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[keep, other_key, other_label], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_edge_in_cap_skips_as_broad() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let from = insert_query_node(&engine, "Person", "edge-in-cap-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "edge-in-cap-to", &[], 1.0);
    engine
        .upsert_edge(
            from,
            to,
            "EDGE_IN_CAP_REL",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("keep".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let info = engine
        .ensure_edge_property_index(
            "EDGE_IN_CAP_REL",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_published_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let in_65_values = (0..65)
        .map(|value| PropValue::String(format!("status-{value}")))
        .collect::<Vec<_>>();
    let query = EdgeQuery {
        label: Some("EDGE_IN_CAP_REL".to_string()),
        from_ids: vec![from],
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyIn {
                key: "status".to_string(),
                values: in_65_values,
            },
            EdgeFilterExpr::WeightRange {
                lower: Some(0.0),
                upper: Some(2.0),
            },
        ])),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(find_compound_details(&plan.root, false).is_none());
    // DEC-37-014: an IN product above the expansion cap is a broad-skip.
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::IndexSkippedAsBroad));
    assert!(!plan
        .warnings
        .contains(&QueryPlanWarning::CompoundIndexPrefixNotSatisfied));

    engine.close().unwrap();
}

#[test]
fn test_compound_non_prefix_predicates_warn_prefix_not_satisfied() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    insert_query_node(
        &engine,
        "Person",
        "prefix-unsat",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("score", PropValue::Int(5)),
            ("region", PropValue::String("east".to_string())),
        ],
        1.0,
    );
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    // Predicates only on the second field cannot satisfy a left prefix.
    let query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Int(5),
            },
            NodeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(find_compound_details(&plan.root, false).is_none());
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::CompoundIndexPrefixNotSatisfied));
    assert!(!plan
        .warnings
        .contains(&QueryPlanWarning::IndexSkippedAsBroad));

    engine.close().unwrap();
}

fn collect_compound_index_ids(node: &QueryPlanNode, ids: &mut Vec<u64>) {
    match node {
        QueryPlanNode::CompoundEqualityIndex { details }
        | QueryPlanNode::CompoundRangeIndex { details } => ids.push(details.index_id),
        QueryPlanNode::Intersect { inputs } | QueryPlanNode::Union { inputs } => {
            for input in inputs {
                collect_compound_index_ids(input, ids);
            }
        }
        QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input } => {
            collect_compound_index_ids(input, ids);
        }
        _ => {}
    }
}

fn tenant_status_props(tenant: &str, status: &str) -> Vec<(&'static str, PropValue)> {
    vec![
        ("tenant", PropValue::String(tenant.to_string())),
        ("status", PropValue::String(status.to_string())),
    ]
}

fn tenant_status_filter() -> Vec<NodeFilterExpr> {
    vec![
        NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("acme".to_string()),
        },
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
    ]
}

#[test]
fn test_compound_three_field_prefix_only_planning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut expected_prefix = Vec::new();
    for index in 0..12 {
        let region = if index % 2 == 0 { "east" } else { "west" };
        let tenant = if index < 8 { "acme" } else { "globex" };
        let id = insert_query_node(
            &engine,
            "Person",
            &format!("three-field-{index}"),
            &[
                ("tenant", PropValue::String(tenant.to_string())),
                ("region", PropValue::String(region.to_string())),
                ("score", PropValue::Int(index)),
            ],
            1.0,
        );
        if tenant == "acme" && region == "east" {
            expected_prefix.push(id);
        }
    }

    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("region"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    // Constraining the first two of three fields uses a left-prefix scan.
    let prefix_query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&prefix_query).unwrap().items,
        expected_prefix
    );
    let plan = engine.explain_node_query(&prefix_query).unwrap();
    let details = compound_equality_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.matched_prefix_len, 2);
    assert!(details.compound);
    assert_eq!(details.residual_predicates, 0);

    // A gap in the prefix (tenant + score, skipping region) stops the prefix
    // after the first field; the score predicate stays residual.
    let gap_query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "score".to_string(),
                value: PropValue::Int(4),
            },
        ],
        false,
    );
    let gap_expected = oracle_query_ids(
        &engine,
        &engine.query_node_ids(&query_ids(Some("Person"), vec![], true)).unwrap().items,
        &gap_query,
    );
    assert_eq!(engine.query_node_ids(&gap_query).unwrap().items, gap_expected);
    let gap_plan = engine.explain_node_query(&gap_query).unwrap();
    let gap_details = compound_equality_details(&gap_plan);
    assert_eq!(gap_details.matched_prefix_len, 1);
    assert_eq!(gap_details.residual_predicates, 1);

    engine.close().unwrap();
}

#[test]
fn test_compound_multi_label_any_union_plans_and_executes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a_match =
        insert_query_node(&engine, "AnyA", "a-match", &tenant_status_props("acme", "active"), 1.0);
    let a_other = insert_query_node(
        &engine,
        "AnyA",
        "a-other",
        &tenant_status_props("acme", "inactive"),
        1.0,
    );
    let b_match =
        insert_query_node(&engine, "AnyB", "b-match", &tenant_status_props("acme", "active"), 1.0);
    let b_other = insert_query_node(
        &engine,
        "AnyB",
        "b-other",
        &tenant_status_props("globex", "active"),
        1.0,
    );
    let outside = insert_query_node(
        &engine,
        "AnyC",
        "c-match",
        &tenant_status_props("acme", "active"),
        1.0,
    );

    let spec = || {
        SecondaryIndexSpec::equality(vec![
            SecondaryIndexField::property("tenant"),
            SecondaryIndexField::property("status"),
        ])
    };
    let info_a = engine.ensure_node_property_index("AnyA", spec()).unwrap();
    wait_for_property_index_state(&engine, info_a.index_id, SecondaryIndexState::Ready);
    let info_b = engine.ensure_node_property_index("AnyB", spec()).unwrap();
    wait_for_property_index_state(&engine, info_b.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(node_label_filter(&["AnyA", "AnyB"], LabelMatchMode::Any)),
        filter: filter_from_conjunction(tenant_status_filter()),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![a_match, b_match]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    let mut compound_ids = Vec::new();
    collect_compound_index_ids(&plan.root, &mut compound_ids);
    compound_ids.sort_unstable();
    let mut expected_ids = vec![info_a.index_id, info_b.index_id];
    expected_ids.sort_unstable();
    // One union branch per Any label, each backed by that label's declaration.
    assert_eq!(compound_ids, expected_ids);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(
            &engine,
            &[a_match, a_other, b_match, b_other, outside],
            &query
        )
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_multi_label_any_falls_back_when_label_uncovered() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a_match = insert_query_node(
        &engine,
        "UncovA",
        "a-match",
        &tenant_status_props("acme", "active"),
        1.0,
    );
    insert_query_node(
        &engine,
        "UncovA",
        "a-other",
        &tenant_status_props("acme", "inactive"),
        1.0,
    );
    let b_match = insert_query_node(
        &engine,
        "UncovB",
        "b-match",
        &tenant_status_props("acme", "active"),
        1.0,
    );

    // Only one of the two Any labels has a Ready declaration: a partial
    // union would drop UncovB rows, so the planner must not use compound
    // sources at all.
    let info = engine
        .ensure_node_property_index(
            "UncovA",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(node_label_filter(&["UncovA", "UncovB"], LabelMatchMode::Any)),
        filter: filter_from_conjunction(tenant_status_filter()),
        ..Default::default()
    };
    let plan = engine.explain_node_query(&query).unwrap();
    let mut compound_ids = Vec::new();
    collect_compound_index_ids(&plan.root, &mut compound_ids);
    assert!(compound_ids.is_empty(), "partial Any union must not be used");
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![a_match, b_match]
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_multi_label_all_uses_one_declaration_and_verifies_rest() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let both = insert_query_node_with_labels(
        &engine,
        &["AllA", "AllB"],
        "both",
        &tenant_status_props("acme", "active"),
        1.0,
    );
    let only_a = insert_query_node(
        &engine,
        "AllA",
        "only-a",
        &tenant_status_props("acme", "active"),
        1.0,
    );

    let info = engine
        .ensure_node_property_index(
            "AllA",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(node_label_filter(&["AllA", "AllB"], LabelMatchMode::All)),
        filter: filter_from_conjunction(tenant_status_filter()),
        ..Default::default()
    };
    // The AllA declaration provides a superset; AllB membership is verified.
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![both]);
    let plan = engine.explain_node_query(&query).unwrap();
    let mut compound_ids = Vec::new();
    collect_compound_index_ids(&plan.root, &mut compound_ids);
    assert_eq!(compound_ids, vec![info.index_id]);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[both, only_a], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_index_suppresses_stale_segment_postings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(
        &engine,
        "Person",
        "keep",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );
    let deleted = insert_query_node(
        &engine,
        "Person",
        "deleted",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );
    let updated = insert_query_node(
        &engine,
        "Person",
        "updated",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );

    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.flush().unwrap();

    // The flushed segment sidecar now holds postings for all three nodes.
    // Shadow them from newer sources: a tombstone, an update that no longer
    // matches the tuple, and one fresh memtable match.
    engine.delete_node(deleted).unwrap();
    insert_query_node(
        &engine,
        "Person",
        "updated",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
        ],
        1.0,
    );
    let fresh = insert_query_node(
        &engine,
        "Person",
        "fresh",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );

    let query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(find_compound_details(&plan.root, false).is_some());
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![keep, fresh]
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[keep, deleted, updated, fresh], &query)
    );

    // Flush again so the tombstone and the stale-tuple update live in a newer
    // segment than the original postings.
    engine.flush().unwrap();
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(find_compound_details(&plan.root, false).is_some());
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![keep, fresh]
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_truncated_segment_scan_reports_too_broad_not_partial_ready() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut ids = Vec::new();
    for ordinal in 0..5 {
        ids.push(insert_query_node(
            &engine,
            "Person",
            &format!("n{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("active".to_string())),
            ],
            1.0,
        ));
    }
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.flush().unwrap();

    // Tombstone the two lowest IDs so a truncated segment scan returns
    // postings that newer-source suppression then drops.
    engine.delete_node(ids[0]).unwrap();
    engine.delete_node(ids[1]).unwrap();

    let view = engine.published_read_view_for_test();
    let entry = view
        .secondary_index_entries
        .iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    let context =
        crate::secondary_index_key::CompoundTupleContext::from_manifest_entry(entry).unwrap();
    let tenant = PropValue::String("acme".to_string());
    let status = PropValue::String("active".to_string());
    let prefix = crate::secondary_index_key::encode_compound_tuple_prefix(
        &context,
        &[
            crate::secondary_index_key::CompoundFieldValue::Property(Some(&tenant)),
            crate::secondary_index_key::CompoundFieldValue::Property(Some(&status)),
        ],
    )
    .unwrap();
    let bounds = crate::secondary_index_key::compound_prefix_bounds(&prefix);

    // A limit below the live match count truncates the segment scan right
    // after the suppressed postings; the read must report TooBroad rather
    // than a silently incomplete Ready set.
    match view
        .sources()
        .node_ids_by_compound_prefix_limited(entry, &bounds, 3)
        .unwrap()
    {
        crate::source_list::LimitedCompoundIndexRead::TooBroad => {}
        crate::source_list::LimitedCompoundIndexRead::Ready(read_ids) => {
            panic!("truncated compound scan returned Ready({read_ids:?}) instead of TooBroad")
        }
        crate::source_list::LimitedCompoundIndexRead::MissingSidecar => {
            panic!("compound sidecar unexpectedly missing")
        }
    }

    // With budget for the full posting list the same read returns the
    // complete tombstone-suppressed result.
    match view
        .sources()
        .node_ids_by_compound_prefix_limited(entry, &bounds, 10)
        .unwrap()
    {
        crate::source_list::LimitedCompoundIndexRead::Ready(read_ids) => {
            assert_eq!(read_ids, vec![ids[2], ids[3], ids[4]]);
        }
        _ => panic!("untruncated compound scan should be Ready"),
    }

    engine.close().unwrap();
}

#[test]
fn test_equality_query_returns_all_memtable_matches_above_verify_chunk() {
    // Planner review N1: the raw-limited equality path clamped each memtable
    // read to one verify chunk (256) without a truncation signal, silently
    // dropping every memtable match past the clamp.
    let (_dir, engine) = query_test_engine();
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let total = 600usize;
    let mut inputs = Vec::with_capacity(total + 200);
    for ordinal in 0..total {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("active-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    // Non-matching rows keep the equality index strictly cheaper than the
    // label scan so the index stays the selected driver.
    for ordinal in 0..200 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("inactive-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(inputs).unwrap();
    let mut expected: Vec<u64> = ids[..total].to_vec();
    expected.sort_unstable();

    let mut query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    query.page = PageRequest {
        limit: Some(1000),
        after: None,
    };
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(
        plan_contains_node(&plan.root, &QueryPlanNode::PropertyEqualityIndex),
        "test requires the equality index driver, got {plan:?}"
    );
    let page = engine.query_node_ids(&query).unwrap();
    assert_eq!(
        page.items.len(),
        total,
        "equality matches beyond one verify chunk must not be silently dropped"
    );
    assert_eq!(page.items, expected);
    assert_eq!(page.next_cursor, None);

    engine.close().unwrap();
}

#[test]
fn test_equality_query_merges_segment_and_memtable_matches_above_verify_chunk() {
    // Planner review N1 (mixed tiers): the segment tier drains fully while an
    // unflushed memtable holding more than one verify chunk of matches lost
    // everything past 256.
    let (_dir, engine) = query_test_engine();
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let flushed = 700usize;
    let unflushed = 500usize;
    let mut inputs = Vec::with_capacity(flushed + 300);
    for ordinal in 0..flushed {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("flushed-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    for ordinal in 0..300 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("inactive-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let flushed_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    let mut memtable_inputs = Vec::with_capacity(unflushed);
    for ordinal in 0..unflushed {
        memtable_inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("memtable-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let memtable_ids = engine.batch_upsert_nodes(memtable_inputs).unwrap();

    let mut expected: Vec<u64> = flushed_ids[..flushed]
        .iter()
        .chain(memtable_ids.iter())
        .copied()
        .collect();
    expected.sort_unstable();

    let mut query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    query.page = PageRequest {
        limit: Some(2000),
        after: None,
    };
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(
        plan_contains_node(&plan.root, &QueryPlanNode::PropertyEqualityIndex),
        "test requires the equality index driver, got {plan:?}"
    );
    let page = engine.query_node_ids(&query).unwrap();
    assert_eq!(
        page.items.len(),
        flushed + unflushed,
        "memtable matches beyond one verify chunk must merge with segment matches"
    );
    assert_eq!(page.items, expected);

    engine.close().unwrap();
}

#[test]
fn test_selective_index_above_default_cap_drives_under_write_load() {
    // Planner review P3: one unflushed write anywhere downgraded stats
    // confidence to Medium database-wide, which kept the 4096 default cap and
    // discarded selective index probes as CandidateCapExceeded in favor of a
    // full label scan. Trusted posting upper bounds must uncap regardless of
    // confidence class.
    let (_dir, engine) = query_test_engine();
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let hot = QUERY_RANGE_CANDIDATE_CAP + 8;
    let mut inputs = Vec::with_capacity(hot + 400);
    for ordinal in 0..hot {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("hot-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    for ordinal in 0..400 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("cold-{ordinal}"),
            props: query_test_props(&[("status", PropValue::String("cold".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    // The unflushed write that previously suppressed the uncap.
    insert_query_node(
        &engine,
        "Person",
        "unflushed",
        &[("status", PropValue::String("cold".to_string()))],
        1.0,
    );

    let query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(
        !plan
            .warnings
            .contains(&QueryPlanWarning::CandidateCapExceeded),
        "selective index must not be discarded under write load, got {:?}",
        plan.warnings
    );
    assert!(
        plan_contains_node(&plan.root, &QueryPlanNode::PropertyEqualityIndex),
        "equality index should drive, got {plan:?}"
    );
    assert_eq!(engine.query_node_ids(&query).unwrap().items.len(), hot);

    engine.close().unwrap();
}

#[test]
fn test_compound_intersection_uses_semantic_equality_across_numeric_forms() {
    // Planner review N2: intersecting same-field equality lists with
    // structural PartialEq dropped values the verifier and tuple encoding
    // treat as equal (Int(2) vs Float(2.0)), driving an under-inclusive
    // compound scan whose lost rows verification cannot recover.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let x1 = insert_query_node(
        &engine,
        "Person",
        "x1",
        &[("x", PropValue::Int(1)), ("y", PropValue::Int(0))],
        1.0,
    );
    let x2 = insert_query_node(
        &engine,
        "Person",
        "x2",
        &[("x", PropValue::Int(2)), ("y", PropValue::Int(0))],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "x9",
        &[("x", PropValue::Int(9)), ("y", PropValue::Int(0))],
        1.0,
    );
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("x"),
                SecondaryIndexField::property("y"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some("Person"),
        vec![
            NodeFilterExpr::PropertyIn {
                key: "x".to_string(),
                values: vec![PropValue::Int(1), PropValue::Int(2)],
            },
            NodeFilterExpr::PropertyIn {
                key: "x".to_string(),
                values: vec![PropValue::Int(1), PropValue::Float(2.0), PropValue::Int(3)],
            },
        ],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(compound_equality_details(&plan).index_id, info.index_id);
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![x1, x2]);

    // Same intersection once the postings live in a segment sidecar.
    engine.flush().unwrap();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![x1, x2]);

    engine.close().unwrap();
}

#[test]
fn test_edge_triple_index_estimate_ranks_first_in_intersect() {
    // Planner review P6: the triple source carried an unknown estimate, so it
    // ranked dead last and broad index inputs materialized ahead of the
    // cheapest probe in the engine.
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "b", &[], 1.0);
    let keep_edge = engine
        .upsert_edge(
            a,
            b,
            "RELATES_TO",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("keep".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    // Broad postings under the same property value with other endpoints so the
    // equality index input is clearly more expensive than the triple probe.
    for index in 0..300 {
        let to = insert_query_node(&engine, "Person", &format!("to-{index}"), &[], 1.0);
        engine
            .upsert_edge(
                a,
                to,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("keep".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let info = engine
        .ensure_edge_property_index(
            "RELATES_TO",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_published_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        from_ids: vec![a],
        to_ids: vec![b],
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        }),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    let QueryPlanNode::VerifyEdgeFilter { input } = &plan.root else {
        panic!("expected VerifyEdgeFilter root, got {:?}", plan.root)
    };
    let QueryPlanNode::Intersect { inputs } = input.as_ref() else {
        panic!("expected Intersect input, got {input:?}")
    };
    assert_eq!(
        inputs.first(),
        Some(&QueryPlanNode::EdgeTripleIndex),
        "triple probe should be the cheapest intersect input, got {inputs:?}"
    );
    assert_eq!(engine.query_edge_ids(&query).unwrap().edge_ids, vec![keep_edge]);

    engine.close().unwrap();
}

#[test]
fn test_edge_query_explicit_anchor_skips_filter_planning_and_index_scan() {
    // Planner review P7: a tiny explicit-edge-ids anchor always wins the
    // driver sort, so filter planning (index probes, compound search) is
    // wasted work — and must not touch index sidecars at all.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let from = insert_query_node(&engine, "Person", "from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "to", &[], 1.0);
    let edge = engine
        .upsert_edge(
            from,
            to,
            "RELATES_TO",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("keep".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index(
            "RELATES_TO",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_published_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    // Removing the sidecar makes any equality-index probe observable as a
    // followup; the explicit-anchor plan must not generate one.
    let segment_id = engine.segments_for_test()[0].segment_id;
    let seg_dir = segment_dir(&db_path, segment_id);
    let sidecar_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::EdgePropertyEqualityIndex {
            index_id: info.index_id,
        },
    );
    corrupt_planner_stats_for_segment(&db_path, segment_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    engine
        .reopen_segment_reader_and_rebuild_sources_for_test(segment_id)
        .unwrap();

    let query = EdgeQuery {
        ids: vec![edge],
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        }),
        ..Default::default()
    };

    let (_followup_ready_rx, followup_release_tx) = engine.set_runtime_publish_pause();
    assert_eq!(engine.query_edge_ids(&query).unwrap().edge_ids, vec![edge]);
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 0);
    followup_release_tx.send(()).unwrap();

    let plan = engine.explain_edge_query(&query).unwrap();
    assert!(
        plan_contains_node(&plan.root, &QueryPlanNode::ExplicitEdgeIds),
        "explicit ids should drive, got {plan:?}"
    );
    assert!(
        !plan_contains_node(&plan.root, &QueryPlanNode::EdgePropertyEqualityIndex),
        "filter planning should be skipped for tiny explicit anchors, got {plan:?}"
    );

    engine.close().unwrap();
}

#[test]
fn test_compound_estimate_counts_sidecar_postings_when_stats_uncovered() {
    // Planner review P4: a stats-uncovered segment previously charged the
    // whole label cardinality, so a selective compound candidate was skipped
    // as broad during stats gaps. The estimator must count real postings from
    // the compound sidecar key table instead.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut acme_ids = Vec::new();
    for ordinal in 0..40 {
        acme_ids.push(insert_query_node(
            &engine,
            "Person",
            &format!("acme-{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("active".to_string())),
            ],
            1.0,
        ));
    }
    for ordinal in 0..200 {
        insert_query_node(
            &engine,
            "Person",
            &format!("globex-{ordinal}"),
            &[
                ("tenant", PropValue::String("globex".to_string())),
                ("status", PropValue::String("active".to_string())),
            ],
            1.0,
        );
    }
    let info = engine
        .ensure_node_property_index(
            "Person",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.flush().unwrap();

    // Remove the segment's planner stats so the compound rollup loses
    // coverage while the compound sidecar itself stays intact.
    let segment_id = engine.segments_for_test()[0].segment_id;
    let seg_dir = segment_dir(&db_path, segment_id);
    let stats_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::PlannerStats,
    );
    engine.close().unwrap();
    std::fs::remove_file(&stats_path).unwrap();
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(engine.segments_for_test()[0].planner_stats().is_none());

    let query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("acme".to_string()),
        }],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(
        !plan.warnings.contains(&QueryPlanWarning::IndexSkippedAsBroad),
        "selective compound candidate must not be skipped during stats gaps, got {:?}",
        plan.warnings
    );
    let details = compound_equality_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(
        details.estimated_candidates,
        Some(40),
        "estimate should count real sidecar postings, not the label cardinality"
    );
    assert_eq!(engine.query_node_ids(&query).unwrap().items, acme_ids);

    engine.close().unwrap();
}

#[test]
fn test_edge_equality_truncated_epoch_read_reports_too_broad_not_partial_ready() {
    // Planner review N3: an immutable-memtable read that consumed the whole
    // raw budget could fall through to Ready when no segments followed,
    // silently dropping the postings the truncated read never returned.
    let (_dir, engine) = query_test_engine();
    let info = engine
        .ensure_edge_property_index(
            "RELATES_TO",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_published_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let from = insert_query_node(&engine, "Person", "from", &[], 1.0);
    let mut to_ids = Vec::new();
    let mut edge_ids = Vec::new();
    for index in 0..6 {
        let to = insert_query_node(&engine, "Person", &format!("to-{index}"), &[], 1.0);
        to_ids.push(to);
        edge_ids.push(
            engine
                .upsert_edge(
                    from,
                    to,
                    "RELATES_TO",
                    UpsertEdgeOptions {
                        props: query_test_props(&[(
                            "status",
                            PropValue::String("x".to_string()),
                        )]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    engine.freeze_memtable().unwrap();
    // Shadow the two lowest edge ids with active-memtable tombstones so a
    // truncated epoch read loses net postings to shadow filtering.
    engine.delete_edge(edge_ids[0]).unwrap();
    engine.delete_edge(edge_ids[1]).unwrap();

    // Publication is asynchronous; wait until the published view reflects the
    // frozen epoch and the two shadowing tombstones.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let view = loop {
        let view = engine.published_read_view_for_test();
        if view.immutable_epochs.len() == 1
            && view
                .sources()
                .edge_ids_by_secondary_eq_hashes_limited_read(
                    info.index_id,
                    &[crate::property_value_semantics::hash_prop_equality_key(
                        &PropValue::String("x".to_string()),
                    )],
                    16,
                )
                .ok()
                .is_some_and(|read| {
                    matches!(
                        read,
                        crate::source_list::LimitedEdgeIndexRead::Ready(ref ids)
                            if *ids == edge_ids[2..]
                    )
                })
        {
            break view;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "published view never reflected the frozen epoch and tombstones"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    };
    let hashes = vec![crate::property_value_semantics::hash_prop_equality_key(
        &PropValue::String("x".to_string()),
    )];
    // A budget of 5 truncates the 6-posting epoch read; shadow filtering then
    // drops two ids, so the result stays under the budget. The read must
    // report TooBroad rather than a silently incomplete Ready set.
    match view
        .sources()
        .edge_ids_by_secondary_eq_hashes_limited_read(info.index_id, &hashes, 5)
        .unwrap()
    {
        crate::source_list::LimitedEdgeIndexRead::TooBroad => {}
        crate::source_list::LimitedEdgeIndexRead::Ready(read_ids) => {
            panic!("truncated epoch read returned Ready({read_ids:?}) instead of TooBroad")
        }
        crate::source_list::LimitedEdgeIndexRead::MissingSidecar => {
            panic!("edge equality sidecar unexpectedly missing")
        }
    }

    // With budget for every raw posting the same read returns the complete
    // shadow-filtered result.
    match view
        .sources()
        .edge_ids_by_secondary_eq_hashes_limited_read(info.index_id, &hashes, 16)
        .unwrap()
    {
        crate::source_list::LimitedEdgeIndexRead::Ready(read_ids) => {
            assert_eq!(read_ids, edge_ids[2..].to_vec());
        }
        _ => panic!("untruncated epoch read should be Ready"),
    }

    engine.close().unwrap();
}

#[test]
fn test_compound_edge_endpoint_property_source_wins_when_selective() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let from = insert_query_node(&engine, "Person", "from", &[], 1.0);
    let mut edge_ids = Vec::new();
    for index in 0..100 {
        let to = insert_query_node(&engine, "Person", &format!("to-{index}"), &[], 1.0);
        let status = if index == 42 { "keep" } else { "skip" };
        edge_ids.push(
            engine
                .upsert_edge(
                    from,
                    to,
                    "RELATES_TO",
                    UpsertEdgeOptions {
                        props: query_test_props(&[(
                            "status",
                            PropValue::String(status.to_string()),
                        )]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    let expected = vec![edge_ids[42]];

    let info = engine
        .ensure_edge_property_index(
            "RELATES_TO",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_published_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("RELATES_TO".to_string()),
        from_ids: vec![from],
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("keep".to_string()),
            },
            EdgeFilterExpr::WeightRange {
                lower: Some(0.0),
                upper: Some(2.0),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(engine.query_edge_ids(&query).unwrap().edge_ids, expected);
    let plan = engine.explain_edge_query(&query).unwrap();
    let details = compound_equality_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.target_kind, QueryPlanCompoundTargetKind::Edge);
    assert_eq!(details.matched_prefix_len, 2);
    // The weight range predicate is not part of the prefix and stays residual.
    assert_eq!(details.residual_predicates, 1);
    assert!(details.final_verification);

    engine.close().unwrap();
}

#[test]
fn test_compound_planned_query_pages_with_cursor_across_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // 15 matches flushed into a segment, then 10 more matches plus noise left
    // in the memtable, so cursor pages cross posting lists and sources.
    let mut expected = Vec::new();
    for ordinal in 0..15 {
        expected.push(insert_query_node(
            &engine,
            "CompoundPagePerson",
            &format!("seg-{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("active".to_string())),
            ],
            1.0,
        ));
    }
    let info = engine
        .ensure_node_property_index(
            "CompoundPagePerson",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.flush().unwrap();
    for ordinal in 0..10 {
        expected.push(insert_query_node(
            &engine,
            "CompoundPagePerson",
            &format!("mem-{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("active".to_string())),
            ],
            1.0,
        ));
        insert_query_node(
            &engine,
            "CompoundPagePerson",
            &format!("mem-noise-{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("inactive".to_string())),
            ],
            1.0,
        );
    }

    let mut query = query_ids(
        Some("CompoundPagePerson"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );
    query.page.limit = Some(4);
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(find_compound_details(&plan.root, false).is_some());

    let mut collected = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = engine.query_node_ids(&query).unwrap();
        assert!(page.items.len() <= 4);
        assert!(
            page.items.windows(2).all(|pair| pair[0] < pair[1]),
            "page items must be strictly ascending"
        );
        if let (Some(first), Some(after)) = (page.items.first(), query.page.after) {
            assert!(*first > after, "page must start after the cursor");
        }
        let next_cursor = page.next_cursor;
        collected.extend(page.items);
        pages += 1;
        assert!(pages <= 25, "cursor pagination failed to terminate");
        match next_cursor {
            Some(cursor) => query.page.after = Some(cursor),
            None => break,
        }
    }
    assert!(pages >= 7, "expected multiple pages, got {pages}");
    assert_eq!(collected, expected);

    engine.close().unwrap();
}

#[test]
fn test_compound_value_dedup_handles_large_endpoint_lists() {
    // Past the IN-expansion cap, dedup switches to canonical byte keys; it
    // must still preserve first-occurrence order and exact uniqueness.
    let mut values: Vec<CompoundOwnedValue> = (0..600)
        .map(|ordinal| CompoundOwnedValue::U64(ordinal % 150))
        .collect();
    dedup_compound_values(&mut values);
    assert_eq!(
        values,
        (0..150).map(CompoundOwnedValue::U64).collect::<Vec<_>>()
    );

    let mut mixed: Vec<CompoundOwnedValue> = (0..400)
        .map(|ordinal| CompoundOwnedValue::Property(PropValue::Int(ordinal % 80)))
        .collect();
    dedup_compound_values(&mut mixed);
    assert_eq!(
        mixed,
        (0..80)
            .map(|ordinal| CompoundOwnedValue::Property(PropValue::Int(ordinal)))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_compound_source_not_cheapest_skips_to_verifier_instead_of_intersect() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // 8 rare acme nodes, 12 common acme nodes (compound prefix `tenant=acme`
    // matches 20 — within the 4x broad-source factor of the 8-candidate
    // status index but not cheaper), and 80 other-tenant nodes so the
    // compound prefix stays well under the label count.
    let mut rare_ids = Vec::new();
    for ordinal in 0..8 {
        rare_ids.push(insert_query_node(
            &engine,
            "IntersectPerson",
            &format!("rare-{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("rare".to_string())),
                ("score", PropValue::Int(ordinal)),
            ],
            1.0,
        ));
    }
    for ordinal in 0..12 {
        insert_query_node(
            &engine,
            "IntersectPerson",
            &format!("common-{ordinal}"),
            &[
                ("tenant", PropValue::String("acme".to_string())),
                ("status", PropValue::String("common".to_string())),
                ("score", PropValue::Int(ordinal)),
            ],
            1.0,
        );
    }
    for ordinal in 0..80 {
        insert_query_node(
            &engine,
            "IntersectPerson",
            &format!("other-{ordinal}"),
            &[
                ("tenant", PropValue::String("other".to_string())),
                ("status", PropValue::String("common".to_string())),
                ("score", PropValue::Int(ordinal)),
            ],
            1.0,
        );
    }

    let status_info = engine
        .ensure_node_property_index(
            "IntersectPerson",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    let compound_info = engine
        .ensure_node_property_index(
            "IntersectPerson",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, status_info.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, compound_info.index_id, SecondaryIndexState::Ready);
    engine.flush().unwrap();

    // `status = rare` is far more selective than the compound prefix
    // `tenant = acme`, so the compound source is not the cheapest input. It
    // must skip to the verifier rather than execute a second sidecar scan
    // alongside the cheaper driver.
    let query = query_ids(
        Some("IntersectPerson"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("rare".to_string()),
            },
        ],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(
        find_compound_details(&plan.root, false).is_none()
            && find_compound_details(&plan.root, true).is_none(),
        "non-cheapest compound source must not execute: {:?}",
        plan.root
    );
    assert_eq!(engine.query_node_ids(&query).unwrap().items, rare_ids);

    engine.close().unwrap();
}

#[test]
fn test_compound_weight_range_demotes_to_prefix_and_matches_infinite_weight() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person", "weight-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "weight-b", &[], 1.0);
    let finite = engine
        .upsert_edge(
            a,
            b,
            "WEIGHTED_REL",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    let infinite = engine
        .upsert_edge(
            b,
            a,
            "WEIGHTED_REL",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                weight: f32::INFINITY,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            b,
            "WEIGHTED_REL",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("cold".to_string()))]),
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();

    let info = engine
        .ensure_edge_property_index(
            "WEIGHTED_REL",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.flush().unwrap();

    // The infinite weight is stored as an EqualityHash-class tuple component
    // that Numeric-class range scans never visit, while the public verifier
    // matches it. The planner must keep the equality-prefix compound
    // candidate and verify the weight range residually so plan choice cannot
    // change results.
    let query = EdgeQuery {
        label: Some("WEIGHTED_REL".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::WeightRange {
                lower: Some(0.5),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    let details = compound_range_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.matched_prefix_len, 1);
    assert_eq!(
        details.range_field, None,
        "weight must not drive a compound range scan"
    );
    let mut edge_ids = engine.query_edge_ids(&query).unwrap().edge_ids;
    edge_ids.sort_unstable();
    let mut expected = vec![finite, infinite];
    expected.sort_unstable();
    assert_eq!(edge_ids, expected);

    engine.close().unwrap();
}

#[test]
fn test_compound_edge_corrupt_sidecar_falls_back_and_enqueues_followup() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person", "compound-corrupt-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "compound-corrupt-b", &[], 1.0);
    let keep = engine
        .upsert_edge(
            a,
            b,
            "COMPOUND_CORRUPT_REL",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index(
            "COMPOUND_CORRUPT_REL",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::Weight),
            ]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = EdgeQuery {
        label: Some("COMPOUND_CORRUPT_REL".to_string()),
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
        ..Default::default()
    };
    let plan = engine.explain_edge_query(&query).unwrap();
    assert_eq!(compound_range_details(&plan).index_id, info.index_id);

    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::edge_compound_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        info.index_id,
    );
    engine.close().unwrap();
    corrupt_compound_sidecar_payload_only_in_place(&sidecar_path);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_edge_property_index_state(&reopened, info.index_id, SecondaryIndexState::Ready);
    let plan = reopened.explain_edge_query(&query).unwrap();
    assert_eq!(compound_range_details(&plan).index_id, info.index_id);
    assert_single_read_followup_enqueued(&reopened, || {
        assert_eq!(reopened.query_edge_ids(&query).unwrap().edge_ids, vec![keep]);
    });
    reopened.close().unwrap();
}

#[test]
fn test_compound_node_missing_sidecar_falls_back_and_enqueues_followup() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(
        &engine,
        "CompoundMissingPerson",
        "compound-missing-keep",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "CompoundMissingPerson",
        "compound-missing-skip",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
        ],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(
            "CompoundMissingPerson",
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("status"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some("CompoundMissingPerson"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(compound_equality_details(&plan).index_id, info.index_id);

    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::node_compound_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        info.index_id,
    );
    std::fs::remove_file(&sidecar_path).unwrap();

    assert_single_read_followup_enqueued(&engine, || {
        assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);
    });
    engine.close().unwrap();
}

#[test]
fn test_compound_range_declaration_prefix_scan_missing_sidecar_self_repairs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(
        &engine,
        "CompoundRangeRepairPerson",
        "range-repair-keep",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(7)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "CompoundRangeRepairPerson",
        "range-repair-skip",
        &[
            ("tenant", PropValue::String("acme".to_string())),
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(3)),
        ],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(
            "CompoundRangeRepairPerson",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    // The equality on `status` is outside the declaration, so the Range-kind
    // declaration is selected for a prefix-only scan on `tenant`.
    let query = query_ids(
        Some("CompoundRangeRepairPerson"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("acme".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    let details = compound_range_details(&plan);
    assert_eq!(details.index_id, info.index_id);
    assert_eq!(details.matched_prefix_len, 1);
    assert_eq!(details.range_field, None);

    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::node_compound_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        info.index_id,
    );
    std::fs::remove_file(&sidecar_path).unwrap();

    // A missing sidecar on a prefix-only scan must still enqueue the
    // Range-kind repair followup: fallback keeps results correct and the
    // declaration rebuilds back to Ready instead of silently falling back
    // forever.
    assert_single_read_followup_enqueued(&engine, || {
        assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);
    });
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    let rebuilt_path = crate::segment_writer::node_compound_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        info.index_id,
    );
    assert!(rebuilt_path.exists(), "repair must rebuild the sidecar");
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_view_rebuilds_only_with_read_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let initial = engine.planner_stats_view_for_test();
    assert!(initial.generation >= 1);
    assert_eq!(initial.segment_count, 0);

    insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let after_write = engine.planner_stats_view_for_test();
    assert!(std::sync::Arc::ptr_eq(&initial, &after_write));
    assert_eq!(after_write.generation, initial.generation);

    engine.flush().unwrap();
    let after_flush = engine.planner_stats_view_for_test();
    assert!(!std::sync::Arc::ptr_eq(&after_write, &after_flush));
    assert!(after_flush.generation > after_write.generation);
    assert_eq!(after_flush.segment_count, 1);
    assert_eq!(after_flush.available_segment_stats, 1);
    assert_eq!(after_flush.full_rollup.node_count, 1);

    engine.close().unwrap();
}
#[test]
fn test_planner_stats_stale_risk_uses_newer_sample_shadowing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for index in 0..16 {
        insert_query_node(&engine, "Person",
            &format!("shadow-{index:02}"),
            &[("version", PropValue::Int(1))],
            1.0,
        );
    }
    engine.flush().unwrap();
    for index in 0..8 {
        insert_query_node(&engine, "Person",
            &format!("shadow-{index:02}"),
            &[("version", PropValue::Int(2))],
            1.0,
        );
    }
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    assert_eq!(
        stats_view.max_segment_stale_risk(),
        crate::planner_stats::StalePostingRisk::High
    );

    engine.close().unwrap();
}
#[test]
fn test_planner_stats_stale_risk_uses_newer_tombstones() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut ids = Vec::new();
    for index in 0..16 {
        ids.push(insert_query_node(&engine, "Person",
            &format!("delete-{index:02}"),
            &[],
            1.0,
        ));
    }
    engine.flush().unwrap();
    engine.delete_node(ids[0]).unwrap();
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    assert_eq!(
        stats_view.max_segment_stale_risk(),
        crate::planner_stats::StalePostingRisk::Medium
    );

    engine.close().unwrap();
}

#[test]
fn test_write_adjacent_helper_reads_do_not_rebuild_planner_stats_view() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let delete_a = insert_query_node(&engine, "Person",  "delete-a", &[], 1.0);
    let delete_b = insert_query_node(&engine, "Person",  "delete-b", &[], 1.0);
    let patch_c = insert_query_node(&engine, "Company",  "patch-c", &[], 1.0);
    let patch_d = insert_query_node(&engine, "Company",  "patch-d", &[], 1.0);
    let prune_e = insert_query_node(&engine, "Metric",  "prune-e", &[], 0.1);
    let prune_f = insert_query_node(&engine, "NodeLabel91",  "prune-f", &[], 1.0);
    engine
        .upsert_edge(delete_a, delete_b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let patch_edge = engine
        .upsert_edge(patch_c, patch_d, "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(prune_e, prune_f, "RATES", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let stats_before = engine.planner_stats_view_for_test();
    let generation_before = stats_before.generation;
    let source_builds_before = engine.published_read_source_build_count_for_test();
    engine.reset_publish_counters_for_test();

    engine.delete_node(delete_a).unwrap();
    engine
        .graph_patch(GraphPatch {
            invalidate_edges: vec![(patch_edge, 1)],
            delete_node_ids: vec![patch_c],
            ..Default::default()
        })
        .unwrap();
    let prune = engine
        .prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            label: Some("Metric".to_string()),
        })
        .unwrap();
    assert_eq!(prune.nodes_pruned, 1);
    assert_eq!(prune.edges_pruned, 1);

    let stats_after = engine.planner_stats_view_for_test();
    let counters = engine.publish_counter_snapshot_for_test();
    assert_eq!(counters.rebuild_sources, 0);
    assert_eq!(counters.source_rebuilds, 0);
    assert_eq!(
        engine.published_read_source_build_count_for_test(),
        source_builds_before
    );
    assert!(std::sync::Arc::ptr_eq(&stats_before, &stats_after));
    assert_eq!(stats_after.generation, generation_before);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_corruption_degrades_without_index_repair_followup() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        insert_query_node(&engine, "Person",
            "red",
            &[("status", PropValue::String("red".to_string()))],
            1.0,
        );
        insert_query_node(&engine, "Person",
            "blue",
            &[("status", PropValue::String("blue".to_string()))],
            1.0,
        );
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 1)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.segment_count, 1);
    assert_eq!(stats_view.available_segment_stats, 0);
    assert_eq!(stats_view.unavailable_segment_stats, 1);

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("red".to_string()),
        }],
        false,
    );
    assert_eq!(reopened.query_node_ids(&query).unwrap().items.len(), 1);
    let plan = reopened.explain_node_query(&query).unwrap();
    assert!(!plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    reopened.close().unwrap();
}

#[test]
fn node_property_query_enqueues_planning_followup_for_corrupt_equality_sidecar() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        keep = insert_query_node(&engine, "Person",
            "node-plan-eq-active",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        insert_query_node(&engine, "Person",
            "node-plan-eq-inactive",
            &[("status", PropValue::String("inactive".to_string()))],
            1.0,
        );
        engine.flush().unwrap();
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
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

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    let plan = reopened.explain_node_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::PropertyEqualityIndex
    ));
    assert!(plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));

    assert_single_read_followup_enqueued(&reopened, || {
        assert_eq!(reopened.query_node_ids(&query).unwrap().items, vec![keep]);
    });
    reopened.close().unwrap();
}

#[test]
fn node_property_query_enqueues_planning_followup_for_corrupt_range_sidecar() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        keep = insert_query_node(&engine, "Person",
            "node-plan-range-keep",
            &[("score", PropValue::Int(7))],
            1.0,
        );
        insert_query_node(&engine, "Person",
            "node-plan-range-skip",
            &[("score", PropValue::Int(20))],
            1.0,
        );
        engine.flush().unwrap();
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        segment_id = engine.segments_for_test()[0].segment_id;
        engine.close().unwrap();
    }

    let sidecar_path = crate::segment_writer::node_prop_range_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_planner_stats_for_segment(&db_path, segment_id);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    corrupt_sidecar_header_in_place(&sidecar_path);

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(10))),
        }],
        false,
    );
    let plan = reopened.explain_node_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::PropertyRangeIndex
    ));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::FallbackNodeLabelScan
    ));

    assert_single_read_followup_enqueued(&reopened, || {
        assert_eq!(reopened.query_node_ids(&query).unwrap().items, vec![keep]);
    });
    reopened.close().unwrap();
}

#[test]
fn node_property_query_enqueues_planning_followup_for_missing_equality_sidecar() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(&engine, "Person",
        "node-plan-missing-active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(&engine, "Person",
        "node-plan-missing-inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let segment_id = engine.segments_for_test()[0].segment_id;
    let seg_dir = crate::segment_writer::segment_dir(&db_path, segment_id);
    corrupt_planner_stats_for_segment(&db_path, segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    engine
        .reopen_segment_reader_and_rebuild_sources_for_test(segment_id)
        .unwrap();

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::PropertyEqualityIndex
    ));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::FallbackNodeLabelScan
    ));

    let (build_ready_rx, build_release_tx) = engine.set_secondary_index_build_pause();
    let (followup_ready_rx, followup_release_tx) = engine.set_runtime_publish_pause();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);
    followup_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 1);
    followup_release_tx.send(()).unwrap();
    build_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    build_release_tx.send(()).unwrap();
    wait_for_pending_secondary_index_followup_count(&engine, 0);
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_zero_is_advisory_not_empty_result() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        index_id = info.index_id;
        wait_for_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
        insert_query_node(&engine, "Person",
            "red",
            &[("status", PropValue::String("red".to_string()))],
            1.0,
        );
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let seg_dir = crate::segment_writer::segment_dir(&db_path, 1);
    let mut stats = match crate::planner_stats::read_planner_stats_sidecar(&seg_dir, 1, 1, 0) {
        crate::planner_stats::PlannerStatsAvailability::Available(stats) => *stats,
        other => panic!("expected available planner stats, got {other:?}"),
    };
    let equality = stats
        .equality_index_stats
        .iter_mut()
        .find(|stats| stats.index_id == index_id)
        .expect("expected equality stats for test index");
    equality.total_postings = 0;
    equality.value_group_count = 0;
    equality.max_group_postings = 0;
    equality.top_value_hashes.clear();
    let ready_indexes = [ready_node_property_equality_entry(index_id, 1, "status")];
    publish_planner_stats_for_test(&seg_dir, stats, &ready_indexes);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let red = PropValue::String("red".to_string());
    let red_hash = hash_prop_equality_key(&red);
    assert_eq!(
        reopened
            .planner_stats_view_for_test()
            .equality_segment_estimate(index_id, 1, &[red_hash])
            .unwrap(),
        crate::planner_stats::PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        }
    );
    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: red,
        }],
        false,
    );
    assert_eq!(reopened.query_node_ids(&query).unwrap().items.len(), 1);
    let plan = reopened.explain_node_query(&query).unwrap();
    assert_eq!(plan.estimated_candidates, Some(0));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_low_equality_estimate_uses_capped_materialization() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        index_id = info.index_id;
        wait_for_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
        for idx in 0..=QUERY_RANGE_CANDIDATE_CAP {
            insert_query_node(&engine, "Person",
                &format!("active-{idx}"),
                &[("status", PropValue::String("active".to_string()))],
                1.0,
            );
        }
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let node_count = (QUERY_RANGE_CANDIDATE_CAP + 1) as u64;
    let seg_dir = crate::segment_writer::segment_dir(&db_path, 1);
    let mut stats = match crate::planner_stats::read_planner_stats_sidecar(&seg_dir, 1, node_count, 0)
    {
        crate::planner_stats::PlannerStatsAvailability::Available(stats) => *stats,
        other => panic!("expected available planner stats, got {other:?}"),
    };
    let equality = stats
        .equality_index_stats
        .iter_mut()
        .find(|stats| stats.index_id == index_id)
        .expect("expected equality stats for test index");
    equality.total_postings = 0;
    equality.value_group_count = 0;
    equality.max_group_postings = 0;
    equality.top_value_hashes.clear();
    let ready_indexes = [ready_node_property_equality_entry(index_id, 1, "status")];
    publish_planner_stats_for_test(&seg_dir, stats, &ready_indexes);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    {
        let (_guard, published) = reopened.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_node_query(&query).unwrap();
        let planned = published.view.plan_normalized_node_query(&normalized).unwrap();
        let NodePhysicalPlan::Source(source) = planned.driver else {
            panic!("expected equality source driver");
        };
        assert_eq!(source.kind, NodeQueryCandidateSourceKind::PropertyEqualityIndex);
        assert_eq!(source.estimate.known_upper_bound(), Some(0));
        assert!(!source.estimate.can_use_uncapped_equality_materialization());
    }
    let result = reopened.query_node_ids(&query).unwrap();
    assert_eq!(result.items.len(), QUERY_RANGE_CANDIDATE_CAP + 1);
    assert_eq!(result.next_cursor, None);

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_back_label_and_full_scan_explain_estimates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for idx in 0..3 {
        insert_query_node(&engine, "Person",  &format!("label1-{idx}"), &[], 1.0);
    }
    for idx in 0..2 {
        insert_query_node(&engine, "Company",  &format!("label2-{idx}"), &[], 1.0);
    }
    engine.flush().unwrap();
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let label_estimate = published.view.node_label_estimate(1).unwrap();
        assert_eq!(label_estimate.kind, PlannerEstimateKind::StatsExact);
        assert_eq!(label_estimate.known_upper_bound(), Some(3));
        let full_estimate = published.view.full_scan_estimate();
        assert_eq!(full_estimate.kind, PlannerEstimateKind::StatsExact);
        assert_eq!(full_estimate.known_upper_bound(), Some(5));
    }

    let label_query = query_ids(Some("Person"), Vec::new(), false);
    assert_eq!(engine.query_node_ids(&label_query).unwrap().items.len(), 3);
    let label_plan = engine.explain_node_query(&label_query).unwrap();
    assert_eq!(label_plan.estimated_candidates, Some(3));
    assert_plan_input_nodes(&label_plan, vec![QueryPlanNode::NodeLabelIndex]);

    let full_scan_query = NodeQuery {
        allow_full_scan: true,
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&full_scan_query).unwrap().items.len(), 5);
    let full_scan_plan = engine.explain_node_query(&full_scan_query).unwrap();
    assert_eq!(full_scan_plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&full_scan_plan, vec![QueryPlanNode::FallbackFullNodeScan]);
    assert_eq!(
        full_scan_plan.warnings,
        vec![QueryPlanWarning::FullScanExplicitlyAllowed]
    );

    engine.close().unwrap();
}

#[test]
fn test_multi_label_any_and_all_membership_estimates_are_conservative() {
    let (_dir, engine) = query_test_engine();
    let person_id = engine.get_node_label_id("Person").unwrap().unwrap();
    let company_id = engine.get_node_label_id("Company").unwrap().unwrap();

    engine
        .upsert_node(
            &["Person", "Company"],
            "overlap",
            UpsertNodeOptions {
                props: BTreeMap::new(),
                ..Default::default()
            },
        )
        .unwrap();
    insert_query_node(&engine, "Person", "person-a", &[], 1.0);
    insert_query_node(&engine, "Person", "person-b", &[], 1.0);
    insert_query_node(&engine, "Company", "company", &[], 1.0);
    engine.flush().unwrap();

    let labels = NodeLabelSet::from_canonical_ids(&[person_id, company_id]).unwrap();
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let any = published
        .view
        .node_label_filter_estimate(&labels, LabelMatchMode::Any)
        .unwrap();
    assert_eq!(any.estimate.kind, PlannerEstimateKind::UpperBound);
    assert_eq!(any.estimate.known_upper_bound(), Some(5));
    assert_eq!(any.driver_label_id, None);
    assert_eq!(published.view.full_scan_estimate().known_upper_bound(), Some(4));

    let all = published
        .view
        .node_label_filter_estimate(&labels, LabelMatchMode::All)
        .unwrap();
    assert_eq!(all.estimate.kind, PlannerEstimateKind::UpperBound);
    assert_eq!(all.estimate.known_upper_bound(), Some(2));
    assert_eq!(all.driver_label_id, Some(company_id));

    drop(published);
    drop(_guard);
    engine.close().unwrap();
}

#[test]
fn test_multi_label_filter_estimates_fall_back_when_stats_are_corrupt() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        seed_query_test_catalog(&engine);
        engine
            .upsert_node(
                &["Person", "Company"],
                "covered-overlap",
                UpsertNodeOptions {
                    props: BTreeMap::new(),
                    ..Default::default()
                },
            )
            .unwrap();
        insert_query_node(&engine, "Person", "covered-person", &[], 1.0);
        engine.flush().unwrap();
        engine
            .upsert_node(
                &["Person", "Company"],
                "fallback-overlap",
                UpsertNodeOptions {
                    props: BTreeMap::new(),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();
        engine.close().unwrap();
    }
    corrupt_planner_stats_for_segment(&db_path, 2);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let person_id = reopened.get_node_label_id("Person").unwrap().unwrap();
    let company_id = reopened.get_node_label_id("Company").unwrap().unwrap();
    let labels = NodeLabelSet::from_canonical_ids(&[person_id, company_id]).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.available_segment_stats, 1);
    assert_eq!(stats_view.unavailable_segment_stats, 1);
    drop(stats_view);

    let (_guard, published) = reopened.runtime.published_snapshot().unwrap();
    let any = published
        .view
        .node_label_filter_estimate(&labels, LabelMatchMode::Any)
        .unwrap();
    assert_eq!(any.estimate.kind, PlannerEstimateKind::UpperBound);
    assert_eq!(any.estimate.known_upper_bound(), Some(5));
    assert_eq!(any.driver_label_id, None);

    let all = published
        .view
        .node_label_filter_estimate(&labels, LabelMatchMode::All)
        .unwrap();
    assert_eq!(all.estimate.kind, PlannerEstimateKind::UpperBound);
    assert_eq!(all.estimate.known_upper_bound(), Some(2));
    assert_eq!(all.driver_label_id, Some(company_id));

    drop(published);
    drop(_guard);
    reopened.close().unwrap();
}

#[test]
fn test_node_query_multi_label_any_all_unknown_and_explain_notes() {
    let (_dir, engine) = query_test_engine();
    let person = insert_query_node(&engine, "Person", "person", &[], 1.0);
    let employee = insert_query_node(&engine, "Employee", "employee", &[], 1.0);
    let both =
        insert_query_node_with_labels(&engine, &["Person", "Employee"], "both", &[], 1.0);
    let _company = insert_query_node(&engine, "Company", "company", &[], 1.0);

    let mut any_expected = vec![person, employee, both];
    any_expected.sort_unstable();
    let any_query = query_label_filter(&["Person", "Employee"], LabelMatchMode::Any);
    assert_eq!(engine.query_node_ids(&any_query).unwrap().items, any_expected);
    let any_plan = engine.explain_node_query(&any_query).unwrap();
    assert_plan_input_nodes(&any_plan, vec![QueryPlanNode::NodeLabelAnyIndex]);
    assert_eq!(
        any_plan.public_inputs.node_labels,
        vec![
            QueryPlanPublicName {
                alias: None,
                name: "Person".to_string(),
                known: true,
                mode: Some(LabelMatchMode::Any),
            },
            QueryPlanPublicName {
                alias: None,
                name: "Employee".to_string(),
                known: true,
                mode: Some(LabelMatchMode::Any),
            },
        ]
    );
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAnyDedupeBeforePagination));
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAnyFinalVerification));
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::StaleNodeLabelMembershipVerification));

    let all_query = query_label_filter(&["Person", "Employee"], LabelMatchMode::All);
    assert_eq!(
        engine.query_node_ids(&all_query).unwrap().items,
        vec![both]
    );
    let all_plan = engine.explain_node_query(&all_query).unwrap();
    assert_eq!(
        all_plan.public_inputs.node_labels,
        vec![
            QueryPlanPublicName {
                alias: None,
                name: "Person".to_string(),
                known: true,
                mode: Some(LabelMatchMode::All),
            },
            QueryPlanPublicName {
                alias: None,
                name: "Employee".to_string(),
                known: true,
                mode: Some(LabelMatchMode::All),
            },
        ]
    );
    assert!(all_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAllSupersetVerification));
    assert!(all_plan
        .notes
        .contains(&QueryPlanNote::StaleNodeLabelMembershipVerification));

    let mixed_unknown_any = query_label_filter(&["Person", "Missing"], LabelMatchMode::Any);
    let mut mixed_expected = vec![person, both];
    mixed_expected.sort_unstable();
    assert_eq!(
        engine.query_node_ids(&mixed_unknown_any).unwrap().items,
        mixed_expected
    );
    let mixed_plan = engine.explain_node_query(&mixed_unknown_any).unwrap();
    assert!(mixed_plan
        .warnings
        .contains(&QueryPlanWarning::UnknownNodeLabel));
    assert_eq!(
        mixed_plan.public_inputs.node_labels,
        vec![
            QueryPlanPublicName {
                alias: None,
                name: "Person".to_string(),
                known: true,
                mode: Some(LabelMatchMode::Any),
            },
            QueryPlanPublicName {
                alias: None,
                name: "Missing".to_string(),
                known: false,
                mode: Some(LabelMatchMode::Any),
            },
        ]
    );

    let all_unknown_any = query_label_filter(&["Missing"], LabelMatchMode::Any);
    assert!(engine
        .query_node_ids(&all_unknown_any)
        .unwrap()
        .items
        .is_empty());
    assert!(engine
        .explain_node_query(&all_unknown_any)
        .unwrap()
        .warnings
        .contains(&QueryPlanWarning::UnknownNodeLabel));

    let mixed_unknown_all = query_label_filter(&["Person", "Missing"], LabelMatchMode::All);
    assert!(engine
        .query_node_ids(&mixed_unknown_all)
        .unwrap()
        .items
        .is_empty());
    assert!(engine
        .explain_node_query(&mixed_unknown_all)
        .unwrap()
        .warnings
        .contains(&QueryPlanWarning::UnknownNodeLabel));

    engine.close().unwrap();
}

#[test]
fn test_node_query_any_dedupes_before_pagination_and_hydrates_final_page() {
    let (_dir, engine) = query_test_engine();
    let both_a =
        insert_query_node_with_labels(&engine, &["Person", "Employee"], "both-a", &[], 1.0);
    let person = insert_query_node(&engine, "Person", "person", &[], 1.0);
    let employee = insert_query_node(&engine, "Employee", "employee", &[], 1.0);
    let both_b =
        insert_query_node_with_labels(&engine, &["Person", "Employee"], "both-b", &[], 1.0);
    let expected = [both_a, person, employee, both_b];

    let mut query = query_label_filter(&["Person", "Employee"], LabelMatchMode::Any);
    query.page = PageRequest {
        limit: Some(3),
        after: None,
    };
    let first = engine.query_node_ids(&query).unwrap();
    assert_eq!(first.items, expected[..3]);
    assert_eq!(first.next_cursor, Some(employee));

    query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&query).unwrap();
    assert_eq!(second.items, expected[3..]);
    assert_eq!(second.next_cursor, None);

    query.page = PageRequest {
        limit: Some(2),
        after: None,
    };
    engine.reset_query_execution_counters_for_test();
    let nodes = engine.query_nodes(&query).unwrap();
    assert_eq!(
        nodes.items.iter().map(|node| node.id).collect::<Vec<_>>(),
        expected[..2]
    );
    assert_eq!(nodes.next_cursor, Some(person));
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 2);

    engine.close().unwrap();
}

#[test]
fn test_node_query_single_label_cursor_ignores_trailing_stale_postings() {
    let (_dir, engine) = query_test_engine();
    let keep_a =
        insert_query_node_with_labels(&engine, &["Employee", "Current"], "keep-a", &[], 1.0);
    let keep_b =
        insert_query_node_with_labels(&engine, &["Employee", "Current"], "keep-b", &[], 1.0);
    let stale_count = 20usize;
    let stale_ids = (0..stale_count)
        .map(|idx| {
            insert_query_node_with_labels(
                &engine,
                &["Employee", "Former"],
                &format!("stale-{idx}"),
                &[],
                1.0,
            )
        })
        .collect::<Vec<_>>();
    engine.flush().unwrap();

    for (idx, expected_id) in stale_ids.iter().copied().enumerate() {
        let updated = insert_query_node_with_labels(
            &engine,
            &["Former"],
            &format!("stale-{idx}"),
            &[],
            1.0,
        );
        assert_eq!(updated, expected_id);
    }

    let mut query = query_label_filter(&["Employee"], LabelMatchMode::All);
    query.page = PageRequest {
        limit: Some(2),
        after: None,
    };
    let first = engine.query_node_ids(&query).unwrap();
    assert_eq!(first.items, vec![keep_a, keep_b]);
    assert_eq!(first.next_cursor, None);

    query.page.after = Some(keep_b);
    assert!(engine.query_node_ids(&query).unwrap().items.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_node_query_single_label_label_only_small_page_stays_page_shaped() {
    let (_dir, engine) = query_test_engine();
    let total = 40usize;
    let expected = (0..total)
        .map(|idx| insert_query_node(&engine, "Person", &format!("person-{idx}"), &[], 1.0))
        .collect::<Vec<_>>();
    engine.flush().unwrap();

    let mut query = query_label_filter(&["Person"], LabelMatchMode::All);
    query.page = PageRequest {
        limit: Some(2),
        after: None,
    };

    engine.reset_query_execution_counters_for_test();
    let page = engine.query_nodes(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        page.items.iter().map(|node| node.id).collect::<Vec<_>>(),
        expected[..2]
    );
    assert_eq!(page.next_cursor, Some(expected[1]));
    assert_eq!(counters.node_record_hydration_reads, 2);
    assert!(
        counters.node_visibility_meta_reads < total,
        "label-only page read should not verify the full posting list"
    );

    engine.close().unwrap();
}

#[test]
fn test_node_query_any_overlap_streams_and_hydrates_final_page() {
    let (_dir, engine) = query_test_engine();
    let total = 40usize;
    let expected = (0..total)
        .map(|idx| {
            insert_query_node_with_labels(
                &engine,
                &["Person", "Employee"],
                &format!("overlap-{idx}"),
                &[],
                1.0,
            )
        })
        .collect::<Vec<_>>();
    engine.flush().unwrap();

    let mut query = query_label_filter(&["Person", "Employee"], LabelMatchMode::Any);
    query.page = PageRequest {
        limit: Some(3),
        after: None,
    };

    engine.reset_query_execution_counters_for_test();
    let page = engine.query_nodes(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        page.items.iter().map(|node| node.id).collect::<Vec<_>>(),
        expected[..3]
    );
    assert_eq!(page.next_cursor, Some(expected[2]));
    assert_eq!(counters.node_record_hydration_reads, 3);
    assert!(
        counters.node_visibility_meta_reads < total,
        "overlapping Any scan should dedupe raw candidates before page verification"
    );

    engine.close().unwrap();
}

#[test]
fn test_node_query_multi_label_property_keys_and_stale_membership() {
    let (_dir, engine) = query_test_engine();
    let active_both = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "active-both",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let _inactive_both = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "inactive-both",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let _active_person = insert_query_node(
        &engine,
        "Person",
        "active-person",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let _active_employee = insert_query_node(
        &engine,
        "Employee",
        "active-employee",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let property_all = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&property_all).unwrap().items,
        vec![active_both]
    );

    let single_key = NodeQuery {
        label_filter: Some(node_label_filter(&["Person"], LabelMatchMode::All)),
        keys: vec!["active-both".to_string()],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&single_key).unwrap().items,
        vec![active_both]
    );

    let ambiguous_key = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::Any)),
        keys: vec!["active-both".to_string()],
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&ambiguous_key).unwrap_err(),
        EngineError::InvalidOperation(message)
            if message.contains("keys require exactly one resolved label")
    ));

    let stale = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "stale",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    assert!(engine.remove_node_label(stale, "Employee").unwrap());

    let employee_query = query_label_filter(&["Employee"], LabelMatchMode::All);
    let employee_ids = engine.query_node_ids(&employee_query).unwrap().items;
    assert!(!employee_ids.contains(&stale));
    let stale_all = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        ids: vec![stale],
        ..Default::default()
    };
    assert!(engine.query_node_ids(&stale_all).unwrap().items.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_node_query_multi_label_all_uses_requested_label_property_index() {
    let (_dir, engine) = query_test_engine();
    let both = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "both",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let _inactive_both = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "inactive-both",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let person_only = insert_query_node(
        &engine,
        "Person",
        "person-only",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let employee_only = insert_query_node(
        &engine,
        "Employee",
        "employee-only",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    engine.flush().unwrap();

    let status = engine
        .ensure_node_property_index("Employee", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let all_query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&all_query).unwrap().items, vec![both]);
    let all_plan = engine.explain_node_query(&all_query).unwrap();
    assert_eq!(all_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&all_plan, vec![QueryPlanNode::PropertyEqualityIndex]);
    assert!(all_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAllSupersetVerification));

    let any_query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::Any)),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    let mut expected_any = vec![both, person_only, employee_only];
    expected_any.sort_unstable();
    assert_eq!(
        engine.query_node_ids(&any_query).unwrap().items,
        expected_any
    );
    let any_plan = engine.explain_node_query(&any_query).unwrap();
    assert!(
        !plan_contains_node(&any_plan.root, &QueryPlanNode::PropertyEqualityIndex),
        "multi-label Any must not use one label's property index as a complete source"
    );

    engine.close().unwrap();
}

#[test]
fn test_node_query_multi_label_all_uses_requested_label_range_and_timestamp_indexes() {
    let (_dir, engine) = query_test_engine();
    let both = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "both",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    let both_out_of_range = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "both-out-of-range",
        &[("score", PropValue::Int(80))],
        1.0,
    );
    let person_only = insert_query_node(
        &engine,
        "Person",
        "person-only",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    let employee_only = insert_query_node(
        &engine,
        "Employee",
        "employee-only",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    set_query_node_updated_at(&engine, both, 1_000);
    set_query_node_updated_at(&engine, both_out_of_range, 2_000);
    set_query_node_updated_at(&engine, person_only, 1_000);
    set_query_node_updated_at(&engine, employee_only, 1_000);
    engine.flush().unwrap();

    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let range_query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&range_query).unwrap().items,
        vec![both]
    );
    let range_plan = engine.explain_node_query(&range_query).unwrap();
    assert_eq!(range_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&range_plan, vec![QueryPlanNode::PropertyRangeIndex]);

    let timestamp_query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(900),
            upper_ms: Some(1_100),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&timestamp_query).unwrap().items,
        vec![both]
    );
    let timestamp_plan = engine.explain_node_query(&timestamp_query).unwrap();
    assert_eq!(timestamp_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&timestamp_plan, vec![QueryPlanNode::TimestampIndex]);

    engine.close().unwrap();
}

#[test]
fn test_node_query_multi_label_all_large_explicit_ids_can_use_property_index() {
    let (_dir, engine) = query_test_engine();
    let mut all_ids = Vec::new();
    let mut expected = Vec::new();
    for index in 0..50 {
        let selected = index < 3;
        let node_id = insert_query_node_with_labels(
            &engine,
            &["Person", "Employee"],
            &format!("both-{index}"),
            &[("status", PropValue::String(if selected { "target" } else { "other" }.to_string()))],
            1.0,
        );
        if selected {
            expected.push(node_id);
        }
        all_ids.push(node_id);
    }
    engine.flush().unwrap();

    let status = engine
        .ensure_node_property_index("Employee", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        ids: all_ids.clone(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("target".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let tiny_query = NodeQuery {
        ids: all_ids[..2].to_vec(),
        ..query
    };
    let tiny_plan = engine.explain_node_query(&tiny_query).unwrap();
    assert_eq!(tiny_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&tiny_plan, vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}




#[test]
fn test_explain_stale_membership_note_follows_label_posting_source() {
    let (_dir, engine) = query_test_engine();
    let both =
        insert_query_node_with_labels(&engine, &["Person", "Employee"], "both", &[], 1.0);
    insert_query_node(&engine, "Person", "person", &[], 1.0);
    insert_query_node(&engine, "Employee", "employee", &[], 1.0);

    let explicit_all = NodeQuery {
        label_filter: Some(node_label_filter(&["Person", "Employee"], LabelMatchMode::All)),
        ids: vec![both],
        ..Default::default()
    };
    let explicit_plan = engine.explain_node_query(&explicit_all).unwrap();
    assert!(explicit_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAllSupersetVerification));
    assert!(!explicit_plan
        .notes
        .contains(&QueryPlanNote::StaleNodeLabelMembershipVerification));

    let key_lookup = NodeQuery {
        label_filter: Some(node_label_filter(&["Person"], LabelMatchMode::All)),
        keys: vec!["both".to_string()],
        ..Default::default()
    };
    let key_plan = engine.explain_node_query(&key_lookup).unwrap();
    assert!(!key_plan
        .notes
        .contains(&QueryPlanNote::StaleNodeLabelMembershipVerification));

    let any_label_scan = query_label_filter(&["Person", "Employee"], LabelMatchMode::Any);
    let any_plan = engine.explain_node_query(&any_label_scan).unwrap();
    assert_plan_input_nodes(&any_plan, vec![QueryPlanNode::NodeLabelAnyIndex]);
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAnyDedupeBeforePagination));
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAnyFinalVerification));
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::StaleNodeLabelMembershipVerification));

    let all_label_scan = query_label_filter(&["Person", "Employee"], LabelMatchMode::All);
    let all_plan = engine.explain_node_query(&all_label_scan).unwrap();
    assert!(all_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAllSupersetVerification));
    assert!(all_plan
        .notes
        .contains(&QueryPlanNote::StaleNodeLabelMembershipVerification));

    engine.close().unwrap();
}

#[test]
fn test_active_memtable_only_estimates_are_exact_cheap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    insert_query_node(&engine, "Person",
        "active",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    insert_query_node(&engine, "Person",
        "inactive",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );

    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let label_estimate = published.view.node_label_estimate(1).unwrap();
    assert_eq!(label_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(label_estimate.known_upper_bound(), Some(2));
    let full_estimate = published.view.full_scan_estimate();
    assert_eq!(full_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(full_estimate.known_upper_bound(), Some(2));
    let (equality_estimate, followup) = published
        .view
        .equality_candidate_estimate(
            status.index_id,
            "status",
            &PropValue::String("active".to_string()),
        )
        .unwrap();
    assert!(followup.is_none());
    let equality_estimate = equality_estimate.unwrap();
    assert_eq!(equality_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(equality_estimate.known_upper_bound(), Some(1));

    let normalized = NormalizedNodeQuery {
        single_label_id: Some(1),
        label_filter: ResolvedNodeLabelFilter::known(
            LabelMatchMode::All,
            NodeLabelSet::single(1).unwrap(),
            0,
        ),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: NormalizedNodeFilter::AlwaysTrue,
        allow_full_scan: false,
        page: PageRequest::default(),
        warnings: Vec::new(),
    };
    let cap_context = published.view.query_cap_context(&normalized).unwrap();
    let mut budget = BooleanPlanningBudget::new();
    let range_probe = published
        .view
        .range_candidate_probe(
            &normalized,
            cap_context,
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            &mut budget,
        )
        .unwrap();
    let range_estimate = range_probe.source.unwrap().estimate;
    assert_eq!(range_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(range_estimate.known_upper_bound(), Some(1));

    let mut budget = BooleanPlanningBudget::new();
    let timestamp_probe = published
        .view
        .timestamp_candidate_probe(&normalized, cap_context, 1, i64::MIN, i64::MAX, &mut budget)
        .unwrap();
    let timestamp_estimate = timestamp_probe.source.unwrap().estimate;
    assert_eq!(timestamp_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(timestamp_estimate.known_upper_bound(), Some(2));

    drop(published);
    drop(_guard);
    engine.close().unwrap();
}

#[test]
fn test_planner_stats_equality_heavy_hitter_and_residual_explain_estimates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let values: Vec<String> = (0..40).map(|idx| format!("status-{idx:02}")).collect();
    for value in &values {
        insert_query_node(&engine, "Person",
            value,
            &[("status", PropValue::String(value.clone()))],
            1.0,
        );
    }
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    let rollup = stats_view.equality_index_rollups.get(&info.index_id).unwrap();
    assert_eq!(rollup.total_postings, 40);
    assert_eq!(
        rollup.top_value_hashes.len(),
        crate::planner_stats::PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY
    );
    let top_value = values
        .iter()
        .find(|value| {
            rollup
                .top_value_hashes
                .contains_key(&hash_prop_equality_key(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    let residual_value = values
        .iter()
        .find(|value| {
            !rollup
                .top_value_hashes
                .contains_key(&hash_prop_equality_key(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    drop(stats_view);

    let top_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String(top_value),
        }],
        false,
    );
    assert_eq!(engine.query_node_ids(&top_query).unwrap().items.len(), 1);
    let top_plan = engine.explain_node_query(&top_query).unwrap();
    assert_eq!(top_plan.estimated_candidates, Some(1));
    assert_plan_input_nodes(&top_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let residual_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String(residual_value),
        }],
        false,
    );
    assert_eq!(engine.query_node_ids(&residual_query).unwrap().items.len(), 1);
    let residual_plan = engine.explain_node_query(&residual_query).unwrap();
    assert_eq!(residual_plan.estimated_candidates, Some(1));
    assert_plan_input_nodes(&residual_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_rare_residual_equality_beats_broad_label_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let value_count =
        QUERY_RANGE_CANDIDATE_CAP + crate::planner_stats::PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY + 1;
    let values: Vec<String> = (0..value_count)
        .map(|idx| format!("rare-status-{idx:04}"))
        .collect();
    for value in &values {
        insert_query_node(&engine, "Person",
            value,
            &[("status", PropValue::String(value.clone()))],
            1.0,
        );
    }
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    let rollup = stats_view.equality_index_rollups.get(&info.index_id).unwrap();
    let residual_value = values
        .iter()
        .find(|value| {
            !rollup
                .top_value_hashes
                .contains_key(&hash_prop_equality_key(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    assert!(rollup.total_postings > QUERY_RANGE_CANDIDATE_CAP as u64);
    drop(stats_view);

    let residual_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String(residual_value),
        }],
        false,
    );
    assert_eq!(engine.query_node_ids(&residual_query).unwrap().items.len(), 1);
    let residual_plan = engine.explain_node_query(&residual_query).unwrap();
    assert_eq!(residual_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(residual_plan.estimated_candidates, Some(1));
    assert_plan_input_nodes(
        &residual_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_broad_heavy_hitter_equality_uses_cheaper_label_scan() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let inputs: Vec<_> = (0..=QUERY_RANGE_CANDIDATE_CAP)
        .map(|index| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("broad-heavy-{index}"),
            props: query_test_props(&[("status", PropValue::String("broad".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("broad".to_string()),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::CandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_range_and_timestamp_explain_use_no_planning_probe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let inputs: Vec<_> = (0..32)
        .map(|index| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("stats-probe-{index}"),
            props: query_test_props(&[("score", PropValue::Int(index))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    engine.reset_query_planning_probe_counters_for_test();
    let range_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(12))),
        }],
        false,
    );
    let range_plan = engine.explain_node_query(&range_query).unwrap();
    assert_plan_input_nodes(&range_plan, vec![QueryPlanNode::PropertyRangeIndex]);
    assert_eq!(
        engine.query_planning_probe_snapshot_for_test().range,
        0,
        "stats-covered range explain must not materialize planning candidates"
    );

    let timestamp_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(i64::MIN),
            upper_ms: Some(i64::MAX),
        }],
        false,
    );
    let timestamp_plan = engine.explain_node_query(&timestamp_query).unwrap();
    assert_plan_input_nodes(&timestamp_plan, vec![QueryPlanNode::TimestampIndex]);
    assert_eq!(
        engine.query_planning_probe_snapshot_for_test().timestamp,
        0,
        "stats-covered timestamp explain must not materialize planning candidates"
    );

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_range_and_timestamp_mixed_coverage_probe_uncovered_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let all_ids;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let score = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
        wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

        let seg1 = [
            ("covered-a", 10, 1_000),
            ("covered-b", 20, 1_100),
            ("covered-c", 100, 9_000),
        ];
        let mut ids = Vec::new();
        for (key, score, updated_at) in seg1 {
            let node_id = insert_query_node(&engine, "Person",
                key,
                &[("score", PropValue::Int(score))],
                1.0,
            );
            set_query_node_updated_at(&engine, node_id, updated_at);
            ids.push(node_id);
        }
        engine.flush().unwrap();

        let seg2 = [
            ("uncovered-a", 15, 1_200),
            ("uncovered-b", 25, 1_300),
            ("uncovered-c", 200, 10_000),
        ];
        for (key, score, updated_at) in seg2 {
            let node_id = insert_query_node(&engine, "Person",
                key,
                &[("score", PropValue::Int(score))],
                1.0,
            );
            set_query_node_updated_at(&engine, node_id, updated_at);
            ids.push(node_id);
        }
        engine.flush().unwrap();
        all_ids = ids;
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 2)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.available_segment_stats, 1);
    assert_eq!(stats_view.unavailable_segment_stats, 1);
    assert_eq!(stats_view.timestamp_coverage.covered_segment_ids, vec![1]);
    let range_index_id = *stats_view.range_index_rollups.keys().next().unwrap();
    assert_eq!(
        stats_view
            .range_index_rollups
            .get(&range_index_id)
            .unwrap()
            .coverage
            .covered_segment_ids,
        vec![1]
    );
    drop(stats_view);

    let range_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(25))),
        }],
        false,
    );
    reopened.reset_query_planning_probe_counters_for_test();
    let range_plan = reopened.explain_node_query(&range_query).unwrap();
    assert_eq!(range_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(range_plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&range_plan, vec![QueryPlanNode::PropertyRangeIndex]);
    assert_eq!(reopened.query_planning_probe_snapshot_for_test().range, 1);
    assert_eq!(
        reopened.query_node_ids(&range_query).unwrap().items,
        oracle_query_ids(&reopened, &all_ids, &range_query)
    );

    let timestamp_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(1_000),
            upper_ms: Some(1_300),
        }],
        false,
    );
    reopened.reset_query_planning_probe_counters_for_test();
    let timestamp_plan = reopened.explain_node_query(&timestamp_query).unwrap();
    assert_eq!(timestamp_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(timestamp_plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&timestamp_plan, vec![QueryPlanNode::TimestampIndex]);
    assert_eq!(
        reopened.query_planning_probe_snapshot_for_test().timestamp,
        1
    );
    assert_eq!(
        reopened.query_node_ids(&timestamp_query).unwrap().items,
        oracle_query_ids(&reopened, &all_ids, &timestamp_query)
    );

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_adaptive_cap_allows_high_confidence_range_above_default() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let selected_count =
        crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 256;
    let total_count = selected_count + 1024;
    let inputs: Vec<_> = (0..total_count)
        .map(|index| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("adaptive-range-{index}"),
            props: query_test_props(&[("score", PropValue::Int(index as i64))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(
                selected_count as i64 - 1,
            ))),
        }),
        page: PageRequest {
            limit: Some(16),
            after: None,
        },
        ..Default::default()
    };

    let expected: Vec<_> = oracle_query_ids(&engine, &all_ids, &query)
        .into_iter()
        .take(16)
        .collect();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyRangeIndex]);
    assert!(
        plan.estimated_candidates
            > Some(crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP as u64)
    );

    engine.close().unwrap();
}

#[test]
fn test_direct_read_apis_are_unchanged_with_planner_stats_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let inputs = vec![
        NodeInput {
            labels: vec!["Person".to_string()],
            key: "direct-a".to_string(),
            props: query_test_props(&[
                ("status", PropValue::String("active".to_string())),
                ("score", PropValue::Int(10)),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            labels: vec!["Person".to_string()],
            key: "direct-b".to_string(),
            props: query_test_props(&[
                ("status", PropValue::String("inactive".to_string())),
                ("score", PropValue::Int(20)),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            labels: vec!["Company".to_string()],
            key: "direct-c".to_string(),
            props: query_test_props(&[
                ("status", PropValue::String("active".to_string())),
                ("score", PropValue::Int(10)),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
    ];
    let ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    assert_eq!(
        engine
            .find_nodes("Person", "status", &PropValue::String("active".to_string()))
            .unwrap(),
        vec![ids[0]]
    );
    assert_eq!(
        engine
            .find_nodes_range(
                "Person",
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(20))),
            )
            .unwrap(),
        vec![ids[0], ids[1]]
    );
    assert_eq!(
        engine
            .find_nodes_by_time_range("Person", i64::MIN, i64::MAX)
            .unwrap(),
        vec![ids[0], ids[1]]
    );
    assert_eq!(engine.nodes_by_labels("Person").unwrap(), vec![ids[0], ids[1]]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_mixed_segment_fallback_estimates_once() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        for idx in 0..2 {
            insert_query_node(&engine, "Person",  &format!("covered-{idx}"), &[], 1.0);
        }
        engine.flush().unwrap();
        for idx in 0..3 {
            insert_query_node(&engine, "Person",  &format!("fallback-{idx}"), &[], 1.0);
        }
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 2)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.segment_count, 2);
    assert_eq!(stats_view.available_segment_stats, 1);
    assert_eq!(stats_view.unavailable_segment_stats, 1);
    assert_eq!(stats_view.node_label_count(1), 2);
    assert_eq!(stats_view.node_label_coverage.covered_segment_ids, vec![1]);
    drop(stats_view);
    {
        let (_guard, published) = reopened.runtime.published_snapshot().unwrap();
        let estimate = published.view.node_label_estimate(1).unwrap();
        assert_eq!(estimate.kind, PlannerEstimateKind::UpperBound);
        assert_eq!(estimate.known_upper_bound(), Some(5));
    }

    let query = query_ids(Some("Person"), Vec::new(), false);
    assert_eq!(reopened.query_node_ids(&query).unwrap().items.len(), 5);
    let plan = reopened.explain_node_query(&query).unwrap();
    assert_eq!(plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::NodeLabelIndex]);

    reopened.close().unwrap();
}

#[test]
fn test_planner_estimate_sort_prefers_cheaper_count_before_source_rank() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let mut candidates = vec![
            NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                1,
                1,
                "status",
                &PropValue::String("active".to_string()),
                PlannerEstimate::stats_estimated(
                    100,
                    EstimateConfidence::High,
                    StalePostingRisk::Low,
                ),
            )),
            NodePhysicalPlan::source(PlannedNodeCandidateSource::fallback_node_label_scan(
                1,
                PlannerEstimate::upper_bound(10),
            )),
        ];
        published
            .view
            .sort_physical_plans_by_selectivity(&mut candidates);
        assert_eq!(candidates[0].plan_node(), QueryPlanNode::FallbackNodeLabelScan);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_validation_and_explain_reject_label_less_scan_without_opt_in() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let query = query_ids(
        None,
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    assert!(matches!(
        engine.query_node_ids(&query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));
    assert!(matches!(
        engine.explain_node_query(&query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let key_query = NodeQuery {
        keys: vec!["alice".to_string()],
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&key_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_range_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: None,
            upper: None,
        }],
        false,
    );
    assert!(matches!(
        engine.query_node_ids(&empty_range_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_time_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: None,
            upper_ms: None,
        }],
        false,
    );
    assert!(matches!(
        engine.explain_node_query(&empty_time_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let inverted_time_query = query_ids(Some("Person"),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(200),
            upper_ms: Some(100),
        }],
        false,
    );
    assert!(engine
        .query_node_ids(&inverted_time_query)
        .unwrap()
        .items
        .is_empty());
    assert!(matches!(
        explain_input_node(&engine.explain_node_query(&inverted_time_query).unwrap()),
        QueryPlanNode::EmptyResult
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_normalization_expands_open_updated_at_bounds() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine.ensure_node_label("Person").unwrap();

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();

        let lower_open_query = query_ids(Some("Person"),
            vec![NodeFilterExpr::UpdatedAtRange {
                lower_ms: None,
                upper_ms: Some(123),
            }],
            false,
        );
        let normalized = published
            .view
            .normalize_node_query(&lower_open_query)
            .unwrap();
        match normalized.filter {
            NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                assert_eq!(lower_ms, i64::MIN);
                assert_eq!(upper_ms, 123);
            }
            _ => panic!("expected normalized updated-at range"),
        }

        let upper_open_query = query_ids(Some("Person"),
            vec![NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(456),
                upper_ms: None,
            }],
            false,
        );
        let normalized = published
            .view
            .normalize_node_query(&upper_open_query)
            .unwrap();
        match normalized.filter {
            NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                assert_eq!(lower_ms, 456);
                assert_eq!(upper_ms, i64::MAX);
            }
            _ => panic!("expected normalized updated-at range"),
        }
    }

    engine.close().unwrap();
}

#[test]
fn test_query_filter_validation_and_empty_result_without_scan_opt_in() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for filter in [
        NodeFilterExpr::And(Vec::new()),
        NodeFilterExpr::Or(Vec::new()),
        NodeFilterExpr::PropertyEquals {
            key: String::new(),
            value: PropValue::String("x".to_string()),
        },
        NodeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: Vec::new(),
        },
    ] {
        let query = NodeQuery {
            label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
            filter: Some(filter),
            ..Default::default()
        };
        assert!(matches!(
            engine.explain_node_query(&query).unwrap_err(),
            EngineError::InvalidOperation(_)
        ));
    }

    let always_false = NodeQuery {
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
        ],
        ..Default::default()
    };
    assert!(engine.query_node_ids(&always_false).unwrap().items.is_empty());
    let plan = engine.explain_node_query(&always_false).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert!(matches!(explain_input_node(&plan), QueryPlanNode::EmptyResult));

    let always_true_without_anchor = NodeQuery {
        filter: Some(NodeFilterExpr::Not(Box::new(
            always_false.filter.clone().unwrap(),
        ))),
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&always_true_without_anchor).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_filter_exists_missing_not_and_or_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let tagged_null = insert_query_node(&engine, "Person",
        "tagged-null",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tag", PropValue::Null),
        ],
        1.0,
    );
    let missing_tag = insert_query_node(&engine, "Person",
        "missing-tag",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let tagged_trial = insert_query_node(&engine, "Person",
        "tagged-trial",
        &[
            ("status", PropValue::String("trial".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("trial".to_string()),
                },
            ]),
            NodeFilterExpr::PropertyExists {
                key: "tag".to_string(),
            },
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            })),
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![tagged_null, tagged_trial]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    let missing_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyMissing {
            key: "tag".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&missing_query).unwrap().items, vec![missing_tag]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_in_dedupes_by_canonical_value_and_uses_union() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut map_value = BTreeMap::new();
    map_value.insert("x".to_string(), PropValue::Int(1));
    let map_value = PropValue::Map(map_value);
    let array_value = PropValue::Array(vec![PropValue::Int(1), PropValue::UInt(2)]);

    let null_id = insert_query_node(&engine, "Person",  "null", &[("kind", PropValue::Null)], 1.0);
    let int_id = insert_query_node(&engine, "Person",  "int", &[("kind", PropValue::Int(1))], 1.0);
    let uint_id = insert_query_node(&engine, "Person",  "uint", &[("kind", PropValue::UInt(1))], 1.0);
    let array_id = insert_query_node(&engine, "Person",  "array", &[("kind", array_value.clone())], 1.0);
    let map_id = insert_query_node(&engine, "Person",  "map", &[("kind", map_value.clone())], 1.0);
    let neg_zero_id = insert_query_node(&engine, "Person",
        "neg-zero-kind",
        &[("kind", PropValue::Float(-0.0))],
        1.0,
    );
    let pos_zero_id = insert_query_node(&engine, "Person",
        "pos-zero-kind",
        &[("kind", PropValue::Float(0.0))],
        1.0,
    );
    let _missing = insert_query_node(&engine, "Person",  "missing", &[], 1.0);

    let index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("kind").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "kind".to_string(),
            values: vec![
                PropValue::Null,
                PropValue::Null,
                PropValue::UInt(1),
                array_value.clone(),
                map_value.clone(),
                map_value.clone(),
            ],
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![null_id, int_id, uint_id, array_id, map_id]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(!plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &plan,
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    let int_only = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "kind".to_string(),
            values: vec![PropValue::Int(1)],
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&int_only).unwrap().items,
        vec![int_id, uint_id]
    );
    let int_only_plan = engine.explain_node_query(&int_only).unwrap();
    assert!(!int_only_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&int_only_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let signed_zero_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "kind".to_string(),
            values: vec![PropValue::Float(-0.0), PropValue::Float(0.0)],
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&signed_zero_query).unwrap().items,
        vec![neg_zero_id, pos_zero_id]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&signed_zero_query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_large_verify_only_in_matches_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let string_match = insert_query_node(&engine, "Person",
        "token-string-match",
        &[("token", PropValue::String("value-63".to_string()))],
        1.0,
    );
    let signed_zero_match = insert_query_node(&engine, "Person",
        "token-signed-zero-match",
        &[("token", PropValue::Float(0.0))],
        1.0,
    );
    let _nested_zero_match = insert_query_node(&engine, "Person",
        "token-nested-zero-match",
        &[(
            "token",
            PropValue::Array(vec![PropValue::Float(0.0)]),
        )],
        1.0,
    );
    let mut nested_map = BTreeMap::new();
    nested_map.insert("zero".to_string(), PropValue::Float(0.0));
    let _nested_map_zero_match = insert_query_node(&engine, "Person",
        "token-nested-map-zero-match",
        &[("token", PropValue::Map(nested_map))],
        1.0,
    );
    insert_query_node(&engine, "Person",
        "token-nan-not-match",
        &[("token", PropValue::Float(f64::NAN))],
        1.0,
    );
    insert_query_node(&engine, "Person",
        "token-miss",
        &[("token", PropValue::String("missing".to_string()))],
        1.0,
    );

    let mut values: Vec<PropValue> = (0..64)
        .map(|index| PropValue::String(format!("value-{index}")))
        .collect();
    values.push(PropValue::Float(-0.0));
    values.push(PropValue::Array(vec![PropValue::Float(-0.0)]));
    let mut nested_map_value = BTreeMap::new();
    nested_map_value.insert("zero".to_string(), PropValue::Float(-0.0));
    values.push(PropValue::Map(nested_map_value));
    values.push(PropValue::Float(f64::NAN));
    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "token".to_string(),
            values,
        }),
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![string_match, signed_zero_match]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_equality_contradictions_match_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let neg_zero = insert_query_node(&engine, "Person",
        "neg-zero",
        &[("temperature", PropValue::Float(-0.0))],
        1.0,
    );
    let pos_zero = insert_query_node(&engine, "Person",
        "pos-zero",
        &[("temperature", PropValue::Float(0.0))],
        1.0,
    );

    let query = NodeQuery {
        ids: vec![neg_zero, pos_zero],
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "temperature".to_string(),
                value: PropValue::Float(-0.0),
            },
            NodeFilterExpr::PropertyEquals {
                key: "temperature".to_string(),
                value: PropValue::Float(0.0),
            },
        ])),
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![neg_zero, pos_zero]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        vec![QueryPlanNode::ExplicitIds],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_indexed_float_signed_zero_equality_matches_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let neg_zero = insert_query_node(&engine, "Person",
        "indexed-neg-zero",
        &[("temperature", PropValue::Float(-0.0))],
        1.0,
    );
    let pos_zero = insert_query_node(&engine, "Person",
        "indexed-pos-zero",
        &[("temperature", PropValue::Float(0.0))],
        1.0,
    );
    insert_query_node(&engine, "Person",
        "indexed-one",
        &[("temperature", PropValue::Float(1.0))],
        1.0,
    );
    engine.flush().unwrap();

    let index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("temperature").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let neg_zero_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "temperature".to_string(),
            value: PropValue::Float(-0.0),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&neg_zero_query).unwrap().items,
        vec![neg_zero, pos_zero]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&neg_zero_query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let pos_zero_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "temperature".to_string(),
            value: PropValue::Float(0.0),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&pos_zero_query).unwrap().items,
        vec![neg_zero, pos_zero]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&pos_zero_query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_or_and_in_extract_complete_index_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(&engine, "Person",
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let _inactive = insert_query_node(&engine, "Person",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let or_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("trial".to_string()),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&or_query).unwrap().items, vec![active, trial]);
    let or_plan = engine.explain_node_query(&or_query).unwrap();
    assert!(!or_plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &or_plan,
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    let singleton_or_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&singleton_or_query).unwrap().items,
        vec![active]
    );
    let singleton_or_plan = engine.explain_node_query(&singleton_or_query).unwrap();
    assert!(!singleton_or_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &singleton_or_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let double_not_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Not(Box::new(NodeFilterExpr::Not(Box::new(
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ))))),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&double_not_query).unwrap().items,
        vec![active]
    );
    let double_not_plan = engine.explain_node_query(&double_not_query).unwrap();
    assert!(!double_not_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&double_not_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let in_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: vec![
                PropValue::String("active".to_string()),
                PropValue::String("trial".to_string()),
            ],
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&in_query).unwrap().items, vec![active, trial]);
    let in_plan = engine.explain_node_query(&in_query).unwrap();
    assert!(!in_plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &in_plan,
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_or_in_union_final_verification_and_pagination() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let stale_active = insert_query_node(&engine, "Person",
        "stale-active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let deleted_trial = insert_query_node(&engine, "Person",
        "deleted-trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let active = insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(&engine, "Person",
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    for index in 0..3 {
        insert_query_node(&engine, "Person",
            &format!("inactive-{index}"),
            &[("status", PropValue::String("inactive".to_string()))],
            1.0,
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let updated = engine
        .upsert_node(
            "Person",
            "stale-active",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("inactive".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated, stale_active);
    engine.delete_node(deleted_trial).unwrap();

    let or_filter = NodeFilterExpr::Or(vec![
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("trial".to_string()),
        },
    ]);
    let mut or_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(or_filter.clone()),
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    let first = engine.query_node_ids(&or_query).unwrap();
    assert_eq!(first.items, vec![active]);
    assert_eq!(first.next_cursor, Some(active));
    or_query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&or_query).unwrap();
    assert_eq!(second.items, vec![trial]);
    assert_eq!(second.next_cursor, None);

    or_query.page = PageRequest::default();
    assert_eq!(engine.query_node_ids(&or_query).unwrap().items, vec![active, trial]);
    assert_plan_input_nodes(
        &engine.explain_node_query(&or_query).unwrap(),
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    let in_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: vec![
                PropValue::String("trial".to_string()),
                PropValue::String("active".to_string()),
                PropValue::String("active".to_string()),
            ],
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&in_query).unwrap().items, vec![active, trial]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_and_of_or_intersects_range() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active_high = insert_query_node(&engine, "Person",
        "active-high",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );
    let trial_high = insert_query_node(&engine, "Person",
        "trial-high",
        &[
            ("status", PropValue::String("trial".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let _active_low = insert_query_node(&engine, "Person",
        "active-low",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(1)),
        ],
        1.0,
    );
    let _inactive_high = insert_query_node(&engine, "Person",
        "inactive-high",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(40)),
        ],
        1.0,
    );
    engine.flush().unwrap();
    let status_index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let score_index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, status_index.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score_index.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("trial".to_string()),
                },
            ]),
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![active_high, trial_high]
    );
    assert_plan_includes_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        &[
            QueryPlanNode::Union {
                inputs: vec![
                    QueryPlanNode::PropertyEqualityIndex,
                    QueryPlanNode::PropertyEqualityIndex,
                ],
            },
            QueryPlanNode::PropertyRangeIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_fallback_budget_and_empty_plan_edges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(&engine, "Person",
        "active",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(1)),
        ],
        1.0,
    );
    let scored = insert_query_node(&engine, "Person",
        "scored",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(50)),
        ],
        1.0,
    );
    for index in 0..8 {
        insert_query_node(&engine, "Person",
            &format!("filler-{index}"),
            &[
                ("status", PropValue::String(format!("v{index}"))),
                ("score", PropValue::Int(index)),
            ],
            1.0,
        );
    }
    engine.flush().unwrap();
    let status_index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status_index.index_id, SecondaryIndexState::Ready);

    let impossible = NodeQuery {
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("trial".to_string()),
            },
        ])),
        ..Default::default()
    };
    assert!(engine.query_node_ids(&impossible).unwrap().items.is_empty());
    assert_plan_input_nodes(
        &engine.explain_node_query(&impossible).unwrap(),
        vec![QueryPlanNode::EmptyResult],
    );

    let always_true_requires_anchor = NodeQuery {
        filter: Some(NodeFilterExpr::Not(Box::new(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("trial".to_string()),
            },
        ])))),
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&always_true_requires_anchor),
        Err(EngineError::InvalidOperation(_))
    ));

    let missing_index_or = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(40))),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&missing_index_or).unwrap().items,
        vec![active, scored]
    );
    let missing_plan = engine.explain_node_query(&missing_index_or).unwrap();
    assert_eq!(
        missing_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&missing_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    let budget_or = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(
            (0..=MAX_BOOLEAN_UNION_INPUTS)
                .map(|index| NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String(format!("v{index}")),
                })
                .collect(),
        )),
        ..Default::default()
    };
    let budget_plan = engine.explain_node_query(&budget_or).unwrap();
    assert_eq!(
        budget_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
            QueryPlanWarning::PlanningProbeBudgetExceeded,
        ]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_or_unknown_branch_falls_back_without_partial_union() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let indexed = insert_query_node(&engine, "Person",
        "indexed",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let missing_index = insert_query_node(&engine, "Person",
        "missing-index",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    let other = insert_query_node(&engine, "Person",  "other", &[], 1.0);
    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
            },
        ])),
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[indexed, missing_index, other], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_verify_only_uses_expected_legal_universe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let label1_inputs: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 8)
        .map(|index| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("label1-archived-{index}"),
            props: query_test_props(&[("archived", PropValue::Bool(true))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let label1_ids = engine.batch_upsert_nodes(label1_inputs).unwrap();
    let label1_archived = label1_ids[0];
    let label1_missing = insert_query_node(&engine, "Person",  "label1-missing", &[], 1.0);
    let small_missing = insert_query_node(&engine, "Company",  "small-missing", &[], 1.0);
    let small_archived = insert_query_node(&engine, "Company",
        "small-archived",
        &[("archived", PropValue::Bool(true))],
        1.0,
    );
    let active_tag = insert_query_node(&engine, "Article",
        "active-tag",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let active_missing = insert_query_node(&engine, "Article",
        "active-missing",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive_tag = insert_query_node(&engine, "Article",
        "inactive-tag",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );

    let status_index = engine
        .ensure_node_property_index("Article", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status_index.index_id, SecondaryIndexState::Ready);

    let mut huge_ids = label1_ids.clone();
    huge_ids.push(label1_missing);
    huge_ids.push(small_missing);
    huge_ids.push(small_archived);
    let label_small_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Company".to_string()], mode: LabelMatchMode::All }),
        ids: huge_ids,
        filter: Some(NodeFilterExpr::PropertyMissing {
            key: "archived".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&label_small_query).unwrap().items,
        vec![small_missing]
    );
    let label_small_plan = engine.explain_node_query(&label_small_query).unwrap();
    assert_eq!(
        label_small_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&label_small_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    let ids_small_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: vec![label1_missing, label1_archived],
        filter: Some(NodeFilterExpr::PropertyMissing {
            key: "archived".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&ids_small_query).unwrap().items,
        vec![label1_missing]
    );
    let ids_small_plan = engine.explain_node_query(&ids_small_query).unwrap();
    assert_eq!(
        ids_small_plan.warnings,
        vec![QueryPlanWarning::VerifyOnlyFilter]
    );
    assert_plan_input_nodes(&ids_small_plan, vec![QueryPlanNode::ExplicitIds]);

    let equality_plus_not_missing = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Article".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            })),
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine
            .query_node_ids(&equality_plus_not_missing)
            .unwrap()
            .items,
        vec![active_tag]
    );
    let equality_plus_not_missing_plan = engine
        .explain_node_query(&equality_plus_not_missing)
        .unwrap();
    assert_eq!(
        equality_plus_not_missing_plan.warnings,
        vec![QueryPlanWarning::VerifyOnlyFilter]
    );
    assert_plan_input_nodes(
        &equality_plus_not_missing_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let or_missing = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Article".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&or_missing).unwrap().items,
        vec![active_tag, active_missing]
    );
    let or_missing_plan = engine.explain_node_query(&or_missing).unwrap();
    assert_eq!(
        or_missing_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&or_missing_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    assert!(!engine
        .query_node_ids(&NodeQuery {
            label_filter: Some(NodeLabelFilter { labels: vec!["Article".to_string()], mode: LabelMatchMode::All }),
            ids: vec![inactive_tag],
            filter: Some(NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            }),
            ..Default::default()
        })
        .unwrap()
        .items
        .contains(&inactive_tag));

    engine.close().unwrap();
}

#[test]
fn test_query_filter_range_and_timestamp_probe_budget_overflow_is_cumulative() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let inputs: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 8)
        .map(|index| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("budget-{index}"),
            props: query_test_props(&[("score", PropValue::Int(index as i64))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();
    let score_index = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, score_index.index_id, SecondaryIndexState::Ready);
    let segment_id = engine.segments_for_test()[0].segment_id;
    let seg_dir = segment_dir(&db_path, segment_id);
    let stats_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::PlannerStats,
    );
    engine.close().unwrap();
    std::fs::remove_file(&stats_path).unwrap();
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(engine.segments_for_test()[0].planner_stats().is_none());

    // Every leaf must cover the whole label so each one individually exceeds
    // the candidate cap and runs an ID-materializing probe: selective leaves
    // with trusted upper bounds below the legal universe become bounded
    // sources directly and never touch the probe budget.
    let range_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::And(
            (-4..1)
                .map(|lower| NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(lower))),
                    upper: None,
                })
                .collect(),
        )),
        ..Default::default()
    };
    let range_plan = engine.explain_node_query(&range_query).unwrap();
    assert_eq!(
        range_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::RangeCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::PlanningProbeBudgetExceeded,
        ]
    );

    let timestamp_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::And(
            (0..5)
                .map(|lower| NodeFilterExpr::UpdatedAtRange {
                    lower_ms: Some(lower),
                    upper_ms: None,
                })
                .collect(),
        )),
        ..Default::default()
    };
    let timestamp_plan = engine.explain_node_query(&timestamp_query).unwrap();
    assert_eq!(
        timestamp_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::TimestampCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::PlanningProbeBudgetExceeded,
        ]
    );

    engine.close().unwrap();
}










// --- scan-backed node queries ---

#[test]
fn test_query_label_only_uses_label_index_path() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Person",  "b", &[], 1.0);
    let _other_label = insert_query_node(&engine, "Company",  "x", &[], 1.0);

    let query = query_ids(Some("Person"), Vec::new(), false);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, _other_label], &query)
    );

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert!(matches!(
        plan.root,
        QueryPlanNode::VerifyNodeFilter { ref input }
            if **input == QueryPlanNode::NodeLabelIndex
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_label_only_pagination_excludes_deleted_and_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let kept = insert_query_node(&engine, "Person",  "kept", &[], 1.0);
    let deleted = insert_query_node(&engine, "Person",  "deleted", &[], 1.0);
    let overwritten = insert_query_node(&engine, "Person",  "overwritten", &[], 1.0);
    let _other_label = insert_query_node(&engine, "Company",  "other", &[], 1.0);
    engine.flush().unwrap();

    engine.delete_node(deleted).unwrap();
    let overwritten_again = insert_query_node(&engine, "Person",
        "overwritten",
        &[("status", PropValue::String("new".to_string()))],
        1.0,
    );
    assert_eq!(overwritten_again, overwritten);
    let memtable = insert_query_node(&engine, "Person",  "memtable", &[], 1.0);

    let mut expected = vec![kept, overwritten, memtable];
    expected.sort_unstable();

    let mut query = query_ids(Some("Person"), Vec::new(), false);
    query.page = PageRequest {
        limit: Some(2),
        after: None,
    };

    let page1 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page1.items, expected[..2]);
    assert_eq!(page1.next_cursor, Some(expected[1]));

    query.page.after = page1.next_cursor;
    let page2 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page2.items, expected[2..]);
    assert_eq!(page2.next_cursor, None);

    engine.flush().unwrap();
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(Some("Person"), Vec::new(), false);
    assert_eq!(reopened.query_node_ids(&query).unwrap().items, expected);
    reopened.close().unwrap();
}

#[test]
fn test_query_label_only_pagination_across_multiple_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut expected = Vec::new();
    for index in 0..6 {
        expected.push(insert_query_node(&engine, "Person",
            &format!("seg-a-{index}"),
            &[],
            1.0,
        ));
    }
    engine.flush().unwrap();

    for index in 0..6 {
        expected.push(insert_query_node(&engine, "Person",
            &format!("seg-b-{index}"),
            &[],
            1.0,
        ));
    }
    engine.flush().unwrap();

    let deleted = expected[3];
    engine.delete_node(deleted).unwrap();
    let memtable = insert_query_node(&engine, "Person",  "memtable", &[], 1.0);
    expected.retain(|id| *id != deleted);
    expected.push(memtable);
    expected.sort_unstable();

    let mut query = query_ids(Some("Person"), Vec::new(), false);
    query.page = PageRequest {
        limit: Some(5),
        after: None,
    };

    let page1 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page1.items, expected[..5]);
    assert_eq!(page1.next_cursor, Some(expected[4]));

    query.page.after = page1.next_cursor;
    let page2 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page2.items, expected[5..10]);
    assert_eq!(page2.next_cursor, Some(expected[9]));

    query.page.after = page2.next_cursor;
    let page3 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page3.items, expected[10..]);
    assert_eq!(page3.next_cursor, None);

    engine.close().unwrap();
}

#[test]
fn test_query_label_universe_beats_large_explicit_ids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let filler: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 32)
        .map(|index| NodeInput {
            labels: vec!["Company".to_string()],
            key: format!("filler-{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mut all_ids = engine.batch_upsert_nodes(filler).unwrap();
    engine.flush().unwrap();

    let mut expected = Vec::new();
    for index in 0..8 {
        let id = insert_query_node(&engine, "Person",  &format!("small-{index}"), &[], 1.0);
        all_ids.push(id);
        expected.push(id);
    }
    all_ids.sort_unstable();
    expected.sort_unstable();

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: all_ids,
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::NodeLabelIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_small_explicit_ids_beat_large_label_universe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut node_ids = Vec::new();
    for index in 0..300 {
        node_ids.push(insert_query_node(&engine, "Person",
            &format!("node-{index}"),
            &[],
            1.0,
        ));
    }

    let expected = vec![node_ids[12], node_ids[223]];
    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: expected.clone(),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}

#[test]
fn test_query_explain_omits_label_scan_when_property_index_drives_execution() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(&engine, "Person",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let other_label = insert_query_node(&engine, "Company",
        "other",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[active, inactive, other_label], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_label_universe_beats_large_key_upper_bound() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for index in 0..300 {
        insert_query_node(&engine, "Company",  &format!("filler-{index}"), &[], 1.0);
    }
    engine.flush().unwrap();

    let mut expected = Vec::new();
    let mut keys = Vec::new();
    for index in 0..QUERY_RANGE_CANDIDATE_CAP + 32 {
        keys.push(format!("missing-{index}"));
    }
    for index in 0..8 {
        let key = format!("small-{index}");
        expected.push(insert_query_node(&engine, "Person",  &key, &[], 1.0));
        keys.push(key);
    }
    expected.sort_unstable();

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        keys,
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::NodeLabelIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_label_universe_verifies_large_ids_and_predicate() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let filler: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 32)
        .map(|index| NodeInput {
            labels: vec!["Company".to_string()],
            key: format!("filler-{index}"),
            props: query_test_props(&[("status", PropValue::String("keep".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mut all_ids = engine.batch_upsert_nodes(filler).unwrap();
    engine.flush().unwrap();

    let keep = insert_query_node(&engine, "Person",
        "keep",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let drop = insert_query_node(&engine, "Person",
        "drop",
        &[("status", PropValue::String("drop".to_string()))],
        1.0,
    );
    all_ids.push(keep);
    all_ids.push(drop);
    all_ids.sort_unstable();

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: all_ids,
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_label_scan_predicates_pagination_and_hydration_parity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let b = insert_query_node(&engine, "Person",
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );
    let c = insert_query_node(&engine, "Person",
        "c",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let _other_label = insert_query_node(&engine, "Company",
        "x",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );

    let mut query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: Some(PropertyRangeBound::Excluded(PropValue::Int(30))),
            },
            NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(0),
                upper_ms: None,
            },
        ],
        false,
    );
    query.page = PageRequest {
        limit: Some(1),
        after: None,
    };

    let first = engine.query_node_ids(&query).unwrap();
    assert_eq!(first.items, vec![a]);
    assert_eq!(first.next_cursor, Some(a));

    query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&query).unwrap();
    assert!(!second.items.contains(&c));
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.next_cursor, None);

    query.page = PageRequest::default();
    let ids = engine.query_node_ids(&query).unwrap();
    assert_eq!(
        ids.items,
        oracle_query_ids(&engine, &[a, b, c, _other_label], &query)
    );
    let nodes = engine.query_nodes(&query).unwrap();
    assert_eq!(
        ids.items,
        nodes.items.iter().map(|node| node.id).collect::<Vec<_>>()
    );

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.kind, QueryPlanKind::NodeQuery);
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert!(matches!(
        explain_input_node(&plan),
        QueryPlanNode::TimestampIndex
    ));

    engine.close().unwrap();
}

#[test]
fn node_query_ids_property_filters_use_selected_field_verification() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "Person",
        "selected-a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
            ("flag", PropValue::Bool(true)),
        ],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "Person",
        "selected-b",
        &[
            ("status", PropValue::String("pending".to_string())),
            ("score", PropValue::Int(20)),
            ("optional", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        "Person",
        "selected-c",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(30)),
            ("flag", PropValue::Bool(true)),
        ],
        1.0,
    );
    let d = insert_query_node(
        &engine,
        "Person",
        "selected-d",
        &[("score", PropValue::Int(40))],
        1.0,
    );
    let other = insert_query_node(
        &engine,
        "Company",
        "selected-company",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let candidates = [a, b, c, d, other];

    let filter_cases = vec![
        (
            "equals",
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ),
        (
            "in",
            NodeFilterExpr::PropertyIn {
                key: "status".to_string(),
                values: vec![
                    PropValue::String("active".to_string()),
                    PropValue::String("pending".to_string()),
                ],
            },
        ),
        (
            "range",
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(15))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(30))),
            },
        ),
        (
            "exists",
            NodeFilterExpr::PropertyExists {
                key: "flag".to_string(),
            },
        ),
        (
            "missing",
            NodeFilterExpr::PropertyMissing {
                key: "optional".to_string(),
            },
        ),
        (
            "and",
            NodeFilterExpr::And(vec![
                NodeFilterExpr::PropertyIn {
                    key: "status".to_string(),
                    values: vec![
                        PropValue::String("active".to_string()),
                        PropValue::String("pending".to_string()),
                    ],
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                    upper: Some(PropertyRangeBound::Excluded(PropValue::Int(20))),
                },
            ]),
        ),
        (
            "or",
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("pending".to_string()),
                },
                NodeFilterExpr::PropertyMissing {
                    key: "status".to_string(),
                },
            ]),
        ),
        (
            "not",
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            })),
        ),
    ];

    for (name, filter) in filter_cases {
        let query = NodeQuery {
            label_filter: Some(node_label_filter(&["Person"], LabelMatchMode::All)),
            filter: Some(filter),
            ..Default::default()
        };
        let expected = oracle_query_ids(&engine, &candidates, &query);
        engine.reset_query_execution_counters_for_test();
        let result = engine.query_node_ids(&query).unwrap();
        let counters = engine.query_execution_counter_snapshot_for_test();

        assert_eq!(result.items, expected, "filter case {name}");
        assert_eq!(
            counters.node_record_hydration_reads, 0,
            "query_node_ids hydrated full nodes for {name}"
        );
        assert_eq!(
            counters.final_verifier_record_reads, 0,
            "query_node_ids used the old full-record verifier for {name}"
        );
        assert!(
            counters.node_visibility_meta_reads > 0,
            "selected verification should read node metadata for {name}"
        );
    }

    engine.close().unwrap();
}

#[test]
fn node_query_nodes_hydrates_only_final_selected_field_page() {
    let (_dir, engine) = query_test_engine();
    let first = insert_query_node(
        &engine,
        "Person",
        "selected-page-first",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let _reject = insert_query_node(
        &engine,
        "Person",
        "selected-page-reject",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let second = insert_query_node(
        &engine,
        "Person",
        "selected-page-second",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let mut query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    query.page = PageRequest {
        limit: Some(1),
        after: None,
    };

    engine.reset_query_execution_counters_for_test();
    let page = engine.query_nodes(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(page.items.iter().map(|node| node.id).collect::<Vec<_>>(), vec![first]);
    assert_eq!(page.next_cursor, Some(first));
    assert_eq!(counters.node_record_hydration_reads, 1);
    assert_eq!(counters.final_verifier_record_reads, 0);

    query.page.after = page.next_cursor;
    let second_page = engine.query_node_ids(&query).unwrap();
    assert_eq!(second_page.items, vec![second]);
    assert_eq!(second_page.next_cursor, None);

    engine.close().unwrap();
}

#[test]
fn node_query_selected_field_verification_uses_segments_after_reopen_and_compaction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let segment_keep;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        seed_query_test_catalog(&engine);
        segment_keep = insert_query_node(
            &engine,
            "Person",
            "selected-reopen-keep",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        insert_query_node(
            &engine,
            "Person",
            "selected-reopen-drop",
            &[("status", PropValue::String("inactive".to_string()))],
            1.0,
        );
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(
        Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    engine.reset_query_execution_counters_for_test();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![segment_keep]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);

    engine.reset_query_execution_counters_for_test();
    let nodes = engine.query_nodes(&query).unwrap();
    assert_eq!(nodes.items.iter().map(|node| node.id).collect::<Vec<_>>(), vec![segment_keep]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 1);
    assert_eq!(counters.final_verifier_record_reads, 0);

    let compact_keep = insert_query_node(
        &engine,
        "Person",
        "selected-compact-keep",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "selected-compact-drop",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    engine.compact().unwrap().unwrap();

    let expected = vec![segment_keep, compact_keep];
    engine.reset_query_execution_counters_for_test();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);

    let mut page_query = query.clone();
    page_query.page = PageRequest {
        limit: Some(1),
        after: None,
    };
    engine.reset_query_execution_counters_for_test();
    let page = engine.query_nodes(&page_query).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.next_cursor, Some(segment_keep));
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 1);
    assert_eq!(counters.final_verifier_record_reads, 0);

    engine.close().unwrap();
}

#[test]
fn node_query_property_index_candidates_verify_key_from_selected_fields() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "Person",
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let bob = insert_query_node(
        &engine,
        "Person",
        "bob",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    let person_id = engine.get_node_label_id("Person").unwrap().unwrap();

    let query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person"], LabelMatchMode::All)),
        keys: vec!["bob".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_node_query(&query).unwrap();
    let cap_context = published.view.query_cap_context(&normalized).unwrap();
    let planned = PlannedNodeQuery {
        driver: NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
            person_id,
            info.index_id,
            "status",
            &PropValue::String("active".to_string()),
            PlannerEstimate::upper_bound(2),
        )),
        cap_context,
        legal_universe_fallback: None,
        warnings: Vec::new(),
        followups: Vec::new(),
    };
    let policy_cutoffs = published.view.query_policy_cutoffs();

    engine.reset_query_execution_counters_for_test();
    let (page, followups) = published
        .view
        .query_node_page_planned(&normalized, planned, false, policy_cutoffs.as_ref())
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert!(followups.is_empty());
    assert_eq!(page.ids, vec![bob]);
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);

    drop(published);
    drop(_guard);
    engine.close().unwrap();
}

#[test]
fn node_query_metadata_prune_tombstone_and_timestamp_stay_metadata_only() {
    let (_dir, engine) = query_test_engine();
    let keep = insert_query_node(&engine, "Person", "meta-keep", &[], 1.0);
    let old = insert_query_node(&engine, "Person", "meta-old", &[], 1.0);
    let low_weight = insert_query_node(&engine, "Person", "meta-low-weight", &[], 0.1);
    let deleted = insert_query_node(&engine, "Person", "meta-deleted", &[], 1.0);
    set_query_node_updated_at(&engine, keep, 2_000);
    set_query_node_updated_at(&engine, old, 500);
    set_query_node_updated_at(&engine, low_weight, 2_000);
    set_query_node_updated_at(&engine, deleted, 2_000);
    engine.flush().unwrap();
    engine.delete_node(deleted).unwrap();
    engine
        .set_prune_policy(
            "node-selected-low-weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("Person".to_string()),
            },
        )
        .unwrap();

    let query = NodeQuery {
        label_filter: Some(node_label_filter(&["Person"], LabelMatchMode::All)),
        ids: vec![keep, old, low_weight, deleted],
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(1_000),
            upper_ms: Some(3_000),
        }],
        ..Default::default()
    };

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_node_ids(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(result.items, vec![keep]);
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);

    engine.close().unwrap();
}

// --- anchor semantics ---

#[test]
fn test_query_multi_anchor_and_semantics_and_conflicts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = insert_query_node(&engine, "Person",
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let bob = insert_query_node(&engine, "Person",
        "bob",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let matched = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: vec![alice, bob],
        keys: vec!["alice".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&matched).unwrap().items,
        oracle_query_ids(&engine, &[alice, bob], &matched)
    );

    let conflict = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: vec![bob],
        keys: vec!["alice".to_string()],
        ..Default::default()
    };
    assert!(engine.query_node_ids(&conflict).unwrap().items.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_query_key_lookup_anchor_normalization_and_source_choice() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = insert_query_node(&engine, "Person",
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let bob = insert_query_node(&engine, "Person",
        "bob",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let carol = insert_query_node(&engine, "Person",
        "carol",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let other_label = insert_query_node(&engine, "Company",
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let key_only = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        keys: vec!["bob".to_string(), "alice".to_string(), "alice".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    let key_only_expected = oracle_query_ids(&engine, &[alice, bob, carol, other_label], &key_only);
    engine.reset_query_execution_counters_for_test();
    assert_eq!(engine.query_node_ids(&key_only).unwrap().items, key_only_expected);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);
    let key_only_plan = engine.explain_node_query(&key_only).unwrap();
    assert_eq!(key_only_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&key_only_plan, vec![QueryPlanNode::KeyLookup]);

    let key_preferred = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: vec![alice, bob, carol],
        keys: vec!["bob".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    let key_preferred_expected =
        oracle_query_ids(&engine, &[alice, bob, carol, other_label], &key_preferred);
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_node_ids(&key_preferred).unwrap().items,
        key_preferred_expected
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);
    let key_preferred_plan = engine.explain_node_query(&key_preferred).unwrap();
    assert_eq!(key_preferred_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&key_preferred_plan, vec![QueryPlanNode::KeyLookup]);

    engine.close().unwrap();
}

// --- indexed candidate-source planning ---

#[test]
fn test_query_intersects_ready_equality_indexes_against_oracle() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
        ],
        1.0,
    );
    let b = insert_query_node(&engine, "Person",
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("silver".to_string())),
        ],
        1.0,
    );
    let c = insert_query_node(&engine, "Person",
        "c",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tier", PropValue::String("gold".to_string())),
        ],
        1.0,
    );
    let d = insert_query_node(&engine, "Person",
        "d",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
        ],
        1.0,
    );

    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let tier = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("tier").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, tier.index_id, SecondaryIndexState::Ready);

    let query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tier".to_string(),
                value: PropValue::String("gold".to_string()),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(
        &plan,
        vec![
            QueryPlanNode::PropertyEqualityIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_intersects_equality_and_range_indexes_against_oracle() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let b = insert_query_node(&engine, "Person",
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );
    let c = insert_query_node(&engine, "Person",
        "c",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let d = insert_query_node(&engine, "Person",
        "d",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(40)),
        ],
        1.0,
    );

    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(20))),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_includes_input_nodes(
        &plan,
        &[
            QueryPlanNode::PropertyRangeIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_intersects_equality_equality_and_range_indexes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let b = insert_query_node(&engine, "Person",
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let c = insert_query_node(&engine, "Person",
        "c",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("silver".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let d = insert_query_node(&engine, "Person",
        "d",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tier", PropValue::String("gold".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );

    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let tier = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("tier").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, tier.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tier".to_string(),
                value: PropValue::String("gold".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(20))),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(
        &plan,
        vec![
            QueryPlanNode::PropertyRangeIndex,
            QueryPlanNode::PropertyEqualityIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_intersects_timestamp_and_property_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let b = insert_query_node(&engine, "Person",
        "b",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let c = insert_query_node(&engine, "Person",
        "c",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let d = insert_query_node(&engine, "Person",
        "d",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    set_query_node_updated_at(&engine, a, 1_000);
    set_query_node_updated_at(&engine, b, 2_000);
    set_query_node_updated_at(&engine, c, 3_000);
    set_query_node_updated_at(&engine, d, 2_500);

    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(1_500),
                upper_ms: Some(2_500),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_includes_input_nodes(
        &plan,
        &[
            QueryPlanNode::TimestampIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_ready_index_sources_match_oracle_across_storage_states() {
    fn lifecycle_query() -> NodeQuery {
        query_ids(Some("Person"),
            vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                    upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
                },
                NodeFilterExpr::UpdatedAtRange {
                    lower_ms: Some(i64::MIN),
                    upper_ms: Some(i64::MAX),
                },
            ],
            false,
        )
    }

    fn insert_lifecycle_segment_nodes(engine: &DatabaseEngine) -> Vec<u64> {
        vec![
            insert_query_node(engine, "Person",
                "a",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(10)),
                ],
                1.0,
            ),
            insert_query_node(engine, "Person",
                "b",
                &[
                    ("status", PropValue::String("inactive".to_string())),
                    ("score", PropValue::Int(10)),
                ],
                1.0,
            ),
            insert_query_node(engine, "Person",
                "c",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(30)),
                ],
                1.0,
            ),
            insert_query_node(engine, "Person",
                "d",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(30)),
                ],
                1.0,
            ),
        ]
    }

    fn insert_lifecycle_active_nodes(engine: &DatabaseEngine) -> Vec<u64> {
        vec![
            insert_query_node(engine, "Person",
                "e",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(12)),
                ],
                1.0,
            ),
            insert_query_node(engine, "Person",
                "f",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(50)),
                ],
                1.0,
            ),
        ]
    }

    fn ensure_lifecycle_indexes(engine: &DatabaseEngine) {
        let status = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let score = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
    }

    fn assert_tiny_lifecycle_query_matches_oracle(
        engine: &DatabaseEngine,
        all_ids: &[u64],
        query: &NodeQuery,
    ) {
        assert_eq!(
            engine.query_node_ids(query).unwrap().items,
            oracle_query_ids(engine, all_ids, query)
        );
        let plan = engine.explain_node_query(query).unwrap();
        assert_eq!(plan.warnings, vec![QueryPlanWarning::UsingFallbackScan]);
        assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackNodeLabelScan]);
    }

    let dir = TempDir::new().unwrap();
    let query = lifecycle_query();

    {
        let db_path = dir.path().join("memtable-only");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_matches_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("mixed");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        engine.flush().unwrap();
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_matches_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("compacted-reopened");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        engine.flush().unwrap();
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        engine.flush().unwrap();
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_matches_oracle(&engine, &all_ids, &query);
        engine.compact().unwrap().unwrap();
        assert_tiny_lifecycle_query_matches_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();

        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_tiny_lifecycle_query_matches_oracle(&reopened, &all_ids, &query);
        reopened.close().unwrap();
    }
}

#[test]
fn test_query_selective_ready_indexes_match_oracle_across_storage_states() {
    fn selective_query() -> NodeQuery {
        query_ids(Some("Person"),
            vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("target".to_string()),
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                    upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
                },
                NodeFilterExpr::UpdatedAtRange {
                    lower_ms: Some(1_000),
                    upper_ms: Some(1_010),
                },
            ],
            false,
        )
    }

    fn ensure_selective_indexes(engine: &DatabaseEngine) {
        let status = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let score = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
        wait_for_published_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_published_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
    }

    fn insert_selective_nodes(engine: &DatabaseEngine, start: usize, count: usize) -> Vec<u64> {
        let mut ids = Vec::with_capacity(count);
        for index in start..start + count {
            let selected = index % 64 == 0;
            let node_id = insert_query_node(engine, "Person",
                &format!("selective-{index}"),
                &[
                    (
                        "status",
                        PropValue::String(if selected { "target" } else { "other" }.to_string()),
                    ),
                    (
                        "score",
                        PropValue::Int(if selected { 10 } else { 1_000 + index as i64 }),
                    ),
                ],
                1.0,
            );
            set_query_node_updated_at(
                engine,
                node_id,
                if selected { 1_005 } else { 10_000 + index as i64 },
            );
            ids.push(node_id);
        }
        ids
    }

    fn assert_selective_indexes_match_oracle(
        engine: &DatabaseEngine,
        all_ids: &[u64],
        query: &NodeQuery,
    ) {
        assert_eq!(
            engine.query_node_ids(query).unwrap().items,
            oracle_query_ids(engine, all_ids, query)
        );
        let plan = engine.explain_node_query(query).unwrap();
        assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
        assert_plan_includes_input_nodes(
            &plan,
            &[
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyRangeIndex,
                QueryPlanNode::TimestampIndex,
            ],
        );
    }

    let dir = TempDir::new().unwrap();
    let query = selective_query();

    {
        let db_path = dir.path().join("memtable-only-selective");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        ensure_selective_indexes(&engine);
        let all_ids = insert_selective_nodes(&engine, 0, 512);
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("mixed-selective");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        ensure_selective_indexes(&engine);
        let mut all_ids = insert_selective_nodes(&engine, 0, 256);
        engine.flush().unwrap();
        all_ids.extend(insert_selective_nodes(&engine, 256, 256));
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("compacted-reopened-selective");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        ensure_selective_indexes(&engine);
        let mut all_ids = insert_selective_nodes(&engine, 0, 256);
        engine.flush().unwrap();
        all_ids.extend(insert_selective_nodes(&engine, 256, 256));
        engine.flush().unwrap();
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.compact().unwrap().unwrap();
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();

        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_selective_indexes_match_oracle(&reopened, &all_ids, &query);
        reopened.close().unwrap();
    }
}

#[test]
fn test_query_bounded_range_uses_index_and_broad_sources_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(QUERY_RANGE_CANDIDATE_CAP + 1);
    for i in 0..=QUERY_RANGE_CANDIDATE_CAP {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n{i}"),
            props: query_test_props(&[
                ("score", PropValue::Int(i as i64)),
                (
                    "status",
                    PropValue::String(if i == 0 { "needle" } else { "other" }.to_string()),
                ),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();
    let score = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    let status = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let range_lower = PropertyRangeBound::Included(PropValue::Int(0));
        let range_upper =
            PropertyRangeBound::Included(PropValue::Int(QUERY_RANGE_CANDIDATE_CAP as i64));
        let (range_candidates, followup) = published
            .view
            .ready_range_candidate_ids(
                score.index_id,
                Some(&range_lower),
                Some(&range_upper),
                QUERY_RANGE_CANDIDATE_CAP + 1,
            )
            .unwrap();
        assert!(followup.is_none());
        assert_eq!(
            range_candidates.unwrap().len(),
            QUERY_RANGE_CANDIDATE_CAP + 1
        );

        let timestamp_candidates = published
            .view
            .timestamp_candidate_ids(1, i64::MIN, i64::MAX, QUERY_RANGE_CANDIDATE_CAP + 1)
            .unwrap();
        assert_eq!(timestamp_candidates.len(), QUERY_RANGE_CANDIDATE_CAP + 1);
    }

    let bounded = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(12))),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&bounded).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &bounded)
    );
    let bounded_plan = engine.explain_node_query(&bounded).unwrap();
    assert_eq!(bounded_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&bounded_plan, vec![QueryPlanNode::PropertyRangeIndex]);

    let broad_range = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(
                QUERY_RANGE_CANDIDATE_CAP as i64,
            ))),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&broad_range).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &broad_range)
    );
    let broad_range_plan = engine.explain_node_query(&broad_range).unwrap();
    assert_eq!(
        broad_range_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::RangeCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&broad_range_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    let broad_timestamp = query_ids(Some("Person"),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(i64::MIN),
            upper_ms: Some(i64::MAX),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&broad_timestamp).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &broad_timestamp)
    );
    let broad_timestamp_plan = engine.explain_node_query(&broad_timestamp).unwrap();
    assert_eq!(
        broad_timestamp_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::TimestampCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(
        &broad_timestamp_plan,
        vec![QueryPlanNode::FallbackNodeLabelScan],
    );

    let broad_or = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("needle".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(
                    QUERY_RANGE_CANDIDATE_CAP as i64,
                ))),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&broad_or).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &broad_or)
    );
    let broad_or_plan = engine.explain_node_query(&broad_or).unwrap();
    assert_eq!(
        broad_or_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::RangeCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&broad_or_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_missing_building_and_failed_indexes_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let b = insert_query_node(&engine, "Person",
        "b",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b], &query)
    );
    let missing_plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        missing_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&missing_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);

    let (build_ready_rx, build_release_tx) = engine.set_secondary_index_build_pause();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    build_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let building_plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        building_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&building_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);
    build_release_tx.send(()).unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    engine.shutdown_secondary_index_worker();
    engine
        .with_runtime_manifest_write(|manifest| {
            let entry = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == info.index_id)
                .unwrap();
            entry.state = SecondaryIndexState::Failed;
            entry.last_error = Some("forced failure".to_string());
            Ok(())
        })
        .unwrap();
    engine.rebuild_secondary_index_catalog().unwrap();
    let failed_plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        failed_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&failed_plan, vec![QueryPlanNode::FallbackNodeLabelScan]);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_query_ready_sidecar_removed_after_open_remains_usable_until_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(&engine, "Person",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    let planned;
    let normalized;
    let policy_cutoffs;
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        normalized = published.view.normalize_node_query(&query).unwrap();
        planned = published.view.plan_normalized_node_query(&normalized).unwrap();
        policy_cutoffs = published.view.query_policy_cutoffs();
    }
    assert_plan_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );
    assert!(engine.declared_index_runtime_coverage_len_for_test() > 0);

    let seg_dir = segment_dir(&db_path, engine.segments_for_test()[0].segment_id);
    let sidecar_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: info.index_id,
        },
    );
    std::fs::remove_file(&sidecar_path).unwrap();

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert_eq!(page.ids, vec![active]);
        assert!(followups.is_empty());
    }

    assert_plan_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let segment_id = engine.segments_for_test()[0].segment_id;
    engine
        .reopen_segment_reader_and_rebuild_sources_for_test(segment_id)
        .unwrap();
    let explain = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        explain.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[active, inactive], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_query_explicit_anchor_does_not_scan_ready_property_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let segment_id = engine.segments_for_test()[0].segment_id;
    let seg_dir = segment_dir(&db_path, segment_id);
    let sidecar_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: info.index_id,
        },
    );
    corrupt_planner_stats_for_segment(&db_path, segment_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    engine
        .reopen_segment_reader_and_rebuild_sources_for_test(segment_id)
        .unwrap();

    let mut query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    query.ids = vec![active];

    let (_followup_ready_rx, followup_release_tx) = engine.set_runtime_publish_pause();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![active]);
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 0);
    followup_release_tx.send(()).unwrap();

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}

#[test]
fn test_query_pagination_does_not_skip_after_rejected_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let reject_first = insert_query_node(&engine, "Person",
        "reject-first",
        &[("score", PropValue::Int(1))],
        1.0,
    );
    let accept_first = insert_query_node(&engine, "Person",
        "accept-first",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    let reject_second = insert_query_node(&engine, "Person",
        "reject-second",
        &[("score", PropValue::Int(2))],
        1.0,
    );
    let accept_second = insert_query_node(&engine, "Person",
        "accept-second",
        &[("score", PropValue::Int(20))],
        1.0,
    );

    let mut query = NodeQuery {
        ids: vec![reject_first, accept_first, reject_second, accept_second],
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: None,
        }],
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    let first = engine.query_node_ids(&query).unwrap();
    assert_eq!(first.items, vec![accept_first]);
    assert_eq!(first.next_cursor, Some(accept_first));

    query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&query).unwrap();
    assert_eq!(second.items, vec![accept_second]);
    assert_eq!(second.next_cursor, None);

    query.page.after = Some(accept_second);
    let third = engine.query_node_ids(&query).unwrap();
    assert!(third.items.is_empty());
    assert!(third.next_cursor.is_none());

    engine.close().unwrap();
}

// --- full-scan opt-in ---

#[test]
fn test_query_explicit_full_scan_opt_in_and_explain_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",
        "a",
        &[("tenant", PropValue::String("t1".to_string()))],
        1.0,
    );
    let b = insert_query_node(&engine, "Company",
        "b",
        &[("tenant", PropValue::String("t1".to_string()))],
        1.0,
    );
    let _c = insert_query_node(&engine, "Article",
        "c",
        &[("tenant", PropValue::String("t2".to_string()))],
        1.0,
    );

    let query = query_ids(
        None,
        vec![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("t1".to_string()),
        }],
        true,
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, _c], &query)
    );

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::FullScanExplicitlyAllowed,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert!(matches!(
        plan.root,
        QueryPlanNode::VerifyNodeFilter { .. }
    ));

    engine.close().unwrap();
}

// --- visibility matrix ---

#[test]
fn test_query_scan_parity_after_flush_reopen_overwrite_delete_and_prune() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let keep;
    let deleted;
    let low;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        keep = insert_query_node(&engine, "Person",
            "keep",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        deleted = insert_query_node(&engine, "Person",
            "delete",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        low = insert_query_node(&engine, "Person",
            "low",
            &[("status", PropValue::String("active".to_string()))],
            0.1,
        );
        engine.flush().unwrap();

        insert_query_node(&engine, "Person",
            "keep",
            &[("status", PropValue::String("inactive".to_string()))],
            1.0,
        );
        insert_query_node(&engine, "Person",
            "keep",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        engine.delete_node(deleted).unwrap();
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

        let query = query_ids(Some("Person"),
            vec![NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }],
            false,
        );
        assert_eq!(
            engine.query_node_ids(&query).unwrap().items,
            oracle_query_ids(&engine, &[keep, deleted, low], &query)
        );
        assert!(engine.get_node(low).unwrap().is_none());
        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    assert_eq!(
        reopened.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&reopened, &[keep, deleted, low], &query)
    );
    reopened.close().unwrap();
}

















































#[test]
fn test_query_broad_index_warning_priority_and_selective_source_choice() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(QUERY_RANGE_CANDIDATE_CAP + 1);
    for index in 0..=QUERY_RANGE_CANDIDATE_CAP {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[
                ("status", PropValue::String("inactive".to_string())),
                (
                    "tenant",
                    PropValue::String(if index < 8 { "tiny" } else { "other" }.to_string()),
                ),
                (
                    "cohort",
                    PropValue::String(if index < 512 { "broad" } else { "other" }.to_string()),
                ),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();

    for key in ["status", "tenant", "cohort"] {
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: (key).to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    }

    let cap_query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("tiny".to_string()),
            },
        ],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&cap_query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &cap_query)
    );
    let cap_plan = engine.explain_node_query(&cap_query).unwrap();
    assert!(cap_plan
        .warnings
        .contains(&QueryPlanWarning::CandidateCapExceeded));
    assert_plan_input_nodes(&cap_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let broad_skip_query = query_ids(Some("Person"),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "cohort".to_string(),
                value: PropValue::String("broad".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("tiny".to_string()),
            },
        ],
        false,
    );
    let broad_skip_plan = engine.explain_node_query(&broad_skip_query).unwrap();
    assert_eq!(
        broad_skip_plan.warnings,
        vec![
            QueryPlanWarning::IndexSkippedAsBroad,
            QueryPlanWarning::VerifyOnlyFilter
        ]
    );
    assert_plan_input_nodes(
        &broad_skip_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_large_explicit_ids_become_membership_check_for_cheaper_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(5_000);
    for index in 0..5_000 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[(
                "tenant",
                PropValue::String(if index < 12 { "tiny" } else { "other" }.to_string()),
            )]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("tenant").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        ids: all_ids.clone(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("tiny".to_string()),
        }],
        ..Default::default()
    };

    let capped_query = NodeQuery {
        ids: all_ids[..QUERY_RANGE_CANDIDATE_CAP].to_vec(),
        ..query.clone()
    };
    assert_eq!(
        engine.query_node_ids(&capped_query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &capped_query)
    );
    let capped_plan = engine.explain_node_query(&capped_query).unwrap();
    assert_eq!(capped_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&capped_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_large_keys_become_membership_check_for_cheaper_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(50);
    for index in 0..50 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[(
                "tenant",
                PropValue::String(if index < 3 { "tiny" } else { "other" }.to_string()),
            )]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("tenant").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut keys: Vec<String> = (0..5_000)
        .map(|index| format!("missing-{index}"))
        .collect();
    keys.push("n-0".to_string());
    let query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        keys,
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("tiny".to_string()),
        }],
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_full_scan_pagination_proves_extra_match_and_skips_tombstone() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let first = insert_query_node(&engine, "Person",
        "first",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let deleted = insert_query_node(&engine, "Person",
        "deleted",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let last = insert_query_node(&engine, "Person",
        "last",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    engine.delete_node(deleted).unwrap();

    let mut query = query_ids(
        None,
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        }],
        true,
    );
    query.page = PageRequest {
        limit: Some(1),
        after: None,
    };

    let page1 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page1.items, vec![first]);
    assert_eq!(page1.next_cursor, Some(first));

    query.page.after = page1.next_cursor;
    let page2 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page2.items, vec![last]);
    assert_eq!(page2.next_cursor, None);

    engine.close().unwrap();
}

#[test]
fn test_query_node_page_planned_preserves_planning_followups_for_core_outcomes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let keep = insert_query_node(&engine, "Person",
        "planned-followup-keep",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(&engine, "Person",
        "planned-followup-skip",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    let person_id = engine.get_node_label_id("Person").unwrap().unwrap();

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_node_query(&query).unwrap();
    let cap_context = published.view.query_cap_context(&normalized).unwrap();
    let policy_cutoffs = published.view.query_policy_cutoffs();

    let empty_plan = PlannedNodeQuery {
        driver: NodePhysicalPlan::Empty,
        cap_context,
        legal_universe_fallback: None,
        warnings: Vec::new(),
        followups: vec![test_equality_read_followup(info.index_id)],
    };
    let (page, followups) = published
        .view
        .query_node_page_planned(&normalized, empty_plan, false, policy_cutoffs.as_ref())
        .unwrap();
    assert!(page.ids.is_empty());
    assert_eq!(followups.len(), 1);

    let source_plan = PlannedNodeQuery {
        driver: NodePhysicalPlan::source(PlannedNodeCandidateSource::fallback_node_label_scan(
            person_id,
            PlannerEstimate::upper_bound(2),
        )),
        cap_context,
        legal_universe_fallback: None,
        warnings: Vec::new(),
        followups: vec![test_equality_read_followup(info.index_id)],
    };
    let (page, followups) = published
        .view
        .query_node_page_planned(&normalized, source_plan, false, policy_cutoffs.as_ref())
        .unwrap();
    assert_eq!(page.ids, vec![keep]);
    assert_eq!(followups.len(), 1);

    let materialized_plan = PlannedNodeQuery {
        driver: NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
            person_id,
            info.index_id,
            "status",
            &PropValue::String("active".to_string()),
            PlannerEstimate::upper_bound(1),
        )),
        cap_context,
        legal_universe_fallback: None,
        warnings: Vec::new(),
        followups: vec![test_equality_read_followup(info.index_id)],
    };
    let (page, followups) = published
        .view
        .query_node_page_planned(
            &normalized,
            materialized_plan,
            false,
            policy_cutoffs.as_ref(),
        )
        .unwrap();
    assert_eq!(page.ids, vec![keep]);
    assert_eq!(followups.len(), 1);

    drop(published);
    drop(_guard);
    engine.close().unwrap();
}

#[test]
fn test_query_unknown_selected_index_source_falls_back_when_execution_cap_exceeded() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(QUERY_RANGE_CANDIDATE_CAP + 1);
    for index in 0..=QUERY_RANGE_CANDIDATE_CAP {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(inputs).unwrap();
    let active = insert_query_node(&engine, "Person",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let public_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("inactive".to_string()),
        }],
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published
            .view
            .normalize_node_query(&public_query)
            .unwrap();
        let cap_context = published.view.query_cap_context(&normalized).unwrap();
        let planned = PlannedNodeQuery {
            driver: NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                    1,
                    info.index_id,
                    "status",
                    &PropValue::String("inactive".to_string()),
                    PlannerEstimate::unknown(),
            )),
            cap_context,
            legal_universe_fallback: None,
            warnings: Vec::new(),
            followups: vec![test_equality_read_followup(info.index_id)],
        };
        let policy_cutoffs = published.view.query_policy_cutoffs();
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert_eq!(followups.len(), 1);
        assert_eq!(page.ids.len(), 1);
        assert_eq!(page.next_cursor, page.ids.last().copied());

        let union_query = NodeQuery {
            label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
            filter: Some(NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("inactive".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
            ])),
            page: PageRequest {
                limit: Some(1),
                after: None,
            },
            ..Default::default()
        };
        let normalized = published
            .view
            .normalize_node_query(&union_query)
            .unwrap();
        let cap_context = published.view.query_cap_context(&normalized).unwrap();
        let planned = PlannedNodeQuery {
            driver: NodePhysicalPlan::union(vec![
                NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                    1,
                    info.index_id,
                    "status",
                    &PropValue::String("inactive".to_string()),
                    PlannerEstimate::unknown(),
                )),
                NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                    1,
                    info.index_id,
                    "status",
                    &PropValue::String("active".to_string()),
                    PlannerEstimate::upper_bound(1),
                )),
            ]),
            cap_context,
            legal_universe_fallback: None,
            warnings: Vec::new(),
            followups: Vec::new(),
        };
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert!(followups.is_empty());
        assert_eq!(page.ids, vec![ids[0]]);
        assert_eq!(page.next_cursor, Some(ids[0]));
        assert_ne!(page.ids, vec![active]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_limited_equality_read_skips_shadowed_ids_before_cap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(3);
    for index in 0..3 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(inputs).unwrap();
    let surviving_old_id = *ids.last().unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    for (index, id) in ids.iter().enumerate().take(2) {
        let updated_id = engine
            .upsert_node(
                "Person",
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, *id);
    }

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (candidate_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited(
                info.index_id,
                "status",
                &PropValue::String("old".to_string()),
                Some(2),
            )
            .unwrap();

        assert!(followup.is_none());
        assert_eq!(candidate_ids.unwrap(), vec![surviving_old_id]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_limited_equality_read_skips_newer_segment_shadowed_ids_before_cap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(3);
    for index in 0..3 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(inputs).unwrap();
    let surviving_old_id = *ids.last().unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    for (index, id) in ids.iter().enumerate().take(2) {
        let updated_id = engine
            .upsert_node(
                "Person",
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, *id);
    }
    engine.flush().unwrap();

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (candidate_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited(
                info.index_id,
                "status",
                &PropValue::String("old".to_string()),
                Some(2),
            )
            .unwrap();

        assert!(followup.is_none());
        assert_eq!(candidate_ids.unwrap(), vec![surviving_old_id]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_limited_equality_read_enforces_raw_posting_cap_for_stale_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut inputs = Vec::new();
    for index in 0..24 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(inputs).unwrap();
    let surviving_old_id = *ids.last().unwrap();
    engine.flush().unwrap();

    for (index, id) in ids.iter().copied().enumerate().take(23) {
        let updated_id = engine
            .upsert_node(
                "Person",
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, id);
    }

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (capped_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited_by_raw_postings(
                info.index_id,
                &PropValue::String("old".to_string()),
                8,
            )
            .unwrap();

        assert!(followup.is_none());
        assert!(capped_ids.is_none());

        let (uncapped_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited(
                info.index_id,
                "status",
                &PropValue::String("old".to_string()),
                Some(9),
            )
            .unwrap();

        assert!(followup.is_none());
        assert_eq!(uncapped_ids.unwrap(), vec![surviving_old_id]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_stats_backed_equality_materialization_uses_raw_ids_only() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut inputs = Vec::new();
    for index in 0..24 {
        inputs.push(NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(inputs).unwrap();
    let surviving_old_id = *all_ids.last().unwrap();
    engine.flush().unwrap();

    for (index, id) in all_ids.iter().copied().enumerate().take(23) {
        let updated_id = engine
            .upsert_node(
                "Person",
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, id);
    }

    let query = query_ids(Some("Person"),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("old".to_string()),
        }],
        false,
    );

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_node_query(&query).unwrap();
        let planned = published.view.plan_normalized_node_query(&normalized).unwrap();
        let NodePhysicalPlan::Source(source) = planned.driver else {
            panic!("expected equality source driver");
        };
        assert_eq!(source.kind, NodeQueryCandidateSourceKind::PropertyEqualityIndex);
        assert_eq!(source.estimate.kind, PlannerEstimateKind::StatsEstimated);
        assert!(!source.estimate.can_use_uncapped_equality_materialization());
    }

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_node_ids(&query).unwrap();
    assert_eq!(result.items, vec![surviving_old_id]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.equality_materialization_record_reads, 0);
    assert_eq!(counters.final_verifier_record_reads, 0);
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert!(counters.node_visibility_meta_reads >= all_ids.len());

    engine.close().unwrap();
}

#[test]
fn test_query_unknown_estimates_use_stable_rank_then_key() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();

        let mut candidates = vec![
            NodePhysicalPlan::source(PlannedNodeCandidateSource::timestamp_index(
                1,
                0,
                100,
                PlannerEstimate::unknown(),
            )),
            NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                1,
                1,
                "b",
                &PropValue::String("x".to_string()),
                PlannerEstimate::unknown(),
            )),
            NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                1,
                2,
                "a",
                &PropValue::String("x".to_string()),
                PlannerEstimate::unknown(),
            )),
        ];
        published
            .view
            .sort_physical_plans_by_selectivity(&mut candidates);
        assert_eq!(
            candidates[0].canonical_key(),
            format!(
                "eq:1:a:{}",
                hash_prop_equality_key(&PropValue::String("x".to_string()))
            )
        );
    }

    engine.close().unwrap();
}
