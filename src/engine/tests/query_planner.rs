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

fn pattern_node_with_labels(
    alias: &str,
    labels: &[&str],
    filter_exprs: Vec<NodeFilterExpr>,
) -> NodePattern {
    NodePattern {
        alias: alias.to_string(),
        label_filter: Some(node_label_filter(labels, LabelMatchMode::All)),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: filter_from_conjunction(filter_exprs),
    }
}

fn pattern_node_with_label_filter(
    alias: &str,
    labels: &[&str],
    mode: LabelMatchMode,
    filter_exprs: Vec<NodeFilterExpr>,
) -> NodePattern {
    NodePattern {
        alias: alias.to_string(),
        label_filter: Some(node_label_filter(labels, mode)),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: filter_from_conjunction(filter_exprs),
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

fn pattern_node(
    alias: &str,
    label: Option<&str>,
    filter_exprs: Vec<NodeFilterExpr>,
) -> NodePattern {
    NodePattern {
        alias: alias.to_string(),
        label_filter: label.map(|label| node_label_filter(&[label], LabelMatchMode::All)),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: filter_from_conjunction(filter_exprs),
    }
}

fn pattern_node_with_ids(alias: &str, ids: Vec<u64>) -> NodePattern {
    NodePattern {
        alias: alias.to_string(),
        label_filter: None,
        ids,
        keys: Vec::new(),
        filter: None,
    }
}

fn pattern_edge(
    alias: Option<&str>,
    from_alias: &str,
    to_alias: &str,
    direction: Direction,
    label_filter: Option<Vec<&str>>,
) -> EdgePattern {
    EdgePattern {
        alias: alias.map(str::to_string),
        from_alias: from_alias.to_string(),
        to_alias: to_alias.to_string(),
        direction,
        label_filter: label_filter
            .map(|edge_labels| edge_labels.into_iter().map(str::to_string).collect())
            .unwrap_or_default(),
        filter: None,
    }
}

fn pattern_query(nodes: Vec<NodePattern>, edges: Vec<EdgePattern>) -> GraphPatternQuery {
    GraphPatternQuery {
        nodes,
        edges,
        at_epoch: None,
        limit: 100,
        order: PatternOrder::AnchorThenAliasesAsc,
    }
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

fn wait_until_after_millis(epoch_ms: i64) {
    for _ in 0..100 {
        if now_millis() > epoch_ms {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(
        now_millis() > epoch_ms,
        "millisecond clock did not advance past {epoch_ms}"
    );
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
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(0.5),
            upper: None,
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
        NormalizedEdgeFilter::PropertyIn { values, value_keys, .. } => {
            assert_eq!(values.len(), 2);
            assert_eq!(value_keys.len(), 2);
        }
        other => panic!("expected signed-zero IN to preserve two canonical values, got {other:?}"),
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
    let err = published
        .view
        .normalize_edge_query(&mixed_property_range)
        .unwrap_err();
    assert!(err.to_string().contains("same PropValue variant"));
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
        .ensure_edge_property_index("EDGE_LABEL_82", "status", SecondaryIndexKind::Equality)
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
        .ensure_edge_property_index("EDGE_LABEL_83",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_edge_property_index("SPECIAL_EDGE_831",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_edge_property_index("EDGE_LABEL_84", "status", SecondaryIndexKind::Equality)
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
        .ensure_edge_property_index("EDGE_LABEL_86", "z", SecondaryIndexKind::Equality)
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
fn edge_property_range_requires_exact_declared_domain() {
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
        .ensure_edge_property_index("EDGE_LABEL_87",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
    assert!(plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert!(plan
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
        .ensure_edge_property_index("EDGE_LABEL_88", "status", SecondaryIndexKind::Equality)
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
        .ensure_edge_property_index("EDGE_LABEL_89", "status", SecondaryIndexKind::Equality)
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
        .ensure_edge_property_index("EDGE_LABEL_89", "status", SecondaryIndexKind::Equality)
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
            .ensure_edge_property_index("EDGE_LABEL_90", "color", SecondaryIndexKind::Equality)
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
        hash_prop_value(&PropValue::String("red".to_string())),
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
            .ensure_edge_property_index("EDGE_LABEL_91", "status", SecondaryIndexKind::Equality)
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
            .ensure_edge_property_index("EDGE_LABEL_94", "status", SecondaryIndexKind::Equality)
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
            .ensure_edge_property_index("EDGE_LABEL_95",
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
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
            .ensure_edge_property_index("EDGE_LABEL_96",
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
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
            .ensure_edge_property_index("EDGE_LABEL_92", "status", SecondaryIndexKind::Equality)
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
        .ensure_edge_property_index("EDGE_LABEL_93", "bucket", SecondaryIndexKind::Equality)
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
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: Some(0.0),
                upper: Some(1.0),
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("allow_full_scan"));
}

#[test]
fn edge_query_pattern_filter_normalization() {
    let (_dir, engine) = query_test_engine();
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();

    let canonical = pattern_query(
        vec![
            pattern_node_with_ids("a", vec![1]),
            pattern_node_with_ids("b", vec![2]),
        ],
        vec![EdgePattern {
            alias: Some("e".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["RELATED_TO".to_string(), "RELATED_TO".to_string(), "LIKES".to_string()],
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: Some(0.25),
                upper: Some(0.75),
            }),
        }],
    );
    let normalized = published
        .view
        .normalize_pattern_query(&canonical)
        .unwrap();
    let mut expected_label_filter_ids = vec![
        engine.get_edge_label_id("LIKES").unwrap().unwrap(),
        engine.get_edge_label_id("RELATED_TO").unwrap().unwrap(),
    ];
    expected_label_filter_ids.sort_unstable();
    assert_eq!(normalized.edges[0].label_filter_ids, Some(expected_label_filter_ids));
    assert!(matches!(
        normalized.edges[0].filter,
        NormalizedEdgeFilter::WeightRange {
            lower: Some(0.25),
            upper: Some(0.75),
        }
    ));

    let property_filter = pattern_query(
        vec![
            pattern_node_with_ids("a", vec![1]),
            pattern_node_with_ids("b", vec![2]),
        ],
        vec![EdgePattern {
            alias: Some("e".to_string()),
            from_alias: "a".to_string(),
            to_alias: "b".to_string(),
            direction: Direction::Outgoing,
            label_filter: Vec::new(),
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "kind".to_string(),
                value: PropValue::String("friend".to_string()),
            }),
        }],
    );
    let normalized = published
        .view
        .normalize_pattern_query(&property_filter)
        .unwrap();
    assert!(matches!(
        normalized.edges[0].filter,
        NormalizedEdgeFilter::PropertyEquals { ref key, .. } if key == "kind"
    ));
}

#[test]
fn edge_query_pattern_filter_is_applied_during_execution() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "source", &[], 1.0);
    let keep = insert_query_node(&engine, "Company",  "keep", &[], 1.0);
    let drop_property = insert_query_node(&engine, "Company",  "drop-property", &[], 1.0);
    let drop_weight = insert_query_node(&engine, "Company",  "drop-weight", &[], 1.0);

    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "FRIENDS_WITH",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 0.5,
                valid_from: Some(10),
                valid_to: Some(20),
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop_property,
            "FRIENDS_WITH",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                weight: 0.5,
                valid_from: Some(10),
                valid_to: Some(20),
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop_weight,
            "FRIENDS_WITH",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 2.0,
                valid_from: Some(10),
                valid_to: Some(20),
            },
        )
        .unwrap();

    let mut query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![EdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["FRIENDS_WITH".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                EdgeFilterExpr::WeightRange {
                    lower: None,
                    upper: Some(1.0),
                },
                EdgeFilterExpr::ValidAt { epoch_ms: 10 },
            ])),
        }],
    );
    query.at_epoch = Some(10);

    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("edge", keep_edge)]
        )]
    );
}

#[test]
fn edge_query_pattern_metadata_only_filter_does_not_hydrate_edges() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "metadata-pattern-source", &[], 1.0);
    let keep = insert_query_node(&engine, "Company",  "metadata-pattern-keep", &[], 1.0);
    let drop = insert_query_node(&engine, "Company",  "metadata-pattern-drop", &[], 1.0);
    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "COLLABORATES_WITH",
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
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![EdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["COLLABORATES_WITH".to_string()],
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: None,
                upper: Some(1.0),
            }),
        }],
    );

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_pattern(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("edge", keep_edge)]
        )]
    );
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn pattern_edge_posting_metadata_filters_prune_before_pending_frontier() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "posting-pattern-source", &[], 1.0);
    let keep = insert_query_node(&engine, "Company",  "posting-pattern-keep", &[], 1.0);
    let drop_by_weight = insert_query_node(&engine, "Company",  "posting-pattern-weight", &[], 1.0);
    let drop_by_valid_to = insert_query_node(&engine, "Company",  "posting-pattern-valid-to", &[], 1.0);

    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                weight: 0.5,
                valid_from: Some(0),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop_by_weight,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                weight: 2.0,
                valid_from: Some(0),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop_by_valid_to,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                weight: 0.5,
                valid_from: Some(0),
                valid_to: Some(10),
                ..Default::default()
            },
        )
        .unwrap();

    let mut query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![EdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["COLLABORATES_WITH".to_string()],
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::WeightRange {
                    lower: None,
                    upper: Some(1.0),
                },
                EdgeFilterExpr::ValidAt { epoch_ms: 10 },
            ])),
        }],
    );
    query.at_epoch = Some(5);

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_pattern(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("edge", keep_edge)]
        )]
    );
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.pattern_edge_pending_entries, 1);
}

#[test]
fn pattern_edge_updated_at_filter_uses_metadata_without_hydration() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "updated-pattern-source", &[], 1.0);
    let old_target = insert_query_node(&engine, "Company",  "updated-pattern-old", &[], 1.0);
    let keep = insert_query_node(&engine, "Company",  "updated-pattern-keep", &[], 1.0);
    let old_edge = engine
        .upsert_edge(source, old_target, "COLLABORATES_WITH", UpsertEdgeOptions::default())
        .unwrap();
    let old_updated_at = engine.get_edge(old_edge).unwrap().unwrap().updated_at;
    wait_until_after_millis(old_updated_at);
    let keep_edge = engine
        .upsert_edge(source, keep, "COLLABORATES_WITH", UpsertEdgeOptions::default())
        .unwrap();
    let keep_updated_at = engine.get_edge(keep_edge).unwrap().unwrap().updated_at;
    assert!(keep_updated_at > old_updated_at);

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![EdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["COLLABORATES_WITH".to_string()],
            filter: Some(EdgeFilterExpr::UpdatedAtRange {
                lower_ms: Some(keep_updated_at),
                upper_ms: Some(keep_updated_at),
            }),
        }],
    );

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_pattern(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("edge", keep_edge)]
        )]
    );
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.pattern_edge_pending_entries, 2);
}

#[test]
fn pattern_edge_property_filter_projects_only_metadata_survivors() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "property-pattern-source", &[], 1.0);
    let keep = insert_query_node(&engine, "Company",  "property-pattern-keep", &[], 1.0);
    let metadata_drop = insert_query_node(&engine, "Company",  "property-pattern-metadata-drop", &[], 1.0);

    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    for index in 0..10 {
        let target = insert_query_node(&engine, "Company",
            &format!("property-pattern-metadata-survivor-{index}"),
            &[],
            1.0,
        );
        engine
            .upsert_edge(
                source,
                target,
                "COLLABORATES_WITH",
                UpsertEdgeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("inactive".to_string()),
                    )]),
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    engine
        .upsert_edge(
            source,
            metadata_drop,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![EdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["COLLABORATES_WITH".to_string()],
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
        }],
    );

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_pattern(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("edge", keep_edge)]
        )]
    );
    assert_eq!(counters.pattern_edge_pending_entries, 11);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
}

#[test]
fn pattern_edge_property_filter_reuses_match_cache_across_pending_flushes() {
    let (_dir, engine) = query_test_engine();
    let root = insert_query_node(&engine, "Person",  "property-cache-root", &[], 1.0);
    let mid = insert_query_node(&engine, "Company",  "property-cache-mid", &[], 1.0);
    let leaf = insert_query_node(&engine, "Article",  "property-cache-leaf", &[], 1.0);
    for _ in 0..300 {
        engine
            .upsert_edge(root, mid, "COLLABORATES_WITH", UpsertEdgeOptions::default())
            .unwrap();
    }
    let second_edge = engine
        .upsert_edge(
            mid,
            leaf,
            "RELATED_TO",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let query = GraphPatternQuery {
        limit: 400,
        ..pattern_query(
            vec![
                pattern_node_with_ids("root", vec![root]),
                pattern_node("mid", Some("Company"), Vec::new()),
                pattern_node("leaf", Some("Article"), Vec::new()),
            ],
            vec![
                EdgePattern {
                    alias: Some("first".to_string()),
                    from_alias: "root".to_string(),
                    to_alias: "mid".to_string(),
                    direction: Direction::Outgoing,
                    label_filter: vec!["COLLABORATES_WITH".to_string()],
                    filter: None,
                },
                EdgePattern {
                    alias: Some("second".to_string()),
                    from_alias: "mid".to_string(),
                    to_alias: "leaf".to_string(),
                    direction: Direction::Outgoing,
                    label_filter: vec!["RELATED_TO".to_string()],
                    filter: Some(EdgeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    }),
                },
            ],
        )
    };

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_pattern(&query).unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert!(!result.truncated);
    assert_eq!(result.matches.len(), 300);
    assert!(result
        .matches
        .iter()
        .all(|matched| matched.edges.get("second") == Some(&second_edge)));
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
}

#[test]
fn pattern_edge_metadata_filter_preserves_high_fanout_order_and_truncation() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "Person",  "fanout-filter-source", &[], 1.0);
    let mut expected = Vec::new();
    for index in 0..10 {
        let target = insert_query_node(&engine, "Company",
            &format!("fanout-filter-target-{index}"),
            &[],
            1.0,
        );
        let edge_id = engine
            .upsert_edge(
                source,
                target,
                "COLLABORATES_WITH",
                UpsertEdgeOptions {
                    weight: if index % 2 == 0 { 0.5 } else { 2.0 },
                    ..Default::default()
                },
            )
            .unwrap();
        if index % 2 == 0 {
            expected.push((target, edge_id));
        }
    }

    let query = GraphPatternQuery {
        limit: 3,
        ..pattern_query(
            vec![
                pattern_node_with_ids("source", vec![source]),
                pattern_node("target", Some("Company"), Vec::new()),
            ],
            vec![EdgePattern {
                alias: Some("edge".to_string()),
                from_alias: "source".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["COLLABORATES_WITH".to_string()],
                filter: Some(EdgeFilterExpr::WeightRange {
                    lower: None,
                    upper: Some(1.0),
                }),
            }],
        )
    };

    let result = engine.query_pattern(&query).unwrap();
    assert!(result.truncated);
    let expected_matches: Vec<_> = expected
        .iter()
        .take(3)
        .map(|(target, edge)| {
            expected_match(&[("source", source), ("target", *target)], &[("edge", *edge)])
        })
        .collect();
    assert_eq!(result.matches, expected_matches);
}

#[test]
fn pattern_edge_filters_work_on_branching_and_self_loop_patterns() {
    let (_dir, engine) = query_test_engine();
    let root = insert_query_node(&engine, "Person",  "filtered-branch-root", &[], 1.0);
    let left = insert_query_node(&engine, "Company",  "filtered-branch-left", &[], 1.0);
    let right = insert_query_node(&engine, "Article",  "filtered-branch-right", &[], 1.0);
    let left_edge = engine
        .upsert_edge(
            root,
            left,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            root,
            right,
            "COLLABORATES_WITH",
            UpsertEdgeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    let right_edge = engine
        .upsert_edge(
            root,
            right,
            "RELATED_TO",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let self_loop = engine
        .upsert_edge(
            root,
            root,
            "KNOWS",
            UpsertEdgeOptions {
                valid_from: Some(0),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();

    let branch_query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node_with_ids("left", vec![left]),
            pattern_node_with_ids("right", vec![right]),
        ],
        vec![
            EdgePattern {
                alias: Some("left_edge".to_string()),
                from_alias: "root".to_string(),
                to_alias: "left".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["COLLABORATES_WITH".to_string()],
                filter: Some(EdgeFilterExpr::WeightRange {
                    lower: None,
                    upper: Some(1.0),
                }),
            },
            EdgePattern {
                alias: Some("right_edge".to_string()),
                from_alias: "root".to_string(),
                to_alias: "right".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["RELATED_TO".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }),
            },
        ],
    );
    assert_eq!(
        engine.query_pattern(&branch_query).unwrap().matches,
        vec![expected_match(
            &[("left", left), ("right", right), ("root", root)],
            &[("left_edge", left_edge), ("right_edge", right_edge)]
        )]
    );

    let self_loop_query = GraphPatternQuery {
        at_epoch: Some(5),
        ..pattern_query(
            vec![pattern_node_with_ids("root", vec![root])],
            vec![EdgePattern {
                alias: Some("loop".to_string()),
                from_alias: "root".to_string(),
                to_alias: "root".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["KNOWS".to_string()],
                filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 10 }),
            }],
        )
    };
    assert_eq!(
        engine.query_pattern(&self_loop_query).unwrap().matches,
        vec![expected_match(&[("root", root)], &[("loop", self_loop)])]
    );
}

fn expected_match(nodes: &[(&str, u64)], edges: &[(&str, u64)]) -> QueryMatch {
    QueryMatch {
        nodes: nodes
            .iter()
            .map(|(alias, id)| ((*alias).to_string(), *id))
            .collect(),
        edges: edges
            .iter()
            .map(|(alias, id)| ((*alias).to_string(), *id))
            .collect(),
    }
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
        | QueryPlanNode::VerifyEdgePredicates { input }
        | QueryPlanNode::PatternExpand { input, .. }
        | QueryPlanNode::PatternEdgeAnchor { input, .. } => plan_contains_node(input, expected),
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

fn pattern_anchor_plan_node(plan: &QueryPlan) -> (&str, &QueryPlanNode) {
    let pattern = match &plan.root {
        QueryPlanNode::PatternExpand { .. } => &plan.root,
        QueryPlanNode::VerifyEdgePredicates { input } => input.as_ref(),
        other => panic!("expected pattern expand root, got {other:?}"),
    };
    match pattern {
        QueryPlanNode::PatternExpand {
            anchor_alias,
            input,
        } => match input.as_ref() {
            QueryPlanNode::VerifyNodeFilter { input } => (anchor_alias.as_str(), input.as_ref()),
            other => panic!("expected VerifyNodeFilter pattern input, got {other:?}"),
        },
        other => panic!("expected pattern expand node, got {other:?}"),
    }
}

fn pattern_anchor_input_nodes(plan: &QueryPlan) -> Vec<QueryPlanNode> {
    match pattern_anchor_plan_node(plan).1 {
        QueryPlanNode::Intersect { inputs } => inputs.clone(),
        node => vec![node.clone()],
    }
}

fn planned_pattern_anchor_and_edge_aliases(
    engine: &DatabaseEngine,
    query: &GraphPatternQuery,
) -> (String, Vec<String>) {
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published
        .view
        .normalize_pattern_query(query)
        .unwrap();
    let planned = published
        .view
        .plan_normalized_pattern_query(&normalized)
        .unwrap();
    let aliases = planned
        .anchor
        .expansion_order()
        .iter()
        .map(|&edge_index| {
            normalized.edges[edge_index]
                .alias
                .clone()
                .unwrap_or_else(|| format!("edge-{edge_index}"))
        })
        .collect();
    let anchor_alias = match &planned.anchor {
        PatternAnchorPlan::Node { node_index, .. } => normalized.nodes[*node_index].alias.clone(),
        PatternAnchorPlan::Edge { edge_alias, .. } => {
            edge_alias.clone().unwrap_or_else(|| "<edge>".to_string())
        }
    };
    (anchor_alias, aliases)
}

fn planned_pattern_anchor_sort_and_edge_aliases(
    engine: &DatabaseEngine,
    query: &GraphPatternQuery,
) -> (String, String, Vec<String>) {
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published
        .view
        .normalize_pattern_query(query)
        .unwrap();
    let planned = published
        .view
        .plan_normalized_pattern_query(&normalized)
        .unwrap();
    let aliases = planned
        .anchor
        .expansion_order()
        .iter()
        .map(|&edge_index| {
            normalized.edges[edge_index]
                .alias
                .clone()
                .unwrap_or_else(|| format!("edge-{edge_index}"))
        })
        .collect();
    let anchor_alias = match &planned.anchor {
        PatternAnchorPlan::Node { node_index, .. } => normalized.nodes[*node_index].alias.clone(),
        PatternAnchorPlan::Edge { edge_alias, .. } => {
            edge_alias.clone().unwrap_or_else(|| "<edge>".to_string())
        }
    };
    (
        anchor_alias,
        planned.anchor.sort_anchor_alias().to_string(),
        aliases,
    )
}

fn plan_contains_fallback_full_node_scan(node: &QueryPlanNode) -> bool {
    match node {
        QueryPlanNode::FallbackFullNodeScan => true,
        QueryPlanNode::Intersect { inputs } | QueryPlanNode::Union { inputs } => {
            inputs.iter().any(plan_contains_fallback_full_node_scan)
        }
        QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input }
        | QueryPlanNode::PatternExpand { input, .. }
        | QueryPlanNode::PatternEdgeAnchor { input, .. } => {
            plan_contains_fallback_full_node_scan(input)
        }
        _ => false,
    }
}

fn plan_contains_pattern_edge_anchor(node: &QueryPlanNode) -> bool {
    match node {
        QueryPlanNode::PatternEdgeAnchor { .. } => true,
        QueryPlanNode::Intersect { inputs } | QueryPlanNode::Union { inputs } => {
            inputs.iter().any(plan_contains_pattern_edge_anchor)
        }
        QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input }
        | QueryPlanNode::PatternExpand { input, .. } => {
            plan_contains_pattern_edge_anchor(input)
        }
        _ => false,
    }
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
            .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
fn test_planner_stats_zero_is_advisory_not_empty_result() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
    let red_hash = hash_prop_value(&red);
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
            .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Employee", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index(
            "Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Employee", "status", SecondaryIndexKind::Equality)
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
fn test_graph_pattern_multi_label_filter_all_and_any() {
    let (_dir, engine) = query_test_engine();
    let anchor = insert_query_node(&engine, "Company", "anchor", &[], 1.0);
    let both =
        insert_query_node_with_labels(&engine, &["Person", "Employee"], "both", &[], 1.0);
    let person = insert_query_node(&engine, "Person", "person", &[], 1.0);
    let employee = insert_query_node(&engine, "Employee", "employee", &[], 1.0);
    let both_edge = engine
        .upsert_edge(anchor, both, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let person_edge = engine
        .upsert_edge(anchor, person, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let employee_edge = engine
        .upsert_edge(anchor, employee, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let edges = vec![pattern_edge(
        Some("edge"),
        "anchor",
        "target",
        Direction::Outgoing,
        Some(vec!["KNOWS"]),
    )];
    let shorthand_all = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node_with_labels("target", &["Person", "Employee"], Vec::new()),
        ],
        edges.clone(),
    );
    assert_eq!(
        engine.query_pattern(&shorthand_all).unwrap().matches,
        vec![expected_match(
            &[("anchor", anchor), ("target", both)],
            &[("edge", both_edge)]
        )]
    );
    let shorthand_plan = engine.explain_pattern_query(&shorthand_all).unwrap();
    assert!(shorthand_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAllSupersetVerification));
    assert!(shorthand_plan.public_inputs.node_labels.contains(&QueryPlanPublicName {
        alias: Some("target".to_string()),
        name: "Person".to_string(),
        known: true,
        mode: Some(LabelMatchMode::All),
    }));

    let explicit_any = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node_with_label_filter(
                "target",
                &["Person", "Employee"],
                LabelMatchMode::Any,
                Vec::new(),
            ),
        ],
        edges,
    );
    let mut any_matches = engine.query_pattern(&explicit_any).unwrap().matches;
    any_matches.sort_by_key(|match_| match_.nodes["target"]);
    assert_eq!(
        any_matches,
        vec![
            expected_match(
                &[("anchor", anchor), ("target", both)],
                &[("edge", both_edge)]
            ),
            expected_match(
                &[("anchor", anchor), ("target", person)],
                &[("edge", person_edge)]
            ),
            expected_match(
                &[("anchor", anchor), ("target", employee)],
                &[("edge", employee_edge)]
            ),
        ]
    );
    let any_plan = engine.explain_pattern_query(&explicit_any).unwrap();
    assert!(any_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAnyFinalVerification));
    assert!(!any_plan
        .notes
        .contains(&QueryPlanNote::NodeLabelAnyDedupeBeforePagination));
    assert!(any_plan.public_inputs.node_labels.contains(&QueryPlanPublicName {
        alias: Some("target".to_string()),
        name: "Employee".to_string(),
        known: true,
        mode: Some(LabelMatchMode::Any),
    }));

    engine.close().unwrap();
}

#[test]
fn test_graph_pattern_label_only_targets_verify_metadata_without_hydration() {
    let (_dir, engine) = query_test_engine();
    let anchor = insert_query_node(&engine, "Company", "anchor", &[], 1.0);
    let both =
        insert_query_node_with_labels(&engine, &["Person", "Employee"], "both", &[], 1.0);
    let person = insert_query_node(&engine, "Person", "person", &[], 1.0);
    let employee = insert_query_node(&engine, "Employee", "employee", &[], 1.0);
    let both_edge = engine
        .upsert_edge(anchor, both, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let person_edge = engine
        .upsert_edge(anchor, person, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let employee_edge = engine
        .upsert_edge(anchor, employee, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let edges = vec![pattern_edge(
        Some("edge"),
        "anchor",
        "target",
        Direction::Outgoing,
        Some(vec!["KNOWS"]),
    )];

    let all_query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node_with_labels("target", &["Person", "Employee"], Vec::new()),
        ],
        edges.clone(),
    );
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&all_query).unwrap().matches,
        vec![expected_match(
            &[("anchor", anchor), ("target", both)],
            &[("edge", both_edge)]
        )]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);

    let any_query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node_with_label_filter(
                "target",
                &["Person", "Employee"],
                LabelMatchMode::Any,
                Vec::new(),
            ),
        ],
        edges,
    );
    engine.reset_query_execution_counters_for_test();
    let mut any_matches = engine.query_pattern(&any_query).unwrap().matches;
    any_matches.sort_by_key(|match_| match_.nodes["target"]);
    assert_eq!(
        any_matches,
        vec![
            expected_match(
                &[("anchor", anchor), ("target", both)],
                &[("edge", both_edge)]
            ),
            expected_match(
                &[("anchor", anchor), ("target", person)],
                &[("edge", person_edge)]
            ),
            expected_match(
                &[("anchor", anchor), ("target", employee)],
                &[("edge", employee_edge)]
            ),
        ]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert!(counters.node_visibility_meta_reads > 0);

    engine.close().unwrap();
}

#[test]
fn test_graph_pattern_property_target_still_hydrates_for_predicate() {
    let (_dir, engine) = query_test_engine();
    let anchor = insert_query_node(&engine, "Company", "anchor", &[], 1.0);
    let active = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let active_edge = engine
        .upsert_edge(anchor, active, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, inactive, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node_with_labels(
                "target",
                &["Person", "Employee"],
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }],
            ),
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("anchor", anchor), ("target", active)],
            &[("edge", active_edge)]
        )]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert!(counters.node_record_hydration_reads > 0);

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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    let score = engine
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
                .contains_key(&hash_prop_value(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    let residual_value = values
        .iter()
        .find(|value| {
            !rollup
                .top_value_hashes
                .contains_key(&hash_prop_value(&PropValue::String((*value).clone())))
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
                .contains_key(&hash_prop_value(&PropValue::String((*value).clone())))
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
            .ensure_node_property_index("Person",
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
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
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let score = engine
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Person", "kind", SecondaryIndexKind::Equality)
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
        vec![null_id, uint_id, array_id, map_id]
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
    assert_eq!(engine.query_node_ids(&int_only).unwrap().items, vec![int_id]);
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
    let nested_zero_match = insert_query_node(&engine, "Person",
        "token-nested-zero-match",
        &[(
            "token",
            PropValue::Array(vec![PropValue::Float(0.0)]),
        )],
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
        vec![string_match, signed_zero_match, nested_zero_match]
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
        .ensure_node_property_index("Person", "temperature", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let score_index = engine
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Article", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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

    let range_query = NodeQuery {
        label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
        filter: Some(NodeFilterExpr::And(
            (0..5)
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

#[test]
fn test_query_pattern_boolean_anchor_uses_union_and_filters_stale_candidates() {
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
    let inactive = insert_query_node(&engine, "Person",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let target = insert_query_node(&engine, "Company",  "target", &[], 1.0);
    for source in [stale_active, deleted_trial, active, trial, inactive] {
        engine
            .upsert_edge(source, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "person".to_string(),
                label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
                ids: Vec::new(),
                keys: Vec::new(),
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
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "person",
            "target",
            Direction::Outgoing,
            None,
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("person", active), ("target", target)], &[("edge", 3)]),
            expected_match(&[("person", trial), ("target", target)], &[("edge", 4)]),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    let (anchor_alias, _) = pattern_anchor_plan_node(&plan);
    assert_eq!(anchor_alias, "person");
    assert_eq!(
        pattern_anchor_input_nodes(&plan),
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_boolean_anchor_with_verify_only_branch_uses_label_scan() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(&engine, "Person",
        "active",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let missing = insert_query_node(&engine, "Person",
        "missing",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let skipped = insert_query_node(&engine, "Person",
        "skipped",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let target = insert_query_node(&engine, "Company",  "target", &[], 1.0);
    for source in [active, missing, skipped] {
        engine
            .upsert_edge(source, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "person".to_string(),
                label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::Or(vec![
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    },
                    NodeFilterExpr::PropertyMissing {
                        key: "tag".to_string(),
                    },
                ])),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "person",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("person", active), ("target", target)], &[("edge", 1)]),
            expected_match(&[("person", missing), ("target", target)], &[("edge", 2)]),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::FallbackNodeLabelScan]);
    assert!(plan.warnings.contains(&QueryPlanWarning::UsingFallbackScan));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert!(plan.warnings.contains(&QueryPlanWarning::BooleanBranchFallback));
    assert!(!plan_contains_fallback_full_node_scan(&plan.root));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_exists_anchor_uses_label_scan_and_presence_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let present = insert_query_node(&engine, "Person",
        "present",
        &[("tag", PropValue::String("present".to_string()))],
        1.0,
    );
    let null_tag = insert_query_node(&engine, "Person",  "null-tag", &[("tag", PropValue::Null)], 1.0);
    let missing = insert_query_node(&engine, "Person",  "missing", &[], 1.0);
    let target = insert_query_node(&engine, "Company",  "target", &[], 1.0);
    let present_edge = engine
        .upsert_edge(present, target, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let null_edge = engine
        .upsert_edge(null_tag, target, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(missing, target, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                label_filter: Some(NodeLabelFilter { labels: vec!["Person".to_string()], mode: LabelMatchMode::All }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyExists {
                    key: "tag".to_string(),
                }),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(
                &[("source", present), ("target", target)],
                &[("edge", present_edge)]
            ),
            expected_match(
                &[("source", null_tag), ("target", target)],
                &[("edge", null_edge)]
            ),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(
        pattern_anchor_input_nodes(&plan),
        vec![QueryPlanNode::FallbackNodeLabelScan]
    );
    assert!(plan.warnings.contains(&QueryPlanWarning::UsingFallbackScan));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert!(!plan_contains_fallback_full_node_scan(&plan.root));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_ids_with_verify_only_filter_is_valid_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let missing = insert_query_node(&engine, "Person",  "missing", &[], 1.0);
    let present = insert_query_node(&engine, "Person",
        "present",
        &[("tag", PropValue::String("present".to_string()))],
        1.0,
    );
    let target = insert_query_node(&engine, "Company",  "target", &[], 1.0);
    let edge = engine
        .upsert_edge(missing, target, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(present, target, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                label_filter: None,
                ids: vec![missing, present],
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyMissing {
                    key: "tag".to_string(),
                }),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("source", missing), ("target", target)], &[("edge", edge)])]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_rejects_labelless_verify_only_initial_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyMissing {
                    key: "tag".to_string(),
                }),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            None,
        )],
    );

    assert!(matches!(
        engine.query_pattern(&query).unwrap_err(),
        EngineError::InvalidOperation(message)
            if message.contains("anchorable node pattern")
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_labelless_missing_target_verifies_after_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, "Person",  "anchor", &[], 1.0);
    let missing = insert_query_node(&engine, "Company",  "missing", &[], 1.0);
    let present = insert_query_node(&engine, "Company",
        "present",
        &[("tag", PropValue::String("present".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(anchor, missing, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, present, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            NodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyMissing {
                    key: "tag".to_string(),
                }),
            },
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("anchor", anchor), ("target", missing)], &[("edge", edge)])]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_labelless_not_target_verifies_after_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, "Person",  "anchor", &[], 1.0);
    let active = insert_query_node(&engine, "Company",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let missing_status = insert_query_node(&engine, "Company",  "missing-status", &[], 1.0);
    let inactive = insert_query_node(&engine, "Company",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let active_edge = engine
        .upsert_edge(anchor, active, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let missing_edge = engine
        .upsert_edge(anchor, missing_status, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, inactive, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            NodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::Not(Box::new(
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("inactive".to_string()),
                    },
                ))),
            },
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(
                &[("anchor", anchor), ("target", active)],
                &[("edge", active_edge)]
            ),
            expected_match(
                &[("anchor", anchor), ("target", missing_status)],
                &[("edge", missing_edge)]
            ),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::ExplicitIds]);
    assert!(!plan_contains_fallback_full_node_scan(&plan.root));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_always_false_node_returns_empty_result() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::And(vec![
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    },
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("inactive".to_string()),
                    },
                ])),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    let result = engine.query_pattern(&query).unwrap();
    assert!(result.matches.is_empty());
    assert!(!result.truncated);
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_plan_node(&plan).0, "source");
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::EmptyResult]);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_target_in_matches_equivalent_or_filter() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, "Person",  "anchor", &[], 1.0);
    let active = insert_query_node(&engine, "Company",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(&engine, "Company",
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(&engine, "Company",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let active_edge = engine
        .upsert_edge(anchor, active, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let trial_edge = engine
        .upsert_edge(anchor, trial, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, inactive, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let base_nodes = vec![pattern_node_with_ids("anchor", vec![anchor])];
    let edges = vec![pattern_edge(
        Some("edge"),
        "anchor",
        "target",
        Direction::Outgoing,
        Some(vec!["KNOWS"]),
    )];
    let in_query = pattern_query(
        {
            let mut nodes = base_nodes.clone();
            nodes.push(NodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyIn {
                    key: "status".to_string(),
                    values: vec![
                        PropValue::String("active".to_string()),
                        PropValue::String("trial".to_string()),
                    ],
                }),
            });
            nodes
        },
        edges.clone(),
    );
    let or_query = pattern_query(
        {
            let mut nodes = base_nodes;
            nodes.push(NodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
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
            });
            nodes
        },
        edges,
    );

    let expected = vec![
        expected_match(&[("anchor", anchor), ("target", active)], &[("edge", active_edge)]),
        expected_match(&[("anchor", anchor), ("target", trial)], &[("edge", trial_edge)]),
    ];
    assert_eq!(engine.query_pattern(&in_query).unwrap().matches, expected);
    assert_eq!(
        engine.query_pattern(&or_query).unwrap().matches,
        engine.query_pattern(&in_query).unwrap().matches
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
    assert_eq!(
        engine.query_node_ids(&key_only).unwrap().items,
        oracle_query_ids(&engine, &[alice, bob, carol, other_label], &key_only)
    );
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
    assert_eq!(
        engine.query_node_ids(&key_preferred).unwrap().items,
        oracle_query_ids(&engine, &[alice, bob, carol, other_label], &key_preferred)
    );
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let tier = engine
        .ensure_node_property_index("Person", "tier", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let score = engine
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let tier = engine
        .ensure_node_property_index("Person", "tier", SecondaryIndexKind::Equality)
        .unwrap();
    let score = engine
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
            .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
            .unwrap();
        let score = engine
            .ensure_node_property_index("Person",
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
    }

    fn assert_tiny_lifecycle_query_uses_fallback(
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
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("mixed");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        engine.flush().unwrap();
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
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
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.compact().unwrap().unwrap();
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.close().unwrap();

        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_tiny_lifecycle_query_uses_fallback(&reopened, &all_ids, &query);
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
            .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
            .unwrap();
        let score = engine
            .ensure_node_property_index("Person",
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
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
        .ensure_node_property_index("Person",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    let status = engine
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
                SecondaryIndexRangeDomain::Int,
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
            .query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let seg_dir = segment_dir(&db_path, engine.segments_for_test()[0].segment_id);
    let sidecar_path = segment_component_path(
        &seg_dir,
        crate::segment_components::SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: info.index_id,
        },
    );
    std::fs::remove_file(&sidecar_path).unwrap();

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

// --- graph pattern execution ---

#[test]
fn test_query_pattern_validation_rejects_invalid_shapes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let empty = pattern_query(Vec::new(), Vec::new());
    assert!(matches!(
        engine.query_pattern(&empty).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let node_only = pattern_query(vec![pattern_node("a", Some("Person"), Vec::new())], Vec::new());
    assert!(matches!(
        engine.explain_pattern_query(&node_only).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let duplicate_node = pattern_query(
        vec![
            pattern_node("a", Some("Person"), Vec::new()),
            pattern_node("a", Some("Company"), Vec::new()),
        ],
        vec![pattern_edge(Some("e"), "a", "a", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&duplicate_node).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_node_alias = pattern_query(
        vec![pattern_node("", Some("Person"), Vec::new())],
        vec![pattern_edge(Some("e"), "", "", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&empty_node_alias).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let bad_reference = pattern_query(
        vec![pattern_node("a", Some("Person"), Vec::new())],
        vec![pattern_edge(Some("e"), "a", "missing", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&bad_reference).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_edge_alias = pattern_query(
        vec![pattern_node("a", Some("Person"), Vec::new())],
        vec![pattern_edge(Some(""), "a", "a", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&empty_edge_alias).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let duplicate_edge_alias = pattern_query(
        vec![
            pattern_node("a", Some("Person"), Vec::new()),
            pattern_node("b", Some("Company"), Vec::new()),
        ],
        vec![
            pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None),
            pattern_edge(Some("e"), "b", "a", Direction::Outgoing, None),
        ],
    );
    assert!(matches!(
        engine.query_pattern(&duplicate_edge_alias).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let zero_limit = GraphPatternQuery {
        limit: 0,
        ..pattern_query(
            vec![
                pattern_node("a", Some("Person"), Vec::new()),
                pattern_node("b", Some("Company"), Vec::new()),
            ],
            vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
        )
    };
    assert!(matches!(
        engine.query_pattern(&zero_limit).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let mut key_without_label = pattern_node("a", None, Vec::new());
    key_without_label.keys.push("a".to_string());
    let key_query = pattern_query(
        vec![key_without_label, pattern_node("b", Some("Company"), Vec::new())],
        vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&key_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let disconnected_extra_node = pattern_query(
        vec![
            pattern_node("a", Some("Person"), Vec::new()),
            pattern_node("b", Some("Company"), Vec::new()),
            pattern_node("c", Some("Article"), Vec::new()),
        ],
        vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&disconnected_extra_node).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let disconnected_components = pattern_query(
        vec![
            pattern_node("a", Some("Person"), Vec::new()),
            pattern_node("b", Some("Company"), Vec::new()),
            pattern_node("c", Some("Article"), Vec::new()),
            pattern_node("d", Some("Topic"), Vec::new()),
        ],
        vec![
            pattern_edge(Some("ab"), "a", "b", Direction::Outgoing, None),
            pattern_edge(Some("cd"), "c", "d", Direction::Outgoing, None),
        ],
    );
    assert!(matches!(
        engine.query_pattern(&disconnected_components).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let unanchored = pattern_query(
        vec![pattern_node("a", None, Vec::new()), pattern_node("b", None, Vec::new())],
        vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.explain_pattern_query(&unanchored).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_anchor_selection_uses_label_cardinality() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for index in 0..8 {
        insert_query_node(&engine, "Person",  &format!("wide-{index}"), &[], 1.0);
    }
    insert_query_node(&engine, "Company",  "narrow", &[], 1.0);

    let query = pattern_query(
        vec![
            pattern_node("aaa_wide", Some("Person"), Vec::new()),
            pattern_node("zzz_narrow", Some("Company"), Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "aaa_wide",
            "zzz_narrow",
            Direction::Both,
            None,
        )],
    );

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_pattern_query(&query).unwrap();
        let planned = published
            .view
            .plan_normalized_pattern_query(&normalized)
            .unwrap();
        match &planned.anchor {
            PatternAnchorPlan::Node { node_index, .. } => {
                assert_eq!(normalized.nodes[*node_index].alias, "zzz_narrow");
            }
            PatternAnchorPlan::Edge { .. } => panic!("expected node anchor"),
        }
    }

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_linear_uses_reverse_direction_from_selective_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = insert_query_node(&engine, "Person",
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let _bob = insert_query_node(&engine, "Person",
        "bob",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let acme = insert_query_node(&engine, "Company",
        "acme",
        &[("tier", PropValue::String("enterprise".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(alice, acme, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node("person", Some("Person"),
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }],
            ),
            pattern_node_with_ids("company", vec![acme]),
        ],
        vec![pattern_edge(
            Some("works_at"),
            "person",
            "company",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("company", acme), ("person", alice)],
            &[("works_at", edge)]
        )]
    );
    assert!(!result.truncated);

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(plan.kind, QueryPlanKind::PatternQuery);
    assert!(matches!(plan.root, QueryPlanNode::PatternExpand { .. }));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_labelless_predicate_target_verifies_after_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, "Person",  "anchor", &[], 1.0);
    let good = insert_query_node(&engine, "Company",
        "good",
        &[("status", PropValue::String("match".to_string()))],
        1.0,
    );
    let bad = insert_query_node(&engine, "Article",
        "bad",
        &[("status", PropValue::String("skip".to_string()))],
        1.0,
    );
    let good_edge = engine
        .upsert_edge(anchor, good, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, bad, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node(
                "target",
                None,
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("match".to_string()),
                }],
            ),
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_pattern_query(&query).unwrap();
        let planned = published
            .view
            .plan_normalized_pattern_query(&normalized)
            .unwrap();
        match &planned.anchor {
            PatternAnchorPlan::Node { node_index, .. } => {
                assert_eq!(normalized.nodes[*node_index].alias, "anchor");
            }
            PatternAnchorPlan::Edge { .. } => panic!("expected node anchor"),
        }
    }

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("anchor", anchor), ("target", good)],
            &[("edge", good_edge)]
        )]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_order_limit_truncated_and_direction_both() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    let low = insert_query_node(&engine, "Company",  "low", &[], 1.0);
    let high = insert_query_node(&engine, "Company",  "high", &[], 1.0);
    let high_edge = engine
        .upsert_edge(root, high, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let low_edge = engine
        .upsert_edge(low, root, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let base = pattern_query(
        vec![pattern_node_with_ids("root", vec![root]), pattern_node("target", Some("Company"), Vec::new())],
        vec![pattern_edge(Some("edge"), "root", "target", Direction::Both, Some(vec!["KNOWS"]))],
    );
    let limited = GraphPatternQuery { limit: 1, ..base.clone() };

    let limited_result = engine.query_pattern(&limited).unwrap();
    assert_eq!(
        limited_result.matches,
        vec![expected_match(
            &[("root", root), ("target", low)],
            &[("edge", low_edge)]
        )]
    );
    assert!(limited_result.truncated);

    let full_result = engine.query_pattern(&base).unwrap();
    assert_eq!(
        full_result.matches,
        vec![
            expected_match(&[("root", root), ("target", low)], &[("edge", low_edge)]),
            expected_match(&[("root", root), ("target", high)], &[("edge", high_edge)]),
        ]
    );
    assert!(!full_result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_high_fanout_limit_keeps_deterministic_top_matches() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    let mut targets = Vec::new();
    for index in 0..32 {
        targets.push(insert_query_node(&engine, "Company",
            &format!("target-{index:02}"),
            &[],
            1.0,
        ));
    }

    let mut edges_by_target = BTreeMap::new();
    for &target in targets.iter().rev() {
        let edge = engine
            .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
        edges_by_target.insert(target, edge);
    }

    let query = GraphPatternQuery {
        limit: 5,
        ..pattern_query(
            vec![
                pattern_node_with_ids("root", vec![root]),
                pattern_node("target", Some("Company"), Vec::new()),
            ],
            vec![pattern_edge(
                Some("edge"),
                "root",
                "target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            )],
        )
    };

    let result = engine.query_pattern(&query).unwrap();
    let expected: Vec<QueryMatch> = targets
        .iter()
        .take(5)
        .map(|&target| {
            expected_match(
                &[("root", root), ("target", target)],
                &[("edge", edges_by_target[&target])],
            )
        })
        .collect();
    assert_eq!(result.matches, expected);
    assert!(result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_fanout_cost_can_choose_larger_lower_expansion_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let hub = insert_query_node(&engine, "Person",  "hub", &[], 1.0);
    let mut mids = Vec::new();
    for index in 0..300 {
        let mid = insert_query_node(&engine, "Article",  &format!("mid-{index:03}"), &[], 1.0);
        engine
            .upsert_edge(hub, mid, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
        mids.push(mid);
    }
    let mut anchors = Vec::new();
    for (index, &mid) in mids.iter().enumerate().take(20) {
        let anchor = insert_query_node(&engine, "Company",  &format!("anchor-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(mid, anchor, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
        anchors.push(anchor);
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node("small_hub", Some("Person"), Vec::new()),
            pattern_node("larger_anchor", Some("Company"), Vec::new()),
            pattern_node("middle", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("hub_to_middle"),
                "small_hub",
                "middle",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("middle_to_anchor"),
                "middle",
                "larger_anchor",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );

    let (anchor_alias, _) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(anchor_alias, "larger_anchor");
    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(result.matches.len(), anchors.len());
    assert!(!result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_fanout_physical_anchor_does_not_change_result_order() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let hub_low = insert_query_node(&engine, "Person",  "hub-low", &[], 1.0);
    let hub_high = insert_query_node(&engine, "Person",  "hub-high", &[], 1.0);
    let mut mids = Vec::new();
    for index in 0..300 {
        mids.push(insert_query_node(&engine, "Article",
            &format!("mid-{index:03}"),
            &[],
            1.0,
        ));
    }
    let mut anchors = Vec::new();
    for index in 0..20 {
        anchors.push(insert_query_node(&engine, "Company",
            &format!("anchor-{index:02}"),
            &[],
            1.0,
        ));
    }
    for &mid in &mids[..150] {
        engine
            .upsert_edge(hub_low, mid, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    for &mid in &mids[150..] {
        engine
            .upsert_edge(hub_high, mid, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine
        .upsert_edge(mids[0], anchors[19], "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(mids[150], anchors[0], "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let mut query = pattern_query(
        vec![
            pattern_node("a_small_hub", Some("Person"), Vec::new()),
            pattern_node("z_larger_anchor", Some("Company"), Vec::new()),
            pattern_node("middle", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("hub_to_middle"),
                "a_small_hub",
                "middle",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("middle_to_anchor"),
                "middle",
                "z_larger_anchor",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );
    query.limit = 1;

    let (physical_anchor, sort_anchor, _) =
        planned_pattern_anchor_sort_and_edge_aliases(&engine, &query);
    assert_eq!(physical_anchor, "middle_to_anchor");
    assert_eq!(sort_anchor, "a_small_hub");

    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(result.matches.len(), 1);
    assert!(result.truncated);
    assert_eq!(result.matches[0].nodes["a_small_hub"], hub_low);
    assert_eq!(result.matches[0].nodes["z_larger_anchor"], anchors[19]);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_fanout_delays_high_hub_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
    engine
        .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
    for index in 0..128 {
        let target = insert_query_node(&engine, "Company",  &format!("hub-target-{index:03}"), &[], 1.0);
        engine
            .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some("Company"), Vec::new()),
            pattern_node("low_target", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("zzz_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(edge_aliases.first().map(String::as_str), Some("zzz_low"));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_mutable_edges_preserve_deterministic_expansion_order() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
    engine
        .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
    for index in 0..96 {
        let target = insert_query_node(&engine, "Company",  &format!("target-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let mutable_target = insert_query_node(&engine, "MissingLabel",  "mutable", &[], 1.0);
    engine
        .upsert_edge(root, mutable_target, "MISSING_EDGE_LABEL", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some("Company"), Vec::new()),
            pattern_node("low_target", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("zzz_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(edge_aliases.first().map(String::as_str), Some("aaa_hub"));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_target_filter_selectivity_reduces_fanout_cost() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .ensure_node_property_index("Company", "status", SecondaryIndexKind::Equality)
        .unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    for index in 0..128 {
        let status = if index == 127 { "selected" } else { "other" };
        let target = insert_query_node(&engine, "Company",
            &format!("candidate-{index:03}"),
            &[("status", PropValue::String(status.to_string()))],
            1.0,
        );
        engine
            .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    for index in 0..16 {
        let target = insert_query_node(&engine, "Article",  &format!("low-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("selected_target", Some("Company"),
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("selected".to_string()),
                }],
            ),
            pattern_node("low_target", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("zzz_selective"),
                "root",
                "selected_target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("aaa_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(
        edge_aliases.first().map(String::as_str),
        Some("zzz_selective")
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_absent_edge_label_uses_complete_zero_fanout() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    let missing_target = insert_query_node(&engine, "Topic",  "missing-target", &[], 1.0);
    for index in 0..64 {
        let target = insert_query_node(&engine, "Company",  &format!("target-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.ensure_edge_label("SPECIAL_EDGE_999").unwrap();
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some("Company"), Vec::new()),
            pattern_node_with_ids("missing_target", vec![missing_target]),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("zzz_missing"),
                "root",
                "missing_target",
                Direction::Outgoing,
                Some(vec!["SPECIAL_EDGE_999"]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(
        edge_aliases.first().map(String::as_str),
        Some("zzz_missing")
    );
    let result = engine.query_pattern(&query).unwrap();
    assert!(result.matches.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_both_bound_constraint_stays_before_unbound_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Company",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Article",  "c", &[], 1.0);
    engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, c, "RATES", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, a, "BLOCKS", UpsertEdgeOptions::default())
        .unwrap();
    for index in 0..96 {
        let dummy = insert_query_node(&engine, "Topic",  &format!("dummy-{index:03}"), &[], 1.0);
        engine
            .upsert_edge(b, dummy, "BLOCKS", UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("a", vec![a]),
            pattern_node("b", Some("Company"), Vec::new()),
            pattern_node("c", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_bind_b"),
                "a",
                "b",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("zzz_constraint"),
                "b",
                "a",
                Direction::Outgoing,
                Some(vec!["BLOCKS"]),
            ),
            pattern_edge(
                Some("zzz_unbound"),
                "a",
                "c",
                Direction::Outgoing,
                Some(vec!["RATES"]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(edge_aliases[0], "aaa_bind_b");
    assert_eq!(edge_aliases[1], "zzz_constraint");

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_missing_fanout_stats_preserves_deterministic_order() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
        let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
        engine
            .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
        for index in 0..64 {
            let target = insert_query_node(&engine, "Company",  &format!("target-{index:02}"), &[], 1.0);
            engine
                .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
                .unwrap();
        }
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 1)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::remove_file(stats_path).unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let root = reopened.get_node_by_key("Person", "root").unwrap().unwrap().id;
    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some("Company"), Vec::new()),
            pattern_node("low_target", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("zzz_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&reopened, &query);
    assert_eq!(edge_aliases.first().map(String::as_str), Some("aaa_hub"));

    reopened.close().unwrap();
}

fn fanout_parity_query(root: u64) -> GraphPatternQuery {
    pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some("Company"), Vec::new()),
            pattern_node("low_target", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("hub_edge"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec!["KNOWS"]),
            ),
            pattern_edge(
                Some("low_edge"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    )
}

fn insert_fanout_parity_tail(engine: &DatabaseEngine, root: u64, start: usize, count: usize) {
    for index in start..start + count {
        let target = insert_query_node(engine, "Company",  &format!("target-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
    }
}

fn assert_fanout_parity_result(engine: &DatabaseEngine, root: u64, expected_matches: &[QueryMatch]) {
    let query = fanout_parity_query(root);
    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(result.matches, expected_matches);
    assert!(!result.truncated);
}

#[test]
fn test_query_pattern_fanout_stats_preserve_results_across_storage_states() {
    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("memtable");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
        insert_fanout_parity_tail(&engine, root, 0, 8);
        let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
        let low_edge = engine
            .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = engine
                    .get_node_by_key("Company", &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = engine
                    .get_edge_by_triple(root, target, "KNOWS")
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&engine, root, &expected);
        engine.close().unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("flushed");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
        insert_fanout_parity_tail(&engine, root, 0, 8);
        let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
        let low_edge = engine
            .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
        engine.flush().unwrap();
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = engine
                    .get_node_by_key("Company", &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = engine
                    .get_edge_by_triple(root, target, "KNOWS")
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&engine, root, &expected);
        engine.close().unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("reopened");
        let root = {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
            insert_fanout_parity_tail(&engine, root, 0, 8);
            let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
            engine
                .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
                .unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
            root
        };
        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let low = reopened.get_node_by_key("Article", "low").unwrap().unwrap().id;
        let low_edge = reopened
            .get_edge_by_triple(root, low, "REPORTS_TO")
            .unwrap()
            .unwrap()
            .id;
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = reopened
                    .get_node_by_key("Company", &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = reopened
                    .get_edge_by_triple(root, target, "KNOWS")
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&reopened, root, &expected);
        reopened.close().unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("compacted");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
        insert_fanout_parity_tail(&engine, root, 0, 4);
        engine.flush().unwrap();
        insert_fanout_parity_tail(&engine, root, 4, 4);
        let low = insert_query_node(&engine, "Article",  "low", &[], 1.0);
        let low_edge = engine
            .upsert_edge(root, low, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap().unwrap();
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = engine
                    .get_node_by_key("Company", &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = engine
                    .get_edge_by_triple(root, target, "KNOWS")
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&engine, root, &expected);
        engine.close().unwrap();
    }
}

#[test]
fn test_query_pattern_distinct_edge_aliases_may_share_edge_id() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Company",  "b", &[], 1.0);
    let edge = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![pattern_node_with_ids("a", vec![a]), pattern_node("b", Some("Company"), Vec::new())],
        vec![
            pattern_edge(Some("first"), "a", "b", Direction::Outgoing, Some(vec!["KNOWS"])),
            pattern_edge(Some("second"), "a", "b", Direction::Outgoing, Some(vec!["KNOWS"])),
        ],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("a", a), ("b", b)],
            &[("first", edge), ("second", edge)]
        )]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_branching_distinct_aliases_and_no_match() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, "Person",  "root", &[], 1.0);
    let left = insert_query_node(&engine, "Company",  "left", &[], 1.0);
    let right = insert_query_node(&engine, "Article",  "right", &[], 1.0);
    let shared = insert_query_node(&engine, "Topic",  "shared", &[], 1.0);
    let left_edge = engine
        .upsert_edge(root, left, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let right_edge = engine
        .upsert_edge(root, right, "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(root, shared, "RATES", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(root, shared, "EDGE_LABEL_40", UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("left", Some("Company"), Vec::new()),
            pattern_node("right", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(Some("left_edge"), "root", "left", Direction::Outgoing, Some(vec!["KNOWS"])),
            pattern_edge(
                Some("right_edge"),
                "root",
                "right",
                Direction::Outgoing,
                Some(vec!["REPORTS_TO"]),
            ),
        ],
    );
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("left", left), ("right", right), ("root", root)],
            &[("left_edge", left_edge), ("right_edge", right_edge)]
        )]
    );

    let distinct_alias_query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("x", Some("Topic"), Vec::new()),
            pattern_node("y", Some("Topic"), Vec::new()),
        ],
        vec![
            pattern_edge(Some("x_edge"), "root", "x", Direction::Outgoing, Some(vec!["RATES"])),
            pattern_edge(Some("y_edge"), "root", "y", Direction::Outgoing, Some(vec!["REFERENCES"])),
        ],
    );
    assert!(engine
        .query_pattern(&distinct_alias_query)
        .unwrap()
        .matches
        .is_empty());

    let no_match_query = pattern_query(
        vec![pattern_node_with_ids("root", vec![root]), pattern_node("missing", Some("MissingLabel"), Vec::new())],
        vec![pattern_edge(None, "root", "missing", Direction::Outgoing, None)],
    );
    assert!(engine
        .query_pattern(&no_match_query)
        .unwrap()
        .matches
        .is_empty());

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_cycle_closing_edge_and_self_loop() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Company",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Article",  "c", &[], 1.0);
    let ab = engine.upsert_edge(a, b, "KNOWS", Default::default()).unwrap();
    let bc = engine.upsert_edge(b, c, "KNOWS", Default::default()).unwrap();
    let ca = engine.upsert_edge(c, a, "KNOWS", Default::default()).unwrap();
    let aa = engine.upsert_edge(a, a, "MISSING_EDGE_LABEL", Default::default()).unwrap();

    let cycle = pattern_query(
        vec![
            pattern_node_with_ids("a", vec![a]),
            pattern_node("b", Some("Company"), Vec::new()),
            pattern_node("c", Some("Article"), Vec::new()),
        ],
        vec![
            pattern_edge(Some("ab"), "a", "b", Direction::Outgoing, Some(vec!["KNOWS"])),
            pattern_edge(Some("bc"), "b", "c", Direction::Outgoing, Some(vec!["KNOWS"])),
            pattern_edge(Some("ca"), "c", "a", Direction::Outgoing, Some(vec!["KNOWS"])),
        ],
    );
    assert_eq!(
        engine.query_pattern(&cycle).unwrap().matches,
        vec![expected_match(
            &[("a", a), ("b", b), ("c", c)],
            &[("ab", ab), ("bc", bc), ("ca", ca)]
        )]
    );

    let self_loop = pattern_query(
        vec![pattern_node_with_ids("a", vec![a])],
        vec![pattern_edge(Some("loop"), "a", "a", Direction::Outgoing, Some(vec!["MISSING_EDGE_LABEL"]))],
    );
    assert_eq!(
        engine.query_pattern(&self_loop).unwrap().matches,
        vec![expected_match(&[("a", a)], &[("loop", aa)])]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_edge_property_post_filters_and_explain_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, "Person",  "a", &[], 1.0);
    let b = insert_query_node(&engine, "Company",  "b", &[], 1.0);
    let c = insert_query_node(&engine, "Company",  "c", &[], 1.0);
    let good = engine
        .upsert_edge(
            a,
            b,
            "KNOWS",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("rel", PropValue::String("friend".to_string())),
                    ("score", PropValue::Int(5)),
                ]),
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
                props: query_test_props(&[
                    ("rel", PropValue::String("friend".to_string())),
                    ("score", PropValue::Int(1)),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let mut edge = pattern_edge(Some("e"), "a", "target", Direction::Outgoing, Some(vec!["KNOWS"]));
    edge.filter = Some(EdgeFilterExpr::And(vec![
        EdgeFilterExpr::PropertyEquals {
            key: "rel".to_string(),
            value: PropValue::String("friend".to_string()),
        },
        EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(3))),
            upper: None,
        },
    ]));
    let query = pattern_query(
        vec![pattern_node_with_ids("a", vec![a]), pattern_node("target", Some("Company"), Vec::new())],
        vec![edge],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("a", a), ("target", b)], &[("e", good)])]
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert!(matches!(
        plan.root,
        QueryPlanNode::VerifyEdgePredicates { .. }
    ));

    engine.close().unwrap();
}

#[test]
fn pattern_edge_property_equality_uses_edge_anchor_when_selective() {
    let (_dir, engine) = query_test_engine();
    let mut sources = Vec::new();
    let mut targets = Vec::new();
    for index in 0..256 {
        sources.push(insert_query_node(&engine, "NodeLabel101",
            &format!("edge-anchor-eq-source-{index}"),
            &[],
            1.0,
        ));
        targets.push(insert_query_node(&engine, "NodeLabel102",
            &format!("edge-anchor-eq-target-{index}"),
            &[],
            1.0,
        ));
    }

    let hot_edge = engine
        .upsert_edge(
            sources[7],
            targets[9],
            "EDGE_LABEL_201",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    for index in 0..256 {
        engine
            .upsert_edge(
                sources[index],
                targets[index],
                "EDGE_LABEL_201",
                UpsertEdgeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("cold".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_201", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut edge = pattern_edge(Some("rel"), "source", "target", Direction::Outgoing, Some(vec!["EDGE_LABEL_201"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("source", Some("NodeLabel101"), Vec::new()),
            pattern_node("target", Some("NodeLabel102"), Vec::new()),
        ],
        vec![edge],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(!plan.warnings.contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(!plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("source", sources[7]), ("target", targets[9])],
            &[("rel", hot_edge)]
        )]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.endpoint_adjacency_candidates, 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn pattern_edge_property_range_uses_edge_anchor_when_selective() {
    let (_dir, engine) = query_test_engine();
    let left = insert_query_node(&engine, "NodeLabel103",  "edge-anchor-range-left", &[], 1.0);
    let mut keep_target = 0;
    let mut keep_edge = 0;
    for index in 0..128 {
        let target = insert_query_node(&engine, "NodeLabel104",
            &format!("edge-anchor-range-target-{index}"),
            &[],
            1.0,
        );
        let score = if index == 77 { 1_000 } else { index as i64 };
        let edge_id = engine
            .upsert_edge(
                left,
                target,
                "EDGE_LABEL_202",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", PropValue::Int(score))]),
                    ..Default::default()
                },
            )
            .unwrap();
        if index == 77 {
            keep_target = target;
            keep_edge = edge_id;
        }
    }
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_202",
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut edge = pattern_edge(Some("rel"), "left", "right", Direction::Outgoing, Some(vec!["EDGE_LABEL_202"]));
    edge.filter = Some(EdgeFilterExpr::PropertyRange {
        key: "score".to_string(),
        lower: Some(PropertyRangeBound::Included(PropValue::Int(1_000))),
        upper: Some(PropertyRangeBound::Included(PropValue::Int(1_000))),
    });
    let query = pattern_query(
        vec![
            pattern_node_with_ids("left", vec![left]),
            pattern_node("right", Some("NodeLabel104"), Vec::new()),
        ],
        vec![edge],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("left", left), ("right", keep_target)],
            &[("rel", keep_edge)]
        )]
    );
}

#[test]
fn pattern_edge_anchor_direction_both_and_self_loop_bind_correctly() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel105",  "edge-anchor-dir-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel105",  "edge-anchor-dir-b", &[], 1.0);
    let ab = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_203",
            UpsertEdgeOptions {
                props: query_test_props(&[("kind", PropValue::String("keep".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let aa = engine
        .upsert_edge(
            a,
            a,
            "EDGE_LABEL_203",
            UpsertEdgeOptions {
                props: query_test_props(&[("kind", PropValue::String("loop".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_203", "kind", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut outgoing = pattern_edge(Some("e"), "left", "right", Direction::Outgoing, Some(vec!["EDGE_LABEL_203"]));
    outgoing.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    let outgoing_query = pattern_query(
        vec![
            pattern_node("left", Some("NodeLabel105"), Vec::new()),
            pattern_node("right", Some("NodeLabel105"), Vec::new()),
        ],
        vec![outgoing],
    );
    assert_eq!(
        engine.query_pattern(&outgoing_query).unwrap().matches,
        vec![expected_match(&[("left", a), ("right", b)], &[("e", ab)])]
    );

    let mut incoming = pattern_edge(Some("e"), "left", "right", Direction::Incoming, Some(vec!["EDGE_LABEL_203"]));
    incoming.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    let incoming_query = pattern_query(
        vec![
            pattern_node("left", Some("NodeLabel105"), Vec::new()),
            pattern_node("right", Some("NodeLabel105"), Vec::new()),
        ],
        vec![incoming],
    );
    assert_eq!(
        engine.query_pattern(&incoming_query).unwrap().matches,
        vec![expected_match(&[("left", b), ("right", a)], &[("e", ab)])]
    );

    let mut both = pattern_edge(Some("e"), "left", "right", Direction::Both, Some(vec!["EDGE_LABEL_203"]));
    both.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    let both_query = pattern_query(
        vec![
            pattern_node("left", Some("NodeLabel105"), Vec::new()),
            pattern_node("right", Some("NodeLabel105"), Vec::new()),
        ],
        vec![both],
    );
    assert_eq!(
        engine.query_pattern(&both_query).unwrap().matches,
        vec![
            expected_match(&[("left", a), ("right", b)], &[("e", ab)]),
            expected_match(&[("left", b), ("right", a)], &[("e", ab)]),
        ]
    );

    let mut loop_edge = pattern_edge(Some("loop"), "same", "same", Direction::Both, Some(vec!["EDGE_LABEL_203"]));
    loop_edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "kind".to_string(),
        value: PropValue::String("loop".to_string()),
    });
    let loop_query = pattern_query(
        vec![pattern_node("same", Some("NodeLabel105"), Vec::new())],
        vec![loop_edge],
    );
    assert_eq!(
        engine.query_pattern(&loop_query).unwrap().matches,
        vec![expected_match(&[("same", a)], &[("loop", aa)])]
    );
}

#[test]
fn pattern_edge_anchor_verifies_endpoint_node_filters_after_binding() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "NodeLabel106",  "edge-anchor-node-filter-source", &[], 1.0);
    let keep = insert_query_node(&engine, "NodeLabel107",
        "edge-anchor-node-filter-keep",
        &[("state", PropValue::String("ok".to_string()))],
        1.0,
    );
    let drop = insert_query_node(&engine, "NodeLabel107",
        "edge-anchor-node-filter-drop",
        &[("state", PropValue::String("drop".to_string()))],
        1.0,
    );
    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "EDGE_LABEL_204",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            drop,
            "EDGE_LABEL_204",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_204", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut edge = pattern_edge(Some("rel"), "source", "target", Direction::Outgoing, Some(vec!["EDGE_LABEL_204"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("source", Some("NodeLabel106"), Vec::new()),
            pattern_node("target", Some("NodeLabel107"),
                vec![NodeFilterExpr::PropertyEquals {
                    key: "state".to_string(),
                    value: PropValue::String("ok".to_string()),
                }],
            ),
        ],
        vec![edge],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("rel", keep_edge)]
        )]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert!(counters.node_record_hydration_reads > 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn pattern_metadata_edge_anchor_uses_metadata_without_property_hydration() {
    let (_dir, engine) = query_test_engine();
    let left = insert_query_node(&engine, "NodeLabel108",  "edge-anchor-meta-left", &[], 1.0);
    let keep = insert_query_node(&engine, "NodeLabel109",  "edge-anchor-meta-keep", &[], 1.0);
    let drop = insert_query_node(&engine, "NodeLabel109",  "edge-anchor-meta-drop", &[], 1.0);
    let keep_edge = engine
        .upsert_edge(
            left,
            keep,
            "EDGE_LABEL_205",
            UpsertEdgeOptions {
                weight: 0.25,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            left,
            drop,
            "EDGE_LABEL_205",
            UpsertEdgeOptions {
                weight: 9.0,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let mut edge = pattern_edge(Some("rel"), "left", "right", Direction::Outgoing, Some(vec!["EDGE_LABEL_205"]));
    edge.filter = Some(EdgeFilterExpr::WeightRange {
        lower: None,
        upper: Some(1.0),
    });
    let query = pattern_query(
        vec![
            pattern_node("left", Some("NodeLabel108"), Vec::new()),
            pattern_node("right", Some("NodeLabel109"), Vec::new()),
        ],
        vec![edge],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(
        plan_contains_node(&plan.root, &QueryPlanNode::EdgeWeightIndex)
            || plan_contains_node(&plan.root, &QueryPlanNode::EdgeMetadataScan)
    );
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("left", left), ("right", keep)],
            &[("rel", keep_edge)]
        )]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn pattern_label_only_edge_anchor_binds_unanchored_aliases() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "SearchNode120",  "label-only-anchor-a", &[], 1.0);
    let b = insert_query_node(&engine, "SearchNode120",  "label-only-anchor-b", &[], 1.0);
    let c = insert_query_node(&engine, "SearchNode120",  "label-only-anchor-c", &[], 1.0);
    let d = insert_query_node(&engine, "SearchNode120",  "label-only-anchor-d", &[], 1.0);
    let ab = engine.upsert_edge(a, b, "EDGE_LABEL_215", UpsertEdgeOptions::default()).unwrap();
    let cd = engine.upsert_edge(c, d, "EDGE_LABEL_215", UpsertEdgeOptions::default()).unwrap();
    engine.upsert_edge(a, d, "EDGE_LABEL_216", UpsertEdgeOptions::default()).unwrap();

    let query = pattern_query(
        vec![
            pattern_node("from", None, Vec::new()),
            pattern_node("to", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("e"),
            "from",
            "to",
            Direction::Outgoing,
            Some(vec!["EDGE_LABEL_215"]),
        )],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeLabelIndex));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("from", a), ("to", b)], &[("e", ab)]),
            expected_match(&[("from", c), ("to", d)], &[("e", cd)]),
        ]
    );
}

#[test]
fn pattern_broad_label_only_edge_anchor_is_rejected_when_over_cap() {
    let (_dir, engine) = query_test_engine();
    let edge_count = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 1;
    let nodes = (0..=edge_count)
        .map(|index| NodeInput {
            labels: vec!["SearchNode120".to_string()],
            key: format!("broad-edge-label-pattern-node-{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub = node_ids[0];
    let edges = node_ids[1..]
        .iter()
        .map(|target| EdgeInput {
            from: hub,
            to: *target,
            label: "EDGE_LABEL_216".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    engine.batch_upsert_edges(edges).unwrap();

    let query = pattern_query(
        vec![
            pattern_node("from", None, Vec::new()),
            pattern_node("to", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("e"),
            "from",
            "to",
            Direction::Outgoing,
            Some(vec!["EDGE_LABEL_216"]),
        )],
    );

    assert!(matches!(
        engine.explain_pattern_query(&query),
        Err(EngineError::InvalidOperation(message))
            if message.contains("anchorable node pattern or edge pattern")
    ));
}

#[test]
fn pattern_edge_anchor_expands_branching_pattern_from_both_endpoints() {
    let (_dir, engine) = query_test_engine();
    let left = insert_query_node(&engine, "NodeLabel121",  "branch-edge-anchor-left", &[], 1.0);
    let right = insert_query_node(&engine, "NodeLabel122",  "branch-edge-anchor-right", &[], 1.0);
    let left_leaf = insert_query_node(&engine, "NodeLabel123",  "branch-edge-anchor-left-leaf", &[], 1.0);
    let right_leaf = insert_query_node(&engine, "NodeLabel124",  "branch-edge-anchor-right-leaf", &[], 1.0);
    let anchor_edge = engine
        .upsert_edge(
            left,
            right,
            "EDGE_LABEL_217",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let left_edge = engine
        .upsert_edge(left, left_leaf, "EDGE_LABEL_218", UpsertEdgeOptions::default())
        .unwrap();
    let right_edge = engine
        .upsert_edge(right, right_leaf, "EDGE_LABEL_219", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_217", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut anchor = pattern_edge(Some("anchor"), "left", "right", Direction::Outgoing, Some(vec!["EDGE_LABEL_217"]));
    anchor.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("left", None, Vec::new()),
            pattern_node("right", None, Vec::new()),
            pattern_node("left_leaf", None, Vec::new()),
            pattern_node("right_leaf", None, Vec::new()),
        ],
        vec![
            anchor,
            pattern_edge(
                Some("left_edge"),
                "left",
                "left_leaf",
                Direction::Outgoing,
                Some(vec!["EDGE_LABEL_218"]),
            ),
            pattern_edge(
                Some("right_edge"),
                "right",
                "right_leaf",
                Direction::Outgoing,
                Some(vec!["EDGE_LABEL_219"]),
            ),
        ],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[
                ("left", left),
                ("right", right),
                ("left_leaf", left_leaf),
                ("right_leaf", right_leaf),
            ],
            &[
                ("anchor", anchor_edge),
                ("left_edge", left_edge),
                ("right_edge", right_edge),
            ],
        )]
    );
}

#[test]
fn pattern_edge_anchor_remaining_property_filter_uses_projection() {
    let (_dir, engine) = query_test_engine();
    let left = insert_query_node(&engine, "NodeLabel131",  "branch-projection-left", &[], 1.0);
    let mid = insert_query_node(&engine, "NodeLabel132",  "branch-projection-mid", &[], 1.0);
    let keep = insert_query_node(&engine, "NodeLabel133",  "branch-projection-keep", &[], 1.0);
    let drop = insert_query_node(&engine, "NodeLabel133",  "branch-projection-drop", &[], 1.0);
    let anchor_edge = engine
        .upsert_edge(
            left,
            mid,
            "EDGE_LABEL_226",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let keep_edge = engine
        .upsert_edge(
            mid,
            keep,
            "EDGE_LABEL_227",
            UpsertEdgeOptions {
                props: query_test_props(&[("role", PropValue::String("keep".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            mid,
            drop,
            "EDGE_LABEL_227",
            UpsertEdgeOptions {
                props: query_test_props(&[("role", PropValue::String("drop".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_226", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut anchor = pattern_edge(Some("anchor"), "left", "mid", Direction::Outgoing, Some(vec!["EDGE_LABEL_226"]));
    anchor.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let mut remaining = pattern_edge(Some("remaining"), "mid", "leaf", Direction::Outgoing, Some(vec!["EDGE_LABEL_227"]));
    remaining.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "role".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("left", Some("NodeLabel131"), Vec::new()),
            pattern_node("mid", Some("NodeLabel132"), Vec::new()),
            pattern_node("leaf", Some("NodeLabel133"), Vec::new()),
        ],
        vec![anchor, remaining],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("left", left), ("mid", mid), ("leaf", keep)],
            &[("anchor", anchor_edge), ("remaining", keep_edge)]
        )]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
}

#[test]
fn pattern_edge_exists_and_missing_filters_use_label_anchor_only() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel125",  "edge-exists-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel125",  "edge-exists-b", &[], 1.0);
    let c = insert_query_node(&engine, "NodeLabel125",  "edge-exists-c", &[], 1.0);
    let present = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_220",
            UpsertEdgeOptions {
                props: query_test_props(&[("flag", PropValue::String("yes".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let missing = engine.upsert_edge(a, c, "EDGE_LABEL_220", UpsertEdgeOptions::default()).unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_220", "flag", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    for (filter, expected_edge, expected_target) in [
        (
            EdgeFilterExpr::PropertyExists {
                key: "flag".to_string(),
            },
            present,
            b,
        ),
        (
            EdgeFilterExpr::PropertyMissing {
                key: "flag".to_string(),
            },
            missing,
            c,
        ),
    ] {
        let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_220"]));
        edge.filter = Some(filter);
        let query = pattern_query(
            vec![
                pattern_node("a", None, Vec::new()),
                pattern_node("b", None, Vec::new()),
            ],
            vec![edge],
        );
        let plan = engine.explain_pattern_query(&query).unwrap();
        assert!(plan_contains_pattern_edge_anchor(&plan.root));
        assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeLabelIndex));
        assert!(!plan_contains_node(
            &plan.root,
            &QueryPlanNode::EdgePropertyEqualityIndex
        ));
        assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
        assert_eq!(
            engine.query_pattern(&query).unwrap().matches,
            vec![expected_match(&[("a", a), ("b", expected_target)], &[("e", expected_edge)])]
        );
    }
}

#[test]
fn pattern_edge_property_unavailable_sidecar_uses_label_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let keep;
    let a;
    let b;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        a = insert_query_node(&engine, "NodeLabel126",  "pattern-corrupt-edge-a", &[], 1.0);
        b = insert_query_node(&engine, "NodeLabel126",  "pattern-corrupt-edge-b", &[], 1.0);
        let c = insert_query_node(&engine, "NodeLabel126",  "pattern-corrupt-edge-c", &[], 1.0);
        keep = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_221",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_221",
                UpsertEdgeOptions {
                    props: query_test_props(&[("status", PropValue::String("cold".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();
        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_221", "status", SecondaryIndexKind::Equality)
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

    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_221"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("a", None, Vec::new()),
            pattern_node("b", None, Vec::new()),
        ],
        vec![edge],
    );
    let plan = reopened.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeLabelIndex));
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert_eq!(
        reopened.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("a", a), ("b", b)], &[("e", keep)])]
    );
}

#[test]
fn pattern_edge_anchor_runtime_sidecar_failure_uses_local_label_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = insert_query_node(&engine, "NodeLabel130",  "pattern-runtime-fallback-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel130",  "pattern-runtime-fallback-b", &[], 1.0);
    let c = insert_query_node(&engine, "NodeLabel130",  "pattern-runtime-fallback-c", &[], 1.0);
    let keep = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_225",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_225",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("cold".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_225", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    let sidecar_path = crate::segment_writer::edge_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, engine.segments_for_test()[0].segment_id),
        info.index_id,
    );

    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_225"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("a", Some("NodeLabel130"), Vec::new()),
            pattern_node("b", Some("NodeLabel130"), Vec::new()),
        ],
        vec![edge],
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));

    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_pattern_query(&query).unwrap();
    let planned = published
        .view
        .plan_normalized_pattern_query(&normalized)
        .unwrap();
    corrupt_sidecar_header_in_place(&sidecar_path);

    engine.reset_query_execution_counters_for_test();
    let outcome = published
        .view
        .query_pattern_planned(&normalized, planned)
        .unwrap();
    assert!(!outcome.followups.is_empty());
    assert_eq!(
        outcome.value.matches,
        vec![expected_match(&[("a", a), ("b", b)], &[("e", keep)])]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.endpoint_adjacency_candidates, 0);
    assert_eq!(counters.public_edge_query_calls, 0);
}

#[test]
fn pattern_edge_property_failed_index_state_uses_label_fallback() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel127",  "pattern-failed-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel127",  "pattern-failed-edge-b", &[], 1.0);
    let c = insert_query_node(&engine, "NodeLabel127",  "pattern-failed-edge-c", &[], 1.0);
    let keep = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_222",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_222",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("cold".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_222", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.shutdown_secondary_index_worker();
    engine
        .with_runtime_manifest_write(|manifest| {
            let entry = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == info.index_id)
                .unwrap();
            entry.state = SecondaryIndexState::Failed;
            entry.last_error = Some("forced pattern fallback".to_string());
            Ok(())
        })
        .unwrap();
    engine.rebuild_secondary_index_catalog().unwrap();

    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_222"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("a", None, Vec::new()),
            pattern_node("b", None, Vec::new()),
        ],
        vec![edge],
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::EdgeLabelIndex));
    assert!(!plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("a", a), ("b", b)], &[("e", keep)])]
    );
}

#[test]
fn pattern_edge_anchor_mixed_sources_dedupes_and_uses_newest_edge_props() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "NodeLabel128",  "pattern-mixed-source", &[], 1.0);
    let target_a = insert_query_node(&engine, "NodeLabel128",  "pattern-mixed-target-a", &[], 1.0);
    let target_b = insert_query_node(&engine, "NodeLabel128",  "pattern-mixed-target-b", &[], 1.0);
    let target_c = insert_query_node(&engine, "NodeLabel128",  "pattern-mixed-target-c", &[], 1.0);
    let edge_a = engine
        .upsert_edge(
            source,
            target_a,
            "EDGE_LABEL_223",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let edge_b = engine
        .upsert_edge(
            source,
            target_b,
            "EDGE_LABEL_223",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_223", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    set_query_edge_props(
        &engine,
        edge_a,
        query_test_props(&[("status", PropValue::String("cold".to_string()))]),
    );
    set_query_edge_props(
        &engine,
        edge_b,
        query_test_props(&[("status", PropValue::String("cold".to_string()))]),
    );
    engine.freeze_memtable().unwrap();
    set_query_edge_props(
        &engine,
        edge_a,
        query_test_props(&[("status", PropValue::String("hot".to_string()))]),
    );
    let edge_c = engine
        .upsert_edge(
            source,
            target_c,
            "EDGE_LABEL_223",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let mut edge = pattern_edge(Some("e"), "source", "target", Direction::Outgoing, Some(vec!["EDGE_LABEL_223"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("source", None, Vec::new()),
            pattern_node("target", None, Vec::new()),
        ],
        vec![edge],
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("source", source), ("target", target_a)], &[("e", edge_a)]),
            expected_match(&[("source", source), ("target", target_c)], &[("e", edge_c)]),
        ]
    );
}

#[test]
fn pattern_edge_anchor_execution_does_not_call_public_edge_queries() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel129",  "pattern-public-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel129",  "pattern-public-edge-b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_224",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_224", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_224"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("a", None, Vec::new()),
            pattern_node("b", None, Vec::new()),
        ],
        vec![edge],
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));

    engine.reset_query_execution_counters_for_test();
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("a", a), ("b", b)], &[("e", edge_id)])]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.public_edge_query_calls, 0);
}

#[test]
fn pattern_node_anchor_still_wins_when_node_filter_is_more_selective() {
    let (_dir, engine) = query_test_engine();
    let mut sources = Vec::new();
    for index in 0..64 {
        sources.push(insert_query_node(&engine, "SearchNode110",
            &format!("node-anchor-still-wins-source-{index}"),
            &[(
                "tenant",
                PropValue::String(if index == 3 { "one" } else { "many" }.to_string()),
            )],
            1.0,
        ));
    }
    let target = insert_query_node(&engine, "NodeLabel111",  "node-anchor-still-wins-target", &[], 1.0);
    let keep_edge = engine
        .upsert_edge(
            sources[3],
            target,
            "EDGE_LABEL_206",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("broad".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    for (index, source) in sources.iter().enumerate() {
        if index == 3 {
            continue;
        }
        engine
            .upsert_edge(
                *source,
                target,
                "EDGE_LABEL_206",
                UpsertEdgeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("broad".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    engine.flush().unwrap();
    let node_index = engine
        .ensure_node_property_index("SearchNode110", "tenant", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, node_index.index_id, SecondaryIndexState::Ready);
    let edge_index = engine
        .ensure_edge_property_index("EDGE_LABEL_206", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, edge_index.index_id, SecondaryIndexState::Ready);

    let mut edge = pattern_edge(Some("rel"), "source", "target", Direction::Outgoing, Some(vec!["EDGE_LABEL_206"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("broad".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("source", Some("SearchNode110"),
                vec![NodeFilterExpr::PropertyEquals {
                    key: "tenant".to_string(),
                    value: PropValue::String("one".to_string()),
                }],
            ),
            pattern_node("target", Some("NodeLabel111"), Vec::new()),
        ],
        vec![edge],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(!plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(&plan.root, &QueryPlanNode::PropertyEqualityIndex));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("source", sources[3]), ("target", target)],
            &[("rel", keep_edge)]
        )]
    );
}

#[test]
fn pattern_edge_property_in_anchor_preserves_signed_zero() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel112",  "pattern-in-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel112",  "pattern-in-b", &[], 1.0);
    let c = insert_query_node(&engine, "NodeLabel112",  "pattern-in-c", &[], 1.0);
    let d = insert_query_node(&engine, "NodeLabel112",  "pattern-in-d", &[], 1.0);
    let positive_zero = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_207",
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
            "EDGE_LABEL_207",
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
            "EDGE_LABEL_207",
            UpsertEdgeOptions {
                props: query_test_props(&[("z", PropValue::Float(1.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    for index in 0..12 {
        engine
            .upsert_edge(
                b,
                d,
                "EDGE_LABEL_207",
                UpsertEdgeOptions {
                    props: query_test_props(&[("z", PropValue::Float(index as f64 + 2.0))]),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_207", "z", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_207"]));
    edge.filter = Some(EdgeFilterExpr::PropertyIn {
        key: "z".to_string(),
        values: vec![PropValue::Float(-0.0), PropValue::Float(0.0)],
    });
    let query = pattern_query(
        vec![
            pattern_node("a", Some("NodeLabel112"), Vec::new()),
            pattern_node("b", Some("NodeLabel112"), Vec::new()),
        ],
        vec![edge],
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("a", a), ("b", b)], &[("e", positive_zero)]),
            expected_match(&[("a", a), ("b", c)], &[("e", negative_zero)]),
        ]
    );
}

#[test]
fn pattern_edge_property_equality_anchor_verifies_hash_collisions() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let segment_id;
    let red_one;
    let red_two;
    let blue;
    let a;
    let b;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        a = insert_query_node(&engine, "NodeLabel113",  "pattern-collision-a", &[], 1.0);
        b = insert_query_node(&engine, "NodeLabel113",  "pattern-collision-b", &[], 1.0);
        let c = insert_query_node(&engine, "NodeLabel113",  "pattern-collision-c", &[], 1.0);
        let d = insert_query_node(&engine, "NodeLabel113",  "pattern-collision-d", &[], 1.0);
        red_one = engine
            .upsert_edge(
                a,
                b,
                "EDGE_LABEL_208",
                UpsertEdgeOptions {
                    props: query_test_props(&[("color", PropValue::String("red".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        red_two = engine
            .upsert_edge(
                a,
                c,
                "EDGE_LABEL_208",
                UpsertEdgeOptions {
                    props: query_test_props(&[("color", PropValue::String("red".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        blue = engine
            .upsert_edge(
                a,
                d,
                "EDGE_LABEL_208",
                UpsertEdgeOptions {
                    props: query_test_props(&[("color", PropValue::String("blue".to_string()))]),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();
        let info = engine
            .ensure_edge_property_index("EDGE_LABEL_208", "color", SecondaryIndexKind::Equality)
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
        hash_prop_value(&PropValue::String("red".to_string())),
        red_two,
        blue,
    );

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_208"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "color".to_string(),
        value: PropValue::String("red".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("a", Some("NodeLabel113"), Vec::new()),
            pattern_node("b", Some("NodeLabel113"), Vec::new()),
        ],
        vec![edge],
    );
    let plan = reopened.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert!(plan_contains_node(
        &plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(
        reopened.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("a", a), ("b", b)], &[("e", red_one)])]
    );
}

#[test]
fn pattern_edge_range_anchor_keeps_numeric_domains_exact() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel114",  "pattern-domain-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel114",  "pattern-domain-b", &[], 1.0);
    let c = insert_query_node(&engine, "NodeLabel114",  "pattern-domain-c", &[], 1.0);
    let d = insert_query_node(&engine, "NodeLabel114",  "pattern-domain-d", &[], 1.0);

    let int_edge = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_209",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::Int(5))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_209",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::UInt(5))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            d,
            "EDGE_LABEL_209",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::Float(5.0))]),
                ..Default::default()
            },
        )
        .unwrap();
    let uint_edge = engine
        .upsert_edge(
            b,
            c,
            "EDGE_LABEL_210",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::UInt(7))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            b,
            d,
            "EDGE_LABEL_210",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::Int(7))]),
                ..Default::default()
            },
        )
        .unwrap();
    let float_edge = engine
        .upsert_edge(
            c,
            d,
            "EDGE_LABEL_211",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::Float(9.5))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            c,
            a,
            "EDGE_LABEL_211",
            UpsertEdgeOptions {
                props: query_test_props(&[("metric", PropValue::Int(9))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    for (label, domain) in [
        ("EDGE_LABEL_209", SecondaryIndexRangeDomain::Int),
        ("EDGE_LABEL_210", SecondaryIndexRangeDomain::UInt),
        ("EDGE_LABEL_211", SecondaryIndexRangeDomain::Float),
    ] {
        let info = engine
            .ensure_edge_property_index(
                label,
                "metric",
                SecondaryIndexKind::Range { domain },
            )
            .unwrap();
        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    }

    let cases = [
        ("EDGE_LABEL_209", PropValue::Int(5), int_edge),
        ("EDGE_LABEL_210", PropValue::UInt(7), uint_edge),
        ("EDGE_LABEL_211", PropValue::Float(9.5), float_edge),
    ];
    for (label, value, expected_edge) in cases {
        let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec![label]));
        edge.filter = Some(EdgeFilterExpr::PropertyRange {
            key: "metric".to_string(),
            lower: Some(PropertyRangeBound::Included(value.clone())),
            upper: Some(PropertyRangeBound::Included(value)),
        });
        let query = pattern_query(
            vec![
                pattern_node("a", Some("NodeLabel114"), Vec::new()),
                pattern_node("b", Some("NodeLabel114"), Vec::new()),
            ],
            vec![edge],
        );
        let result = engine.query_pattern(&query).unwrap();
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].edges["e"], expected_edge);
    }
}

#[test]
fn pattern_edge_or_and_not_use_bounded_fallback_without_partial_index_results() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "NodeLabel115",  "pattern-or-a", &[], 1.0);
    let b = insert_query_node(&engine, "NodeLabel115",  "pattern-or-b", &[], 1.0);
    let c = insert_query_node(&engine, "NodeLabel115",  "pattern-or-c", &[], 1.0);
    let d = insert_query_node(&engine, "NodeLabel115",  "pattern-or-d", &[], 1.0);
    let hot = engine
        .upsert_edge(
            a,
            b,
            "EDGE_LABEL_212",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let archived = engine
        .upsert_edge(
            a,
            c,
            "EDGE_LABEL_212",
            UpsertEdgeOptions {
                props: query_test_props(&[("flag", PropValue::String("archived".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let cold = engine
        .upsert_edge(
            a,
            d,
            "EDGE_LABEL_212",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("cold".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_212", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut or_edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_212"]));
    or_edge.filter = Some(EdgeFilterExpr::Or(vec![
        EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        },
        EdgeFilterExpr::PropertyEquals {
            key: "flag".to_string(),
            value: PropValue::String("archived".to_string()),
        },
    ]));
    let or_query = pattern_query(
        vec![
            pattern_node("a", Some("NodeLabel115"), Vec::new()),
            pattern_node("b", Some("NodeLabel115"), Vec::new()),
        ],
        vec![or_edge],
    );
    let or_plan = engine.explain_pattern_query(&or_query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&or_plan.root));
    assert!(!plan_contains_node(
        &or_plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(or_plan
        .warnings
        .contains(&QueryPlanWarning::BooleanBranchFallback));
    assert_eq!(
        engine.query_pattern(&or_query).unwrap().matches,
        vec![
            expected_match(&[("a", a), ("b", b)], &[("e", hot)]),
            expected_match(&[("a", a), ("b", c)], &[("e", archived)]),
        ]
    );

    let mut not_edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_212"]));
    not_edge.filter = Some(EdgeFilterExpr::Not(Box::new(
        EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        },
    )));
    let not_query = pattern_query(
        vec![
            pattern_node("a", Some("NodeLabel115"), Vec::new()),
            pattern_node("b", Some("NodeLabel115"), Vec::new()),
        ],
        vec![not_edge],
    );
    let not_plan = engine.explain_pattern_query(&not_query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&not_plan.root));
    assert!(!plan_contains_node(
        &not_plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert_eq!(
        engine.query_pattern(&not_query).unwrap().matches,
        vec![
            expected_match(&[("a", a), ("b", c)], &[("e", archived)]),
            expected_match(&[("a", a), ("b", d)], &[("e", cold)]),
        ]
    );
}

#[test]
fn pattern_edge_anchor_excludes_deleted_and_prune_hidden_endpoints() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let source = insert_query_node(&engine, "NodeLabel116",  "pattern-visible-source", &[], 1.0);
    let keep = insert_query_node(&engine, "SearchNode117",  "pattern-visible-keep", &[], 1.0);
    let hidden = insert_query_node(&engine, "SearchNode117",  "pattern-visible-hidden", &[], 0.1);
    let deleted_target = insert_query_node(&engine, "SearchNode117",  "pattern-visible-deleted", &[], 1.0);
    let keep_edge = engine
        .upsert_edge(
            source,
            keep,
            "EDGE_LABEL_213",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            hidden,
            "EDGE_LABEL_213",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let deleted_edge = engine
        .upsert_edge(
            source,
            deleted_target,
            "EDGE_LABEL_213",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_213", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    engine.delete_edge(deleted_edge).unwrap();
    engine
        .set_prune_policy(
            "low-weight-targets",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("SearchNode117".to_string()),
            },
        )
        .unwrap();

    let mut edge = pattern_edge(Some("e"), "source", "target", Direction::Outgoing, Some(vec!["EDGE_LABEL_213"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("source", Some("NodeLabel116"), Vec::new()),
            pattern_node("target", Some("SearchNode117"), Vec::new()),
        ],
        vec![edge],
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&plan.root));
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("source", source), ("target", keep)],
            &[("e", keep_edge)]
        )]
    );
}

#[test]
fn pattern_edge_anchor_preserves_logical_order_and_truncation() {
    let (_dir, engine) = query_test_engine();
    let a1 = insert_query_node(&engine, "NodeLabel118",  "order-a1", &[], 1.0);
    let a2 = insert_query_node(&engine, "NodeLabel118",  "order-a2", &[], 1.0);
    let b1 = insert_query_node(&engine, "NodeLabel119",  "order-b1", &[], 1.0);
    let b2 = insert_query_node(&engine, "NodeLabel119",  "order-b2", &[], 1.0);
    let e2 = engine
        .upsert_edge(
            a2,
            b2,
            "EDGE_LABEL_214",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let e1 = engine
        .upsert_edge(
            a1,
            b1,
            "EDGE_LABEL_214",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("hot".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let mut edge = pattern_edge(Some("e"), "a", "b", Direction::Outgoing, Some(vec!["EDGE_LABEL_214"]));
    edge.filter = Some(EdgeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("hot".to_string()),
    });
    let query = pattern_query(
        vec![
            pattern_node("a", Some("NodeLabel118"), Vec::new()),
            pattern_node("b", Some("NodeLabel119"), Vec::new()),
        ],
        vec![edge.clone()],
    );
    let node_anchor_matches = engine.query_pattern(&query).unwrap().matches;

    engine.flush().unwrap();
    let info = engine
        .ensure_edge_property_index("EDGE_LABEL_214", "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    let edge_anchor_plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan_contains_pattern_edge_anchor(&edge_anchor_plan.root));
    assert_eq!(engine.query_pattern(&query).unwrap().matches, node_anchor_matches);

    let limited = GraphPatternQuery {
        limit: 1,
        ..query
    };
    let limited_result = engine.query_pattern(&limited).unwrap();
    assert_eq!(
        limited_result.matches,
        vec![expected_match(&[("a", a1), ("b", b1)], &[("e", e1)])]
    );
    assert!(limited_result.truncated);
    assert_ne!(e1, e2);
}

#[test]
fn test_query_pattern_visibility_temporal_delete_prune_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let anchor;
    let keep;
    let hidden;
    let expired;
    let deleted;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        anchor = insert_query_node(&engine, "Person",  "anchor", &[], 1.0);
        keep = insert_query_node(&engine, "Company",  "keep", &[], 1.0);
        hidden = insert_query_node(&engine, "Company",  "hidden", &[], 0.1);
        expired = insert_query_node(&engine, "Company",  "expired", &[], 1.0);
        deleted = insert_query_node(&engine, "Company",  "deleted", &[], 1.0);

        engine
            .upsert_edge(
                anchor,
                keep,
                "KNOWS",
                UpsertEdgeOptions {
                    valid_from: Some(100),
                    valid_to: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                anchor,
                hidden,
                "KNOWS",
                UpsertEdgeOptions {
                    valid_from: Some(100),
                    valid_to: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                anchor,
                expired,
                "KNOWS",
                UpsertEdgeOptions {
                    valid_from: Some(0),
                    valid_to: Some(50),
                    ..Default::default()
                },
            )
            .unwrap();
        let deleted_edge = engine
            .upsert_edge(
                anchor,
                deleted,
                "KNOWS",
                UpsertEdgeOptions {
                    valid_from: Some(100),
                    valid_to: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.delete_edge(deleted_edge).unwrap();
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    label: Some("Company".to_string()),
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let query = GraphPatternQuery {
            at_epoch: Some(150),
            ..pattern_query(
                vec![pattern_node_with_ids("anchor", vec![anchor]), pattern_node("target", Some("Company"), Vec::new())],
                vec![pattern_edge(None, "anchor", "target", Direction::Outgoing, Some(vec!["KNOWS"]))],
            )
        };
        assert_eq!(
            engine.query_pattern(&query).unwrap().matches,
            vec![expected_match(&[("anchor", anchor), ("target", keep)], &[])]
        );

        let expired_query = GraphPatternQuery {
            at_epoch: Some(400),
            ..query.clone()
        };
        assert!(engine
            .query_pattern(&expired_query)
            .unwrap()
            .matches
            .is_empty());

        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = GraphPatternQuery {
        at_epoch: Some(150),
        ..pattern_query(
            vec![pattern_node_with_ids("anchor", vec![anchor]), pattern_node("target", Some("Company"), Vec::new())],
            vec![pattern_edge(None, "anchor", "target", Direction::Outgoing, Some(vec!["KNOWS"]))],
        )
    };
    assert_eq!(
        reopened.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("anchor", anchor), ("target", keep)], &[])]
    );
    assert!(reopened.get_node(hidden).unwrap().is_none());
    assert!(reopened.get_node(expired).unwrap().is_some());
    assert!(reopened.get_node(deleted).unwrap().is_some());
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
            .ensure_node_property_index("Person", key, SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "tenant", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "tenant", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
            warnings: Vec::new(),
        };
        let policy_cutoffs = published.view.query_policy_cutoffs();
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert!(followups.is_empty());
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
            warnings: Vec::new(),
        };
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
        .ensure_node_property_index("Person", "status", SecondaryIndexKind::Equality)
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
    assert!(counters.final_verifier_record_reads >= all_ids.len());

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
                hash_prop_value(&PropValue::String("x".to_string()))
            )
        );
    }

    engine.close().unwrap();
}

#[test]
fn test_query_empty_label_filter_matches_unconstrained_and_segment_unnamed_parallel_edges_dedup()
{
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: false,
            ..DbOptions::default()
        },
    )
    .unwrap();

    let source = insert_query_node(&engine, "Person",  "source", &[], 1.0);
    let target = insert_query_node(&engine, "Company",  "target", &[], 1.0);

    let edges: Vec<EdgeInput> = (0..16)
        .map(|_| EdgeInput {
            from: source,
            to: target,
            label: "KNOWS".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(edges).unwrap();
    engine.flush().unwrap();

    let unconstrained = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![pattern_edge(
            None,
            "source",
            "target",
            Direction::Outgoing,
            Some(Vec::new()),
        )],
    );
    let result = engine.query_pattern(&unconstrained).unwrap();
    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", target)],
            &[]
        )]
    );
    assert!(!result.truncated);

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![pattern_edge(
            None,
            "source",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );
    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", target)],
            &[]
        )]
    );
    assert!(!result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_segment_named_parallel_edges_preserve_edge_matches() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: false,
            ..DbOptions::default()
        },
    )
    .unwrap();

    let source = insert_query_node(&engine, "Person",  "source", &[], 1.0);
    let target = insert_query_node(&engine, "Company",  "target", &[], 1.0);
    let edges = vec![
        EdgeInput {
            from: source,
            to: target,
            label: "KNOWS".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: source,
            to: target,
            label: "KNOWS".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        },
    ];
    let edge_ids = engine.batch_upsert_edges(edges).unwrap();
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some("Company"), Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec!["KNOWS"]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(
                &[("source", source), ("target", target)],
                &[("edge", edge_ids[0])]
            ),
            expected_match(
                &[("source", source), ("target", target)],
                &[("edge", edge_ids[1])]
            ),
        ]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_frontier_budget_rejects_pathological_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, "Person",  "anchor", &[], 1.0);
    let sink = insert_query_node(&engine, "Article",  "sink", &[], 1.0);

    let mid_inputs: Vec<NodeInput> = (0..=PATTERN_FRONTIER_BUDGET)
        .map(|index| NodeInput {
            labels: vec!["Company".to_string()],
            key: format!("mid-{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mids = engine.batch_upsert_nodes(mid_inputs).unwrap();
    let edge_inputs: Vec<EdgeInput> = mids
        .iter()
        .map(|&mid| EdgeInput {
            from: anchor,
            to: mid,
            label: "KNOWS".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(edge_inputs).unwrap();
    engine.ensure_edge_label("BLOCKS").unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node("mid", Some("Company"), Vec::new()),
            pattern_node_with_ids("sink", vec![sink]),
        ],
        vec![
            pattern_edge(None, "anchor", "mid", Direction::Outgoing, Some(vec!["KNOWS"])),
            pattern_edge(None, "mid", "sink", Direction::Outgoing, Some(vec!["BLOCKS"])),
        ],
    );

    let error = engine.query_pattern(&query).unwrap_err();
    assert!(matches!(error, EngineError::InvalidOperation(message) if message.contains("frontier exceeded")));

    engine.close().unwrap();
}
