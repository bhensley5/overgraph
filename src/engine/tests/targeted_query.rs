//! Focused result and work regressions for the authorized CP45d.2 Q-prime composition.
use super::*;
use crate::edge_metadata::{i64_matches_bounds, weight_matches_bounds, RangeBoundFlags};
use crate::memtable::take_query_scan_work_for_test;

fn fixture() -> (TempDir, DatabaseEngine) {
    #[cfg(feature = "production-concurrency")]
    crate::engine::post45_read_tests::ready_participants_for_fixture();
    let dir = TempDir::new().unwrap();
    let db = DatabaseEngine::open(
        dir.path(),
        &DbOptions {
            memtable_flush_threshold: 128 * 1024 * 1024,
            compact_after_n_flushes: 0,
            edge_uniqueness: false,
            ..Default::default()
        },
    )
    .unwrap();
    (dir, db)
}

fn node(id: u64, label: u32) -> NodeRecord {
    NodeRecord {
        id,
        label_ids: NodeLabelSet::single(label).unwrap(),
        key: format!("n{id}"),
        props: BTreeMap::from([("keep".into(), PropValue::Bool(true))]),
        created_at: 1,
        updated_at: id as i64,
        weight: 1.,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: 0,
    }
}

fn edge(id: u64, label: u32, i: i64) -> EdgeRecord {
    EdgeRecord {
        id,
        from: 1,
        to: 2,
        label_id: label,
        props: BTreeMap::new(),
        created_at: 1,
        updated_at: i,
        weight: if i % 19 == 0 {
            f32::NAN
        } else {
            (i % 17) as f32
        },
        valid_from: i + 5,
        valid_to: i + 50,
        last_write_seq: 0,
    }
}

fn finish(db: DatabaseEngine) {
    db.close().unwrap();
    #[cfg(feature = "production-concurrency")]
    {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !matches!(
            db.reclamation_stats().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(std::time::Instant::now() < until, "native drain");
            std::thread::yield_now();
        }
    }
}

#[test]
fn targeted_q_exact_edge_counts_and_timestamp_parity() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    let other = db.ensure_node_label("Other").unwrap();
    let e = db.ensure_edge_label("E").unwrap();
    let other_e = db.ensure_edge_label("OtherE").unwrap();
    let ops: Vec<_> = (1..=4205u64)
        .flat_map(|id| {
            [
                WalOp::UpsertNode(node(id, if id % 2 == 0 { n } else { other })),
                WalOp::UpsertEdge(edge(
                    10000 + id,
                    if id % 2 == 0 { e } else { other_e },
                    id as i64,
                )),
            ]
        })
        .collect();
    db.write_op_batch(&ops).unwrap();
    let old = db.runtime.published.load_full().view.clone();
    let timestamp_query = NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec!["N".into()],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(0),
            upper_ms: Some(5000),
        }),
        ..Default::default()
    };
    let old_plan = old.explain_node_query(&timestamp_query).unwrap();
    let mut changed = node(2, other);
    changed.updated_at = 9000;
    db.write_op_batch(&[
        WalOp::DeleteNode {
            id: 4,
            deleted_at: 9000,
        },
        WalOp::UpsertNode(changed),
        WalOp::DeleteEdge {
            id: 10002,
            deleted_at: 9000,
        },
        WalOp::UpsertEdge(edge(10004, other_e, 9000)),
    ])
    .unwrap();
    // A future-only suffix exceeds one complete examined-group quantum.
    let future: Vec<_> = (5000..9205u64)
        .flat_map(|id| {
            [
                WalOp::UpsertNode(node(id, n)),
                WalOp::UpsertEdge(edge(20000 + id, e, id as i64)),
            ]
        })
        .collect();
    db.write_op_batch(&future).unwrap();
    let current = db.runtime.published.load_full().view.clone();
    assert_eq!(old.explain_node_query(&timestamp_query).unwrap(), old_plan);

    for view in [&old, &current] {
        for label in [n, other, u32::MAX] {
            for (from, to) in [
                (0, i64::MAX),
                (2, 2),
                (0, 0),
                (5000, 2),
                (i64::MIN, i64::MAX),
            ] {
                let mut expected = 0usize;
                let _ = view
                    .memtable
                    .for_each_visible_node_by_time_range_at(
                        label,
                        from,
                        to,
                        view.snapshot_seq,
                        &mut |_| {
                            expected += 1;
                            ControlFlow::Continue(())
                        },
                    )
                    .unwrap();
                take_query_scan_work_for_test();
                assert_eq!(
                    view.memtable
                        .visible_node_time_range_count_at(label, from, to, view.snapshot_seq)
                        .unwrap(),
                    expected
                );
            }
        }
        let mut metadata = Vec::new();
        let _ = view
            .memtable
            .for_each_edge_metadata_at(view.snapshot_seq, |meta| {
                metadata.push(meta);
                ControlFlow::Continue(())
            })
            .unwrap();
        for label in [None, Some(e), Some(other_e), Some(u32::MAX)] {
            for inclusive in [false, true] {
                let weight = RangeBoundFlags {
                    lower: Some(3.),
                    upper: Some(8.),
                    lower_inclusive: inclusive,
                    upper_inclusive: inclusive,
                };
                let pred = |meta: EdgeMetadataCandidate| {
                    label.is_none_or(|l| meta.label_id == l)
                        && weight_matches_bounds(meta.weight, weight)
                };
                take_query_scan_work_for_test();
                let expected = metadata.iter().copied().filter(|&meta| pred(meta)).count() as u64;
                assert_eq!(
                    view.memtable
                        .count_edge_metadata_at(view.snapshot_seq, pred)
                        .unwrap(),
                    expected
                );
                let work = take_query_scan_work_for_test();
                assert_eq!(work.edge_transport_records, 0);
                #[cfg(feature = "production-concurrency")]
                assert!(
                    work.edge_count_refills >= 3,
                    "future-only suffix must continue: {work:?}"
                );
                assert_eq!(
                    view.edge_weight_range_estimate(label, weight, true)
                        .unwrap(),
                    PlannerEstimate::upper_bound(expected)
                );
                for field in [0, 1, 2] {
                    let bounds = RangeBoundFlags {
                        lower: Some(5),
                        upper: Some(4100),
                        lower_inclusive: inclusive,
                        upper_inclusive: inclusive,
                    };
                    let value = |meta: EdgeMetadataCandidate| match field {
                        0 => meta.updated_at,
                        1 => meta.valid_from,
                        _ => meta.valid_to,
                    };
                    let pred = |meta: EdgeMetadataCandidate| {
                        label.is_none_or(|l| meta.label_id == l)
                            && i64_matches_bounds(value(meta), bounds)
                    };
                    let expected =
                        metadata.iter().copied().filter(|&meta| pred(meta)).count() as u64;
                    take_query_scan_work_for_test();
                    assert_eq!(
                        view.memtable
                            .count_edge_metadata_at(view.snapshot_seq, pred)
                            .unwrap(),
                        expected
                    );
                    assert_eq!(take_query_scan_work_for_test().edge_transport_records, 0);
                    assert_eq!(
                        view.edge_i64_metadata_range_estimate(
                            label,
                            bounds,
                            true,
                            field != 0,
                            value,
                            |_| None
                        )
                        .unwrap(),
                        PlannerEstimate::upper_bound(expected)
                    );
                }
            }
        }
    }
    take_query_scan_work_for_test();
    assert_eq!(
        current
            .memtable
            .visible_node_time_range_count_at(n, i64::MIN, i64::MAX, current.snapshot_seq)
            .unwrap(),
        6305
    );
    drop(current);
    drop(old);
    finish(db);
}

fn full_scan_query(limit: Option<usize>, after: Option<u64>) -> NodeQuery {
    NodeQuery {
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "keep".into(),
            value: PropValue::Bool(true),
        }),
        page: PageRequest { limit, after },
        allow_full_scan: true,
        ..Default::default()
    }
}

#[test]
fn targeted_q_full_node_limit_and_deep_pages_bound_enumeration_separately() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    let ops: Vec<_> = (1..=4205)
        .map(|id| WalOp::UpsertNode(node(id, n)))
        .collect();
    db.write_op_batch(&ops).unwrap();
    take_query_scan_work_for_test();
    let page = db.query_node_ids(&full_scan_query(Some(1), None)).unwrap();
    assert_eq!((page.items, page.next_cursor), (vec![1], Some(1)));
    #[cfg(feature = "production-concurrency")]
    {
        let work = take_query_scan_work_for_test();
        assert_eq!((work.node_id_refills, work.node_id_copies), (1, 128));
        assert!(
            work.full_node_count_opinions >= 4205 && work.full_node_count_calls >= 1,
            "planning still counts: {work:?}"
        );
    }
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = db
            .query_node_ids(&full_scan_query(Some(137), after))
            .unwrap();
        all.extend(page.items);
        after = page.next_cursor;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(all, (1..=4205).collect::<Vec<_>>());
    for after in [Some(128), Some(4096), Some(u64::MAX)] {
        let page = db.query_node_ids(&full_scan_query(Some(1), after)).unwrap();
        assert_eq!(
            page.items,
            after
                .filter(|id| *id < 4205)
                .map(|id| vec![id + 1])
                .unwrap_or_default()
        );
    }
    finish(db);
}

#[test]
fn targeted_q_full_node_refills_keep_empty_quanta_old_sources_and_bounds() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    let mut ops: Vec<_> = (0..4205)
        .map(|id| WalOp::DeleteNode { id, deleted_at: 1 })
        .collect();
    ops.extend([
        WalOp::UpsertNode(node(5000, n)),
        WalOp::UpsertNode(node(6000, n)),
    ]);
    db.write_op_batch(&ops).unwrap();
    let old = db.runtime.published.load_full().view.clone();
    // Same pinned generation later receives a future-only suffix and a duplicate version.
    let mut future: Vec<_> = (7000..11205)
        .map(|id| WalOp::UpsertNode(node(id, n)))
        .collect();
    future.push(WalOp::UpsertNode(node(5000, n)));
    db.write_op_batch(&future).unwrap();
    let mut ids = Vec::new();
    old.scan_full_node_id_chunks(None, 1, |chunk| {
        ids.extend_from_slice(chunk);
        Ok(ControlFlow::Continue(()))
    })
    .unwrap();
    assert_eq!(ids, vec![5000, 6000]);
    assert_eq!(
        old.query_node_ids_outcome(&full_scan_query(Some(1), None))
            .unwrap()
            .value
            .items,
        vec![5000]
    );
    db.flush().unwrap();
    db.write_op_batch(&[
        WalOp::UpsertNode(node(5000, n)),
        WalOp::DeleteNode {
            id: 6000,
            deleted_at: 9,
        },
    ])
    .unwrap();
    let view = db.runtime.published.load_full().view.clone();
    let mut merged = Vec::new();
    view.scan_full_node_id_chunks(None, 127, |chunk| {
        merged.extend_from_slice(chunk);
        Ok(ControlFlow::Continue(()))
    })
    .unwrap();
    assert!(merged.windows(2).all(|ids| ids[0] < ids[1]));
    assert_eq!(merged.iter().filter(|&&id| id == 5000).count(), 1);
    let page = db
        .query_node_ids(&full_scan_query(Some(2), Some(4999)))
        .unwrap();
    assert_eq!(page.items, vec![5000, 7000]);
    drop(view);
    drop(old);
    finish(db);
}

#[test]
fn targeted_q_node_sources_preserve_negative_opinions_and_duplicate_positions() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    let other = db.ensure_node_label("Other").unwrap();
    db.write_op_batch(
        &(1..=6)
            .map(|id| WalOp::UpsertNode(node(id, n)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.flush().unwrap();
    let segment_view = db.runtime.published.load_full().view.clone();
    let mut rejected = node(1, other);
    rejected
        .props
        .insert("unused".into(), PropValue::Bytes(vec![7; 2 * 1024 * 1024]));
    let mut property_rejected = node(3, n);
    property_rejected
        .props
        .insert("keep".into(), PropValue::Bool(false));
    let mut metadata_accepted = node(4, n);
    metadata_accepted.weight = 2.;
    db.write_op_batch(&[
        WalOp::UpsertNode(rejected),
        WalOp::DeleteNode {
            id: 2,
            deleted_at: 4,
        },
        WalOp::UpsertNode(property_rejected),
        WalOp::UpsertNode(metadata_accepted),
    ])
    .unwrap();
    db.freeze_memtable().unwrap();
    let frozen = db.runtime.published.load_full().view.clone();
    assert_eq!(frozen.immutable_epochs.len(), 1);
    let mut current_three = node(3, n);
    current_three.props.insert(
        "nested".into(),
        PropValue::Map(BTreeMap::from([(
            "v".into(),
            PropValue::Array(vec![PropValue::Int(9)]),
        )])),
    );
    db.write_op(&WalOp::UpsertNode(current_three.clone()))
        .unwrap();
    let view = db.runtime.published.load_full().view.clone();
    db.write_op(&WalOp::UpsertNode(node(9, n))).unwrap();
    let ids = [5, 1, 2, 3, 4, 9, 8, 3, 5, 9, 8];
    let needs = NodeSelectedFieldNeeds {
        key: true,
        created_at: true,
        props: PropertySelection::Keys(vec!["keep".into(), "nested".into()]),
        ..Default::default()
    };
    let visibility = view.sources().find_node_visibility_meta(&ids).unwrap();
    assert!(matches!(visibility[0], NodeVisibilityState::Live(meta) if meta.label_ids.contains(n)));
    assert!(
        matches!(visibility[1], NodeVisibilityState::Live(meta) if meta.label_ids.contains(other))
    );
    assert_eq!(visibility[2], NodeVisibilityState::Deleted);
    for index in [5, 6, 9, 10] {
        assert_eq!(visibility[index], NodeVisibilityState::Missing);
    }
    assert_eq!(visibility[0], visibility[8]);
    assert_eq!(visibility[3], visibility[7]);
    let projected = view
        .sources()
        .find_node_projected_fields(&ids, &needs)
        .unwrap();
    let fields = projected[3].as_ref().unwrap();
    assert_eq!(fields.props, current_three.props);
    assert_eq!(fields.key.as_deref(), Some("n3"));
    assert_eq!(fields.created_at, Some(1));
    assert!(fields.dense_vector.is_none() && fields.sparse_vector.is_none());
    assert!(!projected[1].as_ref().unwrap().props.contains_key("unused"));
    assert_eq!(projected[2], None);
    for index in [5, 6, 9, 10] {
        assert_eq!(projected[index], None);
    }
    assert_eq!(projected[0], projected[8]);
    assert_eq!(projected[3], projected[7]);
    let query = NodeQuery {
        ids: ids.to_vec(),
        label_filter: Some(NodeLabelFilter {
            labels: vec!["N".into()],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "keep".into(),
            value: PropValue::Bool(true),
        }),
        ..Default::default()
    };
    assert_eq!(
        view.query_node_ids_outcome(&query).unwrap().value.items,
        vec![3, 4, 5]
    );
    assert_eq!(
        frozen.query_node_ids_outcome(&query).unwrap().value.items,
        vec![4, 5]
    );
    // A retired old source remains readable while new opinions shadow it.
    assert_eq!(
        segment_view
            .query_node_ids_outcome(&query)
            .unwrap()
            .value
            .items,
        vec![1, 2, 3, 4, 5]
    );
    let old_fields = frozen
        .sources()
        .find_node_projected_fields(&[3], &needs)
        .unwrap();
    assert_eq!(
        old_fields[0].as_ref().unwrap().props["keep"],
        PropValue::Bool(false)
    );
    drop(view);
    drop(frozen);
    drop(segment_view);
    finish(db);
}

#[test]
fn targeted_q_public_node_verifier_late_hydrates_and_rejects_before_copying() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    let mut ops = Vec::new();
    for id in 1..=300 {
        let mut value = node(id, n);
        value.weight = if id % 3 == 0 { 2. } else { 1. };
        value
            .props
            .insert("keep".into(), PropValue::Bool(id % 5 == 0));
        value
            .props
            .insert("unused".into(), PropValue::Bytes(vec![1; 8192]));
        ops.push(WalOp::UpsertNode(value));
    }
    db.write_op_batch(&ops).unwrap();
    let query = NodeQuery {
        ids: (1..=300).collect(),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::WeightRange {
                lower: Some(2.),
                upper: None,
                lower_inclusive: true,
                upper_inclusive: true,
            },
            NodeFilterExpr::PropertyEquals {
                key: "keep".into(),
                value: PropValue::Bool(true),
            },
        ])),
        page: PageRequest {
            limit: Some(1),
            after: Some(270),
        },
        ..Default::default()
    };
    db.reset_query_execution_counters_for_test();
    let page = db.query_nodes(&query).unwrap();
    assert_eq!(
        page.items.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![285]
    );
    assert_eq!(page.next_cursor, Some(285));
    let counters = db.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 1);
    assert_eq!(counters.node_selected_field_ids, 10);
    assert_eq!(
        counters.node_dense_vector_projection_reads + counters.node_sparse_vector_projection_reads,
        0
    );
    let mut boolean_query = query.clone();
    boolean_query.filter = Some(NodeFilterExpr::Or(vec![
        NodeFilterExpr::WeightRange {
            lower: Some(2.),
            upper: None,
            lower_inclusive: true,
            upper_inclusive: true,
        },
        NodeFilterExpr::PropertyEquals {
            key: "keep".into(),
            value: PropValue::Bool(true),
        },
    ]));
    boolean_query.page = PageRequest {
        limit: Some(1),
        after: Some(297),
    };
    db.reset_query_execution_counters_for_test();
    assert_eq!(db.query_node_ids(&boolean_query).unwrap().items, vec![300]);
    let counters = db.query_execution_counter_snapshot_for_test();
    assert_eq!(
        counters.node_selected_field_ids, 2,
        "metadata-true OR branch needs no properties"
    );
    assert_eq!(counters.node_record_hydration_reads, 0);
    finish(db);
}

#[test]
fn targeted_q_public_nested_selected_payload_and_late_hydration_parity() {
    let (_dir, db) = fixture();
    db.ensure_node_label("N").unwrap();
    let props = BTreeMap::from([
        (
            "nested".into(),
            PropValue::Map(BTreeMap::from([(
                "v".into(),
                PropValue::Array((0..256).map(PropValue::Int).collect()),
            )])),
        ),
        ("unused".into(), PropValue::Bytes(vec![1; 8192])),
    ]);
    let ids = db
        .batch_upsert_nodes(
            (0..64)
                .map(|id| NodeInput {
                    labels: vec!["N".into()],
                    key: format!("selected-{id}"),
                    props: props.clone(),
                    weight: 1.,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(ids, (1..=64).collect::<Vec<_>>());
    let query = NodeQuery {
        ids,
        filter: Some(NodeFilterExpr::PropertyExists {
            key: "nested".into(),
        }),
        page: PageRequest {
            limit: None,
            after: None,
        },
        ..Default::default()
    };
    db.reset_query_execution_counters_for_test();
    let page = db.query_node_ids(&query).unwrap();
    assert_eq!(page.items, (1..=64).collect::<Vec<_>>());
    assert_eq!(page.next_cursor, None);
    let counters = db.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_ids, 64);
    db.reset_query_execution_counters_for_test();
    let page = db
        .query_nodes(&NodeQuery {
            page: PageRequest {
                limit: Some(1),
                after: None,
            },
            ..query
        })
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, 1);
    assert_eq!(page.items[0].key, "selected-0");
    assert_eq!(page.items[0].props, props);
    assert_eq!(page.next_cursor, Some(1));
    assert_eq!(
        db.query_execution_counter_snapshot_for_test()
            .node_record_hydration_reads,
        1
    );
    finish(db);
}

#[cfg(feature = "production-concurrency")]
#[test]
fn targeted_q_verification_propagates_participant_refusal_without_empty_success() {
    // Deliberately occupy all process-global participant slots in a child,
    // as the existing collector/refusal tests do. Other parallel fixtures must
    // retain their own participants; this preserves every refusal/retry assertion.
    const CHILD: &str = "OVERGRAPH_TARGETED_Q_PARTICIPANT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::tests::targeted_query::targeted_q_verification_propagates_participant_refusal_without_empty_success",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    db.write_op(&WalOp::UpsertNode(node(1, n))).unwrap();
    let view = db.runtime.published.load_full().view.clone();
    let handle = view.memtable.participant_handle().unwrap();
    let leases = (0..256)
        .map(|_| handle.try_acquire())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let query = NodeQuery {
        ids: vec![1],
        ..Default::default()
    };
    let error = db.query_node_ids(&query).unwrap_err();
    drop(leases);
    assert!(matches!(
        error,
        EngineError::ResourceLimit {
            resource: "participants",
            retryable: true,
            ..
        }
    ));
    assert_eq!(db.query_node_ids(&query).unwrap().items, vec![1]);
    drop(view);
    finish(db);
}

#[cfg(feature = "production-concurrency")]
#[test]
fn targeted_q_full_node_cursor_captures_upper_and_releases_before_callback() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("N").unwrap();
    db.write_op_batch(
        &(0..300)
            .map(|id| WalOp::UpsertNode(node(id, n)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let old = db.runtime.published.load_full().view.clone();
    let mut source = FullScanNodeSource::memtable(&old.memtable, old.snapshot_seq, None).unwrap();
    db.write_op(&WalOp::UpsertNode(node(10000, n))).unwrap();
    old.memtable.take_node_scan_groups_for_test();
    let mut ids = Vec::new();
    while let Some(id) = source.next_id().unwrap() {
        ids.push(id);
    }
    assert_eq!(ids, (0..300).collect::<Vec<_>>());
    assert_eq!(old.memtable.take_node_scan_groups_for_test(), 302);
    // Extreme IDs exercise the adapter directly: the test-only raw WAL writer's
    // ID allocator does not admit MAX (track_id adds one).
    let extreme = db
        .with_core_ref(|core| {
            Memtable::new_candidate_with_catalog(
                core.memtable.participant_handle().unwrap(),
                core.fixed_secondary_catalog.as_ref().unwrap().clone(),
                &core.candidate.as_ref().unwrap().domain,
            )
        })
        .unwrap();
    extreme
        .apply_op_for_test(&WalOp::UpsertNode(node(0, n)), 1)
        .unwrap();
    extreme
        .apply_op_for_test(&WalOp::UpsertNode(node(u64::MAX, n)), 2)
        .unwrap();
    let mut max_source = FullScanNodeSource::memtable(&extreme, 2, Some(u64::MAX - 1)).unwrap();
    assert_eq!(max_source.next_id().unwrap(), Some(u64::MAX));
    assert_eq!(max_source.next_id().unwrap(), None);
    drop(max_source);
    drop(extreme);
    drop(source);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        old.scan_full_node_id_chunks(None, 1, |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(ControlFlow::Break(()))
        })
        .unwrap();
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    db.write_op(&WalOp::UpsertNode(node(301, n))).unwrap();
    release_tx.send(()).unwrap();
    reader.join().unwrap();
    finish(db);
}
