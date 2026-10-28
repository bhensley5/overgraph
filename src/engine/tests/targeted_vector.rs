//! Focused semantics/work checks for the authorized CP45d.2 Stage V.
use super::*;
use crate::row_projection::VectorSelection;

fn fixture() -> (TempDir, DatabaseEngine) {
    fixture_with_open(DatabaseEngine::open)
}

fn fixture_with_open(
    open: fn(&Path, &DbOptions) -> Result<DatabaseEngine, EngineError>,
) -> (TempDir, DatabaseEngine) {
    #[cfg(feature = "production-concurrency")]
    crate::engine::post45_read_tests::ready_participants_for_fixture();
    let dir = TempDir::new().unwrap();
    let db = open(
        dir.path(),
        &DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 2,
                metric: DenseMetric::DotProduct,
                hnsw: HnswConfig::default(),
            }),
            compact_after_n_flushes: 0,
            memtable_flush_threshold: 128 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    (dir, db)
}

fn node(id: u64, label: u32, value: f32) -> NodeRecord {
    NodeRecord {
        id,
        label_ids: NodeLabelSet::single(label).unwrap(),
        key: format!("v{id}"),
        props: BTreeMap::from([("unselected".into(), PropValue::String("x".repeat(64)))]),
        created_at: 1,
        updated_at: 1,
        weight: 1.,
        last_write_seq: 0,
        dense_vector: Some(vec![value, 1.]),
        sparse_vector: Some(vec![(0, value)]),
    }
}

fn finish(db: DatabaseEngine) {
    db.close().unwrap();
    #[cfg(feature = "production-concurrency")]
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !matches!(
            db.reclamation_stats().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
}

fn standalone(db: &DatabaseEngine) -> Memtable {
    #[cfg(feature = "production-concurrency")]
    return db
        .with_core_ref(|core| {
            Memtable::new_candidate_with_catalog(
                core.memtable.participant_handle().unwrap(),
                core.fixed_secondary_catalog.as_ref().unwrap().clone(),
                &core.candidate.as_ref().unwrap().domain,
            )
        })
        .unwrap();
    #[cfg(not(feature = "production-concurrency"))]
    {
        let _ = db;
        Memtable::new()
    }
}

#[test]
fn targeted_v_sorted_requests_keep_duplicates_missing_tombstones_and_selected_fields() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("V").unwrap();
    let m = standalone(&db);
    for (index, id) in [0, 1, 2, 3, u64::MAX].into_iter().enumerate() {
        m.apply_op_for_test(&WalOp::UpsertNode(node(id, n, 1.)), index as u64 + 1)
            .unwrap();
    }
    m.apply_op_for_test(
        &WalOp::DeleteNode {
            id: 0,
            deleted_at: 2,
        },
        6,
    )
    .unwrap();
    for seq in 7..1007 {
        m.apply_op_for_test(&WalOp::UpsertNode(node(2, n, seq as f32)), seq)
            .unwrap();
    }
    m.apply_op_for_test(
        &WalOp::DeleteNode {
            id: 1,
            deleted_at: 3,
        },
        1007,
    )
    .unwrap();
    m.apply_op_for_test(&WalOp::UpsertNode(node(3, n, 99.)), 1008)
        .unwrap();
    m.apply_op_for_test(&WalOp::UpsertNode(node(9, n, 9.)), 1009)
        .unwrap();
    let ids = [0, 0, 1, 2, 2, 3, 4, 4, 9, u64::MAX, u64::MAX];
    for seq in [5, 1007] {
        for selection in [
            VectorSelection::None,
            VectorSelection::Dense,
            VectorSelection::Sparse,
            VectorSelection::Both,
        ] {
            let mut seen = Vec::new();
            let mut missing = Vec::new();
            m.visit_nodes_sorted_at(&ids, seq, selection, &mut missing, &mut |id, row| {
                #[cfg(feature = "production-concurrency")]
                assert_eq!(m.participant_handle().unwrap().snapshot().0, 0);
                assert!(row.has_dense && row.has_sparse);
                let value = if id == 2 && seq == 1007 { 1006. } else { 1. };
                assert_eq!(
                    row.dense_vector.as_deref(),
                    selection.needs_dense().then_some(&[value, 1.][..])
                );
                assert_eq!(
                    row.sparse_vector.as_deref(),
                    selection.needs_sparse().then_some(&[(0, value)][..])
                );
                seen.push(id);
            })
            .unwrap();
            assert_eq!(
                seen,
                if seq == 5 {
                    vec![0, 0, 1, 2, 2, 3, u64::MAX, u64::MAX]
                } else {
                    vec![2, 2, 3, u64::MAX, u64::MAX]
                }
            );
            assert_eq!(missing, vec![4, 4, 9]);
        }
    }
    drop(m);
    finish(db);
}

#[cfg(feature = "production-concurrency")]
#[test]
fn targeted_v_sorted_missing_prefix_does_not_inflate_vector_backing() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("V").unwrap();
    let m = standalone(&db);
    m.apply_op_for_test(&WalOp::UpsertNode(node(10000, n, 1.)), 1)
        .unwrap();
    let ids: Vec<_> = (6000..=10000).collect();
    let mut missing = Vec::with_capacity(ids.len());
    let mut seen = Vec::with_capacity(1);
    let (_, allocations) = participant::allocations::measure(|| {
        m.visit_nodes_sorted_at(
            &ids,
            1,
            VectorSelection::Dense,
            &mut missing,
            &mut |id, row| {
                assert_eq!(row.dense_vector.as_deref(), Some(&[1., 1.][..]));
                seen.push(id);
            },
        )
        .unwrap();
    });
    assert_eq!(seen, vec![10000]);
    assert_eq!(missing, (6000..10000).collect::<Vec<_>>());
    eprintln!("missing-prefix vector allocation calls/bytes = {allocations:?}");
    assert!(
        allocations.2 < 4096,
        "one surviving vector reserved for already-consumed missing requests: {allocations:?}"
    );
    drop(m);
    finish(db);
}

#[cfg(feature = "production-concurrency")]
#[test]
fn targeted_v_sorted_vector_group_refills_release_sessions_and_keep_s() {
    // Deliberately occupy all process-global participant slots in a child,
    // as the existing collector/refusal tests do. Other parallel fixtures must
    // retain their own participants; this preserves every refusal/retry assertion.
    const CHILD: &str = "OVERGRAPH_TARGETED_V_PARTICIPANT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::tests::targeted_vector::targeted_v_sorted_vector_group_refills_release_sessions_and_keep_s",
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
    let n = db.ensure_node_label("V").unwrap();
    let m = standalone(&db);
    let ids: Vec<_> = (0..5000).collect();
    for &id in &ids {
        m.apply_op_for_test(&WalOp::UpsertNode(node(id, n, 1.)), id + 1)
            .unwrap();
    }
    m.take_node_sorted_walk_work_for_test();
    m.take_vector_copy_bytes_for_test();
    let mut seen = Vec::new();
    let mut missing = Vec::new();
    m.visit_nodes_sorted_at(
        &ids,
        5000,
        VectorSelection::Dense,
        &mut missing,
        &mut |id, row| {
            assert_eq!(m.participant_handle().unwrap().snapshot().0, 0);
            assert_eq!(row.dense_vector.as_deref(), Some(&[1., 1.][..]));
            assert!(row.sparse_vector.is_none());
            if seen.is_empty() {
                // This future update can acquire/install during the owned callback;
                // the second refill must still resolve its older opinion at S=5000.
                m.apply_op_for_test(&WalOp::UpsertNode(node(4500, n, 99.)), 5001)
                    .unwrap();
            }
            seen.push(id);
        },
    )
    .unwrap();
    assert_eq!(seen, ids);
    assert!(missing.is_empty());
    // One directory seek per 4096-request session. Future versions are resolved
    // within the captured ID history; adjacent IDs retain sequential reuse.
    assert_eq!(m.take_node_sorted_walk_work_for_test(), [2, 4998]);
    assert_eq!(m.take_vector_copy_bytes_for_test(), [0, 0, 5000 * 2 * 4, 0]);
    // Application unwind happens after the session and dependency entries ended.
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        m.visit_nodes_sorted_at(
            &[0],
            5000,
            VectorSelection::Dense,
            &mut Vec::new(),
            &mut |_, _| panic!("owned callback"),
        )
    }));
    assert!(panic.is_err());
    assert_eq!(m.participant_handle().unwrap().snapshot().0, 0);
    let handle = m.participant_handle().unwrap();
    let leases: Vec<_> = (0..256).map(|_| handle.try_acquire().unwrap()).collect();
    let mut refused_remaining = vec![12345];
    let mut called = false;
    let refusal = m
        .visit_nodes_sorted_at(
            &[0],
            5000,
            VectorSelection::Dense,
            &mut refused_remaining,
            &mut |_, _| called = true,
        )
        .unwrap_err();
    assert!(matches!(
        refusal,
        EngineError::ResourceLimit {
            resource: "participants",
            ..
        }
    ));
    assert!(!called);
    assert_eq!(refused_remaining, vec![12345]);
    drop(leases);
    m.apply_op_for_test(&WalOp::UpsertNode(node(5001, n, 1.)), 5002)
        .unwrap();
    drop(m);
    finish(db);
}

#[test]
fn targeted_v_scoring_preserves_newest_source_hiding_and_old_views() {
    let (_dir, db) = fixture();
    let n = db.ensure_node_label("V").unwrap();
    let other = db.ensure_node_label("Other").unwrap();
    db.write_op_batch(
        &(1..=8)
            .map(|id| WalOp::UpsertNode(node(id, n, id as f32)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.flush().unwrap();
    let segment = db.published_state().view.clone();
    let mut vectorless = node(1, n, 1.);
    vectorless.dense_vector = None;
    vectorless.sparse_vector = None;
    db.write_op_batch(&[
        WalOp::UpsertNode(vectorless),
        WalOp::DeleteNode {
            id: 2,
            deleted_at: 2,
        },
        WalOp::UpsertNode(node(3, n, 30.)),
        WalOp::UpsertNode(node(5, other, 50.)),
        WalOp::UpsertNode(node(9, n, 9.)),
    ])
    .unwrap();
    db.freeze_memtable().unwrap();
    let frozen = db.published_state().view.clone();
    assert_eq!(frozen.immutable_epochs.len(), 1);
    db.write_op(&WalOp::UpsertNode(node(3, n, 60.))).unwrap();
    let current = db.published_state().view.clone();
    db.write_op(&WalOp::UpsertNode(node(10, n, 100.))).unwrap();
    let scope: NodeIdSet = (1..=10).collect();
    for (view, expected) in [
        (
            &segment,
            vec![
                (8, 8.),
                (7, 7.),
                (6, 6.),
                (5, 5.),
                (4, 4.),
                (3, 3.),
                (2, 2.),
                (1, 1.),
            ],
        ),
        (
            &frozen,
            vec![(3, 30.), (9, 9.), (8, 8.), (7, 7.), (6, 6.), (4, 4.)],
        ),
        (
            &current,
            vec![(3, 60.), (9, 9.), (8, 8.), (7, 7.), (6, 6.), (4, 4.)],
        ),
    ] {
        let filter = view
            .label_catalog
            .resolve_node_label_filter_request(Some(&NodeLabelFilter {
                labels: vec!["V".into()],
                mode: LabelMatchMode::Any,
            }))
            .unwrap();
        let dense = view
            .score_dense_candidate_ids(&scope, &[1., 0.], DenseMetric::DotProduct, &filter, None)
            .unwrap();
        let sparse = view
            .vector_search_sparse_exact_scope(20, &[(0, 1.)], &scope, &filter, None)
            .unwrap();
        for hits in [dense, sparse] {
            assert_eq!(
                hits.iter()
                    .map(|h| (h.node_id, h.score))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
    drop((current, frozen, segment));
    finish(db);
}

#[test]
fn targeted_v_small_dense_scope_skips_count_and_preserves_2049_five_percent_decision() {
    // Large semantic setup has the existing functional resource allowance;
    // the count/route thresholds below remain exact and are not latency claims.
    let (_dir, db) = fixture_with_open(open_functional_maintenance_fixture);
    let n = db.ensure_node_label("V").unwrap();
    db.ensure_edge_label("E").unwrap();
    db.write_op_batch(
        &(1..=2049)
            .map(|id| WalOp::UpsertNode(node(id, n, 1.)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let query = vec![1., 0.];
    let request = ResolvedVectorSearchRequest {
        mode: VectorSearchMode::Dense,
        dense_query: Some(&query),
        sparse_query: None,
        k: 10,
        label_filter: ResolvedNodeLabelFilter::Unconstrained,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    };
    let check = |scope_len: usize, counts: usize, exact: usize| {
        let view = db.published_state().view.clone();
        let scope: NodeIdSet = (1..=scope_len as u64).collect();
        db.reset_query_execution_counters_for_test();
        let hits = view
            .vector_search_dense_with_scope(&request, 10, Some(&scope))
            .unwrap();
        assert_eq!(
            hits.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            (1..=10.min(scope_len) as u64).collect::<Vec<_>>()
        );
        let work = db.query_execution_counter_snapshot_for_test();
        assert_eq!(
            (work.dense_scope_count_passes, work.dense_scope_exact_routes),
            (counts, exact)
        );
    };
    check(0, 0, 1);
    check(2048, 0, 1);
    check(2049, 1, 0);
    // The shortcut does not apply to unscoped search, including the separate
    // 9216-vector measurement fixture; the original candidate/scoring path runs.
    db.reset_query_execution_counters_for_test();
    assert_eq!(
        db.published_state()
            .view
            .vector_search_dense_with_scope(&request, 10, None)
            .unwrap()
            .len(),
        10
    );
    let work = db.query_execution_counter_snapshot_for_test();
    assert_eq!(
        (work.dense_scope_count_passes, work.dense_scope_exact_routes),
        (0, 0)
    );
    db.write_op_batch(
        &(2050..=40979)
            .map(|id| WalOp::UpsertNode(node(id, n, 1.)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    check(2049, 1, 0); // floor(40979/20)=2048
    db.write_op(&WalOp::UpsertNode(node(40980, n, 1.))).unwrap();
    check(2049, 1, 1); // floor(40980/20)=2049
                       // Real public small/empty scopes still perform shape validation normally.
    let mut public = VectorSearchRequest {
        mode: VectorSearchMode::Dense,
        dense_query: Some(vec![1.]),
        sparse_query: None,
        k: 10,
        label_filter: None,
        ef_search: None,
        scope: Some(VectorSearchScope {
            start_node_id: 1,
            max_depth: 0,
            direction: Direction::Outgoing,
            edge_label_filter: Some(vec!["E".into()]),
            at_epoch: None,
        }),
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    };
    for start in [1, 50000] {
        public.scope.as_mut().unwrap().start_node_id = start;
        db.reset_query_execution_counters_for_test();
        assert!(db
            .vector_search(&public)
            .unwrap_err()
            .to_string()
            .contains("does not match configured dimension"));
        public.dense_query = Some(vec![1., 0.]);
        public.ef_search = Some(0);
        assert!(db
            .vector_search(&public)
            .unwrap_err()
            .to_string()
            .contains("ef_search must be > 0"));
        public.dense_query = Some(vec![f32::NAN, 0.]);
        public.ef_search = None;
        assert!(db.vector_search(&public).is_err());
        let work = db.query_execution_counter_snapshot_for_test();
        assert_eq!(
            (work.dense_scope_count_passes, work.dense_scope_exact_routes),
            (0, 0)
        );
        public.dense_query = Some(vec![1.]);
    }
    finish(db);
}
