//! Write semantics and durability parity on the accepted preparation paths.
use super::*;
#[cfg(feature = "production-concurrency")]
use crate::memtable::IdentityOpinion;

fn node(id: u64, labels: &[u32]) -> NodeRecord {
    NodeRecord {
        id,
        label_ids: NodeLabelSet::from_label_ids(labels.iter().copied()).unwrap(),
        key: format!("n{id}"),
        props: BTreeMap::from([("value".into(), PropValue::Int(1))]),
        created_at: 1,
        updated_at: 1,
        weight: 1.,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: 0,
    }
}
#[cfg(feature = "production-concurrency")]
fn edge(id: u64, from: u64, to: u64, label: u32) -> EdgeRecord {
    EdgeRecord {
        id,
        from,
        to,
        label_id: label,
        props: BTreeMap::from([("value".into(), PropValue::Int(1))]),
        created_at: 1,
        updated_at: 1,
        weight: 1.,
        valid_from: 0,
        valid_to: i64::MAX,
        last_write_seq: 0,
    }
}
fn fixture(uniqueness: bool, sync: WalSyncMode) -> (TempDir, DatabaseEngine, DbOptions, [u32; 4]) {
    #[cfg(feature = "production-concurrency")]
    crate::engine::post45_read_tests::ready_participants_for_fixture();
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        edge_uniqueness: uniqueness,
        wal_sync_mode: sync,
        compact_after_n_flushes: 0,
        memtable_flush_threshold: 128 * 1024 * 1024,
        ..Default::default()
    };
    let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
    let labels = [
        db.ensure_node_label("N").unwrap(),
        db.ensure_node_label("M").unwrap(),
        db.ensure_edge_label("E").unwrap(),
        db.ensure_edge_label("F").unwrap(),
    ];
    db.write_op_batch(
        &(1..=3)
            .map(|id| WalOp::UpsertNode(node(id, &labels[..1])))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    (dir, db, opts, labels)
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
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
    }
}

#[cfg(feature = "production-concurrency")]
fn physical_bounds(table: &Memtable, owner: u64, edge_label: u32) -> [usize; 3] {
    [
        table
            .visible_edges_from_endpoint_count_estimate(owner, None, u64::MAX)
            .unwrap()
            .count,
        table
            .visible_edges_to_endpoint_count_estimate(owner, None, u64::MAX)
            .unwrap()
            .count,
        table
            .edge_label_posting_count_upper_bound(edge_label)
            .unwrap(),
    ]
}

#[cfg(feature = "production-concurrency")]
#[test]
fn targeted_write_fresh_duplicate_triples_keep_every_opinion() {
    let (dir, db, opts, [_, _, e, _]) = fixture(false, WalSyncMode::default());
    db.write_op(&WalOp::UpsertEdge(edge(1, 1, 2, e))).unwrap();
    db.flush().unwrap();
    let old = db.published_state().view.clone();
    let first_seq = old.snapshot_seq + 1;
    let ops = [
        WalOp::UpsertEdge(edge(2, 1, 2, e)),
        WalOp::UpsertEdge(edge(3, 1, 2, e)),
        WalOp::UpsertEdge(edge(4, 3, 3, e)),
    ];
    db.write_op_batch(&ops).unwrap();
    assert_eq!(
        old.memtable
            .triple_opinions_at(&[(1, 2, e)], first_seq)
            .unwrap(),
        vec![IdentityOpinion::Live(2)]
    );
    assert_eq!(
        old.memtable
            .triple_opinions_at(&[(1, 2, e)], first_seq + 1)
            .unwrap(),
        vec![IdentityOpinion::Live(3)]
    );
    assert_eq!(
        old.sources()
            .find_edge_by_triple(1, 2, e)
            .unwrap()
            .unwrap()
            .id,
        1
    );
    let current = db.published_state().view.clone();
    assert_eq!(db.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id, 3);
    assert!(db.get_edge(2).unwrap().is_some());
    assert_eq!(physical_bounds(&current.memtable, 1, e), [2, 0, 3]);
    assert_eq!(physical_bounds(&current.memtable, 3, e), [1, 1, 3]);
    assert_eq!(current.memtable.primary_distinct_ids_for_test(), [0, 3]);
    assert_eq!(
        current.memtable.estimated_size(),
        current.memtable.estimated_size_full_for_test()
    );
    drop((current, old));
    finish(db);
    let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
    // Singular triple lookup is documented only for uniqueness=true. Parallel
    // records persist by ID; segments need not preserve a chosen duplicate owner.
    assert!([2, 3].contains(&db.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id));
    for id in 1..=4 {
        assert!(db.get_edge(id).unwrap().is_some());
    }
    finish(db);
}

#[cfg(feature = "production-concurrency")]
#[test]
fn targeted_write_batch_moves_relabels_and_future_tombstones_keep_bounds() {
    let (_dir, db, _, [n, m, e, f]) = fixture(false, WalSyncMode::default());
    let table = db
        .with_core_ref(|core| {
            Memtable::new_candidate_with_catalog(
                core.memtable.participant_handle().unwrap(),
                core.fixed_secondary_catalog.as_ref().unwrap().clone(),
                &core.candidate.as_ref().unwrap().domain,
            )
        })
        .unwrap();
    let apply = |ops: &[WalOp], seq| {
        let prepared = table
            .prepare_primary(ops, seq, None, 0, 0)
            .unwrap()
            .unwrap();
        let handle = table.participant_handle().unwrap();
        let session = handle.try_acquire().unwrap();
        table.install_primary(&session, prepared, ops).unwrap();
    };
    apply(
        &[
            WalOp::DeleteNode {
                id: 100,
                deleted_at: 1,
            },
            WalOp::DeleteEdge {
                id: 100,
                deleted_at: 1,
            },
            WalOp::UpsertNode(node(1, &[n])),
            WalOp::UpsertEdge(edge(1, 1, 1, e)),
        ],
        1,
    );
    apply(
        &[
            WalOp::UpsertNode(node(2, &[n, m])),
            WalOp::UpsertNode(node(2, &[n])),
            WalOp::DeleteNode {
                id: 2,
                deleted_at: 2,
            },
            WalOp::UpsertNode(node(2, &[m])),
            WalOp::UpsertEdge(edge(2, 1, 2, e)),
            WalOp::UpsertEdge(edge(2, 2, 2, f)),
            WalOp::DeleteEdge {
                id: 2,
                deleted_at: 2,
            },
            WalOp::UpsertEdge(edge(2, 1, 2, e)),
            WalOp::UpsertEdge(edge(100, 1, 2, e)),
            WalOp::UpsertNode(node(100, &[n])),
        ],
        5,
    );
    assert_eq!(physical_bounds(&table, 1, e), [3, 1, 3]);
    assert_eq!(physical_bounds(&table, 2, f), [1, 2, 1]);
    assert_eq!(
        table.visible_nodes_by_label_id(n, 14).unwrap(),
        vec![1, 100]
    );
    assert_eq!(table.visible_nodes_by_label_id(m, 14).unwrap(), vec![2]);
    assert_eq!(
        table.visible_edges_by_label_id(e, 14).unwrap(),
        vec![1, 2, 100]
    );
    assert!(table.visible_edges_by_label_id(f, 14).unwrap().is_empty());
    assert!(table.get_node_at(2, 4).unwrap().is_none());
    assert!(table.get_edge_at(100, 4).unwrap().is_none());
    assert_eq!(
        table.triple_opinions_at(&[(1, 2, e)], 9).unwrap(),
        vec![IdentityOpinion::Live(2)]
    );
    assert_eq!(
        table.triple_opinions_at(&[(1, 2, e)], 10).unwrap(),
        vec![IdentityOpinion::Removed]
    );
    assert_eq!(
        table.triple_opinions_at(&[(1, 2, e)], 13).unwrap(),
        vec![IdentityOpinion::Live(100)]
    );
    assert_eq!(table.estimated_size(), table.estimated_size_full_for_test());
    drop(table);
    finish(db);
}

#[test]
fn targeted_write_public_duplicate_triples_keep_uniqueness_and_durable_owners() {
    for sync in [WalSyncMode::default(), WalSyncMode::Immediate] {
        for unique in [false, true] {
            let (dir, db, opts, _) = fixture(unique, sync.clone());
            let first = db
                .upsert_edge(1, 2, "E", UpsertEdgeOptions::default())
                .unwrap();
            db.flush().unwrap();
            let input = |to| EdgeInput {
                from: 1,
                to,
                label: "E".into(),
                props: BTreeMap::new(),
                weight: 1.,
                valid_from: None,
                valid_to: None,
            };
            let updated = db.batch_upsert_edges(vec![input(2), input(2)]).unwrap();
            assert_eq!(updated[0] == first, unique);
            assert_eq!(updated[0] == updated[1], unique);
            assert_eq!(
                db.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id,
                updated[1]
            );
            let fresh = db.batch_upsert_edges(vec![input(3), input(3)]).unwrap();
            assert_eq!(fresh[0] == fresh[1], unique);
            let view = db.published_state().view.clone();
            assert_eq!(view.sources().find_edge(fresh[0]).unwrap().unwrap().to, 3);
            db.delete_edge(fresh[1]).unwrap();
            assert!(db.get_edge_by_triple(1, 3, "E").unwrap().is_none());
            // A newer negative owner must not reveal an earlier duplicate.
            assert!(view.sources().find_edge(fresh[1]).unwrap().is_some());
            drop(view);
            finish(db);
            let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
            if unique {
                assert_eq!(
                    db.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id,
                    updated[1]
                );
                assert!(db.get_edge_by_triple(1, 3, "E").unwrap().is_none());
            } else {
                for id in [first, updated[0], updated[1], fresh[0]] {
                    let stored = db.get_edge(id).unwrap().unwrap();
                    assert_eq!(
                        (stored.from, stored.to),
                        (1, if id == fresh[0] { 3 } else { 2 })
                    );
                }
            }
            assert!(db.get_edge(fresh[1]).unwrap().is_none());
            finish(db);
        }
    }
}
