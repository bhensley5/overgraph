//! Matched public-API diagnostic for farther-behind snapshots; no loaded-tail claim.
use overgraph::{
    DatabaseEngine, DbOptions, NodeInput, ReclamationCloseState, TxnNodeRef, WriteTxn,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const IDS: usize = 100;
const READ_IDS: usize = 44;
const GROUPS: usize = 128;
const BLOCKS: usize = 20;

fn inputs(version: usize) -> Vec<NodeInput> {
    (0..IDS)
        .map(|i| NodeInput {
            labels: vec!["N".into()],
            key: format!("diag-{i}"),
            props: BTreeMap::new(),
            weight: version as f32,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

fn no_maintenance(db: &DatabaseEngine, stage: &str) -> Value {
    let s = db.stats().unwrap();
    assert_eq!(
        s.active_wal_generation_id, 0,
        "confounded WAL generation: {stage}"
    );
    assert_eq!(
        s.immutable_memtable_count, 0,
        "confounded frozen sources: {stage}"
    );
    assert_eq!(s.segment_count, 0, "confounded persisted sources: {stage}");
    assert_eq!(
        s.pending_flush_count, 0,
        "confounded pending flush: {stage}"
    );
    assert_eq!(
        s.wal_sync_mode, "group-commit",
        "ordinary GroupCommit required"
    );
    assert_eq!(db.node_count().unwrap(), IDS);
    assert_eq!(db.edge_count().unwrap(), 0);
    json!({"stage":stage,"wal_generation":s.active_wal_generation_id,
        "immutables":s.immutable_memtable_count,"segments":s.segment_count,
        "pending_flush":s.pending_flush_count,"active_estimate":s.active_memtable_bytes,
        "immutable_estimate":s.immutable_memtable_bytes,"wal_mode":s.wal_sync_mode})
}

fn verify(tx: &WriteTxn, ids: &[u64], weight: usize) {
    for (i, &id) in ids.iter().enumerate() {
        let n = tx.get_node(TxnNodeRef::Id(id)).unwrap().unwrap();
        assert_eq!(n.id, Some(id));
        assert_eq!(n.key, format!("diag-{i}"));
        assert_eq!(n.labels, ["N"]);
        assert_eq!(n.weight, weight as f32);
        assert!(n.props.is_empty() && n.dense_vector.is_none() && n.sparse_vector.is_none());
    }
}

fn group(tx: &WriteTxn, ids: &[u64]) {
    for &id in &ids[..READ_IDS] {
        // Same public API, owned conversion and result destruction on both paths.
        // No result cache or overlay entry is populated by these immutable reads.
        black_box(tx.get_node(TxnNodeRef::Id(black_box(id))).unwrap().unwrap());
    }
}

fn block(tx: &WriteTxn, ids: &[u64]) -> u64 {
    let t = Instant::now();
    for _ in 0..GROUPS {
        group(tx, ids);
    }
    t.elapsed().as_nanos().try_into().unwrap()
}

fn summarize(blocks: &[u64]) -> Value {
    let mean_block = blocks.iter().map(|&n| n as f64).sum::<f64>() / blocks.len() as f64;
    json!({"raw_blocks_ns":blocks,"mean_ns_per_call":mean_block/(GROUPS*READ_IDS) as f64,
        "mean_ns_per_44_reads":mean_block/GROUPS as f64,
        "measured_reads":BLOCKS*GROUPS*READ_IDS})
}

// Identical public fixture/read/owned-conversion helpers as the reviewed diagnostic.
// Capture depths that finish 1,16,1024 record versions behind the final view.
fn main() {
    const H: usize = 2048;
    let mut dir = tempfile::Builder::new()
        .prefix("overgraph-history-distance-")
        .tempdir()
        .unwrap();
    dir.disable_cleanup(true);
    let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    db.ensure_node_label("N").unwrap();
    let mut ids = Vec::new();
    let mut views = Vec::new();
    for version in 0..=H {
        let found = db.batch_upsert_nodes(inputs(version)).unwrap();
        if version == 0 {
            ids = found;
            assert_eq!(ids, (1..=IDS as u64).collect::<Vec<_>>());
        } else {
            assert_eq!(found, ids, "stable IDs across every version");
        }
        if [H - 1024, H - 16, H - 1, H].contains(&version) {
            let view = db.begin_write_txn().unwrap();
            verify(&view, &ids, version);
            views.push((H - version, view));
        }
    }
    views.sort_by_key(|(behind, _)| *behind);
    let before = no_maintenance(&db, "after all versions, writes stopped");
    for group_no in 0..GROUPS {
        if group_no % 2 == 0 {
            for (_, v) in &views {
                group(v, &ids);
            }
        } else {
            for (_, v) in views.iter().rev() {
                group(v, &ids);
            }
        }
    }
    let before_timing = no_maintenance(&db, "after equal warmup");
    let mut blocks: Vec<Vec<u64>> = (0..views.len())
        .map(|_| Vec::with_capacity(BLOCKS))
        .collect();
    let mut orders = Vec::new();
    for i in 0..BLOCKS {
        let order: Vec<usize> = if i % 2 == 0 {
            (0..views.len()).collect()
        } else {
            (0..views.len()).rev().collect()
        };
        orders.push(order.iter().map(|&j| views[j].0).collect::<Vec<_>>());
        for j in order {
            blocks[j].push(block(&views[j].1, &ids));
        }
    }
    let after_timing = no_maintenance(&db, "after timing");
    let mut rows = Vec::new();
    for ((behind, v), b) in views.iter_mut().zip(&blocks) {
        verify(v, &ids, H - *behind);
        rows.push(
            json!({"record_versions_behind":behind,"weight":H-*behind,"summary":summarize(b)}),
        );
        v.rollback().unwrap();
    }
    drop(views);
    let before_close = no_maintenance(&db, "after rollback, before close");
    db.close_fast().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let s = db.reclamation_stats().unwrap();
        if matches!(s.close_state, ReclamationCloseState::Drained) {
            assert_eq!(
                (s.registered_objects, s.retired_objects, s.dropping_objects),
                (0, 0, 0)
            );
            assert_eq!(
                (
                    s.leased_participants,
                    s.active_sessions,
                    s.draining_sessions
                ),
                (0, 0, 0)
            );
            break;
        }
        assert!(Instant::now() < until, "native drain failure");
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(db);
    dir.close().unwrap();
    println!(
        "{}",
        json!({"diagnostic":"historical-distance","H":H,"distinct_node_ids":IDS,
        "versions_per_id_after":H+1,"timed_ids":&ids[..READ_IDS],
        "groups_per_block":GROUPS,"reads_per_group":READ_IDS,"blocks_per_snapshot":BLOCKS,
        "warmup_reads_per_snapshot":GROUPS*READ_IDS,"block_orders_versions_behind":orders,
        "paths":rows,"states":[before,before_timing,after_timing,before_close],
        "overlays_unmodified":true,"native_drained":true})
    );
}
