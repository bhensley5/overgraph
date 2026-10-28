//! Public-API qualification harness; no runtime/pressure overrides.
//! All setup/oracles/warmup/close/drain count in the external process ledger.
use overgraph::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

#[path = "id_radix_loaded_timeline.rs"]
mod timeline;

const BATCH: usize = 4096;

fn input(i: usize, version: usize) -> NodeInput {
    NodeInput {
        labels: vec!["N".into()],
        key: format!("q{i}"),
        props: BTreeMap::from([
            ("keep".into(), PropValue::Bool(true)),
            ("version".into(), PropValue::Int(version as i64)),
        ]),
        weight: version as f32,
        dense_vector: None,
        sparse_vector: None,
    }
}
fn state(db: &DatabaseEngine, stage: &str) -> Value {
    let s = db.stats().unwrap();
    assert_eq!(s.wal_sync_mode, "group-commit");
    assert_eq!(db.edge_count().unwrap(), 0);
    json!({"stage":stage,"reported_node_count_approx":db.node_count().unwrap(),
        "wal_mode":s.wal_sync_mode,"wal_generation":s.active_wal_generation_id,
        "oldest_retained_wal_generation":s.oldest_retained_wal_generation_id,
        "segments":s.segment_count,"frozen":s.immutable_memtable_count,
        "pending_flush":s.pending_flush_count,"active_estimate":s.active_memtable_bytes,
        "frozen_estimate":s.immutable_memtable_bytes})
}
fn verify_node(n: &NodeView, i: usize, version: usize) {
    assert_eq!(n.id, i as u64 + 1);
    assert_eq!(n.key, format!("q{i}"));
    assert_eq!(n.labels, ["N"]);
    assert_eq!(n.weight, version as f32);
    assert_eq!(n.props, input(i, version).props);
    assert!(n.dense_vector.is_none() && n.sparse_vector.is_none());
}
fn verify_all(db: &DatabaseEngine, versions: &[usize]) {
    // node_count is an O(sources) monitoring estimate and can count the same
    // updated ID in both active and persisted sources. Verify exact membership.
    assert_eq!(
        db.nodes_by_labels("N").unwrap(),
        (1..=versions.len() as u64).collect::<Vec<_>>()
    );
    for (i, &v) in versions.iter().enumerate() {
        verify_node(&db.get_node(i as u64 + 1).unwrap().unwrap(), i, v);
    }
}
struct Fixture {
    dir: tempfile::TempDir,
    db: DatabaseEngine,
    ids: Vec<u64>,
}
impl Fixture {
    fn new(n: usize) -> Self {
        let mut dir = tempfile::Builder::new()
            .prefix("overgraph-scale-qualification-")
            .tempdir()
            .unwrap();
        dir.disable_cleanup(true);
        // The external guard tracks only this explicitly announced owned fixture.
        eprintln!("{}", json!({"owned_fixture":dir.path()}));
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.ensure_node_label("N").unwrap();
        let mut ids = Vec::with_capacity(n);
        for start in (0..n).step_by(BATCH) {
            let end = (start + BATCH).min(n);
            let found = db
                .batch_upsert_nodes((start..end).map(|i| input(i, 0)).collect())
                .unwrap();
            assert_eq!(found, ((start as u64 + 1)..=end as u64).collect::<Vec<_>>());
            ids.extend(found);
        }
        verify_all(&db, &vec![0; n]);
        Self { dir, db, ids }
    }
    fn finish(self, mut row: Value) {
        let Self { dir, db, .. } = self;
        row["before_close"] = state(&db, "before close");
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
            assert!(Instant::now() < until, "native drain");
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(db);
        dir.close().unwrap();
        row["native_drained"] = json!(true);
        println!("{row}");
    }
}
fn shuffled(ids: &[u64]) -> Vec<u64> {
    let mut ids = ids.to_vec();
    let mut x = 0x7296_40c1_c3af_015du64;
    for i in (1..ids.len()).rev() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        ids.swap(i, x as usize % (i + 1));
    }
    assert_eq!(
        ids.iter().copied().collect::<BTreeSet<_>>().len(),
        ids.len()
    );
    ids
}
fn read_passes(db: &DatabaseEngine, ids: &[u64], oracle: bool) -> Value {
    assert!(!ids.is_empty());
    for &id in ids {
        black_box(db.get_node(id).unwrap().unwrap());
    }
    let count = ids.len().div_ceil(20);
    let mut passes = Vec::new();
    for _ in 0..if oracle { 1 } else { 3 } {
        let mut blocks = Vec::new();
        for chunk in ids.chunks(count) {
            let start = Instant::now();
            for &id in chunk {
                black_box(db.get_node(black_box(id)).unwrap().unwrap());
            }
            blocks.push(json!({"reads":chunk.len(),"ns":start.elapsed().as_nanos() as u64}));
        }
        assert_eq!(
            blocks
                .iter()
                .map(|b| b["reads"].as_u64().unwrap())
                .sum::<u64>(),
            ids.len() as u64
        );
        passes.push(blocks);
    }
    json!({"distinct_ids":ids.len(),"warmup_reads":ids.len(),"passes":passes,
        "coverage":"every supplied ID exactly once per complete pass"})
}
fn scale(n: usize, oracle: bool) {
    let f = Fixture::new(n);
    let before = state(&f.db, "pre-read");
    let random = shuffled(&f.ids);
    let sorted = read_passes(&f.db, &f.ids, oracle);
    let shuffled = read_passes(&f.db, &random, oracle);
    verify_all(&f.db, &vec![0; n]);
    let after = state(&f.db, "post-read");
    f.finish(
        json!({"mode":"scale","oracle":oracle,"N":n,"versions_per_id":1,
        "distribution":"ordinary contiguous public IDs; whole-population permutation",
        "states":[before,after],"sorted":sorted,"shuffled":shuffled}),
    );
}
fn wide_write(n: usize, oracle: bool) {
    let f = Fixture::new(n);
    let mut versions = vec![0; n];
    let mut states = vec![state(&f.db, "pre-write")];
    let mut rows = Vec::new();
    let blocks = if oracle { 1 } else { 20 };
    let calls = if oracle { 2 } else { 20 };
    for fresh in [false, true] {
        let mut samples = Vec::new();
        let mut touched = BTreeSet::new();
        for b in 0..blocks {
            let indices: Vec<Vec<usize>> = (0..calls)
                .map(|c| {
                    (0..100)
                        .map(|j| {
                            let op = b * calls * 100 + c * 100 + j;
                            if fresh {
                                n + op
                            } else {
                                op.wrapping_mul(4099) % n
                            }
                        })
                        .collect()
                })
                .collect();
            // At most2000 matched small inputs prepared before each trace.
            let prepared: Vec<_> = indices
                .iter()
                .map(|is| is.iter().map(|&i| input(i, b + 1)).collect())
                .collect();
            let mut results = Vec::with_capacity(calls);
            let start = Instant::now();
            for rows in prepared {
                results.push(f.db.batch_upsert_nodes(rows).unwrap());
            }
            let elapsed = start.elapsed().as_nanos() as u64;
            for (is, found) in indices.iter().zip(results) {
                assert_eq!(found, is.iter().map(|&i| i as u64 + 1).collect::<Vec<_>>());
                for &i in is {
                    touched.insert(i);
                    if i >= versions.len() {
                        versions.resize(i + 1, 0);
                    }
                    versions[i] = b + 1;
                }
            }
            samples.push(json!({"ns":elapsed,"batch_calls":calls,"successful_updates":calls*100}));
            states.push(state(
                &f.db,
                if fresh {
                    "fresh block"
                } else {
                    "existing block"
                },
            ));
        }
        verify_all(&f.db, &versions);
        rows.push(json!({"shape":if fresh{"fresh"}else{"spread-existing"},
            "distinct_touched_ids":touched.len(),"blocks":samples}));
    }
    f.finish(json!({"mode":"wide-write","oracle":oracle,"N":n,
        "phase_order":["spread-existing","fresh"],"batch_size":100,
        "index_stride":4099,"states":states,"traces":rows}));
}
fn sparse(n: usize, oracle: bool) {
    let f = Fixture::new(n);
    f.db.flush().unwrap();
    let before = state(&f.db, "after baseline flush");
    let changed: Vec<_> = (0..n).step_by(16).collect();
    let mut versions = vec![0; n];
    for chunk in changed.chunks(BATCH) {
        let found =
            f.db.batch_upsert_nodes(chunk.iter().map(|&i| input(i, 1)).collect())
                .unwrap();
        assert_eq!(
            found,
            chunk.iter().map(|&i| i as u64 + 1).collect::<Vec<_>>()
        );
        for &i in chunk {
            versions[i] = 1;
        }
    }
    verify_all(&f.db, &versions);
    let after = state(&f.db, "after spread updates");
    let active: Vec<_> = changed.iter().map(|&i| f.ids[i]).collect();
    let persisted: Vec<_> = (0..n).filter(|i| i % 16 != 0).map(|i| f.ids[i]).collect();
    let a = read_passes(&f.db, &active, oracle);
    let s = read_passes(&f.db, &persisted, oracle);
    verify_all(&f.db, &versions);
    f.finish(
        json!({"mode":"sparse-active","oracle":oracle,"N":n,"update_stride":16,
        "changed_count":changed.len(),"unchanged_count":persisted.len(),
        "states":[before,after],"updated_subset":a,"unchanged_subset":s}),
    );
}
fn scan_query(limit: Option<usize>, after: Option<u64>) -> NodeQuery {
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
fn is_primary(plan: &QueryPlanNode) -> bool {
    match plan {
        QueryPlanNode::FallbackFullNodeScan => true,
        QueryPlanNode::StreamedSource { input }
        | QueryPlanNode::BufferedIdSort { input }
        | QueryPlanNode::VerifyNodeFilter { input } => is_primary(input),
        QueryPlanNode::Intersect { inputs, .. } | QueryPlanNode::Union { inputs, .. } => {
            inputs.iter().any(is_primary)
        }
        _ => false,
    }
}
fn primary(n: usize, oracle: bool) {
    let f = Fixture::new(n);
    let plan =
        f.db.explain_node_query(&scan_query(Some(10), None))
            .unwrap();
    assert!(
        is_primary(&plan.root),
        "requires actual primary FullScan: {:?}",
        plan.root
    );
    let before = state(&f.db, "pre-primary-scan");
    let mut samples = Vec::new();
    for hydrate in [false, true] {
        for limit in [Some(10), None] {
            for pass in 0..=if oracle { 1 } else { 3 } {
                let mut after = None;
                let mut ids = Vec::new();
                let mut nodes = Vec::new();
                let mut calls = 0;
                // Complete ordered walk, including public calls/result collection;
                // exact verification and owned-result drop follow the stop timer.
                let start = Instant::now();
                loop {
                    let query = scan_query(limit, after);
                    calls += 1;
                    let next = if hydrate {
                        let page = f.db.query_nodes(&query).unwrap();
                        nodes.extend(page.items);
                        page.next_cursor
                    } else {
                        let page = f.db.query_node_ids(&query).unwrap();
                        ids.extend(page.items);
                        page.next_cursor
                    };
                    if let Some(next) = next {
                        assert!(after.is_none_or(|last| next > last));
                        after = Some(next);
                    } else {
                        break;
                    }
                }
                let ns = start.elapsed().as_nanos() as u64;
                if hydrate {
                    for (i, node) in nodes.iter().enumerate() {
                        verify_node(node, i, 0);
                    }
                    assert_eq!(nodes.len(), n);
                } else {
                    assert_eq!(ids, f.ids);
                }
                if pass > 0 {
                    samples.push(json!({"hydrated":hydrate,"page_limit":limit,"pass":pass-1,
                "ns":ns,"public_calls":calls,"matched_rows":n,"distinct_ids":n}));
                }
                black_box((ids, nodes));
            }
        }
    }
    let after = state(&f.db, "post-primary-scan");
    f.finish(
        json!({"mode":"primary-scan","oracle":oracle,"N":n,"plan":format!("{:?}",plan.root),
        "states":[before,after],"complete_walks":samples}),
    );
}
fn retention(n: usize, pins: usize, oracle: bool) {
    assert!([0, 4].contains(&pins));
    let f = Fixture::new(n);
    f.db.flush().unwrap();
    let mut states = vec![state(&f.db, "baseline flushed")];
    let mut held = Vec::new();
    for cycle in 0..8 {
        if cycle % 2 == 0 {
            let mut tx = f.db.begin_write_txn().unwrap();
            if pins == 4 {
                held.push((tx, cycle * 2));
            } else {
                tx.rollback().unwrap();
            }
        }
        for version in [cycle * 2 + 1, cycle * 2 + 2] {
            for start in (0..n).step_by(BATCH) {
                let end = (start + BATCH).min(n);
                let found =
                    f.db.batch_upsert_nodes((start..end).map(|i| input(i, version)).collect())
                        .unwrap();
                assert_eq!(found, f.ids[start..end]);
            }
        }
        f.db.flush().unwrap();
        verify_all(&f.db, &vec![cycle * 2 + 2; n]);
        for (tx, version) in &held {
            for (i, &id) in f.ids.iter().enumerate() {
                let node = tx.get_node(TxnNodeRef::Id(id)).unwrap().unwrap();
                assert_eq!(node.id, Some(id));
                assert_eq!(node.key, format!("q{i}"));
                assert_eq!(node.props, input(i, *version).props);
                assert_eq!(node.weight, *version as f32);
            }
        }
        states.push(state(&f.db, "after flushed cycle and held/current oracles"));
    }
    assert_eq!(held.len(), pins);
    for (tx, _) in &mut held {
        tx.rollback().unwrap();
    }
    drop(held);
    states.push(state(&f.db, "after held release"));
    f.finish(
        json!({"mode":"retention","oracle":oracle,"N":n,"held_transactions":pins,
        "flush_cycles":8,"versions_per_cycle":2,"versions_per_id":17,"states":states,
        "held_weights":[0,4,8,12],"all_held_values_checked_each_cycle":true,
        "release":"rollback and drop all views before close; RSS return not asserted"}),
    );
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("loaded-timeline") {
        timeline::run(true);
    } else if args.get(1).map(String::as_str) == Some("oracle") {
        scale(256, true);
        wide_write(256, true);
        sparse(256, true);
        primary(256, true);
        retention(64, 0, true);
        retention(64, 4, true);
        timeline::run(false);
    } else {
        let n: usize = args.get(2).expect("MODE N [PINS]").parse().unwrap();
        assert!(n > 0 && n <= 524288);
        match args.get(1).map(String::as_str).unwrap_or("") {
            "scale" => scale(n, false),
            "wide-write" => wide_write(n, false),
            "sparse-active" => sparse(n, false),
            "primary-scan" => primary(n, false),
            "retention" => {
                assert_eq!(n, 8192);
                retention(n, args.get(3).unwrap().parse().unwrap(), false)
            }
            _ => panic!("MODE: scale,wide-write,sparse-active,primary-scan,retention; or oracle"),
        }
    }
    println!("{}", json!({"qualification_complete":true}));
}
