//! Ignored, private native cost diagnostic. No instrumentation in ordinary runs.
//!
//! The identical driver is wired into an external accepted-a1 archive. Small
//! disposal epochs keep both fixtures inside a1's smaller fixed admission caps.
//! Foreground time, epoch disposal and terminal close/drain are reported apart.

use super::super::reclamation::ReclamationStats;
use super::*;
use crate::types::NodeLabelSet;
use std::hint::black_box;

const SEED: u64 = 0x45a3;
const KEYS: u64 = 1_000;
const READS: usize = 10_000;
const WRITES: usize = 1_000;
const EPOCH: usize = 4;

fn record(id: u64) -> NodeRecord {
    NodeRecord {
        id,
        label_ids: NodeLabelSet::single(1).unwrap(),
        key: format!("seed_{SEED:x}_node_{id:08}"),
        props: BTreeMap::from([("value".into(), PropValue::Int(id as i64))]),
        created_at: 10,
        updated_at: 20,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: 0,
    }
}

fn wait_for(stats: &ReclamationStatsHandle, ready: impl Fn(ReclamationStats) -> bool) -> Duration {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60);
    while !ready(stats.read()) {
        assert!(
            Instant::now() < deadline,
            "unfinished drain: {:?}",
            stats.read()
        );
        std::thread::yield_now();
    }
    start.elapsed()
}

fn open_slice(path: &Path) -> Slice {
    Slice::open(path, WalSyncMode::default()).unwrap()
}

fn quiescent(db: &Slice, stats: &ReclamationStatsHandle, session_producers: usize) -> Duration {
    // The slice has four fixed registered owners: Core, producing source bundle,
    // passive source bundle and current PublishedState. Rotations retain one
    // additional registered owner per generation. A2 also has a session producer
    // without a registry object. An exact count, after releasing this snapshot,
    // excludes pending input/staging/publication/old-bundle destruction.
    let start = Instant::now();
    let generations = db.snapshot().unwrap().sources().len();
    let objects = 3 + generations;
    wait_for(stats, |s| {
        s.occupied == objects && s.producers == objects + session_producers
    });
    start.elapsed()
}

fn slice_case(case: &str) -> serde_json::Value {
    let setup_start = Instant::now();
    let directory = tempfile::tempdir().unwrap();
    let db = open_slice(directory.path());
    let stats = db.stats();
    let initial = stats.read();
    assert_eq!(
        initial.occupied, 4,
        "fresh slice registered owner inventory"
    );
    let session_producers = initial.producers.checked_sub(initial.occupied).unwrap();
    drop(
        db.submit(vec![WalOp::EnsureNodeLabel {
            label: "BenchNode1".into(),
            label_id: 1,
        }])
        .unwrap()
        .wait()
        .unwrap(),
    );
    for batch in 0..10 {
        let first = batch * 100 + 1;
        drop(
            db.submit(
                (first..first + 100)
                    .map(|id| WalOp::UpsertNode(record(id)))
                    .collect(),
            )
            .unwrap()
            .wait()
            .unwrap(),
        );
        quiescent(&db, &stats, session_producers);
    }
    let domain = db.runtime.domain().unwrap();
    let setup = setup_start.elapsed();
    let requests = if case == "slice_batch100" {
        WRITES / 100
    } else if case == "slice_scalar" {
        WRITES
    } else {
        READS
    };
    let mut foreground = Duration::ZERO;
    let mut epochs = Duration::ZERO;
    for first in (0..requests).step_by(EPOCH) {
        let start = Instant::now();
        for index in first..(first + EPOCH).min(requests) {
            match case {
                "slice_snapshot" => drop(black_box(db.snapshot().unwrap())),
                "slice_copy" => {
                    let snapshot = db.snapshot().unwrap();
                    let id = index as u64 % KEYS + 1;
                    let result = snapshot.copy_node(id).unwrap();
                    assert_eq!(black_box(result.get().as_ref().unwrap().id), id);
                    drop(result);
                    drop(snapshot);
                }
                "slice_scalar" => {
                    drop(
                        db.submit(vec![WalOp::UpsertNode(record(KEYS + index as u64 + 1))])
                            .unwrap()
                            .wait()
                            .unwrap(),
                    );
                }
                "slice_batch100" => {
                    let start_id = KEYS + index as u64 * 100 + 1;
                    drop(
                        db.submit(
                            (start_id..start_id + 100)
                                .map(|id| WalOp::UpsertNode(record(id)))
                                .collect(),
                        )
                        .unwrap()
                        .wait()
                        .unwrap(),
                    );
                }
                _ => panic!("unknown slice cell {case}"),
            }
        }
        foreground += start.elapsed();
        epochs += quiescent(&db, &stats, session_producers);
    }
    let source_generations = db.snapshot().unwrap().sources().len();
    let before_close = stats.read();
    let close_start = Instant::now();
    db.close().unwrap();
    let close = close_start.elapsed();
    drop(domain);
    let drain = wait_for(&stats, |s| s.exited);
    assert_eq!(stats.read().occupied, 0);
    drop(db);
    let cleanup_start = Instant::now();
    drop(directory);
    serde_json::json!({
        "cell": case, "seed": SEED, "requests": requests,
        "logical_writes": if matches!(case, "slice_scalar" | "slice_batch100") { WRITES } else { 0 },
        "reads": if case == "slice_copy" { READS } else { 0 },
        "epoch_requests": EPOCH, "setup_ns": setup.as_nanos(),
        "foreground_ns": foreground.as_nanos(), "epoch_drain_ns": epochs.as_nanos(),
        "terminal_close_ns": close.as_nanos(), "terminal_drain_ns": drain.as_nanos(),
        "caller_cleanup_ns": cleanup_start.elapsed().as_nanos(),
        "before_close_objects": before_close.occupied,
        "source_generations": source_generations,
        "session_producers": session_producers,
        "drain_complete": true, "request_tails": "unqualified", "process_cpu": "unknown", "peak_rss": "unknown"
    })
}

fn map_case(case: &str, candidate: bool) -> serde_json::Value {
    let setup_start = Instant::now();
    let admission = ParticipantAdmission::new(256).unwrap();
    let map = candidate.then(AppendMap::<PrimaryKey, NodeRecord>::new);
    let control = (!candidate).then(crossbeam_skiplist::SkipMap::<PrimaryKey, NodeRecord>::new);
    for id in 1..=KEYS {
        if let Some(map) = &map {
            let session = admission.try_acquire().unwrap();
            map.insert_unique(&session, (id, Reverse(1)), record(id));
        } else {
            control
                .as_ref()
                .unwrap()
                .insert((id, Reverse(1)), record(id));
        }
    }
    let setup = setup_start.elapsed();
    let requests = match case {
        "map_point" => READS,
        "map_scalar" => WRITES,
        "map_batch100" => WRITES / 100,
        _ => panic!("unknown map cell {case}"),
    };
    let start = Instant::now();
    for index in 0..requests {
        if case == "map_point" {
            let key = (index as u64 % KEYS + 1, Reverse(1));
            if let Some(map) = &map {
                let session = admission.try_acquire().unwrap();
                let (found, value) = map.lower(&session, Bound::Included(&key)).unwrap();
                assert_eq!(black_box((found.0, value.id)), (key.0, key.0));
            } else {
                let entry = control
                    .as_ref()
                    .unwrap()
                    .lower_bound(Bound::Included(&key))
                    .unwrap();
                assert_eq!(black_box((entry.key().0, entry.value().id)), (key.0, key.0));
            }
        } else {
            let batch = if case == "map_batch100" { 100 } else { 1 };
            let first = KEYS + index as u64 * batch + 1;
            if let Some(map) = &map {
                let session = admission.try_acquire().unwrap();
                for id in first..first + batch {
                    map.insert_unique(&session, (id, Reverse(1)), record(id));
                }
            } else {
                let map = control.as_ref().unwrap();
                for id in first..first + batch {
                    let key = (id, Reverse(1));
                    assert!(map.get(&key).is_none());
                    black_box(map.insert(key, record(id)));
                }
            }
        }
    }
    let foreground = start.elapsed();
    let cleanup_start = Instant::now();
    drop(map);
    drop(control);
    serde_json::json!({"cell": case, "candidate": candidate, "seed": SEED,
        "requests": requests, "logical_writes": if case == "map_point" { 0 } else { WRITES },
        "reads": if case == "map_point" { READS } else { 0 }, "setup_ns": setup.as_nanos(),
        "foreground_ns": foreground.as_nanos(), "caller_cleanup_ns": cleanup_start.elapsed().as_nanos(),
        "drain_complete": true, "request_tails": "unqualified", "process_cpu": "unknown", "peak_rss": "unknown"})
}

#[test]
#[ignore = "private serial release timing; invoke explicitly with the frozen a3 driver"]
fn p45a3_timing() {
    let _serial = super::super::participant::TEST_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let case = std::env::var("OVERGRAPH_P45A3_TIMING_CASE").expect("explicit timing cell");
    let result = if case.starts_with("slice_") {
        slice_case(&case)
    } else {
        map_case(
            &case,
            std::env::var("OVERGRAPH_P45A3_MAP").as_deref() == Ok("candidate"),
        )
    };
    println!("P45A3_TIMING {result}");
}
