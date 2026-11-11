use overgraph::*;
#[cfg(feature = "production-concurrency")]
use std::sync::{Arc, Barrier};

#[test]
fn immediate_reopen_after_normal_and_fast_close_preserves_data() {
    let root = tempfile::tempdir().unwrap();
    for fast in [false, true] {
        let path = root.path().join(if fast { "fast" } else { "normal" });
        for iteration in 0..12 {
            let db = DatabaseEngine::open(&path, &DbOptions::default()).unwrap();
            db.upsert_node("N", &iteration.to_string(), UpsertNodeOptions::default())
                .unwrap();
            if fast { db.close_fast() } else { db.close() }.unwrap();
            // Deliberately no retry helper, sleep, drain, or drop of the wrapper.
            let reopened = DatabaseEngine::open(&path, &DbOptions::default()).unwrap();
            assert!(reopened
                .get_node_by_key("N", &iteration.to_string())
                .unwrap()
                .is_some());
            assert_eq!(reopened.node_count().unwrap(), iteration + 1);
            reopened.close().unwrap();
        }
    }
}

#[cfg(feature = "production-concurrency")]
#[test]
fn immediate_reopen_with_retained_transaction_and_competing_openers() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    // Initialize process participants before racing opens on separate threads.
    let db = DatabaseEngine::open(&path, &DbOptions::default()).unwrap();
    db.upsert_node("N", "persisted", UpsertNodeOptions::default())
        .unwrap();
    let mut retained = db.begin_write_txn().unwrap();
    db.close().unwrap();
    let barrier = Arc::new(Barrier::new(5));
    let mut openers = Vec::new();
    for _ in 0..4 {
        let path = path.clone();
        let barrier = barrier.clone();
        openers.push(std::thread::spawn(move || {
            barrier.wait();
            DatabaseEngine::open(&path, &DbOptions::default())
        }));
    }
    barrier.wait();
    let results: Vec<_> = openers.into_iter().map(|h| h.join().unwrap()).collect();
    let mut admitted = None;
    for result in results {
        match result {
            Ok(engine) => {
                assert!(admitted.replace(engine).is_none());
            }
            Err(EngineError::InvalidOperation(message)) => {
                assert_eq!(message, "candidate directory admission: ActiveSession");
            }
            Err(error) => panic!("unexpected reopen refusal: {error}"),
        }
    }
    let reopened = admitted.expect("one opener must acquire admission");
    assert!(reopened
        .get_node_by_key("N", "persisted")
        .unwrap()
        .is_some());
    // The transaction's legitimate snapshot was retained throughout reopen.
    retained.rollback().unwrap();
    reopened.close().unwrap();
}
