use overgraph::{
    DatabaseEngine, DbOptions, Direction, NeighborOptions, NodeInput, PropValue, UpsertEdgeOptions,
    UpsertNodeOptions, WalSyncMode,
};
use std::collections::BTreeMap;
use tempfile::TempDir;

fn make_props(key: &str, val: &str) -> BTreeMap<String, PropValue> {
    let mut m = BTreeMap::new();
    m.insert(key.to_string(), PropValue::String(val.to_string()));
    m
}

// --- Crash recovery: WAL replay after ungraceful shutdown ---

/// Simulate a crash by dropping the engine without calling close().
/// Reopen and verify all data is recovered from WAL + segments.
#[test]
fn test_crash_recovery_wal_replay() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_db");
    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };

    let node_a;
    let node_b;
    let edge_ab;

    // Phase 1: write data, flush some to segment, leave some in WAL only
    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();
        node_a = db
            .upsert_node(
                1,
                "alice",
                UpsertNodeOptions {
                    props: make_props("role", "admin"),
                    ..Default::default()
                },
            )
            .unwrap();
        node_b = db
            .upsert_node(
                1,
                "bob",
                UpsertNodeOptions {
                    props: make_props("role", "user"),
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
        edge_ab = db
            .upsert_edge(node_a, node_b, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Flush to segment
        db.flush().unwrap();

        // Write more data that stays in WAL only
        db.upsert_node(
            2,
            "charlie",
            UpsertNodeOptions {
                props: make_props("role", "viewer"),
                weight: 0.3,
                ..Default::default()
            },
        )
        .unwrap();
        db.upsert_node(
            2,
            "diana",
            UpsertNodeOptions {
                props: make_props("role", "editor"),
                weight: 0.8,
                ..Default::default()
            },
        )
        .unwrap();

        // Drop without close() -- simulates crash
        // (The Drop impl will attempt cleanup but WAL data should be durable)
    }

    // Phase 2: reopen and verify everything
    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Segment data
        let alice = db.get_node(node_a).unwrap().unwrap();
        assert_eq!(alice.key, "alice");
        assert_eq!(
            alice.props.get("role"),
            Some(&PropValue::String("admin".to_string()))
        );

        let bob = db.get_node(node_b).unwrap().unwrap();
        assert_eq!(bob.key, "bob");

        let edge = db.get_edge(edge_ab).unwrap().unwrap();
        assert_eq!(edge.from, node_a);
        assert_eq!(edge.to, node_b);

        // WAL-only data should be recovered
        let charlie = db.get_node_by_key(2, "charlie").unwrap();
        assert!(
            charlie.is_some(),
            "WAL-only node 'charlie' should be recovered"
        );

        let diana = db.get_node_by_key(2, "diana").unwrap();
        assert!(diana.is_some(), "WAL-only node 'diana' should be recovered");

        // Neighbors should work across recovered data
        let nbrs = db.neighbors(node_a, &NeighborOptions::default()).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, node_b);

        db.close().unwrap();
    }
}

/// Crash after deletes: verify tombstones in WAL are replayed.
#[test]
fn test_crash_recovery_with_deletes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_del_db");
    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };

    let node_a;
    let node_b;

    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();
        node_a = db
            .upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        node_b = db
            .upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_edge(node_a, node_b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Delete node_a (and cascade edge) -- stays in WAL
        db.delete_node(node_a).unwrap();
        // Drop without close
    }

    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();
        assert!(
            db.get_node(node_a).unwrap().is_none(),
            "deleted node should stay deleted after crash"
        );
        assert!(
            db.get_node(node_b).unwrap().is_some(),
            "non-deleted node should survive"
        );
        // Edge should be cascade-deleted
        let nbrs = db
            .neighbors(
                node_b,
                &NeighborOptions {
                    direction: Direction::Incoming,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(nbrs.is_empty(), "cascade-deleted edge should not appear");
        db.close().unwrap();
    }
}

// --- Large-scale integration test ---

/// 100k nodes + edges through the full lifecycle.
#[test]
fn test_large_scale_100k_nodes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("large_db");
    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        memtable_flush_threshold: 8 * 1024 * 1024, // 8MB to trigger more flushes
        ..DbOptions::default()
    };

    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Batch insert 100k nodes in chunks
    let chunk_size = 10_000;
    let total_nodes = 100_000;
    let mut all_ids = Vec::with_capacity(total_nodes);

    for chunk_start in (0..total_nodes).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(total_nodes);
        let batch: Vec<NodeInput> = (chunk_start..chunk_end)
            .map(|i| NodeInput {
                type_id: (i % 5 + 1) as u32,
                key: format!("node-{}", i),
                props: {
                    let mut m = BTreeMap::new();
                    m.insert("idx".to_string(), PropValue::Int(i as i64));
                    m
                },
                weight: (i % 100) as f32 / 100.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();

        let ids = db.batch_upsert_nodes(&batch).unwrap();
        all_ids.extend_from_slice(&ids);

        // Flush every other chunk to create segments
        if (chunk_start / chunk_size) % 2 == 0 {
            db.flush().unwrap();
        }
    }

    assert_eq!(all_ids.len(), total_nodes);

    // Add edges (chain pattern: every node links to the next)
    let edge_batch: Vec<overgraph::EdgeInput> = (0..total_nodes - 1)
        .step_by(10) // every 10th to keep it fast
        .map(|i| overgraph::EdgeInput {
            from: all_ids[i],
            to: all_ids[i + 1],
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();

    db.batch_upsert_edges(&edge_batch).unwrap();
    db.flush().unwrap();

    // Compact all segments
    let stats = db.compact().unwrap();
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(stats.nodes_kept as usize, total_nodes);

    // Spot-check reads
    let spot = db.get_node(all_ids[50_000]).unwrap().unwrap();
    assert_eq!(spot.key, "node-50000");

    // Type query
    let type_1_count = db.count_nodes_by_type(1).unwrap();
    assert_eq!(type_1_count, 20_000); // 100k / 5 types

    // Find nodes
    let found = db.find_nodes(3, "idx", &PropValue::Int(42)).unwrap();
    assert!(!found.is_empty() || 42 % 5 + 1 != 3); // only found if type matches

    // Bulk read
    let sample_ids = &all_ids[0..100];
    let results = db.get_nodes(sample_ids).unwrap();
    assert_eq!(results.len(), 100);
    for r in &results {
        assert!(r.is_some());
    }

    // Neighbors
    let nbrs = db
        .neighbors(all_ids[0], &NeighborOptions::default())
        .unwrap();
    assert!(!nbrs.is_empty());

    // Close and reopen
    db.close().unwrap();
    let db = DatabaseEngine::open(&db_path, &opts).unwrap();
    let node = db.get_node(all_ids[99_999]).unwrap().unwrap();
    assert_eq!(node.key, "node-99999");
    db.close().unwrap();
}

// --- Manifest corruption recovery at engine level ---

/// Corrupt the manifest.current file and verify the engine recovers via manifest.prev.
#[test]
fn test_engine_manifest_corruption_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("manifest_db");
    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };

    // Write data and flush to create a manifest
    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();
        db.upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        // Write a second manifest version so manifest.prev exists
        db.upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Corrupt manifest.current
    let manifest_path = db_path.join("manifest.current");
    assert!(manifest_path.exists(), "manifest.current should exist");
    std::fs::write(&manifest_path, "CORRUPTED {{{").unwrap();

    // Reopen -- should recover via manifest.prev
    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();
        // The engine should open successfully (recovered from prev)
        // We may lose the second flush's manifest entry, but the WAL
        // should replay and recover the data
        let node_a = db.get_node_by_key(1, "a").unwrap();
        assert!(node_a.is_some(), "node 'a' should be recoverable");
        db.close().unwrap();
    }
}

// --- WAL truncation recovery at engine level ---

/// Append garbage to the end of the WAL (simulating crash mid-write),
/// then reopen and verify all valid records are recovered.
#[test]
fn test_engine_wal_truncated_record_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_trunc_db");
    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };

    let node_a;

    // Write valid data
    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();
        node_a = db
            .upsert_node(
                1,
                "valid_node",
                UpsertNodeOptions {
                    props: make_props("k", "v"),
                    ..Default::default()
                },
            )
            .unwrap();
        // Use close_fast() to preserve WAL (close() would flush + retire WAL)
        db.close_fast().unwrap();
    }

    // Append garbage to WAL (simulating crash mid-write of next record)
    let wal_path = db_path.join("wal_0.wal");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Write a partial record frame: valid length but truncated payload
        f.write_all(&100u32.to_le_bytes()).unwrap(); // claims 100 bytes of payload
        f.write_all(&[0xDE, 0xAD]).unwrap(); // but only 2 bytes follow
        f.flush().unwrap();
    }

    // Reopen -- should recover all valid records, ignoring the truncated tail
    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();
        let node = db.get_node(node_a).unwrap();
        assert!(
            node.is_some(),
            "valid node should survive WAL truncation recovery"
        );
        assert_eq!(node.unwrap().key, "valid_node");
        db.close().unwrap();
    }
}

// --- Temporal filtering integration: cross-source (memtable + segment) ---

/// Temporal edge filtering should work consistently across memtable and segments.
#[test]
fn test_temporal_edges_cross_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("temporal_db");
    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };

    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();
    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let d = db
        .upsert_node(1, "d", UpsertNodeOptions::default())
        .unwrap();

    // Edge in segment: A->B valid [1000, 5000)
    db.upsert_edge(
        a,
        b,
        10,
        UpsertEdgeOptions {
            valid_from: Some(1000),
            valid_to: Some(5000),
            ..Default::default()
        },
    )
    .unwrap();
    db.flush().unwrap();

    // Edge in memtable: A->C valid [3000, 9000)
    db.upsert_edge(
        a,
        c,
        10,
        UpsertEdgeOptions {
            valid_from: Some(3000),
            valid_to: Some(9000),
            ..Default::default()
        },
    )
    .unwrap();

    // Always-valid edge in memtable: A->D (explicit valid_from=0 means always-valid)
    db.upsert_edge(
        a,
        d,
        10,
        UpsertEdgeOptions {
            valid_from: Some(0),
            ..Default::default()
        },
    )
    .unwrap();

    // at_epoch=2000: B (segment) + D (always-valid)
    let n = db
        .neighbors(
            a,
            &NeighborOptions {
                at_epoch: Some(2000),
                ..Default::default()
            },
        )
        .unwrap();
    let ids: Vec<u64> = n.iter().map(|e| e.node_id).collect();
    assert!(ids.contains(&b), "B should be visible at t=2000");
    assert!(ids.contains(&d), "D (always-valid) should be visible");
    assert!(!ids.contains(&c), "C should NOT be visible at t=2000");

    // at_epoch=4000: all three
    let n = db
        .neighbors(
            a,
            &NeighborOptions {
                at_epoch: Some(4000),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(n.len(), 3);

    // at_epoch=6000: C (memtable) + D (always-valid)
    let n = db
        .neighbors(
            a,
            &NeighborOptions {
                at_epoch: Some(6000),
                ..Default::default()
            },
        )
        .unwrap();
    let ids: Vec<u64> = n.iter().map(|e| e.node_id).collect();
    assert!(!ids.contains(&b), "B should NOT be visible at t=6000");
    assert!(ids.contains(&c), "C should be visible at t=6000");
    assert!(ids.contains(&d), "D (always-valid) should be visible");

    // Compact and re-verify
    db.upsert_node(1, "filler", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    let n = db
        .neighbors(
            a,
            &NeighborOptions {
                at_epoch: Some(4000),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        n.len(),
        3,
        "temporal filtering should work after compaction"
    );

    db.close().unwrap();
}
