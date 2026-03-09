use overgraph::*;
use std::collections::BTreeMap;

fn make_node(id: u64, type_id: u32, key: &str) -> NodeRecord {
    let mut props = BTreeMap::new();
    props.insert("name".to_string(), PropValue::String(key.to_string()));
    NodeRecord {
        id,
        type_id,
        key: key.to_string(),
        props,
        created_at: id as i64 * 1000,
        updated_at: id as i64 * 1000 + 1,
        weight: 0.5 + (id as f32 * 0.01),
    }
}

fn make_edge(id: u64, from: u64, to: u64, type_id: u32) -> EdgeRecord {
    let mut props = BTreeMap::new();
    props.insert(
        "label".to_string(),
        PropValue::String(format!("{}→{}", from, to)),
    );
    EdgeRecord {
        id,
        from,
        to,
        type_id,
        props,
        created_at: id as i64 * 2000,
        updated_at: id as i64 * 2000 + 1,
        weight: 1.0,
        valid_from: 0,
        valid_to: i64::MAX,
    }
}

/// Full lifecycle integration test:
/// Open DB → write 100 nodes + 200 edges → close → reopen → verify all data
#[test]
fn test_full_phase1_lifecycle() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("integration_db");

    // --- Write phase ---
    {
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Insert 100 nodes across 3 types
        for i in 1..=100 {
            let type_id = match i % 3 {
                0 => 1, // entity
                1 => 2, // chunk
                _ => 3, // fact
            };
            let node = make_node(i, type_id, &format!("key:{}", i));
            engine.write_op(&WalOp::UpsertNode(node)).unwrap();
        }

        // Insert 200 edges across 2 types
        for i in 1..=200 {
            let from = (i % 100) + 1;
            let to = ((i * 7) % 100) + 1;
            let type_id = if i <= 100 { 10 } else { 20 };
            let edge = make_edge(i, from, to, type_id);
            engine.write_op(&WalOp::UpsertEdge(edge)).unwrap();
        }

        assert_eq!(engine.node_count(), 100);
        assert_eq!(engine.edge_count(), 200);

        // Delete a few nodes and edges
        engine
            .write_op(&WalOp::DeleteNode {
                id: 50,
                deleted_at: 999999,
            })
            .unwrap();
        engine
            .write_op(&WalOp::DeleteEdge {
                id: 100,
                deleted_at: 999999,
            })
            .unwrap();

        assert_eq!(engine.node_count(), 99);
        assert_eq!(engine.edge_count(), 199);

        engine.close().unwrap();
    }

    // --- Reopen and verify ---
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Verify counts
        assert_eq!(engine.node_count(), 99);
        assert_eq!(engine.edge_count(), 199);

        // Verify specific nodes
        let node1 = engine.get_node(1).unwrap().unwrap();
        assert_eq!(node1.key, "key:1");
        assert_eq!(node1.type_id, 2); // 1 % 3 == 1 → type 2

        let node99 = engine.get_node(99).unwrap().unwrap();
        assert_eq!(node99.key, "key:99");

        // Verify deleted node is gone
        assert!(engine.get_node(50).unwrap().is_none());

        // Verify specific edges
        let edge1 = engine.get_edge(1).unwrap().unwrap();
        assert_eq!(edge1.from, 2); // (1 % 100) + 1
        assert_eq!(edge1.to, 8); // ((1 * 7) % 100) + 1
        assert_eq!(edge1.type_id, 10);

        // Verify deleted edge is gone
        assert!(engine.get_edge(100).unwrap().is_none());

        // Verify edge in second type range
        let edge150 = engine.get_edge(150).unwrap().unwrap();
        assert_eq!(edge150.type_id, 20);

        // Verify properties survived
        let node_props = &engine.get_node(1).unwrap().unwrap().props;
        assert_eq!(
            node_props.get("name"),
            Some(&PropValue::String("key:1".to_string()))
        );

        let edge_props = &engine.get_edge(1).unwrap().unwrap().props;
        assert_eq!(
            edge_props.get("label"),
            Some(&PropValue::String("2→8".to_string()))
        );

        // Verify manifest ID counters are correct
        assert!(engine.manifest().next_node_id > 100);
        assert!(engine.manifest().next_edge_id > 200);

        engine.close().unwrap();
    }

    // --- Third open: verify stability (no data drift from double close/open) ---
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 99);
        assert_eq!(engine.edge_count(), 199);
        engine.close().unwrap();
    }
}

/// Test that WAL replay handles updates (same ID written twice = last write wins)
#[test]
fn test_wal_replay_last_write_wins() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("update_db");

    {
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write node, then update it
        let v1 = make_node(1, 1, "original");
        engine.write_op(&WalOp::UpsertNode(v1)).unwrap();

        let mut v2 = make_node(1, 1, "updated");
        v2.weight = 0.99;
        v2.updated_at = 999999;
        engine.write_op(&WalOp::UpsertNode(v2)).unwrap();

        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 1);
        let node = engine.get_node(1).unwrap().unwrap();
        assert_eq!(node.key, "updated");
        assert!((node.weight - 0.99).abs() < f32::EPSILON);
        assert_eq!(node.updated_at, 999999);
        engine.close().unwrap();
    }
}

/// Test crash recovery: write data, simulate crash (don't close), reopen
#[test]
fn test_crash_recovery_without_close() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("crash_db");

    // Write data but DON'T call close(). Simulates crash
    {
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        for i in 1..=20 {
            engine
                .write_op(&WalOp::UpsertNode(make_node(i, 1, &format!("crash:{}", i))))
                .unwrap();
        }
        // Intentionally not calling close(). Simulate process death
        // WAL was flushed on each write, so data should be recoverable
        drop(engine);
    }

    // Reopen, should recover from WAL
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 20);
        let node10 = engine.get_node(10).unwrap().unwrap();
        assert_eq!(node10.key, "crash:10");
        engine.close().unwrap();
    }
}
