use overgraph::*;
use std::collections::BTreeMap;

fn node_options(key: &str, weight: f32) -> UpsertNodeOptions {
    let mut props = BTreeMap::new();
    props.insert("name".to_string(), PropValue::String(key.to_string()));
    UpsertNodeOptions {
        props,
        weight,
        ..Default::default()
    }
}

fn edge_options(from: u64, to: u64) -> UpsertEdgeOptions {
    let mut props = BTreeMap::new();
    props.insert(
        "label".to_string(),
        PropValue::String(format!("{}→{}", from, to)),
    );
    UpsertEdgeOptions {
        props,
        weight: 1.0,
        valid_from: Some(0),
        valid_to: Some(i64::MAX),
    }
}

#[test]
fn test_full_phase1_lifecycle() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("integration_db");

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 1..=100 {
            let type_id = match i % 3 {
                0 => 1,
                1 => 2,
                _ => 3,
            };
            let key = format!("key:{}", i);
            let id = engine
                .upsert_node(type_id, &key, node_options(&key, 0.5 + (i as f32 * 0.01)))
                .unwrap();
            assert_eq!(id, i);
        }

        for i in 1..=200 {
            let from = (i % 100) + 1;
            let to = ((i * 7) % 100) + 1;
            let type_id = if i <= 100 { 10 } else { 20 };
            let id = engine
                .upsert_edge(from, to, type_id, edge_options(from, to))
                .unwrap();
            assert_eq!(id, i);
        }

        assert_eq!(engine.node_count().unwrap(), 100);
        assert_eq!(engine.edge_count().unwrap(), 200);

        engine.delete_node(50).unwrap();
        engine.delete_edge(100).unwrap();

        assert_eq!(engine.node_count().unwrap(), 99);
        assert!(engine.edge_count().unwrap() < 200);
        assert!(engine.get_node(50).unwrap().is_none());
        assert!(engine.get_edge(100).unwrap().is_none());

        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let node1 = engine.get_node(1).unwrap().unwrap();
        assert_eq!(node1.key, "key:1");
        assert_eq!(node1.type_id, 2);

        let node99 = engine.get_node(99).unwrap().unwrap();
        assert_eq!(node99.key, "key:99");

        assert!(engine.get_node(50).unwrap().is_none());

        let edge1 = engine.get_edge(1).unwrap().unwrap();
        assert_eq!(edge1.from, 2);
        assert_eq!(edge1.to, 8);
        assert_eq!(edge1.type_id, 10);

        assert!(engine.get_edge(100).unwrap().is_none());

        let edge150 = engine.get_edge(150).unwrap().unwrap();
        assert_eq!(edge150.type_id, 20);

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

        assert!(engine.manifest().unwrap().next_node_id > 100);
        assert!(engine.manifest().unwrap().next_edge_id > 200);

        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(engine.get_node(1).unwrap().is_some());
        assert!(engine.get_node(99).unwrap().is_some());
        assert!(engine.get_node(50).unwrap().is_none());
        assert!(engine.get_edge(1).unwrap().is_some());
        assert!(engine.get_edge(100).unwrap().is_none());
        engine.close().unwrap();
    }
}

#[test]
fn test_wal_replay_last_write_wins() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("update_db");

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "stable-key", node_options("original", 0.51))
            .unwrap();
        let id2 = engine
            .upsert_node(1, "stable-key", node_options("updated", 0.99))
            .unwrap();

        assert_eq!(id1, id2);
        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node = engine.get_node(1).unwrap().unwrap();
        assert_eq!(
            node.props.get("name"),
            Some(&PropValue::String("updated".to_string()))
        );
        assert!((node.weight - 0.99).abs() < f32::EPSILON);
        assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 1);
        engine.close().unwrap();
    }
}

#[test]
fn test_crash_recovery_without_close() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("crash_db");

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        for i in 1..=20 {
            let key = format!("crash:{}", i);
            let id = engine
                .upsert_node(1, &key, node_options(&key, 0.5 + (i as f32 * 0.01)))
                .unwrap();
            assert_eq!(id, i);
        }
        drop(engine);
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count().unwrap(), 20);
        let node10 = engine.get_node(10).unwrap().unwrap();
        assert_eq!(node10.key, "crash:10");
        engine.close().unwrap();
    }
}
