// Write tests: upsert, batch, delete, adjacency verification.

    // --- Upsert API tests ---

    #[test]
    fn test_upsert_node_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let id2 = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();

        assert_ne!(id1, id2);
        assert_eq!(engine.node_count(), 2);
        assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
        assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props_v1 = BTreeMap::new();
        props_v1.insert("version".to_string(), PropValue::Int(1));
        let id1 = engine.upsert_node(1, "alice", props_v1, 0.5).unwrap();

        let mut props_v2 = BTreeMap::new();
        props_v2.insert("version".to_string(), PropValue::Int(2));
        let id2 = engine.upsert_node(1, "alice", props_v2, 0.9).unwrap();

        // Same (type_id, key) → same ID, updated fields
        assert_eq!(id1, id2);
        assert_eq!(engine.node_count(), 1);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_different_types_same_key() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let id2 = engine
            .upsert_node(2, "alice", BTreeMap::new(), 0.5)
            .unwrap();

        // Different type_id → different nodes
        assert_ne!(id1, id2);
        assert_eq!(engine.node_count(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_id_counter_monotonic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(
                engine
                    .upsert_node(1, &format!("node:{}", i), BTreeMap::new(), 0.5)
                    .unwrap(),
            );
        }

        // All IDs should be unique and monotonically increasing
        for i in 1..ids.len() {
            assert!(ids[i] > ids[i - 1]);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let n2 = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();

        let e1 = engine
            .upsert_edge(n1, n2, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        assert_eq!(engine.edge_count(), 1);
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_without_uniqueness_creates_duplicates() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        // Default: edge_uniqueness = false
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let e1 = engine
            .upsert_edge(1, 2, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(1, 2, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Without uniqueness: creates separate edges
        assert_ne!(e1, e2);
        assert_eq!(engine.edge_count(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_with_uniqueness_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let e1 = engine
            .upsert_edge(1, 2, 10, BTreeMap::new(), 0.5, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(1, 2, 10, BTreeMap::new(), 0.9, None, None)
            .unwrap();

        // With uniqueness: same triple → same ID, updated weight
        assert_eq!(e1, e2);
        assert_eq!(engine.edge_count(), 1);
        assert!((engine.get_edge(e1).unwrap().unwrap().weight - 0.9).abs() < f32::EPSILON);

        // Different triple → new edge
        let e3 = engine
            .upsert_edge(1, 2, 20, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        assert_ne!(e1, e3);
        assert_eq!(engine.edge_count(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("node:{}", i),
                props: BTreeMap::new(),
                weight: 0.5,
            })
            .collect();

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 1000);
        assert_eq!(engine.node_count(), 1000);

        // All queryable
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("node:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes_with_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Pre-insert a node
        let pre_id = engine
            .upsert_node(1, "existing", BTreeMap::new(), 0.5)
            .unwrap();

        // Batch with duplicate key and one that matches pre-existing
        let inputs = vec![
            NodeInput {
                type_id: 1,
                key: "new1".into(),
                props: BTreeMap::new(),
                weight: 0.5,
            },
            NodeInput {
                type_id: 1,
                key: "existing".into(),
                props: BTreeMap::new(),
                weight: 0.9,
            },
            NodeInput {
                type_id: 1,
                key: "new1".into(),
                props: BTreeMap::new(),
                weight: 0.8,
            }, // dup within batch
        ];

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[1], pre_id); // "existing" reuses pre-existing ID
        assert_eq!(ids[0], ids[2]); // "new1" appears twice → same ID
        assert_eq!(engine.node_count(), 2); // "existing" + "new1"

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let inputs: Vec<EdgeInput> = (0..100)
            .map(|i| EdgeInput {
                from: i,
                to: i + 1,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();

        let ids = engine.batch_upsert_edges(&inputs).unwrap();
        assert_eq!(ids.len(), 100);
        assert_eq!(engine.edge_count(), 100);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (id1, id2, eid);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            id1 = engine
                .upsert_node(1, "alice", BTreeMap::new(), 0.5)
                .unwrap();
            id2 = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
            eid = engine
                .upsert_edge(id1, id2, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine.close().unwrap();
        }

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.node_count(), 2);
            assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
            assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");
            assert_eq!(engine.get_edge(eid).unwrap().unwrap().from, id1);

            // Upsert dedup should still work after replay
            let id1_again = engine
                .upsert_node(1, "alice", BTreeMap::new(), 0.99)
                .unwrap();
            assert_eq!(id1_again, id1);

            // New allocations should not reuse old IDs
            let id3 = engine
                .upsert_node(1, "charlie", BTreeMap::new(), 0.5)
                .unwrap();
            assert!(id3 > id2);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_upsert_node_preserves_created_at() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let created_at_v1 = engine.get_node(id1).unwrap().unwrap().created_at;

        // Small delay not needed, just upsert again. created_at must be preserved
        let id2 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.9)
            .unwrap();
        assert_eq!(id1, id2);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.created_at, created_at_v1);
        assert!(node.updated_at >= created_at_v1);

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_edges_with_uniqueness() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Pre-insert an edge
        let pre_id = engine
            .upsert_edge(1, 2, 10, BTreeMap::new(), 0.5, None, None)
            .unwrap();

        // Batch with: duplicate within batch + match against pre-existing
        let inputs = vec![
            EdgeInput {
                from: 3,
                to: 4,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 0.5,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: 1,
                to: 2,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 0.9,
                valid_from: None,
                valid_to: None,
            }, // matches pre-existing
            EdgeInput {
                from: 3,
                to: 4,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 0.8,
                valid_from: None,
                valid_to: None,
            }, // dup within batch
        ];

        let ids = engine.batch_upsert_edges(&inputs).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[1], pre_id); // reuses pre-existing ID
        assert_eq!(ids[0], ids[2]); // within-batch dedup
        assert_eq!(engine.edge_count(), 2); // pre-existing + one new

        engine.close().unwrap();
    }

    #[test]
    fn test_id_counters_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let last_node_id;
        let last_edge_id;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("n:{}", i), BTreeMap::new(), 0.5)
                    .unwrap();
            }
            for i in 0..5 {
                engine
                    .upsert_edge(i, i + 1, 10, BTreeMap::new(), 1.0, None, None)
                    .unwrap();
            }
            last_node_id = engine.next_node_id();
            last_edge_id = engine.next_edge_id();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.next_node_id() >= last_node_id);
            assert!(engine.next_edge_id() >= last_edge_id);
            engine.close().unwrap();
        }
    }

    // --- Adjacency, neighbors, delete tests ---

    #[test]
    fn test_neighbors_outgoing() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None)
            .unwrap();

        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 2);
        let neighbor_ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&b));
        assert!(neighbor_ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_incoming() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine
            .upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let inc = engine
            .neighbors(c, Direction::Incoming, None, 0, None, None)
            .unwrap();
        assert_eq!(inc.len(), 2);
        let neighbor_ids: Vec<u64> = inc.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&a));
        assert!(neighbor_ids.contains(&b));

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap(); // type 10
        engine
            .upsert_edge(a, c, 20, BTreeMap::new(), 1.0, None, None)
            .unwrap(); // type 20

        let typed = engine
            .neighbors(a, Direction::Outgoing, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_with_limit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 0.5).unwrap();
        for i in 0..10 {
            let n = engine
                .upsert_node(1, &format!("spoke:{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            engine
                .upsert_edge(hub, n, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }

        let limited = engine
            .neighbors(hub, Direction::Outgoing, None, 3, None, None)
            .unwrap();
        assert_eq!(limited.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        engine.delete_node(b).unwrap();

        assert!(engine.get_node(b).unwrap().is_none());
        assert_eq!(engine.node_count(), 1);

        // b excluded from a's neighbors (node tombstone filtering)
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_edge_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        engine.delete_edge(eid).unwrap();

        assert!(engine.get_edge(eid).unwrap().is_none());
        assert_eq!(engine.edge_count(), 0);
        assert!(engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap()
            .is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (a, b, eid);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            eid = engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine.delete_node(b).unwrap();
            engine.delete_edge(eid).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.get_node(b).unwrap().is_none());
            assert!(engine.get_edge(eid).unwrap().is_none());
            assert_eq!(engine.node_count(), 1);
            assert_eq!(engine.edge_count(), 0);
            assert!(engine
                .neighbors(a, Direction::Outgoing, None, 0, None, None)
                .unwrap()
                .is_empty());
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_neighbors_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (a, b, c);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
            engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine
                .upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None)
                .unwrap();
            engine
                .upsert_edge(b, c, 10, BTreeMap::new(), 0.5, None, None)
                .unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // a → b, c
            let out_a = engine
                .neighbors(a, Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert_eq!(out_a.len(), 2);
            // b → c
            let out_b = engine
                .neighbors(b, Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert_eq!(out_b.len(), 1);
            assert_eq!(out_b[0].node_id, c);
            // c ← a, b
            let inc_c = engine
                .neighbors(c, Direction::Incoming, None, 0, None, None)
                .unwrap();
            assert_eq!(inc_c.len(), 2);
            engine.close().unwrap();
        }
    }

