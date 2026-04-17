// Write tests: upsert, batch, delete, adjacency verification.

    // --- Upsert API tests ---

    #[test]
    fn test_upsert_node_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let id2 = engine.upsert_node(1, "bob", UpsertNodeOptions { weight: 0.6, ..Default::default() }).unwrap();

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
        let id1 = engine.upsert_node(1, "alice", UpsertNodeOptions { props: props_v1, weight: 0.5, ..Default::default() }).unwrap();

        let mut props_v2 = BTreeMap::new();
        props_v2.insert("version".to_string(), PropValue::Int(2));
        let id2 = engine.upsert_node(1, "alice", UpsertNodeOptions { props: props_v2, weight: 0.9, ..Default::default() }).unwrap();

        // Same (type_id, key) → same ID, updated fields
        assert_eq!(id1, id2);
        assert_eq!(engine.node_count(), 1);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_accepts_default_weight() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node_id = engine.upsert_node(1, "alice", UpsertNodeOptions::default()).unwrap();

        let node = engine.get_node(node_id).unwrap().unwrap();
        assert!((node.weight - 1.0).abs() < f32::EPSILON);
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_with_vectors_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };

        let node_id;
        {
            let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            node_id = engine
                .upsert_node(
                    1,
                    "alice",
                    UpsertNodeOptions {
                        weight: 0.5,
                        dense_vector: Some(vec![0.1, 0.2, 0.3]),
                        sparse_vector: Some(vec![(9, 0.0), (4, 1.0), (2, 2.0), (4, 0.5), (2, 0.0)]),
                        ..Default::default()
                    },
                )
                .unwrap();

            let node = engine.get_node(node_id).unwrap().unwrap();
            assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
            assert_eq!(node.sparse_vector, Some(vec![(2, 2.0), (4, 1.5)]));
            engine.close().unwrap();
        }

        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let node = engine.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(node.sparse_vector, Some(vec![(2, 2.0), (4, 1.5)]));
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_dense_vector_requires_config() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let err = engine
            .upsert_node(
                1,
                "alice",
                UpsertNodeOptions {
                    weight: 0.5,
                    dense_vector: Some(vec![0.1, 0.2, 0.3]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_rejects_wrong_dense_dimension() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 2,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let err = engine
            .upsert_node(
                1,
                "alice",
                UpsertNodeOptions {
                    weight: 0.5,
                    dense_vector: Some(vec![0.1, 0.2, 0.3]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
        engine.close().unwrap();
    }

    #[test]
    fn test_write_op_normalizes_node_vectors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 2,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine
            .write_op(&WalOp::UpsertNode(NodeRecord {
                id: 1,
                type_id: 1,
                key: "manual".to_string(),
                props: BTreeMap::new(),
                created_at: 100,
                updated_at: 101,
                weight: 0.5,
                dense_vector: Some(vec![0.1, 0.2]),
                sparse_vector: Some(vec![(5, 0.0), (3, 1.0), (3, 2.0)]),
                last_write_seq: 0,
            }))
            .unwrap();

        let node = engine.get_node(1).unwrap().unwrap();
        assert_eq!(node.dense_vector, Some(vec![0.1, 0.2]));
        assert_eq!(node.sparse_vector, Some(vec![(3, 3.0)]));
        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes_with_vectors_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };

        let alice_id;
        let bob_id;
        {
            let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            let ids = engine
                .batch_upsert_nodes(&[
                    NodeInput {
                        type_id: 1,
                        key: "alice".to_string(),
                        props: BTreeMap::new(),
                        weight: 0.5,
                        dense_vector: Some(vec![0.1, 0.2, 0.3]),
                        sparse_vector: Some(vec![
                            (9, 0.0),
                            (4, 1.0),
                            (2, 2.0),
                            (4, 0.25),
                        ]),
                    },
                    NodeInput {
                        type_id: 1,
                        key: "bob".to_string(),
                        props: BTreeMap::new(),
                        weight: 0.7,
                        dense_vector: None,
                        sparse_vector: None,
                    },
                ])
                .unwrap();

            alice_id = ids[0];
            bob_id = ids[1];

            let alice = engine.get_node(alice_id).unwrap().unwrap();
            assert_eq!(alice.dense_vector, Some(vec![0.1, 0.2, 0.3]));
            assert_eq!(alice.sparse_vector, Some(vec![(2, 2.0), (4, 1.25)]));

            let bob = engine.get_node(bob_id).unwrap().unwrap();
            assert!(bob.dense_vector.is_none());
            assert!(bob.sparse_vector.is_none());
            engine.close().unwrap();
        }

        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let alice = engine.get_node(alice_id).unwrap().unwrap();
        assert_eq!(alice.dense_vector, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(alice.sparse_vector, Some(vec![(2, 2.0), (4, 1.25)]));

        let bob = engine.get_node(bob_id).unwrap().unwrap();
        assert!(bob.dense_vector.is_none());
        assert!(bob.sparse_vector.is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_different_types_same_key() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let id2 = engine
            .upsert_node(2, "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
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
                    .upsert_node(1, &format!("node:{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
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
            .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let n2 = engine.upsert_node(1, "bob", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        let e1 = engine
            .upsert_edge(n1, n2, 10, UpsertEdgeOptions::default())
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
            .upsert_edge(1, 2, 10, UpsertEdgeOptions::default())
            .unwrap();
        let e2 = engine
            .upsert_edge(1, 2, 10, UpsertEdgeOptions::default())
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
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let e2 = engine
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();

        // With uniqueness: same triple → same ID, updated weight
        assert_eq!(e1, e2);
        assert_eq!(engine.edge_count(), 1);
        assert!((engine.get_edge(e1).unwrap().unwrap().weight - 0.9).abs() < f32::EPSILON);

        // Different triple → new edge
        let e3 = engine
            .upsert_edge(1, 2, 20, UpsertEdgeOptions::default())
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
                dense_vector: None,
                sparse_vector: None,
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
            .upsert_node(1, "existing", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();

        // Batch with duplicate key and one that matches pre-existing
        let inputs = vec![
            NodeInput {
                type_id: 1,
                key: "new1".into(),
                props: BTreeMap::new(),
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                type_id: 1,
                key: "existing".into(),
                props: BTreeMap::new(),
                weight: 0.9,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                type_id: 1,
                key: "new1".into(),
                props: BTreeMap::new(),
                weight: 0.8,
                dense_vector: None,
                sparse_vector: None,
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
                .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            id2 = engine.upsert_node(1, "bob", UpsertNodeOptions { weight: 0.6, ..Default::default() }).unwrap();
            eid = engine
                .upsert_edge(id1, id2, 10, UpsertEdgeOptions::default())
                .unwrap();
            engine.close().unwrap();
        }

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // close() flushes to segments; verify via cross-source lookup
            assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 2);
            assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
            assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");
            assert_eq!(engine.get_edge(eid).unwrap().unwrap().from, id1);

            // Upsert dedup should still work after close-flush + reopen
            let id1_again = engine
                .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.99, ..Default::default() })
                .unwrap();
            assert_eq!(id1_again, id1);

            // New allocations should not reuse old IDs
            let id3 = engine
                .upsert_node(1, "charlie", UpsertNodeOptions { weight: 0.5, ..Default::default() })
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
            .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let created_at_v1 = engine.get_node(id1).unwrap().unwrap().created_at;

        // Small delay not needed, just upsert again. created_at must be preserved
        let id2 = engine
            .upsert_node(1, "alice", UpsertNodeOptions { weight: 0.9, ..Default::default() })
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
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
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
                    .upsert_node(1, &format!("n:{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                    .unwrap();
            }
            for i in 0..5 {
                engine
                    .upsert_edge(i, i + 1, 10, UpsertEdgeOptions::default())
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

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(a, c, 20, UpsertEdgeOptions { weight: 0.8, ..Default::default() })
            .unwrap();

        let out = engine
            .neighbors(a, &NeighborOptions::default())
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

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let inc = engine
            .neighbors(c, &NeighborOptions { direction: Direction::Incoming, ..Default::default() })
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

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap(); // type 10
        engine
            .upsert_edge(a, c, 20, UpsertEdgeOptions::default())
            .unwrap(); // type 20

        let typed = engine
            .neighbors(a, &NeighborOptions { type_filter: Some(vec![10]), ..Default::default() })
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

        let hub = engine.upsert_node(1, "hub", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        for i in 0..10 {
            let n = engine
                .upsert_node(1, &format!("spoke:{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            engine
                .upsert_edge(hub, n, 10, UpsertEdgeOptions::default())
                .unwrap();
        }

        let limited = engine
            .neighbors(hub, &NeighborOptions { limit: Some(3), ..Default::default() })
            .unwrap();
        assert_eq!(limited.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        engine
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        engine.delete_node(b).unwrap();

        assert!(engine.get_node(b).unwrap().is_none());
        assert_eq!(engine.node_count(), 1);

        // b excluded from a's neighbors (node tombstone filtering)
        let out = engine
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        assert!(out.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_edge_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        engine.delete_edge(eid).unwrap();

        assert!(engine.get_edge(eid).unwrap().is_none());
        assert_eq!(engine.edge_count(), 0);
        assert!(engine
            .neighbors(a, &NeighborOptions::default())
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
            a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            eid = engine
                .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
                .unwrap();
            engine.delete_node(b).unwrap();
            engine.delete_edge(eid).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.get_node(b).unwrap().is_none());
            assert!(engine.get_edge(eid).unwrap().is_none());
            // close() flushes to segments; use cross-source counts
            assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 1);
            // Verify deleted edge not visible
            assert!(engine
                .neighbors(a, &NeighborOptions::default())
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
            a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            engine
                .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
                .unwrap();
            engine
                .upsert_edge(a, c, 20, UpsertEdgeOptions { weight: 0.8, ..Default::default() })
                .unwrap();
            engine
                .upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // a → b, c
            let out_a = engine
                .neighbors(a, &NeighborOptions::default())
                .unwrap();
            assert_eq!(out_a.len(), 2);
            // b → c
            let out_b = engine
                .neighbors(b, &NeighborOptions::default())
                .unwrap();
            assert_eq!(out_b.len(), 1);
            assert_eq!(out_b[0].node_id, c);
            // c ← a, b
            let inc_c = engine
                .neighbors(c, &NeighborOptions { direction: Direction::Incoming, ..Default::default() })
                .unwrap();
            assert_eq!(inc_c.len(), 2);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_node_property_index_ensure_drop_list_and_conflicting_range_domains() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let eq = engine
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        assert_eq!(eq.state, SecondaryIndexState::Building);

        let eq_again = engine
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        assert_eq!(eq_again.index_id, eq.index_id);

        let range = engine
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        assert_eq!(range.state, SecondaryIndexState::Building);

        let indexes = engine.list_node_property_indexes();
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].prop_key, "color");
        assert_eq!(indexes[1].prop_key, "score");

        let err = engine
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Float,
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));

        assert!(engine
            .drop_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap());
        assert!(!engine
            .drop_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap());

        let indexes = engine.list_node_property_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_id, range.index_id);

        engine.close().unwrap();
    }

    #[test]
    fn test_node_property_index_retry_failed_clears_error_and_preserves_id() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let created = engine
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        engine.shutdown_secondary_index_worker();

        engine
            .with_runtime_manifest_write(|manifest| {
                let entry = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == created.index_id)
                    .unwrap();
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some("boom".to_string());
                Ok(())
            })
            .unwrap();
        engine.rebuild_secondary_index_catalog().unwrap();

        let retried = engine
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        assert_eq!(retried.index_id, created.index_id);
        assert_eq!(retried.state, SecondaryIndexState::Building);
        assert!(retried.last_error.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_ensure_node_property_index_seeds_active_and_immutable_memtables() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut frozen_props = BTreeMap::new();
        frozen_props.insert("status".to_string(), PropValue::String("active".to_string()));
        frozen_props.insert("age".to_string(), PropValue::Int(30));
        let frozen_id = engine
            .upsert_node(
                1,
                "frozen",
                UpsertNodeOptions {
                    props: frozen_props,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let mut active_props = BTreeMap::new();
        active_props.insert("status".to_string(), PropValue::String("active".to_string()));
        active_props.insert("age".to_string(), PropValue::Int(35));
        let active_id = engine
            .upsert_node(
                1,
                "active",
                UpsertNodeOptions {
                    props: active_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let mut bad_props = BTreeMap::new();
        bad_props.insert("status".to_string(), PropValue::String("active".to_string()));
        bad_props.insert("age".to_string(), PropValue::String("old".to_string()));
        let bad_id = engine
            .upsert_node(
                1,
                "bad",
                UpsertNodeOptions {
                    props: bad_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let eq = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        let range = engine
            .ensure_node_property_index(
                1,
                "age",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();

        let status_hash = hash_prop_value(&PropValue::String("active".to_string()));
        let active_eq_ids = engine
            .active_memtable()
            .secondary_eq_state()
            .get(&eq.index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(active_eq_ids.contains(&active_id));
        assert!(active_eq_ids.contains(&bad_id));

        let frozen_eq_ids = engine
            .immutable_memtable(0)
            .secondary_eq_state()
            .get(&eq.index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(frozen_eq_ids.contains(&frozen_id));

        let active_range = engine
            .active_memtable()
            .secondary_range_state()
            .get(&range.index_id)
            .unwrap();
        assert!(active_range.contains(&(35u64 ^ (1u64 << 63), active_id)));
        assert!(!active_range.iter().any(|&(_, node_id)| node_id == bad_id));

        let frozen_range = engine
            .immutable_memtable(0)
            .secondary_range_state()
            .get(&range.index_id)
            .unwrap();
        assert!(frozen_range.contains(&(30u64 ^ (1u64 << 63), frozen_id)));

        engine.close().unwrap();
    }

    #[test]
    fn test_secondary_index_seeding_refreshes_immutable_memtable_bytes_cache() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("status".to_string(), PropValue::String("active".to_string()));
        props.insert("age".to_string(), PropValue::Int(30));
        engine
            .upsert_node(
                1,
                "frozen",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let before = engine.stats().immutable_memtable_bytes;
        let info = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        let after = engine.stats().immutable_memtable_bytes;
        let actual_after: usize = (0..engine.immutable_epoch_count())
            .map(|idx| engine.immutable_memtable(idx).estimated_size())
            .sum();
        assert_eq!(after, actual_after);
        assert!(after >= before);

        engine
            .drop_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        let after_drop = engine.stats().immutable_memtable_bytes;
        let actual_after_drop: usize = (0..engine.immutable_epoch_count())
            .map(|idx| engine.immutable_memtable(idx).estimated_size())
            .sum();
        assert_eq!(after_drop, actual_after_drop);
        assert!(engine
            .list_node_property_indexes()
            .iter()
            .all(|entry| entry.index_id != info.index_id));

        engine.close().unwrap();
    }
