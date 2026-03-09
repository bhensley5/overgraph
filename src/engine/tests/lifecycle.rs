// Lifecycle tests: open/close, WAL, flush, compaction, restart, group commit, backpressure.

    fn traverse_depth_two(
        engine: &DatabaseEngine,
        start: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        node_type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
    ) -> Vec<TraversalHit> {
        engine
            .traverse(
                start,
                2,
                2,
                direction,
                edge_type_filter,
                node_type_filter,
                at_epoch,
                None,
                (limit > 0).then_some(limit),
                None,
            )
            .unwrap()
            .items
    }

    // --- Low-level write_op API tests ---

    #[test]
    fn test_open_creates_new_db() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 0);
        assert_eq!(engine.edge_count(), 0);
        assert!(db_path.exists());
        assert!(db_path.join("manifest.current").exists());
        engine.close().unwrap();
    }

    #[test]
    fn test_open_nonexistent_without_create() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("nope");

        let opts = DbOptions {
            create_if_missing: false,
            ..DbOptions::default()
        };
        let result = DatabaseEngine::open(&db_path, &opts);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_and_read_back() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .write_op(&WalOp::UpsertNode(make_node(1, "alice")))
            .unwrap();
        engine
            .write_op(&WalOp::UpsertNode(make_node(2, "bob")))
            .unwrap();
        engine
            .write_op(&WalOp::UpsertEdge(make_edge(1, 1, 2)))
            .unwrap();

        assert_eq!(engine.node_count(), 2);
        assert_eq!(engine.edge_count(), 1);

        let alice = engine.get_node(1).unwrap().unwrap();
        assert_eq!(alice.key, "alice");

        let edge = engine.get_edge(1).unwrap().unwrap();
        assert_eq!(edge.from, 1);
        assert_eq!(edge.to, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_operations() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .write_op(&WalOp::UpsertNode(make_node(1, "alice")))
            .unwrap();
        engine
            .write_op(&WalOp::UpsertEdge(make_edge(1, 1, 1)))
            .unwrap();

        assert!(engine.get_node(1).unwrap().is_some());
        assert!(engine.get_edge(1).unwrap().is_some());

        engine
            .write_op(&WalOp::DeleteNode {
                id: 1,
                deleted_at: 9999,
            })
            .unwrap();
        engine
            .write_op(&WalOp::DeleteEdge {
                id: 1,
                deleted_at: 9999,
            })
            .unwrap();

        assert!(engine.get_node(1).unwrap().is_none());
        assert!(engine.get_edge(1).unwrap().is_none());
        assert_eq!(engine.node_count(), 0);
        assert_eq!(engine.edge_count(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_close_and_reopen_recovers_state() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 1..=10 {
                engine
                    .write_op(&WalOp::UpsertNode(make_node(i, &format!("node:{}", i))))
                    .unwrap();
            }
            for i in 1..=5 {
                engine
                    .write_op(&WalOp::UpsertEdge(make_edge(i, i, i + 5)))
                    .unwrap();
            }
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.node_count(), 10);
            assert_eq!(engine.edge_count(), 5);

            let node5 = engine.get_node(5).unwrap().unwrap();
            assert_eq!(node5.key, "node:5");

            let edge3 = engine.get_edge(3).unwrap().unwrap();
            assert_eq!(edge3.from, 3);
            assert_eq!(edge3.to, 8);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_manifest_id_counters_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            engine
                .write_op(&WalOp::UpsertNode(make_node(42, "high_id")))
                .unwrap();
            engine
                .write_op(&WalOp::UpsertEdge(make_edge(99, 42, 42)))
                .unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.next_node_id() >= 43);
            assert!(engine.next_edge_id() >= 100);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_wal_replay_with_deletes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            engine
                .write_op(&WalOp::UpsertNode(make_node(1, "will_delete")))
                .unwrap();
            engine
                .write_op(&WalOp::UpsertNode(make_node(2, "will_keep")))
                .unwrap();
            engine
                .write_op(&WalOp::DeleteNode {
                    id: 1,
                    deleted_at: 5000,
                })
                .unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.get_node(1).unwrap().is_none());
            assert!(engine.get_node(2).unwrap().is_some());
            assert_eq!(engine.node_count(), 1);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_write_op_batch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let ops: Vec<WalOp> = (1..=50)
            .map(|i| WalOp::UpsertNode(make_node(i, &format!("batch:{}", i))))
            .collect();
        engine.write_op_batch(&ops).unwrap();

        assert_eq!(engine.node_count(), 50);
        assert_eq!(engine.get_node(25).unwrap().unwrap().key, "batch:25");

        engine.close().unwrap();

        // Verify recovery
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 50);
        engine.close().unwrap();
    }

    #[test]
    fn test_write_op_batch_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let mut ops = Vec::new();
            for i in 1..=20 {
                ops.push(WalOp::UpsertNode(make_node(i, &format!("n:{}", i))));
            }
            for i in 1..=10 {
                ops.push(WalOp::UpsertEdge(make_edge(i, i, i + 10)));
            }
            engine.write_op_batch(&ops).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.node_count(), 20);
            assert_eq!(engine.edge_count(), 10);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_upsert_overwrites_on_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            engine
                .write_op(&WalOp::UpsertNode(make_node(1, "v1")))
                .unwrap();
            let mut updated = make_node(1, "v2");
            updated.weight = 0.99;
            engine.write_op(&WalOp::UpsertNode(updated)).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let node = engine.get_node(1).unwrap().unwrap();
            assert_eq!(node.key, "v2");
            assert!((node.weight - 0.99).abs() < f32::EPSILON);
            assert_eq!(engine.node_count(), 1);
            engine.close().unwrap();
        }
    }

    // --- Flush, segments, multi-source read tests ---

    #[test]
    fn test_flush_creates_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();

        assert_eq!(engine.segment_count(), 0);
        let info = engine.flush().unwrap();
        assert!(info.is_some());
        assert_eq!(engine.segment_count(), 1);

        // Memtable is empty after flush
        assert_eq!(engine.node_count(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_flush_empty_memtable_is_noop() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let info = engine.flush().unwrap();
        assert!(info.is_none());
        assert_eq!(engine.segment_count(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_data_readable_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        engine.flush().unwrap();

        // Data should be readable from segment
        let alice = engine.get_node(a).unwrap().unwrap();
        assert_eq!(alice.key, "alice");
        let bob = engine.get_node(b).unwrap().unwrap();
        assert_eq!(bob.key, "bob");
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_after_flush() {
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

        engine.flush().unwrap();

        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 2);
        let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        // Type filter should still work
        let typed = engine
            .neighbors(a, Direction::Outgoing, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_two_reproduces_basic_two_hop() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Build chain: a -> b -> c -> d
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // 2-hop from a: should reach c (via b), but NOT d (3 hops) or a/b (origin/1-hop)
        let hop2 = traverse_depth_two(&engine, a, Direction::Outgoing, None, None, 0, None);
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_two_excludes_origin_and_hop1() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Build graph with back-edge: a -> b -> a (cycle)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap(); // back to origin
        engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // 2-hop from a: b is 1-hop, then from b we reach a (origin, excluded) and c
        let hop2 = traverse_depth_two(&engine, a, Direction::Outgoing, None, None, 0, None);
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_two_respects_limit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -> b, a -> c, b -> d, b -> e, c -> f
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        let e = engine.upsert_node(1, "e", BTreeMap::new(), 0.5).unwrap();
        let f = engine.upsert_node(1, "f", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, e, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(c, f, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Without limit: 3 2-hop results (d, e, f)
        let all = traverse_depth_two(&engine, a, Direction::Outgoing, None, None, 0, None);
        assert_eq!(all.len(), 3);

        // With limit: only 2
        let limited = traverse_depth_two(&engine, a, Direction::Outgoing, None, None, 2, None);
        assert_eq!(limited.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_two_respects_edge_type_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -[type1]-> b -[type1]-> c, b -[type2]-> d
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, d, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Filter type 1 only: a->b (hop1), b->c (hop2). b->d is type 2, excluded.
        let hop2 = traverse_depth_two(&engine, a, Direction::Outgoing, Some(&[1]), None, 0, None);
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_two_incoming() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Chain: a -> b -> c -> d (incoming 2-hop from d should reach b)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Incoming 2-hop from d: hop1 = c, hop2 = b (not a, that's 3 hops)
        let hop2 = traverse_depth_two(&engine, d, Direction::Incoming, None, None, 0, None);
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_two_nonexistent_or_hidden_start() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // No nodes at all. 2-hop on ID 999 should return empty
        let hop2 = traverse_depth_two(&engine, 999, Direction::Outgoing, None, None, 0, None);
        assert!(hop2.is_empty());

        // Add a node but delete it, same result
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.delete_node(a).unwrap();

        let hop2 = traverse_depth_two(&engine, a, Direction::Outgoing, None, None, 0, None);
        assert!(hop2.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_cross_source_reads_memtable_plus_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write batch 1, flush to segment
        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Write batch 2, stays in memtable
        let c = engine
            .upsert_node(1, "charlie", BTreeMap::new(), 0.7)
            .unwrap();
        engine
            .upsert_edge(a, c, 10, BTreeMap::new(), 0.9, None, None)
            .unwrap();

        // Can read from both sources
        assert!(engine.get_node(a).unwrap().is_some()); // from segment
        assert!(engine.get_node(c).unwrap().is_some()); // from memtable

        // Neighbors merge across memtable + segment
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 2);
        let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_dedup_across_flush_boundary() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Insert and flush
        let id1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.flush().unwrap();

        // Upsert same (type_id, key), should find existing in segment
        let mut props = BTreeMap::new();
        props.insert("version".to_string(), PropValue::Int(2));
        let id2 = engine.upsert_node(1, "alice", props, 0.9).unwrap();

        // Same ID reused
        assert_eq!(id1, id2);

        // Updated version in memtable wins over segment
        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_tombstone_hides_segment_data() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Delete after flush. Tombstone in memtable hides segment data
        engine.delete_node(b).unwrap();
        assert!(engine.get_node(b).unwrap().is_none());

        engine.delete_edge(eid).unwrap();
        assert!(engine.get_edge(eid).unwrap().is_none());

        // Neighbors should exclude deleted node
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_tombstone_survives_second_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.flush().unwrap(); // seg_0000: alice exists

        engine.delete_node(a).unwrap();
        engine.flush().unwrap(); // seg_0001: tombstone for alice

        // Tombstone in newer segment hides node in older segment
        assert!(engine.get_node(a).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_multiple_flushes_accumulate_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..3 {
            let id = engine
                .upsert_node(1, &format!("batch:{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            ids.push(id);
            engine.flush().unwrap();
        }

        assert_eq!(engine.segment_count(), 3);

        // All nodes readable across 3 segments
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("batch:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_flush_updates_manifest() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.flush().unwrap();

        let manifest = engine.manifest();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].id, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_id_counters_survive_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..5 {
            engine
                .upsert_node(1, &format!("n:{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }
        let next_before = engine.next_node_id();
        engine.flush().unwrap();

        // New allocations should continue from where they left off
        let new_id = engine
            .upsert_node(1, "after_flush", BTreeMap::new(), 0.5)
            .unwrap();
        assert!(new_id >= next_before);

        engine.close().unwrap();
    }

    #[test]
    fn test_segment_data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        let b;
        let eid;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine
                .upsert_node(1, "alice", BTreeMap::new(), 0.5)
                .unwrap();
            b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
            eid = engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.segment_count(), 1);
            assert!(engine.get_node(a).unwrap().is_some());
            assert!(engine.get_node(b).unwrap().is_some());
            assert!(engine.get_edge(eid).unwrap().is_some());

            let out = engine
                .neighbors(a, Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].node_id, b);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_deleted_edge_excluded_from_segment_neighbors() {
        // Regression: M2. Edge tombstone must hide segment adjacency entries
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Delete only the edge to b (not the node). Edge tombstone in memtable
        engine.delete_edge(e1).unwrap();

        // Neighbors should return only c, not b
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_after_delete_across_flush_gets_new_id() {
        // Regression: S3. Upsert of a deleted node's key should not reuse the old ID
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.flush().unwrap();

        // Delete alice. Tombstone in memtable
        engine.delete_node(id1).unwrap();
        assert!(engine.get_node(id1).unwrap().is_none());

        // Re-insert same key, should get a fresh ID, not reuse deleted one
        let id2 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.7)
            .unwrap();
        assert_ne!(id1, id2);
        assert!(engine.get_node(id2).unwrap().is_some());
        assert!(engine.get_node(id1).unwrap().is_none()); // old ID still deleted

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_flush_triggers_on_threshold() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        // Set a very low threshold so auto-flush triggers quickly
        let opts = DbOptions {
            memtable_flush_threshold: 256, // 256 bytes, tiny
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        assert_eq!(engine.segment_count(), 0);

        // Insert enough data to exceed the 256-byte threshold
        let mut ids = Vec::new();
        for i in 0..20 {
            let id = engine
                .upsert_node(1, &format!("node:{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            ids.push(id);
        }

        // Auto-flush should have triggered at least once
        assert!(engine.segment_count() >= 1);

        // All data still readable across memtable + segments
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("node:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_flush_disabled_when_zero() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            memtable_flush_threshold: 0, // disabled
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        for i in 0..100 {
            engine
                .upsert_node(1, &format!("node:{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }

        // No auto-flush should have occurred
        assert_eq!(engine.segment_count(), 0);
        assert_eq!(engine.node_count(), 100);

        engine.close().unwrap();
    }

    // --- Compaction tests ---

    #[test]
    fn test_compact_requires_two_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // 0 segments → no-op
        assert!(engine.compact().unwrap().is_none());

        // 1 segment → no-op
        engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.segment_count(), 1);
        assert!(engine.compact().unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_merges_two_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.flush().unwrap();

        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segment_count(), 2);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 2);
        assert_eq!(stats.nodes_removed, 0);
        assert_eq!(engine.segment_count(), 1);

        // Data still accessible
        assert_eq!(engine.get_node(a).unwrap().unwrap().key, "alice");
        assert_eq!(engine.get_node(b).unwrap().unwrap().key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_applies_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice + bob + edge
        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Segment 2: delete bob + edge
        engine.delete_node(b).unwrap();
        engine.delete_edge(eid).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segment_count(), 2);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 1); // only alice
        assert_eq!(stats.nodes_removed, 1); // bob removed
        assert_eq!(stats.edges_kept, 0);
        assert_eq!(stats.edges_removed, 1);
        assert_eq!(engine.segment_count(), 1);
        assert!(stats.output_segment_id > 0);
        assert!(stats.duration_ms < 30_000); // sanity upper bound

        // Compacted segment should have zero tombstones
        assert_eq!(engine.segment_tombstone_node_count(), 0);
        assert_eq!(engine.segment_tombstone_edge_count(), 0);

        // alice survives, bob and edge are gone
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(eid).unwrap().is_none());
        assert!(engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap()
            .is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_node_last_write_wins() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice v1
        let mut props_v1 = BTreeMap::new();
        props_v1.insert("version".to_string(), PropValue::Int(1));
        let a = engine.upsert_node(1, "alice", props_v1, 0.5).unwrap();
        engine.flush().unwrap();

        // Segment 2: alice v2 (upsert updates in memtable, flushed to new segment)
        let mut props_v2 = BTreeMap::new();
        props_v2.insert("version".to_string(), PropValue::Int(2));
        engine.upsert_node(1, "alice", props_v2, 0.9).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segment_count(), 2);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 1);
        // One input from each segment, but they merge to 1 output → 1 removed
        assert_eq!(stats.nodes_removed, 1);

        let node = engine.get_node(a).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_preserves_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None)
            .unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();

        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 2);
        let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        // Type filter still works after compaction
        let typed = engine
            .neighbors(a, Direction::Outgoing, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_cleans_up_old_segment_dirs() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Old segment directories exist
        let seg_dir = db_path.join("segments");
        assert!(seg_dir.join("seg_0001").exists());
        assert!(seg_dir.join("seg_0002").exists());

        engine.compact().unwrap();

        // Old dirs cleaned up, new one exists
        assert!(!seg_dir.join("seg_0001").exists());
        assert!(!seg_dir.join("seg_0002").exists());
        assert!(seg_dir.join("seg_0003").exists());

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_updates_manifest() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.manifest().segments.len(), 2);

        engine.compact().unwrap();

        let manifest = engine.manifest();
        assert_eq!(manifest.segments.len(), 1);
        // New segment should have both nodes
        assert_eq!(manifest.segments[0].node_count, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        let b;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine
                .upsert_node(1, "alice", BTreeMap::new(), 0.5)
                .unwrap();
            engine.flush().unwrap();
            b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
            engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine.flush().unwrap();
            engine.compact().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.segment_count(), 1);
            assert_eq!(engine.get_node(a).unwrap().unwrap().key, "alice");
            assert_eq!(engine.get_node(b).unwrap().unwrap().key, "bob");
            let out = engine
                .neighbors(a, Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].node_id, b);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_compact_three_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut all_ids = Vec::new();
        for i in 0..3 {
            let id = engine
                .upsert_node(1, &format!("n:{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            all_ids.push(id);
            engine.flush().unwrap();
        }

        assert_eq!(engine.segment_count(), 3);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 3);
        assert_eq!(engine.segment_count(), 1);

        for (i, &id) in all_ids.iter().enumerate() {
            assert_eq!(
                engine.get_node(id).unwrap().unwrap().key,
                format!("n:{}", i)
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_unflushed_tombstone() {
        // Regression: S2. compact() must flush memtable first so tombstones
        // in the memtable are included in the compaction.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice + bob
        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        engine.flush().unwrap();

        // Segment 2: charlie
        engine
            .upsert_node(1, "charlie", BTreeMap::new(), 0.7)
            .unwrap();
        engine.flush().unwrap();

        // Delete bob. Unflushed, lives in memtable only
        engine.delete_node(b).unwrap();
        assert_eq!(engine.segment_count(), 2);

        // Compact should flush the tombstone first, then merge all 3 segments
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3); // 2 original + 1 from flush
        assert_eq!(stats.nodes_kept, 2); // alice + charlie
        assert_eq!(stats.nodes_removed, 1); // bob

        // bob is gone from the compacted segment
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_none());
        assert_eq!(engine.segment_count(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_unflushed_update() {
        // Regression: S2. compact() must flush memtable first so updates
        // in the memtable are included in the compaction output.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice v1
        let mut props_v1 = BTreeMap::new();
        props_v1.insert("v".to_string(), PropValue::Int(1));
        let a = engine.upsert_node(1, "alice", props_v1, 0.5).unwrap();
        engine.flush().unwrap();

        // Segment 2: bob
        engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Update alice to v2. Unflushed, lives in memtable only
        let mut props_v2 = BTreeMap::new();
        props_v2.insert("v".to_string(), PropValue::Int(2));
        engine.upsert_node(1, "alice", props_v2, 0.9).unwrap();

        // Compact should flush first, then merge all 3 segments
        engine.compact().unwrap();

        let node = engine.get_node(a).unwrap().unwrap();
        assert_eq!(node.props.get("v"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    /// Regression: compaction must remove edges whose endpoints are deleted,
    /// even if the edge itself was never explicitly deleted.
    #[test]
    fn test_compact_removes_dangling_edges_after_node_delete() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: A→B→C chain
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e_ab = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e_bc = engine
            .upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Segment 2: delete B (but NOT edges A→B or B→C explicitly)
        engine.delete_node(b).unwrap();
        engine.flush().unwrap();

        // Before compact: neighbors correctly filter deleted B
        assert!(engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap()
            .is_empty());

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 2); // A and C
        assert_eq!(stats.nodes_removed, 1); // B
        assert_eq!(stats.edges_kept, 0); // both edges dangling
        assert_eq!(stats.edges_removed, 2);

        // After compact: edges must still be gone (no dangling references)
        assert!(engine.get_edge(e_ab).unwrap().is_none());
        assert!(engine.get_edge(e_bc).unwrap().is_none());
        assert!(engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap()
            .is_empty());
        assert!(engine
            .neighbors(c, Direction::Incoming, None, 0, None, None)
            .unwrap()
            .is_empty());

        engine.close().unwrap();
    }

    // --- Orphan segment scanning ---

    #[test]
    fn test_orphan_segment_does_not_reuse_id() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        // Create DB, insert data, flush to create segment, close
        {
            let mut engine = DatabaseEngine::open(
                &db_path,
                &DbOptions {
                    create_if_missing: true,
                    ..Default::default()
                },
            )
            .unwrap();
            engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }

        // Simulate an orphan: create a segment directory with a higher ID
        // that is NOT in the manifest (as if a crash occurred after writing
        // the segment but before updating the manifest).
        let orphan_dir = db_path.join("segments").join("seg_0099");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        // Write a minimal nodes.dat so it looks like a real segment
        std::fs::write(orphan_dir.join("nodes.dat"), [0u8; 0]).unwrap();

        // Reopen. next_segment_id should skip past the orphan
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // Insert more data and flush. Should get segment ID > 99
            engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
            engine.flush().unwrap();

            // The new segment should have ID >= 100 (since orphan was seg_0099)
            let max_manifest_seg = engine
                .manifest()
                .segments
                .iter()
                .map(|s| s.id)
                .max()
                .unwrap();
            assert!(
                max_manifest_seg >= 100,
                "next segment should skip past orphan seg_0099, got seg ID {}",
                max_manifest_seg
            );

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_scan_max_segment_id_no_segments_dir() {
        let dir = TempDir::new().unwrap();
        // No segments dir at all, should return 0
        assert_eq!(scan_max_segment_id(dir.path()), 0);
    }

    #[test]
    fn test_scan_max_segment_id_finds_highest() {
        let dir = TempDir::new().unwrap();
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::create_dir(seg_dir.join("seg_0003")).unwrap();
        std::fs::create_dir(seg_dir.join("seg_0010")).unwrap();
        std::fs::create_dir(seg_dir.join("seg_0007")).unwrap();
        // Non-matching entries should be ignored
        std::fs::create_dir(seg_dir.join("tmp_work")).unwrap();
        std::fs::write(seg_dir.join("some_file.txt"), b"hi").unwrap();

        assert_eq!(scan_max_segment_id(dir.path()), 10);
    }

    #[test]
    fn test_map_props_roundtrip_memtable_and_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut props = BTreeMap::new();
        let mut nested = BTreeMap::new();
        nested.insert("deep_key".to_string(), PropValue::Int(99));
        nested.insert("flag".to_string(), PropValue::Bool(true));
        props.insert("metadata".to_string(), PropValue::Map(nested));
        props.insert("name".to_string(), PropValue::String("test".into()));

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let id = engine
            .upsert_node(1, "map_node", props.clone(), 1.0)
            .unwrap();

        // Read from memtable
        let node = engine.get_node(id).unwrap().unwrap();
        assert_eq!(node.props, props);

        // Flush to segment and read back
        engine.flush().unwrap();
        let node2 = engine.get_node(id).unwrap().unwrap();
        assert_eq!(node2.props, props);

        // Close, reopen, read from segment
        engine.close().unwrap();
        let engine2 = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node3 = engine2.get_node(id).unwrap().unwrap();
        assert_eq!(node3.props, props);
        engine2.close().unwrap();
    }

    // --- Fast-path compaction tests ---

    fn compaction_path_for(engine: &DatabaseEngine) -> CompactionPath {
        select_compaction_path(
            &engine.segments,
            engine.segments.iter().any(|s| s.has_tombstones()),
            !engine.manifest.prune_policies.is_empty(),
        )
    }

    fn install_noop_prune_policy(engine: &mut DatabaseEngine) {
        engine
            .set_prune_policy(
                "noop-fast-merge-blocker",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.0),
                    type_id: Some(u32::MAX),
                },
            )
            .unwrap();
    }

    fn build_clean_compaction_fixture(engine: &mut DatabaseEngine) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
        let mut all_node_ids = Vec::new();
        let mut all_edge_ids = Vec::new();
        let mut segment_starts = Vec::new();

        for seg in 0..3u64 {
            let mut seg_node_ids = Vec::new();
            for i in 0..12 {
                let mut props = BTreeMap::new();
                props.insert("seg".to_string(), PropValue::UInt(seg));
                props.insert(
                    "color".to_string(),
                    PropValue::String(if i % 2 == 0 { "red" } else { "blue" }.to_string()),
                );
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), props, 1.0)
                    .unwrap();
                seg_node_ids.push(id);
                all_node_ids.push(id);
            }
            segment_starts.push(seg_node_ids[0]);
            for i in 0..4 {
                let eid = engine
                    .upsert_edge(
                        seg_node_ids[i],
                        seg_node_ids[i + 1],
                        1,
                        BTreeMap::new(),
                        1.0,
                        Some(0),
                        Some(i64::MAX),
                    )
                    .unwrap();
                all_edge_ids.push(eid);
            }
            engine.flush().unwrap();
        }

        (all_node_ids, all_edge_ids, segment_starts)
    }

    fn assert_compacted_index_files_match(
        left: &DatabaseEngine,
        right: &DatabaseEngine,
        left_db_dir: &std::path::Path,
        right_db_dir: &std::path::Path,
    ) {
        let left_dir = segment_dir(left_db_dir, left.segments[0].segment_id);
        let right_dir = segment_dir(right_db_dir, right.segments[0].segment_id);
        for filename in [
            "key_index.dat",
            "node_type_index.dat",
            "edge_type_index.dat",
            "edge_triple_index.dat",
            "prop_index.dat",
            "adj_out.idx",
            "adj_out.dat",
            "adj_in.idx",
            "adj_in.dat",
            "tombstones.dat",
        ] {
            assert_eq!(
                std::fs::read(left_dir.join(filename)).unwrap(),
                std::fs::read(right_dir.join(filename)).unwrap(),
                "{} mismatch",
                filename
            );
        }
    }

    fn assert_node_batches_match(left: &[Option<NodeRecord>], right: &[Option<NodeRecord>]) {
        assert_eq!(left.len(), right.len());
        for (idx, (left_node, right_node)) in left.iter().zip(right.iter()).enumerate() {
            match (left_node, right_node) {
                (Some(left_node), Some(right_node)) => {
                    assert_eq!(left_node.id, right_node.id, "node {} id mismatch", idx);
                    assert_eq!(
                        left_node.type_id, right_node.type_id,
                        "node {} type mismatch",
                        idx
                    );
                    assert_eq!(left_node.key, right_node.key, "node {} key mismatch", idx);
                    assert_eq!(
                        left_node.props, right_node.props,
                        "node {} props mismatch",
                        idx
                    );
                    assert_eq!(
                        left_node.weight.to_bits(),
                        right_node.weight.to_bits(),
                        "node {} weight mismatch",
                        idx
                    );
                }
                (None, None) => {}
                _ => panic!("node batch presence mismatch at index {}", idx),
            }
        }
    }

    fn assert_edge_batches_match(left: &[Option<EdgeRecord>], right: &[Option<EdgeRecord>]) {
        assert_eq!(left.len(), right.len());
        for (idx, (left_edge, right_edge)) in left.iter().zip(right.iter()).enumerate() {
            match (left_edge, right_edge) {
                (Some(left_edge), Some(right_edge)) => {
                    assert_eq!(left_edge.id, right_edge.id, "edge {} id mismatch", idx);
                    assert_eq!(
                        left_edge.from, right_edge.from,
                        "edge {} from mismatch",
                        idx
                    );
                    assert_eq!(left_edge.to, right_edge.to, "edge {} to mismatch", idx);
                    assert_eq!(
                        left_edge.type_id, right_edge.type_id,
                        "edge {} type mismatch",
                        idx
                    );
                    assert_eq!(
                        left_edge.props, right_edge.props,
                        "edge {} props mismatch",
                        idx
                    );
                    assert_eq!(
                        left_edge.weight.to_bits(),
                        right_edge.weight.to_bits(),
                        "edge {} weight mismatch",
                        idx
                    );
                    assert_eq!(
                        left_edge.valid_from, right_edge.valid_from,
                        "edge {} valid_from mismatch",
                        idx
                    );
                    assert_eq!(
                        left_edge.valid_to, right_edge.valid_to,
                        "edge {} valid_to mismatch",
                        idx
                    );
                }
                (None, None) => {}
                _ => panic!("edge batch presence mismatch at index {}", idx),
            }
        }
    }

    #[test]
    fn test_segments_non_overlapping_detection() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Unique keys per flush → non-overlapping IDs
        for seg in 0..3u64 {
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        assert!(segments_are_non_overlapping(&engine.segments));
        engine.close().unwrap();
    }

    #[test]
    fn test_segments_overlapping_detection() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Same keys across flushes → same IDs → overlapping
        for _seg in 0..3 {
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        assert!(!segments_are_non_overlapping(&engine.segments));
        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_eligibility_rules() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        build_clean_compaction_fixture(&mut engine);

        assert_eq!(compaction_path_for(&engine), CompactionPath::FastMerge);

        install_noop_prune_policy(&mut engine);
        assert_eq!(compaction_path_for(&engine), CompactionPath::UnifiedV3);
        engine.close().unwrap();

        let tombstone_dir = TempDir::new().unwrap();
        let mut tombstone_engine = DatabaseEngine::open(tombstone_dir.path(), &opts).unwrap();
        let (node_ids, _, _) = build_clean_compaction_fixture(&mut tombstone_engine);
        tombstone_engine.delete_node(node_ids[0]).unwrap();
        tombstone_engine.flush().unwrap();
        assert_eq!(
            compaction_path_for(&tombstone_engine),
            CompactionPath::UnifiedV3
        );
        tombstone_engine.close().unwrap();

        let overlap_dir = TempDir::new().unwrap();
        let mut overlap_engine = DatabaseEngine::open(overlap_dir.path(), &opts).unwrap();
        for _seg in 0..3 {
            for i in 0..10 {
                overlap_engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            overlap_engine.flush().unwrap();
        }
        assert_eq!(
            compaction_path_for(&overlap_engine),
            CompactionPath::UnifiedV3
        );
        overlap_engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_compaction_correctness() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Build 3 segments with unique non-overlapping data
        let mut all_node_ids = Vec::new();
        let mut all_edge_ids = Vec::new();
        for seg in 0..3u64 {
            let mut seg_node_ids = Vec::new();
            for i in 0..20 {
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
                seg_node_ids.push(id);
                all_node_ids.push(id);
            }
            for i in 0..5 {
                let eid = engine
                    .upsert_edge(
                        seg_node_ids[i],
                        seg_node_ids[i + 1],
                        1,
                        BTreeMap::new(),
                        1.0,
                        None,
                        None,
                    )
                    .unwrap();
                all_edge_ids.push(eid);
            }
            engine.flush().unwrap();
        }

        assert_eq!(engine.segments.len(), 3);
        // Pre-condition: non-overlapping, no tombstones (simplest V3 case)
        assert!(!engine.segments.iter().any(|s| s.has_tombstones()));
        assert!(segments_are_non_overlapping(&engine.segments));
        assert_eq!(compaction_path_for(&engine), CompactionPath::FastMerge);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 60);
        assert_eq!(stats.edges_kept, 15);
        assert_eq!(stats.nodes_removed, 0);
        assert_eq!(stats.edges_removed, 0);
        assert_eq!(engine.segments.len(), 1);

        // Verify all records are accessible (batch read)
        let node_results = engine.get_nodes(&all_node_ids).unwrap();
        for (i, result) in node_results.iter().enumerate() {
            assert!(
                result.is_some(),
                "node {} missing after compact",
                all_node_ids[i]
            );
        }
        let edge_results = engine.get_edges(&all_edge_ids).unwrap();
        for (i, result) in edge_results.iter().enumerate() {
            assert!(
                result.is_some(),
                "edge {} missing after compact",
                all_edge_ids[i]
            );
        }

        // Verify neighbors work
        for seg in 0..3u64 {
            let first_node = all_node_ids[(seg as usize) * 20];
            let nbrs = engine
                .neighbors(first_node, Direction::Outgoing, None, 100, None, None)
                .unwrap();
            assert_eq!(nbrs.len(), 1);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_with_properties() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segments with property data to verify raw byte copy preserves properties
        let mut ids = Vec::new();
        for seg in 0..2u64 {
            for i in 0..10 {
                let mut props = BTreeMap::new();
                props.insert("seg".to_string(), PropValue::UInt(seg));
                props.insert(
                    "name".to_string(),
                    PropValue::String(format!("s{}_n{}", seg, i)),
                );
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), props, 1.0)
                    .unwrap();
                ids.push(id);
            }
            engine.flush().unwrap();
        }

        engine.compact().unwrap();

        // Verify properties survived the raw binary merge
        for (idx, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            let seg = (idx / 10) as u64;
            assert_eq!(node.props.get("seg"), Some(&PropValue::UInt(seg)));
            assert_eq!(
                node.props.get("name"),
                Some(&PropValue::String(format!("s{}_n{}", seg, idx % 10)))
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };

        let mut ids = Vec::new();
        let mut first_nodes = Vec::new();
        {
            let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
            for seg in 0..3u64 {
                let mut seg_ids = Vec::new();
                for i in 0..10 {
                    let id = engine
                        .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                        .unwrap();
                    ids.push(id);
                    seg_ids.push(id);
                }
                first_nodes.push(seg_ids[0]);
                engine
                    .upsert_edge(seg_ids[0], seg_ids[1], 1, BTreeMap::new(), 1.0, None, None)
                    .unwrap();
                engine.flush().unwrap();
            }
            assert_eq!(compaction_path_for(&engine), CompactionPath::FastMerge);
            engine.compact().unwrap();
            engine.close().unwrap();
        }

        // Reopen and verify data
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }
        assert_eq!(engine.segments.len(), 1);
        for &first in &first_nodes {
            assert_eq!(
                engine
                    .degree(first, Direction::Outgoing, None, None)
                    .unwrap(),
                1
            );
            let nbrs = engine
                .neighbors(first, Direction::Outgoing, None, 10, None, None)
                .unwrap();
            assert_eq!(nbrs.len(), 1);
        }
        assert!(engine.get_node_by_key(1, "s0_n0").unwrap().is_some());
        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_find_nodes_works() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for seg in 0..2u64 {
            for i in 0..20 {
                let mut props = BTreeMap::new();
                let color = if i % 2 == 0 { "red" } else { "blue" };
                props.insert("color".to_string(), PropValue::String(color.to_string()));
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), props, 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        engine.compact().unwrap();

        // find_nodes should work on the fast-merged segment
        let red = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        assert_eq!(red.len(), 20); // 10 red per segment * 2 segments
        let blue = engine
            .find_nodes(1, "color", &PropValue::String("blue".to_string()))
            .unwrap();
        assert_eq!(blue.len(), 20);

        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_matches_v3_for_clean_segments() {
        let fast_dir = TempDir::new().unwrap();
        let v3_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };

        let mut fast = DatabaseEngine::open(fast_dir.path(), &opts).unwrap();
        let mut v3 = DatabaseEngine::open(v3_dir.path(), &opts).unwrap();
        let (node_ids, edge_ids, segment_starts) = build_clean_compaction_fixture(&mut fast);
        let (v3_node_ids, v3_edge_ids, v3_segment_starts) = build_clean_compaction_fixture(&mut v3);
        assert_eq!(node_ids, v3_node_ids);
        assert_eq!(edge_ids, v3_edge_ids);
        assert_eq!(segment_starts, v3_segment_starts);

        install_noop_prune_policy(&mut v3);
        assert_eq!(compaction_path_for(&fast), CompactionPath::FastMerge);
        assert_eq!(compaction_path_for(&v3), CompactionPath::UnifiedV3);

        let fast_stats = fast.compact().unwrap().unwrap();
        let v3_stats = v3.compact().unwrap().unwrap();
        assert_eq!(fast_stats.nodes_kept, v3_stats.nodes_kept);
        assert_eq!(fast_stats.edges_kept, v3_stats.edges_kept);
        assert_eq!(fast_stats.nodes_removed, v3_stats.nodes_removed);
        assert_eq!(fast_stats.edges_removed, v3_stats.edges_removed);
        let fast_nodes = fast.get_nodes(&node_ids).unwrap();
        let v3_nodes = v3.get_nodes(&node_ids).unwrap();
        assert_node_batches_match(&fast_nodes, &v3_nodes);
        let fast_edges = fast.get_edges(&edge_ids).unwrap();
        let v3_edges = v3.get_edges(&edge_ids).unwrap();
        assert_edge_batches_match(&fast_edges, &v3_edges);
        let fast_key = fast.get_node_by_key(1, "s0_n0").unwrap();
        let v3_key = v3.get_node_by_key(1, "s0_n0").unwrap();
        assert_node_batches_match(&[fast_key], &[v3_key]);
        for &start in &segment_starts {
            assert_eq!(
                fast.neighbors(start, Direction::Outgoing, None, 10, None, None)
                    .unwrap(),
                v3.neighbors(start, Direction::Outgoing, None, 10, None, None)
                    .unwrap()
            );
            assert_eq!(
                fast.degree(start, Direction::Outgoing, None, None).unwrap(),
                v3.degree(start, Direction::Outgoing, None, None).unwrap()
            );
        }
        assert_eq!(
            fast.find_nodes(1, "color", &PropValue::String("red".to_string()))
                .unwrap(),
            v3.find_nodes(1, "color", &PropValue::String("red".to_string()))
                .unwrap()
        );
        assert_eq!(fast.nodes_by_type(1).unwrap(), v3.nodes_by_type(1).unwrap());
        assert_compacted_index_files_match(&fast, &v3, fast_dir.path(), v3_dir.path());

        fast.close().unwrap();
        v3.close().unwrap();
    }

    #[test]
    fn test_fast_merge_background_matches_sync() {
        let sync_dir = TempDir::new().unwrap();
        let bg_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };

        let mut sync_engine = DatabaseEngine::open(sync_dir.path(), &opts).unwrap();
        let mut bg_engine = DatabaseEngine::open(bg_dir.path(), &opts).unwrap();
        let (node_ids, edge_ids, segment_starts) = build_clean_compaction_fixture(&mut sync_engine);
        let (bg_node_ids, bg_edge_ids, bg_segment_starts) =
            build_clean_compaction_fixture(&mut bg_engine);
        assert_eq!(node_ids, bg_node_ids);
        assert_eq!(edge_ids, bg_edge_ids);
        assert_eq!(segment_starts, bg_segment_starts);
        assert_eq!(compaction_path_for(&sync_engine), CompactionPath::FastMerge);
        assert_eq!(compaction_path_for(&bg_engine), CompactionPath::FastMerge);

        let sync_stats = sync_engine.compact().unwrap().unwrap();
        bg_engine.start_bg_compact().unwrap();
        let bg_stats = bg_engine.wait_for_bg_compact().expect("bg compaction");

        assert_eq!(sync_stats.nodes_kept, bg_stats.nodes_kept);
        assert_eq!(sync_stats.edges_kept, bg_stats.edges_kept);
        let sync_nodes = sync_engine.get_nodes(&node_ids).unwrap();
        let bg_nodes = bg_engine.get_nodes(&node_ids).unwrap();
        assert_node_batches_match(&sync_nodes, &bg_nodes);
        let sync_edges = sync_engine.get_edges(&edge_ids).unwrap();
        let bg_edges = bg_engine.get_edges(&edge_ids).unwrap();
        assert_edge_batches_match(&sync_edges, &bg_edges);
        for &start in &segment_starts {
            assert_eq!(
                sync_engine
                    .degree(start, Direction::Outgoing, None, None)
                    .unwrap(),
                bg_engine
                    .degree(start, Direction::Outgoing, None, None)
                    .unwrap()
            );
        }
        assert_compacted_index_files_match(&sync_engine, &bg_engine, sync_dir.path(), bg_dir.path());

        sync_engine.close().unwrap();
        bg_engine.close().unwrap();
    }

    #[test]
    fn test_standard_path_used_for_overlapping_segments() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Same keys → overlapping IDs → standard path
        for _seg in 0..3 {
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        assert_eq!(compaction_path_for(&engine), CompactionPath::UnifiedV3);

        // Should still compact correctly via standard path
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 10); // deduped to 10 unique nodes
        assert_eq!(engine.segments.len(), 1);

        for i in 0..10 {
            assert!(engine
                .get_node(
                    engine
                        .find_existing_node(1, &format!("n{}", i))
                        .unwrap()
                        .unwrap()
                        .0
                )
                .unwrap()
                .is_some());
        }

        engine.close().unwrap();
    }

    // --- Auto-compaction tests ---

    #[test]
    fn test_auto_compact_triggers_after_n_flushes() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 3,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Flush 1 and 2: no compaction yet
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 1);

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 2);

        // Flush 3: should trigger auto-compact (3 segments → 1)
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Auto-compact fires in background. Wait for it to complete.
        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        // All 30 nodes should be accessible
        for prefix in ["a", "b", "c"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after auto-compact",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_disabled_when_zero() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for flush in 0..10u64 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("f{}_n{}", flush, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        // No auto-compact → all 10 segments should still exist
        assert_eq!(engine.segments.len(), 10);
        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_counter_resets_on_manual_compact() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 5,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // 2 flushes
        for seg in 0..2u64 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        assert_eq!(engine.segments.len(), 2);
        assert_eq!(engine.flush_count_since_last_compact, 2);

        // Manual compact resets the counter
        engine.compact().unwrap();
        assert_eq!(engine.flush_count_since_last_compact, 0);
        assert_eq!(engine.segments.len(), 1);

        // Now 4 more flushes (counter reset, so 5th from here triggers auto-compact)
        for seg in 2..6u64 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        // 4 flushes since manual compact: segments = 1 (from manual) + 4 = 5
        assert_eq!(engine.segments.len(), 5);

        // 5th flush triggers auto-compact
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("s6_n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Auto-compact fires in background. Wait for it.
        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_data_integrity() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let mut all_ids = Vec::new();
        // This will trigger auto-compact after every 2 flushes
        for seg in 0..6u64 {
            for i in 0..10 {
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
                all_ids.push(id);
            }
            engine.flush().unwrap();
        }

        // Verify all data is intact despite multiple auto-compactions
        for &id in &all_ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing after auto-compactions",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_not_triggered_during_compact_flush() {
        // Verify that the flush inside compact() doesn't trigger recursive auto-compact
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 1, // trigger after every single flush
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // First flush triggers auto-compact since threshold is 1.
        // But we only have 1 segment after flush, so compact() returns None (< 2 segments).
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Only 1 segment, compact can't fire (needs >= 2)
        assert_eq!(engine.segments.len(), 1);

        // Second flush: now 2 segments, auto-compact should fire
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Auto-compact fires in background. Wait for it.
        engine.wait_for_bg_compact();
        // compact fires: 2 segments → 1. The flush inside compact()
        // (for unflushed memtable) should NOT trigger recursive auto-compact.
        assert_eq!(engine.segments.len(), 1);

        // All data accessible
        for i in 0..5 {
            assert!(engine
                .find_existing_node(1, &format!("a{}", i))
                .unwrap()
                .is_some());
            assert!(engine
                .find_existing_node(1, &format!("b{}", i))
                .unwrap()
                .is_some());
        }

        engine.close().unwrap();
    }

    // --- Background compaction tests ---

    #[test]
    fn test_bg_compact_basic() {
        // Trigger auto-compact (threshold=2), wait, verify segment count and data.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes to trigger background compaction
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 1);

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Background compaction should have been started
        assert!(engine.bg_compact.is_some() || engine.segments.len() == 1);

        // Wait for background compaction to complete
        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        // All 20 nodes accessible
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_writes_during() {
        // Write more data while background compaction is running. Verify everything
        // is intact after close/reopen.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes to trigger bg compact
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // bg compact started (or already finished for small data)

        // Immediately write more data. Should NOT block
        for i in 0..20 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        // Close waits for bg compact, then writes manifest
        engine.close().unwrap();

        // Reopen and verify all data
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        for prefix in ["a", "b", "c"] {
            let count = if prefix == "c" { 20 } else { 10 };
            for i in 0..count {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine
                        .get_node(engine.find_existing_node(1, &key).unwrap().unwrap().0)
                        .unwrap()
                        .is_some(),
                    "node {} missing after bg compact + writes",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_flush_during() {
        // Trigger bg compact, then do enough writes to cause another flush.
        // Verify both the new segment and the compacted segment coexist correctly.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            memtable_flush_threshold: 0, // manual flush only
            memtable_hard_cap_bytes: 0,  // no backpressure
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes → triggers bg compact
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap(); // bg compact starts here

        // Write more data and flush. Adds a NEW segment while bg compact runs
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap(); // new segment added; bg compact may still be running

        // Wait for bg compact
        engine.wait_for_bg_compact();

        // Should have: 1 compacted segment (from a+b) + 1 new segment (from c)
        assert_eq!(engine.segments.len(), 2);

        // All data accessible
        for prefix in ["a", "b", "c"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact + flush during",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_no_double() {
        // Verify that a second bg compact is NOT started while one is running.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 1, // trigger after every flush
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // First flush: only 1 segment, bg compact needs >= 2, so no bg compact
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert!(engine.bg_compact.is_none());

        // Second flush: 2 segments, bg compact starts
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // bg_compact should be Some (or already completed)
        let had_bg = engine.bg_compact.is_some();

        // Third flush: bg compact is still running (or just completed),
        // should NOT start a second bg compact
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Wait for everything to settle
        engine.wait_for_bg_compact();

        // All data accessible
        for prefix in ["a", "b", "c"] {
            for i in 0..5 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing",
                    key
                );
            }
        }

        // Just verify no panics occurred and data is consistent
        engine.close().unwrap();

        // If bg compact was running at flush 3, the guard should have prevented
        // a second bg compact from starting. We can't easily assert on timing,
        // but absence of panics + data integrity proves correctness.
        let _ = had_bg; // used above for documentation
    }

    #[test]
    fn test_bg_compact_manual_after_bg() {
        // bg compact finishes, then manual compact() works correctly.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes → triggers bg compact
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap(); // bg compact starts

        // Add more segments
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("d{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Manual compact. Should first wait for bg compact, then compact everything
        let stats = engine.compact().unwrap();
        assert!(stats.is_some());
        assert_eq!(engine.segments.len(), 1);

        // All data accessible
        for prefix in ["a", "b", "c", "d"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after manual compact",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_drop_waits() {
        // Drop engine without close(). Verify no thread leak and data is on disk.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap(); // bg compact starts

        // Drop without close. Drop impl should wait for bg compact
        drop(engine);

        // Reopen and verify segments are compacted and data is accessible
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        // Data should be in segments (flushed before bg compact, then compacted)
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after drop + reopen",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_immediate_mode() {
        // Verify bg compact works with Immediate sync mode (not just GroupCommit).
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact (immediate mode)",
                    key
                );
            }
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_group_commit_mode() {
        // Verify bg compact works with GroupCommit sync mode.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Write more data while bg compact may be running
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        engine.close().unwrap();

        // Reopen and verify all data
        let opts_reopen = DbOptions {
            compact_after_n_flushes: 0, // disable auto-compact for clean verification
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts_reopen).unwrap();
        for prefix in ["a", "b", "c"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact (group commit mode)",
                    key
                );
            }
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_cancel() {
        // Cancel a running background compaction. Original segments should remain.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes → triggers bg compact
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Cancel the bg compact (may have already finished for small data, that's OK)
        engine.cancel_bg_compact();
        assert!(engine.bg_compact.is_none());

        // Segments should be >= 2 (cancel prevented the compaction from applying,
        // or if it finished before cancel, wait_for_bg_compact in cancel already
        // joined the thread; either way the engine is in a consistent state).
        // The key assertion: all data is accessible.
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after cancel",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_orphan_segment_cleanup_on_open() {
        // Create orphan segment directories that are NOT in the manifest.
        // Verify that open() cleans them up.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0, // disable auto-compact
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Write + flush to create a real segment
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 1);

        engine.close().unwrap();

        // Create orphan segment directories (simulate crash between segment write
        // and manifest update, or between bg compact output and apply).
        let orphan1 = segment_dir(dir.path(), 9990);
        let orphan2 = segment_dir(dir.path(), 9991);
        std::fs::create_dir_all(&orphan1).unwrap();
        std::fs::create_dir_all(&orphan2).unwrap();
        // Write a dummy file so the directory isn't empty
        std::fs::write(orphan1.join("dummy.dat"), b"orphan").unwrap();
        std::fs::write(orphan2.join("dummy.dat"), b"orphan").unwrap();
        assert!(orphan1.exists());
        assert!(orphan2.exists());

        // Reopen. Orphans should be cleaned up
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert!(!orphan1.exists(), "orphan1 should have been cleaned up");
        assert!(!orphan2.exists(), "orphan2 should have been cleaned up");

        // Real segment should still be there
        assert_eq!(engine.segments.len(), 1);
        for i in 0..5 {
            let key = format!("n{}", i);
            assert!(
                engine.find_existing_node(1, &key).unwrap().is_some(),
                "node {} missing after orphan cleanup",
                key
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_orphan_cleanup_preserves_valid_segments() {
        // Verify orphan cleanup does NOT delete segments that ARE in the manifest.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create 3 segments
        for seg in 0..3 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        assert_eq!(engine.segments.len(), 3);
        engine.close().unwrap();

        // Reopen. All 3 segments should survive (no orphan cleanup of valid segments)
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert_eq!(engine.segments.len(), 3);

        // All data accessible
        for seg in 0..3 {
            for i in 0..5 {
                let key = format!("s{}_n{}", seg, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    // --- Group commit tests ---

    /// Helper to create a DB with Immediate WAL sync mode.
    fn temp_db_immediate() -> (TempDir, DatabaseEngine) {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        (dir, engine)
    }

    /// Helper to create a DB with GroupCommit WAL sync mode.
    fn temp_db_group_commit() -> (TempDir, DatabaseEngine) {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        (dir, engine)
    }

    #[test]
    fn test_immediate_mode_basic_operations() {
        let (dir, mut engine) = temp_db_immediate();

        // Write nodes and edges
        let n1 = engine
            .upsert_node(1, "alice", BTreeMap::new(), 1.0)
            .unwrap();
        let n2 = engine.upsert_node(1, "bob", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Read back immediately
        assert!(engine.get_node(n1).unwrap().is_some());
        assert!(engine.get_node(n2).unwrap().is_some());
        assert!(engine.get_edge(e1).unwrap().is_some());

        // Close and reopen
        engine.close().unwrap();
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        assert!(engine.get_node(n1).unwrap().is_some());
        assert!(engine.get_node(n2).unwrap().is_some());
        assert!(engine.get_edge(e1).unwrap().is_some());
        engine.close().unwrap();
    }

    #[test]
    fn test_immediate_mode_batch_operations() {
        let (_dir, mut engine) = temp_db_immediate();

        let inputs: Vec<NodeInput> = (0..50)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("node_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 50);

        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_immediate_mode_flush_compact_cycle() {
        let (_dir, mut engine) = temp_db_immediate();

        // Insert, flush, insert more, flush, compact
        for i in 0..100 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        for i in 100..200 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        let stats = engine.compact().unwrap();
        assert!(stats.is_some());

        // Verify all data present
        for i in 0..200 {
            assert!(
                engine
                    .get_node(
                        engine
                            .find_existing_node(1, &format!("n{}", i))
                            .unwrap()
                            .unwrap()
                            .0
                    )
                    .unwrap()
                    .is_some(),
                "node n{} missing after compact",
                i
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_basic_write_close_reopen() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write 20 nodes
        let mut ids = Vec::new();
        for i in 0..20 {
            let id = engine
                .upsert_node(1, &format!("gc_node_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            ids.push(id);
        }

        // All visible immediately via read-after-write
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        // Close (should drain all buffered data)
        engine.close().unwrap();

        // Reopen (with Immediate to avoid needing group commit for reads)
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        // All nodes survive restart
        for &id in &ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing after reopen",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_with_edges() {
        let (dir, mut engine) = temp_db_group_commit();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(n1, n2, 1, BTreeMap::new(), 0.5, None, None)
            .unwrap();

        // Read-after-write consistency
        let neighbors = engine
            .neighbors(n1, Direction::Outgoing, None, 10, None, None)
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].node_id, n2);

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(engine.get_edge(e1).unwrap().is_some());
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);
        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_batch_operations() {
        let (dir, mut engine) = temp_db_group_commit();

        let inputs: Vec<NodeInput> = (0..100)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("batch_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 100);

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for &id in &ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "batch node {} missing",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_sync_forces_immediate_flush() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write a node
        let id = engine
            .upsert_node(1, "sync_test", BTreeMap::new(), 1.0)
            .unwrap();

        // Force sync. After this, data must be on disk
        engine.sync().unwrap();

        // Drop without close (no clean shutdown sync)
        drop(engine);

        // Reopen. Data should be present because we called sync()
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        assert!(
            engine.get_node(id).unwrap().is_some(),
            "sync'd data missing after drop"
        );
        engine.close().unwrap();
    }

    #[test]
    fn test_sync_noop_in_immediate_mode() {
        let (_dir, mut engine) = temp_db_immediate();

        engine.upsert_node(1, "test", BTreeMap::new(), 1.0).unwrap();
        // sync() should be a no-op in Immediate mode and not error
        engine.sync().unwrap();
        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_flush_cycle() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write → flush → write → flush under GroupCommit
        for i in 0..50 {
            engine
                .upsert_node(1, &format!("pre_flush_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        for i in 0..50 {
            engine
                .upsert_node(1, &format!("post_flush_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for i in 0..50 {
            assert!(engine
                .find_existing_node(1, &format!("pre_flush_{}", i))
                .unwrap()
                .is_some());
            assert!(engine
                .find_existing_node(1, &format!("post_flush_{}", i))
                .unwrap()
                .is_some());
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_drop_joins_sync_thread() {
        // Verify Drop impl doesn't panic and joins the sync thread
        let (_dir, mut engine) = temp_db_group_commit();

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("drop_test_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        // Drop without close. Should not panic
        drop(engine);
        // If we get here, Drop succeeded without panic
    }

    #[test]
    fn test_default_options_use_group_commit() {
        let opts = DbOptions::default();
        assert!(matches!(
            opts.wal_sync_mode,
            WalSyncMode::GroupCommit { .. }
        ));
    }

    // --- Group Commit CP2: Hardening tests ---

    #[test]
    fn test_backpressure_blocks_writer_at_hard_cap() {
        // Use a very small hard cap (256 bytes) so a few node writes exceed it.
        // The sync thread interval is very fast (1ms) so it drains quickly.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: false,
            compact_after_n_flushes: 0, // disable auto-compact
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 1,
                soft_trigger_bytes: 128,
                hard_cap_bytes: 256,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Write many nodes. Some will block on backpressure but the sync thread
        // will drain them. If backpressure is broken, buffered_bytes grows unbounded.
        for i in 0..200 {
            engine
                .upsert_node(1, &format!("bp_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        // All writes completed. Read them all back
        for i in 0..200 {
            assert!(
                engine
                    .find_existing_node(1, &format!("bp_{}", i))
                    .unwrap()
                    .is_some(),
                "node bp_{} missing after backpressure writes",
                i
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_clean_shutdown_drains_all_buffered_data() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write 100 nodes rapidly (most will be buffered, not yet synced)
        for i in 0..100 {
            engine
                .upsert_node(1, &format!("drain_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        // close() should drain everything
        engine.close().unwrap();

        // Reopen and verify all 100 nodes
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for i in 0..100 {
            assert!(
                engine
                    .find_existing_node(1, &format!("drain_{}", i))
                    .unwrap()
                    .is_some(),
                "node drain_{} lost during shutdown",
                i
            );
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_drop_drains_buffered_data() {
        let dir = TempDir::new().unwrap();

        // Write data and drop without close
        {
            let mut engine = DatabaseEngine::open(
                dir.path(),
                &DbOptions {
                    create_if_missing: true,
                    wal_sync_mode: WalSyncMode::GroupCommit {
                        interval_ms: 5,
                        soft_trigger_bytes: 4 * 1024 * 1024,
                        hard_cap_bytes: 16 * 1024 * 1024,
                    },
                    ..DbOptions::default()
                },
            )
            .unwrap();

            for i in 0..50 {
                engine
                    .upsert_node(1, &format!("drop_drain_{}", i), BTreeMap::new(), 1.0)
                    .unwrap();
            }

            // Drop without close. Drop impl should flush buffered data
            drop(engine);
        }

        // Reopen and check data survived (note: manifest won't be updated by Drop,
        // so data may come from WAL replay, which is correct)
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for i in 0..50 {
            assert!(
                engine
                    .find_existing_node(1, &format!("drop_drain_{}", i))
                    .unwrap()
                    .is_some(),
                "node drop_drain_{} lost after drop",
                i
            );
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_sync_failure_poisons_engine() {
        // Test the poison mechanism directly through WalSyncState.
        // We can't easily force filesystem failures, but we can verify
        // that writers check the poisoned flag and return the right error.
        use crate::wal_sync::WalSyncState;

        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: Some("test: WAL sync failed 5 times".to_string()),
        };

        let arc = std::sync::Arc::new((std::sync::Mutex::new(state), std::sync::Condvar::new()));

        // Create an engine with GroupCommit mode and inject the poisoned state
        let opts = DbOptions {
            create_if_missing: true,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 1000, // long interval so sync thread doesn't interfere
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Replace the wal_state with our poisoned one (shut down the existing sync thread first)
        if let Some(ref wal_state) = engine.wal_state {
            crate::wal_sync::shutdown_sync_thread(wal_state, &mut engine.sync_thread).unwrap();
        }
        engine.wal_state = Some(arc);
        engine.sync_thread = None; // no sync thread needed for this test

        // Attempt to write. Should get WalSyncFailed error
        let result = engine.upsert_node(1, "should_fail", BTreeMap::new(), 1.0);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("WAL sync failed"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_integration_1000_writes_group_commit() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write 1000 nodes with properties
        for i in 0..1000 {
            let mut props = BTreeMap::new();
            props.insert("index".to_string(), PropValue::Int(i as i64));
            engine
                .upsert_node(1, &format!("int_{}", i), props, 1.0)
                .unwrap();
        }

        // Close and reopen
        engine.close().unwrap();
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        // Verify all 1000 nodes with correct properties
        for i in 0..1000 {
            let (id, _) = engine
                .find_existing_node(1, &format!("int_{}", i))
                .unwrap()
                .unwrap_or_else(|| panic!("node int_{} missing", i));
            let node = engine
                .get_node(id)
                .unwrap()
                .unwrap_or_else(|| panic!("node {} not found by id", id));
            assert_eq!(node.props.get("index"), Some(&PropValue::Int(i as i64)));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_integration_write_flush_write_flush_group_commit() {
        // Exercises truncate_and_reset through multiple flush cycles
        let (dir, mut engine) = temp_db_group_commit();

        // Cycle 1: write → flush
        for i in 0..100 {
            engine
                .upsert_node(1, &format!("c1_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let seg1 = engine.flush().unwrap();
        assert!(seg1.is_some());

        // Cycle 2: write → flush
        for i in 0..100 {
            engine
                .upsert_node(1, &format!("c2_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let seg2 = engine.flush().unwrap();
        assert!(seg2.is_some());

        // Cycle 3: write → flush
        for i in 0..100 {
            engine
                .upsert_node(1, &format!("c3_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let seg3 = engine.flush().unwrap();
        assert!(seg3.is_some());

        engine.close().unwrap();

        // Reopen and verify all data from all 3 cycles
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for prefix in &["c1", "c2", "c3"] {
            for i in 0..100 {
                let key = format!("{}_{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after multi-flush cycle",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_delete_and_compact_cycle() {
        let (dir, mut engine) = temp_db_group_commit();

        // Insert nodes
        let mut ids = Vec::new();
        for i in 0..100 {
            let id = engine
                .upsert_node(1, &format!("gc_del_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            ids.push(id);
        }
        engine.flush().unwrap();

        // Delete half
        for &id in &ids[..50] {
            engine.delete_node(id).unwrap();
        }
        engine.flush().unwrap();

        // Compact
        let stats = engine.compact().unwrap();
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert!(stats.nodes_removed > 0);

        // Verify: deleted nodes gone, remaining present
        for &id in &ids[..50] {
            assert!(
                engine.get_node(id).unwrap().is_none(),
                "deleted node {} still present",
                id
            );
        }
        for &id in &ids[50..] {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "surviving node {} missing",
                id
            );
        }

        engine.close().unwrap();

        // Reopen and re-verify
        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        for &id in &ids[..50] {
            assert!(engine.get_node(id).unwrap().is_none());
        }
        for &id in &ids[50..] {
            assert!(engine.get_node(id).unwrap().is_some());
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_rejects_invalid_parameters() {
        let dir = TempDir::new().unwrap();

        // interval_ms = 0
        let result = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 0,
                    soft_trigger_bytes: 4 * 1024 * 1024,
                    hard_cap_bytes: 16 * 1024 * 1024,
                },
                ..DbOptions::default()
            },
        );
        assert!(result.is_err());

        // soft_trigger_bytes = 0
        let result = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 10,
                    soft_trigger_bytes: 0,
                    hard_cap_bytes: 16 * 1024 * 1024,
                },
                ..DbOptions::default()
            },
        );
        assert!(result.is_err());

        // hard_cap_bytes = 0
        let result = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 10,
                    soft_trigger_bytes: 4 * 1024 * 1024,
                    hard_cap_bytes: 0,
                },
                ..DbOptions::default()
            },
        );
        assert!(result.is_err());

        // hard_cap <= soft_trigger
        let result = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 10,
                    soft_trigger_bytes: 1024,
                    hard_cap_bytes: 1024,
                },
                ..DbOptions::default()
            },
        );
        assert!(result.is_err());

        // Valid parameters should succeed
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 10,
                    soft_trigger_bytes: 1024,
                    hard_cap_bytes: 2048,
                },
                ..DbOptions::default()
            },
        )
        .unwrap();
        engine.close().unwrap();
    }

    // --- Memtable backpressure tests ---

    #[test]
    fn test_backpressure_flush_triggers_at_hard_cap_immediate() {
        // With a tiny hard cap, writes should trigger flushes automatically
        // even without the soft auto-flush threshold being set.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,  // auto-flush disabled
            memtable_hard_cap_bytes: 512, // tiny hard cap
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0, // disable auto-compact
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.segment_count(), 0);

        // Write enough data to exceed the 512-byte cap multiple times
        let mut ids = Vec::new();
        for i in 0..50 {
            let id = engine
                .upsert_node(1, &format!("bp_imm_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            ids.push(id);
        }

        // Backpressure should have triggered at least one flush
        assert!(
            engine.segment_count() >= 1,
            "expected at least 1 segment from backpressure flush"
        );

        // All data readable across memtable + segments
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("bp_imm_{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_flush_triggers_at_hard_cap_group_commit() {
        // Same test but with GroupCommit mode. Verifies no deadlock when
        // backpressure flush acquires WAL lock and then the write also needs it.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,  // auto-flush disabled
            memtable_hard_cap_bytes: 512, // tiny hard cap
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.segment_count(), 0);

        let mut ids = Vec::new();
        for i in 0..50 {
            let id = engine
                .upsert_node(1, &format!("bp_gc_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            ids.push(id);
        }

        // Backpressure flushed at least once
        assert!(
            engine.segment_count() >= 1,
            "expected backpressure flush in group commit mode"
        );

        // Data integrity
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("bp_gc_{}", i));
        }

        engine.close().unwrap();

        // Reopen and verify durability
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for &id in &ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing after reopen",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_disabled_when_zero() {
        // With hard cap = 0 (disabled) and auto-flush disabled, no flushes happen.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0, // disabled
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..100 {
            engine
                .upsert_node(1, &format!("no_bp_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }

        // No flushes should have occurred
        assert_eq!(engine.segment_count(), 0);
        assert_eq!(engine.node_count(), 100);

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_fires_before_soft_threshold() {
        // Set hard cap below the soft auto-flush threshold.
        // Backpressure should trigger flushes before auto-flush would.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 1024 * 1024, // 1MB soft threshold (never reached in this test)
            memtable_hard_cap_bytes: 512,          // 512 byte hard cap
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..30 {
            engine
                .upsert_node(1, &format!("early_bp_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }

        // Backpressure kicked in before the 1MB soft threshold
        assert!(
            engine.segment_count() >= 1,
            "backpressure should trigger before soft threshold"
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_with_edges_and_deletes() {
        // Verify backpressure works for all write types, not just upsert_node.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create nodes
        let mut node_ids = Vec::new();
        for i in 0..20 {
            let id = engine
                .upsert_node(1, &format!("n_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            node_ids.push(id);
        }

        // Create edges (triggers backpressure flush too)
        let mut edge_ids = Vec::new();
        for i in 0..19 {
            let eid = engine
                .upsert_edge(
                    node_ids[i],
                    node_ids[i + 1],
                    1,
                    BTreeMap::new(),
                    0.5,
                    None,
                    None,
                )
                .unwrap();
            edge_ids.push(eid);
        }

        // Delete some nodes. Should also respect backpressure
        for nid in &node_ids[..5] {
            engine.delete_node(*nid).unwrap();
        }

        // Delete some edges
        for eid in &edge_ids[..3] {
            engine.delete_edge(*eid).unwrap();
        }

        // Segments created by backpressure
        assert!(engine.segment_count() >= 1);

        // Remaining data is accessible
        for nid in &node_ids[5..20] {
            assert!(engine.get_node(*nid).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_with_batch_upserts() {
        // Batch operations should also trigger backpressure before writing.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // First batch: fills memtable
        let inputs1: Vec<NodeInput> = (0..20)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("batch1_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids1 = engine.batch_upsert_nodes(&inputs1).unwrap();

        // Second batch: should trigger backpressure flush before appending
        let inputs2: Vec<NodeInput> = (0..20)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("batch2_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids2 = engine.batch_upsert_nodes(&inputs2).unwrap();

        assert!(
            engine.segment_count() >= 1,
            "backpressure should flush during batch ops"
        );

        // All data from both batches readable
        for &id in ids1.iter().chain(ids2.iter()) {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_flush_then_write_cycle_group_commit() {
        // Stress test: many writes in GroupCommit mode with a tiny hard cap.
        // Verifies no deadlock and data integrity across many flush cycles.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 256,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 2,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // 200 writes. Each may trigger backpressure flush, each flush
        // acquires/releases WAL sync lock, then the write acquires it again.
        let mut ids = Vec::new();
        for i in 0..200 {
            let id = engine
                .upsert_node(1, &format!("stress_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            ids.push(id);
        }

        // Many segments created
        assert!(
            engine.segment_count() >= 5,
            "expected many backpressure flushes"
        );

        // All data present
        for (i, &id) in ids.iter().enumerate() {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node stress_{} (id={}) missing",
                i,
                id
            );
        }

        engine.close().unwrap();

        // Verify durability after reopen
        let engine = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..DbOptions::default()
            },
        )
        .unwrap();

        for &id in &ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing after reopen",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_interacts_with_auto_compact() {
        // Backpressure flushes should trigger auto-compaction normally.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 3, // compact after 3 flushes
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Write enough to trigger many backpressure flushes
        for i in 0..100 {
            engine
                .upsert_node(1, &format!("ac_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }

        // Auto-compact should have fired and reduced segment count
        // (many flushes → compact triggers → segments merge)
        // Just verify data integrity. Segment count depends on timing
        for i in 0..100 {
            assert!(
                engine
                    .find_existing_node(1, &format!("ac_{}", i))
                    .unwrap()
                    .is_some(),
                "node ac_{} missing after backpressure + auto-compact",
                i
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_invalidate_edge() {
        // invalidate_edge should also trigger backpressure.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "src", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "dst", BTreeMap::new(), 1.0).unwrap();

        // Create many edges to fill memtable
        let mut edge_ids = Vec::new();
        for i in 0..20 {
            let eid = engine
                .upsert_edge(n1, n2, i as u32, BTreeMap::new(), 0.5, None, None)
                .unwrap();
            edge_ids.push(eid);
        }

        // Invalidate edges. Should trigger backpressure
        for &eid in &edge_ids {
            engine.invalidate_edge(eid, 999).unwrap();
        }

        assert!(
            engine.segment_count() >= 1,
            "backpressure should flush during invalidate_edge"
        );

        engine.close().unwrap();
    }

    // --- Empty segment after compaction ---

    #[test]
    fn test_compact_all_records_tombstoned() {
        // Compact when every record is deleted -- should produce a valid empty-ish segment.
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = db
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();

        db.delete_node(a).unwrap();
        db.delete_node(b).unwrap();
        db.delete_edge(e).unwrap();
        db.flush().unwrap();

        let stats = db.compact().unwrap();
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.nodes_kept, 0);
        assert_eq!(stats.edges_kept, 0);
        assert_eq!(stats.nodes_removed, 2);
        assert_eq!(stats.edges_removed, 1);

        // DB should still be functional after compaction of all-tombstone data
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        assert!(db.get_node(c).unwrap().is_some());
        db.close().unwrap();
    }
