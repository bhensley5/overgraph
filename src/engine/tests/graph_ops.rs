// Graph algorithm tests: degree, shortest path, BFS, Dijkstra, all_shortest_paths.

    // --- Phase 18a: Degree counts + aggregations ---

    #[test]
    fn test_degree_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 0);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 2);

        assert_eq!(db.degree(b, &DegreeOptions::default()).unwrap(), 0);
        assert_eq!(db.degree(b, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 1);
        assert_eq!(db.degree(b, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_direction() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // a→b, b→a, a→c
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2); // a→b, a→c
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 1); // b→a
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 3);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 20, UpsertEdgeOptions::default())
            .unwrap();

        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, type_filter: Some(vec![10]), ..Default::default() })
                .unwrap(),
            1
        );
        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, type_filter: Some(vec![20]), ..Default::default() })
                .unwrap(),
            1
        );
        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, type_filter: Some(vec![10, 20]), ..Default::default() })
                .unwrap(),
            2
        );
        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, type_filter: Some(vec![99]), ..Default::default() })
                .unwrap(),
            0
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_self_loop() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        // Self-loop: appears in both adj_out and adj_in but must count once
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 1);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_self_loop_with_normal_edges() {
        // Self-loop + outgoing + incoming. Tests Both dedup more thoroughly
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions::default())
            .unwrap(); // self-loop
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap(); // outgoing
        db.upsert_edge(c, a, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap(); // incoming

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2); // self-loop + a→b
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 2); // self-loop + c→a
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 3); // dedup self-loop

        let sum = db.sum_edge_weights(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();
        assert!((sum - 6.0).abs() < 1e-9); // 1.0 + 2.0 + 3.0

        db.close().unwrap();
    }

    #[test]
    fn test_degree_nonexistent_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        assert_eq!(db.degree(999, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 0);
        db.close().unwrap();
    }

    #[test]
    fn test_degree_deleted_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let e = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        db.delete_edge(e).unwrap();
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_deleted_neighbor_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        db.delete_node(b).unwrap();
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_after_flush() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        db.flush().unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 2);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cross_source_dedup() {
        // Edge exists in segment, then re-upserted in memtable. Count once.
        // Requires edge_uniqueness to produce same edge_id for same (from,to,type).
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            edge_uniqueness: true,
            ..Default::default()
        };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Re-upsert same edge (same from/to/type → same edge_id with uniqueness)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        // Weight should be from the memtable version (newer wins)
        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        assert!((sum - 2.0).abs() < 1e-9);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        {
            let db = open_imm(&db_path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.5, ..Default::default() })
                .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        let db = open_imm(&db_path);
        let a = 1; // Known ID from WAL replay
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        db.close().unwrap();
    }

    #[test]
    fn test_sum_edge_weights_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.5, ..Default::default() })
            .unwrap();

        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        assert!((sum - 5.5).abs() < 1e-9);

        // Zero-degree node
        assert_eq!(
            db.sum_edge_weights(c, &DegreeOptions::default())
                .unwrap(),
            0.0
        );

        db.close().unwrap();
    }

    #[test]
    fn test_sum_edge_weights_after_flush() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        // Crosses memtable + segment
        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        assert!((sum - 5.0).abs() < 1e-9);

        db.close().unwrap();
    }

    #[test]
    fn test_avg_edge_weight_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() })
            .unwrap();

        let avg = db
            .avg_edge_weight(a, &DegreeOptions::default())
            .unwrap();
        assert!(avg.is_some());
        assert!((avg.unwrap() - 3.0).abs() < 1e-9);

        db.close().unwrap();
    }

    #[test]
    fn test_avg_edge_weight_none_for_zero_degree() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        assert_eq!(
            db.avg_edge_weight(999, &DegreeOptions { direction: Direction::Both, ..Default::default() })
                .unwrap(),
            None
        );
        db.close().unwrap();
    }

    #[test]
    fn test_degree_matches_neighbors_len() {
        // Degree must always equal neighbors().len() for the same query
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 20, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(d, a, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, d, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() })
            .unwrap();

        for dir_val in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            for tf in [None, Some(vec![10u32]), Some(vec![20]), Some(vec![10, 20])] {
                let tf_ref = tf.as_deref();
                let deg = db.degree(a, &DegreeOptions { direction: dir_val, type_filter: tf_ref.map(|s| s.to_vec()), ..Default::default() }).unwrap();
                let nbrs = db.neighbors(a, &NeighborOptions { direction: dir_val, type_filter: tf_ref.map(|s| s.to_vec()), ..Default::default() }).unwrap();
                assert_eq!(
                    deg,
                    nbrs.len() as u64,
                    "degree mismatch for dir={:?} filter={:?}: degree={} neighbors={}",
                    dir_val,
                    tf_ref,
                    deg,
                    nbrs.len()
                );
            }
        }

        db.close().unwrap();
    }

    #[test]
    fn test_sum_weight_matches_neighbors() {
        // sum_edge_weights must match summing weights from neighbors()
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.5, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.5, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        let nbrs = db
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        let nbr_sum: f64 = nbrs.iter().map(|e| e.weight as f64).sum();
        assert!((sum - nbr_sum).abs() < 1e-9);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_respects_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "hub", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "keep", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "prune_me", UpsertNodeOptions { weight: 0.1, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        // Before policy: degree=2, sum=5.0
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);
        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        assert!((sum - 5.0).abs() < 1e-9);

        // Register policy: exclude nodes with weight <= 0.5
        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        // After policy: "prune_me" excluded → degree=1, sum=2.0
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        assert!((sum - 2.0).abs() < 1e-9);

        // Must match neighbors().len()
        let nbrs = db
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        assert_eq!(
            db.degree(a, &DegreeOptions::default()).unwrap(),
            nbrs.len() as u64
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_after_compaction() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);
        let sum = db
            .sum_edge_weights(a, &DegreeOptions::default())
            .unwrap();
        assert!((sum - 3.0).abs() < 1e-9);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_self_loop_after_flush() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 1);
        let sum = db.sum_edge_weights(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();
        assert!((sum - 5.0).abs() < 1e-9);

        db.close().unwrap();
    }

    // --- Phase 18a CP2: Batch degrees ---

    #[test]
    fn test_degrees_batch_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let degs = db
            .degrees(&[a, b, c], &DegreeOptions::default())
            .unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 2);
        assert_eq!(*degs.get(&b).unwrap(), 1);
        assert!(!degs.contains_key(&c)); // c has 0 outgoing

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_matches_individual() {
        // Batch degrees must match individual degree() calls
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 20, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(d, a, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let ids = [a, b, c, d];
        for dir_val in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            for tf in [None, Some(vec![10u32]), Some(vec![20]), Some(vec![10, 20])] {
                let tf_ref = tf.as_deref();
                let batch = db.degrees(&ids, &DegreeOptions { direction: dir_val, type_filter: tf_ref.map(|s| s.to_vec()), ..Default::default() }).unwrap();
                for &nid in &ids {
                    let individual = db.degree(nid, &DegreeOptions { direction: dir_val, type_filter: tf_ref.map(|s| s.to_vec()), ..Default::default() }).unwrap();
                    let batch_val = batch.get(&nid).copied().unwrap_or(0);
                    assert_eq!(
                        batch_val, individual,
                        "mismatch for node={} dir={:?} filter={:?}: batch={} individual={}",
                        nid, dir_val, tf_ref, batch_val, individual
                    );
                }
            }
        }

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_cross_segment() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // a has edges in segment1, segment2; b has edge in memtable
        let degs = db
            .degrees(&[a, b, c], &DegreeOptions::default())
            .unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 2);
        assert_eq!(*degs.get(&b).unwrap(), 1);
        assert!(!degs.contains_key(&c));

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_dedup_across_sources() {
        // Same edge in segment + memtable (via edge_uniqueness) counts once
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            edge_uniqueness: true,
            ..Default::default()
        };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();

        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_tombstones() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let e1 = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.delete_edge(e1).unwrap();

        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 1); // only a→c remains

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_self_loop() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        let degs = db.degrees(&[a], &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 2); // self-loop counted once + a→b

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_empty_input() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let degs = db.degrees(&[], &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();
        assert!(degs.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_unsorted_input() {
        // Input need not be sorted. Sorted internally.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Pass unsorted, with duplicates
        let degs = db
            .degrees(&[c, a, b, a], &DegreeOptions::default())
            .unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 1);
        assert_eq!(*degs.get(&b).unwrap(), 1);
        assert!(!degs.contains_key(&c));

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_after_compaction() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let degs = db
            .degrees(&[a, b, c], &DegreeOptions::default())
            .unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 2);
        assert!(!degs.contains_key(&b));
        assert!(!degs.contains_key(&c));

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_respects_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "hub", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "keep", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "prune_me", UpsertNodeOptions { weight: 0.1, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Before policy
        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 2);

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        // After policy: "prune_me" excluded
        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(*degs.get(&a).unwrap(), 1);

        // Must match individual degree
        assert_eq!(
            degs.get(&a).copied().unwrap_or(0),
            db.degree(a, &DegreeOptions::default()).unwrap()
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_matches_neighbors_batch() {
        // Batch degrees must match neighbors_batch().len() for each node
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, c, 20, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(d, a, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();

        let ids = [a, b, c, d];
        for dir_val in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            let degs = db.degrees(&ids, &DegreeOptions { direction: dir_val, ..Default::default() }).unwrap();
            let nbrs = db.neighbors_batch(&ids, &NeighborOptions { direction: dir_val, ..Default::default() }).unwrap();
            for &nid in &ids {
                let deg = degs.get(&nid).copied().unwrap_or(0);
                let nbr_count = nbrs.get(&nid).map(|v| v.len() as u64).unwrap_or(0);
                assert_eq!(
                    deg, nbr_count,
                    "mismatch for node={} dir={:?}: degrees={} neighbors_batch={}",
                    nid, dir_val, deg, nbr_count
                );
            }
        }

        db.close().unwrap();
    }

    // --- Temporal validity tests (P1 fix) ---

    #[test]
    fn test_degree_ignores_expired_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        // Edge with valid_to = 1 (expired since epoch start)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 5.0, valid_from: None, valid_to: Some(1), ..Default::default() })
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);
        assert_eq!(
            db.sum_edge_weights(a, &DegreeOptions::default())
                .unwrap(),
            0.0
        );
        assert_eq!(
            db.avg_edge_weight(a, &DegreeOptions::default())
                .unwrap(),
            None
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_ignores_future_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        // Edge with valid_from far in the future
        let future = now_millis() + 100_000_000;
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 5.0, valid_from: Some(future), valid_to: None, ..Default::default() })
            .unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);
        assert_eq!(
            db.sum_edge_weights(a, &DegreeOptions::default())
                .unwrap(),
            0.0
        );
        assert_eq!(
            db.avg_edge_weight(a, &DegreeOptions::default())
                .unwrap(),
            None
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_ignores_invalidated_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let e1 = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 7.0, ..Default::default() })
            .unwrap();

        // Invalidate e1
        db.invalidate_edge(e1, 1).unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        assert_eq!(
            db.sum_edge_weights(a, &DegreeOptions::default())
                .unwrap(),
            7.0
        );
        assert_eq!(
            db.avg_edge_weight(a, &DegreeOptions::default())
                .unwrap(),
            Some(7.0)
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_ignores_expired_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // a→b: valid, a→c: expired
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 1.0, valid_from: None, valid_to: Some(1), ..Default::default() })
            .unwrap();

        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(degs.get(&a).copied().unwrap_or(0), 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_ignores_future_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let future = now_millis() + 100_000_000;
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(future), valid_to: None, ..Default::default() })
            .unwrap();

        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(degs.get(&a).copied().unwrap_or(0), 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degrees_batch_ignores_invalidated_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let e1 = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.invalidate_edge(e1, 1).unwrap();

        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(degs.get(&a).copied().unwrap_or(0), 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_temporal_after_flush() {
        // Temporal edges flushed to segment. Same filtering must apply.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // a→b: valid, a→c: expired
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, valid_from: None, valid_to: Some(1), ..Default::default() })
            .unwrap();

        db.flush().unwrap();

        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
        assert_eq!(
            db.sum_edge_weights(a, &DegreeOptions::default())
                .unwrap(),
            2.0
        );

        // Batch too
        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(degs.get(&a).copied().unwrap_or(0), 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_at_epoch_parity_with_neighbors() {
        // degree(at_epoch=T) must equal neighbors(at_epoch=T).len()
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // a→b: valid 1000..5000
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(1000), valid_to: Some(5000), ..Default::default() })
            .unwrap();
        // a→c: valid 3000..8000
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, valid_from: Some(3000), valid_to: Some(8000), ..Default::default() })
            .unwrap();

        db.flush().unwrap();

        // At T=500: neither valid
        let t = Some(500);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(), 0);
        assert_eq!(
            db.neighbors(a, &NeighborOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() })
                .unwrap()
                .len(),
            0
        );

        // At T=2000: only a→b valid
        let t = Some(2000);
        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(),
            db.neighbors(a, &NeighborOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() })
                .unwrap()
                .len() as u64
        );
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(), 1);

        // At T=4000: both valid
        let t = Some(4000);
        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(),
            db.neighbors(a, &NeighborOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() })
                .unwrap()
                .len() as u64
        );
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(), 2);

        // At T=6000: only a→c valid
        let t = Some(6000);
        assert_eq!(
            db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(),
            db.neighbors(a, &NeighborOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() })
                .unwrap()
                .len() as u64
        );
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(), 1);

        // At T=9000: neither valid
        let t = Some(9000);
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap(), 0);
        assert_eq!(
            db.neighbors(a, &NeighborOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() })
                .unwrap()
                .len(),
            0
        );

        // Batch at_epoch parity
        let t = Some(4000);
        let degs = db.degrees(&[a], &DegreeOptions { direction: Direction::Outgoing, at_epoch: t, ..Default::default() }).unwrap();
        assert_eq!(degs.get(&a).copied().unwrap_or(0), 2);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_invalidate_after_flush_shadows_segment() {
        // Regression: invalidating an edge after flush must shadow the segment
        // version. The memtable's invalid version must prevent the older valid
        // segment version from being counted.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let e1 = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        // Invalidate in memtable. Segment still has the valid version.
        db.invalidate_edge(e1, 1).unwrap();

        // degree must match neighbors (both should be 0)
        let nbrs = db
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        assert_eq!(nbrs.len(), 0, "neighbors should see 0 after invalidation");
        assert_eq!(
            db.degree(a, &DegreeOptions::default()).unwrap(),
            0,
            "degree must match neighbors after invalidate-after-flush"
        );
        assert_eq!(
            db.sum_edge_weights(a, &DegreeOptions::default())
                .unwrap(),
            0.0
        );
        assert_eq!(
            db.avg_edge_weight(a, &DegreeOptions::default())
                .unwrap(),
            None
        );

        // Batch path too
        let degs = db.degrees(&[a], &DegreeOptions::default()).unwrap();
        assert_eq!(degs.get(&a).copied().unwrap_or(0), 0);

        db.close().unwrap();
    }

    // --- Phase 18b: Shortest path (BFS) ---

    /// Helper: build a linear chain A -> B -> C -> D -> E
    fn build_chain(db: &mut DatabaseEngine) -> Vec<u64> {
        let mut nodes = Vec::new();
        for i in 0..5u64 {
            let key = format!("chain_{}", i);
            nodes.push(db.upsert_node(1, &key, UpsertNodeOptions::default()).unwrap());
        }
        for i in 0..4 {
            db.upsert_edge(nodes[i], nodes[i + 1], 10, UpsertEdgeOptions::default())
                .unwrap();
        }
        nodes
    }

    /// Helper: build a diamond graph
    ///    A
    ///   / \
    ///  B   C
    ///   \ /
    ///    D
    fn build_diamond(db: &mut DatabaseEngine) -> (u64, u64, u64, u64) {
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        (a, b, c, d)
    }

    #[test]
    fn test_shortest_path_direct_neighbors() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let e = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        let path = db
            .shortest_path(a, b, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.nodes, vec![a, b]);
        assert_eq!(p.edges, vec![e]);
        assert_eq!(p.total_cost, 1.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_multi_hop() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let nodes = build_chain(&mut db);

        let path = db
            .shortest_path(nodes[0], nodes[4], &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.nodes.len(), 5);
        assert_eq!(p.nodes[0], nodes[0]);
        assert_eq!(p.nodes[4], nodes[4]);
        assert_eq!(p.edges.len(), 4);
        assert_eq!(p.total_cost, 4.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_no_path_disconnected() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        // No edge between a and b

        let path = db
            .shortest_path(a, b, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_same_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        let path = db
            .shortest_path(a, a, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.nodes, vec![a]);
        assert!(p.edges.is_empty());
        assert_eq!(p.total_cost, 0.0);

        db.close().unwrap();
    }

    #[test]
    fn test_path_apis_same_nonexistent_node_return_empty() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        assert!(db
            .shortest_path(999999, 999999, &ShortestPathOptions::default())
            .unwrap()
            .is_none());
        assert!(!db
            .is_connected(999999, 999999, &IsConnectedOptions::default())
            .unwrap());
        assert!(db
            .all_shortest_paths(999999, 999999, &AllShortestPathsOptions::default())
            .unwrap()
            .is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_directed_no_reverse() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        // a -> b exists, but b -> a in Outgoing direction does not
        let path = db
            .shortest_path(b, a, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_none());

        // With Both direction, b -> a should work
        let path = db
            .shortest_path(b, a, &ShortestPathOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.nodes, vec![b, a]);
        assert_eq!(p.total_cost, 1.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_diamond_finds_shortest() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let (a, _b, _c, d) = build_diamond(&mut db);

        let path = db
            .shortest_path(a, d, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        // Should be length 2 (A -> B/C -> D), not longer
        assert_eq!(p.total_cost, 2.0);
        assert_eq!(p.nodes.len(), 3);
        assert_eq!(p.nodes[0], a);
        assert_eq!(p.nodes[2], d);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_edge_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // Direct path a->c with type 20
        db.upsert_edge(a, c, 20, UpsertEdgeOptions::default())
            .unwrap();
        // Indirect path a->b->c with type 10
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Filter to type 10 only. Must go through b.
        let path = db
            .shortest_path(a, c, &ShortestPathOptions { type_filter: Some(vec![10]), ..Default::default() })
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 2.0);
        assert_eq!(p.nodes, vec![a, b, c]);

        // Filter to type 20. Direct path.
        let path = db
            .shortest_path(a, c, &ShortestPathOptions { type_filter: Some(vec![20]), ..Default::default() })
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 1.0);
        assert_eq!(p.nodes, vec![a, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_max_depth_cutoff() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let nodes = build_chain(&mut db);

        // Path is 4 hops, limit to 2. Should fail.
        let path = db
            .shortest_path(nodes[0], nodes[4], &ShortestPathOptions { max_depth: Some(2), ..Default::default() })
            .unwrap();
        assert!(path.is_none());

        // Limit to 4. Should succeed.
        let path = db
            .shortest_path(nodes[0], nodes[4], &ShortestPathOptions { max_depth: Some(4), ..Default::default() })
            .unwrap();
        assert!(path.is_some());
        assert_eq!(path.unwrap().total_cost, 4.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_temporal_filtering() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // Edge a->b valid from 100 to 200
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(100), valid_to: Some(200), ..Default::default() })
            .unwrap();
        // Edge b->c valid from 100 to 300
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(100), valid_to: Some(300), ..Default::default() })
            .unwrap();

        // At time 150: both edges valid, path exists
        let path = db
            .shortest_path(a, c, &ShortestPathOptions { at_epoch: Some(150), ..Default::default() })
            .unwrap();
        assert!(path.is_some());
        assert_eq!(path.unwrap().total_cost, 2.0);

        // At time 250: a->b expired, no path
        let path = db
            .shortest_path(a, c, &ShortestPathOptions { at_epoch: Some(250), ..Default::default() })
            .unwrap();
        assert!(path.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_is_connected_basic() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let nodes = build_chain(&mut db);

        assert!(db
            .is_connected(nodes[0], nodes[4], &IsConnectedOptions::default())
            .unwrap());
        assert!(db
            .is_connected(nodes[0], nodes[2], &IsConnectedOptions::default())
            .unwrap());

        // Reverse direction: not connected in Outgoing
        assert!(!db
            .is_connected(nodes[4], nodes[0], &IsConnectedOptions::default())
            .unwrap());

        // But connected in Both
        assert!(db
            .is_connected(nodes[4], nodes[0], &IsConnectedOptions { direction: Direction::Both, ..Default::default() })
            .unwrap());

        db.close().unwrap();
    }

    #[test]
    fn test_is_connected_same_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        assert!(db
            .is_connected(a, a, &IsConnectedOptions::default())
            .unwrap());

        db.close().unwrap();
    }

    #[test]
    fn test_is_connected_disconnected() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        assert!(!db
            .is_connected(a, b, &IsConnectedOptions::default())
            .unwrap());

        db.close().unwrap();
    }

    #[test]
    fn test_is_connected_max_depth() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let nodes = build_chain(&mut db);

        // 4 hops needed, max_depth=2 should fail
        assert!(!db
            .is_connected(nodes[0], nodes[4], &IsConnectedOptions { max_depth: Some(2), ..Default::default() })
            .unwrap());

        // max_depth=4 should succeed
        assert!(db
            .is_connected(nodes[0], nodes[4], &IsConnectedOptions { max_depth: Some(4), ..Default::default() })
            .unwrap());

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let nodes = build_chain(&mut db);
        db.flush().unwrap();

        let path = db
            .shortest_path(nodes[0], nodes[4], &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 4.0);
        assert_eq!(p.nodes.len(), 5);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_across_flush_boundary() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        // First segment: a -> b -> c
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Second segment: c -> d -> e
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e = db.upsert_node(1, "e", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(d, e, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Path spans both segments
        let path = db
            .shortest_path(a, e, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 4.0);
        assert_eq!(p.nodes, vec![a, b, c, d, e]);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_after_compact() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        assert_eq!(path.unwrap().total_cost, 2.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_with_deleted_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // Direct path a->c and indirect a->b->c
        let direct = db
            .upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Delete direct edge
        db.delete_edge(direct).unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 2.0);
        assert_eq!(p.nodes, vec![a, b, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_with_deleted_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        // a->b->c and a->d->c
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(d, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Delete node b. Path through b should be unavailable.
        db.delete_node(b).unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 2.0);
        // Must go through d, not b
        assert_eq!(p.nodes, vec![a, d, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_path_apis_respect_deleted_edges_after_flush_and_compact() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        let cheap_ab = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        let cheap_bc = db
            .upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, d, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(d, c, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        db.delete_edge(cheap_ab).unwrap();
        db.delete_edge(cheap_bc).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let bfs = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(bfs.nodes, vec![a, d, c]);
        assert_eq!(bfs.total_cost, 2.0);

        assert!(db
            .is_connected(a, c, &IsConnectedOptions::default())
            .unwrap());

        let bfs_paths = db
            .all_shortest_paths(a, c, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(bfs_paths.len(), 1);
        assert_eq!(bfs_paths[0].nodes, vec![a, d, c]);

        let weighted = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(weighted.nodes, vec![a, d, c]);
        assert_eq!(weighted.total_cost, 10.0);

        let weighted_paths = db
            .all_shortest_paths(a, c, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert_eq!(weighted_paths.len(), 1);
        assert_eq!(weighted_paths[0].nodes, vec![a, d, c]);
        assert_eq!(weighted_paths[0].total_cost, 10.0);

        db.close().unwrap();
    }

    #[test]
    fn test_ingest_mode_suppresses_auto_compact() {
        let dir = TempDir::new().unwrap();
        let db = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();

        db.ingest_mode().unwrap();
        for i in 0..10 {
            db.upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
                .unwrap();
            db.flush().unwrap();
        }
        // 10 segments, no auto-compaction
        assert_eq!(db.segment_count().unwrap(), 10);

        // All data survives
        for i in 0..10 {
            assert!(db.get_node_by_key(1, &format!("n{}", i)).unwrap().is_some());
        }

        let stats = db.end_ingest().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 10);
        assert_eq!(db.segment_count().unwrap(), 1);

        // All data still intact after compaction
        for i in 0..10 {
            assert!(db.get_node_by_key(1, &format!("n{}", i)).unwrap().is_some());
        }

        db.close().unwrap();
    }

    #[test]
    fn test_end_ingest_restores_previous_compact_threshold() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 5,
            ..DbOptions::default()
        };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        assert_eq!(db.compact_after_n_flushes_for_test(), 5);
        db.ingest_mode().unwrap();
        assert_eq!(db.compact_after_n_flushes_for_test(), 0);
        assert_eq!(db.ingest_saved_compact_after_n_flushes_for_test(), Some(5));

        let _ = db.end_ingest().unwrap();
        assert_eq!(db.compact_after_n_flushes_for_test(), 5);
        assert_eq!(db.ingest_saved_compact_after_n_flushes_for_test(), None);

        db.close().unwrap();
    }

    #[test]
    fn test_ingest_mode_is_idempotent_for_saved_threshold() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 5,
            ..DbOptions::default()
        };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        db.ingest_mode().unwrap();
        db.ingest_mode().unwrap();
        assert_eq!(db.compact_after_n_flushes_for_test(), 0);
        assert_eq!(db.ingest_saved_compact_after_n_flushes_for_test(), Some(5));

        let _ = db.end_ingest().unwrap();
        assert_eq!(db.compact_after_n_flushes_for_test(), 5);
        assert_eq!(db.ingest_saved_compact_after_n_flushes_for_test(), None);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_large_fan_out() {
        // Bidirectional BFS should handle large fan-out efficiently
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let end = db.upsert_node(1, "end", UpsertNodeOptions::default()).unwrap();
        let bridge = db.upsert_node(1, "bridge", UpsertNodeOptions::default()).unwrap();

        // 100 dead-end nodes from start
        for i in 0..100 {
            let key = format!("dead_s_{}", i);
            let n = db.upsert_node(1, &key, UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(start, n, 10, UpsertEdgeOptions::default())
                .unwrap();
        }

        // 100 dead-end nodes from end (incoming)
        for i in 0..100 {
            let key = format!("dead_e_{}", i);
            let n = db.upsert_node(1, &key, UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(n, end, 10, UpsertEdgeOptions::default())
                .unwrap();
        }

        // The actual path: start -> bridge -> end
        db.upsert_edge(start, bridge, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(bridge, end, 10, UpsertEdgeOptions::default())
            .unwrap();

        let path = db
            .shortest_path(start, end, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 2.0);
        assert_eq!(p.nodes, vec![start, bridge, end]);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_incoming_direction() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // Edges: a -> b -> c
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // From c to a following Incoming edges (reverse traversal)
        let path = db
            .shortest_path(c, a, &ShortestPathOptions { direction: Direction::Incoming, ..Default::default() })
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.nodes.len(), 3);
        assert_eq!(p.nodes[0], c);
        assert_eq!(p.nodes[2], a);
        assert_eq!(p.total_cost, 2.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_cycle_safe() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        // Cycle: a -> b -> c -> a, with d only reachable from c
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Should find path despite cycle
        let path = db
            .shortest_path(a, d, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        assert_eq!(p.total_cost, 3.0);
        assert_eq!(p.nodes, vec![a, b, c, d]);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_nonexistent_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        // Node 999999 was never created
        let path = db
            .shortest_path(a, 999999, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_none());

        let path = db
            .shortest_path(999999, a, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_none());

        assert!(!db
            .is_connected(a, 999999, &IsConnectedOptions::default())
            .unwrap());

        db.close().unwrap();
    }

    #[test]
    fn test_path_apis_respect_prune_policy_visibility() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let hidden = db.upsert_node(1, "hidden", UpsertNodeOptions { weight: 0.2, ..Default::default() }).unwrap();
        let visible = db.upsert_node(1, "visible", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(a, hidden, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(hidden, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, visible, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(visible, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(path.nodes, vec![a, visible, c]);

        assert!(db
            .is_connected(a, c, &IsConnectedOptions::default())
            .unwrap());

        let paths = db
            .all_shortest_paths(a, c, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes, vec![a, visible, c]);

        assert!(db
            .shortest_path(hidden, hidden, &ShortestPathOptions::default())
            .unwrap()
            .is_none());
        assert!(!db
            .is_connected(hidden, hidden, &IsConnectedOptions::default())
            .unwrap());
        assert!(db
            .all_shortest_paths(hidden, hidden, &AllShortestPathsOptions::default())
            .unwrap()
            .is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_weighted_path_apis_respect_prune_policy_visibility() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let hidden = db.upsert_node(1, "hidden", UpsertNodeOptions { weight: 0.2, ..Default::default() }).unwrap();
        let visible = db.upsert_node(1, "visible", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        let mut cheap = BTreeMap::new();
        cheap.insert("cost".to_string(), PropValue::Float(1.0));
        let mut expensive = BTreeMap::new();
        expensive.insert("cost".to_string(), PropValue::Float(10.0));

        db.upsert_edge(a, hidden, 10, UpsertEdgeOptions { props: cheap.clone(), ..Default::default() })
            .unwrap();
        db.upsert_edge(hidden, c, 10, UpsertEdgeOptions { props: cheap, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, visible, 10, UpsertEdgeOptions { props: expensive.clone(), weight: 10.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(visible, c, 10, UpsertEdgeOptions { props: expensive, weight: 10.0, ..Default::default() })
            .unwrap();

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.nodes, vec![a, visible, c]);
        assert_eq!(path.total_cost, 20.0);

        let path = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("cost".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.nodes, vec![a, visible, c]);
        assert_eq!(path.total_cost, 20.0);

        let paths = db
            .all_shortest_paths(a, c, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes, vec![a, visible, c]);
        assert_eq!(paths[0].total_cost, 20.0);

        db.close().unwrap();
    }

    // --- Phase 18b CP2: Dijkstra + all_shortest_paths ---

    #[test]
    fn test_dijkstra_weighted_shortest_path() {
        // A->B (weight 10), A->C (weight 2), C->B (weight 3)
        // BFS shortest: A->B (1 hop). Dijkstra shortest: A->C->B (cost 5)
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 10.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        let _ec = db
            .upsert_edge(c, b, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        let path = db
            .shortest_path(a, b, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);
        assert_eq!(path.nodes.len(), 3);
        assert_eq!(*path.nodes.first().unwrap(), a);
        assert_eq!(*path.nodes.last().unwrap(), b);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_custom_weight_field() {
        // Use a custom property "cost" on edges
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        let mut props_ab = BTreeMap::new();
        props_ab.insert("cost".to_string(), PropValue::Float(100.0));
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { props: props_ab, weight: 1.0, ..Default::default() }).unwrap();

        let mut props_ac = BTreeMap::new();
        props_ac.insert("cost".to_string(), PropValue::Float(1.0));
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { props: props_ac, weight: 1.0, ..Default::default() }).unwrap();

        let mut props_cb = BTreeMap::new();
        props_cb.insert("cost".to_string(), PropValue::Float(2.0));
        db.upsert_edge(c, b, 10, UpsertEdgeOptions { props: props_cb, weight: 1.0, ..Default::default() }).unwrap();

        let path = db
            .shortest_path(a, b, &ShortestPathOptions { weight_field: Some("cost".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 3.0); // A->C(1) + C->B(2)
        assert_eq!(path.nodes, vec![a, c, b]);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_max_cost_ceiling() {
        // A->B (weight 5), A->C (weight 2), C->B (weight 4)
        // Only path A->B with cost 5 should work, A->C->B costs 6
        // max_cost=4 should exclude both, returning None
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(c, b, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() })
            .unwrap();

        // max_cost=4: nothing reachable
        let path = db
            .shortest_path(a, b, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_cost: Some(4.0), ..Default::default() })
            .unwrap();
        assert!(path.is_none());

        // max_cost=5: direct A->B works
        let path = db
            .shortest_path(a, b, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_cost: Some(5.0), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_negative_weight_error() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: -1.0, ..Default::default() })
            .unwrap();

        let result = db.shortest_path(a, b, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() });
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("negative"));

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_nan_weight_error() {
        // NaN specifically bypasses `w < 0.0` (NaN < 0.0 is false).
        // The !w.is_finite() guard must catch it.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("w".to_string(), PropValue::Float(f64::NAN));
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { props, weight: 1.0, ..Default::default() }).unwrap();

        let result = db.shortest_path(a, b, &ShortestPathOptions { weight_field: Some("w".to_string()), ..Default::default() });
        assert!(result.is_err());

        // Also test Infinity
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let mut props2 = BTreeMap::new();
        props2.insert("w".to_string(), PropValue::Float(f64::INFINITY));
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { props: props2, weight: 1.0, ..Default::default() }).unwrap();

        let result = db.shortest_path(a, c, &ShortestPathOptions { weight_field: Some("w".to_string()), ..Default::default() });
        assert!(result.is_err());

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_max_depth_bidir_total() {
        // Regression test for M2: bidirectional Dijkstra must enforce
        // max_depth on total path hops, not per-side.
        // Chain: A->B->C->D (3 hops, all weight 1).
        // With max_depth=2, each side individually can reach 2 hops,
        // but the total path is 3 hops. Must be rejected.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        // max_depth=2: 3-hop path should be rejected
        let path = db
            .shortest_path(a, d, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(2), ..Default::default() })
            .unwrap();
        assert!(path.is_none());

        // max_depth=3: should succeed
        let path = db
            .shortest_path(a, d, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(3), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 3.0);
        assert_eq!(path.edges.len(), 3);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_zero_weight_edges() {
        // A->B (weight 0), B->C (weight 0)
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 0.0);
        assert_eq!(path.nodes, vec![a, b, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_same_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        let path = db
            .shortest_path(a, a, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.nodes, vec![a]);
        assert_eq!(path.edges.len(), 0);
        assert_eq!(path.total_cost, 0.0);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_bidir_termination_non_meeting_path() {
        // Graph where the shortest path goes through a node that is NOT
        // the first meeting point of the bidirectional search.
        //
        //   A --1--> B --1--> D
        //   A --1--> C --1--> D
        //   A --10-> D           (direct but expensive)
        //
        // Both B and C are on shortest paths of cost 2. The direct edge
        // costs 10. Bidirectional should find cost 2 regardless of which
        // side meets first.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, d, 10, UpsertEdgeOptions { weight: 10.0, ..Default::default() })
            .unwrap();

        let path = db
            .shortest_path(a, d, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 2.0);
        assert_eq!(path.nodes.len(), 3);
        assert_eq!(*path.nodes.first().unwrap(), a);
        assert_eq!(*path.nodes.last().unwrap(), d);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_no_path() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        // No edges

        let path = db
            .shortest_path(a, b, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert!(path.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_after_flush() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);
        assert_eq!(path.nodes, vec![a, b, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_after_compact() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);

        db.close().unwrap();
    }

    #[test]
    fn test_bfs_vs_dijkstra_different_paths() {
        // BFS finds shortest hop path, Dijkstra finds lowest cost path
        // A->B->C (2 hops, cost 100+100=200)
        // A->D->E->C (3 hops, cost 1+1+1=3)
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e = db.upsert_node(1, "e", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 100.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 100.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(d, e, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(e, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        // BFS: 2 hops (A->B->C)
        let bfs = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(bfs.total_cost, 2.0);

        // Dijkstra: cost 3 (A->D->E->C)
        let dij = db
            .shortest_path(a, c, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(dij.total_cost, 3.0);
        assert_eq!(dij.nodes.len(), 4); // A, D, E, C

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_max_depth() {
        // A->B->C->D, all weight 1
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        // max_depth=1: can only reach B from A side
        let path = db
            .shortest_path(a, d, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(1), ..Default::default() })
            .unwrap();
        assert!(path.is_none());

        // max_depth=3: should find path
        let path = db
            .shortest_path(a, d, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(3), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 3.0);

        db.close().unwrap();
    }

    // --- all_shortest_paths tests ---

    #[test]
    fn test_all_shortest_paths_bfs_diamond() {
        // Diamond: A->B->D and A->C->D (2 equal-cost BFS paths)
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let (a, _b, _c, d) = build_diamond(&mut db);

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(paths.len(), 2);
        for p in &paths {
            assert_eq!(p.total_cost, 2.0);
            assert_eq!(p.nodes.len(), 3);
            assert_eq!(*p.nodes.first().unwrap(), a);
            assert_eq!(*p.nodes.last().unwrap(), d);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_bfs_max_paths() {
        // Diamond has 2 paths, cap at 1
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let (a, _, _, d) = build_diamond(&mut db);

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions { max_paths: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].total_cost, 2.0);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_bfs_same_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        let paths = db
            .all_shortest_paths(a, a, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes, vec![a]);
        assert_eq!(paths[0].total_cost, 0.0);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_bfs_no_path() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let paths = db
            .all_shortest_paths(a, b, &AllShortestPathsOptions::default())
            .unwrap();
        assert!(paths.is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_dijkstra_diamond() {
        // Diamond with equal weights: A->B(w=3)->D and A->C(w=3)->D, both cost 6
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 2);
        for p in &paths {
            assert_eq!(p.total_cost, 6.0);
            assert_eq!(*p.nodes.first().unwrap(), a);
            assert_eq!(*p.nodes.last().unwrap(), d);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_dijkstra_max_paths() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), max_paths: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 1);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_dijkstra_no_path() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let paths = db
            .all_shortest_paths(a, b, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert!(paths.is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_negative_weight_error() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: -5.0, ..Default::default() })
            .unwrap();

        let result = db.all_shortest_paths(a, b, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() });
        assert!(result.is_err());

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_bfs_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let (a, _, _, d) = build_diamond(&mut db);
        db.flush().unwrap();

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(paths.len(), 2);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_bfs_after_compact() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(paths.len(), 2);
        for p in &paths {
            assert_eq!(p.total_cost, 2.0);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_custom_int_weight() {
        // Use PropValue::Int for the weight field
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("distance".to_string(), PropValue::Int(7));
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { props, weight: 1.0, ..Default::default() }).unwrap();

        let path = db
            .shortest_path(a, b, &ShortestPathOptions { weight_field: Some("distance".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 7.0);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_max_paths_zero_no_limit() {
        // max_paths=0 means no limit
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let (a, _, _, d) = build_diamond(&mut db);

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions { max_paths: Some(0), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 2); // both diamond paths found

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_equal_cost_different_hops_max_depth() {
        // S --4--> A --1--> T   (2 hops, cost 5)
        // S --1--> B --1--> C --3--> T   (3 hops, cost 5)
        // Without max_depth: any cost-5 path is valid.
        // With max_depth=2: must find the 2-hop path S→A→T, not return None.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let s = db.upsert_node(1, "s", UpsertNodeOptions::default()).unwrap();
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let t = db.upsert_node(1, "t", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(s, a, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, t, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(s, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, t, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        // Without max_depth
        let path = db
            .shortest_path(s, t, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);

        // With max_depth=2: must find the 2-hop path
        let path = db
            .shortest_path(s, t, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(2), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);
        assert_eq!(path.edges.len(), 2);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_equal_cost_shorter_hops_arrives_after_settle() {
        // S -> A -> B -> V -> T  (cost 1, 4 hops)
        // S -> X -> V -> T       (cost 1, 3 hops)
        //
        // `V` is discovered first through the longer zero-weight prefix and can be
        // settled before `X` depending on heap tie ordering. The bounded search must
        // still recover the shorter-hop equal-cost route.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let s = db.upsert_node(1, "s", UpsertNodeOptions::default()).unwrap();
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let v = db.upsert_node(1, "v", UpsertNodeOptions::default()).unwrap();
        let x = db.upsert_node(1, "x", UpsertNodeOptions::default()).unwrap();
        let t = db.upsert_node(1, "t", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(s, a, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, v, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(s, x, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(x, v, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(v, t, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();

        let path = db
            .shortest_path(s, t, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(3), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 1.0);
        assert_eq!(path.nodes, vec![s, x, v, t]);

        db.close().unwrap();
    }

    #[test]
    fn test_dijkstra_max_depth_uses_best_constrained_cost() {
        // Cheapest path is too deep; bounded search must return the more expensive
        // valid path instead of filtering the global optimum down to nothing.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let s = db.upsert_node(1, "s", UpsertNodeOptions::default()).unwrap();
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let t = db.upsert_node(1, "t", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(s, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, t, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(s, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(c, t, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        let path = db
            .shortest_path(s, t, &ShortestPathOptions { weight_field: Some("weight".to_string()), max_depth: Some(2), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 6.0);
        assert_eq!(path.nodes, vec![s, c, t]);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_zero_weight_cycle() {
        // A → B (weight 0), B → A (weight 0). Zero-weight cycle.
        // A → T (weight 1)
        // Must not stack-overflow; should return path(s) with cost 1
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let t = db.upsert_node(1, "t", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, a, 10, UpsertEdgeOptions { weight: 0.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, t, 10, UpsertEdgeOptions::default())
            .unwrap();

        let paths = db
            .all_shortest_paths(a, t, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert!(!paths.is_empty());
        for p in &paths {
            assert_eq!(p.total_cost, 1.0);
            assert_eq!(*p.nodes.first().unwrap(), a);
            assert_eq!(*p.nodes.last().unwrap(), t);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_dijkstra_max_depth_filtering() {
        // S --4--> A --1--> T   (2 hops, cost 5)
        // S --1--> B --1--> C --3--> T   (3 hops, cost 5)
        // max_depth=2: only the 2-hop path
        // max_depth=3: both paths
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let s = db.upsert_node(1, "s", UpsertNodeOptions::default()).unwrap();
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let t = db.upsert_node(1, "t", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(s, a, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, t, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(s, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, t, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        // max_depth=2: only the 2-hop path
        let paths = db
            .all_shortest_paths(s, t, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), max_depth: Some(2), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].total_cost, 5.0);
        assert_eq!(paths[0].edges.len(), 2);

        // max_depth=3: both paths
        let paths = db
            .all_shortest_paths(s, t, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), max_depth: Some(3), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 2);
        for p in &paths {
            assert_eq!(p.total_cost, 5.0);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_dijkstra_max_depth_uses_best_constrained_cost() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let s = db.upsert_node(1, "s", UpsertNodeOptions::default()).unwrap();
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let t = db.upsert_node(1, "t", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(s, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, t, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(s, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(c, t, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        let paths = db
            .all_shortest_paths(s, t, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), max_depth: Some(2), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].total_cost, 6.0);
        assert_eq!(paths[0].nodes, vec![s, c, t]);

        db.close().unwrap();
    }

    #[test]
    fn test_all_shortest_paths_dijkstra_after_flush_weighted() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        let paths = db
            .all_shortest_paths(a, d, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert_eq!(paths.len(), 2);
        for p in &paths {
            assert_eq!(p.total_cost, 6.0);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_path_apis_after_reopen_weighted_and_unweighted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (a, d) = {
            let db = open_imm(&db_path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
                .unwrap();
            db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
                .unwrap();
            db.upsert_edge(b, d, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
                .unwrap();
            db.upsert_edge(c, d, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
                .unwrap();
            db.flush().unwrap();
            db.compact().unwrap();
            db.close().unwrap();
            (a, d)
        };

        let reopened = open_imm(&db_path);
        let bfs_path = reopened
            .shortest_path(a, d, &ShortestPathOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(bfs_path.total_cost, 2.0);
        assert_eq!(bfs_path.edges.len(), 2);

        let weighted_path = reopened
            .shortest_path(a, d, &ShortestPathOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(weighted_path.total_cost, 3.0);

        assert!(reopened
            .is_connected(a, d, &IsConnectedOptions::default())
            .unwrap());

        let bfs_paths = reopened
            .all_shortest_paths(a, d, &AllShortestPathsOptions::default())
            .unwrap();
        assert_eq!(bfs_paths.len(), 2);

        let weighted_paths = reopened
            .all_shortest_paths(a, d, &AllShortestPathsOptions { weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap();
        assert_eq!(weighted_paths.len(), 2);
        assert!(weighted_paths.iter().all(|path| path.total_cost == 3.0));

        reopened.close().unwrap();
    }

    #[test]
    fn test_dijkstra_direction_both() {
        // A->B->C, query with Direction::Both from C to A should find path
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        let path = db
            .shortest_path(c, a, &ShortestPathOptions { direction: Direction::Both, weight_field: Some("weight".to_string()), ..Default::default() })
            .unwrap()
            .unwrap();
        assert_eq!(path.total_cost, 5.0);

        db.close().unwrap();
    }

    #[test]
    fn test_shortest_path_memtable_plus_segments() {
        // Path where some edges are in segments and the bridge edge is in memtable
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Bridge edge in memtable only
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let path = db
            .shortest_path(a, c, &ShortestPathOptions::default())
            .unwrap();
        assert!(path.is_some());
        assert_eq!(path.unwrap().total_cost, 2.0);

        // Also test is_connected
        assert!(db
            .is_connected(a, c, &IsConnectedOptions::default())
            .unwrap());

        db.close().unwrap();
    }

    // --- neighbors_paged tests ---

    #[test]
    fn test_neighbors_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        let mut edge_ids = Vec::new();
        for i in 0..8 {
            let neighbor = engine
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
                .unwrap();
            let eid = engine
                .upsert_edge(center, neighbor, 10, UpsertEdgeOptions::default())
                .unwrap();
            edge_ids.push(eid);
        }
        edge_ids.sort();

        // Page through 3 at a time
        let p1 = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(3),
                    after: None,
                })
            .unwrap();
        assert_eq!(p1.items.len(), 3);
        let p1_eids: Vec<u64> = p1.items.iter().map(|e| e.edge_id).collect();
        assert_eq!(p1_eids, edge_ids[0..3]);
        assert!(p1.next_cursor.is_some());

        let p2 = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(3),
                    after: p1.next_cursor,
                })
            .unwrap();
        assert_eq!(p2.items.len(), 3);
        let p2_eids: Vec<u64> = p2.items.iter().map(|e| e.edge_id).collect();
        assert_eq!(p2_eids, edge_ids[3..6]);
        assert!(p2.next_cursor.is_some());

        let p3 = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(3),
                    after: p2.next_cursor,
                })
            .unwrap();
        assert_eq!(p3.items.len(), 2);
        let p3_eids: Vec<u64> = p3.items.iter().map(|e| e.edge_id).collect();
        assert_eq!(p3_eids, edge_ids[6..8]);
        assert!(p3.next_cursor.is_none());
    }

    #[test]
    fn test_neighbors_paged_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();

        // Create 4 edges, flush to segment
        for i in 0..4 {
            let n = engine
                .upsert_node(1, &format!("seg{}", i), UpsertNodeOptions::default())
                .unwrap();
            engine
                .upsert_edge(center, n, 10, UpsertEdgeOptions::default())
                .unwrap();
        }
        engine.flush().unwrap();

        // Create 4 more in memtable
        for i in 0..4 {
            let n = engine
                .upsert_node(1, &format!("mem{}", i), UpsertNodeOptions::default())
                .unwrap();
            engine
                .upsert_edge(center, n, 10, UpsertEdgeOptions::default())
                .unwrap();
        }

        // Round-trip
        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                        limit: Some(3),
                        after: cursor,
                    })
                .unwrap();
            all_paged.extend(page.items.iter().map(|e| e.edge_id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(all_paged.len(), 8);
        for i in 1..all_paged.len() {
            assert!(all_paged[i] > all_paged[i - 1]);
        }
    }

    #[test]
    fn test_neighbors_paged_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        let n1 = engine.upsert_node(1, "n1", UpsertNodeOptions::default()).unwrap();
        let n2 = engine.upsert_node(1, "n2", UpsertNodeOptions::default()).unwrap();
        let n3 = engine.upsert_node(1, "n3", UpsertNodeOptions::default()).unwrap();

        engine
            .upsert_edge(center, n1, 10, UpsertEdgeOptions::default())
            .unwrap();
        let e2 = engine
            .upsert_edge(center, n2, 10, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(center, n3, 10, UpsertEdgeOptions::default())
            .unwrap();

        engine.delete_edge(e2).unwrap();

        let result = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(10),
                    after: None,
                })
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|e| e.edge_id != e2));
    }

    #[test]
    fn test_neighbors_paged_with_temporal_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        let n1 = engine.upsert_node(1, "n1", UpsertNodeOptions::default()).unwrap();
        let n2 = engine.upsert_node(1, "n2", UpsertNodeOptions::default()).unwrap();

        // Edge valid from 100 to 200
        engine
            .upsert_edge(center, n1, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(100), valid_to: Some(200), ..Default::default() })
            .unwrap();
        // Edge valid from 150 to 300
        engine
            .upsert_edge(center, n2, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(150), valid_to: Some(300), ..Default::default() })
            .unwrap();

        // At epoch 175, both valid
        let result = engine
            .neighbors_paged(center, &NeighborOptions { at_epoch: Some(175), ..Default::default() }, &PageRequest {
                    limit: Some(10),
                    after: None,
                })
            .unwrap();
        assert_eq!(result.items.len(), 2);

        // At epoch 250, only n2 valid
        let result = engine
            .neighbors_paged(center, &NeighborOptions { at_epoch: Some(250), ..Default::default() }, &PageRequest {
                    limit: Some(10),
                    after: None,
                })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].node_id, n2);
    }

    #[test]
    fn test_neighbors_paged_roundtrip_matches_neighbors() {
        // Paginated results should match unpaginated neighbors()
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        for i in 0..15 {
            let n = engine
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
                .unwrap();
            engine
                .upsert_edge(center, n, 10, UpsertEdgeOptions::default())
                .unwrap();
        }

        // Collect all via pagination
        let mut paged_eids: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                        limit: Some(4),
                        after: cursor,
                    })
                .unwrap();
            paged_eids.extend(page.items.iter().map(|e| e.edge_id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        // Collect all via neighbors()
        let unpaged = engine
            .neighbors(center, &NeighborOptions::default())
            .unwrap();
        let mut unpaged_eids: Vec<u64> = unpaged.iter().map(|e| e.edge_id).collect();
        unpaged_eids.sort();

        assert_eq!(paged_eids, unpaged_eids);
    }

    #[test]
    fn test_neighbors_paged_temporal_default_matches_neighbors() {
        // at_epoch=None should filter against now, matching neighbors() behavior.
        // Edges with future valid_from or past valid_to must be excluded.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        let n_past = engine.upsert_node(1, "past", UpsertNodeOptions::default()).unwrap();
        let n_current = engine
            .upsert_node(1, "current", UpsertNodeOptions::default())
            .unwrap();
        let n_future = engine
            .upsert_node(1, "future", UpsertNodeOptions::default())
            .unwrap();

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Expired edge (valid_to in the past)
        engine
            .upsert_edge(
                center, n_past, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(now - 2000), valid_to: Some(now - 1000), ..Default::default() },
            )
            .unwrap();
        // Currently valid edge
        engine
            .upsert_edge(
                center, n_current, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(now - 1000), valid_to: Some(now + 100_000), ..Default::default() },
            )
            .unwrap();
        // Future edge (valid_from in the future)
        engine
            .upsert_edge(
                center, n_future, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(now + 50_000), valid_to: Some(now + 100_000), ..Default::default() },
            )
            .unwrap();

        // Non-paginated: should return only n_current
        let unpaged = engine
            .neighbors(center, &NeighborOptions::default())
            .unwrap();
        assert_eq!(unpaged.len(), 1);
        assert_eq!(unpaged[0].node_id, n_current);

        // Paginated with at_epoch=None: must match
        let paged = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(10),
                    after: None,
                })
            .unwrap();
        assert_eq!(
            paged.items.len(),
            1,
            "paged should filter future/expired edges when at_epoch=None"
        );
        assert_eq!(paged.items[0].node_id, n_current);
    }

    #[test]
    fn test_neighbors_paged_temporal_cursor_correctness() {
        // Verify cursor works correctly when temporal filtering removes items.
        // Create a mix of valid and invalid edges, paginate with small limit.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        let epoch = 500_000i64;
        let mut valid_node_ids = Vec::new();

        // Create 10 edges, alternating valid/invalid at epoch=500000
        for i in 0..10u64 {
            let n = engine
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
                .unwrap();
            if i % 2 == 0 {
                // Valid: valid_from=100000, valid_to=900000
                engine
                    .upsert_edge(
                        center, n, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(100_000), valid_to: Some(900_000), ..Default::default() },
                    )
                    .unwrap();
                valid_node_ids.push(n);
            } else {
                // Invalid at epoch: valid_from=600000, valid_to=900000
                engine
                    .upsert_edge(
                        center, n, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(600_000), valid_to: Some(900_000), ..Default::default() },
                    )
                    .unwrap();
            }
        }

        // Collect all via pagination (page size 2) at fixed epoch
        let mut paged_nodes: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        let mut page_count = 0;
        loop {
            let page = engine
                .neighbors_paged(center, &NeighborOptions { at_epoch: Some(epoch), ..Default::default() }, &PageRequest {
                        limit: Some(2),
                        after: cursor,
                    })
                .unwrap();
            paged_nodes.extend(page.items.iter().map(|e| e.node_id));
            cursor = page.next_cursor;
            page_count += 1;
            if cursor.is_none() {
                break;
            }
        }

        // Should have exactly the 5 valid nodes
        valid_node_ids.sort();
        paged_nodes.sort();
        assert_eq!(paged_nodes, valid_node_ids);
        // Should have taken 3 pages (2+2+1)
        assert_eq!(page_count, 3);
    }

    #[test]
    fn test_neighbors_paged_policy_refills_past_sparse_filtered_window() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine
            .upsert_node(1, "center", UpsertNodeOptions::default())
            .unwrap();
        let mut visible_neighbors = Vec::new();

        for i in 0..17u64 {
            let weight = if i < 12 { 0.1 } else { 1.0 };
            let node_id = engine
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions { weight, ..Default::default() })
                .unwrap();
            engine
                .upsert_edge(center, node_id, 10, UpsertEdgeOptions::default())
                .unwrap();
            if weight > 0.5 {
                visible_neighbors.push(node_id);
            }
        }

        engine
            .set_prune_policy(
                "low_weight",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let page1 = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(3),
                    after: None,
                })
            .unwrap();
        assert_eq!(
            page1.items.iter().map(|e| e.node_id).collect::<Vec<_>>(),
            visible_neighbors[..3].to_vec()
        );
        assert!(page1.next_cursor.is_some());

        let page2 = engine
            .neighbors_paged(center, &NeighborOptions::default(), &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                })
            .unwrap();
        assert_eq!(
            page2.items.iter().map(|e| e.node_id).collect::<Vec<_>>(),
            visible_neighbors[3..].to_vec()
        );
        assert!(page2.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_policy_cursor_correctness() {
        // Verify cursor is pushed down in policy-filtered nodes_by_type_paged.
        // Page 2 should not re-return page 1 items.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 10 nodes of type 1
        let mut all_ids = Vec::new();
        for i in 0..10 {
            let id = engine
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
                .unwrap();
            all_ids.push(id);
        }

        // Register a policy that excludes nothing (max_age very large,
        // non-matching type). Just to force the policy code path
        engine
            .set_prune_policy(
                "dummy",
                PrunePolicy {
                    max_age_ms: Some(999_999_999),
                    max_weight: None,
                    type_id: Some(999), // non-matching type, excludes nothing
                },
            )
            .unwrap();

        // Paginate with limit=3
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .nodes_by_type_paged(
                    1,
                    &PageRequest {
                        limit: Some(3),
                        after: cursor,
                    },
                )
                .unwrap();
            collected.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        all_ids.sort();
        collected.sort();
        assert_eq!(collected, all_ids);
        // No duplicates
        let deduped: NodeIdSet = collected.iter().copied().collect();
        assert_eq!(deduped.len(), all_ids.len());
    }

    #[test]
    fn test_find_nodes_paged_cursor_correctness() {
        // Verify cursor is pushed down in find_nodes_paged.
        // Create many matching nodes, paginate, verify no duplicates and parity.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut matching_ids = Vec::new();
        for i in 0..12 {
            let mut props = BTreeMap::new();
            props.insert(
                "color".to_string(),
                PropValue::String(if i % 3 == 0 {
                    "red".to_string()
                } else {
                    "blue".to_string()
                }),
            );
            let id = engine
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions { props, ..Default::default() })
                .unwrap();
            if i % 3 == 0 {
                matching_ids.push(id);
            }
        }

        // Paginate find_nodes with limit=2
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .find_nodes_paged(
                    1,
                    "color",
                    &PropValue::String("red".to_string()),
                    &PageRequest {
                        limit: Some(2),
                        after: cursor,
                    },
                )
                .unwrap();
            collected.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        matching_ids.sort();
        collected.sort();
        assert_eq!(collected, matching_ids);
        // No duplicates
        let deduped: NodeIdSet = collected.iter().copied().collect();
        assert_eq!(deduped.len(), matching_ids.len());
    }

    #[test]
    fn test_find_nodes_paged_cross_source_cursor() {
        // Verify cursor works correctly across memtable + segments.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create some matching nodes, flush, create more
        let mut all_matching = Vec::new();
        for i in 0..5 {
            let mut props = BTreeMap::new();
            props.insert("tag".to_string(), PropValue::String("yes".to_string()));
            let id = engine
                .upsert_node(1, &format!("pre{}", i), UpsertNodeOptions { props, ..Default::default() })
                .unwrap();
            all_matching.push(id);
        }
        engine.flush().unwrap();
        for i in 0..5 {
            let mut props = BTreeMap::new();
            props.insert("tag".to_string(), PropValue::String("yes".to_string()));
            let id = engine
                .upsert_node(1, &format!("post{}", i), UpsertNodeOptions { props, ..Default::default() })
                .unwrap();
            all_matching.push(id);
        }

        // Paginate with limit=3
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .find_nodes_paged(
                    1,
                    "tag",
                    &PropValue::String("yes".to_string()),
                    &PageRequest {
                        limit: Some(3),
                        after: cursor,
                    },
                )
                .unwrap();
            collected.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        all_matching.sort();
        collected.sort();
        assert_eq!(collected, all_matching);
    }

    #[test]
    fn test_find_nodes_stale_match_after_update_across_flush() {
        // Regression: find_nodes must not return stale segment matches when
        // a newer source (memtable or newer segment) has changed the property.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let n1 = engine.upsert_node(1, "n1", UpsertNodeOptions { props, ..Default::default() }).unwrap();

        // Flush: n1 with color=red goes to segment
        engine.flush().unwrap();

        // Update n1 to color=blue in memtable
        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("blue".to_string()));
        engine.upsert_node(1, "n1", UpsertNodeOptions { props: props2, ..Default::default() }).unwrap();

        // find_nodes for color=red must NOT return n1 (stale segment match)
        let red = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        assert!(
            !red.contains(&n1),
            "find_nodes returned stale segment match after update"
        );

        // find_nodes for color=blue SHOULD return n1
        let blue = engine
            .find_nodes(1, "color", &PropValue::String("blue".to_string()))
            .unwrap();
        assert!(blue.contains(&n1));

        // Parity: find_nodes_paged must agree
        let red_paged = engine
            .find_nodes_paged(
                1,
                "color",
                &PropValue::String("red".to_string()),
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        assert!(red_paged.items.is_empty());

        let blue_paged = engine
            .find_nodes_paged(
                1,
                "color",
                &PropValue::String("blue".to_string()),
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        assert!(blue_paged.items.contains(&n1));
    }

    #[test]
    fn test_find_nodes_stale_match_across_segments() {
        // Same bug across segments: older segment has color=red, newer has color=blue.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let n1 = engine.upsert_node(1, "n1", UpsertNodeOptions { props, ..Default::default() }).unwrap();

        // Flush: S1 has n1 with color=red
        engine.flush().unwrap();

        // Update and flush again: S2 has n1 with color=blue
        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("blue".to_string()));
        engine.upsert_node(1, "n1", UpsertNodeOptions { props: props2, ..Default::default() }).unwrap();
        engine.flush().unwrap();

        // find_nodes for color=red must NOT return n1
        let red = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        assert!(
            !red.contains(&n1),
            "find_nodes returned stale match from older segment"
        );

        let blue = engine
            .find_nodes(1, "color", &PropValue::String("blue".to_string()))
            .unwrap();
        assert!(blue.contains(&n1));
    }

    // --- Phase 18c CP1: variable-depth traversal ---

    #[test]
    fn test_traverse_basic_depth_and_order() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let depth1_low = db.upsert_node(1, "depth1-low", UpsertNodeOptions::default()).unwrap();
        let depth1_high = db
            .upsert_node(1, "depth1-high", UpsertNodeOptions::default())
            .unwrap();
        let depth2_low = db.upsert_node(1, "depth2-low", UpsertNodeOptions::default()).unwrap();
        let depth2_mid = db.upsert_node(1, "depth2-mid", UpsertNodeOptions::default()).unwrap();
        let depth2_high = db.upsert_node(1, "depth2-high", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, depth1_high, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, depth1_low, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(depth1_high, depth2_high, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(depth1_high, depth2_mid, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(depth1_low, depth2_low, 10, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .traverse(start, 2, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();

        let ordered: Vec<(u64, u32)> = result.items.iter().map(|hit| (hit.node_id, hit.depth)).collect();
        assert_eq!(
            ordered,
            vec![
                (start, 0),
                (depth1_low, 1),
                (depth1_high, 1),
                (depth2_low, 2),
                (depth2_mid, 2),
                (depth2_high, 2),
            ]
        );
        assert_eq!(result.items[0].via_edge_id, None);
        assert!(result.items.iter().skip(1).all(|hit| hit.via_edge_id.is_some()));
        assert!(result.items.iter().all(|hit| hit.score.is_none()));
        assert!(result.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_depth_window_and_limit() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        let depth2_only = db
            .traverse(a, 2, &TraverseOptions { min_depth: 2, ..Default::default() })
            .unwrap();
        let depth2_pairs: Vec<(u64, u32)> = depth2_only
            .items
            .iter()
            .map(|hit| (hit.node_id, hit.depth))
            .collect();
        assert_eq!(depth2_pairs, vec![(c, 2)]);

        let limited = db
            .traverse(a, 3, &TraverseOptions { decay_lambda: Some(0.5), limit: Some(2), ..Default::default() })
            .unwrap();
        let limited_pairs: Vec<(u64, u32)> = limited
            .items
            .iter()
            .map(|hit| (hit.node_id, hit.depth))
            .collect();
        assert_eq!(limited_pairs, vec![(b, 1), (c, 2)]);
        assert_eq!(
            limited.next_cursor,
            Some(TraversalCursor {
                depth: 2,
                last_node_id: c,
            })
        );
        assert_eq!(limited.items[0].score, Some((-0.5f64).exp()));
        assert_eq!(limited.items[1].score, Some((-1.0f64).exp()));

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_last_page_cursor_is_none_for_isolated_start() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let page = db
            .traverse(start, 3, &TraverseOptions { min_depth: 0, limit: Some(1), ..Default::default() })
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].node_id, start);
        assert!(page.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_last_page_cursor_is_none_when_deeper_work_is_filtered_out() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let middle = db.upsert_node(2, "middle", UpsertNodeOptions::default()).unwrap();
        let hidden = db.upsert_node(2, "hidden", UpsertNodeOptions { weight: 0.2, ..Default::default() }).unwrap();

        db.upsert_edge(start, middle, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(middle, hidden, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let page = db
            .traverse(start, 2, &TraverseOptions { node_type_filter: Some(vec![2]), limit: Some(1), ..Default::default() })
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].node_id, middle);
        assert!(page.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_cycle_safe_and_unique() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .traverse(a, 3, &TraverseOptions::default())
            .unwrap();

        let pairs: Vec<(u64, u32)> = result.items.iter().map(|hit| (hit.node_id, hit.depth)).collect();
        assert_eq!(pairs, vec![(b, 1), (c, 2), (d, 2)]);
        let unique: NodeIdSet = result.items.iter().map(|hit| hit.node_id).collect();
        assert_eq!(unique.len(), result.items.len());
        assert!(!unique.contains(&a));

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_cursor_resume_across_pages_without_dup_or_skip() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        let p1 = db
            .traverse(start, 2, &TraverseOptions { limit: Some(2), ..Default::default() })
            .unwrap();
        let p2 = db
            .traverse(start, 2, &TraverseOptions { limit: Some(2), cursor: p1.next_cursor.clone(), ..Default::default() })
            .unwrap();

        let combined: Vec<(u64, u32)> = p1
            .items
            .iter()
            .chain(p2.items.iter())
            .map(|hit| (hit.node_id, hit.depth))
            .collect();
        assert_eq!(combined, vec![(b, 1), (c, 1), (d, 2)]);
        assert!(p2.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_cursor_resume_within_same_depth_layer() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, d, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let p1 = db
            .traverse(start, 1, &TraverseOptions { limit: Some(2), ..Default::default() })
            .unwrap();
        assert_eq!(
            p1.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(b, 1), (c, 1)]
        );
        assert_eq!(
            p1.next_cursor,
            Some(TraversalCursor {
                depth: 1,
                last_node_id: c,
            })
        );

        let p2 = db
            .traverse(start, 1, &TraverseOptions { limit: Some(2), cursor: p1.next_cursor.clone(), ..Default::default() })
            .unwrap();
        assert_eq!(
            p2.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(d, 1)]
        );
        assert!(p2.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_exact_page_size_cutoff_is_last_page() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let page = db
            .traverse(start, 1, &TraverseOptions { limit: Some(2), ..Default::default() })
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(b, 1), (c, 1)]
        );
        assert!(page.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_cursor_past_end_returns_empty_page() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        let page = db
            .traverse(start, 1, &TraverseOptions { min_depth: 0, limit: Some(10), cursor: Some(TraversalCursor {
                    depth: 99,
                    last_node_id: u64::MAX,
                }), ..Default::default() })
            .unwrap();
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_edge_filter_and_node_type_filter_is_emission_only() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let middle = db.upsert_node(2, "middle", UpsertNodeOptions::default()).unwrap();
        let target = db.upsert_node(3, "target", UpsertNodeOptions::default()).unwrap();
        let wrong_edge = db.upsert_node(3, "wrong-edge", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, middle, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(middle, target, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, wrong_edge, 20, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .traverse(start, 2, &TraverseOptions { edge_type_filter: Some(vec![10]), node_type_filter: Some(vec![3]), ..Default::default() })
            .unwrap();

        let pairs: Vec<(u64, u32)> = result.items.iter().map(|hit| (hit.node_id, hit.depth)).collect();
        assert_eq!(pairs, vec![(target, 2)]);

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_incoming_and_both_directions() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        let incoming = db
            .traverse(b, 1, &TraverseOptions { direction: Direction::Incoming, ..Default::default() })
            .unwrap();
        assert_eq!(
            incoming
                .items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(a, 1), (c, 1)]
        );

        let both = db
            .traverse(b, 1, &TraverseOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();
        assert_eq!(
            both.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(a, 1), (c, 1), (d, 1)]
        );

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_pagination_with_node_type_filter_uses_filtered_path() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let mid_a = db.upsert_node(2, "mid-a", UpsertNodeOptions::default()).unwrap();
        let mid_b = db.upsert_node(2, "mid-b", UpsertNodeOptions::default()).unwrap();
        let hit_a = db.upsert_node(3, "hit-a", UpsertNodeOptions::default()).unwrap();
        let hit_b = db.upsert_node(3, "hit-b", UpsertNodeOptions::default()).unwrap();
        let skip = db.upsert_node(4, "skip", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, mid_a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, mid_b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(mid_a, hit_a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(mid_a, skip, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(mid_b, hit_b, 10, UpsertEdgeOptions::default())
            .unwrap();

        let p1 = db
            .traverse(start, 2, &TraverseOptions { node_type_filter: Some(vec![3]), limit: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(
            p1.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(hit_a, 2)]
        );
        assert_eq!(
            p1.next_cursor,
            Some(TraversalCursor {
                depth: 2,
                last_node_id: hit_a,
            })
        );

        let p2 = db
            .traverse(start, 2, &TraverseOptions { node_type_filter: Some(vec![3]), limit: Some(1), cursor: p1.next_cursor.clone(), ..Default::default() })
            .unwrap();
        assert_eq!(
            p2.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(hit_b, 2)]
        );
        assert!(p2.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_respects_deleted_temporal_and_prune_visibility() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let hidden = db.upsert_node(1, "hidden", UpsertNodeOptions { weight: 0.2, ..Default::default() }).unwrap();
        let hidden_target = db
            .upsert_node(1, "hidden-target", UpsertNodeOptions::default())
            .unwrap();
        let deleted = db.upsert_node(1, "deleted", UpsertNodeOptions::default()).unwrap();
        let future = db.upsert_node(1, "future", UpsertNodeOptions::default()).unwrap();
        let visible = db.upsert_node(1, "visible", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, hidden, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(i64::MAX), ..Default::default() })
            .unwrap();
        db.upsert_edge(
            hidden, hidden_target, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(i64::MAX), ..Default::default() },
        )
        .unwrap();
        db.upsert_edge(start, deleted, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(i64::MAX), ..Default::default() })
            .unwrap();
        db.upsert_edge(start, future, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(5_000), valid_to: Some(6_000), ..Default::default() })
            .unwrap();
        db.upsert_edge(start, visible, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(i64::MAX), ..Default::default() })
            .unwrap();
        db.delete_node(deleted).unwrap();
        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let result = db
            .traverse(start, 2, &TraverseOptions { at_epoch: Some(1_000), ..Default::default() })
            .unwrap();

        let pairs: Vec<(u64, u32)> = result.items.iter().map(|hit| (hit.node_id, hit.depth)).collect();
        assert_eq!(pairs, vec![(visible, 1)]);

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_pagination_with_prune_policy_uses_filtered_path() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let keep_a = db.upsert_node(1, "keep-a", UpsertNodeOptions::default()).unwrap();
        let hidden = db.upsert_node(1, "hidden", UpsertNodeOptions { weight: 0.2, ..Default::default() }).unwrap();
        let keep_b = db.upsert_node(1, "keep-b", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, keep_a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, hidden, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, keep_b, 10, UpsertEdgeOptions::default())
            .unwrap();

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let p1 = db
            .traverse(start, 1, &TraverseOptions { limit: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(
            p1.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(keep_a, 1)]
        );
        assert_eq!(
            p1.next_cursor,
            Some(TraversalCursor {
                depth: 1,
                last_node_id: keep_a,
            })
        );

        let p2 = db
            .traverse(start, 1, &TraverseOptions { limit: Some(1), cursor: p1.next_cursor.clone(), ..Default::default() })
            .unwrap();
        assert_eq!(
            p2.items
                .iter()
                .map(|hit| (hit.node_id, hit.depth))
                .collect::<Vec<_>>(),
            vec![(keep_b, 1)]
        );
        assert!(p2.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_hidden_or_missing_start_returns_empty() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let hidden_start = db.upsert_node(1, "hidden-start", UpsertNodeOptions { weight: 0.2, ..Default::default() }).unwrap();
        let next = db.upsert_node(1, "next", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(hidden_start, next, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let hidden_result = db
            .traverse(hidden_start, 1, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();
        assert!(hidden_result.items.is_empty());
        assert!(hidden_result.next_cursor.is_none());

        let missing_result = db
            .traverse(999_999, 1, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();
        assert!(missing_result.items.is_empty());
        assert!(missing_result.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_traverse_flush_compact_and_reopen_parity() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let db = open_imm(&db_path);

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(start, d, 10, UpsertEdgeOptions::default())
            .unwrap();

        let baseline = db
            .traverse(start, 2, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();

        db.flush().unwrap();
        let after_flush = db
            .traverse(start, 2, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();
        assert_eq!(after_flush, baseline);

        db.compact().unwrap();
        let after_compact = db
            .traverse(start, 2, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();
        assert_eq!(after_compact, baseline);

        db.close().unwrap();
        let reopened = open_imm(&db_path);
        let after_reopen = reopened
            .traverse(start, 2, &TraverseOptions { min_depth: 0, ..Default::default() })
            .unwrap();
        assert_eq!(after_reopen, baseline);
        reopened.close().unwrap();
    }

    #[test]
    fn test_traverse_via_edge_id_uses_deterministic_tie_break() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let db = open_imm(&db_path);

        let start = db.upsert_node(1, "start", UpsertNodeOptions::default()).unwrap();
        let middle = db.upsert_node(1, "middle", UpsertNodeOptions::default()).unwrap();
        let target = db.upsert_node(1, "target", UpsertNodeOptions::default()).unwrap();

        db.upsert_edge(start, middle, 10, UpsertEdgeOptions::default())
            .unwrap();
        let older_edge = db
            .upsert_edge(middle, target, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        let newer_edge = db
            .upsert_edge(middle, target, 11, UpsertEdgeOptions::default())
            .unwrap();
        assert!(older_edge < newer_edge);

        let result = db
            .traverse(start, 2, &TraverseOptions { min_depth: 2, ..Default::default() })
            .unwrap();

        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].node_id, target);
        assert_eq!(result.items[0].via_edge_id, Some(older_edge));

        db.close().unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn traverse_depth_two_page(
        engine: &DatabaseEngine,
        start: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        node_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
        limit: Option<usize>,
        cursor: Option<&TraversalCursor>,
    ) -> TraversalPageResult {
        engine
            .traverse(start, 2, &TraverseOptions {
                min_depth: 2,
                direction,
                edge_type_filter: edge_type_filter.map(|s| s.to_vec()),
                node_type_filter: node_type_filter.map(|s| s.to_vec()),
                at_epoch,
                decay_lambda,
                limit,
                cursor: cursor.cloned(),
            })
            .unwrap()
    }

    #[test]
    fn test_traverse_depth_two_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e = engine.upsert_node(1, "e", UpsertNodeOptions::default()).unwrap();
        engine.upsert_edge(a, b, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, c, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, d, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, e, 1, UpsertEdgeOptions::default()).unwrap();

        let p1 = traverse_depth_two_page(
            &engine,
            a,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
            Some(2),
            None,
        );
        assert_eq!(p1.items.len(), 2);
        assert_eq!(p1.items.iter().map(|hit| hit.depth).collect::<Vec<_>>(), vec![2, 2]);
        assert!(p1.next_cursor.is_some());

        let p2 = traverse_depth_two_page(
            &engine,
            a,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
            Some(2),
            p1.next_cursor.as_ref(),
        );
        assert_eq!(p2.items.len(), 1);
        assert!(p2.next_cursor.is_none());

        let mut all_nodes: Vec<u64> = p1
            .items
            .iter()
            .chain(p2.items.iter())
            .map(|hit| hit.node_id)
            .collect();
        all_nodes.sort();
        assert_eq!(all_nodes, vec![c, d, e]);
    }

    #[test]
    fn test_traverse_depth_two_paged_roundtrip() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        for i in 0..5 {
            let b = engine
                .upsert_node(1, &format!("b{i}"), UpsertNodeOptions::default())
                .unwrap();
            engine.upsert_edge(a, b, 1, UpsertEdgeOptions::default()).unwrap();
            for j in 0..2 {
                let c = engine
                    .upsert_node(1, &format!("c{i}_{j}"), UpsertNodeOptions::default())
                    .unwrap();
                engine.upsert_edge(b, c, 1, UpsertEdgeOptions::default()).unwrap();
            }
        }

        let all = traverse_depth_two_page(
            &engine,
            a,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let mut paged = Vec::new();
        let mut cursor = None;
        loop {
            let page = traverse_depth_two_page(
                &engine,
                a,
                Direction::Outgoing,
                None,
                None,
                None,
                None,
                Some(3),
                cursor.as_ref(),
            );
            paged.extend(page.items.clone());
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(paged, all.items);
    }

    #[test]
    fn test_traverse_depth_two_cursor_past_end_returns_empty() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine.upsert_edge(a, b, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, c, 1, UpsertEdgeOptions::default()).unwrap();

        let page = traverse_depth_two_page(
            &engine,
            a,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
            Some(10),
            Some(&TraversalCursor {
                depth: 2,
                last_node_id: u64::MAX - 1,
            }),
        );
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn test_traverse_depth_two_filtered_emission_matches_old_constrained_shape() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(10, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(10, "d", UpsertNodeOptions::default()).unwrap();
        let e = engine.upsert_node(20, "e", UpsertNodeOptions::default()).unwrap();
        engine.upsert_edge(a, b, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, c, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, d, 2, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, e, 1, UpsertEdgeOptions::default()).unwrap();

        let hits = traverse_depth_two_page(
            &engine,
            a,
            Direction::Outgoing,
            Some(&[1]),
            Some(&[10]),
            None,
            None,
            None,
            None,
        );
        assert_eq!(hits.items.len(), 1);
        assert_eq!(hits.items[0].node_id, c);
    }

    #[test]
    fn test_traverse_depth_two_cross_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine.upsert_edge(a, b, 1, UpsertEdgeOptions::default()).unwrap();
        engine.upsert_edge(b, c, 1, UpsertEdgeOptions::default()).unwrap();
        engine.flush().unwrap();

        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        engine.upsert_edge(b, d, 1, UpsertEdgeOptions::default()).unwrap();

        let page = traverse_depth_two_page(
            &engine,
            a,
            Direction::Outgoing,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let mut nodes: Vec<u64> = page.items.iter().map(|hit| hit.node_id).collect();
        nodes.sort();
        assert_eq!(nodes, vec![c, d]);
    }

    #[test]
    fn test_edge_uniqueness_across_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create edge and flush to segment
        let e1 = engine
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        engine.flush().unwrap();

        // Upsert same triple, should find existing in segment and reuse ID
        let e2 = engine
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();
        assert_eq!(e1, e2, "same triple should reuse edge ID across flush");

        // Updated weight should be in memtable
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert!((edge.weight - 0.9).abs() < f32::EPSILON);

        // Different triple still gets new ID
        let e3 = engine
            .upsert_edge(1, 2, 20, UpsertEdgeOptions::default())
            .unwrap();
        assert_ne!(e1, e3);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbor_weight_survives_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let n2 = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let eid = engine
            .upsert_edge(n1, n2, 1, UpsertEdgeOptions { weight: 0.42, ..Default::default() })
            .unwrap();
        engine.flush().unwrap();

        // Read neighbors from segment. Weight should be preserved
        let nbrs = engine
            .neighbors(n1, &NeighborOptions::default())
            .unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].edge_id, eid);
        assert!(
            (nbrs[0].weight - 0.42).abs() < f32::EPSILON,
            "weight: {}",
            nbrs[0].weight
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_uniqueness_respects_cross_segment_tombstone() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create edge and flush (segment 1 has the edge)
        let e1 = engine
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        engine.flush().unwrap();

        // Delete the edge and flush again (segment 2 has the tombstone)
        engine.delete_edge(e1).unwrap();
        engine.flush().unwrap();

        // Now both segments are on disk, memtable is clean.
        // Upserting the same triple should get a NEW ID, not resurrect the deleted edge.
        let e2 = engine
            .upsert_edge(1, 2, 10, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();
        assert_ne!(
            e1, e2,
            "deleted edge should not be resurrected across segments"
        );

        engine.close().unwrap();
    }



    // ---- Top-k neighbors tests ----

    #[test]
    fn test_top_k_by_weight() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let e = engine.upsert_node(1, "e", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 0.1, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, d, 1, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, e, 1, UpsertEdgeOptions { weight: 0.3, ..Default::default() })
            .unwrap();

        // Top 2 by weight: c (0.9) and d (0.5)
        let top2 = engine
            .top_k_neighbors(a, 2, &TopKOptions::default())
            .unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].node_id, c);
        assert!((top2[0].weight - 0.9).abs() < 0.01);
        assert_eq!(top2[1].node_id, d);
        assert!((top2[1].weight - 0.5).abs() < 0.01);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_by_recency() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        // Edges with explicit valid_from to control recency
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 1.0, valid_from: Some(1000), valid_to: None, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 1.0, valid_from: Some(3000), valid_to: None, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, d, 1, UpsertEdgeOptions { weight: 1.0, valid_from: Some(2000), valid_to: None, ..Default::default() })
            .unwrap();

        // Top 2 by recency: c (3000) and d (2000)
        let top2 = engine
            .top_k_neighbors(a, 2, &TopKOptions { scoring: ScoringMode::Recency, ..Default::default() })
            .unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].node_id, c);
        assert_eq!(top2[1].node_id, d);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_by_decay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // b: high weight but old, c: medium weight and recent, d: low weight very recent
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 1.0, valid_from: Some(now - 7_200_000), valid_to: None, ..Default::default() })
            .unwrap(); // 2 hours ago
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 0.8, valid_from: Some(now - 600_000), valid_to: None, ..Default::default() })
            .unwrap(); // 10 min ago
        engine
            .upsert_edge(a, d, 1, UpsertEdgeOptions { weight: 0.3, valid_from: Some(now - 60_000), valid_to: None, ..Default::default() })
            .unwrap(); // 1 min ago

        // With strong decay (lambda=1.0), recent edges should beat old heavy ones
        let top2 = engine
            .top_k_neighbors(a, 2, &TopKOptions { scoring: ScoringMode::DecayAdjusted { lambda: 1.0 }, ..Default::default() })
            .unwrap();
        assert_eq!(top2.len(), 2);
        // c should rank highest (0.8 * exp(-1.0 * 0.167) ≈ 0.68) over d (0.3 * ~1.0 ≈ 0.3)
        // b is heavily decayed (1.0 * exp(-2.0) ≈ 0.14), lowest
        assert_eq!(top2[0].node_id, c);
        assert_eq!(top2[1].node_id, d);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_zero_returns_empty() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();

        let result = engine
            .top_k_neighbors(a, 0, &TopKOptions::default())
            .unwrap();
        assert!(result.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_k_greater_than_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 0.3, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 0.7, ..Default::default() })
            .unwrap();

        // k=10 but only 2 neighbors. Returns all 2, sorted desc
        let result = engine
            .top_k_neighbors(a, 10, &TopKOptions::default())
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].node_id, c); // higher weight first
        assert_eq!(result[1].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, c, 2, UpsertEdgeOptions { weight: 0.8, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, d, 1, UpsertEdgeOptions { weight: 0.7, ..Default::default() })
            .unwrap();

        // Top 1 of edge type 1 only
        let result = engine
            .top_k_neighbors(a, 1, &TopKOptions { type_filter: Some(vec![1]), ..Default::default() })
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_excludes_deleted_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 0.8, ..Default::default() })
            .unwrap();

        engine.delete_node(b).unwrap();

        let result = engine
            .top_k_neighbors(a, 2, &TopKOptions::default())
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_across_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();

        // Top 1 should be c (0.9) from memtable, beating b (0.5) from segment
        let result = engine
            .top_k_neighbors(a, 1, &TopKOptions::default())
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_negative_lambda_returns_error() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        let result = engine.top_k_neighbors(a, 5, &TopKOptions { scoring: ScoringMode::DecayAdjusted { lambda: -1.0 }, ..Default::default() });
        assert!(result.is_err());

        engine.close().unwrap();
    }

    // =====================================================================
    // extract_subgraph tests
    // =====================================================================

    #[test]
    fn test_subgraph_linear_chain_depth_1() {
        // A→B→C→D, extract from A at depth 1 → only A and B
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e_ab = engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(c, d, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 1, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b]));
        assert_eq!(sg.edges.len(), 1);
        assert_eq!(sg.edges[0].id, e_ab);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_linear_chain_depth_3() {
        // A→B→C→D, extract from A at depth 3 → all nodes and edges
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(c, d, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 3, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c, d]));
        assert_eq!(sg.edges.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_diamond_graph() {
        // Diamond: A→B, A→C, B→D, C→D. Node D should appear once.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, d, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(c, d, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 2, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c, d]));
        // All 4 edges included
        assert_eq!(sg.edges.len(), 4);
        // D appears exactly once in nodes
        assert_eq!(sg.nodes.iter().filter(|n| n.id == d).count(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_cycle() {
        // Cycle: A→B→C→A. BFS should terminate, not loop.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        let e_ca = engine
            .upsert_edge(c, a, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 10, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c]));
        // All 3 edges including the back-edge C→A
        let edge_ids: NodeIdSet = sg.edges.iter().map(|e| e.id).collect();
        assert_eq!(edge_ids.len(), 3);
        assert!(edge_ids.contains(&e_ca));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_self_loop() {
        // A→A (self-loop), A→B. Should not infinite-loop.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let e_aa = engine
            .upsert_edge(a, a, 1, UpsertEdgeOptions::default())
            .unwrap();
        let e_ab = engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 5, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b]));
        let edge_ids: NodeIdSet = sg.edges.iter().map(|e| e.id).collect();
        assert!(edge_ids.contains(&e_aa));
        assert!(edge_ids.contains(&e_ab));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_depth_zero() {
        // Depth 0 returns just the start node, no edges
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 0, &SubgraphOptions::default())
            .unwrap();

        assert_eq!(sg.nodes.len(), 1);
        assert_eq!(sg.nodes[0].id, a);
        assert_eq!(sg.edges.len(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_nonexistent_start_node() {
        // Non-existent start node → empty subgraph
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let sg = engine
            .extract_subgraph(999, 5, &SubgraphOptions::default())
            .unwrap();

        assert!(sg.nodes.is_empty());
        assert!(sg.edges.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_disconnected_not_reached() {
        // A→B, C (isolated). Subgraph from A should not include C.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let _c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 10, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b]));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_direction_incoming() {
        // A→B→C. Extract from C with Incoming direction → C, B, A
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(c, 2, &SubgraphOptions { direction: Direction::Incoming, ..Default::default() })
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_direction_both() {
        // A→B←C. Extract from B with Both direction → A, B, C
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(c, b, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(b, 1, &SubgraphOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_edge_type_filter() {
        // A→B (type 1), A→C (type 2), B→D (type 1).
        // Filter by type 1 → A, B, D (not C).
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(a, c, 2, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, d, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 2, &SubgraphOptions { edge_type_filter: Some(vec![1]), ..Default::default() })
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, d]));
        assert_eq!(sg.edges.len(), 2);
        // All edges should be type 1
        assert!(sg.edges.iter().all(|e| e.type_id == 1));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_cross_segment() {
        // Insert A→B, flush. Insert B→C in memtable. Subgraph spans both.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 2, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_with_deleted_node() {
        // A→B→C, delete B → subgraph from A should only contain A (B is deleted)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine.delete_node(b).unwrap();

        let sg = engine
            .extract_subgraph(a, 5, &SubgraphOptions::default())
            .unwrap();

        // B is deleted, so neighbors() won't return it; C unreachable
        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a]));
        assert_eq!(sg.edges.len(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_with_deleted_edge() {
        // A→B (edge1), A→C (edge2). Delete edge1. Subgraph from A → A, C.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine.delete_edge(e1).unwrap();

        let sg = engine
            .extract_subgraph(a, 1, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, c]));
        assert_eq!(sg.edges.len(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_temporal_filter() {
        // A→B (valid 100-200), A→C (valid 300-400).
        // At epoch 150 → only B reachable. At epoch 350 → only C reachable.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions { weight: 1.0, valid_from: Some(100), valid_to: Some(200), ..Default::default() })
            .unwrap();
        engine
            .upsert_edge(a, c, 1, UpsertEdgeOptions { weight: 1.0, valid_from: Some(300), valid_to: Some(400), ..Default::default() })
            .unwrap();

        let sg_150 = engine
            .extract_subgraph(a, 1, &SubgraphOptions { at_epoch: Some(150), ..Default::default() })
            .unwrap();
        let node_ids_150: NodeIdSet = sg_150.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids_150, NodeIdSet::from_iter([a, b]));

        let sg_350 = engine
            .extract_subgraph(a, 1, &SubgraphOptions { at_epoch: Some(350), ..Default::default() })
            .unwrap();
        let node_ids_350: NodeIdSet = sg_350.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids_350, NodeIdSet::from_iter([a, c]));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_large_fan_out() {
        // Hub node A with 50 outgoing edges to B1..B50
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "hub", UpsertNodeOptions::default()).unwrap();
        let mut expected = NodeIdSet::from_iter([a]);
        for i in 0..50 {
            let n = engine
                .upsert_node(1, &format!("spoke_{}", i), UpsertNodeOptions::default())
                .unwrap();
            engine
                .upsert_edge(a, n, 1, UpsertEdgeOptions::default())
                .unwrap();
            expected.insert(n);
        }

        let sg = engine
            .extract_subgraph(a, 1, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, expected);
        assert_eq!(sg.edges.len(), 50);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_depth_limits_traversal() {
        // Chain: A→B→C→D→E. Depth 2 → A, B, C only.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = engine.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = engine.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = engine.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e = engine.upsert_node(1, "e", UpsertNodeOptions::default()).unwrap();
        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(c, d, 1, UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(d, e, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 2, &SubgraphOptions::default())
            .unwrap();

        let node_ids: NodeIdSet = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, NodeIdSet::from_iter([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_preserves_node_properties() {
        // Verify extracted nodes carry their full properties
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props_a = BTreeMap::new();
        props_a.insert("name".to_string(), PropValue::String("Alice".to_string()));
        let a = engine.upsert_node(1, "a", UpsertNodeOptions { props: props_a, weight: 0.9, ..Default::default() }).unwrap();

        let mut props_b = BTreeMap::new();
        props_b.insert("name".to_string(), PropValue::String("Bob".to_string()));
        let b = engine.upsert_node(2, "b", UpsertNodeOptions { props: props_b, weight: 0.7, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 1, &SubgraphOptions::default())
            .unwrap();

        let node_a = sg.nodes.iter().find(|n| n.id == a).unwrap();
        assert_eq!(node_a.type_id, 1);
        assert_eq!(node_a.key, "a");
        assert_eq!(
            node_a.props.get("name"),
            Some(&PropValue::String("Alice".to_string()))
        );

        let node_b = sg.nodes.iter().find(|n| n.id == b).unwrap();
        assert_eq!(node_b.type_id, 2);
        assert_eq!(
            node_b.props.get("name"),
            Some(&PropValue::String("Bob".to_string()))
        );

        engine.close().unwrap();
    }

    // --- Batch adjacency tests ---

    #[test]
    fn test_neighbors_batch_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .neighbors_batch(&[a, b, c], &NeighborOptions::default())
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 2); // A→B, A→C
        assert_eq!(result.get(&b).unwrap().len(), 1); // B→C
        assert!(!result.contains_key(&c)); // C has no outgoing
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_matches_individual() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let mut ids = Vec::new();
        for i in 0..5 {
            ids.push(
                db.upsert_node(1, &format!("n{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                    .unwrap(),
            );
        }
        db.upsert_edge(ids[0], ids[1], 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(ids[0], ids[2], 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(ids[1], ids[2], 20, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();
        db.upsert_edge(ids[2], ids[3], 10, UpsertEdgeOptions { weight: 1.5, ..Default::default() })
            .unwrap();
        db.upsert_edge(ids[3], ids[4], 20, UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        db.upsert_edge(ids[4], ids[0], 10, UpsertEdgeOptions::default())
            .unwrap();

        let batch = db
            .neighbors_batch(&ids, &NeighborOptions::default())
            .unwrap();

        for &nid in &ids {
            let individual = db
                .neighbors(nid, &NeighborOptions::default())
                .unwrap();
            let batch_entries = batch.get(&nid).cloned().unwrap_or_default();
            let mut ind_edge_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
            let mut bat_edge_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
            ind_edge_ids.sort();
            bat_edge_ids.sort();
            assert_eq!(ind_edge_ids, bat_edge_ids, "mismatch for node {}", nid);
        }
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 20, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .neighbors_batch(&[a], &NeighborOptions { direction: Direction::Outgoing, type_filter: Some(vec![10]), ..Default::default() })
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 1);
        assert_eq!(result[&a][0].edge_type_id, 10);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_cross_segment() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        let c = db.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();
        db.flush().unwrap();

        let d = db.upsert_node(1, "d", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(b, d, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() })
            .unwrap();

        let result = db
            .neighbors_batch(&[a, b], &NeighborOptions::default())
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 2); // B (seg1) + C (seg2)
        assert_eq!(result.get(&b).unwrap().len(), 1); // D (memtable)
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_dedup_across_sources() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Re-upsert same edge triple. Should dedup (triple lookup across segments)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();

        // Batch and individual MUST return the same results
        let result = db
            .neighbors_batch(&[a], &NeighborOptions::default())
            .unwrap();
        let individual = db
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        let batch_entries = result.get(&a).unwrap();

        assert_eq!(batch_entries.len(), individual.len());
        let mut bat_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
        let mut ind_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
        bat_ids.sort();
        ind_ids.sort();
        assert_eq!(bat_ids, ind_ids);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let e1 = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();

        db.delete_edge(e1).unwrap();

        let result = db
            .neighbors_batch(&[a], &NeighborOptions::default())
            .unwrap();
        let entries = result.get(&a).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].node_id, c);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_direction_both() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .neighbors_batch(&[b], &NeighborOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();
        let entries = result.get(&b).unwrap();
        assert_eq!(entries.len(), 2); // incoming from A and C
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_self_loop_dedup() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        // Self-loop: A→A appears in both adj_out and adj_in
        db.upsert_edge(a, a, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();

        let result = db
            .neighbors_batch(&[a], &NeighborOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();
        let individual = db
            .neighbors(a, &NeighborOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();

        let batch_entries = result.get(&a).unwrap();
        // Self-loop should appear once (deduped by edge_id), not twice
        let mut bat_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
        let mut ind_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
        bat_ids.sort();
        ind_ids.sort();
        assert_eq!(bat_ids, ind_ids);
        assert_eq!(bat_ids.len(), ind_ids.len());
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_empty_input() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let result = db
            .neighbors_batch(&[], &NeighborOptions::default())
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_neighbors_batch_unsorted_input() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(c, a, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();

        // Input in reverse order. Batch method sorts internally
        let result = db
            .neighbors_batch(&[c, a], &NeighborOptions::default())
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 1);
        assert_eq!(result.get(&c).unwrap().len(), 1);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_large_graph_parity() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        // Build a graph with 50 nodes and ~100 edges across segment + memtable
        let mut node_ids = Vec::new();
        for i in 0..50 {
            node_ids.push(
                db.upsert_node(1, &format!("n{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                    .unwrap(),
            );
        }
        for i in 0..50 {
            let t1 = (i + 1) % 50;
            let t2 = (i + 25) % 50;
            db.upsert_edge(
                node_ids[i], node_ids[t1], 10, UpsertEdgeOptions::default(),
            )
            .unwrap();
            db.upsert_edge(
                node_ids[i], node_ids[t2], 20, UpsertEdgeOptions { weight: 0.5, ..Default::default() },
            )
            .unwrap();
        }

        db.flush().unwrap();

        // Add more edges in memtable
        for i in 0..10 {
            let t = (i + 5) % 50;
            db.upsert_edge(
                node_ids[i], node_ids[t], 30, UpsertEdgeOptions { weight: 0.3, ..Default::default() },
            )
            .unwrap();
        }

        let batch = db
            .neighbors_batch(&node_ids, &NeighborOptions::default())
            .unwrap();

        for &nid in &node_ids {
            let individual = db
                .neighbors(nid, &NeighborOptions::default())
                .unwrap();
            let batch_entries = batch.get(&nid).cloned().unwrap_or_default();
            let mut ind_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
            let mut bat_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
            ind_ids.sort();
            bat_ids.sort();
            assert_eq!(
                ind_ids, bat_ids,
                "neighbors_batch mismatch for node {} (individual: {:?}, batch: {:?})",
                nid, ind_ids, bat_ids
            );
        }
        db.close().unwrap();
    }


    // --- Self-loop tests (additional coverage) ---

    #[test]
    fn test_self_loop_neighbors_outgoing() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions::default())
            .unwrap();

        let out = db
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, a);

        let inc = db
            .neighbors(a, &NeighborOptions { direction: Direction::Incoming, ..Default::default() })
            .unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].node_id, a);

        // Both direction should dedup the self-loop
        let both = db
            .neighbors(a, &NeighborOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();
        assert_eq!(
            both.len(),
            1,
            "self-loop should appear once in Both, not twice"
        );
        db.close().unwrap();
    }

    #[test]
    fn test_self_loop_survives_flush_and_compact() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let e = db
            .upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 2.5, ..Default::default() })
            .unwrap();

        db.flush().unwrap();
        // Add a second segment to enable compaction
        let _b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let edge = db.get_edge(e).unwrap().unwrap();
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, a);
        assert_eq!(edge.weight, 2.5);

        let nbrs = db
            .neighbors(a, &NeighborOptions { direction: Direction::Both, ..Default::default() })
            .unwrap();
        assert_eq!(nbrs.len(), 1);

        // Self-loop should also appear in PPR
        let ppr = db
            .personalized_pagerank(&[a], &PprOptions::default())
            .unwrap();
        let a_score = ppr.scores.iter().find(|s| s.0 == a).map(|s| s.1).unwrap();
        assert!(a_score > 0.0);

        db.close().unwrap();
    }

    #[test]
    fn test_self_loop_in_top_k() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() })
            .unwrap();

        let top = db
            .top_k_neighbors(a, 10, &TopKOptions::default())
            .unwrap();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].node_id, a);
        assert_eq!(top[0].weight, 5.0);
        db.close().unwrap();
    }

    // --- Phase 18a2: Degree cache rebuild tests ---
    //
    // CP1 tests validate rebuild_degree_cache() correctness. Since mutation
    // hooks are not wired yet (CP2), tests that insert data after open()
    // call rebuild_degree_cache() explicitly. Tests that close/reopen verify
    // the rebuild happens automatically during open().

    #[test]
    fn test_degree_cache_rebuild_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        // a→b (w=2.0), a→c (w=3.0), b→c (w=1.5)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() }).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 1.5, ..Default::default() }).unwrap();

        // Rebuild to pick up new edges (no mutation hooks in CP1)
        db.rebuild_degree_cache().unwrap();

        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 2);
        assert_eq!(ea.in_degree, 0);
        assert!((ea.out_weight_sum - 5.0).abs() < 1e-10);
        assert!((ea.in_weight_sum - 0.0).abs() < 1e-10);
        assert_eq!(ea.self_loop_count, 0);

        let eb = db.degree_cache_entry(b);
        assert_eq!(eb.out_degree, 1);
        assert_eq!(eb.in_degree, 1);
        assert!((eb.out_weight_sum - 1.5).abs() < 1e-10);
        assert!((eb.in_weight_sum - 2.0).abs() < 1e-10);

        let ec = db.degree_cache_entry(c);
        assert_eq!(ec.out_degree, 0);
        assert_eq!(ec.in_degree, 2);
        assert!((ec.in_weight_sum - 4.5).abs() < 1e-10);

        // d has no edges. Should not be in cache (returns ZERO).
        let ed = db.degree_cache_entry(d);
        assert_eq!(ed.out_degree, 0);
        assert_eq!(ed.in_degree, 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_rebuild_segment_only() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        {
            let db = open_imm(&path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() }).unwrap();
            db.upsert_edge(b, a, 10, UpsertEdgeOptions { weight: 2.5, ..Default::default() }).unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        // Reopen. Cache should be rebuilt from segments during open().
        let db = open_imm(&path);
        let ea = db.degree_cache_entry(1);
        assert_eq!(ea.out_degree, 1);
        assert_eq!(ea.in_degree, 1);
        assert!((ea.out_weight_sum - 4.0).abs() < 1e-10);
        assert!((ea.in_weight_sum - 2.5).abs() < 1e-10);

        let eb = db.degree_cache_entry(2);
        assert_eq!(eb.out_degree, 1);
        assert_eq!(eb.in_degree, 1);
        assert!((eb.out_weight_sum - 2.5).abs() < 1e-10);
        assert!((eb.in_weight_sum - 4.0).abs() < 1e-10);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_rebuild_mixed_sources() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        {
            let db = open_imm(&path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            // a→b in segment
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            db.flush().unwrap();
            // a→c in WAL (unflushed)
            db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();
            db.close().unwrap();
        }

        // Reopen. WAL replays a→c into memtable, segment has a→b.
        let db = open_imm(&path);
        let ea = db.degree_cache_entry(1);
        assert_eq!(ea.out_degree, 2); // a→b + a→c
        assert!((ea.out_weight_sum - 3.0).abs() < 1e-10);

        let eb = db.degree_cache_entry(2);
        assert_eq!(eb.in_degree, 1);

        let ec = db.degree_cache_entry(3);
        assert_eq!(ec.in_degree, 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_rebuild_with_tombstones() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        {
            let db = open_imm(&path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            let e1 = db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();
            db.flush().unwrap();
            // Delete one edge. Tombstone in WAL.
            db.delete_edge(e1).unwrap();
            db.close().unwrap();
        }

        // Reopen. Segment has both edges, WAL has delete tombstone for e1.
        let db = open_imm(&path);
        let ea = db.degree_cache_entry(1);
        assert_eq!(ea.out_degree, 1); // only a→c survives
        assert!((ea.out_weight_sum - 2.0).abs() < 1e-10);

        let eb = db.degree_cache_entry(2);
        assert_eq!(eb.in_degree, 0); // b lost its incoming edge

        let ec = db.degree_cache_entry(3);
        assert_eq!(ec.in_degree, 1); // c still has incoming

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_rebuild_self_loop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        {
            let db = open_imm(&path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            // Self-loop on a
            db.upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() }).unwrap();
            // Normal edge a→b
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            db.close().unwrap();
        }

        // Reopen. Rebuild from WAL replay.
        let db = open_imm(&path);
        // Node IDs: a=1, b=2
        let ea = db.degree_cache_entry(1);
        assert_eq!(ea.out_degree, 2); // self-loop + a→b
        assert_eq!(ea.in_degree, 1);  // self-loop only
        assert_eq!(ea.self_loop_count, 1);
        assert!((ea.self_loop_weight_sum - 3.0).abs() < 1e-10);

        // Verify Direction::Both formula: out + in - self_loops = 2 + 1 - 1 = 2
        let both_degree = (ea.out_degree + ea.in_degree - ea.self_loop_count) as u64;
        assert_eq!(both_degree, 2);

        // Verify it matches the walk-based API
        assert_eq!(db.degree(1, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), both_degree);
        assert_eq!(db.degree(1, &DegreeOptions::default()).unwrap(), ea.out_degree as u64);
        assert_eq!(db.degree(1, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), ea.in_degree as u64);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_parity_with_walk() {
        // Exhaustive parity check: build a non-trivial graph, verify every node's
        // cache entry matches the walk-based degree for all directions.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        {
            let db = open_imm(&path);

            let mut nodes = Vec::new();
            for i in 0..10 {
                nodes.push(db.upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default()).unwrap());
            }

            // Chain: 0→1→2→...→9
            for i in 0..9 {
                db.upsert_edge(nodes[i], nodes[i + 1], 10, UpsertEdgeOptions { weight: (i as f32) + 0.5, ..Default::default() }).unwrap();
            }
            // Hub: node 0 → all others (2..10)
            for i in 2..10 {
                db.upsert_edge(nodes[0], nodes[i], 20, UpsertEdgeOptions::default()).unwrap();
            }
            // Self-loop on node 5
            db.upsert_edge(nodes[5], nodes[5], 10, UpsertEdgeOptions { weight: 7.0, ..Default::default() }).unwrap();

            // Flush half to segments
            db.flush().unwrap();

            // Add edge in WAL (unflushed)
            db.upsert_edge(nodes[9], nodes[0], 10, UpsertEdgeOptions { weight: 0.1, ..Default::default() }).unwrap();
            db.close().unwrap();
        }

        // Reopen. Cache rebuilt from segments + WAL.
        let db = open_imm(&path);

        // Node IDs start at 1
        for nid in 1..=10 {
            let cache = db.degree_cache_entry(nid);

            let walk_out_deg = db.degree(nid, &DegreeOptions::default()).unwrap();
            let walk_in_deg = db.degree(nid, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap();
            let walk_both_deg = db.degree(nid, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();
            let walk_out_w = db.sum_edge_weights(nid, &DegreeOptions::default()).unwrap();
            let walk_in_w = db.sum_edge_weights(nid, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap();

            assert_eq!(cache.out_degree as u64, walk_out_deg,
                "out_degree mismatch for node {}", nid);
            assert_eq!(cache.in_degree as u64, walk_in_deg,
                "in_degree mismatch for node {}", nid);
            let cache_both = (cache.out_degree + cache.in_degree - cache.self_loop_count) as u64;
            assert_eq!(cache_both, walk_both_deg,
                "both_degree mismatch for node {}", nid);
            assert!((cache.out_weight_sum - walk_out_w).abs() < 1e-10,
                "out_weight mismatch for node {}", nid);
            assert!((cache.in_weight_sum - walk_in_w).abs() < 1e-10,
                "in_weight mismatch for node {}", nid);
        }

        db.close().unwrap();
    }

    // --- Phase 18a2 CP2: Mutation hook regression tests ---
    //
    // Each test verifies the degree cache is updated correctly by the mutation
    // hooks in append_and_apply, without needing a rebuild.

    #[test]
    fn test_degree_cache_mutation_new_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        // Before any edges: cache should be empty for these nodes
        assert_eq!(db.degree_cache_entry(a).out_degree, 0);
        assert_eq!(db.degree_cache_entry(b).in_degree, 0);

        // Insert edge a→b
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.5, ..Default::default() }).unwrap();

        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 1);
        assert_eq!(ea.in_degree, 0);
        assert!((ea.out_weight_sum - 2.5).abs() < 1e-10);
        assert_eq!(ea.self_loop_count, 0);

        let eb = db.degree_cache_entry(b);
        assert_eq!(eb.out_degree, 0);
        assert_eq!(eb.in_degree, 1);
        assert!((eb.in_weight_sum - 2.5).abs() < 1e-10);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_mutation_update_same_endpoints_weight_change() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { edge_uniqueness: true, ..Default::default() };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        // Insert edge a→b with weight 2.0
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();
        assert_eq!(db.degree_cache_entry(a).out_degree, 1);
        assert!((db.degree_cache_entry(a).out_weight_sum - 2.0).abs() < 1e-10);

        // Update same edge (same from, to, type_id) with weight 5.0
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 5.0, ..Default::default() }).unwrap();

        // Degree should NOT change, only weight
        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 1, "degree should not change on update");
        assert!((ea.out_weight_sum - 5.0).abs() < 1e-10, "weight should update to 5.0");

        let eb = db.degree_cache_entry(b);
        assert_eq!(eb.in_degree, 1, "in_degree should not change on update");
        assert!((eb.in_weight_sum - 5.0).abs() < 1e-10, "in weight should update to 5.0");

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_mutation_delete_memtable_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let e1 = db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() }).unwrap();
        assert_eq!(db.degree_cache_entry(a).out_degree, 1);

        // Delete the edge (still in memtable, not flushed)
        db.delete_edge(e1).unwrap();

        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 0, "out_degree should be 0 after delete");
        assert!((ea.out_weight_sum - 0.0).abs() < 1e-10);

        let eb = db.degree_cache_entry(b);
        assert_eq!(eb.in_degree, 0, "in_degree should be 0 after delete");

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_mutation_delete_segment_only_edge() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        let e1 = db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() }).unwrap();

        // Flush to segments. Edges are now segment-only.
        db.flush().unwrap();

        assert_eq!(db.degree_cache_entry(a).out_degree, 2);

        // Delete edge that exists only in segments
        db.delete_edge(e1).unwrap();

        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 1, "out_degree should be 1 after segment-only delete");
        assert!((ea.out_weight_sum - 3.0).abs() < 1e-10);

        let eb = db.degree_cache_entry(b);
        assert_eq!(eb.in_degree, 0, "in_degree should be 0 after segment-only delete");

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_mutation_node_delete_cascade() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // a→b, b→c, c→a (triangle)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(c, a, 10, UpsertEdgeOptions::default()).unwrap();

        assert_eq!(db.degree_cache_entry(a).out_degree, 1);
        assert_eq!(db.degree_cache_entry(a).in_degree, 1);
        assert_eq!(db.degree_cache_entry(b).out_degree, 1);
        assert_eq!(db.degree_cache_entry(b).in_degree, 1);

        // Delete node b. Cascades: deletes a→b, b→c, then node b.
        db.delete_node(b).unwrap();

        // a: lost outgoing a→b, still has incoming c→a
        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 0);
        assert_eq!(ea.in_degree, 1);

        // b: removed from cache entirely
        let eb = db.degree_cache_entry(b);
        assert_eq!(eb.out_degree, 0);
        assert_eq!(eb.in_degree, 0);

        // c: lost incoming b→c, still has outgoing c→a
        let ec = db.degree_cache_entry(c);
        assert_eq!(ec.out_degree, 1);
        assert_eq!(ec.in_degree, 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_mutation_self_loop() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        // Insert self-loop on a
        let sl = db.upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() }).unwrap();

        let ea = db.degree_cache_entry(a);
        assert_eq!(ea.out_degree, 1);
        assert_eq!(ea.in_degree, 1);
        assert_eq!(ea.self_loop_count, 1);
        assert!((ea.self_loop_weight_sum - 4.0).abs() < 1e-10);
        // Both = out + in - self_loop = 1 + 1 - 1 = 1
        assert_eq!((ea.out_degree + ea.in_degree - ea.self_loop_count) as u64, 1);

        // Add normal edge a→b
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();

        let ea2 = db.degree_cache_entry(a);
        assert_eq!(ea2.out_degree, 2);
        assert_eq!(ea2.in_degree, 1);
        assert_eq!(ea2.self_loop_count, 1);
        // Both = 2 + 1 - 1 = 2
        assert_eq!((ea2.out_degree + ea2.in_degree - ea2.self_loop_count) as u64, 2);

        // Delete self-loop
        db.delete_edge(sl).unwrap();

        let ea3 = db.degree_cache_entry(a);
        assert_eq!(ea3.out_degree, 1); // only a→b remains
        assert_eq!(ea3.in_degree, 0);
        assert_eq!(ea3.self_loop_count, 0);
        assert!((ea3.self_loop_weight_sum - 0.0).abs() < 1e-10);
        // Both = 1 + 0 - 0 = 1
        assert_eq!((ea3.out_degree + ea3.in_degree - ea3.self_loop_count) as u64, 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_mutation_idempotent_delete() {
        // Deleting an already-deleted edge should not change the cache
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let e1 = db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.delete_edge(e1).unwrap();

        let ea_after_first = db.degree_cache_entry(a);
        assert_eq!(ea_after_first.out_degree, 0);

        // Delete again. Should be idempotent.
        db.delete_edge(e1).unwrap();

        let ea_after_second = db.degree_cache_entry(a);
        assert_eq!(ea_after_second.out_degree, 0, "double delete should not underflow");

        db.close().unwrap();
    }

    // --- Phase 18a2 CP3: API wiring + lifecycle tests ---

    #[test]
    fn test_degree_cache_lifecycle_insert_flush_delete_compact_reopen() {
        // Full lifecycle: insert → flush → degree → delete → degree →
        // compact → degree → reopen → degree
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");

        {
            let db = open_imm(&path);

            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

            // Insert edges
            let e1 = db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();
            db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 3.0, ..Default::default() }).unwrap();
            db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

            assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);
            assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 2);
            assert!((db.sum_edge_weights(a, &DegreeOptions::default()).unwrap() - 5.0).abs() < 1e-10);

            // Flush to segments
            db.flush().unwrap();

            // Degree should be unchanged after flush
            assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);
            assert_eq!(db.degree(b, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 2);

            // Delete one edge
            db.delete_edge(e1).unwrap();

            assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1);
            assert_eq!(db.degree(b, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 0);

            // Add more edges and flush again for compaction
            db.upsert_edge(c, a, 10, UpsertEdgeOptions { weight: 4.0, ..Default::default() }).unwrap();
            db.flush().unwrap();

            // Compact
            db.compact().unwrap();

            // Degree should reflect post-compact state
            assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 1); // a→c
            assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 1); // c→a
            assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 2);
            assert_eq!(db.degree(c, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 3); // a→c(in) + b→c(in) + c→a(out)

            db.close().unwrap();
        }

        // Reopen. Cache rebuilt from segments.
        {
            let db = open_imm(&path);
            assert_eq!(db.degree(1, &DegreeOptions::default()).unwrap(), 1);
            assert_eq!(db.degree(1, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 1);
            assert_eq!(db.degree(1, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 2);
            assert_eq!(db.degree(3, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap(), 3);
            db.close().unwrap();
        }
    }

    #[test]
    fn test_degree_cache_filtered_queries_bypass_cache() {
        // Verify that type-filtered and temporal queries still use the walk path
        // (they should return different results from the cache-backed unfiltered path
        // when type filtering narrows the result set).
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // a→b with type 10, a→c with type 20
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(a, c, 20, UpsertEdgeOptions::default()).unwrap();

        // Unfiltered: both edges
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);

        // Type-filtered: only type 10 (uses walk path, not cache)
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, type_filter: Some(vec![10]), ..Default::default() }).unwrap(), 1);

        // Type-filtered: only type 20
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, type_filter: Some(vec![20]), ..Default::default() }).unwrap(), 1);

        // Temporal: at_epoch=Some(now) should still work via walk
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert_eq!(db.degree(a, &DegreeOptions { direction: Direction::Outgoing, at_epoch: Some(now), ..Default::default() }).unwrap(), 2);

        db.close().unwrap();
    }

    // --- Phase 18a2: Temporal edge count tracking ---

    #[test]
    fn test_degree_cache_temporal_edge_bypasses_cache() {
        // An edge with finite valid_to should cause temporal_edge_count > 0,
        // which means degree(at_epoch=None) falls through to the walk path
        // (correct even as wall-clock time passes).
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        // Timeless edge. Cache should be used.
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.temporal_edge_count, 0);
        assert_eq!(entry_a.out_degree, 1);

        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // Temporal edge (valid_to = now + 10 seconds)
        let future_expiry = now_millis() + 10_000;
        db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, valid_from: None, valid_to: Some(future_expiry), ..Default::default() })
            .unwrap();

        // Node a now has temporal_edge_count > 0
        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.temporal_edge_count, 1);
        assert_eq!(entry_a.out_degree, 2);

        // Node c also has temporal_edge_count > 0 (incoming temporal)
        let entry_c = db.degree_cache_entry(c);
        assert_eq!(entry_c.temporal_edge_count, 1);

        // degree() still returns correct answer (falls through to walk)
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);

        // Node b has no temporal edges. Cache is safe for it.
        let entry_b = db.degree_cache_entry(b);
        assert_eq!(entry_b.temporal_edge_count, 0);
    }

    #[test]
    fn test_degree_cache_temporal_delete_clears_count() {
        // Deleting a temporal edge should decrement temporal_edge_count.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let future_expiry = now_millis() + 10_000;
        let e = db
            .upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: None, valid_to: Some(future_expiry), ..Default::default() })
            .unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 1);
        assert_eq!(db.degree_cache_entry(b).temporal_edge_count, 1);

        db.delete_edge(e).unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 0);
        assert_eq!(db.degree_cache_entry(b).temporal_edge_count, 0);
    }

    #[test]
    fn test_degree_cache_temporal_to_timeless_update() {
        // Updating an edge from temporal to timeless should decrement
        // temporal_edge_count. Requires edge_uniqueness so the second
        // upsert replaces the first (same from/to/type_id).
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { edge_uniqueness: true, ..Default::default() };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let future_expiry = now_millis() + 10_000;
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: None, valid_to: Some(future_expiry), ..Default::default() })
            .unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 1);

        // Re-upsert same edge with valid_to = None (timeless)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 0);
        assert_eq!(db.degree_cache_entry(b).temporal_edge_count, 0);
    }

    #[test]
    fn test_degree_cache_timeless_to_temporal_update() {
        // invalidate_edge sets valid_to, making a timeless edge temporal.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let e = db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 0);

        // Invalidate (sets valid_to to past → temporal + not valid)
        db.invalidate_edge(e, 1).unwrap();
        // temporal_edge_count should be 1 (edge has finite valid_to now)
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 1);
        // Degree should be 0 (edge is expired)
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);
    }

    #[test]
    fn test_degree_cache_temporal_self_loop() {
        // A temporal self-loop should increment temporal_edge_count once
        // (not twice) on the node.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        let future_expiry = now_millis() + 10_000;
        let e = db
            .upsert_edge(a, a, 10, UpsertEdgeOptions { weight: 1.0, valid_from: None, valid_to: Some(future_expiry), ..Default::default() })
            .unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 1); // once, not twice

        db.delete_edge(e).unwrap();
        assert_eq!(db.degree_cache_entry(a).temporal_edge_count, 0);
    }

    #[test]
    fn test_degree_cache_temporal_rebuild_after_flush() {
        // After flush + reopen, temporal_edge_count should be correctly
        // rebuilt from segments.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        {
            let db = open_imm(&path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

            // Timeless edge
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            // Temporal edge
            let future = now_millis() + 100_000;
            db.upsert_edge(a, c, 10, UpsertEdgeOptions { weight: 2.0, valid_from: None, valid_to: Some(future), ..Default::default() })
                .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }
        {
            let db = open_imm(&path);
            // After reopen + rebuild, temporal count should reflect the temporal edge
            let entry_a = db.degree_cache_entry(db.get_node_by_key(1, "a").unwrap().unwrap().id);
            assert_eq!(entry_a.temporal_edge_count, 1);
            let entry_b = db.degree_cache_entry(db.get_node_by_key(1, "b").unwrap().unwrap().id);
            assert_eq!(entry_b.temporal_edge_count, 0);
            let entry_c = db.degree_cache_entry(db.get_node_by_key(1, "c").unwrap().unwrap().id);
            assert_eq!(entry_c.temporal_edge_count, 1);
        }
    }

    #[test]
    fn test_degree_cache_expired_edge_not_counted_but_temporal() {
        // An edge with valid_to in the past is not counted in degree
        // but should still register as temporal (so cache is bypassed).
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        // Already expired (valid_to = 1ms after epoch)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: None, valid_to: Some(1), ..Default::default() }).unwrap();

        let entry = db.degree_cache_entry(a);
        assert_eq!(entry.out_degree, 0); // not counted (expired)
        assert_eq!(entry.temporal_edge_count, 1); // still tracked as temporal

        // degree() should return 0 (falls through to walk, which also says 0)
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);
    }

    #[test]
    fn test_degree_cache_future_dated_edge_is_temporal() {
        // An edge with valid_from far in the future (valid_to = i64::MAX) should
        // still be marked temporal because valid_from > now. This means the cache
        // bypasses this node, falling through to the walk path which correctly
        // excludes the not-yet-valid edge.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        // Future-dated edge: valid_from = far future, valid_to = infinity
        let far_future = i64::MAX - 1_000_000;
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(far_future), valid_to: None, ..Default::default() }).unwrap();

        let entry_a = db.degree_cache_entry(a);
        let entry_b = db.degree_cache_entry(b);

        // Not counted in degree (not valid now)
        assert_eq!(entry_a.out_degree, 0);
        assert_eq!(entry_b.in_degree, 0);

        // But IS tracked as temporal (valid_from > now)
        assert_eq!(entry_a.temporal_edge_count, 1);
        assert_eq!(entry_b.temporal_edge_count, 1);

        // degree() returns 0 (walk path also says 0, edge not yet valid)
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 0);
        assert_eq!(db.degree(b, &DegreeOptions { direction: Direction::Incoming, ..Default::default() }).unwrap(), 0);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_future_dated_rebuild_after_flush() {
        // After flush + reopen, future-dated edges should still produce
        // temporal_edge_count > 0 via rebuild.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");

        let a;
        let b;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let far_future = i64::MAX - 1_000_000;
            db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(far_future), valid_to: None, ..Default::default() }).unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }
        {
            let db = DatabaseEngine::open(&path, &DbOptions::default()).unwrap();
            let entry_a = db.degree_cache_entry(a);
            let entry_b = db.degree_cache_entry(b);
            assert_eq!(entry_a.temporal_edge_count, 1);
            assert_eq!(entry_b.temporal_edge_count, 1);
            assert_eq!(entry_a.out_degree, 0);
            assert_eq!(entry_b.in_degree, 0);
        }
    }

    #[test]
    fn test_degree_overlay_explicit_txn_publishes_only_on_commit() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let before = db.active_degree_overlay_for_test();

        let mut txn = db.begin_write_txn().unwrap();
        txn.upsert_edge(
            TxnNodeRef::Id(a),
            TxnNodeRef::Id(b),
            10,
            UpsertEdgeOptions {
                weight: 2.5,
                ..Default::default()
            },
        )
        .unwrap();

        assert!(std::sync::Arc::ptr_eq(
            &before,
            &db.active_degree_overlay_for_test()
        ));
        assert_eq!(db.degree_cache_entry(a).out_degree, 0);

        txn.commit().unwrap();
        let after = db.active_degree_overlay_for_test();
        assert!(!std::sync::Arc::ptr_eq(&before, &after));
        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.out_degree, 1);
        assert!((entry_a.out_weight_sum - 2.5).abs() < 1e-10);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_overlay_txn_rollback_and_conflict_publish_no_state() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();

        let before_rollback = db.active_degree_overlay_for_test();
        let mut rollback_txn = db.begin_write_txn().unwrap();
        rollback_txn
            .upsert_edge(
                TxnNodeRef::Id(a),
                TxnNodeRef::Id(b),
                10,
                UpsertEdgeOptions::default(),
            )
            .unwrap();
        rollback_txn.rollback().unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &before_rollback,
            &db.active_degree_overlay_for_test()
        ));
        assert_eq!(db.degree_cache_entry(a).out_degree, 0);

        let mut conflicted_txn = db.begin_write_txn().unwrap();
        conflicted_txn
            .upsert_edge(
                TxnNodeRef::Id(a),
                TxnNodeRef::Id(b),
                10,
                UpsertEdgeOptions {
                    weight: 2.0,
                    ..Default::default()
                },
            )
            .unwrap();
        db.upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();
        let before_conflict = db.active_degree_overlay_for_test();
        let err = match conflicted_txn.commit() {
            Ok(_) => panic!("expected transaction conflict"),
            Err(err) => err,
        };
        assert!(matches!(err, EngineError::TxnConflict(_)));
        assert!(std::sync::Arc::ptr_eq(
            &before_conflict,
            &db.active_degree_overlay_for_test()
        ));
        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.out_degree, 1);
        assert!((entry_a.out_weight_sum - 3.0).abs() < 1e-10);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_overlay_stale_read_view_and_node_only_no_overlay_edit() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let overlay_before_node_only = db.active_degree_overlay_for_test();
        db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &overlay_before_node_only,
            &db.active_degree_overlay_for_test()
        ));

        let stale = db.published_read_view_for_test();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        assert_eq!(stale.degree_entry_for_test(a).out_degree, 0);
        assert_eq!(db.degree_cache_entry(a).out_degree, 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_overlay_repeated_edge_ids_in_one_wal_batch() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let now = now_millis();
        let first = EdgeRecord {
            id: 42,
            from: a,
            to: b,
            type_id: 10,
            props: std::collections::BTreeMap::new(),
            created_at: now,
            updated_at: now,
            weight: 2.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        };
        let mut second = first.clone();
        second.updated_at = now + 1;
        second.weight = 5.0;

        db.write_op_batch(&[WalOp::UpsertEdge(first), WalOp::UpsertEdge(second)])
            .unwrap();

        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.out_degree, 1);
        assert!((entry_a.out_weight_sum - 5.0).abs() < 1e-10);
        let entry_b = db.degree_cache_entry(b);
        assert_eq!(entry_b.in_degree, 1);
        assert!((entry_b.in_weight_sum - 5.0).abs() < 1e-10);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_overlay_flush_writes_segment_sidecar() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() })
            .unwrap();

        db.flush().unwrap();
        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(segments[0].degree_delta_available());
        assert_eq!(segments[0].degree_delta(a).unwrap().out_degree, 1);
        assert_eq!(segments[0].degree_delta(b).unwrap().in_degree, 1);

        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.out_degree, 1);
        assert!((entry_a.out_weight_sum - 2.0).abs() < 1e-10);

        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(
            a,
            c,
            10,
            UpsertEdgeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();
        let entry_a = db.degree_cache_entry(a);
        assert_eq!(entry_a.out_degree, 2);
        assert!((entry_a.out_weight_sum - 5.0).abs() < 1e-10);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_compaction_folds_valid_inputs() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();
        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(segments[0].degree_delta_available());
        assert_eq!(segments[0].degree_delta(a).unwrap().out_degree, 2);
        assert_eq!(segments[0].degree_delta(b).unwrap().in_degree, 1);
        assert_eq!(segments[0].degree_delta(c).unwrap().in_degree, 1);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_streaming_compaction_large_overlap_fast_path() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let hub = db
            .upsert_node(1, "hub", UpsertNodeOptions::default())
            .unwrap();
        let mut leaves = Vec::new();
        for idx in 0..320 {
            leaves.push(
                db.upsert_node(1, &format!("leaf-{idx}"), UpsertNodeOptions::default())
                    .unwrap(),
            );
        }
        db.flush().unwrap();

        for chunk in leaves.chunks(80) {
            for &leaf in chunk {
                db.upsert_edge(
                    hub,
                    leaf,
                    10,
                    UpsertEdgeOptions {
                        weight: 1.5,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            db.flush().unwrap();
        }

        db.compact().unwrap();
        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(segments[0].degree_delta_available());
        assert_eq!(segments[0].degree_delta(hub).unwrap().out_degree, 320);
        assert_scalar_degree_family_routes(
            &db,
            hub,
            DegreeOptions::default(),
            320,
            480.0,
            Some(1.5),
            3,
            0,
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_compaction_folds_update_and_delete_deltas() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        };
        let db = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
        let ac = db
            .upsert_edge(
                a,
                c,
                10,
                UpsertEdgeOptions {
                    weight: 4.0,
                    ..Default::default()
                },
            )
            .unwrap();
        db.flush().unwrap();

        db.upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                weight: 5.0,
                ..Default::default()
            },
        )
        .unwrap();
        db.delete_edge(ac).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();
        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(segments[0].degree_delta_available());
        let a_delta = segments[0].degree_delta(a).unwrap();
        assert_eq!(a_delta.out_degree, 1);
        assert!((a_delta.out_weight_sum - 5.0).abs() < 1e-10);
        let b_delta = segments[0].degree_delta(b).unwrap();
        assert_eq!(b_delta.in_degree, 1);
        assert!((b_delta.in_weight_sum - 5.0).abs() < 1e-10);
        assert_eq!(
            segments[0].degree_delta(c).unwrap(),
            crate::degree_cache::DegreeDelta::ZERO
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_compaction_omits_when_input_unavailable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        let b;
        let c;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            db.flush().unwrap();
            db.upsert_edge(a, c, 10, UpsertEdgeOptions::default()).unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        std::fs::remove_file(segment_dir(&path, 1).join(crate::degree_cache::DEGREE_DELTA_FILENAME))
            .unwrap();

        let db = open_imm(&path);
        assert!(db
            .segments_for_test()
            .iter()
            .any(|segment| !segment.degree_delta_available()));
        db.compact().unwrap();

        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(!segments[0].degree_delta_available());
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_compaction_omits_when_prune_policy_active() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();

        db.set_prune_policy(
            "active_but_no_match",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.0),
                type_id: None,
            },
        )
        .unwrap();
        db.compact().unwrap();

        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(!segments[0].degree_delta_available());
        assert_eq!(db.degree(a, &DegreeOptions::default()).unwrap(), 2);

        db.close().unwrap();
    }

    fn assert_scalar_degree_family_routes(
        db: &DatabaseEngine,
        node_id: u64,
        options: DegreeOptions,
        expected_degree: u64,
        expected_sum: f64,
        expected_avg: Option<f64>,
        expected_fast: usize,
        expected_walk: usize,
    ) {
        db.reset_degree_query_routes();
        assert_eq!(db.degree(node_id, &options).unwrap(), expected_degree);
        assert!((db.sum_edge_weights(node_id, &options).unwrap() - expected_sum).abs() < 1e-10);
        match (db.avg_edge_weight(node_id, &options).unwrap(), expected_avg) {
            (Some(actual), Some(expected)) => assert!((actual - expected).abs() < 1e-10),
            (None, None) => {}
            (actual, expected) => panic!("avg mismatch: actual={actual:?}, expected={expected:?}"),
        }
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, expected_fast);
        assert_eq!(routes.walk_path, expected_walk);
    }

    fn assert_degrees_batch_routes(
        db: &DatabaseEngine,
        node_ids: &[u64],
        options: DegreeOptions,
        expected: &[(u64, u64)],
        expected_fast: usize,
        expected_walk: usize,
    ) {
        db.reset_degree_query_routes();
        let degrees = db.degrees(node_ids, &options).unwrap();
        assert_eq!(degrees.len(), expected.len());
        for &(node_id, degree) in expected {
            assert_eq!(degrees.get(&node_id), Some(&degree));
        }
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, expected_fast);
        assert_eq!(routes.walk_path, expected_walk);
    }

    fn assert_degree_family_fast_matches_forced_walk(
        db: &DatabaseEngine,
        node_id: u64,
        direction: Direction,
    ) {
        db.reset_degree_query_routes();
        let fast_options = DegreeOptions {
            direction,
            ..Default::default()
        };
        let fast_degree = db.degree(node_id, &fast_options).unwrap();
        let fast_sum = db.sum_edge_weights(node_id, &fast_options).unwrap();
        let fast_avg = db.avg_edge_weight(node_id, &fast_options).unwrap();
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, 3);
        assert_eq!(routes.walk_path, 0);

        db.reset_degree_query_routes();
        let walk_options = DegreeOptions {
            direction,
            at_epoch: Some(now_millis()),
            ..Default::default()
        };
        let walk_degree = db.degree(node_id, &walk_options).unwrap();
        let walk_sum = db.sum_edge_weights(node_id, &walk_options).unwrap();
        let walk_avg = db.avg_edge_weight(node_id, &walk_options).unwrap();
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, 0);
        assert_eq!(routes.walk_path, 3);

        assert_eq!(fast_degree, walk_degree);
        assert!((fast_sum - walk_sum).abs() < 1e-10);
        match (fast_avg, walk_avg) {
            (Some(fast), Some(walk)) => assert!((fast - walk).abs() < 1e-10),
            (None, None) => {}
            (fast, walk) => panic!("avg mismatch: fast={fast:?}, walk={walk:?}"),
        }
    }

    fn unique_node_count(node_ids: &[u64]) -> usize {
        let mut unique = node_ids.to_vec();
        unique.sort_unstable();
        unique.dedup();
        unique.len()
    }

    fn assert_degrees_batch_fast_matches_forced_walk(
        db: &DatabaseEngine,
        node_ids: &[u64],
        direction: Direction,
    ) {
        let unique_count = unique_node_count(node_ids);
        let fast_options = DegreeOptions {
            direction,
            ..Default::default()
        };
        db.reset_degree_query_routes();
        let fast = db.degrees(node_ids, &fast_options).unwrap();
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, unique_count);
        assert_eq!(routes.walk_path, 0);

        let walk_options = DegreeOptions {
            direction,
            at_epoch: Some(now_millis()),
            ..Default::default()
        };
        db.reset_degree_query_routes();
        let walk = db.degrees(node_ids, &walk_options).unwrap();
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, 0);
        assert_eq!(routes.walk_path, unique_count);

        assert_eq!(fast, walk);
    }

    fn assert_degree_family_all_directions_fast_match_walk(
        db: &DatabaseEngine,
        node_id: u64,
        batch_node_ids: &[u64],
    ) {
        for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            assert_degree_family_fast_matches_forced_walk(db, node_id, direction);
            assert_degrees_batch_fast_matches_forced_walk(db, batch_node_ids, direction);
        }
    }

    fn historical_temporal_edge(id: u64, from: u64, to: u64, weight: f32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            type_id: 10,
            props: std::collections::BTreeMap::new(),
            created_at: 1_000,
            updated_at: 1_500,
            weight,
            valid_from: 1_000,
            valid_to: 2_000,
            last_write_seq: 0,
        }
    }

    fn current_timeless_edge(id: u64, from: u64, to: u64, weight: f32) -> EdgeRecord {
        let now = now_millis();
        EdgeRecord {
            id,
            from,
            to,
            type_id: 10,
            props: std::collections::BTreeMap::new(),
            created_at: 1_000,
            updated_at: now,
            weight,
            valid_from: 1_000,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }

    #[test]
    fn test_degree_fast_path_scalar_route_matrix() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
        db.upsert_edge(
            a,
            c,
            20,
            UpsertEdgeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();

        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            2,
            5.0,
            Some(2.5),
            3,
            0,
        );
        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions {
                type_filter: Some(vec![10]),
                ..Default::default()
            },
            1,
            2.0,
            Some(2.0),
            0,
            3,
        );
        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions {
                at_epoch: Some(now_millis()),
                ..Default::default()
            },
            2,
            5.0,
            Some(2.5),
            0,
            3,
        );

        db.set_prune_policy(
            "active_but_no_match",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.0),
                type_id: None,
            },
        )
        .unwrap();
        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            2,
            5.0,
            Some(2.5),
            0,
            3,
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_fast_path_reads_frozen_overlay_while_flush_in_flight() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();

        db.freeze_memtable().unwrap();
        assert_eq!(db.immutable_epoch_count(), 1);

        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            1,
            2.0,
            Some(2.0),
            3,
            0,
        );

        let (ready_rx, release_tx) = db.set_flush_pause();
        db.enqueue_one_flush().unwrap();
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(db.in_flight_count(), 1);

        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            1,
            2.0,
            Some(2.0),
            3,
            0,
        );

        release_tx.send(()).unwrap();
        db.wait_one_flush().unwrap();
        assert_eq!(db.immutable_epoch_count(), 0);
        db.close().unwrap();
    }

    #[test]
    fn test_degree_fast_path_walks_when_sidecar_unavailable_or_temporal() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(
                a,
                b,
                10,
                UpsertEdgeOptions {
                    weight: 2.0,
                    ..Default::default()
                },
            )
            .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }
        std::fs::remove_file(segment_dir(&path, 1).join(crate::degree_cache::DEGREE_DELTA_FILENAME))
            .unwrap();
        let db = open_imm(&path);
        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            1,
            2.0,
            Some(2.0),
            0,
            3,
        );
        db.close().unwrap();

        let temporal_dir = TempDir::new().unwrap();
        let temporal = open_imm(&temporal_dir.path().join("db"));
        let x = temporal
            .upsert_node(1, "x", UpsertNodeOptions::default())
            .unwrap();
        let y = temporal
            .upsert_node(1, "y", UpsertNodeOptions::default())
            .unwrap();
        temporal
            .upsert_edge(
                x,
                y,
                10,
                UpsertEdgeOptions {
                    weight: 4.0,
                    valid_to: Some(now_millis() + 10_000),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_scalar_degree_family_routes(
            &temporal,
            x,
            DegreeOptions::default(),
            1,
            4.0,
            Some(4.0),
            0,
            3,
        );
        temporal.close().unwrap();
    }

    #[test]
    fn test_degree_fast_path_deleting_expired_temporal_edge_reverses_original_delta() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            db.write_op(&WalOp::UpsertEdge(historical_temporal_edge(42, a, b, 2.0)))
                .unwrap();

            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                0,
                0.0,
                None,
                0,
                3,
            );

            db.flush().unwrap();
            db.write_op(&WalOp::DeleteEdge {
                id: 42,
                deleted_at: now_millis(),
            })
            .unwrap();

            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                0,
                0.0,
                None,
                3,
                0,
            );

            db.flush().unwrap();
            db.close().unwrap();
        }

        let reopened = open_imm(&path);
        assert_scalar_degree_family_routes(
            &reopened,
            a,
            DegreeOptions::default(),
            0,
            0.0,
            None,
            3,
            0,
        );
        reopened.close().unwrap();
    }

    #[test]
    fn test_degree_fast_path_updating_expired_temporal_edge_reverses_original_delta() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            db.write_op(&WalOp::UpsertEdge(historical_temporal_edge(42, a, b, 2.0)))
                .unwrap();
            db.flush().unwrap();

            db.write_op(&WalOp::UpsertEdge(current_timeless_edge(42, a, b, 5.0)))
                .unwrap();

            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                1,
                5.0,
                Some(5.0),
                3,
                0,
            );

            db.flush().unwrap();
            db.close().unwrap();
        }

        let reopened = open_imm(&path);
        assert_scalar_degree_family_routes(
            &reopened,
            a,
            DegreeOptions::default(),
            1,
            5.0,
            Some(5.0),
            3,
            0,
        );
        reopened.close().unwrap();
    }

    #[test]
    fn test_degrees_fast_path_batch_routes_all_fast_all_walk_and_mixed() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.upsert_edge(a, c, 20, UpsertEdgeOptions::default())
            .unwrap();

        db.reset_degree_query_routes();
        let degrees = db
            .degrees(&[c, a, b, a], &DegreeOptions::default())
            .unwrap();
        assert_eq!(degrees.get(&a), Some(&2));
        assert!(!degrees.contains_key(&b));
        assert!(!degrees.contains_key(&c));
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, 3);
        assert_eq!(routes.walk_path, 0);

        db.reset_degree_query_routes();
        let degrees = db
            .degrees(
                &[a, b, c],
                &DegreeOptions {
                    type_filter: Some(vec![10]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(degrees.get(&a), Some(&1));
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, 0);
        assert_eq!(routes.walk_path, 3);

        db.upsert_edge(
            b,
            c,
            10,
            UpsertEdgeOptions {
                valid_to: Some(now_millis() + 10_000),
                ..Default::default()
            },
        )
        .unwrap();
        db.reset_degree_query_routes();
        let degrees = db.degrees(&[a, b, c], &DegreeOptions::default()).unwrap();
        assert_eq!(degrees.get(&a), Some(&2));
        assert_eq!(degrees.get(&b), Some(&1));
        assert!(!degrees.contains_key(&c));
        let routes = db.degree_query_route_snapshot();
        assert_eq!(routes.fast_path, 1);
        assert_eq!(routes.walk_path, 2);

        db.close().unwrap();
    }

    #[test]
    fn test_degree_fast_path_matches_forced_walk_across_source_states_and_directions() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        let b;
        let c;
        let d;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(
                a,
                b,
                10,
                UpsertEdgeOptions {
                    weight: 2.0,
                    ..Default::default()
                },
            )
            .unwrap();
            db.upsert_edge(
                c,
                a,
                10,
                UpsertEdgeOptions {
                    weight: 6.0,
                    ..Default::default()
                },
            )
            .unwrap();
            db.upsert_edge(
                a,
                a,
                10,
                UpsertEdgeOptions {
                    weight: 4.0,
                    ..Default::default()
                },
            )
            .unwrap();

            let initial_batch = [a, b, c, a];
            assert_degree_family_all_directions_fast_match_walk(&db, a, &initial_batch);

            db.freeze_memtable().unwrap();
            assert_eq!(db.immutable_epoch_count(), 1);
            assert_degree_family_all_directions_fast_match_walk(&db, a, &initial_batch);

            db.flush().unwrap();
            assert_eq!(db.segments_for_test().len(), 1);
            assert_degree_family_all_directions_fast_match_walk(&db, a, &initial_batch);

            d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(
                a,
                d,
                10,
                UpsertEdgeOptions {
                    weight: 8.0,
                    ..Default::default()
                },
            )
            .unwrap();
            let full_batch = [a, b, c, d, a];
            assert_degree_family_all_directions_fast_match_walk(&db, a, &full_batch);

            db.flush().unwrap();
            assert_eq!(db.segments_for_test().len(), 2);
            assert_degree_family_all_directions_fast_match_walk(&db, a, &full_batch);

            db.compact().unwrap();
            assert_eq!(db.segments_for_test().len(), 1);
            assert_degree_family_all_directions_fast_match_walk(&db, a, &full_batch);

            db.close().unwrap();
        }

        let reopened = open_imm(&path);
        let full_batch = [a, b, c, d, a];
        assert_degree_family_all_directions_fast_match_walk(&reopened, a, &full_batch);
        reopened.close().unwrap();
    }

    #[test]
    fn test_degree_fast_path_parity_across_sources_flush_compact_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

            db.upsert_edge(
                a,
                b,
                10,
                UpsertEdgeOptions {
                    weight: 2.0,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                1,
                2.0,
                Some(2.0),
                3,
                0,
            );

            db.flush().unwrap();
            db.upsert_edge(
                a,
                c,
                10,
                UpsertEdgeOptions {
                    weight: 3.0,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                2,
                5.0,
                Some(2.5),
                3,
                0,
            );

            db.flush().unwrap();
            db.upsert_edge(
                a,
                d,
                10,
                UpsertEdgeOptions {
                    weight: 4.0,
                    ..Default::default()
                },
            )
            .unwrap();
            db.flush().unwrap();
            assert_eq!(db.segments_for_test().len(), 3);
            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                3,
                9.0,
                Some(3.0),
                3,
                0,
            );

            db.compact().unwrap();
            assert_eq!(db.segments_for_test().len(), 1);
            assert_scalar_degree_family_routes(
                &db,
                a,
                DegreeOptions::default(),
                3,
                9.0,
                Some(3.0),
                3,
                0,
            );

            db.close().unwrap();
        }

        let reopened = open_imm(&path);
        assert_scalar_degree_family_routes(
            &reopened,
            a,
            DegreeOptions::default(),
            3,
            9.0,
            Some(3.0),
            3,
            0,
        );
        reopened.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_missing_fallback_is_degree_only_and_no_backfill() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        let b;
        let c;
        let edge_ab;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            edge_ab = db
                .upsert_edge(
                    a,
                    b,
                    10,
                    UpsertEdgeOptions {
                        weight: 2.0,
                        ..Default::default()
                    },
                )
                .unwrap();
            db.upsert_edge(
                a,
                c,
                10,
                UpsertEdgeOptions {
                    weight: 3.0,
                    ..Default::default()
                },
            )
            .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        let sidecar_path =
            segment_dir(&path, 1).join(crate::degree_cache::DEGREE_DELTA_FILENAME);
        assert!(sidecar_path.exists());
        std::fs::remove_file(&sidecar_path).unwrap();

        let db = open_imm(&path);
        assert!(!sidecar_path.exists());
        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(!segments[0].degree_delta_available());

        assert!(db.get_node(a).unwrap().is_some());
        assert!(db.get_edge(edge_ab).unwrap().is_some());
        assert_eq!(
            db.neighbors(
                a,
                &NeighborOptions {
                    direction: Direction::Outgoing,
                    ..Default::default()
                },
            )
            .unwrap()
            .len(),
            2
        );

        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            2,
            5.0,
            Some(2.5),
            0,
            3,
        );
        assert_degrees_batch_routes(
            &db,
            &[c, a, b, a],
            DegreeOptions::default(),
            &[(a, 2)],
            0,
            3,
        );
        assert!(!sidecar_path.exists(), "open/query must not backfill a missing degree sidecar");

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_corrupt_fallback_is_degree_only_and_no_repair() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        let b;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(
                a,
                b,
                10,
                UpsertEdgeOptions {
                    weight: 4.0,
                    ..Default::default()
                },
            )
            .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        let sidecar_path =
            segment_dir(&path, 1).join(crate::degree_cache::DEGREE_DELTA_FILENAME);
        std::fs::write(&sidecar_path, b"not a degree sidecar").unwrap();

        let db = open_imm(&path);
        assert!(sidecar_path.exists());
        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(!segments[0].degree_delta_available());

        assert!(db.get_node(b).unwrap().is_some());
        assert_eq!(
            db.neighbors(
                a,
                &NeighborOptions {
                    direction: Direction::Outgoing,
                    ..Default::default()
                },
            )
            .unwrap()
            .len(),
            1
        );

        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            1,
            4.0,
            Some(4.0),
            0,
            3,
        );
        assert_degrees_batch_routes(
            &db,
            &[a, b, a],
            DegreeOptions::default(),
            &[(a, 1)],
            0,
            2,
        );
        assert_eq!(
            std::fs::read(&sidecar_path).unwrap(),
            b"not a degree sidecar",
            "open/query must not repair a corrupt degree sidecar"
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_sidecar_compaction_omits_when_input_corrupt() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let a;
        {
            let db = open_imm(&path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
                .unwrap();
            db.flush().unwrap();
            db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
                .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        std::fs::write(
            segment_dir(&path, 1).join(crate::degree_cache::DEGREE_DELTA_FILENAME),
            b"not a degree sidecar",
        )
        .unwrap();

        let db = open_imm(&path);
        assert!(db
            .segments_for_test()
            .iter()
            .any(|segment| !segment.degree_delta_available()));
        db.compact().unwrap();

        let segments = db.segments_for_test();
        assert_eq!(segments.len(), 1);
        assert!(!segments[0].degree_delta_available());
        assert_scalar_degree_family_routes(
            &db,
            a,
            DegreeOptions::default(),
            2,
            2.0,
            Some(1.0),
            0,
            3,
        );

        db.close().unwrap();
    }

    #[test]
    fn test_degree_cache_batch_matches_individual() {
        // Verify batch degrees() from cache matches individual degree() calls
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let mut nodes = Vec::new();
        for i in 0..5 {
            nodes.push(db.upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default()).unwrap());
        }

        // Star topology: 0 → 1,2,3,4
        for i in 1..5 {
            db.upsert_edge(nodes[0], nodes[i], 10, UpsertEdgeOptions::default()).unwrap();
        }
        // Self-loop on node 2
        db.upsert_edge(nodes[2], nodes[2], 10, UpsertEdgeOptions { weight: 2.0, ..Default::default() }).unwrap();

        let batch = db.degrees(&nodes, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();

        for &nid in &nodes {
            let individual = db.degree(nid, &DegreeOptions { direction: Direction::Both, ..Default::default() }).unwrap();
            let batch_deg = batch.get(&nid).copied().unwrap_or(0);
            assert_eq!(individual, batch_deg,
                "batch vs individual mismatch for node {}", nid);
        }

        db.close().unwrap();
    }

    // --- Phase 18d: Connected Components (WCC) ---

    #[test]
    fn test_wcc_single_component() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        // A - B - C (all connected via directed edges, WCC ignores direction)
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 3);
        // Component ID = min(node_id) = a
        let comp_id = comps[&a];
        assert_eq!(comp_id, a);
        assert_eq!(comps[&b], comp_id);
        assert_eq!(comps[&c], comp_id);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_multiple_components() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        // Component 1: A - B
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();

        // Component 2: C - D - E
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e = db.upsert_node(1, "e", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(d, e, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 5);
        assert_eq!(comps[&a], a); // min(a, b) = a
        assert_eq!(comps[&b], a);
        assert_eq!(comps[&c], c); // min(c, d, e) = c
        assert_eq!(comps[&d], c);
        assert_eq!(comps[&e], c);

        // Two distinct component IDs
        let unique_comps: NodeIdSet = comps.values().copied().collect();
        assert_eq!(unique_comps.len(), 2);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_isolated_nodes() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // No edges. Each node is its own component.

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 3);
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], b);
        assert_eq!(comps[&c], c);

        let unique_comps: NodeIdSet = comps.values().copied().collect();
        assert_eq!(unique_comps.len(), 3);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_self_loops() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        // Self-loop on A, no connection to B.
        db.upsert_edge(a, a, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 2);
        assert_eq!(comps[&a], a); // self-loop doesn't change membership
        assert_eq!(comps[&b], b);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_parallel_edges() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // Multiple edges between A and B shouldn't create multiple links.
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(a, b, 20, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, a, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 3);
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], a); // same component
        assert_eq!(comps[&c], c); // isolated

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_deleted_nodes_and_edges() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        // A - B - C
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let e1 = db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        // Delete edge A-B: now A is isolated, B-C connected.
        db.delete_edge(e1).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps[&a], a); // isolated
        assert_ne!(comps[&a], comps[&b]); // different component
        assert_eq!(comps[&b], comps[&c]); // still connected

        // Delete node B: now B is gone, C is isolated.
        db.delete_node(b).unwrap();

        let comps2 = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps2.len(), 2); // only A and C
        assert!(!comps2.contains_key(&b));
        assert_eq!(comps2[&a], a);
        assert_eq!(comps2[&c], c);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_edge_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // Edge type 10: A - B
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        // Edge type 20: B - C
        db.upsert_edge(b, c, 20, UpsertEdgeOptions::default()).unwrap();

        // Filter by type 10 only: only A-B connected.
        let comps = db.connected_components(&ComponentOptions { edge_type_filter: Some(vec![10]), ..Default::default() }).unwrap();
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], a);
        assert_eq!(comps[&c], c); // isolated when type 20 excluded

        // Filter by type 20 only: only B-C connected.
        let comps2 = db.connected_components(&ComponentOptions { edge_type_filter: Some(vec![20]), ..Default::default() }).unwrap();
        assert_eq!(comps2[&a], a); // isolated when type 10 excluded
        assert_eq!(comps2[&b], comps2[&c]); // connected

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_node_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        // Type 1: A, B.  Type 2: C
        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(2, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        // Filter by node type 1: only A and B visible.
        let comps = db.connected_components(&ComponentOptions { node_type_filter: Some(vec![1]), ..Default::default() }).unwrap();
        assert_eq!(comps.len(), 2);
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], a);
        assert!(!comps.contains_key(&c));

        // Filter by node type 2: only C visible, isolated.
        let comps2 = db.connected_components(&ComponentOptions { node_type_filter: Some(vec![2]), ..Default::default() }).unwrap();
        assert_eq!(comps2.len(), 1);
        assert_eq!(comps2[&c], c);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_after_flush() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();

        db.flush().unwrap();

        // Add more data in memtable after flush.
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        // All three in one component (segment edge + memtable edge).
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], a);
        assert_eq!(comps[&c], a);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_after_compaction() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], a);
        assert_eq!(comps[&c], a);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_after_close_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        {
            let db = open_imm(&db_path);
            let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        let db = open_imm(&db_path);
        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 3);
        // All in one component.
        let unique_comps: NodeIdSet = comps.values().copied().collect();
        assert_eq!(unique_comps.len(), 1);
    }

    #[test]
    fn test_wcc_empty_graph() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert!(comps.is_empty());
    }

    #[test]
    fn test_wcc_direction_ignored() {
        // Directed edge A→B should put A and B in the same WCC component.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        // Only directed edges: A→B, C→B
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(c, b, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        // All three in one component (WCC ignores direction).
        let comp = comps[&a];
        assert_eq!(comps[&b], comp);
        assert_eq!(comps[&c], comp);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_deterministic_component_ids() {
        // Component IDs must be deterministic (min node_id).
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(c, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, a, 10, UpsertEdgeOptions::default()).unwrap();

        // Run twice. Must produce identical results.
        let comps1 = db.connected_components(&ComponentOptions::default()).unwrap();
        let comps2 = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps1, comps2);
        // Component ID = min(a, b, c) = a.
        assert_eq!(comps1[&a], a);
        assert_eq!(comps1[&b], a);
        assert_eq!(comps1[&c], a);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_prune_policy_hides_nodes() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.1, ..Default::default() }).unwrap(); // low weight
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        // Register prune policy that hides weight <= 0.5.
        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        ).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        // B is hidden by prune policy, so A and C are each isolated.
        assert_eq!(comps.len(), 2);
        assert!(!comps.contains_key(&b));
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&c], c);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_memtable_only() {
        // WCC should work with memtable-only data (no flush).
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps.len(), 4);
        assert_eq!(comps[&a], a);
        assert_eq!(comps[&b], a);
        assert_eq!(comps[&c], c);
        assert_eq!(comps[&d], c);

        db.close().unwrap();
    }

    // --- Phase 18d: component_of ---

    #[test]
    fn test_component_of_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
        // D is isolated.

        let mut members = db.component_of(a, &ComponentOptions::default()).unwrap();
        members.sort_unstable();
        assert_eq!(members, vec![a, b, c]);

        let mut members_b = db.component_of(b, &ComponentOptions::default()).unwrap();
        members_b.sort_unstable();
        assert_eq!(members_b, vec![a, b, c]);

        let members_d = db.component_of(d, &ComponentOptions::default()).unwrap();
        assert_eq!(members_d, vec![d]);

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_missing_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();

        let members = db.component_of(99999, &ComponentOptions::default()).unwrap();
        assert!(members.is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_deleted_node() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.delete_node(a).unwrap();

        let members = db.component_of(a, &ComponentOptions::default()).unwrap();
        assert!(members.is_empty());

        // B is now isolated (its edge to A is broken).
        let members_b = db.component_of(b, &ComponentOptions::default()).unwrap();
        assert_eq!(members_b, vec![b]);

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_node_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(2, "b", UpsertNodeOptions::default()).unwrap(); // type 2
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        // Filter by type 1: B (type 2) is invisible, so A and C are isolated.
        let members = db.component_of(a, &ComponentOptions { node_type_filter: Some(vec![1]), ..Default::default() }).unwrap();
        assert_eq!(members, vec![a]);

        // Start from type 2 node with type 1 filter → empty.
        let members_b = db.component_of(b, &ComponentOptions { node_type_filter: Some(vec![1]), ..Default::default() }).unwrap();
        assert!(members_b.is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_edge_type_filter() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 20, UpsertEdgeOptions::default()).unwrap();

        // Filter by edge type 10: only A-B connected.
        let members = db.component_of(a, &ComponentOptions { edge_type_filter: Some(vec![10]), ..Default::default() }).unwrap();
        assert_eq!(members, vec![a, b]);

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_after_flush() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();

        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        let members = db.component_of(a, &ComponentOptions::default()).unwrap();
        assert_eq!(members, vec![a, b, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_after_compaction() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();

        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let members = db.component_of(c, &ComponentOptions::default()).unwrap();
        assert_eq!(members, vec![a, b, c]);

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_close_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let a;
        let b;
        let c;

        {
            let db = open_imm(&db_path);
            a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
            b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
            c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
            db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
            db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        let db = open_imm(&db_path);
        let members = db.component_of(b, &ComponentOptions::default()).unwrap();
        assert_eq!(members, vec![a, b, c]);
    }

    #[test]
    fn test_component_of_prune_policy() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions { weight: 0.1, ..Default::default() }).unwrap(); // low weight
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        ).unwrap();

        // B is hidden → A and C are each isolated.
        let members_a = db.component_of(a, &ComponentOptions::default()).unwrap();
        assert_eq!(members_a, vec![a]);

        let members_c = db.component_of(c, &ComponentOptions::default()).unwrap();
        assert_eq!(members_c, vec![c]);

        // B is hidden → empty result.
        let members_b = db.component_of(b, &ComponentOptions::default()).unwrap();
        assert!(members_b.is_empty());

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_agrees_with_component_of() {
        // WCC and component_of must produce consistent results.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        let e = db.upsert_node(1, "e", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(d, e, 10, UpsertEdgeOptions::default()).unwrap();

        let comps = db.connected_components(&ComponentOptions::default()).unwrap();

        // For each node, component_of should return the same set.
        for &node in &[a, b, c, d, e] {
            let members = db.component_of(node, &ComponentOptions::default()).unwrap();
            let comp_id = comps[&node];
            // All members should have the same component ID in comps.
            for &member in &members {
                assert_eq!(comps[&member], comp_id,
                    "component_of({}) member {} has different comp ID", node, member);
            }
            // Number of nodes with this component ID should match members length.
            let wcc_count = comps.values().filter(|&&v| v == comp_id).count();
            assert_eq!(wcc_count, members.len(),
                "component_of({}) size mismatch with WCC", node);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_agrees_with_component_of_filtered() {
        // Cross-consistency under both node_type_filter and edge_type_filter.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(2, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(b, c, 20, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(c, d, 10, UpsertEdgeOptions::default()).unwrap();
        db.upsert_edge(a, d, 10, UpsertEdgeOptions::default()).unwrap();

        // Node type filter = [1], edge type filter = [10]
        let ntf = Some(&[1u32][..]);
        let etf = Some(&[10u32][..]);

        let comps = db.connected_components(&ComponentOptions { edge_type_filter: etf.map(|s| s.to_vec()), node_type_filter: ntf.map(|s| s.to_vec()), ..Default::default() }).unwrap();
        for &node in &[a, c, d] {
            let members = db.component_of(node, &ComponentOptions { edge_type_filter: etf.map(|s| s.to_vec()), node_type_filter: ntf.map(|s| s.to_vec()), ..Default::default() }).unwrap();
            let comp_id = comps[&node];
            for &member in &members {
                assert_eq!(comps[&member], comp_id,
                    "filtered component_of({}) member {} disagrees with WCC", node, member);
            }
            let wcc_count = comps.values().filter(|&&v| v == comp_id).count();
            assert_eq!(wcc_count, members.len(),
                "filtered component_of({}) size mismatch with WCC", node);
        }

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_self_loop() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, a, 10, UpsertEdgeOptions::default()).unwrap();

        let members = db.component_of(a, &ComponentOptions::default()).unwrap();
        assert_eq!(members, vec![a]);

        db.close().unwrap();
    }

    #[test]
    fn test_component_of_undirected_reachability() {
        // If A→B exists (directed), component_of(B) should find A
        // because WCC treats edges as undirected.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default()).unwrap();

        // Starting from B, should still find A (Direction::Both in BFS).
        let members = db.component_of(b, &ComponentOptions::default()).unwrap();
        assert_eq!(members, vec![a, b]);

        db.close().unwrap();
    }

    #[test]
    fn test_wcc_at_epoch_temporal_filtering() {
        // Expired and not-yet-valid edges should be invisible to WCC.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();
        let d = db.upsert_node(1, "d", UpsertNodeOptions::default()).unwrap();

        let now = now_millis();

        // A→B: valid window [0, 1000). Expired at `now` but valid at epoch 500.
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(1000), ..Default::default() }).unwrap();
        // B→C: always valid
        db.upsert_edge(b, c, 10, UpsertEdgeOptions::default()).unwrap();
        // C→D: not yet valid (valid_from far in the future)
        let future = now + 100_000_000;
        db.upsert_edge(c, d, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(future), valid_to: None, ..Default::default() }).unwrap();

        // With at_epoch=None (defaults to now):
        //   A→B expired → A isolated
        //   B→C valid → B and C connected
        //   C→D future → D isolated
        let comps = db.connected_components(&ComponentOptions::default()).unwrap();
        assert_eq!(comps[&a], a, "A should be isolated (expired edge)");
        assert_eq!(comps[&b], comps[&c], "B and C should be connected");
        assert_ne!(comps[&a], comps[&b], "A should not be in B-C component");
        assert_eq!(comps[&d], d, "D should be isolated (future edge)");

        // With at_epoch = 500 (inside the A→B validity window):
        //   A→B valid → connected
        //   B→C valid (valid_from defaults to created_at which is ≤ now, valid at 500
        //     only if valid_from ≤ 500, but valid_from=now which is >> 500)
        //   So at epoch 500, B→C is NOT valid because its valid_from = now >> 500.
        //   Use explicit valid_from=0 for the B→C edge to test properly.
        db.close().unwrap();

        // Rebuild with explicit temporal windows for deterministic testing.
        let dir2 = TempDir::new().unwrap();
        let db2 = open_imm(&dir2.path().join("db"));

        let a2 = db2.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b2 = db2.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c2 = db2.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // A→B: valid [0, 1000)
        db2.upsert_edge(a2, b2, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(1000), ..Default::default() }).unwrap();
        // B→C: valid [0, i64::MAX)
        db2.upsert_edge(b2, c2, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: None, ..Default::default() }).unwrap();

        // at_epoch=500: both edges valid → all connected
        let comps_t500 = db2.connected_components(&ComponentOptions { at_epoch: Some(500), ..Default::default() }).unwrap();
        assert_eq!(comps_t500[&a2], comps_t500[&b2], "A-B connected at epoch 500");
        assert_eq!(comps_t500[&b2], comps_t500[&c2], "B-C connected at epoch 500");

        // at_epoch=2000: A→B expired → A isolated, B-C connected
        let comps_t2000 = db2.connected_components(&ComponentOptions { at_epoch: Some(2000), ..Default::default() }).unwrap();
        assert_eq!(comps_t2000[&a2], a2, "A isolated at epoch 2000");
        assert_eq!(comps_t2000[&b2], comps_t2000[&c2], "B-C connected at epoch 2000");
        assert_ne!(comps_t2000[&a2], comps_t2000[&b2], "A not in B-C component at epoch 2000");

        db2.close().unwrap();
    }

    #[test]
    fn test_component_of_at_epoch_temporal_filtering() {
        // component_of should respect at_epoch for edge visibility.
        let dir = TempDir::new().unwrap();
        let db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", UpsertNodeOptions::default()).unwrap();
        let b = db.upsert_node(1, "b", UpsertNodeOptions::default()).unwrap();
        let c = db.upsert_node(1, "c", UpsertNodeOptions::default()).unwrap();

        // A→B: valid [0, 1000)
        db.upsert_edge(a, b, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: Some(1000), ..Default::default() }).unwrap();
        // B→C: always valid from epoch 0
        db.upsert_edge(b, c, 10, UpsertEdgeOptions { weight: 1.0, valid_from: Some(0), valid_to: None, ..Default::default() }).unwrap();

        // at_epoch=2000: A→B expired → A alone, B-C together.
        let members_a = db.component_of(a, &ComponentOptions { at_epoch: Some(2000), ..Default::default() }).unwrap();
        assert_eq!(members_a, vec![a], "A isolated at epoch 2000");

        let mut members_b = db.component_of(b, &ComponentOptions { at_epoch: Some(2000), ..Default::default() }).unwrap();
        members_b.sort_unstable();
        assert_eq!(members_b, vec![b, c], "B-C connected at epoch 2000");

        // at_epoch=500: A→B valid → all three connected.
        let mut members_a_t500 = db.component_of(a, &ComponentOptions { at_epoch: Some(500), ..Default::default() }).unwrap();
        members_a_t500.sort_unstable();
        assert_eq!(members_a_t500, vec![a, b, c], "A-B-C connected at epoch 500");

        db.close().unwrap();
    }
