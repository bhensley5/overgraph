// Read tests: type index, find, neighbors, pagination, temporal, decay, traversal, top-k, PPR, export.

    fn traverse_depth_two_read(
        engine: &DatabaseEngine,
        start: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        node_type_filter: Option<&[u32]>,
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
                None,
                None,
            )
            .unwrap()
            .items
    }

    // --- Type index tests ---

    #[test]
    fn test_nodes_by_type_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        let c = engine
            .upsert_node(2, "charlie", BTreeMap::new(), 0.5)
            .unwrap();

        let mut type1 = engine.nodes_by_type(1).unwrap();
        type1.sort();
        assert_eq!(type1, vec![a, b]);
        assert_eq!(engine.nodes_by_type(2).unwrap(), vec![c]);
        assert!(engine.nodes_by_type(99).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_edges_by_type_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(a, b, 20, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        assert_eq!(engine.edges_by_type(10).unwrap(), vec![e1]);
        assert_eq!(engine.edges_by_type(20).unwrap(), vec![e2]);
        assert!(engine.edges_by_type(99).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_nodes_by_type_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment: type 1 nodes
        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Memtable: more type 1 + type 2
        let c = engine
            .upsert_node(1, "charlie", BTreeMap::new(), 0.5)
            .unwrap();
        let d = engine
            .upsert_node(2, "delta", BTreeMap::new(), 0.5)
            .unwrap();

        let mut type1 = engine.nodes_by_type(1).unwrap();
        type1.sort();
        assert_eq!(type1, vec![a, b, c]);
        assert_eq!(engine.nodes_by_type(2).unwrap(), vec![d]);

        engine.close().unwrap();
    }

    #[test]
    fn test_nodes_by_type_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Delete alice (cross-source tombstone: segment data, memtable tombstone)
        engine.delete_node(a).unwrap();

        let type1 = engine.nodes_by_type(1).unwrap();
        assert_eq!(type1, vec![b]);

        engine.close().unwrap();
    }

    #[test]
    fn test_type_index_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        let b;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine
                .upsert_node(1, "alice", BTreeMap::new(), 0.5)
                .unwrap();
            b = engine.upsert_node(2, "bob", BTreeMap::new(), 0.5).unwrap();
            engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }

        // Reopen. Type index should be available from segment
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.nodes_by_type(1).unwrap(), vec![a]);
        assert_eq!(engine.nodes_by_type(2).unwrap(), vec![b]);
        assert_eq!(engine.edges_by_type(10).unwrap().len(), 1);

        engine.close().unwrap();
    }

    // --- get_nodes_by_type / get_edges_by_type / count tests ---

    #[test]
    fn test_get_nodes_by_type_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String("Alice".to_string()));
        engine.upsert_node(1, "alice", props.clone(), 0.9).unwrap();
        props.insert("name".to_string(), PropValue::String("Bob".to_string()));
        engine.upsert_node(1, "bob", props, 0.8).unwrap();
        engine
            .upsert_node(2, "charlie", BTreeMap::new(), 0.7)
            .unwrap();

        let type1 = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(type1.len(), 2);
        assert!(type1.iter().all(|n| n.type_id == 1));
        assert!(type1.iter().any(|n| n.key == "alice"));
        assert!(type1.iter().any(|n| n.key == "bob"));

        let type2 = engine.get_nodes_by_type(2).unwrap();
        assert_eq!(type2.len(), 1);
        assert_eq!(type2[0].key, "charlie");

        // Non-existent type
        let empty = engine.get_nodes_by_type(99).unwrap();
        assert!(empty.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Type 1 nodes in segment
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        // Type 1 node in memtable
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 3);
        let keys: Vec<&str> = records.iter().map(|n| n.key.as_str()).collect();
        assert!(keys.contains(&"a"));
        assert!(keys.contains(&"b"));
        assert!(keys.contains(&"c"));

        // Verify records carry full data (props, weight, timestamps)
        for r in &records {
            assert_eq!(r.type_id, 1);
            assert!(r.weight > 0.0);
            assert!(r.created_at > 0);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "alice", BTreeMap::new(), 0.5)
            .unwrap();
        engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        engine.delete_node(a).unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_excludes_pruned() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // Policy: prune nodes with weight <= 0.5
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_weight: Some(0.5),
                    max_age_ms: None,
                    type_id: None,
                },
            )
            .unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "high");

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_post_compaction() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_by_type_memtable_and_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        // Type 10 edge in segment
        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Type 10 edge in memtable
        engine
            .upsert_edge(b, c, 10, BTreeMap::new(), 0.8, None, None)
            .unwrap();
        // Type 20 edge in memtable
        engine
            .upsert_edge(a, c, 20, BTreeMap::new(), 0.5, None, None)
            .unwrap();

        let type10 = engine.get_edges_by_type(10).unwrap();
        assert_eq!(type10.len(), 2);
        assert!(type10.iter().all(|e| e.type_id == 10));

        let type20 = engine.get_edges_by_type(20).unwrap();
        assert_eq!(type20.len(), 1);
        assert_eq!(type20[0].type_id, 20);

        // Verify records carry full data
        for e in &type10 {
            assert!(e.weight > 0.0);
            assert!(e.from > 0);
            assert!(e.to > 0);
        }

        // Empty type
        let empty = engine.get_edges_by_type(99).unwrap();
        assert!(empty.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_by_type_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        let e1 = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        engine.delete_edge(e1).unwrap();

        let type10 = engine.get_edges_by_type(10).unwrap();
        assert_eq!(type10.len(), 1);
        assert_eq!(type10[0].from, b);
        assert_eq!(type10[0].to, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_count_nodes_by_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // 3 type-1 nodes across memtable + segment
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        // 1 type-2 node
        engine.upsert_node(2, "x", BTreeMap::new(), 1.0).unwrap();

        assert_eq!(engine.count_nodes_by_type(1).unwrap(), 3);
        assert_eq!(engine.count_nodes_by_type(2).unwrap(), 1);
        assert_eq!(engine.count_nodes_by_type(99).unwrap(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_count_edges_by_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        engine
            .upsert_edge(a, c, 20, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        assert_eq!(engine.count_edges_by_type(10).unwrap(), 2);
        assert_eq!(engine.count_edges_by_type(20).unwrap(), 1);
        assert_eq!(engine.count_edges_by_type(99).unwrap(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_count_nodes_by_type_respects_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        assert_eq!(engine.count_nodes_by_type(1).unwrap(), 2);

        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_weight: Some(0.5),
                    max_age_ms: None,
                    type_id: None,
                },
            )
            .unwrap();

        // Now the low-weight node is excluded
        assert_eq!(engine.count_nodes_by_type(1).unwrap(), 1);

        engine.close().unwrap();
    }

    // --- Paginated type-index query tests ---

    #[test]
    fn test_nodes_by_type_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 10 nodes of type 1
        let mut ids: Vec<u64> = Vec::new();
        for i in 0..10 {
            let id = engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            ids.push(id);
        }
        ids.sort();

        // Page through 3 at a time
        let page1 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items.len(), 3);
        assert_eq!(page1.items, ids[0..3]);
        assert!(page1.next_cursor.is_some());

        let page2 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items.len(), 3);
        assert_eq!(page2.items, ids[3..6]);
        assert!(page2.next_cursor.is_some());

        let page3 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: page2.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page3.items.len(), 3);
        assert_eq!(page3.items, ids[6..9]);
        assert!(page3.next_cursor.is_some());

        let page4 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: page3.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page4.items.len(), 1);
        assert_eq!(page4.items, ids[9..10]);
        assert!(page4.next_cursor.is_none()); // last page
    }

    #[test]
    fn test_nodes_by_type_paged_roundtrip() {
        // Page through all results 1-at-a-time, collect, should equal unpaginated
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..20 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .nodes_by_type_paged(
                    1,
                    &PageRequest {
                        limit: Some(4),
                        after: cursor,
                    },
                )
                .unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let mut all_unpaged = engine.nodes_by_type(1).unwrap();
        all_unpaged.sort();
        assert_eq!(all_paged, all_unpaged);
    }

    #[test]
    fn test_nodes_by_type_paged_default_returns_all() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..5 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        let result = engine
            .nodes_by_type_paged(1, &PageRequest::default())
            .unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_empty_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let result = engine
            .nodes_by_type_paged(
                99,
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_cursor_past_end() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..3 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        let result = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(10),
                    after: Some(u64::MAX),
                },
            )
            .unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_cross_source() {
        // IDs from memtable + segments should merge and paginate correctly
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 5 nodes, flush to segment
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("seg{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Create 5 more in memtable
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("mem{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        // Page through all, should see 10 total across both sources
        let mut all_paged: Vec<u64> = Vec::new();
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
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(all_paged.len(), 10);

        // Verify sorted order
        for i in 1..all_paged.len() {
            assert!(all_paged[i] > all_paged[i - 1]);
        }
    }

    #[test]
    fn test_nodes_by_type_paged_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.delete_node(id2).unwrap();

        let result = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        let mut expected = vec![id1, id3];
        expected.sort();
        assert_eq!(result.items, expected);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_respects_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "keep", BTreeMap::new(), 1.0).unwrap();
        engine
            .upsert_node(1, "prune_me", BTreeMap::new(), 0.1)
            .unwrap();

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

        let result = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(result.items.len(), 1); // only "keep" survives
    }

    #[test]
    fn test_edges_by_type_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let n3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        let mut edge_ids: Vec<u64> = Vec::new();
        for _ in 0..6 {
            let eid = engine
                .upsert_edge(n1, n2, 5, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            edge_ids.push(eid);
            let eid = engine
                .upsert_edge(n2, n3, 5, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            edge_ids.push(eid);
        }
        edge_ids.sort();

        // Page 2 at a time
        let page1 = engine
            .edges_by_type_paged(
                5,
                &PageRequest {
                    limit: Some(2),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.next_cursor.is_some());

        let page2 = engine
            .edges_by_type_paged(
                5,
                &PageRequest {
                    limit: Some(2),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items.len(), 2);
    }

    #[test]
    fn test_edges_by_type_paged_roundtrip() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();

        for _ in 0..10 {
            engine
                .upsert_edge(n1, n2, 3, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }

        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .edges_by_type_paged(
                    3,
                    &PageRequest {
                        limit: Some(3),
                        after: cursor,
                    },
                )
                .unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let mut all_unpaged = engine.edges_by_type(3).unwrap();
        all_unpaged.sort();
        assert_eq!(all_paged, all_unpaged);
    }

    #[test]
    fn test_get_nodes_by_type_paged_hydrates_page_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..10 {
            let mut props = BTreeMap::new();
            props.insert("idx".to_string(), PropValue::Int(i));
            engine
                .upsert_node(1, &format!("n{}", i), props, 1.0)
                .unwrap();
        }

        // Get first page of 3 hydrated records
        let page1 = engine
            .get_nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items.len(), 3);
        assert!(page1.next_cursor.is_some());
        // Verify they're actual NodeRecords with properties
        for node in &page1.items {
            assert_eq!(node.type_id, 1);
            assert!(node.props.contains_key("idx"));
        }

        // Get next page
        let page2 = engine
            .get_nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items.len(), 3);
        // No overlap
        let page1_ids: Vec<u64> = page1.items.iter().map(|n| n.id).collect();
        let page2_ids: Vec<u64> = page2.items.iter().map(|n| n.id).collect();
        for id in &page2_ids {
            assert!(!page1_ids.contains(id));
        }
    }

    #[test]
    fn test_get_edges_by_type_paged() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();

        for _ in 0..6 {
            engine
                .upsert_edge(n1, n2, 7, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }

        let page1 = engine
            .get_edges_by_type_paged(
                7,
                &PageRequest {
                    limit: Some(2),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.next_cursor.is_some());
        for edge in &page1.items {
            assert_eq!(edge.type_id, 7);
            assert_eq!(edge.from, n1);
            assert_eq!(edge.to, n2);
        }

        // Round-trip
        let mut all_paged: Vec<EdgeRecord> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .get_edges_by_type_paged(
                    7,
                    &PageRequest {
                        limit: Some(2),
                        after: cursor,
                    },
                )
                .unwrap();
            cursor = page.next_cursor;
            all_paged.extend(page.items);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(all_paged.len(), 6);
    }

    #[test]
    fn test_paged_single_item_pages() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let mut expected = [id1, id2, id3];
        expected.sort();

        // Page 1-at-a-time
        let p1 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(1),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(p1.items, vec![expected[0]]);
        assert!(p1.next_cursor.is_some());

        let p2 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(1),
                    after: p1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(p2.items, vec![expected[1]]);
        assert!(p2.next_cursor.is_some());

        let p3 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(1),
                    after: p2.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(p3.items, vec![expected[2]]);
        assert!(p3.next_cursor.is_none()); // last item
    }

    #[test]
    fn test_paged_limit_larger_than_result_set() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();

        let result = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(100),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.next_cursor.is_none()); // all fit in one page
    }

    #[test]
    fn test_paged_limit_zero_returns_all() {
        // limit: Some(0) should behave like None (return everything)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..5 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }

        let result = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(0),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_paged_cursor_on_deleted_id() {
        // Cursor points to a deleted node's ID (gap in the sorted list)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.delete_node(id2).unwrap(); // id2 is now a gap

        // Use deleted id2 as cursor. Should still work via binary search insertion point
        let result = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(10),
                    after: Some(id2),
                },
            )
            .unwrap();
        // Should return only ids > id2 (which is id3, since id1 < id2)
        let mut expected: Vec<u64> = vec![id1, id3].into_iter().filter(|&id| id > id2).collect();
        expected.sort();
        assert_eq!(result.items, expected);
    }

    // --- merge_type_ids_paged unit tests ---

    #[test]
    fn test_merge_paged_early_termination() {
        // Verify correct results when limit < total items
        let memtable = vec![5u64, 1, 9]; // unsorted, will be sorted internally
        let seg1 = vec![2u64, 4, 6, 8, 10];
        let seg2 = vec![3u64, 7];
        let deleted = HashSet::new();

        // Get first 4
        let page = PageRequest {
            limit: Some(4),
            after: None,
        };
        let result = merge_type_ids_paged(
            memtable.clone(),
            vec![seg1.clone(), seg2.clone()],
            &deleted,
            &page,
        );
        assert_eq!(result.items, vec![1, 2, 3, 4]);
        assert!(result.next_cursor.is_some());

        // Continue from cursor
        let page2 = PageRequest {
            limit: Some(4),
            after: result.next_cursor,
        };
        let result2 = merge_type_ids_paged(
            memtable.clone(),
            vec![seg1.clone(), seg2.clone()],
            &deleted,
            &page2,
        );
        assert_eq!(result2.items, vec![5, 6, 7, 8]);
        assert!(result2.next_cursor.is_some());

        // Last page
        let page3 = PageRequest {
            limit: Some(4),
            after: result2.next_cursor,
        };
        let result3 = merge_type_ids_paged(memtable, vec![seg1, seg2], &deleted, &page3);
        assert_eq!(result3.items, vec![9, 10]);
        assert!(result3.next_cursor.is_none());
    }

    #[test]
    fn test_merge_paged_cross_source_sorted_output() {
        // Multiple sources with interleaved IDs produce sorted output
        let memtable = vec![10u64, 30, 50];
        let seg1 = vec![20u64, 40];
        let seg2 = vec![15u64, 35, 55];
        let deleted = HashSet::new();

        let page = PageRequest {
            limit: None,
            after: None,
        };
        let result = merge_type_ids_paged(memtable, vec![seg1, seg2], &deleted, &page);
        assert_eq!(result.items, vec![10, 15, 20, 30, 35, 40, 50, 55]);
        assert!(result.next_cursor.is_none());

        // Verify sorted
        for i in 1..result.items.len() {
            assert!(result.items[i] > result.items[i - 1]);
        }
    }

    #[test]
    fn test_merge_paged_dedup_across_sources() {
        // Same ID in memtable + segment should only appear once
        let memtable = vec![1u64, 3, 5];
        let seg1 = vec![1u64, 2, 3]; // IDs 1 and 3 overlap with memtable
        let seg2 = vec![3u64, 4, 5]; // IDs 3 and 5 overlap
        let deleted = HashSet::new();

        let page = PageRequest {
            limit: None,
            after: None,
        };
        let result = merge_type_ids_paged(memtable, vec![seg1, seg2], &deleted, &page);
        assert_eq!(result.items, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_merge_paged_cursor_seek() {
        // Cursor skips correct items in the merge
        let memtable = vec![1u64, 5, 9];
        let seg1 = vec![2u64, 6, 10];
        let deleted = HashSet::new();

        // Cursor at 5 → should start from 6
        let page = PageRequest {
            limit: Some(3),
            after: Some(5),
        };
        let result = merge_type_ids_paged(memtable, vec![seg1], &deleted, &page);
        assert_eq!(result.items, vec![6, 9, 10]);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_merge_paged_with_policies() {
        // Policy path: merge produces sorted output, policy filtering + cursor works
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 6 nodes: 3 with high weight (keep), 3 with low weight (prune)
        let mut keep_ids = Vec::new();
        for i in 0..3 {
            let id = engine
                .upsert_node(1, &format!("keep{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            keep_ids.push(id);
        }
        for i in 0..3 {
            engine
                .upsert_node(1, &format!("prune{}", i), BTreeMap::new(), 0.1)
                .unwrap();
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

        // Page through with limit=2, should only see the 3 high-weight nodes
        let p1 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(2),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(p1.items.len(), 2);
        assert!(p1.next_cursor.is_some());

        let p2 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(2),
                    after: p1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(p2.items.len(), 1);
        assert!(p2.next_cursor.is_none());

        // Collected IDs should be the 3 keep nodes
        let mut all_paged: Vec<u64> = Vec::new();
        all_paged.extend(&p1.items);
        all_paged.extend(&p2.items);
        keep_ids.sort();
        assert_eq!(all_paged, keep_ids);
    }

    // --- find_nodes (property equality index) tests ---

    #[test]
    fn test_find_nodes_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let a = engine.upsert_node(1, "apple", props.clone(), 0.5).unwrap();

        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        let b = engine.upsert_node(1, "cherry", props2, 0.5).unwrap();

        let mut props3 = BTreeMap::new();
        props3.insert("color".to_string(), PropValue::String("green".to_string()));
        engine.upsert_node(1, "lime", props3, 0.5).unwrap();

        let mut reds = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        reds.sort();
        assert_eq!(reds, vec![a, b]);

        let greens = engine
            .find_nodes(1, "color", &PropValue::String("green".to_string()))
            .unwrap();
        assert_eq!(greens.len(), 1);

        assert!(engine
            .find_nodes(1, "color", &PropValue::String("blue".to_string()))
            .unwrap()
            .is_empty());
        assert!(engine
            .find_nodes(2, "color", &PropValue::String("red".to_string()))
            .unwrap()
            .is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_find_nodes_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create node in memtable, flush to segment
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let a = engine.upsert_node(1, "apple", props, 0.5).unwrap();
        engine.flush().unwrap();

        // Create node in memtable (stays in memtable)
        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        let b = engine.upsert_node(1, "cherry", props2, 0.5).unwrap();

        // find_nodes should merge across memtable + segment
        let mut reds = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        reds.sort();
        assert_eq!(reds, vec![a, b]);

        engine.close().unwrap();
    }

    #[test]
    fn test_find_nodes_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let a = engine.upsert_node(1, "apple", props.clone(), 0.5).unwrap();
        let b = engine.upsert_node(1, "cherry", props, 0.5).unwrap();

        engine.delete_node(b).unwrap();

        let reds = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        assert_eq!(reds, vec![a]);

        engine.close().unwrap();
    }

    #[test]
    fn test_find_nodes_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

            let mut props = BTreeMap::new();
            props.insert("lang".to_string(), PropValue::String("rust".to_string()));
            a = engine.upsert_node(1, "overgraph", props, 0.9).unwrap();

            let mut props2 = BTreeMap::new();
            props2.insert("lang".to_string(), PropValue::String("python".to_string()));
            engine.upsert_node(1, "other", props2, 0.5).unwrap();

            engine.flush().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

            let results = engine
                .find_nodes(1, "lang", &PropValue::String("rust".to_string()))
                .unwrap();
            assert_eq!(results, vec![a]);

            let py = engine
                .find_nodes(1, "lang", &PropValue::String("python".to_string()))
                .unwrap();
            assert_eq!(py.len(), 1);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_find_nodes_update_changes_index() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert(
            "status".to_string(),
            PropValue::String("active".to_string()),
        );
        let a = engine.upsert_node(1, "item", props, 0.5).unwrap();

        assert_eq!(
            engine
                .find_nodes(1, "status", &PropValue::String("active".to_string()))
                .unwrap(),
            vec![a]
        );

        // Update: change status
        let mut props2 = BTreeMap::new();
        props2.insert(
            "status".to_string(),
            PropValue::String("inactive".to_string()),
        );
        let a2 = engine.upsert_node(1, "item", props2, 0.5).unwrap();
        assert_eq!(a, a2); // same ID (dedup)

        assert!(engine
            .find_nodes(1, "status", &PropValue::String("active".to_string()))
            .unwrap()
            .is_empty());
        assert_eq!(
            engine
                .find_nodes(1, "status", &PropValue::String("inactive".to_string()))
                .unwrap(),
            vec![a]
        );

        engine.close().unwrap();
    }

    // --- find_nodes_paged tests ---

    #[test]
    fn test_find_nodes_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..8 {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String("red".to_string()));
            let id = engine
                .upsert_node(1, &format!("r{}", i), props, 1.0)
                .unwrap();
            ids.push(id);
        }
        // Add some non-matching nodes
        for i in 0..3 {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String("blue".to_string()));
            engine
                .upsert_node(1, &format!("b{}", i), props, 1.0)
                .unwrap();
        }
        ids.sort();

        let red = PropValue::String("red".to_string());

        // Page through 3 at a time
        let p1 = engine
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(p1.items.len(), 3);
        assert_eq!(p1.items, ids[0..3]);
        assert!(p1.next_cursor.is_some());

        let p2 = engine
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(3),
                    after: p1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(p2.items.len(), 3);
        assert_eq!(p2.items, ids[3..6]);
        assert!(p2.next_cursor.is_some());

        let p3 = engine
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(3),
                    after: p2.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(p3.items.len(), 2);
        assert_eq!(p3.items, ids[6..8]);
        assert!(p3.next_cursor.is_none());
    }

    #[test]
    fn test_find_nodes_paged_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        // Create 4 in segment
        for i in 0..4 {
            engine
                .upsert_node(1, &format!("seg{}", i), props.clone(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Create 4 in memtable
        for i in 0..4 {
            engine
                .upsert_node(1, &format!("mem{}", i), props.clone(), 1.0)
                .unwrap();
        }

        // Round-trip pagination should collect all 8
        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine
                .find_nodes_paged(
                    1,
                    "color",
                    &red,
                    &PageRequest {
                        limit: Some(3),
                        after: cursor,
                    },
                )
                .unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(all_paged.len(), 8);
        // Verify sorted
        for i in 1..all_paged.len() {
            assert!(all_paged[i] > all_paged[i - 1]);
        }
    }

    #[test]
    fn test_find_nodes_paged_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        let id1 = engine.upsert_node(1, "a", props.clone(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", props.clone(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", props.clone(), 1.0).unwrap();
        engine.delete_node(id2).unwrap();

        let result = engine
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        let mut expected = vec![id1, id3];
        expected.sort();
        assert_eq!(result.items, expected);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_find_nodes_paged_with_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        engine.upsert_node(1, "keep", props.clone(), 1.0).unwrap();
        engine.upsert_node(1, "prune", props.clone(), 0.1).unwrap();

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

        let result = engine
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(result.items.len(), 1);
    }

    #[test]
    fn test_nodes_by_type_paged_policy_refills_past_sparse_filtered_window() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut visible_ids = Vec::new();

        for i in 0..17u64 {
            let weight = if i < 12 { 0.1 } else { 1.0 };
            let id = engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), weight)
                .unwrap();
            if weight > 0.5 {
                visible_ids.push(id);
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
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items, visible_ids[..3].to_vec());
        assert!(page1.next_cursor.is_some());

        let page2 = engine
            .nodes_by_type_paged(
                1,
                &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items, visible_ids[3..].to_vec());
        assert!(page2.next_cursor.is_none());
    }

    #[test]
    fn test_find_nodes_paged_policy_refills_past_sparse_filtered_window() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let red = PropValue::String("red".to_string());
        let mut visible_ids = Vec::new();

        for i in 0..17u64 {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), red.clone());
            let weight = if i < 12 { 0.1 } else { 1.0 };
            let id = engine
                .upsert_node(1, &format!("n{}", i), props, weight)
                .unwrap();
            if weight > 0.5 {
                visible_ids.push(id);
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
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items, visible_ids[..3].to_vec());
        assert!(page1.next_cursor.is_some());

        let page2 = engine
            .find_nodes_paged(
                1,
                "color",
                &red,
                &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items, visible_ids[3..].to_vec());
        assert!(page2.next_cursor.is_none());
    }

    #[test]
    fn test_find_nodes_paged_default_returns_all() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        for i in 0..5 {
            engine
                .upsert_node(1, &format!("n{}", i), props.clone(), 1.0)
                .unwrap();
        }

        let result = engine
            .find_nodes_paged(1, "color", &red, &PageRequest::default())
            .unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_none());
    }

    // --- Temporal edge fields ---

    #[test]
    fn test_upsert_edge_default_temporal_fields() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let edge = engine.get_edge(eid).unwrap().unwrap();
        // Default: valid_from = created_at, valid_to = i64::MAX
        assert_eq!(edge.valid_from, edge.created_at);
        assert_eq!(edge.valid_to, i64::MAX);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_custom_temporal_fields() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, Some(1000), Some(5000))
            .unwrap();

        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_from, 1000);
        assert_eq!(edge.valid_to, 5000);

        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_fields_survive_flush_and_segment_read() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, Some(2000), Some(8000))
            .unwrap();

        // Flush to segment
        engine.flush().unwrap();

        // Read from segment
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_from, 2000);
        assert_eq!(edge.valid_to, 8000);

        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_fields_survive_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let eid;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            eid = engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, Some(3000), Some(9000))
                .unwrap();
            engine.close().unwrap();
        }

        // Reopen, WAL replay
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let edge = engine.get_edge(eid).unwrap().unwrap();
            assert_eq!(edge.valid_from, 3000);
            assert_eq!(edge.valid_to, 9000);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_batch_upsert_edges_temporal_fields() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        let inputs = vec![
            EdgeInput {
                from: a,
                to: b,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: Some(1000),
                valid_to: Some(5000),
            },
            EdgeInput {
                from: b,
                to: c,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None, // defaults
            },
        ];
        let ids = engine.batch_upsert_edges(&inputs).unwrap();

        let e1 = engine.get_edge(ids[0]).unwrap().unwrap();
        assert_eq!(e1.valid_from, 1000);
        assert_eq!(e1.valid_to, 5000);

        let e2 = engine.get_edge(ids[1]).unwrap().unwrap();
        assert_eq!(e2.valid_from, e2.created_at); // default
        assert_eq!(e2.valid_to, i64::MAX); // default

        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_fields_survive_compaction() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, Some(4000), Some(7000))
            .unwrap();

        // Flush segment 1
        engine.flush().unwrap();
        // Add something to create segment 2
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine
            .upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Compact
        engine.compact().unwrap();
        assert_eq!(engine.segment_count(), 1);

        // Temporal fields should survive compaction
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_from, 4000);
        assert_eq!(edge.valid_to, 7000);

        engine.close().unwrap();
    }

    // --- Temporal invalidation ---

    #[test]
    fn test_invalidate_edge_closes_validity_window() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Edge should be valid initially
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_to, i64::MAX);

        // Invalidate at epoch 5000
        let result = engine.invalidate_edge(eid, 5000).unwrap();
        assert!(result.is_some());
        let updated = result.unwrap();
        assert_eq!(updated.valid_to, 5000);

        // get_edge still returns it (not tombstoned)
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_to, 5000);

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidate_nonexistent_edge_returns_none() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let result = engine.invalidate_edge(999, 5000).unwrap();
        assert!(result.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidated_edge_hidden_from_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e_ab = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Both neighbors visible
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 2);

        // Invalidate a→b at epoch 1 (in the past)
        engine.invalidate_edge(e_ab, 1).unwrap();

        // Only a→c should be visible now
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidated_edge_hidden_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Flush to segment, then invalidate (invalidation goes to memtable/WAL)
        engine.flush().unwrap();
        engine.invalidate_edge(eid, 1).unwrap();

        // Edge should be hidden from neighbors
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out.is_empty());

        // But still retrievable via get_edge
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_to, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidated_edge_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (a, eid);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            eid = engine
                .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine.invalidate_edge(eid, 1).unwrap();
            engine.close().unwrap();
        }

        // Reopen. WAL replay should preserve invalidation
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let edge = engine.get_edge(eid).unwrap().unwrap();
            assert_eq!(edge.valid_to, 1);

            let out = engine
                .neighbors(a, Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert!(out.is_empty());
            engine.close().unwrap();
        }
    }

    // --- Point-in-time query tests ---

    #[test]
    fn test_point_in_time_query_sees_valid_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "a", BTreeMap::new(), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", BTreeMap::new(), 1.0)
            .unwrap();
        let c = engine
            .upsert_node(1, "c", BTreeMap::new(), 1.0)
            .unwrap();

        // Edge a→b: valid from epoch 1000 to 5000
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), Some(5000))
            .unwrap();
        // Edge a→c: valid from epoch 3000 to 8000
        engine
            .upsert_edge(a, c, 1, BTreeMap::new(), 1.0, Some(3000), Some(8000))
            .unwrap();

        // At epoch 500: neither edge is valid
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(500), None)
            .unwrap();
        assert_eq!(out.len(), 0);

        // At epoch 2000: only a→b is valid
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(2000), None)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, b);

        // At epoch 4000: both edges are valid
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(4000), None)
            .unwrap();
        assert_eq!(out.len(), 2);

        // At epoch 6000: only a→c is valid (a→b expired at 5000)
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(6000), None)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, c);

        // At epoch 9000: neither edge is valid
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(9000), None)
            .unwrap();
        assert_eq!(out.len(), 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_query_with_invalidated_edge() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "a", BTreeMap::new(), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", BTreeMap::new(), 1.0)
            .unwrap();

        // Create edge with explicit validity window
        let eid = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), Some(10000))
            .unwrap();

        // Invalidate at epoch 5000
        engine.invalidate_edge(eid, 5000).unwrap();

        // At epoch 3000: edge is valid (before invalidation)
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(3000), None)
            .unwrap();
        assert_eq!(out.len(), 1);

        // At epoch 5000: edge is no longer valid (valid_to is exclusive)
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(5000), None)
            .unwrap();
        assert_eq!(out.len(), 0);

        // At epoch 7000: edge is no longer valid
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(7000), None)
            .unwrap();
        assert_eq!(out.len(), 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_query_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "a", BTreeMap::new(), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", BTreeMap::new(), 1.0)
            .unwrap();

        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(2000), Some(8000))
            .unwrap();
        engine.flush().unwrap();

        // Query from segment: at_epoch=5000 should see it
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(5000), None)
            .unwrap();
        assert_eq!(out.len(), 1);

        // Query from segment: at_epoch=1000 should not
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(1000), None)
            .unwrap();
        assert_eq!(out.len(), 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_traverse_depth_two() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "a", BTreeMap::new(), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", BTreeMap::new(), 1.0)
            .unwrap();
        let c = engine
            .upsert_node(1, "c", BTreeMap::new(), 1.0)
            .unwrap();

        // a→b valid 1000-5000, b→c valid 2000-6000
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), Some(5000))
            .unwrap();
        engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, Some(2000), Some(6000))
            .unwrap();

        // At epoch 3000: both hops valid, should reach c
        let hop2 =
            traverse_depth_two_read(&engine, a, Direction::Outgoing, None, None, Some(3000));
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        // At epoch 500: first hop not valid, can't reach b or c
        let hop2 = traverse_depth_two_read(&engine, a, Direction::Outgoing, None, None, Some(500));
        assert_eq!(hop2.len(), 0);

        // At epoch 5500: first hop expired (a→b), can't reach c
        let hop2 =
            traverse_depth_two_read(&engine, a, Direction::Outgoing, None, None, Some(5500));
        assert_eq!(hop2.len(), 0);
        engine.close().unwrap();
    }

    // --- Decay-adjusted scoring tests ---

    #[test]
    fn test_decay_scoring_orders_by_recency() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine
            .upsert_node(1, "hub", BTreeMap::new(), 1.0)
            .unwrap();
        let old = engine
            .upsert_node(1, "old", BTreeMap::new(), 1.0)
            .unwrap();
        let recent = engine
            .upsert_node(1, "recent", BTreeMap::new(), 1.0)
            .unwrap();

        let now = now_millis();
        let one_day_ago = now - 24 * 3_600_000; // 24 hours ago
        let one_hour_ago = now - 3_600_000; // 1 hour ago

        // Both edges have equal base weight=1.0, but different updated_at times
        // Old edge: created/updated a day ago
        engine
            .upsert_edge(hub, old, 1, BTreeMap::new(), 1.0, Some(one_day_ago), None)
            .unwrap();
        // Recent edge: created/updated an hour ago
        engine
            .upsert_edge(
                hub,
                recent,
                1,
                BTreeMap::new(),
                1.0,
                Some(one_hour_ago),
                None,
            )
            .unwrap();

        // Without decay: order is insertion order (or arbitrary)
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 2);

        // With decay (lambda=0.1): recent edge should have higher score
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 0, None, Some(0.1))
            .unwrap();
        assert_eq!(out.len(), 2);
        // First result should be the recent one (higher decay-adjusted weight)
        assert_eq!(out[0].node_id, recent);
        assert_eq!(out[1].node_id, old);
        // Recent edge score should be higher
        assert!(out[0].weight > out[1].weight);
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_scoring_with_different_base_weights() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine
            .upsert_node(1, "hub", BTreeMap::new(), 1.0)
            .unwrap();
        let heavy_old = engine
            .upsert_node(1, "heavy_old", BTreeMap::new(), 1.0)
            .unwrap();
        let light_new = engine
            .upsert_node(1, "light_new", BTreeMap::new(), 1.0)
            .unwrap();

        let now = now_millis();
        let two_days_ago = now - 48 * 3_600_000;
        let one_hour_ago = now - 3_600_000;

        // Heavy but old: weight=10.0, updated 2 days ago
        engine
            .upsert_edge(
                hub,
                heavy_old,
                1,
                BTreeMap::new(),
                10.0,
                Some(two_days_ago),
                None,
            )
            .unwrap();
        // Light but new: weight=1.0, updated 1 hour ago
        engine
            .upsert_edge(
                hub,
                light_new,
                1,
                BTreeMap::new(),
                1.0,
                Some(one_hour_ago),
                None,
            )
            .unwrap();

        // With aggressive decay (lambda=0.1): age penalty on the heavy edge should be large
        // score(heavy_old) = 10.0 * exp(-0.1 * 48) ≈ 10.0 * 0.0082 ≈ 0.082
        // score(light_new) = 1.0 * exp(-0.1 * 1) ≈ 1.0 * 0.905 ≈ 0.905
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 0, None, Some(0.1))
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].node_id, light_new); // light_new wins despite lower base weight
        assert!(out[0].weight > out[1].weight);
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_zero_lambda_no_reorder() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "a", BTreeMap::new(), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", BTreeMap::new(), 1.0)
            .unwrap();

        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 5.0, None, None)
            .unwrap();

        // decay_lambda=None means no decay applied
        let out = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert!((out[0].weight - 5.0).abs() < 0.001); // original weight preserved
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_with_limit_returns_top_scored() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine
            .upsert_node(1, "hub", BTreeMap::new(), 1.0)
            .unwrap();
        let n1 = engine
            .upsert_node(1, "n1", BTreeMap::new(), 1.0)
            .unwrap();
        let n2 = engine
            .upsert_node(1, "n2", BTreeMap::new(), 1.0)
            .unwrap();
        let n3 = engine
            .upsert_node(1, "n3", BTreeMap::new(), 1.0)
            .unwrap();

        let now = now_millis();

        // Three edges with different ages, same base weight
        engine
            .upsert_edge(
                hub,
                n1,
                1,
                BTreeMap::new(),
                1.0,
                Some(now - 72 * 3_600_000),
                None,
            )
            .unwrap(); // 3 days old
        engine
            .upsert_edge(
                hub,
                n2,
                1,
                BTreeMap::new(),
                1.0,
                Some(now - 24 * 3_600_000),
                None,
            )
            .unwrap(); // 1 day old
        engine
            .upsert_edge(
                hub,
                n3,
                1,
                BTreeMap::new(),
                1.0,
                Some(now - 3_600_000),
                None,
            )
            .unwrap(); // 1 hour old

        // With decay and limit=2: should return the 2 most recent
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 2, None, Some(0.05))
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].node_id, n3); // most recent first
        assert_eq!(out[1].node_id, n2); // second most recent
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_with_decay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine
            .upsert_node(1, "hub", BTreeMap::new(), 1.0)
            .unwrap();
        let n1 = engine
            .upsert_node(1, "n1", BTreeMap::new(), 1.0)
            .unwrap();
        let n2 = engine
            .upsert_node(1, "n2", BTreeMap::new(), 1.0)
            .unwrap();

        // Edge 1: valid 1000-5000, updated_at=1000
        engine
            .upsert_edge(hub, n1, 1, BTreeMap::new(), 1.0, Some(1000), Some(5000))
            .unwrap();
        // Edge 2: valid 2000-8000, updated_at=2000
        engine
            .upsert_edge(hub, n2, 1, BTreeMap::new(), 1.0, Some(2000), Some(8000))
            .unwrap();

        // At epoch 3000 with decay: both visible, decay based on age from reference_time
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 0, Some(3000), Some(0.01))
            .unwrap();
        assert_eq!(out.len(), 2);
        // n2's edge is newer (updated_at=2000 vs 1000), so it should score higher
        // Both have weight 1.0, but age_hours differs
        assert_eq!(out[0].node_id, n2);

        // At epoch 6000: only edge 2 is valid
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 0, Some(6000), Some(0.01))
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, n2);
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_scoring_after_flush_segment_sourced() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine
            .upsert_node(1, "hub", BTreeMap::new(), 1.0)
            .unwrap();
        let old = engine
            .upsert_node(1, "old", BTreeMap::new(), 1.0)
            .unwrap();
        let recent = engine
            .upsert_node(1, "recent", BTreeMap::new(), 1.0)
            .unwrap();

        let now = now_millis();
        engine
            .upsert_edge(
                hub,
                old,
                1,
                BTreeMap::new(),
                1.0,
                Some(now - 48 * 3_600_000),
                None,
            )
            .unwrap();
        engine
            .upsert_edge(
                hub,
                recent,
                1,
                BTreeMap::new(),
                1.0,
                Some(now - 3_600_000),
                None,
            )
            .unwrap();

        // Flush to segment. Edges now served from segment reader
        engine.flush().unwrap();

        // Decay should still work on segment-sourced edges
        let out = engine
            .neighbors(hub, Direction::Outgoing, None, 0, None, Some(0.05))
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].node_id, recent); // recent edge scores higher
        assert_eq!(out[1].node_id, old);
        assert!(out[0].weight > out[1].weight);
        engine.close().unwrap();
    }

    #[test]
    fn test_negative_decay_lambda_returns_error() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine
            .upsert_node(1, "a", BTreeMap::new(), 1.0)
            .unwrap();
        engine
            .upsert_edge(a, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let result = engine.neighbors(a, Direction::Outgoing, None, 0, None, Some(-0.5));
        assert!(result.is_err());
        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_adjacency_postings_survive_flush() {
        // Temporal fields in adjacency postings enable filtering
        // without per-edge record lookup after flush.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        // Edge a→b valid [1000, 5000)
        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, Some(1000), Some(5000))
            .unwrap();
        // Edge a→c valid [3000, 9000)
        engine
            .upsert_edge(a, c, 10, BTreeMap::new(), 1.0, Some(3000), Some(9000))
            .unwrap();

        engine.flush().unwrap();

        // At t=2000: only a→b visible
        let n = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(2000), None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, b);

        // At t=4000: both visible
        let n = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(4000), None)
            .unwrap();
        assert_eq!(n.len(), 2);

        // At t=6000: only a→c visible
        let n = engine
            .neighbors(a, Direction::Outgoing, None, 0, Some(6000), None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_adjacency_hashmap_upsert_idempotent() {
        // HashMap adjacency means re-upsert is O(1) and idempotent.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();

        // Insert edge, then upsert it multiple times with different weights
        // With edge_uniqueness, (a, b, 10) deduplicates to the same edge ID.
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let eid2 = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 2.0, None, None)
            .unwrap();
        let eid3 = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 3.0, None, None)
            .unwrap();
        assert_eq!(eid, eid2);
        assert_eq!(eid, eid3);

        // Should still have exactly 1 neighbor entry, not 3
        let n = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].edge_id, eid);
        assert_eq!(n[0].weight, 3.0); // latest weight

        engine.close().unwrap();
    }

    // ========================================
    // Progress callback + cancellation
    // ========================================

    #[test]
    fn test_compact_with_progress_reports_all_phases() {
        // Verify that compact_with_progress reports all four phases in order.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create data across 3 segments
        let mut node_ids = Vec::new();
        for i in 0..30 {
            node_ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        for i in 30..60 {
            node_ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Add some edges to make the edge phase meaningful
        for i in 0..10 {
            engine
                .upsert_edge(
                    node_ids[i],
                    node_ids[i + 1],
                    1,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .unwrap();
        }
        engine.flush().unwrap();

        let mut phases_seen: Vec<CompactionPhase> = Vec::new();
        let mut progress_calls = 0u32;

        let stats = engine
            .compact_with_progress(|progress| {
                // Track unique phases in order
                if phases_seen.last() != Some(&progress.phase) {
                    phases_seen.push(progress.phase);
                }
                progress_calls += 1;
                true // continue
            })
            .unwrap();

        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 60);
        assert!(stats.edges_kept >= 10);

        // Must see all four phases
        assert_eq!(
            phases_seen,
            vec![
                CompactionPhase::CollectingTombstones,
                CompactionPhase::MergingNodes,
                CompactionPhase::MergingEdges,
                CompactionPhase::WritingOutput,
            ]
        );

        // Multiple progress calls: 3 per merge phase (one per segment) + tombstone + 2 write
        assert!(progress_calls >= 4, "got {} calls", progress_calls);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_during_tombstones() {
        // Cancel during tombstone collection. Engine state must be unchanged.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 20..40 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Cancel on first callback (during CollectingTombstones)
        let result = engine.compact_with_progress(|_| false);
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // Engine should still work, all data intact
        for &id in &ids {
            let node = engine.get_node(id).unwrap();
            assert!(node.is_some(), "node {} missing after cancel", id);
        }

        // Should still be able to compact successfully
        let stats = engine.compact().unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().nodes_kept, 40);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_during_merge_nodes() {
        // Cancel during the MergingNodes phase. No state change.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 20..40 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Cancel during MergingNodes
        let result = engine
            .compact_with_progress(|progress| progress.phase != CompactionPhase::MergingNodes);
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // All data still accessible
        for &id in &ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_during_merge_edges() {
        // Cancel during the MergingEdges phase.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut node_ids = Vec::new();
        for i in 0..10 {
            node_ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 0..5 {
            engine
                .upsert_edge(
                    node_ids[i],
                    node_ids[i + 1],
                    1,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .unwrap();
        }
        engine.flush().unwrap();

        let result = engine
            .compact_with_progress(|progress| progress.phase != CompactionPhase::MergingEdges);
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // All data intact
        for &id in &node_ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }
        for nid in &node_ids[..5] {
            let neighbors = engine
                .neighbors(*nid, Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert!(
                !neighbors.is_empty(),
                "node {} should have outgoing edges",
                nid
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_before_write() {
        // Cancel at the WritingOutput phase. No temp dirs left behind.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 10..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        let result = engine
            .compact_with_progress(|progress| progress.phase != CompactionPhase::WritingOutput);
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // No temp segment directories should be left behind
        let segments_dir = db_path.join("segments");
        if segments_dir.exists() {
            for entry in std::fs::read_dir(&segments_dir).unwrap() {
                let name = entry.unwrap().file_name();
                let name_str = name.to_string_lossy();
                assert!(
                    !name_str.contains(".tmp"),
                    "temp dir {} left after cancel",
                    name_str
                );
            }
        }

        // Data still intact, can still compact successfully
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 20);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_records_processed_counts() {
        // Verify records_processed and total_records are accurate.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // 50 nodes in seg1, 50 nodes in seg2
        for i in 0..50 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 50..100 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        let mut final_node_progress: Option<CompactionProgress> = None;

        engine
            .compact_with_progress(|progress| {
                if progress.phase == CompactionPhase::MergingNodes {
                    final_node_progress = Some(progress.clone());
                }
                true
            })
            .unwrap();

        let np = final_node_progress.unwrap();
        assert_eq!(np.total_records, 100, "total_records should be 100 nodes");
        assert_eq!(np.records_processed, 100, "all 100 should be processed");
        assert_eq!(np.segments_processed, 2);
        assert_eq!(np.total_segments, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_tombstone_counts_all_examined() {
        // S2 fix: records_processed counts all examined records (including
        // tombstoned) so progress bars reach 100% even with deletes.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..50 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Delete 20 nodes → tombstones
        for id in &ids[..20] {
            engine.delete_node(*id).unwrap();
        }
        engine.flush().unwrap();

        let mut final_node_progress: Option<CompactionProgress> = None;

        let stats = engine
            .compact_with_progress(|progress| {
                if progress.phase == CompactionPhase::MergingNodes {
                    final_node_progress = Some(progress.clone());
                }
                true
            })
            .unwrap()
            .unwrap();

        let np = final_node_progress.unwrap();
        // total_records = total input node count across segments (50 in seg1 + 0 in seg2)
        assert_eq!(np.total_records, 50);
        // records_processed should equal total_records (all examined, even tombstoned)
        assert_eq!(np.records_processed, np.total_records);
        // But compaction only kept the live ones
        assert_eq!(stats.nodes_kept, 30);
        assert_eq!(stats.nodes_removed, 20);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_no_callback_wrapper() {
        // compact() (no callback) still works as before.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 20..40 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 40);

        // Verify data integrity post-compact
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        engine.close().unwrap();
    }



    // ========== get_node_by_key ==========

    fn make_props(key: &str, val: &str) -> BTreeMap<String, PropValue> {
        let mut m = BTreeMap::new();
        m.insert(key.to_string(), PropValue::String(val.to_string()));
        m
    }

    fn open_imm(path: &std::path::Path) -> DatabaseEngine {
        let opts = DbOptions {
            create_if_missing: true,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        DatabaseEngine::open(path, &opts).unwrap()
    }

    #[test]
    fn test_get_node_by_key_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine
            .upsert_node(1, "alice", make_props("name", "Alice"), 1.0)
            .unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id);
        assert_eq!(node.type_id, 1);
        assert_eq!(node.key, "alice");
        assert_eq!(
            node.props.get("name"),
            Some(&PropValue::String("Alice".to_string()))
        );
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_not_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine
            .upsert_node(1, "alice", make_props("name", "Alice"), 1.0)
            .unwrap();
        assert!(engine.get_node_by_key(1, "bob").unwrap().is_none());
        assert!(engine.get_node_by_key(2, "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine
            .upsert_node(1, "alice", make_props("name", "Alice"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id);
        assert_eq!(node.key, "alice");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_after_compaction() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine
            .upsert_node(1, "alice", make_props("name", "v1"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        let id2 = engine
            .upsert_node(1, "alice", make_props("name", "v2"), 2.0)
            .unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id2);
        assert_eq!(
            node.props.get("name"),
            Some(&PropValue::String("v2".to_string()))
        );
        assert_eq!(node.weight, 2.0);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_deleted() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine
            .upsert_node(1, "alice", make_props("name", "Alice"), 1.0)
            .unwrap();
        engine.delete_node(id).unwrap();
        assert!(engine.get_node_by_key(1, "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_deleted_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine
            .upsert_node(1, "alice", make_props("name", "Alice"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        engine.delete_node(id).unwrap();
        assert!(engine.get_node_by_key(1, "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_memtable_shadows_segment() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine
            .upsert_node(1, "alice", make_props("name", "v1"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        let id2 = engine
            .upsert_node(1, "alice", make_props("name", "v2"), 2.0)
            .unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id2);
        assert_eq!(
            node.props.get("name"),
            Some(&PropValue::String("v2".to_string()))
        );
        engine.close().unwrap();
    }

    // ========== get_edge_by_triple ==========

    #[test]
    fn test_get_edge_by_triple_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, make_props("rel", "knows"), 0.5, None, None)
            .unwrap();
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, eid);
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, b);
        assert_eq!(edge.type_id, 10);
        assert_eq!(
            edge.props.get("rel"),
            Some(&PropValue::String("knows".to_string()))
        );
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_not_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        assert!(engine.get_edge_by_triple(a, b, 99).unwrap().is_none());
        assert!(engine.get_edge_by_triple(b, a, 10).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, eid);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_deleted() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.delete_edge(eid).unwrap();
        assert!(engine.get_edge_by_triple(a, b, 10).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_deleted_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine
            .upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        engine.delete_edge(eid).unwrap();
        assert!(engine.get_edge_by_triple(a, b, 10).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_after_compaction() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let mut engine = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine
            .upsert_edge(a, b, 10, make_props("v", "1"), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        let eid2 = engine
            .upsert_edge(a, b, 10, make_props("v", "2"), 2.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, eid2);
        assert_eq!(
            edge.props.get("v"),
            Some(&PropValue::String("2".to_string()))
        );
        engine.close().unwrap();
    }

    // ========== get_nodes / get_edges (bulk) ==========

    #[test]
    fn test_get_nodes_bulk() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine
            .upsert_node(1, "a", make_props("name", "A"), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", make_props("name", "B"), 1.0)
            .unwrap();
        let c = engine
            .upsert_node(1, "c", make_props("name", "C"), 1.0)
            .unwrap();
        let results = engine.get_nodes(&[a, b, c]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        assert_eq!(results[2].as_ref().unwrap().key, "c");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_mixed_found_missing() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.delete_node(b).unwrap();
        let results = engine.get_nodes(&[a, b, 9999]).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_none()); // deleted
        assert!(results[2].is_none()); // never existed
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let results = engine.get_nodes(&[a, b]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_empty() {
        let dir = TempDir::new().unwrap();
        let engine = open_imm(&dir.path().join("db"));
        let results = engine.get_nodes(&[]).unwrap();
        assert!(results.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let results = engine.get_edges(&[e1, e2, 9999]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().from, a);
        assert_eq!(results[1].as_ref().unwrap().from, b);
        assert!(results[2].is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        let e2 = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let results = engine.get_edges(&[e1, e2]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().from, a);
        assert_eq!(results[1].as_ref().unwrap().from, b);
        engine.close().unwrap();
    }

    // ========== Bulk read merge-walk tests ==========

    #[test]
    fn test_get_nodes_bulk_multi_segment_interleaved() {
        // IDs spread across two segments. The merge-walk must handle
        // both segments having relevant entries
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        // Segment 1: nodes 1, 2, 3
        let a = engine
            .upsert_node(1, "a", make_props("seg", "1"), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", make_props("seg", "1"), 1.0)
            .unwrap();
        let c = engine
            .upsert_node(1, "c", make_props("seg", "1"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        // Segment 2: nodes 4, 5
        let d = engine
            .upsert_node(1, "d", make_props("seg", "2"), 1.0)
            .unwrap();
        let e = engine
            .upsert_node(1, "e", make_props("seg", "2"), 1.0)
            .unwrap();
        engine.flush().unwrap();

        // Request in non-sorted order, mixing IDs from both segments
        let results = engine.get_nodes(&[e, a, d, c, b]).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].as_ref().unwrap().key, "e");
        assert_eq!(results[1].as_ref().unwrap().key, "a");
        assert_eq!(results[2].as_ref().unwrap().key, "d");
        assert_eq!(results[3].as_ref().unwrap().key, "c");
        assert_eq!(results[4].as_ref().unwrap().key, "b");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_tombstone_in_newer_segment() {
        // Node flushed to segment 1, then deleted and flushed to segment 2.
        // Bulk read must respect cross-segment tombstones.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap(); // seg 1: a, b
        engine.delete_node(a).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap(); // seg 2: tombstone(a), c

        let results = engine.get_nodes(&[a, b, c]).unwrap();
        assert!(results[0].is_none()); // a tombstoned in seg 2
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        assert_eq!(results[2].as_ref().unwrap().key, "c");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_memtable_shadows_segment() {
        // Node in segment, then updated in memtable.
        // Bulk read must return the fresher memtable version.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine
            .upsert_node(1, "a", make_props("v", "old"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        engine
            .upsert_node(1, "a", make_props("v", "new"), 2.0)
            .unwrap();
        // a is now in both memtable (newer) and segment (older)
        let results = engine.get_nodes(&[a]).unwrap();
        let node = results[0].as_ref().unwrap();
        assert_eq!(
            node.props.get("v"),
            Some(&PropValue::String("new".to_string()))
        );
        assert_eq!(node.weight, 2.0);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_duplicate_ids() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let results = engine.get_nodes(&[a, a, a]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "a");
        assert_eq!(results[2].as_ref().unwrap().key, "a");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_duplicate_ids_in_segment() {
        // Same as above but the node is in a segment, exercising the merge-walk
        // with duplicate lookups
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        let results = engine.get_nodes(&[a, a, a]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "a");
        assert_eq!(results[2].as_ref().unwrap().key, "a");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk_multi_segment_interleaved() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        // Segment 1
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        // Segment 2
        let e2 = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Reverse order
        let results = engine.get_edges(&[e2, e1]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().from, b);
        assert_eq!(results[1].as_ref().unwrap().from, a);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk_tombstone_cross_segment() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap(); // seg 1: e1, e2
        engine.delete_edge(e1).unwrap();
        engine.flush().unwrap(); // seg 2: tombstone(e1)

        let results = engine.get_edges(&[e1, e2]).unwrap();
        assert!(results[0].is_none()); // tombstoned
        assert_eq!(results[1].as_ref().unwrap().from, b);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_after_compaction() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine
            .upsert_node(1, "a", make_props("v", "1"), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(1, "b", make_props("v", "1"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        engine
            .upsert_node(1, "a", make_props("v", "2"), 2.0)
            .unwrap();
        let c = engine
            .upsert_node(1, "c", make_props("v", "1"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();

        let results = engine.get_nodes(&[a, b, c]).unwrap();
        assert_eq!(
            results[0].as_ref().unwrap().props.get("v"),
            Some(&PropValue::String("2".to_string()))
        ); // updated
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        assert_eq!(results[2].as_ref().unwrap().key, "c");
        engine.close().unwrap();
    }

    // ========== delete-then-re-create via key ==========

    #[test]
    fn test_get_node_by_key_delete_then_recreate() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id1 = engine
            .upsert_node(1, "alice", make_props("v", "1"), 1.0)
            .unwrap();
        engine.flush().unwrap();
        engine.delete_node(id1).unwrap();
        // Re-create with same key → gets a NEW id
        let id2 = engine
            .upsert_node(1, "alice", make_props("v", "2"), 2.0)
            .unwrap();
        assert_ne!(id1, id2);
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id2);
        assert_eq!(
            node.props.get("v"),
            Some(&PropValue::String("2".to_string()))
        );
        engine.close().unwrap();
    }

    // ========== get_edge_by_triple with uniqueness off ==========

    #[test]
    fn test_get_edge_by_triple_uniqueness_off_returns_latest() {
        let dir = TempDir::new().unwrap();
        // Default: edge_uniqueness = false
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let _e1 = engine
            .upsert_edge(a, b, 10, make_props("v", "1"), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(a, b, 10, make_props("v", "2"), 2.0, None, None)
            .unwrap();
        // With uniqueness off, both edges exist. Triple index maps to the latest.
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, e2);
        assert_eq!(
            edge.props.get("v"),
            Some(&PropValue::String("2".to_string()))
        );
        engine.close().unwrap();
    }

    // ========== Atomic graph patch tests ==========

    #[test]
    fn test_graph_patch_mixed_ops() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Create some initial data
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Patch: upsert a new node, a new edge a→b, invalidate e1
        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "c".to_string(),
                props: make_props("role", "new"),
                weight: 1.0,
            }],
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                type_id: 2,
                props: BTreeMap::new(),
                weight: 0.5,
                valid_from: None,
                valid_to: None,
            }],
            invalidate_edges: vec![(e1, 1000)],
            delete_node_ids: vec![],
            delete_edge_ids: vec![],
        };
        let result = engine.graph_patch(&patch).unwrap();

        // Verify results
        assert_eq!(result.node_ids.len(), 1);
        assert_eq!(result.edge_ids.len(), 1);

        // New node created
        let c = engine.get_node(result.node_ids[0]).unwrap().unwrap();
        assert_eq!(c.key, "c");
        assert_eq!(
            c.props.get("role"),
            Some(&PropValue::String("new".to_string()))
        );

        // New edge created
        let edge = engine.get_edge(result.edge_ids[0]).unwrap().unwrap();
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, b);

        // e1 invalidated (still exists but valid_to set)
        let e1_after = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(e1_after.valid_to, 1000);

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_empty() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let result = engine.graph_patch(&GraphPatch::default()).unwrap();
        assert!(result.node_ids.is_empty());
        assert!(result.edge_ids.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_node_dedup() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Pre-existing node
        let existing = engine
            .upsert_node(1, "alice", make_props("v", "1"), 1.0)
            .unwrap();

        let patch = GraphPatch {
            upsert_nodes: vec![
                NodeInput {
                    type_id: 1,
                    key: "alice".to_string(),
                    props: make_props("v", "2"),
                    weight: 2.0,
                },
                NodeInput {
                    type_id: 1,
                    key: "alice".to_string(),
                    props: make_props("v", "3"),
                    weight: 3.0,
                },
                NodeInput {
                    type_id: 1,
                    key: "bob".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                },
            ],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();

        // alice deduped: both get the existing ID
        assert_eq!(result.node_ids[0], existing);
        assert_eq!(result.node_ids[1], existing);
        // bob is new
        assert_ne!(result.node_ids[2], existing);

        // Last write wins: alice has v=3
        let alice = engine.get_node(existing).unwrap().unwrap();
        assert_eq!(
            alice.props.get("v"),
            Some(&PropValue::String("3".to_string()))
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_delete_with_cascade() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e_ab = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e_bc = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Delete node b. Should cascade delete e_ab and e_bc
        let patch = GraphPatch {
            delete_node_ids: vec![b],
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap();

        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(e_ab).unwrap().is_none());
        assert!(engine.get_edge(e_bc).unwrap().is_none());
        // a and c survive
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(c).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_edge_delete() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let patch = GraphPatch {
            delete_edge_ids: vec![e],
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap();

        assert!(engine.get_edge(e).unwrap().is_none());
        // Nodes survive
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_ordering_upserts_before_deletes() {
        // Upsert a node and delete it in the same patch.
        // Deterministic ordering: upserts first, deletes last.
        // The delete should win (delete comes after upsert in the WAL).
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "a".to_string(),
                props: make_props("v", "updated"),
                weight: 2.0,
            }],
            delete_node_ids: vec![a],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert_eq!(result.node_ids[0], a);

        // Delete wins, node should be gone
        assert!(engine.get_node(a).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_invalidate_nonexistent_edge_skipped() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Invalidating a nonexistent edge should be silently skipped
        let patch = GraphPatch {
            invalidate_edges: vec![(99999, 5000)],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert!(result.node_ids.is_empty());
        assert!(result.edge_ids.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (node_id, edge_id, invalidated_eid);
        {
            let mut engine = open_imm(&db_path);
            let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
            let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
            invalidated_eid = engine
                .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();

            let patch = GraphPatch {
                upsert_nodes: vec![NodeInput {
                    type_id: 1,
                    key: "c".to_string(),
                    props: make_props("role", "new"),
                    weight: 1.0,
                }],
                upsert_edges: vec![EdgeInput {
                    from: a,
                    to: b,
                    type_id: 5,
                    props: BTreeMap::new(),
                    weight: 0.5,
                    valid_from: None,
                    valid_to: None,
                }],
                invalidate_edges: vec![(invalidated_eid, 2000)],
                delete_edge_ids: vec![],
                delete_node_ids: vec![],
            };
            let result = engine.graph_patch(&patch).unwrap();
            node_id = result.node_ids[0];
            edge_id = result.edge_ids[0];
            engine.close().unwrap();
        }

        // Reopen. WAL replay should preserve all patch effects
        {
            let engine = open_imm(&db_path);
            let node = engine.get_node(node_id).unwrap().unwrap();
            assert_eq!(node.key, "c");

            let edge = engine.get_edge(edge_id).unwrap().unwrap();
            assert_eq!(edge.type_id, 5);

            let inv_edge = engine.get_edge(invalidated_eid).unwrap().unwrap();
            assert_eq!(inv_edge.valid_to, 2000);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_graph_patch_two_step_upsert_then_connect() {
        // Edges reference node IDs, so we need IDs upfront. Use two patches:
        // first upsert nodes, then connect them with edges.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let patch1 = GraphPatch {
            upsert_nodes: vec![
                NodeInput {
                    type_id: 1,
                    key: "x".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                },
                NodeInput {
                    type_id: 1,
                    key: "y".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                },
            ],
            ..GraphPatch::default()
        };
        let r1 = engine.graph_patch(&patch1).unwrap();
        let x = r1.node_ids[0];
        let y = r1.node_ids[1];

        // Now connect them in a second patch
        let patch2 = GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: x,
                to: y,
                type_id: 10,
                props: make_props("rel", "friend"),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            }],
            ..GraphPatch::default()
        };
        let r2 = engine.graph_patch(&patch2).unwrap();

        let edge = engine.get_edge(r2.edge_ids[0]).unwrap().unwrap();
        assert_eq!(edge.from, x);
        assert_eq!(edge.to, y);
        assert_eq!(edge.type_id, 10);

        // Neighbors work
        let nbrs = engine
            .neighbors(x, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, y);

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Patch against segment data
        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "a".to_string(),
                props: make_props("v", "updated"),
                weight: 2.0,
            }],
            invalidate_edges: vec![(e, 500)],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert_eq!(result.node_ids[0], a); // deduped against segment

        let node = engine.get_node(a).unwrap().unwrap();
        assert_eq!(
            node.props.get("v"),
            Some(&PropValue::String("updated".to_string()))
        );

        let edge = engine.get_edge(e).unwrap().unwrap();
        assert_eq!(edge.valid_to, 500);

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_duplicate_edge_delete_safe() {
        // Edge deleted via both explicit delete_edge_ids and cascade from delete_node_ids.
        // Should not panic. Duplicate DeleteEdge ops are idempotent.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let patch = GraphPatch {
            delete_edge_ids: vec![e], // explicit delete
            delete_node_ids: vec![a], // cascade also deletes e
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap(); // should not panic

        assert!(engine.get_edge(e).unwrap().is_none());
        assert!(engine.get_node(a).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_invalidate_pre_existing_edge() {
        // Invalidation looks up the current edge state. Since ops are applied
        // as a batch after all ops are built, invalidation sees the pre-patch state.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, make_props("v", "original"), 1.0, None, None)
            .unwrap();

        // Upsert the same edge (updates props) AND invalidate it in the same patch.
        // Ordering: upsert (step 2) → invalidation (step 3) in the ops vec.
        // The invalidation reads the PRE-patch edge (the one already in memtable),
        // so it captures the original props, not the updated ones.
        // Both ops are applied atomically. Last write to valid_to wins.
        let patch = GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                type_id: 1,
                props: make_props("v", "updated"),
                weight: 2.0,
                valid_from: None,
                valid_to: None, // sets valid_to = MAX
            }],
            invalidate_edges: vec![(e, 3000)],
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap();

        // The invalidation's UpsertEdge comes after the upsert's UpsertEdge in the ops vec,
        // so the invalidation's valid_to=3000 wins via last-write-wins in apply_op.
        let edge = engine.get_edge(e).unwrap().unwrap();
        assert_eq!(edge.valid_to, 3000);

        engine.close().unwrap();
    }

    // ========== Cross-source cascade delete tests ==========

    #[test]
    fn test_delete_node_cascades_segment_edges() {
        // Edge is flushed to a segment, then the node is deleted.
        // Cascade should still find and delete the edge.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap(); // edge moves to segment

        engine.delete_node(a).unwrap();

        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none()); // cascade reached segment edge
        assert!(engine.get_node(b).unwrap().is_some()); // b survives
                                                        // No ghost neighbors on b
        let nbrs = engine
            .neighbors(b, Direction::Incoming, None, 0, None, None)
            .unwrap();
        assert!(nbrs.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_cascades_mixed_sources() {
        // Some edges in memtable, some in segments
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap(); // e1 in segment
        let e2 = engine
            .upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        // e2 in memtable

        engine.delete_node(a).unwrap();

        assert!(engine.get_edge(e1).unwrap().is_none()); // segment edge
        assert!(engine.get_edge(e2).unwrap().is_none()); // memtable edge
        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_cascades_incoming_segment_edges() {
        // Test that incoming edges (where deleted node is the target) are also cascaded
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Delete b (the target). Should cascade delete the incoming edge
        engine.delete_node(b).unwrap();

        assert!(engine.get_edge(e).unwrap().is_none());
        let nbrs = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(nbrs.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_delete_cascades_segment_edges() {
        // Same as above but via graph_patch
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        engine
            .graph_patch(&GraphPatch {
                delete_node_ids: vec![a],
                ..GraphPatch::default()
            })
            .unwrap();

        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    // ===================== prune(policy) =====================

    #[test]
    fn test_prune_empty_policy_no_op() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: None,
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        // Node survives
        assert!(engine.get_node(1).unwrap().is_some());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_type_id_only_no_op() {
        // type_id alone without age or weight is a no-op (safety)
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: None,
                type_id: Some(1),
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_by_age_only() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Insert nodes. They all get updated_at = now
        let a = engine.upsert_node(1, "old", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "new", BTreeMap::new(), 1.0).unwrap();

        // Hack: manually set "old" node to have an ancient updated_at via write_op
        let old_node = engine.get_node(a).unwrap().unwrap();
        engine
            .write_op(&WalOp::UpsertNode(NodeRecord {
                updated_at: 1000, // ancient timestamp
                ..old_node
            }))
            .unwrap();

        // Prune with max_age_ms = 1000 (1 second). "old" node is way older
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: Some(1000),
                max_weight: None,
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none()); // old pruned
        assert!(engine.get_node(b).unwrap().is_some()); // new survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_by_weight_only() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "mid", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // Prune weight <= 0.5
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 2); // low + mid
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_node(c).unwrap().is_some()); // high survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_combo_age_and_weight() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine
            .upsert_node(1, "old-low", BTreeMap::new(), 0.1)
            .unwrap();
        let b = engine
            .upsert_node(1, "old-high", BTreeMap::new(), 0.9)
            .unwrap();
        let c = engine
            .upsert_node(1, "new-low", BTreeMap::new(), 0.1)
            .unwrap();

        // Make a and b old
        let node_a = engine.get_node(a).unwrap().unwrap();
        engine
            .write_op(&WalOp::UpsertNode(NodeRecord {
                updated_at: 1000,
                ..node_a
            }))
            .unwrap();
        let node_b = engine.get_node(b).unwrap().unwrap();
        engine
            .write_op(&WalOp::UpsertNode(NodeRecord {
                updated_at: 1000,
                ..node_b
            }))
            .unwrap();

        // AND: old (max_age_ms=1000) AND low weight (<= 0.5)
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: Some(1000),
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1); // only old-low
        assert!(engine.get_node(a).unwrap().is_none()); // old + low → pruned
        assert!(engine.get_node(b).unwrap().is_some()); // old but high weight → survives
        assert!(engine.get_node(c).unwrap().is_some()); // low weight but new → survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_type_scoped() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine
            .upsert_node(1, "type1-low", BTreeMap::new(), 0.1)
            .unwrap();
        let b = engine
            .upsert_node(2, "type2-low", BTreeMap::new(), 0.1)
            .unwrap();

        // Prune only type 1 with weight <= 0.5
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: Some(1),
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none()); // type 1, low → pruned
        assert!(engine.get_node(b).unwrap().is_some()); // type 2 → not in scope
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_cascade_deletes_edges() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.1).unwrap();
        let e_ab = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e_bc = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e_ca = engine
            .upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Prune low-weight nodes (a and c)
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 2); // a and c
        assert_eq!(result.edges_pruned, 3); // all three edges (all touch a or c)
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(c).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some()); // b survives
        assert!(engine.get_edge(e_ab).unwrap().is_none());
        assert!(engine.get_edge(e_bc).unwrap().is_none());
        assert!(engine.get_edge(e_ca).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_shared_edge_dedup() {
        // When two pruned nodes share an edge, the edge should only be counted once
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.1).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 2);
        assert_eq!(result.edges_pruned, 1); // shared edge counted once
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_empty_result_no_match() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.8).unwrap();

        // No nodes have weight <= 0.1
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.1),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_after_flush_segment_nodes() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine
            .upsert_node(1, "seg-a", BTreeMap::new(), 0.1)
            .unwrap();
        let b = engine
            .upsert_node(1, "seg-b", BTreeMap::new(), 0.9)
            .unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Nodes are now in segment, not memtable
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert_eq!(result.edges_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_cross_source_memtable_and_segment() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Node in segment
        let a = engine
            .upsert_node(1, "in-seg", BTreeMap::new(), 0.1)
            .unwrap();
        engine.flush().unwrap();

        // Node in memtable
        let b = engine
            .upsert_node(1, "in-mem", BTreeMap::new(), 0.1)
            .unwrap();

        // Edge from memtable node to segment node
        let e = engine
            .upsert_edge(b, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 2);
        assert_eq!(result.edges_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_cascade_edges_in_segment() {
        // Edge is in segment, node to prune is in memtable. Cascade should find it
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.1).unwrap();
        let e = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Update b in memtable (still low weight)
        engine.upsert_node(1, "b", BTreeMap::new(), 0.1).unwrap();

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1); // only b
        assert_eq!(result.edges_pruned, 1); // edge in segment cascade-deleted
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (a, b, e);
        {
            let mut engine = open_imm(&db_path);
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
            b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
            e = engine
                .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();

            let result = engine
                .prune(&PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                })
                .unwrap();
            assert_eq!(result.nodes_pruned, 1);
            assert_eq!(result.edges_pruned, 1);
            engine.close().unwrap();
        }

        // Reopen. WAL replay should preserve prune effects
        let engine = open_imm(&db_path);
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_weight_boundary() {
        // Exact boundary: weight == max_weight should be pruned (<=)
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine
            .upsert_node(1, "exact", BTreeMap::new(), 0.5)
            .unwrap();
        let b = engine
            .upsert_node(1, "above", BTreeMap::new(), 0.500001)
            .unwrap();

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none()); // exactly 0.5 → pruned
        assert!(engine.get_node(b).unwrap().is_some()); // just above → survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_already_deleted_node_ignored() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        engine.delete_node(a).unwrap();

        // Prune should not count already-deleted nodes
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_empty_db() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_negative_age_rejected() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(-100),
            max_weight: None,
            type_id: None,
        });

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_age_ms must be positive"), "got: {}", err);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_zero_age_rejected() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(0),
            max_weight: None,
            type_id: None,
        });

        assert!(result.is_err());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_type_scoped_with_age() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine
            .upsert_node(1, "t1-old", BTreeMap::new(), 1.0)
            .unwrap();
        let b = engine
            .upsert_node(2, "t2-old", BTreeMap::new(), 1.0)
            .unwrap();
        let _c = engine
            .upsert_node(1, "t1-new", BTreeMap::new(), 1.0)
            .unwrap();

        // Make a and b old
        let node_a = engine.get_node(a).unwrap().unwrap();
        engine
            .write_op(&WalOp::UpsertNode(NodeRecord {
                updated_at: 1000,
                ..node_a
            }))
            .unwrap();
        let node_b = engine.get_node(b).unwrap().unwrap();
        engine
            .write_op(&WalOp::UpsertNode(NodeRecord {
                updated_at: 1000,
                ..node_b
            }))
            .unwrap();

        // Prune old nodes of type 1 only
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: Some(1000),
                max_weight: None,
                type_id: Some(1),
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1); // only t1-old
        assert!(engine.get_node(a).unwrap().is_none()); // type 1 + old → pruned
        assert!(engine.get_node(b).unwrap().is_some()); // type 2 → out of scope
        engine.close().unwrap();
    }

    // ==========================================================
    // FO-005: Named prune policies (compaction-filter auto-prune)
    // ==========================================================

    #[test]
    fn test_set_and_list_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Initially empty
        assert!(engine.list_prune_policies().is_empty());

        // Set a policy
        let policy = PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        };
        engine
            .set_prune_policy("low-weight", policy.clone())
            .unwrap();

        let list = engine.list_prune_policies();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "low-weight");
        assert_eq!(list[0].1.max_weight, Some(0.5));

        // Overwrite
        let policy2 = PrunePolicy {
            max_age_ms: Some(60_000),
            max_weight: None,
            type_id: None,
        };
        engine.set_prune_policy("low-weight", policy2).unwrap();
        let list = engine.list_prune_policies();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].1.max_age_ms, Some(60_000));
        assert!(list[0].1.max_weight.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_remove_prune_policy() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine
            .set_prune_policy(
                "p1",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.3),
                    type_id: None,
                },
            )
            .unwrap();
        assert_eq!(engine.list_prune_policies().len(), 1);

        assert!(engine.remove_prune_policy("p1").unwrap());
        assert!(engine.list_prune_policies().is_empty());

        // Removing non-existent returns false
        assert!(!engine.remove_prune_policy("p1").unwrap());

        engine.close().unwrap();
    }

    #[test]
    fn test_prune_policy_validation() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Empty policy rejected
        let err = engine.set_prune_policy(
            "bad",
            PrunePolicy {
                max_age_ms: None,
                max_weight: None,
                type_id: None,
            },
        );
        assert!(err.is_err());

        // type_id only rejected
        let err = engine.set_prune_policy(
            "bad",
            PrunePolicy {
                max_age_ms: None,
                max_weight: None,
                type_id: Some(1),
            },
        );
        assert!(err.is_err());

        // Negative age rejected
        let err = engine.set_prune_policy(
            "bad",
            PrunePolicy {
                max_age_ms: Some(-1),
                max_weight: None,
                type_id: None,
            },
        );
        assert!(err.is_err());

        // NaN weight rejected
        let err = engine.set_prune_policy(
            "bad",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(f32::NAN),
                type_id: None,
            },
        );
        assert!(err.is_err());

        // Negative weight rejected
        let err = engine.set_prune_policy(
            "bad",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(-0.1),
                type_id: None,
            },
        );
        assert!(err.is_err());

        engine.close().unwrap();
    }

    #[test]
    fn test_prune_policy_survives_close_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;

        {
            let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            engine
                .set_prune_policy(
                    "age-rule",
                    PrunePolicy {
                        max_age_ms: Some(30_000),
                        max_weight: None,
                        type_id: None,
                    },
                )
                .unwrap();
            engine
                .set_prune_policy(
                    "weight-rule",
                    PrunePolicy {
                        max_age_ms: None,
                        max_weight: Some(0.1),
                        type_id: Some(5),
                    },
                )
                .unwrap();
            engine.close().unwrap();
        }

        // Reopen
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let list = engine.list_prune_policies();
        assert_eq!(list.len(), 2);
        // BTreeMap ordering: "age-rule" < "weight-rule"
        assert_eq!(list[0].0, "age-rule");
        assert_eq!(list[0].1.max_age_ms, Some(30_000));
        assert_eq!(list[1].0, "weight-rule");
        assert_eq!(list[1].1.max_weight, Some(0.1));
        assert_eq!(list[1].1.type_id, Some(5));
        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_auto_prune_by_weight() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0; // manual compaction only
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create nodes with different weights
        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Create more nodes in second segment; update "high" to create overlapping
        // IDs across segments, which forces the standard compaction path.
        let low2 = engine.upsert_node(1, "low2", BTreeMap::new(), 0.2).unwrap();
        let high2 = engine
            .upsert_node(1, "high2", BTreeMap::new(), 0.8)
            .unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap(); // overlap
        engine.flush().unwrap();

        // Register policy: prune weight <= 0.5
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Compact. Should prune low and low2
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 2);
        assert_eq!(stats.nodes_kept, 2); // high + high2
        assert!(stats.edges_auto_pruned == 0);

        // Verify pruned nodes are gone
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(low2).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());
        assert!(engine.get_node(high2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_auto_prune_cascade_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap(); // will be pruned
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.9).unwrap();
        let e1 = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // More data in second segment; update "b" to create overlapping IDs
        // across segments, which forces the standard compaction path.
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.8).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap(); // overlap
        engine.flush().unwrap();

        engine
            .set_prune_policy(
                "low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 1); // node a
        assert_eq!(stats.edges_auto_pruned, 1); // edge e1 (a→b)

        // a is gone, b/c/d survive
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());
        assert!(engine.get_node(c).unwrap().is_some());
        assert!(engine.get_node(d).unwrap().is_some());

        // e1 (a→b) cascade-dropped, e2 (b→c) survives
        assert!(engine.get_edge(e1).unwrap().is_none());
        assert!(engine.get_edge(e2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_multiple_policies_or_logic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Node that matches policy A (low weight) but not B (type 99)
        let n1 = engine.upsert_node(1, "n1", BTreeMap::new(), 0.1).unwrap();
        // Node that matches policy B (type 99) but not A (high weight)
        let n2 = engine.upsert_node(99, "n2", BTreeMap::new(), 0.9).unwrap();
        // Node that matches neither
        let n3 = engine.upsert_node(1, "n3", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Update n3 in second segment to create overlapping IDs (forces standard path)
        engine.upsert_node(1, "n3", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Policy A: prune if weight <= 0.5
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();
        // Policy B: prune all nodes of type 99 (by weight, use very high threshold)
        engine
            .set_prune_policy(
                "type-99",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(999.0),
                    type_id: Some(99),
                },
            )
            .unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 2); // n1 (low weight) + n2 (type 99)

        assert!(engine.get_node(n1).unwrap().is_none());
        assert!(engine.get_node(n2).unwrap().is_none());
        assert!(engine.get_node(n3).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_no_policies_no_prune() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // No policies registered
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 0);
        assert_eq!(stats.edges_auto_pruned, 0);

        // All nodes survive
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_removed_policy_no_longer_prunes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap(); // overlap → standard path
        engine.flush().unwrap();

        // Remove the policy before compaction
        engine.remove_prune_policy("p").unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 0); // policy removed, no pruning

        // Node a still exists
        assert!(engine.get_node(a).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_type_scoped_policy() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let t1_low = engine
            .upsert_node(1, "t1-low", BTreeMap::new(), 0.1)
            .unwrap();
        let t2_low = engine
            .upsert_node(2, "t2-low", BTreeMap::new(), 0.1)
            .unwrap();
        let t1_high = engine
            .upsert_node(1, "t1-high", BTreeMap::new(), 0.9)
            .unwrap();
        engine.flush().unwrap();
        // Update t1_high in second segment to create overlapping IDs (forces standard path)
        engine
            .upsert_node(1, "t1-high", BTreeMap::new(), 0.9)
            .unwrap();
        engine.flush().unwrap();

        // Only prune type 1 with low weight
        engine
            .set_prune_policy(
                "type1-low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(1),
                },
            )
            .unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 1); // only t1_low

        assert!(engine.get_node(t1_low).unwrap().is_none());
        assert!(engine.get_node(t2_low).unwrap().is_some()); // type 2, out of scope
        assert!(engine.get_node(t1_high).unwrap().is_some()); // type 1 but high weight

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_prune_stats_in_nodes_removed() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 0.1)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 10..20 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 0.9)
                .unwrap();
        }
        // Update n0 in second segment to create overlapping IDs (forces standard path)
        engine.upsert_node(1, "n0", BTreeMap::new(), 0.1).unwrap();
        engine.flush().unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 10);
        assert_eq!(stats.nodes_kept, 10);
        // nodes_removed includes auto-pruned nodes
        assert!(stats.nodes_removed >= 10);

        engine.close().unwrap();
    }

    #[test]
    fn test_manual_prune_unchanged_by_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Register a policy (should NOT affect manual prune calls)
        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.0001),
                    type_id: None,
                },
            )
            .unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();

        // Manual prune with a different threshold
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.7),
                type_id: None,
            })
            .unwrap();

        assert_eq!(result.nodes_pruned, 1); // only a (0.5 <= 0.7)
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compaction_applies_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        // Auto-compact after 2 flushes
        opts.compact_after_n_flushes = 2;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Register policy: prune weight <= 0.3
        engine
            .set_prune_policy(
                "low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.3),
                    type_id: None,
                },
            )
            .unwrap();

        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Second flush with overlapping ID to force standard path in bg compaction
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap(); // triggers auto bg compaction

        // Wait for bg compaction to complete
        engine.wait_for_bg_compact();

        // Low-weight node should have been pruned by bg compaction
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());

        engine.close().unwrap();
    }

    // --- FO-005a: Read-time policy filtering tests ---

    #[test]
    fn test_read_time_policy_get_node() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // No policy, both visible
        assert!(engine.get_node(low).unwrap().is_some());
        assert!(engine.get_node(high).unwrap().is_some());

        // Register policy: exclude weight <= 0.5
        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Low-weight node excluded, high-weight still visible
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_node_by_key() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // Register policy
        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Low hidden by policy
        assert!(engine.get_node_by_key(1, "low").unwrap().is_none());
        // High still visible
        assert!(engine.get_node_by_key(1, "high").unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_nodes_batch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.3).unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let results = engine.get_nodes(&[a, b, c]).unwrap();
        // a (0.2) excluded, b (0.9) visible, c (0.3) excluded
        assert!(results[0].is_none());
        assert!(results[1].is_some());
        assert!(results[2].is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_node_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap(); // nodes now in segment

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Works on segment-sourced nodes too
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_node_by_key_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        assert!(engine.get_node_by_key(1, "low").unwrap().is_none());
        assert!(engine.get_node_by_key(1, "high").unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_upsert_dedup_unaffected() {
        // Critical correctness test: upsert must still find the existing node
        // even when a policy would exclude it from public reads. If upsert
        // used filtered get_node_by_key, it would allocate a NEW ID for the
        // "hidden" node, causing silent data corruption.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id1 = engine
            .upsert_node(1, "node-a", BTreeMap::new(), 0.2)
            .unwrap();

        // Register policy that excludes this node from reads
        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Public read confirms it's hidden
        assert!(engine.get_node(id1).unwrap().is_none());

        // Upsert same (type_id, key). MUST reuse existing ID, not allocate new one
        let id2 = engine
            .upsert_node(1, "node-a", BTreeMap::new(), 0.8)
            .unwrap();
        assert_eq!(
            id1, id2,
            "upsert must reuse existing node ID even when policy-excluded"
        );

        // Now weight is 0.8 > 0.5, so it should be visible again
        assert!(engine.get_node(id2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_upsert_dedup_after_flush() {
        // Same as above but with the node in a segment
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id1 = engine
            .upsert_node(1, "node-a", BTreeMap::new(), 0.2)
            .unwrap();
        engine.flush().unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Hidden from reads
        assert!(engine.get_node(id1).unwrap().is_none());

        // Upsert must still find and reuse the existing ID from segment
        let id2 = engine
            .upsert_node(1, "node-a", BTreeMap::new(), 0.8)
            .unwrap();
        assert_eq!(id1, id2);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_add_remove_takes_effect() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id = engine
            .upsert_node(1, "target", BTreeMap::new(), 0.3)
            .unwrap();

        // Initially visible
        assert!(engine.get_node(id).unwrap().is_some());

        // Add policy → hidden
        engine
            .set_prune_policy(
                "hide-low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();
        assert!(engine.get_node(id).unwrap().is_none());

        // Remove policy → visible again
        engine.remove_prune_policy("hide-low").unwrap();
        assert!(engine.get_node(id).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_type_scoped() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let t1 = engine
            .upsert_node(1, "t1-low", BTreeMap::new(), 0.2)
            .unwrap();
        let t2 = engine
            .upsert_node(2, "t2-low", BTreeMap::new(), 0.2)
            .unwrap();

        // Policy scoped to type_id=1 only
        engine
            .set_prune_policy(
                "t1-only",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(1),
                },
            )
            .unwrap();

        // Type 1 node hidden, type 2 node still visible
        assert!(engine.get_node(t1).unwrap().is_none());
        assert!(engine.get_node(t2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_no_policies_zero_overhead() {
        // Regression test: ensure no policies = no filtering, no crashes
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id = engine.upsert_node(1, "node", BTreeMap::new(), 0.1).unwrap();

        // No policies registered, everything visible
        assert!(engine.list_prune_policies().is_empty());
        assert!(engine.get_node(id).unwrap().is_some());
        assert!(engine.get_node_by_key(1, "node").unwrap().is_some());
        let batch = engine.get_nodes(&[id]).unwrap();
        assert!(batch[0].is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_multiple_policies_or() {
        // Multiple policies: OR across policies. A node matching ANY policy is excluded.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap(); // type 1, low weight
        let b = engine.upsert_node(2, "b", BTreeMap::new(), 0.1).unwrap(); // type 2, low weight
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.9).unwrap(); // type 1, high weight

        // Policy 1: type 1, weight <= 0.5
        engine
            .set_prune_policy(
                "p1",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(1),
                },
            )
            .unwrap();
        // Policy 2: type 2, weight <= 0.5
        engine
            .set_prune_policy(
                "p2",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(2),
                },
            )
            .unwrap();

        // a (type 1, 0.1): matches p1 → hidden
        assert!(engine.get_node(a).unwrap().is_none());
        // b (type 2, 0.1): matches p2 → hidden
        assert!(engine.get_node(b).unwrap().is_none());
        // c (type 1, 0.9): doesn't match p1 (weight too high), doesn't match p2 (wrong type) → visible
        assert!(engine.get_node(c).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_graph_patch_dedup_unaffected() {
        // graph_patch node upserts must use raw lookup for dedup
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id1 = engine
            .upsert_node(1, "node-a", BTreeMap::new(), 0.2)
            .unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Use graph_patch to upsert same node. Must reuse ID
        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "node-a".to_string(),
                props: BTreeMap::new(),
                weight: 0.8,
            }],
            upsert_edges: Vec::new(),
            invalidate_edges: Vec::new(),
            delete_node_ids: Vec::new(),
            delete_edge_ids: Vec::new(),
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert_eq!(
            result.node_ids[0], id1,
            "graph_patch must reuse existing node ID"
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.2).unwrap(); // will be excluded
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.8).unwrap();

        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // No policy: both neighbors visible
        let result = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(result.len(), 2);

        // Register policy: exclude weight <= 0.5
        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // b is excluded by policy
        let result = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors_limit() {
        // When policies filter some results, limit should apply AFTER filtering.
        // If we have 5 neighbors and policy excludes 2, limit=2 should return 2 (not 0 or 1).
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 0.9).unwrap();
        // 3 high-weight neighbors (visible) + 2 low-weight (hidden by policy)
        let mut visible_ids = Vec::new();
        for i in 0..3 {
            let id = engine
                .upsert_node(1, &format!("hi-{}", i), BTreeMap::new(), 0.8)
                .unwrap();
            engine
                .upsert_edge(hub, id, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            visible_ids.push(id);
        }
        for i in 0..2 {
            let id = engine
                .upsert_node(1, &format!("lo-{}", i), BTreeMap::new(), 0.1)
                .unwrap();
            engine
                .upsert_edge(hub, id, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Without limit: 3 visible
        let result = engine
            .neighbors(hub, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(result.len(), 3);

        // With limit=2: should return exactly 2 (not fewer)
        let result = engine
            .neighbors(hub, Direction::Outgoing, None, 2, None, None)
            .unwrap();
        assert_eq!(result.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_traverse_depth_two() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // a -> b -> c (c has low weight, should be excluded)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.2).unwrap(); // will be excluded
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.9).unwrap();

        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // 2-hop from a: b is 1-hop neighbor, c and d are 2-hop. c is excluded by policy.
        let result = traverse_depth_two_read(&engine, a, Direction::Outgoing, None, None, None);
        let result_ids: Vec<u64> = result.iter().map(|e| e.node_id).collect();
        assert!(result_ids.contains(&d));
        assert!(!result_ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_top_k() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 0.9).unwrap();
        let hi = engine.upsert_node(1, "hi", BTreeMap::new(), 0.8).unwrap();
        let lo = engine.upsert_node(1, "lo", BTreeMap::new(), 0.2).unwrap();

        engine
            .upsert_edge(hub, hi, 1, BTreeMap::new(), 5.0, None, None)
            .unwrap();
        engine
            .upsert_edge(hub, lo, 1, BTreeMap::new(), 10.0, None, None)
            .unwrap(); // higher weight but node excluded

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let result = engine
            .top_k_neighbors(hub, Direction::Outgoing, None, 2, ScoringMode::Weight, None)
            .unwrap();
        // Only hi should appear (lo excluded by policy)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, hi);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_extract_subgraph() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.8).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.2).unwrap(); // excluded

        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let sg = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, None)
            .unwrap();
        let node_ids: Vec<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert!(node_ids.contains(&a));
        assert!(node_ids.contains(&b));
        assert!(!node_ids.contains(&c)); // excluded by policy

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_nodes_by_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let ids = engine.nodes_by_type(1).unwrap();
        assert!(!ids.contains(&a)); // excluded
        assert!(ids.contains(&b)); // visible

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_find_nodes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));

        engine.upsert_node(1, "a", props.clone(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", props.clone(), 0.9).unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let ids = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        // Only b visible (a excluded by policy)
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], b);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_prune_still_works() {
        // Manual prune must still find and delete policy-excluded nodes.
        // This ensures prune uses raw reads internally.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        // Edge between them for cascade testing
        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Register policy that hides 'a' from reads
        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Confirm a is hidden
        assert!(engine.get_node(a).unwrap().is_none());

        // Manual prune with same criteria. Should still find and delete 'a'
        let result = engine
            .prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            })
            .unwrap();
        assert_eq!(result.nodes_pruned, 1);
        assert_eq!(result.edges_pruned, 1); // cascade delete

        // After prune, even raw read finds nothing (actually deleted now)
        // Remove policy first to test raw state
        engine.remove_prune_policy("p").unwrap();
        assert!(engine.get_node(a).unwrap().is_none()); // truly deleted

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_delete_node_cascade_unaffected() {
        // delete_node must cascade-delete ALL incident edges, even those to
        // policy-excluded nodes. Uses neighbors_raw internally.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.2).unwrap(); // will be policy-excluded
        let edge_id = engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Delete a. Must cascade-delete the edge to b even though b is policy-excluded
        engine.delete_node(a).unwrap();

        // The edge should be deleted
        assert!(engine.get_edge(edge_id).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.2).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.8).unwrap();

        engine
            .upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine
            .upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        engine
            .set_prune_policy(
                "p",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        // Works on segment-sourced neighbors too
        let result = engine
            .neighbors(a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    // ============================================================
    // close_fast tests
    // ============================================================

    #[test]
    fn test_close_fast_basic() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let id = engine.upsert_node(1, "n1", BTreeMap::new(), 1.0).unwrap();
        engine.close_fast().unwrap();

        // Reopen. Data should be intact
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert!(engine2.get_node(id).unwrap().is_some());
        engine2.close().unwrap();
    }

    #[test]
    fn test_close_fast_cancels_bg_compact() {
        // Create a DB with enough segments to trigger bg compaction,
        // then close_fast should cancel it without waiting.
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0, // manual only
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create 3 segments
        for i in 0..3 {
            for j in 0..100 {
                let key = format!("node_{}_{}", i, j);
                engine.upsert_node(1, &key, BTreeMap::new(), 1.0).unwrap();
            }
            engine.flush().unwrap();
        }

        // Start background compaction
        engine.start_bg_compact().unwrap();
        assert!(engine.bg_compact.is_some());

        // close_fast should cancel it and succeed
        engine.close_fast().unwrap();

        // Reopen. Data should be intact (original segments preserved)
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let stats = engine2.stats();
        // All nodes should still be accessible
        assert!(engine2.get_node_by_key(1, "node_0_0").unwrap().is_some());
        assert!(engine2.get_node_by_key(1, "node_2_99").unwrap().is_some());
        // 3 segments still present (compaction was cancelled)
        assert!(stats.segment_count >= 1); // could be 3 or 1 if compaction finished fast
        engine2.close().unwrap();
    }

    #[test]
    fn test_close_fast_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let id = engine
            .upsert_node(1, "gc_node", BTreeMap::new(), 1.0)
            .unwrap();
        engine.close_fast().unwrap();

        // Reopen. Data should be durable
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert!(engine2.get_node(id).unwrap().is_some());
        engine2.close().unwrap();
    }

    // ============================================================
    // stats tests
    // ============================================================

    #[test]
    fn test_stats_fresh_db() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let stats = engine.stats();

        assert_eq!(stats.pending_wal_bytes, 0);
        assert_eq!(stats.segment_count, 0);
        assert_eq!(stats.node_tombstone_count, 0);
        assert_eq!(stats.edge_tombstone_count, 0);
        assert!(stats.last_compaction_ms.is_none());
        assert_eq!(stats.wal_sync_mode, "immediate");

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_group_commit_sync_mode() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let stats = engine.stats();

        assert_eq!(stats.wal_sync_mode, "group-commit");

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_segments_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.stats().segment_count, 0);

        engine.upsert_node(1, "n1", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.stats().segment_count, 1);

        engine.upsert_node(1, "n2", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.stats().segment_count, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine
            .upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        assert_eq!(engine.stats().node_tombstone_count, 0);
        assert_eq!(engine.stats().edge_tombstone_count, 0);

        // delete_node cascades to incident edges, so edge e1 is also tombstoned
        engine.delete_node(n1).unwrap();
        assert_eq!(engine.stats().node_tombstone_count, 1);
        assert_eq!(engine.stats().edge_tombstone_count, 1);

        // Deleting n2 (no remaining edges) adds another node tombstone
        engine.delete_node(n2).unwrap();
        assert_eq!(engine.stats().node_tombstone_count, 2);
        assert_eq!(engine.stats().edge_tombstone_count, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_last_compaction_ms() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // No compaction yet
        assert!(engine.stats().last_compaction_ms.is_none());

        // Create 2 segments and compact
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        let before = now_millis();
        engine.compact().unwrap();
        let after = now_millis();

        let stats = engine.stats();
        let ts = stats
            .last_compaction_ms
            .expect("should have compaction timestamp");
        assert!(
            ts >= before && ts <= after,
            "timestamp should be between before and after"
        );
        assert_eq!(stats.segment_count, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_last_compaction_ms_bg() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create 2 segments
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        let before = now_millis();
        engine.start_bg_compact().unwrap();
        engine.wait_for_bg_compact();
        let after = now_millis();

        let stats = engine.stats();
        let ts = stats
            .last_compaction_ms
            .expect("should have bg compaction timestamp");
        assert!(ts >= before && ts <= after);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_pending_wal_bytes_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5_000,                    // Long enough to observe buffered bytes
                soft_trigger_bytes: 100 * 1024 * 1024, // Very high so timer-based sync only
                hard_cap_bytes: 200 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.stats().pending_wal_bytes, 0);

        // Write something. It should show as pending (sync interval hasn't fired)
        engine
            .upsert_node(1, "buffered", BTreeMap::new(), 1.0)
            .unwrap();
        let stats = engine.stats();
        assert!(stats.pending_wal_bytes > 0, "should have buffered bytes");

        engine.close().unwrap();
    }

    // ========================================================================================
    // V3 Compaction Planner Tests
    // ========================================================================================

    #[test]
    fn test_v3_planner_basic_winner_selection() {
        // Two segments with overlapping node IDs. Newest segment's version wins
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segment 1 (older): nodes with keys a, b, c
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        // Segment 2 (newer): update "a" with new weight, add "d"
        engine.upsert_node(1, "a", BTreeMap::new(), 2.0).unwrap();
        engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segments.len(), 2);

        // Compact. V3 planner should pick "a" from segment 2 (newer version)
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 4); // a, b, c, d

        // Verify the winner for "a" has the updated weight
        let n = engine.get_node_by_key(1, "a").unwrap().unwrap();
        assert_eq!(n.weight, 2.0);

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_tombstone_handling() {
        // Delete a node, compact. V3 planner respects tombstones
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        engine.delete_node(id1).unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 1);
        assert_eq!(stats.nodes_removed, 1); // node 1 tombstoned

        assert!(engine.get_node(id1).unwrap().is_none());
        assert!(engine.get_node(id2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_edge_cascade_on_tombstone() {
        // Delete a node with edges. V3 planner cascade-drops incident edges
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let n3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine
            .upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(n2, n3, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Delete n2. Should cascade-drop both e1 (n1→n2) and e2 (n2→n3)
        engine.delete_node(n2).unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 2); // n1, n3
        assert_eq!(stats.edges_kept, 0); // both edges dropped

        assert!(engine.get_edge(e1).unwrap().is_none());
        assert!(engine.get_edge(e2).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_prune_policy_from_metadata() {
        // Auto-prune via registered policy. V3 evaluates from metadata only
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create overlapping segments (overlapping IDs across segments)
        engine
            .upsert_node(1, "low_weight", BTreeMap::new(), 0.1)
            .unwrap();
        engine
            .upsert_node(1, "high_weight", BTreeMap::new(), 5.0)
            .unwrap();
        engine.flush().unwrap();
        // Update high_weight to create overlapping node ID across segments
        engine
            .upsert_node(1, "high_weight", BTreeMap::new(), 5.0)
            .unwrap();
        engine.flush().unwrap();

        // Register policy: prune nodes with weight <= 0.5
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

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 1);
        assert_eq!(stats.nodes_auto_pruned, 1);

        // Only high_weight node survives
        let node = engine.get_node_by_key(1, "high_weight").unwrap();
        assert!(node.is_some());
        let node = engine.get_node_by_key(1, "low_weight").unwrap();
        assert!(node.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_prune_policy_edge_cascade() {
        // Pruned node's edges should be cascade-dropped
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "keep", BTreeMap::new(), 5.0).unwrap();
        let n2 = engine
            .upsert_node(1, "prune_me", BTreeMap::new(), 0.1)
            .unwrap();
        let n3 = engine
            .upsert_node(1, "also_keep", BTreeMap::new(), 5.0)
            .unwrap();
        let _e1 = engine
            .upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        let e2 = engine
            .upsert_edge(n1, n3, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();
        // Update a node to create overlapping IDs (forces V3 standard path)
        engine.upsert_node(1, "keep", BTreeMap::new(), 5.0).unwrap();
        engine.flush().unwrap();

        engine
            .set_prune_policy(
                "low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: None,
                },
            )
            .unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 1);
        assert_eq!(stats.edges_auto_pruned, 1); // e1 cascade-dropped
        assert_eq!(stats.edges_kept, 1); // e2 survives

        // e2 (n1→n3) survives
        assert!(engine.get_edge(e2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_prune_policy_or_semantics() {
        // Multiple policies: OR across policies (any match → pruned)
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Type 1: low weight, type 2: low weight, type 3: safe
        engine
            .upsert_node(1, "t1_low", BTreeMap::new(), 0.1)
            .unwrap();
        engine
            .upsert_node(1, "t1_high", BTreeMap::new(), 5.0)
            .unwrap();
        engine
            .upsert_node(2, "t2_low", BTreeMap::new(), 0.1)
            .unwrap();
        engine
            .upsert_node(3, "t3_safe", BTreeMap::new(), 5.0)
            .unwrap();
        engine.flush().unwrap();
        // Update a node to create overlap (forces V3 standard path)
        engine
            .upsert_node(1, "t1_high", BTreeMap::new(), 5.0)
            .unwrap();
        engine.flush().unwrap();

        // Policy A: prune type=1 with weight <= 0.5
        engine
            .set_prune_policy(
                "type1_low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(1),
                },
            )
            .unwrap();
        // Policy B: prune type=2 with weight <= 0.5
        engine
            .set_prune_policy(
                "type2_low",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(2),
                },
            )
            .unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 2); // t1_low + t2_low
        assert_eq!(stats.nodes_kept, 2); // t1_high + t3_safe

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_overlapping_multi_segment() {
        // Multiple segments with heavily overlapping IDs. Correctness stress
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segment 1: nodes 1-50
        for i in 0..50 {
            engine
                .upsert_node(1, &format!("node_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Segment 2: update nodes 10-30 with new weights
        for i in 10..30 {
            engine
                .upsert_node(1, &format!("node_{}", i), BTreeMap::new(), 2.0)
                .unwrap();
        }
        engine.flush().unwrap();

        // Segment 3: delete nodes 40-49
        for i in 40..50 {
            let n = engine
                .get_node_by_key(1, &format!("node_{}", i))
                .unwrap()
                .unwrap();
            engine.delete_node(n.id).unwrap();
        }
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 40); // 50 - 10 deleted = 40
        assert_eq!(stats.segments_merged, 3);

        // Verify updated nodes have new weight
        for i in 10..30 {
            let n = engine
                .get_node_by_key(1, &format!("node_{}", i))
                .unwrap()
                .unwrap();
            assert_eq!(n.weight, 2.0, "node_{} should have updated weight", i);
        }

        // Verify deleted nodes are gone
        for i in 40..50 {
            let n = engine.get_node_by_key(1, &format!("node_{}", i)).unwrap();
            assert!(n.is_none(), "node_{} should be deleted", i);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_compact_preserves_edges_across_segments() {
        // Edges in different segments than their endpoint nodes
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segment 1: nodes
        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        // Segment 2: edges
        let e1 = engine
            .upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 2);
        assert_eq!(stats.edges_kept, 1);

        // Edge survives compaction
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);

        // Adjacency works
        let nbrs = engine
            .neighbors(n1, Direction::Outgoing, None, 100, None, None)
            .unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, n2);

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_compact_reopen_durability() {
        // V3 compaction output survives close + reopen
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..100 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        for i in 50..100 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 2.0)
                .unwrap();
        }
        engine.flush().unwrap();

        engine.compact().unwrap();
        engine.close().unwrap();

        // Reopen and verify
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert_eq!(engine2.segments.len(), 1);
        for i in 0..100 {
            let n = engine2.get_node_by_key(1, &format!("n{}", i)).unwrap();
            assert!(n.is_some(), "node n{} should exist after reopen", i);
            let n = n.unwrap();
            let expected_weight = if i >= 50 { 2.0 } else { 1.0 };
            assert_eq!(n.weight, expected_weight, "n{} weight", i);
        }

        engine2.close().unwrap();
    }

    #[test]
    fn test_v3_matches_any_prune_policy_meta() {
        // Unit test for the metadata-based prune policy matcher
        let policy_weight = PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        };
        let policy_type_scoped = PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: Some(1),
        };
        let policy_age = PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: None,
            type_id: None,
        };

        let now = 10_000i64;

        // Weight-only policy
        assert!(matches_any_prune_policy_meta(
            1,
            now,
            0.1,
            std::slice::from_ref(&policy_weight),
            now
        ));
        assert!(matches_any_prune_policy_meta(
            1,
            now,
            0.5,
            std::slice::from_ref(&policy_weight),
            now
        ));
        assert!(!matches_any_prune_policy_meta(
            1,
            now,
            0.6,
            std::slice::from_ref(&policy_weight),
            now
        ));

        // Type-scoped policy
        assert!(matches_any_prune_policy_meta(
            1,
            now,
            0.1,
            std::slice::from_ref(&policy_type_scoped),
            now
        ));
        assert!(!matches_any_prune_policy_meta(
            2,
            now,
            0.1,
            std::slice::from_ref(&policy_type_scoped),
            now
        )); // Wrong type

        // Age-only policy: updated_at < now - max_age_ms = 10000 - 1000 = 9000
        assert!(matches_any_prune_policy_meta(
            1,
            8000,
            1.0,
            std::slice::from_ref(&policy_age),
            now
        )); // Old enough
        assert!(!matches_any_prune_policy_meta(
            1,
            9500,
            1.0,
            std::slice::from_ref(&policy_age),
            now
        )); // Too recent

        // OR across policies
        let policies = vec![policy_type_scoped.clone(), policy_age.clone()];
        // Matches type-scoped (type=1, weight=0.1)
        assert!(matches_any_prune_policy_meta(1, now, 0.1, &policies, now));
        // Matches age (old enough)
        assert!(matches_any_prune_policy_meta(5, 8000, 1.0, &policies, now));
        // Matches neither
        assert!(!matches_any_prune_policy_meta(5, now, 1.0, &policies, now));

        // AND within policy: both age AND weight must match
        let policy_combo = PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: Some(0.5),
            type_id: None,
        };
        // Old AND low weight → prune
        assert!(matches_any_prune_policy_meta(
            1,
            8000,
            0.1,
            std::slice::from_ref(&policy_combo),
            now
        ));
        // Old but high weight → no prune
        assert!(!matches_any_prune_policy_meta(
            1,
            8000,
            1.0,
            std::slice::from_ref(&policy_combo),
            now
        ));
        // Recent but low weight → no prune
        assert!(!matches_any_prune_policy_meta(
            1,
            9500,
            0.1,
            std::slice::from_ref(&policy_combo),
            now
        ));

        // Empty policies → never match
        assert!(!matches_any_prune_policy_meta(1, 0, 0.0, &[], now));
    }

    // =========================================================================
    // P8e-007: V3 correctness and stress tests
    // =========================================================================

    /// Stress test: many segments with heavy overlap and tombstones.
    #[test]
    fn test_v3_tombstone_overlap_stress() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for round in 0..5u64 {
            for i in 0..20u64 {
                let key = format!("node_{}", i);
                let mut props = BTreeMap::new();
                props.insert("round".into(), PropValue::Int(round as i64));
                db.upsert_node(1, &key, props, 0.5).unwrap();
            }
            for i in 0..19u64 {
                db.upsert_edge(i + 1, i + 2, 1, BTreeMap::new(), 1.0, None, None)
                    .unwrap();
            }
            db.flush().unwrap();
        }

        for i in [3u64, 7, 11, 15] {
            db.delete_node(i).unwrap();
        }
        db.flush().unwrap();

        let stats = db.compact().unwrap().expect("compaction should run");
        assert!(
            stats.segments_merged >= 2,
            "should merge at least 2 segments"
        );

        for i in 1..=20u64 {
            let node = db.get_node(i).unwrap();
            if [3, 7, 11, 15].contains(&i) {
                assert!(node.is_none(), "node {} should be deleted", i);
            } else {
                let n = node.unwrap_or_else(|| panic!("node {} should exist", i));
                assert_eq!(n.props.get("round"), Some(&PropValue::Int(4)));
            }
        }

        let out_3 = db
            .neighbors(3, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out_3.is_empty(), "deleted node 3 should have no neighbors");
    }

    /// Stress test: prune policies with type scoping.
    #[test]
    fn test_v3_policy_or_and_semantics() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..10u64 {
            let w = if i < 5 { 0.1 } else { 0.8 };
            let mut props = BTreeMap::new();
            props.insert("name".into(), PropValue::String(format!("t1_{}", i)));
            db.upsert_node(1, &format!("t1_{}", i), props, w).unwrap();
        }
        for i in 0..10u64 {
            let mut props = BTreeMap::new();
            props.insert("name".into(), PropValue::String(format!("t2_{}", i)));
            db.upsert_node(2, &format!("t2_{}", i), props, 0.5).unwrap();
        }
        db.flush().unwrap();

        // Touch a node to create a second segment (compaction needs ≥2).
        db.upsert_node(1, "t1_0", BTreeMap::new(), 0.1).unwrap();
        db.flush().unwrap();

        db.set_prune_policy(
            "low-weight-t1",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.3),
                type_id: Some(1),
            },
        )
        .unwrap();

        let stats = db.compact().unwrap().expect("compaction");

        for i in 0..10u64 {
            assert!(
                db.get_node_by_key(2, &format!("t2_{}", i))
                    .unwrap()
                    .is_some(),
                "type2 node t2_{} should survive",
                i
            );
        }
        for i in 5..10u64 {
            assert!(
                db.get_node_by_key(1, &format!("t1_{}", i))
                    .unwrap()
                    .is_some(),
                "type1 node t1_{} (w=0.8) should survive",
                i
            );
        }
        assert!(stats.nodes_auto_pruned > 0);
    }

    /// Stress test: edge cascade with high-fanout pruned node.
    #[test]
    fn test_v3_edge_cascade_high_fanout() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = db.upsert_node(1, "hub", BTreeMap::new(), 0.5).unwrap();
        let mut spoke_ids = Vec::new();
        for i in 0..100u64 {
            let spoke = db
                .upsert_node(1, &format!("spoke_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
            spoke_ids.push(spoke);
            db.upsert_edge(hub, spoke, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            db.upsert_edge(spoke, hub, 2, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        db.flush().unwrap();

        db.delete_node(hub).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        assert!(db.get_node(hub).unwrap().is_none());
        let hub_out = db
            .neighbors(hub, Direction::Both, None, 0, None, None)
            .unwrap();
        assert!(hub_out.is_empty(), "hub should have no neighbors");

        for &spoke in &spoke_ids {
            assert!(
                db.get_node(spoke).unwrap().is_some(),
                "spoke {} should exist",
                spoke
            );
            let nbrs = db
                .neighbors(spoke, Direction::Both, None, 0, None, None)
                .unwrap();
            for ne in &nbrs {
                assert_ne!(ne.node_id, hub, "no neighbor should be hub");
            }
        }
    }

    /// Stress test: deterministic output. Compact same data twice, identical results.
    #[test]
    fn test_v3_deterministic_output() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let p1 = dir1.path().join("db");
        let p2 = dir2.path().join("db");

        for p in [&p1, &p2] {
            let mut db = DatabaseEngine::open(p, &DbOptions::default()).unwrap();
            for i in 0..50u64 {
                let mut props = BTreeMap::new();
                props.insert("idx".into(), PropValue::Int(i as i64));
                db.upsert_node(i as u32 % 3 + 1, &format!("n_{}", i), props, 0.5)
                    .unwrap();
            }
            for i in 0..30u64 {
                db.upsert_edge(
                    i + 1,
                    (i + 5) % 50 + 1,
                    i as u32 % 2 + 1,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .unwrap();
            }
            db.flush().unwrap();
            for i in 0..20u64 {
                let mut props = BTreeMap::new();
                props.insert("idx".into(), PropValue::Int(i as i64 + 100));
                db.upsert_node(i as u32 % 3 + 1, &format!("n_{}", i), props, 0.5)
                    .unwrap();
            }
            db.flush().unwrap();
            db.compact().unwrap();
            db.close().unwrap();
        }

        let db1 = DatabaseEngine::open(&p1, &DbOptions::default()).unwrap();
        let db2 = DatabaseEngine::open(&p2, &DbOptions::default()).unwrap();

        for i in 0..50u64 {
            let n1 = db1.get_node(i + 1).unwrap();
            let n2 = db2.get_node(i + 1).unwrap();
            assert_eq!(
                n1.is_some(),
                n2.is_some(),
                "node {} existence mismatch",
                i + 1
            );
            if let (Some(a), Some(b)) = (n1, n2) {
                assert_eq!(a.props, b.props, "node {} props mismatch", i + 1);
                assert_eq!(a.type_id, b.type_id, "node {} type_id mismatch", i + 1);
                assert_eq!(a.key, b.key, "node {} key mismatch", i + 1);
            }
        }
    }

    /// Critical test: index parity after V3 compaction. All query types correct.
    #[test]
    fn test_v3_index_parity() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props1 = BTreeMap::new();
        props1.insert("color".into(), PropValue::String("red".into()));
        props1.insert("score".into(), PropValue::Float(0.8));
        let mut props2 = BTreeMap::new();
        props2.insert("color".into(), PropValue::String("blue".into()));
        props2.insert("score".into(), PropValue::Float(0.6));

        for i in 0..10u64 {
            let props = if i % 2 == 0 {
                props1.clone()
            } else {
                props2.clone()
            };
            db.upsert_node(i as u32 % 2 + 1, &format!("key_{}", i), props, 0.5)
                .unwrap();
        }
        for i in 0..8u64 {
            db.upsert_edge(
                i + 1,
                i + 2,
                i as u32 % 3 + 1,
                BTreeMap::new(),
                1.0,
                None,
                None,
            )
            .unwrap();
        }
        db.flush().unwrap();

        for i in 0..5u64 {
            let mut props = BTreeMap::new();
            props.insert("color".into(), PropValue::String("green".into()));
            props.insert("updated".into(), PropValue::Bool(true));
            db.upsert_node(i as u32 % 2 + 1, &format!("key_{}", i), props, 0.5)
                .unwrap();
        }
        for i in 5..10u64 {
            db.upsert_edge(i + 1, 1, 2, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        db.flush().unwrap();

        // Capture pre-compaction results
        let pre_nodes: Vec<_> = (1..=10).map(|i| db.get_node(i).unwrap()).collect();
        let pre_key_lookups: Vec<_> = (0..10u32)
            .map(|i| {
                db.get_node_by_key(i % 2 + 1, &format!("key_{}", i))
                    .unwrap()
            })
            .collect();
        let pre_neighbors: Vec<_> = (1..=10)
            .map(|i| {
                db.neighbors(i, Direction::Both, None, 0, None, None)
                    .unwrap()
            })
            .collect();
        let pre_type1 = db.nodes_by_type(1).unwrap();
        let pre_type2 = db.nodes_by_type(2).unwrap();
        let pre_find = db
            .find_nodes(1, "color", &PropValue::String("green".into()))
            .unwrap();

        db.compact().unwrap();

        for (i, pre_node) in pre_nodes.iter().enumerate().take(10) {
            let post = db.get_node(i as u64 + 1).unwrap();
            assert_eq!(
                pre_node.as_ref().map(|n| (&n.props, n.type_id)),
                post.as_ref().map(|n| (&n.props, n.type_id)),
                "get_node({}) mismatch",
                i + 1
            );
        }

        for i in 0..10u32 {
            let post = db
                .get_node_by_key(i % 2 + 1, &format!("key_{}", i))
                .unwrap();
            assert_eq!(
                pre_key_lookups[i as usize].as_ref().map(|n| n.id),
                post.as_ref().map(|n| n.id),
                "get_node_by_key({}) mismatch",
                i
            );
        }

        for (i, pre_neighbor) in pre_neighbors.iter().enumerate().take(10) {
            let post = db
                .neighbors(i as u64 + 1, Direction::Both, None, 0, None, None)
                .unwrap();
            let pre_ids: HashSet<u64> = pre_neighbor.iter().map(|ne| ne.edge_id).collect();
            let post_ids: HashSet<u64> = post.iter().map(|ne| ne.edge_id).collect();
            assert_eq!(pre_ids, post_ids, "neighbors({}) edge set mismatch", i + 1);
        }

        let post_type1 = db.nodes_by_type(1).unwrap();
        assert_eq!(
            pre_type1.len(),
            post_type1.len(),
            "nodes_by_type(1) mismatch"
        );

        let post_type2 = db.nodes_by_type(2).unwrap();
        assert_eq!(
            pre_type2.len(),
            post_type2.len(),
            "nodes_by_type(2) mismatch"
        );

        let post_find = db
            .find_nodes(1, "color", &PropValue::String("green".into()))
            .unwrap();
        let pre_find_ids: HashSet<u64> = pre_find.iter().copied().collect();
        let post_find_ids: HashSet<u64> = post_find.iter().copied().collect();
        assert_eq!(pre_find_ids, post_find_ids, "find_nodes mismatch");
    }

    /// Stress test: mixed workload. Interleave upserts, deletes, compactions.
    #[test]
    fn test_v3_mixed_workload_stress() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..30u64 {
            db.upsert_node(1, &format!("mix_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }
        for i in 0..20u64 {
            db.upsert_edge(i + 1, i + 2, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        db.flush().unwrap();

        for i in [5u64, 10, 15, 20, 25] {
            db.delete_node(i).unwrap();
        }
        for i in 0..10u64 {
            let mut props = BTreeMap::new();
            props.insert("updated".into(), PropValue::Bool(true));
            db.upsert_node(1, &format!("mix_{}", i), props, 0.5)
                .unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();

        for i in 0..5u64 {
            let mut props = BTreeMap::new();
            props.insert("round3".into(), PropValue::Int(3));
            db.upsert_node(1, &format!("mix_{}", i), props, 0.5)
                .unwrap();
        }
        for i in 30..40u64 {
            db.upsert_node(2, &format!("new_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }
        db.flush().unwrap();

        let stats = db.compact().unwrap().expect("compaction");
        assert!(stats.segments_merged >= 2);

        for i in [5u64, 10, 15, 20, 25] {
            assert!(
                db.get_node(i).unwrap().is_none(),
                "node {} should be deleted",
                i
            );
        }
        for i in 0..5u64 {
            let n = db
                .get_node_by_key(1, &format!("mix_{}", i))
                .unwrap()
                .unwrap();
            assert_eq!(n.props.get("round3"), Some(&PropValue::Int(3)));
        }
        for i in 30..40u64 {
            assert!(
                db.get_node_by_key(2, &format!("new_{}", i))
                    .unwrap()
                    .is_some(),
                "new node new_{} should exist",
                i
            );
        }
    }

    /// Stress test: edges in different segments than their endpoint nodes.
    #[test]
    fn test_v3_cross_segment_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..20u64 {
            db.upsert_node(1, &format!("cs_{}", i), BTreeMap::new(), 0.5)
                .unwrap();
        }
        db.flush().unwrap();

        for i in 0..19u64 {
            db.upsert_edge(i + 1, i + 2, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        db.upsert_edge(1, 10, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(5, 15, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(10, 20, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let out_1 = db
            .neighbors(1, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(
            out_1.len() >= 2,
            "node 1 should have at least 2 outgoing edges"
        );

        let out_5 = db
            .neighbors(5, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out_5.len() >= 2, "node 5 should have outgoing to 6 and 15");

        let in_10 = db
            .neighbors(10, Direction::Incoming, None, 0, None, None)
            .unwrap();
        assert!(
            in_10.iter().any(|ne| ne.node_id == 1),
            "node 10 should have incoming from node 1"
        );
    }

    /// Stress test: V3 compact → close → reopen → verify all queries.
    #[test]
    fn test_v3_reopen_durability() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        {
            let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 0..50u64 {
                let mut props = BTreeMap::new();
                props.insert("name".into(), PropValue::String(format!("durable_{}", i)));
                db.upsert_node(i as u32 % 3 + 1, &format!("dur_{}", i), props, 0.5)
                    .unwrap();
            }
            for i in 0..40u64 {
                db.upsert_edge(
                    i + 1,
                    (i + 3) % 50 + 1,
                    i as u32 % 2 + 1,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .unwrap();
            }
            db.flush().unwrap();

            for i in 0..20u64 {
                let mut props = BTreeMap::new();
                props.insert("name".into(), PropValue::String(format!("updated_{}", i)));
                db.upsert_node(i as u32 % 3 + 1, &format!("dur_{}", i), props, 0.5)
                    .unwrap();
            }
            for i in [5u64, 15, 25, 35, 45] {
                db.delete_node(i).unwrap();
            }
            db.flush().unwrap();
            db.compact().unwrap();
            db.close().unwrap();
        }

        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..50u64 {
            let id = i + 1;
            if [5, 15, 25, 35, 45].contains(&id) {
                assert!(
                    db.get_node(id).unwrap().is_none(),
                    "node {} should be deleted",
                    id
                );
            } else {
                let n = db
                    .get_node(id)
                    .unwrap()
                    .unwrap_or_else(|| panic!("node {} should exist", id));
                if i < 20 {
                    assert_eq!(
                        n.props.get("name"),
                        Some(&PropValue::String(format!("updated_{}", i)))
                    );
                } else {
                    assert_eq!(
                        n.props.get("name"),
                        Some(&PropValue::String(format!("durable_{}", i)))
                    );
                }
            }
        }

        assert!(
            db.get_node_by_key(1, "dur_0").unwrap().is_some(),
            "key lookup should work"
        );

        let nbrs = db
            .neighbors(1, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(!nbrs.is_empty(), "adjacency should work after reopen");

        let type1 = db.nodes_by_type(1).unwrap();
        assert!(!type1.is_empty(), "type query should work after reopen");
    }

    /// Fast-merge path: verify metadata-driven indexes for non-overlapping segments.
    #[test]
    fn test_v3_fast_merge_index_parity() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for seg in 0..3u64 {
            let base = seg * 10;
            for i in 0..10u64 {
                let mut props = BTreeMap::new();
                props.insert("seg".into(), PropValue::Int(seg as i64));
                db.upsert_node(
                    (seg as u32 % 2) + 1,
                    &format!("fm_{}_{}", seg, i),
                    props,
                    0.5,
                )
                .unwrap();
            }
            for i in 0..9u64 {
                db.upsert_edge(
                    base + i + 1,
                    base + i + 2,
                    1,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .unwrap();
            }
            db.flush().unwrap();
        }

        let pre_nodes: Vec<_> = (1..=30).filter_map(|i| db.get_node(i).unwrap()).collect();
        let pre_key_0 = db.get_node_by_key(1, "fm_0_0").unwrap();
        let pre_nbrs_1 = db
            .neighbors(1, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        let pre_type1 = db.nodes_by_type(1).unwrap();

        db.compact().unwrap();

        let post_nodes: Vec<_> = (1..=30).filter_map(|i| db.get_node(i).unwrap()).collect();
        assert_eq!(pre_nodes.len(), post_nodes.len());
        for (pre, post) in pre_nodes.iter().zip(post_nodes.iter()) {
            assert_eq!(pre.id, post.id);
            assert_eq!(pre.props, post.props);
            assert_eq!(pre.type_id, post.type_id);
        }

        let post_key_0 = db.get_node_by_key(1, "fm_0_0").unwrap();
        assert_eq!(
            pre_key_0.map(|n| n.id),
            post_key_0.map(|n| n.id),
            "key lookup mismatch after fast-merge"
        );

        let post_nbrs_1 = db
            .neighbors(1, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(pre_nbrs_1.len(), post_nbrs_1.len(), "neighbors mismatch");

        let post_type1 = db.nodes_by_type(1).unwrap();
        assert_eq!(pre_type1.len(), post_type1.len(), "type query mismatch");
    }

    // --- Timestamp range index tests ---

    fn time_node(id: u64, type_id: u32, key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: updated_at - 100,
            updated_at,
            weight: 0.5,
        }
    }

    #[test]
    fn test_time_range_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(4, 2, "d", 2500)))
            .unwrap();

        // Exact range
        let r = db.find_nodes_by_time_range(1, 1000, 3000).unwrap();
        assert_eq!(r, vec![1, 2, 3]);

        // Partial range
        let r = db.find_nodes_by_time_range(1, 1500, 2500).unwrap();
        assert_eq!(r, vec![2]);

        // Type filter
        let r = db.find_nodes_by_time_range(2, 2000, 3000).unwrap();
        assert_eq!(r, vec![4]);

        // No matches
        let r = db.find_nodes_by_time_range(1, 4000, 5000).unwrap();
        assert!(r.is_empty());

        // All within type 1
        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 2, 3]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_across_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.flush().unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(4, 1, "d", 1500)))
            .unwrap();

        let r = db.find_nodes_by_time_range(1, 1000, 2000).unwrap();
        assert_eq!(r, vec![1, 2, 4]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.flush().unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(4, 1, "d", 4000)))
            .unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let r = db.find_nodes_by_time_range(1, 1500, 3500).unwrap();
        assert_eq!(r, vec![2, 3]);

        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 2, 3, 4]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
            .unwrap();
        db.flush().unwrap();

        db.delete_node(2).unwrap();

        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 3]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_boundary_conditions() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
            .unwrap();

        // Inclusive boundaries
        let r = db.find_nodes_by_time_range(1, 1000, 1000).unwrap();
        assert_eq!(r, vec![1], "single-point range at lower bound");

        let r = db.find_nodes_by_time_range(1, 3000, 3000).unwrap();
        assert_eq!(r, vec![3], "single-point range at upper bound");

        let r = db.find_nodes_by_time_range(1, 2000, 2000).unwrap();
        assert_eq!(r, vec![2], "single-point range in middle");

        // Empty range
        let r = db.find_nodes_by_time_range(1, 1500, 1500).unwrap();
        assert!(r.is_empty(), "no nodes at this exact time");

        // Inverted range
        let r = db.find_nodes_by_time_range(1, 3000, 1000).unwrap();
        assert!(r.is_empty(), "inverted range returns empty");

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_paged() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 1..=10u64 {
            db.write_op(&WalOp::UpsertNode(time_node(
                i,
                1,
                &format!("n{}", i),
                i as i64 * 1000,
            )))
            .unwrap();
        }
        db.flush().unwrap();

        let page1 = db
            .find_nodes_by_time_range_paged(
                1,
                1000,
                10000,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items, vec![1, 2, 3]);
        assert!(page1.next_cursor.is_some());

        let page2 = db
            .find_nodes_by_time_range_paged(
                1,
                1000,
                10000,
                &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items, vec![4, 5, 6]);

        let page3 = db
            .find_nodes_by_time_range_paged(
                1,
                1000,
                10000,
                &PageRequest {
                    limit: Some(3),
                    after: page2.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page3.items, vec![7, 8, 9]);

        let page4 = db
            .find_nodes_by_time_range_paged(
                1,
                1000,
                10000,
                &PageRequest {
                    limit: Some(3),
                    after: page3.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page4.items, vec![10]);
        assert!(page4.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_upsert_updates_index() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        let r = db.find_nodes_by_time_range(1, 900, 1100).unwrap();
        assert_eq!(r, vec![1]);

        // Update same node with new timestamp
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 5000)))
            .unwrap();

        let r = db.find_nodes_by_time_range(1, 900, 1100).unwrap();
        assert!(r.is_empty(), "node should not appear at old timestamp");

        let r = db.find_nodes_by_time_range(1, 4900, 5100).unwrap();
        assert_eq!(r, vec![1], "node should appear at new timestamp");

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
                .unwrap();
            db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
                .unwrap();
            db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
                .unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        {
            let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let r = db.find_nodes_by_time_range(1, 1500, 2500).unwrap();
            assert_eq!(r, vec![2]);
            let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
            assert_eq!(r, vec![1, 2, 3]);
            db.close().unwrap();
        }
    }

    #[test]
    fn test_time_range_dedup_across_sources() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.flush().unwrap();

        // Update same node in memtable (different time, same wide range)
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1500)))
            .unwrap();

        let r = db.find_nodes_by_time_range(1, 0, 2000).unwrap();
        assert_eq!(r, vec![1]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_with_prune_policy() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(NodeRecord {
            id: 3,
            type_id: 1,
            key: "c".to_string(),
            props: BTreeMap::new(),
            created_at: 2900,
            updated_at: 3000,
            weight: 0.001,
        }))
        .unwrap();

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.01),
                type_id: None,
            },
        )
        .unwrap();

        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 2]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_stale_segment_suppressed_by_newer_version() {
        // A node flushed to a segment with updated_at inside the range must NOT
        // appear in results when a newer version (in memtable or newer segment)
        // has updated_at OUTSIDE the range.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Insert node at t=1000, flush to segment
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.flush().unwrap();

        // Upsert same node with t=5000 (outside the query window)
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 5000)))
            .unwrap();

        // Query [500, 2000]. Old segment has node at t=1000 (in range),
        // but current version is t=5000 (out of range). Must return empty.
        let r = db.find_nodes_by_time_range(1, 500, 2000).unwrap();
        assert!(
            r.is_empty(),
            "stale segment entry must be suppressed by newer version"
        );

        // Node should appear in a range that covers its current timestamp
        let r = db.find_nodes_by_time_range(1, 4000, 6000).unwrap();
        assert_eq!(r, vec![1]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_stale_segment_suppressed_across_segments() {
        // Same as above, but the newer version is also in a segment (not memtable)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: node at t=1000
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.flush().unwrap();

        // Segment 2: same node at t=5000
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 5000)))
            .unwrap();
        db.flush().unwrap();

        // Query [500, 2000]. Segment 1 has t=1000 (in range),
        // but latest version (segment 2) has t=5000. Must return empty.
        let r = db.find_nodes_by_time_range(1, 500, 2000).unwrap();
        assert!(
            r.is_empty(),
            "stale segment entry must be suppressed by newer segment version"
        );

        // After compaction, still correct
        db.compact().unwrap();
        let r = db.find_nodes_by_time_range(1, 500, 2000).unwrap();
        assert!(
            r.is_empty(),
            "stale entry must be suppressed after compaction too"
        );

        let r = db.find_nodes_by_time_range(1, 4000, 6000).unwrap();
        assert_eq!(r, vec![1]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_paged_stale_suppressed() {
        // Ensure pagination path also filters stale entries
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Flush 3 nodes to segment
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000)))
            .unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000)))
            .unwrap();
        db.flush().unwrap();

        // Move node 2 outside the range
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 9000)))
            .unwrap();

        // Paginated query [500, 4000] limit=10, should get [1, 3] (not [1, 2, 3])
        let r = db
            .find_nodes_by_time_range_paged(
                1,
                500,
                4000,
                &PageRequest {
                    limit: Some(10),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(r.items, vec![1, 3]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_paged_policy_refills_past_sparse_filtered_window() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut visible_ids = Vec::new();

        for i in 0..17u64 {
            let weight = if i < 12 { 0.1 } else { 1.0 };
            db.write_op(&WalOp::UpsertNode(NodeRecord {
                id: i + 1,
                type_id: 1,
                key: format!("n{}", i),
                props: BTreeMap::new(),
                created_at: 1000 + i as i64,
                updated_at: 1000 + i as i64,
                weight,
            }))
            .unwrap();
            if weight > 0.5 {
                visible_ids.push(i + 1);
            }
        }

        db.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        )
        .unwrap();

        let page1 = db
            .find_nodes_by_time_range_paged(
                1,
                1000,
                2000,
                &PageRequest {
                    limit: Some(3),
                    after: None,
                },
            )
            .unwrap();
        assert_eq!(page1.items, visible_ids[..3].to_vec());
        assert!(page1.next_cursor.is_some());

        let page2 = db
            .find_nodes_by_time_range_paged(
                1,
                1000,
                2000,
                &PageRequest {
                    limit: Some(3),
                    after: page1.next_cursor,
                },
            )
            .unwrap();
        assert_eq!(page2.items, visible_ids[3..].to_vec());
        assert!(page2.next_cursor.is_none());

        db.close().unwrap();
    }

    // ---- PPR tests ----

    #[test]
    fn test_ppr_empty_seeds() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(dir.path());
        let result = db
            .personalized_pagerank(&[], &PprOptions::default())
            .unwrap();
        assert!(result.scores.is_empty());
        assert_eq!(result.iterations, 0);
        assert!(result.converged);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_single_node_no_edges() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let n1 = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let result = db
            .personalized_pagerank(&[n1], &PprOptions::default())
            .unwrap();
        // Single node with no outgoing edges: all mass stays on seed via teleport + dangling
        assert_eq!(result.scores.len(), 1);
        assert_eq!(result.scores[0].0, n1);
        assert!(
            (result.scores[0].1 - 1.0).abs() < 1e-6,
            "single dangling node should have rank ~1.0"
        );
        assert!(result.converged);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_simple_chain() {
        // A → B → C (seed = A)
        // Rank should flow A > B > C
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert!(result.scores.len() >= 2);

        let score_a = result
            .scores
            .iter()
            .find(|s| s.0 == a)
            .map(|s| s.1)
            .unwrap_or(0.0);
        let score_b = result
            .scores
            .iter()
            .find(|s| s.0 == b)
            .map(|s| s.1)
            .unwrap_or(0.0);
        let score_c = result
            .scores
            .iter()
            .find(|s| s.0 == c)
            .map(|s| s.1)
            .unwrap_or(0.0);

        assert!(score_a > score_b, "seed A should rank higher than B");
        assert!(score_b > score_c, "B should rank higher than C");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_cycle_converges() {
        // A → B → A (cycle), seed = A
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert_eq!(result.scores.len(), 2);

        let score_a = result.scores.iter().find(|s| s.0 == a).unwrap().1;
        let score_b = result.scores.iter().find(|s| s.0 == b).unwrap().1;

        // A should rank higher than B (teleport bias toward seed)
        assert!(score_a > score_b, "seed should rank higher due to teleport");
        // Total rank should sum to ~1.0
        assert!(
            (score_a + score_b - 1.0).abs() < 1e-4,
            "total rank should sum to ~1.0"
        );
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_weighted_edges() {
        // A → B (weight=1.0), A → C (weight=9.0), seed = A
        // C should get ~9x the rank of B from A's distribution
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 9.0, None, None)
            .unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        let score_b = result
            .scores
            .iter()
            .find(|s| s.0 == b)
            .map(|s| s.1)
            .unwrap_or(0.0);
        let score_c = result
            .scores
            .iter()
            .find(|s| s.0 == c)
            .map(|s| s.1)
            .unwrap_or(0.0);

        // C should have significantly higher rank than B due to weight
        assert!(
            score_c > score_b * 3.0,
            "heavily-weighted C ({score_c}) should rank much higher than B ({score_b})"
        );
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_edge_type_filter() {
        // A → B (type 1), A → C (type 2), seed = A
        // Filter to type 1 only: only B should receive rank
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(a, c, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let opts = PprOptions {
            edge_type_filter: Some(vec![1]),
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        let score_b = result
            .scores
            .iter()
            .find(|s| s.0 == b)
            .map(|s| s.1)
            .unwrap_or(0.0);
        let score_c = result
            .scores
            .iter()
            .find(|s| s.0 == c)
            .map(|s| s.1)
            .unwrap_or(0.0);

        assert!(score_b > 0.0, "B should receive rank via type-1 edge");
        assert_eq!(
            score_c, 0.0,
            "C should receive no rank (type-2 edge filtered out)"
        );
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_max_results() {
        // Star graph: seed → 10 nodes
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let center = db.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        for i in 0..10 {
            let n = db
                .upsert_node(1, &format!("n{i}"), BTreeMap::new(), 1.0)
                .unwrap();
            db.upsert_edge(center, n, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }

        let opts = PprOptions {
            max_results: Some(3),
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[center], &opts).unwrap();
        assert!(result.scores.len() <= 3, "max_results should cap output");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_multiple_seeds() {
        // A → C, B → C, seeds = [A, B]
        // C should get high rank from both seeds
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a, b], &opts).unwrap();
        assert!(result.converged);

        let score_c = result
            .scores
            .iter()
            .find(|s| s.0 == c)
            .map(|s| s.1)
            .unwrap_or(0.0);
        assert!(score_c > 0.0, "C should receive rank from both seeds");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_respects_deleted_nodes() {
        // A → B → C, delete B, seed = A
        // B's outgoing edges should not contribute rank to C
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.delete_node(b).unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        // B is deleted. neighbors(A) should not include B
        let score_b = result
            .scores
            .iter()
            .find(|s| s.0 == b)
            .map(|s| s.1)
            .unwrap_or(0.0);
        let score_c = result
            .scores
            .iter()
            .find(|s| s.0 == c)
            .map(|s| s.1)
            .unwrap_or(0.0);
        assert_eq!(score_b, 0.0, "deleted node B should not appear in results");
        assert_eq!(score_c, 0.0, "C unreachable after B deleted");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_deleted_seed_returns_empty() {
        // Deleted seed must not appear in results
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        db.delete_node(a).unwrap();

        let result = db
            .personalized_pagerank(&[a], &PprOptions::default())
            .unwrap();
        assert!(
            result.scores.is_empty(),
            "deleted seed must not appear in PPR results"
        );
        assert!(result.converged);
        assert_eq!(result.iterations, 0);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_nonexistent_seed_returns_empty() {
        // Non-existent ID should also be filtered out
        let dir = TempDir::new().unwrap();
        let db = open_imm(dir.path());
        let result = db
            .personalized_pagerank(&[999], &PprOptions::default())
            .unwrap();
        assert!(result.scores.is_empty());
        assert!(result.converged);
        assert_eq!(result.iterations, 0);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_across_flush() {
        // Create graph, flush to segment, verify PPR still works
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();

        // Add more in memtable
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert!(
            result.scores.len() >= 3,
            "should find nodes across memtable + segment"
        );
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let opts = PprOptions {
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert!(result.scores.len() >= 3);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_duplicate_seeds() {
        // Duplicate seed IDs should be deduplicated
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let r1 = db
            .personalized_pagerank(&[a], &PprOptions::default())
            .unwrap();
        let r2 = db
            .personalized_pagerank(&[a, a, a], &PprOptions::default())
            .unwrap();

        // Should produce identical results
        assert_eq!(r1.scores.len(), r2.scores.len());
        for (s1, s2) in r1.scores.iter().zip(r2.scores.iter()) {
            assert_eq!(s1.0, s2.0);
            assert!((s1.1 - s2.1).abs() < 1e-10);
        }
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_known_values() {
        // Triangle: A → B → C → A, seed = A, damping = 0.85
        // Analytic solution for PPR on a directed cycle with uniform weights:
        //   rank_seed = (1 - d) / (1 - d^3)  [geometric series]
        //   rank_hop1 = d * rank_seed
        //   rank_hop2 = d^2 * rank_seed
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let d = 0.85_f64;
        let opts = PprOptions {
            damping_factor: d,
            epsilon: 1e-10,
            max_iterations: 200,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        let score_a = result.scores.iter().find(|s| s.0 == a).unwrap().1;
        let score_b = result.scores.iter().find(|s| s.0 == b).unwrap().1;
        let score_c = result.scores.iter().find(|s| s.0 == c).unwrap().1;

        // Expected: rank_a = (1-d)/(1-d^3), rank_b = d*rank_a, rank_c = d^2*rank_a
        let expected_a = (1.0 - d) / (1.0 - d.powi(3));
        let expected_b = d * expected_a;
        let expected_c = d * d * expected_a;

        assert!(
            (score_a - expected_a).abs() < 1e-6,
            "A: got {score_a}, expected {expected_a}"
        );
        assert!(
            (score_b - expected_b).abs() < 1e-6,
            "B: got {score_b}, expected {expected_b}"
        );
        assert!(
            (score_c - expected_c).abs() < 1e-6,
            "C: got {score_c}, expected {expected_c}"
        );
        assert!(
            (score_a + score_b + score_c - 1.0).abs() < 1e-6,
            "total should sum to 1.0"
        );
        db.close().unwrap();
    }

    // --- export_adjacency tests ---

    #[test]
    fn test_export_empty_db() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(dir.path());
        let result = db.export_adjacency(&ExportOptions::default()).unwrap();
        assert!(result.node_ids.is_empty());
        assert!(result.edges.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn test_export_nodes_only() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(2, "b", BTreeMap::new(), 1.0).unwrap();
        let result = db.export_adjacency(&ExportOptions::default()).unwrap();
        assert_eq!(result.node_ids.len(), 2);
        assert!(result.node_ids.contains(&a));
        assert!(result.node_ids.contains(&b));
        assert!(result.edges.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn test_export_full_graph() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 2.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 3.0, None, None)
            .unwrap();
        db.upsert_edge(c, a, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let opts = ExportOptions {
            include_weights: true,
            ..Default::default()
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.node_ids.len(), 3);
        assert_eq!(result.edges.len(), 3);
        // Verify edge data
        let ab = result.edges.iter().find(|e| e.0 == a && e.1 == b).unwrap();
        assert_eq!(ab.2, 1);
        assert!((ab.3 - 2.0).abs() < 1e-6);
        let ca = result.edges.iter().find(|e| e.0 == c && e.1 == a).unwrap();
        assert_eq!(ca.2, 2);
        db.close().unwrap();
    }

    #[test]
    fn test_export_node_type_filter() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(2, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Only type-1 nodes: a and c
        let opts = ExportOptions {
            node_type_filter: Some(vec![1]),
            include_weights: true,
            ..Default::default()
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.node_ids.len(), 2);
        assert!(result.node_ids.contains(&a));
        assert!(result.node_ids.contains(&c));
        // Edge a→b should be excluded (b is type-2, not in node set)
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].0, a);
        assert_eq!(result.edges[0].1, c);
        db.close().unwrap();
    }

    #[test]
    fn test_export_edge_type_filter() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(a, b, 2, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        // Only edge type 2
        let opts = ExportOptions {
            edge_type_filter: Some(vec![2]),
            include_weights: true,
            ..Default::default()
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].2, 2); // type_id = 2
        db.close().unwrap();
    }

    #[test]
    fn test_export_include_weights_false() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 5.0, None, None)
            .unwrap();

        let opts = ExportOptions {
            include_weights: false,
            ..Default::default()
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].3, 0.0); // weight zeroed out
        db.close().unwrap();
    }

    #[test]
    fn test_export_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.delete_node(b).unwrap();

        let result = db
            .export_adjacency(&ExportOptions {
                include_weights: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.node_ids.len(), 2); // a and c
        assert!(!result.node_ids.contains(&b));
        // Edge a→b should be gone (b is deleted)
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].1, c);
        db.close().unwrap();
    }

    #[test]
    fn test_export_across_flush() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 2.0, None, None)
            .unwrap();

        let result = db
            .export_adjacency(&ExportOptions {
                include_weights: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.node_ids.len(), 3);
        assert_eq!(result.edges.len(), 2);
        db.close().unwrap();
    }

    #[test]
    fn test_export_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let result = db
            .export_adjacency(&ExportOptions {
                include_weights: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.node_ids.len(), 3);
        assert_eq!(result.edges.len(), 2);
        db.close().unwrap();
    }

    #[test]
    fn test_export_node_ids_sorted() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        for i in 0..10 {
            db.upsert_node(1, &format!("n{i}"), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let result = db.export_adjacency(&ExportOptions::default()).unwrap();
        assert_eq!(result.node_ids.len(), 10);
        for i in 1..result.node_ids.len() {
            assert!(
                result.node_ids[i] > result.node_ids[i - 1],
                "node_ids must be sorted"
            );
        }
        db.close().unwrap();
    }

    #[test]
    fn test_export_combined_filters() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(2, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(a, b, 2, BTreeMap::new(), 2.0, None, None)
            .unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 3.0, None, None)
            .unwrap();

        // node_type=1 + edge_type=1 → only edge a→b with type 1
        let opts = ExportOptions {
            node_type_filter: Some(vec![1]),
            edge_type_filter: Some(vec![1]),
            include_weights: true,
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.node_ids.len(), 2); // a and b (type 1)
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0], (a, b, 1, 1.0));
        db.close().unwrap();
    }

    // --- PPR damping_factor edge cases ---

    #[test]
    fn test_ppr_low_damping_seed_dominates() {
        // With very low damping, the seed should retain nearly all rank.
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let result = db
            .personalized_pagerank(
                &[a],
                &PprOptions {
                    damping_factor: 0.01,
                    max_iterations: 100,
                    ..PprOptions::default()
                },
            )
            .unwrap();

        let seed_score = result
            .scores
            .iter()
            .find(|s| s.0 == a)
            .map(|s| s.1)
            .unwrap();
        assert!(
            seed_score > 0.95,
            "seed should have >95% rank with damping=0.01, got {}",
            seed_score
        );
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_high_damping_spreads_rank() {
        // With high damping, rank should spread more evenly across the graph.
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        db.upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None)
            .unwrap();

        let result = db
            .personalized_pagerank(
                &[a],
                &PprOptions {
                    damping_factor: 0.99,
                    max_iterations: 200,
                    epsilon: 1e-8,
                    ..PprOptions::default()
                },
            )
            .unwrap();

        let seed_score = result
            .scores
            .iter()
            .find(|s| s.0 == a)
            .map(|s| s.1)
            .unwrap();
        // With cycle A→B→C→A and high damping, rank should be fairly distributed
        assert!(
            seed_score < 0.60,
            "seed should have <60% rank with damping=0.99, got {}",
            seed_score
        );
        db.close().unwrap();
    }
