use overgraph::{DatabaseEngine, DbOptions, Direction, PropValue};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Large-scale insert, flush, more writes, cross-source queries.
#[test]
fn test_large_graph_with_flush_and_cross_source_queries() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // --- Batch 1: 10k nodes ---
    let mut node_ids = Vec::with_capacity(10_000);
    let batch: Vec<overgraph::NodeInput> = (0..10_000)
        .map(|i| overgraph::NodeInput {
            type_id: (i % 5) as u32 + 1, // types 1..5
            key: format!("node:{}", i),
            props: {
                let mut p = BTreeMap::new();
                p.insert("idx".to_string(), PropValue::Int(i as i64));
                p
            },
            weight: 0.5,
        })
        .collect();
    node_ids.extend(engine.batch_upsert_nodes(&batch).unwrap());
    assert_eq!(node_ids.len(), 10_000);

    // --- Batch 1: 20k edges (chain + cross-links) ---
    let mut edge_ids = Vec::with_capacity(20_000);
    // Chain edges: node[i] → node[i+1]
    let chain_edges: Vec<overgraph::EdgeInput> = (0..9_999)
        .map(|i| overgraph::EdgeInput {
            from: node_ids[i],
            to: node_ids[i + 1],
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    edge_ids.extend(engine.batch_upsert_edges(&chain_edges).unwrap());

    // Cross-link edges: 10,001 wrapping edges (edge_uniqueness=off, one dup is fine)
    let cross_edges: Vec<overgraph::EdgeInput> = (0..10_001)
        .map(|i| overgraph::EdgeInput {
            from: node_ids[i % 10_000],
            to: node_ids[(i + 100) % 10_000],
            type_id: 20,
            props: BTreeMap::new(),
            weight: 0.8,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    edge_ids.extend(engine.batch_upsert_edges(&cross_edges).unwrap());
    assert_eq!(edge_ids.len(), 20_000);

    // --- Force flush ---
    let seg_info = engine.flush().unwrap();
    assert!(seg_info.is_some());
    assert_eq!(engine.segment_count(), 1);

    // --- Batch 2: 500 more nodes + 1000 edges in memtable ---
    let batch2: Vec<overgraph::NodeInput> = (10_000..10_500)
        .map(|i| overgraph::NodeInput {
            type_id: 6,
            key: format!("node:{}", i),
            props: BTreeMap::new(),
            weight: 0.7,
        })
        .collect();
    let new_ids = engine.batch_upsert_nodes(&batch2).unwrap();
    assert_eq!(new_ids.len(), 500);

    let new_edges: Vec<overgraph::EdgeInput> = (0..1000)
        .map(|i| overgraph::EdgeInput {
            from: new_ids[i % 500],
            to: node_ids[i % 10_000], // link new → old (cross-source)
            type_id: 30,
            props: BTreeMap::new(),
            weight: 0.6,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&new_edges).unwrap();

    // --- Cross-source queries ---

    // 1. Get node from segment
    let node_0 = engine.get_node(node_ids[0]).unwrap().unwrap();
    assert_eq!(node_0.key, "node:0");
    assert_eq!(node_0.props.get("idx"), Some(&PropValue::Int(0)));

    // 2. Get node from memtable
    let node_new = engine.get_node(new_ids[0]).unwrap().unwrap();
    assert_eq!(node_new.key, "node:10000");

    // 3. Neighbors from segment: node[500] should have chain + cross-link edges
    let out_500 = engine
        .neighbors(node_ids[500], Direction::Outgoing, None, 0, None, None)
        .unwrap();
    assert!(out_500.len() >= 2); // at least chain(→501) + cross-link(→600)

    // 4. Type-filtered neighbors from segment
    let chain_only = engine
        .neighbors(
            node_ids[500],
            Direction::Outgoing,
            Some(&[10]),
            0,
            None,
            None,
        )
        .unwrap();
    assert_eq!(chain_only.len(), 1); // only the chain edge

    // 5. Cross-source neighbors: new node → old node (memtable edge, segment target)
    let cross = engine
        .neighbors(new_ids[0], Direction::Outgoing, None, 0, None, None)
        .unwrap();
    assert!(!cross.is_empty());
    // The target node should be from the segment
    assert!(engine.get_node(cross[0].node_id).unwrap().is_some());

    // 6. Incoming neighbors on a segment node that has cross-source incoming edges
    let inc = engine
        .neighbors(node_ids[0], Direction::Incoming, None, 0, None, None)
        .unwrap();
    assert!(!inc.is_empty()); // has chain from node[9999]→node[0] or cross-links

    // 7. Limit works across sources
    let limited = engine
        .neighbors(node_ids[0], Direction::Outgoing, None, 1, None, None)
        .unwrap();
    assert_eq!(limited.len(), 1);

    engine.close().unwrap();
}

/// Flush, close, reopen -- all data accessible from segments.
#[test]
fn test_flush_close_reopen_reads_from_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let node_a;
    let node_b;
    let node_c;
    let edge_ab;
    let edge_bc;
    {
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Build a small graph
        node_a = engine
            .upsert_node(
                1,
                "alice",
                {
                    let mut p = BTreeMap::new();
                    p.insert("role".to_string(), PropValue::String("admin".to_string()));
                    p
                },
                0.9,
            )
            .unwrap();

        node_b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        node_c = engine
            .upsert_node(2, "charlie", BTreeMap::new(), 0.6)
            .unwrap();

        edge_ab = engine
            .upsert_edge(node_a, node_b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        edge_bc = engine
            .upsert_edge(node_b, node_c, 10, BTreeMap::new(), 0.8, None, None)
            .unwrap();

        // Delete charlie and his edge
        engine.delete_node(node_c).unwrap();

        // Flush everything to a segment
        engine.flush().unwrap();
        assert_eq!(engine.segment_count(), 1);

        // Add post-flush data (stays in WAL for replay on reopen)
        let _node_d = engine.upsert_node(1, "dave", BTreeMap::new(), 0.4).unwrap();

        engine.close().unwrap();
    }

    // Reopen: segment data should be loaded, WAL replayed for post-flush writes
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.segment_count(), 1);

        // Segment data accessible
        let alice = engine.get_node(node_a).unwrap().unwrap();
        assert_eq!(alice.key, "alice");
        assert_eq!(
            alice.props.get("role"),
            Some(&PropValue::String("admin".to_string()))
        );

        let bob = engine.get_node(node_b).unwrap().unwrap();
        assert_eq!(bob.key, "bob");

        let edge = engine.get_edge(edge_ab).unwrap().unwrap();
        assert_eq!(edge.from, node_a);
        assert_eq!(edge.to, node_b);

        // Deleted node stays deleted after flush + reopen
        assert!(engine.get_node(node_c).unwrap().is_none());
        assert!(engine.get_edge(edge_bc).unwrap().is_none());

        // Neighbors work from segment
        let out_a = engine
            .neighbors(node_a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out_a.len(), 1);
        assert_eq!(out_a[0].node_id, node_b);

        // Deleted node excluded from incoming
        let inc_b = engine
            .neighbors(node_b, Direction::Incoming, None, 0, None, None)
            .unwrap();
        assert_eq!(inc_b.len(), 1);
        assert_eq!(inc_b[0].node_id, node_a);

        // Post-flush WAL data recovered via WAL replay
        let dave = engine
            .get_node(node_a + 3)
            .unwrap()
            .expect("dave not found, WAL replay after flush failed");
        assert_eq!(dave.key, "dave");

        engine.close().unwrap();
    }
}

/// Bonus: Flush → more writes → second flush → reopen → multi-segment reads.
#[test]
fn test_multi_segment_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let id_a;
    let id_b;
    let id_c;
    {
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1
        id_a = engine
            .upsert_node(1, "alpha", BTreeMap::new(), 0.5)
            .unwrap();
        engine
            .upsert_edge(id_a, id_a, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap(); // self-loop
        engine.flush().unwrap();

        // Segment 2
        id_b = engine.upsert_node(1, "beta", BTreeMap::new(), 0.6).unwrap();
        engine
            .upsert_edge(id_a, id_b, 10, BTreeMap::new(), 0.9, None, None)
            .unwrap();
        engine.flush().unwrap();

        // Memtable (will be WAL on reopen)
        id_c = engine
            .upsert_node(1, "gamma", BTreeMap::new(), 0.7)
            .unwrap();
        engine
            .upsert_edge(id_b, id_c, 20, BTreeMap::new(), 0.8, None, None)
            .unwrap();

        assert_eq!(engine.segment_count(), 2);
        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.segment_count(), 2);

        // All three nodes from different sources
        assert_eq!(engine.get_node(id_a).unwrap().unwrap().key, "alpha"); // seg 1
        assert_eq!(engine.get_node(id_b).unwrap().unwrap().key, "beta"); // seg 2
        assert_eq!(engine.get_node(id_c).unwrap().unwrap().key, "gamma"); // WAL replay

        // Neighbors merge across all three sources
        let out_a = engine
            .neighbors(id_a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out_a.len(), 2); // self-loop (seg1) + a→b (seg2)

        let out_b = engine
            .neighbors(id_b, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out_b.len(), 1); // b→c (WAL)
        assert_eq!(out_b[0].node_id, id_c);

        engine.close().unwrap();
    }
}
