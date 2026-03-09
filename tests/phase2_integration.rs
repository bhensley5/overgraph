use overgraph::*;
use std::collections::BTreeMap;

/// Build a small graph (20 nodes, 50 edges, mixed types),
/// verify all query patterns: get, neighbors, deletes.
#[test]
fn test_full_graph_query_patterns() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("graph_db");

    let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Create 20 nodes across 3 types
    let mut node_ids = Vec::new();
    for i in 0..20 {
        let type_id = (i % 3) as u32 + 1; // types 1, 2, 3
        let id = engine
            .upsert_node(type_id, &format!("node:{}", i), BTreeMap::new(), 0.5)
            .unwrap();
        node_ids.push(id);
    }
    assert_eq!(engine.node_count(), 20);

    // Create 50 edges across 2 types
    // Type 10: "knows" edges, chain pattern (0→1→2→...→19)
    // Type 20: "references" edges, skip pattern (i→i+3)
    let mut edge_ids = Vec::new();
    for i in 0..19 {
        let eid = engine
            .upsert_edge(
                node_ids[i],
                node_ids[i + 1],
                10,
                BTreeMap::new(),
                1.0,
                None,
                None,
            )
            .unwrap();
        edge_ids.push(eid);
    }
    for i in 0..17 {
        let eid = engine
            .upsert_edge(
                node_ids[i],
                node_ids[i + 3],
                20,
                BTreeMap::new(),
                0.8,
                None,
                None,
            )
            .unwrap();
        edge_ids.push(eid);
    }
    // Fill remaining edges: hub pattern from node 0
    for i in 5..19 {
        let eid = engine
            .upsert_edge(
                node_ids[0],
                node_ids[i],
                20,
                BTreeMap::new(),
                0.5,
                None,
                None,
            )
            .unwrap();
        edge_ids.push(eid);
    }
    assert_eq!(engine.edge_count(), 50);

    // --- Verify get-by-ID ---
    for &id in &node_ids {
        assert!(engine.get_node(id).unwrap().is_some());
    }
    for &id in &edge_ids {
        assert!(engine.get_edge(id).unwrap().is_some());
    }

    // --- Verify outgoing neighbors ---
    // Node 0 has: 1 "knows" (→1) + 1 "references" (→3) + 14 hub edges = 16 outgoing
    let out_0 = engine
        .neighbors(node_ids[0], Direction::Outgoing, None, 0, None, None)
        .unwrap();
    assert_eq!(out_0.len(), 16);

    // Filter by type 10 ("knows"), node 0 has exactly 1 (→1)
    let knows_0 = engine
        .neighbors(node_ids[0], Direction::Outgoing, Some(&[10]), 0, None, None)
        .unwrap();
    assert_eq!(knows_0.len(), 1);
    assert_eq!(knows_0[0].node_id, node_ids[1]);

    // Node 5: knows →6, references →8, and is a hub target (incoming only for hub)
    let out_5 = engine
        .neighbors(node_ids[5], Direction::Outgoing, Some(&[10]), 0, None, None)
        .unwrap();
    assert_eq!(out_5.len(), 1);
    assert_eq!(out_5[0].node_id, node_ids[6]);

    // --- Verify incoming neighbors ---
    // Node 19: knows ←18, references ←16
    let inc_19 = engine
        .neighbors(node_ids[19], Direction::Incoming, None, 0, None, None)
        .unwrap();
    assert_eq!(inc_19.len(), 2);

    // --- Verify limit ---
    let limited = engine
        .neighbors(node_ids[0], Direction::Outgoing, None, 5, None, None)
        .unwrap();
    assert_eq!(limited.len(), 5);

    // --- Verify both direction ---
    // Node 10: outgoing knows→11, references→13; incoming knows←9, references←7, hub←0
    let both_10 = engine
        .neighbors(node_ids[10], Direction::Both, None, 0, None, None)
        .unwrap();
    assert!(both_10.len() >= 4); // at least 4 connections

    // --- Delete a node and verify cascade ---
    // node_ids[1] has incident edges: 0→1 (knows), 1→2 (knows), 1→4 (references)
    let edge_count_before = engine.edge_count();
    engine.delete_node(node_ids[1]).unwrap();
    assert!(engine.get_node(node_ids[1]).unwrap().is_none());
    assert_eq!(engine.node_count(), 19);

    // Cascade-deleted incident edges
    assert!(engine.get_edge(edge_ids[0]).unwrap().is_none()); // 0→1 knows
    assert!(engine.get_edge(edge_ids[1]).unwrap().is_none()); // 1→2 knows
    assert_eq!(engine.edge_count(), edge_count_before - 3); // 3 incident edges gone

    // Node 0's "knows" neighbor (node 1) should be gone from results
    let knows_after = engine
        .neighbors(node_ids[0], Direction::Outgoing, Some(&[10]), 0, None, None)
        .unwrap();
    assert!(knows_after.is_empty());

    // --- Delete an edge directly ---
    // Pick an edge that still exists: 2→3 knows (edge_ids[2])
    let edge_to_delete = edge_ids[2];
    engine.delete_edge(edge_to_delete).unwrap();
    assert!(engine.get_edge(edge_to_delete).unwrap().is_none());

    engine.close().unwrap();
}

/// WAL replay rebuilds memtable including adjacency.
/// Close and reopen, verify full graph state matches.
#[test]
fn test_graph_state_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("restart_db");

    let (node_a, node_b, node_c, node_d, edge_ab, edge_ac, edge_bc);

    // --- Build graph, delete some items, close ---
    {
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        node_a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        node_b = engine.upsert_node(1, "b", BTreeMap::new(), 0.6).unwrap();
        node_c = engine.upsert_node(2, "c", BTreeMap::new(), 0.7).unwrap();
        node_d = engine.upsert_node(2, "d", BTreeMap::new(), 0.8).unwrap();

        edge_ab = engine
            .upsert_edge(node_a, node_b, 10, BTreeMap::new(), 1.0, None, None)
            .unwrap();
        edge_ac = engine
            .upsert_edge(node_a, node_c, 10, BTreeMap::new(), 0.9, None, None)
            .unwrap();
        edge_bc = engine
            .upsert_edge(node_b, node_c, 20, BTreeMap::new(), 0.8, None, None)
            .unwrap();
        engine
            .upsert_edge(node_c, node_d, 10, BTreeMap::new(), 0.7, None, None)
            .unwrap();

        // Delete node d and edge b→c
        engine.delete_node(node_d).unwrap();
        engine.delete_edge(edge_bc).unwrap();

        engine.close().unwrap();
    }

    // --- Reopen and verify everything ---
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Node counts: d deleted (cascade-deleted c→d edge too), bc explicitly deleted
        assert_eq!(engine.node_count(), 3); // a, b, c (d deleted)
        assert_eq!(engine.edge_count(), 2); // ab, ac (c→d cascade-deleted with d, bc explicitly deleted)

        // Get-by-ID
        assert_eq!(engine.get_node(node_a).unwrap().unwrap().key, "a");
        assert_eq!(engine.get_node(node_b).unwrap().unwrap().key, "b");
        assert_eq!(engine.get_node(node_c).unwrap().unwrap().key, "c");
        assert!(engine.get_node(node_d).unwrap().is_none()); // deleted

        assert!(engine.get_edge(edge_ab).unwrap().is_some());
        assert!(engine.get_edge(edge_ac).unwrap().is_some());
        assert!(engine.get_edge(edge_bc).unwrap().is_none()); // deleted

        // Adjacency: a has outgoing to b and c
        let out_a = engine
            .neighbors(node_a, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert_eq!(out_a.len(), 2);
        let out_a_ids: Vec<u64> = out_a.iter().map(|e| e.node_id).collect();
        assert!(out_a_ids.contains(&node_b));
        assert!(out_a_ids.contains(&node_c));

        // b has no outgoing (b→c was deleted)
        let out_b = engine
            .neighbors(node_b, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out_b.is_empty());

        // c has outgoing c→d, but d is deleted so excluded from neighbors
        let out_c = engine
            .neighbors(node_c, Direction::Outgoing, None, 0, None, None)
            .unwrap();
        assert!(out_c.is_empty());

        // c has incoming from a (edges ab goes to b not c, ac goes to c)
        let inc_c = engine
            .neighbors(node_c, Direction::Incoming, None, 0, None, None)
            .unwrap();
        assert_eq!(inc_c.len(), 1);
        assert_eq!(inc_c[0].node_id, node_a);

        // Type filter works after replay
        let typed = engine
            .neighbors(node_a, Direction::Outgoing, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(typed.len(), 2); // both ab and ac are type 10

        // Upsert dedup still works after replay
        let mut engine = engine; // need mut for upsert
        let a_again = engine.upsert_node(1, "a", BTreeMap::new(), 0.99).unwrap();
        assert_eq!(a_again, node_a);
        assert_eq!(engine.node_count(), 3); // no new node

        // New allocation doesn't collide
        let node_e = engine.upsert_node(1, "e", BTreeMap::new(), 0.5).unwrap();
        assert!(node_e > node_d); // higher than any prior ID

        engine.close().unwrap();
    }

    // --- Third open for stability ---
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 4); // a, b, c, e (d still deleted)
        assert!(engine.get_node(node_d).unwrap().is_none());
        engine.close().unwrap();
    }
}
