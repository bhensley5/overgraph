use overgraph::{DatabaseEngine, DbOptions, Direction, PropValue};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Insert 5k nodes + 10k edges, flush, delete 30%, flush, compact,
/// verify disk shrinks and queries return correct results.
#[test]
fn test_compaction_removes_deleted_records_and_shrinks_disk() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // --- Step 1: Insert 5k nodes ---
    let mut node_ids = Vec::with_capacity(5_000);
    let batch: Vec<overgraph::NodeInput> = (0..5_000)
        .map(|i| overgraph::NodeInput {
            type_id: (i % 3) as u32 + 1, // types 1..3
            key: format!("n:{}", i),
            props: {
                let mut p = BTreeMap::new();
                p.insert("idx".to_string(), PropValue::Int(i as i64));
                p
            },
            weight: 0.5,
        })
        .collect();
    node_ids.extend(engine.batch_upsert_nodes(&batch).unwrap());
    assert_eq!(node_ids.len(), 5_000);

    // --- Step 1b: Insert 10k edges (chain + cross-links) ---
    let mut edge_ids = Vec::with_capacity(10_000);
    // Chain: node[i] → node[i+1]
    let chain: Vec<overgraph::EdgeInput> = (0..4_999)
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
    edge_ids.extend(engine.batch_upsert_edges(&chain).unwrap());

    // Cross-links: 5001 more edges
    let cross: Vec<overgraph::EdgeInput> = (0..5_001)
        .map(|i| overgraph::EdgeInput {
            from: node_ids[i % 5_000],
            to: node_ids[(i + 500) % 5_000],
            type_id: 20,
            props: BTreeMap::new(),
            weight: 0.7,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    edge_ids.extend(engine.batch_upsert_edges(&cross).unwrap());
    assert_eq!(edge_ids.len(), 10_000);

    // --- Flush to segment 1 ---
    engine.flush().unwrap();
    assert_eq!(engine.segment_count(), 1);

    // Measure disk size after first flush
    let size_before = dir_size(&db_path);

    // --- Step 2: Delete ~30% of nodes (first 1500) and their edges ---
    let delete_count = 1_500;
    for &nid in &node_ids[..delete_count] {
        engine.delete_node(nid).unwrap();
    }
    // Delete some edges too (first 3000)
    let edge_delete_count = 3_000;
    for &eid in &edge_ids[..edge_delete_count] {
        engine.delete_edge(eid).unwrap();
    }

    // --- Flush to segment 2 (tombstones go to segment) ---
    engine.flush().unwrap();
    assert_eq!(engine.segment_count(), 2);

    // Disk should be BIGGER now (two segments plus tombstone data)
    let size_after_delete_flush = dir_size(&db_path);
    assert!(
        size_after_delete_flush > size_before,
        "Disk should grow after adding tombstone segment: {} vs {}",
        size_after_delete_flush,
        size_before
    );

    // --- Step 3: Compact ---
    let stats = engine.compact().unwrap().expect("should compact 2 segments");
    assert_eq!(stats.segments_merged, 2);
    assert_eq!(engine.segment_count(), 1); // merged into one

    // Tombstoned records should be removed
    assert!(
        stats.nodes_removed > 0,
        "Expected some nodes removed, got 0"
    );
    assert!(
        stats.edges_removed > 0,
        "Expected some edges removed, got 0"
    );

    // Kept counts should be sensible
    assert!(
        stats.nodes_kept <= 5_000,
        "Kept more nodes than inserted: {}",
        stats.nodes_kept
    );

    // Disk should shrink compared to pre-compaction (two segments → one, minus tombstoned data)
    let size_after_compact = dir_size(&db_path);
    assert!(
        size_after_compact < size_after_delete_flush,
        "Disk should shrink after compaction: {} vs {}",
        size_after_compact,
        size_after_delete_flush
    );

    // --- Step 4: Verify query correctness ---

    // Deleted nodes should be gone (batch read)
    let deleted_node_results = engine.get_nodes(&node_ids[..delete_count]).unwrap();
    for (i, result) in deleted_node_results.iter().enumerate() {
        assert!(
            result.is_none(),
            "Deleted node {} still accessible after compaction",
            node_ids[i]
        );
    }

    // Surviving nodes should still be readable with correct data (batch read)
    let surviving_node_results = engine.get_nodes(&node_ids[delete_count..]).unwrap();
    for (i, result) in surviving_node_results.iter().enumerate() {
        let node = result
            .as_ref()
            .unwrap_or_else(|| panic!("Surviving node {} missing after compaction", node_ids[delete_count + i]));
        assert!(node.props.contains_key("idx"));
    }

    // Deleted edges should be gone (batch read)
    let deleted_edge_results = engine.get_edges(&edge_ids[..edge_delete_count]).unwrap();
    for (i, result) in deleted_edge_results.iter().enumerate() {
        assert!(
            result.is_none(),
            "Deleted edge {} still accessible after compaction",
            edge_ids[i]
        );
    }

    // Surviving edges should still be readable (batch read)
    let surviving_edge_results = engine.get_edges(&edge_ids[edge_delete_count..]).unwrap();
    let surviving_edge_count = surviving_edge_results.iter().filter(|r| r.is_some()).count();
    assert!(
        surviving_edge_count > 0,
        "No surviving edges found after compaction"
    );

    // Neighbor queries should work correctly on surviving nodes
    // Pick a node in the middle of the surviving range
    let mid = node_ids[delete_count + 500];
    let out = engine.neighbors(mid, Direction::Outgoing, None, 0, None, None).unwrap();
    // Should have at least one outgoing edge (chain or cross-link)
    assert!(
        !out.is_empty(),
        "Surviving node {} has no outgoing neighbors after compaction",
        mid
    );
    // All neighbor targets should be surviving nodes
    for entry in &out {
        assert!(
            engine.get_node(entry.node_id).unwrap().is_some(),
            "Neighbor target {} is a deleted node, adjacency not cleaned up",
            entry.node_id
        );
    }

    // Type-filtered neighbors should still work
    let chain_only = engine.neighbors(mid, Direction::Outgoing, Some(&[10]), 0, None, None).unwrap();
    for entry in &chain_only {
        assert_eq!(entry.edge_type_id, 10);
    }

    engine.close().unwrap();

    // --- Step 5: Reopen and verify persistence ---
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.segment_count(), 1);

    // Spot-check a few surviving nodes
    let spot = node_ids[delete_count + 100];
    let node = engine.get_node(spot).unwrap().unwrap();
    assert!(node.props.contains_key("idx"));

    // Deleted nodes still gone after reopen
    assert!(engine.get_node(node_ids[0]).unwrap().is_none());

    engine.close().unwrap();
}

/// Reads before and after compaction return consistent results.
/// Verifies the read path doesn't lose data during the compaction lifecycle.
#[test]
fn test_reads_consistent_through_compaction_lifecycle() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // --- Build a graph across 3 segments + memtable ---

    // Segment 1: nodes A, B, C with edges A→B, B→C
    let a = engine.upsert_node(1, "alpha", props(&[("v", 1)]), 0.9).unwrap();
    let b = engine.upsert_node(1, "beta", props(&[("v", 2)]), 0.8).unwrap();
    let c = engine.upsert_node(2, "gamma", props(&[("v", 3)]), 0.7).unwrap();
    let e_ab = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
    let e_bc = engine.upsert_edge(b, c, 10, BTreeMap::new(), 0.9, None, None).unwrap();
    engine.flush().unwrap();

    // Segment 2: update B's props, add node D, edge C→D, delete edge A→B
    let _ = engine.upsert_node(1, "beta", props(&[("v", 20)]), 0.85).unwrap(); // update B
    let d = engine.upsert_node(3, "delta", props(&[("v", 4)]), 0.6).unwrap();
    let e_cd = engine.upsert_edge(c, d, 20, BTreeMap::new(), 0.8, None, None).unwrap();
    engine.delete_edge(e_ab).unwrap();
    engine.flush().unwrap();

    // Segment 3: delete node C (and its edges should be excluded from neighbors)
    engine.delete_node(c).unwrap();
    let e = engine.upsert_node(1, "epsilon", props(&[("v", 5)]), 0.5).unwrap();
    let e_de = engine.upsert_edge(d, e, 10, BTreeMap::new(), 0.7, None, None).unwrap();
    engine.flush().unwrap();

    assert_eq!(engine.segment_count(), 3);

    // --- Snapshot: record expected state before compaction ---
    let pre_a = engine.get_node(a).unwrap();
    let pre_b = engine.get_node(b).unwrap();
    let pre_c = engine.get_node(c).unwrap(); // should be None (deleted)
    let pre_d = engine.get_node(d).unwrap();
    let pre_e = engine.get_node(e).unwrap();
    // Verify pre-compaction expectations
    assert!(pre_a.is_some(), "alpha should exist before compact");
    assert!(pre_b.is_some(), "beta should exist before compact");
    assert!(pre_c.is_none(), "gamma should be deleted before compact");
    assert!(pre_d.is_some(), "delta should exist before compact");
    assert!(pre_e.is_some(), "epsilon should exist before compact");
    assert!(engine.get_edge(e_ab).unwrap().is_none(), "edge A→B should be deleted before compact");
    // Edges B→C and C→D are cascade-deleted when node C is deleted
    // (delete_node scans all sources including segments)
    assert!(engine.get_edge(e_bc).unwrap().is_none(), "edge B→C should be cascade-deleted with node C");
    assert!(engine.get_edge(e_cd).unwrap().is_none(), "edge C→D should be cascade-deleted with node C");
    assert!(engine.get_edge(e_de).unwrap().is_some(), "edge D→E should exist before compact");
    assert_eq!(pre_b.as_ref().unwrap().props.get("v"), Some(&PropValue::Int(20)),
        "beta should have updated props");
    // Neighbor queries: B has no outgoing edges (B→C was cascade-deleted)
    assert!(engine.neighbors(b, Direction::Outgoing, None, 0, None, None).unwrap().is_empty(),
        "beta outgoing should be empty (B→C cascade-deleted)");
    assert_eq!(engine.neighbors(d, Direction::Outgoing, None, 0, None, None).unwrap().len(), 1,
        "delta should have D→E outgoing");

    // --- Compact ---
    let stats = engine.compact().unwrap().expect("should compact 3 segments");
    assert_eq!(stats.segments_merged, 3);
    assert_eq!(engine.segment_count(), 1);

    // --- Post-compaction: all reads should match pre-compaction ---

    // Nodes
    let post_a = engine.get_node(a).unwrap();
    let post_b = engine.get_node(b).unwrap();
    let post_c = engine.get_node(c).unwrap();
    let post_d = engine.get_node(d).unwrap();
    let post_e = engine.get_node(e).unwrap();

    assert_eq!(post_a.is_some(), pre_a.is_some(), "alpha existence changed");
    assert_eq!(post_b.is_some(), pre_b.is_some(), "beta existence changed");
    assert_eq!(post_c.is_none(), true, "gamma should still be deleted");
    assert_eq!(post_d.is_some(), pre_d.is_some(), "delta existence changed");
    assert_eq!(post_e.is_some(), pre_e.is_some(), "epsilon existence changed");

    // Beta's updated props should survive compaction
    assert_eq!(
        post_b.as_ref().unwrap().props.get("v"),
        Some(&PropValue::Int(20)),
        "beta's updated props lost during compaction"
    );

    // Alpha's original props should survive
    assert_eq!(
        post_a.as_ref().unwrap().props.get("v"),
        Some(&PropValue::Int(1)),
        "alpha's props lost during compaction"
    );

    // Edges
    let post_e_ab = engine.get_edge(e_ab).unwrap();
    let post_e_bc = engine.get_edge(e_bc).unwrap();
    let post_e_cd = engine.get_edge(e_cd).unwrap();
    let post_e_de = engine.get_edge(e_de).unwrap();

    assert!(post_e_ab.is_none(), "deleted edge A→B reappeared");
    // Edges B→C and C→D had a deleted endpoint (node C). Compaction removes
    // dangling edges to prevent stale references after tombstones are consumed.
    assert!(post_e_bc.is_none(), "edge B→C should be removed, target C deleted");
    assert!(post_e_cd.is_none(), "edge C→D should be removed, source C deleted");
    assert!(post_e_de.is_some(), "edge D→E should survive compaction");

    // Neighbor queries, consistent with edges removed
    let post_b_out = engine.neighbors(b, Direction::Outgoing, None, 0, None, None).unwrap();
    let post_d_out = engine.neighbors(d, Direction::Outgoing, None, 0, None, None).unwrap();
    let post_a_out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();

    // B had B→C before (hidden by deleted-node filter), now physically gone
    assert!(post_b_out.is_empty(), "beta should have no outgoing neighbors");
    // D→E survives; D was also target of C→D but that edge is gone
    assert_eq!(post_d_out.len(), 1, "delta should still have D→E");
    assert_eq!(post_d_out[0].node_id, e);
    // A had A→B deleted explicitly, no other outgoing
    assert!(post_a_out.is_empty(), "alpha should have no outgoing neighbors");

    // --- Verify compaction stats ---
    assert!(stats.nodes_removed >= 1, "Expected at least 1 node removed (gamma)");
    // A→B (explicit delete) + B→C (dangling target) + C→D (dangling source) = 3
    assert!(stats.edges_removed >= 3, "Expected at least 3 edges removed");

    // --- Reopen and verify persistence ---
    engine.close().unwrap();
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.segment_count(), 1);

    // Spot checks after reopen
    assert_eq!(engine.get_node(a).unwrap().unwrap().key, "alpha");
    assert_eq!(
        engine.get_node(b).unwrap().unwrap().props.get("v"),
        Some(&PropValue::Int(20))
    );
    assert!(engine.get_node(c).unwrap().is_none());
    assert!(engine.get_edge(e_ab).unwrap().is_none());
    assert!(engine.get_edge(e_bc).unwrap().is_none(), "dangling edge B→C survived reopen");
    assert!(engine.get_edge(e_cd).unwrap().is_none(), "dangling edge C→D survived reopen");
    assert!(engine.get_edge(e_de).unwrap().is_some());

    let d_out = engine.neighbors(d, Direction::Outgoing, None, 0, None, None).unwrap();
    assert_eq!(d_out.len(), 1);
    assert_eq!(d_out[0].node_id, e);

    engine.close().unwrap();
}

// --- Helpers ---

fn props(pairs: &[(&str, i64)]) -> BTreeMap<String, PropValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), PropValue::Int(*v)))
        .collect()
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                total += dir_size(&entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}
