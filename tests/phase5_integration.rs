use overgraph::{DatabaseEngine, DbOptions, NodeInput, PropValue};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Insert 1000 nodes across 5 types with varied properties across
/// multiple segments, verify find_nodes, nodes_by_type, edges_by_type work
/// correctly through flush, compact, and reopen cycles.
#[test]
fn test_secondary_indexes_across_flush_compact_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    // ---- Step 1: Build initial data, flush to segment 1 ----
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let mut all_node_ids = Vec::new();

    // Create 500 nodes across 5 types with "category" property
    let categories = ["alpha", "beta", "gamma", "delta", "epsilon"];
    let batch1: Vec<NodeInput> = (0..500)
        .map(|i| {
            let type_id = (i % 5) as u32 + 1;
            let cat = categories[i % 5];
            let mut props = BTreeMap::new();
            props.insert("category".to_string(), PropValue::String(cat.to_string()));
            props.insert("index".to_string(), PropValue::Int(i as i64));
            NodeInput {
                type_id,
                key: format!("node:{}", i),
                props,
                weight: 0.5,
            }
        })
        .collect();
    let ids1 = engine.batch_upsert_nodes(&batch1).unwrap();
    all_node_ids.extend_from_slice(&ids1);

    // Add edges: chain within each type
    for i in 0..499 {
        if batch1[i].type_id == batch1[i + 1].type_id {
            engine
                .upsert_edge(
                    ids1[i],
                    ids1[i + 1],
                    batch1[i].type_id,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .unwrap();
        }
    }

    // Flush to segment 1
    engine.flush().unwrap();
    assert_eq!(engine.segment_count(), 1);

    // ---- Step 2: Add more nodes, flush to segment 2 ----
    let batch2: Vec<NodeInput> = (500..1000)
        .map(|i| {
            let type_id = (i % 5) as u32 + 1;
            let cat = categories[i % 5];
            let mut props = BTreeMap::new();
            props.insert("category".to_string(), PropValue::String(cat.to_string()));
            props.insert("index".to_string(), PropValue::Int(i as i64));
            // Add a "status" property to even-indexed nodes
            if i % 2 == 0 {
                props.insert(
                    "status".to_string(),
                    PropValue::String("active".to_string()),
                );
            }
            NodeInput {
                type_id,
                key: format!("node:{}", i),
                props,
                weight: 0.6,
            }
        })
        .collect();
    let ids2 = engine.batch_upsert_nodes(&batch2).unwrap();
    all_node_ids.extend_from_slice(&ids2);

    // Flush to segment 2
    engine.flush().unwrap();
    assert_eq!(engine.segment_count(), 2);

    // ---- Step 3: Verify indexes across two segments ----

    // nodes_by_type: each type should have 200 nodes (1000/5)
    for type_id in 1..=5 {
        let by_type = engine.nodes_by_type(type_id).unwrap();
        assert_eq!(
            by_type.len(),
            200,
            "type {} should have 200 nodes, got {}",
            type_id,
            by_type.len()
        );
    }

    // find_nodes: each (type, category) combo has 200 nodes
    // Type 1 has category="alpha", type 2 has "beta", etc.
    for (idx, cat) in categories.iter().enumerate() {
        let type_id = idx as u32 + 1;
        let found = engine
            .find_nodes(type_id, "category", &PropValue::String(cat.to_string()))
            .unwrap();
        assert_eq!(
            found.len(),
            200,
            "type {} category '{}' should have 200 nodes, got {}",
            type_id,
            cat,
            found.len()
        );
    }

    // find_nodes with status=active: 250 nodes (500..1000 step 2 = 250),
    // distributed across 5 types = 50 per type
    for type_id in 1..=5u32 {
        let active = engine
            .find_nodes(type_id, "status", &PropValue::String("active".to_string()))
            .unwrap();
        assert_eq!(
            active.len(),
            50,
            "type {} active should have 50 nodes, got {}",
            type_id,
            active.len()
        );
    }

    // ---- Step 4: Delete some nodes, then compact ----

    // Delete the first 100 nodes (IDs from batch1)
    for &id in &all_node_ids[..100] {
        engine.delete_node(id).unwrap();
    }

    // Flush deletes to a new segment
    engine.flush().unwrap();
    assert_eq!(engine.segment_count(), 3);

    // Compact all 3 segments into 1
    let stats = engine.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 3);
    assert_eq!(stats.nodes_kept, 900); // 1000 - 100 deleted
    assert_eq!(engine.segment_count(), 1);

    // ---- Step 5: Verify indexes correct after compaction ----

    // The deleted nodes were distributed across 5 types: 20 per type deleted
    for type_id in 1..=5 {
        let by_type = engine.nodes_by_type(type_id).unwrap();
        assert_eq!(
            by_type.len(),
            180,
            "after compact, type {} should have 180 nodes, got {}",
            type_id,
            by_type.len()
        );
    }

    // find_nodes by category after compaction
    for (idx, cat) in categories.iter().enumerate() {
        let type_id = idx as u32 + 1;
        let found = engine
            .find_nodes(type_id, "category", &PropValue::String(cat.to_string()))
            .unwrap();
        assert_eq!(
            found.len(),
            180,
            "after compact, type {} category '{}' should have 180, got {}",
            type_id,
            cat,
            found.len()
        );
    }

    // Active status nodes: the first 100 deleted were all from batch1 (0..500),
    // which had no "status" property. So all 250 active nodes should survive.
    for type_id in 1..=5u32 {
        let active = engine
            .find_nodes(type_id, "status", &PropValue::String("active".to_string()))
            .unwrap();
        assert_eq!(
            active.len(),
            50,
            "after compact, type {} active should still have 50, got {}",
            type_id,
            active.len()
        );
    }

    // ---- Step 6: Close + reopen, verify persistence ----
    engine.close().unwrap();

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Same checks after reopen
    for type_id in 1..=5 {
        assert_eq!(
            engine.nodes_by_type(type_id).unwrap().len(),
            180,
            "after reopen, type {} should have 180 nodes",
            type_id
        );
    }

    for (idx, cat) in categories.iter().enumerate() {
        let type_id = idx as u32 + 1;
        let found = engine
            .find_nodes(type_id, "category", &PropValue::String(cat.to_string()))
            .unwrap();
        assert_eq!(
            found.len(),
            180,
            "after reopen, type {} category '{}' should have 180",
            type_id,
            cat
        );
    }

    // Verify a specific node from batch2 is still there with correct props
    let sample = engine.get_node(ids2[0]).unwrap().unwrap();
    assert_eq!(sample.type_id, 1); // 500 % 5 + 1 = 1
    assert_eq!(
        sample.props.get("category"),
        Some(&PropValue::String("alpha".to_string()))
    );
    assert_eq!(sample.props.get("index"), Some(&PropValue::Int(500)));

    // Verify a deleted node is gone
    assert!(engine.get_node(all_node_ids[0]).unwrap().is_none());

    // No false positives: wrong type+category combo returns empty
    assert!(engine
        .find_nodes(1, "category", &PropValue::String("beta".to_string()))
        .unwrap()
        .is_empty());
    assert!(engine
        .find_nodes(2, "category", &PropValue::String("alpha".to_string()))
        .unwrap()
        .is_empty());

    engine.close().unwrap();
}
