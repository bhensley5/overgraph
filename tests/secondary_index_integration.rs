use overgraph::{DatabaseEngine, DbOptions, NodeInput, PropValue, UpsertEdgeOptions};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Insert 1000 nodes across five labels with varied properties across
/// multiple segments, verify find_nodes and nodes_by_labels work
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
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let mut all_node_ids = Vec::new();

    // Create 500 nodes across five labels with a paired "category" property.
    let label_categories = [
        ("Person", "alpha"),
        ("Company", "beta"),
        ("Article", "gamma"),
        ("Topic", "delta"),
        ("Project", "epsilon"),
    ];
    let batch1: Vec<NodeInput> = (0..500)
        .map(|i| {
            let (label, cat) = label_categories[i % label_categories.len()];
            let mut props = BTreeMap::new();
            props.insert("category".to_string(), PropValue::String(cat.to_string()));
            props.insert("index".to_string(), PropValue::Int(i as i64));
            NodeInput {
                labels: vec![label.to_string()],
                key: format!("node:{}", i),
                props,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
            }
        })
        .collect();
    let ids1 = engine.batch_upsert_nodes(batch1.clone()).unwrap();
    all_node_ids.extend_from_slice(&ids1);

    // Add edges only when adjacent generated nodes happen to share a label.
    for i in 0..499 {
        let (label, _) = label_categories[i % label_categories.len()];
        let (next_label, _) = label_categories[(i + 1) % label_categories.len()];
        if label == next_label {
            engine
                .upsert_edge(
                    ids1[i],
                    ids1[i + 1],
                    "RELATED_TO",
                    UpsertEdgeOptions::default(),
                )
                .unwrap();
        }
    }

    // Flush to segment 1
    engine.flush().unwrap();
    assert_eq!(engine.segment_count().unwrap(), 1);

    // ---- Step 2: Add more nodes, flush to segment 2 ----
    let batch2: Vec<NodeInput> = (500..1000)
        .map(|i| {
            let (label, cat) = label_categories[i % label_categories.len()];
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
                labels: vec![label.to_string()],
                key: format!("node:{}", i),
                props,
                weight: 0.6,
                dense_vector: None,
                sparse_vector: None,
            }
        })
        .collect();
    let ids2 = engine.batch_upsert_nodes(batch2.clone()).unwrap();
    all_node_ids.extend_from_slice(&ids2);

    // Flush to segment 2
    engine.flush().unwrap();
    assert_eq!(engine.segment_count().unwrap(), 2);

    // ---- Step 3: Verify indexes across two segments ----

    // nodes_by_labels: each label should have 200 nodes (1000/5)
    for &(label, _) in &label_categories {
        let by_label = engine.nodes_by_labels(label).unwrap();
        assert_eq!(
            by_label.len(),
            200,
            "label '{}' should have 200 nodes, got {}",
            label,
            by_label.len()
        );
    }

    // find_nodes: each paired (label, category) combo has 200 nodes.
    for &(label, cat) in &label_categories {
        let found = engine
            .find_nodes(label, "category", &PropValue::String(cat.to_string()))
            .unwrap();
        assert_eq!(
            found.len(),
            200,
            "label '{}' category '{}' should have 200 nodes, got {}",
            label,
            cat,
            found.len()
        );
    }

    // find_nodes with status=active: 250 nodes (500..1000 step 2 = 250),
    // distributed across five labels = 50 per label.
    for &(label, _) in &label_categories {
        let active = engine
            .find_nodes(label, "status", &PropValue::String("active".to_string()))
            .unwrap();
        assert_eq!(
            active.len(),
            50,
            "label '{}' active should have 50 nodes, got {}",
            label,
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
    assert_eq!(engine.segment_count().unwrap(), 3);

    // Compact all 3 segments into 1
    let stats = engine.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 3);
    assert_eq!(stats.nodes_kept, 900); // 1000 - 100 deleted
    assert_eq!(engine.segment_count().unwrap(), 1);

    // ---- Step 5: Verify indexes correct after compaction ----

    // The deleted nodes were distributed across five labels: 20 per label deleted.
    for &(label, _) in &label_categories {
        let by_label = engine.nodes_by_labels(label).unwrap();
        assert_eq!(
            by_label.len(),
            180,
            "after compact, label '{}' should have 180 nodes, got {}",
            label,
            by_label.len()
        );
    }

    // find_nodes by category after compaction
    for &(label, cat) in &label_categories {
        let found = engine
            .find_nodes(label, "category", &PropValue::String(cat.to_string()))
            .unwrap();
        assert_eq!(
            found.len(),
            180,
            "after compact, label '{}' category '{}' should have 180, got {}",
            label,
            cat,
            found.len()
        );
    }

    // Active status nodes: the first 100 deleted were all from batch1 (0..500),
    // which had no "status" property. So all 250 active nodes should survive.
    for &(label, _) in &label_categories {
        let active = engine
            .find_nodes(label, "status", &PropValue::String("active".to_string()))
            .unwrap();
        assert_eq!(
            active.len(),
            50,
            "after compact, label '{}' active should still have 50, got {}",
            label,
            active.len()
        );
    }

    // ---- Step 6: Close + reopen, verify persistence ----
    engine.close().unwrap();

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Same checks after reopen
    for &(label, _) in &label_categories {
        assert_eq!(
            engine.nodes_by_labels(label).unwrap().len(),
            180,
            "after reopen, label '{}' should have 180 nodes",
            label
        );
    }

    for &(label, cat) in &label_categories {
        let found = engine
            .find_nodes(label, "category", &PropValue::String(cat.to_string()))
            .unwrap();
        assert_eq!(
            found.len(),
            180,
            "after reopen, label '{}' category '{}' should have 180",
            label,
            cat
        );
    }

    // Verify a specific node from batch2 is still there with correct props
    let sample = engine.get_node(ids2[0]).unwrap().unwrap();
    assert_eq!(sample.labels.as_slice(), ["Person"]);
    assert_eq!(
        sample.props.get("category"),
        Some(&PropValue::String("alpha".to_string()))
    );
    assert_eq!(sample.props.get("index"), Some(&PropValue::Int(500)));

    // Verify a deleted node is gone
    assert!(engine.get_node(all_node_ids[0]).unwrap().is_none());

    // No false positives: wrong label+category combo returns empty
    assert!(engine
        .find_nodes("Person", "category", &PropValue::String("beta".to_string()),)
        .unwrap()
        .is_empty());
    assert!(engine
        .find_nodes(
            "Company",
            "category",
            &PropValue::String("alpha".to_string()),
        )
        .unwrap()
        .is_empty());

    engine.close().unwrap();
}
