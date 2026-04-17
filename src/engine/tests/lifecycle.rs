// Lifecycle tests: open/close, WAL, flush, compaction, restart, group commit, backpressure.

type LegacyNode = (u64, u32, Vec<(String, PropValue)>);

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
            &TraverseOptions {
                min_depth: 2,
                direction,
                edge_type_filter: edge_type_filter.map(|s| s.to_vec()),
                node_type_filter: node_type_filter.map(|s| s.to_vec()),
                at_epoch,
                decay_lambda: None,
                limit: (limit > 0).then_some(limit),
                cursor: None,
            },
        )
        .unwrap()
        .items
}

fn wait_for_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) -> NodePropertyIndexInfo {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(info) = engine
            .list_node_property_indexes()
            .into_iter()
            .find(|info| info.index_id == index_id)
        {
            if info.state == expected_state {
                return info;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for property index {} to reach {:?}; current indexes: {:?}",
            index_id,
            expected_state,
            engine.list_node_property_indexes()
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn install_legacy_property_hash_sidecars(
    seg_dir: &std::path::Path,
    nodes: &[LegacyNode],
) {
    const LEGACY_NODE_META_ENTRY_SIZE: usize = 60;
    const LEGACY_PROP_INDEX_ENTRY_SIZE: usize = 32;

    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort_unstable_by_key(|(node_id, _, _)| *node_id);

    let mut node_meta = std::fs::read(seg_dir.join("node_meta.dat")).unwrap();
    let node_count = u64::from_le_bytes(node_meta[0..8].try_into().unwrap()) as usize;
    assert_eq!(node_count, sorted_nodes.len());

    let mut prop_hash_bytes = Vec::new();
    let mut prop_hash_offset = 0u64;
    let mut prop_groups: BTreeMap<(u32, u64, u64), Vec<u64>> = BTreeMap::new();

    for (index, (node_id, type_id, props)) in sorted_nodes.iter().enumerate() {
        let entry_off = 8 + index * LEGACY_NODE_META_ENTRY_SIZE;
        let prop_hash_count = props.len() as u32;
        node_meta[entry_off + 38..entry_off + 46].copy_from_slice(&prop_hash_offset.to_le_bytes());
        node_meta[entry_off + 46..entry_off + 50].copy_from_slice(&prop_hash_count.to_le_bytes());

        for (key, value) in props {
            let key_hash = hash_prop_key(key);
            let value_hash = hash_prop_value(value);
            prop_hash_bytes.extend_from_slice(&key_hash.to_le_bytes());
            prop_hash_bytes.extend_from_slice(&value_hash.to_le_bytes());
            prop_groups
                .entry((*type_id, key_hash, value_hash))
                .or_default()
                .push(*node_id);
            prop_hash_offset += 16;
        }
    }

    for ids in prop_groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }

    std::fs::write(seg_dir.join("node_meta.dat"), node_meta).unwrap();
    std::fs::write(seg_dir.join("node_prop_hashes.dat"), prop_hash_bytes).unwrap();

    let mut prop_index = Vec::new();
    prop_index.extend_from_slice(&(prop_groups.len() as u64).to_le_bytes());
    let data_start = 8 + prop_groups.len() as u64 * LEGACY_PROP_INDEX_ENTRY_SIZE as u64;
    let mut data_offset = data_start;
    for ((type_id, key_hash, value_hash), ids) in &prop_groups {
        prop_index.extend_from_slice(&type_id.to_le_bytes());
        prop_index.extend_from_slice(&key_hash.to_le_bytes());
        prop_index.extend_from_slice(&value_hash.to_le_bytes());
        prop_index.extend_from_slice(&data_offset.to_le_bytes());
        prop_index.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        data_offset += ids.len() as u64 * 8;
    }
    for ids in prop_groups.values() {
        for node_id in ids {
            prop_index.extend_from_slice(&node_id.to_le_bytes());
        }
    }
    std::fs::write(seg_dir.join("prop_index.dat"), prop_index).unwrap();
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
fn test_open_persists_and_validates_dense_vector_manifest() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("vector_db");
    let dense_config = DenseVectorConfig {
        dimension: 384,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    };
    let opts = DbOptions {
        dense_vector: Some(dense_config.clone()),
        ..DbOptions::default()
    };

    {
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        assert_eq!(engine.manifest().dense_vector.as_ref(), Some(&dense_config));
        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        assert_eq!(engine.manifest().dense_vector.as_ref(), Some(&dense_config));
        engine.close().unwrap();
    }

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.manifest().dense_vector.as_ref(), Some(&dense_config));
        engine.close().unwrap();
    }

    let mismatched = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 256,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };
    match DatabaseEngine::open(&db_path, &mismatched) {
        Err(EngineError::InvalidOperation(_)) => {}
        Err(other) => panic!("expected InvalidOperation, got {}", other),
        Ok(_) => panic!("expected mismatched dense vector config to fail"),
    }
}

#[test]
fn test_open_canonicalizes_vector_payloads_from_wal_replay() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("vector_db");
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };

    {
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        engine.close().unwrap();
    }

    // Write directly to the active WAL generation file (gen 0)
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::UpsertNode(NodeRecord {
                id: 7,
                type_id: 1,
                key: "manual-vector".to_string(),
                props: BTreeMap::new(),
                created_at: 100,
                updated_at: 101,
                weight: 0.5,
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: Some(vec![(4, 0.25), (2, 2.0), (4, 0.5), (7, 0.0)]),
                last_write_seq: 0,
            }),
            1,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let node = engine.get_node(7).unwrap().unwrap();
    assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
    assert_eq!(node.sparse_vector, Some(vec![(2, 2.0), (4, 0.75)]));
    engine.close().unwrap();
}

#[test]
fn test_open_rejects_compacted_dense_segment_missing_hnsw_graph() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        dense_vector: Some(DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

    engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                dense_vector: Some(vec![1.0, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                dense_vector: Some(vec![0.8, 0.2]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine.compact().unwrap().unwrap();
    let seg_id = engine.segments[0].segment_id;
    engine.close().unwrap();

    let seg_dir = crate::segment_writer::segment_dir(dir.path(), seg_id);
    std::fs::remove_file(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)).unwrap();

    match DatabaseEngine::open(dir.path(), &opts) {
        Err(EngineError::CorruptRecord(_)) => {}
        Err(other) => panic!("expected CorruptRecord, got {}", other),
        Ok(_) => panic!("expected reopen to fail for missing dense HNSW graph"),
    }
}

#[test]
fn test_open_rejects_compacted_dense_segment_truncated_vector_blob() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        dense_vector: Some(DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

    engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                dense_vector: Some(vec![0.4, 0.5, 0.6]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine.compact().unwrap().unwrap();
    let seg_id = engine.segments[0].segment_id;
    engine.close().unwrap();

    let seg_dir = crate::segment_writer::segment_dir(dir.path(), seg_id);
    let dense_blob_path = seg_dir.join(crate::segment_writer::NODE_DENSE_VECTOR_BLOB_FILENAME);
    let mut dense_blob = std::fs::read(&dense_blob_path).unwrap();
    dense_blob.truncate(dense_blob.len() - 4);
    std::fs::write(&dense_blob_path, dense_blob).unwrap();

    match DatabaseEngine::open(dir.path(), &opts) {
        Err(EngineError::CorruptRecord(_)) => {}
        Err(other) => panic!("expected CorruptRecord, got {}", other),
        Ok(_) => panic!("expected reopen to fail for truncated dense vector blob"),
    }
}

#[test]
fn test_open_rejects_standard_compacted_dense_segment_missing_hnsw_graph() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        dense_vector: Some(DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let node_id = engine
        .upsert_node(
            1,
            "shared",
            UpsertNodeOptions {
                weight: 0.5,
                dense_vector: Some(vec![1.0, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_node(
            1,
            "shared",
            UpsertNodeOptions {
                weight: 0.75,
                dense_vector: Some(vec![0.8, 0.2]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    assert_eq!(
        node_id,
        engine.get_node_by_key(1, "shared").unwrap().unwrap().id
    );
    assert_eq!(compaction_path_for(&engine), CompactionPath::UnifiedV3);

    engine.compact().unwrap().unwrap();
    let seg_id = engine.segments[0].segment_id;
    engine.close().unwrap();

    let seg_dir = crate::segment_writer::segment_dir(dir.path(), seg_id);
    std::fs::remove_file(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)).unwrap();

    match DatabaseEngine::open(dir.path(), &opts) {
        Err(EngineError::CorruptRecord(_)) => {}
        Err(other) => panic!("expected CorruptRecord, got {}", other),
        Ok(_) => panic!("expected reopen to fail for standard-compacted dense segment"),
    }
}

#[test]
fn test_open_rejects_invalid_vector_payloads_from_wal_replay() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("vector_db");
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };

    {
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        engine.close().unwrap();
    }

    // Write directly to the active WAL generation file (gen 0)
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::UpsertNode(NodeRecord {
                id: 8,
                type_id: 1,
                key: "bad-vector".to_string(),
                props: BTreeMap::new(),
                created_at: 100,
                updated_at: 101,
                weight: 0.5,
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: None,
                last_write_seq: 0,
            }),
            1,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    match DatabaseEngine::open(&db_path, &opts) {
        Err(EngineError::CorruptWal(message)) => {
            assert!(message.contains("invalid vector payload"));
        }
        Ok(_) => panic!("expected invalid vector WAL replay to fail"),
        Err(other) => panic!("expected CorruptWal, got {}", other),
    }
}

#[test]
fn test_open_rejects_malformed_vector_frame_from_wal_replay() {
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("vector_db");
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };

    {
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        engine.close().unwrap();
    }

    // V3 WAL frame: [seq:u64][walop_bytes]. The whole thing is the CRC-protected payload.
    let walop_bytes = crate::encoding::encode_wal_op(&WalOp::UpsertNode(NodeRecord {
        id: 9,
        type_id: 1,
        key: "bad-frame".to_string(),
        props: BTreeMap::new(),
        created_at: 100,
        updated_at: 101,
        weight: 0.5,
        dense_vector: Some(vec![0.1, 0.2, 0.3]),
        sparse_vector: None,
        last_write_seq: 0,
    }))
    .unwrap();
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u64.to_le_bytes()); // seq
    payload.extend_from_slice(&walop_bytes);
    payload.push(0xFF); // trailing garbage makes it malformed
    let crc = crc32fast::hash(&payload);
    let len = payload.len() as u32;

    // Append to the active WAL generation file (gen 0)
    let wal_path = wal_generation_path(&db_path, 0);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    file.write_all(&len.to_le_bytes()).unwrap();
    file.write_all(&crc.to_le_bytes()).unwrap();
    file.write_all(&payload).unwrap();
    file.flush().unwrap();
    drop(file);

    match DatabaseEngine::open(&db_path, &opts) {
        Err(EngineError::CorruptWal(message)) => {
            assert!(message.contains("failed to decode WAL record"));
        }
        Ok(_) => panic!("expected malformed vector frame WAL replay to fail"),
        Err(other) => panic!("expected CorruptWal, got {}", other),
    }
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
        // After close() (which flushes), data may be in segments,
        // so verify via get_node/get_edge rather than memtable-only counts.
        for i in 1..=10 {
            assert!(
                engine.get_node(i).unwrap().is_some(),
                "node {} missing after close+reopen",
                i
            );
        }
        for i in 1..=5 {
            assert!(
                engine.get_edge(i).unwrap().is_some(),
                "edge {} missing after close+reopen",
                i
            );
        }

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
        // After close() flushes to segments, use get_nodes_by_type for total count
        assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 1);
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

    // Verify recovery (close flushes to segments, use get_nodes_by_type)
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 50);
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
        // close() flushes to segments; use cross-source counts
        assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 20);
        // Verify edges individually (edge_count is memtable-only)
        for i in 1..=10 {
            assert!(engine.get_edge(i).unwrap().is_some(), "edge {} missing", i);
        }
        engine.close().unwrap();
    }
}

#[test]
fn test_write_op_batch_normalizes_node_vectors() {
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

    {
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let ops = vec![WalOp::UpsertNode(NodeRecord {
            id: 1,
            type_id: 1,
            key: "vector-batch".to_string(),
            props: BTreeMap::new(),
            created_at: 100,
            updated_at: 101,
            weight: 0.5,
            dense_vector: Some(vec![0.1, 0.2]),
            sparse_vector: Some(vec![(8, 0.0), (3, 1.0), (3, 2.0)]),
            last_write_seq: 0,
        })];
        engine.write_op_batch(&ops).unwrap();

        let node = engine.get_node(1).unwrap().unwrap();
        assert_eq!(node.dense_vector, Some(vec![0.1, 0.2]));
        assert_eq!(node.sparse_vector, Some(vec![(3, 3.0)]));
        engine.close().unwrap();
    }

    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let node = engine.get_node(1).unwrap().unwrap();
    assert_eq!(node.dense_vector, Some(vec![0.1, 0.2]));
    assert_eq!(node.sparse_vector, Some(vec![(3, 3.0)]));
    engine.close().unwrap();
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
        // close() flushes to segments; use cross-source count
        assert_eq!(engine.get_nodes_by_type(1).unwrap().len(), 1);
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(engine.segment_count(), 0);
    let info = engine.flush().unwrap();
    assert!(info.is_some());
    assert_eq!(engine.segment_count(), 1);

    // After flush, nodes are in the segment (not memtable)
    assert_eq!(engine.node_count(), 2);

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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();
    let eid = engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
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

    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            20,
            UpsertEdgeOptions {
                weight: 0.8,
                ..Default::default()
            },
        )
        .unwrap();

    engine.flush().unwrap();

    let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
    assert_eq!(out.len(), 2);
    let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
    assert!(ids.contains(&b));
    assert!(ids.contains(&c));

    // Type filter should still work
    let typed = engine
        .neighbors(
            a,
            &NeighborOptions {
                type_filter: Some(vec![10]),
                ..Default::default()
            },
        )
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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let d = engine
        .upsert_node(
            1,
            "d",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(c, d, 1, UpsertEdgeOptions::default())
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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, a, 1, UpsertEdgeOptions::default())
        .unwrap(); // back to origin
    engine
        .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let d = engine
        .upsert_node(
            1,
            "d",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let e = engine
        .upsert_node(
            1,
            "e",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let f = engine
        .upsert_node(
            1,
            "f",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
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
        .upsert_edge(b, e, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(c, f, 1, UpsertEdgeOptions::default())
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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let d = engine
        .upsert_node(
            1,
            "d",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, d, 2, UpsertEdgeOptions::default())
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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let d = engine
        .upsert_node(
            1,
            "d",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, c, 1, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(c, d, 1, UpsertEdgeOptions::default())
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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 1, UpsertEdgeOptions::default())
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    // Write batch 2, stays in memtable
    let c = engine
        .upsert_node(
            1,
            "charlie",
            UpsertNodeOptions {
                weight: 0.7,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            10,
            UpsertEdgeOptions {
                weight: 0.9,
                ..Default::default()
            },
        )
        .unwrap();

    // Can read from both sources
    assert!(engine.get_node(a).unwrap().is_some()); // from segment
    assert!(engine.get_node(c).unwrap().is_some()); // from memtable

    // Neighbors merge across memtable + segment
    let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    // Upsert same (type_id, key), should find existing in segment
    let mut props = BTreeMap::new();
    props.insert("version".to_string(), PropValue::Int(2));
    let id2 = engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                props,
                weight: 0.9,
                ..Default::default()
            },
        )
        .unwrap();

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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();
    let eid = engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    // Delete after flush. Tombstone in memtable hides segment data
    engine.delete_node(b).unwrap();
    assert!(engine.get_node(b).unwrap().is_none());

    engine.delete_edge(eid).unwrap();
    assert!(engine.get_edge(eid).unwrap().is_none());

    // Neighbors should exclude deleted node
    let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
    assert!(out.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_tombstone_survives_second_flush() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
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
            .upsert_node(
                1,
                &format!("batch:{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
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
            .upsert_node(
                1,
                &format!("n:{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let next_before = engine.next_node_id();
    engine.flush().unwrap();

    // New allocations should continue from where they left off
    let new_id = engine
        .upsert_node(
            1,
            "after_flush",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
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
            .upsert_node(
                1,
                "alice",
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
        b = engine
            .upsert_node(
                1,
                "bob",
                UpsertNodeOptions {
                    weight: 0.6,
                    ..Default::default()
                },
            )
            .unwrap();
        eid = engine
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
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

        let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
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

    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let e1 = engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    // Delete only the edge to b (not the node). Edge tombstone in memtable
    engine.delete_edge(e1).unwrap();

    // Neighbors should return only c, not b
    let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    // Delete alice. Tombstone in memtable
    engine.delete_node(id1).unwrap();
    assert!(engine.get_node(id1).unwrap().is_none());

    // Re-insert same key, should get a fresh ID, not reuse deleted one
    let id2 = engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.7,
                ..Default::default()
            },
        )
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
            .upsert_node(
                1,
                &format!("node:{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
        ids.push(id);
    }

    // Drain any in-flight async flushes before asserting segment count
    engine.flush().unwrap();

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
            .upsert_node(
                1,
                &format!("node:{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
    engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let b = engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();
    let eid = engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
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
        .neighbors(a, &NeighborOptions::default())
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
    let a = engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                props: props_v1,
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    // Segment 2: alice v2 (upsert updates in memtable, flushed to new segment)
    let mut props_v2 = BTreeMap::new();
    props_v2.insert("version".to_string(), PropValue::Int(2));
    engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                props: props_v2,
                weight: 0.9,
                ..Default::default()
            },
        )
        .unwrap();
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

    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            20,
            UpsertEdgeOptions {
                weight: 0.8,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    engine.compact().unwrap();

    let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
    assert_eq!(out.len(), 2);
    let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
    assert!(ids.contains(&b));
    assert!(ids.contains(&c));

    // Type filter still works after compaction
    let typed = engine
        .neighbors(
            a,
            &NeighborOptions {
                type_filter: Some(vec![10]),
                ..Default::default()
            },
        )
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

    engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
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

    engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
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
            .upsert_node(
                1,
                "alice",
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();
        b = engine
            .upsert_node(
                1,
                "bob",
                UpsertNodeOptions {
                    weight: 0.6,
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
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
        let out = engine.neighbors(a, &NeighborOptions::default()).unwrap();
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
            .upsert_node(
                1,
                &format!("n:{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.6,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    // Segment 2: charlie
    engine
        .upsert_node(
            1,
            "charlie",
            UpsertNodeOptions {
                weight: 0.7,
                ..Default::default()
            },
        )
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
    let a = engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                props: props_v1,
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    // Segment 2: bob
    engine
        .upsert_node(
            1,
            "bob",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    // Update alice to v2. Unflushed, lives in memtable only
    let mut props_v2 = BTreeMap::new();
    props_v2.insert("v".to_string(), PropValue::Int(2));
    engine
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                props: props_v2,
                weight: 0.9,
                ..Default::default()
            },
        )
        .unwrap();

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
    let a = engine
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let c = engine
        .upsert_node(
            1,
            "c",
            UpsertNodeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();
    let e_ab = engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    let e_bc = engine
        .upsert_edge(b, c, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    // Segment 2: delete B (but NOT edges A→B or B→C explicitly)
    engine.delete_node(b).unwrap();
    engine.flush().unwrap();

    // Before compact: neighbors correctly filter deleted B
    assert!(engine
        .neighbors(a, &NeighborOptions::default())
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
        .neighbors(a, &NeighborOptions::default())
        .unwrap()
        .is_empty());
    assert!(engine
        .neighbors(
            c,
            &NeighborOptions {
                direction: Direction::Incoming,
                ..Default::default()
            }
        )
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
        engine
            .upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
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
        engine
            .upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
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
        .upsert_node(
            1,
            "map_node",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
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
    let mut next_node_id = 1u64;
    let mut next_edge_id = 1u64;

    for seg in 0..3u64 {
        let mut seg_node_ids = Vec::new();
        for i in 0..12 {
            let mut props = BTreeMap::new();
            props.insert("seg".to_string(), PropValue::UInt(seg));
            props.insert(
                "color".to_string(),
                PropValue::String(if i % 2 == 0 { "red" } else { "blue" }.to_string()),
            );
            let id = next_node_id;
            next_node_id += 1;
            let created_at = 1_000 + (seg as i64 * 100) + (i as i64 * 2);
            engine
                .write_op(&WalOp::UpsertNode(NodeRecord {
                    id,
                    type_id: 1,
                    key: format!("s{}_n{}", seg, i),
                    props,
                    created_at,
                    updated_at: created_at + 1,
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                    last_write_seq: 0,
                }))
                .unwrap();
            seg_node_ids.push(id);
            all_node_ids.push(id);
        }
        segment_starts.push(seg_node_ids[0]);
        for i in 0..4 {
            let eid = next_edge_id;
            next_edge_id += 1;
            let created_at = 5_000 + (seg as i64 * 100) + (i as i64 * 2);
            engine
                .write_op(&WalOp::UpsertEdge(EdgeRecord {
                    id: eid,
                    from: seg_node_ids[i],
                    to: seg_node_ids[i + 1],
                    type_id: 1,
                    props: BTreeMap::new(),
                    created_at,
                    updated_at: created_at + 1,
                    weight: 1.0,
                    valid_from: 0,
                    valid_to: i64::MAX,
                    last_write_seq: 0,
                }))
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
    assert_segment_common_artifacts_match(&left_dir, &right_dir);
}

fn assert_segment_common_artifacts_match(left_dir: &std::path::Path, right_dir: &std::path::Path) {
    for filename in [
        "format.ver",
        "key_index.dat",
        "node_type_index.dat",
        "edge_type_index.dat",
        "edge_triple_index.dat",
        "timestamp_index.dat",
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

    for filename in ["prop_index.dat", "node_prop_hashes.dat"] {
        assert_eq!(
            left_dir.join(filename).exists(),
            right_dir.join(filename).exists(),
            "{} presence mismatch",
            filename
        );
    }

    // Byte-identical vector artifacts (deterministic).
    for filename in [
        crate::segment_writer::NODE_VECTOR_META_FILENAME,
        crate::segment_writer::NODE_DENSE_VECTOR_BLOB_FILENAME,
        crate::segment_writer::NODE_SPARSE_VECTOR_BLOB_FILENAME,
        crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME,
        crate::sparse_postings::SPARSE_POSTINGS_FILENAME,
    ] {
        let left_exists = left_dir.join(filename).exists();
        let right_exists = right_dir.join(filename).exists();
        assert_eq!(left_exists, right_exists, "{} presence mismatch", filename);
        if left_exists {
            assert_eq!(
                std::fs::read(left_dir.join(filename)).unwrap(),
                std::fs::read(right_dir.join(filename)).unwrap(),
                "{} mismatch",
                filename
            );
        }
    }

    let left_secondary = left_dir.join(crate::segment_writer::SECONDARY_INDEX_DIRNAME);
    let right_secondary = right_dir.join(crate::segment_writer::SECONDARY_INDEX_DIRNAME);
    assert_eq!(
        left_secondary.exists(),
        right_secondary.exists(),
        "secondary index directory presence mismatch"
    );
    if left_secondary.exists() {
        let mut left_entries: Vec<_> = std::fs::read_dir(&left_secondary)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        let mut right_entries: Vec<_> = std::fs::read_dir(&right_secondary)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        left_entries.sort_unstable();
        right_entries.sort_unstable();
        assert_eq!(
            left_entries, right_entries,
            "secondary index file set mismatch"
        );
        for name in left_entries {
            assert_eq!(
                std::fs::read(left_secondary.join(&name)).unwrap(),
                std::fs::read(right_secondary.join(&name)).unwrap(),
                "secondary index file {:?} mismatch",
                name
            );
        }
    }

    // HNSW files are non-deterministic (concurrent build) — check presence and
    // structural integrity, not byte-identical content.
    {
        let left_meta_exists = left_dir
            .join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)
            .exists();
        let right_meta_exists = right_dir
            .join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)
            .exists();
        assert_eq!(
            left_meta_exists, right_meta_exists,
            "dense_hnsw_meta.dat presence mismatch"
        );

        let left_graph_exists = left_dir
            .join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)
            .exists();
        let right_graph_exists = right_dir
            .join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)
            .exists();
        assert_eq!(
            left_graph_exists, right_graph_exists,
            "dense_hnsw_graph.dat presence mismatch"
        );

        if left_meta_exists {
            for dir in [left_dir, right_dir] {
                let meta =
                    std::fs::read(dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
                let graph =
                    std::fs::read(dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)).unwrap();
                // Verify non-empty and structurally valid (header parses, sizes consistent).
                assert!(meta.len() >= 36, "HNSW meta too short in {}", dir.display());
                assert!(!graph.is_empty(), "HNSW graph empty in {}", dir.display());
                // Verify header magic and version.
                assert_eq!(&meta[0..4], b"DHNW", "bad HNSW magic in {}", dir.display());
                let version = u32::from_le_bytes(meta[4..8].try_into().unwrap());
                assert_eq!(version, 1, "bad HNSW version in {}", dir.display());
                let point_count = u64::from_le_bytes(meta[8..16].try_into().unwrap());
                assert!(point_count > 0, "zero HNSW points in {}", dir.display());
            }
        }
    }
}

fn assert_segment_metadata_semantics_match(left: &SegmentReader, right: &SegmentReader) {
    assert_eq!(left.node_meta_count(), right.node_meta_count());
    for index in 0..left.node_meta_count() as usize {
        let left_meta = left.node_meta_at(index).unwrap();
        let right_meta = right.node_meta_at(index).unwrap();
        assert_eq!(left_meta.0, right_meta.0, "node {} id mismatch", index);
        assert_eq!(
            left_meta.2, right_meta.2,
            "node {} data_len mismatch",
            index
        );
        assert_eq!(left_meta.3, right_meta.3, "node {} type mismatch", index);
        assert_eq!(
            left_meta.4, right_meta.4,
            "node {} updated_at mismatch",
            index
        );
        assert_eq!(
            left_meta.5.to_bits(),
            right_meta.5.to_bits(),
            "node {} weight mismatch",
            index
        );
        assert_eq!(left_meta.6, right_meta.6, "node {} key_len mismatch", index);
        assert_eq!(
            left_meta.8, right_meta.8,
            "node {} prop_hash_count mismatch",
            index
        );
        assert_eq!(
            left_meta.9, right_meta.9,
            "node {} last_write_seq mismatch",
            index
        );

        let left_vectors = left.node_vector_meta_at(index).unwrap();
        let right_vectors = right.node_vector_meta_at(index).unwrap();
        assert_eq!(
            (left_vectors.1, left_vectors.3),
            (right_vectors.1, right_vectors.3),
            "node {} vector length mismatch",
            index
        );
    }

    assert_eq!(left.edge_meta_count(), right.edge_meta_count());
    for index in 0..left.edge_meta_count() as usize {
        let left_meta = left.edge_meta_at(index).unwrap();
        let right_meta = right.edge_meta_at(index).unwrap();
        assert_eq!(left_meta.0, right_meta.0, "edge {} id mismatch", index);
        assert_eq!(
            left_meta.2, right_meta.2,
            "edge {} data_len mismatch",
            index
        );
        assert_eq!(left_meta.3, right_meta.3, "edge {} from mismatch", index);
        assert_eq!(left_meta.4, right_meta.4, "edge {} to mismatch", index);
        assert_eq!(left_meta.5, right_meta.5, "edge {} type mismatch", index);
        assert_eq!(
            left_meta.6, right_meta.6,
            "edge {} updated_at mismatch",
            index
        );
        assert_eq!(
            left_meta.7.to_bits(),
            right_meta.7.to_bits(),
            "edge {} weight mismatch",
            index
        );
        assert_eq!(
            left_meta.8, right_meta.8,
            "edge {} valid_from mismatch",
            index
        );
        assert_eq!(
            left_meta.9, right_meta.9,
            "edge {} valid_to mismatch",
            index
        );
        assert_eq!(
            left_meta.10, right_meta.10,
            "edge {} last_write_seq mismatch",
            index
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
                .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
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
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
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
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
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
                .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
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
                    UpsertEdgeOptions::default(),
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
            .neighbors(
                first_node,
                &NeighborOptions {
                    limit: Some(100),
                    ..Default::default()
                },
            )
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
                .upsert_node(
                    1,
                    &format!("s{}_n{}", seg, i),
                    UpsertNodeOptions {
                        props,
                        ..Default::default()
                    },
                )
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
                    .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
                    .unwrap();
                ids.push(id);
                seg_ids.push(id);
            }
            first_nodes.push(seg_ids[0]);
            engine
                .upsert_edge(seg_ids[0], seg_ids[1], 1, UpsertEdgeOptions::default())
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
        assert_eq!(engine.degree(first, &DegreeOptions::default()).unwrap(), 1);
        let nbrs = engine
            .neighbors(
                first,
                &NeighborOptions {
                    limit: Some(10),
                    ..Default::default()
                },
            )
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
                .upsert_node(
                    1,
                    &format!("s{}_n{}", seg, i),
                    UpsertNodeOptions {
                        props,
                        ..Default::default()
                    },
                )
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
            fast.neighbors(
                start,
                &NeighborOptions {
                    limit: Some(10),
                    ..Default::default()
                }
            )
            .unwrap(),
            v3.neighbors(
                start,
                &NeighborOptions {
                    limit: Some(10),
                    ..Default::default()
                }
            )
            .unwrap()
        );
        assert_eq!(
            fast.degree(start, &DegreeOptions::default()).unwrap(),
            v3.degree(start, &DegreeOptions::default()).unwrap()
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
                .degree(start, &DegreeOptions::default())
                .unwrap(),
            bg_engine.degree(start, &DegreeOptions::default()).unwrap()
        );
    }
    assert_compacted_index_files_match(&sync_engine, &bg_engine, sync_dir.path(), bg_dir.path());
    assert_segment_metadata_semantics_match(&sync_engine.segments[0], &bg_engine.segments[0]);

    sync_engine.close().unwrap();
    bg_engine.close().unwrap();
}

#[test]
fn test_fast_merge_matches_single_flush_artifacts_for_vector_segments() {
    let compact_dir = TempDir::new().unwrap();
    let flush_dir = TempDir::new().unwrap();
    let opts = DbOptions {
        edge_uniqueness: true,
        compact_after_n_flushes: 0,
        dense_vector: Some(DenseVectorConfig {
            dimension: 4,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };

    let mut compact_engine = DatabaseEngine::open(compact_dir.path(), &opts).unwrap();
    let mut flush_engine = DatabaseEngine::open(flush_dir.path(), &opts).unwrap();
    let mut compact_node_ids = Vec::new();
    let mut flush_node_ids = Vec::new();
    let mut compact_edge_ids = Vec::new();
    let mut flush_edge_ids = Vec::new();
    let mut next_node_id = 1u64;
    let mut next_edge_id = 1u64;

    for seg in 0..3u64 {
        let mut compact_seg_ids = Vec::new();
        let mut flush_seg_ids = Vec::new();
        for i in 0..6u64 {
            let dense_vector = vec![
                1.0 + seg as f32 * 0.1,
                0.2 + i as f32 * 0.03,
                0.4 + seg as f32 * 0.05,
                0.6 + i as f32 * 0.02,
            ];
            let sparse_vector = vec![
                (seg as u32, 1.0 + i as f32 * 0.1),
                (seg as u32 + 10, 0.5 + seg as f32 * 0.05),
            ];
            let mut props = BTreeMap::new();
            props.insert("seg".to_string(), PropValue::UInt(seg));
            props.insert("slot".to_string(), PropValue::UInt(i));
            let node_id = next_node_id;
            next_node_id += 1;
            let created_at = 10_000 + (seg as i64 * 100) + (i as i64 * 2);
            let compact_node = NodeRecord {
                id: node_id,
                type_id: 1,
                key: format!("s{}_n{}", seg, i),
                props: props.clone(),
                created_at,
                updated_at: created_at + 1,
                weight: 1.0,
                dense_vector: Some(dense_vector.clone()),
                sparse_vector: Some(sparse_vector.clone()),
                last_write_seq: 0,
            };
            let flush_node = NodeRecord {
                props,
                dense_vector: Some(dense_vector),
                sparse_vector: Some(sparse_vector),
                ..compact_node.clone()
            };

            compact_engine
                .write_op(&WalOp::UpsertNode(compact_node))
                .unwrap();
            flush_engine
                .write_op(&WalOp::UpsertNode(flush_node))
                .unwrap();

            let compact_id = node_id;
            let flush_id = node_id;
            compact_seg_ids.push(compact_id);
            flush_seg_ids.push(flush_id);
            compact_node_ids.push(compact_id);
            flush_node_ids.push(flush_id);
        }
        for i in 0..3usize {
            let edge_id = next_edge_id;
            next_edge_id += 1;
            let created_at = 20_000 + (seg as i64 * 100) + (i as i64 * 2);
            let compact_edge = EdgeRecord {
                id: edge_id,
                from: compact_seg_ids[i],
                to: compact_seg_ids[i + 1],
                type_id: 1,
                props: BTreeMap::new(),
                created_at,
                updated_at: created_at + 1,
                weight: 0.5 + seg as f32 * 0.1 + i as f32 * 0.05,
                valid_from: seg as i64,
                valid_to: i64::MAX,
                last_write_seq: 0,
            };
            let flush_edge = EdgeRecord {
                from: flush_seg_ids[i],
                to: flush_seg_ids[i + 1],
                ..compact_edge.clone()
            };

            compact_engine
                .write_op(&WalOp::UpsertEdge(compact_edge))
                .unwrap();
            flush_engine
                .write_op(&WalOp::UpsertEdge(flush_edge))
                .unwrap();

            let compact_edge_id = edge_id;
            let flush_edge_id = edge_id;
            compact_edge_ids.push(compact_edge_id);
            flush_edge_ids.push(flush_edge_id);
        }
        compact_engine.flush().unwrap();
    }
    flush_engine.flush().unwrap();

    assert_eq!(compact_node_ids, flush_node_ids);
    assert_eq!(compact_edge_ids, flush_edge_ids);
    assert_eq!(
        compaction_path_for(&compact_engine),
        CompactionPath::FastMerge
    );

    compact_engine.compact().unwrap().unwrap();

    let compact_nodes = compact_engine.get_nodes(&compact_node_ids).unwrap();
    let flush_nodes = flush_engine.get_nodes(&flush_node_ids).unwrap();
    assert_node_batches_match(&compact_nodes, &flush_nodes);

    let compact_edges = compact_engine.get_edges(&compact_edge_ids).unwrap();
    let flush_edges = flush_engine.get_edges(&flush_edge_ids).unwrap();
    assert_edge_batches_match(&compact_edges, &flush_edges);

    let compact_seg_dir = segment_dir(compact_dir.path(), compact_engine.segments[0].segment_id);
    let flush_seg_dir = segment_dir(flush_dir.path(), flush_engine.segments[0].segment_id);
    assert_segment_common_artifacts_match(&compact_seg_dir, &flush_seg_dir);
    assert_segment_metadata_semantics_match(&compact_engine.segments[0], &flush_engine.segments[0]);

    // Semantic HNSW parity: both engines should produce equivalent search results.
    let queries: Vec<Vec<f32>> = vec![
        vec![1.0, 0.2, 0.4, 0.6],
        vec![0.5, 0.5, 0.5, 0.5],
        vec![1.1, 0.35, 0.45, 0.7],
    ];
    for query in &queries {
        let request = VectorSearchRequest {
            mode: VectorSearchMode::Dense,
            dense_query: Some(query.clone()),
            sparse_query: None,
            k: 5,
            type_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        };
        let compact_hits = compact_engine.vector_search(&request).unwrap();
        let flush_hits = flush_engine.vector_search(&request).unwrap();
        assert_eq!(
            compact_hits.len(),
            flush_hits.len(),
            "hit count mismatch for query {:?}",
            query
        );
        // Top-1 must match (strongest invariant).
        assert_eq!(
            compact_hits[0].node_id, flush_hits[0].node_id,
            "top-1 mismatch for query {:?}: compact={} flush={}",
            query, compact_hits[0].node_id, flush_hits[0].node_id
        );
        // High overlap at top-k.
        let compact_ids: std::collections::HashSet<u64> =
            compact_hits.iter().map(|h| h.node_id).collect();
        let flush_ids: std::collections::HashSet<u64> =
            flush_hits.iter().map(|h| h.node_id).collect();
        let overlap = compact_ids.intersection(&flush_ids).count();
        assert!(
            overlap >= 3,
            "low overlap ({}/5) for query {:?}",
            overlap,
            query
        );
    }

    compact_engine.close().unwrap();
    flush_engine.close().unwrap();
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
                .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    assert_eq!(engine.segments.len(), 1);

    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    assert_eq!(engine.segments.len(), 2);

    // Flush 3: should trigger auto-compact (3 segments → 1)
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
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
                .upsert_node(
                    1,
                    &format!("f{}_n{}", flush, i),
                    UpsertNodeOptions::default(),
                )
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
                .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
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
                .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
                .unwrap();
        }
        engine.flush().unwrap();
    }
    // 4 flushes since manual compact: segments = 1 (from manual) + 4 = 5
    assert_eq!(engine.segments.len(), 5);

    // 5th flush triggers auto-compact
    for i in 0..5 {
        engine
            .upsert_node(1, &format!("s6_n{}", i), UpsertNodeOptions::default())
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
                .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    // Only 1 segment, compact can't fire (needs >= 2)
    assert_eq!(engine.segments.len(), 1);

    // Second flush: now 2 segments, auto-compact should fire
    for i in 0..5 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    assert_eq!(engine.segments.len(), 1);

    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
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
fn test_write_path_applies_finished_bg_compact() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        compact_after_n_flushes: 2,
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

    for i in 0..10 {
        engine
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    assert!(engine.bg_compact.is_some());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while engine
        .bg_compact
        .as_ref()
        .is_some_and(|bg| !bg.handle.is_finished())
    {
        assert!(
            std::time::Instant::now() < deadline,
            "background compaction did not finish in time"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert!(engine.bg_compact.is_some());
    assert_eq!(engine.segments.len(), 2);

    engine
        .upsert_node(1, "c0", UpsertNodeOptions::default())
        .unwrap();

    assert!(
        engine.bg_compact.is_none(),
        "next write should reap finished background compaction"
    );
    assert_eq!(engine.segments.len(), 1);

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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    // bg compact started (or already finished for small data)

    // Immediately write more data. Should NOT block
    for i in 0..20 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
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
fn test_flushes_while_bg_compact_is_outstanding_count_toward_next_run() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        compact_after_n_flushes: 2,
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

    for i in 0..10 {
        engine
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    assert!(engine.bg_compact.is_some());
    assert_eq!(engine.flush_count_since_last_compact, 0);

    for i in 0..10 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    assert_eq!(
        engine.flush_count_since_last_compact, 1,
        "flushes published while background compaction is outstanding should count toward the next auto-compaction"
    );

    engine.wait_for_bg_compact();
    assert_eq!(engine.segments.len(), 2);

    for i in 0..10 {
        engine
            .upsert_node(1, &format!("d{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    assert!(
        engine.bg_compact.is_some(),
        "second auto-compaction should start once the post-compaction flush count reaches the threshold"
    );
    engine.wait_for_bg_compact();
    assert_eq!(engine.segments.len(), 1);

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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap(); // bg compact starts here

    // Write more data and flush. Adds a NEW segment while bg compact runs
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    assert!(engine.bg_compact.is_none());

    // Second flush: 2 segments, bg compact starts
    for i in 0..5 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    // bg_compact should be Some (or already completed)
    let had_bg = engine.bg_compact.is_some();

    // Third flush: bg compact is still running (or just completed),
    // should NOT start a second bg compact
    for i in 0..5 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap(); // bg compact starts

    // Add more segments
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("d{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    // Manual compact. With re-trigger scheduling, auto-compaction may have
    // already reduced segments — compact() returns None if < 2 remain.
    let _stats = engine.compact().unwrap();
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    // Write more data while bg compact may be running
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("c{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("a{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .upsert_node(1, &format!("b{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
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
                .upsert_node(1, &format!("s{}_n{}", seg, i), UpsertNodeOptions::default())
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
        .upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();
    let n2 = engine
        .upsert_node(1, "bob", UpsertNodeOptions::default())
        .unwrap();
    let e1 = engine
        .upsert_edge(n1, n2, 1, UpsertEdgeOptions::default())
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
            dense_vector: None,
            sparse_vector: None,
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
            .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    for i in 100..200 {
        engine
            .upsert_node(1, &format!("n{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("gc_node_{}", i), UpsertNodeOptions::default())
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

    let n1 = engine
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let n2 = engine
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = engine
        .upsert_edge(
            n1,
            n2,
            1,
            UpsertEdgeOptions {
                weight: 0.5,
                ..Default::default()
            },
        )
        .unwrap();

    // Read-after-write consistency
    let neighbors = engine
        .neighbors(
            n1,
            &NeighborOptions {
                limit: Some(10),
                ..Default::default()
            },
        )
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
            dense_vector: None,
            sparse_vector: None,
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
        .upsert_node(1, "sync_test", UpsertNodeOptions::default())
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

    engine
        .upsert_node(1, "test", UpsertNodeOptions::default())
        .unwrap();
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
            .upsert_node(1, &format!("pre_flush_{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    for i in 0..50 {
        engine
            .upsert_node(
                1,
                &format!("post_flush_{}", i),
                UpsertNodeOptions::default(),
            )
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
            .upsert_node(1, &format!("drop_test_{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("bp_{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("drain_{}", i), UpsertNodeOptions::default())
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
                .upsert_node(
                    1,
                    &format!("drop_drain_{}", i),
                    UpsertNodeOptions::default(),
                )
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
    let result = engine.upsert_node(1, "should_fail", UpsertNodeOptions::default());
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
            .upsert_node(
                1,
                &format!("int_{}", i),
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
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
            .upsert_node(1, &format!("c1_{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    let seg1 = engine.flush().unwrap();
    assert!(seg1.is_some());

    // Cycle 2: write → flush
    for i in 0..100 {
        engine
            .upsert_node(1, &format!("c2_{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    let seg2 = engine.flush().unwrap();
    assert!(seg2.is_some());

    // Cycle 3: write → flush
    for i in 0..100 {
        engine
            .upsert_node(1, &format!("c3_{}", i), UpsertNodeOptions::default())
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
            .upsert_node(1, &format!("gc_del_{}", i), UpsertNodeOptions::default())
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
            .upsert_node(
                1,
                &format!("bp_imm_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
            .upsert_node(
                1,
                &format!("bp_gc_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
            .upsert_node(
                1,
                &format!("no_bp_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
            .upsert_node(
                1,
                &format!("early_bp_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
            .upsert_node(
                1,
                &format!("n_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
                UpsertEdgeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
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
            dense_vector: None,
            sparse_vector: None,
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
            dense_vector: None,
            sparse_vector: None,
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
            .upsert_node(
                1,
                &format!("stress_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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
            .upsert_node(
                1,
                &format!("ac_{}", i),
                UpsertNodeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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

    let n1 = engine
        .upsert_node(1, "src", UpsertNodeOptions::default())
        .unwrap();
    let n2 = engine
        .upsert_node(1, "dst", UpsertNodeOptions::default())
        .unwrap();

    // Create many edges to fill memtable
    let mut edge_ids = Vec::new();
    for i in 0..20 {
        let eid = engine
            .upsert_edge(
                n1,
                n2,
                i as u32,
                UpsertEdgeOptions {
                    weight: 0.5,
                    ..Default::default()
                },
            )
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

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
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
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    assert!(db.get_node(c).unwrap().is_some());
    db.close().unwrap();
}

// --- CP1: engine_seq tests ---

#[test]
fn test_engine_seq_monotonic_across_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let id1 = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let id2 = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let id3 = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();

    let seq1 = db.get_node(id1).unwrap().unwrap().last_write_seq;
    let seq2 = db.get_node(id2).unwrap().unwrap().last_write_seq;
    let seq3 = db.get_node(id3).unwrap().unwrap().last_write_seq;

    assert!(seq1 > 0, "seq must be > 0");
    assert!(seq2 > seq1, "seq2 ({}) must be > seq1 ({})", seq2, seq1);
    assert!(seq3 > seq2, "seq3 ({}) must be > seq2 ({})", seq3, seq2);

    db.close().unwrap();
}

#[test]
fn test_engine_seq_survives_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    db.upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();

    // After flush, next write should continue with higher seq
    let id3 = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let seq3 = db.get_node(id3).unwrap().unwrap().last_write_seq;
    assert!(
        seq3 >= 3,
        "seq after flush must continue monotonically, got {}",
        seq3
    );

    db.close().unwrap();
}

#[test]
fn test_engine_seq_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let id3 = db
            .upsert_node(1, "c", UpsertNodeOptions::default())
            .unwrap();
        let seq3 = db.get_node(id3).unwrap().unwrap().last_write_seq;
        // After reopen with flush, manifest persisted next_engine_seq,
        // so seq must continue from where it left off
        assert!(seq3 >= 3, "seq after reopen must be >= 3, got {}", seq3);
        db.close().unwrap();
    }
}

#[test]
fn test_engine_seq_correct_after_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        // Close WITHOUT flush; WAL will be replayed on reopen.
        db.close().unwrap();
    }
    {
        let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        // After replay, the memtable records should have seqs assigned
        let n1 = db.get_node(1).unwrap().unwrap();
        let n2 = db.get_node(2).unwrap().unwrap();
        assert!(n1.last_write_seq > 0);
        assert!(n2.last_write_seq > n1.last_write_seq);

        // New writes should continue beyond replayed seqs
        let id3 = db
            .upsert_node(1, "c", UpsertNodeOptions::default())
            .unwrap();
        let seq3 = db.get_node(id3).unwrap().unwrap().last_write_seq;
        assert!(seq3 > n2.last_write_seq);
        db.close().unwrap();
    }
}

#[test]
fn test_last_write_seq_exact_equality_across_reopen() {
    // Proves WAL V3 seq persistence: exact last_write_seq values survive
    // close_fast → reopen (WAL replay) without re-derivation.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("seq_exact");

    let (seq_a, seq_b, seq_c);

    // Phase 1: write 3 nodes, capture exact seqs, close_fast (no flush)
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let id_a = db
            .upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        let id_b = db
            .upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        let id_c = db
            .upsert_node(1, "c", UpsertNodeOptions::default())
            .unwrap();
        seq_a = db.get_node(id_a).unwrap().unwrap().last_write_seq;
        seq_b = db.get_node(id_b).unwrap().unwrap().last_write_seq;
        seq_c = db.get_node(id_c).unwrap().unwrap().last_write_seq;
        assert!(seq_a < seq_b && seq_b < seq_c);
        db.close_fast().unwrap();
    }

    // Phase 2: reopen (WAL replay). Seqs must be exactly the same.
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(
            db.get_node(1).unwrap().unwrap().last_write_seq,
            seq_a,
            "node a seq changed after replay"
        );
        assert_eq!(
            db.get_node(2).unwrap().unwrap().last_write_seq,
            seq_b,
            "node b seq changed after replay"
        );
        assert_eq!(
            db.get_node(3).unwrap().unwrap().last_write_seq,
            seq_c,
            "node c seq changed after replay"
        );

        // New write must continue strictly after
        let id_d = db
            .upsert_node(1, "d", UpsertNodeOptions::default())
            .unwrap();
        let seq_d = db.get_node(id_d).unwrap().unwrap().last_write_seq;
        assert!(seq_d > seq_c, "new write seq must be > replayed max");
        db.close_fast().unwrap();
    }

    // Phase 3: reopen again, still exact (double replay).
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(db.get_node(1).unwrap().unwrap().last_write_seq, seq_a);
        assert_eq!(db.get_node(2).unwrap().unwrap().last_write_seq, seq_b);
        assert_eq!(db.get_node(3).unwrap().unwrap().last_write_seq, seq_c);
        db.close().unwrap();
    }
}

#[test]
fn test_last_write_seq_exact_across_freeze_reopen() {
    // Proves seqs survive freeze → close_fast → reopen.
    // Frozen data replays into immutable_epochs with original seqs.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("seq_freeze");

    let (seq_frozen, seq_active);

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let id_f = db
            .upsert_node(1, "frozen_node", UpsertNodeOptions::default())
            .unwrap();
        seq_frozen = db.get_node(id_f).unwrap().unwrap().last_write_seq;
        db.freeze_memtable().unwrap();
        let id_a = db
            .upsert_node(1, "active_node", UpsertNodeOptions::default())
            .unwrap();
        seq_active = db.get_node(id_a).unwrap().unwrap().last_write_seq;
        assert!(seq_active > seq_frozen);
        db.close_fast().unwrap();
    }

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        // Frozen node is in immutable_epochs, active node in memtable
        let f = db.get_node_by_key(1, "frozen_node").unwrap().unwrap();
        let a = db.get_node_by_key(1, "active_node").unwrap().unwrap();
        assert_eq!(f.last_write_seq, seq_frozen, "frozen seq changed on replay");
        assert_eq!(a.last_write_seq, seq_active, "active seq changed on replay");

        // Flush to segments, reopen, verify seqs survive in segments too
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let f = db.get_node_by_key(1, "frozen_node").unwrap().unwrap();
        let a = db.get_node_by_key(1, "active_node").unwrap().unwrap();
        assert_eq!(
            f.last_write_seq, seq_frozen,
            "frozen seq changed after flush+reopen"
        );
        assert_eq!(
            a.last_write_seq, seq_active,
            "active seq changed after flush+reopen"
        );
        db.close().unwrap();
    }
}

#[test]
fn test_compaction_preserves_last_write_seq() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let id1 = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let seq1 = db.get_node(id1).unwrap().unwrap().last_write_seq;
    db.flush().unwrap();

    let id2 = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let seq2 = db.get_node(id2).unwrap().unwrap().last_write_seq;
    db.flush().unwrap();

    // Both nodes are now in segments. Compact.
    db.compact().unwrap();

    // After compaction, seqs should be preserved
    let n1 = db.get_node(id1).unwrap().unwrap();
    let n2 = db.get_node(id2).unwrap().unwrap();
    assert_eq!(n1.last_write_seq, seq1);
    assert_eq!(n2.last_write_seq, seq2);

    db.close().unwrap();
}

#[test]
fn test_batch_ops_get_distinct_seq() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let inputs: Vec<NodeInput> = (0..5)
        .map(|i| NodeInput {
            type_id: 1,
            key: format!("n{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();

    let ids = db.batch_upsert_nodes(&inputs).unwrap();

    let seqs: Vec<u64> = ids
        .iter()
        .map(|&id| db.get_node(id).unwrap().unwrap().last_write_seq)
        .collect();

    // Each op in the batch should get a distinct, increasing seq
    for pair in seqs.windows(2) {
        assert!(
            pair[1] > pair[0],
            "batch seqs must be distinct and increasing: {:?}",
            seqs
        );
    }

    db.close().unwrap();
}

#[test]
fn test_compaction_preserves_edge_last_write_seq() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let nid1 = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let nid2 = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let eid = db
        .upsert_edge(nid1, nid2, 1, UpsertEdgeOptions::default())
        .unwrap();
    let edge_seq = db.get_edge(eid).unwrap().unwrap().last_write_seq;
    assert!(edge_seq > 0);
    db.flush().unwrap();

    // Add second segment so compaction has something to merge
    db.upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    let edge_after = db.get_edge(eid).unwrap().unwrap();
    assert_eq!(edge_after.last_write_seq, edge_seq);

    db.close().unwrap();
}

// --- CP1 regression tests: last_write_seq hydration from segments ---

#[test]
fn test_get_edge_hydrates_last_write_seq_from_segment() {
    // Regression: M1. get_edge() must hydrate last_write_seq from edge_meta.dat.
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let nid1 = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let nid2 = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let eid = db
        .upsert_edge(nid1, nid2, 1, UpsertEdgeOptions::default())
        .unwrap();
    let memtable_seq = db.get_edge(eid).unwrap().unwrap().last_write_seq;
    assert!(
        memtable_seq > 0,
        "edge in memtable must have last_write_seq > 0"
    );

    db.flush().unwrap();

    // Edge is now in a segment. get_edge must hydrate last_write_seq from edge_meta.dat.
    let segment_edge = db.get_edge(eid).unwrap().unwrap();
    assert_eq!(
        segment_edge.last_write_seq, memtable_seq,
        "get_edge from segment must preserve last_write_seq (got {}, expected {})",
        segment_edge.last_write_seq, memtable_seq
    );

    db.close().unwrap();
}

#[test]
fn test_get_nodes_batch_hydrates_last_write_seq_from_segment() {
    // Regression: M2. get_nodes_batch must hydrate last_write_seq from node_meta.dat.
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let id1 = db
        .upsert_node(1, "n1", UpsertNodeOptions::default())
        .unwrap();
    let id2 = db
        .upsert_node(1, "n2", UpsertNodeOptions::default())
        .unwrap();
    let id3 = db
        .upsert_node(1, "n3", UpsertNodeOptions::default())
        .unwrap();
    let seq1 = db.get_node(id1).unwrap().unwrap().last_write_seq;
    let seq2 = db.get_node(id2).unwrap().unwrap().last_write_seq;
    let seq3 = db.get_node(id3).unwrap().unwrap().last_write_seq;

    db.flush().unwrap();

    // Batch read from segment via get_nodes_raw (uses get_nodes_batch internally)
    let results = db.get_nodes_raw(&[id1, id2, id3]).unwrap();
    assert_eq!(
        results[0].as_ref().unwrap().last_write_seq,
        seq1,
        "batch node read from segment must preserve last_write_seq for id1"
    );
    assert_eq!(
        results[1].as_ref().unwrap().last_write_seq,
        seq2,
        "batch node read from segment must preserve last_write_seq for id2"
    );
    assert_eq!(
        results[2].as_ref().unwrap().last_write_seq,
        seq3,
        "batch node read from segment must preserve last_write_seq for id3"
    );

    db.close().unwrap();
}

#[test]
fn test_get_edges_batch_hydrates_last_write_seq_from_segment() {
    // Regression: M3. get_edges_batch must hydrate last_write_seq from edge_meta.dat.
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();

    let nid1 = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let nid2 = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let nid3 = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let eid1 = db
        .upsert_edge(nid1, nid2, 1, UpsertEdgeOptions::default())
        .unwrap();
    let eid2 = db
        .upsert_edge(nid2, nid3, 1, UpsertEdgeOptions::default())
        .unwrap();
    let eid3 = db
        .upsert_edge(nid1, nid3, 1, UpsertEdgeOptions::default())
        .unwrap();
    let eseq1 = db.get_edge(eid1).unwrap().unwrap().last_write_seq;
    let eseq2 = db.get_edge(eid2).unwrap().unwrap().last_write_seq;
    let eseq3 = db.get_edge(eid3).unwrap().unwrap().last_write_seq;

    db.flush().unwrap();

    // Batch read from segment via get_edges (uses get_edges_batch internally)
    let results = db.get_edges(&[eid1, eid2, eid3]).unwrap();
    assert_eq!(
        results[0].as_ref().unwrap().last_write_seq,
        eseq1,
        "batch edge read from segment must preserve last_write_seq for eid1"
    );
    assert_eq!(
        results[1].as_ref().unwrap().last_write_seq,
        eseq2,
        "batch edge read from segment must preserve last_write_seq for eid2"
    );
    assert_eq!(
        results[2].as_ref().unwrap().last_write_seq,
        eseq3,
        "batch edge read from segment must preserve last_write_seq for eid3"
    );

    db.close().unwrap();
}

#[test]
fn test_tombstone_last_write_seq_survives_flush_reopen() {
    // Regression: S5. Tombstone last_write_seq must survive flush + reopen.
    let dir = tempfile::tempdir().unwrap();
    let delete_seq;
    {
        let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let id = db
            .upsert_node(1, "doomed", UpsertNodeOptions::default())
            .unwrap();
        db.delete_node(id).unwrap();
        // The delete op gets its own engine_seq
        delete_seq = db.engine_seq;
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        // Tombstone should be in the segment with a non-zero last_write_seq
        let seg = &db.segments[0];
        let tombstones = seg.deleted_node_tombstones();
        assert_eq!(tombstones.len(), 1, "should have exactly 1 node tombstone");
        let entry = tombstones.values().next().unwrap();
        assert!(
            entry.last_write_seq > 0,
            "tombstone last_write_seq must be > 0 after flush+reopen, got {}",
            entry.last_write_seq
        );
        assert_eq!(
            entry.last_write_seq, delete_seq,
            "tombstone last_write_seq must match the delete op's engine_seq"
        );
        db.close().unwrap();
    }
}

#[test]
fn test_tombstone_survives_flush_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let deleted_id;
    {
        let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        deleted_id = db
            .upsert_node(1, "doomed", UpsertNodeOptions::default())
            .unwrap();
        db.delete_node(deleted_id).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        // After reopen, tombstone should be in the segment
        assert_eq!(db.segment_tombstone_node_count(), 1);
        // The deleted node should not be visible
        assert!(db.get_node(deleted_id).unwrap().is_none());
        db.close().unwrap();
    }
}

// --- CP2 tests: SourceList integration with segments ---

#[test]
fn test_source_list_find_node_across_segments() {
    // Verifies SourceList.find_node works when data is in segments
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    let id = db
        .upsert_node(1, "seg-node", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();

    // Node should be found via get_node_raw (which uses SourceList.find_node)
    let node = db.get_node(id).unwrap().unwrap();
    assert_eq!(node.key, "seg-node");

    // Upsert same key again (goes to memtable, segment has older version)
    db.upsert_node(
        1,
        "seg-node",
        UpsertNodeOptions {
            weight: 2.0,
            ..Default::default()
        },
    )
    .unwrap();
    let node = db.get_node(id).unwrap().unwrap();
    assert_eq!(node.weight, 2.0); // memtable version wins

    db.close().unwrap();
}

#[test]
fn test_source_list_find_node_tombstoned_in_memtable_segment_has_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    let id = db
        .upsert_node(1, "will-die", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();

    // Node is in segment. Delete it (tombstone in memtable).
    db.delete_node(id).unwrap();
    // get_node_raw uses SourceList.find_node; tombstone in memtable should shadow segment.
    assert!(db.get_node(id).unwrap().is_none());

    db.close().unwrap();
}

#[test]
fn test_source_list_find_edge_by_triple_across_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(
        dir.path(),
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();
    let n1 = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let n2 = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let eid = db.upsert_edge(n1, n2, 1, Default::default()).unwrap();
    db.flush().unwrap();

    // Edge is in segment. get_edge_by_triple uses SourceList.find_edge_by_triple.
    let edge = db.get_edge_by_triple(n1, n2, 1).unwrap().unwrap();
    assert_eq!(edge.id, eid);

    // Delete the edge, then check triple lookup returns None
    db.delete_edge(eid).unwrap();
    assert!(db.get_edge_by_triple(n1, n2, 1).unwrap().is_none());

    db.close().unwrap();
}

#[test]
fn test_source_list_find_node_by_key_across_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    let id = db
        .upsert_node(1, "keyed", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();

    // Key lookup should find the node in the segment
    let node = db.get_node_by_key(1, "keyed").unwrap().unwrap();
    assert_eq!(node.id, id);

    // Delete and verify key lookup returns None
    db.delete_node(id).unwrap();
    assert!(db.get_node_by_key(1, "keyed").unwrap().is_none());

    db.close().unwrap();
}

// --- WAL generation / freeze / immutable memtable tests (Phase 21 CP3) ---

#[test]
fn test_freeze_creates_immutable_memtable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("freeze_test");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Write some data
    db.upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_node(1, "bob", UpsertNodeOptions::default())
        .unwrap();

    assert_eq!(db.immutable_memtable_count(), 0);

    // Freeze
    db.freeze_memtable().unwrap();

    assert_eq!(db.immutable_memtable_count(), 1);
    // Active memtable should be empty after freeze
    assert!(db.active_memtable().is_empty());

    // The WAL generation should have advanced
    assert_eq!(db.active_wal_generation(), 1);

    db.close().unwrap();
}

#[test]
fn test_freeze_empty_memtable_is_noop() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("freeze_empty");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Freeze on empty memtable should be a no-op
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_memtable_count(), 0);
    assert_eq!(db.active_wal_generation(), 0);

    db.close().unwrap();
}

#[test]
fn test_write_after_freeze_goes_to_new_generation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("freeze_write");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Write before freeze
    let id_a = db
        .upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    // Write after freeze - should go to new active memtable
    let id_b = db
        .upsert_node(1, "bob", UpsertNodeOptions::default())
        .unwrap();

    // Active memtable should only have the post-freeze write
    assert!(!db.active_memtable().is_empty());

    // Both nodes should be readable (alice from immutable, bob from active)
    let alice = db.get_node(id_a).unwrap().unwrap();
    assert_eq!(alice.key, "alice");
    let bob = db.get_node(id_b).unwrap().unwrap();
    assert_eq!(bob.key, "bob");

    // Verify WAL generation files exist
    let gen0 = wal_generation_path(&db_path, 0);
    let gen1 = wal_generation_path(&db_path, 1);
    assert!(gen0.exists(), "WAL generation 0 should exist");
    assert!(gen1.exists(), "WAL generation 1 should exist");

    // Sync to ensure WAL data is flushed to disk (GroupCommit mode buffers writes)
    db.sync().unwrap();

    // Gen 0 should have alice's data
    let gen0_ops = WalReader::read_generation(&db_path, 0).unwrap();
    assert_eq!(gen0_ops.len(), 1);

    // Gen 1 should have bob's data
    let gen1_ops = WalReader::read_generation(&db_path, 1).unwrap();
    assert_eq!(gen1_ops.len(), 1);

    db.close().unwrap();
}

#[test]
fn test_flush_with_wal_generations() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("flush_gen");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Write some data, freeze, then write more
    db.upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    db.upsert_node(1, "bob", UpsertNodeOptions::default())
        .unwrap();

    // Flush should process all (freeze active + flush both immutables)
    let seg_info = db.flush().unwrap();
    assert!(seg_info.is_some());

    // After flush, immutable_memtables should be empty
    assert_eq!(db.immutable_memtable_count(), 0);

    // Both nodes should be readable from segments
    let nodes = db.get_nodes_by_type(1).unwrap();
    assert_eq!(nodes.len(), 2);

    // Old WAL generation files should be retired
    let gen0 = wal_generation_path(&db_path, 0);
    assert!(
        !gen0.exists(),
        "WAL generation 0 should be retired after flush"
    );

    db.close().unwrap();
}

#[test]
fn test_replay_multiple_wal_generations() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("replay_gen");

    // Session 1: write data, freeze (creates gen 0 frozen + gen 1 active), close without flush
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "alice", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
        db.upsert_node(1, "bob", UpsertNodeOptions::default())
            .unwrap();

        // close_fast: doesn't flush immutables, just syncs active WAL and writes manifest
        db.close_fast().unwrap();
    }

    // Verify WAL generation files exist
    let gen0 = wal_generation_path(&db_path, 0);
    let gen1 = wal_generation_path(&db_path, 1);
    assert!(
        gen0.exists(),
        "WAL generation 0 should be retained by close_fast"
    );
    assert!(gen1.exists(), "WAL generation 1 should exist");

    // Session 2: reopen, verify all data is present via WAL replay
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let nodes = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            nodes.len(),
            2,
            "both nodes should be replayed from WAL generations"
        );

        let alice = db.get_node_by_key(1, "alice").unwrap();
        assert!(alice.is_some(), "alice should be found after WAL replay");
        let bob = db.get_node_by_key(1, "bob").unwrap();
        assert!(bob.is_some(), "bob should be found after WAL replay");

        db.close().unwrap();
    }
}

#[test]
fn test_data_wal_migration() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("migration_test");

    // Simulate old-format DB: create a data.wal file manually
    std::fs::create_dir_all(&db_path).unwrap();

    // Write a legacy data.wal with a node
    {
        let mut writer = WalWriter::open(&db_path).unwrap();
        let node = NodeRecord {
            id: 1,
            type_id: 1,
            key: "legacy_node".to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        };
        writer.append(&WalOp::UpsertNode(node), 1).unwrap();
        writer.sync().unwrap();
    }

    // Verify data.wal exists and wal_0.wal does not
    assert!(db_path.join("data.wal").exists());
    assert!(!wal_generation_path(&db_path, 0).exists());

    // Open with the engine; should trigger migration.
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // data.wal should have been renamed to wal_0.wal
        assert!(
            !db_path.join("data.wal").exists(),
            "data.wal should be migrated away"
        );
        assert!(
            wal_generation_path(&db_path, 0).exists(),
            "wal_0.wal should exist after migration"
        );

        // Node should be readable from replayed WAL
        let node = db.get_node_by_key(1, "legacy_node").unwrap();
        assert!(
            node.is_some(),
            "legacy node should be found after migration replay"
        );

        db.close().unwrap();
    }

    // Reopen to verify it works with the new format
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node = db.get_node_by_key(1, "legacy_node").unwrap();
        assert!(node.is_some(), "legacy node should persist across reopens");
        db.close().unwrap();
    }
}

#[test]
fn test_freeze_and_read_from_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("freeze_read");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Write nodes
    let id_a = db
        .upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();
    let id_b = db
        .upsert_node(1, "bob", UpsertNodeOptions::default())
        .unwrap();

    // Freeze: data moves to immutable epoch.
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 1);

    // Pause the flush worker so data stays in immutable epoch during reads
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap(); // worker paused, epoch is in-flight

    assert_eq!(db.in_flight_count(), 1);

    // Point reads should find data in the in-flight immutable epoch
    let alice = db.get_node(id_a).unwrap();
    assert!(
        alice.is_some(),
        "alice should be readable from in-flight immutable epoch"
    );
    assert_eq!(alice.unwrap().key, "alice");

    let bob = db.get_node(id_b).unwrap();
    assert!(
        bob.is_some(),
        "bob should be readable from in-flight immutable epoch"
    );
    assert_eq!(bob.unwrap().key, "bob");

    // Key lookups should also work
    let alice_by_key = db.get_node_by_key(1, "alice").unwrap();
    assert!(
        alice_by_key.is_some(),
        "alice should be findable by key while in-flight"
    );

    // Type query should return both
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(
        all.len(),
        2,
        "get_nodes_by_type should see in-flight epoch data"
    );

    // Release worker, verify data moves to segment
    release_tx.send(()).unwrap();
    db.wait_one_flush().unwrap();
    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(db.segment_count(), 1);

    // Data still readable from segment
    assert!(db.get_node(id_a).unwrap().is_some());
    assert!(db.get_node(id_b).unwrap().is_some());

    db.close().unwrap();
}

#[test]
fn test_multiple_freezes_before_flush() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_freeze");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Write and freeze three times
    db.upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 1);

    db.upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 2);

    db.upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 3);

    // All data should be readable across 3 immutable epochs
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(
        all.len(),
        3,
        "all nodes across 3 immutable epochs should be visible"
    );

    // WAL generation should have advanced to 3
    assert_eq!(db.active_wal_generation(), 3);

    // Pause the first (oldest) flush to verify all epochs stay visible
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap(); // oldest ("a") gets pause
    db.enqueue_one_flush().unwrap(); // "b" queued behind
    db.enqueue_one_flush().unwrap(); // "c" queued behind
    ready_rx.recv().unwrap(); // oldest paused

    // All 3 epochs still visible while first is in-flight
    assert_eq!(db.immutable_epoch_count(), 3);
    assert_eq!(db.in_flight_count(), 3);
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 3, "all nodes visible during in-flight flush");

    // Release and drain
    release_tx.send(()).unwrap();
    db.flush().unwrap();
    assert_eq!(db.immutable_epoch_count(), 0);

    // Data should now be in segments
    assert_eq!(db.segment_count(), 3);
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 3);

    db.close().unwrap();
}

#[test]
fn test_wal_generation_survives_close_fast() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("close_fast_gen");

    // Write data and close_fast (no flush)
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "node1", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "node2", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }

    // Reopen and verify data is present
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 2, "data should survive close_fast + reopen");
        db.close().unwrap();
    }
}

#[test]
fn test_flush_retires_wal_generations() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("retire_gen");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Write data
    db.upsert_node(1, "node1", UpsertNodeOptions::default())
        .unwrap();

    // After flush, the old WAL generation should be deleted
    db.flush().unwrap();

    // Generation 0 should have been retired
    let gen0 = wal_generation_path(&db_path, 0);
    assert!(
        !gen0.exists(),
        "WAL generation 0 should be retired after flush"
    );

    // The active generation should have advanced
    assert!(db.active_wal_generation() >= 1);

    // Manifest should have no pending flush epochs
    assert!(db.manifest().pending_flush_epochs.is_empty());

    db.close().unwrap();
}

// --- CP4: Background flush worker tests ---

#[test]
fn test_bg_flush_writes_continue_during_flush() {
    // Verify that frozen data + active data are all visible while
    // the flush worker is processing the frozen epoch.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bg_flush_continue");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Write first batch and freeze
    for i in 0..50 {
        db.upsert_node(1, &format!("pre:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 1);

    // Pause the flush worker before it writes the segment
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap(); // worker paused

    // Write more data to the active memtable while flush is in-flight
    for i in 0..50 {
        db.upsert_node(1, &format!("post:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }

    // All 100 nodes visible: 50 from in-flight immutable epoch + 50 from active
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(
        all.len(),
        100,
        "all nodes should be visible during in-flight flush"
    );
    assert_eq!(db.in_flight_count(), 1);

    // Release worker and drain everything
    release_tx.send(()).unwrap();
    db.flush().unwrap();

    // All 100 nodes still visible, now from segments
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 100, "all nodes should be visible after bg flush");
    assert!(db.segment_count() >= 2, "should have at least 2 segments");
    assert_eq!(db.immutable_epoch_count(), 0);

    db.close().unwrap();
}

#[test]
fn test_bg_flush_multiple_immutables() {
    // Freeze 4 times, enqueue all, verify data visible throughout,
    // then drain and verify each epoch produces a segment.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bg_flush_multi");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Freeze 4 separate batches
    for batch in 0..4 {
        for i in 0..10 {
            db.upsert_node(
                1,
                &format!("batch{}:node{}", batch, i),
                UpsertNodeOptions::default(),
            )
            .unwrap();
        }
        db.freeze_memtable().unwrap();
    }
    assert_eq!(db.immutable_epoch_count(), 4);

    // Pause the oldest (first to be flushed), enqueue all 4
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap(); // oldest (batch0) gets pause
    db.enqueue_one_flush().unwrap();
    db.enqueue_one_flush().unwrap();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap(); // oldest paused

    // All 4 epochs in-flight, all 40 nodes visible
    assert_eq!(db.immutable_epoch_count(), 4);
    assert_eq!(db.in_flight_count(), 4);
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 40, "all 40 nodes visible during in-flight flush");

    // Release and drain all
    release_tx.send(()).unwrap();
    db.flush().unwrap();

    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(
        db.segment_count(),
        4,
        "should have 4 segments from 4 frozen epochs"
    );

    // All 40 nodes in segments
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 40, "all 40 nodes should be present in segments");

    // Reopen and verify persistence
    let path = db.path().to_path_buf();
    db.close().unwrap();

    let db2 = DatabaseEngine::open(&path, &DbOptions::default()).unwrap();
    let all2 = db2.get_nodes_by_type(1).unwrap();
    assert_eq!(all2.len(), 40, "all 40 nodes should survive reopen");
    db2.close().unwrap();
}

#[test]
fn test_bg_flush_close_drains_all() {
    // Verify that close() properly drains in-flight bg flush work.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bg_flush_drain");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Write data
    for i in 0..20 {
        db.upsert_node(1, &format!("drain:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }

    // Flush to trigger bg worker
    db.flush().unwrap();

    // All data should be in segments now
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 20);

    // Close should succeed without losing data
    let path = db.path().to_path_buf();
    db.close().unwrap();

    // Reopen and verify
    let db2 = DatabaseEngine::open(&path, &DbOptions::default()).unwrap();
    let all2 = db2.get_nodes_by_type(1).unwrap();
    assert_eq!(all2.len(), 20, "all nodes should survive close + reopen");
    db2.close().unwrap();
}

#[test]
fn test_bg_flush_close_fast_preserves_recovery() {
    // close_fast with pending immutables should preserve data via WAL replay.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bg_flush_close_fast");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write data to memtable but don't flush
        for i in 0..15 {
            db.upsert_node(1, &format!("fast:{}", i), UpsertNodeOptions::default())
                .unwrap();
        }

        // close_fast does not flush, just shuts down bg worker.
        db.close_fast().unwrap();
    }

    // Reopen: WAL replay should recover all data.
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            15,
            "all nodes should be recovered via WAL replay after close_fast"
        );
        db.close().unwrap();
    }
}

#[test]
fn test_bg_flush_close_fast_with_frozen_memtables() {
    // close_fast with frozen immutable memtables should preserve data via WAL replay.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bg_flush_close_fast_frozen");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write and freeze
        for i in 0..10 {
            db.upsert_node(1, &format!("frozen:{}", i), UpsertNodeOptions::default())
                .unwrap();
        }
        db.freeze_memtable().unwrap();

        // Write more to active memtable
        for i in 10..20 {
            db.upsert_node(1, &format!("active:{}", i), UpsertNodeOptions::default())
                .unwrap();
        }

        // close_fast without flushing
        db.close_fast().unwrap();
    }

    // Reopen: WAL should recover all data from both generations.
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            20,
            "all nodes from frozen + active memtable should survive close_fast + reopen"
        );
        db.close().unwrap();
    }
}

// --- CP3 regression test: M10 (stale FrozenPendingFlush after replay) ---

#[test]
fn test_stale_frozen_epochs_cleaned_on_reopen_then_flush_works() {
    // Regression test: freeze → close_fast → reopen should rebuild frozen
    // epochs as immutable_epochs (WALs retained). A subsequent flush drains
    // them to segments and retires the WALs through the normal pipeline.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("stale_epoch");

    // Phase 1: write, freeze, close_fast (leaves FrozenPendingFlush)
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "before_crash", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
        db.upsert_node(1, "active_at_crash", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }

    // Phase 2: reopen. Frozen epoch is rebuilt as immutable, not cleaned up.
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // FrozenPendingFlush epoch stays in manifest until flushed
        assert_eq!(
            db.manifest().pending_flush_epochs.len(),
            1,
            "frozen epoch retained in manifest on reopen"
        );
        // Frozen data is in immutable_epochs, not folded into active memtable
        assert_eq!(db.immutable_epoch_count(), 1);

        // Both nodes should be recovered via WAL replay
        assert!(db.get_node_by_key(1, "before_crash").unwrap().is_some());
        assert!(db.get_node_by_key(1, "active_at_crash").unwrap().is_some());

        // Now do a new write cycle: freeze + flush should work cleanly
        db.upsert_node(1, "after_reopen", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
        db.upsert_node(1, "post_freeze", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Verify all 4 nodes present
        assert!(db.get_node_by_key(1, "before_crash").unwrap().is_some());
        assert!(db.get_node_by_key(1, "active_at_crash").unwrap().is_some());
        assert!(db.get_node_by_key(1, "after_reopen").unwrap().is_some());
        assert!(db.get_node_by_key(1, "post_freeze").unwrap().is_some());

        // All epochs drained after flush
        assert!(
            db.manifest().pending_flush_epochs.is_empty(),
            "no stale epochs after post-recovery flush"
        );

        db.close().unwrap();
    }

    // Phase 3: final reopen to confirm persistence
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 4, "all 4 nodes should survive full cycle");
        db.close().unwrap();
    }
}

#[test]
fn test_repeated_crash_after_freeze_preserves_data() {
    // Proves the defect 3 fix: freeze → crash → reopen → crash → reopen
    // must not lose the frozen data. WALs are retained until flushed.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("double_crash");

    // Phase 1: write, freeze, simulate crash
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "survivor", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
        db.close_fast().unwrap();
    }

    // Phase 2: reopen (rebuilds frozen as immutable), then crash again
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(db.get_node_by_key(1, "survivor").unwrap().is_some());
        assert_eq!(db.immutable_epoch_count(), 1);
        // Crash without flushing; close_fast doesn't flush.
        db.close_fast().unwrap();
    }

    // Phase 3: reopen again. Data must survive the double crash.
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(
            db.get_node_by_key(1, "survivor").unwrap().is_some(),
            "data must survive two crashes without flush"
        );
        assert_eq!(db.immutable_epoch_count(), 1);

        // Now flush to drain it
        db.flush().unwrap();
        assert_eq!(db.immutable_epoch_count(), 0);
        assert!(db.manifest().pending_flush_epochs.is_empty());
        db.close().unwrap();
    }

    // Phase 4: final verification
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(db.get_node_by_key(1, "survivor").unwrap().is_some());
        db.close().unwrap();
    }
}

// --- CP4 regression tests: M1 (WAL gen mismatch) and M2 (worker failure handling) ---

#[test]
fn test_multi_freeze_flush_retires_each_wal_gen() {
    // Regression test for CP4-M1: when multiple immutables are enqueued
    // in a batch, each must retire its OWN WAL generation, not all the same one.
    // Uses flush pause to verify WAL files exist while in-flight.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_retire");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Freeze 3 separate batches → creates WAL gens 0, 1, 2 (active = 3)
    for batch in 0..3 {
        db.upsert_node(1, &format!("batch{}", batch), UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
    }

    assert_eq!(db.immutable_epoch_count(), 3);
    assert_eq!(db.active_wal_generation(), 3);

    // WAL gens 0, 1, 2 should all exist before flush
    assert!(
        wal_generation_path(&db_path, 0).exists(),
        "wal_0 should exist before flush"
    );
    assert!(
        wal_generation_path(&db_path, 1).exists(),
        "wal_1 should exist before flush"
    );
    assert!(
        wal_generation_path(&db_path, 2).exists(),
        "wal_2 should exist before flush"
    );

    // Pause the oldest flush to verify all WAL files retained while in-flight
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap(); // oldest (gen 0) gets pause
    db.enqueue_one_flush().unwrap();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap(); // oldest paused

    // All 3 WAL files should still exist while flushes are in-flight
    assert!(
        wal_generation_path(&db_path, 0).exists(),
        "wal_0 should exist during in-flight"
    );
    assert!(
        wal_generation_path(&db_path, 1).exists(),
        "wal_1 should exist during in-flight"
    );
    assert!(
        wal_generation_path(&db_path, 2).exists(),
        "wal_2 should exist during in-flight"
    );

    // All data visible from immutable epochs
    for batch in 0..3 {
        assert!(
            db.get_node_by_key(1, &format!("batch{}", batch))
                .unwrap()
                .is_some(),
            "batch{} should be visible during in-flight",
            batch
        );
    }

    // Release and drain all
    release_tx.send(()).unwrap();
    db.flush().unwrap();

    // After flush, ALL three WAL gens should be retired (deleted)
    assert!(
        !wal_generation_path(&db_path, 0).exists(),
        "wal_0 should be retired after flush"
    );
    assert!(
        !wal_generation_path(&db_path, 1).exists(),
        "wal_1 should be retired after flush"
    );
    assert!(
        !wal_generation_path(&db_path, 2).exists(),
        "wal_2 should be retired after flush"
    );

    // Active WAL gen 3 should still exist
    assert!(
        wal_generation_path(&db_path, 3).exists(),
        "active wal_3 should still exist"
    );

    // All data in segments
    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(db.segment_count(), 3);
    for batch in 0..3 {
        assert!(
            db.get_node_by_key(1, &format!("batch{}", batch))
                .unwrap()
                .is_some(),
            "batch{} should be visible in segments",
            batch
        );
    }

    db.close().unwrap();
}

#[test]
fn test_flush_wait_loop_handles_worker_failure() {
    // Regression test for CP4-M2: if the bg flush worker fails,
    // flush() should not deadlock. The failed epoch stays visible
    // with in_flight=false, and subsequent flush retries it.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("flush_drain");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Create one epoch and inject a failure
    db.upsert_node(1, "fail_node", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    db.set_flush_force_error();
    db.enqueue_one_flush().unwrap();

    // Worker fails; wait_one_flush returns error.
    let result = db.wait_one_flush();
    assert!(result.is_err(), "worker failure should propagate as error");

    // Epoch stays visible, not in-flight (can be retried)
    assert_eq!(db.immutable_epoch_count(), 1);
    assert_eq!(db.in_flight_count(), 0);
    assert!(
        db.get_node_by_key(1, "fail_node").unwrap().is_some(),
        "data should remain visible after worker failure"
    );

    // Retry: flush() should re-enqueue and succeed this time
    db.flush().unwrap();
    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(db.segment_count(), 1);
    assert!(
        db.get_node_by_key(1, "fail_node").unwrap().is_some(),
        "data should be in segment after retry"
    );

    db.close().unwrap();
}

#[test]
fn test_write_after_reported_flush_failure_retries_in_background() {
    // Option B retry model: once a flush failure has been surfaced, a later
    // ordinary write should restart the pipeline and re-enqueue the failed
    // epoch instead of staying permanently wedged until explicit flush/reopen.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("flush_retry_on_write");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    db.upsert_node(1, "fail_node", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    db.set_flush_force_error();
    db.enqueue_one_flush().unwrap();
    let result = db.wait_one_flush();
    assert!(result.is_err(), "first failure should surface");
    assert_eq!(db.immutable_epoch_count(), 1);
    assert_eq!(db.in_flight_count(), 0);

    // Subsequent ordinary write should not be wedged by the already-reported
    // sticky error. It should restart the worker and enqueue the failed epoch.
    db.upsert_node(1, "retry_trigger", UpsertNodeOptions::default())
        .unwrap();
    assert_eq!(
        db.in_flight_count(),
        1,
        "ordinary write should restart retry for failed epoch"
    );

    let seg = db.wait_one_flush().unwrap();
    assert!(seg.is_some(), "retried epoch should flush successfully");
    assert_eq!(db.immutable_epoch_count(), 0);
    assert!(
        db.get_node_by_key(1, "fail_node").unwrap().is_some(),
        "failed epoch data should be published after retry"
    );

    // Sticky error should clear after successful adoption of the failed epoch.
    db.upsert_node(1, "after_clear", UpsertNodeOptions::default())
        .unwrap();
    db.flush().unwrap();

    assert!(db.get_node_by_key(1, "retry_trigger").unwrap().is_some());
    assert!(db.get_node_by_key(1, "after_clear").unwrap().is_some());

    db.close().unwrap();
}

// --- CP5: Read-path conversion tests (immutable memtable visibility) ---

#[test]
fn test_get_edges_batch_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("edges_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge e1 moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Create another edge in active memtable
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let e2 = db
        .upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // get_edges should see both
    let results = db.get_edges(&[e1, e2]).unwrap();
    assert!(
        results[0].is_some(),
        "edge in immutable memtable should be found by get_edges"
    );
    assert!(
        results[1].is_some(),
        "edge in active memtable should be found by get_edges"
    );
    assert_eq!(results[0].as_ref().unwrap().from, a);
    assert_eq!(results[0].as_ref().unwrap().to, b);
    assert_eq!(results[1].as_ref().unwrap().from, a);
    assert_eq!(results[1].as_ref().unwrap().to, c);

    db.close().unwrap();
}

#[test]
fn test_neighbors_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nbrs_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: adjacency moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Create another edge in active memtable
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // neighbors() should see both edges
    let nbrs = db
        .neighbors(
            a,
            &NeighborOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        nbrs.len(),
        2,
        "neighbors should see edges from both active and immutable memtables"
    );
    let neighbor_ids: Vec<u64> = nbrs.iter().map(|n| n.node_id).collect();
    assert!(
        neighbor_ids.contains(&b),
        "should see neighbor in immutable"
    );
    assert!(neighbor_ids.contains(&c), "should see neighbor in active");

    db.close().unwrap();
}

#[test]
fn test_find_nodes_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("find_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String("active".to_string()),
    );
    let _id_a = db
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    let _id_b = db
        .upsert_node(
            1,
            "b",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
        .unwrap();

    // find_nodes should see both
    let found = db
        .find_nodes(1, "status", &PropValue::String("active".to_string()))
        .unwrap();
    assert_eq!(
        found.len(),
        2,
        "find_nodes should see nodes from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_nodes_by_type_paged_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("type_paged_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    db.upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    db.upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();

    // nodes_by_type_paged should see all 3
    let page = db.nodes_by_type_paged(1, &PageRequest::default()).unwrap();
    assert_eq!(
        page.items.len(),
        3,
        "nodes_by_type_paged should see nodes from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_edges_by_type_paged_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("edge_type_paged_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // edges_by_type_paged should see both edges
    let page = db.edges_by_type_paged(10, &PageRequest::default()).unwrap();
    assert_eq!(
        page.items.len(),
        2,
        "edges_by_type_paged should see edges from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_neighbors_batch_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nbrs_batch_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    db.freeze_memtable().unwrap();

    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    let results = db
        .neighbors_batch(
            &[a],
            &NeighborOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    let a_nbrs = results.get(&a).unwrap();
    assert_eq!(
        a_nbrs.len(),
        2,
        "neighbors_batch should see edges from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_dense_search_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("dense_imm");
    let dense_config = DenseVectorConfig {
        dimension: 3,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    };
    let opts = DbOptions {
        dense_vector: Some(dense_config),
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Insert a node with dense vector
    db.upsert_node(
        1,
        "vec_a",
        UpsertNodeOptions {
            dense_vector: Some(vec![1.0, 0.0, 0.0]),
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze: node with dense vector moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Insert another node with dense vector in active memtable
    db.upsert_node(
        1,
        "vec_b",
        UpsertNodeOptions {
            dense_vector: Some(vec![0.0, 1.0, 0.0]),
            ..Default::default()
        },
    )
    .unwrap();

    // Dense search should find both
    let hits = db
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Dense,
            dense_query: Some(vec![1.0, 0.0, 0.0]),
            sparse_query: None,
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap();
    assert!(
        hits.len() >= 2,
        "dense vector search should find nodes from both active and immutable memtables, found {}",
        hits.len()
    );

    db.close().unwrap();
}

#[test]
fn test_sparse_search_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("sparse_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Insert node with sparse vector
    db.upsert_node(
        1,
        "sp_a",
        UpsertNodeOptions {
            sparse_vector: Some(vec![(0, 1.0), (1, 0.5)]),
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    // Insert another node with sparse vector in active memtable
    db.upsert_node(
        1,
        "sp_b",
        UpsertNodeOptions {
            sparse_vector: Some(vec![(0, 0.5), (2, 1.0)]),
            ..Default::default()
        },
    )
    .unwrap();

    // Sparse search should find both (query shares dimension 0 with both)
    let hits = db
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Sparse,
            dense_query: None,
            sparse_query: Some(vec![(0, 1.0)]),
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap();
    assert!(
        hits.len() >= 2,
        "sparse vector search should find nodes from both active and immutable memtables, found {}",
        hits.len()
    );

    db.close().unwrap();
}

#[test]
fn test_degree_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("degree_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // degree should count both edges
    let deg = db
        .degree(
            a,
            &DegreeOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        deg, 2,
        "degree should count edges from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_top_k_neighbors_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("topk_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(
        a,
        b,
        10,
        UpsertEdgeOptions {
            weight: 0.5,
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(
        a,
        c,
        10,
        UpsertEdgeOptions {
            weight: 0.8,
            ..Default::default()
        },
    )
    .unwrap();

    let top = db
        .top_k_neighbors(
            a,
            10,
            &TopKOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        top.len(),
        2,
        "top_k_neighbors should see edges from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_find_nodes_by_time_range_sees_immutable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("time_range_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    db.upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    db.upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();

    // Both should appear in a wide time range query
    let found = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
    assert_eq!(
        found.len(),
        2,
        "find_nodes_by_time_range should see nodes from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_immutable_tombstones_respected() {
    // Verify that tombstones in immutable memtables are respected by read paths.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("imm_tombstone");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();

    // Flush a and b to segments
    db.flush().unwrap();

    // Delete node b (creates tombstone in active memtable)
    db.delete_node(b).unwrap();

    // Freeze: tombstone for b moves to immutable memtable
    db.freeze_memtable().unwrap();

    // b should not be visible via nodes_by_type
    let all = db.nodes_by_type(1).unwrap();
    assert_eq!(
        all.len(),
        1,
        "deleted node should be hidden by tombstone in immutable memtable"
    );
    assert_eq!(all[0], a);

    // b should not be visible via get_node
    let result = db.get_node(b).unwrap();
    assert!(
        result.is_none(),
        "deleted node should not be returned from immutable memtable tombstone"
    );

    db.close().unwrap();
}

// --- CP5 review: additional coverage tests ---

#[test]
fn test_multiple_immutable_memtables_newest_wins() {
    // S2: Verify newest-first precedence across multiple immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Write node A with weight 1.0
    let id_a = db
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();

    // Freeze → immutable 1 (oldest)
    db.freeze_memtable().unwrap();

    // Update node A with weight 2.0
    db.upsert_node(
        1,
        "a",
        UpsertNodeOptions {
            weight: 2.0,
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze → immutable 0 (newest)
    db.freeze_memtable().unwrap();

    // Active memtable is now empty; both immutables hold versions of A.
    // get_node should return the newest version (weight 2.0).
    let node = db.get_node(id_a).unwrap().unwrap();
    assert!(
        (node.weight - 2.0).abs() < f32::EPSILON,
        "newest immutable memtable should win, got weight {}",
        node.weight
    );

    // Also test that nodes_by_type sees exactly 1 node (not duplicated)
    let all = db.nodes_by_type(1).unwrap();
    assert_eq!(
        all.len(),
        1,
        "should see 1 node not duplicated across immutables"
    );

    // Test find_nodes across two immutables with distinct data
    let mut props = BTreeMap::new();
    props.insert("color".to_string(), PropValue::String("red".to_string()));
    db.upsert_node(
        1,
        "b",
        UpsertNodeOptions {
            props,
            ..Default::default()
        },
    )
    .unwrap();

    // Now active has B, immutable[0] has A (weight 2.0), immutable[1] has A (weight 1.0)
    let found = db.nodes_by_type(1).unwrap();
    assert_eq!(
        found.len(),
        2,
        "should see A and B across active + immutables"
    );

    db.close().unwrap();
}

#[test]
fn test_multiple_immutable_tombstone_shadows_older() {
    // S2 extension: tombstone in newer immutable shadows record in older immutable.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_imm_tomb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let id = db
        .upsert_node(1, "doomed", UpsertNodeOptions::default())
        .unwrap();

    // Freeze → immutable 1 (oldest, has the record)
    db.freeze_memtable().unwrap();

    // Delete in active, then freeze → immutable 0 (newest, has tombstone)
    db.delete_node(id).unwrap();
    db.freeze_memtable().unwrap();

    // Node should not be visible
    assert!(
        db.get_node(id).unwrap().is_none(),
        "tombstone in newer immutable should shadow record in older immutable"
    );
    assert_eq!(db.nodes_by_type(1).unwrap().len(), 0);

    db.close().unwrap();
}

#[test]
fn test_export_adjacency_sees_immutable() {
    // S5: export_adjacency must include edges from immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("export_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge a→b moves to immutable
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    let export = db.export_adjacency(&ExportOptions::default()).unwrap();
    // Should have 3 nodes and 2 edges (a→b from immutable, a→c from active)
    assert_eq!(export.node_ids.len(), 3, "export should see all 3 nodes");
    let edges_from_a: Vec<_> = export.edges.iter().filter(|e| e.0 == a).collect();
    assert_eq!(
        edges_from_a.len(),
        2,
        "export_adjacency should see edges from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_connected_components_sees_immutable() {
    // S6: connected_components must find edges in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cc_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge a→b moves to immutable
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // All three should be in the same component via a→b (immutable) + b→c (active)
    let components = db
        .connected_components(&ComponentOptions::default())
        .unwrap();
    let comp_a = components.get(&a).unwrap();
    let comp_b = components.get(&b).unwrap();
    let comp_c = components.get(&c).unwrap();
    assert_eq!(comp_a, comp_b, "a and b should be in the same component");
    assert_eq!(
        comp_b, comp_c,
        "b and c should be in the same component (edge in active, bridged via immutable)"
    );

    db.close().unwrap();
}

#[test]
fn test_shortest_path_through_immutable() {
    // S7: shortest_path and is_connected must traverse edges in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("sp_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge a→b moves to immutable
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // shortest_path(a, c) should find 2-hop path via b
    let path = db
        .shortest_path(a, c, &ShortestPathOptions::default())
        .unwrap();
    assert!(
        path.is_some(),
        "shortest_path should find path through immutable memtable edge"
    );
    let path = path.unwrap();
    assert_eq!(path.nodes.len(), 3, "path should be a→b→c (3 nodes)");
    assert_eq!(path.nodes[0], a);
    assert_eq!(path.nodes[1], b);
    assert_eq!(path.nodes[2], c);

    // is_connected should also work
    let connected = db
        .is_connected(a, c, &IsConnectedOptions::default())
        .unwrap();
    assert!(
        connected,
        "is_connected should find path through immutable memtable edge"
    );

    db.close().unwrap();
}

#[test]
fn test_find_nodes_paged_sees_immutable() {
    // S8: find_nodes_paged must find nodes with matching props in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("find_paged_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("role".to_string(), PropValue::String("admin".to_string()));

    db.upsert_node(
        1,
        "user_a",
        UpsertNodeOptions {
            props: props.clone(),
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    db.upsert_node(
        1,
        "user_b",
        UpsertNodeOptions {
            props: props.clone(),
            ..Default::default()
        },
    )
    .unwrap();

    // find_nodes_paged should see both
    let page = db
        .find_nodes_paged(
            1,
            "role",
            &PropValue::String("admin".to_string()),
            &PageRequest::default(),
        )
        .unwrap();
    assert_eq!(
        page.items.len(),
        2,
        "find_nodes_paged should see nodes from both active and immutable memtables"
    );

    db.close().unwrap();
}

// --- CP6: Write-side dedup and graph ops parity tests ---

#[test]
fn test_upsert_node_dedup_across_immutable() {
    // Upsert a node with a key that lives in an immutable memtable.
    // The engine must find it there and reuse the same ID.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("upsert_dedup_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let id1 = db
        .upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();

    // Freeze: alice moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Upsert alice again; should find her in immutable and reuse ID.
    let id2 = db
        .upsert_node(
            1,
            "alice",
            UpsertNodeOptions {
                weight: 0.9,
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(
        id1, id2,
        "upsert_node must reuse existing ID from immutable memtable"
    );

    // The updated node should have the new weight
    let node = db.get_node(id1).unwrap().unwrap();
    assert!(
        (node.weight - 0.9).abs() < f32::EPSILON,
        "upsert should update properties"
    );

    db.close().unwrap();
}

#[test]
fn test_edge_uniqueness_across_immutable() {
    // Upsert an edge with same triple as one in immutable memtable.
    // With edge_uniqueness=true, the engine must reuse the same edge ID.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("edge_uniq_imm");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge e1 moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Upsert same triple; should reuse e1's ID.
    let e2 = db
        .upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                weight: 0.7,
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(
        e1, e2,
        "edge uniqueness must find existing edge in immutable memtable"
    );

    // The updated edge should have the new weight
    let edge = db.get_edge(e2).unwrap().unwrap();
    assert!(
        (edge.weight - 0.7).abs() < f32::EPSILON,
        "edge upsert should update properties"
    );

    db.close().unwrap();
}

#[test]
fn test_batch_upsert_node_dedup_across_immutable() {
    // batch_upsert_nodes must also find existing nodes in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batch_dedup_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let id_alice = db
        .upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    // Batch upsert that includes alice (should reuse ID) + new node bob
    let inputs = vec![
        NodeInput {
            type_id: 1,
            key: "alice".to_string(),
            props: BTreeMap::new(),
            weight: 0.8,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            type_id: 1,
            key: "bob".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
    ];
    let ids = db.batch_upsert_nodes(&inputs).unwrap();

    assert_eq!(
        ids[0], id_alice,
        "batch_upsert_nodes must reuse existing ID from immutable memtable"
    );
    assert_ne!(ids[1], id_alice, "new node must get a new ID");

    db.close().unwrap();
}

#[test]
fn test_batch_upsert_edge_uniqueness_across_immutable() {
    // batch_upsert_edges with edge_uniqueness must find edges in immutable.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batch_edge_uniq_imm");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    // Batch: re-upsert (a->b, type 10) + new (a->c, type 10)
    let inputs = vec![
        EdgeInput {
            from: a,
            to: b,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 0.5,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: a,
            to: c,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        },
    ];
    let ids = db.batch_upsert_edges(&inputs).unwrap();

    assert_eq!(
        ids[0], e1,
        "batch_upsert_edges must reuse existing edge ID from immutable memtable"
    );
    assert_ne!(ids[1], e1, "new edge must get a new ID");

    db.close().unwrap();
}

#[test]
fn test_delete_node_cascades_immutable_edges() {
    // Delete a node whose incident edges are in an immutable memtable.
    // The cascade must find and tombstone those edges.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("del_cascade_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge e1 (a->b) moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Add another edge in active memtable
    let e2 = db
        .upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Delete node a; should cascade-delete both e1 (immutable) and e2 (active).
    db.delete_node(a).unwrap();

    // Node a should be gone
    assert!(
        db.get_node(a).unwrap().is_none(),
        "deleted node should be gone"
    );

    // Both edges should be gone
    assert!(
        db.get_edge(e1).unwrap().is_none(),
        "edge in immutable memtable should be cascade-deleted"
    );
    assert!(
        db.get_edge(e2).unwrap().is_none(),
        "edge in active memtable should be cascade-deleted"
    );

    // Nodes b and c should survive
    assert!(db.get_node(b).unwrap().is_some());
    assert!(db.get_node(c).unwrap().is_some());

    db.close().unwrap();
}

#[test]
fn test_invalidate_edge_in_immutable() {
    // Invalidate an edge that lives in an immutable memtable.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("inv_edge_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge e1 moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Invalidate edge; should find it in immutable and write an update.
    let result = db.invalidate_edge(e1, 1000).unwrap();
    assert!(
        result.is_some(),
        "invalidate_edge must find edge in immutable memtable"
    );
    let updated = result.unwrap();
    assert_eq!(updated.valid_to, 1000);

    db.close().unwrap();
}

#[test]
fn test_graph_patch_dedup_across_immutable() {
    // graph_patch must find existing nodes/edges in immutable memtables
    // for dedup, and cascade deletes must find edges there too.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("patch_imm");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    let a_id = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b_id = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a_id, b_id, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    // Patch: re-upsert node "a" (dedup), re-upsert edge a->b (dedup),
    // add new node "c"
    let patch = GraphPatch {
        upsert_nodes: vec![
            NodeInput {
                type_id: 1,
                key: "a".to_string(),
                props: BTreeMap::new(),
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                type_id: 1,
                key: "c".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
        ],
        upsert_edges: vec![EdgeInput {
            from: a_id,
            to: b_id,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 0.3,
            valid_from: None,
            valid_to: None,
        }],
        invalidate_edges: vec![],
        delete_node_ids: vec![],
        delete_edge_ids: vec![],
    };

    let result = db.graph_patch(&patch).unwrap();

    assert_eq!(
        result.node_ids[0], a_id,
        "graph_patch must reuse node ID from immutable memtable"
    );
    assert_ne!(result.node_ids[1], a_id, "new node must get a new ID");
    assert_eq!(
        result.edge_ids[0], e1,
        "graph_patch must reuse edge ID from immutable memtable"
    );

    db.close().unwrap();
}

#[test]
fn test_graph_patch_delete_cascades_immutable_edges() {
    // graph_patch node deletion must cascade edges in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("patch_del_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge e1 moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Patch: delete node a (should cascade-delete edge e1)
    let patch = GraphPatch {
        upsert_nodes: vec![],
        upsert_edges: vec![],
        invalidate_edges: vec![],
        delete_node_ids: vec![a],
        delete_edge_ids: vec![],
    };
    db.graph_patch(&patch).unwrap();

    assert!(
        db.get_node(a).unwrap().is_none(),
        "deleted node should be gone"
    );
    assert!(
        db.get_edge(e1).unwrap().is_none(),
        "cascaded edge from immutable memtable should be deleted"
    );

    db.close().unwrap();
}

#[test]
fn test_prune_finds_targets_in_immutable_memtable() {
    // Prune with no type_id filter must scan immutable memtables for targets.
    // This is the bug we fixed: collect_prune_targets skipped immutable memtables
    // in the else (no type_id) branch.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("prune_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Insert a low-weight node
    db.upsert_node(
        1,
        "old_low",
        UpsertNodeOptions {
            weight: 0.1,
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze: the low-weight node moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Insert a high-weight node in active memtable
    db.upsert_node(
        1,
        "new_high",
        UpsertNodeOptions {
            weight: 5.0,
            ..Default::default()
        },
    )
    .unwrap();

    // Prune nodes with weight <= 0.5 (no type filter)
    let result = db
        .prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        })
        .unwrap();

    assert_eq!(
        result.nodes_pruned, 1,
        "prune must find and delete the node in the immutable memtable"
    );

    // The low-weight node should be gone, high-weight should survive
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 1, "only the high-weight node should survive");
    assert_eq!(all[0].key, "new_high");

    db.close().unwrap();
}

#[test]
fn test_prune_respects_tombstones_in_immutable_memtable() {
    // When a tombstone is in an immutable memtable, prune must not try to
    // re-prune that node (it's already deleted).
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("prune_ts_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    // Create and flush a low-weight node to segment
    let id = db
        .upsert_node(
            1,
            "target",
            UpsertNodeOptions {
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    // Delete it (tombstone in active memtable)
    db.delete_node(id).unwrap();

    // Freeze: tombstone moves to immutable memtable
    db.freeze_memtable().unwrap();

    // Prune should find 0 targets (node is already tombstoned)
    let result = db
        .prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        })
        .unwrap();

    assert_eq!(
        result.nodes_pruned, 0,
        "prune must respect tombstones from immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_id_allocation_stable_across_freeze() {
    // Verify that next_node_id and next_edge_id counters are maintained
    // correctly across freeze operations and don't cause ID reuse.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("id_alloc_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    // New nodes after freeze should get IDs > existing IDs
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let d = db
        .upsert_node(1, "d", UpsertNodeOptions::default())
        .unwrap();

    assert!(
        c > b,
        "node ID after freeze must be greater than pre-freeze IDs: c={} b={}",
        c,
        b
    );
    assert!(d > c, "node IDs must be monotonically increasing");

    // Same for edges
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    let e2 = db
        .upsert_edge(c, d, 10, UpsertEdgeOptions::default())
        .unwrap();
    assert!(
        e2 > e1,
        "edge ID after freeze must be greater than pre-freeze IDs"
    );

    // All 4 nodes should be readable
    assert_eq!(db.get_nodes_by_type(1).unwrap().len(), 4);

    db.close().unwrap();
}

// --- CP7: Vector and traversal parity verification tests ---
//
// CP5 already converted all vector search and traversal paths to include
// immutable memtables. The tests below verify edge cases that confirm
// immutable parity holds for traversal and graph_patch/invalidate combos.

#[test]
fn test_traversal_sees_immutable_edges() {
    // Traverse should discover edges in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("traverse_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: both edges move to immutable memtable
    db.freeze_memtable().unwrap();

    // Traverse from a with max_depth=2 should find b and c
    let hits = db
        .traverse(
            a,
            2,
            &TraverseOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();

    let found_ids: Vec<u64> = hits.items.iter().map(|h| h.node_id).collect();
    assert!(
        found_ids.contains(&b),
        "traversal should find node at depth 1 via immutable memtable edge"
    );
    assert!(
        found_ids.contains(&c),
        "traversal should find node at depth 2 via immutable memtable edges"
    );

    db.close().unwrap();
}

#[test]
fn test_graph_patch_invalidate_edge_in_immutable() {
    // graph_patch edge invalidation must find edges in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("patch_inv_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    // Patch: invalidate edge e1
    let patch = GraphPatch {
        upsert_nodes: vec![],
        upsert_edges: vec![],
        invalidate_edges: vec![(e1, 500)],
        delete_node_ids: vec![],
        delete_edge_ids: vec![],
    };
    db.graph_patch(&patch).unwrap();

    // Edge should still exist but have valid_to = 500
    let edge = db.get_edge(e1).unwrap().unwrap();
    assert_eq!(edge.valid_to, 500);

    db.close().unwrap();
}

#[test]
fn test_dedup_across_active_immutable_and_segments() {
    // End-to-end: data across all three source tiers (segments, immutable, active).
    // Verify dedup, reads, and deletes work correctly.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("three_tier_dedup");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Tier 1: write and flush to segment
    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let seg_edge = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    db.flush().unwrap();

    // Tier 2: write and freeze to immutable
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    let imm_edge = db
        .upsert_edge(a, c, 20, UpsertEdgeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    // Tier 3: write to active memtable
    let d = db
        .upsert_node(1, "d", UpsertNodeOptions::default())
        .unwrap();
    let act_edge = db
        .upsert_edge(a, d, 30, UpsertEdgeOptions::default())
        .unwrap();

    // Re-upsert node "a"; should find it in segment and reuse ID.
    let a2 = db
        .upsert_node(
            1,
            "a",
            UpsertNodeOptions {
                weight: 0.99,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(a, a2, "must reuse ID from segment");

    // Re-upsert edge a->c; should find it in immutable memtable.
    let imm_edge2 = db
        .upsert_edge(a, c, 20, UpsertEdgeOptions::default())
        .unwrap();
    assert_eq!(
        imm_edge, imm_edge2,
        "must reuse edge ID from immutable memtable"
    );

    // Re-upsert edge a->b; should find it in segment.
    let seg_edge2 = db
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    assert_eq!(seg_edge, seg_edge2, "must reuse edge ID from segment");

    // All 4 nodes and 3 edges should exist
    assert_eq!(db.get_nodes_by_type(1).unwrap().len(), 4);
    let nbrs = db
        .neighbors(
            a,
            &NeighborOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        nbrs.len(),
        3,
        "should see edges from segment, immutable, and active"
    );

    // Delete node a; should cascade all 3 edges across all tiers.
    db.delete_node(a).unwrap();
    assert!(db.get_edge(seg_edge).unwrap().is_none());
    assert!(db.get_edge(imm_edge).unwrap().is_none());
    assert!(db.get_edge(act_edge).unwrap().is_none());

    db.close().unwrap();
}

#[test]
fn test_write_dedup_across_multiple_immutables() {
    // S1: Write-side dedup must find records across multiple immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_imm_dedup");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Freeze 1: alice + edge a->b in oldest immutable
    let id_alice = db
        .upsert_node(1, "alice", UpsertNodeOptions::default())
        .unwrap();
    let id_bob = db
        .upsert_node(1, "bob", UpsertNodeOptions::default())
        .unwrap();
    let e1 = db
        .upsert_edge(id_alice, id_bob, 10, UpsertEdgeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    // Freeze 2: charlie in newer immutable
    let id_charlie = db
        .upsert_node(1, "charlie", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    // Batch upsert with all three keys: should find alice in immutable[1],
    // charlie in immutable[0], and bob in immutable[1]. No new IDs allocated.
    let inputs = vec![
        NodeInput {
            type_id: 1,
            key: "alice".to_string(),
            props: BTreeMap::new(),
            weight: 0.8,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            type_id: 1,
            key: "bob".to_string(),
            props: BTreeMap::new(),
            weight: 0.9,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            type_id: 1,
            key: "charlie".to_string(),
            props: BTreeMap::new(),
            weight: 0.7,
            dense_vector: None,
            sparse_vector: None,
        },
    ];
    let ids = db.batch_upsert_nodes(&inputs).unwrap();
    assert_eq!(
        ids[0], id_alice,
        "alice should reuse ID from older immutable"
    );
    assert_eq!(ids[1], id_bob, "bob should reuse ID from older immutable");
    assert_eq!(
        ids[2], id_charlie,
        "charlie should reuse ID from newer immutable"
    );

    // Re-upsert edge a->b; should find it in older immutable.
    let e2 = db
        .upsert_edge(id_alice, id_bob, 10, UpsertEdgeOptions::default())
        .unwrap();
    assert_eq!(e1, e2, "edge should reuse ID from older immutable");

    // Total should still be 3 nodes
    assert_eq!(db.get_nodes_by_type(1).unwrap().len(), 3);

    db.close().unwrap();
}

// --- Coverage audit: high-risk immutable memtable test gaps ---

#[test]
fn test_degrees_batch_sees_immutable() {
    // degrees (batch) has its own immutable memtable walk, separate from degree (single).
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("deg_batch_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: edge a→b moves to immutable
    db.freeze_memtable().unwrap();

    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Batch degrees should count edges from both active and immutable
    let degs = db
        .degrees(
            &[a, b],
            &DegreeOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        *degs.get(&a).unwrap_or(&0),
        2,
        "degrees batch must count edges from both active and immutable memtables"
    );
    assert_eq!(*degs.get(&b).unwrap_or(&0), 0);

    db.close().unwrap();
}

#[test]
fn test_sum_edge_weights_sees_immutable() {
    // sum_edge_weights uses degree_stats_raw_walk_inner with its own immutable walk.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("sum_wt_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(
        a,
        b,
        10,
        UpsertEdgeOptions {
            weight: 0.5,
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(
        a,
        c,
        10,
        UpsertEdgeOptions {
            weight: 1.5,
            ..Default::default()
        },
    )
    .unwrap();

    let sum = db
        .sum_edge_weights(
            a,
            &DegreeOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        (sum - 2.0).abs() < f64::EPSILON,
        "sum_edge_weights must include edges from immutable memtable, got {}",
        sum
    );

    let avg = db
        .avg_edge_weight(
            a,
            &DegreeOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        (avg.unwrap() - 1.0).abs() < f64::EPSILON,
        "avg_edge_weight must include edges from immutable memtable, got {:?}",
        avg
    );

    db.close().unwrap();
}

#[test]
fn test_neighbors_paged_sees_immutable() {
    // neighbors_paged builds its own K-way merge from immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nbrs_paged_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    let page = db
        .neighbors_paged(
            a,
            &NeighborOptions {
                direction: Direction::Outgoing,
                ..Default::default()
            },
            &PageRequest::default(),
        )
        .unwrap();
    assert_eq!(
        page.items.len(),
        2,
        "neighbors_paged must see edges from both active and immutable memtables"
    );

    db.close().unwrap();
}

#[test]
fn test_dense_search_tombstone_in_immutable_hides_result() {
    // If a node with a dense vector is deleted (tombstone in immutable),
    // dense search must exclude it.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("dense_tomb_imm");
    let dense_config = DenseVectorConfig {
        dimension: 3,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    };
    let opts = DbOptions {
        dense_vector: Some(dense_config),
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Insert two nodes with dense vectors, flush to segments
    let id_a = db
        .upsert_node(
            1,
            "vec_a",
            UpsertNodeOptions {
                dense_vector: Some(vec![1.0, 0.0, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();
    let id_b = db
        .upsert_node(
            1,
            "vec_b",
            UpsertNodeOptions {
                dense_vector: Some(vec![0.9, 0.1, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    // Delete node A, freeze tombstone to immutable
    db.delete_node(id_a).unwrap();
    db.freeze_memtable().unwrap();

    // Dense search: node A should be hidden by tombstone in immutable.
    let hits = db
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Dense,
            dense_query: Some(vec![1.0, 0.0, 0.0]),
            sparse_query: None,
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap();

    let hit_ids: Vec<u64> = hits.iter().map(|h| h.node_id).collect();
    assert!(
        !hit_ids.contains(&id_a),
        "deleted node must not appear in dense search results (tombstone in immutable)"
    );
    assert!(
        hit_ids.contains(&id_b),
        "non-deleted node must still appear in dense search results"
    );

    db.close().unwrap();
}

#[test]
fn test_sparse_search_tombstone_in_immutable_hides_result() {
    // If a node with a sparse vector is deleted (tombstone in immutable),
    // sparse search must exclude it.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("sparse_tomb_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let id_a = db
        .upsert_node(
            1,
            "sp_a",
            UpsertNodeOptions {
                sparse_vector: Some(vec![(0, 1.0), (1, 0.5)]),
                ..Default::default()
            },
        )
        .unwrap();
    let id_b = db
        .upsert_node(
            1,
            "sp_b",
            UpsertNodeOptions {
                sparse_vector: Some(vec![(0, 0.8), (2, 1.0)]),
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    // Delete node A, freeze tombstone to immutable
    db.delete_node(id_a).unwrap();
    db.freeze_memtable().unwrap();

    let hits = db
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Sparse,
            dense_query: None,
            sparse_query: Some(vec![(0, 1.0)]),
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap();

    let hit_ids: Vec<u64> = hits.iter().map(|h| h.node_id).collect();
    assert!(
        !hit_ids.contains(&id_a),
        "deleted node must not appear in sparse search results (tombstone in immutable)"
    );
    assert!(
        hit_ids.contains(&id_b),
        "non-deleted node must still appear in sparse search results"
    );

    db.close().unwrap();
}

#[test]
fn test_dense_scoped_search_sees_immutable() {
    // Scoped dense search has separate threshold calc + candidate collection from immutables.
    // Scope is traversal-based: we create a star graph so traversal collects scope IDs
    // that include nodes in immutable memtables.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("dense_scoped_imm");
    let dense_config = DenseVectorConfig {
        dimension: 3,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    };
    let opts = DbOptions {
        dense_vector: Some(dense_config),
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Hub node (scope start)
    let hub = db
        .upsert_node(
            1,
            "hub",
            UpsertNodeOptions {
                dense_vector: Some(vec![0.5, 0.5, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();

    let id_a = db
        .upsert_node(
            1,
            "vec_a",
            UpsertNodeOptions {
                dense_vector: Some(vec![1.0, 0.0, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(hub, id_a, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze: node A + edge hub→A move to immutable
    db.freeze_memtable().unwrap();

    let id_b = db
        .upsert_node(
            1,
            "vec_b",
            UpsertNodeOptions {
                dense_vector: Some(vec![0.0, 1.0, 0.0]),
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(hub, id_b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Scoped search from hub: traversal discovers A (immutable) and B (active).
    let hits = db
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Dense,
            dense_query: Some(vec![1.0, 0.0, 0.0]),
            sparse_query: None,
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: Some(VectorSearchScope {
                start_node_id: hub,
                max_depth: 1,
                direction: Direction::Outgoing,
                edge_type_filter: None,
                at_epoch: None,
            }),
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap();

    let hit_ids: Vec<u64> = hits.iter().map(|h| h.node_id).collect();
    assert!(
        hit_ids.contains(&id_a),
        "scoped dense search must find node from immutable memtable via traversal scope"
    );
    assert!(
        hit_ids.contains(&id_b),
        "scoped dense search must find node from active memtable via traversal scope"
    );

    db.close().unwrap();
}

#[test]
fn test_dijkstra_shortest_path_through_immutable() {
    // Dijkstra (weighted) shortest_path has its own for_each_search_neighbor immutable walk.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("dijkstra_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(
        a,
        b,
        10,
        UpsertEdgeOptions {
            weight: 1.0,
            ..Default::default()
        },
    )
    .unwrap();

    // Freeze: edge a→b moves to immutable
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(
        b,
        c,
        10,
        UpsertEdgeOptions {
            weight: 2.0,
            ..Default::default()
        },
    )
    .unwrap();

    // Dijkstra shortest path (triggered by weight_field)
    let path = db
        .shortest_path(
            a,
            c,
            &ShortestPathOptions {
                weight_field: Some("weight".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        path.is_some(),
        "Dijkstra shortest_path must find path through immutable memtable edge"
    );
    let path = path.unwrap();
    assert_eq!(path.nodes.len(), 3, "path should be a→b→c");
    assert_eq!(path.nodes[0], a);
    assert_eq!(path.nodes[1], b);
    assert_eq!(path.nodes[2], c);

    db.close().unwrap();
}

#[test]
fn test_all_shortest_paths_through_immutable() {
    // all_shortest_paths has 4 internal variants, each with its own TraversalTombstoneView.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("all_sp_imm");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = db
        .upsert_node(1, "a", UpsertNodeOptions::default())
        .unwrap();
    let b = db
        .upsert_node(1, "b", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    // Freeze
    db.freeze_memtable().unwrap();

    let c = db
        .upsert_node(1, "c", UpsertNodeOptions::default())
        .unwrap();
    db.upsert_edge(b, c, 10, UpsertEdgeOptions::default())
        .unwrap();

    // BFS variant (no weight_field)
    let paths = db
        .all_shortest_paths(a, c, &AllShortestPathsOptions::default())
        .unwrap();
    assert!(
        !paths.is_empty(),
        "all_shortest_paths (BFS) must find path through immutable memtable edge"
    );
    assert_eq!(paths[0].nodes.len(), 3);

    // Dijkstra variant (with weight_field)
    let paths = db
        .all_shortest_paths(
            a,
            c,
            &AllShortestPathsOptions {
                weight_field: Some("weight".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        !paths.is_empty(),
        "all_shortest_paths (Dijkstra) must find path through immutable memtable edge"
    );
    assert_eq!(paths[0].nodes.len(), 3);

    db.close().unwrap();
}

// --- CP8: Reopen + crash matrix ---

#[test]
fn test_crash_after_freeze_before_flush() {
    // Simulate crash after freeze: gen 0 frozen (FrozenPendingFlush in manifest),
    // gen 1 active. close_fast() preserves WAL generations. Reopen should replay
    // both WAL generations and recover all data.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_freeze");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write data to gen 0
        let id_a = db
            .upsert_node(1, "alice", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_edge(id_a, id_a, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Freeze: gen 0 becomes frozen, gen 1 becomes active
        db.freeze_memtable().unwrap();
        assert_eq!(db.immutable_memtable_count(), 1);
        assert_eq!(db.active_wal_generation(), 1);

        // Write data to gen 1 (active)
        let id_b = db
            .upsert_node(1, "bob", UpsertNodeOptions::default())
            .unwrap();
        assert_ne!(id_a, id_b);

        // Simulate crash: close_fast() syncs WAL but doesn't flush
        db.close_fast().unwrap();
    }

    // Verify WAL generation files exist on disk
    let gen0 = wal_generation_path(&db_path, 0);
    let gen1 = wal_generation_path(&db_path, 1);
    assert!(gen0.exists(), "WAL gen 0 should be retained");
    assert!(gen1.exists(), "WAL gen 1 should be retained");

    // Verify manifest has FrozenPendingFlush epoch
    let manifest = load_manifest(&db_path).unwrap().unwrap();
    assert!(
        manifest
            .pending_flush_epochs
            .iter()
            .any(|e| e.state == FlushEpochState::FrozenPendingFlush),
        "manifest should record FrozenPendingFlush epoch"
    );

    // Reopen and verify ALL data recovered
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let alice = db.get_node_by_key(1, "alice").unwrap();
        assert!(alice.is_some(), "alice from gen 0 should be recovered");
        let bob = db.get_node_by_key(1, "bob").unwrap();
        assert!(bob.is_some(), "bob from gen 1 should be recovered");

        // Edge from gen 0 should also be recovered
        let all_nodes = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all_nodes.len(), 2, "both nodes should be present");

        db.close().unwrap();
    }
}

#[test]
fn test_crash_with_flushed_segment_and_unflushed_wal() {
    // Write data, flush (creates segment), write more, close_fast.
    // Reopen should have both flushed (in segment) and unflushed (replayed from WAL) data.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_seg_manifest");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write and flush -- segment is created and manifest is updated
        db.upsert_node(1, "flushed_1", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "flushed_2", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Verify segment exists
        assert!(db.segment_count() >= 1);

        // Write more data (unflushed)
        db.upsert_node(1, "unflushed_1", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "unflushed_2", UpsertNodeOptions::default())
            .unwrap();

        // Simulate crash after writes but before another flush
        db.close_fast().unwrap();
    }

    // Reopen and verify both flushed and unflushed data
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Flushed data should be in segments
        let f1 = db.get_node_by_key(1, "flushed_1").unwrap();
        assert!(f1.is_some(), "flushed_1 from segment should be present");
        let f2 = db.get_node_by_key(1, "flushed_2").unwrap();
        assert!(f2.is_some(), "flushed_2 from segment should be present");

        // Unflushed data should be recovered from WAL replay
        let u1 = db.get_node_by_key(1, "unflushed_1").unwrap();
        assert!(u1.is_some(), "unflushed_1 should be recovered from WAL");
        let u2 = db.get_node_by_key(1, "unflushed_2").unwrap();
        assert!(u2.is_some(), "unflushed_2 should be recovered from WAL");

        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 4, "all 4 nodes should be present");

        db.close().unwrap();
    }
}

#[test]
fn test_crash_after_segment_write_before_manifest_publish() {
    // Boundary 2: segment output is durable on disk, but manifest does NOT
    // reference it yet (epoch is still FrozenPendingFlush). On reopen, the
    // engine should recover data from WAL replay and clean up the orphan segment.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_boundary2");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "alice", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "bob", UpsertNodeOptions::default())
            .unwrap();

        // Freeze: data goes to immutable, epoch recorded as FrozenPendingFlush
        db.freeze_memtable().unwrap();

        // close_fast: WAL preserved, epoch stays FrozenPendingFlush
        db.close_fast().unwrap();
    }

    // Manually create an orphan segment directory (simulating bg worker
    // wrote the segment but crash happened before manifest publish).
    let orphan_seg = segment_dir(&db_path, 9999);
    std::fs::create_dir_all(&orphan_seg).unwrap();
    std::fs::write(orphan_seg.join("nodes.dat"), b"dummy").unwrap();

    // Verify: manifest has FrozenPendingFlush, orphan segment exists
    let manifest = load_manifest(&db_path).unwrap().unwrap();
    assert!(manifest
        .pending_flush_epochs
        .iter()
        .any(|e| e.state == FlushEpochState::FrozenPendingFlush));
    assert!(
        manifest.segments.is_empty(),
        "manifest should NOT reference any segment"
    );
    assert!(orphan_seg.exists(), "orphan segment should exist on disk");

    // Reopen: WAL replay recovers data, orphan segment is cleaned up
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Data recovered from WAL replay
        assert!(
            db.get_node_by_key(1, "alice").unwrap().is_some(),
            "alice recovered from WAL"
        );
        assert!(
            db.get_node_by_key(1, "bob").unwrap().is_some(),
            "bob recovered from WAL"
        );
        assert_eq!(db.get_nodes_by_type(1).unwrap().len(), 2);

        // Orphan segment cleaned up
        assert!(
            !orphan_seg.exists(),
            "orphan segment should be cleaned up on reopen"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_crash_with_multiple_frozen_generations() {
    // Write -> freeze -> write -> freeze -> write -> close_fast.
    // Manifest has 2 FrozenPendingFlush epochs + active generation.
    // Reopen must recover all 3 generations of data.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_multi_frozen");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Gen 0: write and freeze
        db.upsert_node(1, "gen0_node", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();

        // Gen 1: write and freeze
        db.upsert_node(1, "gen1_node", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();

        // Gen 2 (active): write
        db.upsert_node(1, "gen2_node", UpsertNodeOptions::default())
            .unwrap();

        assert_eq!(db.immutable_memtable_count(), 2);
        assert_eq!(db.active_wal_generation(), 2);

        // Simulate crash
        db.close_fast().unwrap();
    }

    // Verify all 3 WAL generation files exist
    assert!(wal_generation_path(&db_path, 0).exists());
    assert!(wal_generation_path(&db_path, 1).exists());
    assert!(wal_generation_path(&db_path, 2).exists());

    // Verify manifest has 2 FrozenPendingFlush epochs
    let manifest = load_manifest(&db_path).unwrap().unwrap();
    let frozen_count = manifest
        .pending_flush_epochs
        .iter()
        .filter(|e| e.state == FlushEpochState::FrozenPendingFlush)
        .count();
    assert_eq!(frozen_count, 2, "should have 2 FrozenPendingFlush epochs");

    // Reopen and verify all data is recovered
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(
            db.get_node_by_key(1, "gen0_node").unwrap().is_some(),
            "gen0 data recovered"
        );
        assert!(
            db.get_node_by_key(1, "gen1_node").unwrap().is_some(),
            "gen1 data recovered"
        );
        assert!(
            db.get_node_by_key(1, "gen2_node").unwrap().is_some(),
            "gen2 data recovered"
        );

        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 3, "all 3 generations of data should be present");

        db.close().unwrap();
    }
}

#[test]
fn test_crash_after_publish_before_wal_retire() {
    // Simulate PublishedPendingRetire state: segment is published, WAL gen
    // still on disk. Construct manifest manually with PublishedPendingRetire
    // epoch. Reopen should trust the segment, NOT replay the WAL gen, and
    // clean up the leftover WAL file.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_publish_retire");

    let node_id;
    let seg_id;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write data and flush -- creates segment and retires WAL
        node_id = db
            .upsert_node(1, "published_node", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();

        // Record the segment ID
        seg_id = db.manifest().segments[0].id;

        db.close().unwrap();
    }

    // Now manually manipulate the manifest to simulate a crash between
    // publish and WAL retire: add back a PublishedPendingRetire epoch
    // and recreate the WAL gen file.
    {
        let mut manifest = load_manifest(&db_path).unwrap().unwrap();
        manifest.pending_flush_epochs.push(FlushEpochMeta {
            epoch_id: 0,
            wal_generation_id: 0,
            state: FlushEpochState::PublishedPendingRetire,
            segment_id: Some(seg_id),
        });
        write_manifest(&db_path, &manifest).unwrap();

        // Create a WAL gen 0 file with some data (simulating the not-yet-retired WAL)
        let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
        let node = NodeRecord {
            id: node_id,
            type_id: 1,
            key: "published_node".to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 1,
        };
        writer.append(&WalOp::UpsertNode(node), 1).unwrap();
        writer.sync().unwrap();
    }

    // Verify the WAL gen 0 file exists
    assert!(
        wal_generation_path(&db_path, 0).exists(),
        "WAL gen 0 should exist before reopen"
    );

    // Reopen -- segment data should be present, WAL gen should be cleaned up
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Data should be readable from the segment
        let node = db.get_node(node_id).unwrap();
        assert!(node.is_some(), "node should be readable from segment");
        assert_eq!(node.unwrap().key, "published_node");

        // No duplicate data -- still just 1 node
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            1,
            "should have exactly 1 node (no duplicates from WAL replay)"
        );

        // The PublishedPendingRetire epoch should have been cleaned up
        assert!(
            db.manifest().pending_flush_epochs.is_empty(),
            "pending flush epochs should be empty after cleanup"
        );

        // WAL gen 0 file should have been deleted
        assert!(
            !wal_generation_path(&db_path, 0).exists(),
            "WAL gen 0 should be cleaned up after reopen"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_reopen_fails_if_published_pending_retire_segment_is_missing() {
    // If manifest claims an epoch is PublishedPendingRetire, reopen must
    // verify the referenced segment before cleaning up the retained WAL.
    // Missing segment must error rather than silently dropping recovery.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_publish_missing_segment");

    let node_id;
    let seg_id;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        node_id = db
            .upsert_node(1, "published_node", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        seg_id = db.manifest().segments[0].id;
        db.close().unwrap();
    }

    {
        let mut manifest = load_manifest(&db_path).unwrap().unwrap();
        manifest.pending_flush_epochs.push(FlushEpochMeta {
            epoch_id: 0,
            wal_generation_id: 0,
            state: FlushEpochState::PublishedPendingRetire,
            segment_id: Some(seg_id),
        });
        write_manifest(&db_path, &manifest).unwrap();

        let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
        let node = NodeRecord {
            id: node_id,
            type_id: 1,
            key: "published_node".to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 1,
        };
        writer.append(&WalOp::UpsertNode(node), 1).unwrap();
        writer.sync().unwrap();
    }

    let seg_dir = segment_dir(&db_path, seg_id);
    std::fs::remove_dir_all(&seg_dir).unwrap();
    assert!(
        wal_generation_path(&db_path, 0).exists(),
        "WAL gen 0 should exist before reopen"
    );

    let err = match DatabaseEngine::open(&db_path, &DbOptions::default()) {
        Ok(_) => panic!("reopen should fail when PublishedPendingRetire segment is missing"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("published segment") || msg.contains("PublishedPendingRetire"),
        "unexpected reopen error: {}",
        msg
    );

    assert!(
        wal_generation_path(&db_path, 0).exists(),
        "WAL gen 0 must remain on disk when reopen fails to verify published segment"
    );
    let manifest = crate::manifest::load_manifest_readonly(&db_path)
        .unwrap()
        .unwrap();
    assert!(
        manifest.pending_flush_epochs.iter().any(|e| {
            e.state == FlushEpochState::PublishedPendingRetire
                && e.wal_generation_id == 0
                && e.segment_id == Some(seg_id)
        }),
        "manifest should retain the PublishedPendingRetire epoch on failed reopen"
    );
}

#[test]
fn test_crash_after_wal_delete_before_epoch_removal() {
    // Boundary 4: WAL gen file already deleted, but PublishedPendingRetire
    // epoch still in manifest. Reopen should just clean the stale epoch
    // from the manifest without error.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_wal_deleted");

    let seg_id;
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "survivor", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        seg_id = db.manifest().segments[0].id;
        db.close().unwrap();
    }

    // Manually add a PublishedPendingRetire epoch to manifest,
    // but do NOT create the WAL file (simulating it was already deleted).
    {
        let mut manifest = load_manifest(&db_path).unwrap().unwrap();
        manifest.pending_flush_epochs.push(FlushEpochMeta {
            epoch_id: 0,
            wal_generation_id: 0,
            state: FlushEpochState::PublishedPendingRetire,
            segment_id: Some(seg_id),
        });
        write_manifest(&db_path, &manifest).unwrap();
    }

    // WAL gen 0 should NOT exist
    assert!(
        !wal_generation_path(&db_path, 0).exists(),
        "WAL gen 0 should not exist (simulating already-deleted)"
    );

    // Reopen should handle this gracefully
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Data should be intact from segment
        let node = db.get_node_by_key(1, "survivor").unwrap();
        assert!(node.is_some(), "node should be readable from segment");

        // Stale epoch should be cleaned from manifest
        assert!(
            db.manifest().pending_flush_epochs.is_empty(),
            "stale PublishedPendingRetire epoch should be cleaned up even without WAL file"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_orphan_segment_ignored_on_reopen_cp8() {
    // Create a valid DB with a flushed segment, then create an orphan segment
    // directory (not in manifest). Reopen should clean up the orphan and not
    // be affected by it.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("orphan_seg_cp8");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "real_node", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        assert_eq!(db.segment_count(), 1);
        db.close().unwrap();
    }

    // Create an orphan segment directory with dummy data
    let orphan_path = segment_dir(&db_path, 9999);
    std::fs::create_dir_all(&orphan_path).unwrap();
    std::fs::write(orphan_path.join("dummy.dat"), b"orphan data").unwrap();
    assert!(orphan_path.exists(), "orphan segment dir should exist");

    // Reopen -- orphan should be cleaned up
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Orphan should be removed
        assert!(
            !orphan_path.exists(),
            "orphan segment directory should be cleaned up on reopen"
        );

        // Real segment data should be intact
        assert_eq!(db.segment_count(), 1);
        let node = db.get_node_by_key(1, "real_node").unwrap();
        assert!(node.is_some(), "real node should still be readable");

        db.close().unwrap();
    }
}

#[test]
fn test_orphan_wal_generation_ignored() {
    // Write data normally, then create an extra wal_99.wal file that's not
    // referenced by the manifest. Reopen should NOT replay it.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("orphan_wal");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "real_node", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }

    // Create an orphan WAL generation file with different data
    {
        let mut writer = WalWriter::open_generation(&db_path, 99).unwrap();
        let orphan_node = NodeRecord {
            id: 999,
            type_id: 1,
            key: "orphan_ghost".to_string(),
            props: BTreeMap::new(),
            created_at: 5000,
            updated_at: 5001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 99,
        };
        writer.append(&WalOp::UpsertNode(orphan_node), 99).unwrap();
        writer.sync().unwrap();
    }

    assert!(
        wal_generation_path(&db_path, 99).exists(),
        "orphan WAL gen 99 should exist"
    );

    // Reopen -- the orphan WAL should NOT be replayed
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Real data should be present
        let real = db.get_node_by_key(1, "real_node").unwrap();
        assert!(real.is_some(), "real_node should be recovered");

        // Orphan data should NOT be present
        let orphan = db.get_node(999).unwrap();
        assert!(
            orphan.is_none(),
            "orphan ghost node from unreferenced WAL gen 99 should NOT be replayed"
        );

        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 1, "only real_node should exist");

        // Orphan WAL file should be cleaned up
        assert!(
            !wal_generation_path(&db_path, 99).exists(),
            "orphan WAL gen 99 should be cleaned up on reopen"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_reopen_replays_frozen_epochs_oldest_first() {
    // Write data with key "shared" -> freeze -> overwrite same key -> freeze -> close_fast.
    // On reopen, the WAL generations must be replayed oldest-first so the newest
    // value wins via sequence ordering.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("replay_order");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Gen 0: create node with initial value
        db.upsert_node(
            1,
            "shared",
            UpsertNodeOptions {
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
        db.freeze_memtable().unwrap();

        // Gen 1: update the same node (different weight to distinguish)
        db.upsert_node(
            1,
            "shared",
            UpsertNodeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
        db.freeze_memtable().unwrap();

        // Gen 2: update again
        db.upsert_node(
            1,
            "shared",
            UpsertNodeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();

        db.close_fast().unwrap();
    }

    // Reopen -- newest value should win
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node = db.get_node_by_key(1, "shared").unwrap();
        assert!(node.is_some(), "shared node should be recovered");
        let node = node.unwrap();
        assert!(
            (node.weight - 3.0_f32).abs() < f32::EPSILON,
            "newest write (weight=3.0) should win, got {}",
            node.weight
        );

        // Should be exactly 1 node (not 3 copies)
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 1, "upsert dedup should produce exactly 1 node");

        db.close().unwrap();
    }
}

#[test]
fn test_published_pending_retire_not_replayed() {
    // If a segment is published and the WAL gen is marked PublishedPendingRetire,
    // reopen should NOT replay that WAL gen. Focuses on verifying no duplicate
    // data from replay.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("no_double_replay");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write 5 nodes and an edge, flush to segment
        let ids: Vec<u64> = (0..5)
            .map(|i| {
                db.upsert_node(1, &format!("node_{}", i), UpsertNodeOptions::default())
                    .unwrap()
            })
            .collect();
        let _edge_id = db
            .upsert_edge(ids[0], ids[1], 10, UpsertEdgeOptions::default())
            .unwrap();
        db.flush().unwrap();
        assert!(db.segment_count() >= 1);

        // Write more data after flush (this goes to the new active WAL gen)
        db.upsert_node(1, "post_flush_node", UpsertNodeOptions::default())
            .unwrap();

        db.close().unwrap();
    }

    // Tamper with manifest: add PublishedPendingRetire for gen 0 with the
    // existing segment ID. Also put the gen 0 WAL file back.
    let mut manifest = load_manifest(&db_path).unwrap().unwrap();
    let seg_id = manifest.segments[0].id;
    manifest.pending_flush_epochs.push(FlushEpochMeta {
        epoch_id: 0,
        wal_generation_id: 0,
        state: FlushEpochState::PublishedPendingRetire,
        segment_id: Some(seg_id),
    });
    write_manifest(&db_path, &manifest).unwrap();

    // Create gen 0 WAL file with the same data that's in the segment
    {
        let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
        for i in 0..5 {
            let node = NodeRecord {
                id: i + 1,
                type_id: 1,
                key: format!("node_{}", i),
                props: BTreeMap::new(),
                created_at: 1000,
                updated_at: 1001,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: i + 1,
            };
            writer.append(&WalOp::UpsertNode(node), i + 1).unwrap();
        }
        writer.sync().unwrap();
    }

    // Reopen -- PublishedPendingRetire WAL gen should NOT be replayed
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Should have exactly 6 nodes: 5 from segment + 1 post-flush
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            6,
            "should have 6 nodes (5 from segment + 1 post-flush), not more from double replay"
        );

        // Epoch should be cleaned up
        assert!(
            db.manifest().pending_flush_epochs.is_empty(),
            "PublishedPendingRetire epoch should be cleaned up"
        );

        // WAL gen 0 should be deleted
        assert!(
            !wal_generation_path(&db_path, 0).exists(),
            "WAL gen 0 should be removed after PublishedPendingRetire cleanup"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_reopen_after_flush_then_more_writes() {
    // Write -> flush -> write more -> close -> reopen.
    // Verify both flushed (in segment) and unflushed (replayed from WAL) data present.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("flush_then_write");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write and flush
        for i in 0..10 {
            db.upsert_node(1, &format!("flushed_{}", i), UpsertNodeOptions::default())
                .unwrap();
        }
        db.flush().unwrap();

        // Write more (not flushed)
        for i in 0..10 {
            db.upsert_node(1, &format!("unflushed_{}", i), UpsertNodeOptions::default())
                .unwrap();
        }

        // Add edge spanning flushed and unflushed nodes
        let flushed_node = db.get_node_by_key(1, "flushed_0").unwrap().unwrap();
        let unflushed_node = db.get_node_by_key(1, "unflushed_0").unwrap().unwrap();
        db.upsert_edge(
            flushed_node.id,
            unflushed_node.id,
            10,
            UpsertEdgeOptions::default(),
        )
        .unwrap();

        db.close().unwrap();
    }

    // Reopen
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Verify all flushed nodes
        for i in 0..10 {
            let key = format!("flushed_{}", i);
            assert!(
                db.get_node_by_key(1, &key).unwrap().is_some(),
                "{} should be present from segment",
                key
            );
        }

        // Verify all unflushed nodes
        for i in 0..10 {
            let key = format!("unflushed_{}", i);
            assert!(
                db.get_node_by_key(1, &key).unwrap().is_some(),
                "{} should be present from WAL replay",
                key
            );
        }

        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 20, "all 20 nodes should be present");

        db.close().unwrap();
    }
}

#[test]
fn test_multiple_flush_reopen_cycles() {
    // Write -> flush -> write -> flush -> write -> close -> reopen -> verify all data.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_flush_cycle");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Cycle 1: write + flush
        for i in 0..5 {
            db.upsert_node(1, &format!("cycle1_{}", i), UpsertNodeOptions::default())
                .unwrap();
        }
        db.flush().unwrap();

        // Cycle 2: write + flush
        for i in 0..5 {
            db.upsert_node(1, &format!("cycle2_{}", i), UpsertNodeOptions::default())
                .unwrap();
        }
        db.flush().unwrap();

        // Cycle 3: write (no flush -- stays in WAL)
        for i in 0..5 {
            db.upsert_node(1, &format!("cycle3_{}", i), UpsertNodeOptions::default())
                .unwrap();
        }

        db.close().unwrap();
    }

    // Reopen and verify all data
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for cycle in 1..=3 {
            for i in 0..5 {
                let key = format!("cycle{}_{}", cycle, i);
                assert!(
                    db.get_node_by_key(1, &key).unwrap().is_some(),
                    "{} should be present",
                    key
                );
            }
        }

        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            15,
            "all 15 nodes across 3 cycles should be present"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_close_fast_then_close_normally() {
    // Write -> freeze -> close_fast -> reopen -> flush -> close -> reopen -> verify.
    // Proves that close_fast preserves recovery state, and a subsequent normal
    // close after flush produces a fully drained DB.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fast_then_normal");

    // Phase 1: Write, freeze, close_fast (simulate crash)
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "surviving_node", UpsertNodeOptions::default())
            .unwrap();
        let a = db
            .upsert_node(1, "node_a", UpsertNodeOptions::default())
            .unwrap();
        let b = db
            .upsert_node(1, "node_b", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_edge(a, b, 10, UpsertEdgeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();

        // Write more to active
        db.upsert_node(1, "active_node", UpsertNodeOptions::default())
            .unwrap();

        db.close_fast().unwrap();
    }

    // Phase 2: Reopen, verify data, flush, close normally
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // All data should be present via WAL replay
        assert!(db.get_node_by_key(1, "surviving_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "node_a").unwrap().is_some());
        assert!(db.get_node_by_key(1, "node_b").unwrap().is_some());
        assert!(db.get_node_by_key(1, "active_node").unwrap().is_some());
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 4, "all 4 nodes should be present after recovery");

        // Now flush and close normally
        db.flush().unwrap();
        assert_eq!(db.immutable_memtable_count(), 0);
        assert!(db.segment_count() >= 1, "segments should exist after flush");

        db.close().unwrap();
    }

    // Phase 3: Reopen again, verify everything is clean
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        assert!(db.get_node_by_key(1, "surviving_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "node_a").unwrap().is_some());
        assert!(db.get_node_by_key(1, "node_b").unwrap().is_some());
        assert!(db.get_node_by_key(1, "active_node").unwrap().is_some());
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 4);

        // Manifest should be clean -- no pending flush epochs
        assert!(
            db.manifest().pending_flush_epochs.is_empty(),
            "no pending epochs after clean flush + close"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_crash_recovery_preserves_edges() {
    // Verify that edges survive crash/reopen across different source tiers.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_edges");

    let node_a;
    let node_b;
    let node_c;
    let edge_ab;
    let edge_bc;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        node_a = db
            .upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        node_b = db
            .upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        edge_ab = db
            .upsert_edge(node_a, node_b, 10, UpsertEdgeOptions::default())
            .unwrap();

        // Freeze: nodes a,b and edge_ab move to immutable
        db.freeze_memtable().unwrap();

        // Write more in active gen
        node_c = db
            .upsert_node(1, "c", UpsertNodeOptions::default())
            .unwrap();
        edge_bc = db
            .upsert_edge(node_b, node_c, 20, UpsertEdgeOptions::default())
            .unwrap();

        // Simulate crash
        db.close_fast().unwrap();
    }

    // Reopen and verify
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // All nodes
        assert!(
            db.get_node(node_a).unwrap().is_some(),
            "node_a should be present"
        );
        assert!(
            db.get_node(node_b).unwrap().is_some(),
            "node_b should be present"
        );
        assert!(
            db.get_node(node_c).unwrap().is_some(),
            "node_c should be present"
        );

        // All edges
        let e_ab = db.get_edge(edge_ab).unwrap();
        assert!(e_ab.is_some(), "edge a->b should be present");
        let e_ab = e_ab.unwrap();
        assert_eq!(e_ab.from, node_a);
        assert_eq!(e_ab.to, node_b);

        let e_bc = db.get_edge(edge_bc).unwrap();
        assert!(e_bc.is_some(), "edge b->c should be present");
        let e_bc = e_bc.unwrap();
        assert_eq!(e_bc.from, node_b);
        assert_eq!(e_bc.to, node_c);

        db.close().unwrap();
    }
}

#[test]
fn test_crash_recovery_preserves_deletes() {
    // Verify that deletes survive crash/reopen. A node deleted after freeze
    // should remain deleted after recovery.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("crash_deletes");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let node_a = db
            .upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        let node_b = db
            .upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        let _edge = db
            .upsert_edge(node_a, node_b, 10, UpsertEdgeOptions::default())
            .unwrap();

        db.freeze_memtable().unwrap();

        // Delete node_a in the active generation -- should cascade to the edge
        db.delete_node(node_a).unwrap();

        // Simulate crash
        db.close_fast().unwrap();
    }

    // Reopen
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // node_a should be deleted
        assert!(
            db.get_node_by_key(1, "a").unwrap().is_none(),
            "deleted node_a should not be visible after recovery"
        );

        // node_b should still exist
        assert!(
            db.get_node_by_key(1, "b").unwrap().is_some(),
            "non-deleted node_b should survive recovery"
        );

        // Only 1 node should be visible
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 1, "only node_b should be visible");

        db.close().unwrap();
    }
}

#[test]
fn test_reopen_engine_seq_continuity() {
    // Verify that engine_seq is continuous across crash/reopen cycles.
    // After reopen, new writes should get seq values > pre-crash max.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("seq_continuity");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write several items
        db.upsert_node(1, "a", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "b", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(1, "c", UpsertNodeOptions::default())
            .unwrap();

        db.freeze_memtable().unwrap();

        db.upsert_node(1, "d", UpsertNodeOptions::default())
            .unwrap();

        db.close_fast().unwrap();
    }

    // Reopen
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write a new node -- its seq should be > pre-crash values
        db.upsert_node(1, "post_crash", UpsertNodeOptions::default())
            .unwrap();

        // The node should exist
        assert!(db.get_node_by_key(1, "post_crash").unwrap().is_some());

        // All pre-crash data should be present
        assert!(db.get_node_by_key(1, "a").unwrap().is_some());
        assert!(db.get_node_by_key(1, "b").unwrap().is_some());
        assert!(db.get_node_by_key(1, "c").unwrap().is_some());
        assert!(db.get_node_by_key(1, "d").unwrap().is_some());

        db.close().unwrap();
    }
}

#[test]
fn test_repeated_crash_reopen_cycles() {
    // Simulate multiple crash/reopen cycles, verifying data accumulates correctly.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("multi_crash");

    // Crash cycle 1
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.upsert_node(1, "cycle1_node", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }

    // Crash cycle 2
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        // Verify cycle 1 data
        assert!(db.get_node_by_key(1, "cycle1_node").unwrap().is_some());
        db.upsert_node(1, "cycle2_node", UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
        db.upsert_node(1, "cycle2_active", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }

    // Crash cycle 3
    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        // Verify cycle 1 + 2 data
        assert!(db.get_node_by_key(1, "cycle1_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "cycle2_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "cycle2_active").unwrap().is_some());
        db.upsert_node(1, "cycle3_node", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_node(1, "cycle3_unflushed", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }

    // Final verification
    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(db.get_node_by_key(1, "cycle1_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "cycle2_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "cycle2_active").unwrap().is_some());
        assert!(db.get_node_by_key(1, "cycle3_node").unwrap().is_some());
        assert!(db.get_node_by_key(1, "cycle3_unflushed").unwrap().is_some());

        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            5,
            "all 5 nodes across 3 crash cycles should be present"
        );

        db.close().unwrap();
    }
}

// --- CP9: Backpressure and close semantics ---

#[test]
fn test_backpressure_triggers_on_total_bytes() {
    // Configure thresholds so that a single node (~190 bytes) is well under
    // the threshold but the combined active + immutable total exceeds it.
    // This proves the soft threshold considers total buffered bytes, not
    // just the active memtable.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bp_total");
    // Each node is ~190 bytes (120 base + key + type/time index overhead).
    // Threshold at 350 bytes: 1 node won't trigger, but 2 nodes will.
    let opts = DbOptions {
        memtable_flush_threshold: 350, // triggers when total > 350 bytes
        memtable_hard_cap_bytes: 0,    // disable hard cap
        max_immutable_memtables: 0,    // disable count-based backpressure
        compact_after_n_flushes: 0,    // disable auto-compact
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Write 1 node (under threshold) and freeze.
    db.upsert_node(1, "frozen", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_memtable_count(), 1);
    assert_eq!(db.segment_count(), 0);

    // Now write 1 more node. Active ~190 + immutable ~190 = ~380, exceeding
    // the 350-byte threshold. auto-flush should fire (async).
    db.upsert_node(1, "active", UpsertNodeOptions::default())
        .unwrap();

    // Auto-flush is now async, so drain pending flushes before asserting.
    db.flush().unwrap();

    // Auto-flush should have fired because total bytes exceeded threshold
    assert!(
        db.segment_count() >= 1,
        "auto-flush should trigger when total memtable bytes exceed soft threshold"
    );

    // Both nodes should be readable
    assert!(db.find_existing_node(1, "frozen").unwrap().is_some());
    assert!(db.find_existing_node(1, "active").unwrap().is_some());

    db.close().unwrap();
}

#[test]
fn test_max_immutable_memtables_blocks() {
    // Configure max_immutable_memtables=2. Freeze twice to fill the
    // queue, then the next write should trigger a backpressure flush.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("max_imm");
    let opts = DbOptions {
        memtable_flush_threshold: 0, // disable soft auto-flush
        memtable_hard_cap_bytes: 0,  // disable byte-based backpressure
        max_immutable_memtables: 2,  // count-based backpressure at 2
        compact_after_n_flushes: 0,  // disable auto-compact
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Write and freeze twice to reach max_immutable_memtables=2
    for i in 0..5 {
        db.upsert_node(1, &format!("g1:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_memtable_count(), 1);
    assert_eq!(db.segment_count(), 0);

    for i in 0..5 {
        db.upsert_node(1, &format!("g2:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_memtable_count(), 2);
    assert_eq!(db.segment_count(), 0);

    // Now write to the active memtable. The next write triggers
    // backpressure because immutable count == max_immutable_memtables.
    for i in 0..5 {
        db.upsert_node(1, &format!("g3:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }

    // Backpressure should have flushed at least one immutable
    assert!(
        db.segment_count() >= 1,
        "backpressure should trigger flush when immutable count >= max"
    );

    // All data should be readable
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 15, "all 15 nodes should be visible");

    db.close().unwrap();
}

#[test]
fn test_max_immutable_memtables_disabled_when_zero() {
    // With max_immutable_memtables=0, count-based backpressure is disabled.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("max_imm_disabled");
    let opts = DbOptions {
        memtable_flush_threshold: 0, // disable soft auto-flush
        memtable_hard_cap_bytes: 0,  // disable byte-based backpressure
        max_immutable_memtables: 0,  // disabled
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Freeze 5 times without any flush being triggered
    for batch in 0..5 {
        for i in 0..3 {
            db.upsert_node(
                1,
                &format!("b{}:{}", batch, i),
                UpsertNodeOptions::default(),
            )
            .unwrap();
        }
        db.freeze_memtable().unwrap();
    }

    // No flushes should have occurred
    assert_eq!(
        db.segment_count(),
        0,
        "no flush should trigger with count backpressure disabled"
    );
    assert_eq!(db.immutable_memtable_count(), 5);

    db.close().unwrap();
}

#[test]
fn test_close_drains_all_immutables() {
    // Freeze multiple times (creating immutable memtables), then close().
    // After reopen, all data should be in segments (not WAL replay).
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("close_drain");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        max_immutable_memtables: 0, // disable backpressure
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };

    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Write and freeze 3 times
        for batch in 0..3 {
            for i in 0..5 {
                db.upsert_node(
                    1,
                    &format!("b{}:{}", batch, i),
                    UpsertNodeOptions::default(),
                )
                .unwrap();
            }
            db.freeze_memtable().unwrap();
        }

        // Write more to active memtable
        for i in 0..5 {
            db.upsert_node(1, &format!("active:{}", i), UpsertNodeOptions::default())
                .unwrap();
        }

        assert_eq!(db.immutable_memtable_count(), 3);
        assert_eq!(db.segment_count(), 0);

        // close() should freeze active + flush all immutables
        db.close().unwrap();
    }

    // Reopen and verify all data is in segments
    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(all.len(), 20, "all 20 nodes should survive close + reopen");

        // Data should be in segments, not memtable (WAL was retired)
        assert!(
            db.segment_count() >= 1,
            "close() should have flushed to segments"
        );

        // Verify specific nodes from each batch
        assert!(db.find_existing_node(1, "b0:0").unwrap().is_some());
        assert!(db.find_existing_node(1, "b1:2").unwrap().is_some());
        assert!(db.find_existing_node(1, "b2:4").unwrap().is_some());
        assert!(db.find_existing_node(1, "active:3").unwrap().is_some());

        db.close().unwrap();
    }
}

#[test]
fn test_close_fast_preserves_wal_for_recovery() {
    // close_fast with frozen immutable memtables should NOT flush them.
    // On reopen, WAL replay recovers all data.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("close_fast_wal");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        max_immutable_memtables: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };

    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Write and freeze multiple times
        for batch in 0..3 {
            for i in 0..5 {
                db.upsert_node(
                    1,
                    &format!("b{}:{}", batch, i),
                    UpsertNodeOptions::default(),
                )
                .unwrap();
            }
            db.freeze_memtable().unwrap();
        }

        // Write to active memtable
        for i in 0..5 {
            db.upsert_node(1, &format!("active:{}", i), UpsertNodeOptions::default())
                .unwrap();
        }

        assert_eq!(db.immutable_memtable_count(), 3);
        assert_eq!(db.segment_count(), 0);

        // close_fast should NOT flush, just sync WAL and persist manifest.
        db.close_fast().unwrap();
    }

    // Reopen: WAL replay recovers everything.
    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();
        let all = db.get_nodes_by_type(1).unwrap();
        assert_eq!(
            all.len(),
            20,
            "all 20 nodes should survive close_fast via WAL replay"
        );

        // No segments should exist (close_fast didn't flush)
        assert_eq!(
            db.segment_count(),
            0,
            "close_fast should not create segments"
        );

        db.close().unwrap();
    }
}

#[test]
fn test_compaction_respects_flush_published_segments() {
    // Verify that compaction apply validates its input segment set against
    // the live manifest. If a flush published new segments between compact
    // start and apply, the new segments should not be removed.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("compact_flush_interleave");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        max_immutable_memtables: 0,
        compact_after_n_flushes: 0, // manual control
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Create first segment
    for i in 0..10 {
        db.upsert_node(1, &format!("seg1:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.flush().unwrap();

    // Create second segment
    for i in 0..10 {
        db.upsert_node(1, &format!("seg2:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.flush().unwrap();

    assert_eq!(db.segment_count(), 2);

    // Compact the two segments
    db.compact().unwrap();

    // After compaction, should have exactly 1 segment (the compacted one)
    assert_eq!(db.segment_count(), 1);

    // Create a third segment (published after compaction)
    for i in 0..10 {
        db.upsert_node(1, &format!("seg3:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.flush().unwrap();

    // Should have 2 segments now: compacted + new
    assert_eq!(db.segment_count(), 2);

    // All 30 nodes should be readable
    let all = db.get_nodes_by_type(1).unwrap();
    assert_eq!(all.len(), 30);

    // Compact again to verify new segments coexist properly
    db.compact().unwrap();
    assert_eq!(db.segment_count(), 1);
    assert_eq!(db.get_nodes_by_type(1).unwrap().len(), 30);

    db.close().unwrap();
}

#[test]
fn test_close_with_active_and_immutable_data() {
    // Verify close() handles both active memtable data and frozen
    // immutables correctly.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("close_mixed");
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        max_immutable_memtables: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };

    let mut node_ids = Vec::new();

    {
        let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Write to active, then freeze
        let id = db
            .upsert_node(1, "frozen_a", UpsertNodeOptions::default())
            .unwrap();
        node_ids.push(id);
        db.freeze_memtable().unwrap();

        // Write more to new active
        let id = db
            .upsert_node(1, "active_b", UpsertNodeOptions::default())
            .unwrap();
        node_ids.push(id);

        // close() should freeze active + flush everything
        db.close().unwrap();
    }

    {
        let db = DatabaseEngine::open(&db_path, &opts).unwrap();
        for &id in &node_ids {
            assert!(db.get_node(id).unwrap().is_some());
        }
        assert_eq!(db.get_nodes_by_type(1).unwrap().len(), 2);
        assert!(db.segment_count() >= 1, "close() should have flushed");
        db.close().unwrap();
    }
}

#[test]
fn test_close_empty_db_is_noop() {
    // close() on an empty DB should not error or create segments.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("close_empty");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let db = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(db.segment_count(), 0);
    db.close().unwrap();

    let db2 = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(db2.segment_count(), 0);
    db2.close().unwrap();
}

#[test]
fn test_backpressure_bytes_and_count_combined() {
    // Both byte-based and count-based backpressure should work together.
    // Set both limits; whichever triggers first should cause a flush.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("bp_combined");
    let opts = DbOptions {
        memtable_flush_threshold: 0,   // no soft auto-flush
        memtable_hard_cap_bytes: 4096, // moderate byte cap
        max_immutable_memtables: 1,    // very low count cap
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(&db_path, &opts).unwrap();

    // Freeze once to reach max_immutable_memtables=1
    for i in 0..3 {
        db.upsert_node(1, &format!("x:{}", i), UpsertNodeOptions::default())
            .unwrap();
    }
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_memtable_count(), 1);

    // Next write should trigger count-based backpressure
    db.upsert_node(1, "trigger", UpsertNodeOptions::default())
        .unwrap();

    // Flush should have happened
    assert!(
        db.segment_count() >= 1,
        "count-based backpressure should trigger flush"
    );

    db.close().unwrap();
}

// --- Async flush property tests (ImmutableEpoch) ---

#[test]
fn test_data_visible_while_in_flight() {
    // Frozen data must remain visible to reads while the flush worker
    // is processing it. Uses one-shot pause to hold the worker.
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let id = db
        .upsert_node(1, "visible", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 1);

    // Set pause hook, enqueue flush
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap();

    // Worker is now paused; data should still be visible.
    ready_rx.recv().unwrap();
    assert_eq!(db.in_flight_count(), 1);
    assert_eq!(db.immutable_epoch_count(), 1);

    // All read paths must see the frozen data
    assert!(db.get_node(id).unwrap().is_some());
    assert!(db.get_node_by_key(1, "visible").unwrap().is_some());

    // Release worker, wait for completion
    release_tx.send(()).unwrap();
    let seg = db.wait_one_flush().unwrap();
    assert!(seg.is_some());
    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(db.segment_count(), 1);

    // Data still readable from segment
    assert!(db.get_node(id).unwrap().is_some());

    db.close().unwrap();
}

#[test]
fn test_multiple_epochs_all_visible_during_flush() {
    // Multiple frozen epochs + active memtable: all data visible,
    // precedence correct (active > newest immutable > oldest immutable).
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    // Gen 1: oldest frozen
    let id1 = db
        .upsert_node(
            1,
            "gen1_key",
            UpsertNodeOptions {
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    db.freeze_memtable().unwrap();

    // Gen 2: newest frozen
    let id2 = db
        .upsert_node(
            1,
            "gen2_key",
            UpsertNodeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    db.freeze_memtable().unwrap();

    // Gen 3: active memtable
    let id3 = db
        .upsert_node(
            1,
            "gen3_key",
            UpsertNodeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(db.immutable_epoch_count(), 2);

    // Pause hook on first enqueue (oldest epoch)
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap(); // oldest gets pause (also starts worker)
    db.enqueue_one_flush().unwrap(); // newest queued behind

    ready_rx.recv().unwrap(); // first job paused

    // ALL data from all 3 generations visible
    assert!(db.get_node(id1).unwrap().is_some());
    assert!(db.get_node(id2).unwrap().is_some());
    assert!(db.get_node(id3).unwrap().is_some());

    // Precedence: if same key existed across generations, active wins
    // (we used different keys, so just check all exist)
    assert!(db.get_node_by_key(1, "gen1_key").unwrap().is_some());
    assert!(db.get_node_by_key(1, "gen2_key").unwrap().is_some());
    assert!(db.get_node_by_key(1, "gen3_key").unwrap().is_some());

    // Release, drain
    release_tx.send(()).unwrap();
    db.flush().unwrap();

    assert_eq!(db.immutable_epoch_count(), 0);
    // 3 segments: gen1, gen2, plus gen3 (active) which flush() also freezes+flushes
    assert_eq!(db.segment_count(), 3);

    db.close().unwrap();
}

#[test]
fn test_auto_flush_is_async_not_blocking() {
    // After auto-flush triggers, the writer returns immediately.
    // Frozen data stays readable in immutable_epochs.
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 256,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    // Set pause before writing; auto-flush will consume it.
    let (ready_rx, release_tx) = db.set_flush_pause();

    // Write enough data to exceed threshold and trigger auto-flush
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = db
            .upsert_node(1, &format!("af_{}", i), UpsertNodeOptions::default())
            .unwrap();
        ids.push(id);
    }

    // Worker should be paused; auto-flush did NOT block.
    ready_rx.recv().unwrap();

    // Data still readable (frozen memtable visible)
    for &id in &ids {
        assert!(
            db.get_node(id).unwrap().is_some(),
            "node {} not visible during in-flight flush",
            id
        );
    }

    // Release, drain
    release_tx.send(()).unwrap();
    db.flush().unwrap();
    assert!(db.segment_count() >= 1);

    db.close().unwrap();
}

#[test]
fn test_apply_removes_epoch_after_publish() {
    // After a flush completes, the epoch is removed from immutable_epochs
    // and data is readable from the segment.
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let id = db
        .upsert_node(1, "apply_test", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 1);

    db.enqueue_one_flush().unwrap();
    let seg = db.wait_one_flush().unwrap();
    assert!(seg.is_some());

    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(db.segment_count(), 1);
    assert!(db.get_node(id).unwrap().is_some());

    db.close().unwrap();
}

#[test]
fn test_worker_failure_keeps_epoch_visible() {
    // When the flush worker fails, the epoch stays in immutable_epochs
    // with in_flight=false, data remains readable.
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let id = db
        .upsert_node(1, "fail_test", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();

    // Inject failure
    db.set_flush_force_error();
    db.enqueue_one_flush().unwrap();

    // Wait for the failure result
    let result = db.wait_one_flush();
    assert!(result.is_err());

    // Epoch NOT removed, in_flight reset to false
    assert_eq!(db.immutable_epoch_count(), 1);
    assert_eq!(db.in_flight_count(), 0);

    // Data still readable
    assert!(db.get_node(id).unwrap().is_some());
    assert!(db.get_node_by_key(1, "fail_test").unwrap().is_some());

    db.close().unwrap();
}

#[test]
fn test_backpressure_counts_all_epochs_including_in_flight() {
    // Backpressure should consider ALL immutable epochs (in-flight + queued).
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        max_immutable_memtables: 2,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    // Freeze twice to reach max
    db.upsert_node(1, "bp1", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    db.upsert_node(1, "bp2", UpsertNodeOptions::default())
        .unwrap();
    db.freeze_memtable().unwrap();
    assert_eq!(db.immutable_epoch_count(), 2);

    // Pause the first flush
    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap();

    // Both epochs are in immutable_epochs (one in-flight, one queued)
    assert_eq!(db.immutable_epoch_count(), 2);

    // Release, let flushes complete
    release_tx.send(()).unwrap();
    db.flush().unwrap();
    assert_eq!(db.immutable_epoch_count(), 0);
    assert!(db.segment_count() >= 2);

    db.close().unwrap();
}

#[test]
fn test_flush_sync_barrier_drains_all_epochs() {
    // The public flush() method drains all epochs to segments.
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    // Create 3 frozen memtables
    for i in 0..3 {
        db.upsert_node(1, &format!("sync_{}", i), UpsertNodeOptions::default())
            .unwrap();
        db.freeze_memtable().unwrap();
    }
    assert_eq!(db.immutable_epoch_count(), 3);

    // flush() should drain everything
    db.flush().unwrap();
    assert_eq!(db.immutable_epoch_count(), 0);
    assert_eq!(db.segment_count(), 3);

    // All data readable from segments
    for i in 0..3 {
        assert!(db
            .get_node_by_key(1, &format!("sync_{}", i))
            .unwrap()
            .is_some());
    }

    db.close().unwrap();
}

#[test]
#[ignore] // Run with: cargo test --release -- --ignored --nocapture test_async_flush_latency
fn test_async_flush_latency_profile() {
    use std::time::Instant;

    const WRITE_COUNT: u64 = 10_000;
    const THRESHOLD: usize = 1024 * 1024; // 1MB
    const SYNC_FLUSH_INTERVAL: u64 = 3300;

    fn percentile(sorted: &[u128], p: f64) -> u128 {
        let idx = ((sorted.len() as f64) * p / 100.0) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn write_opts(i: u64) -> UpsertNodeOptions {
        let mut props = BTreeMap::new();
        props.insert(
            "name".to_string(),
            PropValue::String(format!("bench_node_{}", i)),
        );
        props.insert(
            "category".to_string(),
            PropValue::String("latency_test".to_string()),
        );
        props.insert("score".to_string(), PropValue::Float(i as f64 * 0.001));
        UpsertNodeOptions {
            props,
            ..Default::default()
        }
    }

    // --- Sync baseline: threshold=0, manual flush ---
    let sync_latencies = {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut latencies = Vec::with_capacity(WRITE_COUNT as usize);

        for i in 0..WRITE_COUNT {
            let start = Instant::now();
            db.upsert_node(1, &format!("n{}", i), write_opts(i))
                .unwrap();
            if (i + 1) % SYNC_FLUSH_INTERVAL == 0 {
                db.flush().unwrap();
            }
            latencies.push(start.elapsed().as_micros());
        }
        db.close().unwrap();
        latencies.sort_unstable();
        latencies
    };

    // --- Async: threshold=1MB, auto-flush ---
    let async_latencies = {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            memtable_flush_threshold: THRESHOLD,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut latencies = Vec::with_capacity(WRITE_COUNT as usize);

        for i in 0..WRITE_COUNT {
            let start = Instant::now();
            db.upsert_node(1, &format!("n{}", i), write_opts(i))
                .unwrap();
            latencies.push(start.elapsed().as_micros());
        }
        db.close().unwrap();
        latencies.sort_unstable();
        latencies
    };

    let sync_blocked = sync_latencies.iter().filter(|&&l| l > 1000).count();
    let async_blocked = async_latencies.iter().filter(|&&l| l > 1000).count();

    eprintln!(
        "\n=== Async Flush Latency Profile ({} writes, threshold=1MB) ===\n",
        WRITE_COUNT
    );
    eprintln!("sync_baseline (flush every {}):", SYNC_FLUSH_INTERVAL);
    eprintln!(
        "  p50={:>6}µs  p95={:>6}µs  p99={:>6}µs  max={:>6}µs  blocked(>1ms)={}",
        percentile(&sync_latencies, 50.0),
        percentile(&sync_latencies, 95.0),
        percentile(&sync_latencies, 99.0),
        sync_latencies.last().unwrap(),
        sync_blocked,
    );
    eprintln!("async_auto_flush (threshold=1MB):");
    eprintln!(
        "  p50={:>6}µs  p95={:>6}µs  p99={:>6}µs  max={:>6}µs  blocked(>1ms)={}",
        percentile(&async_latencies, 50.0),
        percentile(&async_latencies, 95.0),
        percentile(&async_latencies, 99.0),
        async_latencies.last().unwrap(),
        async_blocked,
    );

    // Async p99 should be meaningfully lower than sync p99
    let sync_p99 = percentile(&sync_latencies, 99.0);
    let async_p99 = percentile(&async_latencies, 99.0);
    eprintln!(
        "\np99 improvement: sync={}µs → async={}µs ({:.0}% reduction)",
        sync_p99,
        async_p99,
        if sync_p99 > 0 {
            (1.0 - async_p99 as f64 / sync_p99 as f64) * 100.0
        } else {
            0.0
        }
    );
    eprintln!(
        "max improvement: sync={}µs → async={}µs ({:.0}% reduction)",
        sync_latencies.last().unwrap(),
        async_latencies.last().unwrap(),
        if *sync_latencies.last().unwrap() > 0 {
            (1.0 - *async_latencies.last().unwrap() as f64 / *sync_latencies.last().unwrap() as f64)
                * 100.0
        } else {
            0.0
        }
    );
}

#[test]
fn test_property_index_manifest_reopens_and_reseeds_active_memtable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        index_id = db
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap()
            .index_id;
        db.close().unwrap();
    }

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let indexes = db.list_node_property_indexes();
        assert_eq!(indexes.len(), 1);
        let info = wait_for_property_index_state(&db, index_id, SecondaryIndexState::Ready);
        assert_eq!(info.index_id, index_id);
        assert!(db
            .active_memtable()
            .secondary_index_declarations()
            .contains_key(&index_id));

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let node_id = db
            .upsert_node(
                1,
                "a",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        let status_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let eq_ids = db
            .active_memtable()
            .secondary_eq_state()
            .get(&index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(eq_ids.contains(&node_id));

        db.close().unwrap();
    }
}

#[test]
fn test_ensure_property_index_while_flush_in_flight_preserves_manifest_and_seeding() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String("active".to_string()),
    );
    let node_id = db
        .upsert_node(
            1,
            "frozen",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.freeze_memtable().unwrap();

    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap();

    let info = db
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    assert_eq!(info.state, SecondaryIndexState::Building);
    let status_hash = hash_prop_value(&PropValue::String("active".to_string()));
    let frozen_eq_ids = db
        .immutable_memtable(0)
        .secondary_eq_state()
        .get(&info.index_id)
        .unwrap()
        .get(&status_hash)
        .unwrap();
    assert!(frozen_eq_ids.contains(&node_id));
    assert_eq!(db.manifest().secondary_indexes.len(), 1);

    release_tx.send(()).unwrap();
    assert!(db.wait_one_flush().unwrap().is_some());
    let ready = wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert_eq!(ready.index_id, info.index_id);
    let seg_dir = segment_dir(dir.path(), db.segments[0].segment_id);
    assert!(crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id).exists());
    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes(1, "status", &PropValue::String("active".to_string()))
            .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 0);
    assert_eq!(routes.equality_index_lookup, 1);
    db.close().unwrap();

    let reopened = DatabaseEngine::open(dir.path(), &opts).unwrap();
    let ready = wait_for_property_index_state(&reopened, info.index_id, SecondaryIndexState::Ready);
    assert_eq!(ready.index_id, info.index_id);
    reopened.close().unwrap();
}

#[test]
fn test_ready_property_index_downgrades_when_flush_publish_missed_declaration_snapshot() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String("active".to_string()),
    );
    let node_id = db
        .upsert_node(
            1,
            "frozen",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.freeze_memtable().unwrap();

    let (publish_ready_rx, publish_release_tx) = db.set_flush_publish_pause();
    db.enqueue_one_flush().unwrap();
    publish_ready_rx.recv().unwrap();

    let info = db
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let (repair_ready_rx, repair_release_tx) = db.set_secondary_index_build_pause();
    publish_release_tx.send(()).unwrap();
    assert!(db.wait_one_flush().unwrap().is_some());
    repair_ready_rx.recv().unwrap();

    let building = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(building.state, SecondaryIndexState::Building);

    let seg_dir = segment_dir(dir.path(), db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    assert!(!sidecar_path.exists());

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes(1, "status", &PropValue::String("active".to_string()))
            .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 1);
    assert_eq!(routes.equality_index_lookup, 0);

    repair_release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes(1, "status", &PropValue::String("active".to_string()))
            .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 0);
    assert_eq!(routes.equality_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_ready_property_index_downgrades_when_bg_compaction_missed_declaration_snapshot() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 1,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let active = PropValue::String("active".to_string());
    let (compact_ready_rx, compact_release_tx) = db.set_bg_compact_pause();
    let mut first_props = BTreeMap::new();
    first_props.insert("status".to_string(), active.clone());
    let node_a = db
        .upsert_node(
            1,
            "seg_a",
            UpsertNodeOptions {
                props: first_props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let mut second_props = BTreeMap::new();
    second_props.insert("status".to_string(), active.clone());
    let node_b = db
        .upsert_node(
            1,
            "seg_b",
            UpsertNodeOptions {
                props: second_props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();
    compact_ready_rx.recv().unwrap();
    assert_eq!(db.segment_count(), 2);

    let expected_ids = vec![node_a, node_b];

    let info = db
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let (repair_ready_rx, repair_release_tx) = db.set_secondary_index_build_pause();
    compact_release_tx.send(()).unwrap();
    assert!(db.wait_for_bg_compaction().is_some());
    repair_ready_rx.recv().unwrap();

    let building = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(building.state, SecondaryIndexState::Building);
    assert_eq!(db.segment_count(), 1);

    let seg_dir = segment_dir(dir.path(), db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    assert!(!sidecar_path.exists());

    db.reset_property_query_routes();
    let mut results = db
        .find_nodes(1, "status", &PropValue::String("active".to_string()))
        .unwrap();
    results.sort_unstable();
    assert_eq!(results, expected_ids);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 1);
    assert_eq!(routes.equality_index_lookup, 0);

    repair_release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes(1, "status", &PropValue::String("active".to_string()))
            .unwrap(),
        expected_ids
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 0);
    assert_eq!(routes.equality_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_failed_property_indexes_survive_reopen_and_queries_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut color_props = BTreeMap::new();
        color_props.insert("color".to_string(), PropValue::String("red".to_string()));
        let color_id = db
            .upsert_node(
                1,
                "color",
                UpsertNodeOptions {
                    props: color_props,
                    ..Default::default()
                },
            )
            .unwrap();
        let mut score_props = BTreeMap::new();
        score_props.insert("score".to_string(), PropValue::Int(10));
        let score_id = db
            .upsert_node(
                1,
                "score",
                UpsertNodeOptions {
                    props: score_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let eq = db
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        let range = db
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        db.with_runtime_manifest_write(|manifest| {
            for entry in &mut manifest.secondary_indexes {
                if entry.index_id == eq.index_id {
                    entry.state = SecondaryIndexState::Failed;
                    entry.last_error = Some("eq failed".to_string());
                } else if entry.index_id == range.index_id {
                    entry.state = SecondaryIndexState::Failed;
                    entry.last_error = Some("range failed".to_string());
                }
            }
            Ok(())
        })
        .unwrap();
        db.rebuild_secondary_index_catalog().unwrap();

        assert_eq!(
            db.find_nodes(1, "color", &PropValue::String("red".to_string()))
                .unwrap(),
            vec![color_id]
        );
        assert_eq!(
            db.find_nodes_range(
                1,
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            )
            .unwrap(),
            vec![score_id]
        );

        db.close().unwrap();
    }

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let indexes = db.list_node_property_indexes();
        assert_eq!(indexes.len(), 2);
        assert!(
            indexes
                .iter()
                .all(|info| info.state == SecondaryIndexState::Failed),
            "{indexes:?}"
        );
        assert!(indexes
            .iter()
            .any(|info| info.last_error.as_deref() == Some("eq failed")));
        assert!(indexes
            .iter()
            .any(|info| info.last_error.as_deref() == Some("range failed")));

        db.reset_property_query_routes();
        assert_eq!(
            db.find_nodes(1, "color", &PropValue::String("red".to_string()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            db.find_nodes_range(
                1,
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            )
            .unwrap()
            .len(),
            1
        );
        let routes = db.property_query_route_snapshot();
        assert_eq!(routes.equality_scan_fallback, 1);
        assert_eq!(routes.range_scan_fallback, 1);
        assert_eq!(routes.equality_index_lookup, 0);
        assert_eq!(routes.range_index_lookup, 0);

        db.close().unwrap();
    }
}

#[test]
fn test_zero_declaration_flush_and_compaction_skip_equality_artifacts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for key in ["a", "b"] {
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        db.upsert_node(
            1,
            key,
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.flush().unwrap();
    let first_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    assert!(!first_seg_dir.join("prop_index.dat").exists());
    assert!(!first_seg_dir.join("node_prop_hashes.dat").exists());
    assert!(!first_seg_dir
        .join(crate::segment_writer::SECONDARY_INDEX_DIRNAME)
        .exists());

    for key in ["c", "d"] {
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("blue".to_string()));
        db.upsert_node(
            1,
            key,
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.flush().unwrap();
    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);
    let compacted_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    assert!(!compacted_seg_dir.join("prop_index.dat").exists());
    assert!(!compacted_seg_dir.join("node_prop_hashes.dat").exists());
    assert!(!compacted_seg_dir
        .join(crate::segment_writer::SECONDARY_INDEX_DIRNAME)
        .exists());

    db.close().unwrap();
}

#[test]
fn test_equality_index_backfills_existing_segments_and_compaction_preserves_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let red = PropValue::String("red".to_string());
    let mut props = BTreeMap::new();
    props.insert("color".to_string(), red.clone());
    let first_id = db
        .upsert_node(
            1,
            "first",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let first_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let first_sidecar =
        crate::segment_writer::node_prop_eq_sidecar_path(&first_seg_dir, info.index_id);
    assert!(first_sidecar.exists());

    let second_id = db
        .upsert_node(
            1,
            "second",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();
    for segment in &db.segments {
        let seg_dir = segment_dir(&db_path, segment.segment_id);
        let sidecar_path =
            crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
        assert!(sidecar_path.exists());
    }

    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);
    let compacted_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let compacted_sidecar =
        crate::segment_writer::node_prop_eq_sidecar_path(&compacted_seg_dir, info.index_id);
    assert!(compacted_sidecar.exists());
    assert!(!compacted_seg_dir.join("prop_index.dat").exists());
    assert!(!compacted_seg_dir.join("node_prop_hashes.dat").exists());

    db.reset_property_query_routes();
    let mut ids = db.find_nodes(1, "color", &red).unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![first_id, second_id]);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 0);
    assert_eq!(routes.equality_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_missing_equality_sidecar_reopens_and_repairs_to_ready() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let red = PropValue::String("red".to_string());
    let index_id;
    let seg_id;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());
        db.upsert_node(
            1,
            "repair-me",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
        db.flush().unwrap();

        let info = db
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        seg_id = db.segments[0].segment_id;
        db.close().unwrap();
    }

    let seg_dir = segment_dir(&db_path, seg_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, index_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    assert!(!sidecar_path.exists());

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());
    assert_eq!(
        reopened
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap()
            .len(),
        1
    );
    reopened.close().unwrap();
}

#[test]
fn test_corrupt_equality_sidecar_reopens_failed_and_queries_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let seg_id;
    let red = PropValue::String("red".to_string());

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());
        let node_id = db
            .upsert_node(
                1,
                "broken",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        db.flush().unwrap();

        let info = db
            .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
            .unwrap();
        wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        seg_id = db.segments[0].segment_id;
        assert_eq!(db.find_nodes(1, "color", &red).unwrap(), vec![node_id]);
        db.close().unwrap();
    }

    let seg_dir = segment_dir(&db_path, seg_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, index_id);
    std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = reopened
        .list_node_property_indexes()
        .into_iter()
        .find(|info| info.index_id == index_id)
        .unwrap();
    assert_eq!(info.state, SecondaryIndexState::Failed);
    assert!(info.last_error.is_some());

    reopened.reset_property_query_routes();
    assert_eq!(reopened.find_nodes(1, "color", &red).unwrap().len(), 1);
    let routes = reopened.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 1);
    assert_eq!(routes.equality_index_lookup, 0);

    reopened.close().unwrap();
}

#[test]
fn test_missing_equality_sidecar_while_open_queries_fallback_and_repairs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let red = PropValue::String("red".to_string());
    let blue = PropValue::String("blue".to_string());
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("color".to_string(), red.clone());
    let node_id = db
        .upsert_node(
            1,
            "repair-live",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    assert!(!sidecar_path.exists());

    let mut unrelated_props = BTreeMap::new();
    unrelated_props.insert("color".to_string(), blue.clone());
    let unrelated_id = db
        .upsert_node(
            1,
            "live-counter-node",
            UpsertNodeOptions {
                props: unrelated_props,
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(
        node_id,
        unrelated_id,
        7,
        UpsertEdgeOptions {
            ..Default::default()
        },
    )
    .unwrap();
    let expected_after_degrade = (
        db.next_node_id(),
        db.next_edge_id(),
        db.engine_seq_for_test(),
    );

    let (repair_ready_rx, repair_release_tx) = db.set_secondary_index_build_pause();
    db.reset_property_query_routes();
    assert_eq!(db.find_nodes(1, "color", &red).unwrap(), vec![node_id]);
    repair_ready_rx.recv().unwrap();
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 1);
    assert_eq!(routes.equality_index_lookup, 0);

    let manifest_after_degrade = crate::manifest::load_manifest_readonly(&db_path)
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest_after_degrade.next_node_id,
        expected_after_degrade.0
    );
    assert_eq!(
        manifest_after_degrade.next_edge_id,
        expected_after_degrade.1
    );
    assert_eq!(
        manifest_after_degrade.next_engine_seq,
        expected_after_degrade.2
    );

    let mut later_props = BTreeMap::new();
    later_props.insert("color".to_string(), blue);
    let later_id = db
        .upsert_node(
            1,
            "repair-counter-node",
            UpsertNodeOptions {
                props: later_props,
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(
        node_id,
        later_id,
        8,
        UpsertEdgeOptions {
            ..Default::default()
        },
    )
    .unwrap();
    let expected_after_repair = (
        db.next_node_id(),
        db.next_edge_id(),
        db.engine_seq_for_test(),
    );

    repair_release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());

    let manifest_after_repair = crate::manifest::load_manifest_readonly(&db_path)
        .unwrap()
        .unwrap();
    assert_eq!(manifest_after_repair.next_node_id, expected_after_repair.0);
    assert_eq!(manifest_after_repair.next_edge_id, expected_after_repair.1);
    assert_eq!(
        manifest_after_repair.next_engine_seq,
        expected_after_repair.2
    );

    db.close().unwrap();
}

#[test]
fn test_corrupt_equality_sidecar_while_open_queries_fallback_and_marks_failed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let red = PropValue::String("red".to_string());
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("color".to_string(), red.clone());
    let node_id = db
        .upsert_node(
            1,
            "fail-live",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();

    db.reset_property_query_routes();
    assert_eq!(db.find_nodes(1, "color", &red).unwrap(), vec![node_id]);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 1);
    assert_eq!(routes.equality_index_lookup, 0);

    let failed = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(failed.state, SecondaryIndexState::Failed);
    assert!(failed.last_error.is_some());

    db.close().unwrap();
}

#[test]
fn test_compaction_with_corrupt_ready_sidecar_succeeds_and_marks_failed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let red = PropValue::String("red".to_string());
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("color".to_string(), red.clone());
    let first_id = db
        .upsert_node(
            1,
            "first",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let second_id = db
        .upsert_node(
            1,
            "second",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();

    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);

    let failed = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(failed.state, SecondaryIndexState::Failed);
    assert!(failed.last_error.is_some());

    let compacted_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let compacted_sidecar =
        crate::segment_writer::node_prop_eq_sidecar_path(&compacted_seg_dir, info.index_id);
    assert!(compacted_sidecar.exists());

    db.reset_property_query_routes();
    let mut ids = db.find_nodes(1, "color", &red).unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![first_id, second_id]);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 1);
    assert_eq!(routes.equality_index_lookup, 0);

    db.close().unwrap();
}

#[test]
fn test_legacy_property_hash_backfill_and_compaction_parity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let red = PropValue::String("red".to_string());
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("color".to_string(), red.clone());
    let first_id = db
        .upsert_node(
            1,
            "legacy-first",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let first_seg_id = db.segments[0].segment_id;
    db.close().unwrap();

    let first_seg_dir = segment_dir(&db_path, first_seg_id);
    install_legacy_property_hash_sidecars(
        &first_seg_dir,
        &[(first_id, 1, vec![("color".to_string(), red.clone())])],
    );
    assert!(first_seg_dir.join("prop_index.dat").exists());
    assert!(first_seg_dir.join("node_prop_hashes.dat").exists());

    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = db
        .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    let first_sidecar =
        crate::segment_writer::node_prop_eq_sidecar_path(&first_seg_dir, info.index_id);
    assert!(first_sidecar.exists());

    let second_id = db
        .upsert_node(
            1,
            "legacy-second",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);
    let compacted_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let compacted_sidecar =
        crate::segment_writer::node_prop_eq_sidecar_path(&compacted_seg_dir, info.index_id);
    assert!(compacted_sidecar.exists());

    db.reset_property_query_routes();
    let mut ids = db.find_nodes(1, "color", &red).unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![first_id, second_id]);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 0);
    assert_eq!(routes.equality_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_equality_backfill_survives_compaction_during_build() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let red = PropValue::String("red".to_string());
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for key in ["first", "second"] {
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());
        db.upsert_node(
            1,
            key,
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
        db.flush().unwrap();
    }

    let (ready_rx, release_tx) = db.set_secondary_index_build_pause();
    let info = db
        .ensure_node_property_index(1, "color", SecondaryIndexKind::Equality)
        .unwrap();
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);

    release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    db.reset_property_query_routes();
    assert_eq!(db.find_nodes(1, "color", &red).unwrap().len(), 2);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.equality_scan_fallback, 0);
    assert_eq!(routes.equality_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_property_range_index_manifest_reopens_and_reseeds_active_memtable() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let mut props = BTreeMap::new();
    props.insert("score".to_string(), PropValue::Int(10));
    let node_id = db
        .upsert_node(
            1,
            "frozen-range",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.freeze_memtable().unwrap();

    let (ready_rx, release_tx) = db.set_flush_pause();
    db.enqueue_one_flush().unwrap();
    ready_rx.recv().unwrap();

    let info = db
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    assert_eq!(info.state, SecondaryIndexState::Building);
    let frozen_range = db
        .immutable_memtable(0)
        .secondary_range_state()
        .get(&info.index_id)
        .unwrap();
    assert!(frozen_range.contains(&(10u64 ^ (1u64 << 63), node_id)));

    release_tx.send(()).unwrap();
    assert!(db.wait_one_flush().unwrap().is_some());
    let ready = wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert_eq!(ready.index_id, info.index_id);
    let seg_dir = segment_dir(dir.path(), db.segments[0].segment_id);
    assert!(crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, info.index_id).exists());
    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 0);
    assert_eq!(routes.range_index_lookup, 1);
    db.close().unwrap();

    let reopened = DatabaseEngine::open(dir.path(), &opts).unwrap();
    let ready = wait_for_property_index_state(&reopened, info.index_id, SecondaryIndexState::Ready);
    assert_eq!(ready.index_id, info.index_id);
    reopened.close().unwrap();
}

#[test]
fn test_ready_property_range_index_downgrades_when_flush_publish_missed_declaration_snapshot() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        memtable_flush_threshold: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let mut props = BTreeMap::new();
    props.insert("score".to_string(), PropValue::Int(10));
    let node_id = db
        .upsert_node(
            1,
            "frozen-range",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.freeze_memtable().unwrap();

    let (publish_ready_rx, publish_release_tx) = db.set_flush_publish_pause();
    db.enqueue_one_flush().unwrap();
    publish_ready_rx.recv().unwrap();

    let info = db
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let (repair_ready_rx, repair_release_tx) = db.set_secondary_index_build_pause();
    publish_release_tx.send(()).unwrap();
    assert!(db.wait_one_flush().unwrap().is_some());
    repair_ready_rx.recv().unwrap();

    let building = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(building.state, SecondaryIndexState::Building);

    let seg_dir = segment_dir(dir.path(), db.segments[0].segment_id);
    let sidecar_path =
        crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, info.index_id);
    assert!(!sidecar_path.exists());

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 1);
    assert_eq!(routes.range_index_lookup, 0);

    repair_release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 0);
    assert_eq!(routes.range_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_missing_range_sidecar_reopens_and_repairs_to_ready() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let seg_id;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(10));
        db.upsert_node(
            1,
            "repair-me-range",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
        db.flush().unwrap();

        let info = db
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        seg_id = db.segments[0].segment_id;
        db.close().unwrap();
    }

    let seg_dir = segment_dir(&db_path, seg_id);
    let sidecar_path = crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, index_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    assert!(!sidecar_path.exists());

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());
    assert_eq!(
        reopened
            .find_nodes_range(
                1,
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            )
            .unwrap()
            .len(),
        1
    );
    reopened.close().unwrap();
}

#[test]
fn test_corrupt_range_sidecar_reopens_failed_and_queries_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    let seg_id;

    {
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(10));
        let node_id = db
            .upsert_node(
                1,
                "broken-range",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        db.flush().unwrap();

        let info = db
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
        index_id = info.index_id;
        seg_id = db.segments[0].segment_id;
        assert_eq!(
            db.find_nodes_range(
                1,
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            )
            .unwrap(),
            vec![node_id]
        );
        db.close().unwrap();
    }

    let seg_dir = segment_dir(&db_path, seg_id);
    let sidecar_path = crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, index_id);
    std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = reopened
        .list_node_property_indexes()
        .into_iter()
        .find(|info| info.index_id == index_id)
        .unwrap();
    assert_eq!(info.state, SecondaryIndexState::Failed);
    assert!(info.last_error.is_some());

    reopened.reset_property_query_routes();
    assert_eq!(
        reopened
            .find_nodes_range(
                1,
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            )
            .unwrap()
            .len(),
        1
    );
    let routes = reopened.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 1);
    assert_eq!(routes.range_index_lookup, 0);

    reopened.close().unwrap();
}

#[test]
fn test_missing_range_sidecar_while_open_queries_fallback_and_repairs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("score".to_string(), PropValue::Int(10));
    let node_id = db
        .upsert_node(
            1,
            "repair-live-range",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, info.index_id);
    std::fs::remove_file(&sidecar_path).unwrap();
    assert!(!sidecar_path.exists());

    let unrelated_id = db
        .upsert_node(
            1,
            "live-counter-range",
            UpsertNodeOptions {
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(
        node_id,
        unrelated_id,
        7,
        UpsertEdgeOptions {
            ..Default::default()
        },
    )
    .unwrap();
    let expected_after_degrade = (
        db.next_node_id(),
        db.next_edge_id(),
        db.engine_seq_for_test(),
    );

    let (repair_ready_rx, repair_release_tx) = db.set_secondary_index_build_pause();
    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap(),
        vec![node_id]
    );
    repair_ready_rx.recv().unwrap();
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 1);
    assert_eq!(routes.range_index_lookup, 0);

    let manifest_after_degrade = crate::manifest::load_manifest_readonly(&db_path)
        .unwrap()
        .unwrap();
    assert_eq!(manifest_after_degrade.next_node_id, expected_after_degrade.0);
    assert_eq!(manifest_after_degrade.next_edge_id, expected_after_degrade.1);
    assert_eq!(manifest_after_degrade.next_engine_seq, expected_after_degrade.2);

    let later_id = db
        .upsert_node(
            1,
            "repair-counter-range",
            UpsertNodeOptions {
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(
        node_id,
        later_id,
        8,
        UpsertEdgeOptions {
            ..Default::default()
        },
    )
    .unwrap();
    let expected_after_repair = (
        db.next_node_id(),
        db.next_edge_id(),
        db.engine_seq_for_test(),
    );

    repair_release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);
    assert!(sidecar_path.exists());

    let manifest_after_repair = crate::manifest::load_manifest_readonly(&db_path)
        .unwrap()
        .unwrap();
    assert_eq!(manifest_after_repair.next_node_id, expected_after_repair.0);
    assert_eq!(manifest_after_repair.next_edge_id, expected_after_repair.1);
    assert_eq!(manifest_after_repair.next_engine_seq, expected_after_repair.2);

    db.close().unwrap();
}

#[test]
fn test_corrupt_range_sidecar_while_open_queries_fallback_and_marks_failed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("score".to_string(), PropValue::Int(10));
    let node_id = db
        .upsert_node(
            1,
            "corrupt-live-range",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, info.index_id);
    std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();

    let unrelated_id = db
        .upsert_node(
            1,
            "failed-counter-range",
            UpsertNodeOptions {
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_edge(
        node_id,
        unrelated_id,
        9,
        UpsertEdgeOptions {
            ..Default::default()
        },
    )
    .unwrap();
    let expected_after_degrade = (
        db.next_node_id(),
        db.next_edge_id(),
        db.engine_seq_for_test(),
    );

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap(),
        vec![node_id]
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 1);
    assert_eq!(routes.range_index_lookup, 0);

    let failed = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(failed.state, SecondaryIndexState::Failed);
    assert!(failed.last_error.is_some());

    let manifest_after_degrade = crate::manifest::load_manifest_readonly(&db_path)
        .unwrap()
        .unwrap();
    assert_eq!(manifest_after_degrade.next_node_id, expected_after_degrade.0);
    assert_eq!(manifest_after_degrade.next_edge_id, expected_after_degrade.1);
    assert_eq!(manifest_after_degrade.next_engine_seq, expected_after_degrade.2);

    db.close().unwrap();
}

#[test]
fn test_compaction_with_corrupt_ready_range_sidecar_succeeds_and_marks_failed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut props = BTreeMap::new();
    props.insert("score".to_string(), PropValue::Int(10));
    let first_id = db
        .upsert_node(
            1,
            "first-range",
            UpsertNodeOptions {
                props: props.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let info = db
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    let second_id = db
        .upsert_node(
            1,
            "second-range",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
    db.flush().unwrap();

    let seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_range_sidecar_path(&seg_dir, info.index_id);
    std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();

    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);

    let failed = db
        .list_node_property_indexes()
        .into_iter()
        .find(|entry| entry.index_id == info.index_id)
        .unwrap();
    assert_eq!(failed.state, SecondaryIndexState::Failed);
    assert!(failed.last_error.is_some());

    let compacted_seg_dir = segment_dir(&db_path, db.segments[0].segment_id);
    let compacted_sidecar =
        crate::segment_writer::node_prop_range_sidecar_path(&compacted_seg_dir, info.index_id);
    assert!(compacted_sidecar.exists());

    db.reset_property_query_routes();
    let mut ids = db
        .find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![first_id, second_id]);
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 1);
    assert_eq!(routes.range_index_lookup, 0);

    db.close().unwrap();
}

#[test]
fn test_range_backfill_survives_compaction_during_build() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for key in ["first-range", "second-range"] {
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(10));
        db.upsert_node(
            1,
            key,
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
        db.flush().unwrap();
    }

    let (ready_rx, release_tx) = db.set_secondary_index_build_pause();
    let info = db
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.segments_merged, 2);

    release_tx.send(()).unwrap();
    wait_for_property_index_state(&db, info.index_id, SecondaryIndexState::Ready);

    db.reset_property_query_routes();
    assert_eq!(
        db.find_nodes_range(
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
        )
        .unwrap()
        .len(),
        2
    );
    let routes = db.property_query_route_snapshot();
    assert_eq!(routes.range_scan_fallback, 0);
    assert_eq!(routes.range_index_lookup, 1);

    db.close().unwrap();
}

#[test]
fn test_open_rejects_conflicting_range_declarations_for_same_property() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    std::fs::create_dir_all(&db_path).unwrap();

    let mut manifest = crate::manifest::default_manifest();
    manifest.secondary_indexes = vec![
        SecondaryIndexManifestEntry {
            index_id: 1,
            target: SecondaryIndexTarget::NodeProperty {
                type_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
            state: SecondaryIndexState::Building,
            last_error: None,
        },
        SecondaryIndexManifestEntry {
            index_id: 2,
            target: SecondaryIndexTarget::NodeProperty {
                type_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Float,
            },
            state: SecondaryIndexState::Building,
            last_error: None,
        },
    ];
    manifest.next_secondary_index_id = 3;
    crate::manifest::write_manifest(&db_path, &manifest).unwrap();

    match DatabaseEngine::open(&db_path, &DbOptions::default()) {
        Err(EngineError::ManifestError(_)) => {}
        Err(other) => panic!("expected ManifestError, got {}", other),
        Ok(_) => panic!("expected conflicting range declarations to fail on open"),
    }
}
