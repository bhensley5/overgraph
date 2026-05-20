use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Clone, Copy)]
enum WalTailDamage {
    MissingCommit,
    CorruptCommit,
}

fn wal_frame_bounds(path: &Path) -> Vec<(u64, u64)> {
    let mut file = std::fs::File::open(path).unwrap();
    let mut pos = 8u64;
    file.seek(SeekFrom::Start(pos)).unwrap();
    let mut frames = Vec::new();
    loop {
        let mut len_buf = [0u8; 4];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => panic!("failed to read WAL frame length: {err}"),
        }
        let payload_len = u32::from_le_bytes(len_buf) as u64;
        let frame_start = pos;
        let frame_end = frame_start + 4 + 4 + payload_len;
        frames.push((frame_start, frame_end));
        pos = frame_end;
        file.seek(SeekFrom::Start(pos)).unwrap();
    }
    frames
}

fn truncate_wal_after_frame(db_path: &Path, frame_count: usize) {
    let path = wal_generation_path(db_path, 0);
    let frames = wal_frame_bounds(&path);
    let new_len = if frame_count == 0 {
        8
    } else {
        frames
            .get(frame_count - 1)
            .map(|(_, end)| *end)
            .expect("requested WAL frame must exist")
    };
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(new_len)
        .unwrap();
}

fn corrupt_last_wal_frame_crc(db_path: &Path) {
    let path = wal_generation_path(db_path, 0);
    let frames = wal_frame_bounds(&path);
    let (last_start, _) = *frames.last().expect("WAL must contain a frame");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(last_start + 4)).unwrap();
    let mut crc_buf = [0u8; 4];
    file.read_exact(&mut crc_buf).unwrap();
    let crc = u32::from_le_bytes(crc_buf) ^ 0xFFFF_FFFF;
    file.seek(SeekFrom::Start(last_start + 4)).unwrap();
    file.write_all(&crc.to_le_bytes()).unwrap();
    file.sync_all().unwrap();
}

fn truncate_last_wal_byte(db_path: &Path) {
    let path = wal_generation_path(db_path, 0);
    let len = std::fs::metadata(&path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(len - 1)
        .unwrap();
}

fn append_raw_wal_frame(db_path: &Path, seq: u64, walop_bytes: &[u8]) {
    let path = wal_generation_path(db_path, 0);
    let seq_bytes = seq.to_le_bytes();
    let total_payload = 8 + walop_bytes.len();
    let len = total_payload as u32;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&seq_bytes);
    hasher.update(walop_bytes);
    let crc = hasher.finalize();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(&len.to_le_bytes()).unwrap();
    file.write_all(&crc.to_le_bytes()).unwrap();
    file.write_all(&seq_bytes).unwrap();
    file.write_all(walop_bytes).unwrap();
    file.sync_all().unwrap();
}

fn damage_last_atomic_commit(db_path: &Path, damage: WalTailDamage) {
    match damage {
        WalTailDamage::MissingCommit => {
            let frame_count = wal_frame_bounds(&wal_generation_path(db_path, 0)).len();
            truncate_wal_after_frame(db_path, frame_count - 1);
        }
        WalTailDamage::CorruptCommit => corrupt_last_wal_frame_crc(db_path),
    }
}

fn write_manifest_with_catalog(
    db_path: &Path,
    node_labels: &[(&str, u32)],
    edge_labels: &[(&str, u32)],
) {
    std::fs::create_dir_all(db_path).unwrap();
    let mut manifest = default_manifest();
    for &(label, label_id) in node_labels {
        manifest.node_label_tokens.insert(label.to_string(), label_id);
        manifest.next_node_label_id = manifest.next_node_label_id.max(label_id + 1);
    }
    for &(label, label_id) in edge_labels {
        manifest.edge_label_tokens.insert(label.to_string(), label_id);
        manifest.next_edge_label_id = manifest.next_edge_label_id.max(label_id + 1);
    }
    write_manifest(db_path, &manifest).unwrap();
}

fn write_atomic_ops(db_path: &Path, ops: &[(u64, WalOp)]) {
    let mut writer = WalWriter::open_generation(db_path, 0).unwrap();
    writer.append_batch(ops).unwrap();
    writer.sync().unwrap();
}

fn append_atomic_ops(writer: &mut WalWriter, ops: &[(u64, WalOp)]) {
    writer.append_batch(ops).unwrap();
}

fn node_op(id: u64, label_id: u32, key: &str) -> WalOp {
    WalOp::UpsertNode(NodeRecord {
        id,
        label_ids: NodeLabelSet::single(label_id).unwrap(),
        key: key.to_string(),
        props: BTreeMap::new(),
        created_at: id as i64,
        updated_at: id as i64,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: 0,
    })
}

fn edge_op(id: u64, from: u64, to: u64, label_id: u32) -> WalOp {
    WalOp::UpsertEdge(EdgeRecord {
        id,
        from,
        to,
        label_id,
        props: BTreeMap::new(),
        created_at: id as i64,
        updated_at: id as i64,
        weight: 1.0,
        valid_from: 0,
        valid_to: i64::MAX,
        last_write_seq: 0,
    })
}

fn node_input(label: &str, key: &str) -> NodeInput {
    NodeInput {
        labels: vec![label.to_string()],
        key: key.to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn edge_input(from: u64, to: u64, label: &str) -> EdgeInput {
    EdgeInput {
        from,
        to,
        label: label.to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }
}

fn assert_single_atomic_batch(records: &[(u64, WalOp)], first_seq: u64, op_count: u32) {
    assert_eq!(records.len(), op_count as usize + 2);
    assert!(matches!(
        records.first().map(|(_, op)| op),
        Some(WalOp::BeginAtomicBatch {
            first_seq: observed_first_seq,
            op_count: observed_op_count,
        }) if *observed_first_seq == first_seq && *observed_op_count == op_count
    ));
    assert!(matches!(
        records.last().map(|(_, op)| op),
        Some(WalOp::CommitAtomicBatch {
            first_seq: observed_first_seq,
            op_count: observed_op_count,
        }) if *observed_first_seq == first_seq && *observed_op_count == op_count
    ));
}

fn assert_last_atomic_batch(records: &[(u64, WalOp)], op_count: u32) {
    let tail_len = op_count as usize + 2;
    assert!(records.len() >= tail_len, "records: {records:?}");
    let tail = &records[records.len() - tail_len..];
    assert!(matches!(
        tail.first().map(|(_, op)| op),
        Some(WalOp::BeginAtomicBatch {
            op_count: observed_op_count,
            ..
        }) if *observed_op_count == op_count
    ));
    assert!(matches!(
        tail.last().map(|(_, op)| op),
        Some(WalOp::CommitAtomicBatch {
            op_count: observed_op_count,
            ..
        }) if *observed_op_count == op_count
    ));
}

#[test]
fn test_atomic_batch_append_emits_begin_ops_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[("Person", 1)], &[]);

    write_atomic_ops(
        &db_path,
        &[(1, node_op(1, 1, "a")), (2, node_op(2, 1, "b"))],
    );

    let records = WalReader::read_generation(&db_path, 0).unwrap();
    assert_single_atomic_batch(&records, 1, 2);
    assert!(matches!(&records[1].1, WalOp::UpsertNode(node) if node.id == 1));
    assert!(matches!(&records[2].1, WalOp::UpsertNode(node) if node.id == 2));
}

#[test]
fn test_representative_public_multi_op_apis_emit_atomic_markers() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    engine.ensure_node_label("Person").unwrap();
    engine.ensure_edge_label("KNOWS").unwrap();

    let ids = engine
        .batch_upsert_nodes(vec![node_input("Person", "batch-a"), node_input("Person", "batch-b")])
        .unwrap();
    assert_last_atomic_batch(&WalReader::read_generation(&db_path, 0).unwrap(), 2);

    engine
        .batch_upsert_edges(vec![
            edge_input(ids[0], ids[1], "KNOWS"),
            edge_input(ids[1], ids[0], "KNOWS"),
        ])
        .unwrap();
    assert_last_atomic_batch(&WalReader::read_generation(&db_path, 0).unwrap(), 2);

    engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![node_input("Person", "patch-a"), node_input("Person", "patch-b")],
            ..Default::default()
        })
        .unwrap();
    assert_last_atomic_batch(&WalReader::read_generation(&db_path, 0).unwrap(), 2);

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_node("Person", "txn-a", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_node("Person", "txn-b", UpsertNodeOptions::default())
        .unwrap();
    txn.commit().unwrap();
    assert_last_atomic_batch(&WalReader::read_generation(&db_path, 0).unwrap(), 2);

    let cascade_a = engine
        .upsert_node("Person", "cascade-a", UpsertNodeOptions::default())
        .unwrap();
    let cascade_b = engine
        .upsert_node("Person", "cascade-b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(cascade_a, cascade_b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.delete_node(cascade_a).unwrap();
    assert_last_atomic_batch(&WalReader::read_generation(&db_path, 0).unwrap(), 2);

    let keep = engine
        .upsert_node("Person", "prune-keep", UpsertNodeOptions::default())
        .unwrap();
    let prune = engine
        .upsert_node(
            "Person",
            "prune-delete",
            UpsertNodeOptions {
                weight: 0.1,
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(keep, prune, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            label: Some("Person".to_string()),
        })
        .unwrap();
    assert_last_atomic_batch(&WalReader::read_generation(&db_path, 0).unwrap(), 2);
    engine.close().unwrap();
}

#[test]
fn test_existing_label_upsert_node_emits_no_atomic_markers() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    engine.ensure_node_label("Person").unwrap();
    let id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();

    let records = WalReader::read_generation(&db_path, 0).unwrap();
    assert_no_atomic_batch_markers(&records);
    assert!(matches!(records.last().map(|(_, op)| op), Some(WalOp::UpsertNode(node)) if node.id == id));
    engine.close().unwrap();
}

#[test]
fn test_existing_label_upsert_edge_emits_no_atomic_markers() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    engine.ensure_node_label("Person").unwrap();
    engine.ensure_edge_label("KNOWS").unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let records = WalReader::read_generation(&db_path, 0).unwrap();
    assert_no_atomic_batch_markers(&records);
    assert!(matches!(records.last().map(|(_, op)| op), Some(WalOp::UpsertEdge(edge)) if edge.id == edge_id));
    engine.close().unwrap();
}

#[test]
fn test_single_op_ensure_delete_and_invalidate_paths_emit_no_atomic_markers() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    engine.ensure_node_label("Person").unwrap();
    engine.ensure_edge_label("KNOWS").unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let orphan = engine
        .upsert_node("Person", "orphan", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.invalidate_edge(edge_id, 123).unwrap();
    engine.delete_edge(edge_id).unwrap();
    engine.delete_node(orphan).unwrap();

    let records = WalReader::read_generation(&db_path, 0).unwrap();
    assert_no_atomic_batch_markers(&records);
    assert!(records
        .iter()
        .any(|(_, op)| matches!(op, WalOp::UpsertEdge(edge) if edge.id == edge_id && edge.valid_to == 123)));
    assert!(records
        .iter()
        .any(|(_, op)| matches!(op, WalOp::DeleteEdge { id, .. } if *id == edge_id)));
    assert!(records
        .iter()
        .any(|(_, op)| matches!(op, WalOp::DeleteNode { id, .. } if *id == orphan)));
    engine.close().unwrap();
}

#[test]
fn test_explicit_label_ensures_replay_as_standalone_token_mutations() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureEdgeLabel {
                label: "KNOWS".to_string(),
                label_id: 1,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let records = WalReader::read_generation(&db_path, 0).unwrap();
    assert_no_atomic_batch_markers(&records);
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(engine.get_edge_label_id("KNOWS").unwrap(), Some(1));
    assert!(engine.get_node(1).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_first_use_upsert_node_emits_atomic_batch_and_replays_fully() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("api_wal_atomic");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let records = WalReader::read_generation(&db_path, 0).unwrap();
    assert_single_atomic_batch(&records, 1, 2);
    assert!(matches!(&records[1].1, WalOp::EnsureNodeLabel { label, .. } if label == "Person"));
    assert!(matches!(&records[2].1, WalOp::UpsertNode(node) if node.id == id));
    engine.close().unwrap();

    let replay_path = dir.path().join("manual_node_replay");
    write_manifest_with_catalog(&replay_path, &[], &[]);
    write_atomic_ops(
        &replay_path,
        &[
            (
                1,
                WalOp::EnsureNodeLabel {
                    label: "Person".to_string(),
                    label_id: 1,
                },
            ),
            (2, node_op(1, 1, "alice")),
        ],
    );
    let replayed = DatabaseEngine::open(&replay_path, &DbOptions::default()).unwrap();
    assert_eq!(replayed.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(
        replayed.get_node_by_key("Person", "alice").unwrap().unwrap().id,
        1
    );
    replayed.close().unwrap();
}

#[test]
fn test_first_use_upsert_edge_emits_atomic_batch_and_replays_fully() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("api_wal_atomic");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    engine.ensure_node_label("Person").unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let records = WalReader::read_generation(&db_path, 0).unwrap();
    let tail = &records[records.len() - 4..];
    assert_single_atomic_batch(tail, 4, 2);
    assert!(matches!(&tail[1].1, WalOp::EnsureEdgeLabel { label, .. } if label == "KNOWS"));
    assert!(matches!(&tail[2].1, WalOp::UpsertEdge(edge) if edge.id == edge_id));
    engine.close().unwrap();

    let replay_path = dir.path().join("manual_edge_replay");
    write_manifest_with_catalog(&replay_path, &[("Person", 1)], &[]);
    let mut writer = WalWriter::open_generation(&replay_path, 0).unwrap();
    writer.append(&node_op(1, 1, "a"), 1).unwrap();
    writer.append(&node_op(2, 1, "b"), 2).unwrap();
    append_atomic_ops(
        &mut writer,
        &[
            (
                3,
                WalOp::EnsureEdgeLabel {
                    label: "KNOWS".to_string(),
                    label_id: 1,
                },
            ),
            (4, edge_op(10, 1, 2, 1)),
        ],
    );
    writer.sync().unwrap();
    drop(writer);
    let replayed = DatabaseEngine::open(&replay_path, &DbOptions::default()).unwrap();
    assert_eq!(replayed.get_edge_label_id("KNOWS").unwrap(), Some(1));
    assert_eq!(replayed.get_edge(10).unwrap().unwrap().label, "KNOWS");
    replayed.close().unwrap();
}

fn assert_first_use_node_tail_discard(damage: WalTailDamage) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    write_atomic_ops(
        &db_path,
        &[
            (
                1,
                WalOp::EnsureNodeLabel {
                    label: "TailNode".to_string(),
                    label_id: 1,
                },
            ),
            (2, node_op(1, 1, "tail")),
        ],
    );
    damage_last_atomic_commit(&db_path, damage);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("TailNode").unwrap(), None);
    assert!(engine.get_node(1).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_missing_commit_discards_first_use_node_batch() {
    assert_first_use_node_tail_discard(WalTailDamage::MissingCommit);
}

#[test]
fn test_corrupt_commit_discards_first_use_node_batch() {
    assert_first_use_node_tail_discard(WalTailDamage::CorruptCommit);
}

fn assert_first_use_edge_tail_discard(damage: WalTailDamage) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[("Person", 1)], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer.append(&node_op(1, 1, "a"), 1).unwrap();
    writer.append(&node_op(2, 1, "b"), 2).unwrap();
    append_atomic_ops(
        &mut writer,
        &[
            (
                3,
                WalOp::EnsureEdgeLabel {
                    label: "TAIL_EDGE".to_string(),
                    label_id: 1,
                },
            ),
            (4, edge_op(10, 1, 2, 1)),
        ],
    );
    writer.sync().unwrap();
    drop(writer);
    damage_last_atomic_commit(&db_path, damage);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_edge_label_id("TAIL_EDGE").unwrap(), None);
    assert!(engine.get_edge(10).unwrap().is_none());
    assert!(engine.get_node(1).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_missing_commit_discards_first_use_edge_batch() {
    assert_first_use_edge_tail_discard(WalTailDamage::MissingCommit);
}

#[test]
fn test_corrupt_commit_discards_first_use_edge_batch() {
    assert_first_use_edge_tail_discard(WalTailDamage::CorruptCommit);
}

#[test]
fn test_batch_upsert_nodes_complete_replay_and_torn_tail_discard() {
    let dir = TempDir::new().unwrap();
    let complete_path = dir.path().join("batch_nodes_complete");
    write_manifest_with_catalog(&complete_path, &[("Person", 1)], &[]);
    write_atomic_ops(
        &complete_path,
        &[(1, node_op(1, 1, "a")), (2, node_op(2, 1, "b"))],
    );
    let engine = DatabaseEngine::open(&complete_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_nodes_by_labels("Person").unwrap().len(), 2);
    engine.close().unwrap();

    let torn_path = dir.path().join("batch_nodes_torn");
    write_manifest_with_catalog(&torn_path, &[("Person", 1)], &[]);
    write_atomic_ops(
        &torn_path,
        &[(1, node_op(1, 1, "a")), (2, node_op(2, 1, "b"))],
    );
    damage_last_atomic_commit(&torn_path, WalTailDamage::MissingCommit);
    let engine = DatabaseEngine::open(&torn_path, &DbOptions::default()).unwrap();
    assert!(engine.get_nodes_by_labels("Person").unwrap().is_empty());
    engine.close().unwrap();
}

#[test]
fn test_batch_upsert_edges_complete_replay_and_torn_tail_discard() {
    let dir = TempDir::new().unwrap();
    let complete_path = dir.path().join("batch_edges_complete");
    write_manifest_with_catalog(&complete_path, &[], &[("KNOWS", 1)]);
    write_atomic_ops(
        &complete_path,
        &[(1, edge_op(10, 1, 2, 1)), (2, edge_op(11, 2, 3, 1))],
    );
    let engine = DatabaseEngine::open(&complete_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_edges_by_label("KNOWS").unwrap().len(), 2);
    engine.close().unwrap();

    let torn_path = dir.path().join("batch_edges_torn");
    write_manifest_with_catalog(&torn_path, &[], &[("KNOWS", 1)]);
    write_atomic_ops(
        &torn_path,
        &[(1, edge_op(10, 1, 2, 1)), (2, edge_op(11, 2, 3, 1))],
    );
    damage_last_atomic_commit(&torn_path, WalTailDamage::CorruptCommit);
    let engine = DatabaseEngine::open(&torn_path, &DbOptions::default()).unwrap();
    assert!(engine.get_edges_by_label("KNOWS").unwrap().is_empty());
    engine.close().unwrap();
}

#[test]
fn test_atomic_batch_replay_tracks_intra_batch_edge_state_for_degree_deltas() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batch_edge_degree");
    write_manifest_with_catalog(&db_path, &[("Person", 1)], &[("KNOWS", 1)]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer.append(&node_op(1, 1, "a"), 1).unwrap();
    writer.append(&node_op(2, 1, "b"), 2).unwrap();
    writer.append(&node_op(3, 1, "c"), 3).unwrap();
    append_atomic_ops(
        &mut writer,
        &[(4, edge_op(10, 1, 2, 1)), (5, edge_op(10, 1, 3, 1))],
    );
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let edge = engine.get_edge(10).unwrap().expect("edge should replay");
    assert_eq!(edge.from, 1);
    assert_eq!(edge.to, 3);
    assert_eq!(engine.degree(1, &DegreeOptions::default()).unwrap(), 1);
    assert_eq!(
        engine
            .degree(
                2,
                &DegreeOptions {
                    direction: Direction::Incoming,
                    ..Default::default()
                },
            )
            .unwrap(),
        0
    );
    assert_eq!(
        engine
            .degree(
                3,
                &DegreeOptions {
                    direction: Direction::Incoming,
                    ..Default::default()
                },
            )
            .unwrap(),
        1
    );
    engine.close().unwrap();
}

#[test]
fn test_graph_patch_complete_replay_and_torn_tail_discard() {
    let dir = TempDir::new().unwrap();
    let complete_path = dir.path().join("graph_patch_complete");
    write_manifest_with_catalog(&complete_path, &[("Person", 1)], &[("KNOWS", 1)]);
    write_atomic_ops(
        &complete_path,
        &[
            (1, node_op(1, 1, "a")),
            (2, node_op(2, 1, "b")),
            (3, edge_op(10, 1, 2, 1)),
        ],
    );
    let engine = DatabaseEngine::open(&complete_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_nodes_by_labels("Person").unwrap().len(), 2);
    assert!(engine.get_edge(10).unwrap().is_some());
    engine.close().unwrap();

    let torn_path = dir.path().join("graph_patch_torn");
    write_manifest_with_catalog(&torn_path, &[("Person", 1)], &[("KNOWS", 1)]);
    write_atomic_ops(
        &torn_path,
        &[
            (1, node_op(1, 1, "a")),
            (2, node_op(2, 1, "b")),
            (3, edge_op(10, 1, 2, 1)),
        ],
    );
    damage_last_atomic_commit(&torn_path, WalTailDamage::MissingCommit);
    let engine = DatabaseEngine::open(&torn_path, &DbOptions::default()).unwrap();
    assert!(engine.get_nodes_by_labels("Person").unwrap().is_empty());
    assert!(engine.get_edge(10).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_transaction_commit_complete_replay_and_torn_tail_discard() {
    let dir = TempDir::new().unwrap();
    let complete_path = dir.path().join("txn_complete");
    write_manifest_with_catalog(&complete_path, &[("TxnNode", 1)], &[("TXN_EDGE", 1)]);
    write_atomic_ops(
        &complete_path,
        &[
            (1, node_op(1, 1, "txn-a")),
            (2, node_op(2, 1, "txn-b")),
            (3, edge_op(10, 1, 2, 1)),
        ],
    );
    let engine = DatabaseEngine::open(&complete_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_nodes_by_labels("TxnNode").unwrap().len(), 2);
    assert!(engine.get_edge(10).unwrap().is_some());
    engine.close().unwrap();

    let torn_path = dir.path().join("txn_torn");
    write_manifest_with_catalog(&torn_path, &[("TxnNode", 1)], &[("TXN_EDGE", 1)]);
    write_atomic_ops(
        &torn_path,
        &[
            (1, node_op(1, 1, "txn-a")),
            (2, node_op(2, 1, "txn-b")),
            (3, edge_op(10, 1, 2, 1)),
        ],
    );
    damage_last_atomic_commit(&torn_path, WalTailDamage::CorruptCommit);
    let engine = DatabaseEngine::open(&torn_path, &DbOptions::default()).unwrap();
    assert!(engine.get_nodes_by_labels("TxnNode").unwrap().is_empty());
    assert!(engine.get_edge(10).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_cascade_delete_node_complete_replay_and_torn_tail_discard() {
    let dir = TempDir::new().unwrap();
    let complete_path = dir.path().join("cascade_complete");
    write_manifest_with_catalog(&complete_path, &[("Person", 1)], &[("KNOWS", 1)]);
    let mut writer = WalWriter::open_generation(&complete_path, 0).unwrap();
    writer.append(&node_op(1, 1, "a"), 1).unwrap();
    writer.append(&node_op(2, 1, "b"), 2).unwrap();
    writer.append(&edge_op(10, 1, 2, 1), 3).unwrap();
    append_atomic_ops(
        &mut writer,
        &[
            (
                4,
                WalOp::DeleteEdge {
                    id: 10,
                    deleted_at: 4,
                },
            ),
            (
                5,
                WalOp::DeleteNode {
                    id: 1,
                    deleted_at: 5,
                },
            ),
        ],
    );
    writer.sync().unwrap();
    drop(writer);
    let engine = DatabaseEngine::open(&complete_path, &DbOptions::default()).unwrap();
    assert!(engine.get_node(1).unwrap().is_none());
    assert!(engine.get_edge(10).unwrap().is_none());
    assert!(engine.get_node(2).unwrap().is_some());
    engine.close().unwrap();

    let torn_path = dir.path().join("cascade_torn");
    write_manifest_with_catalog(&torn_path, &[("Person", 1)], &[("KNOWS", 1)]);
    let mut writer = WalWriter::open_generation(&torn_path, 0).unwrap();
    writer.append(&node_op(1, 1, "a"), 1).unwrap();
    writer.append(&node_op(2, 1, "b"), 2).unwrap();
    writer.append(&edge_op(10, 1, 2, 1), 3).unwrap();
    append_atomic_ops(
        &mut writer,
        &[
            (
                4,
                WalOp::DeleteEdge {
                    id: 10,
                    deleted_at: 4,
                },
            ),
            (
                5,
                WalOp::DeleteNode {
                    id: 1,
                    deleted_at: 5,
                },
            ),
        ],
    );
    writer.sync().unwrap();
    drop(writer);
    damage_last_atomic_commit(&torn_path, WalTailDamage::MissingCommit);
    let engine = DatabaseEngine::open(&torn_path, &DbOptions::default()).unwrap();
    assert!(engine.get_node(1).unwrap().is_some());
    assert!(engine.get_edge(10).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_prune_complete_replay_and_torn_tail_discard() {
    let dir = TempDir::new().unwrap();
    let complete_path = dir.path().join("prune_complete");
    write_manifest_with_catalog(&complete_path, &[("Person", 1)], &[("KNOWS", 1)]);
    let mut writer = WalWriter::open_generation(&complete_path, 0).unwrap();
    writer.append(&node_op(1, 1, "keep"), 1).unwrap();
    writer.append(&node_op(2, 1, "prune"), 2).unwrap();
    writer.append(&edge_op(10, 1, 2, 1), 3).unwrap();
    append_atomic_ops(
        &mut writer,
        &[
            (
                4,
                WalOp::DeleteEdge {
                    id: 10,
                    deleted_at: 4,
                },
            ),
            (
                5,
                WalOp::DeleteNode {
                    id: 2,
                    deleted_at: 5,
                },
            ),
        ],
    );
    writer.sync().unwrap();
    drop(writer);
    let engine = DatabaseEngine::open(&complete_path, &DbOptions::default()).unwrap();
    assert!(engine.get_node(1).unwrap().is_some());
    assert!(engine.get_node(2).unwrap().is_none());
    assert!(engine.get_edge(10).unwrap().is_none());
    engine.close().unwrap();

    let torn_path = dir.path().join("prune_torn");
    write_manifest_with_catalog(&torn_path, &[("Person", 1)], &[("KNOWS", 1)]);
    let mut writer = WalWriter::open_generation(&torn_path, 0).unwrap();
    writer.append(&node_op(1, 1, "keep"), 1).unwrap();
    writer.append(&node_op(2, 1, "prune"), 2).unwrap();
    writer.append(&edge_op(10, 1, 2, 1), 3).unwrap();
    append_atomic_ops(
        &mut writer,
        &[
            (
                4,
                WalOp::DeleteEdge {
                    id: 10,
                    deleted_at: 4,
                },
            ),
            (
                5,
                WalOp::DeleteNode {
                    id: 2,
                    deleted_at: 5,
                },
            ),
        ],
    );
    writer.sync().unwrap();
    drop(writer);
    damage_last_atomic_commit(&torn_path, WalTailDamage::CorruptCommit);
    let engine = DatabaseEngine::open(&torn_path, &DbOptions::default()).unwrap();
    assert!(engine.get_node(2).unwrap().is_some());
    assert!(engine.get_edge(10).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_committed_batch_before_corrupt_tail_batch_is_preserved() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[("Person", 1)], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    append_atomic_ops(
        &mut writer,
        &[(1, node_op(1, 1, "kept-a")), (2, node_op(2, 1, "kept-b"))],
    );
    append_atomic_ops(
        &mut writer,
        &[(3, node_op(3, 1, "lost-a")), (4, node_op(4, 1, "lost-b"))],
    );
    writer.sync().unwrap();
    drop(writer);
    damage_last_atomic_commit(&db_path, WalTailDamage::CorruptCommit);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(engine.get_node(1).unwrap().is_some());
    assert!(engine.get_node(2).unwrap().is_some());
    assert!(engine.get_node(3).unwrap().is_none());
    assert!(engine.get_node(4).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_mismatched_commit_discards_open_batch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::BeginAtomicBatch {
                first_seq: 1,
                op_count: 2,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Mismatch".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer.append(&node_op(1, 1, "lost"), 2).unwrap();
    writer
        .append(
            &WalOp::CommitAtomicBatch {
                first_seq: 99,
                op_count: 2,
            },
            1,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Mismatch").unwrap(), None);
    assert!(engine.get_node(1).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_commit_without_begin_preserves_earlier_standalone_records() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer.append(&node_op(1, 1, "kept"), 2).unwrap();
    writer
        .append(
            &WalOp::CommitAtomicBatch {
                first_seq: 3,
                op_count: 2,
            },
            3,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Person").unwrap(), Some(1));
    assert!(engine.get_node(1).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_corrupt_marker_frame_keeps_prior_standalone_records_and_discards_tail() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer.append(&node_op(1, 1, "kept"), 2).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut corrupt_marker = vec![OpTag::BeginAtomicBatch as u8];
    corrupt_marker.extend_from_slice(&3u64.to_le_bytes());
    append_raw_wal_frame(&db_path, 3, &corrupt_marker);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Person").unwrap(), Some(1));
    assert!(engine.get_node(1).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_truncated_commit_marker_discards_open_batch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    write_atomic_ops(
        &db_path,
        &[
            (
                1,
                WalOp::EnsureNodeLabel {
                    label: "Truncated".to_string(),
                    label_id: 1,
                },
            ),
            (2, node_op(1, 1, "lost")),
        ],
    );
    truncate_last_wal_byte(&db_path);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Truncated").unwrap(), None);
    assert!(engine.get_node(1).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_nested_begin_discards_open_batch_without_applying_partial_ops() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Base".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::BeginAtomicBatch {
                first_seq: 2,
                op_count: 2,
            },
            2,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Lost".to_string(),
                label_id: 2,
            },
            2,
        )
        .unwrap();
    writer
        .append(
            &WalOp::BeginAtomicBatch {
                first_seq: 3,
                op_count: 2,
            },
            3,
        )
        .unwrap();
    writer.append(&node_op(2, 2, "lost"), 3).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Base").unwrap(), Some(1));
    assert_eq!(engine.get_node_label_id("Lost").unwrap(), None);
    assert!(engine.get_node(2).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_huge_atomic_op_count_begin_discards_tail_without_preallocating() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Base".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::BeginAtomicBatch {
                first_seq: 2,
                op_count: u32::MAX,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Base").unwrap(), Some(1));
    engine.close().unwrap();
}

#[test]
fn test_malformed_enclosed_record_discards_open_batch_without_failing_open() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Base".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::BeginAtomicBatch {
                first_seq: 2,
                op_count: 2,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);
    append_raw_wal_frame(&db_path, 2, &[OpTag::EnsureNodeLabel as u8]);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Base").unwrap(), Some(1));
    engine.close().unwrap();
}

#[test]
fn test_recovered_incomplete_atomic_tail_is_truncated_before_later_appends() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    write_atomic_ops(
        &db_path,
        &[
            (
                1,
                WalOp::EnsureNodeLabel {
                    label: "Lost".to_string(),
                    label_id: 1,
                },
            ),
            (2, node_op(1, 1, "lost")),
        ],
    );
    damage_last_atomic_commit(&db_path, WalTailDamage::MissingCommit);

    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(engine.get_node_label_id("Lost").unwrap(), None);
    assert!(engine.get_node(1).unwrap().is_none());

    let survivor = engine
        .upsert_node("Survivor", "kept", UpsertNodeOptions::default())
        .unwrap();
    engine.close_fast().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(reopened.get_node_label_id("Lost").unwrap(), None);
    assert!(reopened.get_node_by_key("Lost", "lost").unwrap().is_none());
    assert_eq!(
        reopened
            .get_node(survivor)
            .unwrap()
            .unwrap()
            .labels
            .as_slice(),
        ["Survivor"]
    );
    reopened.close().unwrap();
}

#[test]
fn test_committed_batch_with_invalid_enclosed_op_fails_without_token_manifest_leak() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal_atomic");
    write_manifest_with_catalog(&db_path, &[], &[]);
    write_atomic_ops(
        &db_path,
        &[
            (
                1,
                WalOp::EnsureNodeLabel {
                    label: "Valid".to_string(),
                    label_id: 1,
                },
            ),
            (2, node_op(1, 99, "missing-token")),
        ],
    );

    assert!(matches!(
        DatabaseEngine::open(&db_path, &DbOptions::default()),
        Err(EngineError::CorruptWal(_))
    ));
    let manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert!(!manifest.node_label_tokens.contains_key("Valid"));
}
