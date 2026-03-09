use overgraph::{DatabaseEngine, DbOptions, PrunePolicy, WalSyncMode};
use serde_json;
use std::collections::BTreeMap;
use std::process::Command;
use tempfile::TempDir;

fn inspect_binary() -> &'static str {
    env!("CARGO_BIN_EXE_overgraph-inspect")
}

#[test]
fn test_inspect_empty_db() {
    let dir = TempDir::new().unwrap();

    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
    db.close().unwrap();

    let output = Command::new(inspect_binary())
        .arg(dir.path().to_str().unwrap())
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("OverGraph Database:"));
    assert!(stdout.contains("Version:       1"));
    assert!(stdout.contains("Segments: 0"));
    assert!(stdout.contains("WAL: data.wal"));
}

#[test]
fn test_inspect_with_data() {
    let dir = TempDir::new().unwrap();

    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    for i in 0..10 {
        db.upsert_node(1, &format!("node_{}", i), BTreeMap::new(), 1.0)
            .unwrap();
    }
    let n1 = db.upsert_node(2, "a", BTreeMap::new(), 1.0).unwrap();
    let n2 = db.upsert_node(2, "b", BTreeMap::new(), 1.0).unwrap();
    db.upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None)
        .unwrap();

    db.flush().unwrap();
    db.close().unwrap();

    let output = Command::new(inspect_binary())
        .arg(dir.path().to_str().unwrap())
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Segments: 1"));
    // Verify the segment table shows 12 nodes and 1 edge in the right-aligned columns
    assert!(stdout.contains("        12"));
    assert!(stdout.contains("Segment Sizes"));
    assert!(stdout.contains("seg_0001"));
    assert!(stdout.contains("Next node ID:  13"));
    assert!(stdout.contains("Next edge ID:  2"));
}

#[test]
fn test_inspect_multiple_segments() {
    let dir = TempDir::new().unwrap();

    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    for i in 0..5 {
        db.upsert_node(1, &format!("batch1_{}", i), BTreeMap::new(), 1.0)
            .unwrap();
    }
    db.flush().unwrap();

    for i in 0..3 {
        db.upsert_node(1, &format!("batch2_{}", i), BTreeMap::new(), 1.0)
            .unwrap();
    }
    db.flush().unwrap();

    db.close().unwrap();

    let output = Command::new(inspect_binary())
        .arg(dir.path().to_str().unwrap())
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("Segments: 2"));
    assert!(stdout.contains("seg_0001"));
    assert!(stdout.contains("seg_0002"));
    // Total row should appear for multi-segment DBs
    assert!(stdout.contains("Total"));
}

#[test]
fn test_inspect_with_prune_policies() {
    let dir = TempDir::new().unwrap();

    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    db.set_prune_policy(
        "old_memories",
        PrunePolicy {
            max_age_ms: Some(86_400_000),
            max_weight: Some(0.1),
            type_id: Some(3),
        },
    )
    .unwrap();

    db.close().unwrap();

    let output = Command::new(inspect_binary())
        .arg(dir.path().to_str().unwrap())
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("Prune Policies: 1"));
    assert!(stdout.contains("old_memories"));
    assert!(stdout.contains("max_age=86400000ms"));
    assert!(stdout.contains("max_weight=0.1"));
    assert!(stdout.contains("type_id=3"));
}

#[test]
fn test_inspect_json_with_data() {
    let dir = TempDir::new().unwrap();

    let opts = DbOptions {
        create_if_missing: true,
        wal_sync_mode: WalSyncMode::Immediate,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let mut db = DatabaseEngine::open(dir.path(), &opts).unwrap();

    for i in 0..5 {
        db.upsert_node(1, &format!("node_{}", i), BTreeMap::new(), 1.0)
            .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();

    let output = Command::new(inspect_binary())
        .args(["--json", dir.path().to_str().unwrap()])
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "inspect --json failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("output should be valid JSON");
    assert_eq!(parsed["manifest_version"], 1);
    assert_eq!(parsed["segment_count"], 1);
    assert_eq!(parsed["total_nodes"], 5);
    assert_eq!(parsed["total_edges"], 0);
    assert_eq!(parsed["next_node_id"], 6);
    assert_eq!(parsed["next_edge_id"], 1);
    assert!(parsed["wal_bytes"].as_u64().unwrap() > 0);
    assert_eq!(parsed["segments"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["segments"][0]["node_count"], 5);
    assert!(parsed["segments"][0]["size_bytes"].as_u64().unwrap() > 0);
}

#[test]
fn test_inspect_json_uninitialized() {
    let dir = TempDir::new().unwrap();

    let output = Command::new(inspect_binary())
        .args(["--json", dir.path().to_str().unwrap()])
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("output should be valid JSON");
    assert_eq!(parsed["initialized"], false);
    assert!(parsed["wal_bytes"].is_null());
}

#[test]
fn test_inspect_nonexistent_path() {
    let output = Command::new(inspect_binary())
        .arg("/tmp/does_not_exist_overgraph_test")
        .output()
        .expect("failed to run overgraph-inspect");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not a directory"));
}

#[test]
fn test_inspect_no_args() {
    let output = Command::new(inspect_binary())
        .output()
        .expect("failed to run overgraph-inspect");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Usage:"));
}

#[test]
fn test_inspect_uninitialized_dir() {
    let dir = TempDir::new().unwrap();

    let output = Command::new(inspect_binary())
        .arg(dir.path().to_str().unwrap())
        .output()
        .expect("failed to run overgraph-inspect");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("No manifest found"));
    assert!(stdout.contains("WAL: not present"));
}
