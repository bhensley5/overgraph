use overgraph::{DatabaseEngine, DbOptions, NodeInput, UpsertEdgeOptions};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Command, Output};
use tempfile::TempDir;

fn scrub_binary() -> &'static str {
    env!("CARGO_BIN_EXE_overgraph-scrub")
}

fn open_test_db(path: &Path) -> DatabaseEngine {
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    DatabaseEngine::open(path, &opts).unwrap()
}

fn populate_and_flush(db: &DatabaseEngine) {
    let nodes: Vec<NodeInput> = (0..10)
        .map(|i| NodeInput {
            labels: vec!["Person".to_string()],
            key: format!("node_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let ids = db.batch_upsert_nodes(nodes).unwrap();

    for i in 0..5 {
        db.upsert_edge(
            ids[i],
            ids[i + 5],
            "RELATES_TO",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    }

    db.flush().unwrap();
}

fn closed_populated_db() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    (dir, db_path)
}

fn run_scrub(args: &[&str]) -> Output {
    Command::new(scrub_binary())
        .args(args)
        .output()
        .expect("failed to run overgraph-scrub")
}

fn assert_exit(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "expected exit {code}, got {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn flip_file_byte(path: &Path, offset: u64) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_recursive(&src_path, &dst_path);
        } else {
            std::fs::copy(&src_path, &dst_path).unwrap();
        }
    }
}

fn parse_json_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout should be JSON: {error}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn has_finding(findings: &Value, finding_type: &str, severity: &str) -> bool {
    findings
        .as_array()
        .unwrap()
        .iter()
        .any(|finding| finding["findingType"] == finding_type && finding["severity"] == severity)
}

#[test]
fn scrub_cli_clean_text_output_exits_zero() {
    let (_dir, db_path) = closed_populated_db();

    let output = run_scrub(&[db_path.to_str().unwrap()]);
    assert_exit(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("OverGraph Scrub:"));
    assert!(stdout.contains("Status: clean"));
    assert!(stdout.contains("Manifest"));
    assert!(stdout.contains("Source:"));
    assert!(stdout.contains("Segments"));
    assert!(stdout.contains("WAL"));
    assert!(stdout.contains("Orphans"));
    assert!(stdout.contains("Findings: none"));
}

#[test]
fn scrub_cli_clean_json_output_matches_connector_shape() {
    let (_dir, db_path) = closed_populated_db();

    let output = run_scrub(&["--json", db_path.to_str().unwrap()]);
    assert_exit(&output, 0);
    let parsed = parse_json_stdout(&output);

    assert_eq!(parsed["manifest"]["source"], "Current");
    assert_eq!(parsed["manifest"]["segmentCount"], 1);
    assert!(parsed["segments"].as_array().unwrap().len() == 1);
    assert!(parsed["walGenerations"].as_array().unwrap().len() >= 1);
    assert!(parsed["findings"].as_array().is_some());
    assert!(parsed["orphanSegments"].as_array().unwrap().is_empty());
    assert!(parsed["totalComponentsChecked"].as_u64().unwrap() > 0);
    assert_eq!(parsed["totalComponentsFailed"], 0);
    assert!(parsed["totalBytesDigested"].as_u64().unwrap() > 0);
    assert!(parsed["totalWalRecordsChecked"].as_u64().is_some());
    assert!(parsed["totalWalBytesChecked"].as_u64().is_some());
    assert!(parsed["durationMs"].as_u64().is_some());

    let segment = &parsed["segments"][0];
    assert_eq!(segment["segmentId"], 1);
    assert!(segment["findings"].as_array().unwrap().is_empty());
    assert!(segment["componentsOk"].as_u64().unwrap() > 0);
    assert!(segment["bytesDigested"].as_u64().unwrap() > 0);

    let wal = &parsed["walGenerations"][0];
    assert!(wal["generationId"].as_u64().is_some());
    assert_eq!(wal["role"], "Active");
    assert!(wal["epochId"].is_null());
    assert!(wal["segmentId"].is_null());
    assert!(wal["fileLen"].as_u64().is_some());
    assert!(wal["durableLen"].as_u64().is_some());
    assert!(wal["trailingBytes"].as_u64().is_some());
    assert!(wal["recordsChecked"].as_u64().is_some());
    assert!(wal["bytesChecked"].as_u64().is_some());
    assert!(wal["findings"].as_array().is_some());
}

#[test]
fn scrub_cli_corrupt_segment_exits_one() {
    let (_dir, db_path) = closed_populated_db();
    let core_path = db_path
        .join("segments")
        .join("seg_0001")
        .join("segment.core");
    let corrupt_offset = std::fs::metadata(&core_path).unwrap().len() / 2;
    flip_file_byte(&core_path, corrupt_offset);

    let output = run_scrub(&[db_path.to_str().unwrap()]);
    assert_exit(&output, 1);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: errors found"));
    assert!(
        stdout.contains("PayloadDigestMismatch") || stdout.contains("IdentityHeaderMismatch"),
        "expected component corruption finding in output:\n{stdout}"
    );
}

#[test]
fn scrub_cli_invocation_errors_exit_two() {
    let no_args = run_scrub(&[]);
    assert_exit(&no_args, 2);
    assert!(String::from_utf8_lossy(&no_args.stderr).contains("Usage:"));

    let unknown_flag = run_scrub(&["--bogus"]);
    assert_exit(&unknown_flag, 2);
    assert!(String::from_utf8_lossy(&unknown_flag.stderr).contains("Unknown argument"));

    let (_dir_one, db_path_one) = closed_populated_db();
    let (_dir_two, db_path_two) = closed_populated_db();
    let duplicate_path = run_scrub(&[db_path_one.to_str().unwrap(), db_path_two.to_str().unwrap()]);
    assert_exit(&duplicate_path, 2);
    assert!(String::from_utf8_lossy(&duplicate_path.stderr).contains("Duplicate database path"));
}

#[test]
fn scrub_cli_warning_only_orphan_discovery_exits_zero() {
    let (_dir, db_path) = closed_populated_db();
    let orphan_path = db_path.join("segments").join("seg_0099");
    copy_dir_recursive(&db_path.join("segments").join("seg_0001"), &orphan_path);

    let output = run_scrub(&[db_path.to_str().unwrap()]);
    assert_exit(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: warnings found"));
    assert!(stdout.contains("[warning]"));
    assert!(stdout.contains("OrphanSegment"));
    assert!(stdout.contains("Orphan segments scrubbed: 0"));
    assert!(orphan_path.exists());
}

#[test]
fn scrub_cli_include_orphan_segments_reports_deep_result_and_preserves_file() {
    let (_dir, db_path) = closed_populated_db();
    let orphan_path = db_path.join("segments").join("seg_0099");
    copy_dir_recursive(&db_path.join("segments").join("seg_0001"), &orphan_path);

    let output = run_scrub(&[
        "--json",
        db_path.to_str().unwrap(),
        "--include-orphan-segments",
    ]);
    assert_exit(&output, 0);
    let parsed = parse_json_stdout(&output);

    assert!(has_finding(
        &parsed["findings"],
        "OrphanSegmentUnpinned",
        "warning"
    ));
    assert_eq!(parsed["orphanSegments"].as_array().unwrap().len(), 1);
    let orphan = &parsed["orphanSegments"][0];
    assert_eq!(orphan["segmentId"], 1);
    assert_eq!(orphan["semanticChecksSkipped"], true);
    assert!(orphan["componentsOk"].as_u64().unwrap() > 0);
    assert!(orphan["bytesDigested"].as_u64().unwrap() > 0);
    assert!(orphan_path.exists());
}

#[test]
fn scrub_cli_missing_manifest_reports_text_and_json_without_creating_manifest() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    std::fs::create_dir(&db_path).unwrap();

    let json_output = run_scrub(&["--json", db_path.to_str().unwrap()]);
    assert_exit(&json_output, 1);
    let parsed = parse_json_stdout(&json_output);
    assert!(parsed["manifest"].is_null());
    assert!(has_finding(&parsed["findings"], "ManifestMissing", "error"));
    assert!(!db_path.join("manifest.current").exists());

    let text_output = run_scrub(&[db_path.to_str().unwrap()]);
    assert_exit(&text_output, 1);
    let stdout = String::from_utf8_lossy(&text_output.stdout);
    assert!(stdout.contains("No valid manifest selected"));
    assert!(stdout.contains("ManifestMissing"));
    assert!(!db_path.join("manifest.current").exists());
}

#[test]
fn scrub_cli_source_does_not_open_engine_or_call_mutating_helpers() {
    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/scrub.rs"))
            .unwrap();

    for forbidden in [
        concat!("DatabaseEngine", "::open"),
        concat!("EngineCore", "::open"),
        concat!("WalWriter", "::open_generation"),
        concat!("truncate", "_wal_generation_to"),
        concat!("remove", "_wal_generation"),
        concat!("cleanup", "_orphan_segments"),
        concat!("cleanup", "_orphan_wal_files"),
    ] {
        assert!(
            !source.contains(forbidden),
            "overgraph-scrub source must not contain {forbidden}"
        );
    }
}
