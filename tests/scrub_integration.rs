use overgraph::{DatabaseEngine, DbOptions, NodeInput, ScrubFindingType, UpsertEdgeOptions};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::TempDir;

fn open_test_db(dir: &std::path::Path) -> DatabaseEngine {
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    DatabaseEngine::open(dir, &opts).unwrap()
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
    let ids = db.batch_upsert_nodes(nodes.clone()).unwrap();

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

fn flip_file_byte(path: &std::path::Path, offset: u64) {
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

#[test]
fn test_scrub_healthy_database_no_findings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);

    let report = db.scrub().unwrap();
    assert_eq!(report.segments.len(), 1);
    assert_eq!(report.total_components_failed, 0);
    assert!(report.total_components_ok > 0);
    assert!(report.total_components_checked > 0);
    for seg in &report.segments {
        assert!(
            seg.findings.is_empty(),
            "unexpected findings: {:?}",
            seg.findings
        );
    }
}

#[test]
fn test_scrub_healthy_multi_label_database_no_findings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    let nodes: Vec<NodeInput> = (0..6)
        .map(|i| NodeInput {
            labels: vec![
                "Person".to_string(),
                if i % 2 == 0 { "Researcher" } else { "Reviewer" }.to_string(),
            ],
            key: format!("node_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    db.batch_upsert_nodes(nodes).unwrap();
    db.flush().unwrap();

    let report = db.scrub().unwrap();
    assert_eq!(report.segments.len(), 1);
    assert_eq!(report.total_components_failed, 0);
    for seg in &report.segments {
        assert!(
            seg.findings.is_empty(),
            "unexpected findings: {:?}",
            seg.findings
        );
    }
}

#[test]
fn test_scrub_detects_packed_range_corruption() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();

    let seg_dir = db_path.join("segments").join("seg_0001");
    let core_path = seg_dir.join("segment.core");
    let corrupt_offset = std::fs::metadata(&core_path).unwrap().len() / 2;
    flip_file_byte(&core_path, corrupt_offset);

    let db = open_test_db(&db_path);
    let report = db.scrub().unwrap();
    assert!(report.total_components_failed > 0);

    let has_digest_mismatch = report
        .segments
        .iter()
        .flat_map(|s| &s.findings)
        .any(|f| f.finding_type == ScrubFindingType::PayloadDigestMismatch);
    assert!(
        has_digest_mismatch,
        "expected PayloadDigestMismatch, got: {:?}",
        report.segments[0].findings
    );
}

#[test]
fn test_scrub_detects_external_payload_corruption() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();

    let seg_dir = db_path.join("segments").join("seg_0001");
    let sidecar_path = find_external_sidecar(&seg_dir)
        .expect("test precondition: expected at least one external sidecar after flush with edges");
    let len = std::fs::metadata(&sidecar_path).unwrap().len();
    assert!(
        len > 192,
        "test precondition: external sidecar must have identity header + payload"
    );
    flip_file_byte(&sidecar_path, 193);

    let db = open_test_db(&db_path);
    let report = db.scrub().unwrap();
    let has_mismatch = report.segments.iter().flat_map(|s| &s.findings).any(|f| {
        f.finding_type == ScrubFindingType::PayloadDigestMismatch
            || f.finding_type == ScrubFindingType::IdentityHeaderMismatch
    });
    assert!(
        has_mismatch,
        "expected corruption finding, got: {:?}",
        report.segments[0].findings
    );
}

#[test]
fn test_scrub_detects_identity_header_tamper() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();

    let seg_dir = db_path.join("segments").join("seg_0001");
    let sidecar_path = find_external_sidecar(&seg_dir)
        .expect("test precondition: expected at least one external sidecar after flush with edges");
    let len = std::fs::metadata(&sidecar_path).unwrap().len();
    assert!(
        len >= 192,
        "test precondition: external sidecar must have identity header"
    );
    flip_file_byte(&sidecar_path, 16);

    let db = open_test_db(&db_path);
    let report = db.scrub().unwrap();
    let has_header_mismatch = report
        .segments
        .iter()
        .flat_map(|s| &s.findings)
        .any(|f| f.finding_type == ScrubFindingType::IdentityHeaderMismatch);
    assert!(
        has_header_mismatch,
        "expected IdentityHeaderMismatch, got: {:?}",
        report.segments[0].findings
    );
}

#[test]
fn test_scrub_handles_missing_segment_gracefully() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);

    let report_before = db.scrub().unwrap();
    assert_eq!(report_before.total_components_failed, 0);

    db.close().unwrap();

    let seg_dir = db_path.join("segments").join("seg_0001");
    std::fs::remove_dir_all(&seg_dir).unwrap();

    let db = open_test_db(&db_path);
    let report = db.scrub().unwrap();
    assert!(report.total_components_failed > 0);

    let has_missing = report
        .segments
        .iter()
        .flat_map(|s| &s.findings)
        .any(|f| f.finding_type == ScrubFindingType::FileMissing);
    assert!(has_missing, "expected FileMissing finding");
}

#[test]
fn test_scrub_parallel_multiple_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);

    for batch in 0..3 {
        let nodes: Vec<NodeInput> = (0..5)
            .map(|i| NodeInput {
                labels: vec!["Person".to_string()],
                key: format!("batch{batch}_node_{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        db.batch_upsert_nodes(nodes.clone()).unwrap();
        db.flush().unwrap();
    }

    let report = db.scrub().unwrap();
    assert_eq!(report.segments.len(), 3);
    assert_eq!(report.total_components_failed, 0);
    assert!(report.total_components_ok >= 3);

    let segment_ids: Vec<u64> = report.segments.iter().map(|s| s.segment_id).collect();
    assert_eq!(segment_ids.len(), 3);
    assert!(segment_ids.contains(&1));
    assert!(segment_ids.contains(&2));
    assert!(segment_ids.contains(&3));
}

fn find_external_sidecar(seg_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let entries = std::fs::read_dir(seg_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name != "segment_manifest.dat" && name != "segment.core" && path.is_file() {
                return Some(path);
            }
        }
    }
    None
}
