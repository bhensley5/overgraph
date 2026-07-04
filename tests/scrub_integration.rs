use overgraph::types::{FlushEpochMeta, FlushEpochState};
use overgraph::{
    scrub_path, scrub_path_with_options, DatabaseEngine, DatabaseScrubFindingType,
    DatabaseScrubReport, DatabaseScrubSeverity, DbOptions, ManifestScrubSource, ManifestState,
    NodeInput, ScrubFindingType, ScrubPathOptions, ScrubReport, UpsertEdgeOptions, WalScrubRole,
};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
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

fn read_manifest_state(db_path: &Path) -> ManifestState {
    serde_json::from_slice(&std::fs::read(db_path.join("manifest.current")).unwrap()).unwrap()
}

fn write_manifest_state(db_path: &Path, manifest: &ManifestState) {
    std::fs::write(
        db_path.join("manifest.current"),
        serde_json::to_vec_pretty(manifest).unwrap(),
    )
    .unwrap();
}

fn wal_path(db_path: &Path, generation_id: u64) -> PathBuf {
    db_path.join(format!("wal_{generation_id}.wal"))
}

fn write_empty_wal(path: &Path) {
    let mut data = Vec::new();
    data.extend_from_slice(b"OVGR");
    data.extend_from_slice(&3u32.to_le_bytes());
    std::fs::write(path, data).unwrap();
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

fn database_finding(
    report: &DatabaseScrubReport,
    finding_type: DatabaseScrubFindingType,
    severity: DatabaseScrubSeverity,
) -> bool {
    report
        .findings
        .iter()
        .any(|finding| finding.finding_type == finding_type && finding.severity == severity)
}

fn wal_role_finding(
    report: &DatabaseScrubReport,
    role: WalScrubRole,
    finding_type: DatabaseScrubFindingType,
    severity: DatabaseScrubSeverity,
) -> bool {
    report
        .wal_generations
        .iter()
        .filter(|wal| wal.role == role)
        .flat_map(|wal| &wal.findings)
        .any(|finding| finding.finding_type == finding_type && finding.severity == severity)
}

fn append_frozen_epoch(manifest: &mut ManifestState, wal_generation_id: u64) {
    manifest.pending_flush_epochs.push(FlushEpochMeta {
        epoch_id: wal_generation_id,
        wal_generation_id,
        state: FlushEpochState::FrozenPendingFlush,
        segment_id: None,
    });
}

fn append_published_pending_retire_epoch(
    manifest: &mut ManifestState,
    wal_generation_id: u64,
    segment_id: Option<u64>,
) {
    manifest.pending_flush_epochs.push(FlushEpochMeta {
        epoch_id: wal_generation_id,
        wal_generation_id,
        state: FlushEpochState::PublishedPendingRetire,
        segment_id,
    });
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
fn test_scrub_path_healthy_closed_database_no_error_findings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();

    let report = scrub_path(&db_path).unwrap();

    let manifest = report.manifest.expect("closed DB should have a manifest");
    assert_eq!(manifest.source, ManifestScrubSource::Current);
    assert_eq!(manifest.segment_count, 1);
    assert_eq!(report.segments.len(), 1);
    assert_eq!(report.total_components_failed, 0);
    assert!(report.total_components_checked > 0);
    assert!(report
        .wal_generations
        .iter()
        .any(|wal| wal.role == WalScrubRole::Active));
    assert!(report.orphan_segments.is_empty());
    assert!(!report
        .findings
        .iter()
        .any(|finding| finding.severity == DatabaseScrubSeverity::Error));
}

#[test]
fn test_scrub_path_detects_core_corruption_without_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();

    let core_path = db_path
        .join("segments")
        .join("seg_0001")
        .join("segment.core");
    let corrupt_offset = std::fs::metadata(&core_path).unwrap().len() / 2;
    flip_file_byte(&core_path, corrupt_offset);

    let report = scrub_path(&db_path).unwrap();

    assert!(report.total_components_failed > 0);
    assert!(report.segments.iter().flat_map(|s| &s.findings).any(|f| {
        f.finding_type == ScrubFindingType::PayloadDigestMismatch
            || f.finding_type == ScrubFindingType::IdentityHeaderMismatch
    }));
}

#[test]
fn test_scrub_path_missing_manifest_reports_finding_without_creating_current() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    std::fs::create_dir(&db_path).unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert!(report.manifest.is_none());
    assert!(report.segments.is_empty());
    assert!(report.findings.iter().any(|finding| {
        finding.finding_type == DatabaseScrubFindingType::ManifestMissing
            && finding.severity == DatabaseScrubSeverity::Error
    }));
    assert!(!db_path.join("manifest.current").exists());
}

#[test]
fn test_scrub_path_does_not_promote_manifest_tmp() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();

    let current = db_path.join("manifest.current");
    let tmp = db_path.join("manifest.tmp");
    let prev = db_path.join("manifest.prev");
    if prev.exists() {
        std::fs::remove_file(&prev).unwrap();
    }
    std::fs::rename(&current, &tmp).unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert_eq!(
        report
            .manifest
            .expect("tmp manifest should be selected")
            .source,
        ManifestScrubSource::Tmp
    );
    assert!(tmp.exists());
    assert!(!current.exists());
}

#[test]
fn test_instance_scrub_still_returns_existing_report_shape() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);

    let report: ScrubReport = db.scrub().unwrap();

    assert_eq!(report.segments.len(), 1);
    assert_eq!(report.total_components_failed, 0);
}

#[test]
fn test_scrub_path_validates_close_fast_retained_wal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.batch_upsert_nodes(vec![NodeInput {
        labels: vec!["Person".to_string()],
        key: "retained".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }])
    .unwrap();
    db.close_fast().unwrap();

    let report = scrub_path(&db_path).unwrap();

    let active = report
        .wal_generations
        .iter()
        .find(|wal| wal.role == WalScrubRole::Active)
        .expect("active WAL should be reported");
    assert!(
        active.records_checked > 0,
        "close_fast retained WAL should be structurally checked"
    );
    assert!(
        active.findings.is_empty(),
        "unexpected WAL findings: {:?}",
        active.findings
    );
}

#[test]
fn test_scrub_path_missing_frozen_wal_is_error() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    append_frozen_epoch(&mut manifest, 99);
    manifest.next_wal_generation_id = manifest.next_wal_generation_id.max(100);
    write_manifest_state(&db_path, &manifest);

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::FrozenPendingFlush,
        DatabaseScrubFindingType::WalMissing,
        DatabaseScrubSeverity::Error
    ));
}

#[test]
fn test_scrub_path_missing_active_wal_with_frozen_epochs_is_error() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    let retained_frozen = manifest.active_wal_generation_id;
    manifest.active_wal_generation_id = 99;
    manifest.next_wal_generation_id = 100;
    append_frozen_epoch(&mut manifest, retained_frozen);
    write_manifest_state(&db_path, &manifest);

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::Active,
        DatabaseScrubFindingType::WalMissing,
        DatabaseScrubSeverity::Error
    ));
}

#[test]
fn test_scrub_path_missing_active_wal_without_frozen_epochs_is_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    manifest.active_wal_generation_id = 99;
    manifest.next_wal_generation_id = 100;
    manifest.pending_flush_epochs.clear();
    write_manifest_state(&db_path, &manifest);

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::Active,
        DatabaseScrubFindingType::WalMissing,
        DatabaseScrubSeverity::Warning
    ));
}

#[test]
fn test_scrub_path_validate_wal_false_skips_manifest_wal_validation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    manifest.active_wal_generation_id = 99;
    manifest.next_wal_generation_id = 100;
    write_manifest_state(&db_path, &manifest);

    let report = scrub_path_with_options(
        &db_path,
        &ScrubPathOptions {
            validate_wal: false,
            include_orphan_segments: false,
            check_manifest_stability: true,
        },
    )
    .unwrap();

    assert!(report.wal_generations.is_empty());
    assert!(!report
        .findings
        .iter()
        .any(|finding| finding.finding_type == DatabaseScrubFindingType::WalMissing));
}

#[test]
fn test_scrub_path_published_pending_retire_missing_wal_healthy_segment_is_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    let segment_id = manifest.segments[0].id;
    append_published_pending_retire_epoch(&mut manifest, 99, Some(segment_id));
    manifest.next_wal_generation_id = manifest.next_wal_generation_id.max(100);
    write_manifest_state(&db_path, &manifest);

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::PublishedPendingRetire,
        DatabaseScrubFindingType::WalMissing,
        DatabaseScrubSeverity::Warning
    ));
}

#[test]
fn test_scrub_path_published_pending_retire_missing_wal_unhealthy_segment_is_error() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    let segment_id = manifest.segments[0].id;
    append_published_pending_retire_epoch(&mut manifest, 99, Some(segment_id));
    manifest.next_wal_generation_id = manifest.next_wal_generation_id.max(100);
    write_manifest_state(&db_path, &manifest);
    std::fs::remove_dir_all(db_path.join("segments").join("seg_0001")).unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::PublishedPendingRetire,
        DatabaseScrubFindingType::WalMissing,
        DatabaseScrubSeverity::Error
    ));
}

#[test]
fn test_scrub_path_published_pending_retire_corrupt_wal_healthy_segment_is_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    let segment_id = manifest.segments[0].id;
    append_published_pending_retire_epoch(&mut manifest, 99, Some(segment_id));
    manifest.next_wal_generation_id = manifest.next_wal_generation_id.max(100);
    write_manifest_state(&db_path, &manifest);
    std::fs::write(wal_path(&db_path, 99), b"BADWAL!!").unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::PublishedPendingRetire,
        DatabaseScrubFindingType::WalCorrupt,
        DatabaseScrubSeverity::Warning
    ));
}

#[test]
fn test_scrub_path_duplicate_wal_generation_roles_are_error_findings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let mut manifest = read_manifest_state(&db_path);
    let active_generation = manifest.active_wal_generation_id;
    append_frozen_epoch(&mut manifest, active_generation);
    write_manifest_state(&db_path, &manifest);

    let report = scrub_path(&db_path).unwrap();

    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::WalCorrupt,
        DatabaseScrubSeverity::Error
    ));
    assert!(report
        .wal_generations
        .iter()
        .filter(|wal| wal.generation_id == active_generation)
        .all(|wal| wal.findings.iter().any(|finding| {
            finding.finding_type == DatabaseScrubFindingType::WalCorrupt
                && finding.severity == DatabaseScrubSeverity::Error
                && finding.detail.contains("multiple manifest roles")
        })));
}

#[test]
fn test_scrub_path_zero_byte_active_wal_uses_missing_severity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let manifest = read_manifest_state(&db_path);
    std::fs::write(wal_path(&db_path, manifest.active_wal_generation_id), &[]).unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::Active,
        DatabaseScrubFindingType::WalMissing,
        DatabaseScrubSeverity::Warning
    ));
}

#[test]
fn test_scrub_path_corrupt_active_wal_is_error() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let manifest = read_manifest_state(&db_path);
    std::fs::write(
        wal_path(&db_path, manifest.active_wal_generation_id),
        b"BADWAL!!",
    )
    .unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::Active,
        DatabaseScrubFindingType::WalCorrupt,
        DatabaseScrubSeverity::Error
    ));
}

#[test]
fn test_scrub_path_active_wal_trailing_bytes_are_warning_and_not_truncated() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.batch_upsert_nodes(vec![NodeInput {
        labels: vec!["Person".to_string()],
        key: "retained".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }])
    .unwrap();
    db.close_fast().unwrap();
    let manifest = read_manifest_state(&db_path);
    let path = wal_path(&db_path, manifest.active_wal_generation_id);
    let original_len = std::fs::metadata(&path).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[0xAA]).unwrap();
    file.flush().unwrap();
    drop(file);
    let len_with_trailing = std::fs::metadata(&path).unwrap().len();

    let report = scrub_path(&db_path).unwrap();

    assert!(wal_role_finding(
        &report,
        WalScrubRole::Active,
        DatabaseScrubFindingType::WalTrailingBytes,
        DatabaseScrubSeverity::Warning
    ));
    let active = report
        .wal_generations
        .iter()
        .find(|wal| wal.role == WalScrubRole::Active)
        .unwrap();
    assert_eq!(active.durable_len, original_len);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len_with_trailing);
}

#[test]
fn test_scrub_path_orphan_segment_is_reported_and_not_deleted() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    let orphan_path = db_path.join("segments").join("seg_0099");
    copy_dir_recursive(&db_path.join("segments").join("seg_0001"), &orphan_path);

    let report = scrub_path(&db_path).unwrap();

    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanSegment,
        DatabaseScrubSeverity::Warning
    ));
    assert!(orphan_path.exists());
    assert!(report.orphan_segments.is_empty());
}

#[test]
fn test_scrub_path_orphan_wal_is_reported_and_not_deleted() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let orphan_path = wal_path(&db_path, 99);
    write_empty_wal(&orphan_path);

    let report = scrub_path(&db_path).unwrap();

    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanWal,
        DatabaseScrubSeverity::Warning
    ));
    assert!(orphan_path.exists());
}

#[test]
fn test_scrub_path_orphan_wal_is_not_validated() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let orphan_path = wal_path(&db_path, 99);
    std::fs::write(&orphan_path, b"BADWAL!!").unwrap();

    let report = scrub_path(&db_path).unwrap();

    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanWal,
        DatabaseScrubSeverity::Warning
    ));
    assert!(!report.findings.iter().any(|finding| {
        finding.finding_type == DatabaseScrubFindingType::WalCorrupt
            && finding.component_kind == "wal_99.wal"
    }));
    assert!(orphan_path.exists());
}

#[test]
fn test_scrub_path_missing_manifest_with_current_layout_artifacts_reports_orphans() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    std::fs::create_dir_all(db_path.join("segments").join("seg_0001")).unwrap();
    write_empty_wal(&wal_path(&db_path, 0));

    let report = scrub_path(&db_path).unwrap();

    assert!(report.manifest.is_none());
    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::ManifestMissing,
        DatabaseScrubSeverity::Error
    ));
    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanSegment,
        DatabaseScrubSeverity::Warning
    ));
    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanWal,
        DatabaseScrubSeverity::Warning
    ));
    assert!(!db_path.join("manifest.current").exists());
}

#[test]
fn test_scrub_path_include_orphan_segments_deep_scrubs_unpinned_segment() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    let orphan_path = db_path.join("segments").join("seg_0099");
    copy_dir_recursive(&db_path.join("segments").join("seg_0001"), &orphan_path);

    let report = scrub_path_with_options(
        &db_path,
        &ScrubPathOptions {
            validate_wal: true,
            include_orphan_segments: true,
            check_manifest_stability: true,
        },
    )
    .unwrap();

    assert_eq!(report.orphan_segments.len(), 1);
    let orphan = &report.orphan_segments[0];
    assert!(orphan.semantic_checks_skipped);
    assert!(orphan.components_ok > 0);
    assert!(orphan.bytes_digested > 0);
    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanSegmentUnpinned,
        DatabaseScrubSeverity::Warning
    ));
    assert!(orphan_path.exists());
}

#[test]
fn test_scrub_path_orphan_segment_component_failure_updates_totals() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    populate_and_flush(&db);
    db.close().unwrap();
    let orphan_path = db_path.join("segments").join("seg_0099");
    copy_dir_recursive(&db_path.join("segments").join("seg_0001"), &orphan_path);
    let core_path = orphan_path.join("segment.core");
    let corrupt_offset = std::fs::metadata(&core_path).unwrap().len() / 2;
    flip_file_byte(&core_path, corrupt_offset);

    let report = scrub_path_with_options(
        &db_path,
        &ScrubPathOptions {
            validate_wal: true,
            include_orphan_segments: true,
            check_manifest_stability: true,
        },
    )
    .unwrap();

    assert_eq!(report.segments.len(), 1);
    assert!(
        report.segments[0].findings.is_empty(),
        "published segment should remain healthy"
    );
    assert_eq!(report.orphan_segments.len(), 1);
    assert!(
        !report.orphan_segments[0].findings.is_empty(),
        "corrupt orphan component should be reported"
    );
    assert!(
        report.total_components_failed > 0,
        "orphan component failure should contribute to database totals"
    );
    assert!(orphan_path.exists());
}

#[test]
fn test_scrub_path_include_orphan_segments_reports_scrub_skipped() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let db = open_test_db(&db_path);
    db.close().unwrap();
    let orphan_path = db_path.join("segments").join("seg_0099");
    std::fs::create_dir_all(&orphan_path).unwrap();

    let report = scrub_path_with_options(
        &db_path,
        &ScrubPathOptions {
            validate_wal: true,
            include_orphan_segments: true,
            check_manifest_stability: true,
        },
    )
    .unwrap();

    assert!(database_finding(
        &report,
        DatabaseScrubFindingType::OrphanSegmentScrubSkipped,
        DatabaseScrubSeverity::Warning
    ));
    assert_eq!(report.orphan_segments.len(), 1);
    assert!(report.orphan_segments[0].semantic_checks_skipped);
    assert!(orphan_path.exists());
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
