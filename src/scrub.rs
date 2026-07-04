use crate::error::EngineError;
use crate::manifest::{validate_manifest_identity, MANIFEST_CURRENT, MANIFEST_PREV, MANIFEST_TMP};
use crate::parallel::engine_cpu_install;
use crate::property_value_semantics::{
    hash_prop_equality_key, numeric_range_sort_key_for_value, NumericRangeSortKey,
};
use crate::schema::normalize_schema_manifest;
use crate::segment_components::{
    component_id, decode_identity_header, decode_manifest_envelope, dependency_digest,
    ComponentHandleV1, ComponentIdentityHeaderV1, SegmentComponentKind, SegmentComponentManifestV1,
    SegmentComponentRecordV1, COMPONENT_IDENTITY_HEADER_LEN, PACKED_CORE_FILENAME,
    SEGMENT_COMPONENT_MANIFEST_FILENAME,
};
use crate::segment_reader::{validate_segment_manifest_identity, SegmentReader};
use crate::segment_writer::segment_dir;
use crate::types::{
    ComponentScrubFinding, DatabaseScrubFinding, DatabaseScrubFindingType, DatabaseScrubReport,
    DatabaseScrubSeverity, FlushEpochState, ManifestScrubSource, ManifestScrubSummary,
    ManifestState, OrphanSegmentScrubResult, ScrubFindingType, ScrubPathOptions, ScrubReport,
    SecondaryIndexKind, SecondaryIndexManifestEntry, SecondaryIndexState, SecondaryIndexTarget,
    SegmentInfo, SegmentScrubResult, WalScrubResult, WalScrubRole,
};
use crate::wal::{scrub_generation_readonly, wal_generation_path, WalScrubTerminal};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Mutex;
use std::time::Instant;

const SCRUB_READ_BUFFER_SIZE: usize = 64 * 1024;

struct ScrubManifestLoad {
    manifest: Option<ManifestState>,
    source: Option<ManifestScrubSource>,
    source_digest: Option<[u8; 32]>,
    source_len: Option<u64>,
    findings: Vec<DatabaseScrubFinding>,
}

#[derive(Clone)]
struct WalRoleRef {
    generation_id: u64,
    role: WalScrubRole,
    epoch_id: Option<u64>,
    segment_id: Option<u64>,
}

struct OrphanSegmentCandidate {
    segment_id: Option<u64>,
    path: PathBuf,
}

struct OrphanScanOutcome {
    findings: Vec<DatabaseScrubFinding>,
    orphan_segments: Vec<OrphanSegmentScrubResult>,
    total_components_checked: u64,
    total_components_ok: u64,
    total_components_failed: u64,
    total_bytes_digested: u64,
}

struct OrphanSegmentDeepScrub {
    result: OrphanSegmentScrubResult,
    component_checks_ran: bool,
}

#[cfg(test)]
struct ManifestStabilityTestHook {
    db_dir: PathBuf,
    callback: Box<dyn FnOnce() + Send>,
}

#[cfg(test)]
static MANIFEST_STABILITY_TEST_HOOK: Mutex<Option<ManifestStabilityTestHook>> = Mutex::new(None);

pub fn scrub_path(path: &Path) -> Result<DatabaseScrubReport, EngineError> {
    scrub_path_with_options(path, &ScrubPathOptions::default())
}

pub fn scrub_path_with_options(
    path: &Path,
    options: &ScrubPathOptions,
) -> Result<DatabaseScrubReport, EngineError> {
    let start = Instant::now();
    validate_scrub_root(path)?;

    let manifest_load = load_manifest_for_scrub(path);
    let selected_manifest_metadata = (
        manifest_load.source,
        manifest_load.source_len,
        manifest_load.source_digest,
    );
    let mut findings = manifest_load.findings;
    let mut manifest_summary = None;
    let mut segments = Vec::new();
    let mut wal_generations = Vec::new();
    let mut total_components_checked = 0;
    let mut total_components_ok = 0;
    let mut total_components_failed = 0;
    let mut total_bytes_digested = 0;
    let mut total_wal_records_checked = 0;
    let mut total_wal_bytes_checked = 0;
    let mut referenced_wal_generations = HashSet::new();

    if let (Some(manifest), Some(source)) = (&manifest_load.manifest, manifest_load.source) {
        manifest_summary = Some(build_manifest_summary(source, &manifest));
        let segment_report = scrub_database(path, manifest)?;
        segments = segment_report.segments;
        total_components_checked = segment_report.total_components_checked;
        total_components_ok = segment_report.total_components_ok;
        total_components_failed = segment_report.total_components_failed;
        total_bytes_digested = segment_report.total_bytes_digested;

        let wal_roles = wal_roles_for_manifest(manifest);
        referenced_wal_generations.extend(wal_roles.iter().map(|role| role.generation_id));
        if options.validate_wal {
            wal_generations = scrub_manifest_wal_generations(path, manifest, &segments, &wal_roles);
            for wal in &wal_generations {
                total_wal_records_checked += wal.records_checked;
                total_wal_bytes_checked += wal.bytes_checked;
                findings.extend(wal.findings.iter().cloned());
            }
        }
    }

    let orphan_scan = scan_orphan_artifacts(
        path,
        manifest_load.manifest.as_ref(),
        &referenced_wal_generations,
        options.include_orphan_segments,
    );
    findings.extend(orphan_scan.findings);
    let orphan_segments = orphan_scan.orphan_segments;
    total_components_checked += orphan_scan.total_components_checked;
    total_components_ok += orphan_scan.total_components_ok;
    total_components_failed += orphan_scan.total_components_failed;
    total_bytes_digested += orphan_scan.total_bytes_digested;

    if options.check_manifest_stability {
        run_manifest_stability_test_hook(path);
        if let Some(finding) = recheck_selected_manifest_stability(
            path,
            selected_manifest_metadata.0,
            selected_manifest_metadata.1,
            selected_manifest_metadata.2,
        ) {
            findings.push(finding);
        }
    }

    Ok(DatabaseScrubReport {
        manifest: manifest_summary,
        findings,
        segments,
        wal_generations,
        orphan_segments,
        total_components_checked,
        total_components_ok,
        total_components_failed,
        total_bytes_digested,
        total_wal_records_checked,
        total_wal_bytes_checked,
        duration_ms: start.elapsed().as_millis() as u64,
    })
}

fn validate_scrub_root(path: &Path) -> Result<(), EngineError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(EngineError::DatabaseNotFound(format!(
                "database path does not exist: {}",
                path.display()
            )));
        }
        Err(error) => return Err(EngineError::IoError(error)),
    };
    if !metadata.is_dir() {
        return Err(EngineError::InvalidOperation(format!(
            "database path is not a directory: {}",
            path.display()
        )));
    }
    fs::read_dir(path)?;
    Ok(())
}

fn load_manifest_for_scrub(db_dir: &Path) -> ScrubManifestLoad {
    let mut findings = Vec::new();
    for (index, (source, name)) in manifest_candidates().iter().copied().enumerate() {
        let path = db_dir.join(name);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                findings.push(manifest_file_invalid_finding(
                    name,
                    format!("cannot read {name}: {error}"),
                ));
                continue;
            }
        };
        let source_len = bytes.len() as u64;
        let source_digest: [u8; 32] = Sha256::digest(&bytes).into();
        match parse_manifest_for_scrub(&bytes) {
            Ok(manifest) => {
                if index > 0 {
                    findings.push(DatabaseScrubFinding {
                        component_kind: "manifest".to_string(),
                        finding_type: DatabaseScrubFindingType::ManifestFallbackUsed,
                        severity: DatabaseScrubSeverity::Warning,
                        detail: format!(
                            "selected {name} after an earlier manifest candidate was absent or invalid"
                        ),
                    });
                }
                return ScrubManifestLoad {
                    manifest: Some(manifest),
                    source: Some(source),
                    source_digest: Some(source_digest),
                    source_len: Some(source_len),
                    findings,
                };
            }
            Err(error) => {
                findings.push(manifest_file_invalid_finding(
                    name,
                    format!("{name} is invalid: {error}"),
                ));
            }
        }
    }

    findings.push(DatabaseScrubFinding {
        component_kind: "manifest".to_string(),
        finding_type: DatabaseScrubFindingType::ManifestMissing,
        severity: DatabaseScrubSeverity::Error,
        detail: "no valid manifest could be selected from manifest.current, manifest.tmp, or manifest.prev"
            .to_string(),
    });

    ScrubManifestLoad {
        manifest: None,
        source: None,
        source_digest: None,
        source_len: None,
        findings,
    }
}

fn manifest_candidates() -> [(ManifestScrubSource, &'static str); 3] {
    [
        (ManifestScrubSource::Current, MANIFEST_CURRENT),
        (ManifestScrubSource::Tmp, MANIFEST_TMP),
        (ManifestScrubSource::Prev, MANIFEST_PREV),
    ]
}

fn parse_manifest_for_scrub(bytes: &[u8]) -> Result<ManifestState, EngineError> {
    let mut manifest: ManifestState = serde_json::from_slice(bytes)
        .map_err(|error| EngineError::ManifestError(format!("parse: {error}")))?;
    normalize_schema_manifest(&mut manifest)?;
    validate_manifest_identity(&manifest)?;
    Ok(manifest)
}

fn build_manifest_summary(
    source: ManifestScrubSource,
    manifest: &ManifestState,
) -> ManifestScrubSummary {
    ManifestScrubSummary {
        source,
        segment_count: manifest.segments.len(),
        pending_flush_count: manifest.pending_flush_epochs.len(),
        active_wal_generation_id: manifest.active_wal_generation_id,
        next_wal_generation_id: manifest.next_wal_generation_id,
    }
}

fn manifest_file_invalid_finding(name: &str, detail: String) -> DatabaseScrubFinding {
    DatabaseScrubFinding {
        component_kind: name.to_string(),
        finding_type: DatabaseScrubFindingType::ManifestFileInvalid,
        severity: DatabaseScrubSeverity::Error,
        detail,
    }
}

fn wal_roles_for_manifest(manifest: &ManifestState) -> Vec<WalRoleRef> {
    let mut roles = Vec::with_capacity(1 + manifest.pending_flush_epochs.len());
    roles.push(WalRoleRef {
        generation_id: manifest.active_wal_generation_id,
        role: WalScrubRole::Active,
        epoch_id: None,
        segment_id: None,
    });
    for epoch in &manifest.pending_flush_epochs {
        let role = match epoch.state {
            FlushEpochState::FrozenPendingFlush => WalScrubRole::FrozenPendingFlush,
            FlushEpochState::PublishedPendingRetire => WalScrubRole::PublishedPendingRetire,
        };
        roles.push(WalRoleRef {
            generation_id: epoch.wal_generation_id,
            role,
            epoch_id: Some(epoch.epoch_id),
            segment_id: epoch.segment_id,
        });
    }
    roles
}

fn scrub_manifest_wal_generations(
    db_dir: &Path,
    manifest: &ManifestState,
    segments: &[SegmentScrubResult],
    roles: &[WalRoleRef],
) -> Vec<WalScrubResult> {
    let duplicate_details = duplicate_wal_role_details(roles);
    roles
        .iter()
        .map(|role_ref| {
            let mut result = scrub_wal_role(db_dir, manifest, segments, role_ref);
            if let Some(detail) = duplicate_details.get(&role_ref.generation_id) {
                result.findings.push(DatabaseScrubFinding {
                    component_kind: wal_component_kind(role_ref.generation_id),
                    finding_type: DatabaseScrubFindingType::WalCorrupt,
                    severity: DatabaseScrubSeverity::Error,
                    detail: detail.clone(),
                });
            }
            result
        })
        .collect()
}

fn duplicate_wal_role_details(roles: &[WalRoleRef]) -> BTreeMap<u64, String> {
    let mut by_generation: BTreeMap<u64, Vec<WalScrubRole>> = BTreeMap::new();
    for role_ref in roles {
        let roles = by_generation.entry(role_ref.generation_id).or_default();
        if !roles.contains(&role_ref.role) {
            roles.push(role_ref.role);
        }
    }
    by_generation
        .into_iter()
        .filter_map(|(generation_id, roles)| {
            if roles.len() <= 1 {
                return None;
            }
            let names = roles
                .iter()
                .map(|role| wal_role_name(*role))
                .collect::<Vec<_>>()
                .join(", ");
            Some((
                generation_id,
                format!(
                    "WAL generation {generation_id} is referenced by multiple manifest roles: {names}"
                ),
            ))
        })
        .collect()
}

fn scrub_wal_role(
    db_dir: &Path,
    manifest: &ManifestState,
    segments: &[SegmentScrubResult],
    role_ref: &WalRoleRef,
) -> WalScrubResult {
    let mut result = WalScrubResult {
        generation_id: role_ref.generation_id,
        role: role_ref.role,
        epoch_id: role_ref.epoch_id,
        segment_id: role_ref.segment_id,
        file_len: 0,
        durable_len: 0,
        trailing_bytes: 0,
        records_checked: 0,
        bytes_checked: 0,
        findings: Vec::new(),
    };

    let wal_path = wal_generation_path(db_dir, role_ref.generation_id);
    match fs::metadata(&wal_path) {
        Ok(metadata) => {
            result.file_len = metadata.len();
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            result.findings.push(DatabaseScrubFinding {
                component_kind: wal_component_kind(role_ref.generation_id),
                finding_type: DatabaseScrubFindingType::WalMissing,
                severity: wal_missing_severity(role_ref, manifest, segments),
                detail: format!(
                    "manifest references missing WAL generation {} at {}",
                    role_ref.generation_id,
                    wal_path.display()
                ),
            });
            return result;
        }
        Err(error) => {
            result.findings.push(DatabaseScrubFinding {
                component_kind: wal_component_kind(role_ref.generation_id),
                finding_type: DatabaseScrubFindingType::WalCorrupt,
                severity: wal_corrupt_severity(role_ref, manifest, segments),
                detail: format!(
                    "cannot stat WAL generation {} at {}: {error}",
                    role_ref.generation_id,
                    wal_path.display()
                ),
            });
            return result;
        }
    }

    match scrub_generation_readonly(db_dir, role_ref.generation_id) {
        Ok(scan) => {
            result.file_len = scan.file_len;
            result.durable_len = scan.durable_len;
            result.trailing_bytes = scan.file_len.saturating_sub(scan.durable_len);
            result.records_checked = scan.records_checked;
            result.bytes_checked = scan.bytes_checked;
            if scan.file_len == 0 {
                result.findings.push(DatabaseScrubFinding {
                    component_kind: wal_component_kind(role_ref.generation_id),
                    finding_type: DatabaseScrubFindingType::WalMissing,
                    severity: wal_missing_severity(role_ref, manifest, segments),
                    detail: format!(
                        "WAL generation {} exists but is zero bytes",
                        role_ref.generation_id
                    ),
                });
            }
            if let Some(terminal) = scan.terminal {
                match terminal {
                    WalScrubTerminal::RecoverableTrailing { reason } => {
                        result.findings.push(DatabaseScrubFinding {
                            component_kind: wal_component_kind(role_ref.generation_id),
                            finding_type: DatabaseScrubFindingType::WalTrailingBytes,
                            severity: DatabaseScrubSeverity::Warning,
                            detail: format!(
                                "WAL generation {} has recoverable trailing bytes after durable offset {}: {reason}",
                                role_ref.generation_id, result.durable_len
                            ),
                        });
                    }
                    WalScrubTerminal::Corrupt { reason } => {
                        result.findings.push(DatabaseScrubFinding {
                            component_kind: wal_component_kind(role_ref.generation_id),
                            finding_type: DatabaseScrubFindingType::WalCorrupt,
                            severity: wal_corrupt_severity(role_ref, manifest, segments),
                            detail: format!(
                                "WAL generation {} is corrupt: {reason}",
                                role_ref.generation_id
                            ),
                        });
                    }
                }
            }
        }
        Err(error) => {
            result.findings.push(DatabaseScrubFinding {
                component_kind: wal_component_kind(role_ref.generation_id),
                finding_type: DatabaseScrubFindingType::WalCorrupt,
                severity: wal_corrupt_severity(role_ref, manifest, segments),
                detail: format!(
                    "cannot read WAL generation {} at {}: {error}",
                    role_ref.generation_id,
                    wal_path.display()
                ),
            });
        }
    }

    result
}

fn wal_missing_severity(
    role_ref: &WalRoleRef,
    manifest: &ManifestState,
    segments: &[SegmentScrubResult],
) -> DatabaseScrubSeverity {
    match role_ref.role {
        WalScrubRole::Active => {
            if manifest
                .pending_flush_epochs
                .iter()
                .any(|epoch| epoch.state == FlushEpochState::FrozenPendingFlush)
            {
                DatabaseScrubSeverity::Error
            } else {
                DatabaseScrubSeverity::Warning
            }
        }
        WalScrubRole::FrozenPendingFlush => DatabaseScrubSeverity::Error,
        WalScrubRole::PublishedPendingRetire => {
            if published_pending_retire_segment_healthy(role_ref, manifest, segments) {
                DatabaseScrubSeverity::Warning
            } else {
                DatabaseScrubSeverity::Error
            }
        }
    }
}

fn wal_corrupt_severity(
    role_ref: &WalRoleRef,
    manifest: &ManifestState,
    segments: &[SegmentScrubResult],
) -> DatabaseScrubSeverity {
    match role_ref.role {
        WalScrubRole::Active | WalScrubRole::FrozenPendingFlush => DatabaseScrubSeverity::Error,
        WalScrubRole::PublishedPendingRetire => {
            if published_pending_retire_segment_healthy(role_ref, manifest, segments) {
                DatabaseScrubSeverity::Warning
            } else {
                DatabaseScrubSeverity::Error
            }
        }
    }
}

fn published_pending_retire_segment_healthy(
    role_ref: &WalRoleRef,
    manifest: &ManifestState,
    segments: &[SegmentScrubResult],
) -> bool {
    let Some(segment_id) = role_ref.segment_id else {
        return false;
    };
    if !manifest
        .segments
        .iter()
        .any(|segment| segment.id == segment_id)
    {
        return false;
    }
    segments
        .iter()
        .find(|segment| segment.segment_id == segment_id)
        .is_some_and(|segment| segment.findings.is_empty())
}

fn wal_component_kind(generation_id: u64) -> String {
    format!("wal_{generation_id}.wal")
}

fn wal_role_name(role: WalScrubRole) -> &'static str {
    match role {
        WalScrubRole::Active => "Active",
        WalScrubRole::FrozenPendingFlush => "FrozenPendingFlush",
        WalScrubRole::PublishedPendingRetire => "PublishedPendingRetire",
    }
}

fn scan_orphan_artifacts(
    db_dir: &Path,
    manifest: Option<&ManifestState>,
    referenced_wal_generations: &HashSet<u64>,
    include_orphan_segments: bool,
) -> OrphanScanOutcome {
    let manifest_segment_ids: HashSet<u64> = manifest
        .map(|manifest| manifest.segments.iter().map(|segment| segment.id).collect())
        .unwrap_or_default();
    let orphan_segment_candidates = scan_orphan_segment_candidates(db_dir, &manifest_segment_ids);
    let mut outcome = OrphanScanOutcome {
        findings: Vec::new(),
        orphan_segments: Vec::new(),
        total_components_checked: 0,
        total_components_ok: 0,
        total_components_failed: 0,
        total_bytes_digested: 0,
    };

    for candidate in orphan_segment_candidates {
        outcome.findings.push(DatabaseScrubFinding {
            component_kind: "segment".to_string(),
            finding_type: DatabaseScrubFindingType::OrphanSegment,
            severity: DatabaseScrubSeverity::Warning,
            detail: format!(
                "segment artifact is not referenced by the selected manifest: {}",
                candidate.path.display()
            ),
        });

        if include_orphan_segments {
            let deep = scrub_orphan_segment_unpinned(&candidate);
            if deep.component_checks_ran {
                outcome.findings.push(DatabaseScrubFinding {
                    component_kind: "segment".to_string(),
                    finding_type: DatabaseScrubFindingType::OrphanSegmentUnpinned,
                    severity: DatabaseScrubSeverity::Warning,
                    detail: format!(
                        "orphan segment was component-scrubbed without root manifest semantic checks: {}",
                        candidate.path.display()
                    ),
                });
                let failed = failed_component_count(&deep.result.findings);
                outcome.total_components_checked += deep.result.components_ok + failed;
                outcome.total_components_ok += deep.result.components_ok;
                outcome.total_components_failed += failed;
                outcome.total_bytes_digested += deep.result.bytes_digested;
            } else {
                outcome.findings.push(DatabaseScrubFinding {
                    component_kind: "segment".to_string(),
                    finding_type: DatabaseScrubFindingType::OrphanSegmentScrubSkipped,
                    severity: DatabaseScrubSeverity::Warning,
                    detail: format!(
                        "orphan segment could not be component-scrubbed: {}",
                        candidate.path.display()
                    ),
                });
            }
            outcome.orphan_segments.push(deep.result);
        }
    }

    for path in scan_orphan_wal_files(db_dir, referenced_wal_generations) {
        outcome.findings.push(DatabaseScrubFinding {
            component_kind: "wal".to_string(),
            finding_type: DatabaseScrubFindingType::OrphanWal,
            severity: DatabaseScrubSeverity::Warning,
            detail: format!(
                "WAL artifact is not referenced by the selected manifest: {}",
                path.display()
            ),
        });
    }

    outcome
}

fn scan_orphan_segment_candidates(
    db_dir: &Path,
    manifest_segment_ids: &HashSet<u64>,
) -> Vec<OrphanSegmentCandidate> {
    let seg_parent = db_dir.join("segments");
    let entries = match fs::read_dir(&seg_parent) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some((segment_id, is_tmp)) = parse_segment_artifact_name(&name) else {
            continue;
        };
        let is_orphan = is_tmp
            || segment_id
                .map(|id| !manifest_segment_ids.contains(&id))
                .unwrap_or(true);
        if is_orphan {
            candidates.push(OrphanSegmentCandidate {
                segment_id,
                path: entry.path(),
            });
        }
    }
    candidates.sort_by(|left, right| left.path.cmp(&right.path));
    candidates
}

fn parse_segment_artifact_name(name: &str) -> Option<(Option<u64>, bool)> {
    let rest = name.strip_prefix("seg_")?;
    let (id_str, is_tmp) = if let Some(id_str) = rest.strip_suffix(".tmp") {
        (id_str, true)
    } else {
        (rest, false)
    };
    let segment_id = id_str.parse::<u64>().ok();
    Some((segment_id, is_tmp))
}

fn scan_orphan_wal_files(db_dir: &Path, referenced_wal_generations: &HashSet<u64>) -> Vec<PathBuf> {
    let entries = match fs::read_dir(db_dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    let mut paths = Vec::new();
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(generation_id) = parse_wal_artifact_name(&name) else {
            continue;
        };
        if generation_id
            .map(|id| !referenced_wal_generations.contains(&id))
            .unwrap_or(true)
        {
            paths.push(entry.path());
        }
    }
    paths.sort();
    paths
}

fn parse_wal_artifact_name(name: &str) -> Option<Option<u64>> {
    let rest = name.strip_prefix("wal_")?;
    let id_str = rest.strip_suffix(".wal")?;
    Some(id_str.parse::<u64>().ok())
}

fn scrub_orphan_segment_unpinned(candidate: &OrphanSegmentCandidate) -> OrphanSegmentDeepScrub {
    let manifest_path = candidate.path.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
    let manifest_data = match fs::read(&manifest_path) {
        Ok(data) => data,
        Err(error) => {
            return OrphanSegmentDeepScrub {
                result: OrphanSegmentScrubResult {
                    segment_id: candidate.segment_id,
                    path: candidate.path.display().to_string(),
                    findings: vec![ComponentScrubFinding {
                        component_kind: "segment_manifest".into(),
                        finding_type: ScrubFindingType::FileMissing,
                        detail: format!("cannot read segment manifest: {error}"),
                    }],
                    components_ok: 0,
                    bytes_digested: 0,
                    semantic_checks_skipped: true,
                },
                component_checks_ran: false,
            };
        }
    };
    let manifest = match decode_manifest_envelope(&manifest_data) {
        Ok(manifest) => manifest,
        Err(error) => {
            return OrphanSegmentDeepScrub {
                result: OrphanSegmentScrubResult {
                    segment_id: candidate.segment_id,
                    path: candidate.path.display().to_string(),
                    findings: vec![ComponentScrubFinding {
                        component_kind: "segment_manifest".into(),
                        finding_type: ScrubFindingType::IoError,
                        detail: format!("cannot decode segment manifest: {error}"),
                    }],
                    components_ok: 0,
                    bytes_digested: 0,
                    semantic_checks_skipped: true,
                },
                component_checks_ran: false,
            };
        }
    };

    let mut findings = Vec::new();
    let mut components_ok = 0;
    let mut bytes_digested = 0;

    let packed_outcome = scrub_packed_core(&candidate.path, &manifest, &mut bytes_digested);
    components_ok += packed_outcome.components_ok;
    findings.extend(packed_outcome.findings);

    for record in &manifest.components {
        if record.kind == SegmentComponentKind::PackedSegmentContainer
            || matches!(record.handle, ComponentHandleV1::PackedRange { .. })
        {
            continue;
        }

        let component_findings = match &record.handle {
            ComponentHandleV1::ExternalFile { .. } => {
                scrub_external_component(&candidate.path, record, &manifest, &mut bytes_digested)
            }
            ComponentHandleV1::PackedRange { .. } => unreachable!("packed ranges handled above"),
        };

        if component_findings.is_empty() {
            components_ok += 1;
        } else {
            findings.extend(component_findings);
        }
    }

    OrphanSegmentDeepScrub {
        result: OrphanSegmentScrubResult {
            segment_id: Some(manifest.segment_id),
            path: candidate.path.display().to_string(),
            findings,
            components_ok,
            bytes_digested,
            semantic_checks_skipped: true,
        },
        component_checks_ran: true,
    }
}

fn failed_component_count(findings: &[ComponentScrubFinding]) -> u64 {
    findings
        .iter()
        .map(|finding| finding.component_kind.as_str())
        .collect::<HashSet<_>>()
        .len() as u64
}

fn recheck_selected_manifest_stability(
    db_dir: &Path,
    source: Option<ManifestScrubSource>,
    expected_len: Option<u64>,
    expected_digest: Option<[u8; 32]>,
) -> Option<DatabaseScrubFinding> {
    let (Some(source), Some(expected_len), Some(expected_digest)) =
        (source, expected_len, expected_digest)
    else {
        return None;
    };
    let name = manifest_source_name(source);
    let path = db_dir.join(name);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Some(DatabaseScrubFinding {
                component_kind: "manifest".to_string(),
                finding_type: DatabaseScrubFindingType::ManifestChangedDuringScrub,
                severity: DatabaseScrubSeverity::Warning,
                detail: format!(
                    "selected manifest {name} could not be reread at end of scrub: {error}"
                ),
            });
        }
    };
    let actual_len = bytes.len() as u64;
    let actual_digest: [u8; 32] = Sha256::digest(&bytes).into();
    if actual_len != expected_len || actual_digest != expected_digest {
        return Some(DatabaseScrubFinding {
            component_kind: "manifest".to_string(),
            finding_type: DatabaseScrubFindingType::ManifestChangedDuringScrub,
            severity: DatabaseScrubSeverity::Warning,
            detail: format!(
                "selected manifest {name} changed during scrub: length {expected_len}->{actual_len}"
            ),
        });
    }
    None
}

fn manifest_source_name(source: ManifestScrubSource) -> &'static str {
    match source {
        ManifestScrubSource::Current => MANIFEST_CURRENT,
        ManifestScrubSource::Tmp => MANIFEST_TMP,
        ManifestScrubSource::Prev => MANIFEST_PREV,
    }
}

#[cfg(test)]
fn set_manifest_stability_test_hook(db_dir: PathBuf, callback: impl FnOnce() + Send + 'static) {
    *MANIFEST_STABILITY_TEST_HOOK.lock().unwrap() = Some(ManifestStabilityTestHook {
        db_dir,
        callback: Box::new(callback),
    });
}

#[cfg(test)]
fn run_manifest_stability_test_hook(db_dir: &Path) {
    let hook = {
        let mut guard = MANIFEST_STABILITY_TEST_HOOK.lock().unwrap();
        if guard
            .as_ref()
            .is_some_and(|hook| hook.db_dir.as_path() == db_dir)
        {
            guard.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        (hook.callback)();
    }
}

#[cfg(not(test))]
fn run_manifest_stability_test_hook(_db_dir: &Path) {}

pub(crate) fn scrub_database(
    db_dir: &Path,
    manifest: &ManifestState,
) -> Result<ScrubReport, EngineError> {
    let start = Instant::now();

    let segment_results: Vec<SegmentScrubResult> = engine_cpu_install(|| {
        manifest
            .segments
            .par_iter()
            .map(|seg_info| scrub_one_segment(db_dir, manifest, seg_info))
            .collect()
    });

    let mut total_components_checked: u64 = 0;
    let mut total_components_ok: u64 = 0;
    let mut total_components_failed: u64 = 0;
    let mut total_bytes_digested: u64 = 0;

    for seg in &segment_results {
        let failed = seg
            .findings
            .iter()
            .map(|finding| finding.component_kind.as_str())
            .collect::<HashSet<_>>()
            .len() as u64;
        let ok = seg.components_ok;
        total_components_checked += ok + failed;
        total_components_ok += ok;
        total_components_failed += failed;
        total_bytes_digested += seg.bytes_digested;
    }

    Ok(ScrubReport {
        segments: segment_results,
        total_components_checked,
        total_components_ok,
        total_components_failed,
        total_bytes_digested,
        duration_ms: start.elapsed().as_millis() as u64,
    })
}

fn scrub_one_segment(
    db_dir: &Path,
    manifest_state: &ManifestState,
    seg_info: &SegmentInfo,
) -> SegmentScrubResult {
    let seg_dir = segment_dir(db_dir, seg_info.id);

    if !seg_dir.exists() {
        return SegmentScrubResult {
            segment_id: seg_info.id,
            findings: vec![ComponentScrubFinding {
                component_kind: "segment".into(),
                finding_type: ScrubFindingType::FileMissing,
                detail: format!("segment directory does not exist: {}", seg_dir.display()),
            }],
            components_ok: 0,
            bytes_digested: 0,
        };
    }

    let manifest_path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
    let manifest_data = match std::fs::read(&manifest_path) {
        Ok(d) => d,
        Err(e) => {
            return SegmentScrubResult {
                segment_id: seg_info.id,
                findings: vec![ComponentScrubFinding {
                    component_kind: "segment_manifest".into(),
                    finding_type: ScrubFindingType::FileMissing,
                    detail: format!("cannot read segment manifest: {e}"),
                }],
                components_ok: 0,
                bytes_digested: 0,
            };
        }
    };

    let manifest = match decode_manifest_envelope(&manifest_data) {
        Ok(m) => m,
        Err(e) => {
            return SegmentScrubResult {
                segment_id: seg_info.id,
                findings: vec![ComponentScrubFinding {
                    component_kind: "segment_manifest".into(),
                    finding_type: ScrubFindingType::IoError,
                    detail: format!("cannot decode segment manifest: {e}"),
                }],
                components_ok: 0,
                bytes_digested: 0,
            };
        }
    };

    let mut findings: Vec<ComponentScrubFinding> = Vec::new();
    let mut components_ok: u64 = 0;
    let mut bytes_digested: u64 = 0;

    if let Err(error) = validate_segment_manifest_identity(seg_info, &manifest) {
        findings.push(ComponentScrubFinding {
            component_kind: "segment".into(),
            finding_type: ScrubFindingType::SegmentIdentityMismatch,
            detail: error.to_string(),
        });
    }

    let packed_outcome = scrub_packed_core(&seg_dir, &manifest, &mut bytes_digested);
    components_ok += packed_outcome.components_ok;
    findings.extend(packed_outcome.findings);

    for record in &manifest.components {
        if record.kind == SegmentComponentKind::PackedSegmentContainer
            || matches!(record.handle, ComponentHandleV1::PackedRange { .. })
        {
            continue;
        }

        let component_findings = match &record.handle {
            ComponentHandleV1::ExternalFile { .. } => {
                scrub_external_component(&seg_dir, record, &manifest, &mut bytes_digested)
            }
            ComponentHandleV1::PackedRange { .. } => unreachable!("packed ranges handled above"),
        };

        if component_findings.is_empty() {
            components_ok += 1;
        } else {
            findings.extend(component_findings);
        }
    }

    let semantic_findings = scrub_segment_node_semantics(&seg_dir, manifest_state, seg_info);
    if semantic_findings.is_empty() {
        components_ok += 1;
    } else {
        findings.extend(semantic_findings);
    }

    SegmentScrubResult {
        segment_id: seg_info.id,
        findings,
        components_ok,
        bytes_digested,
    }
}

fn scrub_segment_node_semantics(
    seg_dir: &Path,
    manifest_state: &ManifestState,
    seg_info: &SegmentInfo,
) -> Vec<ComponentScrubFinding> {
    let mut findings = Vec::new();
    let reader = match SegmentReader::open_with_info(
        seg_dir,
        seg_info,
        manifest_state.dense_vector.as_ref(),
        &manifest_state.secondary_indexes,
    ) {
        Ok(reader) => reader,
        Err(error) => {
            findings.push(semantic_finding(format!(
                "cannot open segment for node semantic scrub: {error}"
            )));
            return findings;
        }
    };

    let known_label_ids: HashSet<u32> =
        manifest_state.node_label_tokens.values().copied().collect();
    let node_record_index = match reader.node_record_index_entries_for_scrub(seg_info.node_count) {
        Ok(entries) => entries,
        Err(error) => {
            findings.push(semantic_finding(format!(
                "cannot read node record index for semantic scrub: {error}"
            )));
            return findings;
        }
    };
    let meta_count = match reader.node_meta_count_for_scrub(seg_info.node_count) {
        Ok(count) => count,
        Err(error) => {
            findings.push(semantic_finding(format!(
                "cannot read node metadata count for semantic scrub: {error}"
            )));
            return findings;
        }
    };
    if node_record_index.len() != meta_count {
        findings.push(semantic_finding(format!(
            "node record index count {} does not match node metadata row count {}",
            node_record_index.len(),
            meta_count
        )));
    }

    let node_property_indexes: Vec<&SecondaryIndexManifestEntry> = manifest_state
        .secondary_indexes
        .iter()
        .filter(|entry| {
            entry.state == SecondaryIndexState::Ready
                && matches!(entry.target, SecondaryIndexTarget::NodeProperty { .. })
        })
        .collect();
    let mut expected_secondary_eq_groups: BTreeMap<u64, BTreeMap<u64, Vec<u64>>> = BTreeMap::new();
    let mut expected_secondary_range_entries: BTreeMap<u64, Vec<(NumericRangeSortKey, u64)>> =
        BTreeMap::new();
    for entry in &node_property_indexes {
        match entry.kind {
            SecondaryIndexKind::Equality => {
                expected_secondary_eq_groups
                    .entry(entry.index_id)
                    .or_default();
            }
            SecondaryIndexKind::Range => {
                expected_secondary_range_entries
                    .entry(entry.index_id)
                    .or_default();
            }
        }
    }

    let mut expected_label_entries: Vec<(u32, u64)> = Vec::new();
    let mut expected_key_entries: Vec<(u32, String, u64)> = Vec::new();
    let mut expected_timestamp_entries: Vec<(u32, i64, u64)> = Vec::new();
    let mut previous_node_id = None;

    for index in 0..meta_count {
        let meta = match reader.node_meta_at(index) {
            Ok(meta) => meta,
            Err(error) => {
                findings.push(semantic_finding(format!(
                    "cannot read node metadata row {}: {error}",
                    index
                )));
                continue;
            }
        };
        if previous_node_id.is_some_and(|previous| previous >= meta.node_id) {
            findings.push(semantic_finding(format!(
                "node metadata row {} is not sorted by unique node_id: previous {:?}, current {}",
                index, previous_node_id, meta.node_id
            )));
        }
        previous_node_id = Some(meta.node_id);
        let index_entry = node_record_index.get(index).copied();
        if index_entry.map(|(node_id, _)| node_id) != Some(meta.node_id) {
            findings.push(semantic_finding(format!(
                "node record index row {} id {:?} does not match metadata node_id {}",
                index,
                index_entry.map(|(node_id, _)| node_id),
                meta.node_id
            )));
        }
        if index_entry.map(|(_, data_offset)| data_offset) != Some(meta.data_offset) {
            findings.push(semantic_finding(format!(
                "node record index row {} offset {:?} does not match metadata data_offset {}",
                index,
                index_entry.map(|(_, data_offset)| data_offset),
                meta.data_offset
            )));
        }

        let node = match reader.node_record_for_meta_scrub(&meta) {
            Ok(node) => node,
            Err(error) => {
                findings.push(semantic_finding(format!(
                    "cannot decode node record {} from metadata span: {error}",
                    meta.node_id
                )));
                continue;
            }
        };
        if node.label_ids != meta.label_ids {
            findings.push(semantic_finding(format!(
                "node {} record labels {:?} do not match metadata labels {:?}",
                meta.node_id, node.label_ids, meta.label_ids
            )));
        }
        if node.key.len() != meta.key_len as usize {
            findings.push(semantic_finding(format!(
                "node {} record key length {} does not match metadata key_len {}",
                meta.node_id,
                node.key.len(),
                meta.key_len
            )));
        }
        if node.updated_at != meta.updated_at {
            findings.push(semantic_finding(format!(
                "node {} record updated_at {} does not match metadata updated_at {}",
                meta.node_id, node.updated_at, meta.updated_at
            )));
        }
        if node.weight.to_bits() != meta.weight.to_bits() {
            findings.push(semantic_finding(format!(
                "node {} record weight {} does not match metadata weight {}",
                meta.node_id, node.weight, meta.weight
            )));
        }

        for &label_id in meta.label_ids.as_slice() {
            if !known_label_ids.contains(&label_id) {
                findings.push(semantic_finding(format!(
                    "node {} references node label_id {} that is absent from the manifest catalog",
                    meta.node_id, label_id
                )));
            }
            expected_label_entries.push((label_id, meta.node_id));
            expected_key_entries.push((label_id, node.key.clone(), meta.node_id));
            expected_timestamp_entries.push((label_id, meta.updated_at, meta.node_id));
        }

        for entry in &node_property_indexes {
            let SecondaryIndexTarget::NodeProperty { label_id, prop_key } = &entry.target else {
                continue;
            };
            if !meta.label_ids.contains(*label_id) {
                continue;
            }
            let Some(value) = node.props.get(prop_key) else {
                continue;
            };
            match entry.kind {
                SecondaryIndexKind::Equality => {
                    expected_secondary_eq_groups
                        .entry(entry.index_id)
                        .or_default()
                        .entry(hash_prop_equality_key(value))
                        .or_default()
                        .push(meta.node_id);
                }
                SecondaryIndexKind::Range => {
                    if let Some(encoded_value) = numeric_range_sort_key_for_value(value) {
                        expected_secondary_range_entries
                            .entry(entry.index_id)
                            .or_default()
                            .push((encoded_value, meta.node_id));
                    }
                }
            }
        }
    }

    expected_label_entries.sort_unstable();
    expected_key_entries.sort();
    expected_timestamp_entries.sort_unstable();
    for groups in expected_secondary_eq_groups.values_mut() {
        for ids in groups.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
    }
    for entries in expected_secondary_range_entries.values_mut() {
        entries.sort_unstable();
        entries.dedup();
    }

    match reader.node_label_index_entries_for_scrub(seg_info.node_count) {
        Ok(actual) => compare_semantic_entries(
            "node label index",
            &expected_label_entries,
            &actual,
            &mut findings,
        ),
        Err(error) => findings.push(semantic_finding(format!(
            "cannot read node label index for semantic scrub: {error}"
        ))),
    }
    match reader.node_key_index_entries_for_scrub() {
        Ok(actual) => compare_semantic_entries(
            "node key index",
            &expected_key_entries,
            &actual,
            &mut findings,
        ),
        Err(error) => findings.push(semantic_finding(format!(
            "cannot read node key index for semantic scrub: {error}"
        ))),
    }
    match reader.node_timestamp_index_entries_for_scrub() {
        Ok(actual) => compare_semantic_entries(
            "node timestamp index",
            &expected_timestamp_entries,
            &actual,
            &mut findings,
        ),
        Err(error) => findings.push(semantic_finding(format!(
            "cannot read node timestamp index for semantic scrub: {error}"
        ))),
    }
    scrub_declared_node_property_indexes(
        &reader,
        &node_property_indexes,
        &expected_secondary_eq_groups,
        &expected_secondary_range_entries,
        &mut findings,
    );

    findings
}

fn scrub_declared_node_property_indexes(
    reader: &SegmentReader,
    node_property_indexes: &[&SecondaryIndexManifestEntry],
    expected_secondary_eq_groups: &BTreeMap<u64, BTreeMap<u64, Vec<u64>>>,
    expected_secondary_range_entries: &BTreeMap<u64, Vec<(NumericRangeSortKey, u64)>>,
    findings: &mut Vec<ComponentScrubFinding>,
) {
    for entry in node_property_indexes {
        match entry.kind {
            SecondaryIndexKind::Equality => {
                let expected = expected_secondary_eq_groups
                    .get(&entry.index_id)
                    .cloned()
                    .unwrap_or_default();
                let mut actual = BTreeMap::new();
                match reader.for_each_secondary_eq_group(entry.index_id, |value_hash, ids| {
                    actual.insert(value_hash, ids.to_vec());
                    Ok(())
                }) {
                    Ok(true) => {
                        let name =
                            format!("declared node-property equality index {}", entry.index_id);
                        let expected_entries: Vec<(u64, Vec<u64>)> =
                            expected.into_iter().collect();
                        let actual_entries: Vec<(u64, Vec<u64>)> = actual.into_iter().collect();
                        compare_semantic_entries(
                            &name,
                            &expected_entries,
                            &actual_entries,
                            findings,
                        );
                    }
                    Ok(false) => {
                        if entry.state == SecondaryIndexState::Ready || !expected.is_empty() {
                            findings.push(semantic_finding(format!(
                                "declared node-property equality index {} sidecar is missing",
                                entry.index_id
                            )));
                        }
                    }
                    Err(error) => findings.push(semantic_finding(format!(
                        "cannot read declared node-property equality index {} for semantic scrub: {error}",
                        entry.index_id
                    ))),
                }
            }
            SecondaryIndexKind::Range => {
                let expected = expected_secondary_range_entries
                    .get(&entry.index_id)
                    .cloned()
                    .unwrap_or_default();
                let mut actual = Vec::new();
                match reader.for_each_secondary_range_entry(entry.index_id, |encoded_value, node_id| {
                    actual.push((encoded_value, node_id));
                    Ok(())
                }) {
                    Ok(true) => {
                        let name =
                            format!("declared node-property range index {}", entry.index_id);
                        compare_semantic_entries(&name, &expected, &actual, findings);
                    }
                    Ok(false) => {
                        if entry.state == SecondaryIndexState::Ready || !expected.is_empty() {
                            findings.push(semantic_finding(format!(
                                "declared node-property range index {} sidecar is missing",
                                entry.index_id
                            )));
                        }
                    }
                    Err(error) => findings.push(semantic_finding(format!(
                        "cannot read declared node-property range index {} for semantic scrub: {error}",
                        entry.index_id
                    ))),
                }
            }
        }
    }
}

fn semantic_finding(detail: String) -> ComponentScrubFinding {
    ComponentScrubFinding {
        component_kind: "NodeSemantic".into(),
        finding_type: ScrubFindingType::SemanticMismatch,
        detail,
    }
}

fn compare_semantic_entries<T: Debug + PartialEq>(
    name: &str,
    expected: &[T],
    actual: &[T],
    findings: &mut Vec<ComponentScrubFinding>,
) {
    if expected == actual {
        return;
    }
    let mismatch_index = expected
        .iter()
        .zip(actual.iter())
        .position(|(left, right)| left != right)
        .unwrap_or_else(|| expected.len().min(actual.len()));
    findings.push(semantic_finding(format!(
        "{} mismatch: expected {} entries, found {}; first mismatch at {} expected {:?}, found {:?}",
        name,
        expected.len(),
        actual.len(),
        mismatch_index,
        expected.get(mismatch_index),
        actual.get(mismatch_index)
    )));
}

fn scrub_external_component(
    seg_dir: &Path,
    record: &crate::segment_components::SegmentComponentRecordV1,
    manifest: &crate::segment_components::SegmentComponentManifestV1,
    bytes_digested: &mut u64,
) -> Vec<ComponentScrubFinding> {
    let mut findings = Vec::new();
    let kind_name = format!("{:?}", record.kind);

    let (relative_path, payload_offset, payload_len) = match &record.handle {
        ComponentHandleV1::ExternalFile {
            relative_path,
            payload_offset,
            payload_len,
        } => (relative_path, *payload_offset, *payload_len),
        _ => return findings,
    };

    let file_path = seg_dir.join(relative_path);
    let mut file = match File::open(&file_path) {
        Ok(file) => file,
        Err(e) => {
            findings.push(ComponentScrubFinding {
                component_kind: kind_name,
                finding_type: ScrubFindingType::FileMissing,
                detail: format!("cannot open component file {relative_path}: {e}"),
            });
            return findings;
        }
    };
    let file_len = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(e) => {
            findings.push(ComponentScrubFinding {
                component_kind: kind_name,
                finding_type: ScrubFindingType::IoError,
                detail: format!("cannot stat component file {relative_path}: {e}"),
            });
            return findings;
        }
    };

    if payload_offset > 0 {
        match read_identity_header_from_file(&mut file) {
            Ok(header) => {
                findings.extend(validate_header_vs_record(&header, record, manifest));
            }
            Err(e) => {
                findings.push(ComponentScrubFinding {
                    component_kind: kind_name.clone(),
                    finding_type: ScrubFindingType::IdentityHeaderMismatch,
                    detail: format!("cannot decode identity header: {e}"),
                });
            }
        }
    }

    let payload_end =
        match checked_component_payload_end(&kind_name, payload_offset, payload_len, file_len) {
            Ok(end) => end,
            Err(finding) => {
                findings.push(finding);
                payload_offset.saturating_add(payload_len).min(file_len)
            }
        };

    if let Some(expected_digest) = &record.payload_digest {
        if payload_offset
            .checked_add(payload_len)
            .is_some_and(|expected_end| payload_end == expected_end && expected_end <= file_len)
        {
            match streaming_sha256(&mut file, payload_offset, payload_len) {
                Ok(actual_digest) => {
                    *bytes_digested += payload_len;
                    if actual_digest != *expected_digest {
                        findings.push(ComponentScrubFinding {
                            component_kind: kind_name.clone(),
                            finding_type: ScrubFindingType::PayloadDigestMismatch,
                            detail: format!(
                                "payload SHA-256 mismatch: expected {}, got {}",
                                hex_short(expected_digest),
                                hex_short(&actual_digest),
                            ),
                        });
                    }
                }
                Err(e) => {
                    findings.push(ComponentScrubFinding {
                        component_kind: kind_name.clone(),
                        finding_type: ScrubFindingType::IoError,
                        detail: format!("I/O error computing payload digest: {e}"),
                    });
                }
            }
        }
    }

    findings.extend(validate_component_record_metadata(
        record, manifest, &kind_name,
    ));

    findings
}

fn checked_component_payload_end(
    kind_name: &str,
    payload_offset: u64,
    payload_len: u64,
    file_len: u64,
) -> Result<u64, ComponentScrubFinding> {
    let Some(end) = payload_offset.checked_add(payload_len) else {
        return Err(ComponentScrubFinding {
            component_kind: kind_name.into(),
            finding_type: ScrubFindingType::RangeOverflow,
            detail: format!(
                "payload range overflows: offset={}, len={}",
                payload_offset, payload_len
            ),
        });
    };
    if end != file_len {
        return Err(ComponentScrubFinding {
            component_kind: kind_name.into(),
            finding_type: ScrubFindingType::RangeOverflow,
            detail: format!(
                "payload range [{}, {}) does not match file length {}",
                payload_offset, end, file_len
            ),
        });
    }
    Ok(end)
}

fn read_identity_header_from_file(file: &mut File) -> std::io::Result<ComponentIdentityHeaderV1> {
    file.seek(SeekFrom::Start(0))?;
    let mut hdr_buf = [0u8; COMPONENT_IDENTITY_HEADER_LEN];
    file.read_exact(&mut hdr_buf)?;
    decode_identity_header(&hdr_buf)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))
}

fn validate_component_record_metadata(
    record: &SegmentComponentRecordV1,
    manifest: &SegmentComponentManifestV1,
    kind_name: &str,
) -> Vec<ComponentScrubFinding> {
    let mut findings = Vec::new();

    let recomputed_dep_digest = dependency_digest(&record.dependencies);
    if recomputed_dep_digest != record.dependency_digest {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.into(),
            finding_type: ScrubFindingType::DependencyDigestMismatch,
            detail: "recomputed dependency digest does not match stored value".into(),
        });
    }

    let recomputed_id = component_id(
        manifest.segment_id,
        &record.kind,
        record.logical_format_version,
        record.payload_len,
        record.payload_digest.as_ref(),
        &record.dependency_digest,
        record.build_fingerprint,
    );
    if recomputed_id != record.component_id {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.into(),
            finding_type: ScrubFindingType::ComponentIdMismatch,
            detail: "recomputed component_id does not match stored value".into(),
        });
    }

    findings
}

fn scrub_packed_core(
    seg_dir: &Path,
    manifest: &SegmentComponentManifestV1,
    bytes_digested: &mut u64,
) -> PackedScrubOutcome {
    let relevant_indices: Vec<usize> = manifest
        .components
        .iter()
        .enumerate()
        .filter_map(|(index, record)| {
            if record.kind == SegmentComponentKind::PackedSegmentContainer
                || matches!(record.handle, ComponentHandleV1::PackedRange { .. })
            {
                Some(index)
            } else {
                None
            }
        })
        .collect();
    if relevant_indices.is_empty() {
        return PackedScrubOutcome::default();
    }

    let mut per_record_findings: Vec<Vec<ComponentScrubFinding>> =
        (0..manifest.components.len()).map(|_| Vec::new()).collect();
    let Some(container_index) = manifest
        .components
        .iter()
        .position(|record| record.kind == SegmentComponentKind::PackedSegmentContainer)
    else {
        for &index in &relevant_indices {
            let kind_name = format!("{:?}", manifest.components[index].kind);
            per_record_findings[index].push(ComponentScrubFinding {
                component_kind: kind_name,
                finding_type: ScrubFindingType::FileMissing,
                detail: "packed range has no segment.core container record".into(),
            });
        }
        return finish_packed_outcome(manifest, relevant_indices, per_record_findings);
    };

    let container_record = &manifest.components[container_index];
    let container_kind_name = format!("{:?}", container_record.kind);
    per_record_findings[container_index].extend(validate_component_record_metadata(
        container_record,
        manifest,
        &container_kind_name,
    ));

    let (relative_path, payload_offset, payload_len) = match &container_record.handle {
        ComponentHandleV1::ExternalFile {
            relative_path,
            payload_offset,
            payload_len,
        } => (relative_path, *payload_offset, *payload_len),
        ComponentHandleV1::PackedRange { .. } => {
            per_record_findings[container_index].push(ComponentScrubFinding {
                component_kind: container_kind_name,
                finding_type: ScrubFindingType::RangeOverflow,
                detail: "packed container must use an external file handle".into(),
            });
            mark_packed_ranges_unverified(
                manifest,
                &relevant_indices,
                &mut per_record_findings,
                ScrubFindingType::RangeOverflow,
                "packed range could not be validated because segment.core container record is not an external file",
            );
            return finish_packed_outcome(manifest, relevant_indices, per_record_findings);
        }
    };

    if relative_path != PACKED_CORE_FILENAME {
        per_record_findings[container_index].push(ComponentScrubFinding {
            component_kind: container_kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!("packed container path must be {PACKED_CORE_FILENAME}"),
        });
    }

    let core_path = seg_dir.join(relative_path);
    let mut file = match File::open(&core_path) {
        Ok(file) => file,
        Err(e) => {
            per_record_findings[container_index].push(ComponentScrubFinding {
                component_kind: container_kind_name.clone(),
                finding_type: ScrubFindingType::FileMissing,
                detail: format!("cannot open segment.core: {e}"),
            });
            mark_packed_ranges_unverified(
                manifest,
                &relevant_indices,
                &mut per_record_findings,
                ScrubFindingType::FileMissing,
                format!("packed range could not be validated because segment.core could not be opened: {e}"),
            );
            return finish_packed_outcome(manifest, relevant_indices, per_record_findings);
        }
    };
    let file_len = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(e) => {
            per_record_findings[container_index].push(ComponentScrubFinding {
                component_kind: container_kind_name.clone(),
                finding_type: ScrubFindingType::IoError,
                detail: format!("cannot stat segment.core: {e}"),
            });
            mark_packed_ranges_unverified(
                manifest,
                &relevant_indices,
                &mut per_record_findings,
                ScrubFindingType::IoError,
                format!("packed range could not be validated because segment.core metadata could not be read: {e}"),
            );
            return finish_packed_outcome(manifest, relevant_indices, per_record_findings);
        }
    };

    match read_identity_header_from_file(&mut file) {
        Ok(header) => {
            per_record_findings[container_index].extend(validate_header_vs_record(
                &header,
                container_record,
                manifest,
            ));
        }
        Err(e) => {
            per_record_findings[container_index].push(ComponentScrubFinding {
                component_kind: container_kind_name.clone(),
                finding_type: ScrubFindingType::IdentityHeaderMismatch,
                detail: format!("cannot decode identity header: {e}"),
            });
        }
    }

    let payload_end = match checked_component_payload_end(
        &container_kind_name,
        payload_offset,
        payload_len,
        file_len,
    ) {
        Ok(end) => Some(end),
        Err(finding) => {
            per_record_findings[container_index].push(finding);
            payload_offset
                .checked_add(payload_len)
                .filter(|end| *end <= file_len)
        }
    };

    let mut range_targets = Vec::new();
    for (index, record) in manifest.components.iter().enumerate() {
        let ComponentHandleV1::PackedRange {
            container_component_id,
            offset,
            len,
        } = &record.handle
        else {
            continue;
        };
        let kind_name = format!("{:?}", record.kind);
        per_record_findings[index].extend(validate_component_record_metadata(
            record, manifest, &kind_name,
        ));
        if *container_component_id != container_record.component_id {
            per_record_findings[index].push(ComponentScrubFinding {
                component_kind: kind_name.clone(),
                finding_type: ScrubFindingType::ContainerIdMismatch,
                detail: format!(
                    "packed range container_component_id {} does not match container {}",
                    hex_short(container_component_id),
                    hex_short(&container_record.component_id),
                ),
            });
        }
        if record.payload_len != *len {
            per_record_findings[index].push(ComponentScrubFinding {
                component_kind: kind_name.clone(),
                finding_type: ScrubFindingType::RangeOverflow,
                detail: format!(
                    "packed range length {} does not match record payload_len {}",
                    len, record.payload_len
                ),
            });
        }
        let Some(end) = offset.checked_add(*len) else {
            per_record_findings[index].push(ComponentScrubFinding {
                component_kind: kind_name,
                finding_type: ScrubFindingType::RangeOverflow,
                detail: format!("packed range overflows: offset={}, len={}", offset, len),
            });
            continue;
        };
        if end > payload_len {
            per_record_findings[index].push(ComponentScrubFinding {
                component_kind: kind_name,
                finding_type: ScrubFindingType::RangeOverflow,
                detail: format!(
                    "packed range [{}, {}) exceeds segment.core payload length {}",
                    offset, end, payload_len
                ),
            });
            continue;
        }
        if per_record_findings[index].iter().any(|finding| {
            matches!(
                finding.finding_type,
                ScrubFindingType::RangeOverflow | ScrubFindingType::ContainerIdMismatch
            )
        }) {
            continue;
        }
        range_targets.push(PackedRangeDigestTarget {
            record_index: index,
            kind_name,
            start: *offset,
            end,
            hasher: Sha256::new(),
        });
    }

    range_targets.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
            .then_with(|| {
                manifest.components[left.record_index]
                    .kind
                    .kind_tag()
                    .cmp(&manifest.components[right.record_index].kind.kind_tag())
            })
            .then_with(|| {
                manifest.components[left.record_index]
                    .kind
                    .index_id()
                    .cmp(&manifest.components[right.record_index].kind.index_id())
            })
    });

    let mut overlapped = HashSet::new();
    let overlap_targets: Vec<&PackedRangeDigestTarget> = range_targets
        .iter()
        .filter(|target| target.start < target.end)
        .collect();
    for pair in overlap_targets.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if current.start < previous.end {
            let detail = format!(
                "packed component ranges overlap: previous=[{}, {}), current=[{}, {})",
                previous.start, previous.end, current.start, current.end
            );
            for target in [previous, current] {
                if overlapped.insert(target.record_index) {
                    per_record_findings[target.record_index].push(ComponentScrubFinding {
                        component_kind: target.kind_name.clone(),
                        finding_type: ScrubFindingType::RangeOverlap,
                        detail: detail.clone(),
                    });
                }
            }
        }
    }
    range_targets.retain(|target| !overlapped.contains(&target.record_index));

    let mut range_digests_verified = false;
    if payload_end.is_some() {
        match stream_packed_core_payload(&mut file, payload_offset, payload_len, &mut range_targets)
        {
            Ok(container_digest) => {
                range_digests_verified = true;
                *bytes_digested += payload_len;
                if let Some(expected_digest) = &container_record.payload_digest {
                    if container_digest != *expected_digest {
                        per_record_findings[container_index].push(ComponentScrubFinding {
                            component_kind: container_kind_name.clone(),
                            finding_type: ScrubFindingType::PayloadDigestMismatch,
                            detail: format!(
                                "payload SHA-256 mismatch: expected {}, got {}",
                                hex_short(expected_digest),
                                hex_short(&container_digest),
                            ),
                        });
                    }
                }
            }
            Err(e) => {
                per_record_findings[container_index].push(ComponentScrubFinding {
                    component_kind: container_kind_name.clone(),
                    finding_type: ScrubFindingType::IoError,
                    detail: format!("I/O error computing segment.core payload digest: {e}"),
                });
                mark_packed_ranges_unverified(
                    manifest,
                    &relevant_indices,
                    &mut per_record_findings,
                    ScrubFindingType::IoError,
                    format!(
                        "packed range could not be validated because segment.core streaming failed: {e}"
                    ),
                );
            }
        }
    } else {
        mark_packed_ranges_unverified(
            manifest,
            &relevant_indices,
            &mut per_record_findings,
            ScrubFindingType::RangeOverflow,
            "packed range could not be validated because segment.core payload range is invalid",
        );
    }

    if range_digests_verified {
        for target in range_targets {
            let record = &manifest.components[target.record_index];
            if let Some(expected_digest) = &record.payload_digest {
                let actual_digest: [u8; 32] = target.hasher.finalize().into();
                if actual_digest != *expected_digest {
                    per_record_findings[target.record_index].push(ComponentScrubFinding {
                        component_kind: target.kind_name,
                        finding_type: ScrubFindingType::PayloadDigestMismatch,
                        detail: format!(
                            "payload SHA-256 mismatch: expected {}, got {}",
                            hex_short(expected_digest),
                            hex_short(&actual_digest),
                        ),
                    });
                }
            }
        }
    }

    finish_packed_outcome(manifest, relevant_indices, per_record_findings)
}

#[derive(Default)]
struct PackedScrubOutcome {
    findings: Vec<ComponentScrubFinding>,
    components_ok: u64,
}

struct PackedRangeDigestTarget {
    record_index: usize,
    kind_name: String,
    start: u64,
    end: u64,
    hasher: Sha256,
}

fn finish_packed_outcome(
    manifest: &SegmentComponentManifestV1,
    relevant_indices: Vec<usize>,
    per_record_findings: Vec<Vec<ComponentScrubFinding>>,
) -> PackedScrubOutcome {
    let mut outcome = PackedScrubOutcome::default();
    for index in relevant_indices {
        if per_record_findings[index].is_empty() {
            outcome.components_ok += 1;
        } else {
            outcome.findings.extend(per_record_findings[index].clone());
        }
    }
    debug_assert!(
        outcome.components_ok as usize + outcome.findings.len() >= 1
            || manifest.components.is_empty()
    );
    outcome
}

fn mark_packed_ranges_unverified(
    manifest: &SegmentComponentManifestV1,
    relevant_indices: &[usize],
    per_record_findings: &mut [Vec<ComponentScrubFinding>],
    finding_type: ScrubFindingType,
    detail: impl Into<String>,
) {
    let detail = detail.into();
    for &index in relevant_indices {
        let record = &manifest.components[index];
        if matches!(record.handle, ComponentHandleV1::PackedRange { .. }) {
            per_record_findings[index].push(ComponentScrubFinding {
                component_kind: format!("{:?}", record.kind),
                finding_type,
                detail: detail.clone(),
            });
        }
    }
}

fn stream_packed_core_payload(
    file: &mut File,
    payload_offset: u64,
    payload_len: u64,
    targets: &mut [PackedRangeDigestTarget],
) -> std::io::Result<[u8; 32]> {
    file.seek(SeekFrom::Start(payload_offset))?;
    let mut container_hasher = Sha256::new();
    let mut remaining = payload_len;
    let mut relative_pos = 0u64;
    let mut target_cursor = 0usize;
    let mut buf = vec![0u8; SCRUB_READ_BUFFER_SIZE];
    while remaining > 0 {
        let to_read = remaining.min(SCRUB_READ_BUFFER_SIZE as u64) as usize;
        let n = file.read(&mut buf[..to_read])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF during packed core scrub digest",
            ));
        }
        let chunk = &buf[..n];
        container_hasher.update(chunk);
        let chunk_start = relative_pos;
        let chunk_end = relative_pos + n as u64;

        while target_cursor < targets.len() && targets[target_cursor].end <= chunk_start {
            target_cursor += 1;
        }
        let mut index = target_cursor;
        while index < targets.len() && targets[index].start < chunk_end {
            let overlap_start = targets[index].start.max(chunk_start);
            let overlap_end = targets[index].end.min(chunk_end);
            if overlap_start < overlap_end {
                let local_start = (overlap_start - chunk_start) as usize;
                let local_end = (overlap_end - chunk_start) as usize;
                targets[index].hasher.update(&chunk[local_start..local_end]);
            }
            index += 1;
        }

        relative_pos = chunk_end;
        remaining -= n as u64;
    }
    Ok(container_hasher.finalize().into())
}

fn streaming_sha256(file: &mut File, offset: u64, len: u64) -> std::io::Result<[u8; 32]> {
    file.seek(SeekFrom::Start(offset))?;
    let mut hasher = Sha256::new();
    let mut remaining = len;
    let mut buf = vec![0u8; SCRUB_READ_BUFFER_SIZE];
    while remaining > 0 {
        let to_read = (remaining as usize).min(SCRUB_READ_BUFFER_SIZE);
        let n = file.read(&mut buf[..to_read])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF during scrub digest",
            ));
        }
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    Ok(hasher.finalize().into())
}

fn validate_header_vs_record(
    header: &ComponentIdentityHeaderV1,
    record: &crate::segment_components::SegmentComponentRecordV1,
    manifest: &crate::segment_components::SegmentComponentManifestV1,
) -> Vec<ComponentScrubFinding> {
    let mut findings = Vec::new();
    let kind_name = format!("{:?}", record.kind);

    if header.segment_id != manifest.segment_id {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header segment_id {} != manifest segment_id {}",
                header.segment_id, manifest.segment_id,
            ),
        });
    }

    if header.component_kind != record.kind {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header component_kind {:?} != record {:?}",
                header.component_kind, record.kind
            ),
        });
    }

    if let ComponentHandleV1::ExternalFile { payload_offset, .. } = &record.handle {
        if header.payload_offset != *payload_offset {
            findings.push(ComponentScrubFinding {
                component_kind: kind_name.clone(),
                finding_type: ScrubFindingType::IdentityHeaderMismatch,
                detail: format!(
                    "header payload_offset {} != record {}",
                    header.payload_offset, payload_offset
                ),
            });
        }
    }

    if header.component_id != record.component_id {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: "header component_id does not match manifest record".into(),
        });
    }

    if header.dependency_digest != record.dependency_digest {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: "header dependency_digest does not match manifest record".into(),
        });
    }

    if header.build_fingerprint != record.build_fingerprint {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header build_fingerprint {} != record {}",
                header.build_fingerprint, record.build_fingerprint,
            ),
        });
    }

    if header.payload_len != record.payload_len {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header payload_len {} != record {}",
                header.payload_len, record.payload_len,
            ),
        });
    }

    if header.logical_format_version != record.logical_format_version {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header logical_format_version {} != record {}",
                header.logical_format_version, record.logical_format_version,
            ),
        });
    }

    if header.segment_format_version != manifest.segment_format_version {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header segment_format_version {} != manifest {}",
                header.segment_format_version, manifest.segment_format_version,
            ),
        });
    }

    if header.created_generation != record.created_generation {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name.clone(),
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: format!(
                "header created_generation {} != record {}",
                header.created_generation, record.created_generation,
            ),
        });
    }

    if header.payload_digest != record.payload_digest {
        findings.push(ComponentScrubFinding {
            component_kind: kind_name,
            finding_type: ScrubFindingType::IdentityHeaderMismatch,
            detail: "header payload_digest does not match manifest record".into(),
        });
    }

    findings
}

fn hex_short(digest: &[u8; 32]) -> String {
    format!("{}..{}", hex_byte(digest[0]), hex_byte(digest[31]),)
}

fn hex_byte(b: u8) -> String {
    format!("{b:02x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_components::{
        encode_manifest_envelope, SegmentComponentManifestV1, SegmentComponentRecordV1,
    };
    use crate::types::{NodeSchemaManifestEntry, SchemaAdditionalPropertiesManifest};
    use crate::{
        DatabaseEngine, DbOptions, NodeInput, PropValue, ScrubFindingType, SecondaryIndexField,
        SecondaryIndexKind, SecondaryIndexSpec, UpsertEdgeOptions, UpsertNodeOptions,
    };
    use std::collections::BTreeMap;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use tempfile::TempDir;

    fn open_test_db(dir: &Path) -> DatabaseEngine {
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

    fn populated_db() -> (TempDir, PathBuf, DatabaseEngine) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let db = open_test_db(&db_path);
        populate_and_flush(&db);
        (dir, db_path, db)
    }

    fn valid_manifest_with_next(next_node_id: u64) -> ManifestState {
        ManifestState {
            next_node_id,
            next_edge_id: next_node_id,
            ..crate::manifest::default_manifest()
        }
    }

    fn write_manifest_candidate(dir: &Path, name: &str, manifest: &ManifestState) -> Vec<u8> {
        let bytes = serde_json::to_vec_pretty(manifest).unwrap();
        std::fs::write(dir.join(name), &bytes).unwrap();
        bytes
    }

    fn count_manifest_finding(
        load: &ScrubManifestLoad,
        component_kind: &str,
        finding_type: DatabaseScrubFindingType,
    ) -> usize {
        load.findings
            .iter()
            .filter(|finding| {
                finding.component_kind == component_kind && finding.finding_type == finding_type
            })
            .count()
    }

    fn has_manifest_finding(
        load: &ScrubManifestLoad,
        finding_type: DatabaseScrubFindingType,
    ) -> bool {
        load.findings
            .iter()
            .any(|finding| finding.finding_type == finding_type)
    }

    fn first_seg_dir(db_path: &Path) -> PathBuf {
        db_path.join("segments").join("seg_0001")
    }

    fn read_segment_manifest(seg_dir: &Path) -> SegmentComponentManifestV1 {
        let data = std::fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        decode_manifest_envelope(&data).unwrap()
    }

    fn write_segment_manifest(seg_dir: &Path, manifest: &SegmentComponentManifestV1) {
        let data = encode_manifest_envelope(manifest).unwrap();
        std::fs::write(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME), data).unwrap();
    }

    fn write_test_bytes_at(path: &Path, offset: usize, bytes: &[u8]) {
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    fn any_finding(report: &ScrubReport, finding_type: ScrubFindingType) -> bool {
        report
            .segments
            .iter()
            .flat_map(|segment| &segment.findings)
            .any(|finding| finding.finding_type == finding_type)
    }

    fn any_finding_detail(
        report: &ScrubReport,
        finding_type: ScrubFindingType,
        detail: &str,
    ) -> bool {
        report
            .segments
            .iter()
            .flat_map(|segment| &segment.findings)
            .any(|finding| finding.finding_type == finding_type && finding.detail.contains(detail))
    }

    fn assert_scrub_reports_segment_identity_mismatch(
        mutate: impl FnOnce(&mut SegmentInfo, &mut SegmentComponentManifestV1),
    ) {
        let (_dir, db_path, db) = populated_db();
        let root_manifest = db.manifest().unwrap();
        let mut root_segment = root_manifest.segments[0].clone();
        let seg_dir = first_seg_dir(&db_path);
        let mut local_manifest = read_segment_manifest(&seg_dir);
        mutate(&mut root_segment, &mut local_manifest);
        write_segment_manifest(&seg_dir, &local_manifest);

        let result = scrub_one_segment(db.path(), &root_manifest, &root_segment);
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.finding_type == ScrubFindingType::SegmentIdentityMismatch),
            "expected segment identity mismatch, got: {:?}",
            result.findings
        );
    }

    fn first_external_non_container_record_mut(
        manifest: &mut SegmentComponentManifestV1,
    ) -> &mut SegmentComponentRecordV1 {
        manifest
            .components
            .iter_mut()
            .find(|record| {
                record.kind != SegmentComponentKind::PackedSegmentContainer
                    && matches!(record.handle, ComponentHandleV1::ExternalFile { .. })
            })
            .expect("test precondition: expected an external sidecar")
    }

    fn read_test_u64(data: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
    }

    fn packed_component_payload_start(
        manifest: &SegmentComponentManifestV1,
        kind: SegmentComponentKind,
    ) -> usize {
        let container_payload_offset = manifest
            .components
            .iter()
            .find_map(|record| {
                if record.kind == SegmentComponentKind::PackedSegmentContainer {
                    if let ComponentHandleV1::ExternalFile { payload_offset, .. } = &record.handle {
                        return Some(*payload_offset);
                    }
                }
                None
            })
            .expect("test precondition: packed container must be external");
        let component_offset = manifest
            .components
            .iter()
            .find_map(|record| {
                if record.kind == kind {
                    if let ComponentHandleV1::PackedRange { offset, .. } = &record.handle {
                        return Some(*offset);
                    }
                }
                None
            })
            .expect("test precondition: component must be packed");
        (container_payload_offset + component_offset) as usize
    }

    fn packed_node_records_payload_start(manifest: &SegmentComponentManifestV1) -> usize {
        packed_component_payload_start(manifest, SegmentComponentKind::NodeRecords)
    }

    fn external_component_payload_offset(
        manifest: &SegmentComponentManifestV1,
        kind: SegmentComponentKind,
    ) -> usize {
        manifest
            .components
            .iter()
            .find_map(|record| {
                if record.kind == kind {
                    if let ComponentHandleV1::ExternalFile { payload_offset, .. } = record.handle {
                        return Some(payload_offset as usize);
                    }
                }
                None
            })
            .expect("test precondition: component must be external")
    }

    fn create_declared_node_property_scrub_db() -> (TempDir, PathBuf, DatabaseEngine, u64, u64) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let db = open_test_db(&db_path);
        let eq = db
            .ensure_node_property_index(
                "Researcher",
                SecondaryIndexSpec {
                    fields: vec![SecondaryIndexField::Property {
                        key: ("status").to_string(),
                    }],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        let range = db
            .ensure_node_property_index(
                "Researcher",
                SecondaryIndexSpec {
                    fields: vec![SecondaryIndexField::Property {
                        key: ("score").to_string(),
                    }],
                    kind: SecondaryIndexKind::Range,
                },
            )
            .unwrap();
        db.shutdown_secondary_index_worker();

        let nodes: Vec<NodeInput> = (0..4)
            .map(|i| {
                let mut props = BTreeMap::new();
                props.insert(
                    "status".to_string(),
                    PropValue::String(if i % 2 == 0 { "active" } else { "idle" }.to_string()),
                );
                props.insert("score".to_string(), PropValue::Int(i as i64 + 10));
                NodeInput {
                    labels: vec!["Person".to_string(), "Researcher".to_string()],
                    key: format!("node_{i}"),
                    props,
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                }
            })
            .collect();
        db.batch_upsert_nodes(nodes).unwrap();
        db.flush().unwrap();
        db.with_runtime_manifest_write(|manifest| {
            for entry in &mut manifest.secondary_indexes {
                if entry.index_id == eq.index_id || entry.index_id == range.index_id {
                    entry.state = SecondaryIndexState::Ready;
                    entry.last_error = None;
                }
            }
            Ok(())
        })
        .unwrap();
        (dir, db_path, db, eq.index_id, range.index_id)
    }

    #[test]
    fn scrub_manifest_scan_selects_valid_current() {
        let dir = TempDir::new().unwrap();
        write_manifest_candidate(dir.path(), MANIFEST_CURRENT, &valid_manifest_with_next(10));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Current));
        assert_eq!(load.manifest.unwrap().next_node_id, 10);
        assert!(load.findings.is_empty());
        assert_eq!(
            load.source_len.unwrap(),
            std::fs::metadata(dir.path().join(MANIFEST_CURRENT))
                .unwrap()
                .len()
        );
        assert!(load.source_digest.is_some());
    }

    #[test]
    fn scrub_manifest_scan_selects_tmp_without_promoting_it() {
        let dir = TempDir::new().unwrap();
        write_manifest_candidate(dir.path(), MANIFEST_TMP, &valid_manifest_with_next(20));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Tmp));
        assert_eq!(load.manifest.unwrap().next_node_id, 20);
        assert!(dir.path().join(MANIFEST_TMP).exists());
        assert!(!dir.path().join(MANIFEST_CURRENT).exists());
    }

    #[test]
    fn scrub_manifest_scan_selects_prev_without_rewriting_it() {
        let dir = TempDir::new().unwrap();
        let prev_bytes =
            write_manifest_candidate(dir.path(), MANIFEST_PREV, &valid_manifest_with_next(30));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Prev));
        assert_eq!(load.manifest.unwrap().next_node_id, 30);
        assert_eq!(
            std::fs::read(dir.path().join(MANIFEST_PREV)).unwrap(),
            prev_bytes
        );
        assert!(!dir.path().join(MANIFEST_CURRENT).exists());
        assert!(!dir.path().join(MANIFEST_TMP).exists());
    }

    #[test]
    fn scrub_manifest_scan_records_invalid_current_and_fallback() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(MANIFEST_CURRENT), b"NOT VALID JSON {{{").unwrap();
        write_manifest_candidate(dir.path(), MANIFEST_PREV, &valid_manifest_with_next(40));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Prev));
        assert_eq!(
            count_manifest_finding(
                &load,
                MANIFEST_CURRENT,
                DatabaseScrubFindingType::ManifestFileInvalid
            ),
            1
        );
        assert!(has_manifest_finding(
            &load,
            DatabaseScrubFindingType::ManifestFallbackUsed
        ));
    }

    #[test]
    fn scrub_manifest_scan_records_validation_invalid_current() {
        let dir = TempDir::new().unwrap();
        let mut invalid = valid_manifest_with_next(50);
        invalid.label_token_schema_version = 0;
        write_manifest_candidate(dir.path(), MANIFEST_CURRENT, &invalid);
        write_manifest_candidate(dir.path(), MANIFEST_PREV, &valid_manifest_with_next(51));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Prev));
        let current_finding = load
            .findings
            .iter()
            .find(|finding| {
                finding.component_kind == MANIFEST_CURRENT
                    && finding.finding_type == DatabaseScrubFindingType::ManifestFileInvalid
            })
            .expect("validation-invalid current must be recorded");
        assert_eq!(current_finding.severity, DatabaseScrubSeverity::Error);
        assert!(current_finding
            .detail
            .contains("missing label token schema"));
    }

    #[test]
    fn scrub_manifest_scan_records_read_error_current() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(MANIFEST_CURRENT)).unwrap();
        write_manifest_candidate(dir.path(), MANIFEST_PREV, &valid_manifest_with_next(55));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Prev));
        let current_finding = load
            .findings
            .iter()
            .find(|finding| {
                finding.component_kind == MANIFEST_CURRENT
                    && finding.finding_type == DatabaseScrubFindingType::ManifestFileInvalid
            })
            .expect("read-error current must be recorded");
        assert_eq!(current_finding.severity, DatabaseScrubSeverity::Error);
        assert!(current_finding.detail.contains("cannot read"));
        assert!(has_manifest_finding(
            &load,
            DatabaseScrubFindingType::ManifestFallbackUsed
        ));
    }

    #[test]
    fn scrub_manifest_scan_records_normalization_invalid_current() {
        let dir = TempDir::new().unwrap();
        let mut invalid = valid_manifest_with_next(56);
        invalid.node_label_tokens.insert("Person".to_string(), 1);
        invalid.next_node_label_id = 2;
        invalid.schema_catalog_version = 0;
        invalid.node_schemas.push(NodeSchemaManifestEntry {
            schema_id: 1,
            revision: 1,
            label_id: 1,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            key: None,
            label_constraints: None,
            weight: None,
            dense_vector: None,
            sparse_vector: None,
        });
        write_manifest_candidate(dir.path(), MANIFEST_CURRENT, &invalid);
        write_manifest_candidate(dir.path(), MANIFEST_PREV, &valid_manifest_with_next(57));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Prev));
        let current_finding = load
            .findings
            .iter()
            .find(|finding| {
                finding.component_kind == MANIFEST_CURRENT
                    && finding.finding_type == DatabaseScrubFindingType::ManifestFileInvalid
            })
            .expect("normalization-invalid current must be recorded");
        assert_eq!(current_finding.severity, DatabaseScrubSeverity::Error);
        assert!(current_finding
            .detail
            .contains("schema_catalog_version 0 cannot contain schema entries"));
        assert!(has_manifest_finding(
            &load,
            DatabaseScrubFindingType::ManifestFallbackUsed
        ));
    }

    #[test]
    fn scrub_manifest_scan_records_invalid_current_and_tmp_before_prev() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(MANIFEST_CURRENT), b"NOT VALID JSON {{{").unwrap();
        let mut invalid_tmp = valid_manifest_with_next(60);
        invalid_tmp.next_node_label_id = 0;
        write_manifest_candidate(dir.path(), MANIFEST_TMP, &invalid_tmp);
        write_manifest_candidate(dir.path(), MANIFEST_PREV, &valid_manifest_with_next(61));

        let load = load_manifest_for_scrub(dir.path());

        assert_eq!(load.source, Some(ManifestScrubSource::Prev));
        assert_eq!(
            count_manifest_finding(
                &load,
                MANIFEST_CURRENT,
                DatabaseScrubFindingType::ManifestFileInvalid
            ),
            1
        );
        assert_eq!(
            count_manifest_finding(
                &load,
                MANIFEST_TMP,
                DatabaseScrubFindingType::ManifestFileInvalid
            ),
            1
        );
    }

    #[test]
    fn scrub_manifest_scan_reports_missing_when_all_absent() {
        let dir = TempDir::new().unwrap();

        let load = load_manifest_for_scrub(dir.path());

        assert!(load.manifest.is_none());
        assert!(load.source.is_none());
        assert_eq!(load.findings.len(), 1);
        assert!(has_manifest_finding(
            &load,
            DatabaseScrubFindingType::ManifestMissing
        ));
    }

    #[test]
    fn scrub_manifest_scan_reports_all_invalid_candidates_and_missing() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(MANIFEST_CURRENT), b"NOT VALID JSON {{{").unwrap();
        std::fs::write(dir.path().join(MANIFEST_TMP), b"NOT VALID JSON {{{").unwrap();
        let mut invalid_prev = valid_manifest_with_next(70);
        invalid_prev.label_token_schema_version = 0;
        write_manifest_candidate(dir.path(), MANIFEST_PREV, &invalid_prev);

        let load = load_manifest_for_scrub(dir.path());

        assert!(load.manifest.is_none());
        assert_eq!(
            load.findings
                .iter()
                .filter(|finding| {
                    finding.finding_type == DatabaseScrubFindingType::ManifestFileInvalid
                })
                .count(),
            3
        );
        assert!(has_manifest_finding(
            &load,
            DatabaseScrubFindingType::ManifestMissing
        ));
    }

    #[test]
    fn scrub_manifest_scan_does_not_write_normalized_defaults() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": {},
  "edge_label_tokens": {},
  "next_node_label_id": 1,
  "next_edge_label_id": 1,
  "segments": [],
  "next_node_id": 10,
  "next_edge_id": 20,
  "prune_policies": {},
  "next_engine_seq": 0,
  "next_wal_generation_id": 0,
  "active_wal_generation_id": 0,
  "pending_flush_epochs": []
}"#;
        let path = dir.path().join(MANIFEST_CURRENT);
        std::fs::write(&path, legacy_json).unwrap();

        let load = load_manifest_for_scrub(dir.path());

        let manifest = load.manifest.unwrap();
        assert_eq!(load.source, Some(ManifestScrubSource::Current));
        assert_eq!(
            manifest.schema_catalog_version,
            crate::types::SCHEMA_CATALOG_VERSION
        );
        assert!(manifest.secondary_indexes.is_empty());
        assert_eq!(std::fs::read_to_string(path).unwrap(), legacy_json);
    }

    #[test]
    fn path_scrub_implementation_does_not_call_open_or_mutating_helpers() {
        let source = include_str!("scrub.rs");
        let implementation = source.split("\n#[cfg(test)]\nmod tests").next().unwrap();
        let wal_source = include_str!("wal.rs");
        let wal_scanner = wal_source
            .split("pub(crate) fn scrub_generation_readonly")
            .nth(1)
            .unwrap()
            .split("impl WalReader")
            .next()
            .unwrap();
        for forbidden in [
            concat!("DatabaseEngine", "::open("),
            concat!("EngineCore", "::open("),
            concat!("WalWriter", "::open_generation("),
            concat!("WalReader", "::"),
            concat!("read_generation_recoverable", "("),
            concat!("replay_wal_generation_to_memtable", "("),
            concat!("write_manifest", "("),
            concat!("load_manifest", "("),
            concat!("load_manifest_readonly", "("),
            concat!("try_load_manifest_file", "("),
            concat!("remove_wal_generation", "("),
            concat!("truncate_wal_generation_to", "("),
            concat!("cleanup_orphan_segments", "("),
            concat!("cleanup_orphan_wal_files", "("),
            concat!("cleanup_orphan_optional_component_files", "("),
            concat!("remove_file", "("),
            concat!("remove_dir_all", "("),
            concat!("rename", "("),
        ] {
            for (name, code) in [
                ("path-level scrub implementation", implementation),
                ("read-only WAL scrub scanner", wal_scanner),
            ] {
                assert!(
                    !code.contains(forbidden),
                    "{name} must not contain {forbidden}"
                );
            }
        }
    }

    #[test]
    fn scrub_path_reports_selected_manifest_changed_during_scrub() {
        let dir = TempDir::new().unwrap();
        write_manifest_candidate(dir.path(), MANIFEST_CURRENT, &valid_manifest_with_next(80));
        let db_dir = dir.path().to_path_buf();
        let hook_dir = db_dir.clone();
        set_manifest_stability_test_hook(db_dir.clone(), move || {
            write_manifest_candidate(&hook_dir, MANIFEST_CURRENT, &valid_manifest_with_next(81));
        });

        let report = scrub_path_with_options(
            &db_dir,
            &ScrubPathOptions {
                validate_wal: false,
                include_orphan_segments: false,
                check_manifest_stability: true,
            },
        )
        .unwrap();

        assert!(report.findings.iter().any(|finding| {
            finding.finding_type == DatabaseScrubFindingType::ManifestChangedDuringScrub
                && finding.severity == DatabaseScrubSeverity::Warning
        }));
    }

    #[test]
    fn scrub_semantic_comparator_reports_index_divergence_without_payload_corruption() {
        let mut findings = Vec::new();
        compare_semantic_entries(
            "node label index",
            &[(1u32, 1u64), (2, 1)],
            &[(1u32, 1u64)],
            &mut findings,
        );
        compare_semantic_entries(
            "node key index",
            &[(1u32, "alice".to_string(), 1u64)],
            &[(1u32, "bob".to_string(), 1u64)],
            &mut findings,
        );
        compare_semantic_entries(
            "node timestamp index",
            &[(1u32, 100i64, 1u64)],
            &[(1u32, 101i64, 1u64)],
            &mut findings,
        );

        assert_eq!(findings.len(), 3);
        for finding in &findings {
            assert_eq!(finding.finding_type, ScrubFindingType::SemanticMismatch);
            assert_eq!(finding.component_kind, "NodeSemantic");
        }
        assert!(findings[0].detail.contains("node label index mismatch"));
        assert!(findings[1].detail.contains("node key index mismatch"));
        assert!(findings[2].detail.contains("node timestamp index mismatch"));
    }

    #[test]
    fn scrub_detects_multi_label_node_record_metadata_mismatch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let db = open_test_db(&db_path);
        let nodes: Vec<NodeInput> = (0..4)
            .map(|i| NodeInput {
                labels: vec!["Person".to_string(), "Researcher".to_string()],
                key: format!("node_{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        db.batch_upsert_nodes(nodes).unwrap();
        db.flush().unwrap();

        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let node_records_payload_start = packed_node_records_payload_start(&manifest);
        let core_path = seg_dir.join(PACKED_CORE_FILENAME);
        let core = std::fs::read(&core_path).unwrap();
        let first_record_offset = read_test_u64(&core, node_records_payload_start + 8 + 8) as usize;
        let first_label_id_offset = node_records_payload_start + first_record_offset + 1;
        write_test_bytes_at(&core_path, first_label_id_offset, &0u32.to_le_bytes());

        let report = db.scrub().unwrap();
        assert!(
            any_finding(&report, ScrubFindingType::SemanticMismatch),
            "expected semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_detects_node_record_index_offset_metadata_mismatch() {
        let (_dir, db_path, db) = populated_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let node_records_payload_start = packed_node_records_payload_start(&manifest);
        let core_path = seg_dir.join(PACKED_CORE_FILENAME);
        let core = std::fs::read(&core_path).unwrap();
        let first_offset_pos = node_records_payload_start + 8 + 8;
        let second_offset_pos = node_records_payload_start + 8 + 16 + 8;
        let second_record_offset = read_test_u64(&core, second_offset_pos);
        write_test_bytes_at(
            &core_path,
            first_offset_pos,
            &second_record_offset.to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "node record index row 0 offset"
            ),
            "expected node record index offset semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_detects_node_label_index_overlapping_posting_ranges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let db = open_test_db(&db_path);
        let nodes: Vec<NodeInput> = (0..4)
            .map(|i| NodeInput {
                labels: vec!["Person".to_string(), "Researcher".to_string()],
                key: format!("node_{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        db.batch_upsert_nodes(nodes).unwrap();
        db.flush().unwrap();

        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let node_label_index_start =
            packed_component_payload_start(&manifest, SegmentComponentKind::NodeLabelIndex);
        let core_path = seg_dir.join(PACKED_CORE_FILENAME);
        let core = std::fs::read(&core_path).unwrap();
        let label_count = read_test_u64(&core, node_label_index_start);
        assert!(
            label_count >= 2,
            "test precondition: expected at least two node-label rows"
        );
        let first_posting_offset = read_test_u64(&core, node_label_index_start + 8 + 4);
        let second_posting_offset_pos = node_label_index_start + 8 + 16 + 4;
        write_test_bytes_at(
            &core_path,
            second_posting_offset_pos,
            &first_posting_offset.to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "node label index posting range"
            ),
            "expected node label index posting range semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_bounds_node_record_count_before_semantic_iteration() {
        let (_dir, db_path, db) = populated_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let node_records_payload_start =
            packed_component_payload_start(&manifest, SegmentComponentKind::NodeRecords);
        let core_path = seg_dir.join(PACKED_CORE_FILENAME);
        write_test_bytes_at(
            &core_path,
            node_records_payload_start,
            &u64::MAX.to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "node records count"
            ),
            "expected bounded node record count semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_bounds_node_metadata_count_before_semantic_iteration() {
        let (_dir, db_path, db) = populated_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let node_metadata_payload_start =
            packed_component_payload_start(&manifest, SegmentComponentKind::NodeMetadata);
        let core_path = seg_dir.join(PACKED_CORE_FILENAME);
        write_test_bytes_at(
            &core_path,
            node_metadata_payload_start,
            &u64::MAX.to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "node metadata row count"
            ),
            "expected bounded node metadata count semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_accepts_healthy_multi_label_declared_node_property_sidecars() {
        let (_dir, _db_path, db, _eq_index_id, _range_index_id) =
            create_declared_node_property_scrub_db();

        let report = db.scrub().unwrap();
        assert!(
            report.segments[0].findings.is_empty(),
            "unexpected findings: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_does_not_require_missing_building_declared_node_property_sidecar() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let db = open_test_db(&db_path);
        let mut props = BTreeMap::new();
        props.insert(
            "status".to_string(),
            PropValue::String("active".to_string()),
        );
        db.upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props,
                ..Default::default()
            },
        )
        .unwrap();
        db.flush().unwrap();

        let (ready_rx, release_tx) = db.set_secondary_index_build_pause();
        let info = db
            .ensure_node_property_index(
                "Person",
                SecondaryIndexSpec {
                    fields: vec![SecondaryIndexField::Property {
                        key: ("status").to_string(),
                    }],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        assert_eq!(info.state, SecondaryIndexState::Building);
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let report = db.scrub().unwrap();
        assert!(
            report.segments[0].findings.is_empty(),
            "unexpected findings for missing building sidecar: {:?}",
            report.segments[0].findings
        );

        release_tx.send(()).unwrap();
        db.shutdown_secondary_index_worker();
    }

    #[test]
    fn scrub_detects_declared_node_property_equality_sidecar_semantic_mismatch() {
        let (_dir, db_path, db, eq_index_id, _range_index_id) =
            create_declared_node_property_scrub_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let payload_offset = external_component_payload_offset(
            &manifest,
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: eq_index_id,
            },
        );
        let sidecar_path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_eq_{eq_index_id}.dat"));
        let sidecar = std::fs::read(&sidecar_path).unwrap();
        let group_payload_offset = read_test_u64(&sidecar, payload_offset + 16) as usize;
        write_test_bytes_at(
            &sidecar_path,
            payload_offset + group_payload_offset,
            &999_999u64.to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "declared node-property equality index"
            ),
            "expected declared equality sidecar semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_detects_missing_declared_node_property_membership() {
        let (_dir, db_path, db, eq_index_id, _range_index_id) =
            create_declared_node_property_scrub_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let payload_offset = external_component_payload_offset(
            &manifest,
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: eq_index_id,
            },
        );
        let sidecar_path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_eq_{eq_index_id}.dat"));
        let sidecar = std::fs::read(&sidecar_path).unwrap();
        let id_count_offset = payload_offset + 24;
        let id_count = u32::from_le_bytes(
            sidecar[id_count_offset..id_count_offset + 4]
                .try_into()
                .unwrap(),
        );
        assert!(
            id_count > 1,
            "test precondition: expected at least two node IDs in the first equality group"
        );
        write_test_bytes_at(
            &sidecar_path,
            id_count_offset,
            &(id_count - 1).to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "declared node-property equality index"
            ),
            "expected declared equality sidecar missing-membership mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_detects_declared_node_property_range_sidecar_semantic_mismatch() {
        let (_dir, db_path, db, _eq_index_id, range_index_id) =
            create_declared_node_property_scrub_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let payload_offset = external_component_payload_offset(
            &manifest,
            SegmentComponentKind::NodePropertyRangeIndex {
                index_id: range_index_id,
            },
        );
        let sidecar_path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_range_{range_index_id}.dat"));
        write_test_bytes_at(
            &sidecar_path,
            payload_offset + 16,
            &999_999u64.to_le_bytes(),
        );

        let report = db.scrub().unwrap();
        assert!(
            any_finding_detail(
                &report,
                ScrubFindingType::SemanticMismatch,
                "declared node-property range index"
            ),
            "expected declared range sidecar semantic mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_detects_packed_container_identity_header_tamper() {
        let (_dir, db_path, db) = populated_db();
        let core_path = first_seg_dir(&db_path).join(PACKED_CORE_FILENAME);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&core_path)
            .unwrap();
        file.seek(SeekFrom::Start(56)).unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.sync_all().unwrap();

        let report = db.scrub().unwrap();
        assert!(
            any_finding(&report, ScrubFindingType::IdentityHeaderMismatch),
            "expected container identity header mismatch, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_detects_packed_container_trailing_bytes() {
        let (_dir, db_path, db) = populated_db();
        let core_path = first_seg_dir(&db_path).join(PACKED_CORE_FILENAME);

        let mut file = OpenOptions::new().append(true).open(&core_path).unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.sync_all().unwrap();

        let report = db.scrub().unwrap();
        assert!(
            report.segments.iter().flat_map(|s| &s.findings).any(|f| {
                f.component_kind == "PackedSegmentContainer"
                    && f.finding_type == ScrubFindingType::RangeOverflow
            }),
            "expected packed container range finding, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_marks_packed_ranges_failed_when_container_missing() {
        let (_dir, db_path, db) = populated_db();
        let seg_dir = first_seg_dir(&db_path);
        let manifest = read_segment_manifest(&seg_dir);
        let packed_kinds: Vec<String> = manifest
            .components
            .iter()
            .filter(|record| matches!(record.handle, ComponentHandleV1::PackedRange { .. }))
            .map(|record| format!("{:?}", record.kind))
            .collect();
        assert!(
            !packed_kinds.is_empty(),
            "test precondition: expected packed components"
        );

        std::fs::remove_file(seg_dir.join(PACKED_CORE_FILENAME)).unwrap();

        let report = db.scrub().unwrap();
        assert!(
            report.segments[0].findings.iter().any(|finding| {
                finding.component_kind == "PackedSegmentContainer"
                    && finding.finding_type == ScrubFindingType::FileMissing
            }),
            "expected missing packed container finding, got: {:?}",
            report.segments[0].findings
        );
        for kind in packed_kinds {
            assert!(
                report
                    .segments
                    .iter()
                    .flat_map(|segment| &segment.findings)
                    .any(|finding| finding.component_kind == kind),
                "expected packed range {kind} to be marked unverified, got: {:?}",
                report.segments[0].findings
            );
        }
    }

    #[test]
    fn scrub_does_not_treat_zero_length_packed_ranges_as_overlaps() {
        let (_dir, db_path, db) = populated_db();
        let seg_dir = first_seg_dir(&db_path);
        let mut manifest = read_segment_manifest(&seg_dir);
        let (node_offset, _) = manifest
            .components
            .iter()
            .find_map(|record| {
                if record.kind == SegmentComponentKind::NodeRecords {
                    if let ComponentHandleV1::PackedRange { offset, len, .. } = &record.handle {
                        return Some((*offset, *len));
                    }
                }
                None
            })
            .expect("test precondition: NodeRecords should be packed");
        let segment_id = manifest.segment_id;
        let empty_digest: [u8; 32] = Sha256::new().finalize().into();
        let record = manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeWeightIndex)
            .expect("test precondition: expected packed edge weight index");
        let ComponentHandleV1::PackedRange { offset, len, .. } = &mut record.handle else {
            panic!("EdgeWeightIndex should be packed");
        };
        *offset = node_offset;
        *len = 0;
        record.payload_len = 0;
        record.payload_digest = Some(empty_digest);
        record.component_id = component_id(
            segment_id,
            &record.kind,
            record.logical_format_version,
            record.payload_len,
            record.payload_digest.as_ref(),
            &record.dependency_digest,
            record.build_fingerprint,
        );
        write_segment_manifest(&seg_dir, &manifest);

        let report = db.scrub().unwrap();
        assert!(
            !report.segments.iter().flat_map(|s| &s.findings).any(|f| {
                f.component_kind == "EdgeWeightIndex"
                    && f.finding_type == ScrubFindingType::RangeOverlap
            }),
            "zero-length packed range should not overlap, got: {:?}",
            report.segments[0].findings
        );
    }

    #[test]
    fn scrub_reports_root_local_segment_identity_mismatches() {
        assert_scrub_reports_segment_identity_mismatch(|_, local| {
            local.node_count += 1;
        });
        assert_scrub_reports_segment_identity_mismatch(|_, local| {
            local.segment_data_id = [9; 32];
        });
        assert_scrub_reports_segment_identity_mismatch(|root, _| {
            root.segment_data_id = [7; 32];
        });
        assert_scrub_reports_segment_identity_mismatch(|_, local| {
            local.segment_format_version += 1;
        });
        assert_scrub_reports_segment_identity_mismatch(|_, local| {
            local.segment_id += 100;
        });
    }

    #[test]
    fn scrub_summary_counts_failed_components_not_findings() {
        let (_dir, db_path, db) = populated_db();
        let seg_dir = first_seg_dir(&db_path);
        let mut manifest = read_segment_manifest(&seg_dir);
        let record = first_external_non_container_record_mut(&mut manifest);
        let digest = record
            .payload_digest
            .as_mut()
            .expect("test precondition: sidecar should have a payload digest");
        digest[0] ^= 0x7F;
        write_segment_manifest(&seg_dir, &manifest);

        let report = db.scrub().unwrap();
        assert!(
            report.segments[0].findings.len() > 1,
            "test precondition: expected multiple findings for one component, got: {:?}",
            report.segments[0].findings
        );
        assert_eq!(report.total_components_failed, 1);
    }
}
