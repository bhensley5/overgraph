use crate::error::EngineError;
use crate::parallel::engine_cpu_install;
use crate::property_value_semantics::{
    hash_prop_equality_key, numeric_range_sort_key_for_value, NumericRangeSortKey,
};
use crate::segment_components::{
    component_id, decode_identity_header, decode_manifest_envelope, dependency_digest,
    ComponentHandleV1, ComponentIdentityHeaderV1, SegmentComponentKind, SegmentComponentManifestV1,
    SegmentComponentRecordV1, COMPONENT_IDENTITY_HEADER_LEN, PACKED_CORE_FILENAME,
    SEGMENT_COMPONENT_MANIFEST_FILENAME,
};
use crate::segment_reader::{validate_segment_manifest_identity, SegmentReader};
use crate::segment_writer::segment_dir;
use crate::types::{
    ComponentScrubFinding, ManifestState, ScrubFindingType, ScrubReport, SecondaryIndexKind,
    SecondaryIndexManifestEntry, SecondaryIndexState, SecondaryIndexTarget, SegmentInfo,
    SegmentScrubResult,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;

const SCRUB_READ_BUFFER_SIZE: usize = 64 * 1024;

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
