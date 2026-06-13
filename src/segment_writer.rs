use crate::degree_cache::{
    write_folded_degree_delta_sidecar_payload_from_sidecars,
    write_sorted_degree_delta_sidecar_payload, DegreeDelta, DegreeOverlaySnapshot,
    DEGREE_DELTA_FILENAME,
};
use crate::dense_hnsw::{
    build_dense_hnsw_from_points, write_prebuilt_hnsw_to_writers, BuiltHnsw, DensePointInput,
};
use crate::dense_hnsw::{DENSE_HNSW_GRAPH_FILENAME, DENSE_HNSW_META_FILENAME};
use crate::edge_metadata::EdgeMetadataIndexEntries;
use crate::error::EngineError;
use crate::memtable::{AdjEntry, Memtable};
use crate::parallel::{engine_cpu_join, engine_cpu_try_join};
use crate::planner_stats::{
    assemble_compaction_stats_from_partials, assemble_flush_stats_from_partials,
    build_compaction_stats_core_partial, build_flush_stats_core_partial,
    compound_index_stats_from_written_entries, equality_index_stats_from_written_groups,
    planner_stats_sidecar_payload, range_index_stats_from_written_entries,
    DeclaredIndexRuntimeCoverageState, DeclaredIndexStatsEvidence, PlannerStatsDeclaredIndexTarget,
    PLANNER_STATS_FILENAME,
};
use crate::property_value_semantics::{
    hash_prop_equality_key, numeric_range_sort_key_for_value, NumericRangeSortKey,
};
use crate::row_projection::{EdgeSelectedFieldNeeds, NodeSelectedFieldNeeds, PropertySelection};
use crate::secondary_index_key::{
    compound_secondary_failure_message, compound_secondary_failure_message_from_str,
    encode_compound_tuple_key, write_compound_sidecar_payload, CompoundFieldValue,
    CompoundSidecarDeclaration, CompoundSidecarTargetKind, CompoundTupleContext,
};
use crate::segment_components::{
    component_build_fingerprint, component_id, decode_identity_header, decode_manifest_envelope,
    dependency_digest, encode_identity_header, encode_manifest_envelope,
    is_packed_core_component_kind, is_refreshable_external_component_kind,
    patch_packed_range_container_id, secondary_declaration_dependency,
    secondary_index_component_dependencies_for_entry, secondary_index_component_kind_for_entry,
    secondary_index_declaration_fingerprint_for_entry, segment_source_groups_from_records,
    source_component_dependency, source_group_dependency, validate_packed_core_records_contract,
    ComponentAvailability, ComponentDependencyV1, ComponentFallbackClass, ComponentHandleV1,
    ComponentIdentityHeaderV1, ComponentIdentityWriter, ComponentRequirement, ComponentTrustClass,
    SegmentComponentBuildKind, SegmentComponentKind, SegmentComponentManifestV1,
    SegmentComponentRecordV1, SegmentComponentSourceGroups, SegmentSourceGroupKind,
    COMPONENT_IDENTITY_HEADER_LEN, PACKED_CORE_FILENAME, PACKED_CORE_TMP_FILENAME,
    SEGMENT_COMPONENT_MANIFEST_FILENAME, SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION,
    SEGMENT_COMPONENT_MANIFEST_TMP_FILENAME, ZERO_DIGEST,
};
use crate::segment_reader::SegmentReader;
use crate::sparse_postings::{
    write_sparse_posting_files_to_writers, SPARSE_POSTINGS_FILENAME, SPARSE_POSTING_INDEX_FILENAME,
};
use crate::types::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// --- Binary write helpers (little-endian) ---

fn write_u8(w: &mut impl Write, v: u8) -> Result<(), EngineError> {
    w.write_all(&[v])?;
    Ok(())
}

fn write_u16(w: &mut impl Write, v: u16) -> Result<(), EngineError> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u32(w: &mut impl Write, v: u32) -> Result<(), EngineError> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u64(w: &mut impl Write, v: u64) -> Result<(), EngineError> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

// --- Segment format version ---

/// Current segment format version.
/// v1: original format
/// v2: added valid_from/valid_to to edge records
/// v3: added valid_from/valid_to to adjacency postings
/// v4: BTreeMap props, removed redundant ID from records, delta-encoded adjacency
/// v5: metadata payloads (node metadata, edge metadata, node property hash metadata)
/// v6: optional node vector sidecars/blobs
/// v7: optional dense HNSW sidecars (dense_hnsw_meta.dat, dense_hnsw_graph.dat)
/// v8: optional sparse posting-list sidecars (sparse_posting_index.dat, sparse_postings.dat)
/// v9: last_write_seq in node_meta (60B), edge_meta (80B), tombstones (25B)
/// v10: component identity manifest, dependency-gated sidecars, and packed core segment file.
///      No checked mmap, no component CRC block tables, no read-time byte verification.
pub const SEGMENT_FORMAT_VERSION: u32 = 10;

pub(crate) const NODE_VECTOR_META_ENTRY_SIZE: usize = 28;
pub(crate) const SECONDARY_INDEX_DIRNAME: &str = "secondary_indexes";
static OPTIONAL_REFRESH_TMP_NONCE: AtomicU64 = AtomicU64::new(1);
const NODE_VECTOR_FLAG_DENSE: u8 = 0b0000_0001;
const NODE_VECTOR_FLAG_SPARSE: u8 = 0b0000_0010;
const NODE_META_HEADER_SIZE: u64 = 48;
const NODE_META_FIXED_ENTRY_SIZE: u16 = 48;
const NODE_META_LABEL_OFFSET_ENTRY_SIZE: u16 = 8;

// --- Segment file format constants ---

/// Size of a node index entry: node_id (8) + offset (8) = 16 bytes
const NODE_INDEX_ENTRY_SIZE: u64 = 16;
/// Size of an edge index entry: edge_id (8) + offset (8) = 16 bytes
const EDGE_INDEX_ENTRY_SIZE: u64 = 16;
/// Size of a label posting index entry: label_id (4) + offset (8) + count (4) = 16 bytes
const LABEL_POSTING_INDEX_ENTRY_SIZE: u64 = 16;
const SECONDARY_EQ_ENTRY_SIZE: u64 = 20;
const DENSE_VECTOR_VALUE_SIZE: u64 = 4;
const SPARSE_VECTOR_ENTRY_SIZE: u64 = 8;

pub(crate) type RecordDataSpan = (u64, u64, u32);
type RecordDataSpans = Vec<RecordDataSpan>;
type CompactionDatOutput = (SegmentComponentRecordV1, RecordDataSpans);
type AdjacencyGroupKey = (u64, u32);
type AdjacencyPosting = (u64, u64, f32, i64, i64);
type AdjacencyGroups = BTreeMap<AdjacencyGroupKey, Vec<AdjacencyPosting>>;

struct KeyIndexPayloadPlan<'a> {
    entries: Vec<KeyIndexEntryPlan<'a>>,
}

struct KeyIndexEntryPlan<'a> {
    label_id: u32,
    key: &'a [u8],
    node_id: u64,
    encoded_len: u64,
}

struct LabelPostingIndexPayloadPlan {
    groups: Vec<(u32, Vec<u64>)>,
}

struct TimestampIndexPayloadPlan {
    entries: Vec<(u32, i64, u64)>,
}

struct EdgeTripleIndexPayloadPlan {
    entries: Vec<(u64, u64, u32, u64)>,
}

#[derive(Clone)]
struct AdjacencyPayloadPlan {
    groups: Vec<AdjacencyGroupPlan>,
}

#[derive(Clone)]
struct AdjacencyGroupPlan {
    node_id: u64,
    label_id: u32,
    offset: u64,
    postings: Vec<AdjacencyPosting>,
}

struct NodeVectorSourcePlan {
    rows: Vec<NodeVectorSourceRow>,
    has_dense: bool,
    has_sparse: bool,
    dense_points: Vec<DensePointInput>,
}

#[derive(Clone, Copy)]
struct NodeVectorSourceRow {
    node_id: u64,
    flags: u8,
    dense_offset: u64,
    dense_len: u32,
    sparse_offset: u64,
    sparse_len: u32,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct SecondaryIndexMaintenanceReport {
    pub failed_equality_indexes: Vec<(u64, String)>,
    pub failed_range_indexes: Vec<(u64, String)>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MaintainedSecondaryIndexIds {
    pub equality_index_ids: NodeIdSet,
    pub range_index_ids: NodeIdSet,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CompactionComponentBuildOutput {
    pub records: Vec<SegmentComponentRecordV1>,
    pub report: SecondaryIndexMaintenanceReport,
}

#[derive(Debug, Default, Clone)]
struct DeclaredSidecarWriteOutcome {
    records: Vec<SegmentComponentRecordV1>,
    report: SecondaryIndexMaintenanceReport,
    stats_evidence: DeclaredIndexStatsEvidence,
}

struct FlushNodeIndexOutput<'a> {
    key_index: KeyIndexPayloadPlan<'a>,
    node_label_index: LabelPostingIndexPayloadPlan,
    timestamp_index: TimestampIndexPayloadPlan,
    external_records: Vec<SegmentComponentRecordV1>,
    declared_evidence: DeclaredIndexStatsEvidence,
}

struct FlushEdgeIndexOutput {
    adj_out: AdjacencyPayloadPlan,
    adj_in: AdjacencyPayloadPlan,
    edge_label_index: LabelPostingIndexPayloadPlan,
    edge_triple_index: EdgeTripleIndexPayloadPlan,
    edge_metadata_indexes: EdgeMetadataIndexEntries,
    external_records: Vec<SegmentComponentRecordV1>,
    declared_evidence: DeclaredIndexStatsEvidence,
}

#[derive(Default)]
struct SecondaryIndexPartitions<'a> {
    node_eq: Vec<&'a SecondaryIndexManifestEntry>,
    node_range: Vec<&'a SecondaryIndexManifestEntry>,
    edge_eq: Vec<&'a SecondaryIndexManifestEntry>,
    edge_range: Vec<&'a SecondaryIndexManifestEntry>,
}

type CompoundSidecarEntries = Vec<(Vec<u8>, u64)>;
type CompoundSidecarBuildResult = Result<CompoundSidecarEntries, EngineError>;
type OptionalCompoundSidecarBuildResult = Option<CompoundSidecarBuildResult>;
type CompoundFlushState = HashMap<u64, CompoundSidecarEntries>;

fn partition_secondary_indexes(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> SecondaryIndexPartitions<'_> {
    let mut partitions = SecondaryIndexPartitions {
        node_eq: Vec::with_capacity(secondary_indexes.len()),
        node_range: Vec::with_capacity(secondary_indexes.len()),
        edge_eq: Vec::with_capacity(secondary_indexes.len()),
        edge_range: Vec::with_capacity(secondary_indexes.len()),
    };
    for entry in secondary_indexes {
        match (&entry.target, &entry.kind) {
            (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Equality) => {
                partitions.node_eq.push(entry);
            }
            (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Range) => {
                partitions.node_range.push(entry);
            }
            (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Equality) => {
                partitions.edge_eq.push(entry);
            }
            (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Range) => {
                partitions.edge_range.push(entry);
            }
            (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
                partitions.node_eq.push(entry);
            }
            (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Range) => {
                partitions.node_range.push(entry);
            }
            (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
                partitions.edge_eq.push(entry);
            }
            (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Range) => {
                partitions.edge_range.push(entry);
            }
        }
    }
    partitions
}

fn compound_flush_index_ids(partitions: &SecondaryIndexPartitions<'_>) -> Vec<u64> {
    let mut ids = Vec::new();
    for entry in partitions
        .node_eq
        .iter()
        .chain(partitions.node_range.iter())
        .chain(partitions.edge_eq.iter())
        .chain(partitions.edge_range.iter())
    {
        if matches!(
            &entry.target,
            SecondaryIndexTarget::NodeFieldIndex { .. }
                | SecondaryIndexTarget::EdgeFieldIndex { .. }
        ) {
            ids.push(entry.index_id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

const FLUSH_COMPONENT_GENERATION: u64 = 1;
const FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION: u32 = 1;

struct FlushComponentBuildSession {
    records: Vec<SegmentComponentRecordV1>,
}

impl FlushComponentBuildSession {
    fn create() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    fn push(&mut self, record: SegmentComponentRecordV1) -> SegmentComponentRecordV1 {
        self.records.push(record.clone());
        record
    }

    fn extend(&mut self, records: Vec<SegmentComponentRecordV1>) {
        self.records.extend(records);
    }

    fn fsync_component_parent_dirs(&self, seg_dir: &Path) -> Result<(), EngineError> {
        let mut parent_dirs = BTreeSet::new();
        for record in &self.records {
            if let ComponentHandleV1::ExternalFile { relative_path, .. } = &record.handle {
                if let Some(parent) = seg_dir.join(relative_path).parent() {
                    parent_dirs.insert(parent.to_path_buf());
                }
            }
        }
        for parent in parent_dirs {
            fsync_dir(&parent)?;
        }
        Ok(())
    }
}

pub(crate) struct PackedCoreWriter {
    seg_dir: PathBuf,
    segment_id: u64,
    generation: u64,
    tmp_path: PathBuf,
    final_path: PathBuf,
    writer: BufWriter<File>,
    container_digest: Sha256,
    container_payload_len: u64,
    records: Vec<SegmentComponentRecordV1>,
}

struct PackedCoreComponentSink<'a> {
    writer: &'a mut BufWriter<File>,
    container_digest: &'a mut Sha256,
    container_payload_len: &'a mut u64,
    payload_digest: Sha256,
    payload_len: u64,
}

impl PackedCoreWriter {
    fn create(seg_dir: &Path, segment_id: u64, generation: u64) -> Result<Self, EngineError> {
        fs::create_dir_all(seg_dir)?;
        let tmp_path = seg_dir.join(PACKED_CORE_TMP_FILENAME);
        let final_path = seg_dir.join(PACKED_CORE_FILENAME);
        let mut writer = BufWriter::new(File::create(&tmp_path)?);
        writer.write_all(&[0; COMPONENT_IDENTITY_HEADER_LEN])?;
        Ok(Self {
            seg_dir: seg_dir.to_path_buf(),
            segment_id,
            generation,
            tmp_path,
            final_path,
            writer,
            container_digest: Sha256::new(),
            container_payload_len: 0,
            records: Vec::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn write_component<T>(
        &mut self,
        kind: SegmentComponentKind,
        requirement: ComponentRequirement,
        trust_class: ComponentTrustClass,
        dependencies: Vec<ComponentDependencyV1>,
        build_fingerprint: u64,
        encode: impl FnOnce(&mut PackedCoreComponentSink<'_>) -> Result<T, EngineError>,
    ) -> Result<(SegmentComponentRecordV1, T), EngineError> {
        if !is_packed_core_component_kind(&kind) {
            return Err(EngineError::CorruptRecord(format!(
                "component {:?} is not eligible for {PACKED_CORE_FILENAME}",
                kind
            )));
        }
        self.align_next_component()?;
        let start_offset = self.container_payload_len;
        let mut sink = PackedCoreComponentSink {
            writer: &mut self.writer,
            container_digest: &mut self.container_digest,
            container_payload_len: &mut self.container_payload_len,
            payload_digest: Sha256::new(),
            payload_len: 0,
        };
        let output = encode(&mut sink)?;
        let payload_len = sink.payload_len;
        let payload_digest: [u8; 32] = sink.payload_digest.finalize().into();
        let dependency_digest = dependency_digest(&dependencies);
        let component_id = component_id(
            self.segment_id,
            &kind,
            FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
            payload_len,
            Some(&payload_digest),
            &dependency_digest,
            build_fingerprint,
        );
        let record = SegmentComponentRecordV1 {
            component_id,
            kind,
            logical_format_version: FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
            created_generation: self.generation,
            requirement,
            trust_class,
            handle: ComponentHandleV1::PackedRange {
                container_component_id: ZERO_DIGEST,
                offset: start_offset,
                len: payload_len,
            },
            payload_len,
            payload_digest: Some(payload_digest),
            dependency_digest,
            dependencies,
            build_fingerprint,
        };
        self.records.push(record.clone());
        Ok((record, output))
    }

    fn finish(mut self) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
        self.writer.flush()?;
        let container_payload_digest: [u8; 32] = self.container_digest.finalize().into();
        let dependencies = Vec::new();
        let dependency_digest = dependency_digest(&dependencies);
        let build_fingerprint = component_fingerprint("flush.packed_segment_container", &[]);
        let kind = SegmentComponentKind::PackedSegmentContainer;
        let container_component_id = component_id(
            self.segment_id,
            &kind,
            FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
            self.container_payload_len,
            Some(&container_payload_digest),
            &dependency_digest,
            build_fingerprint,
        );
        let container_record = SegmentComponentRecordV1 {
            component_id: container_component_id,
            kind: kind.clone(),
            logical_format_version: FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
            created_generation: self.generation,
            requirement: ComponentRequirement::Required,
            trust_class: ComponentTrustClass::AuxiliaryBlob,
            handle: ComponentHandleV1::ExternalFile {
                relative_path: PACKED_CORE_FILENAME.to_string(),
                payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
                payload_len: self.container_payload_len,
            },
            payload_len: self.container_payload_len,
            payload_digest: Some(container_payload_digest),
            dependency_digest,
            dependencies,
            build_fingerprint,
        };
        let header = ComponentIdentityHeaderV1 {
            segment_format_version: SEGMENT_FORMAT_VERSION,
            segment_id: self.segment_id,
            component_kind: kind,
            logical_format_version: FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
            created_generation: self.generation,
            payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
            payload_len: self.container_payload_len,
            component_id: container_component_id,
            dependency_digest,
            build_fingerprint,
            payload_digest: Some(container_payload_digest),
        };
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&encode_identity_header(&header))?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        drop(self.writer);
        fs::rename(&self.tmp_path, &self.final_path)?;
        fsync_dir(&self.seg_dir)?;

        let mut records = self.records;
        patch_packed_range_container_id(&mut records, container_component_id);
        records.push(container_record);
        Ok(records)
    }

    fn align_next_component(&mut self) -> Result<(), EngineError> {
        let padding = (8 - (self.container_payload_len % 8)) % 8;
        if padding == 0 {
            return Ok(());
        }
        const ZEROS: [u8; 8] = [0; 8];
        let padding = padding as usize;
        self.writer.write_all(&ZEROS[..padding])?;
        self.container_digest.update(&ZEROS[..padding]);
        self.container_payload_len += padding as u64;
        Ok(())
    }
}

pub(crate) fn create_compaction_core_writer(
    seg_dir: &Path,
    segment_id: u64,
) -> Result<PackedCoreWriter, EngineError> {
    PackedCoreWriter::create(seg_dir, segment_id, FLUSH_COMPONENT_GENERATION)
}

pub(crate) fn finish_compaction_core_writer(
    core_writer: PackedCoreWriter,
) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
    core_writer.finish()
}

impl Write for PackedCoreComponentSink<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(buf)?;
        let bytes = &buf[..written];
        self.container_digest.update(bytes);
        self.payload_digest.update(bytes);
        *self.container_payload_len += written as u64;
        self.payload_len += written as u64;
        Ok(written)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(buf)?;
        self.container_digest.update(buf);
        self.payload_digest.update(buf);
        *self.container_payload_len += buf.len() as u64;
        self.payload_len += buf.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[allow(clippy::too_many_arguments)]
fn create_segment_component_writer(
    seg_dir: &Path,
    segment_id: u64,
    relative_path: &str,
    kind: SegmentComponentKind,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    build_fingerprint: u64,
) -> Result<ComponentIdentityWriter, EngineError> {
    let path = seg_dir.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    ComponentIdentityWriter::create(
        &path,
        relative_path.to_string(),
        SEGMENT_FORMAT_VERSION,
        segment_id,
        kind,
        FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
        FLUSH_COMPONENT_GENERATION,
        requirement,
        trust_class,
        build_fingerprint,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn create_flush_component_writer(
    seg_dir: &Path,
    segment_id: u64,
    relative_path: &str,
    kind: SegmentComponentKind,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    build_fingerprint: u64,
) -> Result<ComponentIdentityWriter, EngineError> {
    create_segment_component_writer(
        seg_dir,
        segment_id,
        relative_path,
        kind,
        requirement,
        trust_class,
        build_fingerprint,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_flush_component<T>(
    seg_dir: &Path,
    segment_id: u64,
    relative_path: &str,
    kind: SegmentComponentKind,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    dependencies: Vec<ComponentDependencyV1>,
    build_fingerprint: u64,
    encode: impl FnOnce(&mut ComponentIdentityWriter) -> Result<T, EngineError>,
) -> Result<(SegmentComponentRecordV1, T), EngineError> {
    let mut writer = create_flush_component_writer(
        seg_dir,
        segment_id,
        relative_path,
        kind,
        requirement,
        trust_class,
        build_fingerprint,
    )?;
    let output = encode(&mut writer)?;
    let record = writer.finish(dependencies)?;
    Ok((record, output))
}

#[allow(clippy::too_many_arguments)]
fn write_flush_component_pair<T>(
    seg_dir: &Path,
    segment_id: u64,
    first_relative_path: &str,
    first_kind: SegmentComponentKind,
    first_build_fingerprint: u64,
    second_relative_path: &str,
    second_kind: SegmentComponentKind,
    second_build_fingerprint: u64,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    dependencies: Vec<ComponentDependencyV1>,
    encode: impl FnOnce(
        &mut ComponentIdentityWriter,
        &mut ComponentIdentityWriter,
    ) -> Result<T, EngineError>,
) -> Result<(SegmentComponentRecordV1, SegmentComponentRecordV1, T), EngineError> {
    let mut first = create_flush_component_writer(
        seg_dir,
        segment_id,
        first_relative_path,
        first_kind,
        requirement.clone(),
        trust_class,
        first_build_fingerprint,
    )?;
    let mut second = create_flush_component_writer(
        seg_dir,
        segment_id,
        second_relative_path,
        second_kind,
        requirement,
        trust_class,
        second_build_fingerprint,
    )?;
    let output = encode(&mut first, &mut second)?;
    let first_record = first.finish(dependencies.clone())?;
    let second_record = second.finish(dependencies)?;
    Ok((first_record, second_record, output))
}

#[allow(clippy::too_many_arguments)]
fn write_compaction_component<T>(
    seg_dir: &Path,
    segment_id: u64,
    relative_path: &str,
    kind: SegmentComponentKind,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    dependencies: Vec<ComponentDependencyV1>,
    build_fingerprint: u64,
    encode: impl FnOnce(&mut ComponentIdentityWriter) -> Result<T, EngineError>,
) -> Result<(SegmentComponentRecordV1, T), EngineError> {
    let mut writer = create_segment_component_writer(
        seg_dir,
        segment_id,
        relative_path,
        kind,
        requirement,
        trust_class,
        build_fingerprint,
    )?;
    let output = encode(&mut writer)?;
    let record = writer.finish(dependencies)?;
    Ok((record, output))
}

#[allow(clippy::too_many_arguments)]
fn write_compaction_component_pair<T>(
    seg_dir: &Path,
    segment_id: u64,
    first_relative_path: &str,
    first_kind: SegmentComponentKind,
    first_build_fingerprint: u64,
    second_relative_path: &str,
    second_kind: SegmentComponentKind,
    second_build_fingerprint: u64,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    dependencies: Vec<ComponentDependencyV1>,
    encode: impl FnOnce(
        &mut ComponentIdentityWriter,
        &mut ComponentIdentityWriter,
    ) -> Result<T, EngineError>,
) -> Result<(SegmentComponentRecordV1, SegmentComponentRecordV1, T), EngineError> {
    let mut first = create_segment_component_writer(
        seg_dir,
        segment_id,
        first_relative_path,
        first_kind,
        requirement.clone(),
        trust_class,
        first_build_fingerprint,
    )?;
    let mut second = create_segment_component_writer(
        seg_dir,
        segment_id,
        second_relative_path,
        second_kind,
        requirement,
        trust_class,
        second_build_fingerprint,
    )?;
    let output = encode(&mut first, &mut second)?;
    let first_record = first.finish(dependencies.clone())?;
    let second_record = second.finish(dependencies)?;
    Ok((first_record, second_record, output))
}

pub(crate) fn secondary_indexes_dir(seg_dir: &Path) -> PathBuf {
    seg_dir.join(SECONDARY_INDEX_DIRNAME)
}

#[cfg(test)]
pub(crate) fn node_prop_eq_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::NodePropertyEqualityIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("node_prop_eq_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn node_prop_range_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::NodePropertyRangeIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("node_prop_range_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn edge_prop_eq_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::EdgePropertyEqualityIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("edge_prop_eq_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn edge_prop_range_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::EdgePropertyRangeIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("edge_prop_range_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn node_compound_eq_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::NodeCompoundEqualityIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("node_compound_eq_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn node_compound_range_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::NodeCompoundRangeIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("node_compound_range_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn edge_compound_eq_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::EdgeCompoundEqualityIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("edge_compound_eq_{}.dat", index_id))
}

#[cfg(test)]
pub(crate) fn edge_compound_range_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    if let Some(path) = manifested_component_path(
        seg_dir,
        &SegmentComponentKind::EdgeCompoundRangeIndex { index_id },
    ) {
        return path;
    }
    secondary_indexes_dir(seg_dir).join(format!("edge_compound_range_{}.dat", index_id))
}

fn secondary_index_base_relative_path_for_entry(
    entry: &SecondaryIndexManifestEntry,
) -> Option<String> {
    match (&entry.target, &entry.kind) {
        (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Equality) => Some(format!(
            "{}/node_prop_eq_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        )),
        (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Range) => Some(format!(
            "{}/node_prop_range_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        )),
        (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Equality) => Some(format!(
            "{}/edge_prop_eq_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        )),
        (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Range) => Some(format!(
            "{}/edge_prop_range_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        )),
        (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
            Some(format!(
                "{}/node_compound_eq_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ))
        }
        (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Range) => Some(format!(
            "{}/node_compound_range_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        )),
        (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
            Some(format!(
                "{}/edge_compound_eq_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ))
        }
        (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Range) => Some(format!(
            "{}/edge_compound_range_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        )),
    }
}

fn secondary_index_component_kind_matches_entry(
    kind: &SegmentComponentKind,
    entry: &SecondaryIndexManifestEntry,
) -> bool {
    secondary_index_component_kind_for_entry(entry)
        .as_ref()
        .is_some_and(|expected| kind == expected)
}

pub(crate) fn secondary_index_sidecar_paths_for_entry(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
) -> Vec<PathBuf> {
    let Some(base_relative) = secondary_index_base_relative_path_for_entry(entry) else {
        return Vec::new();
    };
    let base_path = seg_dir.join(&base_relative);
    let mut paths = vec![base_path.clone()];
    let Some(parent) = base_path.parent() else {
        return paths;
    };
    let Some(base_name) = base_path.file_name().and_then(|name| name.to_str()) else {
        return paths;
    };
    let stem = Path::new(base_name)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(base_name);
    let generated_prefix = format!("{stem}.g");
    let refresh_prefix = format!(".{stem}.refresh_tmp.");
    if let Ok(entries) = fs::read_dir(parent) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name == base_name
                || file_name.starts_with(&generated_prefix)
                || file_name.starts_with(&refresh_prefix)
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

pub(crate) fn remove_secondary_index_component_records(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
) -> Result<Vec<PathBuf>, EngineError> {
    let mut manifest = read_segment_component_manifest(seg_dir)?;
    let mut removed_paths = Vec::new();
    let before_len = manifest.components.len();
    manifest.components.retain(|record| {
        let matches_index = secondary_index_component_kind_matches_entry(&record.kind, entry);
        if matches_index {
            if let ComponentHandleV1::ExternalFile { relative_path, .. } = &record.handle {
                removed_paths.push(seg_dir.join(relative_path));
            }
            false
        } else {
            true
        }
    });

    if manifest.components.len() == before_len {
        return Ok(removed_paths);
    }

    manifest.generation = manifest.generation.saturating_add(1);
    manifest.built_at_ms = current_time_millis();
    manifest.build_kind = SegmentComponentBuildKind::OptionalRefresh;
    write_segment_component_manifest(seg_dir, &manifest)?;
    Ok(removed_paths)
}

pub(crate) fn maintained_secondary_index_ids_from_component_records(
    records: &[SegmentComponentRecordV1],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> MaintainedSecondaryIndexIds {
    let mut maintained = MaintainedSecondaryIndexIds::default();
    for entry in secondary_indexes {
        if records
            .iter()
            .any(|record| secondary_index_component_kind_matches_entry(&record.kind, entry))
        {
            match entry.kind {
                SecondaryIndexKind::Equality => {
                    maintained.equality_index_ids.insert(entry.index_id);
                }
                SecondaryIndexKind::Range => {
                    maintained.range_index_ids.insert(entry.index_id);
                }
            }
        }
    }
    maintained
}

pub(crate) fn maintained_secondary_index_ids_from_segment_manifest(
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<MaintainedSecondaryIndexIds, EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    Ok(maintained_secondary_index_ids_from_component_records(
        &manifest.components,
        secondary_indexes,
    ))
}

#[cfg(test)]
fn manifested_component_path(seg_dir: &Path, kind: &SegmentComponentKind) -> Option<PathBuf> {
    let manifest = read_segment_component_manifest(seg_dir).ok()?;
    manifest
        .components
        .iter()
        .find(|record| &record.kind == kind)
        .and_then(|record| match &record.handle {
            ComponentHandleV1::ExternalFile { relative_path, .. } => {
                Some(seg_dir.join(relative_path))
            }
            ComponentHandleV1::PackedRange { .. } => None,
        })
}

pub(crate) fn component_fingerprint(namespace: &str, fields: &[u64]) -> u64 {
    component_build_fingerprint(SEGMENT_FORMAT_VERSION, namespace, fields)
}

pub(crate) fn node_property_equality_component_fingerprint(index_id: u64) -> u64 {
    component_fingerprint("semantic.node_prop_eq.v2", &[index_id])
}

pub(crate) fn edge_property_equality_component_fingerprint(index_id: u64) -> u64 {
    component_fingerprint("semantic.edge_prop_eq.v2", &[index_id])
}

pub(crate) fn node_property_range_component_fingerprint(index_id: u64) -> u64 {
    component_fingerprint("semantic.node_prop_range.v2", &[index_id])
}

pub(crate) fn edge_property_range_component_fingerprint(index_id: u64) -> u64 {
    component_fingerprint("semantic.edge_prop_range.v2", &[index_id])
}

pub(crate) fn node_compound_equality_component_fingerprint(
    index_id: u64,
    declaration_fingerprint: u64,
) -> u64 {
    component_fingerprint(
        "semantic.node_compound_eq.v1",
        &[index_id, declaration_fingerprint],
    )
}

pub(crate) fn node_compound_range_component_fingerprint(
    index_id: u64,
    declaration_fingerprint: u64,
) -> u64 {
    component_fingerprint(
        "semantic.node_compound_range.v1",
        &[index_id, declaration_fingerprint],
    )
}

pub(crate) fn edge_compound_equality_component_fingerprint(
    index_id: u64,
    declaration_fingerprint: u64,
) -> u64 {
    component_fingerprint(
        "semantic.edge_compound_eq.v1",
        &[index_id, declaration_fingerprint],
    )
}

pub(crate) fn edge_compound_range_component_fingerprint(
    index_id: u64,
    declaration_fingerprint: u64,
) -> u64 {
    component_fingerprint(
        "semantic.edge_compound_range.v1",
        &[index_id, declaration_fingerprint],
    )
}

pub(crate) fn compound_component_fingerprint_for_kind_and_entry(
    kind: &SegmentComponentKind,
    entry: &SecondaryIndexManifestEntry,
) -> Option<u64> {
    let declaration_fingerprint = secondary_index_declaration_fingerprint_for_entry(entry);
    match (kind, &entry.target, &entry.kind) {
        (
            SegmentComponentKind::NodeCompoundEqualityIndex { index_id },
            SecondaryIndexTarget::NodeFieldIndex { .. },
            SecondaryIndexKind::Equality,
        ) => {
            if *index_id == entry.index_id {
                Some(node_compound_equality_component_fingerprint(
                    *index_id,
                    declaration_fingerprint,
                ))
            } else {
                None
            }
        }
        (
            SegmentComponentKind::NodeCompoundRangeIndex { index_id },
            SecondaryIndexTarget::NodeFieldIndex { .. },
            SecondaryIndexKind::Range,
        ) => {
            if *index_id == entry.index_id {
                Some(node_compound_range_component_fingerprint(
                    *index_id,
                    declaration_fingerprint,
                ))
            } else {
                None
            }
        }
        (
            SegmentComponentKind::EdgeCompoundEqualityIndex { index_id },
            SecondaryIndexTarget::EdgeFieldIndex { .. },
            SecondaryIndexKind::Equality,
        ) => {
            if *index_id == entry.index_id {
                Some(edge_compound_equality_component_fingerprint(
                    *index_id,
                    declaration_fingerprint,
                ))
            } else {
                None
            }
        }
        (
            SegmentComponentKind::EdgeCompoundRangeIndex { index_id },
            SecondaryIndexTarget::EdgeFieldIndex { .. },
            SecondaryIndexKind::Range,
        ) => {
            if *index_id == entry.index_id {
                Some(edge_compound_range_component_fingerprint(
                    *index_id,
                    declaration_fingerprint,
                ))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn planner_stats_component_dependencies(
    segment_data_id: [u8; 32],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Vec<ComponentDependencyV1> {
    let mut entries: Vec<&SecondaryIndexManifestEntry> = secondary_indexes
        .iter()
        .filter(|entry| entry.state == SecondaryIndexState::Ready)
        .collect();
    entries.sort_by_key(|entry| entry.index_id);
    let mut dependencies = Vec::with_capacity(entries.len() + 1);
    dependencies.push(source_group_dependency(
        SegmentSourceGroupKind::SegmentData,
        segment_data_id,
    ));
    dependencies.extend(entries.into_iter().map(secondary_declaration_dependency));
    dependencies
}

pub(crate) fn planner_stats_component_fingerprint(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> u64 {
    let mut entries: Vec<&SecondaryIndexManifestEntry> = secondary_indexes
        .iter()
        .filter(|entry| entry.state == SecondaryIndexState::Ready)
        .collect();
    entries.sort_by_key(|entry| entry.index_id);
    let mut fields = Vec::with_capacity(entries.len() * 2 + 1);
    fields.push(entries.len() as u64);
    for entry in entries {
        fields.push(entry.index_id);
        if let ComponentDependencyV1::SecondaryIndexDeclaration { fingerprint, .. } =
            secondary_declaration_dependency(entry)
        {
            fields.push(fingerprint);
        }
    }
    component_fingerprint("flush.planner_stats", &fields)
}

#[cfg(test)]
pub(crate) fn publish_planner_stats_component_payload(
    seg_dir: &Path,
    ready_secondary_indexes: &[SecondaryIndexManifestEntry],
    payload: &[u8],
) -> Result<(), EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    let dependencies =
        planner_stats_component_dependencies(manifest.segment_data_id, ready_secondary_indexes);
    refresh_optional_component_with_writer(
        seg_dir,
        SegmentComponentKind::PlannerStats,
        PLANNER_STATS_FILENAME,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::PlannerStatsUnavailable,
        },
        ComponentTrustClass::OptionalAdvisoryStats,
        dependencies,
        planner_stats_component_fingerprint(ready_secondary_indexes),
        |writer| {
            writer.write_all(payload)?;
            Ok(())
        },
    )
}

pub(crate) fn publish_planner_stats_component_payload_from_latest(
    seg_dir: &Path,
    ready_secondary_indexes: &[SecondaryIndexManifestEntry],
    build_payload: impl FnOnce(Option<&[u8]>, u64, u64, u64) -> Result<Option<Vec<u8>>, EngineError>,
) -> Result<bool, EngineError> {
    let captured_manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        captured_manifest.segment_id,
        captured_manifest.node_count,
        captured_manifest.edge_count,
        &captured_manifest.components,
    )?;
    if source_groups.segment_data_id != captured_manifest.segment_data_id {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} source identity changed before optional publication",
            captured_manifest.segment_id
        )));
    }
    let current_payload = read_external_component_payload_from_manifest(
        seg_dir,
        &captured_manifest,
        SegmentComponentKind::PlannerStats,
    )?;
    let Some(payload) = build_payload(
        current_payload.as_deref(),
        captured_manifest.segment_id,
        captured_manifest.node_count,
        captured_manifest.edge_count,
    )?
    else {
        return Ok(false);
    };

    let generation = captured_manifest.generation.saturating_add(1);
    let final_relative_path = optional_generation_relative_path(PLANNER_STATS_FILENAME, generation);
    let tmp_relative_path = optional_refresh_tmp_relative_path(PLANNER_STATS_FILENAME, generation);
    let tmp_path = seg_dir.join(&tmp_relative_path);
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let dependencies = planner_stats_component_dependencies(
        captured_manifest.segment_data_id,
        ready_secondary_indexes,
    );
    let mut writer = ComponentIdentityWriter::create(
        &tmp_path,
        final_relative_path.clone(),
        SEGMENT_FORMAT_VERSION,
        captured_manifest.segment_id,
        SegmentComponentKind::PlannerStats,
        FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
        generation,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::PlannerStatsUnavailable,
        },
        ComponentTrustClass::OptionalAdvisoryStats,
        planner_stats_component_fingerprint(ready_secondary_indexes),
        true,
    )?;
    writer.write_all(&payload)?;
    let record = writer.finish(dependencies)?;

    let current_manifest = read_segment_component_manifest(seg_dir)?;
    if current_manifest.segment_id != captured_manifest.segment_id
        || current_manifest.segment_data_id != captured_manifest.segment_data_id
        || current_manifest.generation != captured_manifest.generation
    {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} {}",
            captured_manifest.segment_id, OPTIONAL_COMPONENT_PUBLICATION_CONFLICT_MESSAGE
        )));
    }

    let final_path = seg_dir.join(&final_relative_path);
    fs::rename(&tmp_path, &final_path)?;
    if let Some(parent) = final_path.parent() {
        fsync_dir(parent)?;
    }

    let mut replacement = current_manifest;
    replacement.generation = generation;
    replacement.built_at_ms = current_time_millis();
    replacement.build_kind = SegmentComponentBuildKind::OptionalRefresh;
    replacement
        .components
        .retain(|existing| existing.kind != SegmentComponentKind::PlannerStats);
    replacement.components.push(record);
    write_segment_component_manifest(seg_dir, &replacement)?;
    Ok(true)
}

fn read_external_component_payload_from_manifest(
    seg_dir: &Path,
    manifest: &SegmentComponentManifestV1,
    kind: SegmentComponentKind,
) -> Result<Option<Vec<u8>>, EngineError> {
    let Some(record) = manifest
        .components
        .iter()
        .find(|record| record.kind == kind)
    else {
        return Ok(None);
    };
    let ComponentHandleV1::ExternalFile {
        relative_path,
        payload_offset,
        payload_len,
    } = &record.handle
    else {
        return Ok(None);
    };
    let path = seg_dir.join(relative_path);
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(EngineError::IoError(error)),
    };
    let end = payload_offset.checked_add(*payload_len).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "component payload range overflows for {}",
            path.display()
        ))
    })?;
    if end != data.len() as u64 {
        return Ok(None);
    }
    let Ok(header) = decode_identity_header(&data) else {
        return Ok(None);
    };
    if header.segment_format_version != SEGMENT_FORMAT_VERSION
        || header.segment_id != manifest.segment_id
        || header.component_kind != record.kind
        || header.logical_format_version != record.logical_format_version
        || header.created_generation != record.created_generation
        || header.payload_offset != *payload_offset
        || header.payload_len != *payload_len
        || header.component_id != record.component_id
        || header.dependency_digest != record.dependency_digest
        || header.build_fingerprint != record.build_fingerprint
        || header.payload_digest != record.payload_digest
    {
        return Ok(None);
    }
    Ok(Some(data[*payload_offset as usize..end as usize].to_vec()))
}

const OPTIONAL_COMPONENT_PUBLICATION_CONFLICT_MESSAGE: &str =
    "changed before optional component publication";

pub(crate) fn is_optional_component_publication_conflict(error: &EngineError) -> bool {
    matches!(
        error,
        EngineError::CorruptRecord(message)
            if message.contains(OPTIONAL_COMPONENT_PUBLICATION_CONFLICT_MESSAGE)
    )
}

pub(crate) fn dense_config_fingerprint(config: Option<&DenseVectorConfig>) -> u64 {
    let Some(config) = config else {
        return component_fingerprint("dense_vector_config.none", &[]);
    };
    let metric = match config.metric {
        DenseMetric::Cosine => 1,
        DenseMetric::Euclidean => 2,
        DenseMetric::DotProduct => 3,
    };
    component_fingerprint(
        "dense_vector_config",
        &[
            config.dimension as u64,
            metric,
            config.hnsw.m as u64,
            config.hnsw.ef_construction as u64,
        ],
    )
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Write all segment components for a frozen memtable into the given directory.
///
/// Required core components are packed into `segment.core`; refreshable and
/// accelerator sidecars remain externally materialized.
///
/// IMPORTANT: Two index-writing paths exist and must stay in sync:
///   1. This function (flush path, builds indexes from Memtable)
///   2. `write_indexes_from_metadata_with_secondary_indexes()` (compaction path, builds from sidecars)
///
/// If you add a new index type, you MUST add it to BOTH paths.
pub(crate) fn write_segment_with_degree_overlay_and_secondary_indexes(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
    degree_overlay: &DegreeOverlaySnapshot,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentInfo, EngineError> {
    write_segment_inner(
        seg_dir,
        segment_id,
        memtable,
        dense_config,
        Some(degree_overlay),
        secondary_indexes,
    )
}

#[cfg(test)]
pub(crate) fn write_segment_without_degree_sidecar_for_test(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
) -> Result<SegmentInfo, EngineError> {
    write_segment_inner(seg_dir, segment_id, memtable, dense_config, None, &[])
}

#[cfg(test)]
pub(crate) fn write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentInfo, EngineError> {
    write_segment_inner(
        seg_dir,
        segment_id,
        memtable,
        dense_config,
        None,
        secondary_indexes,
    )
}

fn write_segment_inner(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
    degree_overlay: Option<&DegreeOverlaySnapshot>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentInfo, EngineError> {
    fs::create_dir_all(seg_dir)?;

    let nodes = memtable.nodes();
    let edges = memtable.edges();
    let degree_entries = degree_overlay.map(DegreeOverlaySnapshot::sorted_entries);

    let (flush_result, stats_core_result) = engine_cpu_join(
        || -> Result<
            (
                FlushComponentBuildSession,
                SegmentComponentSourceGroups,
                DeclaredIndexStatsEvidence,
            ),
            EngineError,
        > {
            let mut component_session = FlushComponentBuildSession::create();
            let mut core_writer =
                PackedCoreWriter::create(seg_dir, segment_id, FLUSH_COMPONENT_GENERATION)?;

            let (node_records, node_data) = core_writer.write_component(
                SegmentComponentKind::NodeRecords,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryData,
                Vec::new(),
                component_fingerprint("flush.nodes", &[]),
                |writer| write_nodes_payload(writer, &nodes),
            )?;

            let (edge_records, edge_data) = core_writer.write_component(
                SegmentComponentKind::EdgeRecords,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryData,
                Vec::new(),
                component_fingerprint("flush.edges", &[]),
                |writer| write_edges_payload(writer, &edges),
            )?;

            let (node_meta, _) = core_writer.write_component(
                SegmentComponentKind::NodeMetadata,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryMetadata,
                Vec::new(),
                component_fingerprint("flush.node_meta", &[]),
                |writer| write_node_meta_payload(writer, &node_data, &nodes),
            )?;

            let (edge_meta, _) = core_writer.write_component(
                SegmentComponentKind::EdgeMetadata,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryMetadata,
                Vec::new(),
                component_fingerprint("flush.edge_meta", &[]),
                |writer| write_edge_meta_payload(writer, &edge_data, &edges),
            )?;

            let (tombstones, _) = core_writer.write_component(
                SegmentComponentKind::Tombstones,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryMetadata,
                Vec::new(),
                component_fingerprint("flush.tombstones", &[]),
                |writer| {
                    write_tombstones_payload(
                        writer,
                        &memtable.deleted_nodes(),
                        &memtable.deleted_edges(),
                    )
                },
            )?;

            let source_records = vec![
                node_records.clone(),
                edge_records.clone(),
                node_meta.clone(),
                edge_meta.clone(),
                tombstones.clone(),
            ];
            let source_groups = segment_source_groups_from_records(
                segment_id,
                nodes.len() as u64,
                edges.len() as u64,
                &source_records,
            )?;
            let (vector_records, dense_points) =
                write_node_vector_source_components(
                    &mut core_writer,
                    &node_data,
                    &nodes,
                source_groups.node_source,
            )?;

            let source_groups = compute_flush_source_groups(
                segment_id,
                nodes.len() as u64,
                edges.len() as u64,
                &node_records,
                &edge_records,
                &node_meta,
                &edge_meta,
                &tombstones,
                &vector_records,
            )?;

            let (dependent_records, declared_evidence) = write_flush_dependent_components(
                seg_dir,
                segment_id,
                &mut core_writer,
                memtable,
                &nodes,
                &edges,
                degree_entries.as_deref(),
                dense_config,
                dense_points,
                secondary_indexes,
                source_groups,
            )?;
            component_session.extend(dependent_records);
            component_session.extend(core_writer.finish()?);
            Ok((component_session, source_groups, declared_evidence))
        },
        || build_flush_stats_core_partial(&nodes, &edges, secondary_indexes),
    );
    let (mut component_session, source_groups, declared_evidence) = flush_result?;

    if let Ok(core_partial) = stats_core_result {
        let stats = assemble_flush_stats_from_partials(
            segment_id,
            secondary_indexes,
            core_partial,
            declared_evidence,
        );
        if let Ok(Some(payload)) = planner_stats_sidecar_payload(stats) {
            if let Ok((record, _)) = write_flush_component(
                seg_dir,
                segment_id,
                PLANNER_STATS_FILENAME,
                SegmentComponentKind::PlannerStats,
                ComponentRequirement::Optional {
                    fallback: ComponentFallbackClass::PlannerStatsUnavailable,
                },
                ComponentTrustClass::OptionalAdvisoryStats,
                planner_stats_component_dependencies(
                    source_groups.segment_data_id,
                    secondary_indexes,
                ),
                planner_stats_component_fingerprint(secondary_indexes),
                |writer| {
                    writer.write_all(&payload)?;
                    Ok(())
                },
            ) {
                component_session.push(record);
            }
        }
    }

    validate_required_components_before_manifest(segment_id, &component_session.records)?;
    component_session.fsync_component_parent_dirs(seg_dir)?;
    fsync_dir(seg_dir)?;
    let mut records = component_session.records.clone();
    sort_component_records_for_manifest(&mut records);
    let manifest = SegmentComponentManifestV1 {
        format_version: SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION,
        segment_format_version: SEGMENT_FORMAT_VERSION,
        segment_id,
        generation: FLUSH_COMPONENT_GENERATION,
        built_at_ms: current_time_millis(),
        build_kind: SegmentComponentBuildKind::Flush,
        segment_data_id: source_groups.segment_data_id,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        components: records,
        unknown_optional_components: Vec::new(),
    };
    write_segment_component_manifest(seg_dir, &manifest)?;
    fsync_dir(seg_dir)?;

    Ok(SegmentInfo {
        id: segment_id,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        segment_format_version: SEGMENT_FORMAT_VERSION,
        segment_data_id: source_groups.segment_data_id,
    })
}

pub(crate) fn finalize_compaction_segment(
    seg_dir: &Path,
    segment_id: u64,
    node_count: u64,
    edge_count: u64,
    records: Vec<SegmentComponentRecordV1>,
) -> Result<SegmentInfo, EngineError> {
    let source_groups =
        segment_source_groups_from_records(segment_id, node_count, edge_count, &records)?;
    let component_session = FlushComponentBuildSession { records };
    validate_required_components_before_manifest(segment_id, &component_session.records)?;
    component_session.fsync_component_parent_dirs(seg_dir)?;
    fsync_dir(seg_dir)?;
    let mut records = component_session.records.clone();
    sort_component_records_for_manifest(&mut records);
    let manifest = SegmentComponentManifestV1 {
        format_version: SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION,
        segment_format_version: SEGMENT_FORMAT_VERSION,
        segment_id,
        generation: FLUSH_COMPONENT_GENERATION,
        built_at_ms: current_time_millis(),
        build_kind: SegmentComponentBuildKind::Compaction,
        segment_data_id: source_groups.segment_data_id,
        node_count,
        edge_count,
        components: records,
        unknown_optional_components: Vec::new(),
    };
    write_segment_component_manifest(seg_dir, &manifest)?;
    fsync_dir(seg_dir)?;

    Ok(SegmentInfo {
        id: segment_id,
        node_count,
        edge_count,
        segment_format_version: SEGMENT_FORMAT_VERSION,
        segment_data_id: source_groups.segment_data_id,
    })
}

#[allow(clippy::too_many_arguments)]
fn compute_flush_source_groups(
    segment_id: u64,
    node_count: u64,
    edge_count: u64,
    node_records: &SegmentComponentRecordV1,
    edge_records: &SegmentComponentRecordV1,
    node_meta: &SegmentComponentRecordV1,
    edge_meta: &SegmentComponentRecordV1,
    tombstones: &SegmentComponentRecordV1,
    vector_records: &[SegmentComponentRecordV1],
) -> Result<SegmentComponentSourceGroups, EngineError> {
    let mut records = Vec::with_capacity(5 + vector_records.len());
    records.extend([
        node_records.clone(),
        edge_records.clone(),
        node_meta.clone(),
        edge_meta.clone(),
        tombstones.clone(),
    ]);
    records.extend_from_slice(vector_records);
    segment_source_groups_from_records(segment_id, node_count, edge_count, &records)
}

#[allow(clippy::too_many_arguments)]
fn write_flush_dependent_components(
    seg_dir: &Path,
    segment_id: u64,
    core_writer: &mut PackedCoreWriter,
    memtable: &Memtable,
    nodes: &NodeIdMap<NodeRecord>,
    edges: &NodeIdMap<EdgeRecord>,
    degree_entries: Option<&[(u64, DegreeDelta)]>,
    dense_config: Option<&DenseVectorConfig>,
    dense_points: Vec<DensePointInput>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<(Vec<SegmentComponentRecordV1>, DeclaredIndexStatsEvidence), EngineError> {
    let partitions = partition_secondary_indexes(secondary_indexes);
    let compound_flush_state =
        memtable.compound_secondary_state_for_indexes(&compound_flush_index_ids(&partitions));
    let (((node_output, edge_output), sparse_posting_records), built_hnsw) = engine_cpu_try_join(
        || {
            engine_cpu_try_join(
                || {
                    engine_cpu_try_join(
                        || {
                            write_flush_node_index_components(
                                seg_dir,
                                segment_id,
                                memtable,
                                nodes,
                                &partitions.node_eq,
                                &partitions.node_range,
                                &compound_flush_state,
                                source_groups,
                            )
                        },
                        || {
                            write_flush_edge_index_components(
                                seg_dir,
                                segment_id,
                                memtable,
                                edges,
                                degree_entries,
                                &partitions.edge_eq,
                                &partitions.edge_range,
                                &compound_flush_state,
                                source_groups,
                            )
                        },
                    )
                },
                || write_flush_sparse_posting_components(seg_dir, segment_id, nodes, source_groups),
            )
        },
        || maybe_build_dense_hnsw(dense_points, dense_config),
    )?;

    let mut dense_hnsw_records = write_flush_prebuilt_dense_hnsw_components(
        seg_dir,
        segment_id,
        dense_config,
        built_hnsw,
        source_groups,
    )?;

    emit_flush_node_index_components(core_writer, source_groups, &node_output)?;
    emit_flush_edge_index_components(core_writer, source_groups, &edge_output)?;

    let mut records = Vec::new();
    let mut declared_evidence = node_output.declared_evidence;
    declared_evidence.extend(edge_output.declared_evidence);
    declared_evidence.sort();
    records.extend(node_output.external_records);
    records.extend(edge_output.external_records);
    records.append(&mut dense_hnsw_records);
    records.extend(sparse_posting_records);
    Ok((records, declared_evidence))
}

#[allow(clippy::too_many_arguments)]
fn write_flush_node_index_components<'a>(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    nodes: &'a NodeIdMap<NodeRecord>,
    node_eq_indexes: &[&SecondaryIndexManifestEntry],
    node_range_indexes: &[&SecondaryIndexManifestEntry],
    compound_state: &CompoundFlushState,
    source_groups: SegmentComponentSourceGroups,
) -> Result<FlushNodeIndexOutput<'a>, EngineError> {
    let mut records = Vec::with_capacity(node_eq_indexes.len() + node_range_indexes.len());
    let key_index = prepare_key_index_payload(nodes)?;
    let node_label_index = LabelPostingIndexPayloadPlan {
        groups: memtable.node_label_posting_groups_current(),
    };

    let mut evidence = write_flush_declared_equality_components(
        seg_dir,
        segment_id,
        memtable,
        node_eq_indexes,
        source_groups,
        &mut records,
    )?;
    evidence.extend(write_flush_declared_compound_components(
        seg_dir,
        segment_id,
        compound_state,
        node_eq_indexes,
        source_groups,
        &mut records,
    )?);
    evidence.extend(write_flush_declared_range_components(
        seg_dir,
        segment_id,
        memtable,
        node_range_indexes,
        source_groups,
        &mut records,
    )?);
    evidence.extend(write_flush_declared_compound_components(
        seg_dir,
        segment_id,
        compound_state,
        node_range_indexes,
        source_groups,
        &mut records,
    )?);

    let time_node_index = memtable.time_node_index();
    let timestamp_index = prepare_timestamp_index_payload(&time_node_index);

    evidence.sort();
    Ok(FlushNodeIndexOutput {
        key_index,
        node_label_index,
        timestamp_index,
        external_records: records,
        declared_evidence: evidence,
    })
}

fn emit_flush_node_index_components(
    core_writer: &mut PackedCoreWriter,
    source_groups: SegmentComponentSourceGroups,
    output: &FlushNodeIndexOutput<'_>,
) -> Result<(), EngineError> {
    let node_source_dep = vec![source_group_dependency(
        SegmentSourceGroupKind::NodeSource,
        source_groups.node_source,
    )];
    core_writer.write_component(
        SegmentComponentKind::KeyIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        node_source_dep.clone(),
        component_fingerprint("flush.key_index", &[]),
        |writer| write_key_index_plan_payload(writer, &output.key_index),
    )?;
    core_writer.write_component(
        SegmentComponentKind::NodeLabelIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        node_source_dep.clone(),
        component_fingerprint("flush.node_label_index", &[]),
        |writer| write_label_posting_index_plan_payload(writer, &output.node_label_index),
    )?;
    core_writer.write_component(
        SegmentComponentKind::TimestampIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        node_source_dep,
        component_fingerprint("flush.timestamp_index", &[]),
        |writer| write_timestamp_index_plan_payload(writer, &output.timestamp_index),
    )?;
    Ok(())
}

fn write_flush_declared_equality_components(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    eq_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
    records: &mut Vec<SegmentComponentRecordV1>,
) -> Result<DeclaredIndexStatsEvidence, EngineError> {
    if eq_entries.is_empty() {
        return Ok(DeclaredIndexStatsEvidence::default());
    }
    if !eq_entries
        .iter()
        .any(|entry| matches!(&entry.target, SecondaryIndexTarget::NodeProperty { .. }))
    {
        return Ok(DeclaredIndexStatsEvidence::default());
    }

    let secondary_eq_state = memtable.secondary_eq_state();
    let mut evidence = DeclaredIndexStatsEvidence::default();
    for entry in eq_entries {
        if !matches!(&entry.target, SecondaryIndexTarget::NodeProperty { .. }) {
            continue;
        }
        let mut groups = BTreeMap::new();
        if let Some(values) = secondary_eq_state.get(&entry.index_id) {
            for (&value_hash, ids) in values {
                let mut sorted_ids: Vec<u64> = ids.iter().copied().collect();
                sorted_ids.sort_unstable();
                groups.insert(value_hash, sorted_ids);
            }
        }
        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::NodePropertyContentSource,
                source_groups.node_property_content_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_flush_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/node_prop_eq_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            node_property_equality_component_fingerprint(entry.index_id),
            |writer| write_node_prop_eq_sidecar_payload(writer, &groups),
        )?;
        records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            evidence
                .equality_index_stats
                .push(equality_index_stats_from_written_groups(entry, &groups));
        }
    }
    evidence.sort();
    Ok(evidence)
}

fn write_flush_declared_range_components(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    range_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
    records: &mut Vec<SegmentComponentRecordV1>,
) -> Result<DeclaredIndexStatsEvidence, EngineError> {
    if range_entries.is_empty() {
        return Ok(DeclaredIndexStatsEvidence::default());
    }
    if !range_entries
        .iter()
        .any(|entry| matches!(&entry.target, SecondaryIndexTarget::NodeProperty { .. }))
    {
        return Ok(DeclaredIndexStatsEvidence::default());
    }

    let secondary_range_state = memtable.secondary_range_state();
    let mut evidence = DeclaredIndexStatsEvidence::default();
    for entry in range_entries {
        if !matches!(&entry.target, SecondaryIndexTarget::NodeProperty { .. }) {
            continue;
        }
        let sidecar_entries: Vec<(NumericRangeSortKey, u64)> = secondary_range_state
            .get(&entry.index_id)
            .map(|entries| entries.iter().copied().collect())
            .unwrap_or_default();
        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::NodePropertyContentSource,
                source_groups.node_property_content_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_flush_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/node_prop_range_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::NodePropertyRangeIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            node_property_range_component_fingerprint(entry.index_id),
            |writer| write_node_prop_range_sidecar_payload(writer, &sidecar_entries),
        )?;
        records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            evidence
                .range_index_stats
                .push(range_index_stats_from_written_entries(
                    entry,
                    &sidecar_entries,
                ));
        }
    }
    evidence.sort();
    Ok(evidence)
}

fn write_flush_compound_sidecar_component(
    seg_dir: &Path,
    segment_id: u64,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(Vec<u8>, u64)],
    source_groups: SegmentComponentSourceGroups,
) -> Result<SegmentComponentRecordV1, EngineError> {
    let kind = secondary_index_component_kind_for_entry(entry).ok_or_else(|| {
        EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration has no compound sidecar kind"
                .to_string(),
        )
    })?;
    let relative_path = secondary_index_base_relative_path_for_entry(entry).ok_or_else(|| {
        EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration has no sidecar path".to_string(),
        )
    })?;
    let declaration = CompoundSidecarDeclaration::from_manifest_entry(
        entry,
        secondary_index_declaration_fingerprint_for_entry(entry),
    )?;
    let build_fingerprint = compound_component_fingerprint_for_kind_and_entry(&kind, entry)
        .ok_or_else(|| {
            EngineError::InvalidOperation(
                "compound secondary index unavailable: declaration does not match sidecar kind"
                    .to_string(),
            )
        })?;
    let dependencies = secondary_index_component_dependencies_for_entry(entry, &source_groups);
    let (record, _) = write_flush_component(
        seg_dir,
        segment_id,
        &relative_path,
        kind,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        build_fingerprint,
        |writer| write_compound_sidecar_payload(writer, &declaration, entries),
    )?;
    Ok(record)
}

fn write_compaction_compound_sidecar_component(
    seg_dir: &Path,
    segment_id: u64,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(Vec<u8>, u64)],
    source_groups: SegmentComponentSourceGroups,
) -> Result<SegmentComponentRecordV1, EngineError> {
    let kind = secondary_index_component_kind_for_entry(entry).ok_or_else(|| {
        EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration has no compound sidecar kind"
                .to_string(),
        )
    })?;
    let relative_path = secondary_index_base_relative_path_for_entry(entry).ok_or_else(|| {
        EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration has no sidecar path".to_string(),
        )
    })?;
    let declaration = CompoundSidecarDeclaration::from_manifest_entry(
        entry,
        secondary_index_declaration_fingerprint_for_entry(entry),
    )?;
    let build_fingerprint = compound_component_fingerprint_for_kind_and_entry(&kind, entry)
        .ok_or_else(|| {
            EngineError::InvalidOperation(
                "compound secondary index unavailable: declaration does not match sidecar kind"
                    .to_string(),
            )
        })?;
    let dependencies = secondary_index_component_dependencies_for_entry(entry, &source_groups);
    let (record, _) = write_compaction_component(
        seg_dir,
        segment_id,
        &relative_path,
        kind,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        build_fingerprint,
        |writer| write_compound_sidecar_payload(writer, &declaration, entries),
    )?;
    Ok(record)
}

fn write_flush_declared_compound_components(
    seg_dir: &Path,
    segment_id: u64,
    compound_state: &CompoundFlushState,
    entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
    records: &mut Vec<SegmentComponentRecordV1>,
) -> Result<DeclaredIndexStatsEvidence, EngineError> {
    if entries.is_empty() {
        return Ok(DeclaredIndexStatsEvidence::default());
    }
    let mut evidence = DeclaredIndexStatsEvidence::default();
    for entry in entries {
        if !matches!(
            &entry.target,
            SecondaryIndexTarget::NodeFieldIndex { .. }
                | SecondaryIndexTarget::EdgeFieldIndex { .. }
        ) {
            continue;
        }
        let sidecar_entries = compound_state
            .get(&entry.index_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let record = write_flush_compound_sidecar_component(
            seg_dir,
            segment_id,
            entry,
            sidecar_entries,
            source_groups,
        )?;
        records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            evidence
                .compound_index_stats
                .push(compound_index_stats_from_written_entries(
                    entry,
                    sidecar_entries,
                    DeclaredIndexRuntimeCoverageState::Available,
                )?);
        }
    }
    evidence.sort();
    Ok(evidence)
}

#[allow(clippy::too_many_arguments)]
fn write_flush_edge_index_components(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    edges: &NodeIdMap<EdgeRecord>,
    degree_entries: Option<&[(u64, DegreeDelta)]>,
    edge_eq_indexes: &[&SecondaryIndexManifestEntry],
    edge_range_indexes: &[&SecondaryIndexManifestEntry],
    compound_state: &CompoundFlushState,
    source_groups: SegmentComponentSourceGroups,
) -> Result<FlushEdgeIndexOutput, EngineError> {
    let mut records = Vec::with_capacity(
        (if degree_entries.is_some() { 1 } else { 0 })
            + edge_eq_indexes.len()
            + edge_range_indexes.len(),
    );
    let adj_out = prepare_adjacency_payloads(memtable.adj_out());
    let adj_in = prepare_adjacency_payloads(memtable.adj_in());
    let label_edge_index = memtable.label_edge_index();
    let edge_label_index = prepare_label_posting_index_payload(&label_edge_index);
    let edge_triple_index = prepare_edge_triple_index_payload(edges);
    let edge_metadata_indexes = prepare_flush_edge_metadata_index_components(edges);

    if let Some(entries) = degree_entries {
        let dependencies = vec![source_group_dependency(
            SegmentSourceGroupKind::DegreeSource,
            source_groups.degree_source,
        )];
        let (record, _) = write_flush_component(
            seg_dir,
            segment_id,
            DEGREE_DELTA_FILENAME,
            SegmentComponentKind::DegreeDelta,
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::AdjacencyWalk,
            },
            ComponentTrustClass::OptionalExactAccelerator,
            dependencies,
            component_fingerprint("flush.degree_delta", &[]),
            |writer| write_sorted_degree_delta_sidecar_payload(writer, entries),
        )?;
        records.push(record);
    }

    let mut evidence = write_flush_declared_edge_equality_components(
        seg_dir,
        segment_id,
        memtable,
        edge_eq_indexes,
        source_groups,
        &mut records,
    )?;
    evidence.extend(write_flush_declared_compound_components(
        seg_dir,
        segment_id,
        compound_state,
        edge_eq_indexes,
        source_groups,
        &mut records,
    )?);
    evidence.extend(write_flush_declared_edge_range_components(
        seg_dir,
        segment_id,
        memtable,
        edge_range_indexes,
        source_groups,
        &mut records,
    )?);
    evidence.extend(write_flush_declared_compound_components(
        seg_dir,
        segment_id,
        compound_state,
        edge_range_indexes,
        source_groups,
        &mut records,
    )?);
    evidence.sort();

    Ok(FlushEdgeIndexOutput {
        adj_out,
        adj_in,
        edge_label_index,
        edge_triple_index,
        edge_metadata_indexes,
        external_records: records,
        declared_evidence: evidence,
    })
}

fn write_flush_declared_edge_equality_components(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    eq_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
    records: &mut Vec<SegmentComponentRecordV1>,
) -> Result<DeclaredIndexStatsEvidence, EngineError> {
    if eq_entries.is_empty() {
        return Ok(DeclaredIndexStatsEvidence::default());
    }
    if !eq_entries
        .iter()
        .any(|entry| matches!(&entry.target, SecondaryIndexTarget::EdgeProperty { .. }))
    {
        return Ok(DeclaredIndexStatsEvidence::default());
    }

    let secondary_eq_state = memtable.secondary_eq_state();
    let mut evidence = DeclaredIndexStatsEvidence::default();
    for entry in eq_entries {
        if !matches!(&entry.target, SecondaryIndexTarget::EdgeProperty { .. }) {
            continue;
        }
        let mut groups = BTreeMap::new();
        if let Some(values) = secondary_eq_state.get(&entry.index_id) {
            for (&value_hash, ids) in values {
                let mut sorted_ids: Vec<u64> = ids.iter().copied().collect();
                sorted_ids.sort_unstable();
                groups.insert(value_hash, sorted_ids);
            }
        }
        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::EdgeSource,
                source_groups.edge_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_flush_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/edge_prop_eq_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            edge_property_equality_component_fingerprint(entry.index_id),
            |writer| write_node_prop_eq_sidecar_payload(writer, &groups),
        )?;
        records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            evidence
                .equality_index_stats
                .push(equality_index_stats_from_written_groups(entry, &groups));
        }
    }
    evidence.sort();
    Ok(evidence)
}

fn write_flush_declared_edge_range_components(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    range_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
    records: &mut Vec<SegmentComponentRecordV1>,
) -> Result<DeclaredIndexStatsEvidence, EngineError> {
    if range_entries.is_empty() {
        return Ok(DeclaredIndexStatsEvidence::default());
    }
    if !range_entries
        .iter()
        .any(|entry| matches!(&entry.target, SecondaryIndexTarget::EdgeProperty { .. }))
    {
        return Ok(DeclaredIndexStatsEvidence::default());
    }

    let secondary_range_state = memtable.secondary_range_state();
    let mut evidence = DeclaredIndexStatsEvidence::default();
    for entry in range_entries {
        if !matches!(&entry.target, SecondaryIndexTarget::EdgeProperty { .. }) {
            continue;
        }
        let sidecar_entries: Vec<(NumericRangeSortKey, u64)> = secondary_range_state
            .get(&entry.index_id)
            .map(|entries| entries.iter().copied().collect())
            .unwrap_or_default();
        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::EdgeSource,
                source_groups.edge_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_flush_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/edge_prop_range_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            edge_property_range_component_fingerprint(entry.index_id),
            |writer| write_node_prop_range_sidecar_payload(writer, &sidecar_entries),
        )?;
        records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            evidence
                .range_index_stats
                .push(range_index_stats_from_written_entries(
                    entry,
                    &sidecar_entries,
                ));
        }
    }
    evidence.sort();
    Ok(evidence)
}

fn prepare_flush_edge_metadata_index_components(
    edges: &NodeIdMap<EdgeRecord>,
) -> EdgeMetadataIndexEntries {
    let mut entries = EdgeMetadataIndexEntries::with_capacity(edges.len());
    for edge in edges.values() {
        entries.push(
            edge.label_id,
            edge.updated_at,
            edge.weight,
            edge.valid_from,
            edge.valid_to,
            edge.id,
        );
    }
    entries.sort_all();
    entries
}

fn emit_flush_edge_index_components(
    core_writer: &mut PackedCoreWriter,
    source_groups: SegmentComponentSourceGroups,
    output: &FlushEdgeIndexOutput,
) -> Result<(), EngineError> {
    let edge_source_dep = vec![source_group_dependency(
        SegmentSourceGroupKind::EdgeSource,
        source_groups.edge_source,
    )];
    core_writer.write_component(
        SegmentComponentKind::EdgeLabelIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        edge_source_dep.clone(),
        component_fingerprint("flush.edge_label_index", &[]),
        |writer| write_label_posting_index_plan_payload(writer, &output.edge_label_index),
    )?;
    core_writer.write_component(
        SegmentComponentKind::EdgeTripleIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        edge_source_dep.clone(),
        component_fingerprint("flush.edge_triple_index", &[]),
        |writer| write_edge_triple_index_plan_payload(writer, &output.edge_triple_index),
    )?;
    core_writer.write_component(
        SegmentComponentKind::AdjOutPostings,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        edge_source_dep.clone(),
        component_fingerprint("flush.adj_out_dat", &[]),
        |writer| write_adjacency_postings_payload(writer, &output.adj_out),
    )?;
    core_writer.write_component(
        SegmentComponentKind::AdjOutIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        edge_source_dep.clone(),
        component_fingerprint("flush.adj_out_idx", &[]),
        |writer| write_adjacency_index_payload(writer, &output.adj_out),
    )?;
    core_writer.write_component(
        SegmentComponentKind::AdjInPostings,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        edge_source_dep.clone(),
        component_fingerprint("flush.adj_in_dat", &[]),
        |writer| write_adjacency_postings_payload(writer, &output.adj_in),
    )?;
    core_writer.write_component(
        SegmentComponentKind::AdjInIndex,
        ComponentRequirement::Required,
        ComponentTrustClass::CoreMaintainedIndex,
        edge_source_dep,
        component_fingerprint("flush.adj_in_idx", &[]),
        |writer| write_adjacency_index_payload(writer, &output.adj_in),
    )?;

    let dependencies = vec![source_group_dependency(
        SegmentSourceGroupKind::EdgeMetadataSource,
        source_groups.edge_metadata_source,
    )];

    core_writer.write_component(
        SegmentComponentKind::EdgeWeightIndex,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::MetadataScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies.clone(),
        component_fingerprint("flush.edge_weight_index", &[]),
        |writer| {
            write_edge_weight_metadata_index_payload(writer, &output.edge_metadata_indexes.weight)
        },
    )?;
    core_writer.write_component(
        SegmentComponentKind::EdgeUpdatedAtIndex,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::MetadataScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies.clone(),
        component_fingerprint("flush.edge_updated_at_index", &[]),
        |writer| {
            write_edge_i64_metadata_index_payload(writer, &output.edge_metadata_indexes.updated_at)
        },
    )?;
    core_writer.write_component(
        SegmentComponentKind::EdgeValidFromIndex,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::MetadataScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies.clone(),
        component_fingerprint("flush.edge_valid_from_index", &[]),
        |writer| {
            write_edge_i64_metadata_index_payload(writer, &output.edge_metadata_indexes.valid_from)
        },
    )?;
    core_writer.write_component(
        SegmentComponentKind::EdgeValidToIndex,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::MetadataScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        component_fingerprint("flush.edge_valid_to_index", &[]),
        |writer| {
            write_edge_i64_metadata_index_payload(writer, &output.edge_metadata_indexes.valid_to)
        },
    )?;

    Ok(())
}

fn maybe_build_dense_hnsw(
    dense_points: Vec<DensePointInput>,
    dense_config: Option<&DenseVectorConfig>,
) -> Result<Option<BuiltHnsw>, EngineError> {
    let Some(config) = dense_config else {
        return Ok(None);
    };
    if dense_points.is_empty() {
        return Ok(None);
    }
    build_dense_hnsw_from_points(dense_points, config)
}

fn write_flush_prebuilt_dense_hnsw_components(
    seg_dir: &Path,
    segment_id: u64,
    dense_config: Option<&DenseVectorConfig>,
    built_hnsw: Option<BuiltHnsw>,
    source_groups: SegmentComponentSourceGroups,
) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
    let Some(config) = dense_config else {
        return Ok(Vec::new());
    };
    let Some(built) = built_hnsw else {
        return Ok(Vec::new());
    };
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::DenseVectorSource,
            source_groups.dense_vector_source,
        ),
        ComponentDependencyV1::DenseVectorConfig {
            fingerprint: dense_config_fingerprint(Some(config)),
        },
    ];
    let (meta_record, graph_record, _) = write_flush_component_pair(
        seg_dir,
        segment_id,
        DENSE_HNSW_META_FILENAME,
        SegmentComponentKind::DenseHnswMetadata,
        component_fingerprint("flush.dense_hnsw_meta", &[]),
        DENSE_HNSW_GRAPH_FILENAME,
        SegmentComponentKind::DenseHnswGraph,
        component_fingerprint("flush.dense_hnsw_graph", &[]),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::ExactVectorScan,
        },
        ComponentTrustClass::OptionalApproximateAccelerator,
        dependencies,
        |meta_writer, graph_writer| {
            write_prebuilt_hnsw_to_writers(meta_writer, graph_writer, config, &built)
        },
    )?;
    Ok(vec![meta_record, graph_record])
}

fn write_flush_sparse_posting_components(
    seg_dir: &Path,
    segment_id: u64,
    nodes: &NodeIdMap<NodeRecord>,
    source_groups: SegmentComponentSourceGroups,
) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
    let groups = sparse_posting_groups_from_nodes(nodes)?;
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::SparseVectorSource,
            source_groups.sparse_vector_source,
        ),
        ComponentDependencyV1::SparseVectorConfig {
            fingerprint: component_fingerprint("sparse_vector_config", &[]),
        },
    ];
    let (index_record, postings_record, _) = write_flush_component_pair(
        seg_dir,
        segment_id,
        SPARSE_POSTING_INDEX_FILENAME,
        SegmentComponentKind::SparsePostingIndex,
        component_fingerprint("flush.sparse_posting_index", &[]),
        SPARSE_POSTINGS_FILENAME,
        SegmentComponentKind::SparsePostings,
        component_fingerprint("flush.sparse_postings", &[]),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::ExactVectorScan,
        },
        ComponentTrustClass::OptionalApproximateAccelerator,
        dependencies,
        |index_writer, postings_writer| {
            write_sparse_posting_files_to_writers(index_writer, postings_writer, &groups)
        },
    )?;
    Ok(vec![index_record, postings_record])
}

#[cfg(test)]
pub(crate) fn write_node_prop_eq_sidecar_to_path(
    path: &Path,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    write_node_prop_eq_sidecar_payload(&mut writer, groups)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

pub(crate) fn publish_node_prop_eq_sidecar_component(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        manifest.segment_id,
        manifest.node_count,
        manifest.edge_count,
        &manifest.components,
    )?;
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::NodePropertyContentSource,
            source_groups.node_property_content_source,
        ),
        secondary_declaration_dependency(entry),
    ];
    refresh_optional_component_with_writer(
        seg_dir,
        SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: entry.index_id,
        },
        &format!(
            "{}/node_prop_eq_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        ),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        node_property_equality_component_fingerprint(entry.index_id),
        |writer| write_node_prop_eq_sidecar_payload(writer, groups),
    )
}

fn write_node_prop_eq_sidecar_payload(
    mut writer: &mut impl Write,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    let entry_count = groups.len() as u64;
    write_u64(&mut writer, entry_count)?;

    let data_start = 8 + entry_count * SECONDARY_EQ_ENTRY_SIZE;
    let mut data_offset = data_start;
    for (&value_hash, ids) in groups {
        write_u64(&mut writer, value_hash)?;
        write_u64(&mut writer, data_offset)?;
        write_u32(&mut writer, ids.len() as u32)?;
        data_offset += ids.len() as u64 * 8;
    }

    for ids in groups.values() {
        for &node_id in ids {
            write_u64(&mut writer, node_id)?;
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn write_node_prop_range_sidecar_to_path(
    path: &Path,
    entries: &[(NumericRangeSortKey, u64)],
) -> Result<(), EngineError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    write_node_prop_range_sidecar_payload(&mut writer, entries)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

pub(crate) fn publish_node_prop_range_sidecar_component(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(NumericRangeSortKey, u64)],
) -> Result<(), EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        manifest.segment_id,
        manifest.node_count,
        manifest.edge_count,
        &manifest.components,
    )?;
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::NodePropertyContentSource,
            source_groups.node_property_content_source,
        ),
        secondary_declaration_dependency(entry),
    ];
    refresh_optional_component_with_writer(
        seg_dir,
        SegmentComponentKind::NodePropertyRangeIndex {
            index_id: entry.index_id,
        },
        &format!(
            "{}/node_prop_range_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        ),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        node_property_range_component_fingerprint(entry.index_id),
        |writer| write_node_prop_range_sidecar_payload(writer, entries),
    )
}

pub(crate) fn publish_edge_prop_eq_sidecar_component(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        manifest.segment_id,
        manifest.node_count,
        manifest.edge_count,
        &manifest.components,
    )?;
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::EdgeSource,
            source_groups.edge_source,
        ),
        secondary_declaration_dependency(entry),
    ];
    refresh_optional_component_with_writer(
        seg_dir,
        SegmentComponentKind::EdgePropertyEqualityIndex {
            index_id: entry.index_id,
        },
        &format!(
            "{}/edge_prop_eq_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        ),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        edge_property_equality_component_fingerprint(entry.index_id),
        |writer| write_node_prop_eq_sidecar_payload(writer, groups),
    )
}

pub(crate) fn publish_edge_prop_range_sidecar_component(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(NumericRangeSortKey, u64)],
) -> Result<(), EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        manifest.segment_id,
        manifest.node_count,
        manifest.edge_count,
        &manifest.components,
    )?;
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::EdgeSource,
            source_groups.edge_source,
        ),
        secondary_declaration_dependency(entry),
    ];
    refresh_optional_component_with_writer(
        seg_dir,
        SegmentComponentKind::EdgePropertyRangeIndex {
            index_id: entry.index_id,
        },
        &format!(
            "{}/edge_prop_range_{}.dat",
            SECONDARY_INDEX_DIRNAME, entry.index_id
        ),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        edge_property_range_component_fingerprint(entry.index_id),
        |writer| write_node_prop_range_sidecar_payload(writer, entries),
    )
}

pub(crate) fn publish_compound_sidecar_component(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(Vec<u8>, u64)],
) -> Result<(), EngineError> {
    let manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        manifest.segment_id,
        manifest.node_count,
        manifest.edge_count,
        &manifest.components,
    )?;
    let Some(kind) = secondary_index_component_kind_for_entry(entry) else {
        return Err(EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration has no compound sidecar kind"
                .to_string(),
        ));
    };
    if !matches!(
        kind,
        SegmentComponentKind::NodeCompoundEqualityIndex { .. }
            | SegmentComponentKind::NodeCompoundRangeIndex { .. }
            | SegmentComponentKind::EdgeCompoundEqualityIndex { .. }
            | SegmentComponentKind::EdgeCompoundRangeIndex { .. }
    ) {
        return Err(EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration uses a single-property sidecar kind"
                .to_string(),
        ));
    }
    let Some(relative_path) = secondary_index_base_relative_path_for_entry(entry) else {
        return Err(EngineError::InvalidOperation(
            "compound secondary index unavailable: declaration has no sidecar path".to_string(),
        ));
    };
    let declaration = CompoundSidecarDeclaration::from_manifest_entry(
        entry,
        secondary_index_declaration_fingerprint_for_entry(entry),
    )?;
    let dependencies = secondary_index_component_dependencies_for_entry(entry, &source_groups);
    let build_fingerprint = compound_component_fingerprint_for_kind_and_entry(&kind, entry)
        .ok_or_else(|| {
            EngineError::InvalidOperation(
                "compound secondary index unavailable: declaration does not match sidecar kind"
                    .to_string(),
            )
        })?;
    refresh_optional_component_with_writer(
        seg_dir,
        kind,
        &relative_path,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::RecordScan,
        },
        ComponentTrustClass::OptionalCandidateIndex,
        dependencies,
        build_fingerprint,
        |writer| write_compound_sidecar_payload(writer, &declaration, entries),
    )
}

fn write_node_prop_range_sidecar_payload(
    mut writer: &mut impl Write,
    entries: &[(NumericRangeSortKey, u64)],
) -> Result<(), EngineError> {
    write_u64(&mut writer, entries.len() as u64)?;
    for &(encoded_value, node_id) in entries {
        writer.write_all(&encoded_value.as_bytes())?;
        write_u64(&mut writer, node_id)?;
    }
    Ok(())
}

/// Node records payload format:
/// [count: u64]
/// [index: (node_id: u64, offset: u64) × count, sorted by node_id]
/// [data: node records sequentially]
///
/// Returns Vec of (node_id, data_offset, data_len) sorted by node_id,
/// used by metadata writers to record raw byte spans.
fn write_nodes_payload(
    mut w: &mut impl Write,
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<RecordDataSpans, EngineError> {
    // Sort nodes by ID for binary search in the index
    let mut sorted: Vec<&NodeRecord> = nodes.values().collect();
    sorted.sort_by_key(|n| n.id);

    let count = sorted.len() as u64;
    write_u64(&mut w, count)?;

    // First pass: encode into reused buffer to collect sizes for offset table.
    let mut buf = Vec::new();
    let mut sizes: Vec<u64> = Vec::with_capacity(sorted.len());
    for node in &sorted {
        encode_node_record_into(&mut buf, node)?;
        sizes.push(buf.len() as u64);
    }

    // Write index entries and collect data info for sidecars
    let data_start = 8 + count * NODE_INDEX_ENTRY_SIZE;
    let mut data_offset = data_start;
    let mut node_data = Vec::with_capacity(sorted.len());
    for (i, node) in sorted.iter().enumerate() {
        write_u64(&mut w, node.id)?;
        write_u64(&mut w, data_offset)?;
        node_data.push((node.id, data_offset, sizes[i] as u32));
        data_offset += sizes[i];
    }

    // Second pass: re-encode into reused buffer and write directly.
    for node in &sorted {
        encode_node_record_into(&mut buf, node)?;
        w.write_all(&buf)?;
    }

    Ok(node_data)
}

/// Edge records payload format:
/// [count: u64]
/// [index: (edge_id: u64, offset: u64) × count, sorted by edge_id]
/// [data: edge records sequentially]
///
/// Returns Vec of (edge_id, data_offset, data_len) sorted by edge_id,
/// used by metadata writers to record raw byte spans.
fn write_edges_payload(
    mut w: &mut impl Write,
    edges: &NodeIdMap<EdgeRecord>,
) -> Result<RecordDataSpans, EngineError> {
    let mut sorted: Vec<&EdgeRecord> = edges.values().collect();
    sorted.sort_by_key(|e| e.id);

    let count = sorted.len() as u64;
    write_u64(&mut w, count)?;

    // First pass: encode into reused buffer to collect sizes for offset table.
    let mut buf = Vec::new();
    let mut sizes: Vec<u64> = Vec::with_capacity(sorted.len());
    for edge in &sorted {
        encode_edge_record_into(&mut buf, edge)?;
        sizes.push(buf.len() as u64);
    }

    let data_start = 8 + count * EDGE_INDEX_ENTRY_SIZE;
    let mut data_offset = data_start;
    let mut edge_data = Vec::with_capacity(sorted.len());
    for (i, edge) in sorted.iter().enumerate() {
        write_u64(&mut w, edge.id)?;
        write_u64(&mut w, data_offset)?;
        edge_data.push((edge.id, data_offset, sizes[i] as u32));
        data_offset += sizes[i];
    }

    // Second pass: re-encode into reused buffer and write directly.
    for edge in &sorted {
        encode_edge_record_into(&mut buf, edge)?;
        w.write_all(&buf)?;
    }

    Ok(edge_data)
}

// --- Varint helpers for adjacency delta encoding ---

/// Write a u64 varint into a `Vec<u8>`.
fn write_varint_to_vec(buf: &mut Vec<u8>, mut val: u64) {
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
}

fn varint_len(mut val: u64) -> u64 {
    let mut len = 1;
    while val >= 0x80 {
        val >>= 7;
        len += 1;
    }
    len
}

fn write_adjacency_posting_bytes(
    w: &mut impl Write,
    posting: AdjacencyPosting,
    prev_edge_id: &mut u64,
    scratch: &mut Vec<u8>,
) -> Result<(), EngineError> {
    let (edge_id, neighbor_id, weight, valid_from, valid_to) = posting;
    let delta = edge_id - *prev_edge_id;
    *prev_edge_id = edge_id;
    scratch.clear();
    write_varint_to_vec(scratch, delta);
    write_varint_to_vec(scratch, neighbor_id);
    scratch.extend_from_slice(&weight.to_le_bytes());
    debug_assert!(
        valid_from >= 0,
        "valid_from must be non-negative for varint encoding"
    );
    debug_assert!(
        valid_to >= 0,
        "valid_to must be non-negative for sentinel encoding"
    );
    let vt_enc = if valid_to == i64::MAX {
        0u64
    } else {
        valid_to as u64 + 1
    };
    write_varint_to_vec(scratch, valid_from as u64);
    write_varint_to_vec(scratch, vt_enc);
    w.write_all(scratch)?;
    Ok(())
}

fn adjacency_postings_len(postings: &[AdjacencyPosting]) -> u64 {
    let mut len = 0u64;
    let mut prev_edge_id = 0u64;
    for &(edge_id, neighbor_id, _, valid_from, valid_to) in postings {
        let delta = edge_id - prev_edge_id;
        prev_edge_id = edge_id;
        let vt_enc = if valid_to == i64::MAX {
            0u64
        } else {
            valid_to as u64 + 1
        };
        len += varint_len(delta);
        len += varint_len(neighbor_id);
        len += 4;
        len += varint_len(valid_from as u64);
        len += varint_len(vt_enc);
    }
    len
}

fn prepare_adjacency_payloads(adj: NodeIdMap<NodeIdMap<AdjEntry>>) -> AdjacencyPayloadPlan {
    let mut groups: Vec<(u64, u32, Vec<AdjacencyPosting>)> = Vec::new();
    for (node_id, edge_map) in adj {
        let mut by_label: HashMap<u32, Vec<AdjacencyPosting>> = HashMap::new();
        for entry in edge_map.into_values() {
            by_label.entry(entry.label_id).or_default().push((
                entry.edge_id,
                entry.neighbor_id,
                entry.weight,
                entry.valid_from,
                entry.valid_to,
            ));
        }
        for (label_id, mut postings) in by_label {
            postings.sort_unstable_by_key(|&(edge_id, ..)| edge_id);
            groups.push((node_id, label_id, postings));
        }
    }

    groups.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let mut offset = 0u64;
    let groups = groups
        .into_iter()
        .map(|(node_id, label_id, postings)| {
            let group = AdjacencyGroupPlan {
                node_id,
                label_id,
                offset,
                postings,
            };
            offset += adjacency_postings_len(&group.postings);
            group
        })
        .collect();
    AdjacencyPayloadPlan { groups }
}

fn write_adjacency_postings_payload(
    w: &mut impl Write,
    plan: &AdjacencyPayloadPlan,
) -> Result<(), EngineError> {
    let mut scratch = Vec::with_capacity(32);
    for group in &plan.groups {
        let mut prev_edge_id = 0u64;
        for &posting in &group.postings {
            write_adjacency_posting_bytes(w, posting, &mut prev_edge_id, &mut scratch)?;
        }
    }
    Ok(())
}

fn write_adjacency_index_payload(
    mut w: &mut impl Write,
    plan: &AdjacencyPayloadPlan,
) -> Result<(), EngineError> {
    write_u64(&mut w, plan.groups.len() as u64)?;
    for group in &plan.groups {
        write_u64(&mut w, group.node_id)?;
        write_u32(&mut w, group.label_id)?;
        write_u64(&mut w, group.offset)?;
        write_u32(&mut w, group.postings.len() as u32)?;
    }
    Ok(())
}

/// Adjacency index + delta-encoded postings.
///
/// Index payload:
/// [count: u64]
/// [(node_id: u64, label_id: u32, offset: u64, count: u32) × count, sorted by (node_id, label_id)]
///
/// Postings payload:
/// Per group: delta-encoded postings, variable length.
/// First posting: varint(edge_id) + varint(neighbor_id) + f32(weight) + varint(valid_from_enc) + varint(valid_to_enc)
/// Subsequent:    varint(edge_id_delta) + varint(neighbor_id) + f32(weight) + varint(valid_from_enc) + varint(valid_to_enc)
/// valid_from_enc = valid_from as u64 (valid_from is always >= 0)
/// valid_to_enc = 0 if valid_to == i64::MAX, else (valid_to as u64) + 1
/// Key index payload format:
/// [entry_count: u64]
/// [offset_table: u64 × entry_count]  (byte offset to each entry in data section)
/// [data section: entries sorted by (label_id, key, node_id)]
///
/// Each entry: [label_id: u32][node_id: u64][key_len: u16][key: bytes]
fn prepare_key_index_payload(
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<KeyIndexPayloadPlan<'_>, EngineError> {
    let mut entries: Vec<KeyIndexEntryPlan<'_>> = Vec::new();
    for node in nodes.values() {
        if node.key.len() > u16::MAX as usize {
            return Err(EngineError::SerializationError(format!(
                "node key exceeds maximum length of {} bytes",
                u16::MAX
            )));
        }
        for &label_id in node.label_ids.as_slice() {
            entries.push(KeyIndexEntryPlan {
                label_id,
                key: node.key.as_bytes(),
                node_id: node.id,
                encoded_len: 4 + 8 + 2 + node.key.len() as u64,
            });
        }
    }
    entries.sort_by(|a, b| {
        a.label_id
            .cmp(&b.label_id)
            .then_with(|| a.key.cmp(b.key))
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    for pair in entries.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if left.label_id == right.label_id && left.key == right.key && left.node_id != right.node_id
        {
            let key = std::str::from_utf8(left.key).unwrap_or("<invalid utf8>");
            return Err(EngineError::InvalidOperation(format!(
                "duplicate live node key membership for label {} and key '{}'",
                left.label_id, key
            )));
        }
    }
    Ok(KeyIndexPayloadPlan { entries })
}

fn write_key_index_plan_payload(
    mut w: &mut impl Write,
    plan: &KeyIndexPayloadPlan<'_>,
) -> Result<(), EngineError> {
    let count = plan.entries.len() as u64;
    write_u64(&mut w, count)?;

    let data_start = 8 + count * 8;
    let mut offset = data_start;
    for entry in &plan.entries {
        write_u64(&mut w, offset)?;
        offset += entry.encoded_len;
    }

    for entry in &plan.entries {
        write_u32(&mut w, entry.label_id)?;
        write_u64(&mut w, entry.node_id)?;
        write_u16(&mut w, entry.key.len() as u16)?;
        w.write_all(entry.key)?;
    }

    Ok(())
}

/// Label posting index payload format:
/// [entry_count: u64]
/// [index: entry_count x (target_label_id: u32, offset: u64, count: u32), sorted by target_label_id]
/// [data: packed u64 record IDs per target, grouped contiguously]
fn prepare_label_posting_index_payload(
    label_posting_index: &HashMap<u32, NodeIdSet>,
) -> LabelPostingIndexPayloadPlan {
    let mut groups: Vec<(u32, Vec<u64>)> = label_posting_index
        .iter()
        .filter(|(_, ids)| !ids.is_empty())
        .map(|(&target_label_id, ids)| {
            let mut sorted_ids: Vec<u64> = ids.iter().copied().collect();
            sorted_ids.sort_unstable();
            (target_label_id, sorted_ids)
        })
        .collect();
    groups.sort_by_key(|(target_label_id, _)| *target_label_id);
    LabelPostingIndexPayloadPlan { groups }
}

fn write_label_posting_index_plan_payload(
    mut w: &mut impl Write,
    plan: &LabelPostingIndexPayloadPlan,
) -> Result<(), EngineError> {
    let entry_count = plan.groups.len() as u64;
    write_u64(&mut w, entry_count)?;

    let data_start = 8 + entry_count * LABEL_POSTING_INDEX_ENTRY_SIZE;
    let mut data_offset = data_start;

    for (target_label_id, ids) in &plan.groups {
        write_u32(&mut w, *target_label_id)?;
        write_u64(&mut w, data_offset)?;
        let count = ids.len() as u32;
        write_u32(&mut w, count)?;
        data_offset += count as u64 * 8; // each ID is u64 = 8 bytes
    }

    for (_, ids) in &plan.groups {
        for &id in ids {
            write_u64(&mut w, id)?;
        }
    }

    Ok(())
}

/// Edge triple index payload format:
/// [count: u64]
/// [entries: count × (from: u64, to: u64, label_id: u32, edge_id: u64),
///   sorted by (from, to, label_id, edge_id)]
fn prepare_edge_triple_index_payload(edges: &NodeIdMap<EdgeRecord>) -> EdgeTripleIndexPayloadPlan {
    let mut entries: Vec<(u64, u64, u32, u64)> = edges
        .values()
        .map(|e| (e.from, e.to, e.label_id, e.id))
        .collect();
    entries.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
            .then(a.3.cmp(&b.3))
    });
    EdgeTripleIndexPayloadPlan { entries }
}

fn write_edge_triple_index_plan_payload(
    mut w: &mut impl Write,
    plan: &EdgeTripleIndexPayloadPlan,
) -> Result<(), EngineError> {
    let count = plan.entries.len() as u64;
    write_u64(&mut w, count)?;

    for (from, to, label_id, edge_id) in &plan.entries {
        write_u64(&mut w, *from)?;
        write_u64(&mut w, *to)?;
        write_u32(&mut w, *label_id)?;
        write_u64(&mut w, *edge_id)?;
    }

    Ok(())
}

fn write_edge_weight_metadata_index_payload(
    mut w: &mut impl Write,
    entries: &[(u32, u32, u64)],
) -> Result<(), EngineError> {
    write_u64(&mut w, entries.len() as u64)?;
    for &(label_id, weight_key, edge_id) in entries {
        write_u32(&mut w, label_id)?;
        write_u32(&mut w, weight_key)?;
        write_u64(&mut w, edge_id)?;
    }

    Ok(())
}

fn write_edge_i64_metadata_index_payload(
    mut w: &mut impl Write,
    entries: &[(u32, i64, u64)],
) -> Result<(), EngineError> {
    write_u64(&mut w, entries.len() as u64)?;
    for &(label_id, value, edge_id) in entries {
        write_u32(&mut w, label_id)?;
        w.write_all(&value.to_le_bytes())?;
        write_u64(&mut w, edge_id)?;
    }

    Ok(())
}

/// Timestamp index payload format:
/// [entry_count: u64]
/// [entries: entry_count × (label_id: u32, updated_at: i64, node_id: u64),
///   sorted by (label_id, updated_at, node_id)]
///
/// Each entry is 20 bytes. Binary search for range start (label_id, from_ms),
/// scan to range end (label_id, to_ms). O(log N) seek + O(results) scan.
fn prepare_timestamp_index_payload(
    time_index: &std::collections::BTreeSet<(u32, i64, u64)>,
) -> TimestampIndexPayloadPlan {
    TimestampIndexPayloadPlan {
        entries: time_index.iter().copied().collect(),
    }
}

fn write_timestamp_index_plan_payload(
    mut w: &mut impl Write,
    plan: &TimestampIndexPayloadPlan,
) -> Result<(), EngineError> {
    let count = plan.entries.len() as u64;
    write_u64(&mut w, count)?;

    for &(label_id, updated_at, node_id) in &plan.entries {
        write_u32(&mut w, label_id)?;
        w.write_all(&updated_at.to_le_bytes())?;
        write_u64(&mut w, node_id)?;
    }

    Ok(())
}

/// Tombstones payload format:
/// [count: u64]
/// [(kind: u8, id: u64, deleted_at: i64, last_write_seq: u64) × count]
/// kind: 0 = node, 1 = edge. Entry size: 25 bytes.
fn write_tombstones_payload(
    mut w: &mut impl Write,
    deleted_nodes: &NodeIdMap<TombstoneEntry>,
    deleted_edges: &NodeIdMap<TombstoneEntry>,
) -> Result<(), EngineError> {
    let count = (deleted_nodes.len() + deleted_edges.len()) as u64;
    write_u64(&mut w, count)?;

    // Write node tombstones (sorted by ID for determinism)
    let mut node_entries: Vec<(u64, &TombstoneEntry)> =
        deleted_nodes.iter().map(|(&id, ts)| (id, ts)).collect();
    node_entries.sort_unstable_by_key(|&(id, _)| id);
    for (id, ts) in node_entries {
        write_u8(&mut w, 0)?; // kind = node
        write_u64(&mut w, id)?;
        w.write_all(&ts.deleted_at.to_le_bytes())?;
        write_u64(&mut w, ts.last_write_seq)?;
    }

    // Write edge tombstones (sorted by ID for determinism)
    let mut edge_entries: Vec<(u64, &TombstoneEntry)> =
        deleted_edges.iter().map(|(&id, ts)| (id, ts)).collect();
    edge_entries.sort_unstable_by_key(|&(id, _)| id);
    for (id, ts) in edge_entries {
        write_u8(&mut w, 1)?; // kind = edge
        write_u64(&mut w, id)?;
        w.write_all(&ts.deleted_at.to_le_bytes())?;
        write_u64(&mut w, ts.last_write_seq)?;
    }

    Ok(())
}

// --- Record encoding helpers ---

fn encode_node_record_into(buf: &mut Vec<u8>, node: &NodeRecord) -> Result<(), EngineError> {
    buf.clear();
    // Note: node.id is NOT written here. It's already in the index.
    buf.push(node.label_ids.len() as u8);
    for &label_id in node.label_ids.as_slice() {
        buf.extend_from_slice(&label_id.to_le_bytes());
    }
    let key_bytes = node.key.as_bytes();
    if key_bytes.len() > u16::MAX as usize {
        return Err(EngineError::SerializationError(format!(
            "node key exceeds maximum length of {} bytes",
            u16::MAX
        )));
    }
    buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(&node.created_at.to_le_bytes());
    buf.extend_from_slice(&node.updated_at.to_le_bytes());
    buf.extend_from_slice(&node.weight.to_le_bytes());
    let props_bytes = rmp_serde::to_vec(&node.props)
        .map_err(|e| EngineError::SerializationError(e.to_string()))?;
    buf.extend_from_slice(&(props_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&props_bytes);
    Ok(())
}

fn encode_edge_record_into(buf: &mut Vec<u8>, edge: &EdgeRecord) -> Result<(), EngineError> {
    buf.clear();
    // Note: edge.id is NOT written here. It's already in the index.
    buf.extend_from_slice(&edge.from.to_le_bytes());
    buf.extend_from_slice(&edge.to.to_le_bytes());
    buf.extend_from_slice(&edge.label_id.to_le_bytes());
    buf.extend_from_slice(&edge.created_at.to_le_bytes());
    buf.extend_from_slice(&edge.updated_at.to_le_bytes());
    buf.extend_from_slice(&edge.weight.to_le_bytes());
    buf.extend_from_slice(&edge.valid_from.to_le_bytes());
    buf.extend_from_slice(&edge.valid_to.to_le_bytes());
    let props_bytes = rmp_serde::to_vec(&edge.props)
        .map_err(|e| EngineError::SerializationError(e.to_string()))?;
    buf.extend_from_slice(&(props_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&props_bytes);
    Ok(())
}

// --- Metadata payload writers ---

/// Node metadata payload format:
/// Header + fixed rows + label-offset table + compact label-ID region.
fn write_node_meta_payload(
    mut meta_w: &mut impl Write,
    node_data: &[(u64, u64, u32)],
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<(), EngineError> {
    let count = node_data.len() as u64;
    write_u64(&mut meta_w, count)?;
    write_u16(&mut meta_w, NODE_META_FIXED_ENTRY_SIZE)?;
    write_u16(&mut meta_w, NODE_META_LABEL_OFFSET_ENTRY_SIZE)?;
    write_u32(&mut meta_w, 0)?;
    let fixed_entries_offset = NODE_META_HEADER_SIZE;
    let fixed_entries_len = count
        .checked_mul(NODE_META_FIXED_ENTRY_SIZE as u64)
        .ok_or_else(|| EngineError::CorruptRecord("node metadata fixed table overflow".into()))?;
    let label_offsets_offset = fixed_entries_offset
        .checked_add(fixed_entries_len)
        .ok_or_else(|| EngineError::CorruptRecord("node metadata offset overflow".into()))?;
    let label_offset_entries = count.checked_add(1).ok_or_else(|| {
        EngineError::CorruptRecord("node metadata label offset count overflow".into())
    })?;
    let label_ids_offset = label_offsets_offset
        .checked_add(label_offset_entries * NODE_META_LABEL_OFFSET_ENTRY_SIZE as u64)
        .ok_or_else(|| {
            EngineError::CorruptRecord("node metadata label ID offset overflow".into())
        })?;
    let mut label_offsets = Vec::with_capacity(label_offset_entries as usize);
    let mut label_ids = Vec::new();
    label_offsets.push(0u64);
    for &(node_id, _, _) in node_data {
        let node = nodes.get(&node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} not found for metadata", node_id))
        })?;
        label_ids.extend_from_slice(node.label_ids.as_slice());
        label_offsets.push(label_ids.len() as u64);
    }
    write_u64(&mut meta_w, fixed_entries_offset)?;
    write_u64(&mut meta_w, label_offsets_offset)?;
    write_u64(&mut meta_w, label_ids_offset)?;
    write_u64(&mut meta_w, label_ids.len() as u64)?;

    for &(node_id, data_offset, data_len) in node_data {
        let node = nodes.get(&node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} not found for metadata", node_id))
        })?;

        write_u64(&mut meta_w, node_id)?;
        write_u64(&mut meta_w, data_offset)?;
        write_u32(&mut meta_w, data_len)?;
        meta_w.write_all(&node.updated_at.to_le_bytes())?;
        meta_w.write_all(&node.weight.to_le_bytes())?;
        write_u16(&mut meta_w, node.key.len() as u16)?;
        write_u64(&mut meta_w, node.last_write_seq)?;
        meta_w.write_all(&[0u8; 6])?;
    }

    for offset in label_offsets {
        write_u64(&mut meta_w, offset)?;
    }
    for label_id in label_ids {
        write_u32(&mut meta_w, label_id)?;
    }

    Ok(())
}

fn write_node_vector_source_components(
    core_writer: &mut PackedCoreWriter,
    node_data: &[(u64, u64, u32)],
    nodes: &NodeIdMap<NodeRecord>,
    node_source: [u8; 32],
) -> Result<(Vec<SegmentComponentRecordV1>, Vec<DensePointInput>), EngineError> {
    let plan = prepare_node_vector_source_plan(node_data, nodes)?;
    if !plan.has_dense && !plan.has_sparse {
        return Ok((Vec::new(), Vec::new()));
    }
    let node_source_dep = source_group_dependency(SegmentSourceGroupKind::NodeSource, node_source);
    let (meta_record, _) = core_writer.write_component(
        SegmentComponentKind::NodeVectorMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::AuxiliaryBlob,
        vec![node_source_dep.clone()],
        component_fingerprint("flush.node_vector_meta", &[]),
        |writer| write_node_vector_meta_payload(writer, &plan),
    )?;
    let vector_blob_deps = vec![node_source_dep, source_component_dependency(&meta_record)];
    let mut records = Vec::with_capacity(3);
    records.push(meta_record);
    if plan.has_dense {
        let (record, _) = core_writer.write_component(
            SegmentComponentKind::NodeDenseVectorBlob,
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
            vector_blob_deps.clone(),
            component_fingerprint("flush.node_dense_vectors", &[]),
            |writer| write_node_dense_vector_blob_payload(writer, &plan, nodes),
        )?;
        records.push(record);
    }
    if plan.has_sparse {
        let (record, _) = core_writer.write_component(
            SegmentComponentKind::NodeSparseVectorBlob,
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
            vector_blob_deps,
            component_fingerprint("flush.node_sparse_vectors", &[]),
            |writer| write_node_sparse_vector_blob_payload(writer, &plan, nodes),
        )?;
        records.push(record);
    }
    Ok((records, plan.dense_points))
}

fn prepare_node_vector_source_plan(
    node_data: &[(u64, u64, u32)],
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<NodeVectorSourcePlan, EngineError> {
    let mut rows = Vec::with_capacity(node_data.len());
    let mut has_dense = false;
    let mut has_sparse = false;
    let mut dense_offset = 0u64;
    let mut sparse_offset = 0u64;
    let mut dense_points = Vec::new();

    for &(node_id, _, _) in node_data {
        let node = nodes.get(&node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} not found for vector source", node_id))
        })?;

        let mut flags = 0u8;
        let mut dense_len = 0u32;
        let mut sparse_len = 0u32;
        let mut entry_dense_offset = 0u64;
        let mut entry_sparse_offset = 0u64;

        if let Some(values) = node.dense_vector.as_ref() {
            flags |= NODE_VECTOR_FLAG_DENSE;
            dense_len = values.len() as u32;
            entry_dense_offset = dense_offset;
            has_dense = true;
            dense_points.push(DensePointInput {
                node_id,
                dense_vector_offset: entry_dense_offset,
                values: values.clone(),
            });
            dense_offset = dense_offset
                .checked_add(values.len() as u64 * DENSE_VECTOR_VALUE_SIZE)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("dense vector blob offset overflow".into())
                })?;
        }

        if let Some(values) = node.sparse_vector.as_ref() {
            flags |= NODE_VECTOR_FLAG_SPARSE;
            sparse_len = values.len() as u32;
            entry_sparse_offset = sparse_offset;
            has_sparse = true;
            sparse_offset = sparse_offset
                .checked_add(values.len() as u64 * SPARSE_VECTOR_ENTRY_SIZE)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("sparse vector blob offset overflow".into())
                })?;
        }

        rows.push(NodeVectorSourceRow {
            node_id,
            flags,
            dense_offset: entry_dense_offset,
            dense_len,
            sparse_offset: entry_sparse_offset,
            sparse_len,
        });
    }

    Ok(NodeVectorSourcePlan {
        rows,
        has_dense,
        has_sparse,
        dense_points,
    })
}

fn write_node_vector_meta_payload(
    mut w: &mut impl Write,
    plan: &NodeVectorSourcePlan,
) -> Result<(), EngineError> {
    write_u64(&mut w, plan.rows.len() as u64)?;
    for row in &plan.rows {
        write_u8(&mut w, row.flags)?;
        w.write_all(&[0u8; 3])?;
        write_u64(&mut w, row.dense_offset)?;
        write_u32(&mut w, row.dense_len)?;
        write_u64(&mut w, row.sparse_offset)?;
        write_u32(&mut w, row.sparse_len)?;
    }
    Ok(())
}

fn write_node_dense_vector_blob_payload(
    w: &mut impl Write,
    plan: &NodeVectorSourcePlan,
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<(), EngineError> {
    for row in &plan.rows {
        if row.dense_len == 0 {
            continue;
        }
        let node = nodes.get(&row.node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "node {} not found for dense vector blob",
                row.node_id
            ))
        })?;
        let Some(values) = node.dense_vector.as_ref() else {
            return Err(EngineError::CorruptRecord(format!(
                "node {} missing dense vector for blob",
                row.node_id
            )));
        };
        for &value in values {
            w.write_all(&value.to_le_bytes())?;
        }
    }
    Ok(())
}

fn write_node_sparse_vector_blob_payload(
    w: &mut impl Write,
    plan: &NodeVectorSourcePlan,
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<(), EngineError> {
    for row in &plan.rows {
        if row.sparse_len == 0 {
            continue;
        }
        let node = nodes.get(&row.node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "node {} not found for sparse vector blob",
                row.node_id
            ))
        })?;
        let Some(values) = node.sparse_vector.as_ref() else {
            return Err(EngineError::CorruptRecord(format!(
                "node {} missing sparse vector for blob",
                row.node_id
            )));
        };
        for &(dimension_id, weight) in values {
            write_u32(w, dimension_id)?;
            w.write_all(&weight.to_le_bytes())?;
        }
    }
    Ok(())
}

/// Edge metadata payload format:
/// [count: u64]
/// [entries: count × EdgeMetaEntry, sorted by edge_id]
///
/// EdgeMetaEntry (80 bytes):
///   edge_id: u64, data_offset: u64, data_len: u32,
///   from: u64, to: u64, label_id: u32,
///   updated_at: i64, weight: f32,
///   valid_from: i64, valid_to: i64,
///   last_write_seq: u64, reserved: u32
fn write_edge_meta_payload(
    mut w: &mut impl Write,
    edge_data: &[(u64, u64, u32)],
    edges: &NodeIdMap<EdgeRecord>,
) -> Result<(), EngineError> {
    let count = edge_data.len() as u64;
    write_u64(&mut w, count)?;

    for &(edge_id, data_offset, data_len) in edge_data {
        let edge = edges.get(&edge_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("edge {} not found for metadata", edge_id))
        })?;

        write_u64(&mut w, edge_id)?;
        write_u64(&mut w, data_offset)?;
        write_u32(&mut w, data_len)?;
        write_u64(&mut w, edge.from)?;
        write_u64(&mut w, edge.to)?;
        write_u32(&mut w, edge.label_id)?;
        w.write_all(&edge.updated_at.to_le_bytes())?;
        w.write_all(&edge.weight.to_le_bytes())?;
        w.write_all(&edge.valid_from.to_le_bytes())?;
        w.write_all(&edge.valid_to.to_le_bytes())?;
        write_u64(&mut w, edge.last_write_seq)?;
        write_u32(&mut w, 0)?; // reserved
    }

    Ok(())
}

fn validate_required_components_before_manifest(
    segment_id: u64,
    records: &[SegmentComponentRecordV1],
) -> Result<(), EngineError> {
    let has_component =
        |kind: &SegmentComponentKind| records.iter().any(|record| record.kind == *kind);
    if !has_component(&SegmentComponentKind::PackedSegmentContainer) {
        return Err(EngineError::CorruptRecord(format!(
            "refusing to publish {SEGMENT_COMPONENT_MANIFEST_FILENAME} for segment {segment_id}: missing packed core container"
        )));
    }

    for kind in [
        SegmentComponentKind::NodeRecords,
        SegmentComponentKind::EdgeRecords,
        SegmentComponentKind::NodeMetadata,
        SegmentComponentKind::EdgeMetadata,
        SegmentComponentKind::Tombstones,
        SegmentComponentKind::KeyIndex,
        SegmentComponentKind::NodeLabelIndex,
        SegmentComponentKind::EdgeLabelIndex,
        SegmentComponentKind::EdgeTripleIndex,
        SegmentComponentKind::AdjOutIndex,
        SegmentComponentKind::AdjOutPostings,
        SegmentComponentKind::AdjInIndex,
        SegmentComponentKind::AdjInPostings,
        SegmentComponentKind::TimestampIndex,
    ] {
        if !has_component(&kind) {
            return Err(EngineError::CorruptRecord(format!(
                "refusing to publish {SEGMENT_COMPONENT_MANIFEST_FILENAME} for segment {segment_id}: missing required component {kind:?}"
            )));
        }
        let record = records
            .iter()
            .find(|record| record.kind == kind)
            .expect("required component was checked above");
        if !matches!(record.handle, ComponentHandleV1::PackedRange { .. }) {
            return Err(EngineError::CorruptRecord(format!(
                "refusing to publish {SEGMENT_COMPONENT_MANIFEST_FILENAME} for segment {segment_id}: required component {kind:?} is not packed"
            )));
        }
    }

    let has_vector_blob = has_component(&SegmentComponentKind::NodeDenseVectorBlob)
        || has_component(&SegmentComponentKind::NodeSparseVectorBlob);
    if has_vector_blob && !has_component(&SegmentComponentKind::NodeVectorMetadata) {
        return Err(EngineError::CorruptRecord(format!(
            "refusing to publish {SEGMENT_COMPONENT_MANIFEST_FILENAME} for segment {segment_id}: vector blob component is missing NodeVectorMetadata"
        )));
    }
    for kind in [
        SegmentComponentKind::NodeVectorMetadata,
        SegmentComponentKind::NodeDenseVectorBlob,
        SegmentComponentKind::NodeSparseVectorBlob,
    ] {
        if let Some(record) = records.iter().find(|record| record.kind == kind) {
            if !matches!(record.handle, ComponentHandleV1::PackedRange { .. }) {
                return Err(EngineError::CorruptRecord(format!(
                    "refusing to publish {SEGMENT_COMPONENT_MANIFEST_FILENAME} for segment {segment_id}: vector source truth component {kind:?} is not packed"
                )));
            }
        }
    }

    for record in records {
        if matches!(record.handle, ComponentHandleV1::ExternalFile { .. })
            && record.kind != SegmentComponentKind::PackedSegmentContainer
            && !is_refreshable_external_component_kind(&record.kind)
        {
            return Err(EngineError::CorruptRecord(format!(
                "refusing to publish {SEGMENT_COMPONENT_MANIFEST_FILENAME} for segment {segment_id}: component {:?} is not allowed as an external file",
                record.kind
            )));
        }
    }

    validate_packed_core_records_contract(records)?;

    Ok(())
}

fn sort_component_records_for_manifest(records: &mut [SegmentComponentRecordV1]) {
    records.sort_by(|left, right| {
        let left_key = (
            left.kind.kind_tag(),
            left.kind.index_id().unwrap_or(0),
            left.created_generation,
        );
        let right_key = (
            right.kind.kind_tag(),
            right.kind.index_id().unwrap_or(0),
            right.created_generation,
        );
        left_key
            .cmp(&right_key)
            .then_with(|| compare_component_handles(&left.handle, &right.handle))
    });
}

fn compare_component_handles(
    left: &ComponentHandleV1,
    right: &ComponentHandleV1,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (
            ComponentHandleV1::ExternalFile {
                relative_path: left_path,
                payload_offset: left_offset,
                payload_len: left_len,
            },
            ComponentHandleV1::ExternalFile {
                relative_path: right_path,
                payload_offset: right_offset,
                payload_len: right_len,
            },
        ) => (0u8, left_path.as_str(), *left_offset, *left_len).cmp(&(
            0u8,
            right_path.as_str(),
            *right_offset,
            *right_len,
        )),
        (
            ComponentHandleV1::PackedRange {
                offset: left_offset,
                len: left_len,
                ..
            },
            ComponentHandleV1::PackedRange {
                offset: right_offset,
                len: right_len,
                ..
            },
        ) => (1u8, *left_offset, *left_len).cmp(&(1u8, *right_offset, *right_len)),
        (ComponentHandleV1::ExternalFile { .. }, ComponentHandleV1::PackedRange { .. }) => {
            Ordering::Less
        }
        (ComponentHandleV1::PackedRange { .. }, ComponentHandleV1::ExternalFile { .. }) => {
            Ordering::Greater
        }
    }
}

fn write_segment_component_manifest(
    seg_dir: &Path,
    manifest: &SegmentComponentManifestV1,
) -> Result<(), EngineError> {
    let tmp_path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_TMP_FILENAME);
    let final_path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
    let data = encode_manifest_envelope(manifest)?;
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    fsync_dir(seg_dir)?;
    Ok(())
}

fn read_segment_component_manifest(
    seg_dir: &Path,
) -> Result<SegmentComponentManifestV1, EngineError> {
    let path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
    let data = fs::read(&path)?;
    decode_manifest_envelope(&data)
}

pub(crate) fn cleanup_orphan_optional_component_files(seg_dir: &Path) {
    let Ok(manifest) = read_segment_component_manifest(seg_dir) else {
        return;
    };

    let mut referenced = HashSet::new();
    for record in &manifest.components {
        if let ComponentHandleV1::ExternalFile { relative_path, .. } = &record.handle {
            referenced.insert(PathBuf::from(relative_path));
        }
    }
    for record in &manifest.unknown_optional_components {
        if record.wire.handle.handle_tag == 1 {
            if let Some(relative_path) = &record.wire.handle.relative_path {
                referenced.insert(PathBuf::from(relative_path));
            }
        }
    }

    let mut dirty_dirs = HashSet::new();
    cleanup_orphan_optional_component_files_in_dir(seg_dir, seg_dir, &referenced, &mut dirty_dirs);
    for dir in dirty_dirs {
        let _ = fsync_dir(&dir);
    }
}

fn cleanup_orphan_optional_component_files_in_dir(
    seg_dir: &Path,
    dir: &Path,
    referenced: &HashSet<PathBuf>,
    dirty_dirs: &mut HashSet<PathBuf>,
) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if file_type.is_dir() {
            cleanup_orphan_optional_component_files_in_dir(seg_dir, &path, referenced, dirty_dirs);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let Ok(relative_path) = path.strip_prefix(seg_dir) else {
            continue;
        };
        if referenced.contains(relative_path) {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if (is_optional_refresh_tmp_file_name(file_name)
            || is_optional_generation_file_name(file_name)
            || is_compound_secondary_index_base_file_name(file_name))
            && fs::remove_file(&path).is_ok()
        {
            if let Some(parent) = path.parent() {
                dirty_dirs.insert(parent.to_path_buf());
            }
        }
    }
}

fn is_optional_refresh_tmp_file_name(file_name: &str) -> bool {
    file_name.contains(".refresh_tmp.")
}

fn is_compound_secondary_index_base_file_name(file_name: &str) -> bool {
    const PREFIXES: [&str; 4] = [
        "node_compound_eq_",
        "node_compound_range_",
        "edge_compound_eq_",
        "edge_compound_range_",
    ];
    let Some(id) = PREFIXES
        .iter()
        .find_map(|prefix| file_name.strip_prefix(prefix))
        .and_then(|rest| rest.strip_suffix(".dat"))
    else {
        return false;
    };
    !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_optional_generation_file_name(file_name: &str) -> bool {
    let Some(g_pos) = file_name.rfind(".g") else {
        return false;
    };
    let generation = &file_name[g_pos + 2..];
    let generation = generation
        .split_once('.')
        .map_or(generation, |(generation, _)| generation);
    generation.len() == 16 && generation.bytes().all(|byte| byte.is_ascii_digit())
}

fn optional_generation_relative_path(base_relative_path: &str, generation: u64) -> String {
    let path = Path::new(base_relative_path);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(base_relative_path);
    let extension = path.extension().and_then(|value| value.to_str());
    let file_name = match extension {
        Some(extension) => format!("{stem}.g{generation:016}.{extension}"),
        None => format!("{stem}.g{generation:016}"),
    };
    match path.parent().and_then(|parent| parent.to_str()) {
        Some(parent) if !parent.is_empty() => format!("{parent}/{file_name}"),
        _ => file_name,
    }
}

fn optional_refresh_tmp_relative_path(base_relative_path: &str, generation: u64) -> String {
    let path = Path::new(base_relative_path);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(base_relative_path);
    let nonce = OPTIONAL_REFRESH_TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let file_name = format!(
        ".{stem}.refresh_tmp.{}.{}.g{generation:016}.dat",
        std::process::id(),
        nonce
    );
    match path.parent().and_then(|parent| parent.to_str()) {
        Some(parent) if !parent.is_empty() => format!("{parent}/{file_name}"),
        _ => file_name,
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_optional_component_with_writer(
    seg_dir: &Path,
    kind: SegmentComponentKind,
    base_relative_path: &str,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    dependencies: Vec<ComponentDependencyV1>,
    build_fingerprint: u64,
    write_payload: impl FnOnce(&mut ComponentIdentityWriter) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    if !is_refreshable_external_component_kind(&kind) {
        return Err(EngineError::CorruptRecord(format!(
            "component {kind:?} is not eligible for optional external refresh"
        )));
    }
    if matches!(&requirement, ComponentRequirement::Required) {
        return Err(EngineError::CorruptRecord(format!(
            "component {kind:?} optional refresh cannot publish a required component"
        )));
    }

    let captured_manifest = read_segment_component_manifest(seg_dir)?;
    let source_groups = segment_source_groups_from_records(
        captured_manifest.segment_id,
        captured_manifest.node_count,
        captured_manifest.edge_count,
        &captured_manifest.components,
    )?;
    if source_groups.segment_data_id != captured_manifest.segment_data_id {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} source identity changed before optional publication",
            captured_manifest.segment_id
        )));
    }
    let generation = captured_manifest.generation.saturating_add(1);
    let final_relative_path = optional_generation_relative_path(base_relative_path, generation);
    let tmp_relative_path = optional_refresh_tmp_relative_path(base_relative_path, generation);
    let tmp_path = seg_dir.join(&tmp_relative_path);
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut writer = ComponentIdentityWriter::create(
        &tmp_path,
        final_relative_path.clone(),
        SEGMENT_FORMAT_VERSION,
        captured_manifest.segment_id,
        kind.clone(),
        FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
        generation,
        requirement,
        trust_class,
        build_fingerprint,
        true,
    )?;
    write_payload(&mut writer)?;
    let record = writer.finish(dependencies)?;

    let current_manifest = read_segment_component_manifest(seg_dir)?;
    if current_manifest.segment_id != captured_manifest.segment_id
        || current_manifest.segment_data_id != captured_manifest.segment_data_id
        || current_manifest.generation != captured_manifest.generation
    {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} {}",
            captured_manifest.segment_id, OPTIONAL_COMPONENT_PUBLICATION_CONFLICT_MESSAGE
        )));
    }

    let final_path = seg_dir.join(&final_relative_path);
    fs::rename(&tmp_path, &final_path)?;
    if let Some(parent) = final_path.parent() {
        fsync_dir(parent)?;
    }

    let mut replacement = current_manifest;
    replacement.generation = generation;
    replacement.built_at_ms = current_time_millis();
    replacement.build_kind = SegmentComponentBuildKind::OptionalRefresh;
    replacement
        .components
        .retain(|existing| existing.kind != kind);
    replacement.components.push(record);
    write_segment_component_manifest(seg_dir, &replacement)
}

/// Fsync the directory to ensure metadata (file creation) is durable.
/// No-op on Windows. NTFS doesn't support directory fsync via File::open().
fn fsync_dir(dir: &Path) -> Result<(), EngineError> {
    #[cfg(not(target_os = "windows"))]
    {
        let d = File::open(dir)?;
        d.sync_all()?;
    }
    #[cfg(target_os = "windows")]
    let _ = dir;
    Ok(())
}

/// Return the segment directory path for a given segment ID within a db directory.
pub fn segment_dir(db_dir: &Path, segment_id: u64) -> PathBuf {
    db_dir
        .join("segments")
        .join(format!("seg_{:04}", segment_id))
}

/// Return the temporary segment directory path (used during flush before atomic rename).
pub fn segment_tmp_dir(db_dir: &Path, segment_id: u64) -> PathBuf {
    db_dir
        .join("segments")
        .join(format!("seg_{:04}.tmp", segment_id))
}

// --- Fast-merge compaction support ---

pub(crate) struct FastMergeCopyInfo {
    pub orig_data_start: u64,
    pub new_data_base: u64,
}

/// Write merged node records payload by binary copy from multiple non-overlapping segments.
///
/// Instead of deserializing and re-serializing every record, this copies raw
/// record bytes directly from mmap'd input segments and rebuilds the merged
/// index with adjusted offsets. Record lengths are derived from the source
/// node records index/data layout, which lets the fast path cross-check metadata
/// metadata later instead of trusting it blindly.
///
/// Returns per-segment offset rebasing info so compaction metadata can compute
/// merged `data_offset` values directly from sidecars without a second data scan.
pub(crate) fn write_merged_nodes_dat(
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
) -> Result<(SegmentComponentRecordV1, Vec<FastMergeCopyInfo>), EngineError> {
    let mut seg_info: Vec<(u64, usize, usize)> = Vec::with_capacity(segments.len());
    let mut total_count: u64 = 0;

    for (seg_idx, seg) in segments.iter().enumerate() {
        let mmap = seg.raw_nodes_mmap();
        if mmap.len() < 8 {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} node records payload too short for count header: {} bytes",
                seg.segment_id,
                mmap.len()
            )));
        }
        let count = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        let index_bytes = (count as usize)
            .checked_mul(NODE_INDEX_ENTRY_SIZE as usize)
            .ok_or_else(|| {
                EngineError::CorruptRecord(format!(
                    "segment {} node index size overflow for {} entries",
                    seg.segment_id, count
                ))
            })?;
        let data_start = 8usize.checked_add(index_bytes).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "segment {} node data start overflow",
                seg.segment_id
            ))
        })?;
        if data_start > mmap.len() {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} node records index exceeds payload length: start={}, len={}",
                seg.segment_id,
                data_start,
                mmap.len()
            )));
        }
        seg_info.push((count, data_start, mmap.len() - data_start));
        total_count = total_count.checked_add(count).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "total node count overflow while merging segment {} (index {})",
                seg.segment_id, seg_idx
            ))
        })?;
    }

    let (record, copy_info) = core_writer.write_component(
        SegmentComponentKind::NodeRecords,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryData,
        Vec::new(),
        component_fingerprint("flush.nodes", &[]),
        |w| {
            write_u64(w, total_count)?;

            let merged_data_start =
                8u64.checked_add(total_count.checked_mul(NODE_INDEX_ENTRY_SIZE).ok_or_else(
                    || EngineError::CorruptRecord("merged node index size overflow".into()),
                )?)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("merged node data start overflow".into())
                })?;
            let mut cumulative_data_offset = merged_data_start;
            let mut data_offsets: Vec<u64> = Vec::with_capacity(segments.len());
            for &(_, _, data_size) in &seg_info {
                data_offsets.push(cumulative_data_offset);
                cumulative_data_offset = cumulative_data_offset
                    .checked_add(data_size as u64)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(
                            "merged node records payload size overflow".into(),
                        )
                    })?;
            }
            let mut all_entries: Vec<(u64, u64)> = Vec::with_capacity(total_count as usize);
            for (seg_idx, seg) in segments.iter().enumerate() {
                let mmap = seg.raw_nodes_mmap();
                let (count, orig_data_start, _) = seg_info[seg_idx];
                if count == 0 {
                    continue;
                }

                let offset_adj = data_offsets[seg_idx]
                    .checked_sub(orig_data_start as u64)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "segment {} node offset adjustment underflow",
                            seg.segment_id
                        ))
                    })?;

                for i in 0..count as usize {
                    let entry_off = 8 + i * NODE_INDEX_ENTRY_SIZE as usize;
                    let node_id =
                        u64::from_le_bytes(mmap[entry_off..entry_off + 8].try_into().unwrap());
                    let old_offset =
                        u64::from_le_bytes(mmap[entry_off + 8..entry_off + 16].try_into().unwrap());
                    let new_offset = old_offset.checked_add(offset_adj).ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "segment {} node {} merged offset overflow",
                            seg.segment_id, node_id
                        ))
                    })?;
                    all_entries.push((node_id, new_offset));
                }
            }
            all_entries.sort_unstable_by_key(|(id, _)| *id);

            for &(node_id, offset) in &all_entries {
                write_u64(w, node_id)?;
                write_u64(w, offset)?;
            }

            for (seg_idx, seg) in segments.iter().enumerate() {
                let mmap = seg.raw_nodes_mmap();
                let (_, data_start, data_size) = seg_info[seg_idx];
                if data_size > 0 {
                    w.write_all(&mmap[data_start..data_start + data_size])?;
                }
            }

            Ok(seg_info
                .iter()
                .zip(data_offsets)
                .map(|((_, data_start, _), new_data_base)| FastMergeCopyInfo {
                    orig_data_start: *data_start as u64,
                    new_data_base,
                })
                .collect())
        },
    )?;
    Ok((record, copy_info))
}

/// Write merged edge records payload by binary copy from multiple non-overlapping segments.
/// Same approach as `write_merged_nodes_dat`.
///
/// Returns per-segment offset rebasing info so compaction metadata can compute
/// merged `data_offset` values directly from sidecars without a second data scan.
pub(crate) fn write_merged_edges_dat(
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
) -> Result<(SegmentComponentRecordV1, Vec<FastMergeCopyInfo>), EngineError> {
    let mut seg_info: Vec<(u64, usize, usize)> = Vec::with_capacity(segments.len());
    let mut total_count: u64 = 0;

    for (seg_idx, seg) in segments.iter().enumerate() {
        let mmap = seg.raw_edges_mmap();
        if mmap.len() < 8 {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} edge records payload too short for count header: {} bytes",
                seg.segment_id,
                mmap.len()
            )));
        }
        let count = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        let index_bytes = (count as usize)
            .checked_mul(EDGE_INDEX_ENTRY_SIZE as usize)
            .ok_or_else(|| {
                EngineError::CorruptRecord(format!(
                    "segment {} edge index size overflow for {} entries",
                    seg.segment_id, count
                ))
            })?;
        let data_start = 8usize.checked_add(index_bytes).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "segment {} edge data start overflow",
                seg.segment_id
            ))
        })?;
        if data_start > mmap.len() {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} edge records index exceeds payload length: start={}, len={}",
                seg.segment_id,
                data_start,
                mmap.len()
            )));
        }
        seg_info.push((count, data_start, mmap.len() - data_start));
        total_count = total_count.checked_add(count).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "total edge count overflow while merging segment {} (index {})",
                seg.segment_id, seg_idx
            ))
        })?;
    }

    let (record, copy_info) = core_writer.write_component(
        SegmentComponentKind::EdgeRecords,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryData,
        Vec::new(),
        component_fingerprint("flush.edges", &[]),
        |w| {
            write_u64(w, total_count)?;

            let merged_data_start =
                8u64.checked_add(total_count.checked_mul(EDGE_INDEX_ENTRY_SIZE).ok_or_else(
                    || EngineError::CorruptRecord("merged edge index size overflow".into()),
                )?)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("merged edge data start overflow".into())
                })?;
            let mut cumulative_data_offset = merged_data_start;
            let mut data_offsets: Vec<u64> = Vec::with_capacity(segments.len());
            for &(_, _, data_size) in &seg_info {
                data_offsets.push(cumulative_data_offset);
                cumulative_data_offset = cumulative_data_offset
                    .checked_add(data_size as u64)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(
                            "merged edge records payload size overflow".into(),
                        )
                    })?;
            }

            let mut all_entries: Vec<(u64, u64)> = Vec::with_capacity(total_count as usize);
            for (seg_idx, seg) in segments.iter().enumerate() {
                let mmap = seg.raw_edges_mmap();
                let (count, orig_data_start, _) = seg_info[seg_idx];
                if count == 0 {
                    continue;
                }

                let offset_adj = data_offsets[seg_idx]
                    .checked_sub(orig_data_start as u64)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "segment {} edge offset adjustment underflow",
                            seg.segment_id
                        ))
                    })?;

                for i in 0..count as usize {
                    let entry_off = 8 + i * EDGE_INDEX_ENTRY_SIZE as usize;
                    let edge_id =
                        u64::from_le_bytes(mmap[entry_off..entry_off + 8].try_into().unwrap());
                    let old_offset =
                        u64::from_le_bytes(mmap[entry_off + 8..entry_off + 16].try_into().unwrap());
                    let new_offset = old_offset.checked_add(offset_adj).ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "segment {} edge {} merged offset overflow",
                            seg.segment_id, edge_id
                        ))
                    })?;
                    all_entries.push((edge_id, new_offset));
                }
            }
            all_entries.sort_unstable_by_key(|(id, _)| *id);

            for &(edge_id, offset) in &all_entries {
                write_u64(w, edge_id)?;
                write_u64(w, offset)?;
            }

            for (seg_idx, seg) in segments.iter().enumerate() {
                let mmap = seg.raw_edges_mmap();
                let (_, data_start, data_size) = seg_info[seg_idx];
                if data_size > 0 {
                    w.write_all(&mmap[data_start..data_start + data_size])?;
                }
            }

            Ok(seg_info
                .iter()
                .zip(data_offsets)
                .map(|((_, data_start, _), new_data_base)| FastMergeCopyInfo {
                    orig_data_start: *data_start as u64,
                    new_data_base,
                })
                .collect())
        },
    )?;
    Ok((record, copy_info))
}

/// Write node records payload by raw-copying only winning record byte spans from source segments.
///
/// Used by V3 compaction: the planner has already decided which records win,
/// so we skip all dropped records entirely (never decode them).
///
/// `winners` is sorted by node_id: `(node_id, seg_idx, data_offset, data_len)`.
///
/// Returns Vec of `(node_id, new_data_offset, data_len)` matching the output payload,
/// for metadata writing.
pub(crate) fn write_v3_nodes_dat(
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
    winners: &[(u64, usize, u64, u32)],
) -> Result<CompactionDatOutput, EngineError> {
    core_writer.write_component(
        SegmentComponentKind::NodeRecords,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryData,
        Vec::new(),
        component_fingerprint("flush.nodes", &[]),
        |w| {
            let count = winners.len() as u64;
            write_u64(w, count)?;

            // Calculate data section start
            let data_start = 8 + count * NODE_INDEX_ENTRY_SIZE;

            // Build index entries and output info
            let mut node_data = Vec::with_capacity(winners.len());
            let mut data_offset = data_start;
            for &(node_id, _, _, data_len) in winners {
                // Write index entry: (node_id, offset)
                write_u64(w, node_id)?;
                write_u64(w, data_offset)?;
                node_data.push((node_id, data_offset, data_len));
                data_offset += data_len as u64;
            }

            // Write data section by copying raw bytes from source segments
            for &(node_id, seg_idx, src_offset, data_len) in winners {
                let mmap = segments[seg_idx].raw_nodes_mmap();
                let start = src_offset as usize;
                let end = start.checked_add(data_len as usize).ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "node {} data span offset overflow: start={}, len={}",
                        node_id, start, data_len
                    ))
                })?;
                if end > mmap.len() {
                    return Err(EngineError::CorruptRecord(format!(
                        "node {} data span [{}, {}) exceeds mmap length {}",
                        node_id,
                        start,
                        end,
                        mmap.len()
                    )));
                }
                w.write_all(&mmap[start..end])?;
            }

            Ok(node_data)
        },
    )
}

/// Write edge records payload by raw-copying only winning record byte spans from source segments.
///
/// Same approach as `write_v3_nodes_dat` but for edge records.
///
/// `winners` is sorted by edge_id: `(edge_id, seg_idx, data_offset, data_len)`.
///
/// Returns Vec of `(edge_id, new_data_offset, data_len)` matching the output payload.
pub(crate) fn write_v3_edges_dat(
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
    winners: &[(u64, usize, u64, u32)],
) -> Result<CompactionDatOutput, EngineError> {
    core_writer.write_component(
        SegmentComponentKind::EdgeRecords,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryData,
        Vec::new(),
        component_fingerprint("flush.edges", &[]),
        |w| {
            let count = winners.len() as u64;
            write_u64(w, count)?;

            let data_start = 8 + count * EDGE_INDEX_ENTRY_SIZE;

            let mut edge_data = Vec::with_capacity(winners.len());
            let mut data_offset = data_start;
            for &(edge_id, _, _, data_len) in winners {
                write_u64(w, edge_id)?;
                write_u64(w, data_offset)?;
                edge_data.push((edge_id, data_offset, data_len));
                data_offset += data_len as u64;
            }

            for &(edge_id, seg_idx, src_offset, data_len) in winners {
                let mmap = segments[seg_idx].raw_edges_mmap();
                let start = src_offset as usize;
                let end = start.checked_add(data_len as usize).ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "edge {} data span offset overflow: start={}, len={}",
                        edge_id, start, data_len
                    ))
                })?;
                if end > mmap.len() {
                    return Err(EngineError::CorruptRecord(format!(
                        "edge {} data span [{}, {}) exceeds mmap length {}",
                        edge_id,
                        start,
                        end,
                        mmap.len()
                    )));
                }
                w.write_all(&mmap[start..end])?;
            }

            Ok(edge_data)
        },
    )
}

// ==========================================================================
// Metadata-driven compaction index writers (V3)
// ==========================================================================

/// Node metadata collected from source sidecars for metadata-driven index building.
pub(crate) struct CompactNodeMeta {
    pub node_id: u64,
    pub new_data_offset: u64,
    pub data_len: u32,
    pub label_ids: NodeLabelSet,
    pub updated_at: i64,
    pub weight: f32,
    pub key_len: u16,
    pub dense_vector_offset: u64,
    pub dense_vector_len: u32,
    pub sparse_vector_offset: u64,
    pub sparse_vector_len: u32,
    pub src_seg_idx: usize,
    pub src_data_offset: u64,
    pub last_write_seq: u64,
}

/// Edge metadata collected from source sidecars for metadata-driven index building.
pub(crate) struct CompactEdgeMeta {
    pub edge_id: u64,
    pub new_data_offset: u64,
    pub data_len: u32,
    pub from: u64,
    pub to: u64,
    pub label_id: u32,
    pub updated_at: i64,
    pub weight: f32,
    pub valid_from: i64,
    pub valid_to: i64,
    pub src_seg_idx: usize,
    pub src_data_offset: u64,
    pub last_write_seq: u64,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_compaction_source_components(
    segment_id: u64,
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
    node_record: SegmentComponentRecordV1,
    edge_record: SegmentComponentRecordV1,
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
) -> Result<(SegmentComponentSourceGroups, Vec<DensePointInput>), EngineError> {
    let mut records = vec![node_record, edge_record];

    let (node_meta, _) = core_writer.write_component(
        SegmentComponentKind::NodeMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryMetadata,
        Vec::new(),
        component_fingerprint("flush.node_meta", &[]),
        |writer| write_compact_node_meta_payload(writer, node_metas),
    )?;
    records.push(node_meta);

    let (edge_meta, _) = core_writer.write_component(
        SegmentComponentKind::EdgeMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryMetadata,
        Vec::new(),
        component_fingerprint("flush.edge_meta", &[]),
        |writer| write_compact_edge_meta_payload(writer, edge_metas),
    )?;
    records.push(edge_meta);

    let (tombstones, _) = core_writer.write_component(
        SegmentComponentKind::Tombstones,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryMetadata,
        Vec::new(),
        component_fingerprint("flush.tombstones", &[]),
        |writer| write_u64(writer, 0),
    )?;
    records.push(tombstones);

    let preliminary_source_groups = segment_source_groups_from_records(
        segment_id,
        node_metas.len() as u64,
        edge_metas.len() as u64,
        &records,
    )?;
    let (vector_records, dense_points) = write_node_vector_source_components_from_meta(
        core_writer,
        segments,
        node_metas,
        preliminary_source_groups.node_source,
    )?;
    records.extend(vector_records);

    let source_groups = segment_source_groups_from_records(
        segment_id,
        node_metas.len() as u64,
        edge_metas.len() as u64,
        &records,
    )?;
    Ok((source_groups, dense_points))
}

fn write_compact_node_meta_payload(
    mut w: &mut impl Write,
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    write_u64(&mut w, node_metas.len() as u64)?;
    write_u16(&mut w, NODE_META_FIXED_ENTRY_SIZE)?;
    write_u16(&mut w, NODE_META_LABEL_OFFSET_ENTRY_SIZE)?;
    write_u32(&mut w, 0)?;
    let count = node_metas.len() as u64;
    let fixed_entries_offset = NODE_META_HEADER_SIZE;
    let fixed_entries_len = count
        .checked_mul(NODE_META_FIXED_ENTRY_SIZE as u64)
        .ok_or_else(|| EngineError::CorruptRecord("node metadata fixed table overflow".into()))?;
    let label_offsets_offset = fixed_entries_offset
        .checked_add(fixed_entries_len)
        .ok_or_else(|| EngineError::CorruptRecord("node metadata offset overflow".into()))?;
    let label_offset_entries = count.checked_add(1).ok_or_else(|| {
        EngineError::CorruptRecord("node metadata label offset count overflow".into())
    })?;
    let label_ids_offset = label_offsets_offset
        .checked_add(label_offset_entries * NODE_META_LABEL_OFFSET_ENTRY_SIZE as u64)
        .ok_or_else(|| {
            EngineError::CorruptRecord("node metadata label ID offset overflow".into())
        })?;
    let mut label_offsets = Vec::with_capacity(label_offset_entries as usize);
    let mut label_ids = Vec::new();
    label_offsets.push(0u64);
    for nm in node_metas {
        label_ids.extend_from_slice(nm.label_ids.as_slice());
        label_offsets.push(label_ids.len() as u64);
    }
    write_u64(&mut w, fixed_entries_offset)?;
    write_u64(&mut w, label_offsets_offset)?;
    write_u64(&mut w, label_ids_offset)?;
    write_u64(&mut w, label_ids.len() as u64)?;
    for nm in node_metas {
        write_u64(&mut w, nm.node_id)?;
        write_u64(&mut w, nm.new_data_offset)?;
        write_u32(&mut w, nm.data_len)?;
        w.write_all(&nm.updated_at.to_le_bytes())?;
        w.write_all(&nm.weight.to_le_bytes())?;
        write_u16(&mut w, nm.key_len)?;
        write_u64(&mut w, nm.last_write_seq)?;
        w.write_all(&[0u8; 6])?;
    }
    for offset in label_offsets {
        write_u64(&mut w, offset)?;
    }
    for label_id in label_ids {
        write_u32(&mut w, label_id)?;
    }
    Ok(())
}

fn write_compact_edge_meta_payload(
    mut w: &mut impl Write,
    edge_metas: &[CompactEdgeMeta],
) -> Result<(), EngineError> {
    write_u64(&mut w, edge_metas.len() as u64)?;
    for em in edge_metas {
        write_u64(&mut w, em.edge_id)?;
        write_u64(&mut w, em.new_data_offset)?;
        write_u32(&mut w, em.data_len)?;
        write_u64(&mut w, em.from)?;
        write_u64(&mut w, em.to)?;
        write_u32(&mut w, em.label_id)?;
        w.write_all(&em.updated_at.to_le_bytes())?;
        w.write_all(&em.weight.to_le_bytes())?;
        w.write_all(&em.valid_from.to_le_bytes())?;
        w.write_all(&em.valid_to.to_le_bytes())?;
        write_u64(&mut w, em.last_write_seq)?;
        write_u32(&mut w, 0)?;
    }
    Ok(())
}

fn prepare_key_index_payload_from_meta<'a>(
    segments: &'a [Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<KeyIndexPayloadPlan<'a>, EngineError> {
    let entry_count = node_metas
        .iter()
        .map(|meta| meta.label_ids.len())
        .sum::<usize>();
    let mut entries: Vec<KeyIndexEntryPlan<'a>> = Vec::with_capacity(entry_count);
    for nm in node_metas {
        let key = raw_node_key_bytes_from_meta(segments, nm)?;
        for &label_id in nm.label_ids.as_slice() {
            entries.push(KeyIndexEntryPlan {
                label_id,
                key,
                node_id: nm.node_id,
                encoded_len: 4 + 8 + 2 + nm.key_len as u64,
            });
        }
    }
    entries.sort_by(|a, b| {
        a.label_id
            .cmp(&b.label_id)
            .then_with(|| a.key.cmp(b.key))
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    for pair in entries.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if left.label_id == right.label_id && left.key == right.key && left.node_id != right.node_id
        {
            let key = std::str::from_utf8(left.key).unwrap_or("<invalid utf8>");
            return Err(EngineError::InvalidOperation(format!(
                "duplicate live node key membership for label {} and key '{}'",
                left.label_id, key
            )));
        }
    }
    Ok(KeyIndexPayloadPlan { entries })
}

fn raw_node_key_bytes_from_meta<'a>(
    segments: &'a [Arc<SegmentReader>],
    nm: &CompactNodeMeta,
) -> Result<&'a [u8], EngineError> {
    let src_mmap = segments[nm.src_seg_idx].raw_nodes_mmap();
    let record_start = nm.src_data_offset as usize;
    let record_end = record_start
        .checked_add(nm.data_len as usize)
        .ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} source record span overflow", nm.node_id))
        })?;
    if record_end > src_mmap.len() {
        return Err(EngineError::CorruptRecord(format!(
            "node {} source record span [{}, {}) exceeds source mmap length {}",
            nm.node_id,
            record_start,
            record_end,
            src_mmap.len()
        )));
    }
    if record_start >= record_end {
        return Err(EngineError::CorruptRecord(format!(
            "node {} source record is empty",
            nm.node_id
        )));
    }

    let raw_label_count = src_mmap[record_start] as usize;
    if raw_label_count != nm.label_ids.len() {
        return Err(EngineError::CorruptRecord(format!(
            "node {} raw label count {} does not match metadata label count {}",
            nm.node_id,
            raw_label_count,
            nm.label_ids.len()
        )));
    }
    let key_len_offset = record_start
        .checked_add(1)
        .and_then(|offset| offset.checked_add(raw_label_count.checked_mul(4)?))
        .ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} key length offset overflow", nm.node_id))
        })?;
    let key_start = key_len_offset.checked_add(2).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} key start offset overflow", nm.node_id))
    })?;
    if key_start > record_end {
        return Err(EngineError::CorruptRecord(format!(
            "node {} key length field exceeds source record span",
            nm.node_id
        )));
    }
    let raw_key_len = u16::from_le_bytes(
        src_mmap[key_len_offset..key_len_offset + 2]
            .try_into()
            .unwrap(),
    );
    if raw_key_len != nm.key_len {
        return Err(EngineError::CorruptRecord(format!(
            "node {} raw key length {} does not match metadata key length {}",
            nm.node_id, raw_key_len, nm.key_len
        )));
    }
    let key_end = key_start.checked_add(nm.key_len as usize).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} key end offset overflow", nm.node_id))
    })?;
    if key_end > record_end {
        return Err(EngineError::CorruptRecord(format!(
            "node {} key bytes [{}, {}) exceed source record span ending at {}",
            nm.node_id, key_start, key_end, record_end
        )));
    }
    Ok(&src_mmap[key_start..key_end])
}

fn prepare_node_label_index_payload_from_meta(
    node_metas: &[CompactNodeMeta],
) -> LabelPostingIndexPayloadPlan {
    let mut groups: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for nm in node_metas {
        for &label_id in nm.label_ids.as_slice() {
            groups.entry(label_id).or_default().push(nm.node_id);
        }
    }
    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    LabelPostingIndexPayloadPlan {
        groups: groups.into_iter().collect(),
    }
}

fn prepare_edge_label_index_payload_from_meta(
    edge_metas: &[CompactEdgeMeta],
) -> LabelPostingIndexPayloadPlan {
    let mut groups: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for em in edge_metas {
        groups.entry(em.label_id).or_default().push(em.edge_id);
    }
    for ids in groups.values_mut() {
        ids.sort_unstable();
    }
    LabelPostingIndexPayloadPlan {
        groups: groups.into_iter().collect(),
    }
}

fn prepare_timestamp_index_payload_from_meta(
    node_metas: &[CompactNodeMeta],
) -> TimestampIndexPayloadPlan {
    let mut entries: Vec<(u32, i64, u64)> = node_metas
        .iter()
        .flat_map(|nm| {
            nm.label_ids
                .as_slice()
                .iter()
                .map(move |&label_id| (label_id, nm.updated_at, nm.node_id))
        })
        .collect();
    entries.sort_unstable();
    TimestampIndexPayloadPlan { entries }
}

fn prepare_edge_triple_index_payload_from_meta(
    edge_metas: &[CompactEdgeMeta],
) -> EdgeTripleIndexPayloadPlan {
    let mut entries: Vec<(u64, u64, u32, u64)> = edge_metas
        .iter()
        .map(|em| (em.from, em.to, em.label_id, em.edge_id))
        .collect();
    entries.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
            .then(a.3.cmp(&b.3))
    });
    EdgeTripleIndexPayloadPlan { entries }
}

fn prepare_edge_metadata_index_components_from_meta(
    edge_metas: &[CompactEdgeMeta],
) -> EdgeMetadataIndexEntries {
    let mut entries = EdgeMetadataIndexEntries::with_capacity(edge_metas.len());
    for edge in edge_metas {
        entries.push(
            edge.label_id,
            edge.updated_at,
            edge.weight,
            edge.valid_from,
            edge.valid_to,
            edge.edge_id,
        );
    }
    entries.sort_all();
    entries
}

fn prepare_adjacency_payloads_from_meta(
    edge_metas: &[CompactEdgeMeta],
    is_outgoing: bool,
) -> AdjacencyPayloadPlan {
    let mut groups: AdjacencyGroups = BTreeMap::new();
    for em in edge_metas {
        let (node_id, neighbor_id) = if is_outgoing {
            (em.from, em.to)
        } else {
            (em.to, em.from)
        };
        groups.entry((node_id, em.label_id)).or_default().push((
            em.edge_id,
            neighbor_id,
            em.weight,
            em.valid_from,
            em.valid_to,
        ));
    }
    for postings in groups.values_mut() {
        postings.sort_unstable_by_key(|&(edge_id, ..)| edge_id);
    }

    let mut offset = 0u64;
    let groups = groups
        .into_iter()
        .map(|((node_id, label_id), postings)| {
            let group = AdjacencyGroupPlan {
                node_id,
                label_id,
                offset,
                postings,
            };
            offset += adjacency_postings_len(&group.postings);
            group
        })
        .collect();
    AdjacencyPayloadPlan { groups }
}

/// Build all secondary indexes and sidecars from metadata without Memtable decode.
/// Used by V3 compaction path.
///
/// IMPORTANT: Two index-writing paths exist and must stay in sync:
///   1. `write_segment()` (flush path, builds indexes from Memtable)
///   2. `write_indexes_from_metadata_with_secondary_indexes()` [this fn] (compaction path)
///
/// If you add a new index type, you MUST add it to BOTH paths.
///
/// `node_metas` and `edge_metas` must be sorted by ID.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_indexes_from_metadata_with_secondary_indexes(
    segment_id: u64,
    seg_dir: &Path,
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    dense_config: Option<&DenseVectorConfig>,
    dense_points: Vec<DensePointInput>,
    write_degree_sidecar: bool,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<CompactionComponentBuildOutput, EngineError> {
    let partitions = partition_secondary_indexes(secondary_indexes);
    let (index_result, stats_core_result) = engine_cpu_join(
        || {
            engine_cpu_try_join(
                || {
                    engine_cpu_try_join(
                        || {
                            engine_cpu_try_join(
                                || {
                                    let key_index =
                                        prepare_key_index_payload_from_meta(segments, node_metas)?;
                                    let node_label_index =
                                        prepare_node_label_index_payload_from_meta(node_metas);
                                    let mut outcome = DeclaredSidecarWriteOutcome::default();
                                    let branch_outcome =
                                        write_declared_equality_sidecars_from_metadata(
                                            seg_dir,
                                            segment_id,
                                            segments,
                                            node_metas,
                                            &partitions.node_eq,
                                            source_groups,
                                        )?;
                                    outcome
                                        .report
                                        .failed_equality_indexes
                                        .extend(branch_outcome.report.failed_equality_indexes);
                                    outcome.stats_evidence.extend(branch_outcome.stats_evidence);
                                    outcome.records.extend(branch_outcome.records);
                                    let branch_outcome =
                                        write_declared_range_sidecars_from_metadata(
                                            seg_dir,
                                            segment_id,
                                            segments,
                                            node_metas,
                                            &partitions.node_range,
                                            source_groups,
                                        )?;
                                    outcome
                                        .report
                                        .failed_range_indexes
                                        .extend(branch_outcome.report.failed_range_indexes);
                                    outcome.stats_evidence.extend(branch_outcome.stats_evidence);
                                    outcome.records.extend(branch_outcome.records);

                                    let node_compound_entries: Vec<&SecondaryIndexManifestEntry> =
                                        partitions
                                            .node_eq
                                            .iter()
                                            .chain(partitions.node_range.iter())
                                            .copied()
                                            .filter(|entry| {
                                                matches!(
                                                    &entry.target,
                                                    SecondaryIndexTarget::NodeFieldIndex { .. }
                                                )
                                            })
                                            .collect();
                                    let branch_outcome =
                                        write_declared_node_compound_sidecars_from_metadata(
                                            seg_dir,
                                            segment_id,
                                            segments,
                                            node_metas,
                                            &node_compound_entries,
                                            source_groups,
                                        )?;
                                    outcome
                                        .report
                                        .failed_equality_indexes
                                        .extend(branch_outcome.report.failed_equality_indexes);
                                    outcome
                                        .report
                                        .failed_range_indexes
                                        .extend(branch_outcome.report.failed_range_indexes);
                                    outcome.stats_evidence.extend(branch_outcome.stats_evidence);
                                    outcome.records.extend(branch_outcome.records);
                                    let timestamp_index =
                                        prepare_timestamp_index_payload_from_meta(node_metas);
                                    outcome.stats_evidence.sort();
                                    Ok((
                                        FlushNodeIndexOutput {
                                            key_index,
                                            node_label_index,
                                            timestamp_index,
                                            external_records: outcome.records,
                                            declared_evidence: outcome.stats_evidence,
                                        },
                                        outcome.report,
                                    ))
                                },
                                || {
                                    let adj_out =
                                        prepare_adjacency_payloads_from_meta(edge_metas, true);
                                    let adj_in =
                                        prepare_adjacency_payloads_from_meta(edge_metas, false);
                                    let edge_label_index =
                                        prepare_edge_label_index_payload_from_meta(edge_metas);
                                    let edge_triple_index =
                                        prepare_edge_triple_index_payload_from_meta(edge_metas);
                                    let edge_metadata_indexes =
                                        prepare_edge_metadata_index_components_from_meta(
                                            edge_metas,
                                        );
                                    let mut external_records = Vec::new();
                                    if write_degree_sidecar {
                                        if let Some(sidecars) =
                                            degree_sidecars_for_segments(segments)
                                        {
                                            match write_degree_delta_from_sidecars(
                                                seg_dir,
                                                segment_id,
                                                &sidecars,
                                                source_groups,
                                            ) {
                                                Ok(record) => external_records.push(record),
                                                Err(error)
                                                    if is_optional_degree_delta_invalidity(
                                                        &error,
                                                    ) =>
                                                {
                                                    let _ = fs::remove_file(
                                                        seg_dir.join(DEGREE_DELTA_FILENAME),
                                                    );
                                                }
                                                Err(error) => return Err(error),
                                            }
                                        }
                                    }
                                    let edge_eq_outcome =
                                        write_declared_edge_equality_sidecars_from_metadata(
                                            seg_dir,
                                            segment_id,
                                            segments,
                                            edge_metas,
                                            &partitions.edge_eq,
                                            source_groups,
                                        )?;
                                    let mut report = SecondaryIndexMaintenanceReport::default();
                                    report
                                        .failed_equality_indexes
                                        .extend(edge_eq_outcome.report.failed_equality_indexes);
                                    let mut declared_evidence = edge_eq_outcome.stats_evidence;
                                    external_records.extend(edge_eq_outcome.records);
                                    let edge_range_outcome =
                                        write_declared_edge_range_sidecars_from_metadata(
                                            seg_dir,
                                            segment_id,
                                            segments,
                                            edge_metas,
                                            &partitions.edge_range,
                                            source_groups,
                                        )?;
                                    report
                                        .failed_range_indexes
                                        .extend(edge_range_outcome.report.failed_range_indexes);
                                    declared_evidence.extend(edge_range_outcome.stats_evidence);
                                    external_records.extend(edge_range_outcome.records);

                                    let edge_compound_entries: Vec<&SecondaryIndexManifestEntry> =
                                        partitions
                                            .edge_eq
                                            .iter()
                                            .chain(partitions.edge_range.iter())
                                            .copied()
                                            .filter(|entry| {
                                                matches!(
                                                    &entry.target,
                                                    SecondaryIndexTarget::EdgeFieldIndex { .. }
                                                )
                                            })
                                            .collect();
                                    let edge_compound_outcome =
                                        write_declared_edge_compound_sidecars_from_metadata(
                                            seg_dir,
                                            segment_id,
                                            segments,
                                            edge_metas,
                                            &edge_compound_entries,
                                            source_groups,
                                        )?;
                                    report.failed_equality_indexes.extend(
                                        edge_compound_outcome.report.failed_equality_indexes,
                                    );
                                    report
                                        .failed_range_indexes
                                        .extend(edge_compound_outcome.report.failed_range_indexes);
                                    declared_evidence.extend(edge_compound_outcome.stats_evidence);
                                    external_records.extend(edge_compound_outcome.records);
                                    declared_evidence.sort();
                                    Ok((
                                        FlushEdgeIndexOutput {
                                            adj_out,
                                            adj_in,
                                            edge_label_index,
                                            edge_triple_index,
                                            edge_metadata_indexes,
                                            external_records,
                                            declared_evidence,
                                        },
                                        report,
                                    ))
                                },
                            )
                        },
                        || {
                            write_sparse_posting_index_from_meta(
                                seg_dir,
                                segment_id,
                                segments,
                                node_metas,
                                source_groups,
                            )
                        },
                    )
                },
                || maybe_build_dense_hnsw(dense_points, dense_config),
            )
        },
        || build_compaction_stats_core_partial(segments, node_metas, edge_metas, secondary_indexes),
    );
    let (indexes_result, built_hnsw) = index_result?;
    let (((node_output, mut report), (edge_output, edge_report)), sparse_records) = indexes_result;
    report
        .failed_equality_indexes
        .extend(edge_report.failed_equality_indexes);
    report
        .failed_range_indexes
        .extend(edge_report.failed_range_indexes);
    let dense_records = write_compaction_prebuilt_dense_hnsw_components(
        seg_dir,
        segment_id,
        dense_config,
        built_hnsw,
        source_groups,
    )?;
    emit_flush_node_index_components(core_writer, source_groups, &node_output)?;
    emit_flush_edge_index_components(core_writer, source_groups, &edge_output)?;

    let FlushNodeIndexOutput {
        external_records: node_external_records,
        declared_evidence: stats_evidence,
        ..
    } = node_output;
    let FlushEdgeIndexOutput {
        external_records: edge_external_records,
        declared_evidence: edge_stats_evidence,
        ..
    } = edge_output;
    let mut stats_evidence = stats_evidence;
    stats_evidence.extend(edge_stats_evidence);
    stats_evidence.sort();
    let mut records = Vec::new();
    records.extend(node_external_records);
    records.extend(edge_external_records);
    records.extend(dense_records);
    records.extend(sparse_records);
    if let Ok(core_partial) = stats_core_result {
        let stats = assemble_compaction_stats_from_partials(
            segment_id,
            secondary_indexes,
            core_partial,
            stats_evidence,
        );
        if let Ok(Some(payload)) = planner_stats_sidecar_payload(stats) {
            if let Ok((record, _)) = write_compaction_component(
                seg_dir,
                segment_id,
                PLANNER_STATS_FILENAME,
                SegmentComponentKind::PlannerStats,
                ComponentRequirement::Optional {
                    fallback: ComponentFallbackClass::PlannerStatsUnavailable,
                },
                ComponentTrustClass::OptionalAdvisoryStats,
                planner_stats_component_dependencies(
                    source_groups.segment_data_id,
                    secondary_indexes,
                ),
                planner_stats_component_fingerprint(secondary_indexes),
                |writer| {
                    writer.write_all(&payload)?;
                    Ok(())
                },
            ) {
                records.push(record);
            }
        }
    }
    Ok(CompactionComponentBuildOutput { records, report })
}

fn degree_sidecars_for_segments(
    segments: &[Arc<SegmentReader>],
) -> Option<Vec<&crate::degree_cache::DegreeSidecar>> {
    segments
        .iter()
        .map(|segment| segment.degree_delta_sidecar())
        .collect()
}

fn write_degree_delta_from_sidecars(
    seg_dir: &Path,
    segment_id: u64,
    sidecars: &[&crate::degree_cache::DegreeSidecar],
    source_groups: SegmentComponentSourceGroups,
) -> Result<SegmentComponentRecordV1, EngineError> {
    let (record, _) = write_compaction_component(
        seg_dir,
        segment_id,
        DEGREE_DELTA_FILENAME,
        SegmentComponentKind::DegreeDelta,
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::AdjacencyWalk,
        },
        ComponentTrustClass::OptionalExactAccelerator,
        vec![source_group_dependency(
            SegmentSourceGroupKind::DegreeSource,
            source_groups.degree_source,
        )],
        component_fingerprint("flush.degree_delta", &[]),
        |writer| write_folded_degree_delta_sidecar_payload_from_sidecars(writer, sidecars),
    )?;
    Ok(record)
}

fn is_optional_degree_delta_invalidity(error: &EngineError) -> bool {
    matches!(
        error,
        EngineError::CorruptRecord(_) | EngineError::InvalidOperation(_)
    )
}

fn write_compaction_prebuilt_dense_hnsw_components(
    seg_dir: &Path,
    segment_id: u64,
    dense_config: Option<&DenseVectorConfig>,
    built_hnsw: Option<BuiltHnsw>,
    source_groups: SegmentComponentSourceGroups,
) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
    let Some(config) = dense_config else {
        return Ok(Vec::new());
    };
    let Some(built) = built_hnsw else {
        return Ok(Vec::new());
    };
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::DenseVectorSource,
            source_groups.dense_vector_source,
        ),
        ComponentDependencyV1::DenseVectorConfig {
            fingerprint: dense_config_fingerprint(Some(config)),
        },
    ];
    let (meta_record, graph_record, _) = write_compaction_component_pair(
        seg_dir,
        segment_id,
        DENSE_HNSW_META_FILENAME,
        SegmentComponentKind::DenseHnswMetadata,
        component_fingerprint("flush.dense_hnsw_meta", &[]),
        DENSE_HNSW_GRAPH_FILENAME,
        SegmentComponentKind::DenseHnswGraph,
        component_fingerprint("flush.dense_hnsw_graph", &[]),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::ExactVectorScan,
        },
        ComponentTrustClass::OptionalApproximateAccelerator,
        dependencies,
        |meta_writer, graph_writer| {
            write_prebuilt_hnsw_to_writers(meta_writer, graph_writer, config, &built)
        },
    )?;
    Ok(vec![meta_record, graph_record])
}

fn build_secondary_eq_groups_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    index_id: u64,
    target_label_id: u32,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let winner_sources: HashMap<u64, usize> = node_metas
        .iter()
        .filter(|meta| meta.label_ids.contains(target_label_id))
        .map(|meta| (meta.node_id, meta.src_seg_idx))
        .collect();
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for (seg_idx, seg) in segments.iter().enumerate() {
        seg.for_each_secondary_eq_group(index_id, |value_hash, ids| {
            let group = groups.entry(value_hash).or_default();
            for &node_id in ids {
                if winner_sources.get(&node_id) == Some(&seg_idx) {
                    group.push(node_id);
                }
            }
            Ok(())
        })?;
    }

    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }

    Ok(groups)
}

fn build_edge_secondary_eq_groups_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    entry: &SecondaryIndexManifestEntry,
    target_label_id: u32,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let winner_sources: HashMap<u64, usize> = edge_metas
        .iter()
        .filter(|meta| meta.label_id == target_label_id)
        .map(|meta| (meta.edge_id, meta.src_seg_idx))
        .collect();
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for (seg_idx, seg) in segments.iter().enumerate() {
        seg.for_each_declared_secondary_eq_group(entry, |value_hash, ids| {
            let group = groups.entry(value_hash).or_default();
            for &edge_id in ids {
                if winner_sources.get(&edge_id) == Some(&seg_idx) {
                    group.push(edge_id);
                }
            }
            Ok(())
        })?;
    }

    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }

    Ok(groups)
}

fn build_secondary_eq_groups_from_targeted_decode(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    target_label_id: u32,
    prop_key: &str,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for meta in node_metas
        .iter()
        .filter(|meta| meta.label_ids.contains(target_label_id))
    {
        if let Some(value) = segments[meta.src_seg_idx].node_property_value_at_offset(
            meta.node_id,
            meta.src_data_offset,
            prop_key,
        )? {
            groups
                .entry(hash_prop_equality_key(&value))
                .or_default()
                .push(meta.node_id);
        }
    }

    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }

    Ok(groups)
}

fn sidecar_unavailable_failure_reason(
    segment: &SegmentReader,
    kind: SegmentComponentKind,
) -> Option<String> {
    match segment.optional_component_availability(kind) {
        ComponentAvailability::CorruptIdentity { reason }
        | ComponentAvailability::Incompatible { reason }
        | ComponentAvailability::Unsupported { reason } => Some(reason),
        ComponentAvailability::Available | ComponentAvailability::Missing => None,
    }
}

fn build_secondary_range_entries_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    index_id: u64,
    target_label_id: u32,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let winner_sources: HashMap<u64, usize> = node_metas
        .iter()
        .filter(|meta| meta.label_ids.contains(target_label_id))
        .map(|meta| (meta.node_id, meta.src_seg_idx))
        .collect();
    let mut entries = Vec::new();

    for (seg_idx, seg) in segments.iter().enumerate() {
        seg.for_each_secondary_range_entry(index_id, |encoded_value, node_id| {
            if winner_sources.get(&node_id) == Some(&seg_idx) {
                entries.push((encoded_value, node_id));
            }
            Ok(())
        })?;
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn build_edge_secondary_range_entries_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    entry: &SecondaryIndexManifestEntry,
    target_label_id: u32,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let winner_sources: HashMap<u64, usize> = edge_metas
        .iter()
        .filter(|meta| meta.label_id == target_label_id)
        .map(|meta| (meta.edge_id, meta.src_seg_idx))
        .collect();
    let mut entries = Vec::new();

    for (seg_idx, seg) in segments.iter().enumerate() {
        seg.for_each_declared_secondary_range_entry(entry, |encoded_value, edge_id| {
            if winner_sources.get(&edge_id) == Some(&seg_idx) {
                entries.push((encoded_value, edge_id));
            }
            Ok(())
        })?;
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn build_secondary_range_entries_from_targeted_decode(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    target_label_id: u32,
    prop_key: &str,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let mut entries = Vec::new();

    for meta in node_metas
        .iter()
        .filter(|meta| meta.label_ids.contains(target_label_id))
    {
        let Some(value) = segments[meta.src_seg_idx].node_property_value_at_offset(
            meta.node_id,
            meta.src_data_offset,
            prop_key,
        )?
        else {
            continue;
        };
        let Some(encoded_value) = numeric_range_sort_key_for_value(&value) else {
            continue;
        };
        entries.push((encoded_value, meta.node_id));
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn build_edge_secondary_eq_groups_from_targeted_decode(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    label_id: u32,
    prop_key: &str,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for meta in edge_metas.iter().filter(|meta| meta.label_id == label_id) {
        if let Some(value) = segments[meta.src_seg_idx].edge_property_value_at_offset(
            meta.edge_id,
            meta.src_data_offset,
            prop_key,
        )? {
            groups
                .entry(hash_prop_equality_key(&value))
                .or_default()
                .push(meta.edge_id);
        }
    }

    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }

    Ok(groups)
}

fn build_edge_secondary_range_entries_from_targeted_decode(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    label_id: u32,
    prop_key: &str,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let mut entries = Vec::new();

    for meta in edge_metas.iter().filter(|meta| meta.label_id == label_id) {
        let Some(value) = segments[meta.src_seg_idx].edge_property_value_at_offset(
            meta.edge_id,
            meta.src_data_offset,
            prop_key,
        )?
        else {
            continue;
        };
        let Some(encoded_value) = numeric_range_sort_key_for_value(&value) else {
            continue;
        };
        entries.push((encoded_value, meta.edge_id));
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn compound_property_keys(fields: &[SecondaryIndexFieldManifest]) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for field in fields {
        if let SecondaryIndexFieldManifest::Property { key } = field {
            keys.insert(key.clone());
        }
    }
    keys.into_iter().collect()
}

fn node_compound_selected_field_needs(
    fields: &[SecondaryIndexFieldManifest],
) -> NodeSelectedFieldNeeds {
    let mut needs = NodeSelectedFieldNeeds::default();
    for field in fields {
        if let SecondaryIndexFieldManifest::NodeMetadata { field } = field {
            match field {
                NodeMetadataIndexFieldManifest::Key => needs.key = true,
                NodeMetadataIndexFieldManifest::CreatedAt => needs.created_at = true,
                NodeMetadataIndexFieldManifest::Id
                | NodeMetadataIndexFieldManifest::Weight
                | NodeMetadataIndexFieldManifest::UpdatedAt => {}
            }
        }
    }
    let prop_keys = compound_property_keys(fields);
    if !prop_keys.is_empty() {
        needs.props = PropertySelection::Keys(prop_keys);
    }
    needs
}

fn edge_compound_selected_field_needs(
    fields: &[SecondaryIndexFieldManifest],
) -> EdgeSelectedFieldNeeds {
    let mut needs = EdgeSelectedFieldNeeds::default();
    for field in fields {
        if let SecondaryIndexFieldManifest::EdgeMetadata {
            field: EdgeMetadataIndexFieldManifest::CreatedAt,
        } = field
        {
            needs.created_at = true;
        }
    }
    let prop_keys = compound_property_keys(fields);
    if !prop_keys.is_empty() {
        needs.props = PropertySelection::Keys(prop_keys);
    }
    needs
}

fn node_compound_needs_record(needs: &NodeSelectedFieldNeeds) -> bool {
    needs.key || needs.created_at || !matches!(needs.props, PropertySelection::None)
}

fn edge_compound_needs_record(needs: &EdgeSelectedFieldNeeds) -> bool {
    needs.created_at || !matches!(needs.props, PropertySelection::None)
}

fn node_field_index_parts(
    entry: &SecondaryIndexManifestEntry,
) -> Result<(u32, &[SecondaryIndexFieldManifest]), EngineError> {
    match &entry.target {
        SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => Ok((*label_id, fields)),
        _ => Err(EngineError::InvalidOperation(
            compound_secondary_failure_message_from_str(&format!(
                "index {} is not a node compound declaration",
                entry.index_id
            )),
        )),
    }
}

fn edge_field_index_parts(
    entry: &SecondaryIndexManifestEntry,
) -> Result<(u32, &[SecondaryIndexFieldManifest]), EngineError> {
    match &entry.target {
        SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => Ok((*label_id, fields)),
        _ => Err(EngineError::InvalidOperation(
            compound_secondary_failure_message_from_str(&format!(
                "index {} is not an edge compound declaration",
                entry.index_id
            )),
        )),
    }
}

/// Union of selected-field needs across several declarations on one label,
/// so each survivor record is decoded once for the whole group.
fn merged_node_compound_needs(
    group_fields: &[&[SecondaryIndexFieldManifest]],
) -> NodeSelectedFieldNeeds {
    let mut needs = NodeSelectedFieldNeeds::default();
    let mut prop_keys = BTreeSet::new();
    for fields in group_fields {
        for field in *fields {
            match field {
                SecondaryIndexFieldManifest::Property { key } => {
                    prop_keys.insert(key.clone());
                }
                SecondaryIndexFieldManifest::NodeMetadata { field } => match field {
                    NodeMetadataIndexFieldManifest::Key => needs.key = true,
                    NodeMetadataIndexFieldManifest::CreatedAt => needs.created_at = true,
                    NodeMetadataIndexFieldManifest::Id
                    | NodeMetadataIndexFieldManifest::Weight
                    | NodeMetadataIndexFieldManifest::UpdatedAt => {}
                },
                SecondaryIndexFieldManifest::EdgeMetadata { .. } => {}
            }
        }
    }
    if !prop_keys.is_empty() {
        needs.props = PropertySelection::Keys(prop_keys.into_iter().collect());
    }
    needs
}

fn merged_edge_compound_needs(
    group_fields: &[&[SecondaryIndexFieldManifest]],
) -> EdgeSelectedFieldNeeds {
    let mut needs = EdgeSelectedFieldNeeds::default();
    let mut prop_keys = BTreeSet::new();
    for fields in group_fields {
        for field in *fields {
            match field {
                SecondaryIndexFieldManifest::Property { key } => {
                    prop_keys.insert(key.clone());
                }
                SecondaryIndexFieldManifest::EdgeMetadata {
                    field: EdgeMetadataIndexFieldManifest::CreatedAt,
                } => needs.created_at = true,
                _ => {}
            }
        }
    }
    if !prop_keys.is_empty() {
        needs.props = PropertySelection::Keys(prop_keys.into_iter().collect());
    }
    needs
}

/// Record-decoded fields a compound tuple may reference. Metadata-only
/// components come straight from the compact meta tables instead.
struct CompoundNodeRecordFields {
    key: Option<String>,
    props: BTreeMap<String, PropValue>,
    created_at: Option<i64>,
}

struct CompoundEdgeRecordFields {
    props: BTreeMap<String, PropValue>,
    created_at: Option<i64>,
}

/// Re-materialize a shared decode error for each declaration it fails.
fn replicate_decode_error(error: &EngineError) -> EngineError {
    EngineError::CorruptRecord(match error {
        EngineError::CorruptRecord(message) => message.clone(),
        other => other.to_string(),
    })
}

fn build_node_compound_entries_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    entry: &SecondaryIndexManifestEntry,
) -> CompoundSidecarBuildResult {
    let (target_label_id, _) = node_field_index_parts(entry)?;
    let winner_sources: HashMap<u64, usize> = node_metas
        .iter()
        .filter(|meta| meta.label_ids.contains(target_label_id))
        .map(|meta| (meta.node_id, meta.src_seg_idx))
        .collect();
    let mut entries = Vec::new();
    for (seg_idx, segment) in segments.iter().enumerate() {
        let sidecar_present = segment.for_each_compound_sidecar_entry(entry, |key, node_id| {
            if winner_sources.get(&node_id) == Some(&seg_idx) {
                entries.push((key.to_vec(), node_id));
            }
            Ok(())
        })?;
        if !sidecar_present {
            return Err(EngineError::CorruptRecord(
                compound_secondary_failure_message_from_str(&format!(
                    "source segment at index {seg_idx} has no compound sidecar for index {} during reuse",
                    entry.index_id
                )),
            ));
        }
    }
    entries.sort();
    entries.dedup();
    Ok(entries)
}

fn build_edge_compound_entries_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    entry: &SecondaryIndexManifestEntry,
) -> CompoundSidecarBuildResult {
    let (target_label_id, _) = edge_field_index_parts(entry)?;
    let winner_sources: HashMap<u64, usize> = edge_metas
        .iter()
        .filter(|meta| meta.label_id == target_label_id)
        .map(|meta| (meta.edge_id, meta.src_seg_idx))
        .collect();
    let mut entries = Vec::new();
    for (seg_idx, segment) in segments.iter().enumerate() {
        let sidecar_present = segment.for_each_compound_sidecar_entry(entry, |key, edge_id| {
            if winner_sources.get(&edge_id) == Some(&seg_idx) {
                entries.push((key.to_vec(), edge_id));
            }
            Ok(())
        })?;
        if !sidecar_present {
            return Err(EngineError::CorruptRecord(
                compound_secondary_failure_message_from_str(&format!(
                    "source segment at index {seg_idx} has no compound sidecar for index {} during reuse",
                    entry.index_id
                )),
            ));
        }
    }
    entries.sort();
    entries.dedup();
    Ok(entries)
}

fn selected_node_field_value<'a>(
    meta: &'a CompactNodeMeta,
    selected: Option<&'a CompoundNodeRecordFields>,
    field: &'a SecondaryIndexFieldManifest,
) -> Result<CompoundFieldValue<'a>, EngineError> {
    match field {
        SecondaryIndexFieldManifest::Property { key } => {
            let selected = selected.ok_or_else(|| {
                EngineError::CorruptRecord(format!(
                    "compound secondary index unavailable: node {} selected properties were not decoded",
                    meta.node_id
                ))
            })?;
            Ok(CompoundFieldValue::Property(selected.props.get(key)))
        }
        SecondaryIndexFieldManifest::NodeMetadata { field } => match field {
            NodeMetadataIndexFieldManifest::Id => Ok(CompoundFieldValue::MetadataU64(meta.node_id)),
            NodeMetadataIndexFieldManifest::Key => {
                let key = selected
                    .and_then(|selected| selected.key.as_deref())
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "compound secondary index unavailable: node {} key was not decoded",
                            meta.node_id
                        ))
                    })?;
                Ok(CompoundFieldValue::MetadataString(key))
            }
            NodeMetadataIndexFieldManifest::Weight => {
                Ok(CompoundFieldValue::MetadataF64(meta.weight as f64))
            }
            NodeMetadataIndexFieldManifest::CreatedAt => {
                let created_at = selected
                    .and_then(|selected| selected.created_at)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "compound secondary index unavailable: node {} created_at was not decoded",
                            meta.node_id
                        ))
                    })?;
                Ok(CompoundFieldValue::MetadataI64(created_at))
            }
            NodeMetadataIndexFieldManifest::UpdatedAt => {
                Ok(CompoundFieldValue::MetadataI64(meta.updated_at))
            }
        },
        SecondaryIndexFieldManifest::EdgeMetadata { .. } => Err(EngineError::InvalidOperation(
            "compound secondary index unavailable: node declaration contains edge metadata"
                .to_string(),
        )),
    }
}

fn selected_edge_field_value<'a>(
    meta: &'a CompactEdgeMeta,
    selected: Option<&'a CompoundEdgeRecordFields>,
    field: &'a SecondaryIndexFieldManifest,
) -> Result<CompoundFieldValue<'a>, EngineError> {
    match field {
        SecondaryIndexFieldManifest::Property { key } => {
            let selected = selected.ok_or_else(|| {
                EngineError::CorruptRecord(format!(
                    "compound secondary index unavailable: edge {} selected properties were not decoded",
                    meta.edge_id
                ))
            })?;
            Ok(CompoundFieldValue::Property(selected.props.get(key)))
        }
        SecondaryIndexFieldManifest::EdgeMetadata { field } => match field {
            EdgeMetadataIndexFieldManifest::Id => Ok(CompoundFieldValue::MetadataU64(meta.edge_id)),
            EdgeMetadataIndexFieldManifest::From => Ok(CompoundFieldValue::MetadataU64(meta.from)),
            EdgeMetadataIndexFieldManifest::To => Ok(CompoundFieldValue::MetadataU64(meta.to)),
            EdgeMetadataIndexFieldManifest::Weight => {
                Ok(CompoundFieldValue::MetadataF64(meta.weight as f64))
            }
            EdgeMetadataIndexFieldManifest::CreatedAt => {
                let created_at = selected
                    .and_then(|selected| selected.created_at)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "compound secondary index unavailable: edge {} created_at was not decoded",
                            meta.edge_id
                        ))
                    })?;
                Ok(CompoundFieldValue::MetadataI64(created_at))
            }
            EdgeMetadataIndexFieldManifest::UpdatedAt => {
                Ok(CompoundFieldValue::MetadataI64(meta.updated_at))
            }
            EdgeMetadataIndexFieldManifest::ValidFrom => {
                Ok(CompoundFieldValue::MetadataI64(meta.valid_from))
            }
            EdgeMetadataIndexFieldManifest::ValidTo => {
                Ok(CompoundFieldValue::MetadataI64(meta.valid_to))
            }
        },
        SecondaryIndexFieldManifest::NodeMetadata { .. } => Err(EngineError::InvalidOperation(
            "compound secondary index unavailable: edge declaration contains node metadata"
                .to_string(),
        )),
    }
}

pub(crate) fn build_node_compound_entries_from_metadata(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    entry: &SecondaryIndexManifestEntry,
) -> CompoundSidecarBuildResult {
    build_node_compound_entries_grouped_from_metadata(segments, node_metas, &[entry])
        .pop()
        .expect("grouped compound build returns one result per entry")
}

/// Build compound sidecar entries for several declarations in one pass.
///
/// Declarations are grouped by target label; each survivor record is decoded
/// at most once per label with the union of the group's field needs, directly
/// at the offset pinned by the compact meta table (no ID-based re-location).
/// Results align with `entries` by position. A record decode failure fails
/// every declaration in that label group that reads record fields;
/// metadata-only declarations are unaffected.
pub(crate) fn build_node_compound_entries_grouped_from_metadata(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    entries: &[&SecondaryIndexManifestEntry],
) -> Vec<CompoundSidecarBuildResult> {
    let mut results: Vec<OptionalCompoundSidecarBuildResult> =
        (0..entries.len()).map(|_| None).collect();
    let mut label_groups: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (pos, entry) in entries.iter().enumerate() {
        match node_field_index_parts(entry) {
            Ok((label_id, _)) => label_groups.entry(label_id).or_default().push(pos),
            Err(error) => results[pos] = Some(Err(error)),
        }
    }

    for (target_label_id, positions) in label_groups {
        let group_fields: Vec<&[SecondaryIndexFieldManifest]> = positions
            .iter()
            .map(|&pos| {
                node_field_index_parts(entries[pos])
                    .expect("grouped entries are node field indexes")
                    .1
            })
            .collect();
        let merged_needs = merged_node_compound_needs(&group_fields);

        let mut decoded: Vec<Option<CompoundNodeRecordFields>> = Vec::new();
        let mut decode_error: Option<EngineError> = None;
        if node_compound_needs_record(&merged_needs) {
            decoded = (0..node_metas.len()).map(|_| None).collect();
            for (index, meta) in node_metas.iter().enumerate() {
                if !meta.label_ids.contains(target_label_id) {
                    continue;
                }
                match segments[meta.src_seg_idx].node_selected_fields_at_offset(
                    meta.node_id,
                    meta.src_data_offset,
                    &merged_needs,
                ) {
                    Ok((key, props, created_at)) => {
                        decoded[index] = Some(CompoundNodeRecordFields {
                            key,
                            props,
                            created_at,
                        });
                    }
                    Err(error) => {
                        decode_error = Some(error);
                        break;
                    }
                }
            }
        }

        for (&pos, fields) in positions.iter().zip(&group_fields) {
            if let Some(error) = &decode_error {
                if node_compound_needs_record(&node_compound_selected_field_needs(fields)) {
                    results[pos] = Some(Err(replicate_decode_error(error)));
                    continue;
                }
            }
            results[pos] = Some(node_compound_entries_for_decoded(
                node_metas,
                &decoded,
                target_label_id,
                fields,
            ));
        }
    }

    results
        .into_iter()
        .map(|result| result.expect("every entry position is resolved"))
        .collect()
}

fn node_compound_entries_for_decoded(
    node_metas: &[CompactNodeMeta],
    decoded: &[Option<CompoundNodeRecordFields>],
    target_label_id: u32,
    fields: &[SecondaryIndexFieldManifest],
) -> CompoundSidecarBuildResult {
    let context = CompoundTupleContext {
        target_kind: CompoundSidecarTargetKind::Node,
        target_label_id,
        fields,
    };
    let mut entries = Vec::new();
    for (index, meta) in node_metas.iter().enumerate() {
        if !meta.label_ids.contains(target_label_id) {
            continue;
        }
        let selected = decoded.get(index).and_then(|fields| fields.as_ref());
        let values = fields
            .iter()
            .map(|field| selected_node_field_value(meta, selected, field))
            .collect::<Result<Vec<_>, _>>()?;
        let tuple_key = encode_compound_tuple_key(&context, &values)?;
        entries.push((tuple_key, meta.node_id));
    }
    entries.sort();
    entries.dedup();
    Ok(entries)
}

pub(crate) fn build_edge_compound_entries_from_metadata(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    entry: &SecondaryIndexManifestEntry,
) -> CompoundSidecarBuildResult {
    build_edge_compound_entries_grouped_from_metadata(segments, edge_metas, &[entry])
        .pop()
        .expect("grouped compound build returns one result per entry")
}

/// Edge counterpart of [`build_node_compound_entries_grouped_from_metadata`].
pub(crate) fn build_edge_compound_entries_grouped_from_metadata(
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    entries: &[&SecondaryIndexManifestEntry],
) -> Vec<CompoundSidecarBuildResult> {
    let mut results: Vec<OptionalCompoundSidecarBuildResult> =
        (0..entries.len()).map(|_| None).collect();
    let mut label_groups: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (pos, entry) in entries.iter().enumerate() {
        match edge_field_index_parts(entry) {
            Ok((label_id, _)) => label_groups.entry(label_id).or_default().push(pos),
            Err(error) => results[pos] = Some(Err(error)),
        }
    }

    for (target_label_id, positions) in label_groups {
        let group_fields: Vec<&[SecondaryIndexFieldManifest]> = positions
            .iter()
            .map(|&pos| {
                edge_field_index_parts(entries[pos])
                    .expect("grouped entries are edge field indexes")
                    .1
            })
            .collect();
        let merged_needs = merged_edge_compound_needs(&group_fields);

        let mut decoded: Vec<Option<CompoundEdgeRecordFields>> = Vec::new();
        let mut decode_error: Option<EngineError> = None;
        if edge_compound_needs_record(&merged_needs) {
            decoded = (0..edge_metas.len()).map(|_| None).collect();
            for (index, meta) in edge_metas.iter().enumerate() {
                if meta.label_id != target_label_id {
                    continue;
                }
                match segments[meta.src_seg_idx].edge_selected_fields_at_offset(
                    meta.edge_id,
                    meta.src_data_offset,
                    &merged_needs,
                ) {
                    Ok((props, created_at)) => {
                        decoded[index] = Some(CompoundEdgeRecordFields { props, created_at });
                    }
                    Err(error) => {
                        decode_error = Some(error);
                        break;
                    }
                }
            }
        }

        for (&pos, fields) in positions.iter().zip(&group_fields) {
            if let Some(error) = &decode_error {
                if edge_compound_needs_record(&edge_compound_selected_field_needs(fields)) {
                    results[pos] = Some(Err(replicate_decode_error(error)));
                    continue;
                }
            }
            results[pos] = Some(edge_compound_entries_for_decoded(
                edge_metas,
                &decoded,
                target_label_id,
                fields,
            ));
        }
    }

    results
        .into_iter()
        .map(|result| result.expect("every entry position is resolved"))
        .collect()
}

fn edge_compound_entries_for_decoded(
    edge_metas: &[CompactEdgeMeta],
    decoded: &[Option<CompoundEdgeRecordFields>],
    target_label_id: u32,
    fields: &[SecondaryIndexFieldManifest],
) -> CompoundSidecarBuildResult {
    let context = CompoundTupleContext {
        target_kind: CompoundSidecarTargetKind::Edge,
        target_label_id,
        fields,
    };
    let mut entries = Vec::new();
    for (index, meta) in edge_metas.iter().enumerate() {
        if meta.label_id != target_label_id {
            continue;
        }
        let selected = decoded.get(index).and_then(|fields| fields.as_ref());
        let values = fields
            .iter()
            .map(|field| selected_edge_field_value(meta, selected, field))
            .collect::<Result<Vec<_>, _>>()?;
        let tuple_key = encode_compound_tuple_key(&context, &values)?;
        entries.push((tuple_key, meta.edge_id));
    }
    entries.sort();
    entries.dedup();
    Ok(entries)
}

fn compound_source_sidecar_reuse_status(
    segments: &[Arc<SegmentReader>],
    entry: &SecondaryIndexManifestEntry,
) -> (bool, Option<String>) {
    let kind = secondary_index_component_kind_for_entry(entry);
    for segment in segments {
        match segment.compound_sidecar_lightweight_available_for_entry(entry) {
            Ok(true) => {}
            Ok(false) => {
                let failure_message = if entry.state == SecondaryIndexState::Ready {
                    kind.clone()
                        .and_then(|kind| sidecar_unavailable_failure_reason(segment, kind))
                        .map(|reason| compound_secondary_failure_message_from_str(&reason))
                } else {
                    None
                };
                return (false, failure_message);
            }
            Err(error) => {
                let failure_message = if entry.state == SecondaryIndexState::Ready {
                    Some(compound_secondary_failure_message(&error))
                } else {
                    None
                };
                return (false, failure_message);
            }
        }
    }
    (true, None)
}

fn write_declared_node_compound_sidecars_from_metadata(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<DeclaredSidecarWriteOutcome, EngineError> {
    let mut outcome = DeclaredSidecarWriteOutcome::default();

    // Phase 1: attempt source-sidecar reuse per declaration; collect the
    // declarations that need a metadata rebuild so survivors are decoded
    // once per label instead of once per declaration.
    struct PendingCompoundSidecar<'a> {
        entry: &'a SecondaryIndexManifestEntry,
        failure_message: Option<String>,
        result: OptionalCompoundSidecarBuildResult,
    }
    let mut pending = Vec::new();
    let mut rebuild_positions = Vec::new();
    for entry in entries {
        if !matches!(&entry.target, SecondaryIndexTarget::NodeFieldIndex { .. }) {
            continue;
        }
        let mut failure_message = None;
        let source_entries = if entry.state == SecondaryIndexState::Failed {
            None
        } else {
            let (use_source_sidecars, source_failure_message) =
                compound_source_sidecar_reuse_status(segments, entry);
            failure_message = source_failure_message;
            if use_source_sidecars {
                match build_node_compound_entries_from_source_sidecars(segments, node_metas, entry)
                {
                    Ok(entries) => Some(entries),
                    Err(error) => {
                        if entry.state == SecondaryIndexState::Ready && failure_message.is_none() {
                            failure_message = Some(compound_secondary_failure_message(&error));
                        }
                        None
                    }
                }
            } else {
                None
            }
        };
        if source_entries.is_none() {
            rebuild_positions.push(pending.len());
        }
        pending.push(PendingCompoundSidecar {
            entry,
            failure_message,
            result: source_entries.map(Ok),
        });
    }

    // Phase 2: grouped metadata rebuild for everything reuse could not serve.
    let rebuild_entries: Vec<&SecondaryIndexManifestEntry> = rebuild_positions
        .iter()
        .map(|&pos| pending[pos].entry)
        .collect();
    let rebuild_results =
        build_node_compound_entries_grouped_from_metadata(segments, node_metas, &rebuild_entries);
    for (&pos, result) in rebuild_positions.iter().zip(rebuild_results) {
        pending[pos].result = Some(result);
    }

    // Phase 3: report failures and write sidecars in declaration order.
    for item in pending {
        let entry = item.entry;
        let sidecar_entries = match item.result.expect("every pending declaration is resolved") {
            Ok(entries) => entries,
            Err(error) => {
                let message = compound_secondary_failure_message(&error);
                match entry.kind {
                    SecondaryIndexKind::Equality => outcome
                        .report
                        .failed_equality_indexes
                        .push((entry.index_id, message)),
                    SecondaryIndexKind::Range => outcome
                        .report
                        .failed_range_indexes
                        .push((entry.index_id, message)),
                }
                continue;
            }
        };
        if let Some(message) = item.failure_message {
            match entry.kind {
                SecondaryIndexKind::Equality => outcome
                    .report
                    .failed_equality_indexes
                    .push((entry.index_id, message)),
                SecondaryIndexKind::Range => outcome
                    .report
                    .failed_range_indexes
                    .push((entry.index_id, message)),
            }
        }
        let record = match write_compaction_compound_sidecar_component(
            seg_dir,
            segment_id,
            entry,
            &sidecar_entries,
            source_groups,
        ) {
            Ok(record) => record,
            Err(error) => {
                let message = compound_secondary_failure_message(&error);
                match entry.kind {
                    SecondaryIndexKind::Equality => outcome
                        .report
                        .failed_equality_indexes
                        .push((entry.index_id, message)),
                    SecondaryIndexKind::Range => outcome
                        .report
                        .failed_range_indexes
                        .push((entry.index_id, message)),
                }
                continue;
            }
        };
        outcome.records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            outcome.stats_evidence.compound_index_stats.push(
                compound_index_stats_from_written_entries(
                    entry,
                    &sidecar_entries,
                    DeclaredIndexRuntimeCoverageState::Available,
                )?,
            );
        }
    }
    outcome.stats_evidence.sort();
    Ok(outcome)
}

fn write_declared_edge_compound_sidecars_from_metadata(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<DeclaredSidecarWriteOutcome, EngineError> {
    let mut outcome = DeclaredSidecarWriteOutcome::default();

    // Mirrors the node path: reuse first, then one grouped metadata rebuild.
    struct PendingCompoundSidecar<'a> {
        entry: &'a SecondaryIndexManifestEntry,
        failure_message: Option<String>,
        result: OptionalCompoundSidecarBuildResult,
    }
    let mut pending = Vec::new();
    let mut rebuild_positions = Vec::new();
    for entry in entries {
        if !matches!(&entry.target, SecondaryIndexTarget::EdgeFieldIndex { .. }) {
            continue;
        }
        let mut failure_message = None;
        let source_entries = if entry.state == SecondaryIndexState::Failed {
            None
        } else {
            let (use_source_sidecars, source_failure_message) =
                compound_source_sidecar_reuse_status(segments, entry);
            failure_message = source_failure_message;
            if use_source_sidecars {
                match build_edge_compound_entries_from_source_sidecars(segments, edge_metas, entry)
                {
                    Ok(entries) => Some(entries),
                    Err(error) => {
                        if entry.state == SecondaryIndexState::Ready && failure_message.is_none() {
                            failure_message = Some(compound_secondary_failure_message(&error));
                        }
                        None
                    }
                }
            } else {
                None
            }
        };
        if source_entries.is_none() {
            rebuild_positions.push(pending.len());
        }
        pending.push(PendingCompoundSidecar {
            entry,
            failure_message,
            result: source_entries.map(Ok),
        });
    }

    let rebuild_entries: Vec<&SecondaryIndexManifestEntry> = rebuild_positions
        .iter()
        .map(|&pos| pending[pos].entry)
        .collect();
    let rebuild_results =
        build_edge_compound_entries_grouped_from_metadata(segments, edge_metas, &rebuild_entries);
    for (&pos, result) in rebuild_positions.iter().zip(rebuild_results) {
        pending[pos].result = Some(result);
    }

    for item in pending {
        let entry = item.entry;
        let sidecar_entries = match item.result.expect("every pending declaration is resolved") {
            Ok(entries) => entries,
            Err(error) => {
                let message = compound_secondary_failure_message(&error);
                match entry.kind {
                    SecondaryIndexKind::Equality => outcome
                        .report
                        .failed_equality_indexes
                        .push((entry.index_id, message)),
                    SecondaryIndexKind::Range => outcome
                        .report
                        .failed_range_indexes
                        .push((entry.index_id, message)),
                }
                continue;
            }
        };
        if let Some(message) = item.failure_message {
            match entry.kind {
                SecondaryIndexKind::Equality => outcome
                    .report
                    .failed_equality_indexes
                    .push((entry.index_id, message)),
                SecondaryIndexKind::Range => outcome
                    .report
                    .failed_range_indexes
                    .push((entry.index_id, message)),
            }
        }
        let record = match write_compaction_compound_sidecar_component(
            seg_dir,
            segment_id,
            entry,
            &sidecar_entries,
            source_groups,
        ) {
            Ok(record) => record,
            Err(error) => {
                let message = compound_secondary_failure_message(&error);
                match entry.kind {
                    SecondaryIndexKind::Equality => outcome
                        .report
                        .failed_equality_indexes
                        .push((entry.index_id, message)),
                    SecondaryIndexKind::Range => outcome
                        .report
                        .failed_range_indexes
                        .push((entry.index_id, message)),
                }
                continue;
            }
        };
        outcome.records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            outcome.stats_evidence.compound_index_stats.push(
                compound_index_stats_from_written_entries(
                    entry,
                    &sidecar_entries,
                    DeclaredIndexRuntimeCoverageState::Available,
                )?,
            );
        }
    }
    outcome.stats_evidence.sort();
    Ok(outcome)
}

fn write_declared_equality_sidecars_from_metadata(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    eq_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<DeclaredSidecarWriteOutcome, EngineError> {
    if eq_entries.is_empty() {
        return Ok(DeclaredSidecarWriteOutcome::default());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;
    let mut outcome = DeclaredSidecarWriteOutcome::default();
    outcome.records.reserve(eq_entries.len());

    for entry in eq_entries {
        let SecondaryIndexTarget::NodeProperty { label_id, prop_key } = &entry.target else {
            continue;
        };
        let mut failure_message = None;
        let use_source_sidecars = if entry.state == SecondaryIndexState::Failed {
            false
        } else {
            let mut all_present = true;
            for seg in segments {
                match seg.secondary_eq_sidecar_lightweight_available_for_target(
                    entry.index_id,
                    PlannerStatsDeclaredIndexTarget::NodeProperty,
                ) {
                    Ok(true) => {}
                    Ok(false) => {
                        if entry.state == SecondaryIndexState::Ready {
                            let kind = SegmentComponentKind::NodePropertyEqualityIndex {
                                index_id: entry.index_id,
                            };
                            if let Some(reason) = sidecar_unavailable_failure_reason(seg, kind) {
                                failure_message = Some(reason);
                            }
                        }
                        all_present = false;
                        break;
                    }
                    Err(error) => {
                        all_present = false;
                        if entry.state == SecondaryIndexState::Ready {
                            failure_message = Some(error.to_string());
                        }
                        break;
                    }
                }
            }
            all_present
        };

        let groups = if use_source_sidecars {
            build_secondary_eq_groups_from_source_sidecars(
                segments,
                node_metas,
                entry.index_id,
                *label_id,
            )?
        } else {
            build_secondary_eq_groups_from_targeted_decode(
                segments, node_metas, *label_id, prop_key,
            )?
        };

        if let Some(message) = failure_message {
            outcome
                .report
                .failed_equality_indexes
                .push((entry.index_id, message));
        }

        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::NodePropertyContentSource,
                source_groups.node_property_content_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_compaction_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/node_prop_eq_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            node_property_equality_component_fingerprint(entry.index_id),
            |writer| write_node_prop_eq_sidecar_payload(writer, &groups),
        )?;
        outcome.records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            outcome
                .stats_evidence
                .equality_index_stats
                .push(equality_index_stats_from_written_groups(entry, &groups));
        }
    }

    outcome.stats_evidence.sort();
    Ok(outcome)
}

fn write_declared_range_sidecars_from_metadata(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    range_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<DeclaredSidecarWriteOutcome, EngineError> {
    if range_entries.is_empty() {
        return Ok(DeclaredSidecarWriteOutcome::default());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;
    let mut outcome = DeclaredSidecarWriteOutcome::default();
    outcome.records.reserve(range_entries.len());

    for entry in range_entries {
        let SecondaryIndexTarget::NodeProperty { label_id, prop_key } = &entry.target else {
            continue;
        };
        if !matches!(&entry.kind, SecondaryIndexKind::Range) {
            continue;
        }
        let mut failure_message = None;
        let use_source_sidecars = if entry.state == SecondaryIndexState::Failed {
            false
        } else {
            let mut all_present = true;
            for seg in segments {
                match seg.validate_secondary_range_sidecar_uncached(entry.index_id) {
                    Ok(true) => {}
                    Ok(false) => {
                        if entry.state == SecondaryIndexState::Ready {
                            let kind = SegmentComponentKind::NodePropertyRangeIndex {
                                index_id: entry.index_id,
                            };
                            if let Some(reason) = sidecar_unavailable_failure_reason(seg, kind) {
                                failure_message = Some(reason);
                            }
                        }
                        all_present = false;
                        break;
                    }
                    Err(error) => {
                        all_present = false;
                        if entry.state == SecondaryIndexState::Ready {
                            failure_message = Some(error.to_string());
                        }
                        break;
                    }
                }
            }
            all_present
        };

        let sidecar_entries = if use_source_sidecars {
            build_secondary_range_entries_from_source_sidecars(
                segments,
                node_metas,
                entry.index_id,
                *label_id,
            )?
        } else {
            build_secondary_range_entries_from_targeted_decode(
                segments, node_metas, *label_id, prop_key,
            )?
        };

        if let Some(message) = failure_message {
            outcome
                .report
                .failed_range_indexes
                .push((entry.index_id, message));
        }

        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::NodePropertyContentSource,
                source_groups.node_property_content_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_compaction_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/node_prop_range_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::NodePropertyRangeIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            node_property_range_component_fingerprint(entry.index_id),
            |writer| write_node_prop_range_sidecar_payload(writer, &sidecar_entries),
        )?;
        outcome.records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            outcome
                .stats_evidence
                .range_index_stats
                .push(range_index_stats_from_written_entries(
                    entry,
                    &sidecar_entries,
                ));
        }
    }

    outcome.stats_evidence.sort();
    Ok(outcome)
}

fn write_declared_edge_equality_sidecars_from_metadata(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    eq_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<DeclaredSidecarWriteOutcome, EngineError> {
    if eq_entries.is_empty() {
        return Ok(DeclaredSidecarWriteOutcome::default());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;
    let mut outcome = DeclaredSidecarWriteOutcome::default();
    outcome.records.reserve(eq_entries.len());

    for entry in eq_entries {
        let SecondaryIndexTarget::EdgeProperty { label_id, prop_key } = &entry.target else {
            continue;
        };
        let mut failure_message = None;
        let use_source_sidecars = if entry.state == SecondaryIndexState::Failed {
            false
        } else {
            let mut all_present = true;
            for seg in segments {
                match seg.secondary_eq_sidecar_lightweight_available_for_target(
                    entry.index_id,
                    PlannerStatsDeclaredIndexTarget::EdgeProperty,
                ) {
                    Ok(true) => {}
                    Ok(false) => {
                        if entry.state == SecondaryIndexState::Ready {
                            let kind = SegmentComponentKind::EdgePropertyEqualityIndex {
                                index_id: entry.index_id,
                            };
                            if let Some(reason) = sidecar_unavailable_failure_reason(seg, kind) {
                                failure_message = Some(reason);
                            }
                        }
                        all_present = false;
                        break;
                    }
                    Err(error) => {
                        all_present = false;
                        if entry.state == SecondaryIndexState::Ready {
                            failure_message = Some(error.to_string());
                        }
                        break;
                    }
                }
            }
            all_present
        };

        let groups = if use_source_sidecars {
            build_edge_secondary_eq_groups_from_source_sidecars(
                segments, edge_metas, entry, *label_id,
            )?
        } else {
            build_edge_secondary_eq_groups_from_targeted_decode(
                segments, edge_metas, *label_id, prop_key,
            )?
        };

        if let Some(message) = failure_message {
            outcome
                .report
                .failed_equality_indexes
                .push((entry.index_id, message));
        }
        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::EdgeSource,
                source_groups.edge_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_compaction_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/edge_prop_eq_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            edge_property_equality_component_fingerprint(entry.index_id),
            |writer| write_node_prop_eq_sidecar_payload(writer, &groups),
        )?;
        outcome.records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            outcome
                .stats_evidence
                .equality_index_stats
                .push(equality_index_stats_from_written_groups(entry, &groups));
        }
    }

    outcome.stats_evidence.sort();
    Ok(outcome)
}

fn write_declared_edge_range_sidecars_from_metadata(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    edge_metas: &[CompactEdgeMeta],
    range_entries: &[&SecondaryIndexManifestEntry],
    source_groups: SegmentComponentSourceGroups,
) -> Result<DeclaredSidecarWriteOutcome, EngineError> {
    if range_entries.is_empty() {
        return Ok(DeclaredSidecarWriteOutcome::default());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;
    let mut outcome = DeclaredSidecarWriteOutcome::default();
    outcome.records.reserve(range_entries.len());

    for entry in range_entries {
        let SecondaryIndexTarget::EdgeProperty { label_id, prop_key } = &entry.target else {
            continue;
        };
        if !matches!(&entry.kind, SecondaryIndexKind::Range) {
            continue;
        }
        let mut failure_message = None;
        let use_source_sidecars = if entry.state == SecondaryIndexState::Failed {
            false
        } else {
            let mut all_present = true;
            for seg in segments {
                match seg.validate_secondary_range_sidecar_for_target(
                    entry.index_id,
                    PlannerStatsDeclaredIndexTarget::EdgeProperty,
                ) {
                    Ok(true) => {}
                    Ok(false) => {
                        if entry.state == SecondaryIndexState::Ready {
                            let kind = SegmentComponentKind::EdgePropertyRangeIndex {
                                index_id: entry.index_id,
                            };
                            if let Some(reason) = sidecar_unavailable_failure_reason(seg, kind) {
                                failure_message = Some(reason);
                            }
                        }
                        all_present = false;
                        break;
                    }
                    Err(error) => {
                        all_present = false;
                        if entry.state == SecondaryIndexState::Ready {
                            failure_message = Some(error.to_string());
                        }
                        break;
                    }
                }
            }
            all_present
        };

        let sidecar_entries = if use_source_sidecars {
            build_edge_secondary_range_entries_from_source_sidecars(
                segments, edge_metas, entry, *label_id,
            )?
        } else {
            build_edge_secondary_range_entries_from_targeted_decode(
                segments, edge_metas, *label_id, prop_key,
            )?
        };
        if let Some(message) = failure_message {
            outcome
                .report
                .failed_range_indexes
                .push((entry.index_id, message));
        }
        let dependencies = vec![
            source_group_dependency(
                SegmentSourceGroupKind::EdgeSource,
                source_groups.edge_source,
            ),
            secondary_declaration_dependency(entry),
        ];
        let (record, _) = write_compaction_component(
            seg_dir,
            segment_id,
            &format!(
                "{}/edge_prop_range_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            ),
            SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: entry.index_id,
            },
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
            dependencies,
            edge_property_range_component_fingerprint(entry.index_id),
            |writer| write_node_prop_range_sidecar_payload(writer, &sidecar_entries),
        )?;
        outcome.records.push(record);
        if entry.state == SecondaryIndexState::Ready {
            outcome
                .stats_evidence
                .range_index_stats
                .push(range_index_stats_from_written_entries(
                    entry,
                    &sidecar_entries,
                ));
        }
    }

    outcome.stats_evidence.sort();
    Ok(outcome)
}

fn write_node_vector_source_components_from_meta(
    core_writer: &mut PackedCoreWriter,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    node_source: [u8; 32],
) -> Result<(Vec<SegmentComponentRecordV1>, Vec<DensePointInput>), EngineError> {
    let plan = prepare_node_vector_source_plan_from_meta(segments, node_metas)?;
    if !plan.has_dense && !plan.has_sparse {
        return Ok((Vec::new(), Vec::new()));
    }

    let node_source_dep = source_group_dependency(SegmentSourceGroupKind::NodeSource, node_source);
    let (meta_record, _) = core_writer.write_component(
        SegmentComponentKind::NodeVectorMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::AuxiliaryBlob,
        vec![node_source_dep.clone()],
        component_fingerprint("flush.node_vector_meta", &[]),
        |writer| write_node_vector_meta_payload(writer, &plan),
    )?;
    let vector_blob_deps = vec![node_source_dep, source_component_dependency(&meta_record)];
    let mut records = Vec::with_capacity(3);
    records.push(meta_record);
    if plan.has_dense {
        let (record, _) = core_writer.write_component(
            SegmentComponentKind::NodeDenseVectorBlob,
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
            vector_blob_deps.clone(),
            component_fingerprint("flush.node_dense_vectors", &[]),
            |writer| write_node_dense_vector_blob_payload_from_meta(writer, segments, node_metas),
        )?;
        records.push(record);
    }
    if plan.has_sparse {
        let (record, _) = core_writer.write_component(
            SegmentComponentKind::NodeSparseVectorBlob,
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
            vector_blob_deps,
            component_fingerprint("flush.node_sparse_vectors", &[]),
            |writer| write_node_sparse_vector_blob_payload_from_meta(writer, segments, node_metas),
        )?;
        records.push(record);
    }

    Ok((records, plan.dense_points))
}

fn prepare_node_vector_source_plan_from_meta(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<NodeVectorSourcePlan, EngineError> {
    let mut rows = Vec::with_capacity(node_metas.len());
    let mut has_dense = false;
    let mut has_sparse = false;
    let mut dense_offset = 0u64;
    let mut sparse_offset = 0u64;
    let mut dense_points = Vec::new();

    for nm in node_metas {
        let mut flags = 0u8;
        let mut entry_dense_offset = 0u64;
        let mut entry_sparse_offset = 0u64;

        if nm.dense_vector_len > 0 {
            flags |= NODE_VECTOR_FLAG_DENSE;
            entry_dense_offset = dense_offset;
            has_dense = true;
            let src = segments[nm.src_seg_idx].raw_node_dense_vectors_mmap();
            let (base, len, end) = checked_compaction_vector_range(
                nm.node_id,
                "dense",
                nm.dense_vector_offset,
                nm.dense_vector_len,
                DENSE_VECTOR_VALUE_SIZE,
                src.len(),
            )?;
            let mut values = Vec::with_capacity(nm.dense_vector_len as usize);
            for index in 0..nm.dense_vector_len as usize {
                let value_offset = base + index * DENSE_VECTOR_VALUE_SIZE as usize;
                values.push(f32::from_le_bytes(
                    src[value_offset..value_offset + DENSE_VECTOR_VALUE_SIZE as usize]
                        .try_into()
                        .unwrap(),
                ));
            }
            dense_points.push(DensePointInput {
                node_id: nm.node_id,
                dense_vector_offset: entry_dense_offset,
                values,
            });
            dense_offset = dense_offset.checked_add(len as u64).ok_or_else(|| {
                EngineError::CorruptRecord("dense vector output offset overflow".into())
            })?;
            debug_assert!(end <= src.len());
        }

        if nm.sparse_vector_len > 0 {
            flags |= NODE_VECTOR_FLAG_SPARSE;
            entry_sparse_offset = sparse_offset;
            has_sparse = true;
            let src = segments[nm.src_seg_idx].raw_node_sparse_vectors_mmap();
            let (_, len, _) = checked_compaction_vector_range(
                nm.node_id,
                "sparse",
                nm.sparse_vector_offset,
                nm.sparse_vector_len,
                SPARSE_VECTOR_ENTRY_SIZE,
                src.len(),
            )?;
            sparse_offset = sparse_offset.checked_add(len as u64).ok_or_else(|| {
                EngineError::CorruptRecord("sparse vector output offset overflow".into())
            })?;
        }

        rows.push(NodeVectorSourceRow {
            node_id: nm.node_id,
            flags,
            dense_offset: entry_dense_offset,
            dense_len: nm.dense_vector_len,
            sparse_offset: entry_sparse_offset,
            sparse_len: nm.sparse_vector_len,
        });
    }

    Ok(NodeVectorSourcePlan {
        rows,
        has_dense,
        has_sparse,
        dense_points,
    })
}

fn checked_compaction_vector_range(
    node_id: u64,
    label: &str,
    source_offset: u64,
    element_count: u32,
    element_size: u64,
    source_len: usize,
) -> Result<(usize, usize, usize), EngineError> {
    let base = usize::try_from(source_offset).map_err(|_| {
        EngineError::CorruptRecord(format!("node {node_id} {label} vector offset too large"))
    })?;
    let len_u64 = (element_count as u64)
        .checked_mul(element_size)
        .ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "node {node_id} {label} vector byte length overflow"
            ))
        })?;
    let len = usize::try_from(len_u64).map_err(|_| {
        EngineError::CorruptRecord(format!(
            "node {node_id} {label} vector byte length too large"
        ))
    })?;
    let end = base.checked_add(len).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "node {node_id} {label} vector range offset overflow: start={base}, len={len}"
        ))
    })?;
    if end > source_len {
        return Err(EngineError::CorruptRecord(format!(
            "node {node_id} {label} vector range [{base}, {end}) exceeds source length {source_len}"
        )));
    }
    Ok((base, len, end))
}

fn write_node_dense_vector_blob_payload_from_meta(
    w: &mut impl Write,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    for nm in node_metas {
        if nm.dense_vector_len == 0 {
            continue;
        }
        let src = segments[nm.src_seg_idx].raw_node_dense_vectors_mmap();
        let (base, _, end) = checked_compaction_vector_range(
            nm.node_id,
            "dense",
            nm.dense_vector_offset,
            nm.dense_vector_len,
            DENSE_VECTOR_VALUE_SIZE,
            src.len(),
        )?;
        w.write_all(&src[base..end])?;
    }
    Ok(())
}

fn write_node_sparse_vector_blob_payload_from_meta(
    w: &mut impl Write,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    for nm in node_metas {
        if nm.sparse_vector_len == 0 {
            continue;
        }
        let src = segments[nm.src_seg_idx].raw_node_sparse_vectors_mmap();
        let (base, _, end) = checked_compaction_vector_range(
            nm.node_id,
            "sparse",
            nm.sparse_vector_offset,
            nm.sparse_vector_len,
            SPARSE_VECTOR_ENTRY_SIZE,
            src.len(),
        )?;
        w.write_all(&src[base..end])?;
    }
    Ok(())
}

fn sparse_posting_groups_from_nodes(
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<BTreeMap<u32, Vec<(u64, f32)>>, EngineError> {
    let mut groups: BTreeMap<u32, Vec<(u64, f32)>> = BTreeMap::new();
    for node in nodes.values() {
        let Some(values) = node.sparse_vector.as_ref() else {
            continue;
        };
        for &(dimension_id, weight) in values {
            groups
                .entry(dimension_id)
                .or_default()
                .push((node.id, weight));
        }
    }
    sort_sparse_posting_groups(&mut groups)?;
    Ok(groups)
}

fn write_sparse_posting_index_from_meta(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    source_groups: SegmentComponentSourceGroups,
) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
    let mut groups: BTreeMap<u32, Vec<(u64, f32)>> = BTreeMap::new();
    for nm in node_metas {
        if nm.sparse_vector_len == 0 {
            continue;
        }
        let src = segments[nm.src_seg_idx].raw_node_sparse_vectors_mmap();
        let (base, _, end) = checked_compaction_vector_range(
            nm.node_id,
            "sparse",
            nm.sparse_vector_offset,
            nm.sparse_vector_len,
            SPARSE_VECTOR_ENTRY_SIZE,
            src.len(),
        )?;
        for entry_offset in (base..end).step_by(SPARSE_VECTOR_ENTRY_SIZE as usize) {
            let dimension_id =
                u32::from_le_bytes(src[entry_offset..entry_offset + 4].try_into().unwrap());
            let weight =
                f32::from_le_bytes(src[entry_offset + 4..entry_offset + 8].try_into().unwrap());
            groups
                .entry(dimension_id)
                .or_default()
                .push((nm.node_id, weight));
        }
    }
    sort_sparse_posting_groups(&mut groups)?;
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let dependencies = vec![
        source_group_dependency(
            SegmentSourceGroupKind::SparseVectorSource,
            source_groups.sparse_vector_source,
        ),
        ComponentDependencyV1::SparseVectorConfig {
            fingerprint: component_fingerprint("sparse_vector_config", &[]),
        },
    ];
    let (index_record, postings_record, _) = write_compaction_component_pair(
        seg_dir,
        segment_id,
        SPARSE_POSTING_INDEX_FILENAME,
        SegmentComponentKind::SparsePostingIndex,
        component_fingerprint("flush.sparse_posting_index", &[]),
        SPARSE_POSTINGS_FILENAME,
        SegmentComponentKind::SparsePostings,
        component_fingerprint("flush.sparse_postings", &[]),
        ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::ExactVectorScan,
        },
        ComponentTrustClass::OptionalApproximateAccelerator,
        dependencies,
        |index_writer, postings_writer| {
            write_sparse_posting_files_to_writers(index_writer, postings_writer, &groups)
        },
    )?;
    Ok(vec![index_record, postings_record])
}

fn sort_sparse_posting_groups(
    groups: &mut BTreeMap<u32, Vec<(u64, f32)>>,
) -> Result<(), EngineError> {
    for (&dimension_id, postings) in groups.iter_mut() {
        postings.sort_unstable_by_key(|&(node_id, _)| node_id);
        for window in postings.windows(2) {
            if window[0].0 == window[1].0 {
                return Err(EngineError::CorruptRecord(format!(
                    "sparse posting dimension {} has duplicate node {}",
                    dimension_id, window[0].0
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::degree_cache::DegreeDelta;
    use crate::property_value_semantics::NUMERIC_RANGE_KEY_BYTES;
    use std::sync::Arc;

    fn write_segment(
        seg_dir: &Path,
        segment_id: u64,
        memtable: &Memtable,
        dense_config: Option<&DenseVectorConfig>,
    ) -> Result<SegmentInfo, EngineError> {
        let degree_overlay = DegreeOverlaySnapshot::empty();
        super::write_segment_with_degree_overlay_and_secondary_indexes(
            seg_dir,
            segment_id,
            memtable,
            dense_config,
            degree_overlay.as_ref(),
            &[],
        )
    }

    fn write_segment_with_secondary_indexes(
        seg_dir: &Path,
        segment_id: u64,
        memtable: &Memtable,
        dense_config: Option<&DenseVectorConfig>,
        secondary_indexes: &[SecondaryIndexManifestEntry],
    ) -> Result<SegmentInfo, EngineError> {
        let degree_overlay = DegreeOverlaySnapshot::empty();
        super::write_segment_with_degree_overlay_and_secondary_indexes(
            seg_dir,
            segment_id,
            memtable,
            dense_config,
            degree_overlay.as_ref(),
            secondary_indexes,
        )
    }

    fn make_node(id: u64, label_id: u32, key: &str) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(label_id).unwrap(),
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_node_with_props(id: u64, label_id: u32, key: &str) -> NodeRecord {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String(key.to_string()));
        props.insert("score".to_string(), PropValue::Float(0.95));
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(label_id).unwrap(),
            key: key.to_string(),
            props,
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_edge(id: u64, from: u64, to: u64, label_id: u32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            label_id,
            props: BTreeMap::new(),
            created_at: 2000,
            updated_at: 2001,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }

    fn make_node_with_custom_props(
        id: u64,
        label_id: u32,
        key: &str,
        props: BTreeMap<String, PropValue>,
        updated_at: i64,
    ) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(label_id).unwrap(),
            key: key.to_string(),
            props,
            created_at: 1000,
            updated_at,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_node_with_labels(
        id: u64,
        label_ids: &[u32],
        key: &str,
        props: BTreeMap<String, PropValue>,
        updated_at: i64,
    ) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::from_canonical_ids(label_ids).unwrap(),
            key: key.to_string(),
            props,
            created_at: 1000,
            updated_at,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn read_payload_file(path: &Path) -> Vec<u8> {
        let data = fs::read(path).unwrap();
        if data.len() >= crate::segment_components::COMPONENT_IDENTITY_HEADER_LEN
            && data[0..crate::segment_components::COMPONENT_IDENTITY_HEADER_MAGIC.len()]
                == crate::segment_components::COMPONENT_IDENTITY_HEADER_MAGIC
        {
            let header = crate::segment_components::decode_identity_header(&data).unwrap();
            let start = header.payload_offset as usize;
            let end = start + header.payload_len as usize;
            return data[start..end].to_vec();
        }
        data
    }

    fn read_manifest_component_payload(seg_dir: &Path, kind: SegmentComponentKind) -> Vec<u8> {
        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .unwrap_or_else(|| panic!("missing component {:?}", kind));
        match &record.handle {
            ComponentHandleV1::ExternalFile { relative_path, .. } => {
                read_payload_file(&seg_dir.join(relative_path))
            }
            ComponentHandleV1::PackedRange { offset, len, .. } => {
                let core = read_payload_file(
                    &seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME),
                );
                let start = *offset as usize;
                let end = start + *len as usize;
                core[start..end].to_vec()
            }
        }
    }

    fn write_packed_segment_from_ops(
        ops: Vec<WalOp>,
        dense_config: Option<&DenseVectorConfig>,
    ) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        for (index, op) in ops.iter().enumerate() {
            mt.apply_op(op, (index + 1) as u64);
        }
        write_segment(&seg_dir, 1, &mt, dense_config).unwrap();
        (dir, seg_dir)
    }

    fn read_record_spans_from_payload(data: &[u8]) -> RecordDataSpans {
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            let entry_offset = 8 + index * 16;
            let id = u64::from_le_bytes(data[entry_offset..entry_offset + 8].try_into().unwrap());
            let data_offset = u64::from_le_bytes(
                data[entry_offset + 8..entry_offset + 16]
                    .try_into()
                    .unwrap(),
            );
            let next_offset = if index + 1 == count {
                data.len() as u64
            } else {
                let next_entry_offset = 8 + (index + 1) * 16;
                u64::from_le_bytes(
                    data[next_entry_offset + 8..next_entry_offset + 16]
                        .try_into()
                        .unwrap(),
                )
            };
            entries.push((id, data_offset, (next_offset - data_offset) as u32));
        }
        entries
    }

    fn assert_component_handle_is_packed(seg_dir: &Path, kind: SegmentComponentKind) {
        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .unwrap_or_else(|| panic!("missing component {:?}", kind));
        assert!(
            matches!(record.handle, ComponentHandleV1::PackedRange { .. }),
            "component {:?} should be packed",
            record.kind
        );
    }

    fn assert_component_handle_is_external(seg_dir: &Path, kind: SegmentComponentKind) {
        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .unwrap_or_else(|| panic!("missing component {:?}", kind));
        assert!(
            matches!(record.handle, ComponentHandleV1::ExternalFile { .. }),
            "component {:?} should be external",
            record.kind
        );
    }

    fn assert_no_legacy_property_components(seg_dir: &Path) {
        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        assert!(manifest.components.iter().all(|record| {
            !matches!(
                record.kind,
                SegmentComponentKind::LegacyNodePropertyIndex
                    | SegmentComponentKind::NodePropertyHashMetadata
            )
        }));
    }

    fn assert_only_manifested_component_files(seg_dir: &Path) {
        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        let mut expected = BTreeSet::from([PathBuf::from(SEGMENT_COMPONENT_MANIFEST_FILENAME)]);
        for record in &manifest.components {
            if let ComponentHandleV1::ExternalFile { relative_path, .. } = &record.handle {
                expected.insert(PathBuf::from(relative_path));
            }
        }
        for record in &manifest.unknown_optional_components {
            if record.wire.handle.handle_tag == 1 {
                if let Some(relative_path) = &record.wire.handle.relative_path {
                    expected.insert(PathBuf::from(relative_path));
                }
            }
        }

        let mut actual = Vec::new();
        collect_regular_files_relative_to(seg_dir, seg_dir, &mut actual);
        for relative_path in actual {
            assert!(
                expected.contains(&relative_path),
                "unexpected unmanifested segment file {}",
                relative_path.display()
            );
        }
    }

    fn collect_regular_files_relative_to(root: &Path, dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                collect_regular_files_relative_to(root, &path, files);
            } else if file_type.is_file() {
                files.push(path.strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }

    fn read_weight_index_entries(seg_dir: &Path) -> Vec<(u32, u32, u64)> {
        let data = read_manifest_component_payload(seg_dir, SegmentComponentKind::EdgeWeightIndex);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            let off = 8 + index * 16;
            entries.push((
                u32::from_le_bytes(data[off..off + 4].try_into().unwrap()),
                u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap()),
                u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap()),
            ));
        }
        entries
    }

    fn read_i64_index_entries(seg_dir: &Path, kind: SegmentComponentKind) -> Vec<(u32, i64, u64)> {
        let data = read_manifest_component_payload(seg_dir, kind);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            let off = 8 + index * 20;
            entries.push((
                u32::from_le_bytes(data[off..off + 4].try_into().unwrap()),
                i64::from_le_bytes(data[off + 4..off + 12].try_into().unwrap()),
                u64::from_le_bytes(data[off + 12..off + 20].try_into().unwrap()),
            ));
        }
        entries
    }

    fn read_secondary_eq_groups(
        seg_dir: &Path,
        kind: SegmentComponentKind,
    ) -> BTreeMap<u64, Vec<u64>> {
        let payload = read_manifest_component_payload(seg_dir, kind);
        let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
        let mut groups = BTreeMap::new();
        for index in 0..count {
            let entry_off = 8 + index * SECONDARY_EQ_ENTRY_SIZE as usize;
            let value_hash =
                u64::from_le_bytes(payload[entry_off..entry_off + 8].try_into().unwrap());
            let ids_offset =
                u64::from_le_bytes(payload[entry_off + 8..entry_off + 16].try_into().unwrap())
                    as usize;
            let id_count =
                u32::from_le_bytes(payload[entry_off + 16..entry_off + 20].try_into().unwrap())
                    as usize;
            let ids = (0..id_count)
                .map(|id_index| {
                    let off = ids_offset + id_index * 8;
                    u64::from_le_bytes(payload[off..off + 8].try_into().unwrap())
                })
                .collect::<Vec<_>>();
            groups.insert(value_hash, ids);
        }
        groups
    }

    fn rewrite_eq_sidecar_as_old_raw_hash(
        seg_dir: &Path,
        kind: SegmentComponentKind,
        old_build_fingerprint: u64,
        groups: &BTreeMap<u64, Vec<u64>>,
    ) {
        let mut payload = Vec::new();
        write_node_prop_eq_sidecar_payload(&mut payload, groups).unwrap();
        let payload_digest: [u8; 32] = Sha256::digest(&payload).into();

        let mut manifest = read_segment_component_manifest(seg_dir).unwrap();
        let segment_id = manifest.segment_id;
        let (relative_path, payload_offset, payload_len, header) = {
            let record = manifest
                .components
                .iter_mut()
                .find(|record| record.kind == kind)
                .unwrap_or_else(|| panic!("missing component {:?}", kind));
            let ComponentHandleV1::ExternalFile {
                relative_path,
                payload_offset,
                payload_len,
            } = &mut record.handle
            else {
                panic!("equality sidecar should be external");
            };

            record.payload_len = payload.len() as u64;
            *payload_len = record.payload_len;
            record.payload_digest = Some(payload_digest);
            record.build_fingerprint = old_build_fingerprint;
            record.component_id = component_id(
                segment_id,
                &record.kind,
                record.logical_format_version,
                record.payload_len,
                record.payload_digest.as_ref(),
                &record.dependency_digest,
                record.build_fingerprint,
            );
            let header = ComponentIdentityHeaderV1 {
                segment_format_version: SEGMENT_FORMAT_VERSION,
                segment_id,
                component_kind: record.kind.clone(),
                logical_format_version: record.logical_format_version,
                created_generation: record.created_generation,
                payload_offset: *payload_offset,
                payload_len: record.payload_len,
                component_id: record.component_id,
                dependency_digest: record.dependency_digest,
                build_fingerprint: record.build_fingerprint,
                payload_digest: record.payload_digest,
            };
            (relative_path.clone(), *payload_offset, *payload_len, header)
        };

        let path = seg_dir.join(relative_path);
        let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        let header_bytes = encode_identity_header(&header);
        assert_eq!(header_bytes.len(), COMPONENT_IDENTITY_HEADER_LEN);
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&header_bytes).unwrap();
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        file.write_all(&payload).unwrap();
        assert_eq!(payload_len as usize, payload.len());
        file.sync_all().unwrap();

        write_segment_component_manifest(seg_dir, &manifest).unwrap();
    }

    fn read_secondary_range_entries(
        seg_dir: &Path,
        kind: SegmentComponentKind,
    ) -> Vec<(NumericRangeSortKey, u64)> {
        let payload = read_manifest_component_payload(seg_dir, kind);
        let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
        (0..count)
            .map(|index| {
                let off = 8 + index * 32;
                (
                    NumericRangeSortKey::from_sidecar_bytes(
                        payload[off..off + NUMERIC_RANGE_KEY_BYTES]
                            .try_into()
                            .unwrap(),
                    )
                    .unwrap(),
                    u64::from_le_bytes(
                        payload[off + NUMERIC_RANGE_KEY_BYTES..off + NUMERIC_RANGE_KEY_BYTES + 8]
                            .try_into()
                            .unwrap(),
                    ),
                )
            })
            .collect()
    }

    fn test_component_record(kind: SegmentComponentKind) -> SegmentComponentRecordV1 {
        SegmentComponentRecordV1 {
            component_id: [0; 32],
            kind,
            logical_format_version: 1,
            created_generation: 0,
            requirement: ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            trust_class: ComponentTrustClass::OptionalCandidateIndex,
            handle: ComponentHandleV1::ExternalFile {
                relative_path: "secondary_indexes/test.dat".to_string(),
                payload_offset: 0,
                payload_len: 0,
            },
            payload_len: 0,
            payload_digest: Some([0; 32]),
            dependency_digest: [0; 32],
            dependencies: Vec::new(),
            build_fingerprint: 0,
        }
    }

    fn compact_copy_segment_for_test(
        source: Arc<SegmentReader>,
        out_dir: &Path,
        out_segment_id: u64,
        secondary_indexes: &[SecondaryIndexManifestEntry],
    ) -> SegmentReader {
        std::fs::create_dir_all(out_dir).unwrap();
        let segments = vec![source.clone()];
        let mut core_writer = create_compaction_core_writer(out_dir, out_segment_id).unwrap();
        let (node_record, node_copy_info) =
            write_merged_nodes_dat(&mut core_writer, &segments).unwrap();
        let (edge_record, edge_copy_info) =
            write_merged_edges_dat(&mut core_writer, &segments).unwrap();
        let node_copy = &node_copy_info[0];
        let edge_copy = &edge_copy_info[0];

        let mut node_metas = Vec::new();
        for index in 0..source.node_meta_count() as usize {
            let meta = source.node_meta_at(index).unwrap();
            let (dense_vector_offset, dense_vector_len, sparse_vector_offset, sparse_vector_len) =
                source.node_vector_meta_at(index).unwrap();
            node_metas.push(CompactNodeMeta {
                node_id: meta.node_id,
                new_data_offset: meta.data_offset - node_copy.orig_data_start
                    + node_copy.new_data_base,
                data_len: meta.data_len,
                label_ids: meta.label_ids,
                updated_at: meta.updated_at,
                weight: meta.weight,
                key_len: meta.key_len,
                dense_vector_offset,
                dense_vector_len,
                sparse_vector_offset,
                sparse_vector_len,
                src_seg_idx: 0,
                src_data_offset: meta.data_offset,
                last_write_seq: meta.last_write_seq,
            });
        }

        let mut edge_metas = Vec::new();
        for index in 0..source.edge_meta_count() as usize {
            let (
                edge_id,
                data_offset,
                data_len,
                from,
                to,
                label_id,
                updated_at,
                weight,
                valid_from,
                valid_to,
                last_write_seq,
            ) = source.edge_meta_at(index).unwrap();
            edge_metas.push(CompactEdgeMeta {
                edge_id,
                new_data_offset: data_offset - edge_copy.orig_data_start + edge_copy.new_data_base,
                data_len,
                from,
                to,
                label_id,
                updated_at,
                weight,
                valid_from,
                valid_to,
                src_seg_idx: 0,
                src_data_offset: data_offset,
                last_write_seq,
            });
        }

        let (source_groups, dense_points) = write_compaction_source_components(
            out_segment_id,
            &mut core_writer,
            &segments,
            node_record,
            edge_record,
            &node_metas,
            &edge_metas,
        )
        .unwrap();
        let component_output = write_indexes_from_metadata_with_secondary_indexes(
            out_segment_id,
            out_dir,
            &mut core_writer,
            &segments,
            &node_metas,
            &edge_metas,
            None,
            dense_points,
            true,
            secondary_indexes,
            source_groups,
        )
        .unwrap();
        let mut records = component_output.records;
        records.extend(finish_compaction_core_writer(core_writer).unwrap());
        let info = finalize_compaction_segment(
            out_dir,
            out_segment_id,
            node_metas.len() as u64,
            edge_metas.len() as u64,
            records,
        )
        .unwrap();
        let reference_stats = crate::planner_stats::build_compaction_stats(
            out_segment_id,
            out_dir,
            &segments,
            &node_metas,
            &edge_metas,
            secondary_indexes,
        )
        .unwrap();
        let reader =
            SegmentReader::open_with_info(out_dir, &info, None, secondary_indexes).unwrap();
        assert_eq!(
            reader.planner_stats().expect("planner stats should load"),
            &reference_stats
        );
        reader
    }

    // --- encode_node_record / encode_edge_record ---

    #[test]
    fn test_encode_node_record_roundtrip() {
        let node = make_node_with_props(42, 1, "alice");
        let mut buf = Vec::new();
        encode_node_record_into(&mut buf, &node).unwrap();

        // Verify structure (no id): label_count(1) + label_id(4) + key_len(2) + key(5) + created(8) + updated(8) + weight(4) + props_len(4) + props(N)
        assert!(buf.len() > 30 + 5); // minimum size with key "alice"

        assert_eq!(buf[0], 1);
        let label_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        assert_eq!(label_id, 1);

        let key_len = u16::from_le_bytes(buf[5..7].try_into().unwrap()) as usize;
        assert_eq!(key_len, 5);

        let key = std::str::from_utf8(&buf[7..7 + key_len]).unwrap();
        assert_eq!(key, "alice");
    }

    #[test]
    fn test_encode_edge_record_roundtrip() {
        let edge = make_edge(100, 1, 2, 10);
        let mut buf = Vec::new();
        encode_edge_record_into(&mut buf, &edge).unwrap();

        // No id in data section. Starts with from
        let from = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(from, 1);

        let to = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        assert_eq!(to, 2);

        let label_id = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        assert_eq!(label_id, 10);
    }

    // --- packed node records ---

    #[test]
    fn test_write_node_records_empty() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(Vec::new(), None);
        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeRecords);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
        assert_eq!(data.len(), 8); // just the count
    }

    #[test]
    fn test_write_node_records_multiple() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::UpsertNode(make_node(3, 1, "charlie")),
                WalOp::UpsertNode(make_node(1, 1, "alice")),
                WalOp::UpsertNode(make_node(2, 1, "bob")),
            ],
            None,
        );

        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeRecords);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 3);

        // Index entries should be sorted by node_id
        let idx_start = 8;
        let id0 = u64::from_le_bytes(data[idx_start..idx_start + 8].try_into().unwrap());
        let id1 = u64::from_le_bytes(data[idx_start + 16..idx_start + 24].try_into().unwrap());
        let id2 = u64::from_le_bytes(data[idx_start + 32..idx_start + 40].try_into().unwrap());
        assert_eq!(id0, 1);
        assert_eq!(id1, 2);
        assert_eq!(id2, 3);

        // Verify the offset of the first record leads to valid data.
        // The id is NOT in the record; first fields are label_count + labels.
        let offset0 =
            u64::from_le_bytes(data[idx_start + 8..idx_start + 16].try_into().unwrap()) as usize;
        assert_eq!(data[offset0], 1);
        let label_id = u32::from_le_bytes(data[offset0 + 1..offset0 + 5].try_into().unwrap());
        assert_eq!(label_id, 1);
    }

    // --- packed edge records ---

    #[test]
    fn test_write_edge_records_empty() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(Vec::new(), None);
        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::EdgeRecords);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_write_edge_records_multiple() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::UpsertEdge(make_edge(2, 1, 3, 10)),
                WalOp::UpsertEdge(make_edge(1, 1, 2, 10)),
            ],
            None,
        );

        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::EdgeRecords);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 2);

        // Index should be sorted: edge 1 then edge 2
        let idx_start = 8;
        let eid0 = u64::from_le_bytes(data[idx_start..idx_start + 8].try_into().unwrap());
        let eid1 = u64::from_le_bytes(data[idx_start + 16..idx_start + 24].try_into().unwrap());
        assert_eq!(eid0, 1);
        assert_eq!(eid1, 2);
    }

    // --- write_adjacency_index ---

    fn make_adj(edge_id: u64, label_id: u32, neighbor_id: u64, weight: f32) -> AdjEntry {
        AdjEntry {
            edge_id,
            label_id,
            neighbor_id,
            weight,
            valid_from: 1000,
            valid_to: i64::MAX,
        }
    }

    #[test]
    fn test_write_adjacency_empty() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(Vec::new(), None);
        let idx_data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::AdjOutIndex);
        let count = u64::from_le_bytes(idx_data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_write_adjacency_single_node() {
        let mut edge_10 = make_edge(10, 1, 2, 1);
        edge_10.weight = 0.5;
        let mut edge_11 = make_edge(11, 1, 3, 1);
        edge_11.weight = 0.7;
        let edge_12 = make_edge(12, 1, 4, 2);
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::UpsertEdge(edge_10),
                WalOp::UpsertEdge(edge_11),
                WalOp::UpsertEdge(edge_12),
            ],
            None,
        );

        let idx_data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::AdjOutIndex);
        let count = u64::from_le_bytes(idx_data[0..8].try_into().unwrap());
        // Node 1 has 2 label groups: label_id=1 (2 entries) and label_id=2 (1 entry)
        assert_eq!(count, 2);

        let dat_data =
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::AdjOutPostings);
        // Delta-encoded variable-length postings, much smaller than fixed-size.
        // 3 postings with small ids/deltas → expect < 108 bytes (old fixed-size).
        assert!(!dat_data.is_empty());
        assert!(
            dat_data.len() < 108,
            "delta encoding should be smaller than fixed 36-byte postings"
        );
    }

    #[test]
    fn test_write_adjacency_sorted_index() {
        let mut edge_10 = make_edge(10, 5, 6, 1);
        edge_10.weight = 0.5;
        let mut edge_11 = make_edge(11, 1, 2, 1);
        edge_11.weight = 0.7;
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![WalOp::UpsertEdge(edge_10), WalOp::UpsertEdge(edge_11)],
            None,
        );

        let idx_data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::AdjOutIndex);
        let count = u64::from_le_bytes(idx_data[0..8].try_into().unwrap());
        assert_eq!(count, 2);

        // First index entry should be node_id=1 (sorted)
        let node_id_0 = u64::from_le_bytes(idx_data[8..16].try_into().unwrap());
        let node_id_1 = u64::from_le_bytes(idx_data[8 + 24..16 + 24].try_into().unwrap());
        assert_eq!(node_id_0, 1);
        assert_eq!(node_id_1, 5);
    }

    #[test]
    fn test_adjacency_prepare_emit_matches_packed_payloads() {
        let mut adj: NodeIdMap<NodeIdMap<AdjEntry>> = NodeIdMap::default();

        let mut node_7 = NodeIdMap::default();
        let mut edge_30 = make_adj(30, 2, 9, 1.25);
        edge_30.valid_from = 10;
        edge_30.valid_to = 20;
        node_7.insert(edge_30.edge_id, edge_30);
        let mut edge_31 = make_adj(31, 2, 10, 2.5);
        edge_31.valid_from = 11;
        edge_31.valid_to = i64::MAX;
        node_7.insert(edge_31.edge_id, edge_31);
        adj.insert(7, node_7);

        let mut node_3 = NodeIdMap::default();
        node_3.insert(4, make_adj(4, 1, 2, 0.5));
        node_3.insert(6, make_adj(6, 3, 8, 0.75));
        adj.insert(3, node_3);

        let mut ops = Vec::new();
        for (&from, edges) in &adj {
            for entry in edges.values() {
                let mut edge = make_edge(entry.edge_id, from, entry.neighbor_id, entry.label_id);
                edge.weight = entry.weight;
                edge.valid_from = entry.valid_from;
                edge.valid_to = entry.valid_to;
                ops.push(WalOp::UpsertEdge(edge));
            }
        }
        let (_dir, seg_dir) = write_packed_segment_from_ops(ops, None);

        let plan = prepare_adjacency_payloads(adj.clone());
        let mut direct_idx = Vec::new();
        let mut direct_dat = Vec::new();
        write_adjacency_postings_payload(&mut direct_dat, &plan).unwrap();
        write_adjacency_index_payload(&mut direct_idx, &plan).unwrap();

        assert_eq!(
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::AdjOutIndex),
            direct_idx
        );
        assert_eq!(
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::AdjOutPostings),
            direct_dat
        );
    }

    // --- Packed key index payload ---

    #[test]
    fn test_packed_key_index_payload_empty() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(Vec::new(), None);
        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::KeyIndex);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
        assert_eq!(data.len(), 8);
    }

    #[test]
    fn test_packed_key_index_payload_sorted_by_label_and_key() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::UpsertNode(make_node(1, 2, "zebra")),
                WalOp::UpsertNode(make_node(2, 1, "bob")),
                WalOp::UpsertNode(make_node(3, 1, "alice")),
            ],
            None,
        );

        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::KeyIndex);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 3);

        // Read offset table
        let offsets: Vec<u64> = (0..3)
            .map(|i| {
                let start = 8 + i * 8;
                u64::from_le_bytes(data[start..start + 8].try_into().unwrap())
            })
            .collect();

        // First entry should be label_id=1, key="alice"
        let off0 = offsets[0] as usize;
        let label0 = u32::from_le_bytes(data[off0..off0 + 4].try_into().unwrap());
        let node0 = u64::from_le_bytes(data[off0 + 4..off0 + 12].try_into().unwrap());
        let klen0 = u16::from_le_bytes(data[off0 + 12..off0 + 14].try_into().unwrap()) as usize;
        let key0 = std::str::from_utf8(&data[off0 + 14..off0 + 14 + klen0]).unwrap();
        assert_eq!(label0, 1);
        assert_eq!(key0, "alice");
        assert_eq!(node0, 3);

        // Second entry should be label_id=1, key="bob"
        let off1 = offsets[1] as usize;
        let label1 = u32::from_le_bytes(data[off1..off1 + 4].try_into().unwrap());
        let klen1 = u16::from_le_bytes(data[off1 + 12..off1 + 14].try_into().unwrap()) as usize;
        let key1 = std::str::from_utf8(&data[off1 + 14..off1 + 14 + klen1]).unwrap();
        assert_eq!(label1, 1);
        assert_eq!(key1, "bob");

        // Third entry should be label_id=2, key="zebra"
        let off2 = offsets[2] as usize;
        let label2 = u32::from_le_bytes(data[off2..off2 + 4].try_into().unwrap());
        assert_eq!(label2, 2);
    }

    // --- Packed tombstones payload ---

    #[test]
    fn test_packed_tombstones_payload_empty() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(Vec::new(), None);
        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::Tombstones);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_packed_tombstones_payload_mixed() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::DeleteNode {
                    id: 5,
                    deleted_at: 1000,
                },
                WalOp::DeleteNode {
                    id: 3,
                    deleted_at: 1001,
                },
                WalOp::DeleteEdge {
                    id: 10,
                    deleted_at: 2000,
                },
            ],
            None,
        );

        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::Tombstones);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 3);

        // Each tombstone: 1 byte kind + 8 bytes id + 8 bytes deleted_at + 8 bytes last_write_seq = 25 bytes
        // Node tombstones first (sorted: 3, 5), then edge tombstones (sorted: 10)
        let entry_size = 25;
        let off = 8;

        assert_eq!(data[off], 0); // kind = node
        let id0 = u64::from_le_bytes(data[off + 1..off + 9].try_into().unwrap());
        let ts0 = i64::from_le_bytes(data[off + 9..off + 17].try_into().unwrap());
        assert_eq!(id0, 3);
        assert_eq!(ts0, 1001);

        assert_eq!(data[off + entry_size], 0); // kind = node
        let id1 = u64::from_le_bytes(
            data[off + entry_size + 1..off + entry_size + 9]
                .try_into()
                .unwrap(),
        );
        let ts1 = i64::from_le_bytes(
            data[off + entry_size + 9..off + entry_size + 17]
                .try_into()
                .unwrap(),
        );
        assert_eq!(id1, 5);
        assert_eq!(ts1, 1000);

        assert_eq!(data[off + 2 * entry_size], 1); // kind = edge
        let id2 = u64::from_le_bytes(
            data[off + 2 * entry_size + 1..off + 2 * entry_size + 9]
                .try_into()
                .unwrap(),
        );
        let ts2 = i64::from_le_bytes(
            data[off + 2 * entry_size + 9..off + 2 * entry_size + 17]
                .try_into()
                .unwrap(),
        );
        assert_eq!(id2, 10);
        assert_eq!(ts2, 2000);
    }

    // --- write_segment (full pipeline) ---

    #[test]
    fn test_write_segment_full() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 99,
                deleted_at: 9999,
            },
            0,
        );

        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();
        assert_eq!(info.id, 1);
        assert_eq!(info.node_count, 2);
        assert_eq!(info.edge_count, 1);

        assert!(seg_dir.join(PACKED_CORE_FILENAME).exists());
        assert!(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME).exists());
        assert_only_manifested_component_files(&seg_dir);
        for kind in [
            SegmentComponentKind::NodeRecords,
            SegmentComponentKind::EdgeRecords,
            SegmentComponentKind::NodeMetadata,
            SegmentComponentKind::EdgeMetadata,
            SegmentComponentKind::Tombstones,
            SegmentComponentKind::KeyIndex,
            SegmentComponentKind::NodeLabelIndex,
            SegmentComponentKind::EdgeLabelIndex,
            SegmentComponentKind::EdgeTripleIndex,
            SegmentComponentKind::AdjOutIndex,
            SegmentComponentKind::AdjOutPostings,
            SegmentComponentKind::AdjInIndex,
            SegmentComponentKind::AdjInPostings,
            SegmentComponentKind::TimestampIndex,
            SegmentComponentKind::EdgeWeightIndex,
            SegmentComponentKind::EdgeUpdatedAtIndex,
            SegmentComponentKind::EdgeValidFromIndex,
            SegmentComponentKind::EdgeValidToIndex,
        ] {
            assert_component_handle_is_packed(&seg_dir, kind);
        }
        assert_component_handle_is_external(&seg_dir, SegmentComponentKind::PackedSegmentContainer);
        assert_no_legacy_property_components(&seg_dir);
        assert!(!seg_dir.join(SECONDARY_INDEX_DIRNAME).exists());
        assert!(!seg_dir
            .join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)
            .exists());
        assert!(!seg_dir
            .join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)
            .exists());
    }

    #[test]
    fn test_write_segment_degree_sidecar_overlay_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 3);

        let mut deltas = NodeIdMap::default();
        deltas.insert(1, DegreeDelta::add_valid_edge(1, 2, 1.0));
        deltas.insert(2, DegreeDelta::add_valid_edge_incoming(1.0));
        let overlay = DegreeOverlaySnapshot::from_flat(deltas);

        write_segment_with_degree_overlay_and_secondary_indexes(
            &seg_dir,
            1,
            &mt,
            None,
            overlay.as_ref(),
            &[],
        )
        .unwrap();

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(reader.degree_delta_available());
        assert_eq!(reader.degree_delta(1).unwrap().out_degree, 1);
        assert_eq!(reader.degree_delta(2).unwrap().in_degree, 1);
        assert_eq!(reader.degree_delta(99).unwrap(), DegreeDelta::ZERO);
    }

    #[test]
    fn test_segment_reader_tolerates_missing_and_corrupt_degree_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &mt, None).unwrap();

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.degree_delta_available());
        assert!(reader.get_node(1).unwrap().is_some());

        std::fs::write(seg_dir.join(DEGREE_DELTA_FILENAME), b"not a degree sidecar").unwrap();
        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.degree_delta_available());
        assert!(reader.get_node(1).unwrap().is_some());
    }

    #[test]
    fn test_compaction_copy_writes_v10_manifest_and_root_identity() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");
        let out_dir = dir.path().join("seg_0002");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 3);
        write_segment(&source_dir, 1, &mt, None).unwrap();
        let source = Arc::new(SegmentReader::open_unpinned_for_test(&source_dir, 1, None).unwrap());
        let reader = compact_copy_segment_for_test(source, &out_dir, 2, &[]);

        assert_ne!(reader.segment_data_id(), [0; 32]);
        let manifest_bytes = fs::read(out_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        assert_eq!(manifest.segment_format_version, SEGMENT_FORMAT_VERSION);
        assert_eq!(manifest.segment_id, 2);
        assert_eq!(manifest.segment_data_id, reader.segment_data_id());
        assert_eq!(
            manifest.build_kind,
            crate::segment_components::SegmentComponentBuildKind::Compaction
        );
        assert!(out_dir.join(PACKED_CORE_FILENAME).exists());
        assert_only_manifested_component_files(&out_dir);
        for kind in [
            SegmentComponentKind::NodeRecords,
            SegmentComponentKind::EdgeRecords,
            SegmentComponentKind::NodeMetadata,
            SegmentComponentKind::EdgeMetadata,
            SegmentComponentKind::Tombstones,
            SegmentComponentKind::KeyIndex,
            SegmentComponentKind::NodeLabelIndex,
            SegmentComponentKind::EdgeLabelIndex,
            SegmentComponentKind::EdgeTripleIndex,
            SegmentComponentKind::AdjOutIndex,
            SegmentComponentKind::AdjOutPostings,
            SegmentComponentKind::AdjInIndex,
            SegmentComponentKind::AdjInPostings,
            SegmentComponentKind::TimestampIndex,
            SegmentComponentKind::EdgeWeightIndex,
            SegmentComponentKind::EdgeUpdatedAtIndex,
            SegmentComponentKind::EdgeValidFromIndex,
            SegmentComponentKind::EdgeValidToIndex,
        ] {
            assert_component_handle_is_packed(&out_dir, kind);
        }
        let node_meta = reader.node_meta_at(0).unwrap();
        let node_start = node_meta.data_offset as usize;
        let node_end = node_start + node_meta.data_len as usize;
        assert!(node_end <= reader.raw_nodes_mmap().len());
        assert_eq!(reader.raw_nodes_mmap()[node_start], 1);
        assert_eq!(
            u32::from_le_bytes(
                reader.raw_nodes_mmap()[node_start + 1..node_start + 5]
                    .try_into()
                    .unwrap()
            ),
            node_meta.label_ids.single_label_id()
        );
        let (_, edge_data_offset, edge_data_len, edge_from, edge_to, edge_label_id, ..) =
            reader.edge_meta_at(0).unwrap();
        let edge_start = edge_data_offset as usize;
        let edge_end = edge_start + edge_data_len as usize;
        assert!(edge_end <= reader.raw_edges_mmap().len());
        assert_eq!(
            u64::from_le_bytes(
                reader.raw_edges_mmap()[edge_start..edge_start + 8]
                    .try_into()
                    .unwrap()
            ),
            edge_from
        );
        assert_eq!(
            u64::from_le_bytes(
                reader.raw_edges_mmap()[edge_start + 8..edge_start + 16]
                    .try_into()
                    .unwrap()
            ),
            edge_to
        );
        assert_eq!(
            u32::from_le_bytes(
                reader.raw_edges_mmap()[edge_start + 16..edge_start + 20]
                    .try_into()
                    .unwrap()
            ),
            edge_label_id
        );
    }

    #[test]
    fn test_v10_flush_writes_segment_manifest_and_root_identity() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 3);

        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();
        assert_eq!(info.segment_format_version, SEGMENT_FORMAT_VERSION);
        assert_ne!(info.segment_data_id, [0; 32]);

        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        assert_eq!(manifest.segment_format_version, SEGMENT_FORMAT_VERSION);
        assert_eq!(manifest.segment_id, 1);
        assert_eq!(manifest.segment_data_id, info.segment_data_id);
        assert_eq!(manifest.node_count, 2);
        assert_eq!(manifest.edge_count, 1);

        for kind in [
            SegmentComponentKind::NodeRecords,
            SegmentComponentKind::EdgeRecords,
            SegmentComponentKind::NodeMetadata,
            SegmentComponentKind::EdgeMetadata,
            SegmentComponentKind::Tombstones,
            SegmentComponentKind::KeyIndex,
            SegmentComponentKind::NodeLabelIndex,
            SegmentComponentKind::EdgeLabelIndex,
            SegmentComponentKind::EdgeTripleIndex,
            SegmentComponentKind::AdjOutIndex,
            SegmentComponentKind::AdjOutPostings,
            SegmentComponentKind::AdjInIndex,
            SegmentComponentKind::AdjInPostings,
            SegmentComponentKind::TimestampIndex,
        ] {
            assert!(
                manifest.components.iter().any(|record| record.kind == kind),
                "missing required component {:?}",
                kind
            );
        }

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(reader.get_node(1).unwrap().is_some());
        assert!(reader.get_edge(10).unwrap().is_some());
    }

    #[test]
    fn test_finalize_compaction_segment_rejects_missing_required_component_before_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");
        let out_dir = dir.path().join("seg_0002");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment(&source_dir, 1, &mt, None).unwrap();

        let manifest_bytes =
            fs::read(source_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest = decode_manifest_envelope(&manifest_bytes).unwrap();
        let mut records = manifest.components.clone();
        records.retain(|record| record.kind != SegmentComponentKind::KeyIndex);

        fs::create_dir_all(&out_dir).unwrap();
        let err = finalize_compaction_segment(
            &out_dir,
            2,
            manifest.node_count,
            manifest.edge_count,
            records,
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains(SEGMENT_COMPONENT_MANIFEST_FILENAME));
        assert!(message.contains("KeyIndex"));
        assert!(!out_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME).exists());
    }

    #[test]
    fn test_finalize_segment_rejects_external_vector_source_truth_in_packed_output() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");
        let out_dir = dir.path().join("seg_0002");

        let mt = Memtable::new();
        let mut node = make_node(1, 1, "alice");
        node.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        mt.apply_op(&WalOp::UpsertNode(node), 1);
        let dense_config = DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        };
        write_segment(&source_dir, 1, &mt, Some(&dense_config)).unwrap();

        let manifest_bytes =
            fs::read(source_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest = decode_manifest_envelope(&manifest_bytes).unwrap();
        let mut records = manifest.components.clone();
        let vector_meta = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::NodeVectorMetadata)
            .expect("vector metadata record should exist");
        vector_meta.handle = ComponentHandleV1::ExternalFile {
            relative_path: "external-node-vector-meta.dat".to_string(),
            payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
            payload_len: vector_meta.payload_len,
        };

        fs::create_dir_all(&out_dir).unwrap();
        let err = finalize_compaction_segment(
            &out_dir,
            2,
            manifest.node_count,
            manifest.edge_count,
            records,
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains(SEGMENT_COMPONENT_MANIFEST_FILENAME));
        assert!(message.contains("NodeVectorMetadata"));
        assert!(message.contains("is not packed"));
        assert!(!out_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME).exists());
    }

    #[test]
    fn test_finalize_segment_rejects_external_packed_core_optional_in_packed_output() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");
        let out_dir = dir.path().join("seg_0002");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 1);
        write_segment(&source_dir, 1, &mt, None).unwrap();

        let manifest_bytes =
            fs::read(source_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest = decode_manifest_envelope(&manifest_bytes).unwrap();
        let mut records = manifest.components.clone();
        let edge_weight = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeWeightIndex)
            .expect("edge weight index record should exist");
        edge_weight.handle = ComponentHandleV1::ExternalFile {
            relative_path: "external-edge-weight-index.dat".to_string(),
            payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
            payload_len: edge_weight.payload_len,
        };

        fs::create_dir_all(&out_dir).unwrap();
        let err = finalize_compaction_segment(
            &out_dir,
            2,
            manifest.node_count,
            manifest.edge_count,
            records,
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains(SEGMENT_COMPONENT_MANIFEST_FILENAME));
        assert!(message.contains("EdgeWeightIndex"));
        assert!(message.contains("not allowed as an external file"));
        assert!(!out_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME).exists());
    }

    #[test]
    fn test_optional_refresh_rejects_packed_core_component_kinds() {
        let dir = tempfile::tempdir().unwrap();
        let err = refresh_optional_component_with_writer(
            dir.path(),
            SegmentComponentKind::EdgeWeightIndex,
            "not-used.dat",
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::MetadataScan,
            },
            ComponentTrustClass::OptionalExactAccelerator,
            Vec::new(),
            component_fingerprint("test.bad_optional_refresh", &[]),
            |writer| {
                writer.write_all(b"should not be written")?;
                Ok(())
            },
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("EdgeWeightIndex"), "got: {message}");
        assert!(message.contains("not eligible"), "got: {message}");
        assert!(
            fs::read_dir(dir.path()).unwrap().next().is_none(),
            "guard should reject before creating refresh files"
        );
    }

    #[test]
    fn test_finalize_segment_rejects_external_required_core_in_packed_output() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");
        let out_dir = dir.path().join("seg_0002");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment(&source_dir, 1, &mt, None).unwrap();

        let manifest_bytes =
            fs::read(source_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest = decode_manifest_envelope(&manifest_bytes).unwrap();
        let mut records = manifest.components.clone();
        let node_records = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .expect("node records component should exist");
        node_records.handle = ComponentHandleV1::ExternalFile {
            relative_path: "external-node-records.dat".to_string(),
            payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
            payload_len: node_records.payload_len,
        };

        fs::create_dir_all(&out_dir).unwrap();
        let err = finalize_compaction_segment(
            &out_dir,
            2,
            manifest.node_count,
            manifest.edge_count,
            records,
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains(SEGMENT_COMPONENT_MANIFEST_FILENAME));
        assert!(message.contains("NodeRecords"));
        assert!(message.contains("is not packed"));
        assert!(!out_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME).exists());
    }

    #[test]
    fn test_finalize_segment_rejects_invalid_packed_core_contract_before_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 3);
        write_segment(&source_dir, 1, &mt, None).unwrap();

        let manifest_bytes =
            fs::read(source_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest = decode_manifest_envelope(&manifest_bytes).unwrap();
        let base_records = manifest.components.clone();

        let assert_rejected =
            |case_name: &str, records: Vec<SegmentComponentRecordV1>, expected: &str| {
                let out_dir = dir.path().join(case_name);
                fs::create_dir_all(&out_dir).unwrap();
                let err = finalize_compaction_segment(
                    &out_dir,
                    2,
                    manifest.node_count,
                    manifest.edge_count,
                    records,
                )
                .unwrap_err();
                let message = err.to_string();
                assert!(
                    message.contains(expected),
                    "expected error containing {expected:?}, got {message:?}"
                );
                assert!(!out_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME).exists());
            };

        let mut records = base_records.clone();
        let container = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::PackedSegmentContainer)
            .expect("packed container record should exist");
        let ComponentHandleV1::ExternalFile { relative_path, .. } = &mut container.handle else {
            panic!("packed container should be external");
        };
        *relative_path = "not-segment.core".to_string();
        assert_rejected(
            "bad_container_path",
            records,
            "container path must be segment.core",
        );

        let mut records = base_records.clone();
        let node_records = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .expect("node records component should exist");
        let ComponentHandleV1::PackedRange {
            container_component_id,
            ..
        } = &mut node_records.handle
        else {
            panic!("node records should be packed");
        };
        *container_component_id = [42; 32];
        assert_rejected(
            "wrong_container_id",
            records,
            "points at the wrong container",
        );

        let mut records = base_records.clone();
        let container_payload_len = records
            .iter()
            .find(|record| record.kind == SegmentComponentKind::PackedSegmentContainer)
            .and_then(|record| match &record.handle {
                ComponentHandleV1::ExternalFile { payload_len, .. } => Some(*payload_len),
                ComponentHandleV1::PackedRange { .. } => None,
            })
            .expect("packed container should have external payload length");
        let node_records = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .expect("node records component should exist");
        let ComponentHandleV1::PackedRange { offset, .. } = &mut node_records.handle else {
            panic!("node records should be packed");
        };
        *offset = container_payload_len;
        assert_rejected(
            "range_overflow",
            records,
            "exceeds segment.core payload length",
        );

        let mut records = base_records.clone();
        let node_offset = records
            .iter()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .and_then(|record| match &record.handle {
                ComponentHandleV1::PackedRange { offset, .. } => Some(*offset),
                ComponentHandleV1::ExternalFile { .. } => None,
            })
            .expect("node records should have a packed offset");
        let edge_records = records
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeRecords)
            .expect("edge records component should exist");
        let ComponentHandleV1::PackedRange { offset, .. } = &mut edge_records.handle else {
            panic!("edge records should be packed");
        };
        *offset = node_offset;
        assert_rejected("overlap", records, "overlap");
    }

    #[test]
    fn test_v10_reader_rejects_missing_local_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment(&seg_dir, 1, &mt, None).unwrap();
        fs::remove_file(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();

        let err = match SegmentReader::open_unpinned_for_test(&seg_dir, 1, None) {
            Ok(_) => panic!("open should reject missing segment manifest"),
            Err(error) => error,
        };
        assert!(err.to_string().contains("missing segment_manifest.dat"));
        assert!(err.to_string().contains("rebuild the database"));
    }

    #[test]
    fn test_v10_reader_rejects_required_component_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let mut core = fs::OpenOptions::new()
            .append(true)
            .open(seg_dir.join(PACKED_CORE_FILENAME))
            .unwrap();
        core.write_all(&[0xAA]).unwrap();
        core.sync_all().unwrap();
        drop(core);

        let err = match SegmentReader::open_unpinned_for_test(&seg_dir, 1, None) {
            Ok(_) => panic!("open should reject required component trailing bytes"),
            Err(error) => error,
        };
        assert!(err.to_string().contains("does not match file length"));
    }

    #[test]
    fn test_v10_reader_treats_optional_component_trailing_bytes_as_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 3);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let mut planner_stats = fs::OpenOptions::new()
            .append(true)
            .open(seg_dir.join(PLANNER_STATS_FILENAME))
            .unwrap();
        planner_stats.write_all(&[0xBB]).unwrap();
        planner_stats.sync_all().unwrap();
        drop(planner_stats);

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(reader.get_edge(10).unwrap().is_some());
    }

    #[test]
    fn test_v10_reader_rejects_old_format_version_in_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let manifest_path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
        let mut data = fs::read(&manifest_path).unwrap();
        data[12..16].copy_from_slice(&9u32.to_le_bytes());
        fs::write(&manifest_path, &data).unwrap();

        let err = match SegmentReader::open_unpinned_for_test(&seg_dir, 1, None) {
            Ok(_) => panic!("open should reject old segment format"),
            Err(error) => error,
        };
        assert!(
            err.to_string()
                .contains("unsupported segment manifest version 9"),
            "expected version rejection, got: {}",
            err
        );
    }

    #[test]
    fn test_v10_reader_rejects_future_format_version_in_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let manifest_path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
        let mut data = fs::read(&manifest_path).unwrap();
        data[12..16].copy_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_le_bytes());
        fs::write(&manifest_path, &data).unwrap();

        let err = match SegmentReader::open_unpinned_for_test(&seg_dir, 1, None) {
            Ok(_) => panic!("open should reject future segment format"),
            Err(error) => error,
        };
        assert!(
            err.to_string()
                .contains("unsupported segment manifest version"),
            "expected version rejection, got: {}",
            err
        );
    }

    #[test]
    fn test_write_segment_empty_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();
        assert_eq!(info.node_count, 0);
        assert_eq!(info.edge_count, 0);

        assert!(seg_dir.join(PACKED_CORE_FILENAME).exists());
        assert_only_manifested_component_files(&seg_dir);
        for kind in [
            SegmentComponentKind::EdgeWeightIndex,
            SegmentComponentKind::EdgeUpdatedAtIndex,
            SegmentComponentKind::EdgeValidFromIndex,
            SegmentComponentKind::EdgeValidToIndex,
        ] {
            let data = read_manifest_component_payload(&seg_dir, kind);
            assert_eq!(u64::from_le_bytes(data[0..8].try_into().unwrap()), 0);
        }
    }

    #[test]
    fn test_packed_core_writer_alignment_digests_and_container_patch() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer =
            PackedCoreWriter::create(dir.path(), 42, FLUSH_COMPONENT_GENERATION).unwrap();
        let (node_record, _) = writer
            .write_component(
                SegmentComponentKind::NodeRecords,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryData,
                Vec::new(),
                component_fingerprint("flush.nodes", &[]),
                |sink| {
                    sink.write_all(b"abc")?;
                    Ok(())
                },
            )
            .unwrap();
        let (edge_record, _) = writer
            .write_component(
                SegmentComponentKind::EdgeRecords,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryData,
                Vec::new(),
                component_fingerprint("flush.edges", &[]),
                |sink| {
                    sink.write_all(b"defgh")?;
                    Ok(())
                },
            )
            .unwrap();

        let records = writer.finish().unwrap();
        let container = records
            .iter()
            .find(|record| record.kind == SegmentComponentKind::PackedSegmentContainer)
            .unwrap();
        let final_node = records
            .iter()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .unwrap();
        let final_edge = records
            .iter()
            .find(|record| record.kind == SegmentComponentKind::EdgeRecords)
            .unwrap();

        assert_eq!(final_node.component_id, node_record.component_id);
        assert_eq!(final_edge.component_id, edge_record.component_id);
        assert!(matches!(
            final_node.handle,
            ComponentHandleV1::PackedRange {
                container_component_id,
                offset: 0,
                len: 3,
            } if container_component_id == container.component_id
        ));
        assert!(matches!(
            final_edge.handle,
            ComponentHandleV1::PackedRange {
                container_component_id,
                offset: 8,
                len: 5,
            } if container_component_id == container.component_id
        ));
        assert_eq!(container.payload_len, 13);

        let mut node_digest = Sha256::new();
        node_digest.update(b"abc");
        assert_eq!(
            final_node.payload_digest,
            Some(node_digest.finalize().into())
        );
        let mut edge_digest = Sha256::new();
        edge_digest.update(b"defgh");
        assert_eq!(
            final_edge.payload_digest,
            Some(edge_digest.finalize().into())
        );
        let mut container_digest = Sha256::new();
        container_digest.update(b"abc");
        container_digest.update([0u8; 5]);
        container_digest.update(b"defgh");
        assert_eq!(
            container.payload_digest,
            Some(container_digest.finalize().into())
        );

        let payload = read_payload_file(&dir.path().join(PACKED_CORE_FILENAME));
        assert_eq!(&payload[0..3], b"abc");
        assert_eq!(&payload[3..8], &[0u8; 5]);
        assert_eq!(&payload[8..13], b"defgh");
    }

    #[test]
    fn test_edge_metadata_index_weight_encoding_ordering_and_nan() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();

        let mut neg = make_edge(1, 1, 2, 10);
        neg.weight = -1.0;
        let mut neg_zero = make_edge(2, 1, 3, 10);
        neg_zero.weight = -0.0;
        let mut pos_zero = make_edge(3, 1, 4, 10);
        pos_zero.weight = 0.0;
        let mut pos = make_edge(4, 1, 5, 10);
        pos.weight = 2.0;
        let mut nan = make_edge(5, 1, 6, 10);
        nan.weight = f32::NAN;
        let mut inf = make_edge(6, 1, 7, 10);
        inf.weight = f32::INFINITY;
        let mut neg_inf = make_edge(7, 1, 8, 10);
        neg_inf.weight = f32::NEG_INFINITY;

        for edge in [neg, neg_zero, pos_zero, pos, nan, inf, neg_inf] {
            mt.apply_op(&WalOp::UpsertEdge(edge), 1);
        }
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let entries = read_weight_index_entries(&seg_dir);
        assert_eq!(entries.len(), 6);
        assert!(!entries.iter().any(|entry| entry.2 == 5));
        assert_eq!(
            crate::edge_metadata::encode_edge_weight_key(-0.0),
            crate::edge_metadata::encode_edge_weight_key(0.0)
        );
        let zero_key = crate::edge_metadata::encode_edge_weight_key(0.0).unwrap();
        assert!(entries.contains(&(10, zero_key, 2)));
        assert!(entries.contains(&(10, zero_key, 3)));
        let mut sorted = entries.clone();
        sorted.sort_unstable();
        assert_eq!(entries, sorted);
    }

    #[test]
    fn test_edge_metadata_indexes_rebuilt_from_compaction_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("seg_0001");
        let out_dir = dir.path().join("seg_0002");

        let mt = Memtable::new();
        let mut edge_a = make_edge(10, 1, 2, 5);
        edge_a.weight = -0.0;
        edge_a.updated_at = 300;
        edge_a.valid_from = 10;
        edge_a.valid_to = 100;
        let mut edge_b = make_edge(11, 2, 3, 5);
        edge_b.weight = 1.5;
        edge_b.updated_at = 400;
        edge_b.valid_from = 20;
        edge_b.valid_to = 200;
        mt.apply_op(&WalOp::UpsertEdge(edge_a), 1);
        mt.apply_op(&WalOp::UpsertEdge(edge_b), 2);

        write_segment(&source_dir, 1, &mt, None).unwrap();
        let source = Arc::new(SegmentReader::open_unpinned_for_test(&source_dir, 1, None).unwrap());
        compact_copy_segment_for_test(source, &out_dir, 2, &[]);

        assert_eq!(
            read_weight_index_entries(&source_dir),
            read_weight_index_entries(&out_dir)
        );
        assert_eq!(
            read_i64_index_entries(&source_dir, SegmentComponentKind::EdgeUpdatedAtIndex),
            read_i64_index_entries(&out_dir, SegmentComponentKind::EdgeUpdatedAtIndex)
        );
        assert_eq!(
            read_i64_index_entries(&source_dir, SegmentComponentKind::EdgeValidFromIndex),
            read_i64_index_entries(&out_dir, SegmentComponentKind::EdgeValidFromIndex)
        );
        assert_eq!(
            read_i64_index_entries(&source_dir, SegmentComponentKind::EdgeValidToIndex),
            read_i64_index_entries(&out_dir, SegmentComponentKind::EdgeValidToIndex)
        );
    }

    #[test]
    fn test_node_vector_prepare_emit_matches_packed_payloads() {
        let mut nodes = NodeIdMap::default();

        let mut node_1 = make_node(1, 1, "alice");
        node_1.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        node_1.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        nodes.insert(node_1.id, node_1);

        let mut node_2 = make_node(2, 1, "bob");
        node_2.sparse_vector = Some(vec![(3, 2.5)]);
        nodes.insert(node_2.id, node_2);

        let mut node_3 = make_node(3, 2, "carol");
        node_3.dense_vector = Some(vec![0.4, 0.5, 0.6]);
        nodes.insert(node_3.id, node_3);

        let dense_config = DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        };
        let ops = nodes
            .values()
            .cloned()
            .map(WalOp::UpsertNode)
            .collect::<Vec<_>>();
        let (_dir, seg_dir) = write_packed_segment_from_ops(ops, Some(&dense_config));
        let node_records =
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeRecords);
        let node_data = read_record_spans_from_payload(&node_records);
        let plan = prepare_node_vector_source_plan(&node_data, &nodes).unwrap();

        let mut direct_meta = Vec::new();
        write_node_vector_meta_payload(&mut direct_meta, &plan).unwrap();
        let mut direct_dense = Vec::new();
        write_node_dense_vector_blob_payload(&mut direct_dense, &plan, &nodes).unwrap();
        let mut direct_sparse = Vec::new();
        write_node_sparse_vector_blob_payload(&mut direct_sparse, &plan, &nodes).unwrap();

        assert_eq!(
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeVectorMetadata),
            direct_meta
        );
        assert_eq!(
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeDenseVectorBlob),
            direct_dense
        );
        assert_eq!(
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeSparseVectorBlob),
            direct_sparse
        );

        assert_eq!(plan.dense_points.len(), 2);
        assert_eq!(plan.dense_points[0].node_id, 1);
        assert_eq!(plan.dense_points[1].node_id, 3);
    }

    #[test]
    fn test_write_segment_with_vectors_writes_packed_vector_components() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut node = make_node(1, 1, "alice");
        node.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 0);

        let dense_config = DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        };
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        assert_only_manifested_component_files(&seg_dir);
        assert!(seg_dir
            .join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME)
            .exists());
        assert!(seg_dir
            .join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME)
            .exists());
        assert!(seg_dir
            .join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)
            .exists());
        assert!(seg_dir
            .join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)
            .exists());

        let manifest_bytes = fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        let manifest =
            crate::segment_components::decode_manifest_envelope(&manifest_bytes).unwrap();
        let record_for = |kind: SegmentComponentKind| {
            manifest
                .components
                .iter()
                .find(|record| record.kind == kind)
                .unwrap_or_else(|| panic!("missing component {:?}", kind))
        };
        let node_records = record_for(SegmentComponentKind::NodeRecords);
        let node_meta = record_for(SegmentComponentKind::NodeMetadata);
        let tombstones = record_for(SegmentComponentKind::Tombstones);
        let node_source = crate::segment_components::digest_source_group(
            SegmentSourceGroupKind::NodeSource,
            &[
                node_records.component_id,
                node_meta.component_id,
                tombstones.component_id,
            ],
        );
        let vector_meta = record_for(SegmentComponentKind::NodeVectorMetadata);
        assert_component_handle_is_packed(&seg_dir, SegmentComponentKind::NodeVectorMetadata);
        assert_component_handle_is_packed(&seg_dir, SegmentComponentKind::NodeDenseVectorBlob);
        assert_component_handle_is_packed(&seg_dir, SegmentComponentKind::NodeSparseVectorBlob);
        assert_component_handle_is_external(&seg_dir, SegmentComponentKind::DenseHnswMetadata);
        assert_component_handle_is_external(&seg_dir, SegmentComponentKind::DenseHnswGraph);
        assert_component_handle_is_external(&seg_dir, SegmentComponentKind::SparsePostingIndex);
        assert_component_handle_is_external(&seg_dir, SegmentComponentKind::SparsePostings);
        assert!(vector_meta.dependencies.iter().any(|dependency| {
            matches!(
                dependency,
                ComponentDependencyV1::SourceGroup { group, group_id }
                    if *group == SegmentSourceGroupKind::NodeSource && *group_id == node_source
            )
        }));
        for kind in [
            SegmentComponentKind::NodeDenseVectorBlob,
            SegmentComponentKind::NodeSparseVectorBlob,
        ] {
            let record = record_for(kind);
            assert!(record.dependencies.iter().any(|dependency| {
                matches!(
                    dependency,
                    ComponentDependencyV1::SourceGroup { group, group_id }
                        if *group == SegmentSourceGroupKind::NodeSource && *group_id == node_source
                )
            }));
            assert!(record.dependencies.iter().any(|dependency| {
                matches!(
                    dependency,
                    ComponentDependencyV1::SourceComponent { kind, component_id }
                        if *kind == SegmentComponentKind::NodeVectorMetadata
                            && *component_id == vector_meta.component_id
                )
            }));
        }
    }

    #[test]
    fn test_write_segment_with_sparse_only_vectors_skips_dense_hnsw() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);

        let dense_config = DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        };
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        assert_only_manifested_component_files(&seg_dir);
        assert_component_handle_is_packed(&seg_dir, SegmentComponentKind::NodeVectorMetadata);
        assert_component_handle_is_packed(&seg_dir, SegmentComponentKind::NodeSparseVectorBlob);
        assert!(seg_dir
            .join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME)
            .exists());
        assert!(seg_dir
            .join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME)
            .exists());
        assert!(!seg_dir
            .join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)
            .exists());
        assert!(!seg_dir
            .join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)
            .exists());
    }

    #[test]
    fn test_sparse_posting_rebuild_rejects_overflowing_source_range() {
        let mut node = make_node(1, 1, "alice");
        node.sparse_vector = Some(vec![(2, 1.5)]);
        let (_source_dir_guard, source_dir) =
            write_packed_segment_from_ops(vec![WalOp::UpsertNode(node)], None);
        let source = Arc::new(SegmentReader::open_unpinned_for_test(&source_dir, 1, None).unwrap());
        let out_dir = tempfile::tempdir().unwrap();
        let metas = vec![CompactNodeMeta {
            node_id: 1,
            new_data_offset: 0,
            data_len: 0,
            label_ids: NodeLabelSet::single(1).unwrap(),
            updated_at: 0,
            weight: 1.0,
            key_len: 0,
            dense_vector_offset: 0,
            dense_vector_len: 0,
            sparse_vector_offset: u64::MAX,
            sparse_vector_len: 1,
            src_seg_idx: 0,
            src_data_offset: 0,
            last_write_seq: 0,
        }];
        let source_groups = SegmentComponentSourceGroups {
            node_source: [0; 32],
            edge_source: [0; 32],
            node_property_content_source: [0; 32],
            node_property_hash_source: [0; 32],
            edge_metadata_source: [0; 32],
            degree_source: [0; 32],
            dense_vector_source: [0; 32],
            sparse_vector_source: [0; 32],
            segment_data_id: [0; 32],
        };

        let err = write_sparse_posting_index_from_meta(
            out_dir.path(),
            2,
            &[source],
            &metas,
            source_groups,
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("sparse vector") && err.to_string().contains("overflow"),
            "expected sparse vector overflow error, got: {err}"
        );
    }

    #[test]
    fn test_segment_dir_paths() {
        let db = Path::new("/tmp/mydb");
        assert_eq!(
            segment_dir(db, 1),
            PathBuf::from("/tmp/mydb/segments/seg_0001")
        );
        assert_eq!(
            segment_dir(db, 42),
            PathBuf::from("/tmp/mydb/segments/seg_0042")
        );
        assert_eq!(
            segment_tmp_dir(db, 1),
            PathBuf::from("/tmp/mydb/segments/seg_0001.tmp")
        );
    }

    #[test]
    fn test_write_nodes_with_properties() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![WalOp::UpsertNode(make_node_with_props(1, 1, "alice"))],
            None,
        );

        let data = read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeRecords);
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 1);

        // Offset should point to valid data. The id is NOT in the record.
        // Layout: label_count(1) + label_ids + key_len(2) + key + timestamps(16) + weight(4) + props_len(4) + props
        let offset = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;
        assert_eq!(data[offset], 1);
        let label_id = u32::from_le_bytes(data[offset + 1..offset + 5].try_into().unwrap());
        assert_eq!(label_id, 1);

        // Properties should be serialized
        let key_len = u16::from_le_bytes(data[offset + 5..offset + 7].try_into().unwrap()) as usize;
        let props_len_offset = offset + 7 + key_len + 8 + 8 + 4; // skip key + timestamps + weight
        let props_len = u32::from_le_bytes(
            data[props_len_offset..props_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(props_len > 0); // Properties should be non-empty
    }

    // --- Varint and sentinel encoding roundtrip tests ---

    #[test]
    fn test_varint_roundtrip_zero() {
        use crate::segment_reader::tests::read_varint_at_pub;
        let mut buf = Vec::new();
        write_varint_to_vec(&mut buf, 0);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0);
        let (val, len) = read_varint_at_pub(&buf, 0);
        assert_eq!(val, 0);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_varint_roundtrip_single_byte_max() {
        use crate::segment_reader::tests::read_varint_at_pub;
        let mut buf = Vec::new();
        write_varint_to_vec(&mut buf, 127);
        assert_eq!(buf.len(), 1);
        let (val, len) = read_varint_at_pub(&buf, 0);
        assert_eq!(val, 127);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_varint_roundtrip_two_byte_boundary() {
        use crate::segment_reader::tests::read_varint_at_pub;
        let mut buf = Vec::new();
        write_varint_to_vec(&mut buf, 128);
        assert_eq!(buf.len(), 2);
        let (val, len) = read_varint_at_pub(&buf, 0);
        assert_eq!(val, 128);
        assert_eq!(len, 2);
    }

    #[test]
    fn test_varint_roundtrip_u64_max() {
        use crate::segment_reader::tests::read_varint_at_pub;
        let mut buf = Vec::new();
        write_varint_to_vec(&mut buf, u64::MAX);
        assert_eq!(buf.len(), 10); // ceil(64/7) = 10 bytes
        let (val, len) = read_varint_at_pub(&buf, 0);
        assert_eq!(val, u64::MAX);
        assert_eq!(len, 10);
    }

    #[test]
    fn test_valid_to_sentinel_roundtrip() {
        // i64::MAX encodes as 0
        let vt_max_enc = if i64::MAX == i64::MAX {
            0u64
        } else {
            i64::MAX as u64 + 1
        };
        assert_eq!(vt_max_enc, 0);
        let vt_max_dec = if vt_max_enc == 0 {
            i64::MAX
        } else {
            (vt_max_enc - 1) as i64
        };
        assert_eq!(vt_max_dec, i64::MAX);

        // valid_to = 0 encodes as 1
        let vt_zero: i64 = 0;
        let vt_zero_enc = if vt_zero == i64::MAX {
            0u64
        } else {
            vt_zero as u64 + 1
        };
        assert_eq!(vt_zero_enc, 1);
        let vt_zero_dec = if vt_zero_enc == 0 {
            i64::MAX
        } else {
            (vt_zero_enc - 1) as i64
        };
        assert_eq!(vt_zero_dec, 0);

        // valid_to = 1000 encodes as 1001
        let vt_mid: i64 = 1000;
        let vt_mid_enc = if vt_mid == i64::MAX {
            0u64
        } else {
            vt_mid as u64 + 1
        };
        assert_eq!(vt_mid_enc, 1001);
        let vt_mid_dec = if vt_mid_enc == 0 {
            i64::MAX
        } else {
            (vt_mid_enc - 1) as i64
        };
        assert_eq!(vt_mid_dec, 1000);
    }

    // --- Packed metadata payload tests ---

    #[test]
    fn test_packed_node_metadata_payload_roundtrip() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::UpsertNode(make_node_with_props(1, 1, "alice")),
                WalOp::UpsertNode(make_node(2, 2, "bob")),
            ],
            None,
        );
        let node_records =
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeRecords);
        let node_data = read_record_spans_from_payload(&node_records);
        assert_eq!(node_data.len(), 2);
        // Sorted by id
        assert_eq!(node_data[0].0, 1);
        assert_eq!(node_data[1].0, 2);

        let meta = read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeMetadata);
        let count = u64::from_le_bytes(meta[0..8].try_into().unwrap());
        assert_eq!(count, 2);
        assert_eq!(
            u16::from_le_bytes(meta[8..10].try_into().unwrap()),
            NODE_META_FIXED_ENTRY_SIZE
        );
        assert_eq!(
            u16::from_le_bytes(meta[10..12].try_into().unwrap()),
            NODE_META_LABEL_OFFSET_ENTRY_SIZE
        );
        let fixed_entries_offset = u64::from_le_bytes(meta[16..24].try_into().unwrap()) as usize;
        let label_offsets_offset = u64::from_le_bytes(meta[24..32].try_into().unwrap()) as usize;
        let label_ids_offset = u64::from_le_bytes(meta[32..40].try_into().unwrap()) as usize;
        let label_id_count = u64::from_le_bytes(meta[40..48].try_into().unwrap());
        assert_eq!(fixed_entries_offset, NODE_META_HEADER_SIZE as usize);
        assert_eq!(
            label_offsets_offset,
            fixed_entries_offset + 2 * NODE_META_FIXED_ENTRY_SIZE as usize
        );
        assert_eq!(
            label_ids_offset,
            label_offsets_offset + 3 * NODE_META_LABEL_OFFSET_ENTRY_SIZE as usize
        );
        assert_eq!(label_id_count, 2);

        // Verify first entry fields (node_id=1)
        let off = fixed_entries_offset;
        let nid = u64::from_le_bytes(meta[off..off + 8].try_into().unwrap());
        assert_eq!(nid, 1);
        let data_offset = u64::from_le_bytes(meta[off + 8..off + 16].try_into().unwrap());
        assert_eq!(data_offset, node_data[0].1);
        let data_len = u32::from_le_bytes(meta[off + 16..off + 20].try_into().unwrap());
        assert_eq!(data_len, node_data[0].2);
        let updated_at = i64::from_le_bytes(meta[off + 20..off + 28].try_into().unwrap());
        assert_eq!(updated_at, 1001); // make_node_with_props uses updated_at=1001
        let key_len = u16::from_le_bytes(meta[off + 32..off + 34].try_into().unwrap());
        assert_eq!(key_len, 5); // "alice"
        let label_offset = label_offsets_offset;
        assert_eq!(
            u64::from_le_bytes(meta[label_offset..label_offset + 8].try_into().unwrap()),
            0
        );
        assert_eq!(
            u64::from_le_bytes(
                meta[label_offset + 8..label_offset + 16]
                    .try_into()
                    .unwrap()
            ),
            1
        );
        assert_eq!(
            u32::from_le_bytes(
                meta[label_ids_offset..label_ids_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            1
        );

        // Second entry (node_id=2)
        let off2 = fixed_entries_offset + NODE_META_FIXED_ENTRY_SIZE as usize;
        let nid2 = u64::from_le_bytes(meta[off2..off2 + 8].try_into().unwrap());
        assert_eq!(nid2, 2);
        let label_offset2 = label_offsets_offset + NODE_META_LABEL_OFFSET_ENTRY_SIZE as usize;
        assert_eq!(
            u64::from_le_bytes(meta[label_offset2..label_offset2 + 8].try_into().unwrap()),
            1
        );
        assert_eq!(
            u64::from_le_bytes(
                meta[label_offset2 + 8..label_offset2 + 16]
                    .try_into()
                    .unwrap()
            ),
            2
        );
        assert_eq!(
            u32::from_le_bytes(
                meta[label_ids_offset + 4..label_ids_offset + 8]
                    .try_into()
                    .unwrap()
            ),
            2
        );
        assert!(read_segment_component_manifest(&seg_dir)
            .unwrap()
            .components
            .iter()
            .all(|record| record.kind != SegmentComponentKind::NodePropertyHashMetadata));
    }

    #[test]
    fn test_packed_edge_metadata_payload_roundtrip() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(
            vec![
                WalOp::UpsertEdge(make_edge(10, 1, 2, 5)),
                WalOp::UpsertEdge(make_edge(20, 3, 4, 7)),
            ],
            None,
        );
        let edge_records =
            read_manifest_component_payload(&seg_dir, SegmentComponentKind::EdgeRecords);
        let edge_data = read_record_spans_from_payload(&edge_records);
        assert_eq!(edge_data.len(), 2);

        let meta = read_manifest_component_payload(&seg_dir, SegmentComponentKind::EdgeMetadata);
        let count = u64::from_le_bytes(meta[0..8].try_into().unwrap());
        assert_eq!(count, 2);

        // Verify first entry (edge_id=10)
        let off = 8;
        let eid = u64::from_le_bytes(meta[off..off + 8].try_into().unwrap());
        assert_eq!(eid, 10);
        let data_offset = u64::from_le_bytes(meta[off + 8..off + 16].try_into().unwrap());
        assert_eq!(data_offset, edge_data[0].1);
        let data_len = u32::from_le_bytes(meta[off + 16..off + 20].try_into().unwrap());
        assert_eq!(data_len, edge_data[0].2);
        let from = u64::from_le_bytes(meta[off + 20..off + 28].try_into().unwrap());
        assert_eq!(from, 1);
        let to = u64::from_le_bytes(meta[off + 28..off + 36].try_into().unwrap());
        assert_eq!(to, 2);
        let label_id = u32::from_le_bytes(meta[off + 36..off + 40].try_into().unwrap());
        assert_eq!(label_id, 5);
        let valid_to = i64::from_le_bytes(meta[off + 60..off + 68].try_into().unwrap());
        assert_eq!(valid_to, i64::MAX);
    }

    #[test]
    fn test_metadata_payloads_empty() {
        let (_dir, seg_dir) = write_packed_segment_from_ops(Vec::new(), None);

        let meta = read_manifest_component_payload(&seg_dir, SegmentComponentKind::NodeMetadata);
        assert_eq!(u64::from_le_bytes(meta[0..8].try_into().unwrap()), 0);

        let emeta = read_manifest_component_payload(&seg_dir, SegmentComponentKind::EdgeMetadata);
        assert_eq!(u64::from_le_bytes(emeta[0..8].try_into().unwrap()), 0);
        assert!(read_segment_component_manifest(&seg_dir)
            .unwrap()
            .components
            .iter()
            .all(|record| record.kind != SegmentComponentKind::NodePropertyHashMetadata));
    }

    #[test]
    fn test_write_segment_with_declared_equality_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut red_props = BTreeMap::new();
        red_props.insert("color".to_string(), PropValue::String("red".to_string()));
        let mut green_props = BTreeMap::new();
        green_props.insert("color".to_string(), PropValue::String("green".to_string()));

        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props: red_props.clone(),
                created_at: 1000,
                updated_at: 1001,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            0,
        );
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 2,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "berry".to_string(),
                props: red_props,
                created_at: 1000,
                updated_at: 1001,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            0,
        );
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 3,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "lime".to_string(),
                props: green_props,
                created_at: 1000,
                updated_at: 1001,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            0,
        );

        let entry = SecondaryIndexManifestEntry {
            index_id: 7,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        mt.register_secondary_index(&entry);

        let info = write_segment_with_secondary_indexes(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&entry),
        )
        .unwrap();

        assert_no_legacy_property_components(&seg_dir);
        assert!(node_prop_eq_sidecar_path(&seg_dir, entry.index_id).exists());
        let manifest = crate::segment_components::decode_manifest_envelope(
            &fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap(),
        )
        .unwrap();
        assert!(manifest.components.iter().any(|record| {
            record.kind
                == SegmentComponentKind::NodePropertyEqualityIndex {
                    index_id: entry.index_id,
                }
        }));

        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let green_hash = hash_prop_equality_key(&PropValue::String("green".to_string()));

        let mut reds = reader
            .find_nodes_by_secondary_eq_index(entry.index_id, red_hash)
            .unwrap();
        reds.sort_unstable();
        assert_eq!(reds, vec![1, 2]);
        assert_eq!(
            reader
                .find_nodes_by_secondary_eq_index(entry.index_id, green_hash)
                .unwrap(),
            vec![3]
        );
    }

    #[test]
    fn test_write_segment_with_declared_edge_property_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut red_props = BTreeMap::new();
        red_props.insert("color".to_string(), PropValue::String("red".to_string()));
        red_props.insert("score".to_string(), PropValue::Int(10));
        let mut blue_props = BTreeMap::new();
        blue_props.insert("color".to_string(), PropValue::String("blue".to_string()));
        blue_props.insert("score".to_string(), PropValue::Int(20));
        let mut ignored_props = BTreeMap::new();
        ignored_props.insert("color".to_string(), PropValue::String("red".to_string()));
        ignored_props.insert("score".to_string(), PropValue::Int(30));

        let mut red_edge = make_edge(10, 1, 2, 1);
        red_edge.props = red_props;
        let mut blue_edge = make_edge(11, 1, 3, 1);
        blue_edge.props = blue_props;
        let mut ignored_edge = make_edge(12, 1, 4, 2);
        ignored_edge.props = ignored_props;
        mt.apply_op(&WalOp::UpsertEdge(red_edge), 1);
        mt.apply_op(&WalOp::UpsertEdge(blue_edge), 2);
        mt.apply_op(&WalOp::UpsertEdge(ignored_edge), 3);

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 17,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 18,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];

        let info = write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();

        assert!(edge_prop_eq_sidecar_path(&seg_dir, eq_entry.index_id).exists());
        assert!(edge_prop_range_sidecar_path(&seg_dir, range_entry.index_id).exists());
        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();
        assert_eq!(
            reader.optional_component_availability(
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: eq_entry.index_id,
                }
            ),
            crate::segment_components::ComponentAvailability::Available
        );
        assert_eq!(
            reader.optional_component_availability(SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: range_entry.index_id,
            }),
            crate::segment_components::ComponentAvailability::Available
        );
        let manifest = read_segment_component_manifest(&seg_dir).unwrap();
        let eq_record = manifest
            .components
            .iter()
            .find(|record| {
                record.kind
                    == SegmentComponentKind::EdgePropertyEqualityIndex {
                        index_id: eq_entry.index_id,
                    }
            })
            .expect("edge equality sidecar record");
        assert!(matches!(
            &eq_record.handle,
            ComponentHandleV1::ExternalFile { .. }
        ));
        assert_eq!(
            eq_record.build_fingerprint,
            edge_property_equality_component_fingerprint(eq_entry.index_id)
        );
        assert!(eq_record.dependencies.iter().any(|dependency| {
            matches!(
                dependency,
                ComponentDependencyV1::SourceGroup { group, .. }
                    if *group == SegmentSourceGroupKind::EdgeSource
            )
        }));
        assert!(eq_record.dependencies.iter().any(|dependency| {
            matches!(
                dependency,
                ComponentDependencyV1::SecondaryIndexDeclaration { index_id, target_kind, .. }
                    if *index_id == eq_entry.index_id
                        && *target_kind
                            == crate::segment_components::SecondaryIndexTargetKindForComponents::Edge
            )
        }));
        assert!(manifest.components.iter().any(|record| {
            record.kind
                == SegmentComponentKind::EdgePropertyRangeIndex {
                    index_id: range_entry.index_id,
                }
        }));
        let maintained =
            maintained_secondary_index_ids_from_component_records(&manifest.components, &indexes);
        assert!(maintained.equality_index_ids.contains(&eq_entry.index_id));
        assert!(maintained.range_index_ids.contains(&range_entry.index_id));

        let eq_payload = read_manifest_component_payload(
            &seg_dir,
            SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: eq_entry.index_id,
            },
        );
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let blue_hash = hash_prop_equality_key(&PropValue::String("blue".to_string()));
        let eq_count = u64::from_le_bytes(eq_payload[0..8].try_into().unwrap()) as usize;
        let mut eq_groups = BTreeMap::new();
        for index in 0..eq_count {
            let entry_off = 8 + index * 20;
            let value_hash =
                u64::from_le_bytes(eq_payload[entry_off..entry_off + 8].try_into().unwrap());
            let ids_offset = u64::from_le_bytes(
                eq_payload[entry_off + 8..entry_off + 16]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let id_count = u32::from_le_bytes(
                eq_payload[entry_off + 16..entry_off + 20]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let ids = (0..id_count)
                .map(|id_index| {
                    let off = ids_offset + id_index * 8;
                    u64::from_le_bytes(eq_payload[off..off + 8].try_into().unwrap())
                })
                .collect::<Vec<_>>();
            eq_groups.insert(value_hash, ids);
        }
        assert_eq!(eq_groups.get(&red_hash), Some(&vec![10]));
        assert_eq!(eq_groups.get(&blue_hash), Some(&vec![11]));

        let range_payload = read_manifest_component_payload(
            &seg_dir,
            SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: range_entry.index_id,
            },
        );
        let range_count = u64::from_le_bytes(range_payload[0..8].try_into().unwrap()) as usize;
        let range_entries = (0..range_count)
            .map(|index| {
                let off = 8 + index * 32;
                (
                    NumericRangeSortKey::from_sidecar_bytes(
                        range_payload[off..off + NUMERIC_RANGE_KEY_BYTES]
                            .try_into()
                            .unwrap(),
                    )
                    .unwrap(),
                    u64::from_le_bytes(
                        range_payload
                            [off + NUMERIC_RANGE_KEY_BYTES..off + NUMERIC_RANGE_KEY_BYTES + 8]
                            .try_into()
                            .unwrap(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            range_entries,
            vec![
                (
                    numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap(),
                    10
                ),
                (
                    numeric_range_sort_key_for_value(&PropValue::Int(20)).unwrap(),
                    11
                )
            ]
        );
    }

    #[test]
    fn test_compaction_rebuilds_declared_edge_property_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let source_seg = dir.path().join("seg_0001");
        let compact_seg = dir.path().join("seg_0002");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 3);

        let mut red_props = BTreeMap::new();
        red_props.insert("color".to_string(), PropValue::String("red".to_string()));
        red_props.insert("score".to_string(), PropValue::Int(10));
        let mut blue_props = BTreeMap::new();
        blue_props.insert("color".to_string(), PropValue::String("blue".to_string()));
        blue_props.insert("score".to_string(), PropValue::Int(20));
        let mut ignored_props = BTreeMap::new();
        ignored_props.insert("color".to_string(), PropValue::String("red".to_string()));
        ignored_props.insert("score".to_string(), PropValue::Int(30));

        let mut red_edge = make_edge(10, 1, 2, 1);
        red_edge.props = red_props;
        let mut blue_edge = make_edge(11, 1, 3, 1);
        blue_edge.props = blue_props;
        let mut ignored_edge = make_edge(12, 1, 3, 2);
        ignored_edge.props = ignored_props;
        mt.apply_op(&WalOp::UpsertEdge(red_edge), 4);
        mt.apply_op(&WalOp::UpsertEdge(blue_edge), 5);
        mt.apply_op(&WalOp::UpsertEdge(ignored_edge), 6);

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 117,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 118,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];

        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let compact_reader = compact_copy_segment_for_test(source, &compact_seg, 2, &indexes);

        assert_eq!(
            compact_reader.optional_component_availability(
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: eq_entry.index_id,
                }
            ),
            crate::segment_components::ComponentAvailability::Available
        );
        assert_eq!(
            compact_reader.optional_component_availability(
                SegmentComponentKind::EdgePropertyRangeIndex {
                    index_id: range_entry.index_id,
                }
            ),
            crate::segment_components::ComponentAvailability::Available
        );

        let manifest = read_segment_component_manifest(&compact_seg).unwrap();
        let eq_record = manifest
            .components
            .iter()
            .find(|record| {
                record.kind
                    == SegmentComponentKind::EdgePropertyEqualityIndex {
                        index_id: eq_entry.index_id,
                    }
            })
            .expect("compacted edge equality sidecar record");
        assert_eq!(
            eq_record.build_fingerprint,
            edge_property_equality_component_fingerprint(eq_entry.index_id)
        );
        assert!(eq_record.dependencies.iter().any(|dependency| {
            matches!(
                dependency,
                ComponentDependencyV1::SourceGroup { group, .. }
                    if *group == SegmentSourceGroupKind::EdgeSource
            )
        }));

        let range_record = manifest
            .components
            .iter()
            .find(|record| {
                record.kind
                    == SegmentComponentKind::EdgePropertyRangeIndex {
                        index_id: range_entry.index_id,
                    }
            })
            .expect("compacted edge range sidecar record");
        assert_eq!(
            range_record.build_fingerprint,
            edge_property_range_component_fingerprint(range_entry.index_id)
        );

        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let blue_hash = hash_prop_equality_key(&PropValue::String("blue".to_string()));
        let groups = read_secondary_eq_groups(
            &compact_seg,
            SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: eq_entry.index_id,
            },
        );
        assert_eq!(groups.get(&red_hash), Some(&vec![10]));
        assert_eq!(groups.get(&blue_hash), Some(&vec![11]));

        let range_entries = read_secondary_range_entries(
            &compact_seg,
            SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: range_entry.index_id,
            },
        );
        assert_eq!(
            range_entries,
            vec![
                (
                    numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap(),
                    10
                ),
                (
                    numeric_range_sort_key_for_value(&PropValue::Int(20)).unwrap(),
                    11
                )
            ]
        );
    }

    #[test]
    fn test_old_raw_hash_equality_sidecars_are_rebuilt_during_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let source_seg = dir.path().join("seg_0001");
        let compact_seg = dir.path().join("seg_0002");

        let mt = Memtable::new();
        let mut node_props = BTreeMap::new();
        node_props.insert("score".to_string(), PropValue::Int(1));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "one", node_props, 1001)),
            1,
        );
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(2, 1, "two")), 2);

        let mut edge_props = BTreeMap::new();
        edge_props.insert("score".to_string(), PropValue::UInt(1));
        let mut edge = make_edge(10, 1, 2, 1);
        edge.props = edge_props;
        mt.apply_op(&WalOp::UpsertEdge(edge), 3);

        let node_entry = SecondaryIndexManifestEntry {
            index_id: 317,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let edge_entry = SecondaryIndexManifestEntry {
            index_id: 318,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&node_entry);
        mt.register_secondary_index(&edge_entry);
        let indexes = vec![node_entry.clone(), edge_entry.clone()];

        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let semantic_hash = hash_prop_equality_key(&PropValue::Float(1.0));
        let raw_node_hash = hash_prop_value(&PropValue::Int(1));
        let raw_edge_hash = hash_prop_value(&PropValue::UInt(1));
        assert_ne!(raw_node_hash, semantic_hash);
        assert_ne!(raw_edge_hash, semantic_hash);

        rewrite_eq_sidecar_as_old_raw_hash(
            &source_seg,
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: node_entry.index_id,
            },
            component_fingerprint("flush.node_prop_eq", &[node_entry.index_id]),
            &BTreeMap::from([(raw_node_hash, vec![1])]),
        );
        rewrite_eq_sidecar_as_old_raw_hash(
            &source_seg,
            SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: edge_entry.index_id,
            },
            component_fingerprint("flush.edge_prop_eq", &[edge_entry.index_id]),
            &BTreeMap::from([(raw_edge_hash, vec![10])]),
        );

        let source = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        assert!(matches!(
            source.optional_component_availability(
                SegmentComponentKind::NodePropertyEqualityIndex {
                    index_id: node_entry.index_id,
                }
            ),
            ComponentAvailability::Incompatible { .. }
        ));
        assert!(matches!(
            source.optional_component_availability(
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: edge_entry.index_id,
                }
            ),
            ComponentAvailability::Incompatible { .. }
        ));

        let compact_reader = compact_copy_segment_for_test(source, &compact_seg, 2, &indexes);
        assert_eq!(
            compact_reader
                .secondary_eq_posting_count_if_present(node_entry.index_id, semantic_hash)
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            compact_reader
                .edge_secondary_eq_posting_count_if_present(edge_entry.index_id, semantic_hash)
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            compact_reader.optional_component_availability(
                SegmentComponentKind::NodePropertyEqualityIndex {
                    index_id: node_entry.index_id,
                }
            ),
            ComponentAvailability::Available
        );
        assert_eq!(
            compact_reader.optional_component_availability(
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: edge_entry.index_id,
                }
            ),
            ComponentAvailability::Available
        );

        let node_groups = read_secondary_eq_groups(
            &compact_seg,
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: node_entry.index_id,
            },
        );
        assert_eq!(node_groups.get(&semantic_hash), Some(&vec![1]));
        assert!(!node_groups.contains_key(&raw_node_hash));

        let edge_groups = read_secondary_eq_groups(
            &compact_seg,
            SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: edge_entry.index_id,
            },
        );
        assert_eq!(edge_groups.get(&semantic_hash), Some(&vec![10]));
        assert!(!edge_groups.contains_key(&raw_edge_hash));
    }

    #[test]
    fn test_background_build_edge_sidecar_record_is_reader_compatible() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let mut edge = make_edge(10, 1, 2, 1);
        edge.props = props;
        mt.apply_op(&WalOp::UpsertEdge(edge), 1);

        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();
        let entry = SecondaryIndexManifestEntry {
            index_id: 217,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let groups = BTreeMap::from([(red_hash, vec![10])]);
        publish_edge_prop_eq_sidecar_component(&seg_dir, &entry, &groups).unwrap();

        let manifest = read_segment_component_manifest(&seg_dir).unwrap();
        assert_eq!(
            manifest.build_kind,
            SegmentComponentBuildKind::OptionalRefresh
        );
        let record = manifest
            .components
            .iter()
            .find(|record| {
                record.kind
                    == SegmentComponentKind::EdgePropertyEqualityIndex {
                        index_id: entry.index_id,
                    }
            })
            .expect("background-built edge equality sidecar record");
        assert_eq!(
            record.build_fingerprint,
            edge_property_equality_component_fingerprint(entry.index_id)
        );

        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        assert_eq!(
            reader.optional_component_availability(
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: entry.index_id,
                }
            ),
            crate::segment_components::ComponentAvailability::Available
        );
    }

    #[test]
    fn test_optional_refresh_manifest_keeps_flush_edge_sidecar_available() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut node_props = BTreeMap::new();
        node_props.insert("color".to_string(), PropValue::String("red".to_string()));
        let mut node = make_node(1, 1, "a");
        node.props = node_props;
        mt.apply_op(&WalOp::UpsertNode(node), 1);

        let mut edge_props = BTreeMap::new();
        edge_props.insert("color".to_string(), PropValue::String("red".to_string()));
        let mut edge = make_edge(10, 1, 1, 1);
        edge.props = edge_props;
        mt.apply_op(&WalOp::UpsertEdge(edge), 2);

        let node_entry = SecondaryIndexManifestEntry {
            index_id: 501,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let edge_entry = SecondaryIndexManifestEntry {
            index_id: 502,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        mt.register_secondary_index(&node_entry);
        mt.register_secondary_index(&edge_entry);
        let indexes = vec![node_entry.clone(), edge_entry.clone()];

        let info = write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let node_groups = BTreeMap::from([(red_hash, vec![1])]);
        publish_node_prop_eq_sidecar_component(&seg_dir, &node_entry, &node_groups).unwrap();

        let manifest = read_segment_component_manifest(&seg_dir).unwrap();
        assert_eq!(
            manifest.build_kind,
            SegmentComponentBuildKind::OptionalRefresh
        );
        let edge_record = manifest
            .components
            .iter()
            .find(|record| {
                record.kind
                    == SegmentComponentKind::EdgePropertyEqualityIndex {
                        index_id: edge_entry.index_id,
                    }
            })
            .expect("flush-built edge sidecar survives optional refresh");
        assert_eq!(
            edge_record.build_fingerprint,
            edge_property_equality_component_fingerprint(edge_entry.index_id)
        );

        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();
        assert_eq!(
            reader.optional_component_availability(
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: edge_entry.index_id,
                }
            ),
            crate::segment_components::ComponentAvailability::Available
        );
    }

    #[test]
    fn compound_field_index_paths_and_flush_build_are_wired() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let entry = SecondaryIndexManifestEntry {
            index_id: 701,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        assert_eq!(
            secondary_index_component_kind_for_entry(&entry),
            Some(SegmentComponentKind::NodeCompoundRangeIndex {
                index_id: entry.index_id,
            })
        );
        assert_eq!(
            secondary_index_base_relative_path_for_entry(&entry).unwrap(),
            format!(
                "{}/node_compound_range_{}.dat",
                SECONDARY_INDEX_DIRNAME, entry.index_id
            )
        );
        assert_eq!(
            edge_compound_eq_sidecar_path(&seg_dir, 702),
            secondary_indexes_dir(&seg_dir).join("edge_compound_eq_702.dat")
        );
        assert_eq!(
            edge_compound_range_sidecar_path(&seg_dir, 703),
            secondary_indexes_dir(&seg_dir).join("edge_compound_range_703.dat")
        );

        let partitions = partition_secondary_indexes(std::slice::from_ref(&entry));
        assert!(partitions.node_eq.is_empty());
        assert_eq!(partitions.node_range.len(), 1);
        assert!(partitions.edge_eq.is_empty());
        assert!(partitions.edge_range.is_empty());

        let mt = Memtable::new();
        let info = write_segment_with_secondary_indexes(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&entry),
        )
        .unwrap();
        assert!(node_compound_range_sidecar_path(&seg_dir, entry.index_id).exists());
        let manifest = read_segment_component_manifest(&seg_dir).unwrap();
        assert!(manifest.components.iter().any(|record| {
            record.kind
                == SegmentComponentKind::NodeCompoundRangeIndex {
                    index_id: entry.index_id,
                }
        }));
        let maintained = maintained_secondary_index_ids_from_component_records(
            &manifest.components,
            std::slice::from_ref(&entry),
        );
        assert!(maintained.equality_index_ids.is_empty());
        assert!(maintained.range_index_ids.contains(&entry.index_id));
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        assert!(reader.validate_compound_sidecar_for_entry(&entry).unwrap());
        assert_eq!(
            reader.optional_component_availability(SegmentComponentKind::NodeCompoundRangeIndex {
                index_id: entry.index_id,
            }),
            ComponentAvailability::Available
        );
    }

    fn node_compound_entry_for_storage_test(
        index_id: u64,
        kind: SecondaryIndexKind,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn edge_compound_entry_for_storage_test(
        index_id: u64,
        kind: SecondaryIndexKind,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::EdgeFieldIndex {
                label_id: 4,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "status".to_string(),
                    },
                    SecondaryIndexFieldManifest::EdgeMetadata {
                        field: EdgeMetadataIndexFieldManifest::ValidTo,
                    },
                ],
            },
            kind,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn edge_compound_from_status_valid_to_entry_for_storage_test(
        index_id: u64,
        kind: SecondaryIndexKind,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::EdgeFieldIndex {
                label_id: 4,
                fields: vec![
                    SecondaryIndexFieldManifest::EdgeMetadata {
                        field: EdgeMetadataIndexFieldManifest::From,
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "status".to_string(),
                    },
                    SecondaryIndexFieldManifest::EdgeMetadata {
                        field: EdgeMetadataIndexFieldManifest::ValidTo,
                    },
                ],
            },
            kind,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn compound_entries_for_reader(
        reader: &SegmentReader,
        entry: &SecondaryIndexManifestEntry,
    ) -> BTreeSet<(Vec<u8>, u64)> {
        let mut entries = BTreeSet::new();
        assert!(reader
            .for_each_compound_sidecar_entry(entry, |key, id| {
                entries.insert((key.to_vec(), id));
                Ok(())
            })
            .unwrap());
        entries
    }

    #[test]
    fn compound_compaction_selected_field_needs_are_targeted() {
        let node_metadata_fields = vec![
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::Id,
            },
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::UpdatedAt,
            },
        ];
        let node_metadata_needs = node_compound_selected_field_needs(&node_metadata_fields);
        assert!(!node_metadata_needs.key);
        assert!(!node_metadata_needs.created_at);
        assert!(matches!(node_metadata_needs.props, PropertySelection::None));
        assert!(!node_compound_needs_record(&node_metadata_needs));

        let node_mixed_fields = vec![
            SecondaryIndexFieldManifest::Property {
                key: "tenant".to_string(),
            },
            SecondaryIndexFieldManifest::Property {
                key: "status".to_string(),
            },
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::Key,
            },
        ];
        let node_mixed_needs = node_compound_selected_field_needs(&node_mixed_fields);
        assert!(node_mixed_needs.key);
        assert!(matches!(
            node_mixed_needs.props,
            PropertySelection::Keys(ref keys) if keys == &vec!["status".to_string(), "tenant".to_string()]
        ));
        assert!(node_compound_needs_record(&node_mixed_needs));

        let edge_metadata_fields = vec![
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::From,
            },
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::ValidTo,
            },
        ];
        let edge_metadata_needs = edge_compound_selected_field_needs(&edge_metadata_fields);
        assert!(!edge_metadata_needs.created_at);
        assert!(matches!(edge_metadata_needs.props, PropertySelection::None));
        assert!(!edge_compound_needs_record(&edge_metadata_needs));

        let edge_mixed_fields = vec![
            SecondaryIndexFieldManifest::Property {
                key: "status".to_string(),
            },
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::CreatedAt,
            },
        ];
        let edge_mixed_needs = edge_compound_selected_field_needs(&edge_mixed_fields);
        assert!(edge_mixed_needs.created_at);
        assert!(matches!(
            edge_mixed_needs.props,
            PropertySelection::Keys(ref keys) if keys == &vec!["status".to_string()]
        ));
        assert!(edge_compound_needs_record(&edge_mixed_needs));
    }

    #[test]
    fn flush_writes_node_compound_equality_and_range_sidecars_that_scan() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let eq_entry = node_compound_entry_for_storage_test(810, SecondaryIndexKind::Equality);
        let range_entry = node_compound_entry_for_storage_test(811, SecondaryIndexKind::Range);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];
        let mt = Memtable::new();
        for (id, tenant, updated_at) in [(1, "acme", 100), (2, "acme", 200), (3, "globex", 150)] {
            let mut props = BTreeMap::new();
            props.insert("tenant".to_string(), PropValue::String(tenant.to_string()));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_custom_props(
                    id,
                    1,
                    &format!("node-{id}"),
                    props,
                    updated_at,
                )),
                id,
            );
        }
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);

        let info = write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();
        assert!(node_compound_eq_sidecar_path(&seg_dir, eq_entry.index_id).exists());
        assert!(node_compound_range_sidecar_path(&seg_dir, range_entry.index_id).exists());
        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();

        let tenant = PropValue::String("acme".to_string());
        let context = CompoundTupleContext::from_manifest_entry(&eq_entry).unwrap();
        let prefix = crate::secondary_index_key::encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&tenant))],
        )
        .unwrap();
        assert_eq!(
            reader
                .compound_prefix_candidates_if_present(
                    &eq_entry,
                    &crate::secondary_index_key::compound_prefix_bounds(&prefix)
                )
                .unwrap(),
            Some(vec![1, 2])
        );

        let range_context = CompoundTupleContext::from_manifest_entry(&range_entry).unwrap();
        let lower = crate::secondary_index_key::encode_compound_field_component(
            &range_context,
            1,
            CompoundFieldValue::MetadataI64(150),
        )
        .unwrap();
        let upper = crate::secondary_index_key::encode_compound_field_component(
            &range_context,
            1,
            CompoundFieldValue::MetadataI64(250),
        )
        .unwrap();
        let bounds = crate::secondary_index_key::compound_range_bounds(
            &prefix,
            Some((&lower, true)),
            Some((&upper, true)),
        )
        .unwrap();
        assert_eq!(
            reader
                .compound_range_candidates_if_present(&range_entry, &bounds)
                .unwrap(),
            Some(vec![2])
        );
    }

    #[test]
    fn flush_writes_edge_compound_equality_and_range_sidecars_that_scan() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let eq_entry = edge_compound_entry_for_storage_test(820, SecondaryIndexKind::Equality);
        let range_entry = edge_compound_entry_for_storage_test(821, SecondaryIndexKind::Range);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];
        let mt = Memtable::new();
        for (id, status, valid_to) in [(10, "open", 100), (11, "open", 300), (12, "done", 200)] {
            let mut edge = make_edge(id, 1, id + 10, 4);
            edge.valid_to = valid_to;
            edge.props
                .insert("status".to_string(), PropValue::String(status.to_string()));
            mt.apply_op(&WalOp::UpsertEdge(edge), id);
        }
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);

        let info = write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();
        assert!(edge_compound_eq_sidecar_path(&seg_dir, eq_entry.index_id).exists());
        assert!(edge_compound_range_sidecar_path(&seg_dir, range_entry.index_id).exists());
        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();
        let status = PropValue::String("open".to_string());
        let context = CompoundTupleContext::from_manifest_entry(&eq_entry).unwrap();
        let prefix = crate::secondary_index_key::encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&status))],
        )
        .unwrap();
        assert_eq!(
            reader
                .compound_prefix_candidates_if_present(
                    &eq_entry,
                    &crate::secondary_index_key::compound_prefix_bounds(&prefix)
                )
                .unwrap(),
            Some(vec![10, 11])
        );
        let range_context = CompoundTupleContext::from_manifest_entry(&range_entry).unwrap();
        let lower = crate::secondary_index_key::encode_compound_field_component(
            &range_context,
            1,
            CompoundFieldValue::MetadataI64(250),
        )
        .unwrap();
        let bounds =
            crate::secondary_index_key::compound_range_bounds(&prefix, Some((&lower, true)), None)
                .unwrap();
        assert_eq!(
            reader
                .compound_range_candidates_if_present(&range_entry, &bounds)
                .unwrap(),
            Some(vec![11])
        );
    }

    #[test]
    fn compaction_compound_sidecars_match_flush_and_rebuild_when_missing() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");
        let rebuild_dir = tempfile::tempdir().unwrap();
        let rebuild_seg = rebuild_dir.path().join("seg_0003");
        let eq_entry = node_compound_entry_for_storage_test(830, SecondaryIndexKind::Equality);
        let range_entry = node_compound_entry_for_storage_test(831, SecondaryIndexKind::Range);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];
        let mt = Memtable::new();
        for (id, tenant, updated_at) in [(1, "acme", 100), (2, "acme", 200), (3, "globex", 300)] {
            let mut props = BTreeMap::new();
            props.insert("tenant".to_string(), PropValue::String(tenant.to_string()));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_custom_props(
                    id,
                    1,
                    &format!("node-{id}"),
                    props,
                    updated_at,
                )),
                id,
            );
        }
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );

        let compact_reader =
            compact_copy_segment_for_test(source_reader.clone(), &compact_seg, 2, &indexes);
        assert_eq!(
            compound_entries_for_reader(&source_reader, &eq_entry),
            compound_entries_for_reader(&compact_reader, &eq_entry)
        );
        assert_eq!(
            compound_entries_for_reader(&source_reader, &range_entry),
            compound_entries_for_reader(&compact_reader, &range_entry)
        );

        remove_secondary_index_component_records(&source_seg, &eq_entry).unwrap();
        remove_secondary_index_component_records(&source_seg, &range_entry).unwrap();
        let source_without_compound = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let rebuilt_reader =
            compact_copy_segment_for_test(source_without_compound, &rebuild_seg, 3, &indexes);
        assert_eq!(
            compound_entries_for_reader(&source_reader, &eq_entry),
            compound_entries_for_reader(&rebuilt_reader, &eq_entry)
        );
        assert_eq!(
            compound_entries_for_reader(&source_reader, &range_entry),
            compound_entries_for_reader(&rebuilt_reader, &range_entry)
        );
    }

    #[test]
    fn edge_compound_compaction_matches_flush_and_rebuilds_metadata_property_mix() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");
        let rebuild_dir = tempfile::tempdir().unwrap();
        let rebuild_seg = rebuild_dir.path().join("seg_0003");
        let eq_entry = edge_compound_from_status_valid_to_entry_for_storage_test(
            850,
            SecondaryIndexKind::Equality,
        );
        let range_entry = edge_compound_from_status_valid_to_entry_for_storage_test(
            851,
            SecondaryIndexKind::Range,
        );
        let indexes = vec![eq_entry.clone(), range_entry.clone()];
        let mt = Memtable::new();
        for (id, from, to, status, valid_to) in [
            (10, 1, 101, "open", 100),
            (11, 1, 102, "open", 300),
            (12, 2, 103, "done", 200),
        ] {
            let mut edge = make_edge(id, from, to, 4);
            edge.valid_to = valid_to;
            edge.props
                .insert("status".to_string(), PropValue::String(status.to_string()));
            mt.apply_op(&WalOp::UpsertEdge(edge), id);
        }
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );

        let compact_reader =
            compact_copy_segment_for_test(source_reader.clone(), &compact_seg, 2, &indexes);
        assert_eq!(
            compound_entries_for_reader(&source_reader, &eq_entry),
            compound_entries_for_reader(&compact_reader, &eq_entry)
        );
        assert_eq!(
            compound_entries_for_reader(&source_reader, &range_entry),
            compound_entries_for_reader(&compact_reader, &range_entry)
        );

        remove_secondary_index_component_records(&source_seg, &eq_entry).unwrap();
        remove_secondary_index_component_records(&source_seg, &range_entry).unwrap();
        let source_without_compound = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let rebuilt_reader =
            compact_copy_segment_for_test(source_without_compound, &rebuild_seg, 3, &indexes);
        assert_eq!(
            compound_entries_for_reader(&source_reader, &eq_entry),
            compound_entries_for_reader(&rebuilt_reader, &eq_entry)
        );
        assert_eq!(
            compound_entries_for_reader(&source_reader, &range_entry),
            compound_entries_for_reader(&rebuilt_reader, &range_entry)
        );
    }

    #[test]
    fn source_compound_sidecar_reuse_filters_non_surviving_records() {
        let left_dir = tempfile::tempdir().unwrap();
        let left_seg = left_dir.path().join("seg_left");
        let right_dir = tempfile::tempdir().unwrap();
        let right_seg = right_dir.path().join("seg_right");
        let entry = node_compound_entry_for_storage_test(840, SecondaryIndexKind::Equality);
        let indexes = vec![entry.clone()];

        let left_mt = Memtable::new();
        let mut left_props = BTreeMap::new();
        left_props.insert("tenant".to_string(), PropValue::String("old".to_string()));
        left_mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "old", left_props, 100)),
            1,
        );
        left_mt.register_secondary_index(&entry);
        let left_info =
            write_segment_with_secondary_indexes(&left_seg, 1, &left_mt, None, &indexes).unwrap();
        let left_reader =
            Arc::new(SegmentReader::open_with_info(&left_seg, &left_info, None, &indexes).unwrap());

        let right_mt = Memtable::new();
        let mut right_props = BTreeMap::new();
        right_props.insert("tenant".to_string(), PropValue::String("new".to_string()));
        right_mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "new", right_props, 200)),
            1,
        );
        right_mt.register_secondary_index(&entry);
        let right_info =
            write_segment_with_secondary_indexes(&right_seg, 2, &right_mt, None, &indexes).unwrap();
        let right_reader = Arc::new(
            SegmentReader::open_with_info(&right_seg, &right_info, None, &indexes).unwrap(),
        );

        let segments = vec![left_reader, right_reader.clone()];
        let meta = right_reader.node_meta_at(0).unwrap();
        let node_metas = vec![CompactNodeMeta {
            node_id: meta.node_id,
            new_data_offset: meta.data_offset,
            data_len: meta.data_len,
            label_ids: meta.label_ids,
            updated_at: meta.updated_at,
            weight: meta.weight,
            key_len: meta.key_len,
            dense_vector_offset: 0,
            dense_vector_len: 0,
            sparse_vector_offset: 0,
            sparse_vector_len: 0,
            src_seg_idx: 1,
            src_data_offset: meta.data_offset,
            last_write_seq: meta.last_write_seq,
        }];
        let reused =
            build_node_compound_entries_from_source_sidecars(&segments, &node_metas, &entry)
                .unwrap();
        let expected = compound_entries_for_reader(&right_reader, &entry)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(reused, expected);
    }

    #[test]
    fn source_compound_sidecar_reuse_errors_when_sidecar_absent() {
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join("seg_src");
        let entry = node_compound_entry_for_storage_test(842, SecondaryIndexKind::Equality);
        let indexes = vec![entry.clone()];

        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("tenant".to_string(), PropValue::String("acme".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "absent", props, 100)),
            1,
        );
        mt.register_secondary_index(&entry);
        let info = write_segment_with_secondary_indexes(&seg, 1, &mt, None, &indexes).unwrap();

        remove_secondary_index_component_records(&seg, &entry).unwrap();
        let reader = Arc::new(SegmentReader::open_with_info(&seg, &info, None, &indexes).unwrap());

        let meta = reader.node_meta_at(0).unwrap();
        let node_metas = vec![CompactNodeMeta {
            node_id: meta.node_id,
            new_data_offset: meta.data_offset,
            data_len: meta.data_len,
            label_ids: meta.label_ids,
            updated_at: meta.updated_at,
            weight: meta.weight,
            key_len: meta.key_len,
            dense_vector_offset: 0,
            dense_vector_len: 0,
            sparse_vector_offset: 0,
            sparse_vector_len: 0,
            src_seg_idx: 0,
            src_data_offset: meta.data_offset,
            last_write_seq: meta.last_write_seq,
        }];
        let error =
            build_node_compound_entries_from_source_sidecars(&[reader], &node_metas, &entry)
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("compound secondary index unavailable:"),
            "absent source sidecar must fail reuse with the stable failure prefix, got: {error}"
        );
    }

    #[test]
    fn grouped_compound_rebuild_handles_mixed_declarations_per_label() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let rebuild_dir = tempfile::tempdir().unwrap();
        let rebuild_seg = rebuild_dir.path().join("seg_0002");

        // Three declarations on the same label with distinct record needs:
        // tenant property, score property + node key, and metadata-only.
        let tenant_entry = node_compound_entry_for_storage_test(850, SecondaryIndexKind::Equality);
        let score_key_entry = SecondaryIndexManifestEntry {
            index_id: 851,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "score".to_string(),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::Key,
                    },
                ],
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let metadata_only_entry = SecondaryIndexManifestEntry {
            index_id: 852,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::Id,
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let indexes = vec![
            tenant_entry.clone(),
            score_key_entry.clone(),
            metadata_only_entry.clone(),
        ];

        let mt = Memtable::new();
        for (id, tenant, score, updated_at) in [
            (1, "acme", 10, 100),
            (2, "acme", 20, 200),
            (3, "globex", 30, 300),
        ] {
            let mut props = BTreeMap::new();
            props.insert("tenant".to_string(), PropValue::String(tenant.to_string()));
            props.insert("score".to_string(), PropValue::Int(score));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_custom_props(
                    id,
                    1,
                    &format!("grouped-{id}"),
                    props,
                    updated_at,
                )),
                id,
            );
        }
        for entry in &indexes {
            mt.register_secondary_index(entry);
        }
        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );

        // Drop all source sidecars so compaction must take the grouped
        // metadata-rebuild path; every rebuilt sidecar must match flush.
        for entry in &indexes {
            remove_secondary_index_component_records(&source_seg, entry).unwrap();
        }
        let source_without_compound = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let rebuilt_reader =
            compact_copy_segment_for_test(source_without_compound, &rebuild_seg, 2, &indexes);
        for entry in &indexes {
            assert_eq!(
                compound_entries_for_reader(&source_reader, entry),
                compound_entries_for_reader(&rebuilt_reader, entry),
                "rebuilt sidecar for index {} must match the flush sidecar",
                entry.index_id
            );
        }
    }

    #[test]
    fn compound_component_build_fingerprint_includes_declaration_fingerprint() {
        let base = SecondaryIndexManifestEntry {
            index_id: 702,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let reordered = SecondaryIndexManifestEntry {
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                ],
            },
            ..base.clone()
        };
        let source_changed = SecondaryIndexManifestEntry {
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "updated_at".to_string(),
                    },
                ],
            },
            ..base.clone()
        };
        let kind = secondary_index_component_kind_for_entry(&base).unwrap();
        let base_fingerprint =
            compound_component_fingerprint_for_kind_and_entry(&kind, &base).unwrap();

        assert_ne!(
            base_fingerprint,
            compound_component_fingerprint_for_kind_and_entry(&kind, &reordered).unwrap()
        );
        assert_ne!(
            base_fingerprint,
            compound_component_fingerprint_for_kind_and_entry(&kind, &source_changed).unwrap()
        );
    }

    #[test]
    fn test_edge_drop_cleanup_paths_include_generated_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        props.insert("score".to_string(), PropValue::Int(10));
        let mut edge = make_edge(10, 1, 2, 1);
        edge.props = props;
        mt.apply_op(&WalOp::UpsertEdge(edge), 1);

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 317,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 318,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];
        write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();

        let sidecar_dir = secondary_indexes_dir(&seg_dir);
        let generated_path =
            sidecar_dir.join(format!("edge_prop_eq_{}.g42.dat", eq_entry.index_id));
        let refresh_path = sidecar_dir.join(format!(
            ".edge_prop_eq_{}.refresh_tmp.42",
            eq_entry.index_id
        ));
        std::fs::write(&generated_path, b"generated").unwrap();
        std::fs::write(&refresh_path, b"refresh").unwrap();

        let mut cleanup_paths =
            remove_secondary_index_component_records(&seg_dir, &eq_entry).unwrap();
        cleanup_paths.extend(secondary_index_sidecar_paths_for_entry(&seg_dir, &eq_entry));
        cleanup_paths.sort();
        cleanup_paths.dedup();
        for path in cleanup_paths {
            let _ = std::fs::remove_file(path);
        }

        assert!(!edge_prop_eq_sidecar_path(&seg_dir, eq_entry.index_id).exists());
        assert!(!generated_path.exists());
        assert!(!refresh_path.exists());
        assert!(edge_prop_range_sidecar_path(&seg_dir, range_entry.index_id).exists());

        let manifest = read_segment_component_manifest(&seg_dir).unwrap();
        assert!(!manifest.components.iter().any(|record| {
            record.kind
                == SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: eq_entry.index_id,
                }
        }));
        assert!(manifest.components.iter().any(|record| {
            record.kind
                == SegmentComponentKind::EdgePropertyRangeIndex {
                    index_id: range_entry.index_id,
                }
        }));
    }

    #[test]
    fn test_orphan_cleanup_removes_unreferenced_compound_base_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let eq_entry = node_compound_entry_for_storage_test(851, SecondaryIndexKind::Equality);
        let range_entry = node_compound_entry_for_storage_test(852, SecondaryIndexKind::Range);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];

        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("tenant".to_string(), PropValue::String("acme".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(
                1,
                1,
                "compound-orphan",
                props,
                100,
            )),
            1,
        );
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();

        let eq_sidecar_path = node_compound_eq_sidecar_path(&seg_dir, eq_entry.index_id);
        let range_sidecar_path = node_compound_range_sidecar_path(&seg_dir, range_entry.index_id);
        assert!(eq_sidecar_path.exists());
        assert!(range_sidecar_path.exists());

        remove_secondary_index_component_records(&seg_dir, &eq_entry).unwrap();
        assert!(eq_sidecar_path.exists());
        cleanup_orphan_optional_component_files(&seg_dir);

        assert!(!eq_sidecar_path.exists());
        assert!(range_sidecar_path.exists());
    }

    #[test]
    fn test_maintained_secondary_ids_match_target_kind() {
        let node_entry = SecondaryIndexManifestEntry {
            index_id: 417,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let edge_entry = SecondaryIndexManifestEntry {
            index_id: 417,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };

        let node_record = test_component_record(SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: 417,
        });
        let edge_record = test_component_record(SegmentComponentKind::EdgePropertyEqualityIndex {
            index_id: 417,
        });

        let maintained = maintained_secondary_index_ids_from_component_records(
            std::slice::from_ref(&node_record),
            std::slice::from_ref(&edge_entry),
        );
        assert!(!maintained.equality_index_ids.contains(&edge_entry.index_id));

        let maintained = maintained_secondary_index_ids_from_component_records(
            std::slice::from_ref(&edge_record),
            std::slice::from_ref(&node_entry),
        );
        assert!(!maintained.equality_index_ids.contains(&node_entry.index_id));

        let maintained = maintained_secondary_index_ids_from_component_records(
            std::slice::from_ref(&edge_record),
            std::slice::from_ref(&edge_entry),
        );
        assert!(maintained.equality_index_ids.contains(&edge_entry.index_id));
    }

    #[test]
    fn test_flush_planner_stats_cover_core_declared_indexes_and_adjacency() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut red_10 = BTreeMap::new();
        red_10.insert("color".to_string(), PropValue::String("red".to_string()));
        red_10.insert("score".to_string(), PropValue::Int(10));
        red_10.insert("tag".to_string(), PropValue::String("hot".to_string()));
        let mut red_20 = BTreeMap::new();
        red_20.insert("color".to_string(), PropValue::String("red".to_string()));
        red_20.insert("score".to_string(), PropValue::Int(20));
        let mut blue_30 = BTreeMap::new();
        blue_30.insert("color".to_string(), PropValue::String("blue".to_string()));
        blue_30.insert("score".to_string(), PropValue::Int(30));

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 7, "a", red_10, 1000)),
            1,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(2, 7, "b", red_20, 2000)),
            2,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(3, 8, "c", blue_30, 3000)),
            3,
        );
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 4);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(11, 1, 3, 5)), 5);

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 71,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 7,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 72,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 7,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry, range_entry];

        let info = write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();
        assert!(seg_dir
            .join(crate::planner_stats::PLANNER_STATS_FILENAME)
            .exists());

        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();
        let stats = reader.planner_stats().expect("planner stats should load");
        let reference_stats = crate::planner_stats::build_flush_stats(
            1,
            &seg_dir,
            &mt.nodes(),
            &mt.edges(),
            &indexes,
        )
        .unwrap();
        assert_eq!(stats, &reference_stats);
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 2);
        assert!(stats.general_property_stats_complete);
        assert_eq!(stats.node_id_sample, vec![1, 2, 3]);
        assert_eq!(
            stats
                .node_label_stats
                .iter()
                .map(|node_label_stats| (node_label_stats.label_id, node_label_stats.node_count))
                .collect::<Vec<_>>(),
            vec![(7, 2), (8, 1)]
        );

        let color_stats = stats
            .property_stats
            .iter()
            .find(|prop| prop.label_id == 7 && prop.prop_key == "color")
            .unwrap();
        assert_eq!(
            color_stats.tracked_reason,
            crate::planner_stats::PropertyStatsTrackedReason::DeclaredEquality
        );
        assert_eq!(color_stats.present_count, 2);
        assert_eq!(color_stats.exact_distinct_count, Some(1));

        let equality = stats
            .equality_index_stats
            .iter()
            .find(|stats| stats.index_id == 71)
            .unwrap();
        assert_eq!(equality.total_postings, 2);
        assert_eq!(equality.value_group_count, 1);
        assert!(equality.sidecar_present_at_build);

        let range = stats
            .range_index_stats
            .iter()
            .find(|stats| stats.index_id == 72)
            .unwrap();
        assert_eq!(range.total_entries, 2);
        assert_eq!(range.buckets.len(), 2);
        assert!(range.sidecar_present_at_build);

        let outgoing = stats
            .adjacency_stats
            .iter()
            .find(|stats| {
                stats.direction == crate::planner_stats::PlannerStatsDirection::Outgoing
                    && stats.edge_label_id == Some(5)
            })
            .unwrap();
        assert_eq!(outgoing.source_node_count, 1);
        assert_eq!(outgoing.total_edges, 2);
        assert_eq!(outgoing.max_fanout, 2);
    }

    #[test]
    fn test_edge_property_declared_planner_stats_survive_flush_compaction_and_reopen() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 3);
        for (id, color, score) in [(10, "red", 10), (11, "red", 20), (12, "blue", 30)] {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            props.insert("score".to_string(), PropValue::Int(score));
            let mut edge = make_edge(id, 1, 2, 4);
            edge.props = props;
            mt.apply_op(&WalOp::UpsertEdge(edge), id);
        }
        let mut ignored_props = BTreeMap::new();
        ignored_props.insert("color".to_string(), PropValue::String("red".to_string()));
        ignored_props.insert("score".to_string(), PropValue::Int(40));
        let mut ignored_edge = make_edge(20, 2, 3, 5);
        ignored_edge.props = ignored_props;
        mt.apply_op(&WalOp::UpsertEdge(ignored_edge), 20);

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 171,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 4,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 172,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 4,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];

        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let flush_stats = source_reader.planner_stats().unwrap();
        let reference_stats = crate::planner_stats::build_flush_stats(
            1,
            &source_seg,
            &mt.nodes(),
            &mt.edges(),
            &indexes,
        )
        .unwrap();
        assert_eq!(flush_stats, &reference_stats);

        let equality = flush_stats
            .equality_index_stats
            .iter()
            .find(|stats| stats.index_id == eq_entry.index_id)
            .expect("edge equality stats");
        assert_eq!(equality.target_label_id, 4);
        assert_eq!(equality.prop_key, "color");
        assert_eq!(equality.total_postings, 3);
        assert_eq!(equality.value_group_count, 2);
        assert_eq!(equality.max_group_postings, 2);
        assert!(equality.sidecar_present_at_build);

        let range = flush_stats
            .range_index_stats
            .iter()
            .find(|stats| stats.index_id == range_entry.index_id)
            .expect("edge range stats");
        assert_eq!(range.target_label_id, 4);
        assert_eq!(range.prop_key, "score");
        assert_eq!(range.total_entries, 3);
        assert!(range.sidecar_present_at_build);

        let compact_reader =
            compact_copy_segment_for_test(source_reader.clone(), &compact_seg, 2, &indexes);
        let compact_stats = compact_reader.planner_stats().unwrap();
        assert_eq!(
            compact_stats.build_kind,
            crate::planner_stats::PlannerStatsBuildKind::Compaction
        );
        assert_eq!(
            compact_stats.equality_index_stats,
            flush_stats.equality_index_stats
        );
        assert_eq!(
            compact_stats.range_index_stats,
            flush_stats.range_index_stats
        );
    }

    #[test]
    fn test_planner_stats_sidecar_is_deterministic_for_same_segment_contents() {
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::UInt(10));

        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(2, 1, "b", props.clone(), 2000)),
            1,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "a", props, 1000)),
            2,
        );

        let dir = tempfile::tempdir().unwrap();
        let left = dir.path().join("left");
        let right = dir.path().join("right");
        write_segment(&left, 42, &mt, None).unwrap();
        write_segment(&right, 42, &mt, None).unwrap();

        let left_stats =
            std::fs::read(left.join(crate::planner_stats::PLANNER_STATS_FILENAME)).unwrap();
        let right_stats =
            std::fs::read(right.join(crate::planner_stats::PLANNER_STATS_FILENAME)).unwrap();
        assert_eq!(left_stats, right_stats);
    }

    #[test]
    fn test_flush_planner_stats_caps_general_properties_but_keeps_declared_keys() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        for idx in 0..300 {
            props.insert(format!("prop_{:03}", idx), PropValue::UInt(idx));
        }
        props.insert(
            "zz_declared".to_string(),
            PropValue::String("yes".to_string()),
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "wide", props, 1000)),
            1,
        );
        let declared = SecondaryIndexManifestEntry {
            index_id: 91,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "zz_declared".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&declared);

        let info = write_segment_with_secondary_indexes(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&declared),
        )
        .unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&declared))
                .unwrap();
        let stats = reader.planner_stats().unwrap();
        let label_one_props: Vec<_> = stats
            .property_stats
            .iter()
            .filter(|prop| prop.label_id == 1)
            .collect();
        let general_count = label_one_props
            .iter()
            .filter(|prop| {
                prop.tracked_reason
                    == crate::planner_stats::PropertyStatsTrackedReason::GeneralTopProperty
            })
            .count();
        assert_eq!(
            general_count,
            crate::planner_stats::PLANNER_STATS_MAX_PROPERTY_KEYS_PER_LABEL
        );
        let declared_stats = label_one_props
            .iter()
            .find(|prop| prop.prop_key == "zz_declared")
            .unwrap();
        assert_eq!(
            declared_stats.tracked_reason,
            crate::planner_stats::PropertyStatsTrackedReason::DeclaredEquality
        );
        assert_eq!(declared_stats.present_count, 1);
    }

    #[test]
    fn test_flush_planner_stats_keeps_late_frequent_general_property() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let mut first_props = BTreeMap::new();
        for idx in 0..crate::planner_stats::PLANNER_STATS_MAX_PROPERTY_KEYS_PER_LABEL * 4 {
            first_props.insert(format!("one_off_{:04}", idx), PropValue::UInt(idx as u64));
        }
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "wide", first_props, 1000)),
            1,
        );
        for node_id in 2..=33 {
            let mut props = BTreeMap::new();
            props.insert("zz_late_hot".to_string(), PropValue::UInt(node_id));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_custom_props(
                    node_id,
                    1,
                    &format!("hot_{}", node_id),
                    props,
                    1000 + node_id as i64,
                )),
                node_id,
            );
        }

        write_segment(&seg_dir, 1, &mt, None).unwrap();
        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        let stats = reader.planner_stats().unwrap();
        let late_hot = stats
            .property_stats
            .iter()
            .find(|prop| prop.label_id == 1 && prop.prop_key == "zz_late_hot")
            .expect("late frequent property should be tracked");
        assert_eq!(
            late_hot.tracked_reason,
            crate::planner_stats::PropertyStatsTrackedReason::GeneralTopProperty
        );
        assert_eq!(late_hot.present_count, 32);
        assert_eq!(late_hot.exact_distinct_count, Some(32));
    }

    #[test]
    fn test_planner_stats_declared_index_for_absent_label_stays_available() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");

        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_custom_props(1, 1, "present", props, 1000)),
            1,
        );

        let absent_declared = SecondaryIndexManifestEntry {
            index_id: 101,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 99,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&absent_declared);

        let source_info = write_segment_with_secondary_indexes(
            &source_seg,
            1,
            &mt,
            None,
            std::slice::from_ref(&absent_declared),
        )
        .unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(
                &source_seg,
                &source_info,
                None,
                std::slice::from_ref(&absent_declared),
            )
            .unwrap(),
        );
        let flush_stats = source_reader.planner_stats().unwrap();
        assert!(flush_stats
            .property_stats
            .iter()
            .all(|prop| prop.label_id != 99));
        let equality = flush_stats
            .equality_index_stats
            .iter()
            .find(|stats| stats.index_id == 101)
            .unwrap();
        assert_eq!(equality.total_postings, 0);

        let compact_reader =
            compact_copy_segment_for_test(source_reader, &compact_seg, 2, &[absent_declared]);
        let compact_stats = compact_reader.planner_stats().unwrap();
        assert!(compact_stats
            .property_stats
            .iter()
            .all(|prop| prop.label_id != 99));
        let equality = compact_stats
            .equality_index_stats
            .iter()
            .find(|stats| stats.index_id == 101)
            .unwrap();
        assert_eq!(equality.total_postings, 0);
    }

    #[test]
    fn test_compaction_planner_stats_match_flush_for_complete_evidence() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");

        let mt = Memtable::new();
        for (id, color, score) in [(1, "red", 10), (2, "red", 20), (3, "blue", 30)] {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            props.insert("score".to_string(), PropValue::Int(score));
            props.insert("tag".to_string(), PropValue::String(format!("n{}", id)));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_custom_props(
                    id,
                    9,
                    &format!("k{}", id),
                    props,
                    1000 + id as i64,
                )),
                id,
            );
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 4)), 10);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(11, 2, 3, 4)), 11);

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 81,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 9,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 82,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 9,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry, range_entry];

        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let compact_reader =
            compact_copy_segment_for_test(source_reader.clone(), &compact_seg, 2, &indexes);

        let flush_stats = source_reader.planner_stats().unwrap();
        let compact_stats = compact_reader.planner_stats().unwrap();
        assert_eq!(
            compact_stats.build_kind,
            crate::planner_stats::PlannerStatsBuildKind::Compaction
        );
        assert!(compact_stats.general_property_stats_complete);
        assert_eq!(compact_stats.general_property_sampled_node_count, 3);
        assert_eq!(compact_stats.node_label_stats, flush_stats.node_label_stats);
        assert_eq!(compact_stats.timestamp_stats, flush_stats.timestamp_stats);
        assert_eq!(compact_stats.property_stats, flush_stats.property_stats);
        assert_eq!(
            compact_stats.equality_index_stats,
            flush_stats.equality_index_stats
        );
        assert_eq!(
            compact_stats.range_index_stats,
            flush_stats.range_index_stats
        );
        assert_eq!(compact_stats.adjacency_stats, flush_stats.adjacency_stats);
        assert_eq!(compact_stats.node_id_sample, flush_stats.node_id_sample);
    }

    #[test]
    fn test_multi_label_compaction_rebuilds_label_scoped_indexes_like_flush() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");

        let mt = Memtable::new();
        let mut props_one = BTreeMap::new();
        props_one.insert("color".to_string(), PropValue::String("green".to_string()));
        props_one.insert("score".to_string(), PropValue::Int(10));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[1], "one", props_one, 100)),
            1,
        );

        let mut props_two = BTreeMap::new();
        props_two.insert("color".to_string(), PropValue::String("red".to_string()));
        props_two.insert("score".to_string(), PropValue::Int(20));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(2, &[2, 3], "two", props_two, 200)),
            2,
        );

        let labels_ten = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19];
        let mut props_ten = BTreeMap::new();
        props_ten.insert("color".to_string(), PropValue::String("blue".to_string()));
        props_ten.insert("score".to_string(), PropValue::Int(30));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(3, &labels_ten, "ten", props_ten, 300)),
            3,
        );

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 901,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 3,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 902,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 12,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);
        let indexes = vec![eq_entry.clone(), range_entry.clone()];

        let source_info =
            write_segment_with_secondary_indexes(&source_seg, 1, &mt, None, &indexes).unwrap();
        let source_reader = Arc::new(
            SegmentReader::open_with_info(&source_seg, &source_info, None, &indexes).unwrap(),
        );
        let compact_reader =
            compact_copy_segment_for_test(source_reader.clone(), &compact_seg, 2, &indexes);

        for (node_id, key, updated_at, labels) in [
            (1, "one", 100, vec![1]),
            (2, "two", 200, vec![2, 3]),
            (3, "ten", 300, labels_ten.to_vec()),
        ] {
            let source_node = source_reader.get_node(node_id).unwrap().unwrap();
            let compact_node = compact_reader.get_node(node_id).unwrap().unwrap();
            assert_eq!(compact_node.label_ids.as_slice(), labels.as_slice());
            assert_eq!(compact_node.label_ids, source_node.label_ids);
            for label_id in labels {
                assert_eq!(
                    source_reader
                        .node_by_key(label_id, key)
                        .unwrap()
                        .map(|node| node.id),
                    compact_reader
                        .node_by_key(label_id, key)
                        .unwrap()
                        .map(|node| node.id)
                );
                assert_eq!(
                    source_reader.nodes_by_label_id(label_id).unwrap(),
                    compact_reader.nodes_by_label_id(label_id).unwrap()
                );
                assert_eq!(
                    source_reader
                        .nodes_by_time_range(label_id, updated_at, updated_at)
                        .unwrap(),
                    compact_reader
                        .nodes_by_time_range(label_id, updated_at, updated_at)
                        .unwrap()
                );
            }
        }
        assert!(compact_reader.node_by_key(1, "two").unwrap().is_none());
        assert!(compact_reader.node_by_key(9, "ten").unwrap().is_none());

        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        assert_eq!(
            compact_reader
                .find_nodes_by_secondary_eq_index(eq_entry.index_id, red_hash)
                .unwrap(),
            vec![2]
        );
        let encoded_score = numeric_range_sort_key_for_value(&PropValue::Int(30)).unwrap();
        assert_eq!(
            compact_reader
                .find_nodes_by_secondary_range_index_if_present(
                    range_entry.index_id,
                    Some((encoded_score, true)),
                    Some((encoded_score, true)),
                    None,
                )
                .unwrap(),
            Some(vec![(encoded_score, 3)])
        );

        let flush_stats = source_reader.planner_stats().unwrap();
        let compact_stats = compact_reader.planner_stats().unwrap();
        let flush_label_counts = flush_stats
            .node_label_stats
            .iter()
            .map(|stats| (stats.label_id, stats.node_count))
            .collect::<BTreeMap<_, _>>();
        let expected_label_counts = ([(1, 1), (2, 1), (3, 1)])
            .into_iter()
            .chain(labels_ten.into_iter().map(|label_id| (label_id, 1)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(flush_stats.node_count, 3);
        assert_eq!(flush_label_counts, expected_label_counts);
        assert_eq!(
            flush_stats
                .node_label_stats
                .iter()
                .map(|stats| stats.node_count)
                .sum::<u64>(),
            13
        );
        for node_label_stats in &flush_stats.node_label_stats {
            let timestamp_stats = flush_stats
                .timestamp_stats
                .iter()
                .find(|stats| stats.label_id == node_label_stats.label_id)
                .unwrap();
            assert_eq!(timestamp_stats.count, node_label_stats.node_count);
        }
        let color_declared_property = flush_stats
            .property_stats
            .iter()
            .find(|stats| stats.label_id == 3 && stats.prop_key == "color")
            .unwrap();
        assert_eq!(color_declared_property.present_count, 1);
        let score_declared_property = flush_stats
            .property_stats
            .iter()
            .find(|stats| stats.label_id == 12 && stats.prop_key == "score")
            .unwrap();
        assert_eq!(score_declared_property.present_count, 1);
        assert_eq!(flush_stats.equality_index_stats[0].total_postings, 1);
        assert_eq!(flush_stats.range_index_stats[0].total_entries, 1);

        assert_eq!(compact_stats.node_label_stats, flush_stats.node_label_stats);
        assert_eq!(compact_stats.timestamp_stats, flush_stats.timestamp_stats);
        assert_eq!(compact_stats.property_stats, flush_stats.property_stats);
        assert_eq!(
            compact_stats.equality_index_stats,
            flush_stats.equality_index_stats
        );
        assert_eq!(
            compact_stats.range_index_stats,
            flush_stats.range_index_stats
        );
    }

    #[test]
    fn test_compaction_planner_stats_marks_general_property_decode_budget() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_seg = source_dir.path().join("seg_0001");
        let compact_dir = tempfile::tempdir().unwrap();
        let compact_seg = compact_dir.path().join("seg_0002");

        let mt = Memtable::new();
        for id in 1..=1025u64 {
            let mut props = BTreeMap::new();
            props.insert("sampled".to_string(), PropValue::UInt(id));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_custom_props(
                    id,
                    1,
                    &format!("n{}", id),
                    props,
                    id as i64,
                )),
                id,
            );
        }
        write_segment(&source_seg, 1, &mt, None).unwrap();
        let source_reader =
            Arc::new(SegmentReader::open_unpinned_for_test(&source_seg, 1, None).unwrap());
        let compact_reader = compact_copy_segment_for_test(source_reader, &compact_seg, 2, &[]);
        let stats = compact_reader.planner_stats().unwrap();

        assert!(!stats.general_property_stats_complete);
        assert_eq!(stats.general_property_sampled_node_count, 1024);
        assert!(stats.general_property_budget_exhausted);
        let sampled = stats
            .property_stats
            .iter()
            .find(|prop| prop.label_id == 1 && prop.prop_key == "sampled")
            .unwrap();
        assert_eq!(sampled.present_count, 1024);
    }

    #[test]
    fn test_planner_stats_final_tmp_collision_does_not_block_segment_publish() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        std::fs::create_dir_all(seg_dir.join("planner_stats.tmp")).unwrap();

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();
        assert_eq!(info.node_count, 1);
        assert!(seg_dir.join(PACKED_CORE_FILENAME).exists());
        assert!(seg_dir
            .join(crate::planner_stats::PLANNER_STATS_FILENAME)
            .exists());

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(reader.get_node(1).unwrap().is_some());
        assert!(reader.planner_stats_available());
    }
}
