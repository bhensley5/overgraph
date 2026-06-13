use crate::degree_cache::{DegreeDelta, DegreeSidecar};
use crate::dense_hnsw::{
    dense_score_from_bytes, load_dense_hnsw_query_points, search_dense_hnsw_scoped_with_points,
    search_dense_hnsw_with_points, validate_dense_hnsw_files_for_open, DenseHnswHeader,
    DenseQueryPoint,
};
use crate::edge_metadata::{
    encode_edge_weight_key, EdgeMetadataCandidate, RangeBoundFlags,
    EDGE_I64_METADATA_INDEX_ENTRY_SIZE, EDGE_UPDATED_AT_INDEX_LOGICAL_NAME,
    EDGE_VALID_FROM_INDEX_LOGICAL_NAME, EDGE_VALID_TO_INDEX_LOGICAL_NAME,
    EDGE_WEIGHT_INDEX_ENTRY_SIZE, EDGE_WEIGHT_INDEX_LOGICAL_NAME,
};
use crate::error::EngineError;
#[cfg(test)]
use crate::planner_stats::SegmentPlannerStatsV1;
use crate::planner_stats::{
    planner_stats_declared_index_target, read_planner_stats_payload,
    DeclaredIndexRuntimeCoverageState, PlannerStatsAvailability, PlannerStatsDeclaredIndexKind,
    PlannerStatsDeclaredIndexTarget,
};
#[cfg(test)]
use crate::property_value_semantics::hash_prop_equality_key;
use crate::property_value_semantics::{NumericRangeSortKey, NUMERIC_RANGE_KEY_BYTES};
use crate::row_projection::{EdgeSelectedFieldNeeds, NodeSelectedFieldNeeds, PropertySelection};
use crate::secondary_index_key::{
    for_each_compound_sidecar_entry as for_each_compound_sidecar_payload_entry,
    scan_compound_sidecar_prefix_limited, scan_compound_sidecar_range_limited,
    validate_compound_sidecar_header_only, validate_compound_sidecar_payload, CompoundPrefixBounds,
    CompoundRangeBounds, CompoundSidecarDeclaration,
};
use crate::segment_components::{
    component_id, compound_component_kind_for_entry, decode_identity_header,
    decode_manifest_envelope, dependency_digest, is_packed_core_component_kind,
    packed_core_container_record, secondary_declaration_dependency,
    secondary_index_component_dependencies_for_entry,
    secondary_index_declaration_fingerprint_for_entry, segment_source_groups_from_records,
    source_component_dependency, source_group_dependency,
    validate_packed_core_manifest_contract_for_open, ComponentAvailability, ComponentDependencyV1,
    ComponentFallbackClass, ComponentHandleV1, ComponentRequirement, ComponentTrustClass,
    SegmentComponentKind, SegmentComponentManifestV1, SegmentComponentRecordV1,
    SegmentComponentSourceGroups, SegmentSourceGroupKind, PACKED_CORE_FILENAME,
    SEGMENT_COMPONENT_MANIFEST_FILENAME,
};
use crate::segment_writer::{
    component_fingerprint, compound_component_fingerprint_for_kind_and_entry,
    dense_config_fingerprint, edge_property_equality_component_fingerprint,
    edge_property_range_component_fingerprint, node_property_equality_component_fingerprint,
    node_property_range_component_fingerprint, planner_stats_component_dependencies,
    planner_stats_component_fingerprint, NODE_VECTOR_META_ENTRY_SIZE, SEGMENT_FORMAT_VERSION,
};
use crate::sparse_postings::{
    accumulate_sparse_posting_scores as accumulate_sparse_posting_scores_from_bytes,
    validate_sparse_posting_files_for_open, validate_sparse_posting_index_shape_for_search,
};
use crate::types::*;
use memmap2::Mmap;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::ops::ControlFlow;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

type NodeSelectedFieldsAtOffset = (Option<String>, BTreeMap<String, PropValue>, Option<i64>);

/// Segment file data, either mmap'd or an empty placeholder.
/// Segment component payloads can be 0 bytes when empty.
/// memmap2 can't map empty files, so we handle that case explicitly.
enum MappedData {
    Mmap {
        mmap: Arc<Mmap>,
        payload_offset: usize,
        payload_len: usize,
    },
    Empty,
}

#[derive(Clone, Copy)]
pub(crate) enum SegmentAdjacencyFile {
    Out,
    In,
}

pub(crate) struct SegmentAdjPostingCursor {
    file: SegmentAdjacencyFile,
    cur_off: usize,
    remaining: usize,
    prev_edge_id: u64,
}

struct SecondaryEqSidecarCacheEntry {
    data: MappedData,
    validated: bool,
    index_validated: bool,
}

struct SecondaryRangeSidecarCacheEntry {
    data: MappedData,
    validated: bool,
    header_validated: bool,
}

fn secondary_eq_component_kind(
    index_id: u64,
    target: PlannerStatsDeclaredIndexTarget,
) -> SegmentComponentKind {
    match target {
        PlannerStatsDeclaredIndexTarget::NodeProperty => {
            SegmentComponentKind::NodePropertyEqualityIndex { index_id }
        }
        PlannerStatsDeclaredIndexTarget::EdgeProperty => {
            SegmentComponentKind::EdgePropertyEqualityIndex { index_id }
        }
        PlannerStatsDeclaredIndexTarget::NodeFieldIndex
        | PlannerStatsDeclaredIndexTarget::EdgeFieldIndex => {
            unreachable!("compound equality indexes do not use legacy equality sidecars")
        }
    }
}

fn secondary_range_component_kind(
    index_id: u64,
    target: PlannerStatsDeclaredIndexTarget,
) -> SegmentComponentKind {
    match target {
        PlannerStatsDeclaredIndexTarget::NodeProperty => {
            SegmentComponentKind::NodePropertyRangeIndex { index_id }
        }
        PlannerStatsDeclaredIndexTarget::EdgeProperty => {
            SegmentComponentKind::EdgePropertyRangeIndex { index_id }
        }
        PlannerStatsDeclaredIndexTarget::NodeFieldIndex
        | PlannerStatsDeclaredIndexTarget::EdgeFieldIndex => {
            unreachable!("compound range indexes do not use legacy range sidecars")
        }
    }
}

struct SegmentComponentRegistry {
    segment_id: u64,
    records: HashMap<SegmentComponentKind, SegmentComponentRecordV1>,
    availability: Mutex<HashMap<SegmentComponentKind, ComponentAvailability>>,
}

impl SegmentComponentRegistry {
    fn new(manifest: &SegmentComponentManifestV1) -> Self {
        Self {
            segment_id: manifest.segment_id,
            records: manifest
                .components
                .iter()
                .map(|record| (record.kind.clone(), record.clone()))
                .collect(),
            availability: Mutex::new(HashMap::new()),
        }
    }

    fn record(&self, kind: &SegmentComponentKind) -> Option<&SegmentComponentRecordV1> {
        self.records.get(kind)
    }

    fn set_availability(&self, kind: SegmentComponentKind, state: ComponentAvailability) {
        self.availability.lock().unwrap().insert(kind, state);
    }

    fn availability(&self, kind: &SegmentComponentKind) -> ComponentAvailability {
        self.availability
            .lock()
            .unwrap()
            .get(kind)
            .cloned()
            .unwrap_or(ComponentAvailability::Missing)
    }

    fn recorded_availability(&self, kind: &SegmentComponentKind) -> Option<ComponentAvailability> {
        self.availability.lock().unwrap().get(kind).cloned()
    }
}

struct PackedCoreMapping {
    component_id: crate::segment_components::ComponentDigest32,
    mmap: Arc<Mmap>,
    payload_offset: usize,
    payload_len: usize,
}

struct ComponentOpenContext {
    packed_core: Option<PackedCoreMapping>,
    invalid_optional_packed_ranges: HashMap<SegmentComponentKind, String>,
}

impl Deref for MappedData {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            MappedData::Mmap {
                mmap,
                payload_offset,
                payload_len,
            } => &mmap[*payload_offset..*payload_offset + *payload_len],
            MappedData::Empty => &[],
        }
    }
}

impl MappedData {
    #[cfg(test)]
    fn mapping_identity_for_test(&self) -> Option<usize> {
        match self {
            MappedData::Mmap { mmap, .. } => Some(Arc::as_ptr(mmap) as usize),
            MappedData::Empty => None,
        }
    }
}

// --- Binary read helpers (little-endian, from byte slices) ---

fn read_u16_at(data: &[u8], offset: usize) -> Result<u16, EngineError> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| EngineError::CorruptRecord("u16 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u16 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    // unwrap safe: slice is exactly 2 bytes, guaranteed by get() above
    Ok(u16::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u8_at(data: &[u8], offset: usize) -> Result<u8, EngineError> {
    data.get(offset).copied().ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u8 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })
}

fn read_u32_at(data: &[u8], offset: usize) -> Result<u32, EngineError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| EngineError::CorruptRecord("u32 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u32 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    // unwrap safe: slice is exactly 4 bytes, guaranteed by get() above
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64_at(data: &[u8], offset: usize) -> Result<u64, EngineError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| EngineError::CorruptRecord("u64 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u64 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    // unwrap safe: slice is exactly 8 bytes, guaranteed by get() above
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

fn collect_node_ids(nodes_data: &[u8]) -> Result<Vec<u64>, EngineError> {
    if nodes_data.len() < 8 {
        return Ok(Vec::new());
    }
    let count = read_u64_at(nodes_data, 0)? as usize;
    let mut ids = Vec::with_capacity(count);
    let idx_start = 8;
    for index in 0..count {
        let entry_off = idx_start + index * NODE_INDEX_ENTRY_SIZE;
        ids.push(read_u64_at(nodes_data, entry_off)?);
    }
    Ok(ids)
}

fn read_i64_at(data: &[u8], offset: usize) -> Result<i64, EngineError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| EngineError::CorruptRecord("i64 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "i64 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    // unwrap safe: slice is exactly 8 bytes, guaranteed by get() above
    Ok(i64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_f32_at(data: &[u8], offset: usize) -> Result<f32, EngineError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| EngineError::CorruptRecord("f32 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "f32 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    // unwrap safe: slice is exactly 4 bytes, guaranteed by get() above
    Ok(f32::from_le_bytes(slice.try_into().unwrap()))
}

fn usize_from_u64(value: u64, context: &str) -> Result<usize, EngineError> {
    usize::try_from(value).map_err(|_| {
        EngineError::CorruptRecord(format!("{context} does not fit in usize: {value}"))
    })
}

fn checked_range_end(start: usize, len: usize, context: &str) -> Result<usize, EngineError> {
    start
        .checked_add(len)
        .ok_or_else(|| EngineError::CorruptRecord(format!("{context} range overflow")))
}

fn parse_node_meta_layout(data: &[u8]) -> Result<Option<NodeMetaLayout>, EngineError> {
    if data.len() < 8 {
        return Ok(None);
    }
    let node_count = usize_from_u64(read_u64_at(data, 0)?, "node metadata count")?;
    if node_count == 0 {
        return Ok(Some(NodeMetaLayout {
            node_count,
            fixed_entry_size: NODE_META_FIXED_ENTRY_SIZE,
            label_offset_entry_size: NODE_META_LABEL_OFFSET_ENTRY_SIZE,
            fixed_entries_offset: NODE_META_HEADER_SIZE,
            label_offsets_offset: NODE_META_HEADER_SIZE,
            label_ids_offset: NODE_META_HEADER_SIZE,
            label_id_count: 0,
        }));
    }
    if data.len() < NODE_META_HEADER_SIZE {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata header length {} is shorter than {}",
            data.len(),
            NODE_META_HEADER_SIZE
        )));
    }

    let fixed_entry_size = read_u16_at(data, 8)? as usize;
    let label_offset_entry_size = read_u16_at(data, 10)? as usize;
    if fixed_entry_size != NODE_META_FIXED_ENTRY_SIZE {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported node metadata fixed entry size {}, expected {}",
            fixed_entry_size, NODE_META_FIXED_ENTRY_SIZE
        )));
    }
    if label_offset_entry_size != NODE_META_LABEL_OFFSET_ENTRY_SIZE {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported node metadata label offset entry size {}, expected {}",
            label_offset_entry_size, NODE_META_LABEL_OFFSET_ENTRY_SIZE
        )));
    }

    let fixed_entries_offset =
        usize_from_u64(read_u64_at(data, 16)?, "node metadata fixed entries offset")?;
    let label_offsets_offset =
        usize_from_u64(read_u64_at(data, 24)?, "node metadata label offsets offset")?;
    let label_ids_offset =
        usize_from_u64(read_u64_at(data, 32)?, "node metadata label IDs offset")?;
    let label_id_count = usize_from_u64(read_u64_at(data, 40)?, "node metadata label ID count")?;

    let fixed_bytes = node_count
        .checked_mul(fixed_entry_size)
        .ok_or_else(|| EngineError::CorruptRecord("node metadata fixed table overflow".into()))?;
    let label_offset_entries = node_count.checked_add(1).ok_or_else(|| {
        EngineError::CorruptRecord("node metadata label offset count overflow".into())
    })?;
    let label_offset_bytes = label_offset_entries
        .checked_mul(label_offset_entry_size)
        .ok_or_else(|| {
            EngineError::CorruptRecord("node metadata label offset table overflow".into())
        })?;
    let label_id_bytes = label_id_count.checked_mul(4).ok_or_else(|| {
        EngineError::CorruptRecord("node metadata label ID region overflow".into())
    })?;

    if checked_range_end(
        fixed_entries_offset,
        fixed_bytes,
        "node metadata fixed table",
    )? > data.len()
    {
        return Err(EngineError::CorruptRecord(
            "node metadata fixed table exceeds payload length".into(),
        ));
    }
    if checked_range_end(
        label_offsets_offset,
        label_offset_bytes,
        "node metadata label offset table",
    )? > data.len()
    {
        return Err(EngineError::CorruptRecord(
            "node metadata label offset table exceeds payload length".into(),
        ));
    }
    if checked_range_end(
        label_ids_offset,
        label_id_bytes,
        "node metadata label ID region",
    )? > data.len()
    {
        return Err(EngineError::CorruptRecord(
            "node metadata label ID region exceeds payload length".into(),
        ));
    }
    if label_id_count > node_count.saturating_mul(MAX_NODE_LABELS_PER_NODE) {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata label ID count {} exceeds maximum {}",
            label_id_count,
            node_count.saturating_mul(MAX_NODE_LABELS_PER_NODE)
        )));
    }
    let first_label_offset = usize_from_u64(
        read_u64_at(data, label_offsets_offset)?,
        "node metadata first label offset",
    )?;
    if first_label_offset != 0 {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata first label offset must be 0, got {}",
            first_label_offset
        )));
    }
    let terminal_offset_pos = label_offsets_offset
        .checked_add(
            node_count
                .checked_mul(label_offset_entry_size)
                .ok_or_else(|| {
                    EngineError::CorruptRecord(
                        "node metadata terminal label offset overflow".into(),
                    )
                })?,
        )
        .ok_or_else(|| {
            EngineError::CorruptRecord("node metadata terminal label offset overflow".into())
        })?;
    let terminal_label_offset = usize_from_u64(
        read_u64_at(data, terminal_offset_pos)?,
        "node metadata terminal label offset",
    )?;
    if terminal_label_offset != label_id_count {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata terminal label offset {} does not match label ID count {}",
            terminal_label_offset, label_id_count
        )));
    }

    Ok(Some(NodeMetaLayout {
        node_count,
        fixed_entry_size,
        label_offset_entry_size,
        fixed_entries_offset,
        label_offsets_offset,
        label_ids_offset,
        label_id_count,
    }))
}

fn read_node_meta_entry_at(
    data: &[u8],
    layout: NodeMetaLayout,
    index: usize,
) -> Result<SegmentNodeMeta, EngineError> {
    if index >= layout.node_count {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata index {} out of bounds for count {}",
            index, layout.node_count
        )));
    }
    let off = layout
        .fixed_entries_offset
        .checked_add(index.checked_mul(layout.fixed_entry_size).ok_or_else(|| {
            EngineError::CorruptRecord("node metadata fixed entry offset overflow".into())
        })?)
        .ok_or_else(|| {
            EngineError::CorruptRecord("node metadata fixed entry offset overflow".into())
        })?;
    let node_id = read_u64_at(data, off)?;
    let data_offset = read_u64_at(data, off + 8)?;
    let data_len = read_u32_at(data, off + 16)?;
    let updated_at = read_i64_at(data, off + 20)?;
    let weight = read_f32_at(data, off + 28)?;
    let key_len = read_u16_at(data, off + 32)?;
    let last_write_seq = read_u64_at(data, off + 34)?;

    let label_offset_pos = layout
        .label_offsets_offset
        .checked_add(
            index
                .checked_mul(layout.label_offset_entry_size)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("node metadata label offset entry overflow".into())
                })?,
        )
        .ok_or_else(|| {
            EngineError::CorruptRecord("node metadata label offset entry overflow".into())
        })?;
    let label_start = usize_from_u64(
        read_u64_at(data, label_offset_pos)?,
        "node metadata label start offset",
    )?;
    let label_end = usize_from_u64(
        read_u64_at(data, label_offset_pos + layout.label_offset_entry_size)?,
        "node metadata label end offset",
    )?;
    if label_start > label_end {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata label offsets are not monotonic at row {}: {} > {}",
            index, label_start, label_end
        )));
    }
    if label_end > layout.label_id_count {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata label end {} exceeds label ID count {}",
            label_end, layout.label_id_count
        )));
    }
    let label_count = label_end - label_start;
    if label_count == 0 || label_count > MAX_NODE_LABELS_PER_NODE {
        return Err(EngineError::CorruptRecord(format!(
            "node metadata row {} has invalid label count {}",
            index, label_count
        )));
    }

    let mut label_ids = [0u32; MAX_NODE_LABELS_PER_NODE];
    for label_index in 0..label_count {
        let label_pos = layout
            .label_ids_offset
            .checked_add((label_start + label_index).checked_mul(4).ok_or_else(|| {
                EngineError::CorruptRecord("node metadata label ID offset overflow".into())
            })?)
            .ok_or_else(|| {
                EngineError::CorruptRecord("node metadata label ID offset overflow".into())
            })?;
        label_ids[label_index] = read_u32_at(data, label_pos)?;
        if label_index > 0 && label_ids[label_index - 1] >= label_ids[label_index] {
            return Err(EngineError::CorruptRecord(
                "node metadata label IDs must be sorted ascending and unique".into(),
            ));
        }
    }
    let label_ids = NodeLabelSet::from_canonical_ids(&label_ids[..label_count]).map_err(|err| {
        EngineError::CorruptRecord(format!("invalid node metadata label set: {err}"))
    })?;

    Ok(SegmentNodeMeta {
        node_id,
        data_offset,
        data_len,
        label_ids,
        updated_at,
        weight,
        key_len,
        last_write_seq,
    })
}

fn binary_search_node_meta_index(
    data: &[u8],
    layout: NodeMetaLayout,
    target_id: u64,
) -> Result<Option<usize>, EngineError> {
    let mut lo = 0usize;
    let mut hi = layout.node_count;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_off = layout
            .fixed_entries_offset
            .checked_add(mid.checked_mul(layout.fixed_entry_size).ok_or_else(|| {
                EngineError::CorruptRecord("node metadata fixed entry offset overflow".into())
            })?)
            .ok_or_else(|| {
                EngineError::CorruptRecord("node metadata fixed entry offset overflow".into())
            })?;
        let id = read_u64_at(data, entry_off)?;
        if id < target_id {
            lo = mid + 1;
        } else if id > target_id {
            hi = mid;
        } else {
            return Ok(Some(mid));
        }
    }
    Ok(None)
}

/// Read a LEB128 varint from data starting at `offset`.
/// Returns (value, bytes_consumed).
fn read_varint_at(data: &[u8], offset: usize) -> Result<(u64, usize), EngineError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut pos = offset;
    loop {
        if pos >= data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "varint read at offset {} exceeds data length {}",
                offset,
                data.len()
            )));
        }
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos - offset));
        }
        shift += 7;
        if shift >= 70 {
            return Err(EngineError::CorruptRecord("varint too long".into()));
        }
    }
}

#[inline]
fn checked_adj_edge_id_delta(prev_edge_id: u64, delta: u64) -> Result<u64, EngineError> {
    prev_edge_id
        .checked_add(delta)
        .ok_or_else(|| EngineError::CorruptRecord("adjacency edge id delta overflow".into()))
}

/// Safe byte slice extraction with bounds checking.
fn read_bytes_at(data: &[u8], offset: usize, len: usize) -> Result<&[u8], EngineError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| EngineError::CorruptRecord("byte slice offset overflow".into()))?;
    data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "byte slice [{}, {}) exceeds data length {}",
            offset,
            end,
            data.len()
        ))
    })
}

// Cost model constants for adaptive batch strategy selection.
// We estimate:
// - seek cost ~= K * log2(N) * random_access_penalty
// - merge cost ~= index span touched between min/max requested keys
const BATCH_RANDOM_ACCESS_PENALTY: usize = 4;

// --- Index entry sizes ---

const NODE_INDEX_ENTRY_SIZE: usize = 16; // node_id (8) + offset (8)
const EDGE_INDEX_ENTRY_SIZE: usize = 16; // edge_id (8) + offset (8)
const ADJ_INDEX_ENTRY_SIZE: usize = 24; // node_id (8) + label_id (4) + offset (8) + count (4)
                                        // ADJ_POSTING_SIZE removed. Postings are now variable-length (delta + varint encoded)
const TOMBSTONE_ENTRY_SIZE: usize = 25; // kind (1) + id (8) + deleted_at (8) + last_write_seq (8)
const LABEL_POSTING_INDEX_ENTRY_SIZE: usize = 16; // label_id (4) + offset (8) + count (4)
const EDGE_TRIPLE_ENTRY_SIZE: usize = 28; // from (8) + to (8) + label_id (4) + edge_id (8)
const SECONDARY_EQ_ENTRY_SIZE: usize = 20; // value_hash (8) + offset (8) + count (4)
const SECONDARY_RANGE_ENTRY_SIZE: usize = NUMERIC_RANGE_KEY_BYTES + 8; // numeric key (24) + id (8)

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BatchReadStrategy {
    SeekPerKey,
    MergeWalk,
}

#[inline]
fn ceil_log2_usize(n: usize) -> usize {
    if n <= 1 {
        0
    } else {
        (usize::BITS - (n - 1).leading_zeros()) as usize
    }
}

/// Lower bound for a sorted fixed-width u64-key index. Returns the first index
/// with key >= target in [0, count].
fn lower_bound_u64_index(
    data: &[u8],
    idx_start: usize,
    count: usize,
    entry_size: usize,
    key_offset: usize,
    target: u64,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = idx_start + mid * entry_size + key_offset;
        let key = read_u64_at(data, off)?;
        if key < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Upper bound for a sorted fixed-width u64-key index. Returns the first index
/// with key > target in [0, count].
fn upper_bound_u64_index(
    data: &[u8],
    idx_start: usize,
    count: usize,
    entry_size: usize,
    key_offset: usize,
    target: u64,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = idx_start + mid * entry_size + key_offset;
        let key = read_u64_at(data, off)?;
        if key <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Choose between per-key binary seek and merge-walk using a lightweight
/// shared cost model reused across batch index readers.
#[allow(clippy::too_many_arguments)]
fn choose_batch_read_strategy(
    index_data: &[u8],
    idx_start: usize,
    index_count: usize,
    entry_size: usize,
    key_offset: usize,
    unique_keys: usize,
    min_key: u64,
    max_key: u64,
) -> Result<BatchReadStrategy, EngineError> {
    if unique_keys <= 2 || index_count <= 1 {
        return Ok(BatchReadStrategy::SeekPerKey);
    }

    let span_start = lower_bound_u64_index(
        index_data,
        idx_start,
        index_count,
        entry_size,
        key_offset,
        min_key,
    )?;
    let span_end = upper_bound_u64_index(
        index_data,
        idx_start,
        index_count,
        entry_size,
        key_offset,
        max_key,
    )?;
    let span = span_end.saturating_sub(span_start).max(unique_keys);

    let seek_cost = unique_keys
        .saturating_mul(ceil_log2_usize(index_count))
        .saturating_mul(BATCH_RANDOM_ACCESS_PENALTY);

    if seek_cost <= span {
        Ok(BatchReadStrategy::SeekPerKey)
    } else {
        Ok(BatchReadStrategy::MergeWalk)
    }
}

/// Lower bound for the key index (variable-length entries addressed via offset
/// table). Returns the first entry index where `(label_id, key) >= (target_label,
/// target_key)`, in [0, count].
fn lower_bound_key_index(
    data: &[u8],
    offset_table_start: usize,
    count: usize,
    target_label: u32,
    target_key: &str,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_offset = read_u64_at(data, offset_table_start + mid * 8)? as usize;
        let entry_label_id = read_u32_at(data, entry_offset)?;
        let key_len = read_u16_at(data, entry_offset + 12)? as usize;
        let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
        let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
            EngineError::CorruptRecord(format!(
                "invalid UTF-8 in key index at offset {}",
                entry_offset + 14
            ))
        })?;
        if (entry_label_id, entry_key) < (target_label, target_key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Upper bound for the key index. Returns the first entry index where
/// `(label_id, key) > (target_label, target_key)`, in [0, count].
fn upper_bound_key_index(
    data: &[u8],
    offset_table_start: usize,
    count: usize,
    target_label: u32,
    target_key: &str,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_offset = read_u64_at(data, offset_table_start + mid * 8)? as usize;
        let entry_label_id = read_u32_at(data, entry_offset)?;
        let key_len = read_u16_at(data, entry_offset + 12)? as usize;
        let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
        let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
            EngineError::CorruptRecord(format!(
                "invalid UTF-8 in key index at offset {}",
                entry_offset + 14
            ))
        })?;
        if (entry_label_id, entry_key) <= (target_label, target_key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn read_edge_triple_key_at(
    data: &[u8],
    entries_start: usize,
    index: usize,
) -> Result<(u64, u64, u32), EngineError> {
    let off = entries_start + index * EDGE_TRIPLE_ENTRY_SIZE;
    Ok((
        read_u64_at(data, off)?,
        read_u64_at(data, off + 8)?,
        read_u32_at(data, off + 16)?,
    ))
}

fn lower_bound_edge_triple_index(
    data: &[u8],
    entries_start: usize,
    count: usize,
    target: (u64, u64, u32),
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if read_edge_triple_key_at(data, entries_start, mid)? < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn upper_bound_edge_triple_index(
    data: &[u8],
    entries_start: usize,
    count: usize,
    target: (u64, u64, u32),
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if read_edge_triple_key_at(data, entries_start, mid)? <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn binary_search_edge_triple_index(
    data: &[u8],
    entries_start: usize,
    count: usize,
    target: (u64, u64, u32),
) -> Result<Option<u64>, EngineError> {
    let pos = lower_bound_edge_triple_index(data, entries_start, count, target)?;
    if pos >= count || read_edge_triple_key_at(data, entries_start, pos)? != target {
        return Ok(None);
    }
    Ok(Some(read_u64_at(
        data,
        entries_start + pos * EDGE_TRIPLE_ENTRY_SIZE + 20,
    )?))
}

/// An mmap-backed reader for an immutable segment directory.
///
/// Provides O(log N) lookups by ID for nodes and edges, adjacency queries
/// from pre-built indexes, key-based lookups, and tombstone checks.
/// Size of a node metadata payload entry (60 bytes, v9).
const NODE_META_HEADER_SIZE: usize = 48;
const NODE_META_FIXED_ENTRY_SIZE: usize = 48;
const NODE_META_LABEL_OFFSET_ENTRY_SIZE: usize = 8;

#[derive(Clone, Copy)]
struct NodeMetaLayout {
    node_count: usize,
    fixed_entry_size: usize,
    label_offset_entry_size: usize,
    fixed_entries_offset: usize,
    label_offsets_offset: usize,
    label_ids_offset: usize,
    label_id_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentNodeMeta {
    pub(crate) node_id: u64,
    pub(crate) data_offset: u64,
    pub(crate) data_len: u32,
    pub(crate) label_ids: NodeLabelSet,
    pub(crate) updated_at: i64,
    pub(crate) weight: f32,
    pub(crate) key_len: u16,
    pub(crate) last_write_seq: u64,
}
/// Size of an edge metadata payload entry (80 bytes, v9).
const EDGE_META_ENTRY_SIZE: usize = 80;
const NODE_VECTOR_FLAG_DENSE: u8 = 0b0000_0001;
const NODE_VECTOR_FLAG_SPARSE: u8 = 0b0000_0010;
const DENSE_VECTOR_VALUE_SIZE: usize = 4;
const SPARSE_VECTOR_ENTRY_SIZE: usize = 8;

fn edge_metadata_entry_count(
    data: &[u8],
    entry_size: usize,
    logical_name: &str,
) -> Result<usize, EngineError> {
    if data.len() < 8 {
        return Err(EngineError::CorruptRecord(format!(
            "{logical_name} missing or truncated count header"
        )));
    }
    let count = usize::try_from(read_u64_at(data, 0)?).map_err(|_| {
        EngineError::CorruptRecord(format!("{logical_name} count exceeds addressable memory"))
    })?;
    let expected_len = 8usize
        .checked_add(count.checked_mul(entry_size).ok_or_else(|| {
            EngineError::CorruptRecord(format!("{logical_name} entry count overflow"))
        })?)
        .ok_or_else(|| EngineError::CorruptRecord(format!("{logical_name} length overflow")))?;
    if expected_len != data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "{logical_name} length {} does not match count {} and entry size {}",
            data.len(),
            count,
            entry_size
        )));
    }
    Ok(count)
}

#[cfg(test)]
fn validate_edge_weight_index_data(data: &[u8]) -> Result<usize, EngineError> {
    let count = edge_metadata_entry_count(
        data,
        EDGE_WEIGHT_INDEX_ENTRY_SIZE,
        EDGE_WEIGHT_INDEX_LOGICAL_NAME,
    )?;
    let mut previous = None;
    for index in 0..count {
        let offset = 8 + index * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
        let entry = (
            read_u32_at(data, offset)?,
            read_u32_at(data, offset + 4)?,
            read_u64_at(data, offset + 8)?,
        );
        if previous.is_some_and(|prev| prev > entry) {
            return Err(EngineError::CorruptRecord(format!(
                "{EDGE_WEIGHT_INDEX_LOGICAL_NAME} is not sorted"
            )));
        }
        previous = Some(entry);
    }
    Ok(count)
}

#[cfg(test)]
fn validate_edge_i64_metadata_index_data(
    data: &[u8],
    logical_name: &str,
) -> Result<usize, EngineError> {
    let count = edge_metadata_entry_count(data, EDGE_I64_METADATA_INDEX_ENTRY_SIZE, logical_name)?;
    let mut previous = None;
    for index in 0..count {
        let offset = 8 + index * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
        let entry = (
            read_u32_at(data, offset)?,
            read_i64_at(data, offset + 4)?,
            read_u64_at(data, offset + 12)?,
        );
        if previous.is_some_and(|prev| prev > entry) {
            return Err(EngineError::CorruptRecord(format!(
                "{logical_name} is not sorted"
            )));
        }
        previous = Some(entry);
    }
    Ok(count)
}

fn mark_optional_component_corrupt(
    registry: &SegmentComponentRegistry,
    kind: SegmentComponentKind,
    reason: String,
) {
    if !matches!(
        registry.availability(&kind),
        ComponentAvailability::Missing
            | ComponentAvailability::Incompatible { .. }
            | ComponentAvailability::Unsupported { .. }
    ) {
        registry.set_availability(kind, ComponentAvailability::CorruptIdentity { reason });
    }
}

fn edge_metadata_index_count_from_header(
    registry: &SegmentComponentRegistry,
    kind: SegmentComponentKind,
    mmap: &MappedData,
    entry_size: usize,
    logical_name: &str,
) -> Option<usize> {
    let data = &mmap[..];
    if data.is_empty() {
        if registry.availability(&kind).is_available() {
            mark_optional_component_corrupt(
                registry,
                kind,
                format!("{logical_name} missing or truncated count header"),
            );
        }
        return None;
    }
    match edge_metadata_entry_count(data, entry_size, logical_name) {
        Ok(count) => Some(count),
        Err(error) => {
            mark_optional_component_corrupt(registry, kind, error.to_string());
            None
        }
    }
}

fn edge_i64_metadata_component_kind(logical_name: &str) -> Option<SegmentComponentKind> {
    match logical_name {
        EDGE_UPDATED_AT_INDEX_LOGICAL_NAME => Some(SegmentComponentKind::EdgeUpdatedAtIndex),
        EDGE_VALID_FROM_INDEX_LOGICAL_NAME => Some(SegmentComponentKind::EdgeValidFromIndex),
        EDGE_VALID_TO_INDEX_LOGICAL_NAME => Some(SegmentComponentKind::EdgeValidToIndex),
        _ => None,
    }
}

fn key_matches_bounds<T: Ord>(
    value: T,
    lower: Option<T>,
    lower_inclusive: bool,
    upper: Option<T>,
    upper_inclusive: bool,
) -> bool {
    if let Some(lower) = lower {
        if lower_inclusive {
            if value < lower {
                return false;
            }
        } else if value <= lower {
            return false;
        }
    }
    if let Some(upper) = upper {
        if upper_inclusive {
            if value > upper {
                return false;
            }
        } else if value >= upper {
            return false;
        }
    }
    true
}

#[derive(Clone, Copy)]
struct DenseScoringMeta {
    label_ids: NodeLabelSet,
    updated_at: i64,
    weight: f32,
    dense_offset: usize,
    dense_len: usize,
}

#[derive(Clone, Copy)]
struct SparseScoringMeta {
    label_ids: NodeLabelSet,
    updated_at: i64,
    weight: f32,
    sparse_offset: usize,
    sparse_len: usize,
}

pub struct SegmentReader {
    pub segment_id: u64,
    seg_dir: PathBuf,
    segment_data_id: [u8; 32],
    component_manifest_generation: u64,
    component_registry: SegmentComponentRegistry,
    nodes_mmap: MappedData,
    edges_mmap: MappedData,
    adj_out_idx: MappedData,
    adj_out_dat: MappedData,
    adj_in_idx: MappedData,
    adj_in_dat: MappedData,
    key_index_mmap: MappedData,
    node_label_index_mmap: MappedData,
    edge_label_index_mmap: MappedData,
    edge_triple_index_mmap: MappedData,
    edge_weight_index_mmap: MappedData,
    edge_weight_index_count: Option<usize>,
    edge_updated_at_index_mmap: MappedData,
    edge_updated_at_index_count: Option<usize>,
    edge_valid_from_index_mmap: MappedData,
    edge_valid_from_index_count: Option<usize>,
    edge_valid_to_index_mmap: MappedData,
    edge_valid_to_index_count: Option<usize>,
    // Metadata payloads
    node_meta_mmap: MappedData,
    edge_meta_mmap: MappedData,
    node_vector_meta_mmap: MappedData,
    node_dense_vectors_mmap: MappedData,
    node_sparse_vectors_mmap: MappedData,
    dense_hnsw_meta_mmap: MappedData,
    dense_hnsw_graph_mmap: MappedData,
    dense_hnsw_header: Option<DenseHnswHeader>,
    dense_hnsw_available: AtomicBool,
    dense_hnsw_points: OnceLock<Result<Box<[DenseQueryPoint]>, String>>,
    dense_vector_count: usize,
    sparse_vector_count: usize,
    sparse_posting_index_mmap: MappedData,
    sparse_postings_mmap: MappedData,
    sparse_postings_available: AtomicBool,
    sparse_posting_index_shape: OnceLock<Result<(), String>>,
    degree_delta: Option<DegreeSidecar>,
    planner_stats: PlannerStatsAvailability,
    // Timestamp range index
    timestamp_index_mmap: MappedData,
    deleted_nodes: NodeIdMap<TombstoneEntry>,
    deleted_edges: NodeIdMap<TombstoneEntry>,
    secondary_eq_sidecars:
        Mutex<HashMap<(u64, PlannerStatsDeclaredIndexTarget), SecondaryEqSidecarCacheEntry>>,
    secondary_range_sidecars:
        Mutex<HashMap<(u64, PlannerStatsDeclaredIndexTarget), SecondaryRangeSidecarCacheEntry>>,
    declared_index_runtime_coverage: Mutex<
        HashMap<
            (
                u64,
                PlannerStatsDeclaredIndexTarget,
                PlannerStatsDeclaredIndexKind,
            ),
            DeclaredIndexRuntimeCoverageState,
        >,
    >,
    node_ids: OnceLock<Box<[u64]>>,
    node_count: u64,
    edge_count: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct SegmentLabelPosting {
    offset: usize,
    count: usize,
}

pub(crate) struct SecondaryEqPostingChunk {
    pub(crate) ids: Vec<u64>,
    pub(crate) next_offset: usize,
    pub(crate) exhausted: bool,
}

impl SegmentReader {
    /// Test-only unpinned open helper.
    ///
    /// Production callers must pass the root manifest's `SegmentInfo` through
    /// `open_with_info` so the local segment manifest is pinned to the DB
    /// manifest.
    #[cfg(test)]
    pub(crate) fn open_unpinned_for_test(
        seg_dir: &Path,
        segment_id: u64,
        dense_config: Option<&DenseVectorConfig>,
    ) -> Result<Self, EngineError> {
        let component_manifest = load_component_manifest(seg_dir, segment_id)?;
        let segment_info = SegmentInfo {
            id: segment_id,
            node_count: component_manifest.node_count,
            edge_count: component_manifest.edge_count,
            segment_format_version: component_manifest.segment_format_version,
            segment_data_id: component_manifest.segment_data_id,
        };
        Self::open_with_info(seg_dir, &segment_info, dense_config, &[])
    }

    pub(crate) fn open_with_info(
        seg_dir: &Path,
        segment_info: &SegmentInfo,
        dense_config: Option<&DenseVectorConfig>,
        secondary_indexes: &[SecondaryIndexManifestEntry],
    ) -> Result<Self, EngineError> {
        let component_manifest = read_component_manifest(seg_dir)?;
        let source_groups = validate_segment_manifest_identity(segment_info, &component_manifest)?;
        let component_registry = SegmentComponentRegistry::new(&component_manifest);
        validate_manifest_component_contracts(
            &component_registry,
            &source_groups,
            dense_config,
            secondary_indexes,
        )?;
        let component_open_context =
            ComponentOpenContext::open(seg_dir, segment_info.id, &component_manifest)?;
        warm_edge_property_sidecar_availability(
            &component_registry,
            &component_open_context,
            seg_dir,
            secondary_indexes,
        );
        let nodes_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::NodeRecords,
        )?;
        let edges_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::EdgeRecords,
        )?;
        let adj_out_idx = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::AdjOutIndex,
        )?;
        let adj_out_dat = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::AdjOutPostings,
        )?;
        let adj_in_idx = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::AdjInIndex,
        )?;
        let adj_in_dat = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::AdjInPostings,
        )?;
        let key_index_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::KeyIndex,
        )?;
        let node_label_index_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::NodeLabelIndex,
        )?;
        let edge_label_index_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::EdgeLabelIndex,
        )?;
        let edge_triple_index_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::EdgeTripleIndex,
        )?;
        // Metadata payloads.
        let node_meta_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::NodeMetadata,
        )?;
        let edge_meta_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::EdgeMetadata,
        )?;
        let edge_weight_index_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::EdgeWeightIndex,
        )?;
        let edge_weight_index_count = edge_metadata_index_count_from_header(
            &component_registry,
            SegmentComponentKind::EdgeWeightIndex,
            &edge_weight_index_mmap,
            EDGE_WEIGHT_INDEX_ENTRY_SIZE,
            EDGE_WEIGHT_INDEX_LOGICAL_NAME,
        );
        let edge_updated_at_index_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::EdgeUpdatedAtIndex,
        )?;
        let edge_updated_at_index_count = edge_metadata_index_count_from_header(
            &component_registry,
            SegmentComponentKind::EdgeUpdatedAtIndex,
            &edge_updated_at_index_mmap,
            EDGE_I64_METADATA_INDEX_ENTRY_SIZE,
            EDGE_UPDATED_AT_INDEX_LOGICAL_NAME,
        );
        let edge_valid_from_index_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::EdgeValidFromIndex,
        )?;
        let edge_valid_from_index_count = edge_metadata_index_count_from_header(
            &component_registry,
            SegmentComponentKind::EdgeValidFromIndex,
            &edge_valid_from_index_mmap,
            EDGE_I64_METADATA_INDEX_ENTRY_SIZE,
            EDGE_VALID_FROM_INDEX_LOGICAL_NAME,
        );
        let edge_valid_to_index_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::EdgeValidToIndex,
        )?;
        let edge_valid_to_index_count = edge_metadata_index_count_from_header(
            &component_registry,
            SegmentComponentKind::EdgeValidToIndex,
            &edge_valid_to_index_mmap,
            EDGE_I64_METADATA_INDEX_ENTRY_SIZE,
            EDGE_VALID_TO_INDEX_LOGICAL_NAME,
        );
        let node_vector_meta_mmap = open_manifested_required_payload_or_empty(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::NodeVectorMetadata,
        )?;
        let node_dense_vectors_mmap = open_manifested_required_payload_or_empty(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::NodeDenseVectorBlob,
        )?;
        let node_sparse_vectors_mmap = open_manifested_required_payload_or_empty(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::NodeSparseVectorBlob,
        )?;
        let dense_hnsw_meta_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::DenseHnswMetadata,
        )?;
        let dense_hnsw_graph_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::DenseHnswGraph,
        )?;
        let mut sparse_posting_index_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::SparsePostingIndex,
        )?;
        let mut sparse_postings_mmap = open_optional_manifest_payload(
            &component_registry,
            Some(&component_open_context),
            seg_dir,
            SegmentComponentKind::SparsePostings,
        )?;
        let degree_delta = open_degree_delta_sidecar(&component_registry, seg_dir);
        let timestamp_index_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::TimestampIndex,
        )?;

        let tombstones_mmap = open_required_manifest_payload(
            &component_registry,
            &component_open_context,
            seg_dir,
            SegmentComponentKind::Tombstones,
        )?;
        let (deleted_nodes, deleted_edges) = load_tombstones_from_bytes(&tombstones_mmap)?;

        let node_count = if nodes_mmap.len() >= 8 {
            read_u64_at(&nodes_mmap, 0)?
        } else {
            0
        };
        let edge_count = if edges_mmap.len() >= 8 {
            read_u64_at(&edges_mmap, 0)?
        } else {
            0
        };
        let planner_stats = open_planner_stats(
            &component_registry,
            seg_dir,
            segment_info.id,
            node_count,
            edge_count,
        );
        let node_meta_count = if node_meta_mmap.len() >= 8 {
            read_u64_at(&node_meta_mmap, 0)?
        } else {
            0
        };

        let vector_summary = validate_node_vector_sidecars(
            segment_info.id,
            &node_vector_meta_mmap,
            &node_dense_vectors_mmap,
            &node_sparse_vectors_mmap,
            node_meta_count,
        )?;
        let dense_hnsw_validation = validate_dense_hnsw_files_for_open(
            &dense_hnsw_meta_mmap,
            &dense_hnsw_graph_mmap,
            node_dense_vectors_mmap.len(),
            vector_summary.dense_count,
            dense_config,
        );
        let dense_hnsw_header = match dense_hnsw_validation {
            Ok(header) => header,
            Err(error) => {
                mark_optional_components_corrupt(
                    &component_registry,
                    &[
                        SegmentComponentKind::DenseHnswMetadata,
                        SegmentComponentKind::DenseHnswGraph,
                    ],
                    error.to_string(),
                );
                None
            }
        };
        if validate_sparse_posting_files_for_open(
            &sparse_posting_index_mmap,
            &sparse_postings_mmap,
            vector_summary.sparse_count,
        )
        .map_err(|error| {
            mark_optional_components_corrupt(
                &component_registry,
                &[
                    SegmentComponentKind::SparsePostingIndex,
                    SegmentComponentKind::SparsePostings,
                ],
                error.to_string(),
            );
        })
        .is_err()
        {
            sparse_posting_index_mmap = MappedData::Empty;
            sparse_postings_mmap = MappedData::Empty;
        }
        let sparse_postings_available =
            !sparse_posting_index_mmap.is_empty() && !sparse_postings_mmap.is_empty();

        Ok(SegmentReader {
            segment_id: segment_info.id,
            seg_dir: seg_dir.to_path_buf(),
            segment_data_id: segment_info.segment_data_id,
            component_manifest_generation: component_manifest.generation,
            component_registry,
            nodes_mmap,
            edges_mmap,
            adj_out_idx,
            adj_out_dat,
            adj_in_idx,
            adj_in_dat,
            key_index_mmap,
            node_label_index_mmap,
            edge_label_index_mmap,
            edge_triple_index_mmap,
            edge_weight_index_mmap,
            edge_weight_index_count,
            edge_updated_at_index_mmap,
            edge_updated_at_index_count,
            edge_valid_from_index_mmap,
            edge_valid_from_index_count,
            edge_valid_to_index_mmap,
            edge_valid_to_index_count,
            node_meta_mmap,
            edge_meta_mmap,
            node_vector_meta_mmap,
            node_dense_vectors_mmap,
            node_sparse_vectors_mmap,
            dense_hnsw_meta_mmap,
            dense_hnsw_graph_mmap,
            dense_hnsw_header,
            dense_hnsw_available: AtomicBool::new(dense_hnsw_header.is_some()),
            dense_hnsw_points: OnceLock::new(),
            dense_vector_count: vector_summary.dense_count,
            sparse_vector_count: vector_summary.sparse_count,
            sparse_posting_index_mmap,
            sparse_postings_mmap,
            sparse_postings_available: AtomicBool::new(sparse_postings_available),
            sparse_posting_index_shape: OnceLock::new(),
            degree_delta,
            planner_stats,
            timestamp_index_mmap,
            deleted_nodes,
            deleted_edges,
            secondary_eq_sidecars: Mutex::new(HashMap::new()),
            secondary_range_sidecars: Mutex::new(HashMap::new()),
            declared_index_runtime_coverage: Mutex::new(HashMap::new()),
            node_ids: OnceLock::new(),
            node_count,
            edge_count,
        })
    }

    /// Get a node by ID. Returns None if not found or tombstoned.
    /// Returns Err on corrupt segment data.
    pub fn get_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        if self.deleted_nodes.contains_key(&id) {
            return Ok(None);
        }
        let (index, offset) = match self.binary_search_node_index(id)? {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let mut node = decode_node_at(&self.nodes_mmap, offset, id)?;
        self.hydrate_node_vectors(index, &mut node)?;
        // Hydrate last_write_seq from metadata.
        node.last_write_seq = self.node_meta_at(index)?.last_write_seq;
        Ok(Some(node))
    }

    /// Get an edge by ID. Returns None if not found or tombstoned.
    /// Returns Err on corrupt segment data.
    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        if self.deleted_edges.contains_key(&id) {
            return Ok(None);
        }
        let (index, offset) = match self.binary_search_edge_index(id)? {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let mut edge = decode_edge_at(&self.edges_mmap, offset, id)?;
        // Hydrate last_write_seq from metadata.
        let (_, _, _, _, _, _, _, _, _, _, last_write_seq) = self.edge_meta_at(index)?;
        edge.last_write_seq = last_write_seq;
        Ok(Some(edge))
    }

    /// Get the fixed-width edge fields needed for engine-side cache updates
    /// without decoding properties.
    /// Returns (from, to, created_at, updated_at, weight, valid_from, valid_to).
    #[allow(clippy::type_complexity)]
    pub(crate) fn get_edge_core(
        &self,
        id: u64,
    ) -> Result<Option<(u64, u64, i64, i64, f32, i64, i64)>, EngineError> {
        if self.deleted_edges.contains_key(&id) {
            return Ok(None);
        }
        let (_, offset) = match self.binary_search_edge_index(id)? {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let data = &self.edges_mmap[..];
        let from = read_u64_at(data, offset)?;
        let to = read_u64_at(data, offset + 8)?;
        let created_at = read_i64_at(data, offset + 20)?;
        let updated_at = read_i64_at(data, offset + 28)?;
        let weight = read_f32_at(data, offset + 36)?;
        let valid_from = read_i64_at(data, offset + 40)?;
        let valid_to = read_i64_at(data, offset + 48)?;
        Ok(Some((
            from, to, created_at, updated_at, weight, valid_from, valid_to,
        )))
    }

    fn edge_metadata_at_index(&self, index: usize) -> Result<EdgeMetadataCandidate, EngineError> {
        let (
            edge_id,
            _data_offset,
            _data_len,
            from,
            to,
            label_id,
            updated_at,
            weight,
            valid_from,
            valid_to,
            _last_write_seq,
        ) = self.edge_meta_at(index)?;
        Ok(EdgeMetadataCandidate {
            edge_id,
            from,
            to,
            label_id,
            updated_at,
            weight,
            valid_from,
            valid_to,
        })
    }

    /// Look up a node by (label_id, key). Returns None if not found or tombstoned.
    #[cfg(test)]
    pub fn node_by_key(&self, label_id: u32, key: &str) -> Result<Option<NodeRecord>, EngineError> {
        let node_id = match self.binary_search_key_index(label_id, key)? {
            Some(id) => id,
            None => return Ok(None),
        };
        self.get_node(node_id)
    }

    /// Phase 1 of resolve_keys_batch: walk the key index to map each
    /// (label_id, key) query to a node_id. Returns (orig_idx, node_id) pairs
    /// for keys found in this segment's key index.
    pub(crate) fn resolve_keys_to_ids(
        &self,
        lookups: &[(usize, u32, &str)],
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let mut resolved = Vec::new();
        let data = &self.key_index_mmap[..];
        if data.len() < 8 {
            return Ok(resolved);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(resolved);
        }

        let offset_table_start = 8;

        // Count unique keys for strategy selection
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<(u32, &str)> = None;
            for &(_, tid, key) in lookups {
                if prev != Some((tid, key)) {
                    n += 1;
                    prev = Some((tid, key));
                }
            }
            n
        };

        let strategy = if unique_keys <= 2 || count <= 1 {
            BatchReadStrategy::SeekPerKey
        } else {
            let (min_type, min_key) = (lookups[0].1, lookups[0].2);
            let (max_type, max_key) = (lookups[lookups.len() - 1].1, lookups[lookups.len() - 1].2);

            let span_start =
                lower_bound_key_index(data, offset_table_start, count, min_type, min_key)?;
            let span_end =
                upper_bound_key_index(data, offset_table_start, count, max_type, max_key)?;
            let span = span_end.saturating_sub(span_start).max(unique_keys);

            let seek_cost = unique_keys
                .saturating_mul(ceil_log2_usize(count))
                .saturating_mul(BATCH_RANDOM_ACCESS_PENALTY);

            if seek_cost <= span {
                BatchReadStrategy::SeekPerKey
            } else {
                BatchReadStrategy::MergeWalk
            }
        };

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_query: Option<(u32, &str)> = None;
            let mut prev_node_id: Option<u64> = None;
            for &(orig_idx, label_id, key) in lookups {
                let node_id = if prev_query == Some((label_id, key)) {
                    prev_node_id
                } else {
                    let found = self.binary_search_key_index(label_id, key)?;
                    prev_query = Some((label_id, key));
                    prev_node_id = found;
                    found
                };
                if let Some(nid) = node_id {
                    resolved.push((orig_idx, nid));
                }
            }
        } else {
            // Merge-walk: single cursor through key index entries
            let mut idx_pos = 0usize;
            for &(orig_idx, label_id, key) in lookups {
                while idx_pos < count {
                    let entry_offset =
                        read_u64_at(data, offset_table_start + idx_pos * 8)? as usize;
                    let entry_label_id = read_u32_at(data, entry_offset)?;
                    let key_len = read_u16_at(data, entry_offset + 12)? as usize;
                    let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
                    let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
                        EngineError::CorruptRecord(format!(
                            "invalid UTF-8 in key index at offset {}",
                            entry_offset + 14
                        ))
                    })?;

                    match (entry_label_id, entry_key).cmp(&(label_id, key)) {
                        std::cmp::Ordering::Less => {
                            idx_pos += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            let node_id = read_u64_at(data, entry_offset + 4)?;
                            resolved.push((orig_idx, node_id));
                            break;
                        }
                        std::cmp::Ordering::Greater => {
                            break;
                        }
                    }
                }
            }
        }

        Ok(resolved)
    }

    /// Query neighbors of a node. Checks both outgoing and incoming adjacency
    /// based on the direction parameter.
    pub(crate) fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        let mut results = Vec::new();

        match direction {
            Direction::Outgoing => {
                self.collect_adj_neighbors(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_id,
                    label_filter_ids,
                    limit,
                    None,
                    None,
                    None,
                    &mut results,
                )?;
            }
            Direction::Incoming => {
                self.collect_adj_neighbors(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_id,
                    label_filter_ids,
                    limit,
                    None,
                    None,
                    None,
                    &mut results,
                )?;
            }
            Direction::Both => {
                if limit == 0 {
                    let mut self_loop_edge_ids = NodeIdSet::default();
                    self.collect_adj_neighbors(
                        &self.adj_out_idx,
                        &self.adj_out_dat,
                        node_id,
                        label_filter_ids,
                        0,
                        Some(&mut self_loop_edge_ids),
                        None,
                        None,
                        &mut results,
                    )?;
                    self.collect_adj_neighbors(
                        &self.adj_in_idx,
                        &self.adj_in_dat,
                        node_id,
                        label_filter_ids,
                        0,
                        None,
                        Some(&self_loop_edge_ids),
                        None,
                        &mut results,
                    )?;
                } else {
                    let mut self_loop_edge_ids = NodeIdSet::default();
                    self.collect_adj_neighbors(
                        &self.adj_out_idx,
                        &self.adj_out_dat,
                        node_id,
                        label_filter_ids,
                        limit,
                        Some(&mut self_loop_edge_ids),
                        None,
                        None,
                        &mut results,
                    )?;
                    let mut remaining = limit.saturating_sub(results.len());
                    if remaining == 0 {
                        return Ok(results);
                    }
                    self.collect_adj_neighbors(
                        &self.adj_in_idx,
                        &self.adj_in_dat,
                        node_id,
                        label_filter_ids,
                        0,
                        None,
                        Some(&self_loop_edge_ids),
                        Some(&mut remaining),
                        &mut results,
                    )?;
                }
            }
        }

        Ok(results)
    }

    /// Check if a node ID is tombstoned in this segment.
    pub fn is_node_deleted(&self, id: u64) -> bool {
        self.deleted_nodes.contains_key(&id)
    }

    /// Batch lookup: resolve multiple node IDs over the sorted index.
    /// `lookups` must be sorted by ID (the second element). Each entry is (original_index, id).
    /// Found nodes are written into `results[original_index]`. Tombstoned nodes are skipped.
    ///
    /// Adaptive strategy: small batches use per-key binary seek (O(K log N));
    /// large batches use a merge-walk cursor (O(N + K)).
    pub fn get_nodes_batch(
        &self,
        lookups: &[(usize, u64)],
        results: &mut [Option<NodeRecord>],
    ) -> Result<(), EngineError> {
        if lookups.is_empty() {
            return Ok(());
        }
        let data = &self.nodes_mmap[..];
        if data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(());
        }

        let idx_start = 8;
        let min_key = lookups.first().map(|&(_, id)| id).unwrap_or(0);
        let max_key = lookups.last().map(|&(_, id)| id).unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &(_, id) in lookups {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            NODE_INDEX_ENTRY_SIZE,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            // Seek path selected by shared cost model
            let mut prev_id: Option<u64> = None;
            let mut prev_offset: Option<(usize, usize)> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                let offset = if prev_id == Some(target_id) {
                    prev_offset
                } else {
                    let found = self.binary_search_node_index(target_id)?;
                    prev_id = Some(target_id);
                    prev_offset = found;
                    found
                };
                if let Some((index, offset)) = offset {
                    let mut node = decode_node_at(&self.nodes_mmap, offset, target_id)?;
                    self.hydrate_node_vectors(index, &mut node)?;
                    node.last_write_seq = self.node_meta_at(index)?.last_write_seq;
                    results[orig_idx] = Some(node);
                }
            }
        } else {
            // Merge-walk path selected by shared cost model
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * NODE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let offset = read_u64_at(data, entry_off + 8)? as usize;
                        let mut node = decode_node_at(&self.nodes_mmap, offset, id)?;
                        self.hydrate_node_vectors(idx_pos, &mut node)?;
                        node.last_write_seq = self.node_meta_at(idx_pos)?.last_write_seq;
                        results[orig_idx] = Some(node);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Batch metadata lookup: resolve multiple node IDs without decoding full
    /// node records or hydrating vectors. `lookups` must be sorted by ID.
    /// Found metadata is written into `results[original_index]` as
    /// `(label_ids, updated_at, weight)`.
    pub(crate) fn get_node_meta_batch(
        &self,
        lookups: &[(usize, u64)],
        results: &mut [Option<(NodeLabelSet, i64, f32)>],
    ) -> Result<(), EngineError> {
        if lookups.is_empty() {
            return Ok(());
        }
        let data = &self.node_meta_mmap[..];
        let Some(layout) = parse_node_meta_layout(data)? else {
            return Ok(());
        };
        let count = layout.node_count;
        if count == 0 {
            return Ok(());
        }

        let idx_start = layout.fixed_entries_offset;
        let min_key = lookups.first().map(|&(_, id)| id).unwrap_or(0);
        let max_key = lookups.last().map(|&(_, id)| id).unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &(_, id) in lookups {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            layout.fixed_entry_size,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_meta: Option<(NodeLabelSet, i64, f32)> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                let meta = if prev_id == Some(target_id) {
                    prev_meta
                } else if let Some(index) = binary_search_node_meta_index(data, layout, target_id)?
                {
                    let meta = read_node_meta_entry_at(data, layout, index)?;
                    let found = Some((meta.label_ids, meta.updated_at, meta.weight));
                    prev_id = Some(target_id);
                    prev_meta = found;
                    found
                } else {
                    prev_id = Some(target_id);
                    prev_meta = None;
                    None
                };
                results[orig_idx] = meta;
            }
        } else {
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * layout.fixed_entry_size;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let meta = read_node_meta_entry_at(data, layout, idx_pos)?;
                        results[orig_idx] = Some((meta.label_ids, meta.updated_at, meta.weight));
                        break;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Batch dense scoring over sorted candidate IDs. Found candidates are scored
    /// immediately without hydrating full `NodeRecord`s. Unfound candidates are
    /// appended to `remaining_out` in sorted order for older segments.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn score_dense_candidates_sorted<F>(
        &self,
        ids: &[u64],
        query: &[f32],
        metric: DenseMetric,
        query_norm: Option<f32>,
        mut include: F,
        hits_out: &mut Vec<VectorHit>,
        remaining_out: &mut Vec<u64>,
    ) -> Result<(), EngineError>
    where
        F: FnMut(NodeLabelSet, i64, f32) -> bool,
    {
        if ids.is_empty() {
            return Ok(());
        }
        let node_meta = &self.node_meta_mmap[..];
        let vector_meta = &self.node_vector_meta_mmap[..];
        let Some(node_meta_layout) = parse_node_meta_layout(node_meta)? else {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        };
        let data = node_meta;
        let count = node_meta_layout.node_count;
        if count == 0 {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        }

        let idx_start = node_meta_layout.fixed_entries_offset;
        let min_key = ids.first().copied().unwrap_or(0);
        let max_key = ids.last().copied().unwrap_or(0);
        let mut unique_keys = 0usize;
        let mut prev: Option<u64> = None;
        for &id in ids {
            if prev != Some(id) {
                unique_keys += 1;
                prev = Some(id);
            }
        }
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            node_meta_layout.fixed_entry_size,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_found: Option<DenseScoringMeta> = None;
            for &target_id in ids {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }

                let found = if prev_id == Some(target_id) {
                    prev_found
                } else if let Some(index) =
                    binary_search_node_meta_index(node_meta, node_meta_layout, target_id)?
                {
                    let found = Some(read_dense_scoring_meta(
                        node_meta,
                        node_meta_layout,
                        vector_meta,
                        index,
                    )?);
                    prev_id = Some(target_id);
                    prev_found = found;
                    found
                } else {
                    prev_id = Some(target_id);
                    prev_found = None;
                    None
                };

                let Some(found) = found else {
                    remaining_out.push(target_id);
                    continue;
                };
                if found.dense_len == 0 || !include(found.label_ids, found.updated_at, found.weight)
                {
                    continue;
                }
                hits_out.push(VectorHit {
                    node_id: target_id,
                    score: dense_score_from_bytes(
                        metric,
                        query,
                        query_norm,
                        &self.node_dense_vectors_mmap,
                        found.dense_offset,
                        found.dense_len,
                    )?,
                });
            }
        } else {
            let mut idx_pos = 0usize;
            for &target_id in ids {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }

                let mut found = None;
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * node_meta_layout.fixed_entry_size;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        found = Some(read_dense_scoring_meta(
                            node_meta,
                            node_meta_layout,
                            vector_meta,
                            idx_pos,
                        )?);
                        break;
                    } else {
                        break;
                    }
                }

                let Some(found) = found else {
                    remaining_out.push(target_id);
                    continue;
                };
                if found.dense_len == 0 || !include(found.label_ids, found.updated_at, found.weight)
                {
                    continue;
                }
                hits_out.push(VectorHit {
                    node_id: target_id,
                    score: dense_score_from_bytes(
                        metric,
                        query,
                        query_norm,
                        &self.node_dense_vectors_mmap,
                        found.dense_offset,
                        found.dense_len,
                    )?,
                });
            }
        }

        Ok(())
    }

    /// Score sparse vectors for a sorted list of candidate node IDs.
    /// Reads sparse vectors directly from the segment blob and computes
    /// dot products against the query without allocating per-node Vec.
    /// Nodes not found in this segment are appended to `remaining_out`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn score_sparse_candidates_sorted<F>(
        &self,
        ids: &[u64],
        query: &[(u32, f32)],
        mut include: F,
        hits_out: &mut Vec<(u64, f32)>,
        remaining_out: &mut Vec<u64>,
    ) -> Result<(), EngineError>
    where
        F: FnMut(NodeLabelSet, i64, f32) -> bool,
    {
        if ids.is_empty() || query.is_empty() {
            return Ok(());
        }
        let node_meta = &self.node_meta_mmap[..];
        let vector_meta = &self.node_vector_meta_mmap[..];
        let sparse_blob = &self.node_sparse_vectors_mmap[..];
        let Some(node_meta_layout) = parse_node_meta_layout(node_meta)? else {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        };
        let data = node_meta;
        let count = node_meta_layout.node_count;
        if count == 0 {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        }

        let idx_start = node_meta_layout.fixed_entries_offset;
        let min_key = ids.first().copied().unwrap_or(0);
        let max_key = ids.last().copied().unwrap_or(0);
        let mut unique_keys = 0usize;
        let mut prev: Option<u64> = None;
        for &id in ids {
            if prev != Some(id) {
                unique_keys += 1;
                prev = Some(id);
            }
        }
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            node_meta_layout.fixed_entry_size,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_found: Option<SparseScoringMeta> = None;
            for &target_id in ids {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }

                let found = if prev_id == Some(target_id) {
                    prev_found
                } else if let Some(index) =
                    binary_search_node_meta_index(node_meta, node_meta_layout, target_id)?
                {
                    let found = Some(read_sparse_scoring_meta(
                        node_meta,
                        node_meta_layout,
                        vector_meta,
                        index,
                    )?);
                    prev_id = Some(target_id);
                    prev_found = found;
                    found
                } else {
                    prev_id = Some(target_id);
                    prev_found = None;
                    None
                };

                let Some(found) = found else {
                    remaining_out.push(target_id);
                    continue;
                };
                if found.sparse_len == 0
                    || !include(found.label_ids, found.updated_at, found.weight)
                {
                    continue;
                }
                let score = sparse_dot_score_from_blob(
                    query,
                    sparse_blob,
                    found.sparse_offset,
                    found.sparse_len,
                )?;
                if score > 0.0 {
                    hits_out.push((target_id, score));
                }
            }
        } else {
            let mut idx_pos = 0usize;
            for &target_id in ids {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }

                let mut found = None;
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * node_meta_layout.fixed_entry_size;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        found = Some(read_sparse_scoring_meta(
                            node_meta,
                            node_meta_layout,
                            vector_meta,
                            idx_pos,
                        )?);
                        break;
                    } else {
                        break;
                    }
                }

                let Some(found) = found else {
                    remaining_out.push(target_id);
                    continue;
                };
                if found.sparse_len == 0
                    || !include(found.label_ids, found.updated_at, found.weight)
                {
                    continue;
                }
                let score = sparse_dot_score_from_blob(
                    query,
                    sparse_blob,
                    found.sparse_offset,
                    found.sparse_len,
                )?;
                if score > 0.0 {
                    hits_out.push((target_id, score));
                }
            }
        }

        Ok(())
    }

    /// Batch lookup: resolve multiple edge IDs over the sorted index.
    /// `lookups` must be sorted by ID (the second element). Each entry is (original_index, id).
    /// Found edges are written into `results[original_index]`. Tombstoned edges are skipped.
    ///
    /// Adaptive strategy: small batches use per-key binary seek (O(K log N));
    /// large batches use a merge-walk cursor (O(N + K)).
    pub fn get_edges_batch(
        &self,
        lookups: &[(usize, u64)],
        results: &mut [Option<EdgeRecord>],
    ) -> Result<(), EngineError> {
        if lookups.is_empty() {
            return Ok(());
        }
        let data = &self.edges_mmap[..];
        if data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(());
        }

        let idx_start = 8;
        let min_key = lookups.first().map(|&(_, id)| id).unwrap_or(0);
        let max_key = lookups.last().map(|&(_, id)| id).unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &(_, id) in lookups {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            EDGE_INDEX_ENTRY_SIZE,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            // Seek path selected by shared cost model
            let mut prev_id: Option<u64> = None;
            let mut prev_entry: Option<(usize, usize)> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains_key(&target_id) {
                    continue;
                }
                let entry = if prev_id == Some(target_id) {
                    prev_entry
                } else {
                    let found = self.binary_search_edge_index(target_id)?;
                    prev_id = Some(target_id);
                    prev_entry = found;
                    found
                };
                if let Some((index, offset)) = entry {
                    let mut edge = decode_edge_at(&self.edges_mmap, offset, target_id)?;
                    let (_, _, _, _, _, _, _, _, _, _, lws) = self.edge_meta_at(index)?;
                    edge.last_write_seq = lws;
                    results[orig_idx] = Some(edge);
                }
            }
        } else {
            // Merge-walk path selected by shared cost model
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains_key(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * EDGE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let offset = read_u64_at(data, entry_off + 8)? as usize;
                        let mut edge = decode_edge_at(&self.edges_mmap, offset, id)?;
                        let (_, _, _, _, _, _, _, _, _, _, lws) = self.edge_meta_at(idx_pos)?;
                        edge.last_write_seq = lws;
                        results[orig_idx] = Some(edge);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Batch metadata lookup: resolve multiple edge IDs without decoding full
    /// edge records. `lookups` must be sorted by ID. Found metadata is written
    /// into `results[original_index]`.
    pub(crate) fn get_edge_metadata_batch(
        &self,
        lookups: &[(usize, u64)],
        results: &mut [Option<EdgeMetadataCandidate>],
    ) -> Result<(), EngineError> {
        if lookups.is_empty() {
            return Ok(());
        }
        let data = &self.edges_mmap[..];
        if data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(());
        }

        let idx_start = 8;
        let min_key = lookups.first().map(|&(_, id)| id).unwrap_or(0);
        let max_key = lookups.last().map(|&(_, id)| id).unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &(_, id) in lookups {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            EDGE_INDEX_ENTRY_SIZE,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_meta: Option<EdgeMetadataCandidate> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains_key(&target_id) {
                    continue;
                }
                let meta = if prev_id == Some(target_id) {
                    prev_meta
                } else if let Some((index, _offset)) = self.binary_search_edge_index(target_id)? {
                    let found = Some(self.edge_metadata_at_index(index)?);
                    prev_id = Some(target_id);
                    prev_meta = found;
                    found
                } else {
                    prev_id = Some(target_id);
                    prev_meta = None;
                    None
                };
                results[orig_idx] = meta;
            }
        } else {
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains_key(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * EDGE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        results[orig_idx] = Some(self.edge_metadata_at_index(idx_pos)?);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if an edge ID is tombstoned in this segment.
    pub fn is_edge_deleted(&self, id: u64) -> bool {
        self.deleted_edges.contains_key(&id)
    }

    /// Check if an edge ID exists (has a live record) in this segment's index.
    /// Does NOT check tombstones; only checks whether the edge index contains this ID.
    pub fn has_edge(&self, id: u64) -> bool {
        self.binary_search_edge_index(id).ok().flatten().is_some()
    }

    /// Return the deleted node tombstone map in this segment.
    pub fn deleted_node_tombstones(&self) -> &NodeIdMap<TombstoneEntry> {
        &self.deleted_nodes
    }

    /// Return the deleted edge tombstone map in this segment.
    pub fn deleted_edge_tombstones(&self) -> &NodeIdMap<TombstoneEntry> {
        &self.deleted_edges
    }

    /// Return the number of tombstoned nodes in this segment.
    pub fn deleted_node_count(&self) -> usize {
        self.deleted_nodes.len()
    }

    /// Return the number of tombstoned edges in this segment.
    pub fn deleted_edge_count(&self) -> usize {
        self.deleted_edges.len()
    }

    /// Return the set of deleted node IDs in this segment (keys only).
    pub fn deleted_node_ids(&self) -> NodeIdSet {
        self.deleted_nodes.keys().copied().collect()
    }

    /// Iterate deleted node IDs without allocating a temporary set.
    pub(crate) fn deleted_node_id_iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.deleted_nodes.keys().copied()
    }

    /// Return the set of deleted edge IDs in this segment (keys only).
    pub fn deleted_edge_ids(&self) -> NodeIdSet {
        self.deleted_edges.keys().copied().collect()
    }

    /// Return all node IDs present in this segment.
    pub fn node_ids(&self) -> Result<&[u64], EngineError> {
        if let Some(node_ids) = self.node_ids.get() {
            return Ok(node_ids.as_ref());
        }

        let node_ids = collect_node_ids(&self.nodes_mmap)?.into_boxed_slice();
        let _ = self.node_ids.set(node_ids);
        Ok(self
            .node_ids
            .get()
            .expect("node_ids must be initialized after set")
            .as_ref())
    }

    pub(crate) fn node_record_index_entries_for_scrub(
        &self,
        expected_count: u64,
    ) -> Result<Vec<(u64, u64)>, EngineError> {
        let data = &self.nodes_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(format!(
                "node records payload length {} is too short for count header",
                data.len()
            )));
        }
        let actual_count = read_u64_at(data, 0)?;
        if actual_count != expected_count {
            return Err(EngineError::CorruptRecord(format!(
                "node records count {} does not match segment manifest node_count {}",
                actual_count, expected_count
            )));
        }
        let count = usize_from_u64(actual_count, "node records count")?;
        let index_start = 8usize;
        let index_end = index_start
            .checked_add(count.checked_mul(NODE_INDEX_ENTRY_SIZE).ok_or_else(|| {
                EngineError::CorruptRecord("node records index size overflow".into())
            })?)
            .ok_or_else(|| EngineError::CorruptRecord("node records index end overflow".into()))?;
        if index_end > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "node records index [{}, {}) exceeds payload length {}",
                index_start,
                index_end,
                data.len()
            )));
        }

        let mut entries = Vec::with_capacity(count);
        let mut previous = None;
        for index in 0..count {
            let entry_offset = index_start + index * NODE_INDEX_ENTRY_SIZE;
            let node_id = read_u64_at(data, entry_offset)?;
            if previous.is_some_and(|prev| prev >= node_id) {
                return Err(EngineError::CorruptRecord(format!(
                    "node records index row {} is not sorted by unique node_id",
                    index
                )));
            }
            previous = Some(node_id);
            let data_offset = read_u64_at(data, entry_offset + 8)?;
            let data_offset_usize = usize_from_u64(data_offset, "node records data offset")?;
            if data_offset_usize < index_end || data_offset_usize >= data.len() {
                return Err(EngineError::CorruptRecord(format!(
                    "node records index row {} offset {} is outside payload data region [{}, {})",
                    index,
                    data_offset_usize,
                    index_end,
                    data.len()
                )));
            }
            entries.push((node_id, data_offset));
        }
        Ok(entries)
    }

    pub fn node_count(&self) -> u64 {
        self.node_count
    }

    pub(crate) fn node_id_index_len(&self) -> usize {
        self.node_count as usize
    }

    pub(crate) fn node_id_at_index(&self, index: usize) -> Result<Option<u64>, EngineError> {
        if index >= self.node_id_index_len() {
            return Ok(None);
        }
        Ok(Some(read_u64_at(
            &self.nodes_mmap,
            8 + index * NODE_INDEX_ENTRY_SIZE,
        )?))
    }

    pub(crate) fn node_id_lower_bound(&self, after: u64) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = self.node_id_index_len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let node_id = read_u64_at(&self.nodes_mmap, 8 + mid * NODE_INDEX_ENTRY_SIZE)?;
            if node_id <= after {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    pub fn edge_count(&self) -> u64 {
        self.edge_count
    }

    pub(crate) fn degree_delta_available(&self) -> bool {
        self.degree_delta.is_some()
    }

    pub(crate) fn degree_delta(&self, node_id: u64) -> Option<DegreeDelta> {
        self.degree_delta
            .as_ref()
            .map(|sidecar| sidecar.lookup(node_id))
    }

    pub(crate) fn degree_delta_sidecar(&self) -> Option<&DegreeSidecar> {
        self.degree_delta.as_ref()
    }

    /// Returns true if this segment contains any tombstones (deleted nodes or edges).
    pub fn has_tombstones(&self) -> bool {
        !self.deleted_nodes.is_empty() || !self.deleted_edges.is_empty()
    }

    /// Return the (min_id, max_id) range of node IDs in this segment's index.
    /// Returns None if the segment has no nodes.
    pub fn node_id_range(&self) -> Option<(u64, u64)> {
        let data = &self.nodes_mmap[..];
        if data.len() < 8 {
            return None;
        }
        let count = read_u64_at(data, 0).ok()? as usize;
        if count == 0 {
            return None;
        }
        // Index is sorted by node_id; first and last entries give the range
        let first_id = read_u64_at(data, 8).ok()?;
        let last_id = read_u64_at(data, 8 + (count - 1) * NODE_INDEX_ENTRY_SIZE).ok()?;
        Some((first_id, last_id))
    }

    /// Return the (min_id, max_id) range of edge IDs in this segment's index.
    /// Returns None if the segment has no edges.
    pub fn edge_id_range(&self) -> Option<(u64, u64)> {
        let data = &self.edges_mmap[..];
        if data.len() < 8 {
            return None;
        }
        let count = read_u64_at(data, 0).ok()? as usize;
        if count == 0 {
            return None;
        }
        let first_id = read_u64_at(data, 8).ok()?;
        let last_id = read_u64_at(data, 8 + (count - 1) * EDGE_INDEX_ENTRY_SIZE).ok()?;
        Some((first_id, last_id))
    }

    /// Raw mmap bytes for the node records payload (used by V3 compaction).
    pub(crate) fn raw_nodes_mmap(&self) -> &[u8] {
        &self.nodes_mmap[..]
    }

    /// Raw mmap bytes for the edge records payload (used by V3 compaction).
    pub(crate) fn raw_edges_mmap(&self) -> &[u8] {
        &self.edges_mmap[..]
    }

    pub(crate) fn segment_data_id(&self) -> [u8; 32] {
        self.segment_data_id
    }

    pub(crate) fn component_manifest_generation(&self) -> u64 {
        self.component_manifest_generation
    }

    #[cfg(test)]
    pub(crate) fn optional_component_availability_for_test(
        &self,
        kind: SegmentComponentKind,
    ) -> ComponentAvailability {
        self.optional_component_availability(kind)
    }

    #[cfg(test)]
    pub(crate) fn component_mapping_identity_for_test(
        &self,
        kind: SegmentComponentKind,
    ) -> Option<usize> {
        let data = match kind {
            SegmentComponentKind::NodeRecords => &self.nodes_mmap,
            SegmentComponentKind::EdgeRecords => &self.edges_mmap,
            SegmentComponentKind::NodeMetadata => &self.node_meta_mmap,
            SegmentComponentKind::EdgeMetadata => &self.edge_meta_mmap,
            SegmentComponentKind::Tombstones => return None,
            SegmentComponentKind::KeyIndex => &self.key_index_mmap,
            SegmentComponentKind::NodeLabelIndex => &self.node_label_index_mmap,
            SegmentComponentKind::EdgeLabelIndex => &self.edge_label_index_mmap,
            SegmentComponentKind::EdgeTripleIndex => &self.edge_triple_index_mmap,
            SegmentComponentKind::AdjOutIndex => &self.adj_out_idx,
            SegmentComponentKind::AdjOutPostings => &self.adj_out_dat,
            SegmentComponentKind::AdjInIndex => &self.adj_in_idx,
            SegmentComponentKind::AdjInPostings => &self.adj_in_dat,
            SegmentComponentKind::TimestampIndex => &self.timestamp_index_mmap,
            SegmentComponentKind::NodeVectorMetadata => &self.node_vector_meta_mmap,
            SegmentComponentKind::NodeDenseVectorBlob => &self.node_dense_vectors_mmap,
            SegmentComponentKind::NodeSparseVectorBlob => &self.node_sparse_vectors_mmap,
            SegmentComponentKind::EdgeWeightIndex => &self.edge_weight_index_mmap,
            SegmentComponentKind::EdgeUpdatedAtIndex => &self.edge_updated_at_index_mmap,
            SegmentComponentKind::EdgeValidFromIndex => &self.edge_valid_from_index_mmap,
            SegmentComponentKind::EdgeValidToIndex => &self.edge_valid_to_index_mmap,
            _ => return None,
        };
        data.mapping_identity_for_test()
    }

    pub(crate) fn optional_component_availability(
        &self,
        kind: SegmentComponentKind,
    ) -> ComponentAvailability {
        self.component_registry.availability(&kind)
    }

    #[cfg(test)]
    pub(crate) fn planner_stats(&self) -> Option<&SegmentPlannerStatsV1> {
        self.planner_stats.stats()
    }

    #[cfg(test)]
    pub(crate) fn planner_stats_available(&self) -> bool {
        self.planner_stats.is_available()
    }

    pub(crate) fn planner_stats_availability(&self) -> &PlannerStatsAvailability {
        &self.planner_stats
    }

    #[cfg(test)]
    pub(crate) fn planner_stats_debug_snapshot_for_test(&self) -> PlannerStatsAvailability {
        self.planner_stats.clone()
    }

    // --- Metadata payload accessors (for V3 compaction) ---

    /// Number of node metadata entries.
    pub(crate) fn node_meta_count(&self) -> u64 {
        let data = &self.node_meta_mmap[..];
        if data.len() < 8 {
            return 0;
        }
        read_u64_at(data, 0).unwrap_or(0)
    }

    pub(crate) fn node_meta_count_for_scrub(
        &self,
        expected_count: u64,
    ) -> Result<usize, EngineError> {
        let data = &self.node_meta_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(format!(
                "node metadata payload length {} is too short for count header",
                data.len()
            )));
        }
        let actual_count = read_u64_at(data, 0)?;
        if actual_count != expected_count {
            return Err(EngineError::CorruptRecord(format!(
                "node metadata row count {} does not match segment manifest node_count {}",
                actual_count, expected_count
            )));
        }
        let expected_count = usize_from_u64(expected_count, "segment manifest node_count")?;
        let Some(layout) = parse_node_meta_layout(data)? else {
            return Err(EngineError::CorruptRecord(
                "node metadata payload is missing".into(),
            ));
        };
        if layout.node_count != expected_count {
            return Err(EngineError::CorruptRecord(format!(
                "node metadata parsed row count {} does not match segment manifest node_count {}",
                layout.node_count, expected_count
            )));
        }
        Ok(layout.node_count)
    }

    /// Read a node metadata entry by index (0-based).
    pub(crate) fn node_meta_at(&self, index: usize) -> Result<SegmentNodeMeta, EngineError> {
        let data = &self.node_meta_mmap[..];
        let Some(layout) = parse_node_meta_layout(data)? else {
            return Err(EngineError::CorruptRecord(
                "node metadata payload is missing".into(),
            ));
        };
        read_node_meta_entry_at(data, layout, index)
    }

    pub(crate) fn node_record_for_meta_scrub(
        &self,
        meta: &SegmentNodeMeta,
    ) -> Result<NodeRecord, EngineError> {
        let offset = usize_from_u64(meta.data_offset, "node record offset")?;
        let expected_end = offset
            .checked_add(meta.data_len as usize)
            .ok_or_else(|| EngineError::CorruptRecord("node record span overflow".into()))?;
        let (node, actual_end) = decode_node_at_with_end(&self.nodes_mmap, offset, meta.node_id)?;
        if actual_end != expected_end {
            return Err(EngineError::CorruptRecord(format!(
                "node record {} decoded span [{}, {}) does not match metadata span [{}, {})",
                meta.node_id, offset, actual_end, offset, expected_end
            )));
        }
        Ok(node)
    }

    pub(crate) fn node_label_index_entries_for_scrub(
        &self,
        expected_node_count: u64,
    ) -> Result<Vec<(u32, u64)>, EngineError> {
        let data = &self.node_label_index_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(
                "node label index missing or truncated (< 8 bytes)".into(),
            ));
        }
        let max_memberships = usize_from_u64(
            expected_node_count
                .checked_mul(MAX_NODE_LABELS_PER_NODE as u64)
                .ok_or_else(|| {
                    EngineError::CorruptRecord(
                        "node label index expected membership cap overflow".into(),
                    )
                })?,
            "node label index expected membership cap",
        )?;
        let count = usize_from_u64(read_u64_at(data, 0)?, "node label index row count")?;
        if count > max_memberships {
            return Err(EngineError::CorruptRecord(format!(
                "node label index row count {} exceeds maximum label memberships {}",
                count, max_memberships
            )));
        }
        let index_start = 8usize;
        let index_end = index_start
            .checked_add(
                count
                    .checked_mul(LABEL_POSTING_INDEX_ENTRY_SIZE)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord("node label index table size overflow".into())
                    })?,
            )
            .ok_or_else(|| {
                EngineError::CorruptRecord("node label index table end overflow".into())
            })?;
        if index_end > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "node label index table [{}, {}) exceeds payload length {}",
                index_start,
                index_end,
                data.len()
            )));
        }

        let mut entries = Vec::new();
        let mut prev_label_id = None;
        let mut expected_posting_offset = index_end;
        let mut total_postings = 0usize;
        for index in 0..count {
            let off = index_start + index * LABEL_POSTING_INDEX_ENTRY_SIZE;
            let label_id = read_u32_at(data, off)?;
            if prev_label_id.is_some_and(|prev| prev >= label_id) {
                return Err(EngineError::CorruptRecord(
                    "node label index label IDs must be sorted ascending and unique".into(),
                ));
            }
            prev_label_id = Some(label_id);

            let posting_offset = usize_from_u64(
                read_u64_at(data, off + 4)?,
                "node label index posting offset",
            )?;
            let posting_count = read_u32_at(data, off + 12)? as usize;
            if posting_count == 0 {
                return Err(EngineError::CorruptRecord(format!(
                    "node label index label_id {} has an empty posting list",
                    label_id
                )));
            }
            total_postings = total_postings.checked_add(posting_count).ok_or_else(|| {
                EngineError::CorruptRecord("node label index posting count overflow".into())
            })?;
            if total_postings > max_memberships {
                return Err(EngineError::CorruptRecord(format!(
                    "node label index posting count {} exceeds maximum label memberships {}",
                    total_postings, max_memberships
                )));
            }
            let posting_end = posting_offset
                .checked_add(posting_count.checked_mul(8).ok_or_else(|| {
                    EngineError::CorruptRecord("node label index posting size overflow".into())
                })?)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("node label index posting end overflow".into())
                })?;
            if posting_offset != expected_posting_offset {
                return Err(EngineError::CorruptRecord(format!(
                    "node label index posting range for label_id {} starts at {}, expected {}",
                    label_id, posting_offset, expected_posting_offset
                )));
            }
            if posting_offset < index_end || posting_end > data.len() {
                return Err(EngineError::CorruptRecord(format!(
                    "node label index posting [{}, {}) is outside payload data region [{}, {})",
                    posting_offset,
                    posting_end,
                    index_end,
                    data.len()
                )));
            }

            let mut prev_node_id = None;
            for posting_index in 0..posting_count {
                let node_id = read_u64_at(data, posting_offset + posting_index * 8)?;
                if prev_node_id.is_some_and(|prev| prev >= node_id) {
                    return Err(EngineError::CorruptRecord(format!(
                        "node label index posting for label_id {} must be sorted ascending and unique",
                        label_id
                    )));
                }
                prev_node_id = Some(node_id);
                entries.push((label_id, node_id));
            }
            expected_posting_offset = posting_end;
        }
        Ok(entries)
    }

    pub(crate) fn node_key_index_entries_for_scrub(
        &self,
    ) -> Result<Vec<(u32, String, u64)>, EngineError> {
        let data = &self.key_index_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(
                "node key index missing or truncated (< 8 bytes)".into(),
            ));
        }
        let count = read_u64_at(data, 0)? as usize;
        let offset_table_start = 8usize;
        let data_start = offset_table_start
            .checked_add(count.checked_mul(8).ok_or_else(|| {
                EngineError::CorruptRecord("node key index offset table size overflow".into())
            })?)
            .ok_or_else(|| {
                EngineError::CorruptRecord("node key index offset table end overflow".into())
            })?;
        if data_start > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "node key index offset table end {} exceeds payload length {}",
                data_start,
                data.len()
            )));
        }

        let mut entries = Vec::with_capacity(count);
        let mut prev_entry: Option<(u32, String, u64)> = None;
        let mut prev_offset = data_start;
        for index in 0..count {
            let entry_offset = usize_from_u64(
                read_u64_at(data, offset_table_start + index * 8)?,
                "node key index entry offset",
            )?;
            if entry_offset < data_start || entry_offset < prev_offset || entry_offset >= data.len()
            {
                return Err(EngineError::CorruptRecord(format!(
                    "node key index entry offset {} at row {} is outside or before the data region",
                    entry_offset, index
                )));
            }
            prev_offset = entry_offset;

            let label_id = read_u32_at(data, entry_offset)?;
            let node_id = read_u64_at(data, entry_offset + 4)?;
            let key_len = read_u16_at(data, entry_offset + 12)? as usize;
            let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
            let key = std::str::from_utf8(key_bytes)
                .map_err(|_| {
                    EngineError::CorruptRecord(format!(
                        "invalid UTF-8 in node key index at offset {}",
                        entry_offset + 14
                    ))
                })?
                .to_string();
            let entry = (label_id, key, node_id);
            if prev_entry.as_ref().is_some_and(|prev| prev >= &entry) {
                return Err(EngineError::CorruptRecord(
                    "node key index entries must be sorted ascending and unique".into(),
                ));
            }
            prev_entry = Some(entry.clone());
            entries.push(entry);
        }
        Ok(entries)
    }

    pub(crate) fn node_timestamp_index_entries_for_scrub(
        &self,
    ) -> Result<Vec<(u32, i64, u64)>, EngineError> {
        let data = &self.timestamp_index_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(
                "node timestamp index missing or truncated (< 8 bytes)".into(),
            ));
        }
        let count = read_u64_at(data, 0)? as usize;
        let entry_start = 8usize;
        let entry_size = 20usize;
        let entry_end = entry_start
            .checked_add(count.checked_mul(entry_size).ok_or_else(|| {
                EngineError::CorruptRecord("node timestamp index size overflow".into())
            })?)
            .ok_or_else(|| {
                EngineError::CorruptRecord("node timestamp index end overflow".into())
            })?;
        if entry_end > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "node timestamp index entries [{}, {}) exceed payload length {}",
                entry_start,
                entry_end,
                data.len()
            )));
        }

        let mut entries = Vec::with_capacity(count);
        let mut prev_entry: Option<(u32, i64, u64)> = None;
        for index in 0..count {
            let off = entry_start + index * entry_size;
            let entry = (
                read_u32_at(data, off)?,
                read_i64_at(data, off + 4)?,
                read_u64_at(data, off + 12)?,
            );
            if prev_entry.is_some_and(|prev| prev >= entry) {
                return Err(EngineError::CorruptRecord(
                    "node timestamp index entries must be sorted ascending and unique".into(),
                ));
            }
            prev_entry = Some(entry);
            entries.push(entry);
        }
        Ok(entries)
    }

    pub(crate) fn node_vector_meta_at(
        &self,
        index: usize,
    ) -> Result<(u64, u32, u64, u32), EngineError> {
        let data = &self.node_vector_meta_mmap[..];
        if data.is_empty() {
            return Ok((0, 0, 0, 0));
        }
        let (flags, dense_offset, dense_len, sparse_offset, sparse_len) =
            read_node_vector_meta_entry(data, index)?;
        Ok((
            if flags & NODE_VECTOR_FLAG_DENSE != 0 {
                dense_offset
            } else {
                0
            },
            if flags & NODE_VECTOR_FLAG_DENSE != 0 {
                dense_len
            } else {
                0
            },
            if flags & NODE_VECTOR_FLAG_SPARSE != 0 {
                sparse_offset
            } else {
                0
            },
            if flags & NODE_VECTOR_FLAG_SPARSE != 0 {
                sparse_len
            } else {
                0
            },
        ))
    }

    /// Number of edge metadata entries.
    pub(crate) fn edge_meta_count(&self) -> u64 {
        let data = &self.edge_meta_mmap[..];
        if data.len() < 8 {
            return 0;
        }
        read_u64_at(data, 0).unwrap_or(0)
    }

    /// Read an edge metadata entry by index (0-based).
    /// Returns (edge_id, data_offset, data_len, from, to, label_id, updated_at,
    ///          weight, valid_from, valid_to, last_write_seq).
    #[allow(clippy::type_complexity)]
    pub(crate) fn edge_meta_at(
        &self,
        index: usize,
    ) -> Result<(u64, u64, u32, u64, u64, u32, i64, f32, i64, i64, u64), EngineError> {
        let data = &self.edge_meta_mmap[..];
        let off = 8 + index * EDGE_META_ENTRY_SIZE;
        let edge_id = read_u64_at(data, off)?;
        let data_offset = read_u64_at(data, off + 8)?;
        let data_len = read_u32_at(data, off + 16)?;
        let from = read_u64_at(data, off + 20)?;
        let to = read_u64_at(data, off + 28)?;
        let label_id = read_u32_at(data, off + 36)?;
        let updated_at = read_i64_at(data, off + 40)?;
        let weight = read_f32_at(data, off + 48)?;
        let valid_from = read_i64_at(data, off + 52)?;
        let valid_to = read_i64_at(data, off + 60)?;
        let last_write_seq = read_u64_at(data, off + 68)?;
        Ok((
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
        ))
    }

    pub(crate) fn raw_node_dense_vectors_mmap(&self) -> &[u8] {
        &self.node_dense_vectors_mmap[..]
    }

    pub(crate) fn raw_node_sparse_vectors_mmap(&self) -> &[u8] {
        &self.node_sparse_vectors_mmap[..]
    }

    #[cfg(test)]
    pub(crate) fn raw_sparse_posting_index_mmap(&self) -> &[u8] {
        &self.sparse_posting_index_mmap[..]
    }

    #[cfg(test)]
    pub(crate) fn raw_sparse_postings_mmap(&self) -> &[u8] {
        &self.sparse_postings_mmap[..]
    }

    pub(crate) fn dense_hnsw_header(&self) -> Option<DenseHnswHeader> {
        if !self.dense_hnsw_available.load(Ordering::Acquire) {
            return None;
        }
        self.dense_hnsw_header
    }

    pub(crate) fn dense_vector_count(&self) -> usize {
        self.dense_vector_count
    }

    pub(crate) fn sparse_vector_count(&self) -> usize {
        self.sparse_vector_count
    }

    pub(crate) fn sparse_postings_available(&self) -> bool {
        self.sparse_postings_available.load(Ordering::Acquire)
            && !self.sparse_posting_index_mmap.is_empty()
            && !self.sparse_postings_mmap.is_empty()
    }

    fn mark_dense_hnsw_unavailable(&self, error: impl ToString) {
        self.dense_hnsw_available.store(false, Ordering::Release);
        mark_optional_components_corrupt(
            &self.component_registry,
            &[
                SegmentComponentKind::DenseHnswMetadata,
                SegmentComponentKind::DenseHnswGraph,
            ],
            error.to_string(),
        );
    }

    fn mark_sparse_postings_unavailable(&self, error: impl ToString) {
        self.sparse_postings_available
            .store(false, Ordering::Release);
        mark_optional_components_corrupt(
            &self.component_registry,
            &[
                SegmentComponentKind::SparsePostingIndex,
                SegmentComponentKind::SparsePostings,
            ],
            error.to_string(),
        );
    }

    fn dense_hnsw_points_for_search(
        &self,
        header: DenseHnswHeader,
    ) -> Result<&[DenseQueryPoint], EngineError> {
        let points = self.dense_hnsw_points.get_or_init(|| {
            load_dense_hnsw_query_points(&self.dense_hnsw_meta_mmap, header)
                .map(Vec::into_boxed_slice)
                .map_err(|error| error.to_string())
        });
        match points {
            Ok(points) => Ok(points.as_ref()),
            Err(error) => {
                self.mark_dense_hnsw_unavailable(error);
                Err(EngineError::CorruptRecord(error.clone()))
            }
        }
    }

    fn ensure_sparse_posting_index_shape_for_search(&self) -> Result<(), EngineError> {
        let validation = self.sparse_posting_index_shape.get_or_init(|| {
            validate_sparse_posting_index_shape_for_search(
                &self.sparse_posting_index_mmap,
                &self.sparse_postings_mmap,
            )
            .map_err(|error| error.to_string())
        });
        match validation {
            Ok(()) => Ok(()),
            Err(error) => {
                self.mark_sparse_postings_unavailable(error);
                Err(EngineError::CorruptRecord(error.clone()))
            }
        }
    }

    pub(crate) fn accumulate_sparse_posting_scores(
        &self,
        query: &[(u32, f32)],
        scores: &mut NodeIdMap<f32>,
    ) -> Result<(), EngineError> {
        if !self.sparse_postings_available() {
            return Ok(());
        }
        self.ensure_sparse_posting_index_shape_for_search()?;
        let result = accumulate_sparse_posting_scores_from_bytes(
            &self.sparse_posting_index_mmap,
            &self.sparse_postings_mmap,
            query,
            scores,
        );
        if let Err(error) = &result {
            self.mark_sparse_postings_unavailable(error.to_string());
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exact_dense_vector_search<F>(
        &self,
        query: &[f32],
        metric: DenseMetric,
        query_norm: Option<f32>,
        scope_ids: Option<&NodeIdSet>,
        hidden_ids: &NodeIdSet,
        mut include: F,
        hits_out: &mut Vec<VectorHit>,
    ) -> Result<(), EngineError>
    where
        F: FnMut(NodeLabelSet, i64, f32) -> bool,
    {
        if self.dense_vector_count == 0 {
            return Ok(());
        }

        for index in 0..self.node_meta_count() as usize {
            let meta = self.node_meta_at(index)?;
            if self.deleted_nodes.contains_key(&meta.node_id)
                || hidden_ids.contains(&meta.node_id)
                || scope_ids.is_some_and(|scope| !scope.contains(&meta.node_id))
            {
                continue;
            }
            let (dense_offset, dense_len, _, _) = self.node_vector_meta_at(index)?;
            if dense_len == 0 || !include(meta.label_ids, meta.updated_at, meta.weight) {
                continue;
            }
            hits_out.push(VectorHit {
                node_id: meta.node_id,
                score: dense_score_from_bytes(
                    metric,
                    query,
                    query_norm,
                    &self.node_dense_vectors_mmap,
                    dense_offset as usize,
                    dense_len as usize,
                )?,
            });
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exact_sparse_vector_scores<F>(
        &self,
        query: &[(u32, f32)],
        scope_ids: Option<&NodeIdSet>,
        hidden_ids: &NodeIdSet,
        mut include: F,
        hits_out: &mut Vec<(u64, f32)>,
    ) -> Result<(), EngineError>
    where
        F: FnMut(NodeLabelSet, i64, f32) -> bool,
    {
        if self.sparse_vector_count == 0 || query.is_empty() {
            return Ok(());
        }

        for index in 0..self.node_meta_count() as usize {
            let meta = self.node_meta_at(index)?;
            if self.deleted_nodes.contains_key(&meta.node_id)
                || hidden_ids.contains(&meta.node_id)
                || scope_ids.is_some_and(|scope| !scope.contains(&meta.node_id))
            {
                continue;
            }
            let (_, _, sparse_offset, sparse_len) = self.node_vector_meta_at(index)?;
            if sparse_len == 0 || !include(meta.label_ids, meta.updated_at, meta.weight) {
                continue;
            }
            let score = sparse_dot_score_from_blob(
                query,
                &self.node_sparse_vectors_mmap,
                sparse_offset as usize,
                sparse_len as usize,
            )?;
            if score > 0.0 {
                hits_out.push((meta.node_id, score));
            }
        }

        Ok(())
    }

    pub(crate) fn search_dense_hnsw(
        &self,
        query: &[f32],
        ef_search: usize,
        limit: usize,
    ) -> Result<Vec<(u64, f32)>, EngineError> {
        let Some(header) = self.dense_hnsw_header() else {
            return Ok(Vec::new());
        };
        let points = self.dense_hnsw_points_for_search(header)?;
        let result = search_dense_hnsw_with_points(
            header,
            points,
            &self.dense_hnsw_graph_mmap,
            &self.node_dense_vectors_mmap,
            query,
            ef_search,
            limit,
        );
        if let Err(error) = &result {
            self.mark_dense_hnsw_unavailable(error.to_string());
        }
        result
    }

    pub(crate) fn search_dense_hnsw_scoped(
        &self,
        query: &[f32],
        ef_search: usize,
        limit: usize,
        scope_ids: &crate::types::NodeIdSet,
    ) -> Result<Vec<(u64, f32)>, EngineError> {
        let Some(header) = self.dense_hnsw_header() else {
            return Ok(Vec::new());
        };
        let points = self.dense_hnsw_points_for_search(header)?;
        let result = search_dense_hnsw_scoped_with_points(
            header,
            points,
            &self.dense_hnsw_graph_mmap,
            &self.node_dense_vectors_mmap,
            query,
            ef_search,
            limit,
            scope_ids,
        );
        if let Err(error) = &result {
            self.mark_dense_hnsw_unavailable(error.to_string());
        }
        result
    }

    pub(crate) fn raw_dense_hnsw_meta_mmap(&self) -> &[u8] {
        &self.dense_hnsw_meta_mmap[..]
    }

    #[cfg(test)]
    pub(crate) fn raw_dense_hnsw_graph_mmap(&self) -> &[u8] {
        &self.dense_hnsw_graph_mmap[..]
    }

    fn hydrate_node_vectors(&self, index: usize, node: &mut NodeRecord) -> Result<(), EngineError> {
        let (dense_offset, dense_len, sparse_offset, sparse_len) =
            self.node_vector_meta_at(index)?;

        if dense_len > 0 {
            node.dense_vector = self.read_dense_vector_from_blob(dense_offset, dense_len)?;
        }

        if sparse_len > 0 {
            node.sparse_vector = self.read_sparse_vector_from_blob(sparse_offset, sparse_len)?;
        }

        Ok(())
    }

    fn read_node_dense_vector_at_index(
        &self,
        index: usize,
    ) -> Result<Option<DenseVector>, EngineError> {
        let (dense_offset, dense_len, _, _) = self.node_vector_meta_at(index)?;
        self.read_dense_vector_from_blob(dense_offset, dense_len)
    }

    fn read_node_sparse_vector_at_index(
        &self,
        index: usize,
    ) -> Result<Option<SparseVector>, EngineError> {
        let (_, _, sparse_offset, sparse_len) = self.node_vector_meta_at(index)?;
        self.read_sparse_vector_from_blob(sparse_offset, sparse_len)
    }

    fn read_dense_vector_from_blob(
        &self,
        dense_offset: u64,
        dense_len: u32,
    ) -> Result<Option<DenseVector>, EngineError> {
        if dense_len == 0 {
            return Ok(None);
        }
        let mut values = Vec::with_capacity(dense_len as usize);
        let base = dense_offset as usize;
        for i in 0..dense_len as usize {
            values.push(read_f32_at(
                &self.node_dense_vectors_mmap,
                base + i * DENSE_VECTOR_VALUE_SIZE,
            )?);
        }
        Ok(Some(values))
    }

    fn read_sparse_vector_from_blob(
        &self,
        sparse_offset: u64,
        sparse_len: u32,
    ) -> Result<Option<SparseVector>, EngineError> {
        if sparse_len == 0 {
            return Ok(None);
        }
        let mut values = Vec::with_capacity(sparse_len as usize);
        let base = sparse_offset as usize;
        for i in 0..sparse_len as usize {
            let entry_off = base + i * SPARSE_VECTOR_ENTRY_SIZE;
            let dimension_id = read_u32_at(&self.node_sparse_vectors_mmap, entry_off)?;
            let weight = read_f32_at(&self.node_sparse_vectors_mmap, entry_off + 4)?;
            values.push((dimension_id, weight));
        }
        Ok(Some(values))
    }

    // --- Iteration methods (for compaction) ---

    /// Collect all node records in this segment (including tombstoned ones).
    /// Returns records in index order (sorted by node_id).
    #[cfg(test)]
    pub fn all_nodes(&self) -> Result<Vec<NodeRecord>, EngineError> {
        let data = &self.nodes_mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        let idx_start = 8;
        let mut nodes = Vec::with_capacity(count);
        for i in 0..count {
            let entry_off = idx_start + i * NODE_INDEX_ENTRY_SIZE;
            let id = read_u64_at(data, entry_off)?;
            let offset = read_u64_at(data, entry_off + 8)? as usize;
            let mut node = decode_node_at(data, offset, id)?;
            self.hydrate_node_vectors(i, &mut node)?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    // --- Label posting index queries ---

    /// Return node IDs for a given label_id from this segment's node-label index.
    /// Excludes tombstoned nodes.
    pub fn nodes_by_label_id(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        self.query_label_posting_index(&self.node_label_index_mmap, label_id, &self.deleted_nodes)
    }

    /// Return the posting count for a given node label without decoding the posting list.
    pub(crate) fn node_label_posting_count(&self, label_id: u32) -> Result<usize, EngineError> {
        self.label_posting_index_count(&self.node_label_index_mmap, label_id)
    }

    pub(crate) fn node_label_posting(
        &self,
        label_id: u32,
    ) -> Result<Option<SegmentLabelPosting>, EngineError> {
        self.label_posting_index(&self.node_label_index_mmap, label_id)
    }

    pub(crate) fn node_id_at_label_posting(
        &self,
        posting: SegmentLabelPosting,
        index: usize,
    ) -> Result<Option<u64>, EngineError> {
        if index >= posting.count {
            return Ok(None);
        }
        Ok(Some(read_u64_at(
            &self.node_label_index_mmap,
            posting.offset + index * 8,
        )?))
    }

    pub(crate) fn node_label_posting_lower_bound(
        &self,
        posting: SegmentLabelPosting,
        after: u64,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = posting.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let node_id = read_u64_at(&self.node_label_index_mmap, posting.offset + mid * 8)?;
            if node_id <= after {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Return edge IDs for a given label_id from this segment's label index.
    /// Excludes tombstoned edges.
    pub fn edges_by_label_id(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        self.query_label_posting_index(&self.edge_label_index_mmap, label_id, &self.deleted_edges)
    }

    pub(crate) fn edge_label_posting_count(&self, label_id: u32) -> Result<usize, EngineError> {
        self.label_posting_index_count(&self.edge_label_index_mmap, label_id)
    }

    pub(crate) fn edge_label_posting(
        &self,
        label_id: u32,
    ) -> Result<Option<SegmentLabelPosting>, EngineError> {
        self.label_posting_index(&self.edge_label_index_mmap, label_id)
    }

    pub(crate) fn edge_label_id_at_posting(
        &self,
        posting: SegmentLabelPosting,
        index: usize,
    ) -> Result<Option<u64>, EngineError> {
        if index >= posting.count {
            return Ok(None);
        }
        Ok(Some(read_u64_at(
            &self.edge_label_index_mmap,
            posting.offset + index * 8,
        )?))
    }

    pub(crate) fn edge_label_id_lower_bound_posting(
        &self,
        posting: SegmentLabelPosting,
        after: u64,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = posting.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let edge_id = read_u64_at(&self.edge_label_index_mmap, posting.offset + mid * 8)?;
            if edge_id <= after {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Binary search a label posting index file for a given label ID.
    /// Returns record IDs, excluding any in the deleted set.
    fn query_label_posting_index(
        &self,
        mmap: &MappedData,
        target_label_id: u32,
        deleted: &NodeIdMap<TombstoneEntry>,
    ) -> Result<Vec<u64>, EngineError> {
        let data = &mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(Vec::new());
        }

        // Binary search the index section for target_label_id.
        let idx_start = 8;
        // Entry: label_id (4) + offset (8) + count (4) = 16 bytes.
        let entry_size = LABEL_POSTING_INDEX_ENTRY_SIZE;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * entry_size;
            let entry_label_id = read_u32_at(data, entry_off)?;
            if entry_label_id < target_label_id {
                lo = mid + 1;
            } else if entry_label_id > target_label_id {
                hi = mid;
            } else {
                // Found, read the IDs
                let offset = read_u64_at(data, entry_off + 4)? as usize;
                let id_count = read_u32_at(data, entry_off + 12)? as usize;
                let mut result = Vec::with_capacity(id_count);
                for i in 0..id_count {
                    let id = read_u64_at(data, offset + i * 8)?;
                    if !deleted.contains_key(&id) {
                        result.push(id);
                    }
                }
                return Ok(result);
            }
        }

        Ok(Vec::new())
    }

    fn label_posting_index_count(
        &self,
        mmap: &MappedData,
        target_label_id: u32,
    ) -> Result<usize, EngineError> {
        Ok(self
            .label_posting_index(mmap, target_label_id)?
            .map(|posting| posting.count)
            .unwrap_or(0))
    }

    fn label_posting_index(
        &self,
        mmap: &MappedData,
        target_label_id: u32,
    ) -> Result<Option<SegmentLabelPosting>, EngineError> {
        let data = &mmap[..];
        if data.len() < 8 {
            return Ok(None);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(None);
        }

        let idx_start = 8;
        let entry_size = LABEL_POSTING_INDEX_ENTRY_SIZE;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * entry_size;
            let entry_label_id = read_u32_at(data, entry_off)?;
            match entry_label_id.cmp(&target_label_id) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let offset = read_u64_at(data, entry_off + 4)? as usize;
                    let id_count = read_u32_at(data, entry_off + 12)? as usize;
                    let end = offset
                        .checked_add(id_count.checked_mul(8).ok_or_else(|| {
                            EngineError::CorruptRecord(
                                "label posting index payload overflow".into(),
                            )
                        })?)
                        .ok_or_else(|| {
                            EngineError::CorruptRecord(
                                "label posting index payload end overflow".into(),
                            )
                        })?;
                    if end > data.len() {
                        return Err(EngineError::CorruptRecord(format!(
                            "label posting index payload [{}, {}) exceeds file length {}",
                            offset,
                            end,
                            data.len()
                        )));
                    }
                    return Ok(Some(SegmentLabelPosting {
                        offset,
                        count: id_count,
                    }));
                }
            }
        }

        Ok(None)
    }

    /// Return all distinct node label IDs present in this segment's label index.
    pub fn node_label_ids(&self) -> Result<Vec<u32>, EngineError> {
        Self::label_posting_index_ids(&self.node_label_index_mmap)
    }

    /// Extract all label IDs from a label posting index mmap header.
    fn label_posting_index_ids(mmap: &MappedData) -> Result<Vec<u32>, EngineError> {
        let data = &mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        let mut result = Vec::with_capacity(count);
        let idx_start = 8;
        for i in 0..count {
            let entry_off = idx_start + i * LABEL_POSTING_INDEX_ENTRY_SIZE;
            result.push(read_u32_at(data, entry_off)?);
        }
        Ok(result)
    }

    // --- Timestamp index queries ---

    /// Return node IDs within a time range for a given label_id.
    /// Binary search for range start, scan to range end. O(log N + results).
    /// Results are sorted by node_id for K-way merge compatibility.
    pub fn nodes_by_time_range(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let data = &self.timestamp_index_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(
                "timestamp index missing or truncated (< 8 bytes)".into(),
            ));
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(Vec::new());
        }

        let entry_start = 8usize;
        let entry_size = 20usize; // label_id(4) + updated_at(8) + node_id(8)

        // Binary search for the first entry >= (label_id, from_ms, 0)
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = entry_start + mid * entry_size;
            let entry_label_id = read_u32_at(data, off)?;
            let e_time = read_i64_at(data, off + 4)?;
            if (entry_label_id, e_time) < (label_id, from_ms) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Scan from lo until label_id changes or updated_at > to_ms
        let mut result = Vec::new();
        let mut pos = lo;
        while pos < count {
            let off = entry_start + pos * entry_size;
            let entry_label_id = read_u32_at(data, off)?;
            if entry_label_id != label_id {
                break;
            }
            let e_time = read_i64_at(data, off + 4)?;
            if e_time > to_ms {
                break;
            }
            let node_id = read_u64_at(data, off + 12)?;
            if !self.deleted_nodes.contains_key(&node_id) {
                result.push(node_id);
            }
            pos += 1;
        }

        // Sort by node_id for K-way merge compatibility
        result.sort_unstable();
        Ok(result)
    }

    pub(crate) fn for_each_node_by_time_range<F>(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        mut callback: F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        let data = &self.timestamp_index_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(
                "timestamp index missing or truncated (< 8 bytes)".into(),
            ));
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 || from_ms > to_ms {
            return Ok(ControlFlow::Continue(()));
        }

        let entry_start = 8usize;
        let entry_size = 20usize;

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = entry_start + mid * entry_size;
            let entry_label_id = read_u32_at(data, off)?;
            let e_time = read_i64_at(data, off + 4)?;
            if (entry_label_id, e_time) < (label_id, from_ms) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        let mut pos = lo;
        while pos < count {
            let off = entry_start + pos * entry_size;
            let entry_label_id = read_u32_at(data, off)?;
            if entry_label_id != label_id {
                break;
            }
            let e_time = read_i64_at(data, off + 4)?;
            if e_time > to_ms {
                break;
            }
            let node_id = read_u64_at(data, off + 12)?;
            if !self.deleted_nodes.contains_key(&node_id) && callback(node_id).is_break() {
                return Ok(ControlFlow::Break(()));
            }
            pos += 1;
        }

        Ok(ControlFlow::Continue(()))
    }

    // --- Edge triple index ---

    /// Look up an edge by (from, to, label_id) triple. Returns the edge record
    /// if found and not tombstoned, or None.
    pub fn edge_by_triple(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let data = &self.edge_triple_index_mmap[..];
        if data.len() < 8 {
            return Ok(None);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(None);
        }

        let entries_start = 8;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = entries_start + mid * EDGE_TRIPLE_ENTRY_SIZE;
            let e_from = read_u64_at(data, off)?;
            let e_to = read_u64_at(data, off + 8)?;
            let e_label_id = read_u32_at(data, off + 16)?;

            match e_from.cmp(&from) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => match e_to.cmp(&to) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => match e_label_id.cmp(&label_id) {
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid,
                        std::cmp::Ordering::Equal => {
                            let edge_id = read_u64_at(data, off + 20)?;
                            return self.get_edge(edge_id);
                        }
                    },
                },
            }
        }

        Ok(None)
    }

    pub fn resolve_triples_batch(
        &self,
        lookups: &[(usize, u64, u64, u32)],
        results: &mut [Option<EdgeRecord>],
    ) -> Result<Vec<usize>, EngineError> {
        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let resolved = self.resolve_triples_to_ids(lookups)?;
        if resolved.is_empty() {
            return Ok(Vec::new());
        }

        let found_indices: Vec<usize> = resolved.iter().map(|&(orig_idx, _)| orig_idx).collect();
        let mut edge_lookups: Vec<(usize, u64)> = resolved
            .iter()
            .filter(|&&(_, eid)| !self.deleted_edges.contains_key(&eid))
            .copied()
            .collect();
        edge_lookups.sort_unstable_by_key(|&(_, eid)| eid);
        self.get_edges_batch(&edge_lookups, results)?;

        Ok(found_indices)
    }

    fn resolve_triples_to_ids(
        &self,
        lookups: &[(usize, u64, u64, u32)],
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let mut resolved = Vec::new();
        let data = &self.edge_triple_index_mmap[..];
        if data.len() < 8 {
            return Ok(resolved);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(resolved);
        }

        let entries_start = 8;
        let unique_triples = {
            let mut n = 0usize;
            let mut prev: Option<(u64, u64, u32)> = None;
            for &(_, from, to, label_id) in lookups {
                if prev != Some((from, to, label_id)) {
                    n += 1;
                    prev = Some((from, to, label_id));
                }
            }
            n
        };

        let strategy = if unique_triples <= 2 || count <= 1 {
            BatchReadStrategy::SeekPerKey
        } else {
            let min_triple = (lookups[0].1, lookups[0].2, lookups[0].3);
            let last = lookups[lookups.len() - 1];
            let max_triple = (last.1, last.2, last.3);
            let span_start = lower_bound_edge_triple_index(data, entries_start, count, min_triple)?;
            let span_end = upper_bound_edge_triple_index(data, entries_start, count, max_triple)?;
            let span = span_end.saturating_sub(span_start).max(unique_triples);
            let seek_cost = unique_triples
                .saturating_mul(ceil_log2_usize(count))
                .saturating_mul(BATCH_RANDOM_ACCESS_PENALTY);
            if seek_cost <= span {
                BatchReadStrategy::SeekPerKey
            } else {
                BatchReadStrategy::MergeWalk
            }
        };

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_query: Option<(u64, u64, u32)> = None;
            let mut prev_edge_id: Option<u64> = None;
            for &(orig_idx, from, to, label_id) in lookups {
                let edge_id = if prev_query == Some((from, to, label_id)) {
                    prev_edge_id
                } else {
                    let found = binary_search_edge_triple_index(
                        data,
                        entries_start,
                        count,
                        (from, to, label_id),
                    )?;
                    prev_query = Some((from, to, label_id));
                    prev_edge_id = found;
                    found
                };
                if let Some(eid) = edge_id {
                    resolved.push((orig_idx, eid));
                }
            }
        } else {
            let mut idx_pos = 0usize;
            let mut prev_query: Option<(u64, u64, u32)> = None;
            let mut prev_edge_id: Option<u64> = None;
            for &(orig_idx, from, to, label_id) in lookups {
                if prev_query == Some((from, to, label_id)) {
                    if let Some(eid) = prev_edge_id {
                        resolved.push((orig_idx, eid));
                    }
                    continue;
                }
                prev_query = Some((from, to, label_id));
                prev_edge_id = None;

                while idx_pos < count {
                    let off = entries_start + idx_pos * EDGE_TRIPLE_ENTRY_SIZE;
                    let entry_from = read_u64_at(data, off)?;
                    let entry_to = read_u64_at(data, off + 8)?;
                    let entry_label_id = read_u32_at(data, off + 16)?;
                    match (entry_from, entry_to, entry_label_id).cmp(&(from, to, label_id)) {
                        std::cmp::Ordering::Less => {
                            idx_pos += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            let edge_id = read_u64_at(data, off + 20)?;
                            prev_edge_id = Some(edge_id);
                            resolved.push((orig_idx, edge_id));
                            break;
                        }
                        std::cmp::Ordering::Greater => {
                            break;
                        }
                    }
                }
            }
        }

        Ok(resolved)
    }

    pub(crate) fn edge_ids_by_triple(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Vec<u64>, EngineError> {
        let data = &self.edge_triple_index_mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(Vec::new());
        }

        let entries_start = 8;
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = entries_start + mid * EDGE_TRIPLE_ENTRY_SIZE;
            let e_from = read_u64_at(data, off)?;
            let e_to = read_u64_at(data, off + 8)?;
            let e_label_id = read_u32_at(data, off + 16)?;
            if (e_from, e_to, e_label_id) < (from, to, label_id) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        let mut ids = Vec::new();
        let mut pos = lo;
        while pos < count {
            let off = entries_start + pos * EDGE_TRIPLE_ENTRY_SIZE;
            let e_from = read_u64_at(data, off)?;
            let e_to = read_u64_at(data, off + 8)?;
            let e_label_id = read_u32_at(data, off + 16)?;
            if (e_from, e_to, e_label_id) != (from, to, label_id) {
                break;
            }
            let edge_id = read_u64_at(data, off + 20)?;
            if !self.deleted_edges.contains_key(&edge_id) {
                ids.push(edge_id);
            }
            pos += 1;
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    pub(crate) fn edge_weight_index_available(&self) -> bool {
        self.edge_weight_index_count.is_some()
            && self
                .component_registry
                .availability(&SegmentComponentKind::EdgeWeightIndex)
                .is_available()
    }

    pub(crate) fn edge_updated_at_index_available(&self) -> bool {
        self.edge_i64_metadata_index_available(EDGE_UPDATED_AT_INDEX_LOGICAL_NAME)
    }

    pub(crate) fn edge_valid_from_index_available(&self) -> bool {
        self.edge_i64_metadata_index_available(EDGE_VALID_FROM_INDEX_LOGICAL_NAME)
    }

    pub(crate) fn edge_valid_to_index_available(&self) -> bool {
        self.edge_i64_metadata_index_available(EDGE_VALID_TO_INDEX_LOGICAL_NAME)
    }

    fn edge_i64_metadata_index_available(&self, logical_name: &str) -> bool {
        let Some(kind) = edge_i64_metadata_component_kind(logical_name) else {
            return false;
        };
        self.edge_i64_metadata_index_count(logical_name).is_some()
            && self.component_registry.availability(&kind).is_available()
    }

    fn mark_edge_metadata_component_corrupt(
        &self,
        kind: SegmentComponentKind,
        error: &EngineError,
    ) {
        mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
    }

    fn edge_i64_metadata_index_count(&self, logical_name: &str) -> Option<usize> {
        match logical_name {
            EDGE_UPDATED_AT_INDEX_LOGICAL_NAME => self.edge_updated_at_index_count,
            EDGE_VALID_FROM_INDEX_LOGICAL_NAME => self.edge_valid_from_index_count,
            EDGE_VALID_TO_INDEX_LOGICAL_NAME => self.edge_valid_to_index_count,
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_weight_range(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
    ) -> Option<Vec<u64>> {
        self.edge_ids_by_weight_range_limited(label_id, bounds, usize::MAX)
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_weight_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
        limit: usize,
    ) -> Option<Vec<u64>> {
        let data = &self.edge_weight_index_mmap[..];
        if !self.edge_weight_index_available() {
            return None;
        }
        let count = self.edge_weight_index_count?;
        match self.edge_ids_by_weight_range_inner(data, count, label_id, bounds, limit) {
            Ok(ids) => Some(ids),
            Err(error) => {
                self.mark_edge_metadata_component_corrupt(
                    SegmentComponentKind::EdgeWeightIndex,
                    &error,
                );
                None
            }
        }
    }

    pub(crate) fn for_each_edge_id_by_weight_range<F>(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
        callback: &mut F,
    ) -> Result<Option<ControlFlow<()>>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        let data = &self.edge_weight_index_mmap[..];
        if !self.edge_weight_index_available() {
            return Ok(None);
        }
        let count = self.edge_weight_index_count.expect("availability checked");
        match self.for_each_edge_id_by_weight_range_inner(data, count, label_id, bounds, callback) {
            Ok(flow) => Ok(Some(flow)),
            Err(error) => {
                self.mark_edge_metadata_component_corrupt(
                    SegmentComponentKind::EdgeWeightIndex,
                    &error,
                );
                Err(error)
            }
        }
    }

    pub(crate) fn edge_weight_range_count(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
    ) -> Option<usize> {
        let data = &self.edge_weight_index_mmap[..];
        if !self.edge_weight_index_available() {
            return None;
        }
        let count = self.edge_weight_index_count?;
        match self.edge_weight_range_count_inner(data, count, label_id, bounds) {
            Ok(count) => Some(count),
            Err(error) => {
                self.mark_edge_metadata_component_corrupt(
                    SegmentComponentKind::EdgeWeightIndex,
                    &error,
                );
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_updated_at_range(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Option<Vec<u64>> {
        self.edge_ids_by_updated_at_range_limited(label_id, bounds, usize::MAX)
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_updated_at_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Option<Vec<u64>> {
        self.edge_ids_by_i64_metadata_range(
            &self.edge_updated_at_index_mmap,
            EDGE_UPDATED_AT_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
            limit,
        )
    }

    pub(crate) fn for_each_edge_id_by_updated_at_range<F>(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        callback: &mut F,
    ) -> Result<Option<ControlFlow<()>>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        self.for_each_edge_id_by_i64_metadata_range(
            &self.edge_updated_at_index_mmap,
            EDGE_UPDATED_AT_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
            callback,
        )
    }

    pub(crate) fn edge_updated_at_range_count(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Option<usize> {
        self.edge_i64_metadata_range_count(
            &self.edge_updated_at_index_mmap,
            EDGE_UPDATED_AT_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
        )
    }

    pub(crate) fn for_each_edge_id_by_valid_from_range<F>(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        callback: &mut F,
    ) -> Result<Option<ControlFlow<()>>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        self.for_each_edge_id_by_i64_metadata_range(
            &self.edge_valid_from_index_mmap,
            EDGE_VALID_FROM_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
            callback,
        )
    }

    pub(crate) fn edge_valid_from_range_count(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Option<usize> {
        self.edge_i64_metadata_range_count(
            &self.edge_valid_from_index_mmap,
            EDGE_VALID_FROM_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
        )
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_valid_to_range(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Option<Vec<u64>> {
        self.edge_ids_by_valid_to_range_limited(label_id, bounds, usize::MAX)
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_valid_to_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Option<Vec<u64>> {
        self.edge_ids_by_i64_metadata_range(
            &self.edge_valid_to_index_mmap,
            EDGE_VALID_TO_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
            limit,
        )
    }

    pub(crate) fn for_each_edge_id_by_valid_to_range<F>(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        callback: &mut F,
    ) -> Result<Option<ControlFlow<()>>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        self.for_each_edge_id_by_i64_metadata_range(
            &self.edge_valid_to_index_mmap,
            EDGE_VALID_TO_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
            callback,
        )
    }

    pub(crate) fn edge_valid_to_range_count(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Option<usize> {
        self.edge_i64_metadata_range_count(
            &self.edge_valid_to_index_mmap,
            EDGE_VALID_TO_INDEX_LOGICAL_NAME,
            label_id,
            bounds,
        )
    }

    #[cfg(test)]
    fn edge_ids_by_weight_range_inner(
        &self,
        data: &[u8],
        count: usize,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let lower_key = match bounds.lower {
            Some(lower) => match encode_edge_weight_key(lower) {
                Some(key) => Some(key),
                None => return Ok(Vec::new()),
            },
            None => None,
        };
        let upper_key = match bounds.upper {
            Some(upper) => match encode_edge_weight_key(upper) {
                Some(key) => Some(key),
                None => return Ok(Vec::new()),
            },
            None => None,
        };
        let mut ids = Vec::new();
        let start = match label_id {
            Some(target) => {
                self.edge_weight_label_value_lower_bound(data, count, target, lower_key)?
            }
            None => 0,
        };
        let mut pos = start;
        while pos < count {
            let off = 8 + pos * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            let entry_label_id = read_u32_at(data, off)?;
            if label_id.is_some_and(|target| entry_label_id != target) {
                break;
            }
            let value = read_u32_at(data, off + 4)?;
            if label_id.is_some()
                && upper_key.is_some_and(|upper| {
                    if bounds.upper_inclusive {
                        value > upper
                    } else {
                        value >= upper
                    }
                })
            {
                break;
            }
            if key_matches_bounds(
                value,
                lower_key,
                bounds.lower_inclusive,
                upper_key,
                bounds.upper_inclusive,
            ) {
                let edge_id = read_u64_at(data, off + 8)?;
                if !self.deleted_edges.contains_key(&edge_id) {
                    ids.push(edge_id);
                    if ids.len() > limit {
                        break;
                    }
                }
            }
            pos += 1;
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn for_each_edge_id_by_weight_range_inner<F>(
        &self,
        data: &[u8],
        count: usize,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        let lower_key = match bounds.lower {
            Some(lower) => match encode_edge_weight_key(lower) {
                Some(key) => Some(key),
                None => return Ok(ControlFlow::Continue(())),
            },
            None => None,
        };
        let upper_key = match bounds.upper {
            Some(upper) => match encode_edge_weight_key(upper) {
                Some(key) => Some(key),
                None => return Ok(ControlFlow::Continue(())),
            },
            None => None,
        };
        let start = match label_id {
            Some(target) => {
                self.edge_weight_label_value_lower_bound(data, count, target, lower_key)?
            }
            None => 0,
        };
        let mut pos = start;
        while pos < count {
            let off = 8 + pos * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            let entry_label_id = read_u32_at(data, off)?;
            if label_id.is_some_and(|target| entry_label_id != target) {
                break;
            }
            let value = read_u32_at(data, off + 4)?;
            if label_id.is_some()
                && upper_key.is_some_and(|upper| {
                    if bounds.upper_inclusive {
                        value > upper
                    } else {
                        value >= upper
                    }
                })
            {
                break;
            }
            if key_matches_bounds(
                value,
                lower_key,
                bounds.lower_inclusive,
                upper_key,
                bounds.upper_inclusive,
            ) {
                let edge_id = read_u64_at(data, off + 8)?;
                if !self.deleted_edges.contains_key(&edge_id) && callback(edge_id).is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            pos += 1;
        }
        Ok(ControlFlow::Continue(()))
    }

    fn edge_weight_label_value_lower_bound(
        &self,
        data: &[u8],
        count: usize,
        target_label: u32,
        lower_key: Option<u32>,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            let entry = (
                read_u32_at(data, off)?,
                read_u32_at(data, off + 4)?,
                read_u64_at(data, off + 8)?,
            );
            let target = (target_label, lower_key.unwrap_or(0), 0);
            if entry < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_weight_label_lower_bound(
        &self,
        data: &[u8],
        count: usize,
        target_label: u32,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off)? < target_label {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_weight_label_upper_bound(
        &self,
        data: &[u8],
        count: usize,
        target_label: u32,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off)? <= target_label {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_weight_value_lower_bound_in_span(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        target_value: u32,
    ) -> Result<usize, EngineError> {
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off + 4)? < target_value {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_weight_value_upper_bound_in_span(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        target_value: u32,
    ) -> Result<usize, EngineError> {
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off + 4)? <= target_value {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_weight_range_count_in_label_span(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        bounds: RangeBoundFlags<u32>,
    ) -> Result<usize, EngineError> {
        if start >= end {
            return Ok(0);
        }
        let range_start = match bounds.lower {
            Some(lower) if bounds.lower_inclusive => {
                self.edge_weight_value_lower_bound_in_span(data, start, end, lower)?
            }
            Some(lower) => self.edge_weight_value_upper_bound_in_span(data, start, end, lower)?,
            None => start,
        };
        let range_end = match bounds.upper {
            Some(upper) if bounds.upper_inclusive => {
                self.edge_weight_value_upper_bound_in_span(data, start, end, upper)?
            }
            Some(upper) => self.edge_weight_value_lower_bound_in_span(data, start, end, upper)?,
            None => end,
        };
        Ok(range_end.saturating_sub(range_start))
    }

    fn edge_weight_range_count_inner(
        &self,
        data: &[u8],
        count: usize,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
    ) -> Result<usize, EngineError> {
        let lower_key = match bounds.lower {
            Some(lower) => match encode_edge_weight_key(lower) {
                Some(key) => Some(key),
                None => return Ok(0),
            },
            None => None,
        };
        let upper_key = match bounds.upper {
            Some(upper) => match encode_edge_weight_key(upper) {
                Some(key) => Some(key),
                None => return Ok(0),
            },
            None => None,
        };
        let key_bounds = RangeBoundFlags {
            lower: lower_key,
            lower_inclusive: bounds.lower_inclusive,
            upper: upper_key,
            upper_inclusive: bounds.upper_inclusive,
        };
        if count == 0 {
            return Ok(0);
        }
        if let Some(target) = label_id {
            let start = self.edge_weight_label_lower_bound(data, count, target)?;
            if start == count {
                return Ok(0);
            }
            let off = 8 + start * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off)? != target {
                return Ok(0);
            }
            let end = self.edge_weight_label_upper_bound(data, count, target)?;
            return self.edge_weight_range_count_in_label_span(data, start, end, key_bounds);
        }
        if lower_key.is_none() && upper_key.is_none() {
            return Ok(count);
        }
        let mut matched = 0usize;
        let mut pos = 0usize;
        while pos < count {
            let off = 8 + pos * EDGE_WEIGHT_INDEX_ENTRY_SIZE;
            let entry_label_id = read_u32_at(data, off)?;
            let end = self.edge_weight_label_upper_bound(data, count, entry_label_id)?;
            matched = matched.saturating_add(
                self.edge_weight_range_count_in_label_span(data, pos, end, key_bounds)?,
            );
            pos = end;
        }
        Ok(matched)
    }

    #[cfg(test)]
    fn edge_ids_by_i64_metadata_range(
        &self,
        mmap: &MappedData,
        logical_name: &str,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Option<Vec<u64>> {
        let data = &mmap[..];
        if !self.edge_i64_metadata_index_available(logical_name) {
            return None;
        }
        let count = self.edge_i64_metadata_index_count(logical_name)?;
        match self.edge_ids_by_i64_metadata_range_inner(data, count, label_id, bounds, limit) {
            Ok(ids) => Some(ids),
            Err(error) => {
                if let Some(kind) = edge_i64_metadata_component_kind(logical_name) {
                    self.mark_edge_metadata_component_corrupt(kind, &error);
                }
                None
            }
        }
    }

    fn for_each_edge_id_by_i64_metadata_range<F>(
        &self,
        mmap: &MappedData,
        logical_name: &str,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        callback: &mut F,
    ) -> Result<Option<ControlFlow<()>>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        let data = &mmap[..];
        if !self.edge_i64_metadata_index_available(logical_name) {
            return Ok(None);
        }
        let count = self
            .edge_i64_metadata_index_count(logical_name)
            .expect("availability checked");
        match self
            .for_each_edge_id_by_i64_metadata_range_inner(data, count, label_id, bounds, callback)
        {
            Ok(flow) => Ok(Some(flow)),
            Err(error) => {
                if let Some(kind) = edge_i64_metadata_component_kind(logical_name) {
                    self.mark_edge_metadata_component_corrupt(kind, &error);
                }
                Err(error)
            }
        }
    }

    fn edge_i64_metadata_range_count(
        &self,
        mmap: &MappedData,
        logical_name: &str,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Option<usize> {
        let data = &mmap[..];
        if !self.edge_i64_metadata_index_available(logical_name) {
            return None;
        }
        let count = self.edge_i64_metadata_index_count(logical_name)?;
        match self.edge_i64_metadata_range_count_inner(data, count, label_id, bounds) {
            Ok(count) => Some(count),
            Err(error) => {
                if let Some(kind) = edge_i64_metadata_component_kind(logical_name) {
                    self.mark_edge_metadata_component_corrupt(kind, &error);
                }
                None
            }
        }
    }

    #[cfg(test)]
    fn edge_ids_by_i64_metadata_range_inner(
        &self,
        data: &[u8],
        count: usize,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let mut ids = Vec::new();
        let start = match label_id {
            Some(target) => {
                self.edge_i64_metadata_label_value_lower_bound(data, count, target, bounds.lower)?
            }
            None => 0,
        };
        let mut pos = start;
        while pos < count {
            let off = 8 + pos * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            let entry_label_id = read_u32_at(data, off)?;
            if label_id.is_some_and(|target| entry_label_id != target) {
                break;
            }
            let value = read_i64_at(data, off + 4)?;
            if label_id.is_some()
                && bounds.upper.is_some_and(|upper| {
                    if bounds.upper_inclusive {
                        value > upper
                    } else {
                        value >= upper
                    }
                })
            {
                break;
            }
            if crate::edge_metadata::i64_matches_bounds(value, bounds) {
                let edge_id = read_u64_at(data, off + 12)?;
                if !self.deleted_edges.contains_key(&edge_id) {
                    ids.push(edge_id);
                    if ids.len() > limit {
                        break;
                    }
                }
            }
            pos += 1;
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn for_each_edge_id_by_i64_metadata_range_inner<F>(
        &self,
        data: &[u8],
        count: usize,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        let start = match label_id {
            Some(target) => {
                self.edge_i64_metadata_label_value_lower_bound(data, count, target, bounds.lower)?
            }
            None => 0,
        };
        let mut pos = start;
        while pos < count {
            let off = 8 + pos * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            let entry_label_id = read_u32_at(data, off)?;
            if label_id.is_some_and(|target| entry_label_id != target) {
                break;
            }
            let value = read_i64_at(data, off + 4)?;
            if label_id.is_some()
                && bounds.upper.is_some_and(|upper| {
                    if bounds.upper_inclusive {
                        value > upper
                    } else {
                        value >= upper
                    }
                })
            {
                break;
            }
            if crate::edge_metadata::i64_matches_bounds(value, bounds) {
                let edge_id = read_u64_at(data, off + 12)?;
                if !self.deleted_edges.contains_key(&edge_id) && callback(edge_id).is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            pos += 1;
        }
        Ok(ControlFlow::Continue(()))
    }

    fn edge_i64_metadata_label_value_lower_bound(
        &self,
        data: &[u8],
        count: usize,
        target_label: u32,
        lower_value: Option<i64>,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            let entry = (
                read_u32_at(data, off)?,
                read_i64_at(data, off + 4)?,
                read_u64_at(data, off + 12)?,
            );
            let target = (target_label, lower_value.unwrap_or(i64::MIN), 0);
            if entry < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_i64_metadata_label_lower_bound(
        &self,
        data: &[u8],
        count: usize,
        target_label: u32,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off)? < target_label {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_i64_metadata_label_upper_bound(
        &self,
        data: &[u8],
        count: usize,
        target_label: u32,
    ) -> Result<usize, EngineError> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off)? <= target_label {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_i64_metadata_value_lower_bound_in_span(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        target_value: i64,
    ) -> Result<usize, EngineError> {
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            if read_i64_at(data, off + 4)? < target_value {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_i64_metadata_value_upper_bound_in_span(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        target_value: i64,
    ) -> Result<usize, EngineError> {
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = 8 + mid * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            if read_i64_at(data, off + 4)? <= target_value {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn edge_i64_metadata_range_count_in_label_span(
        &self,
        data: &[u8],
        start: usize,
        end: usize,
        bounds: RangeBoundFlags<i64>,
    ) -> Result<usize, EngineError> {
        if start >= end {
            return Ok(0);
        }
        let range_start = match bounds.lower {
            Some(lower) if bounds.lower_inclusive => {
                self.edge_i64_metadata_value_lower_bound_in_span(data, start, end, lower)?
            }
            Some(lower) => {
                self.edge_i64_metadata_value_upper_bound_in_span(data, start, end, lower)?
            }
            None => start,
        };
        let range_end = match bounds.upper {
            Some(upper) if bounds.upper_inclusive => {
                self.edge_i64_metadata_value_upper_bound_in_span(data, start, end, upper)?
            }
            Some(upper) => {
                self.edge_i64_metadata_value_lower_bound_in_span(data, start, end, upper)?
            }
            None => end,
        };
        Ok(range_end.saturating_sub(range_start))
    }

    fn edge_i64_metadata_range_count_inner(
        &self,
        data: &[u8],
        count: usize,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Result<usize, EngineError> {
        if count == 0 {
            return Ok(0);
        }
        if let Some(target) = label_id {
            let start = self.edge_i64_metadata_label_lower_bound(data, count, target)?;
            if start == count {
                return Ok(0);
            }
            let off = 8 + start * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            if read_u32_at(data, off)? != target {
                return Ok(0);
            }
            let end = self.edge_i64_metadata_label_upper_bound(data, count, target)?;
            return self.edge_i64_metadata_range_count_in_label_span(data, start, end, bounds);
        }
        if bounds.lower.is_none() && bounds.upper.is_none() {
            return Ok(count);
        }
        let mut matched = 0usize;
        let mut pos = 0usize;
        while pos < count {
            let off = 8 + pos * EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
            let entry_label_id = read_u32_at(data, off)?;
            let end = self.edge_i64_metadata_label_upper_bound(data, count, entry_label_id)?;
            matched = matched.saturating_add(
                self.edge_i64_metadata_range_count_in_label_span(data, pos, end, bounds)?,
            );
            pos = end;
        }
        Ok(matched)
    }

    #[cfg(test)]
    pub(crate) fn edge_metadata_scan_ids<F>(
        &self,
        mut predicate: F,
    ) -> Result<Vec<u64>, EngineError>
    where
        F: FnMut(EdgeMetadataCandidate) -> bool,
    {
        let mut ids = Vec::new();
        for index in 0..self.edge_meta_count() as usize {
            let (
                edge_id,
                _data_offset,
                _data_len,
                from,
                to,
                label_id,
                updated_at,
                weight,
                valid_from,
                valid_to,
                _last_write_seq,
            ) = self.edge_meta_at(index)?;
            if self.deleted_edges.contains_key(&edge_id) {
                continue;
            }
            let meta = EdgeMetadataCandidate {
                edge_id,
                from,
                to,
                label_id,
                updated_at,
                weight,
                valid_from,
                valid_to,
            };
            if predicate(meta) {
                ids.push(edge_id);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    pub(crate) fn for_each_edge_metadata<F>(
        &self,
        mut callback: F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(EdgeMetadataCandidate) -> ControlFlow<()>,
    {
        for index in 0..self.edge_meta_count() as usize {
            let meta = self.edge_metadata_at_index(index)?;
            if self.deleted_edges.contains_key(&meta.edge_id) {
                continue;
            }
            if callback(meta).is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn secondary_component_unavailable_state(
        &self,
        kind: SegmentComponentKind,
    ) -> DeclaredIndexRuntimeCoverageState {
        match self.component_registry.availability(&kind) {
            ComponentAvailability::Missing => DeclaredIndexRuntimeCoverageState::Missing,
            ComponentAvailability::Available => DeclaredIndexRuntimeCoverageState::Available,
            ComponentAvailability::Incompatible { .. }
            | ComponentAvailability::CorruptIdentity { .. }
            | ComponentAvailability::Unsupported { .. } => {
                DeclaredIndexRuntimeCoverageState::Corrupt
            }
        }
    }

    fn open_secondary_eq_sidecar_payload(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) -> Result<Option<MappedData>, EngineError> {
        try_open_optional_manifest_payload(
            &self.component_registry,
            None,
            &self.seg_dir,
            secondary_eq_component_kind(index_id, target),
        )
    }

    fn open_secondary_range_sidecar_payload(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) -> Result<Option<MappedData>, EngineError> {
        try_open_optional_manifest_payload(
            &self.component_registry,
            None,
            &self.seg_dir,
            secondary_range_component_kind(index_id, target),
        )
    }

    fn open_compound_sidecar_payload(
        &self,
        kind: SegmentComponentKind,
    ) -> Result<Option<MappedData>, EngineError> {
        try_open_optional_manifest_payload(&self.component_registry, None, &self.seg_dir, kind)
    }

    fn set_declared_index_runtime_coverage_state(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        kind: PlannerStatsDeclaredIndexKind,
        state: DeclaredIndexRuntimeCoverageState,
    ) {
        self.declared_index_runtime_coverage
            .lock()
            .unwrap()
            .insert((index_id, target, kind), state);
    }

    #[cfg(test)]
    pub(crate) fn declared_index_runtime_coverage_state(
        &self,
        index_id: u64,
        kind: PlannerStatsDeclaredIndexKind,
    ) -> DeclaredIndexRuntimeCoverageState {
        self.declared_index_runtime_coverage_state_for_target(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            kind,
        )
    }

    pub(crate) fn declared_index_runtime_coverage_state_for_target(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        kind: PlannerStatsDeclaredIndexKind,
    ) -> DeclaredIndexRuntimeCoverageState {
        self.declared_index_runtime_coverage
            .lock()
            .unwrap()
            .get(&(index_id, target, kind))
            .copied()
            .unwrap_or(DeclaredIndexRuntimeCoverageState::Unknown)
    }

    pub(crate) fn warm_declared_index_runtime_coverage(&self, entry: &SecondaryIndexManifestEntry) {
        if entry.state != SecondaryIndexState::Ready {
            return;
        }
        let target = planner_stats_declared_index_target(entry);
        if matches!(
            entry.target,
            SecondaryIndexTarget::NodeFieldIndex { .. }
                | SecondaryIndexTarget::EdgeFieldIndex { .. }
        ) {
            self.warm_compound_runtime_coverage(entry, target);
            return;
        }
        match entry.kind {
            SecondaryIndexKind::Equality => {
                self.warm_secondary_eq_runtime_coverage(entry.index_id, target)
            }
            SecondaryIndexKind::Range => {
                self.warm_secondary_range_runtime_coverage(entry.index_id, target)
            }
        }
    }

    fn warm_compound_runtime_coverage(
        &self,
        entry: &SecondaryIndexManifestEntry,
        target: PlannerStatsDeclaredIndexTarget,
    ) {
        let Some(component_kind) = compound_component_kind_for_entry(entry) else {
            return;
        };
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
            SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
        };
        let state = match self.open_compound_sidecar_payload(component_kind.clone()) {
            Ok(Some(data)) => {
                let declaration = CompoundSidecarDeclaration::from_manifest_entry(
                    entry,
                    secondary_index_declaration_fingerprint_for_entry(entry),
                );
                match declaration.and_then(|declaration| {
                    validate_compound_sidecar_header_only(&data, &declaration)
                }) {
                    Ok(()) => DeclaredIndexRuntimeCoverageState::Available,
                    Err(_) => DeclaredIndexRuntimeCoverageState::Corrupt,
                }
            }
            Ok(None) => self.secondary_component_unavailable_state(component_kind),
            Err(_) => DeclaredIndexRuntimeCoverageState::Corrupt,
        };
        self.set_declared_index_runtime_coverage_state(entry.index_id, target, kind, state);
    }

    fn warm_secondary_eq_runtime_coverage(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) {
        let component_kind = secondary_eq_component_kind(index_id, target);
        let state =
            match self.with_secondary_eq_sidecar_index_validated(index_id, target, |_| Ok(())) {
                Ok(Some(())) => DeclaredIndexRuntimeCoverageState::Available,
                Ok(None) => self.secondary_component_unavailable_state(component_kind),
                Err(_) => DeclaredIndexRuntimeCoverageState::Corrupt,
            };
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Equality,
            state,
        );
    }

    fn warm_secondary_range_runtime_coverage(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) {
        let component_kind = secondary_range_component_kind(index_id, target);
        let state = match self.with_secondary_range_sidecar_header_validated(
            index_id,
            target,
            |_| Ok(()),
        ) {
            Ok(Some(())) => DeclaredIndexRuntimeCoverageState::Available,
            Ok(None) => self.secondary_component_unavailable_state(component_kind),
            Err(_) => DeclaredIndexRuntimeCoverageState::Corrupt,
        };
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Range,
            state,
        );
    }

    fn with_secondary_eq_sidecar<T>(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        callback: impl FnOnce(&[u8]) -> Result<T, EngineError>,
    ) -> Result<Option<T>, EngineError> {
        let mut cache = self.secondary_eq_sidecars.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry((index_id, target)) {
            let component_kind = secondary_eq_component_kind(index_id, target);
            let Some(data) = self.open_secondary_eq_sidecar_payload(index_id, target)? else {
                self.set_declared_index_runtime_coverage_state(
                    index_id,
                    target,
                    PlannerStatsDeclaredIndexKind::Equality,
                    self.secondary_component_unavailable_state(component_kind),
                );
                return Ok(None);
            };
            entry.insert(SecondaryEqSidecarCacheEntry {
                data,
                validated: false,
                index_validated: false,
            });
        }

        let validation_error = {
            let entry = cache
                .get_mut(&(index_id, target))
                .expect("secondary equality sidecar cache entry must exist");
            if entry.validated {
                None
            } else {
                match validate_secondary_eq_sidecar_data(&entry.data) {
                    Ok(()) => {
                        entry.validated = true;
                        entry.index_validated = true;
                        None
                    }
                    Err(error) => Some(error),
                }
            }
        };
        if let Some(error) = validation_error {
            cache.remove(&(index_id, target));
            drop(cache);
            self.mark_secondary_eq_sidecar_corrupt(index_id, target, &error);
            return Err(error);
        }
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Equality,
            DeclaredIndexRuntimeCoverageState::Available,
        );

        let data = &cache
            .get(&(index_id, target))
            .expect("secondary equality sidecar cache entry must exist")
            .data[..];
        match callback(data) {
            Ok(value) => Ok(Some(value)),
            Err(error) => {
                cache.remove(&(index_id, target));
                drop(cache);
                self.mark_secondary_eq_sidecar_corrupt(index_id, target, &error);
                Err(error)
            }
        }
    }

    fn with_secondary_eq_sidecar_index_validated<T>(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        callback: impl FnOnce(&[u8]) -> Result<T, EngineError>,
    ) -> Result<Option<T>, EngineError> {
        let mut cache = self.secondary_eq_sidecars.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry((index_id, target)) {
            let component_kind = secondary_eq_component_kind(index_id, target);
            let Some(data) = self.open_secondary_eq_sidecar_payload(index_id, target)? else {
                self.set_declared_index_runtime_coverage_state(
                    index_id,
                    target,
                    PlannerStatsDeclaredIndexKind::Equality,
                    self.secondary_component_unavailable_state(component_kind),
                );
                return Ok(None);
            };
            entry.insert(SecondaryEqSidecarCacheEntry {
                data,
                validated: false,
                index_validated: false,
            });
        }

        let validation_error = {
            let entry = cache
                .get_mut(&(index_id, target))
                .expect("secondary equality sidecar cache entry must exist");
            if entry.validated || entry.index_validated {
                None
            } else {
                match validate_secondary_eq_sidecar_index_header(&entry.data) {
                    Ok(()) => {
                        entry.index_validated = true;
                        None
                    }
                    Err(error) => Some(error),
                }
            }
        };
        if let Some(error) = validation_error {
            cache.remove(&(index_id, target));
            drop(cache);
            self.mark_secondary_eq_sidecar_corrupt(index_id, target, &error);
            return Err(error);
        }
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Equality,
            DeclaredIndexRuntimeCoverageState::Available,
        );

        let data = &cache
            .get(&(index_id, target))
            .expect("secondary equality sidecar cache entry must exist")
            .data[..];
        match callback(data) {
            Ok(value) => Ok(Some(value)),
            Err(error) => {
                cache.remove(&(index_id, target));
                drop(cache);
                self.mark_secondary_eq_sidecar_corrupt(index_id, target, &error);
                Err(error)
            }
        }
    }

    fn with_secondary_range_sidecar<T>(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        callback: impl FnOnce(&[u8]) -> Result<T, EngineError>,
    ) -> Result<Option<T>, EngineError> {
        let mut cache = self.secondary_range_sidecars.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry((index_id, target)) {
            let component_kind = secondary_range_component_kind(index_id, target);
            let Some(data) = self.open_secondary_range_sidecar_payload(index_id, target)? else {
                self.set_declared_index_runtime_coverage_state(
                    index_id,
                    target,
                    PlannerStatsDeclaredIndexKind::Range,
                    self.secondary_component_unavailable_state(component_kind),
                );
                return Ok(None);
            };
            entry.insert(SecondaryRangeSidecarCacheEntry {
                data,
                validated: false,
                header_validated: false,
            });
        }

        let validation_error = {
            let entry = cache
                .get_mut(&(index_id, target))
                .expect("secondary range sidecar cache entry must exist");
            if entry.validated {
                None
            } else {
                match validate_secondary_range_sidecar_data(&entry.data) {
                    Ok(()) => {
                        entry.validated = true;
                        entry.header_validated = true;
                        None
                    }
                    Err(error) => Some(error),
                }
            }
        };
        if let Some(error) = validation_error {
            cache.remove(&(index_id, target));
            drop(cache);
            self.mark_secondary_range_sidecar_corrupt(index_id, target, &error);
            return Err(error);
        }
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Range,
            DeclaredIndexRuntimeCoverageState::Available,
        );

        let data = &cache
            .get(&(index_id, target))
            .expect("secondary range sidecar cache entry must exist")
            .data[..];
        match callback(data) {
            Ok(value) => Ok(Some(value)),
            Err(error) => {
                cache.remove(&(index_id, target));
                drop(cache);
                self.mark_secondary_range_sidecar_corrupt(index_id, target, &error);
                Err(error)
            }
        }
    }

    fn with_secondary_range_sidecar_header_validated<T>(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        callback: impl FnOnce(&[u8]) -> Result<T, EngineError>,
    ) -> Result<Option<T>, EngineError> {
        // Query-time range lookup trusts current component identity and does
        // only the fixed-width shape check needed for binary-search access.
        // Full ordering validation is reserved for scrub, repair, and
        // compaction source-sidecar reuse.
        let mut cache = self.secondary_range_sidecars.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry((index_id, target)) {
            let component_kind = secondary_range_component_kind(index_id, target);
            let Some(data) = self.open_secondary_range_sidecar_payload(index_id, target)? else {
                self.set_declared_index_runtime_coverage_state(
                    index_id,
                    target,
                    PlannerStatsDeclaredIndexKind::Range,
                    self.secondary_component_unavailable_state(component_kind),
                );
                return Ok(None);
            };
            entry.insert(SecondaryRangeSidecarCacheEntry {
                data,
                validated: false,
                header_validated: false,
            });
        }

        let validation_error = {
            let entry = cache
                .get_mut(&(index_id, target))
                .expect("secondary range sidecar cache entry must exist");
            if entry.validated || entry.header_validated {
                None
            } else {
                match validate_secondary_range_sidecar_header(&entry.data) {
                    Ok(()) => {
                        entry.header_validated = true;
                        None
                    }
                    Err(error) => Some(error),
                }
            }
        };
        if let Some(error) = validation_error {
            cache.remove(&(index_id, target));
            drop(cache);
            self.mark_secondary_range_sidecar_corrupt(index_id, target, &error);
            return Err(error);
        }
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Range,
            DeclaredIndexRuntimeCoverageState::Available,
        );

        let data = &cache
            .get(&(index_id, target))
            .expect("secondary range sidecar cache entry must exist")
            .data[..];
        match callback(data) {
            Ok(value) => Ok(Some(value)),
            Err(error) => {
                cache.remove(&(index_id, target));
                drop(cache);
                self.mark_secondary_range_sidecar_corrupt(index_id, target, &error);
                Err(error)
            }
        }
    }

    fn mark_secondary_eq_sidecar_corrupt(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        error: &EngineError,
    ) {
        let component_kind = secondary_eq_component_kind(index_id, target);
        mark_optional_component_corrupt(
            &self.component_registry,
            component_kind,
            error.to_string(),
        );
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Equality,
            DeclaredIndexRuntimeCoverageState::Corrupt,
        );
    }

    fn mark_secondary_range_sidecar_corrupt(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        error: &EngineError,
    ) {
        let component_kind = secondary_range_component_kind(index_id, target);
        mark_optional_component_corrupt(
            &self.component_registry,
            component_kind,
            error.to_string(),
        );
        self.set_declared_index_runtime_coverage_state(
            index_id,
            target,
            PlannerStatsDeclaredIndexKind::Range,
            DeclaredIndexRuntimeCoverageState::Corrupt,
        );
    }

    #[cfg(test)]
    pub(crate) fn validate_secondary_eq_sidecar(&self, index_id: u64) -> Result<bool, EngineError> {
        self.validate_secondary_eq_sidecar_for_target(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
        )
    }

    pub(crate) fn validate_secondary_eq_sidecar_for_target(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) -> Result<bool, EngineError> {
        match self.with_secondary_eq_sidecar(
            index_id,
            target,
            validate_secondary_eq_sidecar_data,
        )? {
            Some(()) => Ok(true),
            None => Ok(false),
        }
    }

    pub(crate) fn secondary_eq_sidecar_lightweight_available_for_target(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) -> Result<bool, EngineError> {
        self.secondary_eq_sidecars
            .lock()
            .unwrap()
            .remove(&(index_id, target));
        match self.with_secondary_eq_sidecar_index_validated(index_id, target, |_| Ok(())) {
            Ok(Some(())) => Ok(true),
            Ok(None) | Err(_) => Ok(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn find_nodes_by_secondary_eq_index(
        &self,
        index_id: u64,
        value_hash: u64,
    ) -> Result<Vec<u64>, EngineError> {
        match self.find_nodes_by_secondary_eq_index_if_present(index_id, value_hash)? {
            Some(result) => Ok(result),
            None => Ok(Vec::new()),
        }
    }

    pub(crate) fn find_nodes_by_secondary_eq_index_if_present(
        &self,
        index_id: u64,
        value_hash: u64,
    ) -> Result<Option<Vec<u64>>, EngineError> {
        self.with_secondary_eq_sidecar_index_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            |data| find_nodes_in_secondary_eq_sidecar(data, &self.deleted_nodes, value_hash),
        )
    }

    pub(crate) fn secondary_eq_posting_chunk_if_present(
        &self,
        index_id: u64,
        value_hash: u64,
        start: usize,
        raw_limit: usize,
    ) -> Result<Option<SecondaryEqPostingChunk>, EngineError> {
        self.with_secondary_eq_sidecar_index_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            |data| {
                secondary_eq_posting_chunk(data, &self.deleted_nodes, value_hash, start, raw_limit)
            },
        )
    }

    pub(crate) fn edge_secondary_eq_posting_chunk_if_present(
        &self,
        index_id: u64,
        value_hash: u64,
        start: usize,
        raw_limit: usize,
    ) -> Result<Option<SecondaryEqPostingChunk>, EngineError> {
        self.with_secondary_eq_sidecar_index_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::EdgeProperty,
            |data| {
                secondary_eq_posting_chunk(data, &self.deleted_edges, value_hash, start, raw_limit)
            },
        )
    }

    pub(crate) fn secondary_eq_posting_count_if_present(
        &self,
        index_id: u64,
        value_hash: u64,
    ) -> Result<Option<usize>, EngineError> {
        self.with_secondary_eq_sidecar_index_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            |data| secondary_eq_posting_count(data, value_hash),
        )
    }

    pub(crate) fn edge_secondary_eq_posting_count_if_present(
        &self,
        index_id: u64,
        value_hash: u64,
    ) -> Result<Option<usize>, EngineError> {
        self.with_secondary_eq_sidecar_index_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::EdgeProperty,
            |data| secondary_eq_visible_posting_count(data, &self.deleted_edges, value_hash),
        )
    }

    pub(crate) fn for_each_secondary_eq_group<F>(
        &self,
        index_id: u64,
        callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(u64, &[u64]) -> Result<(), EngineError>,
    {
        self.for_each_secondary_eq_group_for_target(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            callback,
        )
    }

    pub(crate) fn for_each_declared_secondary_eq_group<F>(
        &self,
        entry: &SecondaryIndexManifestEntry,
        callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(u64, &[u64]) -> Result<(), EngineError>,
    {
        if entry.target.single_property_key().is_none() {
            return Ok(false);
        }
        self.for_each_secondary_eq_group_for_target(
            entry.index_id,
            planner_stats_declared_index_target(entry),
            callback,
        )
    }

    fn for_each_secondary_eq_group_for_target<F>(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        mut callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(u64, &[u64]) -> Result<(), EngineError>,
    {
        match self.with_secondary_eq_sidecar(index_id, target, |data| {
            let count = read_u64_at(data, 0)? as usize;
            let idx_start = 8;
            for index in 0..count {
                let entry_off = idx_start + index * SECONDARY_EQ_ENTRY_SIZE;
                let value_hash = read_u64_at(data, entry_off)?;
                let offset = read_u64_at(data, entry_off + 8)? as usize;
                let id_count = read_u32_at(data, entry_off + 16)? as usize;
                let mut ids = Vec::with_capacity(id_count);
                for id_index in 0..id_count {
                    ids.push(read_u64_at(data, offset + id_index * 8)?);
                }
                callback(value_hash, &ids)?;
            }
            Ok(())
        })? {
            Some(()) => Ok(true),
            None => Ok(false),
        }
    }

    pub(crate) fn validate_secondary_range_sidecar(
        &self,
        index_id: u64,
    ) -> Result<bool, EngineError> {
        self.validate_secondary_range_sidecar_for_target(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
        )
    }

    pub(crate) fn validate_secondary_range_sidecar_for_target(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) -> Result<bool, EngineError> {
        match self.with_secondary_range_sidecar(
            index_id,
            target,
            validate_secondary_range_sidecar_data,
        )? {
            Some(()) => Ok(true),
            None => Ok(false),
        }
    }

    pub(crate) fn secondary_range_sidecar_lightweight_available_for_target(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
    ) -> Result<bool, EngineError> {
        match self.with_secondary_range_sidecar_header_validated(index_id, target, |_| Ok(()))? {
            Some(()) => Ok(true),
            None => Ok(false),
        }
    }

    pub(crate) fn validate_secondary_range_sidecar_uncached(
        &self,
        index_id: u64,
    ) -> Result<bool, EngineError> {
        self.secondary_range_sidecars
            .lock()
            .unwrap()
            .remove(&(index_id, PlannerStatsDeclaredIndexTarget::NodeProperty));
        self.validate_secondary_range_sidecar(index_id)
    }

    pub(crate) fn validate_compound_sidecar_for_entry(
        &self,
        entry: &SecondaryIndexManifestEntry,
    ) -> Result<bool, EngineError> {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(false);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(false);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match validate_compound_sidecar_payload(&data, &declaration) {
            Ok(_) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(true)
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    pub(crate) fn compound_sidecar_lightweight_available_for_entry(
        &self,
        entry: &SecondaryIndexManifestEntry,
    ) -> Result<bool, EngineError> {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(false);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(false);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match validate_compound_sidecar_header_only(&data, &declaration) {
            Ok(()) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(true)
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    pub(crate) fn for_each_compound_sidecar_entry<F>(
        &self,
        entry: &SecondaryIndexManifestEntry,
        callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(&[u8], u64) -> Result<(), EngineError>,
    {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(false);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(false);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match for_each_compound_sidecar_payload_entry(&data, &declaration, callback) {
            Ok(()) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(true)
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn compound_prefix_candidates_if_present(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &CompoundPrefixBounds,
    ) -> Result<Option<Vec<u64>>, EngineError> {
        self.compound_prefix_candidates_if_present_limited(entry, bounds, usize::MAX)
    }

    pub(crate) fn compound_prefix_candidates_if_present_limited(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &CompoundPrefixBounds,
        limit: usize,
    ) -> Result<Option<Vec<u64>>, EngineError> {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(None);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(None);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match scan_compound_sidecar_prefix_limited(&data, &declaration, bounds, limit) {
            Ok(ids) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(Some(ids))
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    // Planner-stats gap probe: sums key-table postings counts over the bound
    // range without decoding postings. Returns Ok(None) when the sidecar is
    // absent; corrupt visited bytes mark the component corrupt and error,
    // exactly like the candidate scans.
    pub(crate) fn compound_prefix_posting_count_if_present(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &CompoundPrefixBounds,
        cap: u64,
    ) -> Result<Option<u64>, EngineError> {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(None);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(None);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match crate::secondary_index_key::count_compound_sidecar_prefix(
            &data,
            &declaration,
            bounds,
            cap,
        ) {
            Ok(count) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(Some(count))
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    pub(crate) fn compound_range_posting_count_if_present(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &CompoundRangeBounds,
        cap: u64,
    ) -> Result<Option<u64>, EngineError> {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(None);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(None);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match crate::secondary_index_key::count_compound_sidecar_range(
            &data,
            &declaration,
            bounds,
            cap,
        ) {
            Ok(count) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(Some(count))
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn compound_range_candidates_if_present(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &CompoundRangeBounds,
    ) -> Result<Option<Vec<u64>>, EngineError> {
        self.compound_range_candidates_if_present_limited(entry, bounds, usize::MAX)
    }

    pub(crate) fn compound_range_candidates_if_present_limited(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &CompoundRangeBounds,
        limit: usize,
    ) -> Result<Option<Vec<u64>>, EngineError> {
        let Some(kind) = compound_component_kind_for_entry(entry) else {
            return Ok(None);
        };
        let Some(data) = self.open_compound_sidecar_payload(kind.clone())? else {
            return Ok(None);
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        match scan_compound_sidecar_range_limited(&data, &declaration, bounds, limit) {
            Ok(ids) => {
                self.component_registry
                    .set_availability(kind, ComponentAvailability::Available);
                Ok(Some(ids))
            }
            Err(error) => {
                mark_optional_component_corrupt(&self.component_registry, kind, error.to_string());
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn find_nodes_by_secondary_range_index_if_present(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
    ) -> Result<Option<Vec<(NumericRangeSortKey, u64)>>, EngineError> {
        self.find_nodes_by_secondary_range_index_if_present_limited(
            index_id, lower, upper, after, None,
        )
    }

    pub(crate) fn find_nodes_by_secondary_range_index_if_present_limited(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        limit: Option<usize>,
    ) -> Result<Option<Vec<(NumericRangeSortKey, u64)>>, EngineError> {
        self.with_secondary_range_sidecar_header_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            |data| {
                find_nodes_in_secondary_range_sidecar(
                    data,
                    &self.deleted_nodes,
                    lower,
                    upper,
                    after,
                    limit,
                )
            },
        )
    }

    pub(crate) fn count_nodes_by_secondary_range_index_if_present(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
    ) -> Result<Option<usize>, EngineError> {
        self.with_secondary_range_sidecar_header_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            |data| count_nodes_in_secondary_range_sidecar(data, &self.deleted_nodes, lower, upper),
        )
    }

    pub(crate) fn find_edges_by_secondary_range_index_if_present_limited(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        limit: Option<usize>,
    ) -> Result<Option<Vec<(NumericRangeSortKey, u64)>>, EngineError> {
        self.with_secondary_range_sidecar_header_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::EdgeProperty,
            |data| {
                find_nodes_in_secondary_range_sidecar(
                    data,
                    &self.deleted_edges,
                    lower,
                    upper,
                    after,
                    limit,
                )
            },
        )
    }

    pub(crate) fn count_edges_by_secondary_range_index_if_present(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
    ) -> Result<Option<usize>, EngineError> {
        self.with_secondary_range_sidecar_header_validated(
            index_id,
            PlannerStatsDeclaredIndexTarget::EdgeProperty,
            |data| count_nodes_in_secondary_range_sidecar(data, &self.deleted_edges, lower, upper),
        )
    }

    pub(crate) fn for_each_secondary_range_entry<F>(
        &self,
        index_id: u64,
        callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(NumericRangeSortKey, u64) -> Result<(), EngineError>,
    {
        self.for_each_secondary_range_entry_for_target(
            index_id,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            callback,
        )
    }

    pub(crate) fn for_each_declared_secondary_range_entry<F>(
        &self,
        entry: &SecondaryIndexManifestEntry,
        callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(NumericRangeSortKey, u64) -> Result<(), EngineError>,
    {
        if entry.target.single_property_key().is_none() {
            return Ok(false);
        }
        self.for_each_secondary_range_entry_for_target(
            entry.index_id,
            planner_stats_declared_index_target(entry),
            callback,
        )
    }

    fn for_each_secondary_range_entry_for_target<F>(
        &self,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        mut callback: F,
    ) -> Result<bool, EngineError>
    where
        F: FnMut(NumericRangeSortKey, u64) -> Result<(), EngineError>,
    {
        match self.with_secondary_range_sidecar(index_id, target, |data| {
            let count = read_u64_at(data, 0)? as usize;
            for index in 0..count {
                let entry_off = 8 + index * SECONDARY_RANGE_ENTRY_SIZE;
                let encoded_value = read_numeric_range_sidecar_key_at(data, entry_off)?;
                let node_id = read_u64_at(data, entry_off + NUMERIC_RANGE_KEY_BYTES)?;
                callback(encoded_value, node_id)?;
            }
            Ok(())
        })? {
            Some(()) => Ok(true),
            None => Ok(false),
        }
    }

    pub(crate) fn node_property_value_at_offset(
        &self,
        node_id: u64,
        data_offset: u64,
        prop_key: &str,
    ) -> Result<Option<PropValue>, EngineError> {
        decode_node_property_at(&self.nodes_mmap, data_offset as usize, node_id, prop_key)
    }

    pub(crate) fn edge_property_value_at_offset(
        &self,
        edge_id: u64,
        data_offset: u64,
        prop_key: &str,
    ) -> Result<Option<PropValue>, EngineError> {
        decode_edge_property_at(&self.edges_mmap, data_offset as usize, edge_id, prop_key)
    }

    pub(crate) fn get_node_selected_fields_batch(
        &self,
        lookups: &[(usize, u64)],
        needs: &NodeSelectedFieldNeeds,
        results: &mut [Option<SelectedNodeFields>],
        #[cfg(test)] selected_field_read_counters: Option<&SelectedFieldReadCounters>,
    ) -> Result<(), EngineError> {
        if lookups.is_empty() {
            return Ok(());
        }
        let data = &self.node_meta_mmap[..];
        let Some(layout) = parse_node_meta_layout(data)? else {
            return Ok(());
        };
        let count = layout.node_count;
        if count == 0 {
            return Ok(());
        }

        let idx_start = layout.fixed_entries_offset;
        let min_key = lookups.first().map(|&(_, id)| id).unwrap_or(0);
        let max_key = lookups.last().map(|&(_, id)| id).unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &(_, id) in lookups {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            layout.fixed_entry_size,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_fields: Option<Option<SelectedNodeFields>> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                let fields = if prev_id == Some(target_id) {
                    prev_fields.clone().flatten()
                } else if let Some(index) = binary_search_node_meta_index(data, layout, target_id)?
                {
                    let meta = read_node_meta_entry_at(data, layout, index)?;
                    let found = Some(self.selected_node_fields_from_meta(
                        index,
                        &meta,
                        needs,
                        #[cfg(test)]
                        selected_field_read_counters,
                    )?);
                    prev_id = Some(target_id);
                    prev_fields = Some(found.clone());
                    found
                } else {
                    prev_id = Some(target_id);
                    prev_fields = Some(None);
                    None
                };
                results[orig_idx] = fields;
            }
        } else {
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * layout.fixed_entry_size;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let meta = read_node_meta_entry_at(data, layout, idx_pos)?;
                        results[orig_idx] = Some(self.selected_node_fields_from_meta(
                            idx_pos,
                            &meta,
                            needs,
                            #[cfg(test)]
                            selected_field_read_counters,
                        )?);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Decode selected node record fields directly at a known data offset.
    ///
    /// Used by compound index builds where the caller already holds the
    /// record's offset from a meta table, so no ID-based re-location is
    /// needed. Vector needs are not supported on this path.
    pub(crate) fn node_selected_fields_at_offset(
        &self,
        node_id: u64,
        data_offset: u64,
        needs: &NodeSelectedFieldNeeds,
    ) -> Result<NodeSelectedFieldsAtOffset, EngineError> {
        let offset = usize_from_u64(data_offset, "node selected-field record offset")?;
        decode_node_selected_fields_at(&self.nodes_mmap, offset, node_id, needs)
    }

    /// Decode selected edge record fields directly at a known data offset.
    pub(crate) fn edge_selected_fields_at_offset(
        &self,
        edge_id: u64,
        data_offset: u64,
        needs: &EdgeSelectedFieldNeeds,
    ) -> Result<(BTreeMap<String, PropValue>, Option<i64>), EngineError> {
        let offset = usize_from_u64(data_offset, "edge selected-field record offset")?;
        decode_edge_selected_fields_at(&self.edges_mmap, offset, edge_id, needs)
    }

    fn selected_node_fields_from_meta(
        &self,
        index: usize,
        meta: &SegmentNodeMeta,
        needs: &NodeSelectedFieldNeeds,
        #[cfg(test)] selected_field_read_counters: Option<&SelectedFieldReadCounters>,
    ) -> Result<SelectedNodeFields, EngineError> {
        let needs_node_record =
            needs.key || needs.created_at || !matches!(needs.props, PropertySelection::None);
        let (key, props, created_at) = if needs_node_record {
            let offset = usize_from_u64(meta.data_offset, "node selected-field record offset")?;
            decode_node_selected_fields_at(&self.nodes_mmap, offset, meta.node_id, needs)?
        } else {
            (None, BTreeMap::new(), None)
        };
        #[cfg(test)]
        let dense_vector = if needs.vectors.needs_dense() {
            let vector = self.read_node_dense_vector_at_index(index)?;
            if vector.is_some() {
                if let Some(counters) = selected_field_read_counters {
                    counters.note_node_dense_vector_projection_read();
                }
            }
            vector
        } else {
            None
        };
        #[cfg(not(test))]
        let dense_vector = if needs.vectors.needs_dense() {
            self.read_node_dense_vector_at_index(index)?
        } else {
            None
        };
        #[cfg(test)]
        let sparse_vector = if needs.vectors.needs_sparse() {
            let vector = self.read_node_sparse_vector_at_index(index)?;
            if vector.is_some() {
                if let Some(counters) = selected_field_read_counters {
                    counters.note_node_sparse_vector_projection_read();
                }
            }
            vector
        } else {
            None
        };
        #[cfg(not(test))]
        let sparse_vector = if needs.vectors.needs_sparse() {
            self.read_node_sparse_vector_at_index(index)?
        } else {
            None
        };
        Ok(SelectedNodeFields {
            meta: NodeMetadataForQuery {
                id: meta.node_id,
                label_ids: meta.label_ids,
                updated_at: meta.updated_at,
                weight: meta.weight,
            },
            key,
            props,
            created_at,
            dense_vector,
            sparse_vector,
        })
    }

    pub(crate) fn get_edge_selected_fields_batch(
        &self,
        lookups: &[(usize, u64)],
        needs: &EdgeSelectedFieldNeeds,
        results: &mut [Option<SelectedEdgeFields>],
    ) -> Result<(), EngineError> {
        if lookups.is_empty() {
            return Ok(());
        }
        let data = &self.edges_mmap[..];
        if data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(());
        }

        let idx_start = 8;
        let min_key = lookups.first().map(|&(_, id)| id).unwrap_or(0);
        let max_key = lookups.last().map(|&(_, id)| id).unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &(_, id) in lookups {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let strategy = choose_batch_read_strategy(
            data,
            idx_start,
            count,
            EDGE_INDEX_ENTRY_SIZE,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_fields: Option<Option<SelectedEdgeFields>> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains_key(&target_id) {
                    continue;
                }
                let fields = if prev_id == Some(target_id) {
                    prev_fields.clone().flatten()
                } else if let Some((index, offset)) = self.binary_search_edge_index(target_id)? {
                    let found = Some(self.selected_edge_fields_from_index(index, offset, needs)?);
                    prev_id = Some(target_id);
                    prev_fields = Some(found.clone());
                    found
                } else {
                    prev_id = Some(target_id);
                    prev_fields = Some(None);
                    None
                };
                results[orig_idx] = fields;
            }
        } else {
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains_key(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * EDGE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let offset = read_u64_at(data, entry_off + 8)? as usize;
                        results[orig_idx] =
                            Some(self.selected_edge_fields_from_index(idx_pos, offset, needs)?);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn selected_edge_fields_from_index(
        &self,
        index: usize,
        offset: usize,
        needs: &EdgeSelectedFieldNeeds,
    ) -> Result<SelectedEdgeFields, EngineError> {
        let meta = self.edge_metadata_at_index(index)?;
        let (props, created_at) =
            decode_edge_selected_fields_at(&self.edges_mmap, offset, meta.edge_id, needs)?;
        Ok(SelectedEdgeFields {
            meta: EdgeMetadataForQuery::from(meta),
            props,
            created_at,
        })
    }

    // --- Internal binary search methods ---

    /// Binary search the node index for a given node_id.
    /// Returns the node's index position and byte offset into the data section.
    fn binary_search_node_index(
        &self,
        target_id: u64,
    ) -> Result<Option<(usize, usize)>, EngineError> {
        let data = &self.nodes_mmap[..];
        if data.len() < 8 {
            return Ok(None);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(None);
        }

        let idx_start = 8;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * NODE_INDEX_ENTRY_SIZE;
            let id = read_u64_at(data, entry_off)?;
            if id < target_id {
                lo = mid + 1;
            } else if id > target_id {
                hi = mid;
            } else {
                let offset = read_u64_at(data, entry_off + 8)? as usize;
                return Ok(Some((mid, offset)));
            }
        }
        Ok(None)
    }

    /// Binary search the edge index for a given edge_id.
    fn binary_search_edge_index(
        &self,
        target_id: u64,
    ) -> Result<Option<(usize, usize)>, EngineError> {
        let data = &self.edges_mmap[..];
        if data.len() < 8 {
            return Ok(None);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(None);
        }

        let idx_start = 8;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * EDGE_INDEX_ENTRY_SIZE;
            let id = read_u64_at(data, entry_off)?;
            if id < target_id {
                lo = mid + 1;
            } else if id > target_id {
                hi = mid;
            } else {
                let offset = read_u64_at(data, entry_off + 8)? as usize;
                return Ok(Some((mid, offset)));
            }
        }
        Ok(None)
    }

    /// Binary search the key index for a (label_id, key) pair.
    /// Returns the node_id if found, or None.
    fn binary_search_key_index(
        &self,
        target_label: u32,
        target_key: &str,
    ) -> Result<Option<u64>, EngineError> {
        let data = &self.key_index_mmap[..];
        if data.len() < 8 {
            return Ok(None);
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(None);
        }

        // Offset table starts at byte 8, each entry is u64
        let offset_table_start = 8;

        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_offset = read_u64_at(data, offset_table_start + mid * 8)? as usize;

            // Read entry: label_id (4) + node_id (8) + key_len (2) + key
            let entry_label_id = read_u32_at(data, entry_offset)?;
            let key_len = read_u16_at(data, entry_offset + 12)? as usize;
            let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
            let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
                EngineError::CorruptRecord(format!(
                    "invalid UTF-8 in key index at offset {}",
                    entry_offset + 14
                ))
            })?;

            match entry_label_id.cmp(&target_label) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => match entry_key.cmp(target_key) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => {
                        let node_id = read_u64_at(data, entry_offset + 4)?;
                        return Ok(Some(node_id));
                    }
                },
            }
        }
        Ok(None)
    }

    /// Find the first adjacency index entry for a given node_id using binary search.
    /// Returns the index of the first entry, or None if the node has no adjacency.
    fn find_first_adj_entry(
        &self,
        idx_data: &[u8],
        target_node_id: u64,
    ) -> Result<Option<usize>, EngineError> {
        if idx_data.len() < 8 {
            return Ok(None);
        }
        let count = read_u64_at(idx_data, 0)? as usize;
        if count == 0 {
            return Ok(None);
        }

        let idx_start = 8;

        // Binary search for any entry with target_node_id
        let mut lo = 0usize;
        let mut hi = count;
        let mut found = None;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * ADJ_INDEX_ENTRY_SIZE;
            let node_id = read_u64_at(idx_data, entry_off)?;

            if node_id < target_node_id {
                lo = mid + 1;
            } else if node_id > target_node_id {
                hi = mid;
            } else {
                // Found a match. Keep searching left for the first one
                found = Some(mid);
                hi = mid;
            }
        }

        Ok(found)
    }

    /// Collect neighbor entries from an adjacency index + data file pair.
    /// Postings are delta-encoded with varints.
    #[allow(clippy::too_many_arguments)]
    fn collect_adj_neighbors(
        &self,
        idx_mmap: &MappedData,
        dat_mmap: &MappedData,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
        mut record_self_loop_edge_ids: Option<&mut NodeIdSet>,
        skip_self_loop_edge_ids: Option<&NodeIdSet>,
        mut raw_budget: Option<&mut usize>,
        results: &mut Vec<NeighborRecord>,
    ) -> Result<(), EngineError> {
        let idx_data = &idx_mmap[..];
        let dat_data = &dat_mmap[..];

        let first = match self.find_first_adj_entry(idx_data, node_id)? {
            Some(i) => i,
            None => return Ok(()),
        };

        let count = read_u64_at(idx_data, 0)? as usize;
        let idx_start = 8;

        // Scan forward from first entry while node_id matches
        for i in first..count {
            if let Some(remaining) = raw_budget.as_ref() {
                if **remaining == 0 {
                    break;
                }
            } else if limit > 0 && results.len() >= limit {
                break;
            }

            let entry_off = idx_start + i * ADJ_INDEX_ENTRY_SIZE;
            let entry_node = read_u64_at(idx_data, entry_off)?;
            if entry_node != node_id {
                break;
            }

            let entry_label_id = read_u32_at(idx_data, entry_off + 8)?;
            let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
            let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

            if let Some(label_ids) = label_filter_ids {
                if !label_ids.contains(&entry_label_id) {
                    continue;
                }
            }

            // Decode delta-encoded postings sequentially
            let mut cur_off = posting_offset;
            let mut prev_edge_id: u64 = 0;

            for _j in 0..posting_count {
                if let Some(remaining) = raw_budget.as_ref() {
                    if **remaining == 0 {
                        break;
                    }
                } else if limit > 0 && results.len() >= limit {
                    break;
                }

                let (delta, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let edge_id = checked_adj_edge_id_delta(prev_edge_id, delta)?;
                prev_edge_id = edge_id;

                let (neighbor_id, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;

                let weight = read_f32_at(dat_data, cur_off)?;
                cur_off += 4;

                let (vf_enc, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let valid_from = vf_enc as i64;

                let (vt_enc, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let valid_to = if vt_enc == 0 {
                    i64::MAX
                } else {
                    (vt_enc - 1) as i64
                };

                if self.deleted_edges.contains_key(&edge_id) {
                    continue;
                }
                if self.deleted_nodes.contains_key(&neighbor_id) {
                    continue;
                }
                if let Some(remaining) = raw_budget.as_deref_mut() {
                    *remaining = remaining.saturating_sub(1);
                }
                if neighbor_id == node_id {
                    if let Some(skip) = skip_self_loop_edge_ids {
                        if skip.contains(&edge_id) {
                            continue;
                        }
                    }
                    if let Some(record) = record_self_loop_edge_ids.as_deref_mut() {
                        record.insert(edge_id);
                    }
                }

                results.push(NeighborRecord {
                    node_id: neighbor_id,
                    edge_id,
                    edge_label_id: entry_label_id,
                    weight,
                    valid_from,
                    valid_to,
                });
            }
        }

        Ok(())
    }

    /// Iterate adjacency postings for a node, calling the callback for each valid
    /// (non-tombstoned, type-matching) posting. Used by degree/weight aggregation
    /// to avoid materializing `Vec<NeighborRecord>`.
    ///
    /// Callback receives `(edge_id, neighbor_id, weight, valid_from, valid_to)`.
    /// For `Direction::Both`, self-loops may invoke the callback twice. Caller
    /// handles dedup.
    pub fn for_each_adj_posting<F>(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        match direction {
            Direction::Outgoing => self.decode_adj_postings_cb(
                &self.adj_out_idx,
                &self.adj_out_dat,
                node_id,
                label_filter_ids,
                callback,
            ),
            Direction::Incoming => self.decode_adj_postings_cb(
                &self.adj_in_idx,
                &self.adj_in_dat,
                node_id,
                label_filter_ids,
                callback,
            ),
            Direction::Both => {
                if self
                    .decode_adj_postings_cb(
                        &self.adj_out_idx,
                        &self.adj_out_dat,
                        node_id,
                        label_filter_ids,
                        callback,
                    )?
                    .is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
                self.decode_adj_postings_cb(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_id,
                    label_filter_ids,
                    callback,
                )
            }
        }
    }

    /// Decode adjacency postings from one index+data file pair, invoking the
    /// callback for each non-tombstoned posting. Passes valid_from/valid_to
    /// through for caller-side temporal filtering.
    fn decode_adj_postings_cb<F>(
        &self,
        idx_mmap: &MappedData,
        dat_mmap: &MappedData,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        let idx_data = &idx_mmap[..];
        let dat_data = &dat_mmap[..];

        let first = match self.find_first_adj_entry(idx_data, node_id)? {
            Some(i) => i,
            None => return Ok(ControlFlow::Continue(())),
        };

        let count = read_u64_at(idx_data, 0)? as usize;
        let idx_start = 8;

        for i in first..count {
            let entry_off = idx_start + i * ADJ_INDEX_ENTRY_SIZE;
            let entry_node = read_u64_at(idx_data, entry_off)?;
            if entry_node != node_id {
                break;
            }

            let entry_label_id = read_u32_at(idx_data, entry_off + 8)?;
            let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
            let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

            if let Some(label_ids) = label_filter_ids {
                if !label_ids.contains(&entry_label_id) {
                    continue;
                }
            }

            let mut cur_off = posting_offset;
            let mut prev_edge_id: u64 = 0;

            for _ in 0..posting_count {
                let (delta, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let edge_id = checked_adj_edge_id_delta(prev_edge_id, delta)?;
                prev_edge_id = edge_id;

                let (neighbor_id, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;

                let weight = read_f32_at(dat_data, cur_off)?;
                cur_off += 4;

                let (valid_from_raw, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let (vt_enc, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let valid_to = if vt_enc == 0 {
                    i64::MAX
                } else {
                    (vt_enc - 1) as i64
                };

                if self.deleted_edges.contains_key(&edge_id) {
                    continue;
                }
                if self.deleted_nodes.contains_key(&neighbor_id) {
                    continue;
                }

                if callback(
                    edge_id,
                    neighbor_id,
                    weight,
                    valid_from_raw as i64,
                    valid_to,
                )
                .is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    }

    /// Batch neighbor query: collect neighbors for multiple node IDs in a single
    /// cursor walk through the adjacency index. Input `node_ids` must be sorted
    /// and deduplicated. O(N+M) per direction where N = index entry count,
    /// M = number of queried nodes, vs O(M log N) for M individual binary searches.
    pub(crate) fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
    ) -> Result<NodeIdMap<Vec<NeighborRecord>>, EngineError> {
        let mut results: NodeIdMap<Vec<NeighborRecord>> =
            NodeIdMap::with_capacity_and_hasher(node_ids.len(), Default::default());

        match direction {
            Direction::Outgoing => {
                self.collect_adj_neighbors_batch(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_ids,
                    label_filter_ids,
                    &mut results,
                )?;
            }
            Direction::Incoming => {
                self.collect_adj_neighbors_batch(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_ids,
                    label_filter_ids,
                    &mut results,
                )?;
            }
            Direction::Both => {
                self.collect_adj_neighbors_batch(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_ids,
                    label_filter_ids,
                    &mut results,
                )?;
                self.collect_adj_neighbors_batch(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_ids,
                    label_filter_ids,
                    &mut results,
                )?;
                // Deduplicate by edge_id per node (self-loops appear in both)
                for entries in results.values_mut() {
                    let mut seen = NodeIdSet::default();
                    entries.retain(|e| seen.insert(e.edge_id));
                }
            }
        }

        Ok(results)
    }

    /// Single-pass cursor walk through an adjacency index file, collecting
    /// neighbors for all requested node IDs. `node_ids` must be sorted.
    /// Appends results into the existing HashMap (for Direction::Both merging).
    fn collect_adj_neighbors_batch(
        &self,
        idx_mmap: &MappedData,
        dat_mmap: &MappedData,
        node_ids: &[u64],
        label_filter_ids: Option<&[u32]>,
        results: &mut NodeIdMap<Vec<NeighborRecord>>,
    ) -> Result<(), EngineError> {
        let idx_data = &idx_mmap[..];
        let dat_data = &dat_mmap[..];

        if idx_data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(idx_data, 0)? as usize;
        if count == 0 {
            return Ok(());
        }

        let idx_start = 8;
        let min_key = node_ids.first().copied().unwrap_or(0);
        let max_key = node_ids.last().copied().unwrap_or(0);
        let unique_keys = {
            let mut n = 0usize;
            let mut prev: Option<u64> = None;
            for &id in node_ids {
                if prev != Some(id) {
                    n += 1;
                    prev = Some(id);
                }
            }
            n
        };
        let use_seek = choose_batch_read_strategy(
            idx_data,
            idx_start,
            count,
            ADJ_INDEX_ENTRY_SIZE,
            0,
            unique_keys,
            min_key,
            max_key,
        )? == BatchReadStrategy::SeekPerKey;
        let mut idx_pos = 0usize; // cursor for merge-walk path

        for &target_id in node_ids {
            // Find starting position via the strategy selected by the shared
            // cost model: per-key seek or merge-walk cursor advance.
            if use_seek {
                idx_pos = match self.find_first_adj_entry(idx_data, target_id)? {
                    Some(pos) => pos,
                    None => continue,
                };
            } else {
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * ADJ_INDEX_ENTRY_SIZE;
                    let entry_node = read_u64_at(idx_data, entry_off)?;
                    if entry_node < target_id {
                        idx_pos += 1;
                    } else {
                        break;
                    }
                }
            }

            // Collect all entries with node_id == target_id
            while idx_pos < count {
                let entry_off = idx_start + idx_pos * ADJ_INDEX_ENTRY_SIZE;
                let entry_node = read_u64_at(idx_data, entry_off)?;
                if entry_node != target_id {
                    break;
                }

                let entry_label_id = read_u32_at(idx_data, entry_off + 8)?;
                let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
                let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

                idx_pos += 1;

                if let Some(label_ids) = label_filter_ids {
                    if !label_ids.contains(&entry_label_id) {
                        continue;
                    }
                }

                // Decode delta-encoded postings
                let entries = results.entry(target_id).or_default();
                let mut cur_off = posting_offset;
                let mut prev_edge_id: u64 = 0;

                for _ in 0..posting_count {
                    let (delta, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let edge_id = checked_adj_edge_id_delta(prev_edge_id, delta)?;
                    prev_edge_id = edge_id;

                    let (neighbor_id, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;

                    let weight = read_f32_at(dat_data, cur_off)?;
                    cur_off += 4;

                    let (vf_enc, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let valid_from = vf_enc as i64;

                    let (vt_enc, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let valid_to = if vt_enc == 0 {
                        i64::MAX
                    } else {
                        (vt_enc - 1) as i64
                    };

                    if self.deleted_edges.contains_key(&edge_id) {
                        continue;
                    }
                    if self.deleted_nodes.contains_key(&neighbor_id) {
                        continue;
                    }

                    entries.push(NeighborRecord {
                        node_id: neighbor_id,
                        edge_id,
                        edge_label_id: entry_label_id,
                        weight,
                        valid_from,
                        valid_to,
                    });
                }
            }
        }

        Ok(())
    }

    /// Batch callback-based adjacency posting iteration. Same adaptive cost model
    /// as `collect_adj_neighbors_batch` (SeekPerKey vs MergeWalk) but invokes a
    /// callback instead of building `Vec<NeighborRecord>`.
    ///
    /// `node_ids` must be sorted and deduplicated. For `Direction::Both`, self-loops
    /// may invoke the callback twice per edge. Caller handles dedup.
    ///
    /// Callback receives `(queried_node_id, edge_id, neighbor_id, weight, valid_from, valid_to)`.
    pub fn for_each_adj_posting_batch<F>(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        match direction {
            Direction::Outgoing => self.decode_adj_postings_batch_cb(
                &self.adj_out_idx,
                &self.adj_out_dat,
                node_ids,
                label_filter_ids,
                callback,
            ),
            Direction::Incoming => self.decode_adj_postings_batch_cb(
                &self.adj_in_idx,
                &self.adj_in_dat,
                node_ids,
                label_filter_ids,
                callback,
            ),
            Direction::Both => {
                if self
                    .decode_adj_postings_batch_cb(
                        &self.adj_out_idx,
                        &self.adj_out_dat,
                        node_ids,
                        label_filter_ids,
                        callback,
                    )?
                    .is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
                self.decode_adj_postings_batch_cb(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_ids,
                    label_filter_ids,
                    callback,
                )
            }
        }
    }

    pub(crate) fn endpoint_adj_posting_cursors(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
    ) -> Result<Vec<SegmentAdjPostingCursor>, EngineError> {
        let mut cursors = Vec::new();
        match direction {
            Direction::Outgoing => self.collect_adj_posting_cursors(
                &self.adj_out_idx,
                SegmentAdjacencyFile::Out,
                node_ids,
                label_filter_ids,
                &mut cursors,
            )?,
            Direction::Incoming => self.collect_adj_posting_cursors(
                &self.adj_in_idx,
                SegmentAdjacencyFile::In,
                node_ids,
                label_filter_ids,
                &mut cursors,
            )?,
            Direction::Both => {
                self.collect_adj_posting_cursors(
                    &self.adj_out_idx,
                    SegmentAdjacencyFile::Out,
                    node_ids,
                    label_filter_ids,
                    &mut cursors,
                )?;
                self.collect_adj_posting_cursors(
                    &self.adj_in_idx,
                    SegmentAdjacencyFile::In,
                    node_ids,
                    label_filter_ids,
                    &mut cursors,
                )?;
            }
        }
        Ok(cursors)
    }

    pub(crate) fn endpoint_adj_posting_count(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
    ) -> Result<usize, EngineError> {
        let cursors = self.endpoint_adj_posting_cursors(node_ids, direction, label_filter_ids)?;
        Ok(cursors.iter().fold(0usize, |total, cursor| {
            total.saturating_add(cursor.remaining)
        }))
    }

    fn collect_adj_posting_cursors(
        &self,
        idx_mmap: &MappedData,
        file: SegmentAdjacencyFile,
        node_ids: &[u64],
        label_filter_ids: Option<&[u32]>,
        cursors: &mut Vec<SegmentAdjPostingCursor>,
    ) -> Result<(), EngineError> {
        let idx_data = &idx_mmap[..];
        if idx_data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(idx_data, 0)? as usize;
        if count == 0 {
            return Ok(());
        }

        let idx_start = 8;
        let min_key = node_ids.first().copied().unwrap_or(0);
        let max_key = node_ids.last().copied().unwrap_or(0);
        let use_seek = choose_batch_read_strategy(
            idx_data,
            idx_start,
            count,
            ADJ_INDEX_ENTRY_SIZE,
            0,
            node_ids.len(),
            min_key,
            max_key,
        )? == BatchReadStrategy::SeekPerKey;
        let mut idx_pos = 0usize;

        for &target_id in node_ids {
            if use_seek {
                idx_pos = match self.find_first_adj_entry(idx_data, target_id)? {
                    Some(pos) => pos,
                    None => continue,
                };
            } else {
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * ADJ_INDEX_ENTRY_SIZE;
                    let entry_node = read_u64_at(idx_data, entry_off)?;
                    if entry_node < target_id {
                        idx_pos += 1;
                    } else {
                        break;
                    }
                }
            }

            while idx_pos < count {
                let entry_off = idx_start + idx_pos * ADJ_INDEX_ENTRY_SIZE;
                let entry_node = read_u64_at(idx_data, entry_off)?;
                if entry_node != target_id {
                    break;
                }

                let entry_label_id = read_u32_at(idx_data, entry_off + 8)?;
                let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
                let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;
                idx_pos += 1;

                if label_filter_ids.is_some_and(|label_ids| !label_ids.contains(&entry_label_id)) {
                    continue;
                }

                if posting_count > 0 {
                    cursors.push(SegmentAdjPostingCursor {
                        file,
                        cur_off: posting_offset,
                        remaining: posting_count,
                        prev_edge_id: 0,
                    });
                }
            }
        }

        Ok(())
    }

    pub(crate) fn next_adj_posting_edge_id(
        &self,
        cursor: &mut SegmentAdjPostingCursor,
    ) -> Result<Option<u64>, EngineError> {
        let dat_data = match cursor.file {
            SegmentAdjacencyFile::Out => &self.adj_out_dat[..],
            SegmentAdjacencyFile::In => &self.adj_in_dat[..],
        };

        while cursor.remaining > 0 {
            cursor.remaining -= 1;

            let (delta, n) = read_varint_at(dat_data, cursor.cur_off)?;
            cursor.cur_off += n;
            let edge_id = checked_adj_edge_id_delta(cursor.prev_edge_id, delta)?;
            cursor.prev_edge_id = edge_id;

            let (neighbor_id, n) = read_varint_at(dat_data, cursor.cur_off)?;
            cursor.cur_off += n;

            let _ = read_f32_at(dat_data, cursor.cur_off)?;
            cursor.cur_off += 4;

            let (_, n) = read_varint_at(dat_data, cursor.cur_off)?;
            cursor.cur_off += n;
            let (_, n) = read_varint_at(dat_data, cursor.cur_off)?;
            cursor.cur_off += n;

            if self.deleted_edges.contains_key(&edge_id) {
                continue;
            }
            if self.deleted_nodes.contains_key(&neighbor_id) {
                continue;
            }
            return Ok(Some(edge_id));
        }

        Ok(None)
    }

    /// Batch decode adjacency postings from one index+data file pair using the
    /// adaptive cost model. Invokes the callback for each non-tombstoned posting.
    fn decode_adj_postings_batch_cb<F>(
        &self,
        idx_mmap: &MappedData,
        dat_mmap: &MappedData,
        node_ids: &[u64],
        label_filter_ids: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        let idx_data = &idx_mmap[..];
        let dat_data = &dat_mmap[..];

        if idx_data.len() < 8 {
            return Ok(ControlFlow::Continue(()));
        }
        let count = read_u64_at(idx_data, 0)? as usize;
        if count == 0 {
            return Ok(ControlFlow::Continue(()));
        }

        let idx_start = 8;
        let min_key = node_ids.first().copied().unwrap_or(0);
        let max_key = node_ids.last().copied().unwrap_or(0);
        // node_ids is pre-sorted and deduped, so len() == unique count
        let use_seek = choose_batch_read_strategy(
            idx_data,
            idx_start,
            count,
            ADJ_INDEX_ENTRY_SIZE,
            0,
            node_ids.len(),
            min_key,
            max_key,
        )? == BatchReadStrategy::SeekPerKey;
        let mut idx_pos = 0usize;

        for &target_id in node_ids {
            if use_seek {
                idx_pos = match self.find_first_adj_entry(idx_data, target_id)? {
                    Some(pos) => pos,
                    None => continue,
                };
            } else {
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * ADJ_INDEX_ENTRY_SIZE;
                    let entry_node = read_u64_at(idx_data, entry_off)?;
                    if entry_node < target_id {
                        idx_pos += 1;
                    } else {
                        break;
                    }
                }
            }

            while idx_pos < count {
                let entry_off = idx_start + idx_pos * ADJ_INDEX_ENTRY_SIZE;
                let entry_node = read_u64_at(idx_data, entry_off)?;
                if entry_node != target_id {
                    break;
                }

                let entry_label_id = read_u32_at(idx_data, entry_off + 8)?;
                let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
                let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

                idx_pos += 1;

                if let Some(label_ids) = label_filter_ids {
                    if !label_ids.contains(&entry_label_id) {
                        continue;
                    }
                }

                let mut cur_off = posting_offset;
                let mut prev_edge_id: u64 = 0;

                for _ in 0..posting_count {
                    let (delta, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let edge_id = checked_adj_edge_id_delta(prev_edge_id, delta)?;
                    prev_edge_id = edge_id;

                    let (neighbor_id, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;

                    let weight = read_f32_at(dat_data, cur_off)?;
                    cur_off += 4;

                    let (valid_from_raw, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let (vt_enc, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let valid_to = if vt_enc == 0 {
                        i64::MAX
                    } else {
                        (vt_enc - 1) as i64
                    };

                    if self.deleted_edges.contains_key(&edge_id) {
                        continue;
                    }
                    if self.deleted_nodes.contains_key(&neighbor_id) {
                        continue;
                    }

                    if callback(
                        target_id,
                        edge_id,
                        neighbor_id,
                        weight,
                        valid_from_raw as i64,
                        valid_to,
                    )
                    .is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    }
}

// --- Helpers ---

#[cfg(test)]
fn load_component_manifest(
    seg_dir: &Path,
    segment_id: u64,
) -> Result<SegmentComponentManifestV1, EngineError> {
    let manifest = read_component_manifest(seg_dir)?;
    if manifest.segment_id != segment_id {
        return Err(EngineError::CorruptRecord(format!(
            "segment manifest id {} does not match directory segment {}",
            manifest.segment_id, segment_id
        )));
    }
    Ok(manifest)
}

pub(crate) fn validate_segment_manifest_identity(
    segment_info: &SegmentInfo,
    manifest: &SegmentComponentManifestV1,
) -> Result<SegmentComponentSourceGroups, EngineError> {
    if manifest.segment_id != segment_info.id {
        return Err(EngineError::CorruptRecord(format!(
            "segment manifest id {} does not match root segment {}",
            manifest.segment_id, segment_info.id
        )));
    }
    if manifest.node_count != segment_info.node_count {
        return Err(EngineError::CorruptRecord(format!(
            "segment manifest node_count {} does not match root node_count {} for segment {}",
            manifest.node_count, segment_info.node_count, segment_info.id
        )));
    }
    if manifest.edge_count != segment_info.edge_count {
        return Err(EngineError::CorruptRecord(format!(
            "segment manifest edge_count {} does not match root edge_count {} for segment {}",
            manifest.edge_count, segment_info.edge_count, segment_info.id
        )));
    }
    if manifest.segment_data_id != segment_info.segment_data_id {
        return Err(EngineError::CorruptRecord(format!(
            "segment manifest segment_data_id does not match root for segment {}",
            segment_info.id
        )));
    }
    validate_root_segment_info(segment_info, manifest.segment_format_version)?;
    validate_manifest_segment_data_id(manifest)
}

fn read_component_manifest(seg_dir: &Path) -> Result<SegmentComponentManifestV1, EngineError> {
    let path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
    if !path.exists() {
        return Err(EngineError::CorruptRecord(
            "unsupported segment format: missing segment_manifest.dat; rebuild the database".into(),
        ));
    }
    let data = std::fs::read(&path)?;
    let manifest = decode_manifest_envelope(&data)?;
    if manifest.segment_format_version != SEGMENT_FORMAT_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported segment manifest version {}; rebuild the database",
            manifest.segment_format_version
        )));
    }
    Ok(manifest)
}

fn validate_root_segment_info(
    segment_info: &SegmentInfo,
    format_version: u32,
) -> Result<(), EngineError> {
    if format_version != SEGMENT_FORMAT_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported segment manifest version {}; rebuild the database",
            format_version
        )));
    }
    if segment_info.segment_format_version != SEGMENT_FORMAT_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "root manifest segment {} has unsupported segment manifest version {}; rebuild the database",
            segment_info.id, segment_info.segment_format_version
        )));
    }
    if segment_info.segment_data_id == [0; 32] {
        return Err(EngineError::CorruptRecord(format!(
            "root manifest segment {} is missing segment_data_id; rebuild the database",
            segment_info.id
        )));
    }
    Ok(())
}

fn validate_manifest_segment_data_id(
    manifest: &SegmentComponentManifestV1,
) -> Result<SegmentComponentSourceGroups, EngineError> {
    let source_groups = segment_source_groups_from_records(
        manifest.segment_id,
        manifest.node_count,
        manifest.edge_count,
        &manifest.components,
    )?;
    if source_groups.segment_data_id != manifest.segment_data_id {
        return Err(EngineError::CorruptRecord(format!(
            "segment manifest segment_data_id does not match component source groups for segment {}",
            manifest.segment_id
        )));
    }
    Ok(source_groups)
}

fn validate_manifest_component_contracts(
    registry: &SegmentComponentRegistry,
    source_groups: &SegmentComponentSourceGroups,
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<(), EngineError> {
    for record in registry.records.values() {
        let result = validate_manifest_component_contract(
            record,
            registry,
            source_groups,
            dense_config,
            secondary_indexes,
        );
        if let Err(error) = result {
            match record.requirement {
                ComponentRequirement::Required => return Err(error),
                ComponentRequirement::Optional { .. } => registry.set_availability(
                    record.kind.clone(),
                    ComponentAvailability::Incompatible {
                        reason: error.to_string(),
                    },
                ),
            }
        }
    }
    Ok(())
}

fn warm_edge_property_sidecar_availability(
    registry: &SegmentComponentRegistry,
    context: &ComponentOpenContext,
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) {
    for entry in secondary_indexes {
        let kind = match (&entry.target, &entry.kind) {
            (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Equality) => {
                SegmentComponentKind::EdgePropertyEqualityIndex {
                    index_id: entry.index_id,
                }
            }
            (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Range) => {
                SegmentComponentKind::EdgePropertyRangeIndex {
                    index_id: entry.index_id,
                }
            }
            _ => continue,
        };
        let _ = try_open_optional_manifest_payload(registry, Some(context), seg_dir, kind);
    }
}

fn validate_manifest_component_contract(
    record: &SegmentComponentRecordV1,
    registry: &SegmentComponentRegistry,
    source_groups: &SegmentComponentSourceGroups,
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<(), EngineError> {
    let (expected_requirement, expected_trust_class) = expected_component_contract(&record.kind)
        .ok_or_else(|| {
            EngineError::CorruptRecord(format!("unsupported component kind {:?}", record.kind))
        })?;
    if record.requirement != expected_requirement {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} requirement does not match manifest contract",
            record.kind
        )));
    }
    if record.trust_class != expected_trust_class {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} trust class does not match manifest contract",
            record.kind
        )));
    }
    if record.logical_format_version != 1 {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} has unsupported logical format version {}",
            record.kind, record.logical_format_version
        )));
    }
    let dependencies = expected_component_dependencies(
        &record.kind,
        registry,
        source_groups,
        dense_config,
        secondary_indexes,
    )?;
    if record.dependency_digest != dependency_digest(&dependencies) {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} dependency digest does not match current source identity",
            record.kind
        )));
    }
    if !component_build_fingerprint_matches(
        &record.kind,
        record.build_fingerprint,
        secondary_indexes,
    ) {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} build fingerprint does not match current semantics",
            record.kind
        )));
    }
    let expected_component_id = component_id(
        registry.segment_id,
        &record.kind,
        record.logical_format_version,
        record.payload_len,
        record.payload_digest.as_ref(),
        &record.dependency_digest,
        record.build_fingerprint,
    );
    if record.component_id != expected_component_id {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} id does not match expected identity fields",
            record.kind
        )));
    }
    Ok(())
}

fn expected_component_contract(
    kind: &SegmentComponentKind,
) -> Option<(ComponentRequirement, ComponentTrustClass)> {
    use SegmentComponentKind::*;
    let contract = match kind {
        NodeRecords | EdgeRecords => (
            ComponentRequirement::Required,
            ComponentTrustClass::PrimaryData,
        ),
        NodeMetadata | EdgeMetadata | Tombstones => (
            ComponentRequirement::Required,
            ComponentTrustClass::PrimaryMetadata,
        ),
        KeyIndex | NodeLabelIndex | EdgeLabelIndex | EdgeTripleIndex | AdjOutIndex
        | AdjOutPostings | AdjInIndex | AdjInPostings | TimestampIndex => (
            ComponentRequirement::Required,
            ComponentTrustClass::CoreMaintainedIndex,
        ),
        NodeVectorMetadata | NodeDenseVectorBlob | NodeSparseVectorBlob => (
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
        ),
        LegacyNodePropertyIndex
        | NodePropertyHashMetadata
        | NodePropertyEqualityIndex { .. }
        | NodePropertyRangeIndex { .. } => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
        ),
        EdgeWeightIndex | EdgeUpdatedAtIndex | EdgeValidFromIndex | EdgeValidToIndex => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::MetadataScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
        ),
        DegreeDelta => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::AdjacencyWalk,
            },
            ComponentTrustClass::OptionalExactAccelerator,
        ),
        PlannerStats => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::PlannerStatsUnavailable,
            },
            ComponentTrustClass::OptionalAdvisoryStats,
        ),
        DenseHnswMetadata | DenseHnswGraph | SparsePostingIndex | SparsePostings => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::ExactVectorScan,
            },
            ComponentTrustClass::OptionalApproximateAccelerator,
        ),
        EdgePropertyEqualityIndex { .. } | EdgePropertyRangeIndex { .. } => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
        ),
        NodeCompoundEqualityIndex { .. }
        | NodeCompoundRangeIndex { .. }
        | EdgeCompoundEqualityIndex { .. }
        | EdgeCompoundRangeIndex { .. } => (
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::RecordScan,
            },
            ComponentTrustClass::OptionalCandidateIndex,
        ),
        PackedSegmentContainer => (
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
        ),
    };
    Some(contract)
}

fn expected_component_dependencies(
    kind: &SegmentComponentKind,
    registry: &SegmentComponentRegistry,
    source_groups: &SegmentComponentSourceGroups,
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<ComponentDependencyV1>, EngineError> {
    use SegmentComponentKind::*;
    Ok(match kind {
        NodeRecords | EdgeRecords | NodeMetadata | EdgeMetadata | Tombstones => Vec::new(),
        KeyIndex | NodeLabelIndex | TimestampIndex => vec![source_group_dependency(
            SegmentSourceGroupKind::NodeSource,
            source_groups.node_source,
        )],
        EdgeLabelIndex | EdgeTripleIndex | AdjOutIndex | AdjOutPostings | AdjInIndex
        | AdjInPostings => vec![source_group_dependency(
            SegmentSourceGroupKind::EdgeSource,
            source_groups.edge_source,
        )],
        LegacyNodePropertyIndex => vec![source_group_dependency(
            SegmentSourceGroupKind::NodePropertyContentSource,
            source_groups.node_property_content_source,
        )],
        NodePropertyHashMetadata => vec![source_group_dependency(
            SegmentSourceGroupKind::NodeSource,
            source_groups.node_source,
        )],
        NodeVectorMetadata => vec![source_group_dependency(
            SegmentSourceGroupKind::NodeSource,
            source_groups.node_source,
        )],
        NodeDenseVectorBlob | NodeSparseVectorBlob => {
            let vector_meta = registry.record(&NodeVectorMetadata).ok_or_else(|| {
                EngineError::CorruptRecord("vector blob missing metadata component".into())
            })?;
            vec![
                source_group_dependency(
                    SegmentSourceGroupKind::NodeSource,
                    source_groups.node_source,
                ),
                source_component_dependency(vector_meta),
            ]
        }
        NodePropertyEqualityIndex { index_id } | NodePropertyRangeIndex { index_id } => {
            let entry = secondary_indexes
                .iter()
                .find(|entry| {
                    entry.index_id == *index_id
                        && matches!(entry.target, SecondaryIndexTarget::NodeProperty { .. })
                })
                .ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "component {:?} has no matching secondary index declaration",
                        kind
                    ))
                })?;
            vec![
                source_group_dependency(
                    SegmentSourceGroupKind::NodePropertyContentSource,
                    source_groups.node_property_content_source,
                ),
                secondary_declaration_dependency(entry),
            ]
        }
        EdgePropertyEqualityIndex { index_id } | EdgePropertyRangeIndex { index_id } => {
            let entry = secondary_indexes
                .iter()
                .find(|entry| {
                    entry.index_id == *index_id
                        && matches!(entry.target, SecondaryIndexTarget::EdgeProperty { .. })
                })
                .ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "component {:?} has no matching secondary index declaration",
                        kind
                    ))
                })?;
            vec![
                source_group_dependency(
                    SegmentSourceGroupKind::EdgeSource,
                    source_groups.edge_source,
                ),
                secondary_declaration_dependency(entry),
            ]
        }
        NodeCompoundEqualityIndex { index_id } | NodeCompoundRangeIndex { index_id } => {
            let entry = secondary_indexes
                .iter()
                .find(|entry| {
                    entry.index_id == *index_id
                        && matches!(entry.target, SecondaryIndexTarget::NodeFieldIndex { .. })
                })
                .ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "component {:?} has no matching secondary index declaration",
                        kind
                    ))
                })?;
            secondary_index_component_dependencies_for_entry(entry, source_groups)
        }
        EdgeCompoundEqualityIndex { index_id } | EdgeCompoundRangeIndex { index_id } => {
            let entry = secondary_indexes
                .iter()
                .find(|entry| {
                    entry.index_id == *index_id
                        && matches!(entry.target, SecondaryIndexTarget::EdgeFieldIndex { .. })
                })
                .ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "component {:?} has no matching secondary index declaration",
                        kind
                    ))
                })?;
            secondary_index_component_dependencies_for_entry(entry, source_groups)
        }
        EdgeWeightIndex | EdgeUpdatedAtIndex | EdgeValidFromIndex | EdgeValidToIndex => {
            vec![source_group_dependency(
                SegmentSourceGroupKind::EdgeMetadataSource,
                source_groups.edge_metadata_source,
            )]
        }
        DegreeDelta => vec![source_group_dependency(
            SegmentSourceGroupKind::DegreeSource,
            source_groups.degree_source,
        )],
        PlannerStats => {
            planner_stats_component_dependencies(source_groups.segment_data_id, secondary_indexes)
        }
        DenseHnswMetadata | DenseHnswGraph => vec![
            source_group_dependency(
                SegmentSourceGroupKind::DenseVectorSource,
                source_groups.dense_vector_source,
            ),
            ComponentDependencyV1::DenseVectorConfig {
                fingerprint: dense_config_fingerprint(dense_config),
            },
        ],
        SparsePostingIndex | SparsePostings => vec![
            source_group_dependency(
                SegmentSourceGroupKind::SparseVectorSource,
                source_groups.sparse_vector_source,
            ),
            ComponentDependencyV1::SparseVectorConfig {
                fingerprint: component_fingerprint("sparse_vector_config", &[]),
            },
        ],
        PackedSegmentContainer => Vec::new(),
    })
}

fn component_build_fingerprint_matches(
    kind: &SegmentComponentKind,
    actual: u64,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> bool {
    use SegmentComponentKind::*;
    match kind {
        EdgePropertyEqualityIndex { index_id } => {
            actual == edge_property_equality_component_fingerprint(*index_id)
        }
        _ => actual == expected_component_build_fingerprint(kind, secondary_indexes),
    }
}

fn expected_component_build_fingerprint(
    kind: &SegmentComponentKind,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> u64 {
    use SegmentComponentKind::*;
    match kind {
        NodeRecords => component_fingerprint("flush.nodes", &[]),
        EdgeRecords => component_fingerprint("flush.edges", &[]),
        NodeMetadata => component_fingerprint("flush.node_meta", &[]),
        EdgeMetadata => component_fingerprint("flush.edge_meta", &[]),
        Tombstones => component_fingerprint("flush.tombstones", &[]),
        KeyIndex => component_fingerprint("flush.key_index", &[]),
        NodeLabelIndex => component_fingerprint("flush.node_label_index", &[]),
        EdgeLabelIndex => component_fingerprint("flush.edge_label_index", &[]),
        EdgeTripleIndex => component_fingerprint("flush.edge_triple_index", &[]),
        AdjOutIndex => component_fingerprint("flush.adj_out_idx", &[]),
        AdjOutPostings => component_fingerprint("flush.adj_out_dat", &[]),
        AdjInIndex => component_fingerprint("flush.adj_in_idx", &[]),
        AdjInPostings => component_fingerprint("flush.adj_in_dat", &[]),
        TimestampIndex => component_fingerprint("flush.timestamp_index", &[]),
        LegacyNodePropertyIndex => component_fingerprint("flush.prop_index", &[]),
        NodePropertyHashMetadata => component_fingerprint("flush.node_prop_hashes", &[]),
        NodePropertyEqualityIndex { index_id } => {
            node_property_equality_component_fingerprint(*index_id)
        }
        NodePropertyRangeIndex { index_id } => node_property_range_component_fingerprint(*index_id),
        EdgeWeightIndex => component_fingerprint("flush.edge_weight_index", &[]),
        EdgeUpdatedAtIndex => component_fingerprint("flush.edge_updated_at_index", &[]),
        EdgeValidFromIndex => component_fingerprint("flush.edge_valid_from_index", &[]),
        EdgeValidToIndex => component_fingerprint("flush.edge_valid_to_index", &[]),
        DegreeDelta => component_fingerprint("flush.degree_delta", &[]),
        PlannerStats => planner_stats_component_fingerprint(secondary_indexes),
        NodeVectorMetadata => component_fingerprint("flush.node_vector_meta", &[]),
        NodeDenseVectorBlob => component_fingerprint("flush.node_dense_vectors", &[]),
        NodeSparseVectorBlob => component_fingerprint("flush.node_sparse_vectors", &[]),
        DenseHnswMetadata => component_fingerprint("flush.dense_hnsw_meta", &[]),
        DenseHnswGraph => component_fingerprint("flush.dense_hnsw_graph", &[]),
        SparsePostingIndex => component_fingerprint("flush.sparse_posting_index", &[]),
        SparsePostings => component_fingerprint("flush.sparse_postings", &[]),
        EdgePropertyEqualityIndex { index_id } => {
            edge_property_equality_component_fingerprint(*index_id)
        }
        EdgePropertyRangeIndex { index_id } => edge_property_range_component_fingerprint(*index_id),
        NodeCompoundEqualityIndex { .. }
        | NodeCompoundRangeIndex { .. }
        | EdgeCompoundEqualityIndex { .. }
        | EdgeCompoundRangeIndex { .. } => secondary_indexes
            .iter()
            .find_map(|entry| compound_component_fingerprint_for_kind_and_entry(kind, entry))
            .unwrap_or(0),
        PackedSegmentContainer => component_fingerprint("flush.packed_segment_container", &[]),
    }
}

fn open_required_manifest_payload(
    registry: &SegmentComponentRegistry,
    context: &ComponentOpenContext,
    seg_dir: &Path,
    kind: SegmentComponentKind,
) -> Result<MappedData, EngineError> {
    let record = registry.record(&kind).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "segment manifest missing required component {:?}",
            kind
        ))
    })?;
    match open_manifest_component_record(seg_dir, registry.segment_id, record, Some(context)) {
        Ok(data) => {
            registry.set_availability(kind, ComponentAvailability::Available);
            Ok(data)
        }
        Err(error) => {
            registry.set_availability(
                kind.clone(),
                ComponentAvailability::CorruptIdentity {
                    reason: error.to_string(),
                },
            );
            Err(error)
        }
    }
}

fn open_manifested_required_payload_or_empty(
    registry: &SegmentComponentRegistry,
    context: &ComponentOpenContext,
    seg_dir: &Path,
    kind: SegmentComponentKind,
) -> Result<MappedData, EngineError> {
    if registry.record(&kind).is_none() {
        registry.set_availability(kind, ComponentAvailability::Missing);
        return Ok(MappedData::Empty);
    }
    open_required_manifest_payload(registry, context, seg_dir, kind)
}

fn open_optional_manifest_payload(
    registry: &SegmentComponentRegistry,
    context: Option<&ComponentOpenContext>,
    seg_dir: &Path,
    kind: SegmentComponentKind,
) -> Result<MappedData, EngineError> {
    try_open_optional_manifest_payload(registry, context, seg_dir, kind)
        .map(|data| data.unwrap_or(MappedData::Empty))
}

fn try_open_optional_manifest_payload(
    registry: &SegmentComponentRegistry,
    context: Option<&ComponentOpenContext>,
    seg_dir: &Path,
    kind: SegmentComponentKind,
) -> Result<Option<MappedData>, EngineError> {
    if let Some(state) = registry.recorded_availability(&kind) {
        if !state.is_available() {
            return Ok(None);
        }
    }
    let Some(record) = registry.record(&kind) else {
        registry.set_availability(kind, ComponentAvailability::Missing);
        return Ok(None);
    };
    match open_manifest_component_record(seg_dir, registry.segment_id, record, context) {
        Ok(data) => {
            registry.set_availability(kind, ComponentAvailability::Available);
            Ok(Some(data))
        }
        Err(EngineError::IoError(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            registry.set_availability(kind, ComponentAvailability::Missing);
            Ok(None)
        }
        Err(error) => {
            registry.set_availability(
                kind,
                ComponentAvailability::CorruptIdentity {
                    reason: error.to_string(),
                },
            );
            Ok(None)
        }
    }
}

fn open_planner_stats(
    registry: &SegmentComponentRegistry,
    seg_dir: &Path,
    segment_id: u64,
    node_count: u64,
    edge_count: u64,
) -> PlannerStatsAvailability {
    match try_open_optional_manifest_payload(
        registry,
        None,
        seg_dir,
        SegmentComponentKind::PlannerStats,
    ) {
        Ok(Some(data)) => {
            let availability =
                read_planner_stats_payload(&data, segment_id, node_count, edge_count);
            if let PlannerStatsAvailability::Unavailable { reason } = &availability {
                registry.set_availability(
                    SegmentComponentKind::PlannerStats,
                    ComponentAvailability::CorruptIdentity {
                        reason: reason.clone(),
                    },
                );
            }
            availability
        }
        Ok(None) => match registry.availability(&SegmentComponentKind::PlannerStats) {
            ComponentAvailability::Missing => PlannerStatsAvailability::Missing,
            state => PlannerStatsAvailability::Unavailable {
                reason: format!("{:?}", state),
            },
        },
        Err(error) => PlannerStatsAvailability::Unavailable {
            reason: error.to_string(),
        },
    }
}

fn open_degree_delta_sidecar(
    registry: &SegmentComponentRegistry,
    seg_dir: &Path,
) -> Option<DegreeSidecar> {
    let record = registry.record(&SegmentComponentKind::DegreeDelta)?;
    let ComponentHandleV1::ExternalFile { relative_path, .. } = &record.handle else {
        registry.set_availability(
            SegmentComponentKind::DegreeDelta,
            ComponentAvailability::Unsupported {
                reason: "packed degree sidecar handles are unsupported".into(),
            },
        );
        return None;
    };
    try_open_optional_manifest_payload(registry, None, seg_dir, SegmentComponentKind::DegreeDelta)
        .ok()
        .flatten()?;
    match DegreeSidecar::open(&seg_dir.join(relative_path)) {
        Ok(sidecar) => Some(sidecar),
        Err(error) => {
            registry.set_availability(
                SegmentComponentKind::DegreeDelta,
                ComponentAvailability::CorruptIdentity {
                    reason: error.to_string(),
                },
            );
            None
        }
    }
}

fn mark_optional_components_corrupt(
    registry: &SegmentComponentRegistry,
    kinds: &[SegmentComponentKind],
    reason: String,
) {
    for kind in kinds {
        if registry.record(kind).is_some()
            && !matches!(registry.availability(kind), ComponentAvailability::Missing)
        {
            registry.set_availability(
                kind.clone(),
                ComponentAvailability::CorruptIdentity {
                    reason: reason.clone(),
                },
            );
        }
    }
}

impl ComponentOpenContext {
    fn open(
        seg_dir: &Path,
        segment_id: u64,
        manifest: &SegmentComponentManifestV1,
    ) -> Result<Self, EngineError> {
        validate_packed_core_manifest_contract_for_open(manifest)?;
        let has_packed_ranges = manifest
            .components
            .iter()
            .any(|record| matches!(record.handle, ComponentHandleV1::PackedRange { .. }));
        let has_container = manifest
            .components
            .iter()
            .any(|record| record.kind == SegmentComponentKind::PackedSegmentContainer);
        if !has_packed_ranges && !has_container {
            return Ok(Self {
                packed_core: None,
                invalid_optional_packed_ranges: HashMap::new(),
            });
        }
        if !has_container {
            return Ok(Self {
                packed_core: None,
                invalid_optional_packed_ranges: HashMap::new(),
            });
        }

        let container = packed_core_container_record(manifest)?;
        let mapped = open_external_manifest_component_record(seg_dir, segment_id, container)?;
        let MappedData::Mmap {
            mmap,
            payload_offset,
            payload_len,
        } = mapped
        else {
            return Err(EngineError::CorruptRecord(format!(
                "{PACKED_CORE_FILENAME} must contain an identity header"
            )));
        };
        let packed_core = PackedCoreMapping {
            component_id: container.component_id,
            mmap,
            payload_offset,
            payload_len,
        };
        let invalid_optional_packed_ranges =
            collect_invalid_optional_packed_ranges(manifest, &packed_core);
        Ok(Self {
            packed_core: Some(packed_core),
            invalid_optional_packed_ranges,
        })
    }
}

#[derive(Clone)]
struct PackedRangeForOpen {
    kind: SegmentComponentKind,
    optional: bool,
    start: u64,
    end: u64,
}

fn collect_invalid_optional_packed_ranges(
    manifest: &SegmentComponentManifestV1,
    packed_core: &PackedCoreMapping,
) -> HashMap<SegmentComponentKind, String> {
    let mut invalid = HashMap::new();
    let mut ranges = Vec::new();
    for record in &manifest.components {
        if !matches!(record.handle, ComponentHandleV1::PackedRange { .. }) {
            continue;
        }
        match validate_packed_range_for_open(record, packed_core) {
            Ok(Some(range)) => ranges.push(range),
            Ok(None) => {}
            Err(error) if record.requirement != ComponentRequirement::Required => {
                invalid.insert(record.kind.clone(), error);
            }
            Err(_) => {}
        }
    }

    ranges.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
            .then_with(|| left.kind.kind_tag().cmp(&right.kind.kind_tag()))
            .then_with(|| left.kind.index_id().cmp(&right.kind.index_id()))
    });
    for pair in ranges.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.start < previous.end {
            let reason = format!(
                "packed component range overlaps another range: previous={:?} [{}, {}), current={:?} [{}, {})",
                previous.kind, previous.start, previous.end, current.kind, current.start, current.end
            );
            if previous.optional {
                invalid.insert(previous.kind.clone(), reason.clone());
            }
            if current.optional {
                invalid.insert(current.kind.clone(), reason);
            }
        }
    }

    invalid
}

fn validate_packed_range_for_open(
    record: &SegmentComponentRecordV1,
    packed_core: &PackedCoreMapping,
) -> Result<Option<PackedRangeForOpen>, String> {
    if !is_packed_core_component_kind(&record.kind) {
        return Err(format!(
            "component {:?} is not allowed in {PACKED_CORE_FILENAME}",
            record.kind
        ));
    }
    let ComponentHandleV1::PackedRange {
        container_component_id,
        offset,
        len,
    } = &record.handle
    else {
        return Ok(None);
    };
    if *container_component_id != packed_core.component_id {
        return Err(format!(
            "packed component {:?} points at the wrong {PACKED_CORE_FILENAME} container",
            record.kind
        ));
    }
    if record.payload_len != *len {
        return Err(format!(
            "packed component {:?} payload length does not match packed range",
            record.kind
        ));
    }
    let end = offset
        .checked_add(*len)
        .ok_or_else(|| format!("packed component {:?} range overflows", record.kind))?;
    if end > packed_core.payload_len as u64 {
        return Err(format!(
            "packed component {:?} range [{}, {}) exceeds {PACKED_CORE_FILENAME} payload length {}",
            record.kind, offset, end, packed_core.payload_len
        ));
    }
    if *len == 0 {
        return Ok(None);
    }
    Ok(Some(PackedRangeForOpen {
        kind: record.kind.clone(),
        optional: record.requirement != ComponentRequirement::Required,
        start: *offset,
        end,
    }))
}

fn open_manifest_component_record(
    seg_dir: &Path,
    segment_id: u64,
    record: &SegmentComponentRecordV1,
    context: Option<&ComponentOpenContext>,
) -> Result<MappedData, EngineError> {
    match &record.handle {
        ComponentHandleV1::ExternalFile { .. } => {
            open_external_manifest_component_record(seg_dir, segment_id, record)
        }
        ComponentHandleV1::PackedRange { .. } => {
            open_packed_manifest_component_record(record, context)
        }
    }
}

fn open_external_manifest_component_record(
    seg_dir: &Path,
    segment_id: u64,
    record: &SegmentComponentRecordV1,
) -> Result<MappedData, EngineError> {
    let ComponentHandleV1::ExternalFile {
        relative_path,
        payload_offset,
        payload_len,
    } = &record.handle
    else {
        return Err(EngineError::CorruptRecord(
            "external component opener received a packed range handle".into(),
        ));
    };
    let path = seg_dir.join(relative_path);
    let data = mmap_file_payload(&path, *payload_offset, *payload_len)?;
    let MappedData::Mmap { mmap, .. } = &data else {
        return Ok(data);
    };
    let header = decode_identity_header(mmap)?;
    if header.segment_format_version != SEGMENT_FORMAT_VERSION
        || header.segment_id != segment_id
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
        return Err(EngineError::CorruptRecord(format!(
            "component identity header does not match manifest for {:?}",
            record.kind
        )));
    }
    Ok(data)
}

fn open_packed_manifest_component_record(
    record: &SegmentComponentRecordV1,
    context: Option<&ComponentOpenContext>,
) -> Result<MappedData, EngineError> {
    let Some(context) = context else {
        return Err(EngineError::CorruptRecord(format!(
            "packed component {:?} cannot be opened without {PACKED_CORE_FILENAME}",
            record.kind
        )));
    };
    let Some(packed_core) = &context.packed_core else {
        return Err(EngineError::CorruptRecord(format!(
            "packed component {:?} has no {PACKED_CORE_FILENAME} mapping",
            record.kind
        )));
    };
    if let Some(reason) = context.invalid_optional_packed_ranges.get(&record.kind) {
        return Err(EngineError::CorruptRecord(reason.clone()));
    }
    if !is_packed_core_component_kind(&record.kind) {
        return Err(EngineError::CorruptRecord(format!(
            "component {:?} is not allowed in {PACKED_CORE_FILENAME}",
            record.kind
        )));
    }
    let ComponentHandleV1::PackedRange {
        container_component_id,
        offset,
        len,
    } = &record.handle
    else {
        unreachable!("packed component opener received external handle")
    };
    if *container_component_id != packed_core.component_id {
        return Err(EngineError::CorruptRecord(format!(
            "packed component {:?} points at the wrong {PACKED_CORE_FILENAME} container",
            record.kind
        )));
    }
    if record.payload_len != *len {
        return Err(EngineError::CorruptRecord(format!(
            "packed component {:?} payload length does not match packed range",
            record.kind
        )));
    }
    let end = offset.checked_add(*len).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "packed component {:?} range overflows",
            record.kind
        ))
    })?;
    if end > packed_core.payload_len as u64 {
        return Err(EngineError::CorruptRecord(format!(
            "packed component {:?} range [{}, {}) exceeds {PACKED_CORE_FILENAME} payload length {}",
            record.kind, offset, end, packed_core.payload_len
        )));
    }
    let range_offset = usize::try_from(*offset).map_err(|_| {
        EngineError::CorruptRecord(format!(
            "packed component {:?} offset does not fit in usize",
            record.kind
        ))
    })?;
    let range_len = usize::try_from(*len).map_err(|_| {
        EngineError::CorruptRecord(format!(
            "packed component {:?} length does not fit in usize",
            record.kind
        ))
    })?;
    let payload_offset = packed_core
        .payload_offset
        .checked_add(range_offset)
        .ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "packed component {:?} physical offset overflows",
                record.kind
            ))
        })?;
    Ok(MappedData::Mmap {
        mmap: Arc::clone(&packed_core.mmap),
        payload_offset,
        payload_len: range_len,
    })
}

fn mmap_file_payload(
    path: &Path,
    payload_offset: u64,
    payload_len: u64,
) -> Result<MappedData, EngineError> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    let file_len = meta.len();
    let (payload_offset, payload_len) = if payload_len == u64::MAX {
        if file_len == 0 {
            return Ok(MappedData::Empty);
        }
        (0usize, file_len as usize)
    } else {
        let end = payload_offset.checked_add(payload_len).ok_or_else(|| {
            EngineError::CorruptRecord(format!(
                "component payload range overflows for {}",
                path.display()
            ))
        })?;
        if end != file_len {
            return Err(EngineError::CorruptRecord(format!(
                "component payload range [{}, {}) does not match file length {} for {}",
                payload_offset,
                end,
                file_len,
                path.display()
            )));
        }
        (payload_offset as usize, payload_len as usize)
    };
    if file_len == 0 {
        return Err(EngineError::CorruptRecord(format!(
            "manifest component file is empty for {}",
            path.display()
        )));
    }
    // SAFETY: Segment files are immutable after write. No concurrent modification.
    let mmap = unsafe { Mmap::map(&file).map_err(EngineError::IoError)? };
    Ok(MappedData::Mmap {
        mmap: Arc::new(mmap),
        payload_offset,
        payload_len,
    })
}

fn validate_secondary_eq_sidecar_data(data: &[u8]) -> Result<(), EngineError> {
    let (count, idx_bytes) = secondary_eq_sidecar_index_bounds(data)?;

    let mut prev_value_hash = None;
    let mut prev_end = idx_bytes;
    for index in 0..count {
        let entry_off = 8 + index * SECONDARY_EQ_ENTRY_SIZE;
        let value_hash = read_u64_at(data, entry_off)?;
        let (offset, id_count) = secondary_eq_group_range_from_entry(data, idx_bytes, entry_off)?;
        let end = offset + id_count * 8;
        if let Some(previous) = prev_value_hash {
            if value_hash <= previous {
                return Err(EngineError::CorruptRecord(format!(
                    "secondary equality sidecar value hashes are not strictly increasing at group {}",
                    index
                )));
            }
        }
        if offset < prev_end {
            return Err(EngineError::CorruptRecord(format!(
                "secondary equality sidecar group {} range [{}, {}) overlaps a previous group",
                index, offset, end
            )));
        }
        let mut previous_node_id = None;
        for id_index in 0..id_count {
            let node_id = read_u64_at(data, offset + id_index * 8)?;
            if let Some(previous) = previous_node_id {
                if node_id <= previous {
                    return Err(EngineError::CorruptRecord(format!(
                        "secondary equality sidecar group {} node IDs are not strictly increasing",
                        index
                    )));
                }
            }
            previous_node_id = Some(node_id);
        }
        prev_value_hash = Some(value_hash);
        prev_end = end;
    }

    Ok(())
}

fn secondary_eq_sidecar_index_bounds(data: &[u8]) -> Result<(usize, usize), EngineError> {
    if data.len() < 8 {
        return Err(EngineError::CorruptRecord(
            "secondary equality sidecar missing header".into(),
        ));
    }

    let count = read_u64_at(data, 0)? as usize;
    let idx_bytes = count
        .checked_mul(SECONDARY_EQ_ENTRY_SIZE)
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or_else(|| {
            EngineError::CorruptRecord("secondary equality sidecar index overflow".into())
        })?;
    if idx_bytes > data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "secondary equality sidecar index length {} exceeds file length {}",
            idx_bytes,
            data.len()
        )));
    }

    Ok((count, idx_bytes))
}

fn validate_secondary_eq_sidecar_index_header(data: &[u8]) -> Result<(), EngineError> {
    secondary_eq_sidecar_index_bounds(data).map(|_| ())
}

fn secondary_eq_group_range_from_entry(
    data: &[u8],
    idx_bytes: usize,
    entry_off: usize,
) -> Result<(usize, usize), EngineError> {
    let offset = read_u64_at(data, entry_off + 8)? as usize;
    let id_count = read_u32_at(data, entry_off + 16)? as usize;
    let end = offset
        .checked_add(id_count.checked_mul(8).ok_or_else(|| {
            EngineError::CorruptRecord("secondary equality sidecar group overflow".into())
        })?)
        .ok_or_else(|| {
            EngineError::CorruptRecord("secondary equality sidecar group end overflow".into())
        })?;
    if offset < idx_bytes || end > data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "secondary equality sidecar group range [{}, {}) exceeds file length {}",
            offset,
            end,
            data.len()
        )));
    }
    Ok((offset, id_count))
}

fn secondary_eq_group_range(
    data: &[u8],
    value_hash: u64,
) -> Result<Option<(usize, usize)>, EngineError> {
    let (count, idx_bytes) = secondary_eq_sidecar_index_bounds(data)?;
    if count == 0 {
        return Ok(None);
    }

    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_off = 8 + mid * SECONDARY_EQ_ENTRY_SIZE;
        let entry_value_hash = read_u64_at(data, entry_off)?;
        match entry_value_hash.cmp(&value_hash) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                return Ok(Some(secondary_eq_group_range_from_entry(
                    data, idx_bytes, entry_off,
                )?));
            }
        }
    }

    Ok(None)
}

fn read_numeric_range_sidecar_key_at(
    data: &[u8],
    offset: usize,
) -> Result<NumericRangeSortKey, EngineError> {
    let key_bytes: [u8; NUMERIC_RANGE_KEY_BYTES] =
        read_bytes_at(data, offset, NUMERIC_RANGE_KEY_BYTES)?
            .try_into()
            .expect("fixed numeric range sidecar key length");
    NumericRangeSortKey::from_sidecar_bytes(key_bytes)
}

fn validate_secondary_range_sidecar_data(data: &[u8]) -> Result<(), EngineError> {
    validate_secondary_range_sidecar_header(data)?;
    let count = read_u64_at(data, 0)? as usize;

    let mut previous = None;
    for index in 0..count {
        let entry_off = 8 + index * SECONDARY_RANGE_ENTRY_SIZE;
        let current = (
            read_numeric_range_sidecar_key_at(data, entry_off)?,
            read_u64_at(data, entry_off + NUMERIC_RANGE_KEY_BYTES)?,
        );
        if let Some(prev) = previous {
            if current <= prev {
                return Err(EngineError::CorruptRecord(format!(
                    "secondary range sidecar entries are not strictly increasing at entry {}",
                    index
                )));
            }
        }
        previous = Some(current);
    }

    Ok(())
}

fn validate_secondary_range_sidecar_header(data: &[u8]) -> Result<(), EngineError> {
    if data.len() < 8 {
        return Err(EngineError::CorruptRecord(
            "secondary range sidecar missing header".into(),
        ));
    }

    let count = read_u64_at(data, 0)? as usize;
    let entries_bytes = count
        .checked_mul(SECONDARY_RANGE_ENTRY_SIZE)
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or_else(|| {
            EngineError::CorruptRecord("secondary range sidecar index overflow".into())
        })?;
    if entries_bytes != data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "secondary range sidecar length {} does not match expected fixed-width length {}",
            data.len(),
            entries_bytes
        )));
    }
    Ok(())
}

fn find_nodes_in_secondary_eq_sidecar(
    data: &[u8],
    deleted_nodes: &NodeIdMap<TombstoneEntry>,
    value_hash: u64,
) -> Result<Vec<u64>, EngineError> {
    let Some((offset, id_count)) = secondary_eq_group_range(data, value_hash)? else {
        return Ok(Vec::new());
    };

    let mut result = Vec::with_capacity(id_count);
    for index in 0..id_count {
        let node_id = read_u64_at(data, offset + index * 8)?;
        if !deleted_nodes.contains_key(&node_id) {
            result.push(node_id);
        }
    }
    Ok(result)
}

fn secondary_eq_posting_chunk(
    data: &[u8],
    deleted_nodes: &NodeIdMap<TombstoneEntry>,
    value_hash: u64,
    start: usize,
    raw_limit: usize,
) -> Result<SecondaryEqPostingChunk, EngineError> {
    let Some((offset, id_count)) = secondary_eq_group_range(data, value_hash)? else {
        return Ok(SecondaryEqPostingChunk {
            ids: Vec::new(),
            next_offset: 0,
            exhausted: true,
        });
    };
    if start >= id_count {
        return Ok(SecondaryEqPostingChunk {
            ids: Vec::new(),
            next_offset: id_count,
            exhausted: true,
        });
    }

    let end = start.saturating_add(raw_limit.max(1)).min(id_count);
    let mut ids = Vec::with_capacity(end - start);
    for index in start..end {
        let node_id = read_u64_at(data, offset + index * 8)?;
        if !deleted_nodes.contains_key(&node_id) {
            ids.push(node_id);
        }
    }

    Ok(SecondaryEqPostingChunk {
        ids,
        next_offset: end,
        exhausted: end >= id_count,
    })
}

fn secondary_eq_posting_count(data: &[u8], value_hash: u64) -> Result<usize, EngineError> {
    Ok(secondary_eq_group_range(data, value_hash)?
        .map(|(_, count)| count)
        .unwrap_or(0))
}

fn secondary_eq_visible_posting_count(
    data: &[u8],
    deleted_ids: &NodeIdMap<TombstoneEntry>,
    value_hash: u64,
) -> Result<usize, EngineError> {
    let Some((offset, id_count)) = secondary_eq_group_range(data, value_hash)? else {
        return Ok(0);
    };
    let mut count = 0usize;
    for id_index in 0..id_count {
        let id = read_u64_at(data, offset + id_index * 8)?;
        if !deleted_ids.contains_key(&id) {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

fn secondary_range_sidecar_lower_bound(
    data: &[u8],
    target: (NumericRangeSortKey, u64),
    strict: bool,
) -> Result<usize, EngineError> {
    let count = read_u64_at(data, 0)? as usize;
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_off = 8 + mid * SECONDARY_RANGE_ENTRY_SIZE;
        let current = (
            read_numeric_range_sidecar_key_at(data, entry_off)?,
            read_u64_at(data, entry_off + NUMERIC_RANGE_KEY_BYTES)?,
        );
        let ordering = current.cmp(&target);
        if ordering == std::cmp::Ordering::Less || (strict && ordering == std::cmp::Ordering::Equal)
        {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn find_nodes_in_secondary_range_sidecar(
    data: &[u8],
    deleted_nodes: &NodeIdMap<TombstoneEntry>,
    lower: Option<(NumericRangeSortKey, bool)>,
    upper: Option<(NumericRangeSortKey, bool)>,
    after: Option<(NumericRangeSortKey, u64)>,
    limit: Option<usize>,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let count = read_u64_at(data, 0)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut start: Option<((NumericRangeSortKey, u64), bool)> = None;
    if let Some((encoded_value, inclusive)) = lower {
        let candidate = if inclusive {
            ((encoded_value, 0), false)
        } else {
            ((encoded_value, u64::MAX), true)
        };
        start = Some(candidate);
    }
    if let Some(after) = after {
        let candidate = (after, true);
        start = Some(match start {
            Some(existing) if existing.0 > candidate.0 => existing,
            Some(existing) if existing.0 < candidate.0 => candidate,
            Some(existing) => (existing.0, existing.1 || candidate.1),
            None => candidate,
        });
    }

    let start_index = if let Some((target, strict)) = start {
        secondary_range_sidecar_lower_bound(data, target, strict)?
    } else {
        0
    };

    let mut results = Vec::new();
    for index in start_index..count {
        let entry_off = 8 + index * SECONDARY_RANGE_ENTRY_SIZE;
        let encoded_value = read_numeric_range_sidecar_key_at(data, entry_off)?;
        let node_id = read_u64_at(data, entry_off + NUMERIC_RANGE_KEY_BYTES)?;
        if let Some((upper_value, inclusive)) = upper {
            if encoded_value > upper_value || (!inclusive && encoded_value == upper_value) {
                break;
            }
        }
        if !deleted_nodes.contains_key(&node_id) {
            results.push((encoded_value, node_id));
            if limit.is_some_and(|limit| results.len() >= limit) {
                break;
            }
        }
    }

    Ok(results)
}

fn secondary_range_sidecar_start_index(
    data: &[u8],
    lower: Option<(NumericRangeSortKey, bool)>,
    after: Option<(NumericRangeSortKey, u64)>,
) -> Result<usize, EngineError> {
    let mut start: Option<((NumericRangeSortKey, u64), bool)> = None;
    if let Some((encoded_value, inclusive)) = lower {
        let candidate = if inclusive {
            ((encoded_value, 0), false)
        } else {
            ((encoded_value, u64::MAX), true)
        };
        start = Some(candidate);
    }
    if let Some(after) = after {
        let candidate = (after, true);
        start = Some(match start {
            Some(existing) if existing.0 > candidate.0 => existing,
            Some(existing) if existing.0 < candidate.0 => candidate,
            Some(existing) => (existing.0, existing.1 || candidate.1),
            None => candidate,
        });
    }

    if let Some((target, strict)) = start {
        secondary_range_sidecar_lower_bound(data, target, strict)
    } else {
        Ok(0)
    }
}

fn secondary_range_sidecar_end_index(
    data: &[u8],
    upper: Option<(NumericRangeSortKey, bool)>,
) -> Result<usize, EngineError> {
    let count = read_u64_at(data, 0)? as usize;
    let Some((upper_value, inclusive)) = upper else {
        return Ok(count);
    };
    if inclusive {
        secondary_range_sidecar_lower_bound(data, (upper_value, u64::MAX), true)
    } else {
        secondary_range_sidecar_lower_bound(data, (upper_value, 0), false)
    }
}

fn count_nodes_in_secondary_range_sidecar(
    data: &[u8],
    deleted_nodes: &NodeIdMap<TombstoneEntry>,
    lower: Option<(NumericRangeSortKey, bool)>,
    upper: Option<(NumericRangeSortKey, bool)>,
) -> Result<usize, EngineError> {
    let count = read_u64_at(data, 0)? as usize;
    if count == 0 {
        return Ok(0);
    }

    let start_index = secondary_range_sidecar_start_index(data, lower, None)?;
    let end_index = secondary_range_sidecar_end_index(data, upper)?;
    if end_index <= start_index {
        return Ok(0);
    }
    if deleted_nodes.is_empty() {
        return Ok(end_index - start_index);
    }

    let mut visible = 0usize;
    for index in start_index..end_index {
        let entry_off = 8 + index * SECONDARY_RANGE_ENTRY_SIZE;
        let node_id = read_u64_at(data, entry_off + NUMERIC_RANGE_KEY_BYTES)?;
        if !deleted_nodes.contains_key(&node_id) {
            visible += 1;
        }
    }
    Ok(visible)
}

struct PropLookupSeed<'a> {
    target: &'a str,
}

impl<'de> DeserializeSeed<'de> for PropLookupSeed<'_> {
    type Value = Option<PropValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PropLookupVisitor {
            target: self.target,
        })
    }
}

struct PropLookupVisitor<'a> {
    target: &'a str,
}

impl<'de> Visitor<'de> for PropLookupVisitor<'_> {
    type Value = Option<PropValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a node property map")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut found = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == self.target {
                found = Some(map.next_value()?);
            } else {
                let _: IgnoredAny = map.next_value()?;
            }
        }
        Ok(found)
    }
}

struct PropProjectionSeed<'a> {
    targets: &'a [String],
}

impl<'de> DeserializeSeed<'de> for PropProjectionSeed<'_> {
    type Value = BTreeMap<String, PropValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PropProjectionVisitor {
            targets: self.targets,
        })
    }
}

struct PropProjectionVisitor<'a> {
    targets: &'a [String],
}

impl<'de> Visitor<'de> for PropProjectionVisitor<'_> {
    type Value = BTreeMap<String, PropValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a property map")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut found = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            if self.targets.iter().any(|target| target == &key) {
                found.insert(key, map.next_value()?);
            } else {
                let _: IgnoredAny = map.next_value()?;
            }
        }
        Ok(found)
    }
}

fn decode_node_property_at(
    data: &[u8],
    offset: usize,
    id: u64,
    prop_key: &str,
) -> Result<Option<PropValue>, EngineError> {
    let label_count = read_u8_at(data, offset)? as usize;
    if label_count == 0 || label_count > MAX_NODE_LABELS_PER_NODE {
        return Err(EngineError::CorruptRecord(format!(
            "node {} targeted props decode has invalid label count {}",
            id, label_count
        )));
    }
    let key_len_offset = offset
        .checked_add(1 + label_count * 4)
        .ok_or_else(|| EngineError::CorruptRecord("node property key offset overflow".into()))?;
    let key_len = read_u16_at(data, key_len_offset)? as usize;
    let key_start = key_len_offset + 2;
    let _key_bytes = read_bytes_at(data, key_start, key_len)?;

    let pos = key_start + key_len;
    let props_len = read_u32_at(data, pos + 20)? as usize;
    let props_bytes = read_bytes_at(data, pos + 24, props_len)?;
    let mut deserializer = rmp_serde::Deserializer::from_read_ref(props_bytes);
    PropLookupSeed { target: prop_key }
        .deserialize(&mut deserializer)
        .map_err(|error| {
            EngineError::CorruptRecord(format!(
                "node {} targeted props decode at offset {}: {}",
                id,
                pos + 24,
                error
            ))
        })
}

type DecodedNodeSelectedFields = (Option<String>, BTreeMap<String, PropValue>, Option<i64>);

fn decode_node_selected_fields_at(
    data: &[u8],
    offset: usize,
    id: u64,
    needs: &NodeSelectedFieldNeeds,
) -> Result<DecodedNodeSelectedFields, EngineError> {
    let label_count = read_u8_at(data, offset)? as usize;
    if label_count == 0 || label_count > MAX_NODE_LABELS_PER_NODE {
        return Err(EngineError::CorruptRecord(format!(
            "node {} selected-field decode has invalid label count {}",
            id, label_count
        )));
    }
    let key_len_offset = offset
        .checked_add(1 + label_count * 4)
        .ok_or_else(|| EngineError::CorruptRecord("node selected key offset overflow".into()))?;
    let key_len = read_u16_at(data, key_len_offset)? as usize;
    let key_start = key_len_offset
        .checked_add(2)
        .ok_or_else(|| EngineError::CorruptRecord("node selected key offset overflow".into()))?;
    let key_end = key_start
        .checked_add(key_len)
        .ok_or_else(|| EngineError::CorruptRecord("node selected key offset overflow".into()))?;

    let key = if needs.key {
        let key_bytes = read_bytes_at(data, key_start, key_len)?;
        Some(
            std::str::from_utf8(key_bytes)
                .map_err(|_| {
                    EngineError::CorruptRecord(format!(
                        "invalid UTF-8 in node key at offset {}",
                        key_start
                    ))
                })?
                .to_string(),
        )
    } else {
        None
    };

    let created_at = if needs.created_at {
        Some(read_i64_at(data, key_end)?)
    } else {
        None
    };

    let props_len_offset = key_end
        .checked_add(20)
        .ok_or_else(|| EngineError::CorruptRecord("node props length offset overflow".into()))?;
    let props_offset = key_end
        .checked_add(24)
        .ok_or_else(|| EngineError::CorruptRecord("node props offset overflow".into()))?;
    let props = decode_selected_props_at(
        data,
        props_len_offset,
        props_offset,
        id,
        "node",
        &needs.props,
    )?;

    Ok((key, props, created_at))
}

fn decode_selected_props_at(
    data: &[u8],
    props_len_offset: usize,
    props_offset: usize,
    id: u64,
    kind: &str,
    selection: &PropertySelection,
) -> Result<BTreeMap<String, PropValue>, EngineError> {
    match selection {
        PropertySelection::None => Ok(BTreeMap::new()),
        PropertySelection::Keys(prop_keys) if prop_keys.is_empty() => Ok(BTreeMap::new()),
        PropertySelection::Keys(prop_keys) => {
            let props_len = read_u32_at(data, props_len_offset)? as usize;
            let props_bytes = read_bytes_at(data, props_offset, props_len)?;
            let mut deserializer = rmp_serde::Deserializer::from_read_ref(props_bytes);
            PropProjectionSeed { targets: prop_keys }
                .deserialize(&mut deserializer)
                .map_err(|error| {
                    EngineError::CorruptRecord(format!(
                        "{kind} {} projected props decode at offset {}: {}",
                        id, props_offset, error
                    ))
                })
        }
        PropertySelection::All => {
            let props_len = read_u32_at(data, props_len_offset)? as usize;
            let props_bytes = read_bytes_at(data, props_offset, props_len)?;
            rmp_serde::from_slice(props_bytes).map_err(|error| {
                EngineError::CorruptRecord(format!(
                    "{kind} {} full props decode at offset {}: {}",
                    id, props_offset, error
                ))
            })
        }
    }
}

fn decode_edge_property_at(
    data: &[u8],
    offset: usize,
    id: u64,
    prop_key: &str,
) -> Result<Option<PropValue>, EngineError> {
    let props_len = read_u32_at(data, offset + 56)? as usize;
    let props_bytes = read_bytes_at(data, offset + 60, props_len)?;
    let mut deserializer = rmp_serde::Deserializer::from_read_ref(props_bytes);
    PropLookupSeed { target: prop_key }
        .deserialize(&mut deserializer)
        .map_err(|error| {
            EngineError::CorruptRecord(format!(
                "edge {} targeted props decode at offset {}: {}",
                id,
                offset + 60,
                error
            ))
        })
}

fn decode_edge_selected_fields_at(
    data: &[u8],
    offset: usize,
    id: u64,
    needs: &EdgeSelectedFieldNeeds,
) -> Result<(BTreeMap<String, PropValue>, Option<i64>), EngineError> {
    let created_at = if needs.created_at {
        Some(read_i64_at(data, offset + 20)?)
    } else {
        None
    };
    let props = decode_selected_props_at(data, offset + 56, offset + 60, id, "edge", &needs.props)?;
    Ok((props, created_at))
}

fn validate_node_vector_sidecars(
    segment_id: u64,
    vector_meta: &[u8],
    dense_blob: &[u8],
    sparse_blob: &[u8],
    expected_count: u64,
) -> Result<NodeVectorSidecarSummary, EngineError> {
    if vector_meta.is_empty() {
        if !dense_blob.is_empty() || !sparse_blob.is_empty() {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} has vector blobs without node vector metadata",
                segment_id
            )));
        }
        return Ok(NodeVectorSidecarSummary {
            dense_count: 0,
            sparse_count: 0,
        });
    }

    if vector_meta.len() < 8 {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} node vector metadata too short: {} bytes",
            segment_id,
            vector_meta.len()
        )));
    }

    let count = read_u64_at(vector_meta, 0)?;
    if count != expected_count {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} node vector metadata count {} does not match node metadata count {}",
            segment_id, count, expected_count
        )));
    }

    let count = usize::try_from(count).map_err(|_| {
        EngineError::CorruptRecord(format!(
            "segment {} node vector metadata count exceeds addressable memory",
            segment_id
        ))
    })?;
    let index_bytes = count
        .checked_mul(NODE_VECTOR_META_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("node vector metadata size overflow".into()))?;
    let expected_len = 8usize
        .checked_add(index_bytes)
        .ok_or_else(|| EngineError::CorruptRecord("node vector metadata size overflow".into()))?;
    if vector_meta.len() != expected_len {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} node vector metadata size {} does not match expected {}",
            segment_id,
            vector_meta.len(),
            expected_len
        )));
    }

    let mut has_dense = false;
    let mut has_sparse = false;
    let mut next_dense_offset = 0usize;
    let mut next_sparse_offset = 0usize;
    let mut dense_count = 0usize;
    let mut sparse_count = 0usize;

    for index in 0..count {
        let (flags, dense_offset, dense_len, sparse_offset, sparse_len) =
            read_node_vector_meta_entry(vector_meta, index)?;
        if flags & !(NODE_VECTOR_FLAG_DENSE | NODE_VECTOR_FLAG_SPARSE) != 0 {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} node vector entry {} has invalid flags {:#010b}",
                segment_id, index, flags
            )));
        }

        if flags & NODE_VECTOR_FLAG_DENSE == 0 {
            if dense_offset != 0 || dense_len != 0 {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} has dense payload without dense flag",
                    segment_id, index
                )));
            }
        } else {
            has_dense = true;
            dense_count += 1;
            let dense_offset = usize::try_from(dense_offset).map_err(|_| {
                EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} dense offset exceeds addressable memory",
                    segment_id, index
                ))
            })?;
            let dense_len = usize::try_from(dense_len).map_err(|_| {
                EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} dense length exceeds addressable memory",
                    segment_id, index
                ))
            })?;
            let dense_bytes = dense_len
                .checked_mul(DENSE_VECTOR_VALUE_SIZE)
                .ok_or_else(|| EngineError::CorruptRecord("dense blob size overflow".into()))?;
            if dense_offset != next_dense_offset {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} dense offset {} does not match expected {}",
                    segment_id, index, dense_offset, next_dense_offset
                )));
            }
            validate_blob_range(
                dense_blob,
                dense_offset as u64,
                dense_bytes,
                "dense",
                segment_id,
                index,
            )?;
            next_dense_offset = next_dense_offset
                .checked_add(dense_bytes)
                .ok_or_else(|| EngineError::CorruptRecord("dense blob size overflow".into()))?;
        }

        if flags & NODE_VECTOR_FLAG_SPARSE == 0 {
            if sparse_offset != 0 || sparse_len != 0 {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} has sparse payload without sparse flag",
                    segment_id, index
                )));
            }
        } else {
            has_sparse = true;
            sparse_count += 1;
            let sparse_offset = usize::try_from(sparse_offset).map_err(|_| {
                EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} sparse offset exceeds addressable memory",
                    segment_id, index
                ))
            })?;
            let sparse_len = usize::try_from(sparse_len).map_err(|_| {
                EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} sparse length exceeds addressable memory",
                    segment_id, index
                ))
            })?;
            let sparse_bytes = sparse_len
                .checked_mul(SPARSE_VECTOR_ENTRY_SIZE)
                .ok_or_else(|| EngineError::CorruptRecord("sparse blob size overflow".into()))?;
            if sparse_offset != next_sparse_offset {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} sparse offset {} does not match expected {}",
                    segment_id, index, sparse_offset, next_sparse_offset
                )));
            }
            validate_blob_range(
                sparse_blob,
                sparse_offset as u64,
                sparse_bytes,
                "sparse",
                segment_id,
                index,
            )?;
            next_sparse_offset = next_sparse_offset
                .checked_add(sparse_bytes)
                .ok_or_else(|| EngineError::CorruptRecord("sparse blob size overflow".into()))?;
        }
    }

    if has_dense && dense_blob.is_empty() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} references dense vectors but dense blob is missing",
            segment_id
        )));
    }
    if has_sparse && sparse_blob.is_empty() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} references sparse vectors but sparse blob is missing",
            segment_id
        )));
    }
    if !has_dense && !dense_blob.is_empty() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} has orphaned dense vector blob",
            segment_id
        )));
    }
    if !has_sparse && !sparse_blob.is_empty() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} has orphaned sparse vector blob",
            segment_id
        )));
    }
    if has_dense && next_dense_offset != dense_blob.len() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} dense vector blob has trailing or unreferenced bytes: expected {}, got {}",
            segment_id,
            next_dense_offset,
            dense_blob.len()
        )));
    }
    if has_sparse && next_sparse_offset != sparse_blob.len() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} sparse vector blob has trailing or unreferenced bytes: expected {}, got {}",
            segment_id,
            next_sparse_offset,
            sparse_blob.len()
        )));
    }

    Ok(NodeVectorSidecarSummary {
        dense_count,
        sparse_count,
    })
}

struct NodeVectorSidecarSummary {
    dense_count: usize,
    sparse_count: usize,
}

fn validate_blob_range(
    blob: &[u8],
    offset: u64,
    len: usize,
    kind: &str,
    segment_id: u64,
    index: usize,
) -> Result<(), EngineError> {
    let base = offset as usize;
    let end = base
        .checked_add(len)
        .ok_or_else(|| EngineError::CorruptRecord(format!("{kind} vector range overflow")))?;
    if end > blob.len() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} node vector entry {} {} range [{}, {}) exceeds blob length {}",
            segment_id,
            index,
            kind,
            base,
            end,
            blob.len()
        )));
    }
    Ok(())
}

fn read_node_vector_meta_entry(
    data: &[u8],
    index: usize,
) -> Result<(u8, u64, u32, u64, u32), EngineError> {
    let off = 8 + index * NODE_VECTOR_META_ENTRY_SIZE;
    let flags = read_u8_at(data, off)?;
    let dense_offset = read_u64_at(data, off + 4)?;
    let dense_len = read_u32_at(data, off + 12)?;
    let sparse_offset = read_u64_at(data, off + 16)?;
    let sparse_len = read_u32_at(data, off + 24)?;
    Ok((flags, dense_offset, dense_len, sparse_offset, sparse_len))
}

fn read_dense_scoring_meta(
    node_meta: &[u8],
    layout: NodeMetaLayout,
    vector_meta: &[u8],
    index: usize,
) -> Result<DenseScoringMeta, EngineError> {
    let node_entry = read_node_meta_entry_at(node_meta, layout, index)?;

    let vector_off = 8 + index * NODE_VECTOR_META_ENTRY_SIZE;
    let vector_end = vector_off
        .checked_add(NODE_VECTOR_META_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("node vector meta offset overflow".into()))?;
    let vector_entry = vector_meta.get(vector_off..vector_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "node vector meta read at index {} exceeds data length {}",
            index,
            vector_meta.len()
        ))
    })?;

    Ok(DenseScoringMeta {
        label_ids: node_entry.label_ids,
        updated_at: node_entry.updated_at,
        weight: node_entry.weight,
        dense_offset: u64::from_le_bytes(vector_entry[4..12].try_into().unwrap()) as usize,
        dense_len: u32::from_le_bytes(vector_entry[12..16].try_into().unwrap()) as usize,
    })
}

fn read_sparse_scoring_meta(
    node_meta: &[u8],
    layout: NodeMetaLayout,
    vector_meta: &[u8],
    index: usize,
) -> Result<SparseScoringMeta, EngineError> {
    let node_entry = read_node_meta_entry_at(node_meta, layout, index)?;

    let vector_off = 8 + index * NODE_VECTOR_META_ENTRY_SIZE;
    let vector_end = vector_off
        .checked_add(NODE_VECTOR_META_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("node vector meta offset overflow".into()))?;
    let vector_entry = vector_meta.get(vector_off..vector_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "node vector meta read at index {} exceeds data length {}",
            index,
            vector_meta.len()
        ))
    })?;

    Ok(SparseScoringMeta {
        label_ids: node_entry.label_ids,
        updated_at: node_entry.updated_at,
        weight: node_entry.weight,
        sparse_offset: u64::from_le_bytes(vector_entry[16..24].try_into().unwrap()) as usize,
        sparse_len: u32::from_le_bytes(vector_entry[24..28].try_into().unwrap()) as usize,
    })
}

/// Decode a NodeRecord from mmap data at a given byte offset.
/// The ID is passed separately; it comes from the index, not the data section.
/// Layout: label_count(1) label_id(4)*count key_len(2) key(N) created_at(8) updated_at(8) weight(4) props_len(4) props(M)
fn decode_node_at(data: &[u8], offset: usize, id: u64) -> Result<NodeRecord, EngineError> {
    decode_node_at_with_end(data, offset, id).map(|(node, _)| node)
}

fn decode_node_at_with_end(
    data: &[u8],
    offset: usize,
    id: u64,
) -> Result<(NodeRecord, usize), EngineError> {
    let label_count = read_u8_at(data, offset)? as usize;
    if label_count == 0 || label_count > MAX_NODE_LABELS_PER_NODE {
        return Err(EngineError::CorruptRecord(format!(
            "node record {} has invalid label count {}",
            id, label_count
        )));
    }
    let mut label_ids = [0u32; MAX_NODE_LABELS_PER_NODE];
    for label_index in 0..label_count {
        label_ids[label_index] = read_u32_at(data, offset + 1 + label_index * 4)?;
        if label_index > 0 && label_ids[label_index - 1] >= label_ids[label_index] {
            return Err(EngineError::CorruptRecord(format!(
                "node record {} label IDs must be sorted ascending and unique",
                id
            )));
        }
    }
    let label_ids = NodeLabelSet::from_canonical_ids(&label_ids[..label_count]).map_err(|err| {
        EngineError::CorruptRecord(format!("invalid label set on node record {}: {err}", id))
    })?;

    let key_len_offset = offset + 1 + label_count * 4;
    let key_len = read_u16_at(data, key_len_offset)? as usize;
    let key_start = key_len_offset + 2;
    let key_bytes = read_bytes_at(data, key_start, key_len)?;
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| {
            EngineError::CorruptRecord(format!("invalid UTF-8 in node key at offset {}", key_start))
        })?
        .to_string();

    let pos = key_start + key_len;
    let created_at = read_i64_at(data, pos)?;
    let updated_at = read_i64_at(data, pos + 8)?;
    let weight = read_f32_at(data, pos + 16)?;
    let props_len = read_u32_at(data, pos + 20)? as usize;
    let props_bytes = read_bytes_at(data, pos + 24, props_len)?;
    let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(props_bytes).map_err(|e| {
        EngineError::CorruptRecord(format!("node props decode at offset {}: {}", pos + 24, e))
    })?;
    let end = pos
        .checked_add(24)
        .and_then(|base| base.checked_add(props_len))
        .ok_or_else(|| EngineError::CorruptRecord("node record end offset overflow".into()))?;

    Ok((
        NodeRecord {
            id,
            label_ids,
            key,
            props,
            created_at,
            updated_at,
            weight,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        },
        end,
    ))
}

/// Decode an EdgeRecord from mmap data at a given byte offset.
/// The ID is passed separately; it comes from the index, not the data section.
/// Layout: from(8) to(8) label_id(4) created_at(8) updated_at(8) weight(4) valid_from(8) valid_to(8) props_len(4) props(N)
fn decode_edge_at(data: &[u8], offset: usize, id: u64) -> Result<EdgeRecord, EngineError> {
    let from = read_u64_at(data, offset)?;
    let to = read_u64_at(data, offset + 8)?;
    let label_id = read_u32_at(data, offset + 16)?;
    let created_at = read_i64_at(data, offset + 20)?;
    let updated_at = read_i64_at(data, offset + 28)?;
    let weight = read_f32_at(data, offset + 36)?;
    let valid_from = read_i64_at(data, offset + 40)?;
    let valid_to = read_i64_at(data, offset + 48)?;

    let props_len = read_u32_at(data, offset + 56)? as usize;
    let props_bytes = read_bytes_at(data, offset + 60, props_len)?;
    let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(props_bytes).map_err(|e| {
        EngineError::CorruptRecord(format!(
            "edge props decode at offset {}: {}",
            offset + 60,
            e
        ))
    })?;

    Ok(EdgeRecord {
        id,
        from,
        to,
        label_id,
        props,
        created_at,
        updated_at,
        weight,
        valid_from,
        valid_to,
        last_write_seq: 0,
    })
}

fn load_tombstones_from_bytes(
    data: &[u8],
) -> Result<(NodeIdMap<TombstoneEntry>, NodeIdMap<TombstoneEntry>), EngineError> {
    if data.len() < 8 {
        return Ok((NodeIdMap::default(), NodeIdMap::default()));
    }
    let count = read_u64_at(data, 0)? as usize;

    let mut deleted_nodes = NodeIdMap::default();
    let mut deleted_edges = NodeIdMap::default();

    for i in 0..count {
        let off = 8 + i * TOMBSTONE_ENTRY_SIZE;
        if off + TOMBSTONE_ENTRY_SIZE > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "tombstone entry {} at offset {} exceeds file length {}",
                i,
                off,
                data.len()
            )));
        }
        let kind = data[off];
        let id = read_u64_at(data, off + 1)?;
        let deleted_at = read_i64_at(data, off + 9)?;
        let last_write_seq = read_u64_at(data, off + 17)?;
        let entry = TombstoneEntry {
            deleted_at,
            last_write_seq,
        };
        match kind {
            0 => {
                deleted_nodes.insert(id, entry);
            }
            1 => {
                deleted_edges.insert(id, entry);
            }
            _ => {} // Unknown kind, skip
        }
    }

    Ok((deleted_nodes, deleted_edges))
}

/// Compute sparse dot product between a sorted query and a sparse vector
/// stored in the segment blob at `offset` with `entry_count` entries.
/// Both query and blob entries are sorted by dimension_id; uses merge-walk.
fn sparse_dot_score_from_blob(
    query: &[(u32, f32)],
    sparse_blob: &[u8],
    offset: usize,
    entry_count: usize,
) -> Result<f32, EngineError> {
    let mut score = 0.0f32;
    let mut qi = 0usize;
    let mut vi = 0usize;
    while qi < query.len() && vi < entry_count {
        let entry_off = offset + vi * SPARSE_VECTOR_ENTRY_SIZE;
        let dim_id = read_u32_at(sparse_blob, entry_off)?;
        let weight = read_f32_at(sparse_blob, entry_off + 4)?;
        match query[qi].0.cmp(&dim_id) {
            std::cmp::Ordering::Less => qi += 1,
            std::cmp::Ordering::Greater => vi += 1,
            std::cmp::Ordering::Equal => {
                score += query[qi].1 * weight;
                qi += 1;
                vi += 1;
            }
        }
    }
    Ok(score)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::property_value_semantics::numeric_range_sort_key_for_value;
    use crate::secondary_index_key::{
        compound_prefix_bounds, encode_compound_tuple_key, encode_compound_tuple_prefix,
        CompoundFieldValue, CompoundTupleContext,
    };
    use crate::segment_writer::{
        node_compound_eq_sidecar_path, publish_compound_sidecar_component,
        write_segment_without_degree_sidecar_for_test as write_segment,
        write_segment_without_degree_sidecar_with_secondary_indexes_for_test as write_segment_with_secondary_indexes,
    };

    /// Test-only wrapper to expose read_varint_at for cross-module varint tests.
    pub fn read_varint_at_pub(data: &[u8], offset: usize) -> (u64, usize) {
        read_varint_at(data, offset).unwrap()
    }

    fn write_varint_for_test(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn read_payload_file(path: &std::path::Path) -> Vec<u8> {
        let data = std::fs::read(path).unwrap();
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

    fn rewrite_payload_file(path: &std::path::Path, rewrite: impl FnOnce(&mut [u8])) {
        use std::io::{Seek, SeekFrom, Write};

        let data = std::fs::read(path).unwrap();
        let range = if data.len() >= crate::segment_components::COMPONENT_IDENTITY_HEADER_LEN
            && data[0..crate::segment_components::COMPONENT_IDENTITY_HEADER_MAGIC.len()]
                == crate::segment_components::COMPONENT_IDENTITY_HEADER_MAGIC
        {
            let header = crate::segment_components::decode_identity_header(&data).unwrap();
            let start = header.payload_offset as usize;
            let end = start + header.payload_len as usize;
            start..end
        } else {
            0..data.len()
        };
        let mut payload = data[range.clone()].to_vec();
        rewrite(&mut payload);

        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(range.start as u64)).unwrap();
        file.write_all(&payload).unwrap();
        file.sync_all().unwrap();
    }

    fn tamper_envelope_format_version(seg_dir: &std::path::Path, version: u32) {
        use std::io::{Seek, SeekFrom, Write};

        let path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(12)).unwrap();
        file.write_all(&version.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    fn read_segment_manifest_for_test(seg_dir: &std::path::Path) -> SegmentComponentManifestV1 {
        let data = std::fs::read(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();
        crate::segment_components::decode_manifest_envelope(&data).unwrap()
    }

    fn write_segment_manifest_for_test(
        seg_dir: &std::path::Path,
        manifest: &SegmentComponentManifestV1,
    ) {
        let data = crate::segment_components::encode_manifest_envelope(manifest).unwrap();
        std::fs::write(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME), data).unwrap();
    }

    fn manifest_component_path_for_test(
        seg_dir: &std::path::Path,
        kind: SegmentComponentKind,
    ) -> std::path::PathBuf {
        let manifest = read_segment_manifest_for_test(seg_dir);
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .expect("component must be present in test manifest");
        match &record.handle {
            ComponentHandleV1::ExternalFile { relative_path, .. } => seg_dir.join(relative_path),
            ComponentHandleV1::PackedRange { .. } => {
                panic!("test component unexpectedly used a packed handle")
            }
        }
    }

    fn node_compound_entry_for_test(index_id: u64) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "score".to_string(),
                    },
                ],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn node_compound_key_for_test(
        entry: &SecondaryIndexManifestEntry,
        tenant: &str,
        score: i64,
    ) -> Vec<u8> {
        let context = CompoundTupleContext::from_manifest_entry(entry).unwrap();
        encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String(tenant.to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(score))),
            ],
        )
        .unwrap()
    }

    fn rewrite_component_payload_for_test(
        seg_dir: &std::path::Path,
        kind: SegmentComponentKind,
        rewrite: impl FnOnce(&mut [u8]),
    ) {
        use std::io::{Seek, SeekFrom, Write};

        let manifest = read_segment_manifest_for_test(seg_dir);
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .expect("component must be present in test manifest");
        match &record.handle {
            ComponentHandleV1::ExternalFile { relative_path, .. } => {
                let path = seg_dir.join(relative_path);
                let data = std::fs::read(&path).unwrap();
                let header = crate::segment_components::decode_identity_header(&data).unwrap();
                let start = header.payload_offset as usize;
                let end = start + header.payload_len as usize;
                let mut payload = data[start..end].to_vec();
                rewrite(&mut payload);

                let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::Start(start as u64)).unwrap();
                file.write_all(&payload).unwrap();
                file.sync_all().unwrap();
            }
            ComponentHandleV1::PackedRange { offset, len, .. } => {
                let path = seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME);
                let data = std::fs::read(&path).unwrap();
                let header = crate::segment_components::decode_identity_header(&data).unwrap();
                let start = header.payload_offset as usize + *offset as usize;
                let end = start + *len as usize;
                let mut payload = data[start..end].to_vec();
                rewrite(&mut payload);

                let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::Start(start as u64)).unwrap();
                file.write_all(&payload).unwrap();
                file.sync_all().unwrap();
            }
        }
    }

    fn component_payload_bytes_for_test(
        seg_dir: &std::path::Path,
        kind: SegmentComponentKind,
    ) -> Vec<u8> {
        let manifest = read_segment_manifest_for_test(seg_dir);
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .expect("component must be present in test manifest");
        match &record.handle {
            ComponentHandleV1::ExternalFile { relative_path, .. } => {
                let data = std::fs::read(seg_dir.join(relative_path)).unwrap();
                let header = crate::segment_components::decode_identity_header(&data).unwrap();
                let start = header.payload_offset as usize;
                let end = start + header.payload_len as usize;
                data[start..end].to_vec()
            }
            ComponentHandleV1::PackedRange { offset, len, .. } => {
                let data =
                    std::fs::read(seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME))
                        .unwrap();
                let header = crate::segment_components::decode_identity_header(&data).unwrap();
                let start = header.payload_offset as usize + *offset as usize;
                let end = start + *len as usize;
                data[start..end].to_vec()
            }
        }
    }

    fn packed_range_for_test(
        manifest: &SegmentComponentManifestV1,
        kind: SegmentComponentKind,
    ) -> (u64, u64) {
        let record = manifest
            .components
            .iter()
            .find(|record| record.kind == kind)
            .expect("component must be present in test manifest");
        let ComponentHandleV1::PackedRange { offset, len, .. } = &record.handle else {
            panic!("component must be packed in test manifest");
        };
        (*offset, *len)
    }

    fn write_u64_at_for_test(data: &mut [u8], offset: usize, value: u64) {
        data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn write_numeric_key_at_for_test(data: &mut [u8], offset: usize, value: PropValue) {
        let key = numeric_range_sort_key_for_value(&value)
            .expect("test numeric value must encode as range key");
        data[offset..offset + NUMERIC_RANGE_KEY_BYTES].copy_from_slice(&key.as_bytes());
    }

    fn raw_nonzero_numeric_range_key_for_test(
        class: u8,
        rank: i32,
        fraction: u128,
    ) -> [u8; NUMERIC_RANGE_KEY_BYTES] {
        let mut bytes = [0_u8; NUMERIC_RANGE_KEY_BYTES];
        bytes[0] = class;
        let mut rank_bits = (rank as u32) ^ 0x8000_0000;
        let mut fraction_bits = fraction;
        if class == 0 {
            rank_bits = !rank_bits;
            fraction_bits = !fraction_bits;
        }
        bytes[1..5].copy_from_slice(&rank_bits.to_be_bytes());
        bytes[5..21].copy_from_slice(&fraction_bits.to_be_bytes());
        bytes
    }

    fn reopen_test_segment_with_index(
        seg_dir: &std::path::Path,
        entry: &SecondaryIndexManifestEntry,
    ) -> SegmentReader {
        let manifest = read_segment_manifest_for_test(seg_dir);
        let info = segment_info_from_manifest(&manifest);
        SegmentReader::open_with_info(seg_dir, &info, None, std::slice::from_ref(entry)).unwrap()
    }

    fn segment_info_from_manifest(manifest: &SegmentComponentManifestV1) -> SegmentInfo {
        SegmentInfo {
            id: manifest.segment_id,
            node_count: manifest.node_count,
            edge_count: manifest.edge_count,
            segment_format_version: manifest.segment_format_version,
            segment_data_id: manifest.segment_data_id,
        }
    }

    fn expect_engine_error<T>(result: Result<T, EngineError>) -> String {
        match result {
            Ok(_) => panic!("expected EngineError"),
            Err(error) => error.to_string(),
        }
    }

    fn write_segment_with_info(
        mt: &Memtable,
        dense_config: Option<&DenseVectorConfig>,
    ) -> (tempfile::TempDir, std::path::PathBuf, SegmentInfo) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let info = write_segment(&seg_dir, 1, mt, dense_config).unwrap();
        (dir, seg_dir, info)
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

    fn make_node_with_labels(id: u64, label_ids: &[u32], key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::from_canonical_ids(label_ids).unwrap(),
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_node_with_labels_and_props(
        id: u64,
        label_ids: &[u32],
        key: &str,
        props: BTreeMap<String, PropValue>,
        updated_at: i64,
    ) -> NodeRecord {
        NodeRecord {
            props,
            ..make_node_with_labels(id, label_ids, key, updated_at)
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
            updated_at: 2000,
            weight: 0.75,
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

    /// Helper: build a memtable, write segment, open reader
    fn write_and_open(mt: &Memtable) -> (tempfile::TempDir, SegmentReader) {
        write_and_open_with_dense_config(mt, None)
    }

    fn write_and_open_with_dense_config(
        mt: &Memtable,
        dense_config: Option<&DenseVectorConfig>,
    ) -> (tempfile::TempDir, SegmentReader) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        write_segment(&seg_dir, 1, mt, dense_config).unwrap();
        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, dense_config).unwrap();
        (dir, reader)
    }

    #[test]
    fn compound_sidecar_component_reports_missing_available_and_corrupt() {
        let entry = node_compound_entry_for_test(700);
        let mt = Memtable::new();
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();

        let missing =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        assert!(!missing.validate_compound_sidecar_for_entry(&entry).unwrap());
        assert!(matches!(
            missing.optional_component_availability_for_test(
                SegmentComponentKind::NodeCompoundEqualityIndex {
                    index_id: entry.index_id,
                },
            ),
            ComponentAvailability::Missing
        ));

        let key_one = node_compound_key_for_test(&entry, "acme", 1);
        let key_two = node_compound_key_for_test(&entry, "acme", 2);
        publish_compound_sidecar_component(
            &seg_dir,
            &entry,
            &[
                (key_two, 9),
                (key_one, 3),
                (node_compound_key_for_test(&entry, "beta", 1), 11),
            ],
        )
        .unwrap();

        let available =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        assert!(available
            .validate_compound_sidecar_for_entry(&entry)
            .unwrap());
        assert!(matches!(
            available.optional_component_availability_for_test(
                SegmentComponentKind::NodeCompoundEqualityIndex {
                    index_id: entry.index_id,
                },
            ),
            ComponentAvailability::Available
        ));
        let context = CompoundTupleContext::from_manifest_entry(&entry).unwrap();
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        assert_eq!(
            available
                .compound_prefix_candidates_if_present(&entry, &compound_prefix_bounds(&prefix))
                .unwrap(),
            Some(vec![3, 9])
        );

        let path = node_compound_eq_sidecar_path(&seg_dir, entry.index_id);
        rewrite_payload_file(&path, |payload| {
            let last = payload.last_mut().unwrap();
            *last ^= 0x01;
        });
        let corrupt =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        let error = corrupt
            .validate_compound_sidecar_for_entry(&entry)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("compound secondary index unavailable: corrupt sidecar"));
        assert!(matches!(
            corrupt.optional_component_availability_for_test(
                SegmentComponentKind::NodeCompoundEqualityIndex {
                    index_id: entry.index_id,
                },
            ),
            ComponentAvailability::CorruptIdentity { .. }
        ));
    }

    #[test]
    fn test_planner_stats_valid_sidecar_is_available() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 7, "alice")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 7, "bob")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 3)), 3);

        let (_dir, reader) = write_and_open(&mt);
        let stats = reader.planner_stats().expect("planner stats should load");
        assert_eq!(stats.segment_id, 1);
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
        assert!(stats.general_property_stats_complete);
        assert_eq!(stats.general_property_sampled_node_count, 2);
        assert_eq!(stats.node_id_sample, vec![1, 2]);
        assert!(reader.planner_stats_available());
    }

    #[test]
    fn test_planner_stats_missing_or_corrupt_sidecar_does_not_fail_open() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);

        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        write_segment(&seg_dir, 1, &mt, None).unwrap();
        std::fs::remove_file(seg_dir.join(crate::planner_stats::PLANNER_STATS_FILENAME)).unwrap();
        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(matches!(
            reader.planner_stats_debug_snapshot_for_test(),
            PlannerStatsAvailability::Missing
        ));
        assert_eq!(
            reader.optional_component_availability_for_test(SegmentComponentKind::PlannerStats),
            ComponentAvailability::Missing
        );
        assert!(reader.get_node(1).unwrap().is_some());

        crate::segment_writer::publish_planner_stats_component_payload(
            &seg_dir,
            &[],
            b"not planner stats",
        )
        .unwrap();
        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(matches!(
            reader.planner_stats_debug_snapshot_for_test(),
            PlannerStatsAvailability::Unavailable { .. }
        ));
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::PlannerStats),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert!(reader.get_node(1).unwrap().is_some());
    }

    #[test]
    fn test_open_rejects_zero_byte_required_manifest_component() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        std::fs::write(
            seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME),
            [],
        )
        .unwrap();

        let err = expect_engine_error(SegmentReader::open_unpinned_for_test(&seg_dir, 1, None));
        assert!(
            err.contains("identity header")
                || err.contains("does not match file length")
                || err.contains("component file is empty"),
            "got: {err}"
        );
    }

    #[test]
    fn test_zero_byte_optional_manifest_component_is_unavailable() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        let stats_path =
            manifest_component_path_for_test(&seg_dir, SegmentComponentKind::PlannerStats);
        std::fs::write(stats_path, []).unwrap();

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(matches!(
            reader.planner_stats_debug_snapshot_for_test(),
            PlannerStatsAvailability::Unavailable { .. }
        ));
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::PlannerStats),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert!(reader.get_node(1).unwrap().is_some());
    }

    #[test]
    fn test_planner_stats_identity_ignores_unrepresented_building_indexes() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert(
            "status".to_string(),
            PropValue::String("active".to_string()),
        );
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "alice".to_string(),
                props,
                created_at: 1000,
                updated_at: 1001,
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            1,
        );
        let building = SecondaryIndexManifestEntry {
            index_id: 101,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "status".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let ready = SecondaryIndexManifestEntry {
            index_id: 102,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "queued".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&building);
        mt.register_secondary_index(&ready);

        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let indexes = vec![ready, building];
        let info = crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            &indexes,
        )
        .unwrap();
        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();
        assert!(
            reader.planner_stats_available(),
            "{:?}",
            reader.planner_stats_debug_snapshot_for_test()
        );
        assert_eq!(
            reader.optional_component_availability_for_test(SegmentComponentKind::PlannerStats),
            ComponentAvailability::Available
        );
    }

    fn write_and_open_with_secondary_eq_sidecar(
        mt: &Memtable,
        entry: &SecondaryIndexManifestEntry,
    ) -> (tempfile::TempDir, SegmentReader) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = mt.clone();
        mt.register_secondary_index(entry);
        let info = crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(entry),
        )
        .unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(entry))
                .unwrap();
        (dir, reader)
    }

    fn write_and_open_with_secondary_range_sidecar(
        mt: &Memtable,
        entry: &SecondaryIndexManifestEntry,
    ) -> (tempfile::TempDir, SegmentReader) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = mt.clone();
        mt.register_secondary_index(entry);
        let info = crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(entry),
        )
        .unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(entry))
                .unwrap();
        (dir, reader)
    }

    #[test]
    fn test_runtime_coverage_cache_tracks_equality_sidecar_states() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 91,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (_dir, reader) = write_and_open_with_secondary_eq_sidecar(&mt, &entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Equality
            ),
            DeclaredIndexRuntimeCoverageState::Unknown
        );

        reader.warm_declared_index_runtime_coverage(&entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Equality
            ),
            DeclaredIndexRuntimeCoverageState::Available
        );

        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = mt.clone();
        mt.register_secondary_index(&entry);
        let info = crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&entry),
        )
        .unwrap();
        let sidecar_path = manifest_component_path_for_test(
            &seg_dir,
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: entry.index_id,
            },
        );
        std::fs::remove_file(&sidecar_path).unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        reader.warm_declared_index_runtime_coverage(&entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Equality
            ),
            DeclaredIndexRuntimeCoverageState::Missing
        );

        crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&entry),
        )
        .unwrap();
        let sidecar_path = manifest_component_path_for_test(
            &seg_dir,
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: entry.index_id,
            },
        );
        std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        reader.warm_declared_index_runtime_coverage(&entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Equality
            ),
            DeclaredIndexRuntimeCoverageState::Corrupt
        );
    }

    #[test]
    fn test_runtime_coverage_cache_tracks_range_sidecar_states() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(10));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 92,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (_dir, reader) = write_and_open_with_secondary_range_sidecar(&mt, &entry);

        reader.warm_declared_index_runtime_coverage(&entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Range
            ),
            DeclaredIndexRuntimeCoverageState::Available
        );

        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = mt.clone();
        mt.register_secondary_index(&entry);
        let info = crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&entry),
        )
        .unwrap();
        let sidecar_path = manifest_component_path_for_test(
            &seg_dir,
            SegmentComponentKind::NodePropertyRangeIndex {
                index_id: entry.index_id,
            },
        );
        std::fs::remove_file(&sidecar_path).unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        reader.warm_declared_index_runtime_coverage(&entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Range
            ),
            DeclaredIndexRuntimeCoverageState::Missing
        );

        crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
            &seg_dir,
            1,
            &mt,
            None,
            std::slice::from_ref(&entry),
        )
        .unwrap();
        let sidecar_path = manifest_component_path_for_test(
            &seg_dir,
            SegmentComponentKind::NodePropertyRangeIndex {
                index_id: entry.index_id,
            },
        );
        std::fs::write(&sidecar_path, [1u8, 2, 3]).unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, std::slice::from_ref(&entry))
                .unwrap();
        reader.warm_declared_index_runtime_coverage(&entry);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Range
            ),
            DeclaredIndexRuntimeCoverageState::Corrupt
        );
    }

    fn write_sparse_segment(nodes: Vec<NodeRecord>) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        for (seq, node) in nodes.into_iter().enumerate() {
            mt.apply_op(&WalOp::UpsertNode(node), seq as u64);
        }
        write_segment(&seg_dir, 1, &mt, None).unwrap();
        (dir, seg_dir)
    }

    fn dense_config(dimension: u32) -> DenseVectorConfig {
        DenseVectorConfig {
            dimension,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }
    }

    fn build_u64_key_index_with_start(
        keys: &[u64],
        idx_start: usize,
        entry_size: usize,
        key_offset: usize,
    ) -> Vec<u8> {
        let mut data = vec![0u8; idx_start + keys.len() * entry_size];
        data[0..8].copy_from_slice(&(keys.len() as u64).to_le_bytes());
        for (i, key) in keys.iter().enumerate() {
            let off = idx_start + i * entry_size + key_offset;
            data[off..off + 8].copy_from_slice(&key.to_le_bytes());
        }
        data
    }

    fn build_u64_key_index(keys: &[u64], entry_size: usize, key_offset: usize) -> Vec<u8> {
        build_u64_key_index_with_start(keys, 8, entry_size, key_offset)
    }

    #[test]
    fn test_batch_strategy_prefers_seek_for_tiny_key_count() {
        let keys: Vec<u64> = (1..=10_000).collect();
        let idx = build_u64_key_index(&keys, NODE_INDEX_ENTRY_SIZE, 0);
        let strategy =
            choose_batch_read_strategy(&idx, 8, keys.len(), NODE_INDEX_ENTRY_SIZE, 0, 2, 500, 501)
                .unwrap();
        assert_eq!(strategy, BatchReadStrategy::SeekPerKey);
    }

    #[test]
    fn test_batch_strategy_prefers_merge_for_dense_large_range() {
        let keys: Vec<u64> = (1..=10_000).collect();
        let idx = build_u64_key_index(&keys, NODE_INDEX_ENTRY_SIZE, 0);
        let strategy = choose_batch_read_strategy(
            &idx,
            8,
            keys.len(),
            NODE_INDEX_ENTRY_SIZE,
            0,
            256,
            2_000,
            2_255,
        )
        .unwrap();
        assert_eq!(strategy, BatchReadStrategy::MergeWalk);
    }

    #[test]
    fn test_batch_strategy_prefers_seek_for_sparse_range() {
        let keys: Vec<u64> = (1..=10_000).collect();
        let idx = build_u64_key_index(&keys, NODE_INDEX_ENTRY_SIZE, 0);
        let strategy = choose_batch_read_strategy(
            &idx,
            8,
            keys.len(),
            NODE_INDEX_ENTRY_SIZE,
            0,
            64,
            100,
            9_900,
        )
        .unwrap();
        assert_eq!(strategy, BatchReadStrategy::SeekPerKey);
    }

    #[test]
    fn test_batch_strategy_uses_explicit_index_start() {
        let keys: Vec<u64> = (1..=10_000).collect();
        let idx = build_u64_key_index_with_start(&keys, 48, NODE_INDEX_ENTRY_SIZE, 0);
        let strategy = choose_batch_read_strategy(
            &idx,
            48,
            keys.len(),
            NODE_INDEX_ENTRY_SIZE,
            0,
            64,
            100,
            9_900,
        )
        .unwrap();
        assert_eq!(strategy, BatchReadStrategy::SeekPerKey);
    }

    // --- get_node ---

    #[test]
    fn test_get_node_found() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(42, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        let node = reader.get_node(42).unwrap().unwrap();
        assert_eq!(node.id, 42);
        assert_eq!(node.label_ids.as_slice(), [1]);
        assert_eq!(node.key, "alice");
        assert_eq!(node.created_at, 1000);
        assert!((node.weight - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_get_node_not_found() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.get_node(999).unwrap().is_none());
    }

    #[test]
    fn test_get_node_with_properties() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        let node = reader.get_node(1).unwrap().unwrap();
        assert_eq!(
            node.props.get("name"),
            Some(&PropValue::String("alice".to_string()))
        );
        if let Some(PropValue::Float(f)) = node.props.get("score") {
            assert!((f - 0.95).abs() < f64::EPSILON);
        } else {
            panic!("expected Float property");
        }
    }

    #[test]
    fn test_get_node_with_vectors() {
        let mt = Memtable::new();
        let dense_config = dense_config(3);
        let mut node = make_node(7, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        node.sparse_vector = Some(vec![(2, 1.5), (9, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);

        let (_dir, reader) = write_and_open_with_dense_config(&mt, Some(&dense_config));
        let node = reader.get_node(7).unwrap().unwrap();
        assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(node.sparse_vector, Some(vec![(2, 1.5), (9, 0.25)]));
    }

    #[test]
    fn test_multi_label_flush_reopen_indexes_every_member_label() {
        let mt = Memtable::new();
        let cases: &[(u64, &[u32], &str, i64)] = &[
            (1, &[1], "n1", 100),
            (2, &[10, 11], "n2", 200),
            (3, &[20, 21, 22, 23, 24], "n5", 500),
            (
                4,
                &[100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
                "n10",
                1000,
            ),
        ];
        for &(id, labels, key, updated_at) in cases {
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_labels(id, labels, key, updated_at)),
                id,
            );
        }

        let (_dir, reader) = write_and_open(&mt);
        assert_eq!(reader.node_meta_count(), cases.len() as u64);
        for (meta_index, &(id, labels, key, updated_at)) in cases.iter().enumerate() {
            let node = reader.get_node(id).unwrap().unwrap();
            assert_eq!(node.label_ids.as_slice(), labels);
            let meta = reader.node_meta_at(meta_index).unwrap();
            assert_eq!(meta.node_id, id);
            assert_eq!(meta.label_ids.as_slice(), labels);

            for &label_id in labels {
                let by_key = reader.node_by_key(label_id, key).unwrap().unwrap();
                assert_eq!(by_key.id, id);
                assert_eq!(by_key.label_ids.as_slice(), labels);
                assert_eq!(reader.nodes_by_label_id(label_id).unwrap(), vec![id]);
                assert_eq!(reader.node_label_posting_count(label_id).unwrap(), 1);
                assert_eq!(
                    reader
                        .nodes_by_time_range(label_id, updated_at, updated_at)
                        .unwrap(),
                    vec![id]
                );
            }
        }
        assert!(reader.node_by_key(999, "n10").unwrap().is_none());
        assert!(reader.nodes_by_label_id(999).unwrap().is_empty());
    }

    #[test]
    fn test_multi_label_flush_declared_property_sidecars_by_member_label() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        props.insert("score".to_string(), PropValue::Int(42));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels_and_props(
                1,
                &[1, 2, 3],
                "item",
                props,
                100,
            )),
            1,
        );

        let eq_label_1 = SecondaryIndexManifestEntry {
            index_id: 10,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let eq_label_2 = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 2,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let eq_absent = SecondaryIndexManifestEntry {
            index_id: 12,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 9,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_label_2 = SecondaryIndexManifestEntry {
            index_id: 13,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 2,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_label_3 = SecondaryIndexManifestEntry {
            index_id: 14,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 3,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let indexes = vec![
            eq_label_1.clone(),
            eq_label_2.clone(),
            eq_absent.clone(),
            range_label_2.clone(),
            range_label_3.clone(),
        ];
        for entry in &indexes {
            mt.register_secondary_index(entry);
        }

        let info = write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, &indexes).unwrap();
        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &indexes).unwrap();

        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        assert_eq!(
            reader
                .find_nodes_by_secondary_eq_index(eq_label_1.index_id, red_hash)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            reader
                .find_nodes_by_secondary_eq_index(eq_label_2.index_id, red_hash)
                .unwrap(),
            vec![1]
        );
        assert!(reader
            .find_nodes_by_secondary_eq_index(eq_absent.index_id, red_hash)
            .unwrap()
            .is_empty());

        let encoded_score = numeric_range_sort_key_for_value(&PropValue::Int(42)).unwrap();
        assert_eq!(
            reader
                .find_nodes_by_secondary_range_index_if_present(
                    range_label_2.index_id,
                    Some((encoded_score, true)),
                    Some((encoded_score, true)),
                    None,
                )
                .unwrap()
                .unwrap(),
            vec![(encoded_score, 1)]
        );
        assert_eq!(
            reader
                .find_nodes_by_secondary_range_index_if_present(
                    range_label_3.index_id,
                    Some((encoded_score, true)),
                    Some((encoded_score, true)),
                    None,
                )
                .unwrap()
                .unwrap(),
            vec![(encoded_score, 1)]
        );
    }

    #[test]
    fn test_all_nodes_hydrates_mixed_vectors() {
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut with_vectors = make_node(1, 1, "with_vectors");
        with_vectors.dense_vector = Some(vec![0.5, 0.6]);
        mt.apply_op(&WalOp::UpsertNode(with_vectors), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "plain")), 0);

        let (_dir, reader) = write_and_open_with_dense_config(&mt, Some(&dense_config));
        let nodes = reader.all_nodes().unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].dense_vector, Some(vec![0.5, 0.6]));
        assert!(nodes[0].sparse_vector.is_none());
        assert!(nodes[1].dense_vector.is_none());
        assert!(nodes[1].sparse_vector.is_none());
    }

    #[test]
    fn test_get_node_tombstoned() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 9999,
            },
            0,
        );

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.get_node(1).unwrap().is_none());
        assert!(reader.is_node_deleted(1));
    }

    // --- get_edge ---

    #[test]
    fn test_get_edge_found() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let edge = reader.get_edge(100).unwrap().unwrap();
        assert_eq!(edge.id, 100);
        assert_eq!(edge.from, 1);
        assert_eq!(edge.to, 2);
        assert_eq!(edge.label_id, 10);
    }

    #[test]
    fn test_get_edge_not_found() {
        let mt = Memtable::new();
        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.get_edge(1).unwrap().is_none());
    }

    // --- node_by_key ---

    #[test]
    fn test_node_by_key_found() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 2, "alice")), 0); // different type

        let (_dir, reader) = write_and_open(&mt);
        let node = reader.node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, 1);

        let node = reader.node_by_key(1, "bob").unwrap().unwrap();
        assert_eq!(node.id, 2);

        let node = reader.node_by_key(2, "alice").unwrap().unwrap();
        assert_eq!(node.id, 3);
    }

    #[test]
    fn test_node_by_key_not_found() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.node_by_key(1, "bob").unwrap().is_none());
        assert!(reader.node_by_key(2, "alice").unwrap().is_none());
    }

    // --- neighbors ---

    #[test]
    fn test_neighbors_outgoing() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert_eq!(nbrs.len(), 2);

        let ids: NodeIdSet = nbrs.iter().map(|n| n.node_id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    #[test]
    fn test_neighbors_incoming() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(2, Direction::Incoming, None, 0).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 1);
    }

    #[test]
    fn test_neighbors_with_label_filter() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 20)), 0);

        let (_dir, reader) = write_and_open(&mt);

        // Filter label 10 only
        let nbrs = reader
            .neighbors(1, Direction::Outgoing, Some(&[10]), 0)
            .unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 2);

        // Filter label 20 only
        let nbrs = reader
            .neighbors(1, Direction::Outgoing, Some(&[20]), 0)
            .unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 3);

        // Filter non-existent type
        let nbrs = reader
            .neighbors(1, Direction::Outgoing, Some(&[99]), 0)
            .unwrap();
        assert!(nbrs.is_empty());
    }

    #[test]
    fn test_neighbors_with_limit() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "hub")), 0);
        for i in 2..=6 {
            mt.apply_op(&WalOp::UpsertNode(make_node(i, 1, &format!("n{}", i))), 0);
            mt.apply_op(&WalOp::UpsertEdge(make_edge(i - 1, 1, i, 10)), 0);
        }

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 3).unwrap();
        assert_eq!(nbrs.len(), 3);

        // No limit
        let all = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_neighbors_both_with_limit_preserves_self_loop_budget_semantics() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 1, 10)), 0); // self-loop
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 2, 1, 10)), 0); // incoming unique
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 3, 1, 10)), 0); // incoming unique

        let (_dir, reader) = write_and_open(&mt);
        let both = reader.neighbors(1, Direction::Both, None, 2).unwrap();
        assert_eq!(both.len(), 1);
        assert_eq!(both[0].edge_id, 1);
    }

    #[test]
    fn test_neighbors_no_adjacency() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "lonely")), 0);

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert!(nbrs.is_empty());
    }

    #[test]
    fn test_for_each_adj_posting_breaks_early() {
        let mt = Memtable::new();
        for id in 1..=4 {
            mt.apply_op(&WalOp::UpsertNode(make_node(id, 1, &format!("n{}", id))), 0);
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 4, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let mut seen = 0usize;
        let flow = reader
            .for_each_adj_posting(
                1,
                Direction::Outgoing,
                None,
                &mut |_edge_id, _neighbor_id, _weight, _valid_from, _valid_to| {
                    seen += 1;
                    ControlFlow::Break(())
                },
            )
            .unwrap();

        assert!(matches!(flow, ControlFlow::Break(())));
        assert_eq!(seen, 1);
    }

    #[test]
    fn test_for_each_adj_posting_batch_breaks_early() {
        let mt = Memtable::new();
        for id in 1..=4 {
            mt.apply_op(&WalOp::UpsertNode(make_node(id, 1, &format!("n{}", id))), 0);
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 4, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let mut seen = 0usize;
        let flow = reader
            .for_each_adj_posting_batch(
                &[1],
                Direction::Outgoing,
                None,
                &mut |_node_id, _edge_id, _neighbor_id, _weight, _valid_from, _valid_to| {
                    seen += 1;
                    ControlFlow::Break(())
                },
            )
            .unwrap();

        assert!(matches!(flow, ControlFlow::Break(())));
        assert_eq!(seen, 1);
    }

    #[test]
    fn test_adjacency_posting_delta_overflow_returns_corruption() {
        let mt = Memtable::new();
        for id in 1..=3 {
            mt.apply_op(&WalOp::UpsertNode(make_node(id, 1, &format!("n{}", id))), 0);
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(u64::MAX - 1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(u64::MAX, 1, 3, 10)), 0);

        let (dir, reader) = write_and_open(&mt);
        assert_eq!(
            reader
                .neighbors(1, Direction::Outgoing, Some(&[10]), 0)
                .unwrap()
                .len(),
            2
        );

        let seg_dir = dir.path().join("seg_0001");
        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::AdjOutPostings,
            |payload| {
                let (first_delta, mut offset) = read_varint_at(payload, 0).unwrap();
                assert_eq!(first_delta, u64::MAX - 1);
                let (_, len) = read_varint_at(payload, offset).unwrap();
                offset += len;
                offset += 4;
                let (_, len) = read_varint_at(payload, offset).unwrap();
                offset += len;
                let (_, len) = read_varint_at(payload, offset).unwrap();
                offset += len;

                let (second_delta, second_len) = read_varint_at(payload, offset).unwrap();
                assert_eq!(second_delta, 1);
                let mut replacement = Vec::new();
                write_varint_for_test(&mut replacement, 2);
                assert_eq!(replacement.len(), second_len);
                payload[offset..offset + second_len].copy_from_slice(&replacement);
            },
        );

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        let err = reader
            .neighbors(1, Direction::Outgoing, Some(&[10]), 0)
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::CorruptRecord(message) if message.contains("delta overflow")),
            "expected adjacency delta overflow corruption, got {err}"
        );

        let mut cursors = reader
            .endpoint_adj_posting_cursors(&[1], Direction::Outgoing, Some(&[10]))
            .unwrap();
        assert_eq!(
            reader.next_adj_posting_edge_id(&mut cursors[0]).unwrap(),
            Some(u64::MAX - 1)
        );
        let err = reader
            .next_adj_posting_edge_id(&mut cursors[0])
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::CorruptRecord(message) if message.contains("delta overflow")),
            "expected cursor delta overflow corruption, got {err}"
        );
    }

    // --- Empty segment ---

    #[test]
    fn test_empty_segment_reader() {
        let mt = Memtable::new();
        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.node_count(), 0);
        assert_eq!(reader.edge_count(), 0);
        assert!(reader.get_node(1).unwrap().is_none());
        assert!(reader.get_edge(1).unwrap().is_none());
        assert!(reader
            .neighbors(1, Direction::Outgoing, None, 0)
            .unwrap()
            .is_empty());
    }

    // --- Binary search stress ---

    #[test]
    fn test_binary_search_many_nodes() {
        let mt = Memtable::new();
        for i in 1..=100 {
            mt.apply_op(&WalOp::UpsertNode(make_node(i, 1, &format!("n{}", i))), 0);
        }

        let (_dir, reader) = write_and_open(&mt);

        // Every node should be findable
        for i in 1..=100 {
            let node = reader.get_node(i).unwrap().unwrap();
            assert_eq!(node.id, i);
        }

        // Non-existent IDs
        assert!(reader.get_node(0).unwrap().is_none());
        assert!(reader.get_node(101).unwrap().is_none());
    }

    #[test]
    fn test_binary_search_key_index_many() {
        let mt = Memtable::new();
        for i in 1..=50 {
            mt.apply_op(
                &WalOp::UpsertNode(make_node(i, (i % 3) as u32 + 1, &format!("key_{:04}", i))),
                0,
            );
        }

        let (_dir, reader) = write_and_open(&mt);

        // Every node should be findable by key
        for i in 1..=50 {
            let label_id = (i % 3) as u32 + 1;
            let key = format!("key_{:04}", i);
            let node = reader.node_by_key(label_id, &key).unwrap().unwrap();
            assert_eq!(node.id, i);
        }
    }

    // --- Roundtrip: write segment + read back ---

    #[test]
    fn test_full_segment_roundtrip() {
        let mt = Memtable::new();

        // Build a small graph
        for i in 1..=5 {
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_props(i, 1, &format!("node_{}", i))),
                0,
            );
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 2, 3, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 3, 20)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(4, 4, 5, 10)), 0);

        // Delete one node and one edge
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 99,
                deleted_at: 9999,
            },
            0,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 99,
                deleted_at: 9999,
            },
            0,
        );

        let (_dir, reader) = write_and_open(&mt);

        // Verify nodes
        assert_eq!(reader.node_count(), 5);
        for i in 1..=5 {
            let node = reader.get_node(i).unwrap().unwrap();
            assert_eq!(node.key, format!("node_{}", i));
            assert_eq!(
                node.props.get("name"),
                Some(&PropValue::String(format!("node_{}", i)))
            );
        }

        // Verify edges
        assert_eq!(reader.edge_count(), 4);
        let e1 = reader.get_edge(1).unwrap().unwrap();
        assert_eq!(e1.from, 1);
        assert_eq!(e1.to, 2);

        // Verify key lookup
        let n = reader.node_by_key(1, "node_3").unwrap().unwrap();
        assert_eq!(n.id, 3);

        // Verify neighbors
        let out1 = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert_eq!(out1.len(), 2); // edges to 2 and 3
        let ids: NodeIdSet = out1.iter().map(|n| n.node_id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));

        // Verify label-filtered neighbors
        let out1_t10 = reader
            .neighbors(1, Direction::Outgoing, Some(&[10]), 0)
            .unwrap();
        assert_eq!(out1_t10.len(), 1);
        assert_eq!(out1_t10[0].node_id, 2);

        // Verify tombstones
        assert!(reader.is_node_deleted(99));
        assert!(reader.is_edge_deleted(99));
    }

    #[test]
    fn test_secondary_eq_sidecar_roundtrip() {
        let mt = Memtable::new();

        let mut props1 = BTreeMap::new();
        props1.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props: props1,
                created_at: 1000,
                updated_at: 1001,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            0,
        );

        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 2,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "cherry".to_string(),
                props: props2,
                created_at: 1000,
                updated_at: 1001,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            0,
        );

        let mut props3 = BTreeMap::new();
        props3.insert("color".to_string(), PropValue::String("green".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 3,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "lime".to_string(),
                props: props3,
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
            index_id: 41,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (_dir, reader) = write_and_open_with_secondary_eq_sidecar(&mt, &entry);

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
    fn test_secondary_eq_sidecar_cache_reloads_after_validation_failure_and_repair() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 51,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, reader) = write_and_open_with_secondary_eq_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let sidecar_path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_eq_{}.dat", entry.index_id));
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));

        let corrupt_path = seg_dir.join("secondary_indexes").join(".corrupt_eq.dat");
        std::fs::write(&corrupt_path, [1u8, 2, 3]).unwrap();
        std::fs::rename(&corrupt_path, &sidecar_path).unwrap();

        assert!(!reader
            .validate_secondary_eq_sidecar(entry.index_id)
            .unwrap());

        let repaired_path = seg_dir.join("secondary_indexes").join(".repaired_eq.dat");
        let mut repaired_groups = BTreeMap::new();
        repaired_groups.insert(red_hash, vec![1]);
        crate::segment_writer::write_node_prop_eq_sidecar_to_path(&repaired_path, &repaired_groups)
            .unwrap();
        std::fs::rename(&repaired_path, &sidecar_path).unwrap();

        assert!(reader
            .find_nodes_by_secondary_eq_index(entry.index_id, red_hash)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_secondary_eq_sidecar_lookup_uses_validated_cache() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 52,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, reader) = write_and_open_with_secondary_eq_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let sidecar_path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_eq_{}.dat", entry.index_id));
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));

        assert_eq!(
            reader
                .find_nodes_by_secondary_eq_index(entry.index_id, red_hash)
                .unwrap(),
            vec![1]
        );

        let corrupt_path = seg_dir.join("secondary_indexes").join(".corrupt_eq.dat");
        std::fs::write(&corrupt_path, [1u8, 2, 3]).unwrap();
        std::fs::rename(&corrupt_path, &sidecar_path).unwrap();

        assert_eq!(
            reader
                .find_nodes_by_secondary_eq_index(entry.index_id, red_hash)
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn test_secondary_eq_lookup_does_not_full_validate_unqueried_group() {
        let mt = Memtable::new();
        for (id, color) in [(1, "red"), (2, "red"), (3, "green"), (4, "green")] {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            mt.apply_op(
                &WalOp::UpsertNode(NodeRecord {
                    id,
                    label_ids: NodeLabelSet::single(1).unwrap(),
                    key: format!("node-{id}"),
                    props,
                    created_at: 1000,
                    updated_at: 1001,
                    weight: 0.5,
                    dense_vector: None,
                    sparse_vector: None,
                    last_write_seq: 0,
                }),
                id,
            );
        }

        let entry = SecondaryIndexManifestEntry {
            index_id: 53,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, _) = write_and_open_with_secondary_eq_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let kind = SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: entry.index_id,
        };
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let green_hash = hash_prop_equality_key(&PropValue::String("green".to_string()));

        rewrite_component_payload_for_test(&seg_dir, kind.clone(), |payload| {
            let (offset, id_count) = secondary_eq_group_range(payload, green_hash)
                .unwrap()
                .expect("green group must exist");
            assert!(id_count >= 2);
            let first = read_u64_at(payload, offset).unwrap();
            let second = read_u64_at(payload, offset + 8).unwrap();
            write_u64_at_for_test(payload, offset, second);
            write_u64_at_for_test(payload, offset + 8, first);
        });

        let payload = component_payload_bytes_for_test(&seg_dir, kind);
        assert!(validate_secondary_eq_sidecar_data(&payload).is_err());

        let reader = reopen_test_segment_with_index(&seg_dir, &entry);
        let mut reds = reader
            .find_nodes_by_secondary_eq_index(entry.index_id, red_hash)
            .unwrap();
        reds.sort_unstable();
        assert_eq!(reds, vec![1, 2]);
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Equality
            ),
            DeclaredIndexRuntimeCoverageState::Available
        );
    }

    #[test]
    fn test_secondary_eq_lookup_latches_selected_group_malformed() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 54,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, _) = write_and_open_with_secondary_eq_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let kind = SegmentComponentKind::NodePropertyEqualityIndex {
            index_id: entry.index_id,
        };
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));

        rewrite_component_payload_for_test(&seg_dir, kind.clone(), |payload| {
            let (count, _) = secondary_eq_sidecar_index_bounds(payload).unwrap();
            for index in 0..count {
                let entry_off = 8 + index * SECONDARY_EQ_ENTRY_SIZE;
                if read_u64_at(payload, entry_off).unwrap() == red_hash {
                    write_u64_at_for_test(payload, entry_off + 8, payload.len() as u64 + 8);
                    return;
                }
            }
            panic!("red group must exist");
        });

        let reader = reopen_test_segment_with_index(&seg_dir, &entry);
        let err = reader
            .find_nodes_by_secondary_eq_index_if_present(entry.index_id, red_hash)
            .unwrap_err();
        assert!(err.to_string().contains("exceeds file length"));
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Equality
            ),
            DeclaredIndexRuntimeCoverageState::Corrupt
        );
        assert!(matches!(
            reader.component_registry.availability(&kind),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert_eq!(
            reader
                .find_nodes_by_secondary_eq_index_if_present(entry.index_id, red_hash)
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_validate_secondary_eq_sidecar_rejects_unsorted_node_ids() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&28u64.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());

        match validate_secondary_eq_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("node IDs are not strictly increasing"));
            }
            other => panic!(
                "expected corrupt secondary equality sidecar, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_validate_secondary_eq_sidecar_rejects_missing_header() {
        match validate_secondary_eq_sidecar_data(&[1u8, 2, 3]) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("missing header"));
            }
            other => panic!(
                "expected corrupt secondary equality sidecar, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_validate_secondary_eq_sidecar_rejects_index_length_past_eof() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&28u64.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());

        match validate_secondary_eq_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("index length"));
            }
            other => panic!(
                "expected corrupt secondary equality sidecar, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_validate_secondary_eq_sidecar_rejects_non_increasing_value_hashes() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u64.to_le_bytes());

        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&48u64.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());

        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&56u64.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());

        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());

        match validate_secondary_eq_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("value hashes are not strictly increasing"));
            }
            other => panic!(
                "expected corrupt secondary equality sidecar, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_validate_secondary_eq_sidecar_rejects_group_past_eof() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&28u64.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());

        match validate_secondary_eq_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("exceeds file length"));
            }
            other => panic!(
                "expected corrupt secondary equality sidecar, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_validate_secondary_eq_sidecar_rejects_overlapping_group_ranges() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u64.to_le_bytes());

        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&48u64.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());

        data.extend_from_slice(&8u64.to_le_bytes());
        data.extend_from_slice(&56u64.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());

        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&3u64.to_le_bytes());

        match validate_secondary_eq_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("overlaps a previous group"));
            }
            other => panic!(
                "expected corrupt secondary equality sidecar, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_secondary_range_sidecar_cache_reloads_after_validation_failure_and_repair() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(10));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 61,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, reader) = write_and_open_with_secondary_range_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let sidecar_path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_range_{}.dat", entry.index_id));

        let corrupt_path = seg_dir.join("secondary_indexes").join(".corrupt_range.dat");
        std::fs::write(&corrupt_path, [1u8, 2, 3]).unwrap();
        std::fs::rename(&corrupt_path, &sidecar_path).unwrap();

        assert!(!reader
            .validate_secondary_range_sidecar(entry.index_id)
            .unwrap());

        let repaired_path = seg_dir
            .join("secondary_indexes")
            .join(".repaired_range.dat");
        crate::segment_writer::write_node_prop_range_sidecar_to_path(
            &repaired_path,
            &[(
                numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap(),
                1,
            )],
        )
        .unwrap();
        let encoded_10 = numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap();
        std::fs::rename(&repaired_path, &sidecar_path).unwrap();

        assert_eq!(
            reader
                .find_nodes_by_secondary_range_index_if_present(
                    entry.index_id,
                    Some((encoded_10, true)),
                    Some((encoded_10, true)),
                    None,
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_validate_secondary_range_sidecar_rejects_missing_header() {
        match validate_secondary_range_sidecar_data(&[1u8, 2, 3]) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("missing header"));
            }
            other => panic!("expected corrupt secondary range sidecar, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_secondary_range_sidecar_rejects_length_mismatch() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(
            &numeric_range_sort_key_for_value(&PropValue::Int(10))
                .unwrap()
                .as_bytes(),
        );
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0xFF);

        match validate_secondary_range_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("does not match expected fixed-width length"));
            }
            other => panic!("expected corrupt secondary range sidecar, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_secondary_range_sidecar_rejects_impossible_numeric_key() {
        let significand = (1_u128 << 53) | 1;
        let bit_length = 54_u32;
        let exponent = -1_i32;
        let rank = exponent + bit_length as i32 - 1;
        let fraction = significand << (128 - bit_length);

        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&raw_nonzero_numeric_range_key_for_test(2, rank, fraction));
        data.extend_from_slice(&1u64.to_le_bytes());

        match validate_secondary_range_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("canonical finite numeric key"));
            }
            other => panic!("expected corrupt secondary range sidecar, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_secondary_range_sidecar_rejects_unsorted_entries() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(
            &numeric_range_sort_key_for_value(&PropValue::Int(11))
                .unwrap()
                .as_bytes(),
        );
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(
            &numeric_range_sort_key_for_value(&PropValue::Int(10))
                .unwrap()
                .as_bytes(),
        );
        data.extend_from_slice(&1u64.to_le_bytes());

        match validate_secondary_range_sidecar_data(&data) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("not strictly increasing"));
            }
            other => panic!("expected corrupt secondary range sidecar, got {:?}", other),
        }
    }

    #[test]
    fn test_secondary_range_query_trusts_current_identity_without_scrubbing_unvisited_entries() {
        let mt = Memtable::new();
        for (id, score) in [(1, 10), (2, 20), (3, 30)] {
            let mut props = BTreeMap::new();
            props.insert("score".to_string(), PropValue::Int(score));
            mt.apply_op(
                &WalOp::UpsertNode(NodeRecord {
                    id,
                    label_ids: NodeLabelSet::single(1).unwrap(),
                    key: format!("node-{id}"),
                    props,
                    created_at: 1000,
                    updated_at: 1001,
                    weight: 0.5,
                    dense_vector: None,
                    sparse_vector: None,
                    last_write_seq: 0,
                }),
                id,
            );
        }

        let entry = SecondaryIndexManifestEntry {
            index_id: 62,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, _) = write_and_open_with_secondary_range_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let kind = SegmentComponentKind::NodePropertyRangeIndex {
            index_id: entry.index_id,
        };
        let encoded_10 = numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap();

        rewrite_component_payload_for_test(&seg_dir, kind.clone(), |payload| {
            let entry_off = 8 + 2 * SECONDARY_RANGE_ENTRY_SIZE;
            write_numeric_key_at_for_test(payload, entry_off, PropValue::Int(5));
        });

        let payload = component_payload_bytes_for_test(&seg_dir, kind);
        assert!(validate_secondary_range_sidecar_data(&payload).is_err());

        let reader = reopen_test_segment_with_index(&seg_dir, &entry);
        assert_eq!(
            reader
                .find_nodes_by_secondary_range_index_if_present(
                    entry.index_id,
                    Some((encoded_10, true)),
                    Some((encoded_10, true)),
                    None,
                )
                .unwrap(),
            Some(vec![(encoded_10, 1)])
        );
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Range
            ),
            DeclaredIndexRuntimeCoverageState::Available
        );
    }

    #[test]
    fn test_secondary_range_lookup_latches_header_malformed() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(10));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "apple".to_string(),
                props,
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
            index_id: 63,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let (dir, _) = write_and_open_with_secondary_range_sidecar(&mt, &entry);
        let seg_dir = dir.path().join("seg_0001");
        let kind = SegmentComponentKind::NodePropertyRangeIndex {
            index_id: entry.index_id,
        };
        let encoded_10 = numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap();

        rewrite_component_payload_for_test(&seg_dir, kind.clone(), |payload| {
            write_u64_at_for_test(payload, 0, 2);
        });

        let reader = reopen_test_segment_with_index(&seg_dir, &entry);
        let err = reader
            .find_nodes_by_secondary_range_index_if_present(
                entry.index_id,
                Some((encoded_10, true)),
                Some((encoded_10, true)),
                None,
            )
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("does not match expected fixed-width length"));
        assert_eq!(
            reader.declared_index_runtime_coverage_state(
                entry.index_id,
                PlannerStatsDeclaredIndexKind::Range
            ),
            DeclaredIndexRuntimeCoverageState::Corrupt
        );
        assert!(matches!(
            reader.component_registry.availability(&kind),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert_eq!(
            reader
                .find_nodes_by_secondary_range_index_if_present(
                    entry.index_id,
                    Some((encoded_10, true)),
                    Some((encoded_10, true)),
                    None,
                )
                .unwrap(),
            None
        );
    }

    // --- Weight preservation in adjacency postings ---

    #[test]
    fn test_neighbor_weight_preserved_in_segment() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);
        mt.apply_op(
            &WalOp::UpsertEdge(EdgeRecord {
                id: 10,
                from: 1,
                to: 2,
                label_id: 5,
                props: BTreeMap::new(),
                created_at: 100,
                updated_at: 100,
                weight: 0.75,
                valid_from: 0,
                valid_to: i64::MAX,
                last_write_seq: 0,
            }),
            0,
        );
        mt.apply_op(
            &WalOp::UpsertEdge(EdgeRecord {
                id: 11,
                from: 1,
                to: 3,
                label_id: 5,
                props: BTreeMap::new(),
                created_at: 100,
                updated_at: 100,
                weight: 0.25,
                valid_from: 0,
                valid_to: i64::MAX,
                last_write_seq: 0,
            }),
            0,
        );

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert_eq!(nbrs.len(), 2);

        // Check that each neighbor has the correct weight
        for n in &nbrs {
            if n.edge_id == 10 {
                assert!(
                    (n.weight - 0.75).abs() < f32::EPSILON,
                    "edge 10 weight: {}",
                    n.weight
                );
            } else if n.edge_id == 11 {
                assert!(
                    (n.weight - 0.25).abs() < f32::EPSILON,
                    "edge 11 weight: {}",
                    n.weight
                );
            } else {
                panic!("unexpected edge_id: {}", n.edge_id);
            }
        }
    }

    // --- Edge triple index ---

    #[test]
    fn test_edge_triple_index_roundtrip() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(101, 1, 3, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(102, 2, 3, 20)), 0);

        let (_dir, reader) = write_and_open(&mt);

        // Exact triple match
        let e = reader.edge_by_triple(1, 2, 10).unwrap().unwrap();
        assert_eq!(e.id, 100);
        assert_eq!(e.from, 1);
        assert_eq!(e.to, 2);

        let e = reader.edge_by_triple(1, 3, 10).unwrap().unwrap();
        assert_eq!(e.id, 101);

        let e = reader.edge_by_triple(2, 3, 20).unwrap().unwrap();
        assert_eq!(e.id, 102);

        // Non-existent triples
        assert!(reader.edge_by_triple(1, 2, 20).unwrap().is_none()); // wrong label
        assert!(reader.edge_by_triple(2, 1, 10).unwrap().is_none()); // reversed direction
        assert!(reader.edge_by_triple(3, 1, 10).unwrap().is_none()); // no such edge
    }

    #[test]
    fn test_edge_triple_index_excludes_tombstoned() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)), 0);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 100,
                deleted_at: 9999,
            },
            0,
        );

        let (_dir, reader) = write_and_open(&mt);

        // Edge is tombstoned, triple lookup should return None
        assert!(reader.edge_by_triple(1, 2, 10).unwrap().is_none());
    }

    #[test]
    fn test_edge_triple_index_returns_parallel_edges() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(101, 1, 2, 10)), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(102, 1, 3, 10)), 3);

        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.edge_ids_by_triple(1, 2, 10).unwrap(), vec![100, 101]);
        assert_eq!(reader.edge_ids_by_triple(1, 3, 10).unwrap(), vec![102]);
    }

    #[test]
    fn test_edge_metadata_index_ranges_and_weight_zero_nan() {
        let mt = Memtable::new();
        let mut neg_zero = make_edge(10, 1, 2, 5);
        neg_zero.weight = -0.0;
        neg_zero.updated_at = 100;
        neg_zero.valid_from = 0;
        neg_zero.valid_to = 100;
        let mut pos_zero = make_edge(11, 1, 3, 5);
        pos_zero.weight = 0.0;
        pos_zero.updated_at = 150;
        pos_zero.valid_from = 10;
        pos_zero.valid_to = 200;
        let mut positive = make_edge(12, 1, 4, 5);
        positive.weight = 2.0;
        positive.updated_at = 250;
        positive.valid_from = 20;
        positive.valid_to = 300;
        let mut other_label = make_edge(13, 1, 5, 6);
        other_label.weight = 0.0;
        other_label.updated_at = 175;
        let mut nan = make_edge(14, 1, 6, 5);
        nan.weight = f32::NAN;

        for edge in [neg_zero, pos_zero, positive, other_label, nan] {
            mt.apply_op(&WalOp::UpsertEdge(edge), 1);
        }

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.edge_weight_index_available());
        assert!(reader.edge_updated_at_index_available());
        assert!(reader.edge_valid_from_index_available());
        assert!(reader.edge_valid_to_index_available());

        let zero_bounds = RangeBoundFlags::inclusive(Some(0.0), Some(0.0));
        assert_eq!(
            reader.edge_ids_by_weight_range(Some(5), zero_bounds),
            Some(vec![10, 11])
        );
        assert_eq!(
            reader.edge_weight_range_count(Some(5), zero_bounds),
            Some(2)
        );
        assert_eq!(
            reader.edge_ids_by_weight_range(None, zero_bounds),
            Some(vec![10, 11, 13])
        );
        assert_eq!(reader.edge_weight_range_count(None, zero_bounds), Some(3));
        assert!(!reader
            .edge_ids_by_weight_range(None, RangeBoundFlags::inclusive(None, Some(f32::INFINITY)))
            .unwrap()
            .contains(&14));
        assert_eq!(
            reader.edge_weight_range_count(
                None,
                RangeBoundFlags::inclusive(None, Some(f32::INFINITY))
            ),
            Some(4)
        );

        assert_eq!(
            reader.edge_ids_by_updated_at_range(
                Some(5),
                RangeBoundFlags::inclusive(Some(100), Some(200))
            ),
            Some(vec![10, 11])
        );
        assert_eq!(
            reader.edge_updated_at_range_count(
                Some(5),
                RangeBoundFlags::inclusive(Some(100), Some(200))
            ),
            Some(2)
        );
        assert_eq!(
            reader.edge_ids_by_valid_to_range(
                Some(5),
                RangeBoundFlags {
                    lower: Some(100),
                    lower_inclusive: false,
                    upper: None,
                    upper_inclusive: true,
                }
            ),
            Some(vec![11, 12, 14])
        );
        assert_eq!(
            reader.edge_valid_to_range_count(
                Some(5),
                RangeBoundFlags {
                    lower: Some(100),
                    lower_inclusive: false,
                    upper: None,
                    upper_inclusive: true,
                }
            ),
            Some(3)
        );
        assert_eq!(
            reader
                .edge_metadata_scan_ids(|meta| meta.valid_from <= 100)
                .unwrap(),
            vec![10, 11, 12, 13, 14]
        );

        let mut metadata = vec![None; 3];
        reader
            .get_edge_metadata_batch(&[(1, 10), (0, 12), (2, 999)], &mut metadata)
            .unwrap();
        assert_eq!(metadata[0].unwrap().edge_id, 12);
        assert_eq!(metadata[0].unwrap().weight, 2.0);
        assert_eq!(metadata[1].unwrap().edge_id, 10);
        assert_eq!(metadata[2], None);
    }

    #[test]
    fn test_packed_core_components_share_one_mapping_and_logical_slices() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "b")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 3);
        let (dir, reader) = write_and_open(&mt);
        let seg_dir = dir.path().join("seg_0001");

        let node_mapping = reader
            .component_mapping_identity_for_test(SegmentComponentKind::NodeRecords)
            .unwrap();
        for kind in [
            SegmentComponentKind::EdgeRecords,
            SegmentComponentKind::NodeMetadata,
            SegmentComponentKind::EdgeMetadata,
            SegmentComponentKind::KeyIndex,
            SegmentComponentKind::NodeLabelIndex,
            SegmentComponentKind::AdjOutPostings,
            SegmentComponentKind::AdjOutIndex,
            SegmentComponentKind::EdgeWeightIndex,
        ] {
            assert_eq!(
                reader.component_mapping_identity_for_test(kind).unwrap(),
                node_mapping
            );
        }

        assert_eq!(
            reader.raw_nodes_mmap(),
            component_payload_bytes_for_test(&seg_dir, SegmentComponentKind::NodeRecords)
        );
        assert_eq!(
            &reader.adj_out_dat[..],
            component_payload_bytes_for_test(&seg_dir, SegmentComponentKind::AdjOutPostings)
        );
        assert_eq!(
            &reader.adj_out_idx[..],
            component_payload_bytes_for_test(&seg_dir, SegmentComponentKind::AdjOutIndex)
        );
    }

    #[test]
    fn test_open_rejects_missing_packed_core_container_file() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        std::fs::remove_file(seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME))
            .unwrap();

        let err = expect_engine_error(SegmentReader::open_unpinned_for_test(&seg_dir, 1, None));
        assert!(
            err.contains("No such file")
                || err.contains("cannot find the file")
                || err.contains("segment.core"),
            "got: {err}"
        );
    }

    #[test]
    fn test_open_rejects_external_packed_core_component_handles() {
        fn rewrite_handle_to_external(
            seg_dir: &std::path::Path,
            kind: SegmentComponentKind,
        ) -> SegmentInfo {
            let mt = Memtable::new();
            mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
            mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
            mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 3);
            let info = write_segment(seg_dir, 1, &mt, None).unwrap();
            let mut manifest = read_segment_manifest_for_test(seg_dir);
            let record = manifest
                .components
                .iter_mut()
                .find(|record| record.kind == kind)
                .expect("test component must exist");
            record.handle = ComponentHandleV1::ExternalFile {
                relative_path: "external-packed-core-component.dat".to_string(),
                payload_offset: crate::segment_components::COMPONENT_IDENTITY_HEADER_LEN as u64,
                payload_len: record.payload_len,
            };
            write_segment_manifest_for_test(seg_dir, &manifest);
            info
        }

        for kind in [
            SegmentComponentKind::NodeRecords,
            SegmentComponentKind::EdgeWeightIndex,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let seg_dir = dir.path().join(format!("seg_{:04}", kind.kind_tag()));
            let info = rewrite_handle_to_external(&seg_dir, kind.clone());

            let err =
                expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
            assert!(err.contains("must use a PackedRange handle"), "got: {err}");
            assert!(err.contains(&format!("{:?}", kind)), "got: {err}");
        }
    }

    #[test]
    fn test_open_rejects_required_packed_range_wrong_container_id() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let record = manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .unwrap();
        let ComponentHandleV1::PackedRange {
            container_component_id,
            ..
        } = &mut record.handle
        else {
            panic!("NodeRecords should be packed");
        };
        *container_component_id = [42; 32];
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("wrong container"), "got: {err}");
    }

    #[test]
    fn test_open_rejects_required_packed_range_overflow_and_overlap() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 1, 5)), 2);

        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let record = manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::KeyIndex)
            .unwrap();
        let ComponentHandleV1::PackedRange { offset, .. } = &mut record.handle else {
            panic!("KeyIndex should be packed");
        };
        *offset = u64::MAX - 1;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("range overflows"), "got: {err}");

        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let (node_offset, _) = packed_range_for_test(&manifest, SegmentComponentKind::NodeRecords);
        let record = manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeRecords)
            .unwrap();
        let ComponentHandleV1::PackedRange { offset, .. } = &mut record.handle else {
            panic!("EdgeRecords should be packed");
        };
        *offset = node_offset;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("overlap"), "got: {err}");
    }

    #[test]
    fn test_optional_packed_edge_metadata_bad_range_falls_back() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 3);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let (node_offset, _) = packed_range_for_test(&manifest, SegmentComponentKind::NodeRecords);
        let record = manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeWeightIndex)
            .unwrap();
        let ComponentHandleV1::PackedRange { offset, .. } = &mut record.handle else {
            panic!("EdgeWeightIndex should be packed");
        };
        *offset = node_offset;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &[]).unwrap();
        assert!(reader.get_edge(10).unwrap().is_some());
        assert!(!reader.edge_weight_index_available());
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::EdgeWeightIndex),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert_eq!(
            reader.edge_ids_by_weight_range(
                Some(5),
                RangeBoundFlags::inclusive(Some(0.0), Some(2.0))
            ),
            None
        );
    }

    #[test]
    fn test_missing_and_corrupt_edge_metadata_indexes_are_unavailable() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 1);
        let (dir, reader) = write_and_open(&mt);
        assert!(reader.edge_weight_index_available());
        let seg_dir = dir.path().join("seg_0001");

        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        manifest.components.retain(|record| {
            !matches!(
                record.kind,
                SegmentComponentKind::EdgeWeightIndex
                    | SegmentComponentKind::EdgeUpdatedAtIndex
                    | SegmentComponentKind::EdgeValidFromIndex
                    | SegmentComponentKind::EdgeValidToIndex
            )
        });
        write_segment_manifest_for_test(&seg_dir, &manifest);
        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.edge_weight_index_available());
        assert!(reader.get_edge(10).unwrap().is_some());
        assert_eq!(
            reader.edge_ids_by_weight_range(
                Some(5),
                RangeBoundFlags::inclusive(Some(0.0), Some(2.0))
            ),
            None
        );

        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        write_segment(&seg_dir, 1, &mt, None).unwrap();
        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::EdgeWeightIndex,
            |payload| {
                payload[0..8].copy_from_slice(&2u64.to_le_bytes());
            },
        );
        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::EdgeUpdatedAtIndex,
            |payload| {
                payload[0..8].copy_from_slice(&2u64.to_le_bytes());
            },
        );

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.edge_weight_index_available());
        assert!(!reader.edge_updated_at_index_available());
        assert!(reader.get_edge(10).unwrap().is_some());
    }

    #[test]
    fn test_open_does_not_full_scan_edge_metadata_index_sortedness() {
        let mt = Memtable::new();
        let mut first = make_edge(10, 1, 2, 5);
        first.weight = 1.0;
        let mut second = make_edge(11, 1, 3, 5);
        second.weight = 2.0;
        mt.apply_op(&WalOp::UpsertEdge(first), 1);
        mt.apply_op(&WalOp::UpsertEdge(second), 2);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);

        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::EdgeWeightIndex,
            |payload| {
                assert_eq!(payload.len(), 8 + 2 * EDGE_WEIGHT_INDEX_ENTRY_SIZE);
                let first_start = 8;
                let second_start = first_start + EDGE_WEIGHT_INDEX_ENTRY_SIZE;
                for offset in 0..EDGE_WEIGHT_INDEX_ENTRY_SIZE {
                    payload.swap(first_start + offset, second_start + offset);
                }
                assert!(validate_edge_weight_index_data(payload).is_err());
            },
        );
        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::EdgeUpdatedAtIndex,
            |payload| {
                assert_eq!(payload.len(), 8 + 2 * EDGE_I64_METADATA_INDEX_ENTRY_SIZE);
                let first_start = 8;
                let second_start = first_start + EDGE_I64_METADATA_INDEX_ENTRY_SIZE;
                for offset in 0..EDGE_I64_METADATA_INDEX_ENTRY_SIZE {
                    payload.swap(first_start + offset, second_start + offset);
                }
                assert!(validate_edge_i64_metadata_index_data(
                    payload,
                    EDGE_UPDATED_AT_INDEX_LOGICAL_NAME
                )
                .is_err());
            },
        );

        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &[]).unwrap();
        assert!(reader.edge_weight_index_available());
        assert!(reader.edge_updated_at_index_available());
        assert_eq!(
            reader.optional_component_availability_for_test(SegmentComponentKind::EdgeWeightIndex),
            ComponentAvailability::Available
        );
        assert_eq!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::EdgeUpdatedAtIndex),
            ComponentAvailability::Available
        );
    }

    #[test]
    fn test_validate_node_vector_sidecars_rejects_metadata_length_overflow() {
        let mut vector_meta = Vec::new();
        vector_meta.extend_from_slice(&u64::MAX.to_le_bytes());

        match validate_node_vector_sidecars(1, &vector_meta, &[], &[], u64::MAX) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("overflow") || message.contains("addressable"));
            }
            Err(other) => panic!(
                "expected node vector metadata overflow corruption, got {}",
                other
            ),
            Ok(_) => panic!("expected node vector metadata overflow error"),
        }
    }

    // --- Bounds checking regression tests ---

    #[test]
    fn test_truncated_packed_core_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let core_path = seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME);
        let mut data = std::fs::read(&core_path).unwrap();
        data.pop();
        std::fs::write(&core_path, data).unwrap();

        let err_msg = expect_engine_error(SegmentReader::open_unpinned_for_test(&seg_dir, 1, None));
        assert!(
            err_msg.contains("does not match file length"),
            "error should describe identity length issue: {}",
            err_msg
        );
    }

    #[test]
    fn test_truncated_tombstones_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 99,
                deleted_at: 1234,
            },
            1,
        );
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        rewrite_component_payload_for_test(&seg_dir, SegmentComponentKind::Tombstones, |payload| {
            payload[0..8].copy_from_slice(&5u64.to_le_bytes());
        });

        let result = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None);
        assert!(
            result.is_err(),
            "truncated packed tombstones should return error, not panic"
        );
    }

    #[test]
    fn test_decode_node_at_truncated_returns_error() {
        // Minimal data that starts a valid node but is truncated mid-record
        // Format v4: no id in data, starts with label_id
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes()); // label_id
                                                     // Missing key_len and beyond (truncated)
        let result = decode_node_at(&data, 0, 42);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_edge_at_truncated_returns_error() {
        // Partial edge record (format v4: no id in data, starts with from)
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes()); // from
                                                     // Missing to, label_id, timestamps, etc.
        let result = decode_edge_at(&data, 0, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_edge_property_at_projects_requested_property() {
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        props.insert("score".to_string(), PropValue::Int(10));
        let props_bytes = rmp_serde::to_vec(&props).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&7u32.to_le_bytes());
        data.extend_from_slice(&1000i64.to_le_bytes());
        data.extend_from_slice(&1001i64.to_le_bytes());
        data.extend_from_slice(&1.0f32.to_le_bytes());
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&i64::MAX.to_le_bytes());
        data.extend_from_slice(&(props_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(&props_bytes);

        assert_eq!(
            decode_edge_property_at(&data, 0, 100, "color").unwrap(),
            Some(PropValue::String("red".to_string()))
        );
        assert_eq!(
            decode_edge_property_at(&data, 0, 100, "score").unwrap(),
            Some(PropValue::Int(10))
        );
        assert_eq!(
            decode_edge_property_at(&data, 0, 100, "missing").unwrap(),
            None
        );

        let mut truncated = data;
        truncated.pop();
        assert!(decode_edge_property_at(&truncated, 0, 100, "color").is_err());
    }

    #[test]
    fn test_open_with_info_rejects_root_local_segment_id_mismatch() {
        let mt = Memtable::new();
        let (_dir, seg_dir, mut info) = write_segment_with_info(&mt, None);
        info.id = 2;

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("does not match root segment"), "got: {err}");
    }

    #[test]
    fn test_open_with_info_rejects_root_local_count_mismatch() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        let (_dir, seg_dir, mut info) = write_segment_with_info(&mt, None);
        info.node_count += 1;

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("does not match root node_count"), "got: {err}");
    }

    #[test]
    fn test_open_with_info_rejects_root_local_segment_data_id_mismatch() {
        let mt = Memtable::new();
        let (_dir, seg_dir, mut info) = write_segment_with_info(&mt, None);
        info.segment_data_id = [7; 32];

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(
            err.contains("segment_data_id does not match root"),
            "got: {err}"
        );
    }

    #[test]
    fn test_open_with_info_rejects_segment_data_id_recompute_mismatch() {
        let mt = Memtable::new();
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        manifest.segment_data_id = [9; 32];
        write_segment_manifest_for_test(&seg_dir, &manifest);
        let info = segment_info_from_manifest(&manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(
            err.contains("segment_data_id does not match component source groups"),
            "got: {err}"
        );
    }

    #[test]
    fn test_open_rejects_missing_v10_segment_manifest() {
        let mt = Memtable::new();
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        std::fs::remove_file(seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap();

        let err = expect_engine_error(SegmentReader::open_unpinned_for_test(&seg_dir, 1, None));
        assert!(err.contains("missing segment_manifest.dat"), "got: {err}");
    }

    #[test]
    fn test_open_rejects_missing_required_manifest_record() {
        let mt = Memtable::new();
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        manifest
            .components
            .retain(|record| record.kind != SegmentComponentKind::NodeRecords);
        let info = segment_info_from_manifest(&manifest);
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("NodeRecords"), "got: {err}");
    }

    #[test]
    fn test_open_rejects_required_component_dependency_mismatch() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let record = manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::KeyIndex)
            .unwrap();
        record.dependencies.clear();
        record.dependency_digest = crate::segment_components::dependency_digest(&[]);
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("dependency digest"), "got: {err}");
    }

    #[test]
    fn test_open_rejects_required_component_build_mismatch() {
        let mt = Memtable::new();
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::NodeRecords)
            .unwrap()
            .build_fingerprint ^= 1;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("build fingerprint"), "got: {err}");
    }

    #[test]
    fn test_open_rejects_required_component_id_recompute_mismatch() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let bogus_id = [17u8; 32];
        manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::KeyIndex)
            .unwrap()
            .component_id = bogus_id;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let err = expect_engine_error(SegmentReader::open_with_info(&seg_dir, &info, None, &[]));
        assert!(err.contains("component KeyIndex id"), "got: {err}");
    }

    #[test]
    fn test_open_disables_only_optional_component_on_build_fingerprint_mismatch() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 3);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeWeightIndex)
            .unwrap()
            .build_fingerprint ^= 1;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &[]).unwrap();
        assert!(reader.get_node(1).unwrap().is_some());
        assert!(reader.get_edge(10).unwrap().is_some());
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::EdgeWeightIndex),
            ComponentAvailability::Incompatible { .. }
        ));
        assert_eq!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::EdgeUpdatedAtIndex),
            ComponentAvailability::Available
        );
    }

    #[test]
    fn test_open_disables_only_optional_component_on_component_id_mismatch() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 3);
        let (_dir, seg_dir, info) = write_segment_with_info(&mt, None);
        let mut manifest = read_segment_manifest_for_test(&seg_dir);
        let bogus_id = [23u8; 32];
        manifest
            .components
            .iter_mut()
            .find(|record| record.kind == SegmentComponentKind::EdgeWeightIndex)
            .unwrap()
            .component_id = bogus_id;
        write_segment_manifest_for_test(&seg_dir, &manifest);

        let reader = SegmentReader::open_with_info(&seg_dir, &info, None, &[]).unwrap();
        assert!(reader.get_node(1).unwrap().is_some());
        assert!(reader.get_edge(10).unwrap().is_some());
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::EdgeWeightIndex),
            ComponentAvailability::Incompatible { .. }
        ));
        assert_eq!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::EdgeUpdatedAtIndex),
            ComponentAvailability::Available
        );
    }

    #[test]
    fn test_open_rejects_required_component_identity_header_mismatch() {
        let mt = Memtable::new();
        let (_dir, seg_dir, _info) = write_segment_with_info(&mt, None);
        let core_path = seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME);
        let mut data = std::fs::read(&core_path).unwrap();
        data[120] ^= 1;
        std::fs::write(&core_path, data).unwrap();

        let err = expect_engine_error(SegmentReader::open_unpinned_for_test(&seg_dir, 1, None));
        assert!(err.contains("identity header"), "got: {err}");
    }

    #[test]
    fn test_open_rejects_vector_metadata_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        };
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::NodeVectorMetadata,
            |meta| {
                meta[0..8].copy_from_slice(&2u64.to_le_bytes());
            },
        );

        let err = SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(err
            .to_string()
            .contains("does not match node metadata count"));
    }

    #[test]
    fn test_open_rejects_vector_blob_with_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let core_path = seg_dir.join(crate::segment_components::PACKED_CORE_FILENAME);
        let mut core = std::fs::read(&core_path).unwrap();
        core.extend_from_slice(&0.9f32.to_le_bytes());
        std::fs::write(&core_path, core).unwrap();

        let err = SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        let message = err.to_string();
        assert!(
            message.contains("does not match file length")
                || message.contains("exceeds blob length"),
            "{message}"
        );
    }

    #[test]
    fn test_open_exposes_dense_hnsw_header_for_dense_segments() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(3);

        let mut first = make_node(1, 1, "a");
        first.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        mt.apply_op(&WalOp::UpsertNode(first), 0);

        let mut second = make_node(2, 1, "b");
        second.dense_vector = Some(vec![0.3, 0.2, 0.1]);
        mt.apply_op(&WalOp::UpsertNode(second), 0);

        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let reader =
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config)).unwrap();
        let header = reader.dense_hnsw_header().unwrap();

        assert_eq!(header.point_count, 2);
        assert_eq!(header.metric, DenseMetric::Cosine);
        assert_eq!(header.dimension, 3);
        assert_eq!(header.m, dense_config.hnsw.m);
        assert_eq!(
            reader.raw_dense_hnsw_meta_mmap(),
            &read_payload_file(&seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME))
        );
        assert_eq!(
            reader.raw_dense_hnsw_graph_mmap(),
            &read_payload_file(&seg_dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME))
        );
    }

    #[test]
    fn test_open_rejects_missing_dense_hnsw_files_for_dense_segments() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(3);

        let mut first = make_node(1, 1, "a");
        first.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        mt.apply_op(&WalOp::UpsertNode(first), 0);

        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        std::fs::remove_file(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
        std::fs::remove_file(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)).unwrap();

        let reader =
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config)).unwrap();
        assert!(reader.dense_hnsw_header().is_none());
        assert!(reader.raw_dense_hnsw_meta_mmap().is_empty());
        assert!(reader.raw_dense_hnsw_graph_mmap().is_empty());
    }

    #[test]
    fn test_open_keeps_dense_hnsw_empty_for_vectorless_segments() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "plain")), 0);

        let (_dir, reader) = write_and_open(&mt);

        assert!(reader.dense_hnsw_header().is_none());
        assert!(reader.raw_dense_hnsw_meta_mmap().is_empty());
        assert!(reader.raw_dense_hnsw_graph_mmap().is_empty());
    }

    #[test]
    fn test_open_rejects_dense_hnsw_files_in_v6_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        tamper_envelope_format_version(&seg_dir, 6);

        let err = SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("unsupported segment manifest version"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_dense_hnsw_metric_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        rewrite_payload_file(
            &seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            |meta| {
                meta[26] = 1; // Euclidean
            },
        );

        let reader =
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config)).unwrap();
        assert!(reader.dense_hnsw_header().is_none());
        assert!(matches!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::DenseHnswMetadata),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::DenseHnswGraph),
            ComponentAvailability::CorruptIdentity { .. }
        ));
    }

    #[test]
    fn test_open_rejects_missing_sparse_posting_files_for_sparse_segments() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME))
            .unwrap();
        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME))
            .unwrap();

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(reader.raw_sparse_posting_index_mmap().is_empty());
        assert!(reader.raw_sparse_postings_mmap().is_empty());
    }

    #[test]
    fn test_open_rejects_sparse_posting_files_in_v7_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        tamper_envelope_format_version(&seg_dir, 7);

        let err = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None)
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("unsupported segment manifest version"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_sparse_vectors_in_v7_segment_without_postings() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME))
            .unwrap();
        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME))
            .unwrap();

        tamper_envelope_format_version(&seg_dir, 7);

        let err = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None)
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("unsupported segment manifest version"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_sparse_posting_payload_semantic_mismatch_is_not_open_time_scrubbed() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        rewrite_payload_file(
            &seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME),
            |postings| {
                postings[8..12].copy_from_slice(&9.0f32.to_le_bytes());
            },
        );

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.raw_sparse_posting_index_mmap().is_empty());
        assert!(!reader.raw_sparse_postings_mmap().is_empty());
        assert_eq!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::SparsePostingIndex),
            ComponentAvailability::Available
        );
        assert_eq!(
            reader.optional_component_availability_for_test(SegmentComponentKind::SparsePostings),
            ComponentAvailability::Available
        );
    }

    #[test]
    fn test_sparse_vector_source_payload_semantics_are_not_open_time_scrubbed() {
        let mut node = make_node(1, 1, "sparse-negative");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        let (_dir, seg_dir) = write_sparse_segment(vec![node]);

        rewrite_component_payload_for_test(
            &seg_dir,
            SegmentComponentKind::NodeSparseVectorBlob,
            |sparse_blob| {
                sparse_blob[4..8].copy_from_slice(&(-1.5f32).to_le_bytes());
            },
        );

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.raw_node_sparse_vectors_mmap().is_empty());
    }

    #[test]
    fn test_sparse_posting_dimension_mismatch_is_not_open_time_scrubbed() {
        let mut node = make_node(1, 1, "sparse-missing-dim");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        let (_dir, seg_dir) = write_sparse_segment(vec![node]);

        let index_path = seg_dir.join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME);
        rewrite_payload_file(&index_path, |index| {
            index[24..28].copy_from_slice(&9u32.to_le_bytes());
        });

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.raw_sparse_posting_index_mmap().is_empty());
        assert!(!reader.raw_sparse_postings_mmap().is_empty());
        assert_eq!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::SparsePostingIndex),
            ComponentAvailability::Available
        );
        assert_eq!(
            reader.optional_component_availability_for_test(SegmentComponentKind::SparsePostings),
            ComponentAvailability::Available
        );
    }

    #[test]
    fn test_sparse_posting_index_shape_mismatch_is_latched_on_first_use() {
        let mut first = make_node(1, 1, "sparse-count-a");
        first.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        let mut second = make_node(2, 1, "sparse-count-b");
        second.sparse_vector = Some(vec![(2, 0.5)]);
        let (_dir, seg_dir) = write_sparse_segment(vec![first, second]);

        let index_path = seg_dir.join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME);

        rewrite_payload_file(&index_path, |index| {
            index[20..24].copy_from_slice(&1u32.to_le_bytes());
            index[28..36].copy_from_slice(&12u64.to_le_bytes());
        });

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(!reader.raw_sparse_posting_index_mmap().is_empty());
        assert!(!reader.raw_sparse_postings_mmap().is_empty());
        assert!(reader.sparse_postings_available());
        assert_eq!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::SparsePostingIndex),
            ComponentAvailability::Available
        );
        assert!(reader
            .accumulate_sparse_posting_scores(&[(2, 1.0)], &mut NodeIdMap::default())
            .is_err());
        assert!(!reader.sparse_postings_available());
        assert!(matches!(
            reader
                .optional_component_availability_for_test(SegmentComponentKind::SparsePostingIndex),
            ComponentAvailability::CorruptIdentity { .. }
        ));
        assert!(matches!(
            reader.optional_component_availability_for_test(SegmentComponentKind::SparsePostings),
            ComponentAvailability::CorruptIdentity { .. }
        ));
    }

    #[test]
    fn test_open_rejects_dense_hnsw_hnsw_param_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        rewrite_payload_file(
            &seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            |meta| {
                meta[22..24].copy_from_slice(&(dense_config.hnsw.m + 1).to_le_bytes());
            },
        );

        let reader =
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config)).unwrap();
        assert!(reader.dense_hnsw_header().is_none());
    }

    #[test]
    fn test_open_rejects_dense_hnsw_dimension_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        rewrite_payload_file(
            &seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            |meta| {
                meta[28..32].copy_from_slice(&3u32.to_le_bytes());
            },
        );

        let reader =
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config)).unwrap();
        assert!(reader.dense_hnsw_header().is_none());
    }

    #[test]
    fn test_open_rejects_dense_hnsw_ef_construction_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        rewrite_payload_file(
            &seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            |meta| {
                meta[24..26]
                    .copy_from_slice(&(dense_config.hnsw.ef_construction + 1).to_le_bytes());
            },
        );

        let reader =
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, Some(&dense_config)).unwrap();
        assert!(reader.dense_hnsw_header().is_none());
    }

    #[test]
    fn test_open_rejects_dense_hnsw_without_dense_config() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let reader = SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap();
        assert!(reader.dense_hnsw_header().is_none());
    }

    // --- Packed metadata reader tests ---

    fn build_node_meta_payload_for_layout_test(
        label_offsets: &[u64],
        label_ids: &[u32],
    ) -> Vec<u8> {
        let node_count = label_offsets.len() - 1;
        let fixed_entries_offset = NODE_META_HEADER_SIZE;
        let fixed_entries_len = node_count * NODE_META_FIXED_ENTRY_SIZE;
        let label_offsets_offset = fixed_entries_offset + fixed_entries_len;
        let label_ids_offset =
            label_offsets_offset + label_offsets.len() * NODE_META_LABEL_OFFSET_ENTRY_SIZE;
        let mut data = vec![0u8; label_ids_offset + label_ids.len() * 4];

        data[0..8].copy_from_slice(&(node_count as u64).to_le_bytes());
        data[8..10].copy_from_slice(&(NODE_META_FIXED_ENTRY_SIZE as u16).to_le_bytes());
        data[10..12].copy_from_slice(&(NODE_META_LABEL_OFFSET_ENTRY_SIZE as u16).to_le_bytes());
        data[16..24].copy_from_slice(&(fixed_entries_offset as u64).to_le_bytes());
        data[24..32].copy_from_slice(&(label_offsets_offset as u64).to_le_bytes());
        data[32..40].copy_from_slice(&(label_ids_offset as u64).to_le_bytes());
        data[40..48].copy_from_slice(&(label_ids.len() as u64).to_le_bytes());

        for (index, offset) in label_offsets.iter().enumerate() {
            let pos = label_offsets_offset + index * NODE_META_LABEL_OFFSET_ENTRY_SIZE;
            data[pos..pos + 8].copy_from_slice(&offset.to_le_bytes());
        }
        for (index, label_id) in label_ids.iter().enumerate() {
            let pos = label_ids_offset + index * 4;
            data[pos..pos + 4].copy_from_slice(&label_id.to_le_bytes());
        }

        data
    }

    #[test]
    fn test_node_meta_layout_rejects_invalid_label_offset_sentinels() {
        let mut nonzero_first = build_node_meta_payload_for_layout_test(&[1, 1, 2], &[1, 2]);
        assert!(parse_node_meta_layout(&nonzero_first).is_err());

        let terminal_mismatch = build_node_meta_payload_for_layout_test(&[0, 1, 1], &[1, 2]);
        assert!(parse_node_meta_layout(&terminal_mismatch).is_err());

        let label_offsets_offset = NODE_META_HEADER_SIZE + 2 * NODE_META_FIXED_ENTRY_SIZE;
        nonzero_first[label_offsets_offset..label_offsets_offset + 8]
            .copy_from_slice(&0u64.to_le_bytes());
        assert!(parse_node_meta_layout(&nonzero_first).is_ok());
    }

    #[test]
    fn test_packed_node_metadata_roundtrip() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 1, "alice")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")), 0);

        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.node_meta_count(), 2);

        // First entry: node_id=1
        let meta = reader.node_meta_at(0).unwrap();
        assert_eq!(meta.node_id, 1);
        assert_eq!(meta.label_ids.as_slice(), [1]);
        assert_eq!(meta.updated_at, 2000);
        assert!((meta.weight - 0.75).abs() < f32::EPSILON);
        assert_eq!(meta.key_len, 5); // "alice"

        // Second entry: node_id=2
        let meta2 = reader.node_meta_at(1).unwrap();
        assert_eq!(meta2.node_id, 2);
        assert_eq!(meta2.label_ids.as_slice(), [2]);
        assert_eq!(meta2.key_len, 3); // "bob"
    }

    #[test]
    fn test_node_meta_reader_rejects_malformed_label_sets() {
        let layout_data = build_node_meta_payload_for_layout_test(&[0, 2], &[2, 1]);
        let layout = parse_node_meta_layout(&layout_data).unwrap().unwrap();
        let error = read_node_meta_entry_at(&layout_data, layout, 0).unwrap_err();
        assert!(
            error.to_string().contains("sorted ascending and unique"),
            "got: {error}"
        );

        let layout_data = build_node_meta_payload_for_layout_test(&[0, 2], &[1, 1]);
        let layout = parse_node_meta_layout(&layout_data).unwrap().unwrap();
        let error = read_node_meta_entry_at(&layout_data, layout, 0).unwrap_err();
        assert!(
            error.to_string().contains("sorted ascending and unique"),
            "got: {error}"
        );
    }

    #[test]
    fn test_node_meta_batch_reads_node_meta_without_node_records_index() {
        let mt = Memtable::new();
        for id in 1..=8u64 {
            mt.apply_op(
                &WalOp::UpsertNode(make_node(id, id as u32, &format!("n{id}"))),
                id,
            );
        }

        let (_dir, mut reader) = write_and_open(&mt);
        reader.nodes_mmap = MappedData::Empty;

        let seek_lookups = vec![(0usize, 1u64), (1, 8)];
        let mut seek_results = vec![None; seek_lookups.len()];
        reader
            .get_node_meta_batch(&seek_lookups, &mut seek_results)
            .unwrap();
        assert_eq!(
            seek_results,
            vec![
                Some((NodeLabelSet::single(1).unwrap(), 1001, 0.5)),
                Some((NodeLabelSet::single(8).unwrap(), 1001, 0.5)),
            ]
        );

        let merge_lookups = vec![(0usize, 2u64), (1, 3), (2, 4), (3, 5)];
        let mut merge_results = vec![None; merge_lookups.len()];
        reader
            .get_node_meta_batch(&merge_lookups, &mut merge_results)
            .unwrap();
        assert_eq!(
            merge_results,
            vec![
                Some((NodeLabelSet::single(2).unwrap(), 1001, 0.5)),
                Some((NodeLabelSet::single(3).unwrap(), 1001, 0.5)),
                Some((NodeLabelSet::single(4).unwrap(), 1001, 0.5)),
                Some((NodeLabelSet::single(5).unwrap(), 1001, 0.5)),
            ]
        );
    }

    #[test]
    fn test_node_selected_fields_metadata_only_uses_sidecar_without_node_records() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 7, "metadata-only")), 0);

        let (_dir, mut reader) = write_and_open(&mt);
        reader.nodes_mmap = MappedData::Empty;

        let mut results = vec![None];
        reader
            .get_node_selected_fields_batch(
                &[(0usize, 1u64)],
                &NodeSelectedFieldNeeds::default(),
                &mut results,
                None,
            )
            .unwrap();

        let selected = results[0].as_ref().unwrap();
        assert_eq!(selected.meta.id, 1);
        assert_eq!(selected.meta.label_ids, NodeLabelSet::single(7).unwrap());
        assert_eq!(selected.meta.updated_at, 1001);
        assert!((selected.meta.weight - 0.5).abs() < f32::EPSILON);
        assert!(selected.key.is_none());
        assert!(selected.props.is_empty());
        assert!(selected.created_at.is_none());
        assert!(selected.dense_vector.is_none());
        assert!(selected.sparse_vector.is_none());
    }

    #[test]
    fn test_packed_edge_metadata_roundtrip() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 0);

        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.edge_meta_count(), 1);

        let (eid, _off, _len, from, to, tid, updated_at, weight, vf, vt, _lws) =
            reader.edge_meta_at(0).unwrap();
        assert_eq!(eid, 10);
        assert_eq!(from, 1);
        assert_eq!(to, 2);
        assert_eq!(tid, 5);
        assert_eq!(updated_at, 2001);
        assert!((weight - 1.0).abs() < f32::EPSILON);
        assert_eq!(vf, 0);
        assert_eq!(vt, i64::MAX);
    }

    #[test]
    fn test_packed_node_metadata_offsets_decode_node_records() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bb")), 0);

        let (_dir, reader) = write_and_open(&mt);

        // Read data_offset/data_len from metadata and verify node can be decoded there.
        for i in 0..reader.node_meta_count() as usize {
            let meta = reader.node_meta_at(i).unwrap();
            // Verify the offset points to valid data in the node records payload.
            let record_start = meta.data_offset as usize;
            assert_eq!(reader.nodes_mmap[record_start], 1);
            assert_eq!(
                u32::from_le_bytes(
                    reader.nodes_mmap[record_start + 1..record_start + 5]
                        .try_into()
                        .unwrap()
                ),
                meta.label_ids.single_label_id()
            );
            let node = decode_node_at(&reader.nodes_mmap, record_start, meta.node_id).unwrap();
            assert_eq!(node.id, meta.node_id);
            assert_eq!(node.label_ids, meta.label_ids);
            assert!(meta.data_len > 0);
        }
    }

    #[test]
    fn test_node_selected_fields_batch_returns_corrupt_record_for_bad_projected_props() {
        use std::io::Write;

        let mt = Memtable::new();
        let mut node = make_node(1, 1, "bad-props");
        node.props.insert(
            "status".to_string(),
            PropValue::String("active".to_string()),
        );
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        let (_dir, mut reader) = write_and_open(&mt);

        let mut corrupt_payload = reader.raw_nodes_mmap().to_vec();
        let record_offset = read_u64_at(&corrupt_payload, 16).unwrap() as usize;
        let label_count = corrupt_payload[record_offset] as usize;
        let key_len_offset = record_offset + 1 + label_count * 4;
        let key_len = read_u16_at(&corrupt_payload, key_len_offset).unwrap() as usize;
        let props_offset = key_len_offset + 2 + key_len + 24;
        corrupt_payload[props_offset] = 0xc1;

        let mut corrupt_file = tempfile::NamedTempFile::new().unwrap();
        corrupt_file.write_all(&corrupt_payload).unwrap();
        corrupt_file.as_file().sync_all().unwrap();
        let mmap = unsafe { Mmap::map(corrupt_file.as_file()).unwrap() };
        reader.nodes_mmap = MappedData::Mmap {
            mmap: Arc::new(mmap),
            payload_offset: 0,
            payload_len: corrupt_payload.len(),
        };

        let mut results = vec![None];
        let err = reader
            .get_node_selected_fields_batch(
                &[(0, 1)],
                &NodeSelectedFieldNeeds {
                    props: PropertySelection::Keys(vec!["status".to_string()]),
                    ..NodeSelectedFieldNeeds::default()
                },
                &mut results,
                None,
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::CorruptRecord(_)));
        assert!(
            err.to_string().contains("projected props decode"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_legacy_single_type_node_record_payload_is_rejected() {
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&1u16.to_le_bytes());
        legacy.extend_from_slice(b"a");
        legacy.extend_from_slice(&1000i64.to_le_bytes());
        legacy.extend_from_slice(&1001i64.to_le_bytes());
        legacy.extend_from_slice(&0.5f32.to_le_bytes());
        let props = rmp_serde::to_vec(&BTreeMap::<String, PropValue>::new()).unwrap();
        legacy.extend_from_slice(&(props.len() as u32).to_le_bytes());
        legacy.extend_from_slice(&props);
        legacy.push(0);

        assert!(decode_node_at(&legacy, 0, 1).is_err());
    }

    #[test]
    fn test_node_record_reader_rejects_malformed_label_sets() {
        fn raw_node_record(label_ids: &[u32]) -> Vec<u8> {
            let mut data = Vec::new();
            data.push(label_ids.len() as u8);
            for &label_id in label_ids {
                data.extend_from_slice(&label_id.to_le_bytes());
            }
            data.extend_from_slice(&1u16.to_le_bytes());
            data.extend_from_slice(b"a");
            data.extend_from_slice(&1000i64.to_le_bytes());
            data.extend_from_slice(&1001i64.to_le_bytes());
            data.extend_from_slice(&0.5f32.to_le_bytes());
            let props = rmp_serde::to_vec(&BTreeMap::<String, PropValue>::new()).unwrap();
            data.extend_from_slice(&(props.len() as u32).to_le_bytes());
            data.extend_from_slice(&props);
            data
        }

        let empty = raw_node_record(&[]);
        assert!(decode_node_at(&empty, 0, 1).is_err());

        let unsorted = raw_node_record(&[2, 1]);
        assert!(decode_node_at(&unsorted, 0, 1).is_err());

        let duplicate = raw_node_record(&[1, 1]);
        assert!(decode_node_at(&duplicate, 0, 1).is_err());
    }

    #[test]
    fn test_cross_segment_copy_external_sidecar_rejected() {
        // Create segment A (id=1) with edges to produce external sidecars
        let mt_a = Memtable::new();
        mt_a.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a1")), 1);
        mt_a.apply_op(&WalOp::UpsertNode(make_node(2, 1, "a2")), 2);
        mt_a.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 1)), 3);
        let dir_a = tempfile::tempdir().unwrap();
        let seg_dir_a = dir_a.path().join("seg_0001");
        write_segment(&seg_dir_a, 1, &mt_a, None).unwrap();

        // Create segment B (id=2) with different data
        let mt_b = Memtable::new();
        mt_b.apply_op(&WalOp::UpsertNode(make_node(3, 2, "b1")), 4);
        mt_b.apply_op(&WalOp::UpsertNode(make_node(4, 2, "b2")), 5);
        mt_b.apply_op(&WalOp::UpsertEdge(make_edge(20, 3, 4, 2)), 6);
        let dir_b = tempfile::tempdir().unwrap();
        let seg_dir_b = dir_b.path().join("seg_0002");
        write_segment(&seg_dir_b, 2, &mt_b, None).unwrap();

        // Find an external optional sidecar file in segment A
        let manifest_a = read_segment_manifest_for_test(&seg_dir_a);
        let external_record = manifest_a.components.iter().find(|r| {
            matches!(r.handle, ComponentHandleV1::ExternalFile { .. })
                && matches!(r.requirement, ComponentRequirement::Optional { .. })
        });

        let record = external_record
            .expect("test precondition: expected optional external sidecar in segment A");
        let relative_path = match &record.handle {
            ComponentHandleV1::ExternalFile { relative_path, .. } => relative_path,
            _ => panic!("test precondition: expected ExternalFile handle"),
        };
        let src_path = seg_dir_a.join(relative_path);
        let dst_path = seg_dir_b.join(relative_path);
        assert!(
            src_path.exists(),
            "test precondition: source sidecar file must exist"
        );
        std::fs::copy(&src_path, &dst_path).unwrap();

        // Patch segment B's manifest to reference the copied file's record
        let mut manifest_b = read_segment_manifest_for_test(&seg_dir_b);
        let matching = manifest_b
            .components
            .iter_mut()
            .find(|r| r.kind == record.kind);
        if let Some(target) = matching {
            *target = record.clone();
        }
        write_segment_manifest_for_test(&seg_dir_b, &manifest_b);

        // Opening segment B succeeds but the optional component
        // is unavailable due to identity header mismatch
        let info_b = segment_info_from_manifest(&manifest_b);
        let reader = SegmentReader::open_with_info(&seg_dir_b, &info_b, None, &[]).unwrap();

        let avail = reader.optional_component_availability_for_test(record.kind.clone());
        assert!(
            matches!(
                avail,
                ComponentAvailability::Incompatible { .. }
                    | ComponentAvailability::CorruptIdentity { .. }
            ),
            "cross-segment optional sidecar should be unavailable, got: {:?}",
            avail,
        );
    }

    #[test]
    fn test_cross_segment_copy_packed_manifest_record_rejected() {
        // Create segment A (id=1)
        let mt_a = Memtable::new();
        mt_a.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a1")), 1);
        mt_a.apply_op(&WalOp::UpsertNode(make_node(2, 1, "a2")), 2);
        let dir_a = tempfile::tempdir().unwrap();
        let seg_dir_a = dir_a.path().join("seg_0001");
        write_segment(&seg_dir_a, 1, &mt_a, None).unwrap();

        // Create segment B (id=2) with different data
        let mt_b = Memtable::new();
        mt_b.apply_op(&WalOp::UpsertNode(make_node(3, 2, "b1")), 3);
        mt_b.apply_op(&WalOp::UpsertNode(make_node(4, 2, "b2")), 4);
        let dir_b = tempfile::tempdir().unwrap();
        let seg_dir_b = dir_b.path().join("seg_0002");
        write_segment(&seg_dir_b, 2, &mt_b, None).unwrap();

        // Inject segment A's NodeRecords packed range record into B's manifest
        let manifest_a = read_segment_manifest_for_test(&seg_dir_a);
        let node_record_a = manifest_a
            .components
            .iter()
            .find(|r| r.kind == SegmentComponentKind::NodeRecords)
            .unwrap()
            .clone();

        let mut manifest_b = read_segment_manifest_for_test(&seg_dir_b);
        let node_record_b = manifest_b
            .components
            .iter_mut()
            .find(|r| r.kind == SegmentComponentKind::NodeRecords)
            .unwrap();
        *node_record_b = node_record_a;
        write_segment_manifest_for_test(&seg_dir_b, &manifest_b);

        // Opening should fail: the injected record has a different component_id
        // (computed from segment A's segment_id) and a different container_component_id
        let info_b = segment_info_from_manifest(&manifest_b);
        let result = SegmentReader::open_with_info(&seg_dir_b, &info_b, None, &[]);
        assert!(
            result.is_err(),
            "injecting another segment's packed record should be rejected, got Ok"
        );
    }
}
