use crate::dense_hnsw::{
    dense_score_from_bytes, load_dense_hnsw_query_points, search_dense_hnsw_scoped_with_points,
    search_dense_hnsw_with_points, validate_dense_hnsw_files, DenseHnswHeader, DenseQueryPoint,
    DENSE_HNSW_GRAPH_FILENAME, DENSE_HNSW_META_FILENAME,
};
use crate::error::EngineError;
use crate::segment_writer::{
    NODE_DENSE_VECTOR_BLOB_FILENAME, NODE_SPARSE_VECTOR_BLOB_FILENAME, NODE_VECTOR_META_ENTRY_SIZE,
    NODE_VECTOR_META_FILENAME, SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC,
};
use crate::sparse_postings::{
    read_sparse_posting_groups, validate_sparse_posting_files, SPARSE_POSTINGS_FILENAME,
    SPARSE_POSTING_INDEX_FILENAME,
};
use crate::types::*;
use memmap2::Mmap;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::ops::ControlFlow;
use std::ops::Deref;
use std::path::Path;
use std::sync::OnceLock;

/// Segment file data, either mmap'd or an empty placeholder.
/// Segment data files (e.g. adj_out.dat) can be 0 bytes when empty.
/// memmap2 can't map empty files, so we handle that case explicitly.
enum MappedData {
    Mmap(Mmap),
    Empty,
}

impl Deref for MappedData {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            MappedData::Mmap(m) => m,
            MappedData::Empty => &[],
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
const ADJ_INDEX_ENTRY_SIZE: usize = 24; // node_id (8) + type_id (4) + offset (8) + count (4)
                                        // ADJ_POSTING_SIZE removed. Postings are now variable-length (delta + varint encoded)
const TOMBSTONE_ENTRY_SIZE: usize = 25; // kind (1) + id (8) + deleted_at (8) + last_write_seq (8)
const TYPE_INDEX_ENTRY_SIZE: usize = 16; // type_id (4) + offset (8) + count (4)
const PROP_INDEX_ENTRY_SIZE: usize = 32; // type_id (4) + key_hash (8) + value_hash (8) + offset (8) + count (4)
const EDGE_TRIPLE_ENTRY_SIZE: usize = 28; // from (8) + to (8) + type_id (4) + edge_id (8)

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
fn choose_batch_read_strategy(
    index_data: &[u8],
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

    let idx_start = 8;
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
/// table). Returns the first entry index where `(type_id, key) >= (target_type,
/// target_key)`, in [0, count].
fn lower_bound_key_index(
    data: &[u8],
    offset_table_start: usize,
    count: usize,
    target_type: u32,
    target_key: &str,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_offset = read_u64_at(data, offset_table_start + mid * 8)? as usize;
        let entry_type = read_u32_at(data, entry_offset)?;
        let key_len = read_u16_at(data, entry_offset + 12)? as usize;
        let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
        let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
            EngineError::CorruptRecord(format!(
                "invalid UTF-8 in key index at offset {}",
                entry_offset + 14
            ))
        })?;
        if (entry_type, entry_key) < (target_type, target_key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Upper bound for the key index. Returns the first entry index where
/// `(type_id, key) > (target_type, target_key)`, in [0, count].
fn upper_bound_key_index(
    data: &[u8],
    offset_table_start: usize,
    count: usize,
    target_type: u32,
    target_key: &str,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_offset = read_u64_at(data, offset_table_start + mid * 8)? as usize;
        let entry_type = read_u32_at(data, entry_offset)?;
        let key_len = read_u16_at(data, entry_offset + 12)? as usize;
        let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
        let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
            EngineError::CorruptRecord(format!(
                "invalid UTF-8 in key index at offset {}",
                entry_offset + 14
            ))
        })?;
        if (entry_type, entry_key) <= (target_type, target_key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// An mmap-backed reader for an immutable segment directory.
///
/// Provides O(log N) lookups by ID for nodes and edges, adjacency queries
/// from pre-built indexes, key-based lookups, and tombstone checks.
/// Size of a node metadata entry in node_meta.dat (60 bytes, v9).
const NODE_META_ENTRY_SIZE: usize = 60;
/// Size of an edge metadata entry in edge_meta.dat (80 bytes, v9).
const EDGE_META_ENTRY_SIZE: usize = 80;
const NODE_VECTOR_FLAG_DENSE: u8 = 0b0000_0001;
const NODE_VECTOR_FLAG_SPARSE: u8 = 0b0000_0010;
const DENSE_VECTOR_VALUE_SIZE: usize = 4;
const SPARSE_VECTOR_ENTRY_SIZE: usize = 8;

#[derive(Clone, Copy)]
struct DenseScoringMeta {
    type_id: u32,
    updated_at: i64,
    weight: f32,
    dense_offset: usize,
    dense_len: usize,
}

#[derive(Clone, Copy)]
struct SparseScoringMeta {
    type_id: u32,
    updated_at: i64,
    weight: f32,
    sparse_offset: usize,
    sparse_len: usize,
}

pub struct SegmentReader {
    pub segment_id: u64,
    #[allow(dead_code)] // Kept for future version-dependent logic
    format_version: u32,
    nodes_mmap: MappedData,
    edges_mmap: MappedData,
    adj_out_idx: MappedData,
    adj_out_dat: MappedData,
    adj_in_idx: MappedData,
    adj_in_dat: MappedData,
    key_index_mmap: MappedData,
    node_type_index_mmap: MappedData,
    edge_type_index_mmap: MappedData,
    prop_node_index_mmap: MappedData,
    edge_triple_index_mmap: MappedData,
    // V5 metadata sidecars
    node_meta_mmap: MappedData,
    edge_meta_mmap: MappedData,
    node_prop_hashes_mmap: MappedData,
    node_vector_meta_mmap: MappedData,
    node_dense_vectors_mmap: MappedData,
    node_sparse_vectors_mmap: MappedData,
    dense_hnsw_meta_mmap: MappedData,
    dense_hnsw_graph_mmap: MappedData,
    dense_hnsw_header: Option<DenseHnswHeader>,
    dense_hnsw_points: Vec<DenseQueryPoint>,
    sparse_posting_index_mmap: MappedData,
    sparse_postings_mmap: MappedData,
    // Timestamp range index
    timestamp_index_mmap: MappedData,
    deleted_nodes: NodeIdMap<TombstoneEntry>,
    deleted_edges: NodeIdMap<TombstoneEntry>,
    node_ids: OnceLock<Box<[u64]>>,
    node_count: u64,
    edge_count: u64,
}

impl SegmentReader {
    /// Open a segment directory and mmap all files.
    /// Validates the format version file.
    pub fn open(
        seg_dir: &Path,
        segment_id: u64,
        dense_config: Option<&DenseVectorConfig>,
    ) -> Result<Self, EngineError> {
        let format_version = read_format_version(seg_dir)?;
        let vector_meta_path = seg_dir.join(NODE_VECTOR_META_FILENAME);
        let dense_blob_path = seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME);
        let sparse_blob_path = seg_dir.join(NODE_SPARSE_VECTOR_BLOB_FILENAME);
        let dense_hnsw_meta_path = seg_dir.join(DENSE_HNSW_META_FILENAME);
        let dense_hnsw_graph_path = seg_dir.join(DENSE_HNSW_GRAPH_FILENAME);
        let sparse_posting_index_path = seg_dir.join(SPARSE_POSTING_INDEX_FILENAME);
        let sparse_postings_path = seg_dir.join(SPARSE_POSTINGS_FILENAME);
        let nodes_mmap = mmap_file(&seg_dir.join("nodes.dat"))?;
        let edges_mmap = mmap_file(&seg_dir.join("edges.dat"))?;
        let adj_out_idx = mmap_file(&seg_dir.join("adj_out.idx"))?;
        let adj_out_dat = mmap_file(&seg_dir.join("adj_out.dat"))?;
        let adj_in_idx = mmap_file(&seg_dir.join("adj_in.idx"))?;
        let adj_in_dat = mmap_file(&seg_dir.join("adj_in.dat"))?;
        let key_index_mmap = mmap_file(&seg_dir.join("key_index.dat"))?;
        let node_type_index_mmap = mmap_file_optional(&seg_dir.join("node_type_index.dat"))?;
        let edge_type_index_mmap = mmap_file_optional(&seg_dir.join("edge_type_index.dat"))?;
        let prop_node_index_mmap = mmap_file_optional(&seg_dir.join("prop_index.dat"))?;
        let edge_triple_index_mmap = mmap_file_optional(&seg_dir.join("edge_triple_index.dat"))?;
        // V5 metadata sidecars
        let node_meta_mmap = mmap_file(&seg_dir.join("node_meta.dat"))?;
        let edge_meta_mmap = mmap_file(&seg_dir.join("edge_meta.dat"))?;
        let node_prop_hashes_mmap = mmap_file_optional(&seg_dir.join("node_prop_hashes.dat"))?;
        let node_vector_meta_mmap = mmap_file_optional(&vector_meta_path)?;
        let node_dense_vectors_mmap = mmap_file_optional(&dense_blob_path)?;
        let node_sparse_vectors_mmap = mmap_file_optional(&sparse_blob_path)?;
        let dense_hnsw_meta_mmap = mmap_file_optional(&dense_hnsw_meta_path)?;
        let dense_hnsw_graph_mmap = mmap_file_optional(&dense_hnsw_graph_path)?;
        let sparse_posting_index_mmap = mmap_file_optional(&sparse_posting_index_path)?;
        let sparse_postings_mmap = mmap_file_optional(&sparse_postings_path)?;
        let timestamp_index_mmap = mmap_file(&seg_dir.join("timestamp_index.dat"))?;

        let (deleted_nodes, deleted_edges) = load_tombstones(&seg_dir.join("tombstones.dat"))?;

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
        let node_meta_count = if node_meta_mmap.len() >= 8 {
            read_u64_at(&node_meta_mmap, 0)?
        } else {
            0
        };

        if format_version < 6
            && (!node_vector_meta_mmap.is_empty()
                || !node_dense_vectors_mmap.is_empty()
                || !node_sparse_vectors_mmap.is_empty())
        {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} has unexpected vector sidecars for format version {}",
                segment_id, format_version
            )));
        }
        if format_version < 7
            && (!dense_hnsw_meta_mmap.is_empty() || !dense_hnsw_graph_mmap.is_empty())
        {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} has unexpected dense HNSW files for format version {}",
                segment_id, format_version
            )));
        }
        if format_version < 8
            && (!sparse_posting_index_mmap.is_empty() || !sparse_postings_mmap.is_empty())
        {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} has unexpected sparse posting files for format version {}",
                segment_id, format_version
            )));
        }

        let vector_summary = validate_node_vector_sidecars(
            segment_id,
            &node_vector_meta_mmap,
            &node_dense_vectors_mmap,
            &node_sparse_vectors_mmap,
            node_meta_count,
        )?;
        let dense_hnsw_header = validate_dense_hnsw_files(
            &dense_hnsw_meta_mmap,
            &dense_hnsw_graph_mmap,
            node_dense_vectors_mmap.len(),
            vector_summary.dense_count,
            dense_config,
        )?;
        let dense_hnsw_points = if let Some(header) = dense_hnsw_header {
            load_dense_hnsw_query_points(&dense_hnsw_meta_mmap, header)?
        } else {
            Vec::new()
        };
        if format_version < 8 && vector_summary.sparse_count > 0 {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} format version {} predates sparse posting support; sparse-bearing segments must be rewritten as v8",
                segment_id, format_version
            )));
        }
        validate_sparse_posting_files(
            &sparse_posting_index_mmap,
            &sparse_postings_mmap,
            vector_summary.sparse_count,
            true,
        )?;
        validate_sparse_posting_parity(
            segment_id,
            &node_meta_mmap,
            &node_vector_meta_mmap,
            &node_sparse_vectors_mmap,
            &sparse_posting_index_mmap,
            &sparse_postings_mmap,
        )?;

        Ok(SegmentReader {
            segment_id,
            format_version,
            nodes_mmap,
            edges_mmap,
            adj_out_idx,
            adj_out_dat,
            adj_in_idx,
            adj_in_dat,
            key_index_mmap,
            node_type_index_mmap,
            edge_type_index_mmap,
            prop_node_index_mmap,
            edge_triple_index_mmap,
            node_meta_mmap,
            edge_meta_mmap,
            node_prop_hashes_mmap,
            node_vector_meta_mmap,
            node_dense_vectors_mmap,
            node_sparse_vectors_mmap,
            dense_hnsw_meta_mmap,
            dense_hnsw_graph_mmap,
            dense_hnsw_header,
            dense_hnsw_points,
            sparse_posting_index_mmap,
            sparse_postings_mmap,
            timestamp_index_mmap,
            deleted_nodes,
            deleted_edges,
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
        // Hydrate last_write_seq from metadata sidecar
        let (_, _, _, _, _, _, _, _, _, last_write_seq) = self.node_meta_at(index)?;
        node.last_write_seq = last_write_seq;
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
        // Hydrate last_write_seq from metadata sidecar
        let (_, _, _, _, _, _, _, _, _, _, last_write_seq) = self.edge_meta_at(index)?;
        edge.last_write_seq = last_write_seq;
        Ok(Some(edge))
    }

    /// Get the fixed-width edge fields needed for engine-side cache updates
    /// without decoding properties.
    /// Returns (from, to, created_at, weight, valid_from, valid_to).
    #[allow(clippy::type_complexity)]
    pub(crate) fn get_edge_core(
        &self,
        id: u64,
    ) -> Result<Option<(u64, u64, i64, f32, i64, i64)>, EngineError> {
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
        let weight = read_f32_at(data, offset + 36)?;
        let valid_from = read_i64_at(data, offset + 40)?;
        let valid_to = read_i64_at(data, offset + 48)?;
        Ok(Some((from, to, created_at, weight, valid_from, valid_to)))
    }

    /// Look up a node by (type_id, key). Returns None if not found or tombstoned.
    pub fn node_by_key(&self, type_id: u32, key: &str) -> Result<Option<NodeRecord>, EngineError> {
        let node_id = match self.binary_search_key_index(type_id, key)? {
            Some(id) => id,
            None => return Ok(None),
        };
        self.get_node(node_id)
    }

    /// Batch resolve (type_id, key) pairs to node records using the key index.
    ///
    /// `lookups` must be sorted by `(type_id, key)`. Each entry is
    /// `(orig_idx, type_id, key)`. Found (non-tombstoned) records are written
    /// into `results[orig_idx]`. Returns the set of `orig_idx` values that
    /// were found in this segment's key index (regardless of tombstone status),
    /// so the caller can remove them from further searching.
    ///
    /// Two-phase design:
    ///   Phase 1: resolve keys -> node_ids via key_index (dual-strategy)
    ///   Phase 2: batch-fetch node records via get_nodes_batch (one sorted
    ///             merge-walk through nodes.dat, not N binary searches)
    pub fn resolve_keys_batch(
        &self,
        lookups: &[(usize, u32, &str)],
        results: &mut [Option<NodeRecord>],
    ) -> Result<Vec<usize>, EngineError> {
        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: key_index → (orig_idx, node_id) pairs
        let resolved = self.resolve_keys_to_ids(lookups)?;
        if resolved.is_empty() {
            return Ok(Vec::new());
        }

        let found_indices: Vec<usize> = resolved.iter().map(|&(orig_idx, _)| orig_idx).collect();

        // Phase 2: batch-fetch node records from nodes.dat
        // Build (orig_idx, node_id) sorted by node_id for get_nodes_batch.
        let mut node_lookups: Vec<(usize, u64)> = resolved
            .iter()
            .filter(|&&(_, nid)| !self.deleted_nodes.contains_key(&nid))
            .copied()
            .collect();
        node_lookups.sort_unstable_by_key(|&(_, nid)| nid);
        self.get_nodes_batch(&node_lookups, results)?;

        Ok(found_indices)
    }

    /// Phase 1 of resolve_keys_batch: walk the key index to map each
    /// (type_id, key) query to a node_id. Returns (orig_idx, node_id) pairs
    /// for keys found in this segment's key index.
    fn resolve_keys_to_ids(
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
            for &(orig_idx, type_id, key) in lookups {
                let node_id = if prev_query == Some((type_id, key)) {
                    prev_node_id
                } else {
                    let found = self.binary_search_key_index(type_id, key)?;
                    prev_query = Some((type_id, key));
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
            for &(orig_idx, type_id, key) in lookups {
                while idx_pos < count {
                    let entry_offset =
                        read_u64_at(data, offset_table_start + idx_pos * 8)? as usize;
                    let entry_type = read_u32_at(data, entry_offset)?;
                    let key_len = read_u16_at(data, entry_offset + 12)? as usize;
                    let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
                    let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
                        EngineError::CorruptRecord(format!(
                            "invalid UTF-8 in key index at offset {}",
                            entry_offset + 14
                        ))
                    })?;

                    match (entry_type, entry_key).cmp(&(type_id, key)) {
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
    pub fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        let mut results = Vec::new();

        match direction {
            Direction::Outgoing => {
                self.collect_adj_neighbors(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_id,
                    type_filter,
                    limit,
                    &mut results,
                )?;
            }
            Direction::Incoming => {
                self.collect_adj_neighbors(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_id,
                    type_filter,
                    limit,
                    &mut results,
                )?;
            }
            Direction::Both => {
                self.collect_adj_neighbors(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_id,
                    type_filter,
                    limit,
                    &mut results,
                )?;
                let remaining = if limit > 0 {
                    limit.saturating_sub(results.len())
                } else {
                    0 // 0 means no limit
                };
                if limit == 0 || remaining > 0 {
                    self.collect_adj_neighbors(
                        &self.adj_in_idx,
                        &self.adj_in_dat,
                        node_id,
                        type_filter,
                        remaining,
                        &mut results,
                    )?;
                }
                // Deduplicate by edge_id (self-loops appear in both)
                let mut seen = HashSet::new();
                results.retain(|e| seen.insert(e.edge_id));
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
                    let (_, _, _, _, _, _, _, _, _, lws) = self.node_meta_at(index)?;
                    node.last_write_seq = lws;
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
                        let (_, _, _, _, _, _, _, _, _, lws) = self.node_meta_at(idx_pos)?;
                        node.last_write_seq = lws;
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
    /// `(type_id, updated_at, weight)`.
    pub(crate) fn get_node_meta_batch(
        &self,
        lookups: &[(usize, u64)],
        results: &mut [Option<(u32, i64, f32)>],
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
            count,
            NODE_INDEX_ENTRY_SIZE,
            0,
            unique_keys,
            min_key,
            max_key,
        )?;

        if strategy == BatchReadStrategy::SeekPerKey {
            let mut prev_id: Option<u64> = None;
            let mut prev_meta: Option<(u32, i64, f32)> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains_key(&target_id) {
                    continue;
                }
                let meta = if prev_id == Some(target_id) {
                    prev_meta
                } else if let Some((index, _offset)) = self.binary_search_node_index(target_id)? {
                    let (
                        _node_id,
                        _data_offset,
                        _data_len,
                        type_id,
                        updated_at,
                        weight,
                        _key_len,
                        _prop_hash_offset,
                        _prop_hash_count,
                        _last_write_seq,
                    ) = self.node_meta_at(index)?;
                    let found = Some((type_id, updated_at, weight));
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
                    let entry_off = idx_start + idx_pos * NODE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let (
                            _node_id,
                            _data_offset,
                            _data_len,
                            type_id,
                            updated_at,
                            weight,
                            _key_len,
                            _prop_hash_offset,
                            _prop_hash_count,
                            _last_write_seq,
                        ) = self.node_meta_at(idx_pos)?;
                        results[orig_idx] = Some((type_id, updated_at, weight));
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
        F: FnMut(u32, i64, f32) -> bool,
    {
        if ids.is_empty() {
            return Ok(());
        }
        let data = &self.nodes_mmap[..];
        let node_meta = &self.node_meta_mmap[..];
        let vector_meta = &self.node_vector_meta_mmap[..];
        if data.len() < 8 {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        }

        let idx_start = 8;
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
            count,
            NODE_INDEX_ENTRY_SIZE,
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
                } else if let Some((index, _offset)) = self.binary_search_node_index(target_id)? {
                    let found = Some(read_dense_scoring_meta(node_meta, vector_meta, index)?);
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
                if found.dense_len == 0 || !include(found.type_id, found.updated_at, found.weight) {
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
                    let entry_off = idx_start + idx_pos * NODE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        found = Some(read_dense_scoring_meta(node_meta, vector_meta, idx_pos)?);
                        break;
                    } else {
                        break;
                    }
                }

                let Some(found) = found else {
                    remaining_out.push(target_id);
                    continue;
                };
                if found.dense_len == 0 || !include(found.type_id, found.updated_at, found.weight) {
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
        F: FnMut(u32, i64, f32) -> bool,
    {
        if ids.is_empty() || query.is_empty() {
            return Ok(());
        }
        let data = &self.nodes_mmap[..];
        let node_meta = &self.node_meta_mmap[..];
        let vector_meta = &self.node_vector_meta_mmap[..];
        let sparse_blob = &self.node_sparse_vectors_mmap[..];
        if data.len() < 8 {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            remaining_out.extend_from_slice(ids);
            return Ok(());
        }

        let idx_start = 8;
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
            count,
            NODE_INDEX_ENTRY_SIZE,
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
                } else if let Some((index, _offset)) = self.binary_search_node_index(target_id)? {
                    let found = Some(read_sparse_scoring_meta(node_meta, vector_meta, index)?);
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
                if found.sparse_len == 0 || !include(found.type_id, found.updated_at, found.weight)
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
                    let entry_off = idx_start + idx_pos * NODE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        found = Some(read_sparse_scoring_meta(node_meta, vector_meta, idx_pos)?);
                        break;
                    } else {
                        break;
                    }
                }

                let Some(found) = found else {
                    remaining_out.push(target_id);
                    continue;
                };
                if found.sparse_len == 0 || !include(found.type_id, found.updated_at, found.weight)
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

    /// Check if an edge ID is tombstoned in this segment.
    pub fn is_edge_deleted(&self, id: u64) -> bool {
        self.deleted_edges.contains_key(&id)
    }

    /// Check if a node ID exists (has a live record) in this segment's index.
    /// Does NOT check tombstones; only checks whether the node index contains this ID.
    pub fn has_node(&self, id: u64) -> bool {
        self.binary_search_node_index(id).ok().flatten().is_some()
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

    /// Enumerate unique node IDs that appear in this segment's adjacency indexes.
    /// Scans both adj_out and adj_in index files and returns the union of node IDs.
    /// Used by degree cache rebuild to find adjacency-bearing nodes without
    /// enumerating all visible node records.
    pub fn adj_node_ids(&self) -> Result<NodeIdSet, EngineError> {
        let mut ids = NodeIdSet::default();
        Self::collect_adj_index_node_ids(&self.adj_out_idx, &mut ids)?;
        Self::collect_adj_index_node_ids(&self.adj_in_idx, &mut ids)?;
        Ok(ids)
    }

    /// Extract unique node IDs from a single adjacency index file.
    /// Index format: [count: u64] then count entries of ADJ_INDEX_ENTRY_SIZE,
    /// each starting with [node_id: u64]. Entries are sorted by node_id so
    /// consecutive duplicates (same node, different type_id) are common.
    fn collect_adj_index_node_ids(
        idx_mmap: &MappedData,
        out: &mut NodeIdSet,
    ) -> Result<(), EngineError> {
        let idx_data = &idx_mmap[..];
        if idx_data.len() < 8 {
            return Ok(());
        }
        let count = read_u64_at(idx_data, 0)? as usize;
        let idx_start = 8;
        let mut prev_node: u64 = u64::MAX;
        for i in 0..count {
            let entry_off = idx_start + i * ADJ_INDEX_ENTRY_SIZE;
            let node_id = read_u64_at(idx_data, entry_off)?;
            // Index is sorted by node_id; skip consecutive duplicates
            if node_id != prev_node {
                out.insert(node_id);
                prev_node = node_id;
            }
        }
        Ok(())
    }

    pub fn node_count(&self) -> u64 {
        self.node_count
    }

    pub fn edge_count(&self) -> u64 {
        self.edge_count
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

    /// Raw mmap bytes for nodes.dat (used by V3 compaction for raw binary copy).
    pub(crate) fn raw_nodes_mmap(&self) -> &[u8] {
        &self.nodes_mmap[..]
    }

    /// Raw mmap bytes for edges.dat (used by V3 compaction for raw binary copy).
    pub(crate) fn raw_edges_mmap(&self) -> &[u8] {
        &self.edges_mmap[..]
    }

    // --- V5 metadata sidecar accessors (for V3 compaction) ---

    /// Number of node metadata entries.
    pub(crate) fn node_meta_count(&self) -> u64 {
        let data = &self.node_meta_mmap[..];
        if data.len() < 8 {
            return 0;
        }
        read_u64_at(data, 0).unwrap_or(0)
    }

    /// Read a node metadata entry by index (0-based).
    /// Returns (node_id, data_offset, data_len, type_id, updated_at, weight, key_len,
    ///          prop_hash_offset, prop_hash_count, last_write_seq).
    #[allow(clippy::type_complexity)]
    pub(crate) fn node_meta_at(
        &self,
        index: usize,
    ) -> Result<(u64, u64, u32, u32, i64, f32, u16, u64, u32, u64), EngineError> {
        let data = &self.node_meta_mmap[..];
        let off = 8 + index * NODE_META_ENTRY_SIZE;
        let node_id = read_u64_at(data, off)?;
        let data_offset = read_u64_at(data, off + 8)?;
        let data_len = read_u32_at(data, off + 16)?;
        let type_id = read_u32_at(data, off + 20)?;
        let updated_at = read_i64_at(data, off + 24)?;
        let weight = read_f32_at(data, off + 32)?;
        let key_len = read_u16_at(data, off + 36)?;
        let prop_hash_offset = read_u64_at(data, off + 38)?;
        let prop_hash_count = read_u32_at(data, off + 46)?;
        let last_write_seq = read_u64_at(data, off + 50)?;
        Ok((
            node_id,
            data_offset,
            data_len,
            type_id,
            updated_at,
            weight,
            key_len,
            prop_hash_offset,
            prop_hash_count,
            last_write_seq,
        ))
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
    /// Returns (edge_id, data_offset, data_len, from, to, type_id, updated_at,
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
        let type_id = read_u32_at(data, off + 36)?;
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
            type_id,
            updated_at,
            weight,
            valid_from,
            valid_to,
            last_write_seq,
        ))
    }

    /// Raw mmap bytes for node_prop_hashes.dat (used by V3 compaction).
    pub(crate) fn raw_node_prop_hashes_mmap(&self) -> &[u8] {
        &self.node_prop_hashes_mmap[..]
    }

    pub(crate) fn raw_node_dense_vectors_mmap(&self) -> &[u8] {
        &self.node_dense_vectors_mmap[..]
    }

    pub(crate) fn raw_node_sparse_vectors_mmap(&self) -> &[u8] {
        &self.node_sparse_vectors_mmap[..]
    }

    pub(crate) fn raw_sparse_posting_index_mmap(&self) -> &[u8] {
        &self.sparse_posting_index_mmap[..]
    }

    pub(crate) fn raw_sparse_postings_mmap(&self) -> &[u8] {
        &self.sparse_postings_mmap[..]
    }

    pub(crate) fn dense_hnsw_header(&self) -> Option<DenseHnswHeader> {
        self.dense_hnsw_header
    }

    pub(crate) fn search_dense_hnsw(
        &self,
        query: &[f32],
        ef_search: usize,
        limit: usize,
    ) -> Result<Vec<(u64, f32)>, EngineError> {
        let Some(header) = self.dense_hnsw_header else {
            return Ok(Vec::new());
        };
        search_dense_hnsw_with_points(
            header,
            &self.dense_hnsw_points,
            &self.dense_hnsw_graph_mmap,
            &self.node_dense_vectors_mmap,
            query,
            ef_search,
            limit,
        )
    }

    pub(crate) fn search_dense_hnsw_scoped(
        &self,
        query: &[f32],
        ef_search: usize,
        limit: usize,
        scope_ids: &crate::types::NodeIdSet,
    ) -> Result<Vec<(u64, f32)>, EngineError> {
        let Some(header) = self.dense_hnsw_header else {
            return Ok(Vec::new());
        };
        search_dense_hnsw_scoped_with_points(
            header,
            &self.dense_hnsw_points,
            &self.dense_hnsw_graph_mmap,
            &self.node_dense_vectors_mmap,
            query,
            ef_search,
            limit,
            scope_ids,
        )
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
            let mut values = Vec::with_capacity(dense_len as usize);
            let base = dense_offset as usize;
            for i in 0..dense_len as usize {
                values.push(read_f32_at(
                    &self.node_dense_vectors_mmap,
                    base + i * DENSE_VECTOR_VALUE_SIZE,
                )?);
            }
            node.dense_vector = Some(values);
        }

        if sparse_len > 0 {
            let mut values = Vec::with_capacity(sparse_len as usize);
            let base = sparse_offset as usize;
            for i in 0..sparse_len as usize {
                let entry_off = base + i * SPARSE_VECTOR_ENTRY_SIZE;
                let dimension_id = read_u32_at(&self.node_sparse_vectors_mmap, entry_off)?;
                let weight = read_f32_at(&self.node_sparse_vectors_mmap, entry_off + 4)?;
                values.push((dimension_id, weight));
            }
            node.sparse_vector = Some(values);
        }

        Ok(())
    }

    // --- Iteration methods (for compaction) ---

    /// Collect all node records in this segment (including tombstoned ones).
    /// Returns records in index order (sorted by node_id).
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

    /// Collect all edge records in this segment (including tombstoned ones).
    /// Returns records in index order (sorted by edge_id).
    pub fn all_edges(&self) -> Result<Vec<EdgeRecord>, EngineError> {
        let data = &self.edges_mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        let idx_start = 8;
        let mut edges = Vec::with_capacity(count);
        for i in 0..count {
            let entry_off = idx_start + i * EDGE_INDEX_ENTRY_SIZE;
            let id = read_u64_at(data, entry_off)?;
            let offset = read_u64_at(data, entry_off + 8)? as usize;
            edges.push(decode_edge_at(data, offset, id)?);
        }
        Ok(edges)
    }

    // --- Type index queries ---

    /// Return node IDs for a given type_id from this segment's type index.
    /// Excludes tombstoned nodes.
    pub fn nodes_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        self.query_type_index(&self.node_type_index_mmap, type_id, &self.deleted_nodes)
    }

    /// Return edge IDs for a given type_id from this segment's type index.
    /// Excludes tombstoned edges.
    pub fn edges_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        self.query_type_index(&self.edge_type_index_mmap, type_id, &self.deleted_edges)
    }

    /// Binary search a type index file for a given type_id.
    /// Returns record IDs, excluding any in the deleted set.
    fn query_type_index(
        &self,
        mmap: &MappedData,
        target_type: u32,
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

        // Binary search the index section for target_type
        let idx_start = 8;
        // Entry: type_id (4) + offset (8) + count (4) = 16 bytes
        let entry_size = TYPE_INDEX_ENTRY_SIZE;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * entry_size;
            let entry_type = read_u32_at(data, entry_off)?;
            if entry_type < target_type {
                lo = mid + 1;
            } else if entry_type > target_type {
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

    /// Return all distinct node type IDs present in this segment's type index.
    pub fn node_type_ids(&self) -> Result<Vec<u32>, EngineError> {
        Self::type_ids_from_index(&self.node_type_index_mmap)
    }

    /// Return all distinct edge type IDs present in this segment's type index.
    pub fn edge_type_ids(&self) -> Result<Vec<u32>, EngineError> {
        Self::type_ids_from_index(&self.edge_type_index_mmap)
    }

    /// Extract all type IDs from a type index mmap header.
    fn type_ids_from_index(mmap: &MappedData) -> Result<Vec<u32>, EngineError> {
        let data = &mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        let mut result = Vec::with_capacity(count);
        let idx_start = 8;
        for i in 0..count {
            let entry_off = idx_start + i * TYPE_INDEX_ENTRY_SIZE;
            result.push(read_u32_at(data, entry_off)?);
        }
        Ok(result)
    }

    // --- Timestamp index queries ---

    /// Return node IDs within a time range for a given type_id.
    /// Binary search for range start, scan to range end. O(log N + results).
    /// Results are sorted by node_id for K-way merge compatibility.
    pub fn nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let data = &self.timestamp_index_mmap[..];
        if data.len() < 8 {
            return Err(EngineError::CorruptRecord(
                "timestamp_index.dat missing or truncated (< 8 bytes)".into(),
            ));
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(Vec::new());
        }

        let entry_start = 8usize;
        let entry_size = 20usize; // type_id(4) + updated_at(8) + node_id(8)

        // Binary search for the first entry >= (type_id, from_ms, 0)
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = entry_start + mid * entry_size;
            let e_type = read_u32_at(data, off)?;
            let e_time = read_i64_at(data, off + 4)?;
            if (e_type, e_time) < (type_id, from_ms) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Scan from lo until type_id changes or updated_at > to_ms
        let mut result = Vec::new();
        let mut pos = lo;
        while pos < count {
            let off = entry_start + pos * entry_size;
            let e_type = read_u32_at(data, off)?;
            if e_type != type_id {
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

    // --- Edge triple index ---

    /// Look up an edge by (from, to, type_id) triple. Returns the edge record
    /// if found and not tombstoned, or None.
    pub fn edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
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
            let e_type = read_u32_at(data, off + 16)?;

            match e_from.cmp(&from) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => match e_to.cmp(&to) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => match e_type.cmp(&type_id) {
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

    // --- Property index queries ---

    /// Find candidate node IDs matching (type_id, key_hash, value_hash) from
    /// this segment's property index. Excludes tombstoned nodes.
    /// Returns candidate IDs. Caller must post-filter with actual property values.
    pub fn find_nodes_by_prop_hash(
        &self,
        type_id: u32,
        key_hash: u64,
        value_hash: u64,
    ) -> Result<Vec<u64>, EngineError> {
        let data = &self.prop_node_index_mmap[..];
        if data.len() < 8 {
            return Ok(Vec::new());
        }
        let count = read_u64_at(data, 0)? as usize;
        if count == 0 {
            return Ok(Vec::new());
        }

        // Binary search the index for (type_id, key_hash, value_hash)
        let idx_start = 8;
        let mut lo = 0usize;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off = idx_start + mid * PROP_INDEX_ENTRY_SIZE;
            let e_type = read_u32_at(data, entry_off)?;
            let e_key_hash = read_u64_at(data, entry_off + 4)?;
            let e_val_hash = read_u64_at(data, entry_off + 12)?;

            match e_type.cmp(&type_id) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => match e_key_hash.cmp(&key_hash) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => match e_val_hash.cmp(&value_hash) {
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid,
                        std::cmp::Ordering::Equal => {
                            // Found, read candidate IDs
                            let offset = read_u64_at(data, entry_off + 20)? as usize;
                            let id_count = read_u32_at(data, entry_off + 28)? as usize;
                            let mut result = Vec::with_capacity(id_count);
                            for i in 0..id_count {
                                let id = read_u64_at(data, offset + i * 8)?;
                                if !self.deleted_nodes.contains_key(&id) {
                                    result.push(id);
                                }
                            }
                            return Ok(result);
                        }
                    },
                },
            }
        }

        Ok(Vec::new())
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

    /// Binary search the key index for a (type_id, key) pair.
    /// Returns the node_id if found, or None.
    fn binary_search_key_index(
        &self,
        target_type: u32,
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

            // Read entry: type_id (4) + node_id (8) + key_len (2) + key
            let entry_type = read_u32_at(data, entry_offset)?;
            let key_len = read_u16_at(data, entry_offset + 12)? as usize;
            let key_bytes = read_bytes_at(data, entry_offset + 14, key_len)?;
            let entry_key = std::str::from_utf8(key_bytes).map_err(|_| {
                EngineError::CorruptRecord(format!(
                    "invalid UTF-8 in key index at offset {}",
                    entry_offset + 14
                ))
            })?;

            match entry_type.cmp(&target_type) {
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
    fn collect_adj_neighbors(
        &self,
        idx_mmap: &MappedData,
        dat_mmap: &MappedData,
        node_id: u64,
        type_filter: Option<&[u32]>,
        limit: usize,
        results: &mut Vec<NeighborEntry>,
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
            if limit > 0 && results.len() >= limit {
                break;
            }

            let entry_off = idx_start + i * ADJ_INDEX_ENTRY_SIZE;
            let entry_node = read_u64_at(idx_data, entry_off)?;
            if entry_node != node_id {
                break;
            }

            let entry_type = read_u32_at(idx_data, entry_off + 8)?;
            let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
            let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

            if let Some(types) = type_filter {
                if !types.contains(&entry_type) {
                    continue;
                }
            }

            // Decode delta-encoded postings sequentially
            let mut cur_off = posting_offset;
            let mut prev_edge_id: u64 = 0;

            for _j in 0..posting_count {
                if limit > 0 && results.len() >= limit {
                    break;
                }

                let (delta, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let edge_id = prev_edge_id + delta;
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

                results.push(NeighborEntry {
                    node_id: neighbor_id,
                    edge_id,
                    edge_type_id: entry_type,
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
    /// to avoid materializing `Vec<NeighborEntry>`.
    ///
    /// Callback receives `(edge_id, neighbor_id, weight, valid_from, valid_to)`.
    /// For `Direction::Both`, self-loops may invoke the callback twice. Caller
    /// handles dedup.
    pub fn for_each_adj_posting<F>(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
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
                type_filter,
                callback,
            ),
            Direction::Incoming => self.decode_adj_postings_cb(
                &self.adj_in_idx,
                &self.adj_in_dat,
                node_id,
                type_filter,
                callback,
            ),
            Direction::Both => {
                if self
                    .decode_adj_postings_cb(
                        &self.adj_out_idx,
                        &self.adj_out_dat,
                        node_id,
                        type_filter,
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
                    type_filter,
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
        type_filter: Option<&[u32]>,
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

            let entry_type = read_u32_at(idx_data, entry_off + 8)?;
            let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
            let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

            if let Some(types) = type_filter {
                if !types.contains(&entry_type) {
                    continue;
                }
            }

            let mut cur_off = posting_offset;
            let mut prev_edge_id: u64 = 0;

            for _ in 0..posting_count {
                let (delta, n) = read_varint_at(dat_data, cur_off)?;
                cur_off += n;
                let edge_id = prev_edge_id + delta;
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
    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
    ) -> Result<NodeIdMap<Vec<NeighborEntry>>, EngineError> {
        let mut results: NodeIdMap<Vec<NeighborEntry>> =
            NodeIdMap::with_capacity_and_hasher(node_ids.len(), Default::default());

        match direction {
            Direction::Outgoing => {
                self.collect_adj_neighbors_batch(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_ids,
                    type_filter,
                    &mut results,
                )?;
            }
            Direction::Incoming => {
                self.collect_adj_neighbors_batch(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_ids,
                    type_filter,
                    &mut results,
                )?;
            }
            Direction::Both => {
                self.collect_adj_neighbors_batch(
                    &self.adj_out_idx,
                    &self.adj_out_dat,
                    node_ids,
                    type_filter,
                    &mut results,
                )?;
                self.collect_adj_neighbors_batch(
                    &self.adj_in_idx,
                    &self.adj_in_dat,
                    node_ids,
                    type_filter,
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
        type_filter: Option<&[u32]>,
        results: &mut NodeIdMap<Vec<NeighborEntry>>,
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

                let entry_type = read_u32_at(idx_data, entry_off + 8)?;
                let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
                let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

                idx_pos += 1;

                if let Some(types) = type_filter {
                    if !types.contains(&entry_type) {
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
                    let edge_id = prev_edge_id + delta;
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

                    entries.push(NeighborEntry {
                        node_id: neighbor_id,
                        edge_id,
                        edge_type_id: entry_type,
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
    /// callback instead of building `Vec<NeighborEntry>`.
    ///
    /// `node_ids` must be sorted and deduplicated. For `Direction::Both`, self-loops
    /// may invoke the callback twice per edge. Caller handles dedup.
    ///
    /// Callback receives `(queried_node_id, edge_id, neighbor_id, weight, valid_from, valid_to)`.
    pub fn for_each_adj_posting_batch<F>(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
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
                type_filter,
                callback,
            ),
            Direction::Incoming => self.decode_adj_postings_batch_cb(
                &self.adj_in_idx,
                &self.adj_in_dat,
                node_ids,
                type_filter,
                callback,
            ),
            Direction::Both => {
                if self
                    .decode_adj_postings_batch_cb(
                        &self.adj_out_idx,
                        &self.adj_out_dat,
                        node_ids,
                        type_filter,
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
                    type_filter,
                    callback,
                )
            }
        }
    }

    /// Batch decode adjacency postings from one index+data file pair using the
    /// adaptive cost model. Invokes the callback for each non-tombstoned posting.
    fn decode_adj_postings_batch_cb<F>(
        &self,
        idx_mmap: &MappedData,
        dat_mmap: &MappedData,
        node_ids: &[u64],
        type_filter: Option<&[u32]>,
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

                let entry_type = read_u32_at(idx_data, entry_off + 8)?;
                let posting_offset = read_u64_at(idx_data, entry_off + 12)? as usize;
                let posting_count = read_u32_at(idx_data, entry_off + 20)? as usize;

                idx_pos += 1;

                if let Some(types) = type_filter {
                    if !types.contains(&entry_type) {
                        continue;
                    }
                }

                let mut cur_off = posting_offset;
                let mut prev_edge_id: u64 = 0;

                for _ in 0..posting_count {
                    let (delta, n) = read_varint_at(dat_data, cur_off)?;
                    cur_off += n;
                    let edge_id = prev_edge_id + delta;
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

/// Validate the segment format version file. If the file is absent (pre-version
/// segment), this is allowed. If present, the magic and version must match.
/// Validate and return the segment format version.
/// Returns 0 for pre-version segments (no format.ver file).
fn read_format_version(seg_dir: &Path) -> Result<u32, EngineError> {
    let path = seg_dir.join("format.ver");
    if !path.exists() {
        return Err(EngineError::CorruptRecord(
            "segment format version 0 is too old (minimum supported: 5)".into(),
        ));
    }
    let data = std::fs::read(&path)?;
    if data.len() != 8 {
        return Err(EngineError::CorruptRecord(format!(
            "format.ver has invalid size {} (expected 8)",
            data.len()
        )));
    }
    if data[..4] != SEGMENT_MAGIC {
        return Err(EngineError::CorruptRecord(format!(
            "format.ver has invalid magic {:?} (expected {:?})",
            &data[..4],
            SEGMENT_MAGIC
        )));
    }
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version < 5 {
        return Err(EngineError::CorruptRecord(format!(
            "segment format version {} is too old (minimum supported: 5)",
            version
        )));
    }
    if version > SEGMENT_FORMAT_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "segment format version {} is newer than supported version {}",
            version, SEGMENT_FORMAT_VERSION
        )));
    }
    Ok(version)
}

/// Memory-map a file, returning Empty if the file doesn't exist.
/// Used for index files that may not be present in older segments.
fn mmap_file_optional(path: &Path) -> Result<MappedData, EngineError> {
    if !path.exists() {
        return Ok(MappedData::Empty);
    }
    mmap_file(path)
}

/// Memory-map a file. Returns MappedData::Empty for zero-byte files.
fn mmap_file(path: &Path) -> Result<MappedData, EngineError> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    if meta.len() == 0 {
        return Ok(MappedData::Empty);
    }
    // SAFETY: Segment files are immutable after write. No concurrent modification.
    let mmap = unsafe { Mmap::map(&file).map_err(EngineError::IoError)? };
    Ok(MappedData::Mmap(mmap))
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

    let expected_len = 8usize
        .checked_add(count as usize * NODE_VECTOR_META_ENTRY_SIZE)
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

    for index in 0..count as usize {
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
            let dense_offset = dense_offset as usize;
            if dense_offset != next_dense_offset {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} dense offset {} does not match expected {}",
                    segment_id, index, dense_offset, next_dense_offset
                )));
            }
            validate_blob_range(
                dense_blob,
                dense_offset as u64,
                dense_len as usize * DENSE_VECTOR_VALUE_SIZE,
                "dense",
                segment_id,
                index,
            )?;
            next_dense_offset = next_dense_offset
                .checked_add(dense_len as usize * DENSE_VECTOR_VALUE_SIZE)
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
            let sparse_offset = sparse_offset as usize;
            if sparse_offset != next_sparse_offset {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} node vector entry {} sparse offset {} does not match expected {}",
                    segment_id, index, sparse_offset, next_sparse_offset
                )));
            }
            validate_blob_range(
                sparse_blob,
                sparse_offset as u64,
                sparse_len as usize * SPARSE_VECTOR_ENTRY_SIZE,
                "sparse",
                segment_id,
                index,
            )?;
            next_sparse_offset = next_sparse_offset
                .checked_add(sparse_len as usize * SPARSE_VECTOR_ENTRY_SIZE)
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

fn validate_sparse_posting_parity(
    segment_id: u64,
    node_meta: &[u8],
    vector_meta: &[u8],
    sparse_blob: &[u8],
    sparse_posting_index: &[u8],
    sparse_postings: &[u8],
) -> Result<(), EngineError> {
    if vector_meta.is_empty() {
        if !sparse_posting_index.is_empty() || !sparse_postings.is_empty() {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} has sparse posting files without node vector metadata",
                segment_id
            )));
        }
        return Ok(());
    }

    let mut expected = BTreeMap::<u32, Vec<(u64, f32)>>::new();
    let count = read_u64_at(node_meta, 0)? as usize;
    for index in 0..count {
        let node_id = read_u64_at(node_meta, 8 + index * NODE_META_ENTRY_SIZE)?;
        let (flags, _dense_offset, _dense_len, sparse_offset, sparse_len) =
            read_node_vector_meta_entry(vector_meta, index)?;
        if flags & NODE_VECTOR_FLAG_SPARSE == 0 {
            continue;
        }

        let base = sparse_offset as usize;
        for entry_index in 0..sparse_len as usize {
            let entry_off = base + entry_index * SPARSE_VECTOR_ENTRY_SIZE;
            let dimension_id = read_u32_at(sparse_blob, entry_off)?;
            let weight = read_f32_at(sparse_blob, entry_off + 4)?;
            if weight < 0.0 {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} sparse vector payload for node {} dimension {} has negative weight",
                    segment_id, node_id, dimension_id
                )));
            }
            expected
                .entry(dimension_id)
                .or_default()
                .push((node_id, weight));
        }
    }

    let actual = read_sparse_posting_groups(sparse_posting_index, sparse_postings)?;
    if expected.len() != actual.len() {
        return Err(EngineError::CorruptRecord(format!(
            "segment {} sparse posting dimension count {} does not match sparse vector payload count {}",
            segment_id,
            actual.len(),
            expected.len()
        )));
    }

    for (dimension_id, expected_postings) in &expected {
        let Some(actual_postings) = actual.get(dimension_id) else {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} sparse posting files are missing dimension {} from sparse vectors",
                segment_id, dimension_id
            )));
        };
        if expected_postings.len() != actual_postings.len() {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} sparse posting dimension {} count {} does not match sparse vector payload count {}",
                segment_id,
                dimension_id,
                actual_postings.len(),
                expected_postings.len()
            )));
        }
        for (expected_posting, actual_posting) in
            expected_postings.iter().zip(actual_postings.iter())
        {
            if expected_posting.0 != actual_posting.0
                || expected_posting.1.to_bits() != actual_posting.1.to_bits()
            {
                return Err(EngineError::CorruptRecord(format!(
                    "segment {} sparse posting dimension {} does not match sparse vector payloads",
                    segment_id, dimension_id
                )));
            }
        }
    }

    for dimension_id in actual.keys() {
        if !expected.contains_key(dimension_id) {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} sparse posting dimension {} is not present in sparse vector payloads",
                segment_id, dimension_id
            )));
        }
    }

    Ok(())
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
    vector_meta: &[u8],
    index: usize,
) -> Result<DenseScoringMeta, EngineError> {
    let node_off = 8 + index * NODE_META_ENTRY_SIZE;
    let node_end = node_off
        .checked_add(NODE_META_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("node meta offset overflow".into()))?;
    let node_entry = node_meta.get(node_off..node_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "node meta read at index {} exceeds data length {}",
            index,
            node_meta.len()
        ))
    })?;

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
        type_id: u32::from_le_bytes(node_entry[20..24].try_into().unwrap()),
        updated_at: i64::from_le_bytes(node_entry[24..32].try_into().unwrap()),
        weight: f32::from_le_bytes(node_entry[32..36].try_into().unwrap()),
        dense_offset: u64::from_le_bytes(vector_entry[4..12].try_into().unwrap()) as usize,
        dense_len: u32::from_le_bytes(vector_entry[12..16].try_into().unwrap()) as usize,
    })
}

fn read_sparse_scoring_meta(
    node_meta: &[u8],
    vector_meta: &[u8],
    index: usize,
) -> Result<SparseScoringMeta, EngineError> {
    let node_off = 8 + index * NODE_META_ENTRY_SIZE;
    let node_end = node_off
        .checked_add(NODE_META_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("node meta offset overflow".into()))?;
    let node_entry = node_meta.get(node_off..node_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "node meta read at index {} exceeds data length {}",
            index,
            node_meta.len()
        ))
    })?;

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
        type_id: u32::from_le_bytes(node_entry[20..24].try_into().unwrap()),
        updated_at: i64::from_le_bytes(node_entry[24..32].try_into().unwrap()),
        weight: f32::from_le_bytes(node_entry[32..36].try_into().unwrap()),
        sparse_offset: u64::from_le_bytes(vector_entry[16..24].try_into().unwrap()) as usize,
        sparse_len: u32::from_le_bytes(vector_entry[24..28].try_into().unwrap()) as usize,
    })
}

/// Decode a NodeRecord from mmap data at a given byte offset.
/// The ID is passed separately; it comes from the index, not the data section.
/// Layout: type_id(4) key_len(2) key(N) created_at(8) updated_at(8) weight(4) props_len(4) props(M)
fn decode_node_at(data: &[u8], offset: usize, id: u64) -> Result<NodeRecord, EngineError> {
    let type_id = read_u32_at(data, offset)?;
    let key_len = read_u16_at(data, offset + 4)? as usize;
    let key_bytes = read_bytes_at(data, offset + 6, key_len)?;
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| {
            EngineError::CorruptRecord(format!(
                "invalid UTF-8 in node key at offset {}",
                offset + 6
            ))
        })?
        .to_string();

    let pos = offset + 6 + key_len;
    let created_at = read_i64_at(data, pos)?;
    let updated_at = read_i64_at(data, pos + 8)?;
    let weight = read_f32_at(data, pos + 16)?;
    let props_len = read_u32_at(data, pos + 20)? as usize;
    let props_bytes = read_bytes_at(data, pos + 24, props_len)?;
    let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(props_bytes).map_err(|e| {
        EngineError::CorruptRecord(format!("node props decode at offset {}: {}", pos + 24, e))
    })?;

    Ok(NodeRecord {
        id,
        type_id,
        key,
        props,
        created_at,
        updated_at,
        weight,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: 0,
    })
}

/// Decode an EdgeRecord from mmap data at a given byte offset.
/// The ID is passed separately; it comes from the index, not the data section.
/// Layout: from(8) to(8) type_id(4) created_at(8) updated_at(8) weight(4) valid_from(8) valid_to(8) props_len(4) props(N)
fn decode_edge_at(data: &[u8], offset: usize, id: u64) -> Result<EdgeRecord, EngineError> {
    let from = read_u64_at(data, offset)?;
    let to = read_u64_at(data, offset + 8)?;
    let type_id = read_u32_at(data, offset + 16)?;
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
        type_id,
        props,
        created_at,
        updated_at,
        weight,
        valid_from,
        valid_to,
        last_write_seq: 0,
    })
}

/// Load tombstone maps from a tombstones.dat file.
fn load_tombstones(
    path: &Path,
) -> Result<(NodeIdMap<TombstoneEntry>, NodeIdMap<TombstoneEntry>), EngineError> {
    let data = std::fs::read(path)?;
    if data.len() < 8 {
        return Ok((NodeIdMap::default(), NodeIdMap::default()));
    }
    let count = read_u64_at(&data, 0)? as usize;

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
        let id = read_u64_at(&data, off + 1)?;
        let deleted_at = read_i64_at(&data, off + 9)?;
        let last_write_seq = read_u64_at(&data, off + 17)?;
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
    use crate::segment_writer::write_segment;

    /// Test-only wrapper to expose read_varint_at for cross-module varint tests.
    pub fn read_varint_at_pub(data: &[u8], offset: usize) -> (u64, usize) {
        read_varint_at(data, offset).unwrap()
    }

    /// Write a valid format.ver file for tests that build manual segment dirs.
    fn write_format_ver(seg_dir: &std::path::Path) {
        use crate::segment_writer::{SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};
        let mut data = Vec::new();
        data.extend_from_slice(&SEGMENT_MAGIC);
        data.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), &data).unwrap();
    }

    fn make_node(id: u64, type_id: u32, key: &str) -> NodeRecord {
        NodeRecord {
            id,
            type_id,
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

    fn make_node_with_props(id: u64, type_id: u32, key: &str) -> NodeRecord {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String(key.to_string()));
        props.insert("score".to_string(), PropValue::Float(0.95));
        NodeRecord {
            id,
            type_id,
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

    fn make_edge(id: u64, from: u64, to: u64, type_id: u32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            type_id,
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
        let reader = SegmentReader::open(&seg_dir, 1, dense_config).unwrap();
        (dir, reader)
    }

    fn dense_config(dimension: u32) -> DenseVectorConfig {
        DenseVectorConfig {
            dimension,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }
    }

    fn build_u64_key_index(keys: &[u64], entry_size: usize, key_offset: usize) -> Vec<u8> {
        let mut data = vec![0u8; 8 + keys.len() * entry_size];
        data[0..8].copy_from_slice(&(keys.len() as u64).to_le_bytes());
        for (i, key) in keys.iter().enumerate() {
            let off = 8 + i * entry_size + key_offset;
            data[off..off + 8].copy_from_slice(&key.to_le_bytes());
        }
        data
    }

    #[test]
    fn test_batch_strategy_prefers_seek_for_tiny_key_count() {
        let keys: Vec<u64> = (1..=10_000).collect();
        let idx = build_u64_key_index(&keys, NODE_INDEX_ENTRY_SIZE, 0);
        let strategy =
            choose_batch_read_strategy(&idx, keys.len(), NODE_INDEX_ENTRY_SIZE, 0, 2, 500, 501)
                .unwrap();
        assert_eq!(strategy, BatchReadStrategy::SeekPerKey);
    }

    #[test]
    fn test_batch_strategy_prefers_merge_for_dense_large_range() {
        let keys: Vec<u64> = (1..=10_000).collect();
        let idx = build_u64_key_index(&keys, NODE_INDEX_ENTRY_SIZE, 0);
        let strategy = choose_batch_read_strategy(
            &idx,
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
        let strategy =
            choose_batch_read_strategy(&idx, keys.len(), NODE_INDEX_ENTRY_SIZE, 0, 64, 100, 9_900)
                .unwrap();
        assert_eq!(strategy, BatchReadStrategy::SeekPerKey);
    }

    // --- get_node ---

    #[test]
    fn test_get_node_found() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(42, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        let node = reader.get_node(42).unwrap().unwrap();
        assert_eq!(node.id, 42);
        assert_eq!(node.type_id, 1);
        assert_eq!(node.key, "alice");
        assert_eq!(node.created_at, 1000);
        assert!((node.weight - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_get_node_not_found() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.get_node(999).unwrap().is_none());
    }

    #[test]
    fn test_get_node_with_properties() {
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
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
    fn test_all_nodes_hydrates_mixed_vectors() {
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let edge = reader.get_edge(100).unwrap().unwrap();
        assert_eq!(edge.id, 100);
        assert_eq!(edge.from, 1);
        assert_eq!(edge.to, 2);
        assert_eq!(edge.type_id, 10);
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
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 0);

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.node_by_key(1, "bob").unwrap().is_none());
        assert!(reader.node_by_key(2, "alice").unwrap().is_none());
    }

    // --- neighbors ---

    #[test]
    fn test_neighbors_outgoing() {
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(2, Direction::Incoming, None, 0).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 1);
    }

    #[test]
    fn test_neighbors_with_type_filter() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 20)), 0);

        let (_dir, reader) = write_and_open(&mt);

        // Filter type 10 only
        let nbrs = reader
            .neighbors(1, Direction::Outgoing, Some(&[10]), 0)
            .unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 2);

        // Filter type 20 only
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
        let mut mt = Memtable::new();
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
    fn test_neighbors_no_adjacency() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "lonely")), 0);

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert!(nbrs.is_empty());
    }

    #[test]
    fn test_for_each_adj_posting_breaks_early() {
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
        for i in 1..=50 {
            mt.apply_op(
                &WalOp::UpsertNode(make_node(i, (i % 3) as u32 + 1, &format!("key_{:04}", i))),
                0,
            );
        }

        let (_dir, reader) = write_and_open(&mt);

        // Every node should be findable by key
        for i in 1..=50 {
            let type_id = (i % 3) as u32 + 1;
            let key = format!("key_{:04}", i);
            let node = reader.node_by_key(type_id, &key).unwrap().unwrap();
            assert_eq!(node.id, i);
        }
    }

    // --- Roundtrip: write segment + read back ---

    #[test]
    fn test_full_segment_roundtrip() {
        let mut mt = Memtable::new();

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

        // Verify type-filtered neighbors
        let out1_t10 = reader
            .neighbors(1, Direction::Outgoing, Some(&[10]), 0)
            .unwrap();
        assert_eq!(out1_t10.len(), 1);
        assert_eq!(out1_t10[0].node_id, 2);

        // Verify tombstones
        assert!(reader.is_node_deleted(99));
        assert!(reader.is_edge_deleted(99));
    }

    // --- Property index roundtrip ---

    #[test]
    fn test_prop_index_roundtrip() {
        use crate::types::{hash_prop_key, hash_prop_value};

        let mut mt = Memtable::new();

        let mut props1 = BTreeMap::new();
        props1.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                type_id: 1,
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
                type_id: 1,
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
                type_id: 1,
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

        let (_dir, reader) = write_and_open(&mt);

        // Query for red nodes
        let key_hash = hash_prop_key("color");
        let val_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let mut reds = reader
            .find_nodes_by_prop_hash(1, key_hash, val_hash)
            .unwrap();
        reds.sort();
        assert_eq!(reds, vec![1, 2]);

        // Query for green nodes
        let val_hash_green = hash_prop_value(&PropValue::String("green".to_string()));
        let greens = reader
            .find_nodes_by_prop_hash(1, key_hash, val_hash_green)
            .unwrap();
        assert_eq!(greens, vec![3]);

        // Non-existent value
        let val_hash_blue = hash_prop_value(&PropValue::String("blue".to_string()));
        assert!(reader
            .find_nodes_by_prop_hash(1, key_hash, val_hash_blue)
            .unwrap()
            .is_empty());

        // Wrong type_id
        assert!(reader
            .find_nodes_by_prop_hash(99, key_hash, val_hash)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_prop_index_excludes_tombstoned() {
        use crate::types::{hash_prop_key, hash_prop_value};

        let mut mt = Memtable::new();

        let mut props = BTreeMap::new();
        props.insert("tag".to_string(), PropValue::String("x".to_string()));
        mt.apply_op(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                type_id: 1,
                key: "a".to_string(),
                props: props.clone(),
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
                type_id: 1,
                key: "b".to_string(),
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

        // Delete node 2 (tombstone)
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 9999,
            },
            0,
        );

        let (_dir, reader) = write_and_open(&mt);

        let key_hash = hash_prop_key("tag");
        let val_hash = hash_prop_value(&PropValue::String("x".to_string()));

        // Node 2 is tombstoned, so only node 1 should be returned
        let results = reader
            .find_nodes_by_prop_hash(1, key_hash, val_hash)
            .unwrap();
        assert_eq!(results, vec![1]);
    }

    // --- Weight preservation in adjacency postings ---

    #[test]
    fn test_neighbor_weight_preserved_in_segment() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 0);
        mt.apply_op(
            &WalOp::UpsertEdge(EdgeRecord {
                id: 10,
                from: 1,
                to: 2,
                type_id: 5,
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
                type_id: 5,
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
        let mut mt = Memtable::new();
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
        assert!(reader.edge_by_triple(1, 2, 20).unwrap().is_none()); // wrong type
        assert!(reader.edge_by_triple(2, 1, 10).unwrap().is_none()); // reversed direction
        assert!(reader.edge_by_triple(3, 1, 10).unwrap().is_none()); // no such edge
    }

    #[test]
    fn test_edge_triple_index_excludes_tombstoned() {
        let mut mt = Memtable::new();
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

    // --- Bounds checking regression tests ---

    #[test]
    fn test_truncated_nodes_dat_returns_error() {
        // A nodes.dat with only 4 bytes (truncated count header) should
        // still open fine (< 8 bytes → count = 0), but a file that claims
        // many records while being truncated should return CorruptRecord.
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        std::fs::create_dir_all(&seg_dir).unwrap();
        write_format_ver(&seg_dir);

        // Write a nodes.dat that says "1 node" but has no index/data
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes()); // count = 1
                                                     // No index entry follows (truncated)
        std::fs::write(seg_dir.join("nodes.dat"), &data).unwrap();

        // Write empty files for the other required segment files
        for name in &[
            "edges.dat",
            "adj_out.idx",
            "adj_out.dat",
            "adj_in.idx",
            "adj_in.dat",
            "key_index.dat",
            "tombstones.dat",
            "node_meta.dat",
            "edge_meta.dat",
            "timestamp_index.dat",
        ] {
            std::fs::write(seg_dir.join(name), []).unwrap();
        }

        let reader = SegmentReader::open(&seg_dir, 1, None).unwrap();
        // get_node triggers binary search which reads an index entry past EOF
        let result = reader.get_node(42);
        assert!(
            result.is_err(),
            "truncated segment should return error, not panic"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("exceeds data length"),
            "error should describe bounds issue: {}",
            err_msg
        );
    }

    #[test]
    fn test_truncated_tombstones_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        std::fs::create_dir_all(&seg_dir).unwrap();
        write_format_ver(&seg_dir);

        // Write empty required files
        for name in &[
            "nodes.dat",
            "edges.dat",
            "adj_out.idx",
            "adj_out.dat",
            "adj_in.idx",
            "adj_in.dat",
            "key_index.dat",
            "node_meta.dat",
            "edge_meta.dat",
            "timestamp_index.dat",
        ] {
            std::fs::write(seg_dir.join(name), []).unwrap();
        }

        // Write a tombstones.dat that claims 5 entries but only has the header
        let mut data = Vec::new();
        data.extend_from_slice(&5u64.to_le_bytes()); // count = 5
                                                     // No actual tombstone entries (truncated)
        std::fs::write(seg_dir.join("tombstones.dat"), &data).unwrap();

        let result = SegmentReader::open(&seg_dir, 1, None);
        assert!(
            result.is_err(),
            "truncated tombstones should return error, not panic"
        );
    }

    #[test]
    fn test_decode_node_at_truncated_returns_error() {
        // Minimal data that starts a valid node but is truncated mid-record
        // Format v4: no id in data, starts with type_id
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes()); // type_id
                                                     // Missing key_len and beyond (truncated)
        let result = decode_node_at(&data, 0, 42);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_edge_at_truncated_returns_error() {
        // Partial edge record (format v4: no id in data, starts with from)
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes()); // from
                                                     // Missing to, type_id, timestamps, etc.
        let result = decode_edge_at(&data, 0, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_version_bad_magic_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_test");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let mut bad = Vec::new();
        bad.extend_from_slice(b"BAAD");
        bad.extend_from_slice(&1u32.to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), &bad).unwrap();

        let err = read_format_version(&seg_dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid magic"), "got: {}", msg);
    }

    #[test]
    fn test_format_version_bad_size_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_test");
        std::fs::create_dir_all(&seg_dir).unwrap();

        std::fs::write(seg_dir.join("format.ver"), b"short").unwrap();

        let err = read_format_version(&seg_dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid size"), "got: {}", msg);
    }

    #[test]
    fn test_format_version_future_version_rejected() {
        use crate::segment_writer::{SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};

        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_test");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&SEGMENT_MAGIC);
        data.extend_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), &data).unwrap();

        let err = read_format_version(&seg_dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("newer than supported"), "got: {}", msg);
    }

    #[test]
    fn test_format_version_absent_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_test");
        std::fs::create_dir_all(&seg_dir).unwrap();
        // No format.ver file. Pre-version segment, too old (v0)
        let err = read_format_version(&seg_dir).unwrap_err();
        assert!(err.to_string().contains("too old"), "got: {}", err);
    }

    #[test]
    fn test_format_version_v4_rejected() {
        use crate::segment_writer::SEGMENT_MAGIC;
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_test");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&SEGMENT_MAGIC);
        data.extend_from_slice(&4u32.to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), &data).unwrap();

        let err = read_format_version(&seg_dir).unwrap_err();
        assert!(err.to_string().contains("too old"), "got: {}", err);
    }

    #[test]
    fn test_open_rejects_orphan_vector_blob() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "plain")), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        std::fs::write(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME), [0u8; 4]).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, None).err().unwrap();
        assert!(
            err.to_string()
                .contains("vector blobs without node vector metadata"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_vector_metadata_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        };
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut meta = std::fs::read(seg_dir.join(NODE_VECTOR_META_FILENAME)).unwrap();
        meta[0..8].copy_from_slice(&2u64.to_le_bytes());
        std::fs::write(seg_dir.join(NODE_VECTOR_META_FILENAME), meta).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
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
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut dense_blob = std::fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();
        dense_blob.extend_from_slice(&0.9f32.to_le_bytes());
        std::fs::write(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME), dense_blob).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(err.to_string().contains("trailing or unreferenced bytes"));
    }

    #[test]
    fn test_open_exposes_dense_hnsw_header_for_dense_segments() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(3);

        let mut first = make_node(1, 1, "a");
        first.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        mt.apply_op(&WalOp::UpsertNode(first), 0);

        let mut second = make_node(2, 1, "b");
        second.dense_vector = Some(vec![0.3, 0.2, 0.1]);
        mt.apply_op(&WalOp::UpsertNode(second), 0);

        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let reader = SegmentReader::open(&seg_dir, 1, Some(&dense_config)).unwrap();
        let header = reader.dense_hnsw_header().unwrap();

        assert_eq!(header.point_count, 2);
        assert_eq!(header.metric, DenseMetric::Cosine);
        assert_eq!(header.dimension, 3);
        assert_eq!(header.m, dense_config.hnsw.m);
        assert_eq!(
            reader.raw_dense_hnsw_meta_mmap(),
            &std::fs::read(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap()
        );
        assert_eq!(
            reader.raw_dense_hnsw_graph_mmap(),
            &std::fs::read(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)).unwrap()
        );
    }

    #[test]
    fn test_open_rejects_missing_dense_hnsw_files_for_dense_segments() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(3);

        let mut first = make_node(1, 1, "a");
        first.dense_vector = Some(vec![0.1, 0.2, 0.3]);
        mt.apply_op(&WalOp::UpsertNode(first), 0);

        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        std::fs::remove_file(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
        std::fs::remove_file(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_GRAPH_FILENAME)).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("dense HNSW files are missing for 1 dense vectors"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_keeps_dense_hnsw_empty_for_vectorless_segments() {
        let mut mt = Memtable::new();
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
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut format_ver = std::fs::read(seg_dir.join("format.ver")).unwrap();
        format_ver[4..8].copy_from_slice(&6u32.to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), format_ver).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("unexpected dense HNSW files for format version 6"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_dense_hnsw_metric_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut meta =
            std::fs::read(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
        meta[26] = 1; // Euclidean
        std::fs::write(
            seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            meta,
        )
        .unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("does not match configured metric"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_missing_sparse_posting_files_for_sparse_segments() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME))
            .unwrap();
        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME))
            .unwrap();

        let err = SegmentReader::open(&seg_dir, 1, None).err().unwrap();
        assert!(
            err.to_string()
                .contains("segment has sparse vectors but sparse posting files are missing"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_sparse_posting_files_in_v7_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let mut format_ver = std::fs::read(seg_dir.join("format.ver")).unwrap();
        format_ver[4..8].copy_from_slice(&7u32.to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), format_ver).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, None).err().unwrap();
        assert!(
            err.to_string()
                .contains("unexpected sparse posting files for format version 7"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_sparse_vectors_in_v7_segment_without_postings() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTING_INDEX_FILENAME))
            .unwrap();
        std::fs::remove_file(seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME))
            .unwrap();

        let mut format_ver = std::fs::read(seg_dir.join("format.ver")).unwrap();
        format_ver[4..8].copy_from_slice(&7u32.to_le_bytes());
        std::fs::write(seg_dir.join("format.ver"), format_ver).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, None).err().unwrap();
        assert!(
            err.to_string().contains("predates sparse posting support"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_sparse_posting_parity_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();

        let mut node = make_node(1, 1, "sparse");
        node.sparse_vector = Some(vec![(2, 1.5), (7, 0.25)]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, None).unwrap();

        let mut postings =
            std::fs::read(seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME)).unwrap();
        postings[8..12].copy_from_slice(&9.0f32.to_le_bytes());
        std::fs::write(
            seg_dir.join(crate::sparse_postings::SPARSE_POSTINGS_FILENAME),
            postings,
        )
        .unwrap();

        let err = SegmentReader::open(&seg_dir, 1, None).err().unwrap();
        assert!(
            err.to_string()
                .contains("does not match sparse vector payloads"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_dense_hnsw_hnsw_param_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut meta =
            std::fs::read(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
        meta[22..24].copy_from_slice(&(dense_config.hnsw.m + 1).to_le_bytes());
        std::fs::write(
            seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            meta,
        )
        .unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("does not match configured m"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_dense_hnsw_dimension_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut meta =
            std::fs::read(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
        meta[28..32].copy_from_slice(&3u32.to_le_bytes());
        std::fs::write(
            seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            meta,
        )
        .unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("does not match configured dimension"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_dense_hnsw_ef_construction_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let mut meta =
            std::fs::read(seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME)).unwrap();
        meta[24..26].copy_from_slice(&(dense_config.hnsw.ef_construction + 1).to_le_bytes());
        std::fs::write(
            seg_dir.join(crate::dense_hnsw::DENSE_HNSW_META_FILENAME),
            meta,
        )
        .unwrap();

        let err = SegmentReader::open(&seg_dir, 1, Some(&dense_config))
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("does not match configured ef_construction"),
            "got: {}",
            err
        );
    }

    #[test]
    fn test_open_rejects_dense_hnsw_without_dense_config() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let mut mt = Memtable::new();
        let dense_config = dense_config(2);
        let mut node = make_node(1, 1, "vector");
        node.dense_vector = Some(vec![0.1, 0.2]);
        mt.apply_op(&WalOp::UpsertNode(node), 0);
        write_segment(&seg_dir, 1, &mt, Some(&dense_config)).unwrap();

        let err = SegmentReader::open(&seg_dir, 1, None).err().unwrap();
        assert!(
            err.to_string()
                .contains("require DbOptions::dense_vector to be configured"),
            "got: {}",
            err
        );
    }

    // --- V5 sidecar reader tests ---

    #[test]
    fn test_sidecar_node_meta_roundtrip() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 1, "alice")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")), 0);

        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.node_meta_count(), 2);

        // First entry: node_id=1
        let (nid, _off, _len, tid, updated_at, weight, key_len, _pho, phc, _lws) =
            reader.node_meta_at(0).unwrap();
        assert_eq!(nid, 1);
        assert_eq!(tid, 1);
        assert_eq!(updated_at, 2000);
        assert!((weight - 0.75).abs() < f32::EPSILON);
        assert_eq!(key_len, 5); // "alice"
        assert_eq!(phc, 2); // 2 props: "name", "score"

        // Second entry: node_id=2
        let (nid2, _, _, tid2, _, _, key_len2, _, phc2, _) = reader.node_meta_at(1).unwrap();
        assert_eq!(nid2, 2);
        assert_eq!(tid2, 2);
        assert_eq!(key_len2, 3); // "bob"
        assert_eq!(phc2, 0); // no props
    }

    #[test]
    fn test_sidecar_edge_meta_roundtrip() {
        let mut mt = Memtable::new();
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
    fn test_sidecar_data_offset_matches_nodes_dat() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 0);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bb")), 0);

        let (_dir, reader) = write_and_open(&mt);

        // Read data_offset/data_len from sidecar and verify node can be decoded there
        for i in 0..reader.node_meta_count() as usize {
            let (nid, data_offset, data_len, tid, _, _, _, _, _, _) =
                reader.node_meta_at(i).unwrap();
            // Verify the offset points to valid data in nodes.dat
            let node = decode_node_at(&reader.nodes_mmap, data_offset as usize, nid).unwrap();
            assert_eq!(node.id, nid);
            assert_eq!(node.type_id, tid);
            assert!(data_len > 0);
        }
    }
}
