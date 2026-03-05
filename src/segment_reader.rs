use crate::error::EngineError;
use crate::segment_writer::{SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};
use crate::types::*;
use memmap2::Mmap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::ops::Deref;
use std::path::Path;

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
    let end = offset.checked_add(2).ok_or_else(|| EngineError::CorruptRecord("u16 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| EngineError::CorruptRecord(
        format!("u16 read at offset {} exceeds data length {}", offset, data.len())
    ))?;
    // unwrap safe: slice is exactly 2 bytes, guaranteed by get() above
    Ok(u16::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u32_at(data: &[u8], offset: usize) -> Result<u32, EngineError> {
    let end = offset.checked_add(4).ok_or_else(|| EngineError::CorruptRecord("u32 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| EngineError::CorruptRecord(
        format!("u32 read at offset {} exceeds data length {}", offset, data.len())
    ))?;
    // unwrap safe: slice is exactly 4 bytes, guaranteed by get() above
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64_at(data: &[u8], offset: usize) -> Result<u64, EngineError> {
    let end = offset.checked_add(8).ok_or_else(|| EngineError::CorruptRecord("u64 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| EngineError::CorruptRecord(
        format!("u64 read at offset {} exceeds data length {}", offset, data.len())
    ))?;
    // unwrap safe: slice is exactly 8 bytes, guaranteed by get() above
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_i64_at(data: &[u8], offset: usize) -> Result<i64, EngineError> {
    let end = offset.checked_add(8).ok_or_else(|| EngineError::CorruptRecord("i64 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| EngineError::CorruptRecord(
        format!("i64 read at offset {} exceeds data length {}", offset, data.len())
    ))?;
    // unwrap safe: slice is exactly 8 bytes, guaranteed by get() above
    Ok(i64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_f32_at(data: &[u8], offset: usize) -> Result<f32, EngineError> {
    let end = offset.checked_add(4).ok_or_else(|| EngineError::CorruptRecord("f32 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| EngineError::CorruptRecord(
        format!("f32 read at offset {} exceeds data length {}", offset, data.len())
    ))?;
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
                "varint read at offset {} exceeds data length {}", offset, data.len()
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
    let end = offset.checked_add(len).ok_or_else(|| EngineError::CorruptRecord("byte slice offset overflow".into()))?;
    data.get(offset..end).ok_or_else(|| EngineError::CorruptRecord(
        format!("byte slice [{}, {}) exceeds data length {}", offset, end, data.len())
    ))
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
const TOMBSTONE_ENTRY_SIZE: usize = 17; // kind (1) + id (8) + deleted_at (8)
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

/// An mmap-backed reader for an immutable segment directory.
///
/// Provides O(log N) lookups by ID for nodes and edges, adjacency queries
/// from pre-built indexes, key-based lookups, and tombstone checks.
/// Size of a node metadata entry in node_meta.dat (52 bytes).
const NODE_META_ENTRY_SIZE: usize = 52;
/// Size of an edge metadata entry in edge_meta.dat (72 bytes).
const EDGE_META_ENTRY_SIZE: usize = 72;
/// Size of a property hash pair in node_prop_hashes.dat (16 bytes).
const PROP_HASH_PAIR_SIZE: usize = 16;

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
    // Timestamp range index
    timestamp_index_mmap: MappedData,
    deleted_nodes: HashSet<u64>,
    deleted_edges: HashSet<u64>,
    node_count: u64,
    edge_count: u64,
}

impl SegmentReader {
    /// Open a segment directory and mmap all files.
    /// Validates the format version file.
    pub fn open(seg_dir: &Path, segment_id: u64) -> Result<Self, EngineError> {
        let format_version = read_format_version(seg_dir)?;
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
            timestamp_index_mmap,
            deleted_nodes,
            deleted_edges,
            node_count,
            edge_count,
        })
    }

    /// Get a node by ID. Returns None if not found or tombstoned.
    /// Returns Err on corrupt segment data.
    pub fn get_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        if self.deleted_nodes.contains(&id) {
            return Ok(None);
        }
        let offset = match self.binary_search_node_index(id)? {
            Some(o) => o,
            None => return Ok(None),
        };
        Ok(Some(decode_node_at(&self.nodes_mmap, offset, id)?))
    }

    /// Get an edge by ID. Returns None if not found or tombstoned.
    /// Returns Err on corrupt segment data.
    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        if self.deleted_edges.contains(&id) {
            return Ok(None);
        }
        let offset = match self.binary_search_edge_index(id)? {
            Some(o) => o,
            None => return Ok(None),
        };
        Ok(Some(decode_edge_at(&self.edges_mmap, offset, id)?))
    }

    /// Look up a node by (type_id, key). Returns None if not found or tombstoned.
    pub fn node_by_key(&self, type_id: u32, key: &str) -> Result<Option<NodeRecord>, EngineError> {
        let node_id = match self.binary_search_key_index(type_id, key)? {
            Some(id) => id,
            None => return Ok(None),
        };
        self.get_node(node_id)
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
        self.deleted_nodes.contains(&id)
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
            let mut prev_offset: Option<usize> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains(&target_id) {
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
                if let Some(offset) = offset {
                    results[orig_idx] = Some(decode_node_at(&self.nodes_mmap, offset, target_id)?);
                }
            }
        } else {
            // Merge-walk path selected by shared cost model
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_nodes.contains(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * NODE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let offset = read_u64_at(data, entry_off + 8)? as usize;
                        results[orig_idx] = Some(decode_node_at(&self.nodes_mmap, offset, id)?);
                        break;
                    } else {
                        break;
                    }
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
            let mut prev_offset: Option<usize> = None;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains(&target_id) {
                    continue;
                }
                let offset = if prev_id == Some(target_id) {
                    prev_offset
                } else {
                    let found = self.binary_search_edge_index(target_id)?;
                    prev_id = Some(target_id);
                    prev_offset = found;
                    found
                };
                if let Some(offset) = offset {
                    results[orig_idx] = Some(decode_edge_at(&self.edges_mmap, offset, target_id)?);
                }
            }
        } else {
            // Merge-walk path selected by shared cost model
            let mut idx_pos = 0usize;
            for &(orig_idx, target_id) in lookups {
                if self.deleted_edges.contains(&target_id) {
                    continue;
                }
                while idx_pos < count {
                    let entry_off = idx_start + idx_pos * EDGE_INDEX_ENTRY_SIZE;
                    let id = read_u64_at(data, entry_off)?;
                    if id < target_id {
                        idx_pos += 1;
                    } else if id == target_id {
                        let offset = read_u64_at(data, entry_off + 8)? as usize;
                        results[orig_idx] = Some(decode_edge_at(&self.edges_mmap, offset, id)?);
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
        self.deleted_edges.contains(&id)
    }

    /// Return the set of deleted node IDs in this segment.
    pub fn deleted_node_ids(&self) -> &HashSet<u64> {
        &self.deleted_nodes
    }

    /// Return the set of deleted edge IDs in this segment.
    pub fn deleted_edge_ids(&self) -> &HashSet<u64> {
        &self.deleted_edges
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
    ///          prop_hash_offset, prop_hash_count).
    pub(crate) fn node_meta_at(
        &self,
        index: usize,
    ) -> Result<(u64, u64, u32, u32, i64, f32, u16, u64, u32), EngineError> {
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
    ///          weight, valid_from, valid_to).
    pub(crate) fn edge_meta_at(
        &self,
        index: usize,
    ) -> Result<(u64, u64, u32, u64, u64, u32, i64, f32, i64, i64), EngineError> {
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
        Ok((
            edge_id, data_offset, data_len, from, to, type_id, updated_at, weight, valid_from,
            valid_to,
        ))
    }

    /// Read a property hash pair from node_prop_hashes.dat.
    /// `byte_offset` is the absolute byte offset into the file.
    /// Returns (key_hash, value_hash).
    pub(crate) fn prop_hash_at(
        &self,
        byte_offset: usize,
    ) -> Result<(u64, u64), EngineError> {
        let data = &self.node_prop_hashes_mmap[..];
        let key_hash = read_u64_at(data, byte_offset)?;
        let value_hash = read_u64_at(data, byte_offset + 8)?;
        Ok((key_hash, value_hash))
    }

    /// Raw mmap bytes for node_prop_hashes.dat (used by V3 compaction).
    pub(crate) fn raw_node_prop_hashes_mmap(&self) -> &[u8] {
        &self.node_prop_hashes_mmap[..]
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
            nodes.push(decode_node_at(data, offset, id)?);
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
        deleted: &HashSet<u64>,
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
                    if !deleted.contains(&id) {
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
            if !self.deleted_nodes.contains(&node_id) {
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
    pub fn edge_by_triple(&self, from: u64, to: u64, type_id: u32) -> Result<Option<EdgeRecord>, EngineError> {
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
                                if !self.deleted_nodes.contains(&id) {
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
    /// Returns the byte offset into the data section, or None if not found.
    fn binary_search_node_index(&self, target_id: u64) -> Result<Option<usize>, EngineError> {
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
                return Ok(Some(offset));
            }
        }
        Ok(None)
    }

    /// Binary search the edge index for a given edge_id.
    fn binary_search_edge_index(&self, target_id: u64) -> Result<Option<usize>, EngineError> {
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
                return Ok(Some(offset));
            }
        }
        Ok(None)
    }

    /// Binary search the key index for a (type_id, key) pair.
    /// Returns the node_id if found, or None.
    fn binary_search_key_index(&self, target_type: u32, target_key: &str) -> Result<Option<u64>, EngineError> {
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
            let entry_key = std::str::from_utf8(key_bytes)
                .map_err(|_| EngineError::CorruptRecord(format!(
                    "invalid UTF-8 in key index at offset {}", entry_offset + 14
                )))?;

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
    fn find_first_adj_entry(&self, idx_data: &[u8], target_node_id: u64) -> Result<Option<usize>, EngineError> {
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
                let valid_to = if vt_enc == 0 { i64::MAX } else { (vt_enc - 1) as i64 };

                if self.deleted_edges.contains(&edge_id) {
                    continue;
                }
                if self.deleted_nodes.contains(&neighbor_id) {
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

    /// Batch neighbor query: collect neighbors for multiple node IDs in a single
    /// cursor walk through the adjacency index. Input `node_ids` must be sorted
    /// and deduplicated. O(N+M) per direction where N = index entry count,
    /// M = number of queried nodes, vs O(M log N) for M individual binary searches.
    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
    ) -> Result<HashMap<u64, Vec<NeighborEntry>>, EngineError> {
        let mut results: HashMap<u64, Vec<NeighborEntry>> = HashMap::with_capacity(node_ids.len());

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
                    let mut seen = HashSet::new();
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
        results: &mut HashMap<u64, Vec<NeighborEntry>>,
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
                    let valid_to = if vt_enc == 0 { i64::MAX } else { (vt_enc - 1) as i64 };

                    if self.deleted_edges.contains(&edge_id) {
                        continue;
                    }
                    if self.deleted_nodes.contains(&neighbor_id) {
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
            "format.ver has invalid size {} (expected 8)", data.len()
        )));
    }
    if data[..4] != SEGMENT_MAGIC {
        return Err(EngineError::CorruptRecord(format!(
            "format.ver has invalid magic {:?} (expected {:?})", &data[..4], SEGMENT_MAGIC
        )));
    }
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version < 5 {
        return Err(EngineError::CorruptRecord(format!(
            "segment format version {} is too old (minimum supported: 5)", version
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

/// Decode a NodeRecord from mmap data at a given byte offset.
/// The ID is passed separately; it comes from the index, not the data section.
/// Layout: type_id(4) key_len(2) key(N) created_at(8) updated_at(8) weight(4) props_len(4) props(M)
fn decode_node_at(data: &[u8], offset: usize, id: u64) -> Result<NodeRecord, EngineError> {
    let type_id = read_u32_at(data, offset)?;
    let key_len = read_u16_at(data, offset + 4)? as usize;
    let key_bytes = read_bytes_at(data, offset + 6, key_len)?;
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| EngineError::CorruptRecord(format!(
            "invalid UTF-8 in node key at offset {}", offset + 6
        )))?
        .to_string();

    let pos = offset + 6 + key_len;
    let created_at = read_i64_at(data, pos)?;
    let updated_at = read_i64_at(data, pos + 8)?;
    let weight = read_f32_at(data, pos + 16)?;
    let props_len = read_u32_at(data, pos + 20)? as usize;
    let props_bytes = read_bytes_at(data, pos + 24, props_len)?;
    let props: BTreeMap<String, PropValue> =
        rmp_serde::from_slice(props_bytes)
            .map_err(|e| EngineError::CorruptRecord(format!(
                "node props decode at offset {}: {}", pos + 24, e
            )))?;

    Ok(NodeRecord {
        id,
        type_id,
        key,
        props,
        created_at,
        updated_at,
        weight,
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
    let props: BTreeMap<String, PropValue> =
        rmp_serde::from_slice(props_bytes)
            .map_err(|e| EngineError::CorruptRecord(format!(
                "edge props decode at offset {}: {}", offset + 60, e
            )))?;

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
    })
}

/// Load tombstone sets from a tombstones.dat file.
fn load_tombstones(path: &Path) -> Result<(HashSet<u64>, HashSet<u64>), EngineError> {
    let data = std::fs::read(path)?;
    if data.len() < 8 {
        return Ok((HashSet::new(), HashSet::new()));
    }
    let count = read_u64_at(&data, 0)? as usize;

    let mut deleted_nodes = HashSet::new();
    let mut deleted_edges = HashSet::new();

    for i in 0..count {
        let off = 8 + i * TOMBSTONE_ENTRY_SIZE;
        if off + TOMBSTONE_ENTRY_SIZE > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "tombstone entry {} at offset {} exceeds file length {}", i, off, data.len()
            )));
        }
        let kind = data[off];
        let id = read_u64_at(&data, off + 1)?;
        // deleted_at at off + 9 (available but not needed for lookup)
        match kind {
            0 => {
                deleted_nodes.insert(id);
            }
            1 => {
                deleted_edges.insert(id);
            }
            _ => {} // Unknown kind, skip
        }
    }

    Ok((deleted_nodes, deleted_edges))
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
        use crate::segment_writer::{SEGMENT_MAGIC, SEGMENT_FORMAT_VERSION};
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
        }
    }

    /// Helper: build a memtable, write segment, open reader
    fn write_and_open(mt: &Memtable) -> (tempfile::TempDir, SegmentReader) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        write_segment(&seg_dir, 1, mt).unwrap();
        let reader = SegmentReader::open(&seg_dir, 1).unwrap();
        (dir, reader)
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
        let strategy = choose_batch_read_strategy(
            &idx,
            keys.len(),
            NODE_INDEX_ENTRY_SIZE,
            0,
            2,
            500,
            501,
        )
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
        let strategy = choose_batch_read_strategy(
            &idx,
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
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(42, 1, "alice")));

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
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.get_node(999).unwrap().is_none());
    }

    #[test]
    fn test_get_node_with_properties() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 1, "alice")));

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
    fn test_get_node_tombstoned() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.get_node(1).unwrap().is_none());
        assert!(reader.is_node_deleted(1));
    }

    // --- get_edge ---

    #[test]
    fn test_get_edge_found() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)));

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
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 2, "alice"))); // different type

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
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));

        let (_dir, reader) = write_and_open(&mt);
        assert!(reader.node_by_key(1, "bob").unwrap().is_none());
        assert!(reader.node_by_key(2, "alice").unwrap().is_none());
    }

    // --- neighbors ---

    #[test]
    fn test_neighbors_outgoing() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 10)));

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert_eq!(nbrs.len(), 2);

        let ids: HashSet<u64> = nbrs.iter().map(|n| n.node_id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    #[test]
    fn test_neighbors_incoming() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(2, Direction::Incoming, None, 0).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 1);
    }

    #[test]
    fn test_neighbors_with_type_filter() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 20)));

        let (_dir, reader) = write_and_open(&mt);

        // Filter type 10 only
        let nbrs = reader.neighbors(1, Direction::Outgoing, Some(&[10]), 0).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 2);

        // Filter type 20 only
        let nbrs = reader.neighbors(1, Direction::Outgoing, Some(&[20]), 0).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, 3);

        // Filter non-existent type
        let nbrs = reader.neighbors(1, Direction::Outgoing, Some(&[99]), 0).unwrap();
        assert!(nbrs.is_empty());
    }

    #[test]
    fn test_neighbors_with_limit() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "hub")));
        for i in 2..=6 {
            mt.apply_op(&WalOp::UpsertNode(make_node(i, 1, &format!("n{}", i))));
            mt.apply_op(&WalOp::UpsertEdge(make_edge(i - 1, 1, i, 10)));
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
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "lonely")));

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert!(nbrs.is_empty());
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
        assert!(reader.neighbors(1, Direction::Outgoing, None, 0).unwrap().is_empty());
    }

    // --- Binary search stress ---

    #[test]
    fn test_binary_search_many_nodes() {
        let mut mt = Memtable::new();
        for i in 1..=100 {
            mt.apply_op(&WalOp::UpsertNode(make_node(i, 1, &format!("n{}", i))));
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
            mt.apply_op(&WalOp::UpsertNode(make_node(
                i,
                (i % 3) as u32 + 1,
                &format!("key_{:04}", i),
            )));
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
            mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
                i,
                1,
                &format!("node_{}", i),
            )));
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 2, 3, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 3, 20)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(4, 4, 5, 10)));

        // Delete one node and one edge
        mt.apply_op(&WalOp::DeleteNode {
            id: 99,
            deleted_at: 9999,
        });
        mt.apply_op(&WalOp::DeleteEdge {
            id: 99,
            deleted_at: 9999,
        });

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
        let ids: HashSet<u64> = out1.iter().map(|n| n.node_id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));

        // Verify type-filtered neighbors
        let out1_t10 = reader.neighbors(1, Direction::Outgoing, Some(&[10]), 0).unwrap();
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
        mt.apply_op(&WalOp::UpsertNode(NodeRecord {
            id: 1, type_id: 1, key: "apple".to_string(), props: props1,
            created_at: 1000, updated_at: 1001, weight: 0.5,
        }));

        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(&WalOp::UpsertNode(NodeRecord {
            id: 2, type_id: 1, key: "cherry".to_string(), props: props2,
            created_at: 1000, updated_at: 1001, weight: 0.5,
        }));

        let mut props3 = BTreeMap::new();
        props3.insert("color".to_string(), PropValue::String("green".to_string()));
        mt.apply_op(&WalOp::UpsertNode(NodeRecord {
            id: 3, type_id: 1, key: "lime".to_string(), props: props3,
            created_at: 1000, updated_at: 1001, weight: 0.5,
        }));

        let (_dir, reader) = write_and_open(&mt);

        // Query for red nodes
        let key_hash = hash_prop_key("color");
        let val_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let mut reds = reader.find_nodes_by_prop_hash(1, key_hash, val_hash).unwrap();
        reds.sort();
        assert_eq!(reds, vec![1, 2]);

        // Query for green nodes
        let val_hash_green = hash_prop_value(&PropValue::String("green".to_string()));
        let greens = reader.find_nodes_by_prop_hash(1, key_hash, val_hash_green).unwrap();
        assert_eq!(greens, vec![3]);

        // Non-existent value
        let val_hash_blue = hash_prop_value(&PropValue::String("blue".to_string()));
        assert!(reader.find_nodes_by_prop_hash(1, key_hash, val_hash_blue).unwrap().is_empty());

        // Wrong type_id
        assert!(reader.find_nodes_by_prop_hash(99, key_hash, val_hash).unwrap().is_empty());
    }

    #[test]
    fn test_prop_index_excludes_tombstoned() {
        use crate::types::{hash_prop_key, hash_prop_value};

        let mut mt = Memtable::new();

        let mut props = BTreeMap::new();
        props.insert("tag".to_string(), PropValue::String("x".to_string()));
        mt.apply_op(&WalOp::UpsertNode(NodeRecord {
            id: 1, type_id: 1, key: "a".to_string(), props: props.clone(),
            created_at: 1000, updated_at: 1001, weight: 0.5,
        }));
        mt.apply_op(&WalOp::UpsertNode(NodeRecord {
            id: 2, type_id: 1, key: "b".to_string(), props,
            created_at: 1000, updated_at: 1001, weight: 0.5,
        }));

        // Delete node 2 (tombstone)
        mt.apply_op(&WalOp::DeleteNode { id: 2, deleted_at: 9999 });

        let (_dir, reader) = write_and_open(&mt);

        let key_hash = hash_prop_key("tag");
        let val_hash = hash_prop_value(&PropValue::String("x".to_string()));

        // Node 2 is tombstoned, so only node 1 should be returned
        let results = reader.find_nodes_by_prop_hash(1, key_hash, val_hash).unwrap();
        assert_eq!(results, vec![1]);
    }

    // --- Weight preservation in adjacency postings ---

    #[test]
    fn test_neighbor_weight_preserved_in_segment() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(EdgeRecord {
            id: 10, from: 1, to: 2, type_id: 5,
            props: BTreeMap::new(), created_at: 100, updated_at: 100, weight: 0.75,
            valid_from: 0, valid_to: i64::MAX,
        }));
        mt.apply_op(&WalOp::UpsertEdge(EdgeRecord {
            id: 11, from: 1, to: 3, type_id: 5,
            props: BTreeMap::new(), created_at: 100, updated_at: 100, weight: 0.25,
            valid_from: 0, valid_to: i64::MAX,
        }));

        let (_dir, reader) = write_and_open(&mt);
        let nbrs = reader.neighbors(1, Direction::Outgoing, None, 0).unwrap();
        assert_eq!(nbrs.len(), 2);

        // Check that each neighbor has the correct weight
        for n in &nbrs {
            if n.edge_id == 10 {
                assert!((n.weight - 0.75).abs() < f32::EPSILON, "edge 10 weight: {}", n.weight);
            } else if n.edge_id == 11 {
                assert!((n.weight - 0.25).abs() < f32::EPSILON, "edge 11 weight: {}", n.weight);
            } else {
                panic!("unexpected edge_id: {}", n.edge_id);
            }
        }
    }

    // --- Edge triple index ---

    #[test]
    fn test_edge_triple_index_roundtrip() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(101, 1, 3, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(102, 2, 3, 20)));

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
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(100, 1, 2, 10)));
        mt.apply_op(&WalOp::DeleteEdge { id: 100, deleted_at: 9999 });

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
        for name in &["edges.dat", "adj_out.idx", "adj_out.dat", "adj_in.idx",
                       "adj_in.dat", "key_index.dat", "tombstones.dat",
                       "node_meta.dat", "edge_meta.dat", "timestamp_index.dat"] {
            std::fs::write(seg_dir.join(name), &[]).unwrap();
        }

        let reader = SegmentReader::open(&seg_dir, 1).unwrap();
        // get_node triggers binary search which reads an index entry past EOF
        let result = reader.get_node(42);
        assert!(result.is_err(), "truncated segment should return error, not panic");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceeds data length"), "error should describe bounds issue: {}", err_msg);
    }

    #[test]
    fn test_truncated_tombstones_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        std::fs::create_dir_all(&seg_dir).unwrap();
        write_format_ver(&seg_dir);

        // Write empty required files
        for name in &["nodes.dat", "edges.dat", "adj_out.idx", "adj_out.dat",
                       "adj_in.idx", "adj_in.dat", "key_index.dat",
                       "node_meta.dat", "edge_meta.dat", "timestamp_index.dat"] {
            std::fs::write(seg_dir.join(name), &[]).unwrap();
        }

        // Write a tombstones.dat that claims 5 entries but only has the header
        let mut data = Vec::new();
        data.extend_from_slice(&5u64.to_le_bytes()); // count = 5
        // No actual tombstone entries (truncated)
        std::fs::write(seg_dir.join("tombstones.dat"), &data).unwrap();

        let result = SegmentReader::open(&seg_dir, 1);
        assert!(result.is_err(), "truncated tombstones should return error, not panic");
    }

    #[test]
    fn test_decode_node_at_truncated_returns_error() {
        // Minimal data that starts a valid node but is truncated mid-record
        // Format v4: no id in data, starts with type_id
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes());  // type_id
        // Missing key_len and beyond (truncated)
        let result = decode_node_at(&data, 0, 42);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_edge_at_truncated_returns_error() {
        // Partial edge record (format v4: no id in data, starts with from)
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());   // from
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
        use crate::segment_writer::{SEGMENT_MAGIC, SEGMENT_FORMAT_VERSION};

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

    // --- V5 sidecar reader tests ---

    #[test]
    fn test_sidecar_node_meta_roundtrip() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")));

        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.node_meta_count(), 2);

        // First entry: node_id=1
        let (nid, _off, _len, tid, updated_at, weight, key_len, _pho, phc) =
            reader.node_meta_at(0).unwrap();
        assert_eq!(nid, 1);
        assert_eq!(tid, 1);
        assert_eq!(updated_at, 2000);
        assert!((weight - 0.75).abs() < f32::EPSILON);
        assert_eq!(key_len, 5); // "alice"
        assert_eq!(phc, 2); // 2 props: "name", "score"

        // Second entry: node_id=2
        let (nid2, _, _, tid2, _, _, key_len2, _, phc2) =
            reader.node_meta_at(1).unwrap();
        assert_eq!(nid2, 2);
        assert_eq!(tid2, 2);
        assert_eq!(key_len2, 3); // "bob"
        assert_eq!(phc2, 0); // no props
    }

    #[test]
    fn test_sidecar_edge_meta_roundtrip() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)));

        let (_dir, reader) = write_and_open(&mt);

        assert_eq!(reader.edge_meta_count(), 1);

        let (eid, _off, _len, from, to, tid, updated_at, weight, vf, vt) =
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
    fn test_sidecar_prop_hash_roundtrip() {
        use crate::types::{hash_prop_key, hash_prop_value};

        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(1, 1, "alice")));

        let (_dir, reader) = write_and_open(&mt);

        let (_, _, _, _, _, _, _, pho, phc) = reader.node_meta_at(0).unwrap();
        assert_eq!(phc, 2);

        // Read prop hashes and verify against expected
        let mut hashes = Vec::new();
        for i in 0..phc as usize {
            let (kh, vh) = reader.prop_hash_at(pho as usize + i * PROP_HASH_PAIR_SIZE).unwrap();
            hashes.push((kh, vh));
        }

        // Props are BTreeMap so sorted: "name" before "score"
        let expected_name_kh = hash_prop_key("name");
        let expected_name_vh = hash_prop_value(&PropValue::String("alice".to_string()));
        let expected_score_kh = hash_prop_key("score");
        let expected_score_vh = hash_prop_value(&PropValue::Float(0.95));

        assert_eq!(hashes[0].0, expected_name_kh);
        assert_eq!(hashes[0].1, expected_name_vh);
        assert_eq!(hashes[1].0, expected_score_kh);
        assert_eq!(hashes[1].1, expected_score_vh);
    }

    #[test]
    fn test_sidecar_data_offset_matches_nodes_dat() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bb")));

        let (_dir, reader) = write_and_open(&mt);

        // Read data_offset/data_len from sidecar and verify node can be decoded there
        for i in 0..reader.node_meta_count() as usize {
            let (nid, data_offset, data_len, tid, _, _, _, _, _) =
                reader.node_meta_at(i).unwrap();
            // Verify the offset points to valid data in nodes.dat
            let node = decode_node_at(&reader.nodes_mmap, data_offset as usize, nid).unwrap();
            assert_eq!(node.id, nid);
            assert_eq!(node.type_id, tid);
            assert!(data_len > 0);
        }
    }
}
