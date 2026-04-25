use crate::degree_cache::{
    write_folded_degree_delta_sidecar_from_sidecars, write_sorted_degree_delta_sidecar,
    DegreeOverlaySnapshot, DEGREE_DELTA_FILENAME,
};
use crate::dense_hnsw::{write_dense_hnsw_index_from_points, DensePointInput};
use crate::error::EngineError;
use crate::memtable::{encode_range_prop_value, AdjEntry, Memtable};
use crate::parallel::engine_cpu_try_join;
use crate::segment_reader::SegmentReader;
use crate::sparse_postings::write_sparse_posting_files;
use crate::types::*;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

/// Magic bytes identifying an OverGraph segment directory.
pub const SEGMENT_MAGIC: [u8; 4] = *b"EGRM";
/// Current segment format version.
/// v1: original format
/// v2: added valid_from/valid_to to edge records
/// v3: added valid_from/valid_to to adjacency postings
/// v4: BTreeMap props, removed redundant ID from records, delta-encoded adjacency
/// v5: metadata sidecars (node_meta.dat, edge_meta.dat, node_prop_hashes.dat)
/// v6: optional node vector sidecars/blobs
/// v7: optional dense HNSW sidecars (dense_hnsw_meta.dat, dense_hnsw_graph.dat)
/// v8: optional sparse posting-list sidecars (sparse_posting_index.dat, sparse_postings.dat)
/// v9: last_write_seq in node_meta (60B), edge_meta (80B), tombstones (25B)
pub const SEGMENT_FORMAT_VERSION: u32 = 9;

pub(crate) const NODE_VECTOR_META_FILENAME: &str = "node_vector_meta.dat";
pub(crate) const NODE_DENSE_VECTOR_BLOB_FILENAME: &str = "node_dense_vectors.dat";
pub(crate) const NODE_SPARSE_VECTOR_BLOB_FILENAME: &str = "node_sparse_vectors.dat";
pub(crate) const NODE_VECTOR_META_ENTRY_SIZE: usize = 28;
pub(crate) const SECONDARY_INDEX_DIRNAME: &str = "secondary_indexes";
const NODE_VECTOR_FLAG_DENSE: u8 = 0b0000_0001;
const NODE_VECTOR_FLAG_SPARSE: u8 = 0b0000_0010;

// --- Segment file format constants ---

/// Size of a node index entry: node_id (8) + offset (8) = 16 bytes
const NODE_INDEX_ENTRY_SIZE: u64 = 16;
/// Size of an edge index entry: edge_id (8) + offset (8) = 16 bytes
const EDGE_INDEX_ENTRY_SIZE: u64 = 16;
/// Size of a type index entry: type_id (4) + offset (8) + count (4) = 16 bytes
const TYPE_INDEX_ENTRY_SIZE: u64 = 16;
const SECONDARY_EQ_ENTRY_SIZE: u64 = 20;
const DENSE_VECTOR_VALUE_SIZE: u64 = 4;
const SPARSE_VECTOR_ENTRY_SIZE: u64 = 8;

#[derive(Debug, Default, Clone)]
pub(crate) struct SecondaryIndexMaintenanceReport {
    pub failed_equality_indexes: Vec<(u64, String)>,
    pub failed_range_indexes: Vec<(u64, String)>,
}

pub(crate) fn secondary_indexes_dir(seg_dir: &Path) -> PathBuf {
    seg_dir.join(SECONDARY_INDEX_DIRNAME)
}

pub(crate) fn node_prop_eq_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    secondary_indexes_dir(seg_dir).join(format!("node_prop_eq_{}.dat", index_id))
}

pub(crate) fn node_prop_range_sidecar_path(seg_dir: &Path, index_id: u64) -> PathBuf {
    secondary_indexes_dir(seg_dir).join(format!("node_prop_range_{}.dat", index_id))
}

/// Write all segment files for a frozen memtable into the given directory.
///
/// Creates: nodes.dat, edges.dat, adj_out.idx, adj_out.dat, adj_in.idx,
/// adj_in.dat, key_index.dat, node_type_index.dat, edge_type_index.dat,
/// edge_triple_index.dat, tombstones.dat, and any declared secondary sidecars
///
/// IMPORTANT: Two index-writing paths exist and must stay in sync:
///   1. This function (flush path, builds indexes from Memtable)
///   2. `write_indexes_from_metadata()` (compaction path, builds from sidecars)
///
/// If you add a new index type, you MUST add it to BOTH paths.
#[allow(dead_code)]
pub(crate) fn write_segment(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
    degree_overlay: &DegreeOverlaySnapshot,
) -> Result<SegmentInfo, EngineError> {
    write_segment_with_secondary_indexes(
        seg_dir,
        segment_id,
        memtable,
        dense_config,
        degree_overlay,
        &[],
    )
}

#[allow(dead_code)]
pub(crate) fn write_segment_with_secondary_indexes(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
    degree_overlay: &DegreeOverlaySnapshot,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentInfo, EngineError> {
    write_segment_with_degree_overlay_and_secondary_indexes(
        seg_dir,
        segment_id,
        memtable,
        dense_config,
        degree_overlay,
        secondary_indexes,
    )
}

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

    let node_data = write_nodes_dat(seg_dir, &nodes)?;
    let edge_data = write_edges_dat(seg_dir, &edges)?;
    run_index_fanout(
        || {
            write_key_index(seg_dir, &nodes)?;
            write_type_index(seg_dir, "node_type_index", &memtable.type_node_index())?;
            write_declared_equality_sidecars(seg_dir, memtable, secondary_indexes)?;
            write_declared_range_sidecars(seg_dir, memtable, secondary_indexes)?;
            write_timestamp_index(seg_dir, &memtable.time_node_index())?;
            Ok(())
        },
        || {
            write_adjacency_index(seg_dir, "adj_out", &memtable.adj_out())?;
            write_adjacency_index(seg_dir, "adj_in", &memtable.adj_in())?;
            write_type_index(seg_dir, "edge_type_index", &memtable.type_edge_index())?;
            write_edge_triple_index(seg_dir, &edges)?;
            write_tombstones(
                seg_dir,
                &memtable.deleted_nodes(),
                &memtable.deleted_edges(),
            )?;
            if let Some(entries) = degree_entries.as_ref() {
                write_sorted_degree_delta_sidecar(&seg_dir.join(DEGREE_DELTA_FILENAME), entries)?;
            }
            Ok(())
        },
        || {
            let dense_points = write_sidecars(seg_dir, &node_data, &edge_data, &nodes, &edges)?;
            write_dense_hnsw_index_from_points(seg_dir, dense_config, dense_points)?;
            Ok(())
        },
        || write_sparse_posting_index(seg_dir, &nodes),
    )?;
    write_format_version(seg_dir)?;

    // fsync all files and the directory
    fsync_dir(seg_dir)?;

    Ok(SegmentInfo {
        id: segment_id,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
    })
}

fn run_index_fanout<NodeBranch, EdgeBranch, VectorBranch, SparseBranch>(
    node_branch: NodeBranch,
    edge_branch: EdgeBranch,
    vector_branch: VectorBranch,
    sparse_branch: SparseBranch,
) -> Result<(), EngineError>
where
    NodeBranch: FnOnce() -> Result<(), EngineError> + Send,
    EdgeBranch: FnOnce() -> Result<(), EngineError> + Send,
    VectorBranch: FnOnce() -> Result<(), EngineError> + Send,
    SparseBranch: FnOnce() -> Result<(), EngineError> + Send,
{
    let _ = engine_cpu_try_join(node_branch, edge_branch)?;
    let _ = engine_cpu_try_join(vector_branch, sparse_branch)?;
    Ok(())
}

pub(crate) fn write_node_prop_eq_sidecar_to_path(
    path: &Path,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

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

    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

pub(crate) fn write_node_prop_range_sidecar_to_path(
    path: &Path,
    entries: &[(u64, u64)],
) -> Result<(), EngineError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    write_u64(&mut writer, entries.len() as u64)?;
    for &(encoded_value, node_id) in entries {
        write_u64(&mut writer, encoded_value)?;
        write_u64(&mut writer, node_id)?;
    }

    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_declared_equality_sidecars(
    seg_dir: &Path,
    memtable: &Memtable,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<(), EngineError> {
    let eq_entries: Vec<&SecondaryIndexManifestEntry> = secondary_indexes
        .iter()
        .filter(|entry| matches!(entry.kind, SecondaryIndexKind::Equality))
        .collect();
    if eq_entries.is_empty() {
        return Ok(());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;

    for entry in eq_entries {
        let mut groups = BTreeMap::new();
        if let Some(values) = memtable.secondary_eq_state().get(&entry.index_id) {
            for (&value_hash, ids) in values {
                let mut sorted_ids: Vec<u64> = ids.iter().copied().collect();
                sorted_ids.sort_unstable();
                groups.insert(value_hash, sorted_ids);
            }
        }
        write_node_prop_eq_sidecar_to_path(
            &node_prop_eq_sidecar_path(seg_dir, entry.index_id),
            &groups,
        )?;
    }

    fsync_dir(&index_dir)?;
    Ok(())
}

fn write_declared_range_sidecars(
    seg_dir: &Path,
    memtable: &Memtable,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<(), EngineError> {
    let range_entries: Vec<&SecondaryIndexManifestEntry> = secondary_indexes
        .iter()
        .filter(|entry| matches!(entry.kind, SecondaryIndexKind::Range { .. }))
        .collect();
    if range_entries.is_empty() {
        return Ok(());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;

    for entry in range_entries {
        let sidecar_entries: Vec<(u64, u64)> = memtable
            .secondary_range_state()
            .get(&entry.index_id)
            .map(|entries| entries.iter().copied().collect())
            .unwrap_or_default();
        write_node_prop_range_sidecar_to_path(
            &node_prop_range_sidecar_path(seg_dir, entry.index_id),
            &sidecar_entries,
        )?;
    }

    fsync_dir(&index_dir)?;
    Ok(())
}

/// nodes.dat format:
/// [count: u64]
/// [index: (node_id: u64, offset: u64) × count, sorted by node_id]
/// [data: node records sequentially]
///
/// Returns Vec of (node_id, data_offset, data_len) sorted by node_id,
/// used by sidecar writers to record raw byte spans.
fn write_nodes_dat(
    seg_dir: &Path,
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<Vec<(u64, u64, u32)>, EngineError> {
    let path = seg_dir.join("nodes.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

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

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(node_data)
}

/// edges.dat format:
/// [count: u64]
/// [index: (edge_id: u64, offset: u64) × count, sorted by edge_id]
/// [data: edge records sequentially]
///
/// Returns Vec of (edge_id, data_offset, data_len) sorted by edge_id,
/// used by sidecar writers to record raw byte spans.
fn write_edges_dat(
    seg_dir: &Path,
    edges: &NodeIdMap<EdgeRecord>,
) -> Result<Vec<(u64, u64, u32)>, EngineError> {
    let path = seg_dir.join("edges.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

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

    w.flush()?;
    w.get_ref().sync_all()?;
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

/// Adjacency index + delta-encoded postings.
///
/// Index file (adj_out.idx / adj_in.idx):
/// [count: u64]
/// [(node_id: u64, type_id: u32, offset: u64, count: u32) × count, sorted by (node_id, type_id)]
///
/// Data file (adj_out.dat / adj_in.dat):
/// Per group: delta-encoded postings, variable length.
/// First posting: varint(edge_id) + varint(neighbor_id) + f32(weight) + varint(valid_from_enc) + varint(valid_to_enc)
/// Subsequent:    varint(edge_id_delta) + varint(neighbor_id) + f32(weight) + varint(valid_from_enc) + varint(valid_to_enc)
/// valid_from_enc = valid_from as u64 (valid_from is always >= 0)
/// valid_to_enc = 0 if valid_to == i64::MAX, else (valid_to as u64) + 1
fn write_adjacency_index(
    seg_dir: &Path,
    prefix: &str,
    adj: &NodeIdMap<NodeIdMap<AdjEntry>>,
) -> Result<(), EngineError> {
    let idx_path = seg_dir.join(format!("{}.idx", prefix));
    let dat_path = seg_dir.join(format!("{}.dat", prefix));

    let idx_file = File::create(&idx_path)?;
    let dat_file = File::create(&dat_path)?;
    let mut idx_w = BufWriter::new(idx_file);
    let mut dat_w = BufWriter::new(dat_file);

    // Group entries by (node_id, type_id)
    let mut groups: Vec<(u64, u32, Vec<&AdjEntry>)> = Vec::new();
    for (&node_id, edge_map) in adj {
        let mut by_type: HashMap<u32, Vec<&AdjEntry>> = HashMap::new();
        for entry in edge_map.values() {
            by_type.entry(entry.type_id).or_default().push(entry);
        }
        for (type_id, mut postings) in by_type {
            postings.sort_unstable_by_key(|e| e.edge_id);
            groups.push((node_id, type_id, postings));
        }
    }

    groups.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let count = groups.len() as u64;
    write_u64(&mut idx_w, count)?;

    // Encode postings into a buffer first to measure byte sizes
    let mut posting_buf = Vec::new();
    let mut dat_offset: u64 = 0;
    let mut index_entries: Vec<(u64, u32, u64, u32)> = Vec::with_capacity(groups.len());

    for (node_id, type_id, postings) in &groups {
        let posting_count = postings.len() as u32;
        index_entries.push((*node_id, *type_id, dat_offset, posting_count));

        posting_buf.clear();
        let mut prev_edge_id: u64 = 0;
        for entry in postings {
            let delta = entry.edge_id - prev_edge_id;
            prev_edge_id = entry.edge_id;

            write_varint_to_vec(&mut posting_buf, delta);
            write_varint_to_vec(&mut posting_buf, entry.neighbor_id);
            posting_buf.extend_from_slice(&entry.weight.to_le_bytes());
            debug_assert!(
                entry.valid_from >= 0,
                "valid_from must be non-negative for varint encoding"
            );
            debug_assert!(
                entry.valid_to >= 0,
                "valid_to must be non-negative for sentinel encoding"
            );
            write_varint_to_vec(&mut posting_buf, entry.valid_from as u64);
            // Sentinel: 0 means i64::MAX, otherwise value + 1
            let vt_enc = if entry.valid_to == i64::MAX {
                0u64
            } else {
                entry.valid_to as u64 + 1
            };
            write_varint_to_vec(&mut posting_buf, vt_enc);
        }

        dat_w.write_all(&posting_buf)?;
        dat_offset += posting_buf.len() as u64;
    }

    // Write index entries
    for (node_id, type_id, offset, posting_count) in &index_entries {
        write_u64(&mut idx_w, *node_id)?;
        write_u32(&mut idx_w, *type_id)?;
        write_u64(&mut idx_w, *offset)?;
        write_u32(&mut idx_w, *posting_count)?;
    }

    idx_w.flush()?;
    idx_w.get_ref().sync_all()?;
    dat_w.flush()?;
    dat_w.get_ref().sync_all()?;
    Ok(())
}

/// key_index.dat format:
/// [entry_count: u64]
/// [offset_table: u64 × entry_count]  (byte offset to each entry in data section)
/// [data section: entries sorted by (type_id, key)]
///
/// Each entry: [type_id: u32][node_id: u64][key_len: u16][key: bytes]
fn write_key_index(seg_dir: &Path, nodes: &NodeIdMap<NodeRecord>) -> Result<(), EngineError> {
    let path = seg_dir.join("key_index.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    // Collect and sort entries by (type_id, key)
    let mut entries: Vec<(u32, &str, u64)> = nodes
        .values()
        .map(|n| (n.type_id, n.key.as_str(), n.id))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));

    let count = entries.len() as u64;
    write_u64(&mut w, count)?;

    // Pre-compute entry sizes to build offset table
    // Each entry: type_id (4) + node_id (8) + key_len (2) + key_bytes
    let entry_sizes: Vec<u64> = entries
        .iter()
        .map(|(_, key, _)| 4 + 8 + 2 + key.len() as u64)
        .collect();

    // Data section starts after: count (8) + offset_table (count * 8)
    let data_start = 8 + count * 8;
    let mut offset = data_start;
    for &size in &entry_sizes {
        write_u64(&mut w, offset)?;
        offset += size;
    }

    // Write data entries
    for (type_id, key, node_id) in &entries {
        write_u32(&mut w, *type_id)?;
        write_u64(&mut w, *node_id)?;
        let key_bytes = key.as_bytes();
        if key_bytes.len() > u16::MAX as usize {
            return Err(EngineError::SerializationError(format!(
                "node key exceeds maximum length of {} bytes",
                u16::MAX
            )));
        }
        write_u16(&mut w, key_bytes.len() as u16)?;
        w.write_all(key_bytes)?;
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// type index format (node_type_index.dat / edge_type_index.dat):
/// [entry_count: u64]
/// [index: entry_count × (type_id: u32, offset: u64, count: u32), sorted by type_id]
/// [data: packed u64 record IDs per type, grouped contiguously]
fn write_type_index(
    seg_dir: &Path,
    filename: &str,
    type_index: &HashMap<u32, NodeIdSet>,
) -> Result<(), EngineError> {
    let path = seg_dir.join(format!("{}.dat", filename));
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    // Collect non-empty type groups, sorted by type_id
    let mut groups: Vec<(u32, Vec<u64>)> = type_index
        .iter()
        .filter(|(_, ids)| !ids.is_empty())
        .map(|(&type_id, ids)| {
            let mut sorted_ids: Vec<u64> = ids.iter().copied().collect();
            sorted_ids.sort_unstable();
            (type_id, sorted_ids)
        })
        .collect();
    groups.sort_by_key(|(type_id, _)| *type_id);

    let entry_count = groups.len() as u64;
    write_u64(&mut w, entry_count)?;

    // Data section starts after header + index
    let data_start = 8 + entry_count * TYPE_INDEX_ENTRY_SIZE;
    let mut data_offset = data_start;

    // Write index entries
    for (type_id, ids) in &groups {
        write_u32(&mut w, *type_id)?;
        write_u64(&mut w, data_offset)?;
        let count = ids.len() as u32;
        write_u32(&mut w, count)?;
        data_offset += count as u64 * 8; // each ID is u64 = 8 bytes
    }

    // Write data section (packed u64 IDs)
    for (_, ids) in &groups {
        for &id in ids {
            write_u64(&mut w, id)?;
        }
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// edge_triple_index.dat format:
/// [count: u64]
/// [entries: count × (from: u64, to: u64, type_id: u32, edge_id: u64), sorted by (from, to, type_id)]
fn write_edge_triple_index(
    seg_dir: &Path,
    edges: &NodeIdMap<EdgeRecord>,
) -> Result<(), EngineError> {
    let path = seg_dir.join("edge_triple_index.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    // Collect and sort by (from, to, type_id)
    let mut entries: Vec<(u64, u64, u32, u64)> = edges
        .values()
        .map(|e| (e.from, e.to, e.type_id, e.id))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));

    let count = entries.len() as u64;
    write_u64(&mut w, count)?;

    for (from, to, type_id, edge_id) in &entries {
        write_u64(&mut w, *from)?;
        write_u64(&mut w, *to)?;
        write_u32(&mut w, *type_id)?;
        write_u64(&mut w, *edge_id)?;
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// timestamp_index.dat format:
/// [entry_count: u64]
/// [entries: entry_count × (type_id: u32, updated_at: i64, node_id: u64),
///   sorted by (type_id, updated_at, node_id)]
///
/// Each entry is 20 bytes. Binary search for range start (type_id, from_ms),
/// scan to range end (type_id, to_ms). O(log N) seek + O(results) scan.
fn write_timestamp_index(
    seg_dir: &Path,
    time_index: &std::collections::BTreeSet<(u32, i64, u64)>,
) -> Result<(), EngineError> {
    let path = seg_dir.join("timestamp_index.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let count = time_index.len() as u64;
    write_u64(&mut w, count)?;

    // BTreeSet is already sorted by (type_id, updated_at, node_id)
    for &(type_id, updated_at, node_id) in time_index {
        write_u32(&mut w, type_id)?;
        w.write_all(&updated_at.to_le_bytes())?;
        write_u64(&mut w, node_id)?;
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// tombstones.dat format (v9):
/// [count: u64]
/// [(kind: u8, id: u64, deleted_at: i64, last_write_seq: u64) × count]
/// kind: 0 = node, 1 = edge. Entry size: 25 bytes.
fn write_tombstones(
    seg_dir: &Path,
    deleted_nodes: &NodeIdMap<TombstoneEntry>,
    deleted_edges: &NodeIdMap<TombstoneEntry>,
) -> Result<(), EngineError> {
    let path = seg_dir.join("tombstones.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

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

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

// --- Record encoding helpers ---

fn encode_node_record_into(buf: &mut Vec<u8>, node: &NodeRecord) -> Result<(), EngineError> {
    buf.clear();
    // Note: node.id is NOT written here. It's already in the index.
    buf.extend_from_slice(&node.type_id.to_le_bytes());
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
    buf.extend_from_slice(&edge.type_id.to_le_bytes());
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

// --- V5 metadata sidecar writers ---

/// Write metadata sidecars for node and edge records.
///
/// `node_data` and `edge_data` are (id, data_offset, data_len) tuples sorted by id,
/// matching the actual byte positions in nodes.dat/edges.dat.
pub(crate) fn write_sidecars(
    seg_dir: &Path,
    node_data: &[(u64, u64, u32)],
    edge_data: &[(u64, u64, u32)],
    nodes: &NodeIdMap<NodeRecord>,
    edges: &NodeIdMap<EdgeRecord>,
) -> Result<Vec<DensePointInput>, EngineError> {
    write_node_meta(seg_dir, node_data, nodes)?;
    let dense_points = write_node_vector_sidecars(seg_dir, node_data, nodes)?;
    write_edge_meta(seg_dir, edge_data, edges)?;
    Ok(dense_points)
}

/// node_meta.dat format:
/// [count: u64]
/// [entries: count × NodeMetaEntry, sorted by node_id]
///
/// NodeMetaEntry (60 bytes):
///   node_id: u64, data_offset: u64, data_len: u32, type_id: u32,
///   updated_at: i64, weight: f32, key_len: u16,
///   prop_hash_offset: u64, prop_hash_count: u32,
///   last_write_seq: u64, reserved: u16
///
fn write_node_meta(
    seg_dir: &Path,
    node_data: &[(u64, u64, u32)],
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<(), EngineError> {
    let meta_path = seg_dir.join("node_meta.dat");

    let meta_file = File::create(&meta_path)?;
    let mut meta_w = BufWriter::new(meta_file);

    let count = node_data.len() as u64;
    write_u64(&mut meta_w, count)?;

    for &(node_id, data_offset, data_len) in node_data {
        let node = nodes.get(&node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} not found for sidecar", node_id))
        })?;

        // Write node_meta entry (60 bytes)
        write_u64(&mut meta_w, node_id)?;
        write_u64(&mut meta_w, data_offset)?;
        write_u32(&mut meta_w, data_len)?;
        write_u32(&mut meta_w, node.type_id)?;
        meta_w.write_all(&node.updated_at.to_le_bytes())?;
        meta_w.write_all(&node.weight.to_le_bytes())?;
        write_u16(&mut meta_w, node.key.len() as u16)?;
        write_u64(&mut meta_w, 0)?;
        write_u32(&mut meta_w, 0)?;
        write_u64(&mut meta_w, node.last_write_seq)?;
        write_u16(&mut meta_w, 0)?; // reserved
    }

    meta_w.flush()?;
    meta_w.get_ref().sync_all()?;
    Ok(())
}

fn write_node_vector_sidecars(
    seg_dir: &Path,
    node_data: &[(u64, u64, u32)],
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<Vec<DensePointInput>, EngineError> {
    let mut has_dense = false;
    let mut has_sparse = false;
    for &(node_id, _, _) in node_data {
        let node = nodes.get(&node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} not found for vector sidecar", node_id))
        })?;
        has_dense |= node.dense_vector.is_some();
        has_sparse |= node.sparse_vector.is_some();
    }

    if !has_dense && !has_sparse {
        return Ok(Vec::new());
    }

    let meta_file = File::create(seg_dir.join(NODE_VECTOR_META_FILENAME))?;
    let mut meta_w = BufWriter::new(meta_file);
    write_u64(&mut meta_w, node_data.len() as u64)?;

    let mut dense_w = if has_dense {
        Some(BufWriter::new(File::create(
            seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME),
        )?))
    } else {
        None
    };
    let mut sparse_w = if has_sparse {
        Some(BufWriter::new(File::create(
            seg_dir.join(NODE_SPARSE_VECTOR_BLOB_FILENAME),
        )?))
    } else {
        None
    };

    let mut dense_offset = 0u64;
    let mut sparse_offset = 0u64;
    let mut dense_points = Vec::new();

    for &(node_id, _, _) in node_data {
        let node = nodes.get(&node_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} not found for vector sidecar", node_id))
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
            dense_points.push(DensePointInput {
                node_id,
                dense_vector_offset: entry_dense_offset,
                values: values.clone(),
            });
            let w = dense_w.as_mut().expect("dense blob writer must exist");
            for &value in values {
                w.write_all(&value.to_le_bytes())?;
            }
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
            let w = sparse_w.as_mut().expect("sparse blob writer must exist");
            for &(dimension_id, weight) in values {
                write_u32(w, dimension_id)?;
                w.write_all(&weight.to_le_bytes())?;
            }
            sparse_offset = sparse_offset
                .checked_add(values.len() as u64 * SPARSE_VECTOR_ENTRY_SIZE)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("sparse vector blob offset overflow".into())
                })?;
        }

        write_u8(&mut meta_w, flags)?;
        meta_w.write_all(&[0u8; 3])?;
        write_u64(&mut meta_w, entry_dense_offset)?;
        write_u32(&mut meta_w, dense_len)?;
        write_u64(&mut meta_w, entry_sparse_offset)?;
        write_u32(&mut meta_w, sparse_len)?;
    }

    meta_w.flush()?;
    meta_w.get_ref().sync_all()?;

    if let Some(mut w) = dense_w {
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    if let Some(mut w) = sparse_w {
        w.flush()?;
        w.get_ref().sync_all()?;
    }

    Ok(dense_points)
}

/// edge_meta.dat format:
/// [count: u64]
/// [entries: count × EdgeMetaEntry, sorted by edge_id]
///
/// EdgeMetaEntry (80 bytes):
///   edge_id: u64, data_offset: u64, data_len: u32,
///   from: u64, to: u64, type_id: u32,
///   updated_at: i64, weight: f32,
///   valid_from: i64, valid_to: i64,
///   last_write_seq: u64, reserved: u32
fn write_edge_meta(
    seg_dir: &Path,
    edge_data: &[(u64, u64, u32)],
    edges: &NodeIdMap<EdgeRecord>,
) -> Result<(), EngineError> {
    let path = seg_dir.join("edge_meta.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let count = edge_data.len() as u64;
    write_u64(&mut w, count)?;

    for &(edge_id, data_offset, data_len) in edge_data {
        let edge = edges.get(&edge_id).ok_or_else(|| {
            EngineError::CorruptRecord(format!("edge {} not found for sidecar", edge_id))
        })?;

        write_u64(&mut w, edge_id)?;
        write_u64(&mut w, data_offset)?;
        write_u32(&mut w, data_len)?;
        write_u64(&mut w, edge.from)?;
        write_u64(&mut w, edge.to)?;
        write_u32(&mut w, edge.type_id)?;
        w.write_all(&edge.updated_at.to_le_bytes())?;
        w.write_all(&edge.weight.to_le_bytes())?;
        w.write_all(&edge.valid_from.to_le_bytes())?;
        w.write_all(&edge.valid_to.to_le_bytes())?;
        write_u64(&mut w, edge.last_write_seq)?;
        write_u32(&mut w, 0)?; // reserved
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// Write segment format version file: 4-byte magic + 4-byte version (little-endian).
fn write_format_version(seg_dir: &Path) -> Result<(), EngineError> {
    let path = seg_dir.join("format.ver");
    let mut data = Vec::with_capacity(8);
    data.extend_from_slice(&SEGMENT_MAGIC);
    data.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    fs::write(path, &data)?;
    Ok(())
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

/// Write merged nodes.dat by binary copy from multiple non-overlapping segments.
///
/// Instead of deserializing and re-serializing every record, this copies raw
/// record bytes directly from mmap'd input segments and rebuilds the merged
/// index with adjusted offsets. Record lengths are derived from the source
/// nodes.dat index/data layout, which lets the fast path cross-check sidecar
/// metadata later instead of trusting it blindly.
///
/// Returns per-segment offset rebasing info so compaction metadata can compute
/// merged `data_offset` values directly from sidecars without a second data scan.
pub(crate) fn write_merged_nodes_dat(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
) -> Result<Vec<FastMergeCopyInfo>, EngineError> {
    let path = seg_dir.join("nodes.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let mut seg_info: Vec<(u64, usize, usize)> = Vec::with_capacity(segments.len());
    let mut total_count: u64 = 0;

    for (seg_idx, seg) in segments.iter().enumerate() {
        let mmap = seg.raw_nodes_mmap();
        if mmap.len() < 8 {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} nodes.dat too short for count header: {} bytes",
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
                "segment {} nodes.dat index exceeds file length: start={}, len={}",
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

    write_u64(&mut w, total_count)?;

    let merged_data_start = 8u64
        .checked_add(
            total_count
                .checked_mul(NODE_INDEX_ENTRY_SIZE)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("merged node index size overflow".into())
                })?,
        )
        .ok_or_else(|| EngineError::CorruptRecord("merged node data start overflow".into()))?;
    let mut cumulative_data_offset = merged_data_start;
    let mut data_offsets: Vec<u64> = Vec::with_capacity(segments.len());
    for &(_, _, data_size) in &seg_info {
        data_offsets.push(cumulative_data_offset);
        cumulative_data_offset = cumulative_data_offset
            .checked_add(data_size as u64)
            .ok_or_else(|| EngineError::CorruptRecord("merged nodes.dat size overflow".into()))?;
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
            let node_id = u64::from_le_bytes(mmap[entry_off..entry_off + 8].try_into().unwrap());
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
        write_u64(&mut w, node_id)?;
        write_u64(&mut w, offset)?;
    }

    for (seg_idx, seg) in segments.iter().enumerate() {
        let mmap = seg.raw_nodes_mmap();
        let (_, data_start, data_size) = seg_info[seg_idx];
        if data_size > 0 {
            w.write_all(&mmap[data_start..data_start + data_size])?;
        }
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(seg_info
        .into_iter()
        .zip(data_offsets)
        .map(|((_, data_start, _), new_data_base)| FastMergeCopyInfo {
            orig_data_start: data_start as u64,
            new_data_base,
        })
        .collect())
}

/// Write merged edges.dat by binary copy from multiple non-overlapping segments.
/// Same approach as `write_merged_nodes_dat`.
///
/// Returns per-segment offset rebasing info so compaction metadata can compute
/// merged `data_offset` values directly from sidecars without a second data scan.
pub(crate) fn write_merged_edges_dat(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
) -> Result<Vec<FastMergeCopyInfo>, EngineError> {
    let path = seg_dir.join("edges.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let mut seg_info: Vec<(u64, usize, usize)> = Vec::with_capacity(segments.len());
    let mut total_count: u64 = 0;

    for (seg_idx, seg) in segments.iter().enumerate() {
        let mmap = seg.raw_edges_mmap();
        if mmap.len() < 8 {
            return Err(EngineError::CorruptRecord(format!(
                "segment {} edges.dat too short for count header: {} bytes",
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
                "segment {} edges.dat index exceeds file length: start={}, len={}",
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

    write_u64(&mut w, total_count)?;

    let merged_data_start = 8u64
        .checked_add(
            total_count
                .checked_mul(EDGE_INDEX_ENTRY_SIZE)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("merged edge index size overflow".into())
                })?,
        )
        .ok_or_else(|| EngineError::CorruptRecord("merged edge data start overflow".into()))?;
    let mut cumulative_data_offset = merged_data_start;
    let mut data_offsets: Vec<u64> = Vec::with_capacity(segments.len());
    for &(_, _, data_size) in &seg_info {
        data_offsets.push(cumulative_data_offset);
        cumulative_data_offset = cumulative_data_offset
            .checked_add(data_size as u64)
            .ok_or_else(|| EngineError::CorruptRecord("merged edges.dat size overflow".into()))?;
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
            let edge_id = u64::from_le_bytes(mmap[entry_off..entry_off + 8].try_into().unwrap());
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
        write_u64(&mut w, edge_id)?;
        write_u64(&mut w, offset)?;
    }

    for (seg_idx, seg) in segments.iter().enumerate() {
        let mmap = seg.raw_edges_mmap();
        let (_, data_start, data_size) = seg_info[seg_idx];
        if data_size > 0 {
            w.write_all(&mmap[data_start..data_start + data_size])?;
        }
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(seg_info
        .into_iter()
        .zip(data_offsets)
        .map(|((_, data_start, _), new_data_base)| FastMergeCopyInfo {
            orig_data_start: data_start as u64,
            new_data_base,
        })
        .collect())
}

/// Write nodes.dat by raw-copying only winning record byte spans from source segments.
///
/// Used by V3 compaction: the planner has already decided which records win,
/// so we skip all dropped records entirely (never decode them).
///
/// `winners` is sorted by node_id: `(node_id, seg_idx, data_offset, data_len)`.
///
/// Returns Vec of `(node_id, new_data_offset, data_len)` matching the output file,
/// for sidecar writing.
pub(crate) fn write_v3_nodes_dat(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    winners: &[(u64, usize, u64, u32)],
) -> Result<Vec<(u64, u64, u32)>, EngineError> {
    let path = seg_dir.join("nodes.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let count = winners.len() as u64;
    write_u64(&mut w, count)?;

    // Calculate data section start
    let data_start = 8 + count * NODE_INDEX_ENTRY_SIZE;

    // Build index entries and output info
    let mut node_data = Vec::with_capacity(winners.len());
    let mut data_offset = data_start;
    for &(node_id, _, _, data_len) in winners {
        // Write index entry: (node_id, offset)
        write_u64(&mut w, node_id)?;
        write_u64(&mut w, data_offset)?;
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

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(node_data)
}

/// Write edges.dat by raw-copying only winning record byte spans from source segments.
///
/// Same approach as `write_v3_nodes_dat` but for edge records.
///
/// `winners` is sorted by edge_id: `(edge_id, seg_idx, data_offset, data_len)`.
///
/// Returns Vec of `(edge_id, new_data_offset, data_len)` matching the output file.
pub(crate) fn write_v3_edges_dat(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    winners: &[(u64, usize, u64, u32)],
) -> Result<Vec<(u64, u64, u32)>, EngineError> {
    let path = seg_dir.join("edges.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let count = winners.len() as u64;
    write_u64(&mut w, count)?;

    let data_start = 8 + count * EDGE_INDEX_ENTRY_SIZE;

    let mut edge_data = Vec::with_capacity(winners.len());
    let mut data_offset = data_start;
    for &(edge_id, _, _, data_len) in winners {
        write_u64(&mut w, edge_id)?;
        write_u64(&mut w, data_offset)?;
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

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(edge_data)
}

// ==========================================================================
// Metadata-driven compaction index writers (V3)
// ==========================================================================

/// Node metadata collected from source sidecars for metadata-driven index building.
pub(crate) struct CompactNodeMeta {
    pub node_id: u64,
    pub new_data_offset: u64,
    pub data_len: u32,
    pub type_id: u32,
    pub updated_at: i64,
    pub weight: f32,
    pub key_len: u16,
    #[allow(dead_code)]
    pub prop_hash_offset: u64,
    #[allow(dead_code)]
    pub prop_hash_count: u32,
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
    pub type_id: u32,
    pub updated_at: i64,
    pub weight: f32,
    pub valid_from: i64,
    pub valid_to: i64,
    pub last_write_seq: u64,
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
#[allow(dead_code)]
pub(crate) fn write_indexes_from_metadata(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    dense_config: Option<&DenseVectorConfig>,
) -> Result<(), EngineError> {
    let _ = write_indexes_from_metadata_with_secondary_indexes(
        seg_dir,
        segments,
        node_metas,
        edge_metas,
        dense_config,
        true,
        &[],
    )?;
    Ok(())
}

pub(crate) fn write_indexes_from_metadata_with_secondary_indexes(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    dense_config: Option<&DenseVectorConfig>,
    write_degree_sidecar: bool,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SecondaryIndexMaintenanceReport, EngineError> {
    let report = Arc::new(Mutex::new(SecondaryIndexMaintenanceReport::default()));
    let node_report = Arc::clone(&report);
    run_index_fanout(
        || {
            write_key_index_from_meta(seg_dir, segments, node_metas)?;
            write_node_type_index_from_meta(seg_dir, node_metas)?;
            let branch_report = write_declared_equality_sidecars_from_metadata(
                seg_dir,
                segments,
                node_metas,
                secondary_indexes,
            )?;
            node_report
                .lock()
                .unwrap()
                .failed_equality_indexes
                .extend(branch_report.failed_equality_indexes);
            let branch_report = write_declared_range_sidecars_from_metadata(
                seg_dir,
                segments,
                node_metas,
                secondary_indexes,
            )?;
            let mut report = node_report.lock().unwrap();
            report
                .failed_range_indexes
                .extend(branch_report.failed_range_indexes);
            write_timestamp_index_from_meta(seg_dir, node_metas)?;
            Ok(())
        },
        || {
            write_adjacency_from_meta(seg_dir, "adj_out", edge_metas, true)?;
            write_adjacency_from_meta(seg_dir, "adj_in", edge_metas, false)?;
            write_edge_type_index_from_meta(seg_dir, edge_metas)?;
            write_edge_triple_index_from_meta(seg_dir, edge_metas)?;
            write_empty_tombstones(seg_dir)?;
            if write_degree_sidecar {
                if let Some(sidecars) = degree_sidecars_for_segments(segments) {
                    write_folded_degree_delta_sidecar_from_sidecars(
                        &seg_dir.join(DEGREE_DELTA_FILENAME),
                        &sidecars,
                    )?;
                }
            }
            Ok(())
        },
        || {
            let dense_points = write_sidecars_from_meta(seg_dir, segments, node_metas, edge_metas)?;
            write_dense_hnsw_index_from_points(seg_dir, dense_config, dense_points)?;
            Ok(())
        },
        || write_sparse_posting_index_from_meta(seg_dir, segments, node_metas),
    )?;
    write_format_version(seg_dir)?;
    fsync_dir(seg_dir)?;
    let final_report = report.lock().unwrap().clone();
    Ok(final_report)
}

fn degree_sidecars_for_segments(
    segments: &[Arc<SegmentReader>],
) -> Option<Vec<&crate::degree_cache::DegreeSidecar>> {
    segments
        .iter()
        .map(|segment| segment.degree_delta_sidecar())
        .collect()
}

/// key_index.dat from metadata: read key bytes via partial header parse from source segments.
fn write_key_index_from_meta(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    let path = seg_dir.join("key_index.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    // Collect (type_id, key_bytes, node_id) by reading key from source segment raw data
    let mut entries: Vec<(u32, Vec<u8>, u64)> = Vec::with_capacity(node_metas.len());
    for nm in node_metas {
        let src_mmap = segments[nm.src_seg_idx].raw_nodes_mmap();
        // Raw node record layout: type_id(4) + key_len(2) + key_bytes(key_len) + ...
        let key_start = nm.src_data_offset as usize + 6;
        let key_end = key_start + nm.key_len as usize;
        if key_end > src_mmap.len() {
            return Err(EngineError::CorruptRecord(format!(
                "node {} key bytes [{}, {}) exceed source mmap length {}",
                nm.node_id,
                key_start,
                key_end,
                src_mmap.len()
            )));
        }
        entries.push((
            nm.type_id,
            src_mmap[key_start..key_end].to_vec(),
            nm.node_id,
        ));
    }

    // Sort by (type_id, key)
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let count = entries.len() as u64;
    write_u64(&mut w, count)?;

    // Pre-compute entry sizes for offset table
    // Each entry: type_id (4) + node_id (8) + key_len (2) + key_bytes
    let entry_sizes: Vec<u64> = entries
        .iter()
        .map(|(_, key, _)| 4 + 8 + 2 + key.len() as u64)
        .collect();

    let data_start = 8 + count * 8;
    let mut offset = data_start;
    for &size in &entry_sizes {
        write_u64(&mut w, offset)?;
        offset += size;
    }

    // Write data entries
    for (type_id, key_bytes, node_id) in &entries {
        write_u32(&mut w, *type_id)?;
        write_u64(&mut w, *node_id)?;
        write_u16(&mut w, key_bytes.len() as u16)?;
        w.write_all(key_bytes)?;
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// node_type_index.dat from metadata.
fn write_node_type_index_from_meta(
    seg_dir: &Path,
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    let mut groups: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for nm in node_metas {
        groups.entry(nm.type_id).or_default().push(nm.node_id);
    }
    for ids in groups.values_mut() {
        ids.sort_unstable();
    }
    write_type_index_groups(seg_dir, "node_type_index", &groups)
}

/// edge_type_index.dat from metadata.
fn write_edge_type_index_from_meta(
    seg_dir: &Path,
    edge_metas: &[CompactEdgeMeta],
) -> Result<(), EngineError> {
    let mut groups: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for em in edge_metas {
        groups.entry(em.type_id).or_default().push(em.edge_id);
    }
    for ids in groups.values_mut() {
        ids.sort_unstable();
    }
    write_type_index_groups(seg_dir, "edge_type_index", &groups)
}

/// Shared writer for type index files from pre-grouped data.
fn write_type_index_groups(
    seg_dir: &Path,
    filename: &str,
    groups: &BTreeMap<u32, Vec<u64>>,
) -> Result<(), EngineError> {
    let path = seg_dir.join(format!("{}.dat", filename));
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let entry_count = groups.len() as u64;
    write_u64(&mut w, entry_count)?;

    let data_start = 8 + entry_count * TYPE_INDEX_ENTRY_SIZE;
    let mut data_offset = data_start;

    for (&type_id, ids) in groups {
        let count = ids.len() as u32;
        write_u32(&mut w, type_id)?;
        write_u64(&mut w, data_offset)?;
        write_u32(&mut w, count)?;
        data_offset += count as u64 * 8;
    }

    for ids in groups.values() {
        for &id in ids {
            write_u64(&mut w, id)?;
        }
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// edge_triple_index.dat from metadata.
fn write_edge_triple_index_from_meta(
    seg_dir: &Path,
    edge_metas: &[CompactEdgeMeta],
) -> Result<(), EngineError> {
    let path = seg_dir.join("edge_triple_index.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    let mut entries: Vec<(u64, u64, u32, u64)> = edge_metas
        .iter()
        .map(|em| (em.from, em.to, em.type_id, em.edge_id))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));

    let count = entries.len() as u64;
    write_u64(&mut w, count)?;

    for &(from, to, type_id, edge_id) in &entries {
        write_u64(&mut w, from)?;
        write_u64(&mut w, to)?;
        write_u32(&mut w, type_id)?;
        write_u64(&mut w, edge_id)?;
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// Adjacency index from edge metadata. Builds adj_out (is_outgoing=true) or adj_in (is_outgoing=false).
#[allow(clippy::type_complexity)]
fn write_adjacency_from_meta(
    seg_dir: &Path,
    prefix: &str,
    edge_metas: &[CompactEdgeMeta],
    is_outgoing: bool,
) -> Result<(), EngineError> {
    let idx_path = seg_dir.join(format!("{}.idx", prefix));
    let dat_path = seg_dir.join(format!("{}.dat", prefix));

    let idx_file = File::create(&idx_path)?;
    let dat_file = File::create(&dat_path)?;
    let mut idx_w = BufWriter::new(idx_file);
    let mut dat_w = BufWriter::new(dat_file);

    // Group entries by (node_id, type_id)
    // For adj_out: node_id = from, neighbor = to
    // For adj_in:  node_id = to,   neighbor = from
    let mut groups: BTreeMap<(u64, u32), Vec<(u64, u64, f32, i64, i64)>> = BTreeMap::new();
    for em in edge_metas {
        let (node_id, neighbor_id) = if is_outgoing {
            (em.from, em.to)
        } else {
            (em.to, em.from)
        };
        groups.entry((node_id, em.type_id)).or_default().push((
            em.edge_id,
            neighbor_id,
            em.weight,
            em.valid_from,
            em.valid_to,
        ));
    }

    // Sort postings within each group by edge_id
    for postings in groups.values_mut() {
        postings.sort_unstable_by_key(|&(edge_id, ..)| edge_id);
    }

    let count = groups.len() as u64;
    write_u64(&mut idx_w, count)?;

    let mut posting_buf = Vec::new();
    let mut dat_offset: u64 = 0;
    let mut index_entries: Vec<(u64, u32, u64, u32)> = Vec::with_capacity(groups.len());

    for (&(node_id, type_id), postings) in &groups {
        let posting_count = postings.len() as u32;
        index_entries.push((node_id, type_id, dat_offset, posting_count));

        posting_buf.clear();
        let mut prev_edge_id: u64 = 0;
        for &(edge_id, neighbor_id, weight, valid_from, valid_to) in postings {
            let delta = edge_id - prev_edge_id;
            prev_edge_id = edge_id;

            write_varint_to_vec(&mut posting_buf, delta);
            write_varint_to_vec(&mut posting_buf, neighbor_id);
            posting_buf.extend_from_slice(&weight.to_le_bytes());
            write_varint_to_vec(&mut posting_buf, valid_from as u64);
            let vt_enc = if valid_to == i64::MAX {
                0u64
            } else {
                valid_to as u64 + 1
            };
            write_varint_to_vec(&mut posting_buf, vt_enc);
        }

        dat_w.write_all(&posting_buf)?;
        dat_offset += posting_buf.len() as u64;
    }

    // Write index entries
    for &(node_id, type_id, offset, posting_count) in &index_entries {
        write_u64(&mut idx_w, node_id)?;
        write_u32(&mut idx_w, type_id)?;
        write_u64(&mut idx_w, offset)?;
        write_u32(&mut idx_w, posting_count)?;
    }

    idx_w.flush()?;
    idx_w.get_ref().sync_all()?;
    dat_w.flush()?;
    dat_w.get_ref().sync_all()?;
    Ok(())
}

fn build_secondary_eq_groups_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    index_id: u64,
    type_id: u32,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let winner_sources: HashMap<u64, usize> = node_metas
        .iter()
        .filter(|meta| meta.type_id == type_id)
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

fn build_secondary_eq_groups_from_targeted_decode(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    type_id: u32,
    prop_key: &str,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for meta in node_metas.iter().filter(|meta| meta.type_id == type_id) {
        if let Some(value) = segments[meta.src_seg_idx].node_property_value_at_offset(
            meta.node_id,
            meta.src_data_offset,
            prop_key,
        )? {
            groups
                .entry(hash_prop_value(&value))
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

fn build_secondary_range_entries_from_source_sidecars(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    index_id: u64,
    type_id: u32,
) -> Result<Vec<(u64, u64)>, EngineError> {
    let winner_sources: HashMap<u64, usize> = node_metas
        .iter()
        .filter(|meta| meta.type_id == type_id)
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

fn build_secondary_range_entries_from_targeted_decode(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    type_id: u32,
    prop_key: &str,
    domain: SecondaryIndexRangeDomain,
) -> Result<Vec<(u64, u64)>, EngineError> {
    let mut entries = Vec::new();

    for meta in node_metas.iter().filter(|meta| meta.type_id == type_id) {
        let Some(value) = segments[meta.src_seg_idx].node_property_value_at_offset(
            meta.node_id,
            meta.src_data_offset,
            prop_key,
        )?
        else {
            continue;
        };
        let Some(encoded_value) = encode_range_prop_value(domain, &value) else {
            continue;
        };
        entries.push((encoded_value, meta.node_id));
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn write_declared_equality_sidecars_from_metadata(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SecondaryIndexMaintenanceReport, EngineError> {
    let eq_entries: Vec<&SecondaryIndexManifestEntry> = secondary_indexes
        .iter()
        .filter(|entry| matches!(entry.kind, SecondaryIndexKind::Equality))
        .collect();
    if eq_entries.is_empty() {
        return Ok(SecondaryIndexMaintenanceReport::default());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;
    let mut report = SecondaryIndexMaintenanceReport::default();

    for entry in eq_entries {
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        let mut failure_message = None;
        let use_source_sidecars = if entry.state == SecondaryIndexState::Failed {
            false
        } else {
            let mut all_present = true;
            for seg in segments {
                match seg.validate_secondary_eq_sidecar(entry.index_id) {
                    Ok(true) => {}
                    Ok(false) => {
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
                *type_id,
            )?
        } else {
            build_secondary_eq_groups_from_targeted_decode(
                segments, node_metas, *type_id, prop_key,
            )?
        };

        if let Some(message) = failure_message {
            report
                .failed_equality_indexes
                .push((entry.index_id, message));
        }

        write_node_prop_eq_sidecar_to_path(
            &node_prop_eq_sidecar_path(seg_dir, entry.index_id),
            &groups,
        )?;
    }

    fsync_dir(&index_dir)?;
    Ok(report)
}

fn write_declared_range_sidecars_from_metadata(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SecondaryIndexMaintenanceReport, EngineError> {
    let range_entries: Vec<&SecondaryIndexManifestEntry> = secondary_indexes
        .iter()
        .filter(|entry| matches!(entry.kind, SecondaryIndexKind::Range { .. }))
        .collect();
    if range_entries.is_empty() {
        return Ok(SecondaryIndexMaintenanceReport::default());
    }

    let index_dir = secondary_indexes_dir(seg_dir);
    fs::create_dir_all(&index_dir)?;
    let mut report = SecondaryIndexMaintenanceReport::default();

    for entry in range_entries {
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        let SecondaryIndexKind::Range { domain } = entry.kind else {
            continue;
        };
        let mut failure_message = None;
        let use_source_sidecars = if entry.state == SecondaryIndexState::Failed {
            false
        } else {
            let mut all_present = true;
            for seg in segments {
                match seg.validate_secondary_range_sidecar(entry.index_id) {
                    Ok(true) => {}
                    Ok(false) => {
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
                *type_id,
            )?
        } else {
            build_secondary_range_entries_from_targeted_decode(
                segments, node_metas, *type_id, prop_key, domain,
            )?
        };

        if let Some(message) = failure_message {
            report.failed_range_indexes.push((entry.index_id, message));
        }

        write_node_prop_range_sidecar_to_path(
            &node_prop_range_sidecar_path(seg_dir, entry.index_id),
            &sidecar_entries,
        )?;
    }

    fsync_dir(&index_dir)?;
    Ok(report)
}

/// timestamp_index.dat from metadata.
fn write_timestamp_index_from_meta(
    seg_dir: &Path,
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    let path = seg_dir.join("timestamp_index.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);

    // Build sorted entries from metadata
    let mut entries: Vec<(u32, i64, u64)> = node_metas
        .iter()
        .map(|nm| (nm.type_id, nm.updated_at, nm.node_id))
        .collect();
    entries.sort_unstable();

    let count = entries.len() as u64;
    write_u64(&mut w, count)?;

    for &(type_id, updated_at, node_id) in &entries {
        write_u32(&mut w, type_id)?;
        w.write_all(&updated_at.to_le_bytes())?;
        write_u64(&mut w, node_id)?;
    }

    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// Write empty tombstones.dat (count=0). After compaction, tombstones are consumed.
fn write_empty_tombstones(seg_dir: &Path) -> Result<(), EngineError> {
    let path = seg_dir.join("tombstones.dat");
    let file = File::create(&path)?;
    let mut w = BufWriter::new(file);
    write_u64(&mut w, 0)?;
    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}

/// Write output metadata sidecars from compaction metadata.
fn write_sidecars_from_meta(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
) -> Result<Vec<DensePointInput>, EngineError> {
    // node_meta.dat
    let meta_path = seg_dir.join("node_meta.dat");

    let meta_file = File::create(&meta_path)?;
    let mut meta_w = BufWriter::new(meta_file);

    let count = node_metas.len() as u64;
    write_u64(&mut meta_w, count)?;

    for nm in node_metas {
        // Write node_meta entry with updated data_offset and prop_hash_offset (60 bytes)
        write_u64(&mut meta_w, nm.node_id)?;
        write_u64(&mut meta_w, nm.new_data_offset)?;
        write_u32(&mut meta_w, nm.data_len)?;
        write_u32(&mut meta_w, nm.type_id)?;
        meta_w.write_all(&nm.updated_at.to_le_bytes())?;
        meta_w.write_all(&nm.weight.to_le_bytes())?;
        write_u16(&mut meta_w, nm.key_len)?;
        write_u64(&mut meta_w, 0)?;
        write_u32(&mut meta_w, 0)?;
        write_u64(&mut meta_w, nm.last_write_seq)?;
        write_u16(&mut meta_w, 0)?; // reserved
    }

    meta_w.flush()?;
    meta_w.get_ref().sync_all()?;

    let dense_points = write_node_vector_sidecars_from_meta(seg_dir, segments, node_metas)?;

    // edge_meta.dat
    let edge_meta_path = seg_dir.join("edge_meta.dat");
    let edge_meta_file = File::create(&edge_meta_path)?;
    let mut em_w = BufWriter::new(edge_meta_file);

    let edge_count = edge_metas.len() as u64;
    write_u64(&mut em_w, edge_count)?;

    for em in edge_metas {
        write_u64(&mut em_w, em.edge_id)?;
        write_u64(&mut em_w, em.new_data_offset)?;
        write_u32(&mut em_w, em.data_len)?;
        write_u64(&mut em_w, em.from)?;
        write_u64(&mut em_w, em.to)?;
        write_u32(&mut em_w, em.type_id)?;
        em_w.write_all(&em.updated_at.to_le_bytes())?;
        em_w.write_all(&em.weight.to_le_bytes())?;
        em_w.write_all(&em.valid_from.to_le_bytes())?;
        em_w.write_all(&em.valid_to.to_le_bytes())?;
        write_u64(&mut em_w, em.last_write_seq)?;
        write_u32(&mut em_w, 0)?; // reserved
    }

    em_w.flush()?;
    em_w.get_ref().sync_all()?;
    Ok(dense_points)
}

fn write_node_vector_sidecars_from_meta(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<Vec<DensePointInput>, EngineError> {
    let has_dense = node_metas.iter().any(|nm| nm.dense_vector_len > 0);
    let has_sparse = node_metas.iter().any(|nm| nm.sparse_vector_len > 0);

    if !has_dense && !has_sparse {
        return Ok(Vec::new());
    }

    let meta_file = File::create(seg_dir.join(NODE_VECTOR_META_FILENAME))?;
    let mut meta_w = BufWriter::new(meta_file);
    write_u64(&mut meta_w, node_metas.len() as u64)?;

    let mut dense_w = if has_dense {
        Some(BufWriter::new(File::create(
            seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME),
        )?))
    } else {
        None
    };
    let mut sparse_w = if has_sparse {
        Some(BufWriter::new(File::create(
            seg_dir.join(NODE_SPARSE_VECTOR_BLOB_FILENAME),
        )?))
    } else {
        None
    };

    let mut new_dense_offset = 0u64;
    let mut new_sparse_offset = 0u64;
    let mut dense_points = Vec::new();

    for nm in node_metas {
        let mut flags = 0u8;
        let mut entry_dense_offset = 0u64;
        let mut entry_sparse_offset = 0u64;

        if nm.dense_vector_len > 0 {
            flags |= NODE_VECTOR_FLAG_DENSE;
            entry_dense_offset = new_dense_offset;
        }
        if nm.sparse_vector_len > 0 {
            flags |= NODE_VECTOR_FLAG_SPARSE;
            entry_sparse_offset = new_sparse_offset;
        }

        write_u8(&mut meta_w, flags)?;
        meta_w.write_all(&[0u8; 3])?;
        write_u64(&mut meta_w, entry_dense_offset)?;
        write_u32(&mut meta_w, nm.dense_vector_len)?;
        write_u64(&mut meta_w, entry_sparse_offset)?;
        write_u32(&mut meta_w, nm.sparse_vector_len)?;

        if nm.dense_vector_len > 0 {
            let src = segments[nm.src_seg_idx].raw_node_dense_vectors_mmap();
            let base = nm.dense_vector_offset as usize;
            let len = nm.dense_vector_len as usize * DENSE_VECTOR_VALUE_SIZE as usize;
            let end = base + len;
            if end > src.len() {
                return Err(EngineError::CorruptRecord(format!(
                    "node {} dense vector range [{}, {}) exceeds source length {}",
                    nm.node_id,
                    base,
                    end,
                    src.len()
                )));
            }
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
            dense_w
                .as_mut()
                .expect("dense blob writer must exist")
                .write_all(&src[base..end])?;
            new_dense_offset = new_dense_offset.checked_add(len as u64).ok_or_else(|| {
                EngineError::CorruptRecord("dense vector output offset overflow".into())
            })?;
        }

        if nm.sparse_vector_len > 0 {
            let src = segments[nm.src_seg_idx].raw_node_sparse_vectors_mmap();
            let base = nm.sparse_vector_offset as usize;
            let len = nm.sparse_vector_len as usize * SPARSE_VECTOR_ENTRY_SIZE as usize;
            let end = base + len;
            if end > src.len() {
                return Err(EngineError::CorruptRecord(format!(
                    "node {} sparse vector range [{}, {}) exceeds source length {}",
                    nm.node_id,
                    base,
                    end,
                    src.len()
                )));
            }
            sparse_w
                .as_mut()
                .expect("sparse blob writer must exist")
                .write_all(&src[base..end])?;
            new_sparse_offset = new_sparse_offset.checked_add(len as u64).ok_or_else(|| {
                EngineError::CorruptRecord("sparse vector output offset overflow".into())
            })?;
        }
    }

    meta_w.flush()?;
    meta_w.get_ref().sync_all()?;

    if let Some(mut w) = dense_w {
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    if let Some(mut w) = sparse_w {
        w.flush()?;
        w.get_ref().sync_all()?;
    }

    Ok(dense_points)
}

fn write_sparse_posting_index(
    seg_dir: &Path,
    nodes: &NodeIdMap<NodeRecord>,
) -> Result<(), EngineError> {
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
    write_sparse_posting_files(seg_dir, &groups)
}

fn write_sparse_posting_index_from_meta(
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
) -> Result<(), EngineError> {
    let mut groups: BTreeMap<u32, Vec<(u64, f32)>> = BTreeMap::new();
    for nm in node_metas {
        if nm.sparse_vector_len == 0 {
            continue;
        }
        let src = segments[nm.src_seg_idx].raw_node_sparse_vectors_mmap();
        let base = nm.sparse_vector_offset as usize;
        let len = nm.sparse_vector_len as usize * SPARSE_VECTOR_ENTRY_SIZE as usize;
        let end = base + len;
        if end > src.len() {
            return Err(EngineError::CorruptRecord(format!(
                "node {} sparse posting source range [{}, {}) exceeds source length {}",
                nm.node_id,
                base,
                end,
                src.len()
            )));
        }
        for index in 0..nm.sparse_vector_len as usize {
            let entry_offset = base + index * SPARSE_VECTOR_ENTRY_SIZE as usize;
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
    write_sparse_posting_files(seg_dir, &groups)
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

    fn write_segment(
        seg_dir: &Path,
        segment_id: u64,
        memtable: &Memtable,
        dense_config: Option<&DenseVectorConfig>,
    ) -> Result<SegmentInfo, EngineError> {
        let degree_overlay = DegreeOverlaySnapshot::empty();
        super::write_segment(
            seg_dir,
            segment_id,
            memtable,
            dense_config,
            degree_overlay.as_ref(),
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
        super::write_segment_with_secondary_indexes(
            seg_dir,
            segment_id,
            memtable,
            dense_config,
            degree_overlay.as_ref(),
            secondary_indexes,
        )
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
            updated_at: 1001,
            weight: 0.5,
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

    // --- encode_node_record / encode_edge_record ---

    #[test]
    fn test_encode_node_record_roundtrip() {
        let node = make_node_with_props(42, 1, "alice");
        let mut buf = Vec::new();
        encode_node_record_into(&mut buf, &node).unwrap();

        // Verify structure (no id): type_id(4) + key_len(2) + key(5) + created(8) + updated(8) + weight(4) + props_len(4) + props(N)
        assert!(buf.len() > 30 + 5); // minimum size with key "alice"

        let type_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(type_id, 1);

        let key_len = u16::from_le_bytes(buf[4..6].try_into().unwrap()) as usize;
        assert_eq!(key_len, 5);

        let key = std::str::from_utf8(&buf[6..6 + key_len]).unwrap();
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

        let type_id = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        assert_eq!(type_id, 10);
    }

    // --- write_nodes_dat ---

    #[test]
    fn test_write_nodes_dat_empty() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = NodeIdMap::default();
        write_nodes_dat(dir.path(), &nodes).unwrap();

        let data = fs::read(dir.path().join("nodes.dat")).unwrap();
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
        assert_eq!(data.len(), 8); // just the count
    }

    #[test]
    fn test_write_nodes_dat_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = NodeIdMap::default();
        nodes.insert(3, make_node(3, 1, "charlie"));
        nodes.insert(1, make_node(1, 1, "alice"));
        nodes.insert(2, make_node(2, 1, "bob"));

        write_nodes_dat(dir.path(), &nodes).unwrap();

        let data = fs::read(dir.path().join("nodes.dat")).unwrap();
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

        // Verify the offset of the first record leads to valid data
        // Format v4: id is NOT in the record, first field is type_id (u32)
        let offset0 =
            u64::from_le_bytes(data[idx_start + 8..idx_start + 16].try_into().unwrap()) as usize;
        let type_id = u32::from_le_bytes(data[offset0..offset0 + 4].try_into().unwrap());
        assert_eq!(type_id, 1);
    }

    // --- write_edges_dat ---

    #[test]
    fn test_write_edges_dat_empty() {
        let dir = tempfile::tempdir().unwrap();
        let edges = NodeIdMap::default();
        write_edges_dat(dir.path(), &edges).unwrap();

        let data = fs::read(dir.path().join("edges.dat")).unwrap();
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_write_edges_dat_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let mut edges = NodeIdMap::default();
        edges.insert(2, make_edge(2, 1, 3, 10));
        edges.insert(1, make_edge(1, 1, 2, 10));

        write_edges_dat(dir.path(), &edges).unwrap();

        let data = fs::read(dir.path().join("edges.dat")).unwrap();
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

    fn make_adj(edge_id: u64, type_id: u32, neighbor_id: u64, weight: f32) -> AdjEntry {
        AdjEntry {
            edge_id,
            type_id,
            neighbor_id,
            weight,
            valid_from: 1000,
            valid_to: i64::MAX,
        }
    }

    fn adj_map_from(node_id: u64, entries: Vec<AdjEntry>) -> NodeIdMap<NodeIdMap<AdjEntry>> {
        let mut outer = NodeIdMap::default();
        let mut inner = NodeIdMap::default();
        for e in entries {
            inner.insert(e.edge_id, e);
        }
        outer.insert(node_id, inner);
        outer
    }

    #[test]
    fn test_write_adjacency_empty() {
        let dir = tempfile::tempdir().unwrap();
        let adj: NodeIdMap<NodeIdMap<AdjEntry>> = NodeIdMap::default();
        write_adjacency_index(dir.path(), "adj_out", &adj).unwrap();

        let idx_data = fs::read(dir.path().join("adj_out.idx")).unwrap();
        let count = u64::from_le_bytes(idx_data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_write_adjacency_single_node() {
        let dir = tempfile::tempdir().unwrap();
        let adj = adj_map_from(
            1,
            vec![
                make_adj(10, 1, 2, 0.5),
                make_adj(11, 1, 3, 0.7),
                make_adj(12, 2, 4, 1.0),
            ],
        );

        write_adjacency_index(dir.path(), "adj_out", &adj).unwrap();

        let idx_data = fs::read(dir.path().join("adj_out.idx")).unwrap();
        let count = u64::from_le_bytes(idx_data[0..8].try_into().unwrap());
        // Node 1 has 2 type groups: type_id=1 (2 entries) and type_id=2 (1 entry)
        assert_eq!(count, 2);

        let dat_data = fs::read(dir.path().join("adj_out.dat")).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let mut adj: NodeIdMap<NodeIdMap<AdjEntry>> = NodeIdMap::default();
        let mut m5 = NodeIdMap::default();
        m5.insert(10, make_adj(10, 1, 6, 0.5));
        adj.insert(5, m5);
        let mut m1 = NodeIdMap::default();
        m1.insert(11, make_adj(11, 1, 2, 0.7));
        adj.insert(1, m1);

        write_adjacency_index(dir.path(), "adj_out", &adj).unwrap();

        let idx_data = fs::read(dir.path().join("adj_out.idx")).unwrap();
        let count = u64::from_le_bytes(idx_data[0..8].try_into().unwrap());
        assert_eq!(count, 2);

        // First index entry should be node_id=1 (sorted)
        let node_id_0 = u64::from_le_bytes(idx_data[8..16].try_into().unwrap());
        let node_id_1 = u64::from_le_bytes(idx_data[8 + 24..16 + 24].try_into().unwrap());
        assert_eq!(node_id_0, 1);
        assert_eq!(node_id_1, 5);
    }

    // --- write_key_index ---

    #[test]
    fn test_write_key_index_empty() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = NodeIdMap::default();
        write_key_index(dir.path(), &nodes).unwrap();

        let data = fs::read(dir.path().join("key_index.dat")).unwrap();
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
        assert_eq!(data.len(), 8);
    }

    #[test]
    fn test_write_key_index_sorted_by_type_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = NodeIdMap::default();
        nodes.insert(1, make_node(1, 2, "zebra"));
        nodes.insert(2, make_node(2, 1, "bob"));
        nodes.insert(3, make_node(3, 1, "alice"));

        write_key_index(dir.path(), &nodes).unwrap();

        let data = fs::read(dir.path().join("key_index.dat")).unwrap();
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 3);

        // Read offset table
        let offsets: Vec<u64> = (0..3)
            .map(|i| {
                let start = 8 + i * 8;
                u64::from_le_bytes(data[start..start + 8].try_into().unwrap())
            })
            .collect();

        // First entry should be type_id=1, key="alice"
        let off0 = offsets[0] as usize;
        let type0 = u32::from_le_bytes(data[off0..off0 + 4].try_into().unwrap());
        let node0 = u64::from_le_bytes(data[off0 + 4..off0 + 12].try_into().unwrap());
        let klen0 = u16::from_le_bytes(data[off0 + 12..off0 + 14].try_into().unwrap()) as usize;
        let key0 = std::str::from_utf8(&data[off0 + 14..off0 + 14 + klen0]).unwrap();
        assert_eq!(type0, 1);
        assert_eq!(key0, "alice");
        assert_eq!(node0, 3);

        // Second entry should be type_id=1, key="bob"
        let off1 = offsets[1] as usize;
        let type1 = u32::from_le_bytes(data[off1..off1 + 4].try_into().unwrap());
        let klen1 = u16::from_le_bytes(data[off1 + 12..off1 + 14].try_into().unwrap()) as usize;
        let key1 = std::str::from_utf8(&data[off1 + 14..off1 + 14 + klen1]).unwrap();
        assert_eq!(type1, 1);
        assert_eq!(key1, "bob");

        // Third entry should be type_id=2, key="zebra"
        let off2 = offsets[2] as usize;
        let type2 = u32::from_le_bytes(data[off2..off2 + 4].try_into().unwrap());
        assert_eq!(type2, 2);
    }

    // --- write_tombstones ---

    #[test]
    fn test_write_tombstones_empty() {
        let dir = tempfile::tempdir().unwrap();
        let dn = NodeIdMap::default();
        let de = NodeIdMap::default();
        write_tombstones(dir.path(), &dn, &de).unwrap();

        let data = fs::read(dir.path().join("tombstones.dat")).unwrap();
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_write_tombstones_mixed() {
        let dir = tempfile::tempdir().unwrap();
        let mut dn = NodeIdMap::default();
        dn.insert(
            5,
            TombstoneEntry {
                deleted_at: 1000,
                last_write_seq: 0,
            },
        );
        dn.insert(
            3,
            TombstoneEntry {
                deleted_at: 1001,
                last_write_seq: 0,
            },
        );
        let mut de = NodeIdMap::default();
        de.insert(
            10,
            TombstoneEntry {
                deleted_at: 2000,
                last_write_seq: 0,
            },
        );

        write_tombstones(dir.path(), &dn, &de).unwrap();

        let data = fs::read(dir.path().join("tombstones.dat")).unwrap();
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

        // Verify all files exist
        assert!(seg_dir.join("nodes.dat").exists());
        assert!(seg_dir.join("edges.dat").exists());
        assert!(seg_dir.join("adj_out.idx").exists());
        assert!(seg_dir.join("adj_out.dat").exists());
        assert!(seg_dir.join("adj_in.idx").exists());
        assert!(seg_dir.join("adj_in.dat").exists());
        assert!(seg_dir.join("key_index.dat").exists());
        assert!(seg_dir.join("tombstones.dat").exists());
        assert!(seg_dir.join("format.ver").exists());
        assert!(seg_dir.join("node_type_index.dat").exists());
        assert!(seg_dir.join("edge_type_index.dat").exists());
        assert!(seg_dir.join("edge_triple_index.dat").exists());
        // V5 sidecar files
        assert!(seg_dir.join("node_meta.dat").exists());
        assert!(seg_dir.join("edge_meta.dat").exists());
        assert!(!seg_dir.join("prop_index.dat").exists());
        assert!(!seg_dir.join("node_prop_hashes.dat").exists());
        assert!(!seg_dir.join(SECONDARY_INDEX_DIRNAME).exists());
        assert!(!seg_dir.join(NODE_VECTOR_META_FILENAME).exists());
        assert!(!seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME).exists());
        assert!(!seg_dir.join(NODE_SPARSE_VECTOR_BLOB_FILENAME).exists());
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

        let reader = SegmentReader::open(&seg_dir, 1, None).unwrap();
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

        let reader = SegmentReader::open(&seg_dir, 1, None).unwrap();
        assert!(!reader.degree_delta_available());
        assert!(reader.get_node(1).unwrap().is_some());

        std::fs::write(seg_dir.join(DEGREE_DELTA_FILENAME), b"not a degree sidecar").unwrap();
        let reader = SegmentReader::open(&seg_dir, 1, None).unwrap();
        assert!(!reader.degree_delta_available());
        assert!(reader.get_node(1).unwrap().is_some());
    }

    #[test]
    fn test_write_segment_empty_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");

        let mt = Memtable::new();
        let info = write_segment(&seg_dir, 1, &mt, None).unwrap();
        assert_eq!(info.node_count, 0);
        assert_eq!(info.edge_count, 0);

        // All files should still be created
        assert!(seg_dir.join("nodes.dat").exists());
        assert!(seg_dir.join("edges.dat").exists());
    }

    #[test]
    fn test_write_segment_with_vectors_writes_vector_sidecars() {
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

        assert!(seg_dir.join(NODE_VECTOR_META_FILENAME).exists());
        assert!(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME).exists());
        assert!(seg_dir.join(NODE_SPARSE_VECTOR_BLOB_FILENAME).exists());
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

        assert!(seg_dir.join(NODE_VECTOR_META_FILENAME).exists());
        assert!(!seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME).exists());
        assert!(seg_dir.join(NODE_SPARSE_VECTOR_BLOB_FILENAME).exists());
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
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = NodeIdMap::default();
        nodes.insert(1, make_node_with_props(1, 1, "alice"));

        write_nodes_dat(dir.path(), &nodes).unwrap();

        let data = fs::read(dir.path().join("nodes.dat")).unwrap();
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(count, 1);

        // Offset should point to valid data
        // Format v4: id NOT in record. Layout: type_id(4) + key_len(2) + key + timestamps(16) + weight(4) + props_len(4) + props
        let offset = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;
        let type_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        assert_eq!(type_id, 1);

        // Properties should be serialized
        let key_len = u16::from_le_bytes(data[offset + 4..offset + 6].try_into().unwrap()) as usize;
        let props_len_offset = offset + 6 + key_len + 8 + 8 + 4; // skip key + timestamps + weight
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

    // --- V5 sidecar tests ---

    #[test]
    fn test_write_node_meta_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = NodeIdMap::default();
        nodes.insert(1, make_node_with_props(1, 1, "alice"));
        nodes.insert(2, make_node(2, 2, "bob"));

        let node_data = write_nodes_dat(dir.path(), &nodes).unwrap();
        assert_eq!(node_data.len(), 2);
        // Sorted by id
        assert_eq!(node_data[0].0, 1);
        assert_eq!(node_data[1].0, 2);

        write_node_meta(dir.path(), &node_data, &nodes).unwrap();

        let meta = fs::read(dir.path().join("node_meta.dat")).unwrap();
        let count = u64::from_le_bytes(meta[0..8].try_into().unwrap());
        assert_eq!(count, 2);

        // Verify first entry fields (node_id=1)
        let off = 8;
        let nid = u64::from_le_bytes(meta[off..off + 8].try_into().unwrap());
        assert_eq!(nid, 1);
        let data_offset = u64::from_le_bytes(meta[off + 8..off + 16].try_into().unwrap());
        assert_eq!(data_offset, node_data[0].1);
        let data_len = u32::from_le_bytes(meta[off + 16..off + 20].try_into().unwrap());
        assert_eq!(data_len, node_data[0].2);
        let type_id = u32::from_le_bytes(meta[off + 20..off + 24].try_into().unwrap());
        assert_eq!(type_id, 1);
        let updated_at = i64::from_le_bytes(meta[off + 24..off + 32].try_into().unwrap());
        assert_eq!(updated_at, 1001); // make_node_with_props uses updated_at=1001
        let key_len = u16::from_le_bytes(meta[off + 36..off + 38].try_into().unwrap());
        assert_eq!(key_len, 5); // "alice"

        // CP2 stops emitting legacy property hash metadata for new segments.
        let prop_hash_count = u32::from_le_bytes(meta[off + 46..off + 50].try_into().unwrap());
        assert_eq!(prop_hash_count, 0);

        // Second entry (node_id=2)
        let off2 = 8 + 60; // NODE_META_ENTRY_SIZE = 60
        let nid2 = u64::from_le_bytes(meta[off2..off2 + 8].try_into().unwrap());
        assert_eq!(nid2, 2);
        let type_id2 = u32::from_le_bytes(meta[off2 + 20..off2 + 24].try_into().unwrap());
        assert_eq!(type_id2, 2);
        let prop_hash_count2 = u32::from_le_bytes(meta[off2 + 46..off2 + 50].try_into().unwrap());
        assert_eq!(prop_hash_count2, 0);
        assert!(!dir.path().join("node_prop_hashes.dat").exists());
    }

    #[test]
    fn test_write_edge_meta_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut edges = NodeIdMap::default();
        edges.insert(10, make_edge(10, 1, 2, 5));
        edges.insert(20, make_edge(20, 3, 4, 7));

        let edge_data = write_edges_dat(dir.path(), &edges).unwrap();
        assert_eq!(edge_data.len(), 2);

        write_edge_meta(dir.path(), &edge_data, &edges).unwrap();

        let meta = fs::read(dir.path().join("edge_meta.dat")).unwrap();
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
        let type_id = u32::from_le_bytes(meta[off + 36..off + 40].try_into().unwrap());
        assert_eq!(type_id, 5);
        let valid_to = i64::from_le_bytes(meta[off + 60..off + 68].try_into().unwrap());
        assert_eq!(valid_to, i64::MAX);
    }

    #[test]
    fn test_sidecars_empty() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = NodeIdMap::default();
        let edges = NodeIdMap::default();
        let node_data = write_nodes_dat(dir.path(), &nodes).unwrap();
        let edge_data = write_edges_dat(dir.path(), &edges).unwrap();
        write_sidecars(dir.path(), &node_data, &edge_data, &nodes, &edges).unwrap();

        let meta = fs::read(dir.path().join("node_meta.dat")).unwrap();
        assert_eq!(u64::from_le_bytes(meta[0..8].try_into().unwrap()), 0);

        let emeta = fs::read(dir.path().join("edge_meta.dat")).unwrap();
        assert_eq!(u64::from_le_bytes(emeta[0..8].try_into().unwrap()), 0);
        assert!(!dir.path().join("node_prop_hashes.dat").exists());
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
                type_id: 1,
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
                type_id: 1,
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
                type_id: 1,
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
                type_id: 1,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        mt.register_secondary_index(&entry);

        write_segment_with_secondary_indexes(&seg_dir, 1, &mt, None, std::slice::from_ref(&entry))
            .unwrap();

        assert!(!seg_dir.join("prop_index.dat").exists());
        assert!(!seg_dir.join("node_prop_hashes.dat").exists());
        assert!(node_prop_eq_sidecar_path(&seg_dir, entry.index_id).exists());

        let reader = SegmentReader::open(&seg_dir, 1, None).unwrap();
        let red_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let green_hash = hash_prop_value(&PropValue::String("green".to_string()));

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
}
