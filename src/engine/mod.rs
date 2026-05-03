use crate::degree_cache::{DegreeDelta, DegreeOverlayEdit, DegreeOverlaySnapshot};
use crate::dense_hnsw::exact_dense_search_above_cutoff;
use crate::error::EngineError;
use crate::manifest::{default_manifest, load_manifest, load_manifest_readonly, write_manifest};
use crate::memtable::{encode_range_prop_value, Memtable};
use crate::planner_stats::{
    planner_stats_declaration_fingerprint_for_entry,
    write_targeted_secondary_index_planner_stats_sidecar, DeclaredIndexRuntimeCoverage,
    EstimateConfidence, PlannerEstimateKind, PlannerStatsDirection, PlannerStatsView,
    PlannerStatsWriteOutcome, StalePostingRisk,
};
use crate::segment_reader::{SegmentReader, SegmentTypePosting};
use crate::segment_writer::{
    node_prop_eq_sidecar_path, node_prop_range_sidecar_path, segment_dir, segment_tmp_dir,
    write_indexes_from_metadata_with_secondary_indexes, write_merged_edges_dat,
    write_merged_nodes_dat, write_node_prop_eq_sidecar_to_path,
    write_node_prop_range_sidecar_to_path, write_segment_with_degree_overlay_and_secondary_indexes,
    write_v3_edges_dat, write_v3_nodes_dat, CompactEdgeMeta, CompactNodeMeta, FastMergeCopyInfo,
    SecondaryIndexMaintenanceReport,
};
use crate::source_list::SourceList;
use crate::sparse_postings::{accumulate_sparse_posting_scores, sparse_dot_score};
use crate::types::*;
use crate::wal::{remove_wal_generation, wal_generation_path, WalReader, WalWriter};
use crate::wal_sync::{shutdown_sync_thread, sync_thread_loop, WalSyncState};
use arc_swap::ArcSwap;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::SystemTime;

type SecondaryIndexCatalog =
    HashMap<u32, HashMap<String, HashMap<SecondaryIndexKind, SecondaryIndexManifestEntry>>>;
type SecondaryIndexEntries = Vec<SecondaryIndexManifestEntry>;

/// Generic K-way merge across already-sorted sources with early termination
/// for pagination. Segment sources must be pre-sorted ascending by key.
/// Memtable items (unsorted) are sorted once up front. Uses a min-heap for
/// O(log K) per item where K = number of sources (typically 2-6).
///
/// When a cursor is provided, each source is binary-searched to seek past it
/// in O(K log N) rather than walking through all pre-cursor items.
///
/// `key_fn` extracts the u64 sort key from each item (e.g., node ID or edge ID).
/// `skip_fn` returns true for items to exclude (deleted, filtered, etc.).
/// Deduplicates across sources by key. Stops as soon as `limit` items are
/// emitted. Note: `next_cursor` may be `Some` even when no further items
/// exist (remaining heap entries may all be duplicates or skipped). Callers
/// must handle a zero-item final page.
fn merge_sorted_paged<T: Clone>(
    mut memtable_items: Vec<T>,
    segment_sorted_items: Vec<Vec<T>>,
    key_fn: impl Fn(&T) -> u64,
    skip_fn: impl Fn(&T) -> bool,
    page: &PageRequest,
) -> PageResult<T> {
    // Sort memtable items by key (small set, typically 0-few hundred)
    memtable_items.sort_unstable_by_key(|item| key_fn(item));

    // Build source list: memtable first, then each segment
    let sources_count = 1 + segment_sorted_items.len();
    let mut sources: Vec<&[T]> = Vec::with_capacity(sources_count);
    sources.push(&memtable_items);
    for seg_items in &segment_sorted_items {
        sources.push(seg_items);
    }

    // Initialize min-heap: (key, source_index)
    // When a cursor is present, binary-search each source to start after it.
    // O(K log N) seek instead of O(cursor_position) heap pops.
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::with_capacity(sources_count);
    let mut positions: Vec<usize> = vec![0; sources_count];

    for (i, source) in sources.iter().enumerate() {
        if source.is_empty() {
            continue;
        }
        let start = if let Some(cursor) = page.after {
            // Binary search by key: find first position with key > cursor
            match source.binary_search_by_key(&cursor, &key_fn) {
                Ok(pos) => pos + 1, // cursor found, start after it
                Err(pos) => pos,    // cursor not found, insertion point
            }
        } else {
            0
        };
        if start < source.len() {
            heap.push(Reverse((key_fn(&source[start]), i)));
            positions[i] = start + 1;
        }
    }

    let limit = page.limit;
    let mut result: Vec<T> = Vec::with_capacity(limit.unwrap_or(64).min(1024));
    let mut last_seen_key: Option<u64> = None;

    while let Some(Reverse((key, src_idx))) = heap.pop() {
        // Item position is one behind the current position pointer
        let item_pos = positions[src_idx] - 1;

        // Advance this source
        let src = sources[src_idx];
        let next_pos = positions[src_idx];
        if next_pos < src.len() {
            heap.push(Reverse((key_fn(&src[next_pos]), src_idx)));
            positions[src_idx] = next_pos + 1;
        }

        // Skip duplicates (same key from multiple sources, always adjacent in sorted merge)
        if last_seen_key == Some(key) {
            continue;
        }
        last_seen_key = Some(key);

        // Skip filtered items (deleted, policy-excluded, etc.)
        if skip_fn(&src[item_pos]) {
            continue;
        }

        // Emit
        result.push(src[item_pos].clone());

        // Early termination when limit reached
        if let Some(lim) = limit {
            if lim > 0 && result.len() >= lim {
                let has_more = !heap.is_empty();
                return PageResult {
                    next_cursor: if has_more { Some(key) } else { None },
                    items: result,
                };
            }
        }
    }

    PageResult {
        items: result,
        next_cursor: None,
    }
}

/// K-way merge for u64 ID lists. Thin wrapper around `merge_sorted_paged`
/// with identity key and deleted-set skip function.
fn merge_type_ids_paged(
    memtable_ids: Vec<u64>,
    segment_sorted_ids: Vec<Vec<u64>>,
    deleted: &NodeIdSet,
    page: &PageRequest,
) -> PageResult<u64> {
    merge_sorted_paged(
        memtable_ids,
        segment_sorted_ids,
        |&id| id,
        |&id| deleted.contains(&id),
        page,
    )
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn reconcile_dense_vector_manifest(
    manifest: &mut ManifestState,
    options: &DbOptions,
) -> Result<bool, EngineError> {
    if let Some(config) = manifest.dense_vector.as_ref() {
        validate_dense_vector_config(config)?;
    }
    if let Some(config) = options.dense_vector.as_ref() {
        validate_dense_vector_config(config)?;
    }

    match (&manifest.dense_vector, &options.dense_vector) {
        (Some(existing), Some(requested)) if existing != requested => {
            Err(EngineError::InvalidOperation(format!(
                "dense vector configuration mismatch: manifest has {:?}, open requested {:?}",
                existing, requested
            )))
        }
        (None, Some(requested)) => {
            manifest.dense_vector = Some(requested.clone());
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn secondary_index_lookup_key(entry: &SecondaryIndexManifestEntry) -> SecondaryIndexLookupKey {
    match &entry.target {
        SecondaryIndexTarget::NodeProperty { type_id, prop_key } => SecondaryIndexLookupKey {
            type_id: *type_id,
            prop_key: prop_key.clone(),
            kind: entry.kind.clone(),
        },
    }
}

fn normalize_secondary_index_manifest(manifest: &mut ManifestState) -> Result<bool, EngineError> {
    let mut dirty = false;
    let mut seen_ids = HashSet::new();
    let mut seen_keys = HashSet::new();
    let mut seen_range_targets = HashSet::new();
    let mut max_index_id = 0u64;

    for entry in &mut manifest.secondary_indexes {
        if !seen_ids.insert(entry.index_id) {
            return Err(EngineError::ManifestError(format!(
                "duplicate secondary index id {} in manifest",
                entry.index_id
            )));
        }
        if !seen_keys.insert(secondary_index_lookup_key(entry)) {
            return Err(EngineError::ManifestError(format!(
                "duplicate secondary index declaration for {:?}",
                entry.target
            )));
        }
        if matches!(entry.kind, SecondaryIndexKind::Range { .. }) {
            let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
            if !seen_range_targets.insert((*type_id, prop_key.clone())) {
                return Err(EngineError::ManifestError(format!(
                    "duplicate range declaration for node property ({}, {})",
                    type_id, prop_key
                )));
            }
        }
        max_index_id = max_index_id.max(entry.index_id);
    }

    let next_secondary_index_id = if max_index_id == 0 {
        manifest.next_secondary_index_id.max(1)
    } else {
        manifest
            .next_secondary_index_id
            .max(max_index_id.saturating_add(1))
    };
    if next_secondary_index_id != manifest.next_secondary_index_id {
        manifest.next_secondary_index_id = next_secondary_index_id;
        dirty = true;
    }

    Ok(dirty)
}

fn build_secondary_index_catalog(
    entries: &[SecondaryIndexManifestEntry],
) -> Result<SecondaryIndexCatalog, EngineError> {
    let mut catalog: SecondaryIndexCatalog = HashMap::with_capacity(entries.len());
    let mut seen_range_targets = HashSet::new();
    for entry in entries {
        match &entry.target {
            SecondaryIndexTarget::NodeProperty { type_id, prop_key } => {
                if matches!(entry.kind, SecondaryIndexKind::Range { .. })
                    && !seen_range_targets.insert((*type_id, prop_key.clone()))
                {
                    return Err(EngineError::ManifestError(format!(
                        "duplicate range declaration loaded from manifest for node property ({}, {})",
                        type_id, prop_key
                    )));
                }
                let kind_map = catalog
                    .entry(*type_id)
                    .or_default()
                    .entry(prop_key.clone())
                    .or_default();
                if kind_map.insert(entry.kind.clone(), entry.clone()).is_some() {
                    return Err(EngineError::ManifestError(format!(
                        "duplicate secondary index declaration loaded from manifest: {:?}",
                        entry.target
                    )));
                }
            }
        }
    }
    Ok(catalog)
}

fn sync_secondary_index_runtime_state(
    catalog_lock: &RwLock<SecondaryIndexCatalog>,
    entries_lock: &RwLock<SecondaryIndexEntries>,
    entries: &[SecondaryIndexManifestEntry],
) -> Result<(), EngineError> {
    let catalog = build_secondary_index_catalog(entries)?;
    *catalog_lock.write().unwrap() = catalog;
    *entries_lock.write().unwrap() = entries.to_vec();
    Ok(())
}

fn merge_runtime_manifest_counters_from_shared(
    manifest: &mut ManifestState,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
) {
    manifest.next_node_id = manifest
        .next_node_id
        .max(next_node_id_seen.load(Ordering::Acquire));
    manifest.next_edge_id = manifest
        .next_edge_id
        .max(next_edge_id_seen.load(Ordering::Acquire));
    manifest.next_engine_seq = manifest
        .next_engine_seq
        .max(engine_seq_seen.load(Ordering::Acquire));
}

#[allow(clippy::too_many_arguments)]
fn update_secondary_index_manifest_runtime(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    catalog_lock: &Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: &Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
    mutate: impl FnOnce(&mut ManifestState) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let _guard = manifest_write_lock.lock().unwrap();
    let mut manifest = load_manifest_readonly(db_dir)?
        .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
    mutate(&mut manifest)?;
    merge_runtime_manifest_counters_from_shared(
        &mut manifest,
        next_node_id_seen,
        next_edge_id_seen,
        engine_seq_seen,
    );
    write_manifest(db_dir, &manifest)?;
    sync_secondary_index_runtime_state(catalog_lock, entries_lock, &manifest.secondary_indexes)?;
    Ok(())
}

fn is_not_found_io_error(error: &EngineError) -> bool {
    matches!(
        error,
        EngineError::IoError(io_error) if io_error.kind() == std::io::ErrorKind::NotFound
    )
}

fn apply_secondary_index_failure_report(
    manifest: &mut ManifestState,
    report: &SecondaryIndexMaintenanceReport,
) {
    for (index_id, message) in &report.failed_equality_indexes {
        if let Some(entry) = manifest
            .secondary_indexes
            .iter_mut()
            .find(|entry| entry.index_id == *index_id)
        {
            if matches!(entry.kind, SecondaryIndexKind::Equality) {
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some(message.clone());
            }
        }
    }
    for (index_id, message) in &report.failed_range_indexes {
        if let Some(entry) = manifest
            .secondary_indexes
            .iter_mut()
            .find(|entry| entry.index_id == *index_id)
        {
            if matches!(entry.kind, SecondaryIndexKind::Range { .. }) {
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some(message.clone());
            }
        }
    }
}

fn equality_index_ids_snapshot(entries: &[SecondaryIndexManifestEntry]) -> NodeIdSet {
    entries
        .iter()
        .filter(|entry| matches!(entry.kind, SecondaryIndexKind::Equality))
        .map(|entry| entry.index_id)
        .collect()
}

fn range_index_ids_snapshot(entries: &[SecondaryIndexManifestEntry]) -> NodeIdSet {
    entries
        .iter()
        .filter(|entry| matches!(entry.kind, SecondaryIndexKind::Range { .. }))
        .map(|entry| entry.index_id)
        .collect()
}

fn reconcile_background_output_equality_declarations(
    manifest: &mut ManifestState,
    maintained_equality_index_ids: &NodeIdSet,
) -> Vec<u64> {
    let mut rebuild_index_ids = Vec::new();
    for entry in &mut manifest.secondary_indexes {
        if !matches!(entry.kind, SecondaryIndexKind::Equality)
            || maintained_equality_index_ids.contains(&entry.index_id)
        {
            continue;
        }

        match entry.state {
            SecondaryIndexState::Failed => {}
            SecondaryIndexState::Building => {
                entry.last_error = None;
                rebuild_index_ids.push(entry.index_id);
            }
            SecondaryIndexState::Ready => {
                entry.state = SecondaryIndexState::Building;
                entry.last_error = None;
                rebuild_index_ids.push(entry.index_id);
            }
        }
    }
    rebuild_index_ids.sort_unstable();
    rebuild_index_ids.dedup();
    rebuild_index_ids
}

fn reconcile_background_output_range_declarations(
    manifest: &mut ManifestState,
    maintained_range_index_ids: &NodeIdSet,
) -> Vec<u64> {
    let mut rebuild_index_ids = Vec::new();
    for entry in &mut manifest.secondary_indexes {
        if !matches!(entry.kind, SecondaryIndexKind::Range { .. })
            || maintained_range_index_ids.contains(&entry.index_id)
        {
            continue;
        }

        match entry.state {
            SecondaryIndexState::Failed => {}
            SecondaryIndexState::Building => {
                entry.last_error = None;
                rebuild_index_ids.push(entry.index_id);
            }
            SecondaryIndexState::Ready => {
                entry.state = SecondaryIndexState::Building;
                entry.last_error = None;
                rebuild_index_ids.push(entry.index_id);
            }
        }
    }
    rebuild_index_ids.sort_unstable();
    rebuild_index_ids.dedup();
    rebuild_index_ids
}

#[allow(clippy::too_many_arguments)]
fn mark_secondary_index_failed(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    catalog_lock: &Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: &Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
    index_id: u64,
    error: &EngineError,
) {
    let message = error.to_string();
    let _ = update_secondary_index_manifest_runtime(
        db_dir,
        manifest_write_lock,
        catalog_lock,
        entries_lock,
        next_node_id_seen,
        next_edge_id_seen,
        engine_seq_seen,
        |manifest| {
            if let Some(entry) = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == index_id)
            {
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some(message.clone());
            }
            Ok(())
        },
    );
}

fn build_secondary_eq_groups_for_segment(
    segment: &SegmentReader,
    type_id: u32,
    prop_key: &str,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let target_key_hash = hash_prop_key(prop_key);
    let legacy_hashes = segment.raw_node_prop_hashes_mmap();
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for index in 0..segment.node_meta_count() as usize {
        let (
            node_id,
            data_offset,
            _data_len,
            node_type_id,
            _updated_at,
            _weight,
            _key_len,
            prop_hash_offset,
            prop_hash_count,
            _last_write_seq,
        ) = segment.node_meta_at(index)?;
        if node_type_id != type_id {
            continue;
        }

        let mut value_hash = None;
        if !legacy_hashes.is_empty() && prop_hash_count > 0 {
            let base = prop_hash_offset as usize;
            for pair_index in 0..prop_hash_count as usize {
                let pair_off = base + pair_index * 16;
                let pair_end = pair_off + 16;
                if pair_end > legacy_hashes.len() {
                    return Err(EngineError::CorruptRecord(format!(
                        "node {} prop hash pair at offset {} exceeds source length {}",
                        node_id,
                        pair_off,
                        legacy_hashes.len()
                    )));
                }
                let key_hash =
                    u64::from_le_bytes(legacy_hashes[pair_off..pair_off + 8].try_into().unwrap());
                if key_hash != target_key_hash {
                    continue;
                }
                value_hash = Some(u64::from_le_bytes(
                    legacy_hashes[pair_off + 8..pair_off + 16]
                        .try_into()
                        .unwrap(),
                ));
                break;
            }
        }

        if value_hash.is_none() {
            value_hash = segment
                .node_property_value_at_offset(node_id, data_offset, prop_key)?
                .map(|value| hash_prop_value(&value));
        }

        if let Some(value_hash) = value_hash {
            groups.entry(value_hash).or_default().push(node_id);
        }
    }

    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    Ok(groups)
}

fn build_secondary_range_entries_for_segment(
    segment: &SegmentReader,
    type_id: u32,
    prop_key: &str,
    domain: SecondaryIndexRangeDomain,
) -> Result<Vec<(u64, u64)>, EngineError> {
    let mut entries = Vec::new();

    for index in 0..segment.node_meta_count() as usize {
        let (
            node_id,
            data_offset,
            _data_len,
            node_type_id,
            _updated_at,
            _weight,
            _key_len,
            _prop_hash_offset,
            _prop_hash_count,
            _last_write_seq,
        ) = segment.node_meta_at(index)?;
        if node_type_id != type_id {
            continue;
        }

        let Some(value) = segment.node_property_value_at_offset(node_id, data_offset, prop_key)?
        else {
            continue;
        };
        let Some(encoded_value) = encode_range_prop_value(domain, &value) else {
            continue;
        };
        entries.push((encoded_value, node_id));
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn install_secondary_eq_sidecar(
    seg_dir: &Path,
    index_id: u64,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    let index_dir = seg_dir.join("secondary_indexes");
    match std::fs::create_dir(&index_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let final_path = node_prop_eq_sidecar_path(seg_dir, index_id);
    let tmp_path = index_dir.join(format!(".node_prop_eq_{}.tmp", index_id));
    write_node_prop_eq_sidecar_to_path(&tmp_path, groups)?;
    std::fs::rename(&tmp_path, &final_path)?;
    fsync_dir(&index_dir)?;
    Ok(())
}

fn install_secondary_range_sidecar(
    seg_dir: &Path,
    index_id: u64,
    entries: &[(u64, u64)],
) -> Result<(), EngineError> {
    let index_dir = seg_dir.join("secondary_indexes");
    match std::fs::create_dir(&index_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let final_path = node_prop_range_sidecar_path(seg_dir, index_id);
    let tmp_path = index_dir.join(format!(".node_prop_range_{}.tmp", index_id));
    write_node_prop_range_sidecar_to_path(&tmp_path, entries)?;
    std::fs::rename(&tmp_path, &final_path)?;
    fsync_dir(&index_dir)?;
    Ok(())
}

#[derive(Clone)]
struct SecondaryEqBuildSnapshot {
    dense_config: Option<DenseVectorConfig>,
    type_id: u32,
    prop_key: String,
    segment_ids: Vec<u64>,
}

enum SecondaryEqCoverageStatus {
    Covered,
    Incomplete,
    Failed(String),
    Cancelled,
}

enum SecondaryEqFinalizeOutcome {
    ReadyApplied(SecondaryIndexReadyApplied),
    Applied,
    Retry,
    Inactive,
}

#[derive(Clone)]
struct SecondaryRangeBuildSnapshot {
    dense_config: Option<DenseVectorConfig>,
    type_id: u32,
    prop_key: String,
    domain: SecondaryIndexRangeDomain,
    segment_ids: Vec<u64>,
}

enum SecondaryRangeCoverageStatus {
    Covered,
    Incomplete,
    Failed(String),
    Cancelled,
}

enum SecondaryRangeFinalizeOutcome {
    ReadyApplied(SecondaryIndexReadyApplied),
    Applied,
    Retry,
    Inactive,
}

#[derive(Clone, Debug)]
struct SecondaryIndexReadyApplied {
    index_id: u64,
    kind: SecondaryIndexKind,
    type_id: u32,
    prop_key: String,
    declaration_fingerprint: u64,
    snapshot_segment_ids: Vec<u64>,
}

impl SecondaryIndexReadyApplied {
    fn from_ready_entry(
        entry: &SecondaryIndexManifestEntry,
        snapshot_segment_ids: Vec<u64>,
    ) -> Option<Self> {
        if entry.state != SecondaryIndexState::Ready {
            return None;
        }
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        Some(Self {
            index_id: entry.index_id,
            kind: entry.kind.clone(),
            type_id: *type_id,
            prop_key: prop_key.clone(),
            declaration_fingerprint: planner_stats_declaration_fingerprint_for_entry(entry),
            snapshot_segment_ids,
        })
    }

    fn matches_entry(&self, entry: &SecondaryIndexManifestEntry) -> bool {
        if entry.state != SecondaryIndexState::Ready
            || entry.index_id != self.index_id
            || entry.kind != self.kind
        {
            return false;
        }
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        *type_id == self.type_id
            && prop_key == &self.prop_key
            && planner_stats_declaration_fingerprint_for_entry(entry)
                == self.declaration_fingerprint
    }
}

fn load_secondary_eq_build_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    index_id: u64,
) -> Result<Option<SecondaryEqBuildSnapshot>, EngineError> {
    let _guard = manifest_write_lock.lock().unwrap();
    let manifest = load_manifest_readonly(db_dir)?
        .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
    let Some(entry) = manifest
        .secondary_indexes
        .iter()
        .find(|entry| entry.index_id == index_id)
        .cloned()
    else {
        return Ok(None);
    };
    if entry.state != SecondaryIndexState::Building
        || !matches!(entry.kind, SecondaryIndexKind::Equality)
    {
        return Ok(None);
    }

    let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = entry.target;
    let mut segment_ids: Vec<u64> = manifest.segments.iter().map(|segment| segment.id).collect();
    segment_ids.sort_unstable();
    Ok(Some(SecondaryEqBuildSnapshot {
        dense_config: manifest.dense_vector.clone(),
        type_id,
        prop_key,
        segment_ids,
    }))
}

fn build_secondary_eq_sidecars_for_snapshot(
    db_dir: &Path,
    index_id: u64,
    snapshot: &SecondaryEqBuildSnapshot,
    cancel: &AtomicBool,
) -> Result<(), EngineError> {
    for &segment_id in &snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }

        let seg_path = segment_dir(db_dir, segment_id);
        if !seg_path.exists() {
            continue;
        }

        let segment =
            match SegmentReader::open(&seg_path, segment_id, snapshot.dense_config.as_ref()) {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => continue,
                Err(error) => return Err(error),
            };

        match segment.validate_secondary_eq_sidecar(index_id) {
            Ok(true) => continue,
            Ok(false) => {
                let groups = build_secondary_eq_groups_for_segment(
                    &segment,
                    snapshot.type_id,
                    &snapshot.prop_key,
                )?;
                match install_secondary_eq_sidecar(&seg_path, index_id, &groups) {
                    Ok(()) => {}
                    Err(error) if is_not_found_io_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) if is_not_found_io_error(&error) => {}
            Err(_) => {
                let groups = build_secondary_eq_groups_for_segment(
                    &segment,
                    snapshot.type_id,
                    &snapshot.prop_key,
                )?;
                match install_secondary_eq_sidecar(&seg_path, index_id, &groups) {
                    Ok(()) => {}
                    Err(error) if is_not_found_io_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }

    Ok(())
}

fn validate_secondary_eq_snapshot_coverage(
    db_dir: &Path,
    index_id: u64,
    snapshot: &SecondaryEqBuildSnapshot,
    cancel: &AtomicBool,
) -> Result<SecondaryEqCoverageStatus, EngineError> {
    let mut all_present = true;

    for &segment_id in &snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            return Ok(SecondaryEqCoverageStatus::Cancelled);
        }

        let seg_path = segment_dir(db_dir, segment_id);
        if !seg_path.exists() {
            all_present = false;
            continue;
        }

        let segment =
            match SegmentReader::open(&seg_path, segment_id, snapshot.dense_config.as_ref()) {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => {
                    all_present = false;
                    continue;
                }
                Err(error) => return Err(error),
            };

        match segment.validate_secondary_eq_sidecar(index_id) {
            Ok(true) => {}
            Ok(false) => {
                all_present = false;
            }
            Err(error) => {
                return Ok(SecondaryEqCoverageStatus::Failed(error.to_string()));
            }
        }
    }

    Ok(if all_present {
        SecondaryEqCoverageStatus::Covered
    } else {
        SecondaryEqCoverageStatus::Incomplete
    })
}

#[allow(clippy::too_many_arguments)]
fn finalize_secondary_eq_build_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    catalog_lock: &Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: &Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
    index_id: u64,
    snapshot: &SecondaryEqBuildSnapshot,
    coverage: &SecondaryEqCoverageStatus,
) -> Result<SecondaryEqFinalizeOutcome, EngineError> {
    let mut outcome = SecondaryEqFinalizeOutcome::Applied;
    update_secondary_index_manifest_runtime(
        db_dir,
        manifest_write_lock,
        catalog_lock,
        entries_lock,
        next_node_id_seen,
        next_edge_id_seen,
        engine_seq_seen,
        |manifest| {
            let Some(entry_pos) = manifest
                .secondary_indexes
                .iter()
                .position(|entry| entry.index_id == index_id)
            else {
                outcome = SecondaryEqFinalizeOutcome::Inactive;
                return Ok(());
            };

            let mut current_segment_ids: Vec<u64> =
                manifest.segments.iter().map(|segment| segment.id).collect();
            current_segment_ids.sort_unstable();
            if current_segment_ids != snapshot.segment_ids {
                outcome = SecondaryEqFinalizeOutcome::Retry;
                return Ok(());
            }

            let entry = &mut manifest.secondary_indexes[entry_pos];
            if entry.state != SecondaryIndexState::Building
                || !matches!(entry.kind, SecondaryIndexKind::Equality)
            {
                outcome = SecondaryEqFinalizeOutcome::Inactive;
                return Ok(());
            }

            match coverage {
                SecondaryEqCoverageStatus::Covered => {
                    entry.state = SecondaryIndexState::Ready;
                    entry.last_error = None;
                    let mut snapshot_segment_ids = snapshot.segment_ids.clone();
                    snapshot_segment_ids.sort_unstable();
                    if let Some(ready) =
                        SecondaryIndexReadyApplied::from_ready_entry(entry, snapshot_segment_ids)
                    {
                        outcome = SecondaryEqFinalizeOutcome::ReadyApplied(ready);
                    }
                }
                SecondaryEqCoverageStatus::Incomplete => {
                    entry.state = SecondaryIndexState::Building;
                    entry.last_error = None;
                }
                SecondaryEqCoverageStatus::Failed(message) => {
                    entry.state = SecondaryIndexState::Failed;
                    entry.last_error = Some(message.clone());
                }
                SecondaryEqCoverageStatus::Cancelled => {
                    outcome = SecondaryEqFinalizeOutcome::Inactive;
                }
            }
            Ok(())
        },
    )?;
    Ok(outcome)
}

fn load_secondary_range_build_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    index_id: u64,
) -> Result<Option<SecondaryRangeBuildSnapshot>, EngineError> {
    let _guard = manifest_write_lock.lock().unwrap();
    let manifest = load_manifest_readonly(db_dir)?
        .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
    let Some(entry) = manifest
        .secondary_indexes
        .iter()
        .find(|entry| entry.index_id == index_id)
        .cloned()
    else {
        return Ok(None);
    };
    if entry.state != SecondaryIndexState::Building {
        return Ok(None);
    }

    let SecondaryIndexKind::Range { domain } = entry.kind else {
        return Ok(None);
    };
    let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = entry.target;
    let mut segment_ids: Vec<u64> = manifest.segments.iter().map(|segment| segment.id).collect();
    segment_ids.sort_unstable();
    Ok(Some(SecondaryRangeBuildSnapshot {
        dense_config: manifest.dense_vector.clone(),
        type_id,
        prop_key,
        domain,
        segment_ids,
    }))
}

fn build_secondary_range_sidecars_for_snapshot(
    db_dir: &Path,
    index_id: u64,
    snapshot: &SecondaryRangeBuildSnapshot,
    cancel: &AtomicBool,
) -> Result<(), EngineError> {
    for &segment_id in &snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }

        let seg_path = segment_dir(db_dir, segment_id);
        if !seg_path.exists() {
            continue;
        }

        let segment =
            match SegmentReader::open(&seg_path, segment_id, snapshot.dense_config.as_ref()) {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => continue,
                Err(error) => return Err(error),
            };

        match segment.validate_secondary_range_sidecar(index_id) {
            Ok(true) => continue,
            Ok(false) => {
                let entries = build_secondary_range_entries_for_segment(
                    &segment,
                    snapshot.type_id,
                    &snapshot.prop_key,
                    snapshot.domain,
                )?;
                match install_secondary_range_sidecar(&seg_path, index_id, &entries) {
                    Ok(()) => {}
                    Err(error) if is_not_found_io_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) if is_not_found_io_error(&error) => {}
            Err(_) => {
                let entries = build_secondary_range_entries_for_segment(
                    &segment,
                    snapshot.type_id,
                    &snapshot.prop_key,
                    snapshot.domain,
                )?;
                match install_secondary_range_sidecar(&seg_path, index_id, &entries) {
                    Ok(()) => {}
                    Err(error) if is_not_found_io_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }

    Ok(())
}

fn validate_secondary_range_snapshot_coverage(
    db_dir: &Path,
    index_id: u64,
    snapshot: &SecondaryRangeBuildSnapshot,
    cancel: &AtomicBool,
) -> Result<SecondaryRangeCoverageStatus, EngineError> {
    let mut all_present = true;

    for &segment_id in &snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            return Ok(SecondaryRangeCoverageStatus::Cancelled);
        }

        let seg_path = segment_dir(db_dir, segment_id);
        if !seg_path.exists() {
            all_present = false;
            continue;
        }

        let segment =
            match SegmentReader::open(&seg_path, segment_id, snapshot.dense_config.as_ref()) {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => {
                    all_present = false;
                    continue;
                }
                Err(error) => return Err(error),
            };

        match segment.validate_secondary_range_sidecar(index_id) {
            Ok(true) => {}
            Ok(false) => {
                all_present = false;
            }
            Err(error) => {
                return Ok(SecondaryRangeCoverageStatus::Failed(error.to_string()));
            }
        }
    }

    Ok(if all_present {
        SecondaryRangeCoverageStatus::Covered
    } else {
        SecondaryRangeCoverageStatus::Incomplete
    })
}

#[allow(clippy::too_many_arguments)]
fn finalize_secondary_range_build_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    catalog_lock: &Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: &Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
    index_id: u64,
    snapshot: &SecondaryRangeBuildSnapshot,
    coverage: &SecondaryRangeCoverageStatus,
) -> Result<SecondaryRangeFinalizeOutcome, EngineError> {
    let mut outcome = SecondaryRangeFinalizeOutcome::Applied;
    update_secondary_index_manifest_runtime(
        db_dir,
        manifest_write_lock,
        catalog_lock,
        entries_lock,
        next_node_id_seen,
        next_edge_id_seen,
        engine_seq_seen,
        |manifest| {
            let Some(entry_pos) = manifest
                .secondary_indexes
                .iter()
                .position(|entry| entry.index_id == index_id)
            else {
                outcome = SecondaryRangeFinalizeOutcome::Inactive;
                return Ok(());
            };

            let mut current_segment_ids: Vec<u64> =
                manifest.segments.iter().map(|segment| segment.id).collect();
            current_segment_ids.sort_unstable();
            if current_segment_ids != snapshot.segment_ids {
                outcome = SecondaryRangeFinalizeOutcome::Retry;
                return Ok(());
            }

            let entry = &mut manifest.secondary_indexes[entry_pos];
            if entry.state != SecondaryIndexState::Building
                || !matches!(entry.kind, SecondaryIndexKind::Range { .. })
            {
                outcome = SecondaryRangeFinalizeOutcome::Inactive;
                return Ok(());
            }

            match coverage {
                SecondaryRangeCoverageStatus::Covered => {
                    entry.state = SecondaryIndexState::Ready;
                    entry.last_error = None;
                    let mut snapshot_segment_ids = snapshot.segment_ids.clone();
                    snapshot_segment_ids.sort_unstable();
                    if let Some(ready) =
                        SecondaryIndexReadyApplied::from_ready_entry(entry, snapshot_segment_ids)
                    {
                        outcome = SecondaryRangeFinalizeOutcome::ReadyApplied(ready);
                    }
                }
                SecondaryRangeCoverageStatus::Incomplete => {
                    entry.state = SecondaryIndexState::Building;
                    entry.last_error = None;
                }
                SecondaryRangeCoverageStatus::Failed(message) => {
                    entry.state = SecondaryIndexState::Failed;
                    entry.last_error = Some(message.clone());
                }
                SecondaryRangeCoverageStatus::Cancelled => {
                    outcome = SecondaryRangeFinalizeOutcome::Inactive;
                }
            }
            Ok(())
        },
    )?;
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn process_secondary_index_build(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    catalog_lock: &Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: &Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
    #[cfg(test)] build_pause: &Arc<Mutex<Option<SecondaryIndexBuildPauseHook>>>,
    index_id: u64,
    cancel: &AtomicBool,
) -> Result<Option<SecondaryIndexReadyApplied>, EngineError> {
    #[cfg(test)]
    let mut build_pause_applied = false;

    loop {
        if cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }

        #[cfg(test)]
        if !build_pause_applied {
            if let Some(hook) = build_pause.lock().unwrap().take() {
                let _ = hook.ready_tx.send(());
                let _ = hook.release_rx.recv();
            }
            build_pause_applied = true;
        }

        if let Some(snapshot) =
            load_secondary_eq_build_snapshot(db_dir, manifest_write_lock, index_id)?
        {
            build_secondary_eq_sidecars_for_snapshot(db_dir, index_id, &snapshot, cancel)?;
            let coverage =
                validate_secondary_eq_snapshot_coverage(db_dir, index_id, &snapshot, cancel)?;
            if matches!(coverage, SecondaryEqCoverageStatus::Cancelled) {
                return Ok(None);
            }

            match finalize_secondary_eq_build_snapshot(
                db_dir,
                manifest_write_lock,
                catalog_lock,
                entries_lock,
                next_node_id_seen,
                next_edge_id_seen,
                engine_seq_seen,
                index_id,
                &snapshot,
                &coverage,
            )? {
                SecondaryEqFinalizeOutcome::ReadyApplied(ready) => return Ok(Some(ready)),
                SecondaryEqFinalizeOutcome::Applied | SecondaryEqFinalizeOutcome::Inactive => {
                    return Ok(None)
                }
                SecondaryEqFinalizeOutcome::Retry => continue,
            }
        } else if let Some(snapshot) =
            load_secondary_range_build_snapshot(db_dir, manifest_write_lock, index_id)?
        {
            build_secondary_range_sidecars_for_snapshot(db_dir, index_id, &snapshot, cancel)?;
            let coverage =
                validate_secondary_range_snapshot_coverage(db_dir, index_id, &snapshot, cancel)?;
            if matches!(coverage, SecondaryRangeCoverageStatus::Cancelled) {
                return Ok(None);
            }

            match finalize_secondary_range_build_snapshot(
                db_dir,
                manifest_write_lock,
                catalog_lock,
                entries_lock,
                next_node_id_seen,
                next_edge_id_seen,
                engine_seq_seen,
                index_id,
                &snapshot,
                &coverage,
            )? {
                SecondaryRangeFinalizeOutcome::ReadyApplied(ready) => return Ok(Some(ready)),
                SecondaryRangeFinalizeOutcome::Applied
                | SecondaryRangeFinalizeOutcome::Inactive => return Ok(None),
                SecondaryRangeFinalizeOutcome::Retry => continue,
            }
        } else {
            return Ok(None);
        }
    }
}

#[derive(Clone)]
struct TargetedStatsRefreshSnapshot {
    dense_config: Option<DenseVectorConfig>,
    target_entry: SecondaryIndexManifestEntry,
    ready_indexes: Vec<SecondaryIndexManifestEntry>,
    segment_ids: Vec<u64>,
}

fn load_targeted_stats_refresh_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    ready: &SecondaryIndexReadyApplied,
) -> Result<Option<TargetedStatsRefreshSnapshot>, EngineError> {
    let _guard = manifest_write_lock.lock().unwrap();
    let manifest = load_manifest_readonly(db_dir)?
        .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
    let Some(target_entry) = manifest
        .secondary_indexes
        .iter()
        .find(|entry| entry.index_id == ready.index_id)
        .cloned()
    else {
        return Ok(None);
    };
    if !ready.matches_entry(&target_entry) {
        return Ok(None);
    }

    let snapshot_segment_ids: HashSet<u64> = ready.snapshot_segment_ids.iter().copied().collect();
    let mut segment_ids: Vec<u64> = manifest
        .segments
        .iter()
        .map(|segment| segment.id)
        .filter(|segment_id| snapshot_segment_ids.contains(segment_id))
        .collect();
    segment_ids.sort_unstable();
    if segment_ids.is_empty() {
        return Ok(None);
    }

    let mut ready_indexes: Vec<_> = manifest
        .secondary_indexes
        .iter()
        .filter(|entry| entry.state == SecondaryIndexState::Ready)
        .cloned()
        .collect();
    ready_indexes.sort_by_key(|entry| entry.index_id);
    Ok(Some(TargetedStatsRefreshSnapshot {
        dense_config: manifest.dense_vector,
        target_entry,
        ready_indexes,
        segment_ids,
    }))
}

fn targeted_refresh_snapshot_contains_segment(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    ready: &SecondaryIndexReadyApplied,
    segment_id: u64,
) -> Result<Option<TargetedStatsRefreshSnapshot>, EngineError> {
    let Some(snapshot) = load_targeted_stats_refresh_snapshot(db_dir, manifest_write_lock, ready)?
    else {
        return Ok(None);
    };
    if snapshot.segment_ids.binary_search(&segment_id).is_ok() {
        Ok(Some(snapshot))
    } else {
        Ok(None)
    }
}

fn target_secondary_sidecar_is_valid(
    segment: &SegmentReader,
    ready: &SecondaryIndexReadyApplied,
) -> Result<bool, EngineError> {
    match ready.kind {
        SecondaryIndexKind::Equality => segment.validate_secondary_eq_sidecar(ready.index_id),
        SecondaryIndexKind::Range { .. } => {
            segment.validate_secondary_range_sidecar(ready.index_id)
        }
    }
}

fn refresh_ready_secondary_index_planner_stats(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    ready: &SecondaryIndexReadyApplied,
    cancel: &AtomicBool,
) -> Vec<(u64, Arc<SegmentReader>)> {
    let Some(initial_snapshot) =
        load_targeted_stats_refresh_snapshot(db_dir, manifest_write_lock, ready)
            .ok()
            .flatten()
    else {
        return Vec::new();
    };
    let mut refreshed = Vec::new();

    for segment_id in initial_snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        let seg_dir = segment_dir(db_dir, segment_id);
        let segment =
            match SegmentReader::open(&seg_dir, segment_id, initial_snapshot.dense_config.as_ref())
            {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => continue,
                Err(_) => continue,
            };
        match target_secondary_sidecar_is_valid(&segment, ready) {
            Ok(true) => {}
            Ok(false) | Err(_) => continue,
        }

        let Some(snapshot) = targeted_refresh_snapshot_contains_segment(
            db_dir,
            manifest_write_lock,
            ready,
            segment_id,
        )
        .ok()
        .flatten() else {
            continue;
        };
        match write_targeted_secondary_index_planner_stats_sidecar(
            &seg_dir,
            &segment,
            &snapshot.target_entry,
            &snapshot.ready_indexes,
        ) {
            Ok(PlannerStatsWriteOutcome::Written) => {}
            Ok(PlannerStatsWriteOutcome::SkippedOversize)
            | Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable)
            | Err(_) => continue,
        }

        let refreshed_reader =
            match SegmentReader::open(&seg_dir, segment_id, snapshot.dense_config.as_ref()) {
                Ok(reader) => reader,
                Err(error) if is_not_found_io_error(&error) => continue,
                Err(_) => continue,
            };
        for entry in &snapshot.ready_indexes {
            refreshed_reader.warm_declared_index_runtime_coverage(entry);
        }
        if target_secondary_sidecar_is_valid(&refreshed_reader, ready).unwrap_or(false) {
            refreshed.push((segment_id, Arc::new(refreshed_reader)));
        }
    }

    refreshed
}

fn process_secondary_index_drop_cleanup(
    db_dir: &Path,
    index_id: u64,
    cancel: &AtomicBool,
) -> Result<(), EngineError> {
    let manifest = load_manifest_readonly(db_dir)?
        .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
    for segment_info in &manifest.segments {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }

        let seg_dir = segment_dir(db_dir, segment_info.id);
        for sidecar_path in [
            node_prop_eq_sidecar_path(&seg_dir, index_id),
            node_prop_range_sidecar_path(&seg_dir, index_id),
        ] {
            if sidecar_path.exists() {
                let _ = std::fs::remove_file(&sidecar_path);
                if let Some(parent) = sidecar_path.parent() {
                    let _ = fsync_dir(parent);
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bg_secondary_index_worker(
    rx: std::sync::mpsc::Receiver<SecondaryIndexJob>,
    cancel: Arc<AtomicBool>,
    runtime: Option<std::sync::Weak<DbRuntime>>,
    db_dir: PathBuf,
    manifest_write_lock: Arc<Mutex<()>>,
    catalog_lock: Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: Arc<AtomicU64>,
    next_edge_id_seen: Arc<AtomicU64>,
    engine_seq_seen: Arc<AtomicU64>,
    #[cfg(test)] build_pause: Arc<Mutex<Option<SecondaryIndexBuildPauseHook>>>,
) {
    while let Ok(job) = rx.recv() {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        match job {
            SecondaryIndexJob::Build { index_id } => {
                let ready_applied = match process_secondary_index_build(
                    &db_dir,
                    &manifest_write_lock,
                    &catalog_lock,
                    &entries_lock,
                    next_node_id_seen.as_ref(),
                    next_edge_id_seen.as_ref(),
                    engine_seq_seen.as_ref(),
                    #[cfg(test)]
                    &build_pause,
                    index_id,
                    &cancel,
                ) {
                    Ok(ready) => ready,
                    Err(error) => {
                        mark_secondary_index_failed(
                            &db_dir,
                            &manifest_write_lock,
                            &catalog_lock,
                            &entries_lock,
                            next_node_id_seen.as_ref(),
                            next_edge_id_seen.as_ref(),
                            engine_seq_seen.as_ref(),
                            index_id,
                            &error,
                        );
                        None
                    }
                };
                let refreshed_readers = ready_applied.as_ref().map_or_else(Vec::new, |ready| {
                    refresh_ready_secondary_index_planner_stats(
                        &db_dir,
                        &manifest_write_lock,
                        ready,
                        &cancel,
                    )
                });
                if let Some(runtime) = runtime.as_ref().and_then(std::sync::Weak::upgrade) {
                    if let Some(ready) = ready_applied {
                        runtime.republish_secondary_index_state_and_refreshed_stats_if_open(
                            &ready,
                            refreshed_readers,
                        );
                    } else {
                        runtime.republish_secondary_index_state_if_open();
                    }
                }
            }
            SecondaryIndexJob::DropCleanup { index_id } => {
                let _ = process_secondary_index_drop_cleanup(&db_dir, index_id, &cancel);
            }
            SecondaryIndexJob::Shutdown => break,
        }
    }
}

fn normalize_node_vectors_for_write(
    dense_config: Option<&DenseVectorConfig>,
    dense_vector: Option<&DenseVector>,
    sparse_vector: Option<&SparseVector>,
) -> Result<(Option<DenseVector>, Option<SparseVector>), EngineError> {
    let dense_vector = match dense_vector {
        Some(values) => {
            let config = dense_config.ok_or_else(|| {
                EngineError::InvalidOperation(
                    "dense vector writes require DbOptions::dense_vector to be configured".into(),
                )
            })?;
            validate_dense_vector(values, config)?;
            Some(values.clone())
        }
        None => None,
    };

    let sparse_vector = match sparse_vector {
        Some(values) => canonicalize_sparse_vector(values)?,
        None => None,
    };

    Ok((dense_vector, sparse_vector))
}

fn normalize_wal_op_for_write(
    dense_config: Option<&DenseVectorConfig>,
    op: &WalOp,
) -> Result<WalOp, EngineError> {
    match op {
        WalOp::UpsertNode(node) => {
            let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
                dense_config,
                node.dense_vector.as_ref(),
                node.sparse_vector.as_ref(),
            )?;
            let mut normalized = node.clone();
            normalized.dense_vector = dense_vector;
            normalized.sparse_vector = sparse_vector;
            Ok(WalOp::UpsertNode(normalized))
        }
        _ => Ok(op.clone()),
    }
}

fn normalize_wal_op_for_replay(
    dense_config: Option<&DenseVectorConfig>,
    op: WalOp,
) -> Result<WalOp, EngineError> {
    normalize_wal_op_for_write(dense_config, &op).map_err(|err| match (&op, err) {
        (WalOp::UpsertNode(node), EngineError::InvalidOperation(message)) => {
            EngineError::CorruptWal(format!(
                "invalid vector payload for replayed node {} (key={}): {}",
                node.id, node.key, message
            ))
        }
        (_, err) => err,
    })
}

/// Returns true if an edge is valid (not expired, not future) at the given reference time.
/// Same predicate used by `neighbors()`. Extracted to prevent drift.
#[inline]
fn is_edge_valid_at(valid_from: i64, valid_to: i64, reference_time: i64) -> bool {
    valid_from <= reference_time && valid_to > reference_time
}

/// Precomputed cutoffs for all registered prune policies. Created once per
/// batch read call with a `now_millis()` snapshot to avoid redundant time
/// syscalls. Policies combine with OR across policies, AND within each policy.
/// Core prune-policy match: does a single (precomputed) policy match the given fields?
/// AND within policy: all set criteria must match.
fn matches_prune_cutoff(
    type_id: u32,
    updated_at: i64,
    weight: f32,
    policy_age_cutoff: Option<i64>,
    policy_max_weight: Option<f32>,
    policy_type_id: Option<u32>,
) -> bool {
    if let Some(tid) = policy_type_id {
        if type_id != tid {
            return false;
        }
    }
    if let Some(cutoff) = policy_age_cutoff {
        if updated_at >= cutoff {
            return false;
        }
    }
    if let Some(max_w) = policy_max_weight {
        if weight > max_w {
            return false;
        }
    }
    true
}

struct PrecomputedPruneCutoffs {
    /// (age_cutoff, max_weight, type_id) per policy.
    policies: Vec<(Option<i64>, Option<f32>, Option<u32>)>,
}

impl PrecomputedPruneCutoffs {
    fn from_policies(policies: &BTreeMap<String, PrunePolicy>, now: i64) -> Self {
        let policies = policies
            .values()
            .map(|p| {
                let age_cutoff = p.max_age_ms.map(|age| now - age);
                (age_cutoff, p.max_weight, p.type_id)
            })
            .collect();
        Self { policies }
    }

    /// Returns true if the node matches ANY registered policy (should be excluded).
    fn excludes(&self, node: &NodeRecord) -> bool {
        self.excludes_fields(node.type_id, node.updated_at, node.weight)
    }

    fn excludes_fields(&self, node_type_id: u32, updated_at: i64, weight: f32) -> bool {
        for &(age_cutoff, max_weight, policy_type_id) in &self.policies {
            if matches_prune_cutoff(
                node_type_id,
                updated_at,
                weight,
                age_cutoff,
                max_weight,
                policy_type_id,
            ) {
                return true;
            }
        }
        false
    }
}

/// Runtime degree cache entry. Stores aggregate degree and weight data
/// for O(1) lookups on the unfiltered, non-temporal, no-policy path.
/// Self-loop fields are required for correct `Direction::Both` semantics:
/// a self-loop increments both out_degree and in_degree, but Both must
/// count it once (out + in - self_loop_count).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DegreeEntry {
    pub out_degree: u32,
    pub in_degree: u32,
    pub out_weight_sum: f64,
    pub in_weight_sum: f64,
    pub self_loop_count: u32,
    pub self_loop_weight_sum: f64,
    /// Count of incident edges that require cache bypass because their degree
    /// contribution is not timeless for cache purposes: finite `valid_to` or
    /// explicit delayed start (`valid_from > created_at`).
    ///
    /// This is intentionally conservative for future-dated edges. Once such an
    /// edge exists, the node remains on the walk path until rebuild/open/
    /// compact, preserving correctness without timer-driven invalidation.
    pub temporal_edge_count: u32,
}

impl DegreeEntry {
    #[cfg(test)]
    pub const ZERO: DegreeEntry = DegreeEntry {
        out_degree: 0,
        in_degree: 0,
        out_weight_sum: 0.0,
        in_weight_sum: 0.0,
        self_loop_count: 0,
        self_loop_weight_sum: 0.0,
        temporal_edge_count: 0,
    };
}

/// Cache-bypass classification for degree entries.
/// This is edge-intrinsic and therefore reversible exactly on update/delete
/// without depending on wall-clock time at mutation time.
#[inline]
fn is_cache_bypass_edge(valid_from: i64, valid_to: i64, created_at: i64) -> bool {
    valid_to != i64::MAX || valid_from > created_at
}

#[derive(Clone)]
struct ReadManifestState {
    prune_policies: BTreeMap<String, PrunePolicy>,
    dense_vector: Option<DenseVectorConfig>,
}

pub(crate) struct PublishedReadSources {
    manifest: ReadManifestState,
    memtable: Arc<Memtable>,
    immutable_epochs: Vec<ReadViewImmutableEpoch>,
    segments: Vec<Arc<SegmentReader>>,
    secondary_index_catalog: SecondaryIndexCatalog,
    secondary_index_entries: SecondaryIndexEntries,
    pub(crate) declared_index_runtime_coverage: Arc<DeclaredIndexRuntimeCoverage>,
    planner_stats: Arc<PlannerStatsView>,
    #[cfg(test)]
    planning_probe_counters: QueryPlanningProbeCounters,
    #[cfg(test)]
    query_execution_counters: QueryExecutionCounters,
}

#[cfg(test)]
#[derive(Default)]
struct QueryPlanningProbeCounters {
    range: AtomicUsize,
    timestamp: AtomicUsize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryPlanningProbeSnapshot {
    pub range: usize,
    pub timestamp: usize,
}

#[cfg(test)]
#[derive(Default)]
struct QueryExecutionCounters {
    equality_materialization_record_reads: AtomicUsize,
    final_verifier_record_reads: AtomicUsize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryExecutionCounterSnapshot {
    pub equality_materialization_record_reads: usize,
    pub final_verifier_record_reads: usize,
}

/// Published read-visible snapshot for CP1 point/dedup reads.
#[derive(Clone)]
pub(crate) struct ReadView {
    sources: Arc<PublishedReadSources>,
    snapshot_seq: u64,
    active_degree_overlay: Arc<DegreeOverlaySnapshot>,
}

pub(crate) type ReadViewImmutableEpoch = ImmutableEpoch;

struct PublishedReadState {
    view: Arc<ReadView>,
    edge_uniqueness: bool,
    #[cfg(test)]
    engine_seq: u64,
    #[cfg(test)]
    active_wal_generation_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PublishImpact {
    NoPublish,
    SnapshotOnly,
    RebuildSources,
}

impl PublishImpact {
    fn combine(self, other: Self) -> Self {
        self.max(other)
    }
}

impl std::ops::Deref for ReadView {
    type Target = PublishedReadSources;

    fn deref(&self) -> &Self::Target {
        self.sources.as_ref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum RuntimeOpenState {
    Open = 0,
    Closing = 1,
    Closed = 2,
}

struct RuntimeLifecycleState {
    active_non_read_ops: usize,
    active_mutating_ops: usize,
    closing: bool,
    closed: bool,
    close_in_progress: bool,
    mutating_barrier_active: bool,
    lifecycle_work_ready: bool,
    coordinator_shutdown_requested: bool,
    coordinator_stopped: bool,
    pending_core_writes: VecDeque<QueuedCoreWrite>,
    coordinator_queue_capacity: usize,
    coordinator_active_command: bool,
    pending_secondary_index_followups: HashSet<SecondaryIndexReadFollowupKey>,
    progress_seq: u64,
    completed_flushes_by_epoch: HashMap<u64, SegmentInfo>,
    completed_flush_order: VecDeque<u64>,
    completed_bg_compactions: VecDeque<CompactionStats>,
}

impl Default for RuntimeLifecycleState {
    fn default() -> Self {
        Self {
            active_non_read_ops: 0,
            active_mutating_ops: 0,
            closing: false,
            closed: false,
            close_in_progress: false,
            mutating_barrier_active: false,
            lifecycle_work_ready: false,
            coordinator_shutdown_requested: false,
            coordinator_stopped: false,
            pending_core_writes: VecDeque::new(),
            coordinator_queue_capacity: 1024,
            coordinator_active_command: false,
            pending_secondary_index_followups: HashSet::new(),
            progress_seq: 0,
            completed_flushes_by_epoch: HashMap::new(),
            completed_flush_order: VecDeque::new(),
            completed_bg_compactions: VecDeque::new(),
        }
    }
}

struct RuntimeReadGuard<'a> {
    runtime: &'a DbRuntime,
}

impl Drop for RuntimeReadGuard<'_> {
    fn drop(&mut self) {
        self.runtime.finish_read_operation();
    }
}

#[cfg(test)]
struct RuntimeOperationGuard<'a> {
    runtime: &'a DbRuntime,
}

#[cfg(test)]
impl Drop for RuntimeOperationGuard<'_> {
    fn drop(&mut self) {
        let mut lifecycle = self.runtime.lifecycle.lock().unwrap();
        lifecycle.active_non_read_ops = lifecycle.active_non_read_ops.saturating_sub(1);
        lifecycle.active_mutating_ops = lifecycle.active_mutating_ops.saturating_sub(1);
        let should_notify =
            lifecycle.active_non_read_ops == 0 || lifecycle.active_mutating_ops == 0;
        if should_notify {
            self.runtime.lifecycle_cv.notify_all();
        }
    }
}

struct RuntimeMutatingBarrierGuard<'a> {
    runtime: &'a DbRuntime,
}

impl Drop for RuntimeMutatingBarrierGuard<'_> {
    fn drop(&mut self) {
        let mut lifecycle = self.runtime.lifecycle.lock().unwrap();
        lifecycle.mutating_barrier_active = false;
        lifecycle.active_non_read_ops = lifecycle.active_non_read_ops.saturating_sub(1);
        self.runtime.lifecycle_cv.notify_all();
    }
}

struct DbRuntime {
    db_dir: PathBuf,
    core: Mutex<Option<EngineCore>>,
    published: ArcSwap<PublishedReadState>,
    open_state: AtomicU8,
    active_read_ops: AtomicUsize,
    read_drain: Mutex<()>,
    read_drain_cv: Condvar,
    lifecycle: Mutex<RuntimeLifecycleState>,
    lifecycle_cv: Condvar,
    coordinator_thread: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    property_query_routes: PropertyQueryRouteCounters,
    #[cfg(test)]
    degree_query_routes: DegreeQueryRouteCounters,
    #[cfg(test)]
    publish_counters: PublishCounters,
    #[cfg(test)]
    write_publish_pause: Mutex<Option<RuntimePublishPauseHook>>,
    #[cfg(test)]
    read_admission_pause: Mutex<Option<RuntimeReadPauseHook>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PropertyQueryRouteKind {
    EqualityScanFallback,
    EqualityIndexLookup,
    RangeScanFallback,
    RangeIndexLookup,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct DegreeQueryRouteTally {
    fast_path: usize,
    walk_path: usize,
}

impl DegreeQueryRouteTally {
    fn fast_path() -> Self {
        Self {
            fast_path: 1,
            walk_path: 0,
        }
    }

    fn walk_path() -> Self {
        Self {
            fast_path: 0,
            walk_path: 1,
        }
    }

    fn add_fast_path(&mut self) {
        self.fast_path += 1;
    }

    fn add_walk_path(&mut self) {
        self.walk_path += 1;
    }

    fn add_walk_paths(&mut self, count: usize) {
        self.walk_path += count;
    }
}

#[derive(Debug)]
enum SecondaryIndexReadFollowup {
    EqualitySidecarFailure {
        index_id: u64,
        error: Option<EngineError>,
    },
    RangeSidecarFailure {
        index_id: u64,
        error: Option<EngineError>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SecondaryIndexReadFollowupKey {
    EqualityBuilding { index_id: u64 },
    EqualityFailed { index_id: u64, message: String },
    RangeBuilding { index_id: u64 },
    RangeFailed { index_id: u64, message: String },
}

impl SecondaryIndexReadFollowup {
    fn dedup_key(&self) -> SecondaryIndexReadFollowupKey {
        match self {
            SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id, error } => match error {
                Some(error) if !is_not_found_io_error(error) => {
                    SecondaryIndexReadFollowupKey::EqualityFailed {
                        index_id: *index_id,
                        message: error.to_string(),
                    }
                }
                _ => SecondaryIndexReadFollowupKey::EqualityBuilding {
                    index_id: *index_id,
                },
            },
            SecondaryIndexReadFollowup::RangeSidecarFailure { index_id, error } => match error {
                Some(error) if !is_not_found_io_error(error) => {
                    SecondaryIndexReadFollowupKey::RangeFailed {
                        index_id: *index_id,
                        message: error.to_string(),
                    }
                }
                _ => SecondaryIndexReadFollowupKey::RangeBuilding {
                    index_id: *index_id,
                },
            },
        }
    }
}

struct PropertyQueryOutcome<T> {
    value: T,
    route: PropertyQueryRouteKind,
    followup: Option<SecondaryIndexReadFollowup>,
}

pub(crate) struct DegreeQueryOutcome<T> {
    value: T,
    routes: DegreeQueryRouteTally,
}

enum CoreWriteRequest {
    UpsertNode {
        type_id: u32,
        key: String,
        options: UpsertNodeOptions,
    },
    UpsertEdge {
        from: u64,
        to: u64,
        type_id: u32,
        options: UpsertEdgeOptions,
    },
    BatchUpsertNodes {
        inputs: Vec<NodeInput>,
    },
    BatchUpsertEdges {
        inputs: Vec<EdgeInput>,
    },
    DeleteNode {
        id: u64,
    },
    DeleteEdge {
        id: u64,
    },
    InvalidateEdge {
        id: u64,
        valid_to: i64,
    },
    #[cfg(test)]
    WriteOp {
        op: WalOp,
    },
    #[cfg(test)]
    WriteOpBatch {
        ops: Vec<WalOp>,
    },
    GraphPatch {
        patch: GraphPatch,
    },
    TxnCommit {
        request: TxnCommitRequest,
    },
    Prune {
        policy: PrunePolicy,
    },
    SetPrunePolicy {
        name: String,
        policy: PrunePolicy,
    },
    RemovePrunePolicy {
        name: String,
    },
    EnsureNodePropertyIndex {
        type_id: u32,
        prop_key: String,
        kind: SecondaryIndexKind,
    },
    DropNodePropertyIndex {
        type_id: u32,
        prop_key: String,
        kind: SecondaryIndexKind,
    },
    ApplySecondaryIndexReadFollowup {
        followup: SecondaryIndexReadFollowup,
    },
    Sync,
    Flush,
    IngestMode,
    EndIngest,
    Compact,
}

enum CoreWriteReply {
    U64(u64),
    VecU64(Vec<u64>),
    Unit,
    OptionEdge(Option<EdgeRecord>),
    PatchResult(PatchResult),
    TxnCommitResult(TxnCommitResult),
    PruneResult(PruneResult),
    Bool(bool),
    NodePropertyIndexInfo(NodePropertyIndexInfo),
    OptionSegmentInfo(Option<SegmentInfo>),
    OptionCompactionStats(Option<CompactionStats>),
}

struct CoreWritePlan {
    ops: Vec<WalOp>,
    reply: CoreWriteReply,
    auto_flush: bool,
    track_ids: bool,
}

struct QueuedCoreWrite {
    request: CoreWriteRequest,
    reply_tx: Option<std::sync::mpsc::SyncSender<Result<CoreWriteReply, EngineError>>>,
    followup_key: Option<SecondaryIndexReadFollowupKey>,
}

enum QueuedWriteProgress {
    Complete {
        command: QueuedCoreWrite,
        result: Result<CoreWriteReply, EngineError>,
    },
    WaitForLifecycle {
        command: QueuedCoreWrite,
    },
}

impl DbRuntime {
    fn new(db_dir: PathBuf, core: EngineCore) -> Self {
        let published = Arc::new(core.published_read_state());
        Self {
            db_dir,
            core: Mutex::new(Some(core)),
            published: ArcSwap::new(published),
            open_state: AtomicU8::new(RuntimeOpenState::Open as u8),
            active_read_ops: AtomicUsize::new(0),
            read_drain: Mutex::new(()),
            read_drain_cv: Condvar::new(),
            lifecycle: Mutex::new(RuntimeLifecycleState::default()),
            lifecycle_cv: Condvar::new(),
            coordinator_thread: Mutex::new(None),
            #[cfg(test)]
            property_query_routes: PropertyQueryRouteCounters::default(),
            #[cfg(test)]
            degree_query_routes: DegreeQueryRouteCounters::default(),
            #[cfg(test)]
            publish_counters: PublishCounters::default(),
            #[cfg(test)]
            write_publish_pause: Mutex::new(None),
            #[cfg(test)]
            read_admission_pause: Mutex::new(None),
        }
    }

    fn install_core_runtime_handle(self: &Arc<Self>) {
        let mut core_guard = self.core.lock().unwrap();
        let Some(core) = core_guard.as_mut() else {
            return;
        };
        core.runtime = Some(Arc::downgrade(self));
        core.ensure_secondary_index_worker_if_needed();
        core.schedule_building_secondary_indexes();
        drop(core_guard);
        self.start_coordinator();
    }

    fn path(&self) -> &Path {
        &self.db_dir
    }

    fn open_state(&self) -> RuntimeOpenState {
        match self.open_state.load(Ordering::Acquire) {
            x if x == RuntimeOpenState::Open as u8 => RuntimeOpenState::Open,
            x if x == RuntimeOpenState::Closing as u8 => RuntimeOpenState::Closing,
            _ => RuntimeOpenState::Closed,
        }
    }

    fn set_open_state(&self, state: RuntimeOpenState) {
        self.open_state.store(state as u8, Ordering::Release);
    }

    fn finish_read_operation(&self) {
        let previous = self.active_read_ops.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "read guard underflow");
        if previous == 1 && self.open_state() == RuntimeOpenState::Closing {
            self.read_drain_cv.notify_all();
        }
    }

    fn wait_for_reads_to_drain(&self) {
        if self.active_read_ops.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut guard = self.read_drain.lock().unwrap();
        while self.active_read_ops.load(Ordering::Acquire) > 0 {
            guard = self.read_drain_cv.wait(guard).unwrap();
        }
    }

    fn admit_operation(&self) -> Result<RuntimeReadGuard<'_>, EngineError> {
        if self.open_state() != RuntimeOpenState::Open {
            return Err(EngineError::DatabaseClosed);
        }
        self.active_read_ops.fetch_add(1, Ordering::AcqRel);
        if self.open_state() != RuntimeOpenState::Open {
            self.finish_read_operation();
            return Err(EngineError::DatabaseClosed);
        }
        #[cfg(test)]
        if let Some(hook) = self.read_admission_pause.lock().unwrap().take() {
            let _ = hook.ready_tx.send(());
            let _ = hook.release_rx.recv();
        }
        Ok(RuntimeReadGuard { runtime: self })
    }

    #[cfg(test)]
    fn admit_mutating_operation(&self) -> Result<RuntimeOperationGuard<'_>, EngineError> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        loop {
            if lifecycle.closing || lifecycle.closed {
                return Err(EngineError::DatabaseClosed);
            }
            if !lifecycle.mutating_barrier_active {
                lifecycle.active_non_read_ops += 1;
                lifecycle.active_mutating_ops += 1;
                return Ok(RuntimeOperationGuard { runtime: self });
            }
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
    }

    fn begin_mutating_barrier(&self) -> Result<RuntimeMutatingBarrierGuard<'_>, EngineError> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        loop {
            if lifecycle.closing || lifecycle.closed {
                return Err(EngineError::DatabaseClosed);
            }
            if !lifecycle.mutating_barrier_active {
                lifecycle.mutating_barrier_active = true;
                lifecycle.active_non_read_ops += 1;
                #[cfg(test)]
                self.lifecycle_cv.notify_all();
                while lifecycle.active_mutating_ops > 0 {
                    lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
                    if lifecycle.closing || lifecycle.closed {
                        lifecycle.mutating_barrier_active = false;
                        lifecycle.active_non_read_ops =
                            lifecycle.active_non_read_ops.saturating_sub(1);
                        self.lifecycle_cv.notify_all();
                        return Err(EngineError::DatabaseClosed);
                    }
                }
                return Ok(RuntimeMutatingBarrierGuard { runtime: self });
            }
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
    }

    fn published_snapshot(
        &self,
    ) -> Result<(RuntimeReadGuard<'_>, Arc<PublishedReadState>), EngineError> {
        let guard = self.admit_operation()?;
        let snapshot = self.published.load_full();
        Ok((guard, snapshot))
    }

    fn submit_core_write(&self, request: CoreWriteRequest) -> Result<CoreWriteReply, EngineError> {
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
        let mut request = Some(request);
        let mut lifecycle = self.lifecycle.lock().unwrap();
        loop {
            if lifecycle.closing || lifecycle.closed {
                return Err(EngineError::DatabaseClosed);
            }
            if lifecycle.mutating_barrier_active {
                lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
                continue;
            }
            let occupancy = lifecycle.pending_core_writes.len()
                + usize::from(lifecycle.coordinator_active_command);
            if occupancy < lifecycle.coordinator_queue_capacity {
                lifecycle.active_non_read_ops += 1;
                lifecycle.active_mutating_ops += 1;
                lifecycle.pending_core_writes.push_back(QueuedCoreWrite {
                    request: request
                        .take()
                        .expect("queued core write request should only enqueue once"),
                    reply_tx: Some(reply_tx),
                    followup_key: None,
                });
                self.lifecycle_cv.notify_all();
                break;
            }
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
        drop(lifecycle);

        reply_rx
            .recv()
            .map_err(|_| EngineError::InvalidOperation("coordinator thread died".into()))?
    }

    fn enqueue_secondary_index_read_followup(&self, followup: SecondaryIndexReadFollowup) {
        let followup_key = followup.dedup_key();
        let mut lifecycle = self.lifecycle.lock().unwrap();
        loop {
            if lifecycle.closing || lifecycle.closed {
                return;
            }
            if lifecycle
                .pending_secondary_index_followups
                .contains(&followup_key)
            {
                return;
            }
            if lifecycle.mutating_barrier_active {
                lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
                continue;
            }
            let occupancy = lifecycle.pending_core_writes.len()
                + usize::from(lifecycle.coordinator_active_command);
            if occupancy < lifecycle.coordinator_queue_capacity {
                lifecycle.active_non_read_ops += 1;
                lifecycle.active_mutating_ops += 1;
                lifecycle
                    .pending_secondary_index_followups
                    .insert(followup_key.clone());
                lifecycle.pending_core_writes.push_back(QueuedCoreWrite {
                    request: CoreWriteRequest::ApplySecondaryIndexReadFollowup { followup },
                    reply_tx: None,
                    followup_key: Some(followup_key),
                });
                self.lifecycle_cv.notify_all();
                return;
            }
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
    }

    fn lifecycle_progress_seq(&self) -> u64 {
        self.lifecycle.lock().unwrap().progress_seq
    }

    #[cfg(test)]
    fn wait_for_lifecycle_progress(&self, observed_seq: u64) -> Result<(), EngineError> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        while lifecycle.progress_seq == observed_seq
            && !lifecycle.closed
            && !lifecycle.coordinator_stopped
        {
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
        if lifecycle.closed {
            return Err(EngineError::DatabaseClosed);
        }
        Ok(())
    }

    fn wait_for_lifecycle_event(&self, observed_seq: u64) -> Result<(), EngineError> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        while lifecycle.progress_seq == observed_seq
            && !lifecycle.lifecycle_work_ready
            && !lifecycle.closed
            && !lifecycle.coordinator_stopped
            && !lifecycle.coordinator_shutdown_requested
        {
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
        if lifecycle.closed {
            return Err(EngineError::DatabaseClosed);
        }
        Ok(())
    }

    fn notify_lifecycle_work(&self) {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        lifecycle.lifecycle_work_ready = true;
        self.lifecycle_cv.notify_all();
    }

    fn take_completed_flush_for_epoch(&self, epoch_id: u64) -> Option<SegmentInfo> {
        self.lifecycle
            .lock()
            .unwrap()
            .completed_flushes_by_epoch
            .remove(&epoch_id)
    }

    #[cfg(test)]
    fn take_next_completed_flush(&self) -> Option<SegmentInfo> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        while let Some(epoch_id) = lifecycle.completed_flush_order.pop_front() {
            if let Some(info) = lifecycle.completed_flushes_by_epoch.remove(&epoch_id) {
                return Some(info);
            }
        }
        None
    }

    #[cfg(test)]
    fn take_next_completed_bg_compaction(&self) -> Option<CompactionStats> {
        self.lifecycle
            .lock()
            .unwrap()
            .completed_bg_compactions
            .pop_front()
    }

    #[cfg(test)]
    fn publish_counter_snapshot(&self) -> PublishCounterSnapshot {
        PublishCounterSnapshot {
            skipped: self.publish_counters.skipped.load(Ordering::Relaxed),
            snapshot_only: self.publish_counters.snapshot_only.load(Ordering::Relaxed),
            rebuild_sources: self
                .publish_counters
                .rebuild_sources
                .load(Ordering::Relaxed),
            source_rebuilds: self
                .publish_counters
                .source_rebuilds
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn reset_publish_counters(&self) {
        self.publish_counters.skipped.store(0, Ordering::Relaxed);
        self.publish_counters
            .snapshot_only
            .store(0, Ordering::Relaxed);
        self.publish_counters
            .rebuild_sources
            .store(0, Ordering::Relaxed);
        self.publish_counters
            .source_rebuilds
            .store(0, Ordering::Relaxed);
    }

    fn publish_locked(
        &self,
        core: &mut EngineCore,
        impact: PublishImpact,
        _apply_test_pause: bool,
    ) {
        #[cfg(test)]
        match impact {
            PublishImpact::NoPublish => {
                self.publish_counters
                    .skipped
                    .fetch_add(1, Ordering::Relaxed);
            }
            PublishImpact::SnapshotOnly => {
                self.publish_counters
                    .snapshot_only
                    .fetch_add(1, Ordering::Relaxed);
            }
            PublishImpact::RebuildSources => {
                self.publish_counters
                    .rebuild_sources
                    .fetch_add(1, Ordering::Relaxed);
                self.publish_counters
                    .source_rebuilds
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        if impact == PublishImpact::NoPublish {
            core.retry_deferred_segment_cleanup();
            return;
        }

        #[cfg(test)]
        if _apply_test_pause {
            if let Some(hook) = self.write_publish_pause.lock().unwrap().take() {
                let _ = hook.ready_tx.send(());
                let _ = hook.release_rx.recv();
            }
        }

        if impact == PublishImpact::RebuildSources {
            core.rebuild_published_read_sources();
        }

        let published = Arc::new(core.published_read_state());
        // Publish before releasing the core mutex so later writers cannot overtake
        // this committed snapshot and install an older view afterward.
        self.published.store(published);
        core.retry_deferred_segment_cleanup();
    }

    fn with_core_ref<T>(
        &self,
        f: impl FnOnce(&EngineCore) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let _guard = self.admit_operation()?;
        let core_guard = self.core.lock().unwrap();
        let core = core_guard.as_ref().ok_or(EngineError::DatabaseClosed)?;
        f(core)
    }

    #[cfg(test)]
    fn with_core_mut<T>(
        &self,
        f: impl FnOnce(&mut EngineCore) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let _guard = self.admit_mutating_operation()?;
        let mut core_guard = self.core.lock().unwrap();
        let core = core_guard.as_mut().ok_or(EngineError::DatabaseClosed)?;

        let result = f(core);

        self.publish_locked(core, PublishImpact::RebuildSources, true);
        drop(core_guard);
        result
    }

    fn republish_secondary_index_state_if_open(&self) {
        let mut core_guard = self.core.lock().unwrap();
        let Some(core) = core_guard.as_mut() else {
            return;
        };
        core.manifest.secondary_indexes = core.secondary_index_entries_snapshot();
        self.publish_locked(core, PublishImpact::RebuildSources, true);
    }

    fn republish_secondary_index_state_and_refreshed_stats_if_open(
        &self,
        ready: &SecondaryIndexReadyApplied,
        refreshed_readers: Vec<(u64, Arc<SegmentReader>)>,
    ) {
        let mut core_guard = self.core.lock().unwrap();
        let Some(core) = core_guard.as_mut() else {
            return;
        };
        core.manifest.secondary_indexes = core.secondary_index_entries_snapshot();

        let ready_still_current = core
            .manifest
            .secondary_indexes
            .iter()
            .any(|entry| ready.matches_entry(entry));
        if ready_still_current {
            for (segment_id, reader) in refreshed_readers {
                if !core
                    .manifest
                    .segments
                    .iter()
                    .any(|segment| segment.id == segment_id)
                {
                    continue;
                }
                if !target_secondary_sidecar_is_valid(&reader, ready).unwrap_or(false) {
                    continue;
                }
                let Some(position) = core
                    .segments
                    .iter()
                    .position(|segment| segment.segment_id == segment_id)
                else {
                    continue;
                };
                if core.segments[position].node_count() == reader.node_count()
                    && core.segments[position].edge_count() == reader.edge_count()
                {
                    core.segments[position] = reader;
                }
            }
        }

        self.publish_locked(core, PublishImpact::RebuildSources, true);
    }

    fn start_coordinator(self: &Arc<Self>) {
        let mut coordinator_guard = self.coordinator_thread.lock().unwrap();
        if coordinator_guard.is_some() {
            return;
        }
        {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            lifecycle.coordinator_shutdown_requested = false;
            lifecycle.coordinator_stopped = false;
        }
        let runtime = Arc::clone(self);
        *coordinator_guard = Some(std::thread::spawn(move || runtime.coordinator_loop()));
    }

    fn request_coordinator_shutdown(&self) {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        lifecycle.coordinator_shutdown_requested = true;
        lifecycle.lifecycle_work_ready = true;
        self.lifecycle_cv.notify_all();
    }

    fn join_coordinator(&self) {
        let handle = self.coordinator_thread.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
        let mut lifecycle = self.lifecycle.lock().unwrap();
        lifecycle.coordinator_stopped = true;
        self.lifecycle_cv.notify_all();
    }

    fn coordinator_loop(self: Arc<Self>) {
        let mut current_command: Option<QueuedCoreWrite> = None;
        let mut waiting_for_lifecycle = false;
        loop {
            let lifecycle_work_ready;
            let shutdown_requested;
            {
                let mut lifecycle = self.lifecycle.lock().unwrap();
                while !lifecycle.coordinator_shutdown_requested
                    && !lifecycle.lifecycle_work_ready
                    && ((current_command.is_none() && lifecycle.pending_core_writes.is_empty())
                        || waiting_for_lifecycle)
                {
                    lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
                }
                if current_command.is_none() {
                    if let Some(command) = lifecycle.pending_core_writes.pop_front() {
                        lifecycle.coordinator_active_command = true;
                        current_command = Some(command);
                    }
                }
                shutdown_requested = lifecycle.coordinator_shutdown_requested;
                lifecycle_work_ready = lifecycle.lifecycle_work_ready;
                lifecycle.lifecycle_work_ready = false;
            }

            let progressed =
                if lifecycle_work_ready || current_command.is_some() || shutdown_requested {
                    self.run_lifecycle_batch_if_open()
                } else {
                    false
                };

            if waiting_for_lifecycle {
                if progressed {
                    waiting_for_lifecycle = false;
                } else if !shutdown_requested {
                    continue;
                }
            }

            if shutdown_requested && current_command.is_none() {
                if !progressed {
                    let mut lifecycle = self.lifecycle.lock().unwrap();
                    lifecycle.coordinator_stopped = true;
                    self.lifecycle_cv.notify_all();
                    return;
                }
                continue;
            }

            let Some(command) = current_command.take() else {
                continue;
            };

            match self.execute_core_write_command(command) {
                QueuedWriteProgress::Complete { command, result } => {
                    let _ = self.run_lifecycle_batch_if_open();
                    let mut lifecycle = self.lifecycle.lock().unwrap();
                    if let Some(followup_key) = &command.followup_key {
                        lifecycle
                            .pending_secondary_index_followups
                            .remove(followup_key);
                    }
                    lifecycle.coordinator_active_command = false;
                    lifecycle.active_non_read_ops = lifecycle.active_non_read_ops.saturating_sub(1);
                    lifecycle.active_mutating_ops = lifecycle.active_mutating_ops.saturating_sub(1);
                    if let Some(reply_tx) = command.reply_tx {
                        let _ = reply_tx.send(result);
                    }
                    self.lifecycle_cv.notify_all();
                }
                QueuedWriteProgress::WaitForLifecycle { command } => {
                    current_command = Some(command);
                    waiting_for_lifecycle = true;
                }
            }
        }
    }

    fn execute_core_write_command(&self, command: QueuedCoreWrite) -> QueuedWriteProgress {
        match &command.request {
            CoreWriteRequest::Sync => {
                let result = {
                    let mut core_guard = self.core.lock().unwrap();
                    let Some(core) = core_guard.as_mut() else {
                        return QueuedWriteProgress::Complete {
                            command,
                            result: Err(EngineError::DatabaseClosed),
                        };
                    };
                    let result = core.sync().map(|_| CoreWriteReply::Unit);
                    drop(core_guard);
                    result
                };
                return QueuedWriteProgress::Complete { command, result };
            }
            CoreWriteRequest::Flush => {
                let result = self
                    .execute_flush_barrier()
                    .map(CoreWriteReply::OptionSegmentInfo);
                return QueuedWriteProgress::Complete { command, result };
            }
            CoreWriteRequest::EndIngest => {
                let result = self
                    .restore_ingest_threshold()
                    .and_then(|_| self.execute_compaction_barrier(|_| true))
                    .map(CoreWriteReply::OptionCompactionStats);
                return QueuedWriteProgress::Complete { command, result };
            }
            CoreWriteRequest::Compact => {
                let result = self
                    .execute_compaction_barrier(|_| true)
                    .map(CoreWriteReply::OptionCompactionStats);
                return QueuedWriteProgress::Complete { command, result };
            }
            _ => {}
        }

        let mut core_guard = self.core.lock().unwrap();
        let Some(core) = core_guard.as_mut() else {
            return QueuedWriteProgress::Complete {
                command,
                result: Err(EngineError::DatabaseClosed),
            };
        };

        let uses_write_backpressure = matches!(
            &command.request,
            CoreWriteRequest::UpsertNode { .. }
                | CoreWriteRequest::UpsertEdge { .. }
                | CoreWriteRequest::BatchUpsertNodes { .. }
                | CoreWriteRequest::BatchUpsertEdges { .. }
                | CoreWriteRequest::DeleteNode { .. }
                | CoreWriteRequest::DeleteEdge { .. }
                | CoreWriteRequest::InvalidateEdge { .. }
                | CoreWriteRequest::GraphPatch { .. }
                | CoreWriteRequest::TxnCommit { .. }
                | CoreWriteRequest::Prune { .. }
        );

        let mut publish_impact = PublishImpact::NoPublish;
        if uses_write_backpressure {
            let (backpressure_result, backpressure_impact) = core.prepare_backpressure_flush();
            publish_impact = publish_impact.combine(backpressure_impact);
            match backpressure_result {
                Ok(BackpressureFlushAction::Ready) => {}
                Ok(BackpressureFlushAction::Wait) => {
                    self.publish_locked(core, publish_impact, true);
                    drop(core_guard);
                    return QueuedWriteProgress::WaitForLifecycle { command };
                }
                Err(err) => {
                    self.publish_locked(core, publish_impact, true);
                    drop(core_guard);
                    return QueuedWriteProgress::Complete {
                        command,
                        result: Err(err),
                    };
                }
            }
        }

        let (result, request_publish_impact) = match &command.request {
            CoreWriteRequest::SetPrunePolicy { name, policy } => {
                match core.set_prune_policy(name, policy.clone()) {
                    Ok(impact) => (Ok(CoreWriteReply::Unit), impact),
                    Err(err) => (Err(err), PublishImpact::NoPublish),
                }
            }
            CoreWriteRequest::RemovePrunePolicy { name } => match core.remove_prune_policy(name) {
                Ok((removed, impact)) => (Ok(CoreWriteReply::Bool(removed)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::EnsureNodePropertyIndex {
                type_id,
                prop_key,
                kind,
            } => match core.ensure_node_property_index(*type_id, prop_key, kind.clone()) {
                Ok((info, impact)) => (Ok(CoreWriteReply::NodePropertyIndexInfo(info)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::DropNodePropertyIndex {
                type_id,
                prop_key,
                kind,
            } => match core.drop_node_property_index(*type_id, prop_key, kind.clone()) {
                Ok((removed, impact)) => (Ok(CoreWriteReply::Bool(removed)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::ApplySecondaryIndexReadFollowup { followup } => {
                let impact = match followup {
                    SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id, error } => core
                        .degrade_ready_equality_index_after_sidecar_failure(
                            *index_id,
                            error.as_ref(),
                        ),
                    SecondaryIndexReadFollowup::RangeSidecarFailure { index_id, error } => core
                        .degrade_ready_range_index_after_sidecar_failure(*index_id, error.as_ref()),
                };
                (Ok(CoreWriteReply::Unit), impact)
            }
            CoreWriteRequest::IngestMode => (Ok(CoreWriteReply::Unit), core.ingest_mode()),
            _ => core
                .plan_core_write(&command.request)
                .map(|plan| core.commit_core_write_plan(plan))
                .unwrap_or_else(|err| (Err(err), PublishImpact::NoPublish)),
        };
        publish_impact = publish_impact.combine(request_publish_impact);
        self.publish_locked(core, publish_impact, true);
        drop(core_guard);
        QueuedWriteProgress::Complete { command, result }
    }

    fn execute_flush_barrier(&self) -> Result<Option<SegmentInfo>, EngineError> {
        let mut target_epoch: Option<u64> = None;
        loop {
            if let Some(epoch_id) = target_epoch {
                if let Some(info) = self.take_completed_flush_for_epoch(epoch_id) {
                    return Ok(Some(info));
                }
            }

            let wait_seq = self.lifecycle_progress_seq();
            let mut core_guard = self.core.lock().unwrap();
            let core = core_guard.as_mut().ok_or(EngineError::DatabaseClosed)?;

            core.maybe_surface_or_retry_flush_pipeline_error()?;

            if target_epoch.is_none() {
                if core.memtable.is_empty() && core.immutable_epochs.is_empty() {
                    let result = core.current_flush_pipeline_error().map_or(Ok(None), Err);
                    drop(core_guard);
                    return result;
                }

                let mut mutated = false;
                if !core.memtable.is_empty() {
                    core.freeze_memtable()?;
                    mutated = true;
                }

                if core.immutable_epochs.is_empty() {
                    if mutated {
                        self.publish_locked(core, PublishImpact::RebuildSources, true);
                    }
                    let result = core.current_flush_pipeline_error().map_or(Ok(None), Err);
                    drop(core_guard);
                    return result;
                }

                core.ensure_bg_flush_worker();
                if let Err(error) = core.enqueue_all_non_in_flight() {
                    if mutated {
                        self.publish_locked(core, PublishImpact::RebuildSources, true);
                    }
                    drop(core_guard);
                    return Err(error);
                }
                target_epoch = core.immutable_epochs.first().map(|epoch| epoch.epoch_id);
                if mutated {
                    self.publish_locked(core, PublishImpact::RebuildSources, true);
                }
            } else if !core
                .immutable_epochs
                .iter()
                .any(|epoch| Some(epoch.epoch_id) == target_epoch)
            {
                let result = if let Some(target_epoch) = target_epoch {
                    self.take_completed_flush_for_epoch(target_epoch)
                        .map_or(Ok(None), |seg| Ok(Some(seg)))
                } else {
                    Ok(None)
                };
                drop(core_guard);
                return result;
            }

            drop(core_guard);
            if self.run_lifecycle_batch_if_open() {
                continue;
            }
            self.wait_for_lifecycle_event(wait_seq)?;
        }
    }

    fn restore_ingest_threshold(&self) -> Result<(), EngineError> {
        let mut core_guard = self.core.lock().unwrap();
        let core = core_guard.as_mut().ok_or(EngineError::DatabaseClosed)?;
        if let Some(previous) = core.ingest_saved_compact_after_n_flushes.take() {
            core.compact_after_n_flushes = previous;
        }
        self.publish_locked(core, PublishImpact::NoPublish, true);
        drop(core_guard);
        Ok(())
    }

    fn execute_compaction_barrier<F>(
        &self,
        mut progress: F,
    ) -> Result<Option<CompactionStats>, EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        loop {
            let wait_seq = self.lifecycle_progress_seq();
            let mut core_guard = self.core.lock().unwrap();
            let core = core_guard.as_mut().ok_or(EngineError::DatabaseClosed)?;

            if !core.memtable.is_empty() || !core.immutable_epochs.is_empty() {
                drop(core_guard);
                self.execute_flush_barrier()?;
                continue;
            }

            if core.bg_compact.is_some() {
                drop(core_guard);
                if self.run_lifecycle_batch_if_open() {
                    continue;
                }
                self.wait_for_lifecycle_event(wait_seq)?;
                continue;
            }

            let result = core.compact_with_progress(&mut progress);
            let publish_impact = if matches!(result, Ok(Some(_))) {
                PublishImpact::RebuildSources
            } else {
                PublishImpact::NoPublish
            };
            self.publish_locked(core, publish_impact, true);
            drop(core_guard);
            return result;
        }
    }

    fn run_lifecycle_batch_if_open(&self) -> bool {
        let mut core_guard = self.core.lock().unwrap();
        let Some(core) = core_guard.as_mut() else {
            return false;
        };

        let flush_result = core.drain_ready_bg_flush_events_for_runtime();
        let compaction_finished = core
            .bg_compact
            .as_ref()
            .is_some_and(|bg| bg.completed.load(Ordering::Acquire));
        let compaction_stats = if compaction_finished {
            core.wait_for_bg_compact()
        } else {
            None
        };
        let progressed = flush_result.progressed || compaction_finished;
        let publish_impact = flush_result
            .publish_impact
            .combine(if compaction_stats.is_some() {
                PublishImpact::RebuildSources
            } else {
                PublishImpact::NoPublish
            });
        self.publish_locked(core, publish_impact, false);
        drop(core_guard);

        if progressed {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            for (epoch_id, seg_info) in flush_result.completed_flushes {
                lifecycle.completed_flush_order.push_back(epoch_id);
                lifecycle
                    .completed_flushes_by_epoch
                    .insert(epoch_id, seg_info);
            }
            if let Some(stats) = compaction_stats {
                lifecycle.completed_bg_compactions.push_back(stats);
            }
            lifecycle.progress_seq = lifecycle.progress_seq.wrapping_add(1);
            self.lifecycle_cv.notify_all();
        }
        progressed
    }

    fn close(&self, fast: bool) -> Result<(), EngineError> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if lifecycle.closed || lifecycle.closing || lifecycle.close_in_progress {
            return Err(EngineError::DatabaseClosed);
        }
        lifecycle.closing = true;
        lifecycle.close_in_progress = true;
        self.set_open_state(RuntimeOpenState::Closing);
        drop(lifecycle);

        self.wait_for_reads_to_drain();

        let mut lifecycle = self.lifecycle.lock().unwrap();
        while lifecycle.active_non_read_ops > 0 {
            lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
        }
        drop(lifecycle);

        self.request_coordinator_shutdown();
        self.join_coordinator();

        let core = {
            let mut core_guard = self.core.lock().unwrap();
            core_guard.take().ok_or(EngineError::DatabaseClosed)?
        };
        let result = if fast {
            core.close_fast()
        } else {
            core.close()
        };

        self.set_open_state(RuntimeOpenState::Closed);
        let mut lifecycle = self.lifecycle.lock().unwrap();
        lifecycle.closed = true;
        lifecycle.close_in_progress = false;
        self.lifecycle_cv.notify_all();
        result
    }

    fn best_effort_shutdown(&self) {
        let should_close = {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            if lifecycle.closed || lifecycle.closing || lifecycle.close_in_progress {
                false
            } else {
                lifecycle.closing = true;
                lifecycle.close_in_progress = true;
                true
            }
        };
        if !should_close {
            return;
        }

        self.set_open_state(RuntimeOpenState::Closing);
        self.wait_for_reads_to_drain();
        {
            let mut lifecycle = self.lifecycle.lock().unwrap();
            while lifecycle.active_non_read_ops > 0 {
                lifecycle = self.lifecycle_cv.wait(lifecycle).unwrap();
            }
        }

        self.request_coordinator_shutdown();
        self.join_coordinator();

        let maybe_core = {
            let mut core_guard = self.core.lock().unwrap();
            core_guard.take()
        };
        if let Some(core) = maybe_core {
            let _ = core.close_fast();
        }
        self.set_open_state(RuntimeOpenState::Closed);
        let mut lifecycle = self.lifecycle.lock().unwrap();
        lifecycle.closed = true;
        lifecycle.close_in_progress = false;
        self.lifecycle_cv.notify_all();
    }

    #[cfg(test)]
    fn wait_one_flush_public(&self) -> Result<Option<SegmentInfo>, EngineError> {
        let _guard = self.admit_operation()?;
        loop {
            if let Some(info) = self.take_next_completed_flush() {
                return Ok(Some(info));
            }

            let wait_seq = self.lifecycle_progress_seq();
            let mut core_guard = self.core.lock().unwrap();
            let core = core_guard.as_mut().ok_or(EngineError::DatabaseClosed)?;
            if core.flush_pipeline_error.is_some() && !core.flush_pipeline_error_reported {
                let err = core
                    .current_flush_pipeline_error()
                    .expect("flush pipeline error must be present");
                self.publish_locked(core, PublishImpact::NoPublish, true);
                drop(core_guard);
                return Err(err);
            }
            if !core.immutable_epochs.iter().any(|epoch| epoch.in_flight) {
                drop(core_guard);
                return Ok(None);
            }
            drop(core_guard);
            self.wait_for_lifecycle_progress(wait_seq)?;
        }
    }

    #[cfg(test)]
    fn wait_for_bg_compaction_public(&self) -> Option<CompactionStats> {
        let _guard = self.admit_operation().ok()?;
        loop {
            if let Some(stats) = self.take_next_completed_bg_compaction() {
                return Some(stats);
            }

            let wait_seq = self.lifecycle_progress_seq();
            let core_guard = self.core.lock().unwrap();
            let core = core_guard.as_ref()?;
            core.bg_compact.as_ref()?;
            drop(core_guard);
            if self.wait_for_lifecycle_progress(wait_seq).is_err() {
                return None;
            }
        }
    }

    fn record_property_query_route(&self, _route: PropertyQueryRouteKind) {
        #[cfg(test)]
        match _route {
            PropertyQueryRouteKind::EqualityScanFallback => {
                self.property_query_routes
                    .equality_scan_fallback
                    .fetch_add(1, Ordering::Relaxed);
            }
            PropertyQueryRouteKind::EqualityIndexLookup => {
                self.property_query_routes
                    .equality_index_lookup
                    .fetch_add(1, Ordering::Relaxed);
            }
            PropertyQueryRouteKind::RangeScanFallback => {
                self.property_query_routes
                    .range_scan_fallback
                    .fetch_add(1, Ordering::Relaxed);
            }
            PropertyQueryRouteKind::RangeIndexLookup => {
                self.property_query_routes
                    .range_index_lookup
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_degree_query_routes(&self, _routes: DegreeQueryRouteTally) {
        #[cfg(test)]
        {
            self.degree_query_routes
                .fast_path
                .fetch_add(_routes.fast_path, Ordering::Relaxed);
            self.degree_query_routes
                .walk_path
                .fetch_add(_routes.walk_path, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    fn property_query_route_snapshot(&self) -> PropertyQueryRouteSnapshot {
        PropertyQueryRouteSnapshot {
            equality_scan_fallback: self
                .property_query_routes
                .equality_scan_fallback
                .load(Ordering::Relaxed),
            equality_index_lookup: self
                .property_query_routes
                .equality_index_lookup
                .load(Ordering::Relaxed),
            range_scan_fallback: self
                .property_query_routes
                .range_scan_fallback
                .load(Ordering::Relaxed),
            range_index_lookup: self
                .property_query_routes
                .range_index_lookup
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn degree_query_route_snapshot(&self) -> DegreeQueryRouteSnapshot {
        DegreeQueryRouteSnapshot {
            fast_path: self.degree_query_routes.fast_path.load(Ordering::Relaxed),
            walk_path: self.degree_query_routes.walk_path.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn reset_property_query_routes(&self) {
        self.property_query_routes
            .equality_scan_fallback
            .store(0, Ordering::Relaxed);
        self.property_query_routes
            .equality_index_lookup
            .store(0, Ordering::Relaxed);
        self.property_query_routes
            .range_scan_fallback
            .store(0, Ordering::Relaxed);
        self.property_query_routes
            .range_index_lookup
            .store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn reset_degree_query_routes(&self) {
        self.degree_query_routes
            .fast_path
            .store(0, Ordering::Relaxed);
        self.degree_query_routes
            .walk_path
            .store(0, Ordering::Relaxed);
    }
}

impl ReadView {
    fn from_published_sources(
        sources: Arc<PublishedReadSources>,
        snapshot_seq: u64,
        active_degree_overlay: Arc<DegreeOverlaySnapshot>,
    ) -> Self {
        debug_assert!(
            sources.declared_index_runtime_coverage.entry_count()
                <= sources
                    .segments
                    .len()
                    .saturating_mul(sources.secondary_index_entries.len())
        );
        Self {
            sources,
            snapshot_seq,
            active_degree_overlay,
        }
    }

    fn sources(&self) -> SourceList<'_> {
        SourceList {
            active: self.memtable.as_ref(),
            immutable: &self.immutable_epochs,
            segments: &self.segments,
            snapshot_seq: self.snapshot_seq,
        }
    }

    #[cfg(test)]
    fn note_range_planning_probe(&self) {
        self.planning_probe_counters
            .range
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_timestamp_planning_probe(&self) {
        self.planning_probe_counters
            .timestamp
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_equality_materialization_record_reads(&self, count: usize) {
        self.query_execution_counters
            .equality_materialization_record_reads
            .fetch_add(count, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_final_verifier_record_reads(&self, count: usize) {
        self.query_execution_counters
            .final_verifier_record_reads
            .fetch_add(count, Ordering::Relaxed);
    }

    fn node_property_index_entry(
        &self,
        type_id: u32,
        prop_key: &str,
        kind: &SecondaryIndexKind,
    ) -> Option<SecondaryIndexManifestEntry> {
        self.secondary_index_catalog
            .get(&type_id)?
            .get(prop_key)?
            .get(kind)
            .cloned()
    }

    fn get_node_raw(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node(id)
    }

    fn get_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_raw(id)? {
            Some(node) => node,
            None => return Ok(None),
        };
        if self.is_node_excluded_by_policies(&node) {
            return Ok(None);
        }
        Ok(Some(node))
    }

    fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge(id)
    }

    fn get_node_by_key_raw(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node_by_key(type_id, key)
    }

    fn get_node_by_key(&self, type_id: u32, key: &str) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_by_key_raw(type_id, key)? {
            Some(node) => node,
            None => return Ok(None),
        };
        if self.is_node_excluded_by_policies(&node) {
            return Ok(None);
        }
        Ok(Some(node))
    }

    fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge_by_triple(from, to, type_id)
    }

    fn get_nodes_raw(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        self.sources().find_nodes(ids)
    }

    fn get_nodes(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = self.get_nodes_raw(ids)?;
        if !self.manifest.prune_policies.is_empty() {
            let cutoffs =
                PrecomputedPruneCutoffs::from_policies(&self.manifest.prune_policies, now_millis());
            for slot in &mut results {
                if let Some(node) = slot.as_ref() {
                    if cutoffs.excludes(node) {
                        *slot = None;
                    }
                }
            }
        }
        Ok(results)
    }

    fn get_nodes_by_keys_raw(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        self.sources().find_nodes_by_keys(keys)
    }

    fn get_nodes_by_keys(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = self.get_nodes_by_keys_raw(keys)?;
        if !self.manifest.prune_policies.is_empty() {
            let cutoffs =
                PrecomputedPruneCutoffs::from_policies(&self.manifest.prune_policies, now_millis());
            for slot in &mut results {
                if let Some(node) = slot.as_ref() {
                    if cutoffs.excludes(node) {
                        *slot = None;
                    }
                }
            }
        }
        Ok(results)
    }

    fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        self.sources().find_edges(ids)
    }
}

impl EngineCore {
    fn sources(&self) -> SourceList<'_> {
        SourceList {
            active: self.memtable.as_ref(),
            immutable: &self.immutable_epochs,
            segments: &self.segments,
            snapshot_seq: self.engine_seq,
        }
    }

    fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge(id)
    }

    fn get_node_by_key_raw(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node_by_key(type_id, key)
    }

    fn get_nodes_by_keys_raw(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        self.sources().find_nodes_by_keys(keys)
    }

    fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge_by_triple(from, to, type_id)
    }

    fn get_nodes_raw(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        self.sources().find_nodes(ids)
    }

    fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        self.read_view().get_edges(ids)
    }

    fn nodes_by_type_raw(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        self.read_view().nodes_by_type_raw(type_id)
    }

    fn collect_tombstones(
        &self,
    ) -> (
        HashSet<u64, NodeIdBuildHasher>,
        HashSet<u64, NodeIdBuildHasher>,
    ) {
        self.read_view().collect_tombstones()
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn neighbors_raw(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
        tombstones: Option<(
            &HashSet<u64, NodeIdBuildHasher>,
            &HashSet<u64, NodeIdBuildHasher>,
        )>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        self.read_view().neighbors_raw(
            node_id,
            direction,
            type_filter,
            limit,
            at_epoch,
            decay_lambda,
            tombstones,
        )
    }

    fn degrade_ready_equality_index_after_sidecar_failure(
        &mut self,
        index_id: u64,
        error: Option<&EngineError>,
    ) -> PublishImpact {
        let next_state = if error.is_some_and(|error| !is_not_found_io_error(error)) {
            SecondaryIndexState::Failed
        } else {
            SecondaryIndexState::Building
        };
        let next_last_error = if next_state == SecondaryIndexState::Failed {
            error.map(ToString::to_string)
        } else {
            None
        };

        let Some(current_entry) = self
            .secondary_index_entries_snapshot()
            .into_iter()
            .find(|entry| entry.index_id == index_id)
        else {
            return PublishImpact::NoPublish;
        };
        if !matches!(current_entry.kind, SecondaryIndexKind::Equality) {
            return PublishImpact::NoPublish;
        }
        let should_queue_build = next_state == SecondaryIndexState::Building
            && current_entry.state != SecondaryIndexState::Building;
        if current_entry.state == next_state && current_entry.last_error == next_last_error {
            return PublishImpact::NoPublish;
        }

        let _ = update_secondary_index_manifest_runtime(
            &self.db_dir,
            &self.manifest_write_lock,
            &self.secondary_index_catalog,
            &self.secondary_index_entries,
            &self.next_node_id_seen,
            &self.next_edge_id_seen,
            &self.engine_seq_seen,
            |manifest| {
                if let Some(entry) = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == index_id)
                {
                    if matches!(entry.kind, SecondaryIndexKind::Equality) {
                        entry.state = next_state;
                        entry.last_error = next_last_error.clone();
                    }
                }
                Ok(())
            },
        );
        self.manifest.secondary_indexes = self.secondary_index_entries_snapshot();

        if should_queue_build {
            if let Some(bg) = &self.secondary_index_bg {
                let _ = bg.job_tx.send(SecondaryIndexJob::Build { index_id });
            }
        }
        PublishImpact::RebuildSources
    }

    fn degrade_ready_range_index_after_sidecar_failure(
        &mut self,
        index_id: u64,
        error: Option<&EngineError>,
    ) -> PublishImpact {
        let next_state = if error.is_some_and(|error| !is_not_found_io_error(error)) {
            SecondaryIndexState::Failed
        } else {
            SecondaryIndexState::Building
        };
        let next_last_error = if next_state == SecondaryIndexState::Failed {
            error.map(ToString::to_string)
        } else {
            None
        };

        let Some(current_entry) = self
            .secondary_index_entries_snapshot()
            .into_iter()
            .find(|entry| entry.index_id == index_id)
        else {
            return PublishImpact::NoPublish;
        };
        if !matches!(current_entry.kind, SecondaryIndexKind::Range { .. }) {
            return PublishImpact::NoPublish;
        }
        let should_queue_build = next_state == SecondaryIndexState::Building
            && current_entry.state != SecondaryIndexState::Building;
        if current_entry.state == next_state && current_entry.last_error == next_last_error {
            return PublishImpact::NoPublish;
        }

        let _ = update_secondary_index_manifest_runtime(
            &self.db_dir,
            &self.manifest_write_lock,
            &self.secondary_index_catalog,
            &self.secondary_index_entries,
            &self.next_node_id_seen,
            &self.next_edge_id_seen,
            &self.engine_seq_seen,
            |manifest| {
                if let Some(entry) = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == index_id)
                {
                    if matches!(entry.kind, SecondaryIndexKind::Range { .. }) {
                        entry.state = next_state;
                        entry.last_error = next_last_error.clone();
                    }
                }
                Ok(())
            },
        );
        self.manifest.secondary_indexes = self.secondary_index_entries_snapshot();

        if should_queue_build {
            if let Some(bg) = &self.secondary_index_bg {
                let _ = bg.job_tx.send(SecondaryIndexJob::Build { index_id });
            }
        }
        PublishImpact::RebuildSources
    }
}

/// Cloneable shared-handle database runtime for OverGraph.
#[derive(Clone)]
pub struct DatabaseEngine {
    runtime: Arc<DbRuntime>,
}

impl DatabaseEngine {
    pub fn open(path: &Path, options: &DbOptions) -> Result<Self, EngineError> {
        let core = EngineCore::open(path, options)?;
        let runtime = Arc::new(DbRuntime::new(path.to_path_buf(), core));
        runtime.install_core_runtime_handle();
        Ok(Self { runtime })
    }

    fn with_core_ref<T>(
        &self,
        f: impl FnOnce(&EngineCore) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        self.runtime.with_core_ref(f)
    }

    pub fn close(&self) -> Result<(), EngineError> {
        self.runtime.close(false)
    }

    pub fn close_fast(&self) -> Result<(), EngineError> {
        self.runtime.close(true)
    }

    pub fn upsert_node(
        &self,
        type_id: u32,
        key: &str,
        options: UpsertNodeOptions,
    ) -> Result<u64, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::UpsertNode {
                type_id,
                key: key.to_string(),
                options,
            })? {
            CoreWriteReply::U64(id) => Ok(id),
            _ => unreachable!("upsert_node must return a node id"),
        }
    }

    pub fn upsert_edge(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
        options: UpsertEdgeOptions,
    ) -> Result<u64, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::UpsertEdge {
                from,
                to,
                type_id,
                options,
            })? {
            CoreWriteReply::U64(id) => Ok(id),
            _ => unreachable!("upsert_edge must return an edge id"),
        }
    }

    pub fn batch_upsert_nodes(&self, inputs: &[NodeInput]) -> Result<Vec<u64>, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::BatchUpsertNodes {
                inputs: inputs.to_vec(),
            })? {
            CoreWriteReply::VecU64(ids) => Ok(ids),
            _ => unreachable!("batch_upsert_nodes must return node ids"),
        }
    }

    pub fn batch_upsert_edges(&self, inputs: &[EdgeInput]) -> Result<Vec<u64>, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::BatchUpsertEdges {
                inputs: inputs.to_vec(),
            })? {
            CoreWriteReply::VecU64(ids) => Ok(ids),
            _ => unreachable!("batch_upsert_edges must return edge ids"),
        }
    }

    pub fn delete_node(&self, id: u64) -> Result<(), EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DeleteNode { id })?
        {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("delete_node must return unit"),
        }
    }

    pub fn delete_edge(&self, id: u64) -> Result<(), EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DeleteEdge { id })?
        {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("delete_edge must return unit"),
        }
    }

    pub fn invalidate_edge(
        &self,
        id: u64,
        valid_to: i64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::InvalidateEdge { id, valid_to })?
        {
            CoreWriteReply::OptionEdge(edge) => Ok(edge),
            _ => unreachable!("invalidate_edge must return an optional edge"),
        }
    }

    pub fn graph_patch(&self, patch: &GraphPatch) -> Result<PatchResult, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::GraphPatch {
                patch: patch.clone(),
            })? {
            CoreWriteReply::PatchResult(result) => Ok(result),
            _ => unreachable!("graph_patch must return patch results"),
        }
    }

    pub fn prune(&self, policy: &PrunePolicy) -> Result<PruneResult, EngineError> {
        match self.runtime.submit_core_write(CoreWriteRequest::Prune {
            policy: policy.clone(),
        })? {
            CoreWriteReply::PruneResult(result) => Ok(result),
            _ => unreachable!("prune must return prune results"),
        }
    }

    pub fn set_prune_policy(&self, name: &str, policy: PrunePolicy) -> Result<(), EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::SetPrunePolicy {
                name: name.to_string(),
                policy,
            })? {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("set_prune_policy must return unit"),
        }
    }

    pub fn remove_prune_policy(&self, name: &str) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::RemovePrunePolicy {
                name: name.to_string(),
            })? {
            CoreWriteReply::Bool(removed) => Ok(removed),
            _ => unreachable!("remove_prune_policy must return bool"),
        }
    }

    pub fn list_prune_policies(&self) -> Result<Vec<(String, PrunePolicy)>, EngineError> {
        self.with_core_ref(|core| Ok(core.list_prune_policies()))
    }

    pub fn ensure_node_property_index(
        &self,
        type_id: u32,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<NodePropertyIndexInfo, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::EnsureNodePropertyIndex {
                type_id,
                prop_key: prop_key.to_string(),
                kind,
            })? {
            CoreWriteReply::NodePropertyIndexInfo(info) => Ok(info),
            _ => unreachable!("ensure_node_property_index must return index info"),
        }
    }

    pub fn drop_node_property_index(
        &self,
        type_id: u32,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DropNodePropertyIndex {
                type_id,
                prop_key: prop_key.to_string(),
                kind,
            })? {
            CoreWriteReply::Bool(dropped) => Ok(dropped),
            _ => unreachable!("drop_node_property_index must return bool"),
        }
    }

    pub fn list_node_property_indexes(&self) -> Result<Vec<NodePropertyIndexInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut indexes: Vec<NodePropertyIndexInfo> = published
            .view
            .secondary_index_entries
            .iter()
            .map(EngineCore::node_property_index_info)
            .collect();
        indexes.sort_unstable_by(|left, right| {
            left.type_id
                .cmp(&right.type_id)
                .then_with(|| left.prop_key.cmp(&right.prop_key))
                .then_with(|| format!("{:?}", left.kind).cmp(&format!("{:?}", right.kind)))
                .then_with(|| left.index_id.cmp(&right.index_id))
        });
        Ok(indexes)
    }

    pub fn get_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_node(id)
    }

    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_edge(id)
    }

    pub fn get_node_by_key(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_node_by_key(type_id, key)
    }

    pub fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_edge_by_triple(from, to, type_id)
    }

    pub fn get_nodes(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_nodes(ids)
    }

    pub fn get_nodes_by_keys(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_nodes_by_keys(keys)
    }

    pub fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_edges(ids)
    }

    pub fn vector_search(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.vector_search(request)
    }

    pub fn nodes_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.nodes_by_type(type_id)
    }

    pub fn edges_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.edges_by_type(type_id)
    }

    pub fn get_nodes_by_type(&self, type_id: u32) -> Result<Vec<NodeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_nodes_by_type(type_id)
    }

    pub fn get_edges_by_type(&self, type_id: u32) -> Result<Vec<EdgeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_edges_by_type(type_id)
    }

    pub fn count_nodes_by_type(&self, type_id: u32) -> Result<u64, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.count_nodes_by_type(type_id)
    }

    pub fn count_edges_by_type(&self, type_id: u32) -> Result<u64, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.count_edges_by_type(type_id)
    }

    pub fn nodes_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.nodes_by_type_paged(type_id, page)
    }

    pub fn edges_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.edges_by_type_paged(type_id, page)
    }

    pub fn get_nodes_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<NodeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_nodes_by_type_paged(type_id, page)
    }

    pub fn get_edges_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<EdgeRecord>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_edges_by_type_paged(type_id, page)
    }

    pub fn find_nodes(
        &self,
        type_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published
            .view
            .find_nodes_outcome(type_id, prop_key, prop_value)?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn find_nodes_paged(
        &self,
        type_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published
            .view
            .find_nodes_paged_outcome(type_id, prop_key, prop_value, page)?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn find_nodes_range(
        &self,
        type_id: u32,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.find_nodes_range_paged_outcome(
            type_id,
            prop_key,
            lower,
            upper,
            &PropertyRangePageRequest::default(),
        )?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value.items)
    }

    pub fn find_nodes_range_paged(
        &self,
        type_id: u32,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        page: &PropertyRangePageRequest,
    ) -> Result<PropertyRangePageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published
            .view
            .find_nodes_range_paged_outcome(type_id, prop_key, lower, upper, page)?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn find_nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published
            .view
            .find_nodes_by_time_range(type_id, from_ms, to_ms)
    }

    pub fn find_nodes_by_time_range_paged(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published
            .view
            .find_nodes_by_time_range_paged(type_id, from_ms, to_ms, page)
    }

    pub fn personalized_pagerank(
        &self,
        seeds: &[u64],
        options: &PprOptions,
    ) -> Result<PprResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.personalized_pagerank(seeds, options)
    }

    pub fn export_adjacency(
        &self,
        options: &ExportOptions,
    ) -> Result<AdjacencyExport, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.export_adjacency(options)
    }

    pub fn degree(&self, node_id: u64, options: &DegreeOptions) -> Result<u64, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.degree_outcome(node_id, options)?;
        self.runtime.record_degree_query_routes(outcome.routes);
        Ok(outcome.value)
    }

    pub fn sum_edge_weights(
        &self,
        node_id: u64,
        options: &DegreeOptions,
    ) -> Result<f64, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.sum_edge_weights_outcome(node_id, options)?;
        self.runtime.record_degree_query_routes(outcome.routes);
        Ok(outcome.value)
    }

    pub fn avg_edge_weight(
        &self,
        node_id: u64,
        options: &DegreeOptions,
    ) -> Result<Option<f64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.avg_edge_weight_outcome(node_id, options)?;
        self.runtime.record_degree_query_routes(outcome.routes);
        Ok(outcome.value)
    }

    pub fn degrees(
        &self,
        node_ids: &[u64],
        options: &DegreeOptions,
    ) -> Result<NodeIdMap<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.degrees_outcome(node_ids, options)?;
        self.runtime.record_degree_query_routes(outcome.routes);
        Ok(outcome.value)
    }

    pub fn shortest_path(
        &self,
        from: u64,
        to: u64,
        options: &ShortestPathOptions,
    ) -> Result<Option<ShortestPath>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.shortest_path(from, to, options)
    }

    pub fn is_connected(
        &self,
        from: u64,
        to: u64,
        options: &IsConnectedOptions,
    ) -> Result<bool, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.is_connected(from, to, options)
    }

    pub fn traverse(
        &self,
        start_node_id: u64,
        max_depth: u32,
        options: &TraverseOptions,
    ) -> Result<TraversalPageResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.traverse(start_node_id, max_depth, options)
    }

    pub fn all_shortest_paths(
        &self,
        from: u64,
        to: u64,
        options: &AllShortestPathsOptions,
    ) -> Result<Vec<ShortestPath>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.all_shortest_paths(from, to, options)
    }

    pub fn neighbors(
        &self,
        node_id: u64,
        options: &NeighborOptions,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.neighbors(node_id, options)
    }

    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        options: &NeighborOptions,
    ) -> Result<NodeIdMap<Vec<NeighborEntry>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.neighbors_batch(node_ids, options)
    }

    pub fn neighbors_paged(
        &self,
        node_id: u64,
        options: &NeighborOptions,
        page: &PageRequest,
    ) -> Result<PageResult<NeighborEntry>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.neighbors_paged(node_id, options, page)
    }

    pub fn top_k_neighbors(
        &self,
        node_id: u64,
        k: usize,
        options: &TopKOptions,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.top_k_neighbors(node_id, k, options)
    }

    pub fn extract_subgraph(
        &self,
        start: u64,
        max_depth: u32,
        options: &SubgraphOptions,
    ) -> Result<Subgraph, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.extract_subgraph(start, max_depth, options)
    }

    pub fn connected_components(
        &self,
        options: &ComponentOptions,
    ) -> Result<NodeIdMap<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.connected_components(options)
    }

    pub fn component_of(
        &self,
        node_id: u64,
        options: &ComponentOptions,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.component_of(node_id, options)
    }

    pub fn sync(&self) -> Result<(), EngineError> {
        match self.runtime.submit_core_write(CoreWriteRequest::Sync)? {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("sync must return unit"),
        }
    }

    pub fn flush(&self) -> Result<Option<SegmentInfo>, EngineError> {
        match self.runtime.submit_core_write(CoreWriteRequest::Flush)? {
            CoreWriteReply::OptionSegmentInfo(info) => Ok(info),
            _ => unreachable!("flush must return optional segment info"),
        }
    }

    pub fn ingest_mode(&self) -> Result<(), EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::IngestMode)?
        {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("ingest_mode must return unit"),
        }
    }

    pub fn end_ingest(&self) -> Result<Option<CompactionStats>, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::EndIngest)?
        {
            CoreWriteReply::OptionCompactionStats(stats) => Ok(stats),
            _ => unreachable!("end_ingest must return optional compaction stats"),
        }
    }

    pub fn compact(&self) -> Result<Option<CompactionStats>, EngineError> {
        match self.runtime.submit_core_write(CoreWriteRequest::Compact)? {
            CoreWriteReply::OptionCompactionStats(stats) => Ok(stats),
            _ => unreachable!("compact must return optional compaction stats"),
        }
    }

    pub fn compact_with_progress<F>(
        &self,
        progress: F,
    ) -> Result<Option<CompactionStats>, EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        let _barrier = self.runtime.begin_mutating_barrier()?;
        self.runtime.execute_compaction_barrier(progress)
    }

    pub fn stats(&self) -> Result<DbStats, EngineError> {
        self.with_core_ref(|core| Ok(core.stats()))
    }

    #[cfg(test)]
    pub(crate) fn write_op(&self, op: &WalOp) -> Result<(), EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::WriteOp { op: op.clone() })?
        {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("write_op must return unit"),
        }
    }

    #[cfg(test)]
    pub(crate) fn write_op_batch(&self, ops: &[WalOp]) -> Result<(), EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::WriteOpBatch { ops: ops.to_vec() })?
        {
            CoreWriteReply::Unit => Ok(()),
            _ => unreachable!("write_op_batch must return unit"),
        }
    }

    pub fn path(&self) -> &Path {
        self.runtime.path()
    }

    pub fn manifest(&self) -> Result<ManifestState, EngineError> {
        self.with_core_ref(|core| {
            let mut manifest = core.manifest.clone();
            merge_runtime_manifest_counters_from_shared(
                &mut manifest,
                &core.next_node_id_seen,
                &core.next_edge_id_seen,
                &core.engine_seq_seen,
            );
            Ok(manifest)
        })
    }

    pub fn node_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| Ok(core.node_count()))
    }

    pub fn edge_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| Ok(core.edge_count()))
    }

    pub fn next_node_id(&self) -> Result<u64, EngineError> {
        self.with_core_ref(|core| Ok(core.next_node_id))
    }

    pub fn next_edge_id(&self) -> Result<u64, EngineError> {
        self.with_core_ref(|core| Ok(core.next_edge_id))
    }

    pub fn segment_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| Ok(core.segments.len()))
    }

    pub fn segment_tombstone_node_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| {
            Ok(core
                .segments
                .iter()
                .map(|segment| segment.deleted_node_count())
                .sum())
        })
    }

    pub fn segment_tombstone_edge_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| {
            Ok(core
                .segments
                .iter()
                .map(|segment| segment.deleted_edge_count())
                .sum())
        })
    }
}

#[cfg(test)]
impl DatabaseEngine {
    fn with_core_mut<T>(
        &self,
        f: impl FnOnce(&mut EngineCore) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        self.runtime.with_core_mut(f)
    }

    fn published_state(&self) -> Arc<PublishedReadState> {
        self.runtime.published.load_full()
    }

    pub(crate) fn find_existing_node(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        Ok(published
            .view
            .get_node_by_key_raw(type_id, key)?
            .map(|node| (node.id, node.created_at)))
    }

    pub(crate) fn get_nodes_raw(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.get_nodes_raw(ids)
    }

    pub(crate) fn segments_for_test(&self) -> Vec<Arc<SegmentReader>> {
        self.with_core_ref(|core| Ok(core.segments.clone()))
            .unwrap_or_else(|_| self.published_state().view.segments.clone())
    }

    pub(crate) fn bg_compact_active_for_test(&self) -> bool {
        self.with_core_ref(|core| Ok(core.bg_compact.is_some()))
            .unwrap_or(false)
    }

    pub(crate) fn bg_compact_incomplete_for_test(&self) -> bool {
        self.with_core_ref(|core| {
            Ok(core
                .bg_compact
                .as_ref()
                .is_some_and(|bg| !bg.completed.load(Ordering::Acquire)))
        })
        .unwrap_or(false)
    }

    pub(crate) fn flush_count_since_last_compact_for_test(&self) -> u32 {
        self.with_core_ref(|core| Ok(core.flush_count_since_last_compact))
            .unwrap_or(0)
    }

    pub(crate) fn start_bg_compact(&self) -> Result<(), EngineError> {
        self.with_core_mut(|core| core.start_bg_compact())
    }

    pub(crate) fn wait_for_bg_compact(&self) -> Option<CompactionStats> {
        self.runtime.wait_for_bg_compaction_public()
    }

    pub(crate) fn cancel_bg_compact(&self) {
        let _ = self.with_core_mut(|core| {
            core.cancel_bg_compact();
            Ok(())
        });
    }

    pub(crate) fn degree_cache_entry(&self, node_id: u64) -> DegreeEntry {
        self.with_core_ref(|core| Ok(core.degree_cache_entry(node_id)))
            .unwrap_or(DegreeEntry::ZERO)
    }

    pub(crate) fn published_read_view_for_test(&self) -> Arc<ReadView> {
        Arc::clone(&self.published_state().view)
    }

    pub(crate) fn planner_stats_view_for_test(&self) -> Arc<PlannerStatsView> {
        Arc::clone(&self.published_state().view.planner_stats)
    }

    #[cfg(test)]
    pub(crate) fn declared_index_runtime_coverage_len_for_test(&self) -> usize {
        self.published_state()
            .view
            .declared_index_runtime_coverage
            .entry_count()
    }

    #[cfg(test)]
    pub(crate) fn reopen_segment_reader_and_rebuild_sources_for_test(
        &self,
        segment_id: u64,
    ) -> Result<(), EngineError> {
        let mut core_guard = self.runtime.core.lock().unwrap();
        let core = core_guard
            .as_mut()
            .ok_or_else(|| EngineError::InvalidOperation("database is closed".into()))?;
        let seg_path = segment_dir(&core.db_dir, segment_id);
        let reader =
            SegmentReader::open(&seg_path, segment_id, core.manifest.dense_vector.as_ref())?;
        core.warm_declared_index_runtime_coverage_for_reader(&reader);
        let Some(position) = core
            .segments
            .iter()
            .position(|segment| segment.segment_id == segment_id)
        else {
            return Err(EngineError::InvalidOperation(format!(
                "segment {} is not published",
                segment_id
            )));
        };
        core.segments[position] = Arc::new(reader);
        self.runtime
            .publish_locked(core, PublishImpact::RebuildSources, false);
        Ok(())
    }

    pub(crate) fn active_degree_overlay_for_test(&self) -> Arc<DegreeOverlaySnapshot> {
        self.with_core_ref(|core| Ok(Arc::clone(&core.active_degree_overlay)))
            .unwrap_or_else(|_| Arc::clone(&self.published_state().view.active_degree_overlay))
    }

    pub(crate) fn immutable_memtable_count(&self) -> usize {
        self.with_core_ref(|core| Ok(core.immutable_memtable_count()))
            .unwrap_or_else(|_| self.published_state().view.immutable_epochs.len())
    }

    pub(crate) fn active_wal_generation(&self) -> u64 {
        self.with_core_ref(|core| Ok(core.active_wal_generation()))
            .unwrap_or_else(|_| self.published_state().active_wal_generation_id)
    }

    pub(crate) fn engine_seq_for_test(&self) -> u64 {
        self.with_core_ref(|core| Ok(core.engine_seq_for_test()))
            .unwrap_or_else(|_| self.published_state().engine_seq)
    }

    pub(crate) fn active_memtable(&self) -> Memtable {
        self.with_core_ref(|core| Ok(core.active_memtable().clone()))
            .unwrap_or_else(|_| self.published_state().view.memtable.as_ref().clone())
    }

    pub(crate) fn immutable_memtable(&self, idx: usize) -> Memtable {
        self.with_core_ref(|core| Ok(core.immutable_memtable(idx).clone()))
            .unwrap_or_else(|_| {
                self.published_state().view.immutable_epochs[idx]
                    .memtable
                    .as_ref()
                    .clone()
            })
    }

    pub(crate) fn property_query_route_snapshot(&self) -> PropertyQueryRouteSnapshot {
        self.runtime.property_query_route_snapshot()
    }

    pub(crate) fn reset_property_query_routes(&self) {
        self.runtime.reset_property_query_routes();
    }

    pub(crate) fn degree_query_route_snapshot(&self) -> DegreeQueryRouteSnapshot {
        self.runtime.degree_query_route_snapshot()
    }

    pub(crate) fn reset_degree_query_routes(&self) {
        self.runtime.reset_degree_query_routes();
    }

    pub(crate) fn publish_counter_snapshot_for_test(&self) -> PublishCounterSnapshot {
        self.runtime.publish_counter_snapshot()
    }

    pub(crate) fn reset_publish_counters_for_test(&self) {
        self.runtime.reset_publish_counters();
    }

    pub(crate) fn published_read_source_build_count_for_test(&self) -> usize {
        self.with_core_ref(|core| Ok(core.published_read_source_builds.load(Ordering::Relaxed)))
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn query_planning_probe_snapshot_for_test(&self) -> QueryPlanningProbeSnapshot {
        let published = self.published_state();
        QueryPlanningProbeSnapshot {
            range: published
                .view
                .planning_probe_counters
                .range
                .load(Ordering::Relaxed),
            timestamp: published
                .view
                .planning_probe_counters
                .timestamp
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_query_planning_probe_counters_for_test(&self) {
        let published = self.published_state();
        published
            .view
            .planning_probe_counters
            .range
            .store(0, Ordering::Relaxed);
        published
            .view
            .planning_probe_counters
            .timestamp
            .store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn query_execution_counter_snapshot_for_test(
        &self,
    ) -> QueryExecutionCounterSnapshot {
        let published = self.published_state();
        QueryExecutionCounterSnapshot {
            equality_materialization_record_reads: published
                .view
                .query_execution_counters
                .equality_materialization_record_reads
                .load(Ordering::Relaxed),
            final_verifier_record_reads: published
                .view
                .query_execution_counters
                .final_verifier_record_reads
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_query_execution_counters_for_test(&self) {
        let published = self.published_state();
        published
            .view
            .query_execution_counters
            .equality_materialization_record_reads
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .final_verifier_record_reads
            .store(0, Ordering::Relaxed);
    }

    pub(crate) fn set_flush_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.with_core_mut(|core| Ok(core.set_flush_pause()))
            .expect("set flush pause")
    }

    pub(crate) fn set_flush_publish_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.with_core_ref(|core| Ok(core.set_flush_publish_pause()))
            .expect("set flush publish pause")
    }

    pub(crate) fn set_bg_compact_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.with_core_ref(|core| Ok(core.set_bg_compact_pause()))
            .expect("set bg compact pause")
    }

    pub(crate) fn set_flush_force_error(&self) {
        let _ = self.with_core_mut(|core| {
            core.set_flush_force_error();
            Ok(())
        });
    }

    pub(crate) fn set_secondary_index_build_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.with_core_ref(|core| Ok(core.set_secondary_index_build_pause()))
            .expect("set secondary index build pause")
    }

    pub(crate) fn set_runtime_publish_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.runtime.write_publish_pause.lock().unwrap() = Some(RuntimePublishPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    pub(crate) fn set_runtime_read_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.runtime.read_admission_pause.lock().unwrap() = Some(RuntimeReadPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    pub(crate) fn set_core_write_queue_capacity_for_test(&self, capacity: usize) {
        let mut lifecycle = self.runtime.lifecycle.lock().unwrap();
        lifecycle.coordinator_queue_capacity = capacity.max(1);
        self.runtime.lifecycle_cv.notify_all();
    }

    pub(crate) fn wait_for_mutating_barrier_active_for_test(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut lifecycle = self.runtime.lifecycle.lock().unwrap();
        while !lifecycle.mutating_barrier_active {
            assert!(
                !lifecycle.closing && !lifecycle.closed,
                "database closed before mutating barrier became active"
            );
            let now = std::time::Instant::now();
            assert!(
                now < deadline,
                "timed out waiting for mutating barrier; active_mutating_ops={}, active_non_read_ops={}",
                lifecycle.active_mutating_ops,
                lifecycle.active_non_read_ops
            );
            let remaining = deadline.saturating_duration_since(now);
            let wait_for = remaining.min(std::time::Duration::from_millis(50));
            let (next, _) = self
                .runtime
                .lifecycle_cv
                .wait_timeout(lifecycle, wait_for)
                .unwrap();
            lifecycle = next;
        }
    }

    pub(crate) fn pending_secondary_index_followup_count_for_test(&self) -> usize {
        let lifecycle = self.runtime.lifecycle.lock().unwrap();
        lifecycle.pending_secondary_index_followups.len()
    }

    pub(crate) fn enqueue_one_flush(&self) -> Result<(), EngineError> {
        self.with_core_mut(|core| core.enqueue_one_flush())
    }

    pub(crate) fn freeze_memtable(&self) -> Result<(), EngineError> {
        self.with_core_mut(|core| core.freeze_memtable())
    }

    pub(crate) fn wait_one_flush(&self) -> Result<Option<SegmentInfo>, EngineError> {
        self.runtime.wait_one_flush_public()
    }

    pub(crate) fn wait_for_bg_compaction(&self) -> Option<CompactionStats> {
        self.runtime.wait_for_bg_compaction_public()
    }

    pub(crate) fn immutable_epoch_count(&self) -> usize {
        self.with_core_ref(|core| Ok(core.immutable_epoch_count()))
            .unwrap_or_else(|_| self.published_state().view.immutable_epochs.len())
    }

    pub(crate) fn in_flight_count(&self) -> usize {
        self.with_core_ref(|core| Ok(core.in_flight_count()))
            .unwrap_or(0)
    }

    pub(crate) fn replace_wal_state_for_test(
        &self,
        wal_state: Arc<(Mutex<WalSyncState>, Condvar)>,
    ) -> Result<(), EngineError> {
        self.with_core_mut(|core| {
            if let Some(ref current_state) = core.wal_state {
                shutdown_sync_thread(current_state, &mut core.sync_thread)?;
            }
            core.wal_state = Some(wal_state);
            core.sync_thread = None;
            Ok(())
        })
    }

    pub(crate) fn with_runtime_manifest_write<T>(
        &self,
        f: impl FnOnce(&mut ManifestState) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        self.with_core_mut(move |core| core.with_runtime_manifest_write(f))
    }

    pub(crate) fn rebuild_secondary_index_catalog(&self) -> Result<(), EngineError> {
        self.with_core_mut(|core| core.rebuild_secondary_index_catalog())
    }

    pub(crate) fn seed_secondary_index_entry(
        &self,
        entry: &SecondaryIndexManifestEntry,
    ) -> Result<(), EngineError> {
        self.with_core_mut(|core| core.seed_secondary_index_entry(entry))
    }

    pub(crate) fn shutdown_secondary_index_worker(&self) {
        let Ok(_guard) = self.runtime.admit_mutating_operation() else {
            return;
        };

        let handle = {
            let mut core_guard = self.runtime.core.lock().unwrap();
            let Some(core) = core_guard.as_mut() else {
                return;
            };
            let Some(mut bg) = core.secondary_index_bg.take() else {
                return;
            };
            bg.cancel.store(true, Ordering::Relaxed);
            let _ = bg.job_tx.send(SecondaryIndexJob::Shutdown);
            bg.handle.take()
        };

        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    pub(crate) fn validate_property_range_bounds(
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        after: Option<&PropertyRangeCursor>,
    ) -> Result<SecondaryIndexRangeDomain, EngineError> {
        ReadView::validate_property_range_bounds(lower, upper, after)
    }

    pub(crate) fn compact_after_n_flushes_for_test(&self) -> u32 {
        self.with_core_ref(|core| Ok(core.compact_after_n_flushes))
            .unwrap_or(0)
    }

    pub(crate) fn ingest_saved_compact_after_n_flushes_for_test(&self) -> Option<u32> {
        self.with_core_ref(|core| Ok(core.ingest_saved_compact_after_n_flushes))
            .unwrap_or(None)
    }
}

/// The mutable shared engine core.
struct EngineCore {
    db_dir: PathBuf,
    manifest: ManifestState,
    /// In Immediate mode, the WAL writer is stored here directly.
    /// In GroupCommit mode, it's inside `wal_state`.
    wal_writer_immediate: Option<WalWriter>,
    /// Shared WAL state for GroupCommit mode. Also used in Immediate mode
    /// as None (the writer lives in wal_writer_immediate instead).
    wal_state: Option<Arc<(Mutex<WalSyncState>, Condvar)>>,
    /// Background sync thread handle (GroupCommit mode only).
    sync_thread: Option<JoinHandle<()>>,
    memtable: Arc<Memtable>,
    /// Open segment readers, ordered newest-first for read merging.
    segments: Vec<Arc<SegmentReader>>,
    /// Running node ID counter. Monotonically increasing.
    next_node_id: u64,
    /// Running edge ID counter. Monotonically increasing.
    next_edge_id: u64,
    /// Whether to enforce edge uniqueness on (from, to, type_id).
    edge_uniqueness: bool,
    /// Memtable size threshold for auto-flush (bytes). 0 = manual only.
    flush_threshold: usize,
    /// Next segment ID to allocate.
    next_segment_id: u64,
    /// Auto-compact after this many flushes. 0 = disabled.
    compact_after_n_flushes: u32,
    /// Saved auto-compaction threshold while ingest mode is active.
    ingest_saved_compact_after_n_flushes: Option<u32>,
    /// Flushes since last compaction (manual or auto).
    flush_count_since_last_compact: u32,
    /// Guard against re-entrant compaction (auto-compact during compact's flush).
    compacting: bool,
    /// The WAL sync mode this engine was opened with.
    wal_sync_mode: WalSyncMode,
    /// Hard cap on memtable size in bytes. Writes trigger a flush when exceeded. 0 = disabled.
    memtable_hard_cap: usize,
    /// Maximum number of immutable memtables before writers block. 0 = disabled.
    max_immutable_memtables: usize,
    /// In-progress background compaction, if any.
    bg_compact: Option<BgCompactHandle>,
    /// Timestamp of the last completed compaction (manual or background).
    last_compaction_ms: Option<i64>,
    /// Published active-WAL/memtable degree delta overlay.
    active_degree_overlay: Arc<DegreeOverlaySnapshot>,
    /// Monotonic engine sequence counter. Incremented per WAL op.
    /// Assigned to records and tombstones via `last_write_seq`.
    engine_seq: u64,
    /// Monotonic shared view of the next node ID. Used by publisher-thread
    /// manifest writes so counters can never regress behind published segments.
    next_node_id_seen: Arc<AtomicU64>,
    /// Monotonic shared view of the next edge ID.
    next_edge_id_seen: Arc<AtomicU64>,
    /// Monotonic shared view of the latest durable engine_seq.
    engine_seq_seen: Arc<AtomicU64>,
    /// Serialize all manifest writes across engine, flush publisher, and compaction.
    manifest_write_lock: Arc<Mutex<()>>,
    /// Frozen memtable epochs awaiting or undergoing flush, newest-first.
    /// Single source of truth: entries stay here until the output segment is
    /// published, keeping frozen data visible to reads throughout the flush.
    immutable_epochs: Vec<ImmutableEpoch>,
    /// Cached sum of all immutable epoch memtable sizes. Updated on
    /// freeze (add) and apply_bg_flush_result (remove) to avoid
    /// iterating immutable_epochs on every write for backpressure checks.
    immutable_bytes_total: usize,
    /// Active WAL generation ID.
    active_wal_generation_id: u64,
    /// Handle for the persistent background flush worker thread.
    bg_flush: Option<BgFlushHandle>,
    /// Runtime declaration catalog keyed by `(type_id, prop_key, kind)`.
    secondary_index_catalog: Arc<RwLock<SecondaryIndexCatalog>>,
    /// Runtime declaration entries kept in sync with background state changes.
    secondary_index_entries: Arc<RwLock<SecondaryIndexEntries>>,
    /// Cached published read-visible source bundle reused by ordinary writes.
    published_read_sources: Option<Arc<PublishedReadSources>>,
    /// Monotonic generation for rebuilt read-source bundles and their stats rollup.
    published_read_sources_generation: u64,
    /// Counts read-source bundle rebuilds so tests can catch accidental helper-read rebuilds.
    #[cfg(test)]
    published_read_source_builds: AtomicUsize,
    /// Segment dirs whose cleanup is retried after published snapshots release
    /// mmap-backed readers. This matters on Windows, where mapped files cannot
    /// be removed while a reader handle is still alive.
    deferred_segment_dir_cleanup: Vec<PathBuf>,
    /// Handle for the background secondary-index lifecycle worker.
    secondary_index_bg: Option<SecondaryIndexBgHandle>,
    /// Weak back-reference used by the background secondary-index worker to
    /// republish read-visible routing state after out-of-band state changes.
    runtime: Option<std::sync::Weak<DbRuntime>>,
    /// Oldest unresolved flush pipeline error. Cleared when the same epoch
    /// later publishes and is adopted successfully.
    flush_pipeline_error: Option<FlushPipelineError>,
    /// Whether the current sticky flush error has already been surfaced once.
    flush_pipeline_error_reported: bool,
    /// One-shot pause hook for the next enqueued flush (test only).
    /// Wrapped in Mutex so DatabaseEngine stays Sync for scoped threads.
    #[cfg(test)]
    flush_pause: Mutex<Option<FlushPauseHook>>,
    #[cfg(test)]
    flush_publish_pause: Arc<Mutex<Option<FlushPublishPauseHook>>>,
    #[cfg(test)]
    bg_compact_pause: Arc<Mutex<Option<BgCompactPauseHook>>>,
    #[cfg(test)]
    secondary_index_build_pause: Arc<Mutex<Option<SecondaryIndexBuildPauseHook>>>,
    /// One-shot failure injection flag (test only).
    #[cfg(test)]
    flush_force_error: bool,
}

/// Captured pre-mutation edge state for degree cache updates.
struct OldEdgeInfo {
    from: u64,
    to: u64,
    weight: f32,
    created_at: i64,
    updated_at: i64,
    valid_from: i64,
    valid_to: i64,
}

#[derive(Clone, Copy)]
struct EdgeCore {
    from: u64,
    to: u64,
    created_at: i64,
    updated_at: i64,
    weight: f32,
    valid_from: i64,
    valid_to: i64,
}

impl OldEdgeInfo {
    fn from_core(edge: EdgeCore) -> Self {
        Self {
            from: edge.from,
            to: edge.to,
            weight: edge.weight,
            created_at: edge.created_at,
            updated_at: edge.updated_at,
            valid_from: edge.valid_from,
            valid_to: edge.valid_to,
        }
    }
}

/// Handle for an in-progress background compaction thread.
struct BgCompactHandle {
    handle: JoinHandle<Result<BgCompactResult, EngineError>>,
    /// Shared cancel flag. Set to true to request early termination.
    cancel: Arc<AtomicBool>,
    /// Durable completion signal used by lifecycle polling before join.
    completed: Arc<AtomicBool>,
}

struct BgCompactCompletionSignal {
    completed: Arc<AtomicBool>,
    runtime: Option<Weak<DbRuntime>>,
}

impl Drop for BgCompactCompletionSignal {
    fn drop(&mut self) {
        self.completed.store(true, Ordering::Release);
        if let Some(runtime) = self.runtime.as_ref().and_then(Weak::upgrade) {
            runtime.notify_lifecycle_work();
        }
    }
}

/// Result returned by the background compaction worker thread.
struct BgCompactResult {
    seg_info: SegmentInfo,
    reader: SegmentReader,
    old_seg_dirs: Vec<PathBuf>,
    stats: CompactionStats,
    input_segment_ids: NodeIdSet,
    maintained_equality_index_ids: NodeIdSet,
    maintained_range_index_ids: NodeIdSet,
    secondary_index_report: SecondaryIndexMaintenanceReport,
}

/// Handle for the split async flush pipeline.
struct BgFlushHandle {
    /// Send frozen memtables + metadata to the build worker.
    work_tx: std::sync::mpsc::Sender<BgFlushWork>,
    /// Receive completed adoption or failure events. Mutex provides Sync.
    event_rx: Mutex<std::sync::mpsc::Receiver<BgFlushEvent>>,
    /// Build worker thread handle.
    build_handle: Option<JoinHandle<()>>,
    /// Publisher worker thread handle.
    publish_handle: Option<JoinHandle<()>>,
    /// Shared cancel flag for the whole pipeline.
    cancel: Arc<AtomicBool>,
    /// Number of completion events enqueued by the pipeline.
    events_ready: Arc<AtomicUsize>,
    /// Number of completion events consumed by the engine thread.
    events_applied: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SecondaryIndexLookupKey {
    type_id: u32,
    prop_key: String,
    kind: SecondaryIndexKind,
}

enum SecondaryIndexJob {
    Build { index_id: u64 },
    DropCleanup { index_id: u64 },
    Shutdown,
}

struct SecondaryIndexBgHandle {
    job_tx: std::sync::mpsc::Sender<SecondaryIndexJob>,
    handle: Option<JoinHandle<()>>,
    cancel: Arc<AtomicBool>,
}

#[cfg(test)]
#[derive(Default)]
struct PublishCounters {
    skipped: AtomicUsize,
    snapshot_only: AtomicUsize,
    rebuild_sources: AtomicUsize,
    source_rebuilds: AtomicUsize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PublishCounterSnapshot {
    pub skipped: usize,
    pub snapshot_only: usize,
    pub rebuild_sources: usize,
    pub source_rebuilds: usize,
}

#[cfg(test)]
#[derive(Default)]
struct PropertyQueryRouteCounters {
    equality_scan_fallback: AtomicUsize,
    equality_index_lookup: AtomicUsize,
    range_scan_fallback: AtomicUsize,
    range_index_lookup: AtomicUsize,
}

#[cfg(test)]
#[derive(Default)]
struct DegreeQueryRouteCounters {
    fast_path: AtomicUsize,
    walk_path: AtomicUsize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PropertyQueryRouteSnapshot {
    pub equality_scan_fallback: usize,
    pub equality_index_lookup: usize,
    pub range_scan_fallback: usize,
    pub range_index_lookup: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DegreeQueryRouteSnapshot {
    pub fast_path: usize,
    pub walk_path: usize,
}

/// Work item sent to the background build worker.
struct BgFlushWork {
    epoch_id: u64,
    frozen: Arc<Memtable>,
    degree_overlay: Arc<DegreeOverlaySnapshot>,
    seg_id: u64,
    tmp_dir: PathBuf,
    final_dir: PathBuf,
    dense_config: Option<DenseVectorConfig>,
    wal_gen_id: u64,
    #[cfg(test)]
    pause: Option<FlushPauseHook>,
    #[cfg(test)]
    force_write_error: bool,
}

/// Segment built durably on disk and ready for publisher-thread manifest work.
struct BuiltFlushResult {
    epoch_id: u64,
    wal_gen_to_retire: u64,
    seg_info: SegmentInfo,
    seg_id: u64,
    final_dir: PathBuf,
    dense_config: Option<DenseVectorConfig>,
    maintained_equality_index_ids: NodeIdSet,
    maintained_range_index_ids: NodeIdSet,
}

/// Cheap foreground-only adoption payload. No disk I/O remains at this stage.
struct PublishedFlushAdoption {
    epoch_id: u64,
    wal_gen_to_retire: u64,
    seg_info: SegmentInfo,
    reader: SegmentReader,
    rebuild_equality_index_ids: Vec<u64>,
    rebuild_range_index_ids: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushPipelineStage {
    Build,
    PublishOpenReader,
    PublishManifest,
}

#[derive(Debug, Clone)]
struct FlushPipelineError {
    epoch_id: u64,
    wal_generation_id: u64,
    stage: FlushPipelineStage,
    message: String,
}

impl FlushPipelineError {
    fn to_engine_error(&self) -> EngineError {
        EngineError::InvalidOperation(format!(
            "bg flush {:?} failed for epoch {} wal {}: {}",
            self.stage, self.epoch_id, self.wal_generation_id, self.message
        ))
    }
}

#[allow(clippy::large_enum_variant)]
enum BgFlushEvent {
    Adopt(PublishedFlushAdoption),
    Failed(FlushPipelineError),
}

enum BackpressureFlushAction {
    Ready,
    Wait,
}

struct RuntimeFlushDrainResult {
    progressed: bool,
    publish_impact: PublishImpact,
    completed_flushes: Vec<(u64, SegmentInfo)>,
}

/// A frozen memtable epoch awaiting (or undergoing) background flush.
/// Stays visible to reads until the output segment is published and the
/// epoch is retired. Single source of truth for frozen-memtable lifecycle.
#[derive(Clone)]
pub(crate) struct ImmutableEpoch {
    /// Logical epoch identifier. Currently equal to `wal_generation_id` but
    /// kept separate so epoch allocation can diverge from WAL generations
    /// in the future without a data model change.
    pub(crate) epoch_id: u64,
    /// WAL generation that contains this epoch's data. Used to retire the
    /// WAL file after the segment is published.
    pub(crate) wal_generation_id: u64,
    pub(crate) memtable: Arc<Memtable>,
    pub(crate) degree_overlay: Arc<DegreeOverlaySnapshot>,
    pub(crate) in_flight: bool,
}

/// One-shot pause token consumed by exactly one BgFlushWork item.
/// Worker signals `ready_tx` when paused, then blocks on `release_rx`.
#[cfg(test)]
struct FlushPauseHook {
    ready_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct FlushPublishPauseHook {
    ready_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct BgCompactPauseHook {
    ready_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct SecondaryIndexBuildPauseHook {
    ready_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct RuntimePublishPauseHook {
    ready_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct RuntimeReadPauseHook {
    ready_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: std::sync::mpsc::Receiver<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionPath {
    FastMerge,
    UnifiedV3,
}

impl EngineCore {
    /// Open or create a database at the given directory path.
    pub fn open(path: &Path, options: &DbOptions) -> Result<Self, EngineError> {
        // Create directory if needed
        if !path.exists() {
            if options.create_if_missing {
                std::fs::create_dir_all(path)?;
            } else {
                return Err(EngineError::DatabaseNotFound(format!("{}", path.display())));
            }
        }

        // Load or create manifest
        let loaded_manifest = load_manifest(path)?;
        let created_manifest = loaded_manifest.is_none();
        let mut manifest = match loaded_manifest {
            Some(m) => m,
            None => default_manifest(),
        };
        let manifest_dirty = reconcile_dense_vector_manifest(&mut manifest, options)?
            || normalize_secondary_index_manifest(&mut manifest)?;
        if created_manifest || manifest_dirty {
            write_manifest(path, &manifest)?;
        };

        // --- WAL generation migration ---
        // If this is an old manifest (next_wal_generation_id == 0) and data.wal
        // exists, migrate to WAL generation format by renaming data.wal → wal_0.wal.
        let legacy_wal_path = path.join("data.wal");
        let gen0_path = wal_generation_path(path, 0);
        if manifest.next_wal_generation_id == 0
            && manifest.active_wal_generation_id == 0
            && manifest.pending_flush_epochs.is_empty()
            && legacy_wal_path.exists()
            && !gen0_path.exists()
        {
            std::fs::rename(&legacy_wal_path, &gen0_path)?;
        }

        // Ensure next_wal_generation_id is at least active + 1 and above any
        // pending epoch generation IDs.
        let mut max_gen = manifest.active_wal_generation_id;
        for epoch in &manifest.pending_flush_epochs {
            max_gen = max_gen.max(epoch.wal_generation_id);
        }
        manifest.next_wal_generation_id = manifest.next_wal_generation_id.max(max_gen + 1);

        // Load existing segments (newest-first for read merging).
        // Only segments listed in the manifest are opened. Orphan segment
        // directories (from a crash between segment write and manifest update)
        // are intentionally NOT loaded. Their data may be partial or corrupt.
        // Their IDs are still accounted for in next_segment_id below.
        //
        // If a PublishedPendingRetire epoch references a segment, that segment
        // must exist and be readable on reopen. Otherwise we'd lose both the
        // segment and the retained WAL recovery path.
        let mut segments = Vec::new();
        for seg_info in manifest.segments.iter().rev() {
            let seg_path = segment_dir(path, seg_info.id);
            if seg_path.exists() {
                let reader =
                    SegmentReader::open(&seg_path, seg_info.id, manifest.dense_vector.as_ref())?;
                segments.push(Arc::new(reader));
            } else if manifest.pending_flush_epochs.iter().any(|e| {
                e.state == FlushEpochState::PublishedPendingRetire
                    && e.segment_id == Some(seg_info.id)
            }) {
                return Err(EngineError::InvalidOperation(format!(
                    "manifest references published segment {} for pending flush recovery, but {} is missing",
                    seg_info.id,
                    seg_path.display()
                )));
            }
        }

        // PublishedPendingRetire epochs: their segment is now verified live, so
        // clean up the retained WAL generation file and remove the epoch entry.
        let live_segment_ids: NodeIdSet = manifest.segments.iter().map(|s| s.id).collect();
        let mut published_gen_ids = Vec::new();
        for epoch in manifest
            .pending_flush_epochs
            .iter()
            .filter(|e| e.state == FlushEpochState::PublishedPendingRetire)
        {
            let seg_id = epoch.segment_id.ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "PublishedPendingRetire epoch {} is missing segment_id",
                    epoch.epoch_id
                ))
            })?;
            if !live_segment_ids.contains(&seg_id) {
                return Err(EngineError::InvalidOperation(format!(
                    "PublishedPendingRetire epoch {} references segment {} that is not present in the manifest",
                    epoch.epoch_id, seg_id
                )));
            }
            published_gen_ids.push(epoch.wal_generation_id);
        }
        if !published_gen_ids.is_empty() {
            manifest
                .pending_flush_epochs
                .retain(|e| e.state != FlushEpochState::PublishedPendingRetire);
            for gen_id in &published_gen_ids {
                let _ = remove_wal_generation(path, *gen_id);
            }
            write_manifest(path, &manifest)?;
        }

        // --- Replay WAL generations ---
        // Frozen epochs are replayed into separate immutable memtables so their
        // WAL files and manifest entries are preserved. Published degree overlays
        // are replayed from the same WAL records, using already-open segments to
        // recover old edge state for updates and deletes.
        let mut frozen_epochs: Vec<(u64, u64)> = manifest
            .pending_flush_epochs
            .iter()
            .filter(|e| e.state == FlushEpochState::FrozenPendingFlush)
            .map(|e| (e.epoch_id, e.wal_generation_id))
            .collect();
        frozen_epochs.sort_unstable_by_key(|&(_, gen)| gen);

        let mut engine_seq = manifest.next_engine_seq;
        let mut immutable_epochs_on_open: Vec<ImmutableEpoch> = Vec::new();
        let mut immutable_bytes_on_open: usize = 0;

        for &(epoch_id, wal_gen_id) in &frozen_epochs {
            let (frozen_mt, degree_overlay) = replay_wal_generation_to_memtable_and_overlay(
                path,
                wal_gen_id,
                manifest.dense_vector.as_ref(),
                &mut engine_seq,
                &immutable_epochs_on_open,
                &segments,
            )?;
            immutable_bytes_on_open += frozen_mt.estimated_size();
            // Newest-first: insert at front so older epochs are at the back.
            immutable_epochs_on_open.insert(
                0,
                ImmutableEpoch {
                    epoch_id,
                    wal_generation_id: wal_gen_id,
                    memtable: Arc::new(frozen_mt),
                    degree_overlay,
                    in_flight: false,
                },
            );
        }

        // Replay active WAL generation into the active memtable and overlay.
        // Use the persisted engine_seq from each WAL record (V3 format).
        let (memtable, active_degree_overlay) = replay_wal_generation_to_memtable_and_overlay(
            path,
            manifest.active_wal_generation_id,
            manifest.dense_vector.as_ref(),
            &mut engine_seq,
            &immutable_epochs_on_open,
            &segments,
        )?;

        // Compute next IDs from active memtable + immutable epochs + manifest.
        let mut max_node_id = manifest
            .next_node_id
            .max(memtable.max_node_id().saturating_add(1));
        let mut max_edge_id = manifest
            .next_edge_id
            .max(memtable.max_edge_id().saturating_add(1));
        for epoch in &immutable_epochs_on_open {
            max_node_id = max_node_id.max(epoch.memtable.max_node_id().saturating_add(1));
            max_edge_id = max_edge_id.max(epoch.memtable.max_edge_id().saturating_add(1));
        }
        let next_node_id = max_node_id;
        let next_edge_id = max_edge_id;

        // Compute next_segment_id from both manifest AND filesystem.
        // Orphan segments (from a crash between segment write and manifest update)
        // must not have their IDs reused.
        let manifest_max = manifest.segments.iter().map(|s| s.id).max().unwrap_or(0);
        let fs_max = scan_max_segment_id(path);
        let next_segment_id = manifest_max.max(fs_max).saturating_add(1);

        // Clean up orphan segment directories: any seg_XXXX on disk that the
        // manifest doesn't reference is left over from a crash (between segment
        // write and manifest update, or between bg compact output and apply).
        // Safe to delete. The manifest is the source of truth.
        cleanup_orphan_segments(path, &manifest);
        cleanup_orphan_wal_files(path, &manifest);

        // Open WAL writer for the active generation
        let wal_writer = WalWriter::open_generation(path, manifest.active_wal_generation_id)?;

        // Validate GroupCommit parameters
        if let WalSyncMode::GroupCommit {
            interval_ms,
            soft_trigger_bytes,
            hard_cap_bytes,
        } = &options.wal_sync_mode
        {
            if *interval_ms == 0 {
                return Err(EngineError::InvalidOperation(
                    "GroupCommit interval_ms must be > 0".into(),
                ));
            }
            if *soft_trigger_bytes == 0 {
                return Err(EngineError::InvalidOperation(
                    "GroupCommit soft_trigger_bytes must be > 0".into(),
                ));
            }
            if *hard_cap_bytes == 0 {
                return Err(EngineError::InvalidOperation(
                    "GroupCommit hard_cap_bytes must be > 0".into(),
                ));
            }
            if *hard_cap_bytes <= *soft_trigger_bytes {
                return Err(EngineError::InvalidOperation(format!(
                    "GroupCommit hard_cap_bytes ({}) must be > soft_trigger_bytes ({})",
                    hard_cap_bytes, soft_trigger_bytes
                )));
            }
        }

        // Initialize WAL sync based on mode
        let wal_sync_mode = options.wal_sync_mode.clone();
        let (wal_writer_immediate, wal_state, sync_thread) = match &wal_sync_mode {
            WalSyncMode::Immediate => (Some(wal_writer), None, None),
            WalSyncMode::GroupCommit { interval_ms, .. } => {
                let state = WalSyncState {
                    wal_writer,
                    buffered_bytes: 0,
                    shutdown: false,
                    sync_error_count: 0,
                    poisoned: None,
                };
                let arc = Arc::new((Mutex::new(state), Condvar::new()));
                let arc_clone = Arc::clone(&arc);
                let interval = std::time::Duration::from_millis(*interval_ms);
                let handle = std::thread::spawn(move || {
                    sync_thread_loop(arc_clone, interval);
                });
                (None, Some(arc), Some(handle))
            }
        };

        let active_wal_generation_id = manifest.active_wal_generation_id;
        let next_node_id_seen = Arc::new(AtomicU64::new(next_node_id));
        let next_edge_id_seen = Arc::new(AtomicU64::new(next_edge_id));
        let engine_seq_seen = Arc::new(AtomicU64::new(engine_seq));
        let manifest_write_lock = Arc::new(Mutex::new(()));
        let mut engine = EngineCore {
            db_dir: path.to_path_buf(),
            manifest,
            wal_writer_immediate,
            wal_state,
            sync_thread,
            memtable: Arc::new(memtable),
            segments,
            next_node_id,
            next_edge_id,
            edge_uniqueness: options.edge_uniqueness,
            flush_threshold: options.memtable_flush_threshold,
            next_segment_id,
            compact_after_n_flushes: options.compact_after_n_flushes,
            ingest_saved_compact_after_n_flushes: None,
            flush_count_since_last_compact: 0,
            compacting: false,
            wal_sync_mode,
            memtable_hard_cap: options.memtable_hard_cap_bytes,
            max_immutable_memtables: options.max_immutable_memtables,
            bg_compact: None,
            last_compaction_ms: None,
            active_degree_overlay,
            engine_seq,
            next_node_id_seen,
            next_edge_id_seen,
            engine_seq_seen,
            manifest_write_lock,
            immutable_epochs: immutable_epochs_on_open,
            immutable_bytes_total: immutable_bytes_on_open,
            active_wal_generation_id,
            bg_flush: None,
            secondary_index_catalog: Arc::new(RwLock::new(HashMap::new())),
            secondary_index_entries: Arc::new(RwLock::new(Vec::new())),
            published_read_sources: None,
            published_read_sources_generation: 0,
            #[cfg(test)]
            published_read_source_builds: AtomicUsize::new(0),
            deferred_segment_dir_cleanup: Vec::new(),
            secondary_index_bg: None,
            runtime: None,
            flush_pipeline_error: None,
            flush_pipeline_error_reported: false,
            #[cfg(test)]
            flush_pause: Mutex::new(None),
            #[cfg(test)]
            flush_publish_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            bg_compact_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            secondary_index_build_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            flush_force_error: false,
        };

        engine.recover_secondary_index_states_on_open()?;
        engine.rebuild_secondary_index_catalog()?;
        engine.seed_secondary_indexes_from_manifest()?;
        engine.warm_declared_index_runtime_coverage_for_current_readers();
        engine.rebuild_published_read_sources();

        Ok(engine)
    }

    /// Close the database cleanly. Freezes the active memtable (if non-empty),
    /// flushes all pending immutable memtables to segments, waits for
    /// background compaction, and writes the final manifest.
    /// After close() returns, no immutable memtables or retained WAL
    /// generations remain.
    pub fn close(mut self) -> Result<(), EngineError> {
        self.try_apply_all_bg_flushes();
        let mut first_error: Option<EngineError> = None;

        if !self.memtable.is_empty() || !self.immutable_epochs.is_empty() {
            if let Err(e) = self.flush() {
                first_error = Some(e);
            }
        } else {
            self.drain_bg_flush();
        }

        self.wait_for_bg_compact();
        for event in self.shutdown_bg_flush() {
            let _ = self.process_bg_flush_event(event);
        }
        self.shutdown_secondary_index_worker();
        let close_result = self.close_inner();
        match (first_error, close_result) {
            (Some(err), _) => Err(err),
            (None, Err(err)) => Err(err),
            (None, Ok(())) => self.current_flush_pipeline_error().map_or(Ok(()), Err),
        }
    }

    /// Close the database, cancelling any in-progress background compaction
    /// instead of waiting for it to finish. Syncs the active WAL and persists
    /// manifest with retained WAL generations. Use this for fast shutdown when
    /// you don't need the bg compaction result.
    pub fn close_fast(mut self) -> Result<(), EngineError> {
        self.cancel_bg_compact();
        self.try_apply_all_bg_flushes();
        for event in self.shutdown_bg_flush() {
            let _ = self.process_bg_flush_event(event);
        }
        self.shutdown_secondary_index_worker();
        self.close_inner()?;
        self.current_flush_pipeline_error().map_or(Ok(()), Err)
    }

    /// Shared close logic: sync WAL, write manifest.
    fn close_inner(&mut self) -> Result<(), EngineError> {
        self.retry_deferred_segment_cleanup();
        match &self.wal_sync_mode {
            WalSyncMode::Immediate => {
                if let Some(ref mut w) = self.wal_writer_immediate {
                    w.sync()?;
                }
            }
            WalSyncMode::GroupCommit { .. } => {
                if let Some(ref wal_state) = self.wal_state {
                    shutdown_sync_thread(wal_state, &mut self.sync_thread)?;
                }
            }
        }
        let active_wal_generation_id = self.active_wal_generation_id;
        self.with_runtime_manifest_write(|manifest| {
            manifest.active_wal_generation_id = active_wal_generation_id;
            Ok(())
        })
    }

    fn active_memtable(&self) -> &Memtable {
        &self.memtable
    }

    fn build_read_manifest_state(&self) -> ReadManifestState {
        ReadManifestState {
            prune_policies: self.manifest.prune_policies.clone(),
            dense_vector: self.manifest.dense_vector.clone(),
        }
    }

    fn warm_declared_index_runtime_coverage_for_reader(&self, reader: &SegmentReader) {
        let entries = self.secondary_index_entries_snapshot();
        for entry in &entries {
            reader.warm_declared_index_runtime_coverage(entry);
        }
    }

    fn warm_declared_index_runtime_coverage_for_current_readers(&self) {
        let entries = self.secondary_index_entries_snapshot();
        for segment in &self.segments {
            for entry in &entries {
                segment.warm_declared_index_runtime_coverage(entry);
            }
        }
    }

    fn build_published_read_sources(&self) -> Arc<PublishedReadSources> {
        #[cfg(test)]
        self.published_read_source_builds
            .fetch_add(1, Ordering::Relaxed);

        let secondary_index_entries = self.secondary_index_entries_snapshot();
        let secondary_index_catalog = build_secondary_index_catalog(&secondary_index_entries)
            .expect("secondary index runtime state must stay internally consistent");
        let declared_index_runtime_coverage = Arc::new(DeclaredIndexRuntimeCoverage::from_readers(
            &self.segments,
            &secondary_index_entries,
        ));
        let planner_stats = Arc::new(PlannerStatsView::build_from_readers(
            self.published_read_sources_generation,
            &self.segments,
            &secondary_index_entries,
            declared_index_runtime_coverage.as_ref(),
        ));
        Arc::new(PublishedReadSources {
            manifest: self.build_read_manifest_state(),
            memtable: Arc::clone(&self.memtable),
            immutable_epochs: self.immutable_epochs.clone(),
            segments: self.segments.iter().map(Arc::clone).collect(),
            secondary_index_catalog,
            secondary_index_entries,
            declared_index_runtime_coverage,
            planner_stats,
            #[cfg(test)]
            planning_probe_counters: QueryPlanningProbeCounters::default(),
            #[cfg(test)]
            query_execution_counters: QueryExecutionCounters::default(),
        })
    }

    fn rebuild_published_read_sources(&mut self) {
        self.published_read_sources_generation =
            self.published_read_sources_generation.saturating_add(1);
        self.published_read_sources = Some(self.build_published_read_sources());
    }

    fn current_published_read_sources(&self) -> Arc<PublishedReadSources> {
        Arc::clone(
            self.published_read_sources
                .as_ref()
                .expect("published read sources must be initialized before publish"),
        )
    }

    fn defer_segment_dir_cleanup(&mut self, dirs: impl IntoIterator<Item = PathBuf>) {
        self.deferred_segment_dir_cleanup.extend(dirs);
    }

    fn retry_deferred_segment_cleanup(&mut self) {
        if self.deferred_segment_dir_cleanup.is_empty() {
            return;
        }

        let mut still_pending = Vec::new();
        for dir in self.deferred_segment_dir_cleanup.drain(..) {
            if dir.exists() && std::fs::remove_dir_all(&dir).is_err() {
                still_pending.push(dir);
            }
        }
        self.deferred_segment_dir_cleanup = still_pending;
    }

    fn read_view(&self) -> ReadView {
        ReadView::from_published_sources(
            self.current_published_read_sources(),
            self.engine_seq,
            Arc::clone(&self.active_degree_overlay),
        )
    }

    fn published_read_state(&self) -> PublishedReadState {
        PublishedReadState {
            view: Arc::new(ReadView::from_published_sources(
                self.current_published_read_sources(),
                self.engine_seq,
                Arc::clone(&self.active_degree_overlay),
            )),
            edge_uniqueness: self.edge_uniqueness,
            #[cfg(test)]
            engine_seq: self.engine_seq,
            #[cfg(test)]
            active_wal_generation_id: self.active_wal_generation_id,
        }
    }

    fn secondary_index_entries_snapshot(&self) -> SecondaryIndexEntries {
        self.secondary_index_entries.read().unwrap().clone()
    }

    fn rebuild_secondary_index_catalog(&mut self) -> Result<(), EngineError> {
        sync_secondary_index_runtime_state(
            &self.secondary_index_catalog,
            &self.secondary_index_entries,
            &self.manifest.secondary_indexes,
        )
    }

    fn recover_secondary_index_states_on_open(&mut self) -> Result<(), EngineError> {
        let mut dirty = false;
        for entry in &mut self.manifest.secondary_indexes {
            if entry.state != SecondaryIndexState::Ready {
                continue;
            }

            for segment in &self.segments {
                let validation = match entry.kind {
                    SecondaryIndexKind::Equality => {
                        segment.validate_secondary_eq_sidecar(entry.index_id)
                    }
                    SecondaryIndexKind::Range { .. } => {
                        segment.validate_secondary_range_sidecar(entry.index_id)
                    }
                };
                match validation {
                    Ok(true) => continue,
                    Ok(false) => {
                        entry.state = SecondaryIndexState::Building;
                        entry.last_error = None;
                        dirty = true;
                        break;
                    }
                    Err(error) => {
                        entry.state = SecondaryIndexState::Failed;
                        entry.last_error = Some(error.to_string());
                        dirty = true;
                        break;
                    }
                }
            }
        }

        if dirty {
            write_manifest(&self.db_dir, &self.manifest)?;
        }
        Ok(())
    }

    fn seed_secondary_indexes_from_manifest(&mut self) -> Result<(), EngineError> {
        let entries = self.secondary_index_entries_snapshot();
        for entry in &entries {
            self.active_memtable().register_secondary_index(entry);
        }
        for epoch in &self.immutable_epochs {
            let memtable = epoch.memtable.as_ref();
            for entry in &entries {
                memtable.register_secondary_index(entry);
            }
        }
        self.refresh_immutable_bytes_total();
        Ok(())
    }

    fn seed_secondary_index_entry(
        &mut self,
        entry: &SecondaryIndexManifestEntry,
    ) -> Result<(), EngineError> {
        self.active_memtable().register_secondary_index(entry);
        for epoch in &self.immutable_epochs {
            let memtable = epoch.memtable.as_ref();
            memtable.register_secondary_index(entry);
        }
        self.refresh_immutable_bytes_total();
        Ok(())
    }

    fn remove_secondary_index_entry_from_memtables(
        &mut self,
        index_id: u64,
    ) -> Result<(), EngineError> {
        self.active_memtable().unregister_secondary_index(index_id);
        for epoch in &self.immutable_epochs {
            let memtable = epoch.memtable.as_ref();
            memtable.unregister_secondary_index(index_id);
        }
        self.refresh_immutable_bytes_total();
        Ok(())
    }

    fn ensure_secondary_index_worker(&mut self) {
        if self.secondary_index_bg.is_some() {
            return;
        }
        let (job_tx, job_rx) = std::sync::mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = Arc::clone(&cancel);
        let runtime = self.runtime.clone();
        let db_dir = self.db_dir.clone();
        let manifest_write_lock = Arc::clone(&self.manifest_write_lock);
        let catalog_lock = Arc::clone(&self.secondary_index_catalog);
        let entries_lock = Arc::clone(&self.secondary_index_entries);
        let next_node_id_seen = Arc::clone(&self.next_node_id_seen);
        let next_edge_id_seen = Arc::clone(&self.next_edge_id_seen);
        let engine_seq_seen = Arc::clone(&self.engine_seq_seen);
        #[cfg(test)]
        let build_pause = Arc::clone(&self.secondary_index_build_pause);
        let handle = std::thread::spawn(move || {
            bg_secondary_index_worker(
                job_rx,
                cancel_clone,
                runtime,
                db_dir,
                manifest_write_lock,
                catalog_lock,
                entries_lock,
                next_node_id_seen,
                next_edge_id_seen,
                engine_seq_seen,
                #[cfg(test)]
                build_pause,
            )
        });
        self.secondary_index_bg = Some(SecondaryIndexBgHandle {
            job_tx,
            handle: Some(handle),
            cancel,
        });
    }

    fn ensure_secondary_index_worker_if_needed(&mut self) {
        let has_secondary_declarations = self
            .secondary_index_entries_snapshot()
            .into_iter()
            .next()
            .is_some();
        if has_secondary_declarations {
            self.ensure_secondary_index_worker();
        }
    }

    fn enqueue_secondary_index_job(&mut self, job: SecondaryIndexJob) {
        self.ensure_secondary_index_worker();
        if let Some(bg) = &self.secondary_index_bg {
            let _ = bg.job_tx.send(job);
        }
    }

    fn schedule_building_secondary_indexes(&mut self) {
        let building_ids: Vec<u64> = self
            .secondary_index_entries_snapshot()
            .into_iter()
            .filter(|entry| entry.state == SecondaryIndexState::Building)
            .map(|entry| entry.index_id)
            .collect();
        for index_id in building_ids {
            self.enqueue_secondary_index_job(SecondaryIndexJob::Build { index_id });
        }
    }

    fn shutdown_secondary_index_worker(&mut self) {
        if let Some(mut bg) = self.secondary_index_bg.take() {
            bg.cancel.store(true, Ordering::Relaxed);
            let _ = bg.job_tx.send(SecondaryIndexJob::Shutdown);
            if let Some(handle) = bg.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn update_next_node_id_seen(&self) {
        self.next_node_id_seen
            .fetch_max(self.next_node_id, Ordering::Release);
    }

    fn update_next_edge_id_seen(&self) {
        self.next_edge_id_seen
            .fetch_max(self.next_edge_id, Ordering::Release);
    }

    fn refresh_immutable_bytes_total(&mut self) {
        self.immutable_bytes_total = self
            .immutable_epochs
            .iter()
            .map(|epoch| epoch.memtable.estimated_size())
            .sum();
    }

    fn update_engine_seq_seen(&self) {
        self.engine_seq_seen
            .fetch_max(self.engine_seq, Ordering::Release);
    }

    fn merge_runtime_manifest_counters(&self, manifest: &mut ManifestState) {
        merge_runtime_manifest_counters_from_shared(
            manifest,
            &self.next_node_id_seen,
            &self.next_edge_id_seen,
            &self.engine_seq_seen,
        );
    }

    fn load_current_manifest_for_write(&self) -> Result<ManifestState, EngineError> {
        let mut manifest = load_manifest_readonly(&self.db_dir)?
            .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
        manifest.next_wal_generation_id = manifest
            .next_wal_generation_id
            .max(self.manifest.next_wal_generation_id);
        manifest.active_wal_generation_id = manifest
            .active_wal_generation_id
            .max(self.manifest.active_wal_generation_id);
        Ok(manifest)
    }

    fn with_runtime_manifest_write<T>(
        &mut self,
        mutate: impl FnOnce(&mut ManifestState) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let _guard = self.manifest_write_lock.lock().unwrap();
        let mut manifest = self.load_current_manifest_for_write()?;
        let result = mutate(&mut manifest)?;
        self.merge_runtime_manifest_counters(&mut manifest);
        write_manifest(&self.db_dir, &manifest)?;
        self.manifest = manifest;
        Ok(result)
    }

    // --- Write path helpers ---

    /// Metadata-only logical edge lookup for degree-cache maintenance.
    /// Checks memtable first, then segments newest-to-oldest, respecting
    /// tombstones. Avoids full property decode on the write path.
    fn get_edge_core_for_cache(&self, id: u64) -> Result<Option<EdgeCore>, EngineError> {
        get_edge_core_from_sources(&self.memtable, &self.immutable_epochs, &self.segments, id)
    }

    fn add_degree_delta(deltas: &mut NodeIdMap<DegreeDelta>, node_id: u64, delta: DegreeDelta) {
        if delta.is_zero() {
            return;
        }
        let entry = deltas.entry(node_id).or_insert(DegreeDelta::ZERO);
        entry.add_assign_delta(delta);
        if entry.is_zero() {
            deltas.remove(&node_id);
        }
    }

    fn add_valid_edge_delta(
        deltas: &mut NodeIdMap<DegreeDelta>,
        from: u64,
        to: u64,
        weight: f32,
        add: bool,
    ) {
        let from_delta = if add {
            DegreeDelta::add_valid_edge(from, to, weight)
        } else {
            DegreeDelta::remove_valid_edge(from, to, weight)
        };
        Self::add_degree_delta(deltas, from, from_delta);
        if from != to {
            let to_delta = if add {
                DegreeDelta::add_valid_edge_incoming(weight)
            } else {
                DegreeDelta::remove_valid_edge_incoming(weight)
            };
            Self::add_degree_delta(deltas, to, to_delta);
        }
    }

    fn add_temporal_edge_delta(deltas: &mut NodeIdMap<DegreeDelta>, from: u64, to: u64, add: bool) {
        let delta = if add {
            DegreeDelta::add_temporal_marker()
        } else {
            DegreeDelta::remove_temporal_marker()
        };
        Self::add_degree_delta(deltas, from, delta);
        if from != to {
            Self::add_degree_delta(deltas, to, delta);
        }
    }

    fn collect_degree_delta_for_old_edge(deltas: &mut NodeIdMap<DegreeDelta>, old: OldEdgeInfo) {
        if is_edge_valid_at(old.valid_from, old.valid_to, old.updated_at) {
            Self::add_valid_edge_delta(deltas, old.from, old.to, old.weight, false);
        }
        if is_cache_bypass_edge(old.valid_from, old.valid_to, old.created_at) {
            Self::add_temporal_edge_delta(deltas, old.from, old.to, false);
        }
    }

    fn collect_degree_delta_for_new_edge(deltas: &mut NodeIdMap<DegreeDelta>, edge: &EdgeRecord) {
        if is_edge_valid_at(edge.valid_from, edge.valid_to, edge.updated_at) {
            Self::add_valid_edge_delta(deltas, edge.from, edge.to, edge.weight, true);
        }
        if is_cache_bypass_edge(edge.valid_from, edge.valid_to, edge.created_at) {
            Self::add_temporal_edge_delta(deltas, edge.from, edge.to, true);
        }
    }

    fn collect_degree_delta_for_op(
        op: &WalOp,
        old_edge: Option<OldEdgeInfo>,
        deltas: &mut NodeIdMap<DegreeDelta>,
    ) {
        match op {
            WalOp::UpsertEdge(edge) => {
                if let Some(old) = old_edge {
                    Self::collect_degree_delta_for_old_edge(deltas, old);
                }
                Self::collect_degree_delta_for_new_edge(deltas, edge);
            }
            WalOp::DeleteEdge { .. } => {
                if let Some(old) = old_edge {
                    Self::collect_degree_delta_for_old_edge(deltas, old);
                }
            }
            WalOp::DeleteNode { .. } | WalOp::UpsertNode(_) => {}
        }
    }

    fn apply_degree_deltas_to_active_overlay(&mut self, deltas: NodeIdMap<DegreeDelta>) {
        if deltas.is_empty() {
            return;
        }
        let mut edit = DegreeOverlayEdit::new(Arc::clone(&self.active_degree_overlay));
        for (node_id, delta) in deltas {
            edit.add_delta(node_id, delta);
        }
        self.active_degree_overlay = edit.finish();
    }

    fn edge_id_for_degree_op(op: &WalOp) -> Option<u64> {
        match op {
            WalOp::UpsertEdge(edge) => Some(edge.id),
            WalOp::DeleteEdge { id, .. } => Some(*id),
            WalOp::UpsertNode(_) | WalOp::DeleteNode { .. } => None,
        }
    }

    fn edge_core_after_op(op: &WalOp) -> Option<EdgeCore> {
        match op {
            WalOp::UpsertEdge(edge) => Some(EdgeCore {
                from: edge.from,
                to: edge.to,
                created_at: edge.created_at,
                updated_at: edge.updated_at,
                weight: edge.weight,
                valid_from: edge.valid_from,
                valid_to: edge.valid_to,
            }),
            WalOp::DeleteEdge { .. } | WalOp::DeleteNode { .. } | WalOp::UpsertNode(_) => None,
        }
    }

    fn capture_batch_edge_states(
        &self,
        ops: &[WalOp],
    ) -> Result<NodeIdMap<Option<EdgeCore>>, EngineError> {
        let mut states: NodeIdMap<Option<EdgeCore>> = NodeIdMap::default();
        for op in ops {
            let Some(edge_id) = Self::edge_id_for_degree_op(op) else {
                continue;
            };
            if states.contains_key(&edge_id) {
                continue;
            }
            states.insert(edge_id, self.get_edge_core_for_cache(edge_id)?);
        }
        Ok(states)
    }

    /// Internal helper that handles WAL append + memtable apply for both sync modes.
    fn append_and_apply_normalized(&mut self, ops: &[WalOp]) -> Result<(), EngineError> {
        // Assign sequences before WAL write so the WAL persists exact seqs.
        let base_seq = self.engine_seq;
        let sequenced: Vec<(u64, WalOp)> = ops
            .iter()
            .enumerate()
            .map(|(i, op)| (base_seq + 1 + i as u64, op.clone()))
            .collect();
        let mut edge_states = self.capture_batch_edge_states(ops)?;
        let mut degree_deltas: NodeIdMap<DegreeDelta> = NodeIdMap::default();
        self.wal_append(|w| w.append_batch(&sequenced))?;
        for (seq, op) in &sequenced {
            self.engine_seq = *seq;
            let edge_id = Self::edge_id_for_degree_op(op);
            let old_edge = edge_id
                .and_then(|id| edge_states.get(&id).copied().flatten())
                .map(OldEdgeInfo::from_core);
            self.active_memtable().apply_op(op, *seq);
            Self::collect_degree_delta_for_op(op, old_edge, &mut degree_deltas);
            if let Some(edge_id) = edge_id {
                edge_states.insert(edge_id, Self::edge_core_after_op(op));
            }
        }
        self.apply_degree_deltas_to_active_overlay(degree_deltas);
        self.update_engine_seq_seen();
        Ok(())
    }

    /// Internal helper for a single pre-normalized WAL op.
    fn append_and_apply_one_normalized(&mut self, op: &WalOp) -> Result<(), EngineError> {
        let seq = self.engine_seq + 1;
        let old_edge = Self::edge_id_for_degree_op(op)
            .map(|id| self.get_edge_core_for_cache(id))
            .transpose()?
            .flatten()
            .map(OldEdgeInfo::from_core);
        self.wal_append(|w| w.append(op, seq))?;
        self.engine_seq = seq;
        self.active_memtable().apply_op(op, seq);
        let mut degree_deltas: NodeIdMap<DegreeDelta> = NodeIdMap::default();
        Self::collect_degree_delta_for_op(op, old_edge, &mut degree_deltas);
        self.apply_degree_deltas_to_active_overlay(degree_deltas);
        self.update_engine_seq_seen();
        Ok(())
    }

    /// WAL append with mode-specific sync/backpressure handling.
    /// The closure receives the WalWriter and returns bytes written.
    /// In Immediate mode: append + fsync.
    /// In GroupCommit mode: append under lock with poison check and backpressure.
    fn wal_append<F>(&mut self, f: F) -> Result<(), EngineError>
    where
        F: FnOnce(&mut WalWriter) -> Result<usize, EngineError>,
    {
        match &self.wal_sync_mode {
            WalSyncMode::Immediate => {
                let w = self
                    .wal_writer_immediate
                    .as_mut()
                    .expect("immediate WAL writer");
                f(w)?;
                w.sync()?;
            }
            WalSyncMode::GroupCommit {
                soft_trigger_bytes,
                hard_cap_bytes,
                ..
            } => {
                let soft = *soft_trigger_bytes;
                let hard = *hard_cap_bytes;
                let arc = self.wal_state.as_ref().expect("group commit WAL state");
                let (lock, cvar) = &**arc;
                let mut state = lock.lock().unwrap();

                // Check poison
                if let Some(ref msg) = state.poisoned {
                    return Err(EngineError::WalSyncFailed(msg.clone()));
                }

                // Backpressure: block if at hard cap
                while state.buffered_bytes >= hard {
                    state = cvar.wait(state).unwrap();
                    if let Some(ref msg) = state.poisoned {
                        return Err(EngineError::WalSyncFailed(msg.clone()));
                    }
                }

                // Append to WAL (in-memory BufWriter only, no fsync)
                let bytes_written = f(&mut state.wal_writer)?;
                state.buffered_bytes += bytes_written;

                // Notify sync thread if soft trigger hit
                if state.buffered_bytes >= soft {
                    cvar.notify_all();
                }
            }
        }
        Ok(())
    }

    /// Force an immediate WAL fsync. Blocks until the sync completes.
    /// In Immediate mode, this is a no-op (every write already syncs).
    pub fn sync(&self) -> Result<(), EngineError> {
        match &self.wal_sync_mode {
            WalSyncMode::Immediate => Ok(()),
            WalSyncMode::GroupCommit { .. } => {
                let arc = self.wal_state.as_ref().expect("group commit WAL state");
                let (lock, cvar) = &**arc;
                let mut state = lock.lock().unwrap();
                if let Some(ref msg) = state.poisoned {
                    return Err(EngineError::WalSyncFailed(msg.clone()));
                }
                if state.buffered_bytes > 0 {
                    state.wal_writer.sync()?;
                    state.buffered_bytes = 0;
                    state.sync_error_count = 0;
                    cvar.notify_all();
                }
                Ok(())
            }
        }
    }

    // --- Flush pipeline ---

    /// Freeze the active memtable: sync the current WAL generation, allocate a
    /// new WAL generation, record the frozen epoch in the manifest, open a new
    /// WAL writer, and move the active memtable to the immutable queue.
    ///
    /// No-op if the active memtable is empty.
    pub(crate) fn freeze_memtable(&mut self) -> Result<(), EngineError> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        // 1. Sync current active WAL generation
        match &self.wal_sync_mode {
            WalSyncMode::Immediate => {
                self.wal_writer_immediate
                    .as_mut()
                    .expect("immediate WAL writer")
                    .sync()?;
            }
            WalSyncMode::GroupCommit { .. } => {
                let arc = self.wal_state.as_ref().expect("group commit WAL state");
                let (lock, cvar) = &**arc;
                let mut state = lock.lock().unwrap();
                if let Some(ref msg) = state.poisoned {
                    return Err(EngineError::WalSyncFailed(msg.clone()));
                }
                if state.buffered_bytes > 0 {
                    state.wal_writer.sync()?;
                    state.buffered_bytes = 0;
                    state.sync_error_count = 0;
                    cvar.notify_all();
                }
            }
        }
        self.update_engine_seq_seen();

        // 2. Allocate new WAL generation
        let old_wal_gen = self.active_wal_generation_id;
        let epoch_id = old_wal_gen;
        let new_wal_gen = self.with_runtime_manifest_write(|manifest| {
            let new_wal_gen = manifest.next_wal_generation_id;
            manifest.next_wal_generation_id = new_wal_gen + 1;
            manifest.pending_flush_epochs.push(FlushEpochMeta {
                epoch_id,
                wal_generation_id: old_wal_gen,
                state: FlushEpochState::FrozenPendingFlush,
                segment_id: None,
            });
            manifest.active_wal_generation_id = new_wal_gen;
            Ok(new_wal_gen)
        })?;

        // 4. Open new WAL writer (after manifest is durable)
        match &self.wal_sync_mode {
            WalSyncMode::Immediate => {
                self.wal_writer_immediate =
                    Some(WalWriter::open_generation(&self.db_dir, new_wal_gen)?);
            }
            WalSyncMode::GroupCommit { .. } => {
                let arc = self.wal_state.as_ref().expect("group commit WAL state");
                let (lock, _cvar) = &**arc;
                let mut state = lock.lock().unwrap();
                state.wal_writer = WalWriter::open_generation(&self.db_dir, new_wal_gen)?;
            }
        }

        // 5. Swap memtable to immutable queue (newest-first = insert at front)
        self.active_wal_generation_id = new_wal_gen;
        let next_memtable = Arc::new(Memtable::new());
        for entry in self.secondary_index_entries_snapshot() {
            next_memtable.register_secondary_index(&entry);
        }
        let frozen = std::mem::replace(&mut self.memtable, next_memtable);
        let frozen_degree_overlay = std::mem::replace(
            &mut self.active_degree_overlay,
            DegreeOverlaySnapshot::empty(),
        );
        let frozen_size = frozen.estimated_size();
        self.immutable_epochs.insert(
            0,
            ImmutableEpoch {
                epoch_id: old_wal_gen,
                wal_generation_id: old_wal_gen,
                memtable: frozen,
                degree_overlay: frozen_degree_overlay,
                in_flight: false,
            },
        );
        self.immutable_bytes_total += frozen_size;

        Ok(())
    }

    /// Flush the current memtable to an immutable on-disk segment.
    ///
    /// 1. Apply any already-completed background flush results
    /// 2. Freeze current memtable (sync WAL, allocate new WAL generation)
    /// 3. Enqueue all pending immutable epochs to the background flush worker
    /// 4. Wait for all in-flight flushes to complete and publish results
    ///
    /// Returns the last SegmentInfo written, or None if memtable was empty.
    /// On worker failure, returns `Err` and failed epochs remain in
    /// `immutable_epochs` with `in_flight = false`. Data is safe (WAL
    /// retained for replay on reopen). A subsequent `flush()` call will
    /// re-enqueue and retry the failed epochs.
    pub fn flush(&mut self) -> Result<Option<SegmentInfo>, EngineError> {
        self.try_complete_bg_compact();
        self.try_apply_all_bg_flushes();

        if self.memtable.is_empty() && self.immutable_epochs.is_empty() {
            return self.current_flush_pipeline_error().map_or(Ok(None), Err);
        }

        if !self.memtable.is_empty() {
            self.freeze_memtable()?;
        }

        if self.immutable_epochs.is_empty() {
            return self.current_flush_pipeline_error().map_or(Ok(None), Err);
        }

        self.ensure_bg_flush_worker();
        self.enqueue_all_non_in_flight()?;

        let mut last_seg_info = None;
        while self.immutable_epochs.iter().any(|e| e.in_flight) {
            match self.wait_for_one_flush() {
                Ok(Some(info)) => {
                    last_seg_info = Some(info);
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }

        if let Some(err) = self.current_flush_pipeline_error() {
            return Err(err);
        }
        Ok(last_seg_info)
    }

    /// Total buffered memtable bytes: active + all immutables.
    fn total_memtable_bytes(&self) -> usize {
        self.memtable.estimated_size() + self.immutable_bytes_total
    }

    /// Check if the active memtable should be frozen based on size threshold.
    /// Called automatically after writes when flush_threshold > 0.
    /// Only the active memtable size is checked; immutable epochs are already
    /// queued for the bg worker and handled by backpressure separately.
    fn maybe_auto_flush(&mut self) -> (Result<(), EngineError>, PublishImpact) {
        let result = self.maybe_surface_or_retry_flush_pipeline_error();
        if let Err(error) = result {
            return (Err(error), PublishImpact::NoPublish);
        }
        if self.flush_threshold > 0 && self.memtable.estimated_size() >= self.flush_threshold {
            if !self.memtable.is_empty() {
                if let Err(error) = self.freeze_memtable() {
                    return (Err(error), PublishImpact::NoPublish);
                }
            }
            self.ensure_bg_flush_worker();
            if let Err(error) = self.enqueue_all_non_in_flight() {
                return (Err(error), PublishImpact::RebuildSources);
            }
            // Return immediately, no waiting! Data stays visible in immutable_epochs.
            return (Ok(()), PublishImpact::RebuildSources);
        }
        (Ok(()), PublishImpact::NoPublish)
    }

    /// Prepare any flush work required to relieve write pressure without
    /// blocking. Runtime wrappers wait outside the core lock if this returns
    /// `Wait`.
    fn prepare_backpressure_flush(
        &mut self,
    ) -> (Result<BackpressureFlushAction, EngineError>, PublishImpact) {
        if let Err(error) = self.maybe_surface_or_retry_flush_pipeline_error() {
            return (Err(error), PublishImpact::NoPublish);
        }
        let bytes_exceeded =
            self.memtable_hard_cap > 0 && self.total_memtable_bytes() >= self.memtable_hard_cap;
        let count_exceeded = self.max_immutable_memtables > 0
            && self.immutable_epochs.len() >= self.max_immutable_memtables;
        if !(bytes_exceeded || count_exceeded) {
            return (Ok(BackpressureFlushAction::Ready), PublishImpact::NoPublish);
        }
        if self.immutable_epochs.iter().any(|e| e.in_flight) {
            return (Ok(BackpressureFlushAction::Wait), PublishImpact::NoPublish);
        }
        if !self.immutable_epochs.is_empty() {
            self.ensure_bg_flush_worker();
            return match self.enqueue_flush() {
                Ok(()) => (Ok(BackpressureFlushAction::Wait), PublishImpact::NoPublish),
                Err(error) => (Err(error), PublishImpact::NoPublish),
            };
        }

        if let Err(error) = self.freeze_memtable() {
            return (Err(error), PublishImpact::NoPublish);
        }
        self.ensure_bg_flush_worker();
        match self.enqueue_flush() {
            Ok(()) => (
                Ok(BackpressureFlushAction::Wait),
                PublishImpact::RebuildSources,
            ),
            Err(error) => (Err(error), PublishImpact::RebuildSources),
        }
    }

    // --- Background flush ---

    /// Lazily start the persistent background flush worker thread.
    fn ensure_bg_flush_worker(&mut self) {
        if self.bg_flush.is_some() {
            return;
        }
        let (work_tx, work_rx) = std::sync::mpsc::channel();
        let (built_tx, built_rx) = std::sync::mpsc::sync_channel(1);
        let event_cap = self.max_immutable_memtables.max(4) + 1;
        let (event_tx, event_rx) = std::sync::mpsc::sync_channel(event_cap);
        let cancel = Arc::new(AtomicBool::new(false));
        let events_ready = Arc::new(AtomicUsize::new(0));

        let build_cancel = Arc::clone(&cancel);
        let build_events_ready = Arc::clone(&events_ready);
        let build_event_tx = event_tx.clone();
        let build_secondary_indexes = Arc::clone(&self.secondary_index_entries);
        let build_runtime = self.runtime.clone();
        let build_handle = std::thread::spawn(move || {
            bg_flush_build_worker(
                work_rx,
                built_tx,
                build_event_tx,
                build_cancel,
                build_events_ready,
                build_runtime,
                build_secondary_indexes,
            );
        });

        let publish_cancel = Arc::clone(&cancel);
        let publish_events_ready = Arc::clone(&events_ready);
        let db_dir = self.db_dir.clone();
        let manifest_write_lock = Arc::clone(&self.manifest_write_lock);
        let publish_catalog = Arc::clone(&self.secondary_index_catalog);
        let publish_entries = Arc::clone(&self.secondary_index_entries);
        let next_node_id_seen = Arc::clone(&self.next_node_id_seen);
        let next_edge_id_seen = Arc::clone(&self.next_edge_id_seen);
        let engine_seq_seen = Arc::clone(&self.engine_seq_seen);
        let publish_runtime = self.runtime.clone();
        #[cfg(test)]
        let publish_pause = Arc::clone(&self.flush_publish_pause);
        let publish_handle = std::thread::spawn(move || {
            bg_flush_publish_worker(
                db_dir,
                built_rx,
                event_tx,
                manifest_write_lock,
                publish_catalog,
                publish_entries,
                next_node_id_seen,
                next_edge_id_seen,
                engine_seq_seen,
                publish_cancel,
                publish_events_ready,
                publish_runtime,
                #[cfg(test)]
                publish_pause,
            );
        });

        self.bg_flush = Some(BgFlushHandle {
            work_tx,
            event_rx: Mutex::new(event_rx),
            build_handle: Some(build_handle),
            publish_handle: Some(publish_handle),
            cancel,
            events_ready,
            events_applied: 0,
        });
    }

    /// Find the oldest non-in-flight epoch, mark it in-flight, and send it
    /// to the background build worker. The epoch stays in `immutable_epochs`
    /// until a later cheap adoption event removes it after durable publish.
    fn enqueue_flush(&mut self) -> Result<(), EngineError> {
        let bg = self
            .bg_flush
            .as_ref()
            .expect("bg flush worker must be running before enqueue");

        // Find oldest non-in-flight epoch (last in vec since newest-first)
        let epoch_idx = self
            .immutable_epochs
            .iter()
            .rposition(|e| !e.in_flight)
            .expect("enqueue_flush: no non-in-flight epoch available");

        let epoch_id = self.immutable_epochs[epoch_idx].epoch_id;
        let wal_gen_id = self.immutable_epochs[epoch_idx].wal_generation_id;
        let frozen = Arc::clone(&self.immutable_epochs[epoch_idx].memtable);
        let degree_overlay = Arc::clone(&self.immutable_epochs[epoch_idx].degree_overlay);

        let seg_id = self.next_segment_id;

        let segments_dir = self.db_dir.join("segments");
        std::fs::create_dir_all(&segments_dir)?;

        let work = BgFlushWork {
            epoch_id,
            frozen,
            degree_overlay,
            seg_id,
            tmp_dir: segment_tmp_dir(&self.db_dir, seg_id),
            final_dir: segment_dir(&self.db_dir, seg_id),
            dense_config: self.manifest.dense_vector.clone(),
            wal_gen_id,
            #[cfg(test)]
            pause: self.flush_pause.lock().unwrap().take(),
            #[cfg(test)]
            force_write_error: {
                let err = self.flush_force_error;
                self.flush_force_error = false;
                err
            },
        };

        bg.work_tx
            .send(work)
            .map_err(|_| EngineError::InvalidOperation("bg flush worker died".into()))?;

        // Commit state only after all fallible operations succeed.
        // Setting in_flight before send would create a phantom in-flight epoch
        // if send fails, because the drain loop would deadlock waiting for a result
        // that will never arrive.
        self.immutable_epochs[epoch_idx].in_flight = true;
        self.next_segment_id += 1;
        Ok(())
    }

    /// Enqueue all non-in-flight immutable epochs to the background flush worker.
    fn enqueue_all_non_in_flight(&mut self) -> Result<(), EngineError> {
        while self.immutable_epochs.iter().any(|e| !e.in_flight) {
            self.enqueue_flush()?;
        }
        Ok(())
    }

    /// Non-blocking: drain all completed flush results.
    fn try_apply_all_bg_flushes(&mut self) {
        loop {
            let event = {
                let bg = match self.bg_flush.as_ref() {
                    Some(bg) => bg,
                    None => return,
                };
                let ready = bg.events_ready.load(Ordering::Acquire);
                if ready <= bg.events_applied {
                    return;
                }
                let rx = bg.event_rx.lock().unwrap();
                match rx.try_recv() {
                    Ok(event) => event,
                    Err(_) => return,
                }
            };
            if let Some(bg) = self.bg_flush.as_mut() {
                bg.events_applied += 1;
            }
            match self.process_bg_flush_event(event) {
                Ok(_) => {}
                Err(e) => eprintln!("try_apply_all_bg_flushes: {}", e),
            }
        }
    }

    fn drain_ready_bg_flush_events_for_runtime(&mut self) -> RuntimeFlushDrainResult {
        let mut completed_flushes = Vec::new();
        let mut progressed = false;
        let mut publish_impact = PublishImpact::NoPublish;
        loop {
            let event = {
                let bg = match self.bg_flush.as_ref() {
                    Some(bg) => bg,
                    None => break,
                };
                let ready = bg.events_ready.load(Ordering::Acquire);
                if ready <= bg.events_applied {
                    break;
                }
                let rx = bg.event_rx.lock().unwrap();
                match rx.try_recv() {
                    Ok(event) => event,
                    Err(_) => break,
                }
            };
            progressed = true;
            if let Some(bg) = self.bg_flush.as_mut() {
                bg.events_applied += 1;
            }
            let completed_epoch_id = match &event {
                BgFlushEvent::Adopt(adoption) => Some(adoption.epoch_id),
                BgFlushEvent::Failed(_) => None,
            };
            match self.process_bg_flush_event(event) {
                Ok(Some(seg_info)) => {
                    publish_impact = publish_impact.combine(PublishImpact::RebuildSources);
                    if let Some(epoch_id) = completed_epoch_id {
                        completed_flushes.push((epoch_id, seg_info));
                    }
                }
                Ok(None) => {}
                Err(e) => eprintln!("drain_ready_bg_flush_events_for_runtime: {}", e),
            }
        }
        RuntimeFlushDrainResult {
            progressed,
            publish_impact,
            completed_flushes,
        }
    }

    /// Blocking: wait for one flush result from the background worker.
    fn wait_for_one_flush(&mut self) -> Result<Option<SegmentInfo>, EngineError> {
        let recv_result = {
            let bg = self
                .bg_flush
                .as_ref()
                .ok_or_else(|| EngineError::InvalidOperation("no bg flush worker".into()))?;
            let rx = bg.event_rx.lock().unwrap();
            rx.recv()
        };
        match recv_result {
            Ok(event) => {
                if let Some(bg) = self.bg_flush.as_mut() {
                    bg.events_applied += 1;
                }
                let result = self.process_bg_flush_event(event);
                if result.is_err() {
                    self.flush_pipeline_error_reported = true;
                }
                result
            }
            Err(_) => {
                let shutdown_events = self.shutdown_bg_flush();
                for event in shutdown_events {
                    let _ = self.process_bg_flush_event(event);
                }
                self.reset_all_flush_in_flight();
                if let Some(err) = self.current_flush_pipeline_error() {
                    Err(err)
                } else {
                    Err(EngineError::InvalidOperation("bg flush worker died".into()))
                }
            }
        }
    }

    fn process_bg_flush_event(
        &mut self,
        event: BgFlushEvent,
    ) -> Result<Option<SegmentInfo>, EngineError> {
        match event {
            BgFlushEvent::Adopt(adoption) => {
                let seg_info = adoption.seg_info.clone();
                for index_id in &adoption.rebuild_equality_index_ids {
                    if let Some(entry) = self
                        .manifest
                        .secondary_indexes
                        .iter_mut()
                        .find(|entry| entry.index_id == *index_id)
                    {
                        if matches!(entry.kind, SecondaryIndexKind::Equality)
                            && entry.state != SecondaryIndexState::Failed
                        {
                            entry.state = SecondaryIndexState::Building;
                            entry.last_error = None;
                        }
                    }
                }
                for index_id in &adoption.rebuild_range_index_ids {
                    if let Some(entry) = self
                        .manifest
                        .secondary_indexes
                        .iter_mut()
                        .find(|entry| entry.index_id == *index_id)
                    {
                        if matches!(entry.kind, SecondaryIndexKind::Range { .. })
                            && entry.state != SecondaryIndexState::Failed
                        {
                            entry.state = SecondaryIndexState::Building;
                            entry.last_error = None;
                        }
                    }
                }
                if !self
                    .manifest
                    .segments
                    .iter()
                    .any(|s| s.id == adoption.seg_info.id)
                {
                    self.manifest.segments.push(adoption.seg_info);
                }
                self.manifest.pending_flush_epochs.retain(|epoch| {
                    !(epoch.epoch_id == adoption.epoch_id
                        && epoch.wal_generation_id == adoption.wal_gen_to_retire)
                });
                self.warm_declared_index_runtime_coverage_for_reader(&adoption.reader);
                self.segments.insert(0, Arc::new(adoption.reader));
                if let Some(idx) = self
                    .immutable_epochs
                    .iter()
                    .position(|epoch| epoch.epoch_id == adoption.epoch_id)
                {
                    let removed = self.immutable_epochs.remove(idx);
                    self.immutable_bytes_total = self
                        .immutable_bytes_total
                        .saturating_sub(removed.memtable.estimated_size());
                }
                if self
                    .flush_pipeline_error
                    .as_ref()
                    .is_some_and(|err| err.epoch_id == adoption.epoch_id)
                {
                    self.flush_pipeline_error = None;
                    self.flush_pipeline_error_reported = false;
                }

                if !self.compacting {
                    self.flush_count_since_last_compact =
                        self.flush_count_since_last_compact.saturating_add(1);
                    let _ = self.maybe_schedule_bg_compact();
                }
                for index_id in adoption.rebuild_equality_index_ids {
                    self.enqueue_secondary_index_job(SecondaryIndexJob::Build { index_id });
                }
                for index_id in adoption.rebuild_range_index_ids {
                    self.enqueue_secondary_index_job(SecondaryIndexJob::Build { index_id });
                }

                Ok(Some(seg_info))
            }
            BgFlushEvent::Failed(err) => {
                self.record_flush_pipeline_error(err.clone());
                self.reset_all_flush_in_flight();
                let shutdown_events = self.shutdown_bg_flush();
                for shutdown_event in shutdown_events {
                    let _ = self.process_bg_flush_event(shutdown_event);
                }
                Err(err.to_engine_error())
            }
        }
    }

    /// Block until all in-flight background flush work completes and is applied.
    /// Does not enqueue new work from immutable_epochs.
    fn drain_bg_flush(&mut self) {
        while self.immutable_epochs.iter().any(|e| e.in_flight) {
            if self.bg_flush.is_none() {
                self.reset_all_flush_in_flight();
                break;
            }
            match self.wait_for_one_flush() {
                Ok(_) => {}
                Err(e) => {
                    // Continue draining; don't leave in-flight epochs orphaned.
                    // wait_for_one_flush resets in_flight on error, so the loop
                    // will terminate when no more in-flight work remains.
                    eprintln!("drain_bg_flush: error waiting for flush: {}", e);
                }
            }
        }
    }

    fn reset_all_flush_in_flight(&mut self) {
        for epoch in &mut self.immutable_epochs {
            epoch.in_flight = false;
        }
    }

    fn record_flush_pipeline_error(&mut self, err: FlushPipelineError) {
        match &self.flush_pipeline_error {
            Some(existing) if existing.wal_generation_id < err.wal_generation_id => {}
            _ => {
                self.flush_pipeline_error = Some(err);
                self.flush_pipeline_error_reported = false;
            }
        }
    }

    fn maybe_surface_or_retry_flush_pipeline_error(&mut self) -> Result<(), EngineError> {
        if let Some(err) = self.flush_pipeline_error.clone() {
            if !self.flush_pipeline_error_reported {
                self.flush_pipeline_error_reported = true;
                return Err(err.to_engine_error());
            }
            if self.immutable_epochs.iter().any(|epoch| !epoch.in_flight) {
                self.ensure_bg_flush_worker();
                self.enqueue_all_non_in_flight()?;
            }
        }
        Ok(())
    }

    fn current_flush_pipeline_error(&mut self) -> Option<EngineError> {
        self.flush_pipeline_error.clone().map(|err| {
            self.flush_pipeline_error_reported = true;
            err.to_engine_error()
        })
    }

    /// Shut down the background flush workers, if running, and return any
    /// already-queued completion events so callers can perform lazy adoption.
    /// Any remaining epochs are no longer in flight once the worker is gone.
    fn shutdown_bg_flush(&mut self) -> Vec<BgFlushEvent> {
        let events = if let Some(mut bg) = self.bg_flush.take() {
            bg.cancel.store(true, Ordering::Relaxed);
            drop(bg.work_tx);
            if let Some(handle) = bg.build_handle.take() {
                let _ = handle.join();
            }
            if let Some(handle) = bg.publish_handle.take() {
                let _ = handle.join();
            }
            let mut events = Vec::new();
            let rx = bg.event_rx.lock().unwrap();
            while let Ok(event) = rx.try_recv() {
                events.push(event);
            }
            events
        } else {
            Vec::new()
        };
        self.reset_all_flush_in_flight();
        events
    }

    // --- Background compaction ---

    /// Spawn a background thread to compact all current segments.
    /// Returns early if a background compaction is already running or < 2 segments.
    fn maybe_schedule_bg_compact(&mut self) -> Result<(), EngineError> {
        if self.compacting
            || self.bg_compact.is_some()
            || self.compact_after_n_flushes == 0
            || self.flush_count_since_last_compact < self.compact_after_n_flushes
            || self.segments.len() < 2
        {
            return Ok(());
        }
        self.start_bg_compact()
    }

    /// Spawn a background thread to compact all current segments.
    /// Returns early if a background compaction is already running or < 2 segments.
    fn start_bg_compact(&mut self) -> Result<(), EngineError> {
        if self.bg_compact.is_some() || self.segments.len() < 2 {
            return Ok(());
        }

        // Snapshot current segment IDs and paths for the background thread.
        let input_segments: Vec<(u64, PathBuf)> = self
            .segments
            .iter()
            .map(|s| (s.segment_id, segment_dir(&self.db_dir, s.segment_id)))
            .collect();
        // Allocate the output segment ID on the main thread.
        let seg_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Reset flush counter (same as synchronous compact).
        self.flush_count_since_last_compact = 0;

        let db_dir = self.db_dir.clone();
        let prune_policies: Vec<PrunePolicy> =
            self.manifest.prune_policies.values().cloned().collect();
        let dense_vector = self.manifest.dense_vector.clone();
        let secondary_indexes = self.secondary_index_entries_snapshot();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = Arc::clone(&cancel);
        let runtime = self.runtime.clone();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = Arc::clone(&completed);
        #[cfg(test)]
        let compact_pause = Arc::clone(&self.bg_compact_pause);
        let handle = std::thread::spawn(move || {
            let _completion_signal = BgCompactCompletionSignal {
                completed: completed_clone,
                runtime,
            };
            bg_compact_worker(
                db_dir,
                seg_id,
                input_segments,
                prune_policies,
                dense_vector,
                secondary_indexes,
                &cancel_clone,
                #[cfg(test)]
                &compact_pause,
            )
        });

        self.bg_compact = Some(BgCompactHandle {
            handle,
            cancel,
            completed,
        });

        Ok(())
    }

    /// Non-blocking check: if a background compaction has finished, apply its result.
    fn try_complete_bg_compact(&mut self) -> Option<CompactionStats> {
        let is_finished = self
            .bg_compact
            .as_ref()
            .is_some_and(|bg| bg.completed.load(Ordering::Acquire));
        if !is_finished {
            return None;
        }
        let bg = self.bg_compact.take().unwrap();
        self.join_bg_compact(bg)
    }

    /// Blocking wait: if a background compaction is running, wait for it and apply.
    fn wait_for_bg_compact(&mut self) -> Option<CompactionStats> {
        let bg = self.bg_compact.take()?;
        self.join_bg_compact(bg)
    }

    /// Cancel a running background compaction and wait for the thread to exit.
    /// The compaction result is discarded. Original segments remain intact.
    /// If no bg compact is running, this is a no-op.
    fn cancel_bg_compact(&mut self) {
        if let Some(bg) = self.bg_compact.take() {
            bg.cancel.store(true, Ordering::Relaxed);
            // Join the thread. It will see the cancel flag and exit early.
            // Discard the result (it's either CompactionCancelled or partial).
            let _ = bg.handle.join();
        }
    }

    /// Join a background compaction handle and apply its result.
    fn join_bg_compact(&mut self, bg: BgCompactHandle) -> Option<CompactionStats> {
        match bg.handle.join() {
            Ok(Ok(result)) => self.apply_bg_compact_result(result),
            Ok(Err(e)) => {
                eprintln!("Background compaction failed: {}", e);
                None
            }
            Err(_) => {
                eprintln!("Background compaction thread panicked");
                None
            }
        }
    }

    /// Apply a completed background compaction result: update manifest, swap
    /// segments, and delete old segment directories.
    fn apply_bg_compact_result(&mut self, result: BgCompactResult) -> Option<CompactionStats> {
        let (updated_manifest, rebuild_equality_index_ids, rebuild_range_index_ids) = {
            let _guard = self.manifest_write_lock.lock().unwrap();
            let mut manifest = match self.load_current_manifest_for_write() {
                Ok(manifest) => manifest,
                Err(e) => {
                    eprintln!("Background compaction: manifest load failed: {}", e);
                    let output_dir = segment_dir(&self.db_dir, result.stats.output_segment_id);
                    let _ = std::fs::remove_dir_all(output_dir);
                    return None;
                }
            };

            let live_seg_ids: NodeIdSet = manifest.segments.iter().map(|s| s.id).collect();
            for input_id in &result.input_segment_ids {
                if !live_seg_ids.contains(input_id) {
                    let output_dir = segment_dir(&self.db_dir, result.stats.output_segment_id);
                    let _ = std::fs::remove_dir_all(output_dir);
                    return None;
                }
            }

            manifest
                .segments
                .retain(|s| !result.input_segment_ids.contains(&s.id));
            manifest.segments.push(result.seg_info.clone());
            apply_secondary_index_failure_report(&mut manifest, &result.secondary_index_report);
            let rebuild_equality_index_ids = reconcile_background_output_equality_declarations(
                &mut manifest,
                &result.maintained_equality_index_ids,
            );
            let rebuild_range_index_ids = reconcile_background_output_range_declarations(
                &mut manifest,
                &result.maintained_range_index_ids,
            );
            self.merge_runtime_manifest_counters(&mut manifest);

            if let Err(e) = write_manifest(&self.db_dir, &manifest) {
                eprintln!("Background compaction: manifest write failed: {}", e);
                let output_dir = segment_dir(&self.db_dir, result.stats.output_segment_id);
                let _ = std::fs::remove_dir_all(output_dir);
                return None;
            }
            (
                manifest,
                rebuild_equality_index_ids,
                rebuild_range_index_ids,
            )
        };

        self.manifest = updated_manifest;
        if let Err(error) = self.rebuild_secondary_index_catalog() {
            eprintln!(
                "Background compaction: secondary index runtime sync failed: {}",
                error
            );
        }
        // Remove input segments, keep any new segments added by flushes during
        // background compaction (they have different IDs).
        self.segments
            .retain(|s| !result.input_segment_ids.contains(&s.segment_id));
        // Compacted segment is oldest; push to end (segments are newest-first).
        self.warm_declared_index_runtime_coverage_for_reader(&result.reader);
        self.segments.push(Arc::new(result.reader));

        // Defer old segment cleanup until after the new published snapshot is
        // installed. Windows keeps mmap-backed files locked while old readers
        // remain reachable.
        self.defer_segment_dir_cleanup(result.old_seg_dirs);

        self.last_compaction_ms = Some(now_millis());

        for index_id in rebuild_equality_index_ids {
            self.enqueue_secondary_index_job(SecondaryIndexJob::Build { index_id });
        }
        for index_id in rebuild_range_index_ids {
            self.enqueue_secondary_index_job(SecondaryIndexJob::Build { index_id });
        }

        let _ = self.maybe_schedule_bg_compact();

        Some(result.stats)
    }

    // --- Compaction ---

    /// Enter ingest mode: disables auto-compaction so bulk writes produce
    /// segments without triggering background merges. Call `end_ingest` when
    /// loading is complete to compact and restore normal operation.
    pub fn ingest_mode(&mut self) -> PublishImpact {
        let mut changed = false;
        if self.ingest_saved_compact_after_n_flushes.is_none() {
            self.ingest_saved_compact_after_n_flushes = Some(self.compact_after_n_flushes);
            changed = true;
        }
        if self.compact_after_n_flushes != 0 {
            self.compact_after_n_flushes = 0;
            changed = true;
        }
        let _ = changed;
        PublishImpact::NoPublish
    }

    /// Compact all segments into a single segment with progress reporting.
    ///
    /// The `callback` receives a `CompactionProgress` at key points during
    /// compaction. Return `true` to continue, `false` to cancel. Cancellation
    /// is safe. No state is modified until the output segment is fully written
    /// and verified.
    ///
    /// Uses a fast raw-merge path when segments are non-overlapping, contain no
    /// tombstones, and no prune policies are active. All other cases fall back
    /// to the unified V3 compaction path: metadata-only planning (winner
    /// selection from sidecars), raw binary copy of winning records, and
    /// metadata-driven index building. No MessagePack decode occurs on either
    /// path.
    ///
    /// Returns `CompactionStats` on success, `None` if fewer than 2 segments,
    /// or `Err(CompactionCancelled)` if the callback returned false.
    pub fn compact_with_progress<F>(
        &mut self,
        mut callback: F,
    ) -> Result<Option<CompactionStats>, EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        // Wait for any in-progress background work before proceeding.
        self.wait_for_bg_compact();
        self.try_apply_all_bg_flushes();

        if self.segments.len() < 2 {
            return Ok(None);
        }

        self.compacting = true;
        let result = self.compact_with_progress_inner(&mut callback);
        self.compacting = false;
        // Reset flush counter after any compaction attempt (success or failure)
        self.flush_count_since_last_compact = 0;

        result
    }

    fn compact_with_progress_inner<F>(
        &mut self,
        callback: &mut F,
    ) -> Result<Option<CompactionStats>, EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        let compact_start = std::time::Instant::now();

        // Flush memtable + any pending immutable epochs first so compaction
        // sees all data and tombstones (async auto-flush may leave epochs
        // in-flight that contain tombstones needed for correct compaction).
        if !self.memtable.is_empty() || !self.immutable_epochs.is_empty() {
            self.flush()?;
        }

        // Count input records for stats
        let input_segment_count = self.segments.len();
        let total_input_nodes: u64 = self.segments.iter().map(|s| s.node_count()).sum();
        let total_input_edges: u64 = self.segments.iter().map(|s| s.edge_count()).sum();

        let has_tombstones = self.segments.iter().any(|s| s.has_tombstones());
        let policies: Vec<PrunePolicy> = self.manifest.prune_policies.values().cloned().collect();
        let secondary_indexes = self.secondary_index_entries_snapshot();
        let degree_sidecar_expected =
            policies.is_empty() && self.segments.iter().all(|s| s.degree_delta_available());
        let compaction_path =
            select_compaction_path(&self.segments, has_tombstones, !policies.is_empty());

        // Ensure segments directory exists before allocating segment ID (M1 fix)
        let segments_dir = self.db_dir.join("segments");
        std::fs::create_dir_all(&segments_dir)?;

        // Allocate new segment ID
        let seg_id = self.next_segment_id;
        self.next_segment_id += 1;
        let tmp_dir = segment_tmp_dir(&self.db_dir, seg_id);
        let final_dir = segment_dir(&self.db_dir, seg_id);

        let (seg_info, nodes_auto_pruned, edges_auto_pruned, secondary_index_report) =
            match compaction_path {
                CompactionPath::FastMerge => match self.compact_fast_merge(
                    &tmp_dir,
                    seg_id,
                    callback,
                    &secondary_indexes,
                    input_segment_count,
                    total_input_nodes,
                    total_input_edges,
                ) {
                    Ok((seg_info, report)) => (seg_info, 0, 0, report),
                    Err(e) => {
                        self.next_segment_id -= 1;
                        let _ = std::fs::remove_dir_all(&tmp_dir);
                        return Err(e);
                    }
                },
                CompactionPath::UnifiedV3 => match self.compact_standard(
                    &tmp_dir,
                    seg_id,
                    callback,
                    has_tombstones,
                    &policies,
                    &secondary_indexes,
                    input_segment_count,
                    total_input_nodes,
                    total_input_edges,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        self.next_segment_id -= 1;
                        let _ = std::fs::remove_dir_all(&tmp_dir);
                        return Err(e);
                    }
                },
            };

        if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
            self.next_segment_id -= 1;
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(e.into());
        }

        // Open new segment reader BEFORE modifying any state (M3 fix)
        let new_reader =
            match SegmentReader::open(&final_dir, seg_id, self.manifest.dense_vector.as_ref()) {
                Ok(r) => r,
                Err(e) => {
                    // Output segment exists on disk but we can't read it.
                    // Clean up orphan directory and release the segment ID.
                    self.next_segment_id -= 1;
                    let _ = std::fs::remove_dir_all(&final_dir);
                    return Err(e);
                }
            };
        if degree_sidecar_expected && !new_reader.degree_delta_available() {
            self.next_segment_id -= 1;
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(EngineError::CorruptRecord(format!(
                "compaction output segment {} degree sidecar is missing or invalid",
                seg_id
            )));
        }

        // Collect old segment info for cleanup
        let old_seg_ids: NodeIdSet = self.segments.iter().map(|s| s.segment_id).collect();
        let old_seg_dirs: Vec<PathBuf> = old_seg_ids
            .iter()
            .map(|&id| segment_dir(&self.db_dir, id))
            .collect();

        let new_manifest = {
            let _guard = self.manifest_write_lock.lock().unwrap();
            let mut manifest = self.load_current_manifest_for_write()?;
            manifest.segments.retain(|s| !old_seg_ids.contains(&s.id));
            manifest.segments.push(seg_info.clone());
            apply_secondary_index_failure_report(&mut manifest, &secondary_index_report);
            self.merge_runtime_manifest_counters(&mut manifest);
            write_manifest(&self.db_dir, &manifest)?;
            manifest
        };

        // Manifest is durable. Now safe to swap in-memory state
        self.manifest = new_manifest;
        self.rebuild_secondary_index_catalog()?;
        self.segments.clear();
        self.warm_declared_index_runtime_coverage_for_reader(&new_reader);
        self.segments.push(Arc::new(new_reader));

        // Defer old segment cleanup until after the new published snapshot is
        // installed. Windows keeps mmap-backed files locked while old readers
        // remain reachable.
        self.defer_segment_dir_cleanup(old_seg_dirs);

        let stats = CompactionStats {
            segments_merged: input_segment_count,
            nodes_kept: seg_info.node_count,
            nodes_removed: total_input_nodes.saturating_sub(seg_info.node_count),
            edges_kept: seg_info.edge_count,
            edges_removed: total_input_edges.saturating_sub(seg_info.edge_count),
            duration_ms: compact_start.elapsed().as_millis() as u64,
            output_segment_id: seg_id,
            nodes_auto_pruned,
            edges_auto_pruned,
        };

        self.last_compaction_ms = Some(now_millis());
        Ok(Some(stats))
    }

    /// Fast-merge compaction: binary-copy data files and rebuild indexes from metadata.
    ///
    /// Pre-condition: segments are non-overlapping by node/edge ID, contain no
    /// tombstones, and no prune policies are active.
    #[allow(clippy::too_many_arguments)]
    fn compact_fast_merge<F>(
        &self,
        tmp_dir: &Path,
        seg_id: u64,
        callback: &mut F,
        secondary_indexes: &[SecondaryIndexManifestEntry],
        input_segment_count: usize,
        total_input_nodes: u64,
        total_input_edges: u64,
    ) -> Result<(SegmentInfo, SecondaryIndexMaintenanceReport), EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        std::fs::create_dir_all(tmp_dir)?;

        let cont = callback(&CompactionProgress {
            phase: CompactionPhase::CollectingTombstones,
            segments_processed: input_segment_count,
            total_segments: input_segment_count,
            records_processed: 0,
            total_records: 0,
        });
        if !cont {
            return Err(EngineError::CompactionCancelled);
        }

        let mut nodes_counted: u64 = 0;
        for (i, seg) in self.segments.iter().enumerate() {
            nodes_counted += seg.node_meta_count();
            let cont = callback(&CompactionProgress {
                phase: CompactionPhase::MergingNodes,
                segments_processed: i + 1,
                total_segments: input_segment_count,
                records_processed: nodes_counted,
                total_records: total_input_nodes,
            });
            if !cont {
                return Err(EngineError::CompactionCancelled);
            }
        }

        let mut edges_counted: u64 = 0;
        for (i, seg) in self.segments.iter().enumerate() {
            edges_counted += seg.edge_meta_count();
            let cont = callback(&CompactionProgress {
                phase: CompactionPhase::MergingEdges,
                segments_processed: i + 1,
                total_segments: input_segment_count,
                records_processed: edges_counted,
                total_records: total_input_edges,
            });
            if !cont {
                return Err(EngineError::CompactionCancelled);
            }
        }

        let cont = callback(&CompactionProgress {
            phase: CompactionPhase::WritingOutput,
            segments_processed: 0,
            total_segments: 1,
            records_processed: 0,
            total_records: total_input_nodes + total_input_edges,
        });
        if !cont {
            return Err(EngineError::CompactionCancelled);
        }

        build_fast_merge_output(
            tmp_dir,
            seg_id,
            &self.segments,
            self.manifest.dense_vector.as_ref(),
            secondary_indexes,
        )
    }

    /// V3 compaction: metadata-only planning + raw binary copy.
    ///
    /// Replaces the old decode-everything approach with:
    /// 1. Plan winners from metadata sidecars (never decode dropped records)
    /// 2. Raw-copy winning record bytes to output data files
    /// 3. Build all secondary indexes from metadata sidecars (no Memtable decode)
    ///
    /// Returns `(SegmentInfo, nodes_auto_pruned, edges_auto_pruned)`.
    #[allow(clippy::too_many_arguments)]
    fn compact_standard<F>(
        &self,
        tmp_dir: &Path,
        seg_id: u64,
        callback: &mut F,
        has_tombstones: bool,
        prune_policies: &[PrunePolicy],
        secondary_indexes: &[SecondaryIndexManifestEntry],
        input_segment_count: usize,
        total_input_nodes: u64,
        total_input_edges: u64,
    ) -> Result<(SegmentInfo, u64, u64, SecondaryIndexMaintenanceReport), EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        // --- Phase 1: Collect tombstones ---
        let mut deleted_nodes: NodeIdSet = NodeIdSet::default();
        let mut deleted_edges: NodeIdSet = NodeIdSet::default();
        if has_tombstones {
            for (i, seg) in self.segments.iter().enumerate() {
                deleted_nodes.extend(seg.deleted_node_ids());
                deleted_edges.extend(seg.deleted_edge_ids());

                let cont = callback(&CompactionProgress {
                    phase: CompactionPhase::CollectingTombstones,
                    segments_processed: i + 1,
                    total_segments: input_segment_count,
                    records_processed: 0,
                    total_records: 0,
                });
                if !cont {
                    return Err(EngineError::CompactionCancelled);
                }
            }
        } else {
            let cont = callback(&CompactionProgress {
                phase: CompactionPhase::CollectingTombstones,
                segments_processed: input_segment_count,
                total_segments: input_segment_count,
                records_processed: 0,
                total_records: 0,
            });
            if !cont {
                return Err(EngineError::CompactionCancelled);
            }
        }

        // --- Phase 2+3: Plan winners from metadata (no full-record decode) ---
        // Report phase starts so callback can cancel before planning begins.
        let cont = callback(&CompactionProgress {
            phase: CompactionPhase::MergingNodes,
            segments_processed: 0,
            total_segments: input_segment_count,
            records_processed: 0,
            total_records: total_input_nodes,
        });
        if !cont {
            return Err(EngineError::CompactionCancelled);
        }

        let plan = v3_plan_winners(
            &self.segments,
            prune_policies,
            &deleted_nodes,
            &deleted_edges,
        )?;

        // Report node planning complete
        let cont = callback(&CompactionProgress {
            phase: CompactionPhase::MergingNodes,
            segments_processed: input_segment_count,
            total_segments: input_segment_count,
            records_processed: total_input_nodes,
            total_records: total_input_nodes,
        });
        if !cont {
            return Err(EngineError::CompactionCancelled);
        }

        // Report edge planning complete
        let cont = callback(&CompactionProgress {
            phase: CompactionPhase::MergingEdges,
            segments_processed: input_segment_count,
            total_segments: input_segment_count,
            records_processed: total_input_edges,
            total_records: total_input_edges,
        });
        if !cont {
            return Err(EngineError::CompactionCancelled);
        }

        // --- Phase 4: Build output segment ---
        let cont = callback(&CompactionProgress {
            phase: CompactionPhase::WritingOutput,
            segments_processed: 0,
            total_segments: 1,
            records_processed: 0,
            total_records: total_input_nodes + total_input_edges,
        });
        if !cont {
            return Err(EngineError::CompactionCancelled);
        }

        let nodes_auto_pruned = plan.pruned_node_ids.len() as u64;
        let edges_auto_pruned = plan.edges_auto_pruned;

        let (seg_info, secondary_index_report) = v3_build_output(
            tmp_dir,
            seg_id,
            &self.segments,
            &plan,
            self.manifest.dense_vector.as_ref(),
            prune_policies.is_empty(),
            secondary_indexes,
        )?;

        Ok((
            seg_info,
            nodes_auto_pruned,
            edges_auto_pruned,
            secondary_index_report,
        ))
    }

    // --- Runtime introspection ---

    /// Return read-only runtime statistics for this database.
    pub fn stats(&self) -> DbStats {
        let pending_wal_bytes = match &self.wal_sync_mode {
            WalSyncMode::Immediate => 0,
            WalSyncMode::GroupCommit { .. } => self
                .wal_state
                .as_ref()
                .map(|arc| {
                    let (lock, _) = &**arc;
                    lock.lock().map(|s| s.buffered_bytes).unwrap_or(0)
                })
                .unwrap_or(0),
        };
        let sync_mode_str = match &self.wal_sync_mode {
            WalSyncMode::Immediate => "immediate".to_string(),
            WalSyncMode::GroupCommit { .. } => "group-commit".to_string(),
        };
        let immutable_memtable_bytes = self.immutable_bytes_total;
        let oldest_retained_wal_gen = self
            .manifest
            .pending_flush_epochs
            .iter()
            .map(|e| e.wal_generation_id)
            .min()
            .unwrap_or(self.active_wal_generation_id);
        DbStats {
            pending_wal_bytes,
            segment_count: self.segments.len(),
            node_tombstone_count: self.memtable.deleted_nodes().len(),
            edge_tombstone_count: self.memtable.deleted_edges().len(),
            last_compaction_ms: self.last_compaction_ms,
            wal_sync_mode: sync_mode_str,
            active_memtable_bytes: self.memtable.estimated_size(),
            immutable_memtable_bytes,
            immutable_memtable_count: self.immutable_epochs.len(),
            pending_flush_count: self.immutable_epochs.iter().filter(|e| e.in_flight).count(),
            active_wal_generation_id: self.active_wal_generation_id,
            oldest_retained_wal_generation_id: oldest_retained_wal_gen,
        }
    }

    /// Advance running ID counters if the op contains an ID >= next_*_id.
    /// Needed for the low-level write_op API where IDs are caller-assigned.
    fn track_id(&mut self, op: &WalOp) {
        match op {
            WalOp::UpsertNode(node) => {
                if node.id >= self.next_node_id {
                    self.next_node_id = node.id + 1;
                    self.update_next_node_id_seen();
                }
            }
            WalOp::UpsertEdge(edge) => {
                if edge.id >= self.next_edge_id {
                    self.next_edge_id = edge.id + 1;
                    self.update_next_edge_id_seen();
                }
            }
            _ => {}
        }
    }

    /// Approximate count of live nodes across all sources.
    ///
    /// Sums memtable and segment counts minus tombstones. This is O(sources),
    /// not O(data). May overcount when the same node ID exists across
    /// active memtable, immutable memtables, and segments (e.g., a node
    /// upserted in gen 0, frozen, then upserted again in gen 1 counts
    /// from both memtables). Not suitable for exact cardinality; use as
    /// a monitoring/inspection metric.
    pub fn node_count(&self) -> usize {
        let mut count = self.memtable.node_count();
        for epoch in &self.immutable_epochs {
            count += epoch.memtable.node_count();
        }
        for seg in &self.segments {
            count += (seg.node_count() as usize).saturating_sub(seg.deleted_node_count());
        }
        count
    }

    /// Approximate count of live edges across all sources.
    ///
    /// Same caveats as [`node_count`]: O(sources), may overcount when the
    /// same edge spans active, immutable, and segment sources.
    pub fn edge_count(&self) -> usize {
        let mut count = self.memtable.edge_count();
        for epoch in &self.immutable_epochs {
            count += epoch.memtable.edge_count();
        }
        for seg in &self.segments {
            count += (seg.edge_count() as usize).saturating_sub(seg.deleted_edge_count());
        }
        count
    }
}

fn get_edge_core_from_sources(
    memtable: &Memtable,
    immutable_epochs: &[ImmutableEpoch],
    segments: &[Arc<SegmentReader>],
    id: u64,
) -> Result<Option<EdgeCore>, EngineError> {
    if let Some((from, to, created_at, updated_at, weight, valid_from, valid_to)) =
        memtable.get_edge_core_at(id, u64::MAX)
    {
        return Ok(Some(EdgeCore {
            from,
            to,
            created_at,
            updated_at,
            weight,
            valid_from,
            valid_to,
        }));
    }
    if memtable.is_edge_deleted_at(id, u64::MAX) {
        return Ok(None);
    }
    for epoch in immutable_epochs {
        if let Some((from, to, created_at, updated_at, weight, valid_from, valid_to)) =
            epoch.memtable.get_edge_core_at(id, u64::MAX)
        {
            return Ok(Some(EdgeCore {
                from,
                to,
                created_at,
                updated_at,
                weight,
                valid_from,
                valid_to,
            }));
        }
        if epoch.memtable.is_edge_deleted_at(id, u64::MAX) {
            return Ok(None);
        }
    }
    for seg in segments {
        if seg.is_edge_deleted(id) {
            return Ok(None);
        }
        if let Some((from, to, created_at, updated_at, weight, valid_from, valid_to)) =
            seg.get_edge_core(id)?
        {
            return Ok(Some(EdgeCore {
                from,
                to,
                created_at,
                updated_at,
                weight,
                valid_from,
                valid_to,
            }));
        }
    }
    Ok(None)
}

fn replay_wal_generation_to_memtable_and_overlay(
    db_dir: &Path,
    wal_generation_id: u64,
    dense_config: Option<&DenseVectorConfig>,
    engine_seq: &mut u64,
    immutable_epochs: &[ImmutableEpoch],
    segments: &[Arc<SegmentReader>],
) -> Result<(Memtable, Arc<DegreeOverlaySnapshot>), EngineError> {
    let memtable = Memtable::new();
    let mut degree_deltas: NodeIdMap<DegreeDelta> = NodeIdMap::default();

    for (seq, op) in WalReader::read_generation(db_dir, wal_generation_id)? {
        let op = normalize_wal_op_for_replay(dense_config, op)?;
        let old_edge = EngineCore::edge_id_for_degree_op(&op)
            .map(|id| get_edge_core_from_sources(&memtable, immutable_epochs, segments, id))
            .transpose()?
            .flatten()
            .map(OldEdgeInfo::from_core);
        *engine_seq = (*engine_seq).max(seq);
        memtable.apply_op(&op, seq);
        EngineCore::collect_degree_delta_for_op(&op, old_edge, &mut degree_deltas);
    }

    Ok((memtable, DegreeOverlaySnapshot::from_flat(degree_deltas)))
}

impl Drop for EngineCore {
    fn drop(&mut self) {
        // Best-effort: wait for background compaction to finish.
        // Errors are swallowed. Drop must not panic.
        self.wait_for_bg_compact();

        // Best-effort: drain any completed bg flush results, then shut down worker.
        self.drain_bg_flush();
        self.shutdown_bg_flush();
        self.shutdown_secondary_index_worker();
        self.retry_deferred_segment_cleanup();

        // Best-effort: shut down sync thread and flush buffered data.
        if self.sync_thread.is_some() {
            if let Some(ref wal_state) = self.wal_state {
                let _ = shutdown_sync_thread(wal_state, &mut self.sync_thread);
            }
        }
    }
}

impl Drop for DatabaseEngine {
    fn drop(&mut self) {
        if Arc::strong_count(&self.runtime) <= 2 {
            self.runtime.best_effort_shutdown();
        }
    }
}

/// Scan the `segments/` subdirectory for the highest segment ID on the filesystem.
/// Returns 0 if no segments directory or no matching entries exist.
/// This catches orphan segments left behind by crashes between segment write
/// and manifest update, preventing ID reuse.
fn scan_max_segment_id(db_dir: &Path) -> u64 {
    let seg_parent = db_dir.join("segments");
    let entries = match std::fs::read_dir(&seg_parent) {
        Ok(e) => e,
        Err(_) => return 0, // No segments dir yet, fine
    };
    let mut max_id: u64 = 0;
    // Per-entry I/O errors are intentionally silenced via flatten().
    // This is best-effort: a missing entry just means we might not
    // detect that orphan, but the worst case is a harmless gap in IDs.
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Segment directories are named "seg_XXXX" (e.g., seg_0001)
        if let Some(id_str) = name.strip_prefix("seg_") {
            if let Ok(id) = id_str.parse::<u64>() {
                max_id = max_id.max(id);
            }
        }
    }
    max_id
}

/// Fsync a directory so metadata updates like rename() are durable.
/// No-op on Windows, where directory fsync is not supported the same way.
fn fsync_dir(dir: &Path) -> Result<(), EngineError> {
    #[cfg(not(target_os = "windows"))]
    {
        let file = std::fs::File::open(dir)?;
        file.sync_all()?;
    }
    #[cfg(target_os = "windows")]
    let _ = dir;
    Ok(())
}

/// Remove segment directories on disk that are not referenced by the manifest.
/// These are orphans from crashes between segment write and manifest update
/// (or between background compaction output and apply). Best-effort: I/O errors
/// during cleanup are silently ignored; the orphan just wastes space.
fn cleanup_orphan_segments(db_dir: &Path, manifest: &ManifestState) {
    let seg_parent = db_dir.join("segments");
    let entries = match std::fs::read_dir(&seg_parent) {
        Ok(e) => e,
        Err(_) => return,
    };
    let manifest_ids: NodeIdSet = manifest.segments.iter().map(|s| s.id).collect();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id_str) = name.strip_prefix("seg_") {
            // Clean up .tmp directories from interrupted flush/compaction
            if id_str.ends_with(".tmp") {
                let _ = std::fs::remove_dir_all(entry.path());
                continue;
            }
            if let Ok(id) = id_str.parse::<u64>() {
                if !manifest_ids.contains(&id) {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
    }
}

/// Remove WAL generation files on disk that are not referenced by the manifest.
/// Orphan WAL files can appear when a crash occurs after WAL retirement completes
/// on disk but before the manifest is updated, or from other interrupted sequences.
fn cleanup_orphan_wal_files(db_dir: &Path, manifest: &ManifestState) {
    // Collect all WAL gen IDs that the manifest knows about
    let mut live_gens: NodeIdSet = NodeIdSet::default();
    live_gens.insert(manifest.active_wal_generation_id);
    for epoch in &manifest.pending_flush_epochs {
        live_gens.insert(epoch.wal_generation_id);
    }
    let entries = match std::fs::read_dir(db_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("wal_") {
            if let Some(id_str) = rest.strip_suffix(".wal") {
                if let Ok(gen_id) = id_str.parse::<u64>() {
                    if !live_gens.contains(&gen_id) {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    }
}

/// Test helpers: access internal state for validation.
#[cfg(test)]
impl EngineCore {
    pub(crate) fn degree_cache_entry(&self, node_id: u64) -> DegreeEntry {
        self.read_view().degree_entry_for_test(node_id)
    }

    pub(crate) fn immutable_memtable_count(&self) -> usize {
        self.immutable_epochs.len()
    }

    pub(crate) fn active_wal_generation(&self) -> u64 {
        self.active_wal_generation_id
    }

    pub(crate) fn engine_seq_for_test(&self) -> u64 {
        self.engine_seq
    }

    pub(crate) fn immutable_memtable(&self, idx: usize) -> &Memtable {
        &self.immutable_epochs[idx].memtable
    }

    /// Set a one-shot pause hook. Consumed by the next enqueue_flush call.
    /// Returns (ready_rx, release_tx). Test waits on ready_rx to confirm
    /// worker is paused, then sends on release_tx to resume.
    pub(crate) fn set_flush_pause(
        &mut self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.flush_pause.lock().unwrap() = Some(FlushPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    pub(crate) fn set_flush_publish_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.flush_publish_pause.lock().unwrap() = Some(FlushPublishPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    pub(crate) fn set_bg_compact_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.bg_compact_pause.lock().unwrap() = Some(BgCompactPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    /// Set a one-shot failure injection. The next `enqueue_flush` call will
    /// produce a BgFlushWork with `force_write_error = true`, causing the worker
    /// to return an error without writing a segment. Only affects one enqueue;
    /// subsequent enqueues are normal. Best used with a single pending epoch.
    pub(crate) fn set_flush_force_error(&mut self) {
        self.flush_force_error = true;
    }

    pub(crate) fn set_secondary_index_build_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.secondary_index_build_pause.lock().unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    /// Enqueue one flush (expose for tests).
    pub(crate) fn enqueue_one_flush(&mut self) -> Result<(), EngineError> {
        self.ensure_bg_flush_worker();
        self.enqueue_flush()
    }

    /// Number of immutable epochs (frozen memtables, in-flight or not).
    pub(crate) fn immutable_epoch_count(&self) -> usize {
        self.immutable_epochs.len()
    }

    /// Number of in-flight flushes.
    pub(crate) fn in_flight_count(&self) -> usize {
        self.immutable_epochs.iter().filter(|e| e.in_flight).count()
    }
}

/// Check if all segments have non-overlapping node and edge ID ranges.
/// Returns true when every record ID appears in at most one segment,
/// the common case for append-only workloads without updates.
fn segments_are_non_overlapping(segments: &[Arc<SegmentReader>]) -> bool {
    // Check node ID ranges
    let mut node_ranges: Vec<(u64, u64)> =
        segments.iter().filter_map(|s| s.node_id_range()).collect();
    node_ranges.sort_unstable_by_key(|(min, _)| *min);
    for window in node_ranges.windows(2) {
        if window[0].1 >= window[1].0 {
            return false;
        }
    }

    // Check edge ID ranges
    let mut edge_ranges: Vec<(u64, u64)> =
        segments.iter().filter_map(|s| s.edge_id_range()).collect();
    edge_ranges.sort_unstable_by_key(|(min, _)| *min);
    for window in edge_ranges.windows(2) {
        if window[0].1 >= window[1].0 {
            return false;
        }
    }

    true
}

fn select_compaction_path(
    segments: &[Arc<SegmentReader>],
    has_tombstones: bool,
    has_active_prune_policies: bool,
) -> CompactionPath {
    if !has_tombstones && !has_active_prune_policies && segments_are_non_overlapping(segments) {
        CompactionPath::FastMerge
    } else {
        CompactionPath::UnifiedV3
    }
}

fn send_bg_flush_event(
    tx: &std::sync::mpsc::SyncSender<BgFlushEvent>,
    events_ready: &AtomicUsize,
    runtime: &Option<std::sync::Weak<DbRuntime>>,
    event: BgFlushEvent,
) {
    if tx.send(event).is_ok() {
        events_ready.fetch_add(1, Ordering::Release);
        if let Some(runtime) = runtime.as_ref().and_then(|weak| weak.upgrade()) {
            runtime.notify_lifecycle_work();
        }
    }
}

/// Background build worker. Writes immutable epochs to segment directories and
/// hands only durably renamed results to the publisher thread.
fn bg_flush_build_worker(
    rx: std::sync::mpsc::Receiver<BgFlushWork>,
    built_tx: std::sync::mpsc::SyncSender<BuiltFlushResult>,
    event_tx: std::sync::mpsc::SyncSender<BgFlushEvent>,
    cancel: Arc<AtomicBool>,
    events_ready: Arc<AtomicUsize>,
    runtime: Option<std::sync::Weak<DbRuntime>>,
    secondary_index_entries: Arc<RwLock<SecondaryIndexEntries>>,
) {
    while let Ok(work) = rx.recv() {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        // Test hooks: one-shot pause and failure injection.
        #[cfg(test)]
        {
            if let Some(hook) = work.pause {
                let _ = hook.ready_tx.send(()); // signal "I'm paused"
                let _ = hook.release_rx.recv(); // block until test releases
            }
            if work.force_write_error {
                send_bg_flush_event(
                    &event_tx,
                    &events_ready,
                    &runtime,
                    BgFlushEvent::Failed(FlushPipelineError {
                        epoch_id: work.epoch_id,
                        wal_generation_id: work.wal_gen_id,
                        stage: FlushPipelineStage::Build,
                        message: "injected test failure".into(),
                    }),
                );
                cancel.store(true, Ordering::Relaxed);
                break;
            }
        }

        // Ensure segments/ parent exists
        if let Some(parent) = work.tmp_dir.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let current_secondary_indexes = secondary_index_entries.read().unwrap().clone();
        let maintained_equality_index_ids = equality_index_ids_snapshot(&current_secondary_indexes);
        let maintained_range_index_ids = range_index_ids_snapshot(&current_secondary_indexes);
        let needs_reseed = current_secondary_indexes.iter().any(|entry| {
            !work
                .frozen
                .secondary_index_declarations()
                .contains_key(&entry.index_id)
        });
        let reseeded_frozen = needs_reseed.then(|| {
            let memtable = (*work.frozen).clone();
            for entry in &current_secondary_indexes {
                memtable.register_secondary_index(entry);
            }
            memtable
        });
        let frozen_ref: &Memtable = if let Some(memtable) = reseeded_frozen.as_ref() {
            memtable
        } else {
            work.frozen.as_ref()
        };

        let built_result = match write_segment_with_degree_overlay_and_secondary_indexes(
            &work.tmp_dir,
            work.seg_id,
            frozen_ref,
            work.dense_config.as_ref(),
            work.degree_overlay.as_ref(),
            &current_secondary_indexes,
        ) {
            Ok(seg_info) => match std::fs::rename(&work.tmp_dir, &work.final_dir) {
                Ok(()) => {
                    if let Some(parent) = work.final_dir.parent() {
                        if let Err(e) = fsync_dir(parent) {
                            let _ = std::fs::remove_dir_all(&work.final_dir);
                            send_bg_flush_event(
                                &event_tx,
                                &events_ready,
                                &runtime,
                                BgFlushEvent::Failed(FlushPipelineError {
                                    epoch_id: work.epoch_id,
                                    wal_generation_id: work.wal_gen_id,
                                    stage: FlushPipelineStage::Build,
                                    message: format!("segment parent fsync failed: {}", e),
                                }),
                            );
                            cancel.store(true, Ordering::Relaxed);
                            break;
                        } else {
                            BuiltFlushResult {
                                epoch_id: work.epoch_id,
                                wal_gen_to_retire: work.wal_gen_id,
                                seg_info,
                                seg_id: work.seg_id,
                                final_dir: work.final_dir,
                                dense_config: work.dense_config,
                                maintained_equality_index_ids,
                                maintained_range_index_ids,
                            }
                        }
                    } else {
                        let _ = std::fs::remove_dir_all(&work.final_dir);
                        send_bg_flush_event(
                            &event_tx,
                            &events_ready,
                            &runtime,
                            BgFlushEvent::Failed(FlushPipelineError {
                                epoch_id: work.epoch_id,
                                wal_generation_id: work.wal_gen_id,
                                stage: FlushPipelineStage::Build,
                                message: "segment final dir is missing a parent".into(),
                            }),
                        );
                        cancel.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                Err(e) => {
                    let _ = std::fs::remove_dir_all(&work.tmp_dir);
                    send_bg_flush_event(
                        &event_tx,
                        &events_ready,
                        &runtime,
                        BgFlushEvent::Failed(FlushPipelineError {
                            epoch_id: work.epoch_id,
                            wal_generation_id: work.wal_gen_id,
                            stage: FlushPipelineStage::Build,
                            message: format!("segment rename failed: {}", e),
                        }),
                    );
                    cancel.store(true, Ordering::Relaxed);
                    break;
                }
            },
            Err(e) => {
                let _ = std::fs::remove_dir_all(&work.tmp_dir);
                send_bg_flush_event(
                    &event_tx,
                    &events_ready,
                    &runtime,
                    BgFlushEvent::Failed(FlushPipelineError {
                        epoch_id: work.epoch_id,
                        wal_generation_id: work.wal_gen_id,
                        stage: FlushPipelineStage::Build,
                        message: format!("segment write failed: {}", e),
                    }),
                );
                cancel.store(true, Ordering::Relaxed);
                break;
            }
        };

        if built_tx.send(built_result).is_err() {
            break;
        }
    }
}

/// Publisher worker. Converts built segment outputs into durable manifest
/// state and emits cheap foreground adoption payloads.
#[allow(clippy::too_many_arguments)]
fn bg_flush_publish_worker(
    db_dir: PathBuf,
    rx: std::sync::mpsc::Receiver<BuiltFlushResult>,
    event_tx: std::sync::mpsc::SyncSender<BgFlushEvent>,
    manifest_write_lock: Arc<Mutex<()>>,
    secondary_index_catalog: Arc<RwLock<SecondaryIndexCatalog>>,
    secondary_index_entries: Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: Arc<AtomicU64>,
    next_edge_id_seen: Arc<AtomicU64>,
    engine_seq_seen: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    events_ready: Arc<AtomicUsize>,
    runtime: Option<std::sync::Weak<DbRuntime>>,
    #[cfg(test)] publish_pause: Arc<Mutex<Option<FlushPublishPauseHook>>>,
) {
    while let Ok(result) = rx.recv() {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        let reader = match SegmentReader::open(
            &result.final_dir,
            result.seg_id,
            result.dense_config.as_ref(),
        ) {
            Ok(reader) => reader,
            Err(e) => {
                send_bg_flush_event(
                    &event_tx,
                    &events_ready,
                    &runtime,
                    BgFlushEvent::Failed(FlushPipelineError {
                        epoch_id: result.epoch_id,
                        wal_generation_id: result.wal_gen_to_retire,
                        stage: FlushPipelineStage::PublishOpenReader,
                        message: format!("failed to open segment {}: {}", result.seg_id, e),
                    }),
                );
                cancel.store(true, Ordering::Relaxed);
                break;
            }
        };
        if !reader.degree_delta_available() {
            send_bg_flush_event(
                &event_tx,
                &events_ready,
                &runtime,
                BgFlushEvent::Failed(FlushPipelineError {
                    epoch_id: result.epoch_id,
                    wal_generation_id: result.wal_gen_to_retire,
                    stage: FlushPipelineStage::PublishOpenReader,
                    message: format!(
                        "segment {} degree sidecar is missing or invalid",
                        result.seg_id
                    ),
                }),
            );
            cancel.store(true, Ordering::Relaxed);
            break;
        }

        #[cfg(test)]
        if let Some(hook) = publish_pause.lock().unwrap().take() {
            let _ = hook.ready_tx.send(());
            let _ = hook.release_rx.recv();
        }

        let publish_result: Result<(Vec<u64>, Vec<u64>), EngineError> = (|| {
            let _guard = manifest_write_lock.lock().unwrap();
            let mut manifest = load_manifest_readonly(&db_dir)?
                .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
            if !manifest
                .segments
                .iter()
                .any(|seg| seg.id == result.seg_info.id)
            {
                manifest.segments.push(result.seg_info.clone());
            }
            let pending_idx = manifest
                .pending_flush_epochs
                .iter()
                .position(|epoch| {
                    epoch.epoch_id == result.epoch_id
                        && epoch.wal_generation_id == result.wal_gen_to_retire
                        && epoch.state == FlushEpochState::FrozenPendingFlush
                })
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "missing FrozenPendingFlush epoch {} wal {} during publish",
                        result.epoch_id, result.wal_gen_to_retire
                    ))
                })?;
            manifest.pending_flush_epochs.remove(pending_idx);
            manifest.next_node_id = manifest
                .next_node_id
                .max(next_node_id_seen.load(Ordering::Acquire));
            manifest.next_edge_id = manifest
                .next_edge_id
                .max(next_edge_id_seen.load(Ordering::Acquire));
            manifest.next_engine_seq = manifest
                .next_engine_seq
                .max(engine_seq_seen.load(Ordering::Acquire));
            let rebuild_equality_index_ids = reconcile_background_output_equality_declarations(
                &mut manifest,
                &result.maintained_equality_index_ids,
            );
            let rebuild_range_index_ids = reconcile_background_output_range_declarations(
                &mut manifest,
                &result.maintained_range_index_ids,
            );
            write_manifest(&db_dir, &manifest)?;
            sync_secondary_index_runtime_state(
                &secondary_index_catalog,
                &secondary_index_entries,
                &manifest.secondary_indexes,
            )?;
            Ok((rebuild_equality_index_ids, rebuild_range_index_ids))
        })();

        let (rebuild_equality_index_ids, rebuild_range_index_ids) = match publish_result {
            Ok(ids) => ids,
            Err(e) => {
                send_bg_flush_event(
                    &event_tx,
                    &events_ready,
                    &runtime,
                    BgFlushEvent::Failed(FlushPipelineError {
                        epoch_id: result.epoch_id,
                        wal_generation_id: result.wal_gen_to_retire,
                        stage: FlushPipelineStage::PublishManifest,
                        message: e.to_string(),
                    }),
                );
                cancel.store(true, Ordering::Relaxed);
                break;
            }
        };

        let _ = remove_wal_generation(&db_dir, result.wal_gen_to_retire);
        {
            let entries = secondary_index_entries.read().unwrap().clone();
            for entry in &entries {
                reader.warm_declared_index_runtime_coverage(entry);
            }
        }
        send_bg_flush_event(
            &event_tx,
            &events_ready,
            &runtime,
            BgFlushEvent::Adopt(PublishedFlushAdoption {
                epoch_id: result.epoch_id,
                wal_gen_to_retire: result.wal_gen_to_retire,
                seg_info: result.seg_info,
                reader,
                rebuild_equality_index_ids,
                rebuild_range_index_ids,
            }),
        );
    }
}

/// Background compaction worker. Runs on a spawned thread.
/// Re-opens input segments via independent mmap handles, merges them into a
/// single output segment, and returns the result for the main thread to apply.
#[allow(clippy::too_many_arguments)]
fn bg_compact_worker(
    db_dir: PathBuf,
    seg_id: u64,
    input_segments: Vec<(u64, PathBuf)>,
    prune_policies: Vec<PrunePolicy>,
    dense_vector: Option<DenseVectorConfig>,
    secondary_indexes: SecondaryIndexEntries,
    cancel: &AtomicBool,
    #[cfg(test)] compact_pause: &Arc<Mutex<Option<BgCompactPauseHook>>>,
) -> Result<BgCompactResult, EngineError> {
    let compact_start = std::time::Instant::now();
    let maintained_equality_index_ids = equality_index_ids_snapshot(&secondary_indexes);
    let maintained_range_index_ids = range_index_ids_snapshot(&secondary_indexes);

    #[cfg(test)]
    if let Some(hook) = compact_pause.lock().unwrap().take() {
        let _ = hook.ready_tx.send(());
        let _ = hook.release_rx.recv();
    }

    // Re-open input segments (independent mmap handles, safe to use concurrently
    // with the main thread's readers of the same files).
    let mut segments = Vec::with_capacity(input_segments.len());
    for (id, path) in &input_segments {
        segments.push(Arc::new(SegmentReader::open(
            path,
            *id,
            dense_vector.as_ref(),
        )?));
    }

    let input_segment_count = segments.len();
    let total_input_nodes: u64 = segments.iter().map(|s| s.node_count()).sum();
    let total_input_edges: u64 = segments.iter().map(|s| s.edge_count()).sum();

    let has_tombstones = segments.iter().any(|s| s.has_tombstones());
    let degree_sidecar_expected =
        prune_policies.is_empty() && segments.iter().all(|s| s.degree_delta_available());
    let compaction_path =
        select_compaction_path(&segments, has_tombstones, !prune_policies.is_empty());

    let segments_dir = db_dir.join("segments");
    std::fs::create_dir_all(&segments_dir)?;
    let tmp_dir = segment_tmp_dir(&db_dir, seg_id);
    let final_dir = segment_dir(&db_dir, seg_id);

    let (seg_info, nodes_auto_pruned, edges_auto_pruned, secondary_index_report) =
        match compaction_path {
            CompactionPath::FastMerge => {
                match bg_fast_merge(
                    &segments,
                    &tmp_dir,
                    seg_id,
                    dense_vector.as_ref(),
                    &secondary_indexes,
                    cancel,
                ) {
                    Ok((seg_info, report)) => (seg_info, 0, 0, report),
                    Err(e) => {
                        let _ = std::fs::remove_dir_all(&tmp_dir);
                        return Err(e);
                    }
                }
            }
            CompactionPath::UnifiedV3 => match bg_standard_merge(
                &segments,
                &tmp_dir,
                seg_id,
                has_tombstones,
                &prune_policies,
                dense_vector.as_ref(),
                &secondary_indexes,
                cancel,
            ) {
                Ok(result) => result,
                Err(e) => {
                    let _ = std::fs::remove_dir_all(&tmp_dir);
                    return Err(e);
                }
            },
        };

    // Atomic rename tmp → final
    if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
        let _ = std::fs::remove_dir_all(&tmp_dir);
        return Err(e.into());
    }

    // Open the output segment reader (will be sent back to the main thread).
    let reader = match SegmentReader::open(&final_dir, seg_id, dense_vector.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(e);
        }
    };
    if degree_sidecar_expected && !reader.degree_delta_available() {
        let _ = std::fs::remove_dir_all(&final_dir);
        return Err(EngineError::CorruptRecord(format!(
            "background compaction output segment {} degree sidecar is missing or invalid",
            seg_id
        )));
    }
    for entry in &secondary_indexes {
        reader.warm_declared_index_runtime_coverage(entry);
    }

    let input_segment_ids: NodeIdSet = input_segments.iter().map(|(id, _)| *id).collect();
    let old_seg_dirs: Vec<PathBuf> = input_segments
        .iter()
        .map(|(id, _)| segment_dir(&db_dir, *id))
        .collect();

    let stats = CompactionStats {
        segments_merged: input_segment_count,
        nodes_kept: seg_info.node_count,
        nodes_removed: total_input_nodes.saturating_sub(seg_info.node_count),
        edges_kept: seg_info.edge_count,
        edges_removed: total_input_edges.saturating_sub(seg_info.edge_count),
        duration_ms: compact_start.elapsed().as_millis() as u64,
        output_segment_id: seg_id,
        nodes_auto_pruned,
        edges_auto_pruned,
    };

    Ok(BgCompactResult {
        seg_info,
        reader,
        old_seg_dirs,
        stats,
        input_segment_ids,
        maintained_equality_index_ids,
        maintained_range_index_ids,
        secondary_index_report,
    })
}

// ========================================================================================
// V3 Compaction: Metadata-only planning + raw binary copy
// ========================================================================================

/// Winning node record with full metadata from sidecar. Avoids re-reading sidecar in output.
struct NodeWinner {
    seg_idx: usize,
    data_offset: u64,
    data_len: u32,
    type_id: u32,
    updated_at: i64,
    weight: f32,
    key_len: u16,
    dense_vector_offset: u64,
    dense_vector_len: u32,
    sparse_vector_offset: u64,
    sparse_vector_len: u32,
    last_write_seq: u64,
}

/// Winning edge record with full metadata from sidecar. Avoids re-reading sidecar in output.
struct EdgeWinner {
    seg_idx: usize,
    data_offset: u64,
    data_len: u32,
    from: u64,
    to: u64,
    type_id: u32,
    updated_at: i64,
    weight: f32,
    valid_from: i64,
    valid_to: i64,
    last_write_seq: u64,
}

/// Result of V3 compaction planning: which records survive and pruning stats.
struct V3Plan {
    node_winners: BTreeMap<u64, NodeWinner>,
    edge_winners: BTreeMap<u64, EdgeWinner>,
    pruned_node_ids: NodeIdSet,
    edges_auto_pruned: u64,
}

/// Check whether a node's metadata fields match any registered prune policy.
/// OR across policies (any match → pruned), AND within each policy.
/// Uses the shared `matches_prune_cutoff` helper (same logic as read-time filtering).
fn matches_any_prune_policy_meta(
    type_id: u32,
    updated_at: i64,
    weight: f32,
    policies: &[PrunePolicy],
    now: i64,
) -> bool {
    for policy in policies {
        let age_cutoff = policy.max_age_ms.map(|age| now - age);
        if matches_prune_cutoff(
            type_id,
            updated_at,
            weight,
            age_cutoff,
            policy.max_weight,
            policy.type_id,
        ) {
            return true;
        }
    }
    false
}

/// V3 compaction planner: select winning records from metadata sidecars only.
///
/// Iterates segments newest-first (first seen per ID wins). Applies tombstone
/// filtering, prune policy evaluation, and edge cascade, all from metadata
/// fields without decoding full records.
fn v3_plan_winners(
    segments: &[Arc<SegmentReader>],
    prune_policies: &[PrunePolicy],
    deleted_nodes: &NodeIdSet,
    deleted_edges: &NodeIdSet,
) -> Result<V3Plan, EngineError> {
    let now = now_millis();
    let has_policies = !prune_policies.is_empty();

    // --- Select winning nodes from metadata (newest-first, first seen wins) ---
    let mut node_winners: BTreeMap<u64, NodeWinner> = BTreeMap::new();
    let mut pruned_node_ids: NodeIdSet = NodeIdSet::default();
    let mut seen_nodes: NodeIdSet = NodeIdSet::default();

    for (seg_idx, seg) in segments.iter().enumerate() {
        let count = seg.node_meta_count() as usize;
        for i in 0..count {
            let (
                node_id,
                data_offset,
                data_len,
                type_id,
                updated_at,
                weight,
                key_len,
                _prop_hash_offset,
                _prop_hash_count,
                last_write_seq,
            ) = seg.node_meta_at(i)?;
            let (dense_vector_offset, dense_vector_len, sparse_vector_offset, sparse_vector_len) =
                seg.node_vector_meta_at(i)?;

            if seen_nodes.contains(&node_id) {
                continue; // Already have a newer version
            }
            seen_nodes.insert(node_id);

            if deleted_nodes.contains(&node_id) {
                continue; // Tombstoned
            }

            if has_policies
                && matches_any_prune_policy_meta(type_id, updated_at, weight, prune_policies, now)
            {
                pruned_node_ids.insert(node_id);
                continue;
            }

            node_winners.insert(
                node_id,
                NodeWinner {
                    seg_idx,
                    data_offset,
                    data_len,
                    type_id,
                    updated_at,
                    weight,
                    key_len,
                    dense_vector_offset,
                    dense_vector_len,
                    sparse_vector_offset,
                    sparse_vector_len,
                    last_write_seq,
                },
            );
        }
    }

    // --- Select winning edges from metadata ---
    // O(1) endpoint lookup for edge cascade filtering (BTreeMap.contains_key is O(log N))
    let surviving_node_ids: NodeIdSet = node_winners.keys().copied().collect();
    let mut edge_winners: BTreeMap<u64, EdgeWinner> = BTreeMap::new();
    let mut seen_edges: NodeIdSet = NodeIdSet::default();
    let mut edges_auto_pruned: u64 = 0;

    for (seg_idx, seg) in segments.iter().enumerate() {
        let count = seg.edge_meta_count() as usize;
        for i in 0..count {
            let (
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
            ) = seg.edge_meta_at(i)?;

            if !seen_edges.insert(edge_id) {
                continue;
            }

            if deleted_edges.contains(&edge_id) {
                continue;
            }

            // Skip edges whose endpoints are tombstoned
            if deleted_nodes.contains(&from) || deleted_nodes.contains(&to) {
                continue;
            }

            // Cascade: drop if either endpoint is not a winner (pruned or missing)
            if !surviving_node_ids.contains(&from) || !surviving_node_ids.contains(&to) {
                if pruned_node_ids.contains(&from) || pruned_node_ids.contains(&to) {
                    edges_auto_pruned += 1;
                }
                continue;
            }

            edge_winners.insert(
                edge_id,
                EdgeWinner {
                    seg_idx,
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
                },
            );
        }
    }

    Ok(V3Plan {
        node_winners,
        edge_winners,
        pruned_node_ids,
        edges_auto_pruned,
    })
}

/// Build the output segment from a V3 plan: raw-copy data files, build indexes
/// from metadata sidecars (no Memtable decode).
fn v3_build_output(
    tmp_dir: &Path,
    seg_id: u64,
    segments: &[Arc<SegmentReader>],
    plan: &V3Plan,
    dense_config: Option<&DenseVectorConfig>,
    write_degree_sidecar: bool,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<(SegmentInfo, SecondaryIndexMaintenanceReport), EngineError> {
    std::fs::create_dir_all(tmp_dir)?;

    // Prepare sorted winner lists for the materializer: (id, seg_idx, data_offset, data_len)
    let node_winner_list: Vec<(u64, usize, u64, u32)> = plan
        .node_winners
        .iter()
        .map(|(&id, w)| (id, w.seg_idx, w.data_offset, w.data_len))
        .collect();
    let edge_winner_list: Vec<(u64, usize, u64, u32)> = plan
        .edge_winners
        .iter()
        .map(|(&id, w)| (id, w.seg_idx, w.data_offset, w.data_len))
        .collect();

    // Raw-copy winning records to output data files
    let node_data = write_v3_nodes_dat(tmp_dir, segments, &node_winner_list)?;
    let edge_data = write_v3_edges_dat(tmp_dir, segments, &edge_winner_list)?;

    // Build CompactNodeMeta/CompactEdgeMeta by zipping planner winners with output offsets.
    // Both are sorted by ID (BTreeMap iteration + write order), so a linear zip replaces
    // HashMap lookups. Runtime checks guard against length/ID divergence.
    if plan.node_winners.len() != node_data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "compaction node winner count ({}) != output data count ({})",
            plan.node_winners.len(),
            node_data.len()
        )));
    }
    if plan.edge_winners.len() != edge_data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "compaction edge winner count ({}) != output data count ({})",
            plan.edge_winners.len(),
            edge_data.len()
        )));
    }

    let mut node_metas = Vec::with_capacity(plan.node_winners.len());
    for ((&node_id, w), &(data_id, new_data_offset, data_len)) in
        plan.node_winners.iter().zip(node_data.iter())
    {
        if node_id != data_id {
            return Err(EngineError::CorruptRecord(format!(
                "compaction node ID mismatch: winner={}, data={}",
                node_id, data_id
            )));
        }
        node_metas.push(CompactNodeMeta {
            node_id,
            new_data_offset,
            data_len,
            type_id: w.type_id,
            updated_at: w.updated_at,
            weight: w.weight,
            key_len: w.key_len,
            dense_vector_offset: w.dense_vector_offset,
            dense_vector_len: w.dense_vector_len,
            sparse_vector_offset: w.sparse_vector_offset,
            sparse_vector_len: w.sparse_vector_len,
            src_seg_idx: w.seg_idx,
            src_data_offset: w.data_offset,
            last_write_seq: w.last_write_seq,
        });
    }

    let mut edge_metas = Vec::with_capacity(plan.edge_winners.len());
    for ((&edge_id, w), &(data_id, new_data_offset, data_len)) in
        plan.edge_winners.iter().zip(edge_data.iter())
    {
        if edge_id != data_id {
            return Err(EngineError::CorruptRecord(format!(
                "compaction edge ID mismatch: winner={}, data={}",
                edge_id, data_id
            )));
        }
        edge_metas.push(CompactEdgeMeta {
            edge_id,
            new_data_offset,
            data_len,
            from: w.from,
            to: w.to,
            type_id: w.type_id,
            updated_at: w.updated_at,
            weight: w.weight,
            valid_from: w.valid_from,
            valid_to: w.valid_to,
            last_write_seq: w.last_write_seq,
        });
    }

    // Build all secondary indexes and sidecars from metadata
    let secondary_index_report = write_indexes_from_metadata_with_secondary_indexes(
        seg_id,
        tmp_dir,
        segments,
        &node_metas,
        &edge_metas,
        dense_config,
        write_degree_sidecar,
        secondary_indexes,
    )?;

    let node_count = plan.node_winners.len() as u64;
    let edge_count = plan.edge_winners.len() as u64;

    Ok((
        SegmentInfo {
            id: seg_id,
            node_count,
            edge_count,
        },
        secondary_index_report,
    ))
}

fn collect_fast_merge_node_metas(
    segments: &[Arc<SegmentReader>],
    copy_info: &[FastMergeCopyInfo],
) -> Result<Vec<CompactNodeMeta>, EngineError> {
    let mut metas = Vec::new();
    for (seg_idx, seg) in segments.iter().enumerate() {
        let info = &copy_info[seg_idx];
        for i in 0..seg.node_meta_count() as usize {
            let (
                node_id,
                data_offset,
                data_len,
                type_id,
                updated_at,
                weight,
                key_len,
                _prop_hash_offset,
                _prop_hash_count,
                last_write_seq,
            ) = seg.node_meta_at(i)?;
            let (dense_vector_offset, dense_vector_len, sparse_vector_offset, sparse_vector_len) =
                seg.node_vector_meta_at(i)?;
            let rebased_offset =
                info.new_data_base
                    .checked_add(data_offset.checked_sub(info.orig_data_start).ok_or_else(
                        || {
                            EngineError::CorruptRecord(format!(
                                "segment {} node {} data offset {} precedes data section {}",
                                seg.segment_id, node_id, data_offset, info.orig_data_start
                            ))
                        },
                    )?)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "segment {} node {} merged offset overflow",
                            seg.segment_id, node_id
                        ))
                    })?;
            metas.push(CompactNodeMeta {
                node_id,
                new_data_offset: rebased_offset,
                data_len,
                type_id,
                updated_at,
                weight,
                key_len,
                dense_vector_offset,
                dense_vector_len,
                sparse_vector_offset,
                sparse_vector_len,
                src_seg_idx: seg_idx,
                src_data_offset: data_offset,
                last_write_seq,
            });
        }
    }
    metas.sort_unstable_by_key(|m| m.node_id);
    for pair in metas.windows(2) {
        if pair[0].node_id == pair[1].node_id {
            return Err(EngineError::CorruptRecord(format!(
                "fast-merge requires non-overlapping node IDs, found duplicate {}",
                pair[0].node_id
            )));
        }
    }
    Ok(metas)
}

fn collect_fast_merge_edge_metas(
    segments: &[Arc<SegmentReader>],
    copy_info: &[FastMergeCopyInfo],
) -> Result<Vec<CompactEdgeMeta>, EngineError> {
    let mut metas = Vec::new();
    for (seg_idx, seg) in segments.iter().enumerate() {
        let info = &copy_info[seg_idx];
        for i in 0..seg.edge_meta_count() as usize {
            let (
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
            ) = seg.edge_meta_at(i)?;
            let rebased_offset =
                info.new_data_base
                    .checked_add(data_offset.checked_sub(info.orig_data_start).ok_or_else(
                        || {
                            EngineError::CorruptRecord(format!(
                                "segment {} edge {} data offset {} precedes data section {}",
                                seg.segment_id, edge_id, data_offset, info.orig_data_start
                            ))
                        },
                    )?)
                    .ok_or_else(|| {
                        EngineError::CorruptRecord(format!(
                            "segment {} edge {} merged offset overflow",
                            seg.segment_id, edge_id
                        ))
                    })?;
            metas.push(CompactEdgeMeta {
                edge_id,
                new_data_offset: rebased_offset,
                data_len,
                from,
                to,
                type_id,
                updated_at,
                weight,
                valid_from,
                valid_to,
                last_write_seq,
            });
        }
    }
    metas.sort_unstable_by_key(|m| m.edge_id);
    for pair in metas.windows(2) {
        if pair[0].edge_id == pair[1].edge_id {
            return Err(EngineError::CorruptRecord(format!(
                "fast-merge requires non-overlapping edge IDs, found duplicate {}",
                pair[0].edge_id
            )));
        }
    }
    Ok(metas)
}

fn build_fast_merge_output(
    tmp_dir: &Path,
    seg_id: u64,
    segments: &[Arc<SegmentReader>],
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<(SegmentInfo, SecondaryIndexMaintenanceReport), EngineError> {
    std::fs::create_dir_all(tmp_dir)?;

    let node_copy_info = write_merged_nodes_dat(tmp_dir, segments)?;
    let edge_copy_info = write_merged_edges_dat(tmp_dir, segments)?;
    let node_metas = collect_fast_merge_node_metas(segments, &node_copy_info)?;
    let edge_metas = collect_fast_merge_edge_metas(segments, &edge_copy_info)?;

    let secondary_index_report = write_indexes_from_metadata_with_secondary_indexes(
        seg_id,
        tmp_dir,
        segments,
        &node_metas,
        &edge_metas,
        dense_config,
        true,
        secondary_indexes,
    )?;

    Ok((
        SegmentInfo {
            id: seg_id,
            node_count: node_metas.len() as u64,
            edge_count: edge_metas.len() as u64,
        },
        secondary_index_report,
    ))
}

/// V3 background merge: metadata-only planning + raw binary copy.
/// Same algorithm as compact_standard but with cancel flag instead of progress callback.
/// Returns `(SegmentInfo, nodes_auto_pruned, edges_auto_pruned)`.
fn bg_fast_merge(
    segments: &[Arc<SegmentReader>],
    tmp_dir: &Path,
    seg_id: u64,
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    cancel: &AtomicBool,
) -> Result<(SegmentInfo, SecondaryIndexMaintenanceReport), EngineError> {
    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    let result =
        build_fast_merge_output(tmp_dir, seg_id, segments, dense_config, secondary_indexes)?;

    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    Ok(result)
}

/// V3 background merge: metadata-only planning + raw binary copy.
/// Same algorithm as compact_standard but with cancel flag instead of progress callback.
/// Returns `(SegmentInfo, nodes_auto_pruned, edges_auto_pruned)`.
#[allow(clippy::too_many_arguments)]
fn bg_standard_merge(
    segments: &[Arc<SegmentReader>],
    tmp_dir: &Path,
    seg_id: u64,
    has_tombstones: bool,
    prune_policies: &[PrunePolicy],
    dense_config: Option<&DenseVectorConfig>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    cancel: &AtomicBool,
) -> Result<(SegmentInfo, u64, u64, SecondaryIndexMaintenanceReport), EngineError> {
    // Collect tombstones
    let mut deleted_nodes: NodeIdSet = NodeIdSet::default();
    let mut deleted_edges: NodeIdSet = NodeIdSet::default();
    if has_tombstones {
        for seg in segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }
    }

    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    // V3 plan: metadata-only winner selection
    let plan = v3_plan_winners(segments, prune_policies, &deleted_nodes, &deleted_edges)?;

    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    let nodes_auto_pruned = plan.pruned_node_ids.len() as u64;
    let edges_auto_pruned = plan.edges_auto_pruned;

    let (seg_info, secondary_index_report) = v3_build_output(
        tmp_dir,
        seg_id,
        segments,
        &plan,
        dense_config,
        prune_policies.is_empty(),
        secondary_indexes,
    )?;

    Ok((
        seg_info,
        nodes_auto_pruned,
        edges_auto_pruned,
        secondary_index_report,
    ))
}

include!("graph_ops.rs");
include!("txn.rs");
include!("write.rs");
include!("read.rs");
include!("query_ir.rs");
include!("query_plan.rs");
include!("query_exec.rs");
include!("query.rs");

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_node(id: u64, key: &str) -> NodeRecord {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String(key.to_string()));
        NodeRecord {
            id,
            type_id: 1,
            key: key.to_string(),
            props,
            created_at: 1000 * id as i64,
            updated_at: 1000 * id as i64 + 1,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_edge(id: u64, from: u64, to: u64) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            type_id: 10,
            props: BTreeMap::new(),
            created_at: 2000 * id as i64,
            updated_at: 2000 * id as i64 + 1,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }

    include!("tests/lifecycle.rs");
    include!("tests/txn.rs");
    include!("tests/write.rs");
    include!("tests/read.rs");
    include!("tests/graph_ops.rs");
    include!("tests/query_planner.rs");
}
