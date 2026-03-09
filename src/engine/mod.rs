use crate::error::EngineError;
use crate::manifest::{default_manifest, load_manifest, write_manifest};
use crate::memtable::Memtable;
use crate::segment_reader::SegmentReader;
use crate::segment_writer::{
    segment_dir, segment_tmp_dir, write_indexes_from_metadata, write_merged_edges_dat,
    write_merged_nodes_dat, write_segment, write_v3_edges_dat, write_v3_nodes_dat, CompactEdgeMeta,
    CompactNodeMeta, FastMergeCopyInfo,
};
use crate::types::*;
use crate::wal::{truncate_wal, WalReader, WalWriter};
use crate::wal_sync::{shutdown_sync_thread, sync_thread_loop, WalSyncState};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::SystemTime;

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
    deleted: &HashSet<u64>,
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

/// Returns true if an edge is valid (not expired, not future) at the given reference time.
/// Same predicate used by `neighbors()` — extracted to prevent drift.
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
    fn from_manifest(manifest: &ManifestState, now: i64) -> Self {
        let policies = manifest
            .prune_policies
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
        for &(age_cutoff, max_weight, type_id) in &self.policies {
            if matches_prune_cutoff(
                node.type_id,
                node.updated_at,
                node.weight,
                age_cutoff,
                max_weight,
                type_id,
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

/// The core database engine. Manages the lifecycle of an OverGraph database.
///
/// Provides both low-level WAL access (write_op) and high-level graph APIs
/// (upsert_node, upsert_edge, get_node, get_edge, batch operations).
pub struct DatabaseEngine {
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
    memtable: Memtable,
    /// Open segment readers, ordered newest-first for read merging.
    segments: Vec<SegmentReader>,
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
    /// In-progress background compaction, if any.
    bg_compact: Option<BgCompactHandle>,
    /// Timestamp of the last completed compaction (manual or background).
    last_compaction_ms: Option<i64>,
    /// Runtime degree cache: node_id → aggregate degree/weight stats.
    /// Accelerates unfiltered, non-temporal, no-policy degree queries to O(1).
    /// Rebuilt on open() and after compaction; updated incrementally on mutations.
    degree_cache: HashMap<u64, DegreeEntry>,
}

/// Captured pre-mutation edge state for degree cache updates.
struct OldEdgeInfo {
    from: u64,
    to: u64,
    weight: f32,
    valid_at_now: bool,
    created_at: i64,
    valid_from: i64,
    valid_to: i64,
}

#[derive(Clone, Copy)]
struct EdgeCore {
    from: u64,
    to: u64,
    created_at: i64,
    weight: f32,
    valid_from: i64,
    valid_to: i64,
}

/// Handle for an in-progress background compaction thread.
struct BgCompactHandle {
    handle: JoinHandle<Result<BgCompactResult, EngineError>>,
    /// Shared cancel flag. Set to true to request early termination.
    cancel: Arc<AtomicBool>,
}

/// Result returned by the background compaction worker thread.
struct BgCompactResult {
    seg_info: SegmentInfo,
    reader: SegmentReader,
    old_seg_dirs: Vec<PathBuf>,
    stats: CompactionStats,
    input_segment_ids: HashSet<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionPath {
    FastMerge,
    UnifiedV3,
}

impl DatabaseEngine {
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
        let manifest = match load_manifest(path)? {
            Some(m) => m,
            None => {
                let m = default_manifest();
                write_manifest(path, &m)?;
                m
            }
        };

        // Replay WAL to rebuild in-memory state
        let wal_reader = WalReader::new(path);
        let wal_ops = wal_reader.read_all()?;

        let mut memtable = Memtable::new();
        for op in wal_ops {
            memtable.apply_op(&op);
        }

        // Compute next IDs from max of manifest and replayed state
        let next_node_id = manifest
            .next_node_id
            .max(memtable.max_node_id().saturating_add(1));
        let next_edge_id = manifest
            .next_edge_id
            .max(memtable.max_edge_id().saturating_add(1));

        // Load existing segments (newest-first for read merging).
        // Only segments listed in the manifest are opened. Orphan segment
        // directories (from a crash between segment write and manifest update)
        // are intentionally NOT loaded. Their data may be partial or corrupt.
        // Their IDs are still accounted for in next_segment_id below.
        let mut segments = Vec::new();
        for seg_info in manifest.segments.iter().rev() {
            let seg_path = segment_dir(path, seg_info.id);
            if seg_path.exists() {
                let reader = SegmentReader::open(&seg_path, seg_info.id)?;
                segments.push(reader);
            }
        }

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

        // Open WAL writer for new appends
        let wal_writer = WalWriter::open(path)?;

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

        let mut engine = DatabaseEngine {
            db_dir: path.to_path_buf(),
            manifest,
            wal_writer_immediate,
            wal_state,
            sync_thread,
            memtable,
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
            bg_compact: None,
            last_compaction_ms: None,
            degree_cache: HashMap::new(),
        };

        engine.rebuild_degree_cache()?;

        Ok(engine)
    }

    /// Close the database cleanly. Syncs WAL and writes manifest.
    /// Waits for any in-progress background compaction to finish.
    pub fn close(mut self) -> Result<(), EngineError> {
        self.wait_for_bg_compact();
        self.close_inner()
    }

    /// Close the database, cancelling any in-progress background compaction
    /// instead of waiting for it to finish. Use this for fast shutdown when
    /// you don't need the bg compaction result.
    pub fn close_fast(mut self) -> Result<(), EngineError> {
        self.cancel_bg_compact();
        self.close_inner()
    }

    /// Shared close logic: sync WAL, write manifest.
    fn close_inner(&mut self) -> Result<(), EngineError> {
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
        self.manifest.next_node_id = self.next_node_id;
        self.manifest.next_edge_id = self.next_edge_id;
        write_manifest(&self.db_dir, &self.manifest)?;
        Ok(())
    }

    // --- Write path helpers ---

    /// Metadata-only logical edge lookup for degree-cache maintenance.
    /// Checks memtable first, then segments newest-to-oldest, respecting
    /// tombstones. Avoids full property decode on the write path.
    fn get_edge_core_for_cache(&self, id: u64) -> Result<Option<EdgeCore>, EngineError> {
        if let Some(edge) = self.memtable.get_edge(id) {
            return Ok(Some(EdgeCore {
                from: edge.from,
                to: edge.to,
                created_at: edge.created_at,
                weight: edge.weight,
                valid_from: edge.valid_from,
                valid_to: edge.valid_to,
            }));
        }
        if self.memtable.deleted_edges().contains_key(&id) {
            return Ok(None);
        }
        for seg in &self.segments {
            if seg.is_edge_deleted(id) {
                return Ok(None);
            }
            if let Some((from, to, created_at, weight, valid_from, valid_to)) =
                seg.get_edge_core(id)?
            {
                return Ok(Some(EdgeCore {
                    from,
                    to,
                    created_at,
                    weight,
                    valid_from,
                    valid_to,
                }));
            }
        }
        Ok(None)
    }

    /// Capture the pre-mutation edge state needed for degree cache updates.
    fn capture_old_edge_for_cache(&self, op: &WalOp, now: i64) -> Option<OldEdgeInfo> {
        match op {
            WalOp::UpsertEdge(edge) => {
                self.get_edge_core_for_cache(edge.id)
                    .ok()
                    .flatten()
                    .map(|e| OldEdgeInfo {
                        from: e.from,
                        to: e.to,
                        weight: e.weight,
                        valid_at_now: is_edge_valid_at(e.valid_from, e.valid_to, now),
                        created_at: e.created_at,
                        valid_from: e.valid_from,
                        valid_to: e.valid_to,
                    })
            }
            WalOp::DeleteEdge { id, .. } => {
                self.get_edge_core_for_cache(*id)
                    .ok()
                    .flatten()
                    .map(|e| OldEdgeInfo {
                        from: e.from,
                        to: e.to,
                        weight: e.weight,
                        valid_at_now: is_edge_valid_at(e.valid_from, e.valid_to, now),
                        created_at: e.created_at,
                        valid_from: e.valid_from,
                        valid_to: e.valid_to,
                    })
            }
            _ => None,
        }
    }

    /// Increment degree cache for a new edge at the given endpoints.
    /// If the edge is temporal (finite validity window or future-dated),
    /// also increments temporal_edge_count on each endpoint (self-loop: once).
    fn degree_cache_increment(
        &mut self,
        from: u64,
        to: u64,
        weight: f32,
        created_at: i64,
        valid_from: i64,
        valid_to: i64,
    ) {
        let temporal = is_cache_bypass_edge(valid_from, valid_to, created_at);
        let from_entry = self.degree_cache.entry(from).or_default();
        from_entry.out_degree += 1;
        from_entry.out_weight_sum += weight as f64;
        if from == to {
            from_entry.in_degree += 1;
            from_entry.in_weight_sum += weight as f64;
            from_entry.self_loop_count += 1;
            from_entry.self_loop_weight_sum += weight as f64;
            if temporal {
                from_entry.temporal_edge_count += 1;
            }
        } else {
            if temporal {
                from_entry.temporal_edge_count += 1;
            }
            let to_entry = self.degree_cache.entry(to).or_default();
            to_entry.in_degree += 1;
            to_entry.in_weight_sum += weight as f64;
            if temporal {
                to_entry.temporal_edge_count += 1;
            }
        }
    }

    /// Decrement degree cache for a removed edge at the given endpoints.
    /// If the edge is temporal (finite validity window or future-dated),
    /// also decrements temporal_edge_count on each endpoint (self-loop: once).
    fn degree_cache_decrement(
        &mut self,
        from: u64,
        to: u64,
        weight: f32,
        created_at: i64,
        valid_from: i64,
        valid_to: i64,
    ) {
        let temporal = is_cache_bypass_edge(valid_from, valid_to, created_at);
        if let Some(from_entry) = self.degree_cache.get_mut(&from) {
            from_entry.out_degree = from_entry.out_degree.saturating_sub(1);
            from_entry.out_weight_sum -= weight as f64;
            if temporal {
                from_entry.temporal_edge_count = from_entry.temporal_edge_count.saturating_sub(1);
            }
        }
        if from == to {
            if let Some(entry) = self.degree_cache.get_mut(&from) {
                entry.in_degree = entry.in_degree.saturating_sub(1);
                entry.in_weight_sum -= weight as f64;
                entry.self_loop_count = entry.self_loop_count.saturating_sub(1);
                entry.self_loop_weight_sum -= weight as f64;
            }
        } else if let Some(to_entry) = self.degree_cache.get_mut(&to) {
            to_entry.in_degree = to_entry.in_degree.saturating_sub(1);
            to_entry.in_weight_sum -= weight as f64;
            if temporal {
                to_entry.temporal_edge_count = to_entry.temporal_edge_count.saturating_sub(1);
            }
        }
    }

    /// Adjust temporal_edge_count for both endpoints of an edge.
    /// `delta` is +1 (temporal edge added) or -1 (temporal edge removed).
    /// Self-loop: adjusts once.
    fn degree_cache_temporal_adjust(&mut self, from: u64, to: u64, delta: i32) {
        let from_entry = self.degree_cache.entry(from).or_default();
        if delta > 0 {
            from_entry.temporal_edge_count += delta as u32;
        } else {
            from_entry.temporal_edge_count = from_entry
                .temporal_edge_count
                .saturating_sub((-delta) as u32);
        }
        if from != to {
            let to_entry = self.degree_cache.entry(to).or_default();
            if delta > 0 {
                to_entry.temporal_edge_count += delta as u32;
            } else {
                to_entry.temporal_edge_count =
                    to_entry.temporal_edge_count.saturating_sub((-delta) as u32);
            }
        }
    }

    /// Update the degree cache for a single mutation op. Called after apply_op
    /// so the memtable reflects the new state, but we use the pre-captured
    /// old edge state to compute the delta.
    ///
    /// Only edges valid at `now` are counted in the cache. This matches the
    /// semantics of `degree(at_epoch=None)` which uses `now_millis()`.
    fn update_degree_cache_for_op(&mut self, op: &WalOp, old_edge: Option<OldEdgeInfo>, now: i64) {
        match op {
            WalOp::UpsertEdge(edge) => {
                let new_valid = is_edge_valid_at(edge.valid_from, edge.valid_to, now);
                let new_temporal =
                    is_cache_bypass_edge(edge.valid_from, edge.valid_to, edge.created_at);
                match old_edge {
                    None => {
                        // New edge: increment only if valid at now
                        if new_valid {
                            self.degree_cache_increment(
                                edge.from,
                                edge.to,
                                edge.weight,
                                edge.created_at,
                                edge.valid_from,
                                edge.valid_to,
                            );
                        } else if new_temporal {
                            // Not valid now but temporal — track so cache bypasses this node
                            self.degree_cache_temporal_adjust(edge.from, edge.to, 1);
                        }
                    }
                    Some(old) => {
                        let old_temporal =
                            is_cache_bypass_edge(old.valid_from, old.valid_to, old.created_at);
                        if old.from == edge.from && old.to == edge.to {
                            // Same endpoints
                            if old.valid_at_now && new_valid {
                                // Both valid: weight update only + temporal transition
                                let weight_delta = (edge.weight as f64) - (old.weight as f64);
                                if weight_delta.abs() > f64::EPSILON {
                                    let from_entry =
                                        self.degree_cache.entry(edge.from).or_default();
                                    from_entry.out_weight_sum += weight_delta;
                                    if edge.from == edge.to {
                                        from_entry.in_weight_sum += weight_delta;
                                        from_entry.self_loop_weight_sum += weight_delta;
                                    } else {
                                        let to_entry =
                                            self.degree_cache.entry(edge.to).or_default();
                                        to_entry.in_weight_sum += weight_delta;
                                    }
                                }
                                // Temporal status may have changed (timeless→temporal or vice versa)
                                if old_temporal != new_temporal {
                                    let delta: i32 = if new_temporal { 1 } else { -1 };
                                    self.degree_cache_temporal_adjust(edge.from, edge.to, delta);
                                }
                            } else if old.valid_at_now && !new_valid {
                                // Was valid, now invalid (e.g., valid_to set to past)
                                self.degree_cache_decrement(
                                    old.from,
                                    old.to,
                                    old.weight,
                                    old.created_at,
                                    old.valid_from,
                                    old.valid_to,
                                );
                                // If new edge is temporal but not valid, still track it
                                if new_temporal && !old_temporal {
                                    self.degree_cache_temporal_adjust(edge.from, edge.to, 1);
                                }
                            } else if !old.valid_at_now && new_valid {
                                // Was invalid, now valid
                                self.degree_cache_increment(
                                    edge.from,
                                    edge.to,
                                    edge.weight,
                                    edge.created_at,
                                    edge.valid_from,
                                    edge.valid_to,
                                );
                                // If old was temporal (but invalid), remove that tracking
                                if old_temporal && !new_temporal {
                                    self.degree_cache_temporal_adjust(edge.from, edge.to, -1);
                                }
                            } else {
                                // Both invalid: only temporal tracking may change
                                if old_temporal != new_temporal {
                                    let delta: i32 = if new_temporal { 1 } else { -1 };
                                    self.degree_cache_temporal_adjust(edge.from, edge.to, delta);
                                }
                            }
                        } else {
                            // Endpoint change: decrement old if was valid, increment new if valid.
                            // Note: currently unreachable through public APIs (upsert_edge and
                            // graph_patch both deduplicate by (from, to, type_id) triple, never
                            // reusing an edge ID with different endpoints). Kept for defensive
                            // robustness if a future API path allows endpoint reassignment.
                            if old.valid_at_now {
                                self.degree_cache_decrement(
                                    old.from,
                                    old.to,
                                    old.weight,
                                    old.created_at,
                                    old.valid_from,
                                    old.valid_to,
                                );
                            } else if old_temporal {
                                self.degree_cache_temporal_adjust(old.from, old.to, -1);
                            }
                            if new_valid {
                                self.degree_cache_increment(
                                    edge.from,
                                    edge.to,
                                    edge.weight,
                                    edge.created_at,
                                    edge.valid_from,
                                    edge.valid_to,
                                );
                            } else if new_temporal {
                                self.degree_cache_temporal_adjust(edge.from, edge.to, 1);
                            }
                        }
                    }
                }
            }
            WalOp::DeleteEdge { .. } => {
                if let Some(old) = old_edge {
                    if old.valid_at_now {
                        self.degree_cache_decrement(
                            old.from,
                            old.to,
                            old.weight,
                            old.created_at,
                            old.valid_from,
                            old.valid_to,
                        );
                    } else if is_cache_bypass_edge(old.valid_from, old.valid_to, old.created_at) {
                        // Edge wasn't valid but was temporal — remove tracking
                        self.degree_cache_temporal_adjust(old.from, old.to, -1);
                    }
                }
                // If old_edge is None, edge doesn't exist — no cache update needed
                // (matches idempotent tombstone semantics)
            }
            WalOp::DeleteNode { id, .. } => {
                // The cascade delete_node already emits DeleteEdge ops for all
                // incident edges before the DeleteNode op. Those edge deletes
                // update neighbor degrees. We just remove this node's entry.
                self.degree_cache.remove(id);
            }
            WalOp::UpsertNode(_) => {
                // Node upserts don't affect degree
            }
        }
    }

    /// Internal helper that handles WAL append + memtable apply for both sync modes.
    fn append_and_apply(&mut self, ops: &[WalOp]) -> Result<(), EngineError> {
        self.wal_append(|w| w.append_batch(ops))?;
        let now = now_millis();
        for op in ops {
            let old_edge = self.capture_old_edge_for_cache(op, now);
            self.memtable.apply_op(op);
            self.update_degree_cache_for_op(op, old_edge, now);
        }
        Ok(())
    }

    /// Internal helper for a single WAL op (avoids vec allocation for common case).
    fn append_and_apply_one(&mut self, op: &WalOp) -> Result<(), EngineError> {
        self.wal_append(|w| w.append(op))?;
        let now = now_millis();
        let old_edge = self.capture_old_edge_for_cache(op, now);
        self.memtable.apply_op(op);
        self.update_degree_cache_for_op(op, old_edge, now);
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

    /// Flush the current memtable to an immutable on-disk segment.
    ///
    /// 1. Freeze current memtable, replace with empty one
    /// 2. Write segment to tmp directory
    /// 3. Atomic rename tmp → final
    /// 4. Update manifest with new segment
    /// 5. Truncate WAL
    /// 6. Re-open WAL writer (Immediate) or truncate-and-reset (GroupCommit)
    /// 7. Open segment reader for the new segment
    ///
    /// Returns the new SegmentInfo, or None if memtable was empty.
    pub fn flush(&mut self) -> Result<Option<SegmentInfo>, EngineError> {
        // Pick up any completed background compaction before proceeding.
        self.try_complete_bg_compact();

        if self.memtable.is_empty() {
            return Ok(None);
        }

        // Sync WAL before flush to ensure all ops are durable.
        // In GroupCommit mode, only sync here. Truncation happens AFTER the
        // segment + manifest are written to disk (see below), to avoid data loss
        // if a crash occurs between WAL truncation and segment write.
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
                // Lock released here. Safe because &mut self prevents concurrent writes
            }
        }

        // Freeze memtable: swap in empty, keep frozen for restore on failure
        let frozen = std::mem::take(&mut self.memtable);

        let seg_id = self.next_segment_id;
        self.next_segment_id += 1;

        let tmp_dir = segment_tmp_dir(&self.db_dir, seg_id);
        let final_dir = segment_dir(&self.db_dir, seg_id);

        // Ensure segments/ directory exists
        let segments_dir = self.db_dir.join("segments");
        if let Err(e) = std::fs::create_dir_all(&segments_dir) {
            self.memtable = frozen;
            self.next_segment_id -= 1;
            return Err(e.into());
        }

        // Write segment files to tmp directory
        let seg_info = match write_segment(&tmp_dir, seg_id, &frozen) {
            Ok(info) => info,
            Err(e) => {
                self.memtable = frozen;
                self.next_segment_id -= 1;
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(e);
            }
        };

        // Atomic rename tmp → final
        if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
            self.memtable = frozen;
            self.next_segment_id -= 1;
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(e.into());
        }

        // Update manifest
        self.manifest.segments.push(seg_info.clone());
        self.manifest.next_node_id = self.next_node_id;
        self.manifest.next_edge_id = self.next_edge_id;
        write_manifest(&self.db_dir, &self.manifest)?;

        // Truncate WAL (all data now safe in segment + manifest).
        // This MUST happen after the manifest is durable. Otherwise a crash
        // between WAL truncation and manifest write would lose data.
        match &self.wal_sync_mode {
            WalSyncMode::Immediate => {
                truncate_wal(&self.db_dir)?;
                self.wal_writer_immediate = Some(WalWriter::open(&self.db_dir)?);
            }
            WalSyncMode::GroupCommit { .. } => {
                // Hold the lock across truncate_and_reset to prevent the sync thread
                // from accessing a stale/mid-truncate writer handle.
                let arc = self.wal_state.as_ref().expect("group commit WAL state");
                let (lock, cvar) = &**arc;
                let mut state = lock.lock().unwrap();
                state.wal_writer.truncate_and_reset()?;
                cvar.notify_all();
            }
        }

        // Open segment reader (newest-first: insert at front)
        let reader = SegmentReader::open(&final_dir, seg_id)?;
        self.segments.insert(0, reader);

        // Auto-compact: track flushes and trigger background compaction when
        // threshold is reached. Skip when we're already inside a sync compact()
        // call (re-entry guard) or when a background compaction is in progress.
        if !self.compacting && self.bg_compact.is_none() && self.compact_after_n_flushes > 0 {
            self.flush_count_since_last_compact += 1;
            if self.flush_count_since_last_compact >= self.compact_after_n_flushes
                && self.segments.len() >= 2
            {
                let _ = self.start_bg_compact();
            }
        }

        Ok(Some(seg_info))
    }

    /// Check if the memtable should be flushed based on size threshold.
    /// Called automatically after writes when flush_threshold > 0.
    fn maybe_auto_flush(&mut self) -> Result<(), EngineError> {
        if self.flush_threshold > 0 && self.memtable.estimated_size() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    /// Check if the memtable has reached the hard cap. If so, trigger a
    /// synchronous flush before allowing the next write to proceed.
    /// This is the backpressure mechanism: writes "block" (flush inline) when
    /// the memtable is at capacity, preventing unbounded memory growth.
    fn maybe_backpressure_flush(&mut self) -> Result<(), EngineError> {
        if self.memtable_hard_cap > 0 && self.memtable.estimated_size() >= self.memtable_hard_cap {
            self.flush()?;
        }
        Ok(())
    }

    // --- Background compaction ---

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
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = Arc::clone(&cancel);
        let handle = std::thread::spawn(move || {
            bg_compact_worker(
                db_dir,
                seg_id,
                input_segments,
                prune_policies,
                &cancel_clone,
            )
        });

        self.bg_compact = Some(BgCompactHandle { handle, cancel });

        Ok(())
    }

    /// Non-blocking check: if a background compaction has finished, apply its result.
    fn try_complete_bg_compact(&mut self) -> Option<CompactionStats> {
        let is_finished = self
            .bg_compact
            .as_ref()
            .is_some_and(|bg| bg.handle.is_finished());
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
        // Build new manifest: remove compacted input segments, add output.
        let mut new_manifest = self.manifest.clone();
        new_manifest
            .segments
            .retain(|s| !result.input_segment_ids.contains(&s.id));
        new_manifest.segments.push(result.seg_info.clone());

        if let Err(e) = write_manifest(&self.db_dir, &new_manifest) {
            eprintln!("Background compaction: manifest write failed: {}", e);
            // Output segment exists on disk but manifest doesn't reference it.
            // Old segments are still intact, so no data loss. Clean up orphan.
            let output_dir = segment_dir(&self.db_dir, result.stats.output_segment_id);
            let _ = std::fs::remove_dir_all(output_dir);
            return None;
        }

        // Manifest is durable. Swap in-memory state.
        self.manifest = new_manifest;
        // Remove input segments, keep any new segments added by flushes during
        // background compaction (they have different IDs).
        self.segments
            .retain(|s| !result.input_segment_ids.contains(&s.segment_id));
        // Compacted segment is oldest; push to end (segments are newest-first).
        self.segments.push(result.reader);

        // Delete old segment directories (best-effort, after mmap handles released).
        for dir in &result.old_seg_dirs {
            let _ = std::fs::remove_dir_all(dir);
        }

        self.last_compaction_ms = Some(now_millis());

        // Rebuild degree cache — segments changed during background compaction
        if let Err(e) = self.rebuild_degree_cache() {
            eprintln!("Background compaction: degree cache rebuild failed: {}", e);
        }

        Some(result.stats)
    }

    // --- Compaction ---

    /// Enter ingest mode: disables auto-compaction so bulk writes produce
    /// segments without triggering background merges. Call `end_ingest` when
    /// loading is complete to compact and restore normal operation.
    pub fn ingest_mode(&mut self) {
        if self.ingest_saved_compact_after_n_flushes.is_none() {
            self.ingest_saved_compact_after_n_flushes = Some(self.compact_after_n_flushes);
        }
        self.compact_after_n_flushes = 0;
    }

    /// Exit ingest mode: restores the previous auto-compaction threshold and
    /// immediately compacts all accumulated segments.
    pub fn end_ingest(&mut self) -> Result<Option<CompactionStats>, EngineError> {
        if let Some(previous) = self.ingest_saved_compact_after_n_flushes.take() {
            self.compact_after_n_flushes = previous;
        }
        self.compact()
    }

    /// Compact all segments into a single segment.
    ///
    /// Convenience wrapper around `compact_with_progress` that never cancels.
    pub fn compact(&mut self) -> Result<Option<CompactionStats>, EngineError> {
        self.compact_with_progress(|_| true)
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
        // Wait for any in-progress background compaction before proceeding.
        self.wait_for_bg_compact();

        if self.segments.len() < 2 {
            return Ok(None);
        }

        self.compacting = true;
        let result = self.compact_with_progress_inner(&mut callback);
        self.compacting = false;
        // Reset flush counter after any compaction attempt (success or failure)
        self.flush_count_since_last_compact = 0;

        // Rebuild degree cache after successful compaction — segments have changed
        if let Ok(Some(_)) = &result {
            self.rebuild_degree_cache()?;
        }

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

        // Flush memtable first so compaction sees all data and tombstones
        if !self.memtable.is_empty() {
            self.flush()?;
        }

        // Count input records for stats
        let input_segment_count = self.segments.len();
        let total_input_nodes: u64 = self.segments.iter().map(|s| s.node_count()).sum();
        let total_input_edges: u64 = self.segments.iter().map(|s| s.edge_count()).sum();

        let has_tombstones = self.segments.iter().any(|s| s.has_tombstones());
        let policies: Vec<PrunePolicy> = self.manifest.prune_policies.values().cloned().collect();
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

        let (seg_info, nodes_auto_pruned, edges_auto_pruned) = match compaction_path {
            CompactionPath::FastMerge => match self.compact_fast_merge(
                &tmp_dir,
                seg_id,
                callback,
                input_segment_count,
                total_input_nodes,
                total_input_edges,
            ) {
                Ok(seg_info) => (seg_info, 0, 0),
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
        let new_reader = match SegmentReader::open(&final_dir, seg_id) {
            Ok(r) => r,
            Err(e) => {
                // Output segment exists on disk but we can't read it.
                // Clean up orphan directory and release the segment ID.
                self.next_segment_id -= 1;
                let _ = std::fs::remove_dir_all(&final_dir);
                return Err(e);
            }
        };

        // Collect old segment info for cleanup
        let old_seg_ids: HashSet<u64> = self.segments.iter().map(|s| s.segment_id).collect();
        let old_seg_dirs: Vec<PathBuf> = old_seg_ids
            .iter()
            .map(|&id| segment_dir(&self.db_dir, id))
            .collect();

        // Build new manifest before mutating in-memory state (M2 fix)
        let mut new_manifest = self.manifest.clone();
        new_manifest
            .segments
            .retain(|s| !old_seg_ids.contains(&s.id));
        new_manifest.segments.push(seg_info.clone());
        write_manifest(&self.db_dir, &new_manifest)?;

        // Manifest is durable. Now safe to swap in-memory state
        self.manifest = new_manifest;
        self.segments.clear();
        self.segments.push(new_reader);

        // Delete old segment directories (best-effort, after mmap handles released)
        for dir in &old_seg_dirs {
            let _ = std::fs::remove_dir_all(dir);
        }

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
    fn compact_fast_merge<F>(
        &self,
        tmp_dir: &Path,
        seg_id: u64,
        callback: &mut F,
        input_segment_count: usize,
        total_input_nodes: u64,
        total_input_edges: u64,
    ) -> Result<SegmentInfo, EngineError>
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

        build_fast_merge_output(tmp_dir, seg_id, &self.segments)
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
        input_segment_count: usize,
        total_input_nodes: u64,
        total_input_edges: u64,
    ) -> Result<(SegmentInfo, u64, u64), EngineError>
    where
        F: FnMut(&CompactionProgress) -> bool,
    {
        // --- Phase 1: Collect tombstones ---
        let mut deleted_nodes: HashSet<u64> = HashSet::new();
        let mut deleted_edges: HashSet<u64> = HashSet::new();
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

        let seg_info = v3_build_output(tmp_dir, seg_id, &self.segments, &plan)?;

        Ok((seg_info, nodes_auto_pruned, edges_auto_pruned))
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
        DbStats {
            pending_wal_bytes,
            segment_count: self.segments.len(),
            node_tombstone_count: self.memtable.deleted_nodes().len(),
            edge_tombstone_count: self.memtable.deleted_edges().len(),
            last_compaction_ms: self.last_compaction_ms,
            wal_sync_mode: sync_mode_str,
        }
    }

    // --- Low-level WAL access (kept for backward compatibility and tests) ---

    /// Write a WalOp to the WAL and apply it to in-memory state.
    /// Note: Does not trigger auto-flush or backpressure. Use the high-level
    /// graph APIs for automatic flushing and memtable size management.
    pub fn write_op(&mut self, op: &WalOp) -> Result<(), EngineError> {
        self.append_and_apply_one(op)?;
        self.track_id(op);
        Ok(())
    }

    /// Write multiple WalOps with a single fsync at the end.
    /// Note: Does not trigger auto-flush or backpressure. Use the high-level
    /// graph APIs for automatic flushing and memtable size management.
    pub fn write_op_batch(&mut self, ops: &[WalOp]) -> Result<(), EngineError> {
        self.append_and_apply(ops)?;
        for op in ops {
            self.track_id(op);
        }
        Ok(())
    }

    /// Advance running ID counters if the op contains an ID >= next_*_id.
    /// Needed for the low-level write_op API where IDs are caller-assigned.
    fn track_id(&mut self, op: &WalOp) {
        match op {
            WalOp::UpsertNode(node) => {
                if node.id >= self.next_node_id {
                    self.next_node_id = node.id + 1;
                }
            }
            WalOp::UpsertEdge(edge) => {
                if edge.id >= self.next_edge_id {
                    self.next_edge_id = edge.id + 1;
                }
            }
            _ => {}
        }
    }

    // --- Accessors ---

    /// Return the database directory path.
    pub fn path(&self) -> &Path {
        &self.db_dir
    }

    /// Return a reference to the current manifest state.
    pub fn manifest(&self) -> &ManifestState {
        &self.manifest
    }

    /// Return the count of live nodes in memtable only.
    pub fn node_count(&self) -> usize {
        self.memtable.node_count()
    }

    /// Return the count of live edges in memtable only.
    pub fn edge_count(&self) -> usize {
        self.memtable.edge_count()
    }

    /// Return the next node ID that will be allocated.
    pub fn next_node_id(&self) -> u64 {
        self.next_node_id
    }

    /// Return the next edge ID that will be allocated.
    pub fn next_edge_id(&self) -> u64 {
        self.next_edge_id
    }

    /// Return the number of open segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Return the total number of tombstoned node IDs across all segments.
    pub fn segment_tombstone_node_count(&self) -> usize {
        self.segments
            .iter()
            .map(|s| s.deleted_node_ids().len())
            .sum()
    }

    /// Return the total number of tombstoned edge IDs across all segments.
    pub fn segment_tombstone_edge_count(&self) -> usize {
        self.segments
            .iter()
            .map(|s| s.deleted_edge_ids().len())
            .sum()
    }
}

impl Drop for DatabaseEngine {
    fn drop(&mut self) {
        // Best-effort: wait for background compaction to finish.
        // Errors are swallowed. Drop must not panic.
        self.wait_for_bg_compact();

        // Best-effort: shut down sync thread and flush buffered data.
        if self.sync_thread.is_some() {
            if let Some(ref wal_state) = self.wal_state {
                let _ = shutdown_sync_thread(wal_state, &mut self.sync_thread);
            }
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
    let manifest_ids: HashSet<u64> = manifest.segments.iter().map(|s| s.id).collect();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id_str) = name.strip_prefix("seg_") {
            if let Ok(id) = id_str.parse::<u64>() {
                if !manifest_ids.contains(&id) {
                    // Also skip tmp dirs (seg_XXXX.tmp). strip_prefix won't match those
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
    }
}

/// Test helper: access the degree cache for validation.
#[cfg(test)]
impl DatabaseEngine {
    pub(crate) fn degree_cache_entry(&self, node_id: u64) -> DegreeEntry {
        self.degree_cache
            .get(&node_id)
            .copied()
            .unwrap_or(DegreeEntry::ZERO)
    }
}

/// Check if all segments have non-overlapping node and edge ID ranges.
/// Returns true when every record ID appears in at most one segment,
/// the common case for append-only workloads without updates.
fn segments_are_non_overlapping(segments: &[SegmentReader]) -> bool {
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
    segments: &[SegmentReader],
    has_tombstones: bool,
    has_active_prune_policies: bool,
) -> CompactionPath {
    if !has_tombstones && !has_active_prune_policies && segments_are_non_overlapping(segments) {
        CompactionPath::FastMerge
    } else {
        CompactionPath::UnifiedV3
    }
}

/// Background compaction worker. Runs on a spawned thread.
/// Re-opens input segments via independent mmap handles, merges them into a
/// single output segment, and returns the result for the main thread to apply.
fn bg_compact_worker(
    db_dir: PathBuf,
    seg_id: u64,
    input_segments: Vec<(u64, PathBuf)>,
    prune_policies: Vec<PrunePolicy>,
    cancel: &AtomicBool,
) -> Result<BgCompactResult, EngineError> {
    let compact_start = std::time::Instant::now();

    // Re-open input segments (independent mmap handles, safe to use concurrently
    // with the main thread's readers of the same files).
    let mut segments = Vec::with_capacity(input_segments.len());
    for (id, path) in &input_segments {
        segments.push(SegmentReader::open(path, *id)?);
    }

    let input_segment_count = segments.len();
    let total_input_nodes: u64 = segments.iter().map(|s| s.node_count()).sum();
    let total_input_edges: u64 = segments.iter().map(|s| s.edge_count()).sum();

    let has_tombstones = segments.iter().any(|s| s.has_tombstones());
    let compaction_path =
        select_compaction_path(&segments, has_tombstones, !prune_policies.is_empty());

    let segments_dir = db_dir.join("segments");
    std::fs::create_dir_all(&segments_dir)?;
    let tmp_dir = segment_tmp_dir(&db_dir, seg_id);
    let final_dir = segment_dir(&db_dir, seg_id);

    let (seg_info, nodes_auto_pruned, edges_auto_pruned) = match compaction_path {
        CompactionPath::FastMerge => match bg_fast_merge(&segments, &tmp_dir, seg_id, cancel) {
            Ok(seg_info) => (seg_info, 0, 0),
            Err(e) => {
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(e);
            }
        },
        CompactionPath::UnifiedV3 => match bg_standard_merge(
            &segments,
            &tmp_dir,
            seg_id,
            has_tombstones,
            &prune_policies,
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
    let reader = match SegmentReader::open(&final_dir, seg_id) {
        Ok(r) => r,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(e);
        }
    };

    let input_segment_ids: HashSet<u64> = input_segments.iter().map(|(id, _)| *id).collect();
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
    prop_hash_offset: u64,
    prop_hash_count: u32,
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
}

/// Result of V3 compaction planning: which records survive and pruning stats.
struct V3Plan {
    node_winners: BTreeMap<u64, NodeWinner>,
    edge_winners: BTreeMap<u64, EdgeWinner>,
    pruned_node_ids: HashSet<u64>,
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
    segments: &[SegmentReader],
    prune_policies: &[PrunePolicy],
    deleted_nodes: &HashSet<u64>,
    deleted_edges: &HashSet<u64>,
) -> Result<V3Plan, EngineError> {
    let now = now_millis();
    let has_policies = !prune_policies.is_empty();

    // --- Select winning nodes from metadata (newest-first, first seen wins) ---
    let mut node_winners: BTreeMap<u64, NodeWinner> = BTreeMap::new();
    let mut pruned_node_ids: HashSet<u64> = HashSet::new();
    let mut seen_nodes: HashSet<u64> = HashSet::new();

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
                prop_hash_offset,
                prop_hash_count,
            ) = seg.node_meta_at(i)?;

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
                    prop_hash_offset,
                    prop_hash_count,
                },
            );
        }
    }

    // --- Select winning edges from metadata ---
    // O(1) endpoint lookup for edge cascade filtering (BTreeMap.contains_key is O(log N))
    let surviving_node_ids: HashSet<u64> = node_winners.keys().copied().collect();
    let mut edge_winners: BTreeMap<u64, EdgeWinner> = BTreeMap::new();
    let mut seen_edges: HashSet<u64> = HashSet::new();
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
    segments: &[SegmentReader],
    plan: &V3Plan,
) -> Result<SegmentInfo, EngineError> {
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
            prop_hash_offset: w.prop_hash_offset,
            prop_hash_count: w.prop_hash_count,
            src_seg_idx: w.seg_idx,
            src_data_offset: w.data_offset,
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
        });
    }

    // Build all secondary indexes and sidecars from metadata
    write_indexes_from_metadata(tmp_dir, segments, &node_metas, &edge_metas)?;

    let node_count = plan.node_winners.len() as u64;
    let edge_count = plan.edge_winners.len() as u64;

    Ok(SegmentInfo {
        id: seg_id,
        node_count,
        edge_count,
    })
}

fn collect_fast_merge_node_metas(
    segments: &[SegmentReader],
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
                prop_hash_offset,
                prop_hash_count,
            ) = seg.node_meta_at(i)?;
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
                prop_hash_offset,
                prop_hash_count,
                src_seg_idx: seg_idx,
                src_data_offset: data_offset,
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
    segments: &[SegmentReader],
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
    segments: &[SegmentReader],
) -> Result<SegmentInfo, EngineError> {
    std::fs::create_dir_all(tmp_dir)?;

    let node_copy_info = write_merged_nodes_dat(tmp_dir, segments)?;
    let edge_copy_info = write_merged_edges_dat(tmp_dir, segments)?;
    let node_metas = collect_fast_merge_node_metas(segments, &node_copy_info)?;
    let edge_metas = collect_fast_merge_edge_metas(segments, &edge_copy_info)?;

    write_indexes_from_metadata(tmp_dir, segments, &node_metas, &edge_metas)?;

    Ok(SegmentInfo {
        id: seg_id,
        node_count: node_metas.len() as u64,
        edge_count: edge_metas.len() as u64,
    })
}

/// V3 background merge: metadata-only planning + raw binary copy.
/// Same algorithm as compact_standard but with cancel flag instead of progress callback.
/// Returns `(SegmentInfo, nodes_auto_pruned, edges_auto_pruned)`.
fn bg_fast_merge(
    segments: &[SegmentReader],
    tmp_dir: &Path,
    seg_id: u64,
    cancel: &AtomicBool,
) -> Result<SegmentInfo, EngineError> {
    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    let seg_info = build_fast_merge_output(tmp_dir, seg_id, segments)?;

    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    Ok(seg_info)
}

/// V3 background merge: metadata-only planning + raw binary copy.
/// Same algorithm as compact_standard but with cancel flag instead of progress callback.
/// Returns `(SegmentInfo, nodes_auto_pruned, edges_auto_pruned)`.
fn bg_standard_merge(
    segments: &[SegmentReader],
    tmp_dir: &Path,
    seg_id: u64,
    has_tombstones: bool,
    prune_policies: &[PrunePolicy],
    cancel: &AtomicBool,
) -> Result<(SegmentInfo, u64, u64), EngineError> {
    // Collect tombstones
    let mut deleted_nodes: HashSet<u64> = HashSet::new();
    let mut deleted_edges: HashSet<u64> = HashSet::new();
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

    let seg_info = v3_build_output(tmp_dir, seg_id, segments, &plan)?;

    Ok((seg_info, nodes_auto_pruned, edges_auto_pruned))
}

include!("graph_ops.rs");
include!("write.rs");
include!("read.rs");

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
        }
    }

    include!("tests/lifecycle.rs");
    include!("tests/write.rs");
    include!("tests/read.rs");
    include!("tests/graph_ops.rs");
}
